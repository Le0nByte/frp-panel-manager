package dao_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func testUserInfo() models.UserInfo {
	return &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1}
}

var daoDSNCounter uint64

// uniqueMemoryDSN returns a per-call in-memory SQLite DSN. cache=shared is
// required so the gorm connection pool shares one database, and the unique
// name isolates every test from its neighbours (an anonymous shared-cache
// memory DB is shared process-wide, which leaks rows between tests).
// The name is a monotonic counter + the test name, NOT the *testing.T
// pointer: Go reuses T addresses across tests, so a pointer-based name can
// collide with a still-alive DB from the previous test and leak its rows.
// busy_timeout makes concurrent CAS writers wait for the winner instead of
// failing with SQLITE_BUSY, so concurrency tests observe the deterministic
// conflict sentinel.
func uniqueMemoryDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&daoDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:dao-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

// setupTestApp builds an app.Context backed by an in-memory SQLite database
// with all port-manager tables migrated.
func setupTestApp(t *testing.T) *app.Context {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(uniqueMemoryDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.PortReservation{},
		&models.ChangeTransaction{},
		&models.ConfigSnapshot{},
		&models.AuditEvent{},
	))

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)

	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst)
}

// acquireWithLifecycle is the REAL-LIFECYCLE fixture (Recovery A2): it
// creates the pending owner transaction, acquires the lease for it, then
// advances the transaction through the legal state machine to the target
// status (spec §6.2). Since TryAcquire now refuses to create a lease for
// anything but a same-tenant+actor pending owner, this is the ONLY way
// tests build leases owned by preflight/applying/…/terminal transactions —
// never a bare TryAcquire without a pending owner, and never a direct
// non-pending seed (that path is reserved for orphans, seedOrphanReservation).
func acquireWithLifecycle(t *testing.T, m dao.Mutation, user models.UserInfo, serverID, protocol string, port uint32, txID, idemKey string, lease time.Duration, now time.Time, status string) {
	t.Helper()
	require.NoError(t, m.Create(user, newTestTx(user, txID, idemKey, "h", models.TxStatusPending)))
	_, err := m.TryAcquire(user, serverID, protocol, port, txID, lease, now)
	require.NoError(t, err)
	for _, s := range txStatusPath(status) {
		require.NoError(t, m.UpdateStatus(user, txID, s, "", ""))
	}
}

// txStatusPath returns the legal status transitions (excluding the initial
// pending) needed to reach target from a freshly created pending
// transaction (spec §6.2 state machine).
func txStatusPath(target string) []string {
	switch target {
	case models.TxStatusPending:
		return nil
	case models.TxStatusPreflight:
		return []string{models.TxStatusPreflight}
	case models.TxStatusApplying:
		return []string{models.TxStatusPreflight, models.TxStatusApplying}
	case models.TxStatusVerifying:
		return []string{models.TxStatusPreflight, models.TxStatusApplying, models.TxStatusVerifying}
	case models.TxStatusRollingBack:
		return []string{models.TxStatusPreflight, models.TxStatusApplying, models.TxStatusRollingBack}
	case models.TxStatusCommitted:
		return []string{models.TxStatusPreflight, models.TxStatusApplying, models.TxStatusVerifying, models.TxStatusCommitted}
	case models.TxStatusFailed:
		return []string{models.TxStatusFailed}
	case models.TxStatusRolledBack:
		return []string{models.TxStatusPreflight, models.TxStatusApplying, models.TxStatusRollingBack, models.TxStatusRolledBack}
	case models.TxStatusRollbackFailed:
		return []string{models.TxStatusPreflight, models.TxStatusApplying, models.TxStatusRollbackFailed}
	}
	panic("txStatusPath: unknown status " + target)
}

// seedOrphanReservation inserts a reservation row DIRECTLY, bypassing
// TryAcquire. ALLOWED ONLY for orphan leases (no owning transaction row)
// and other historical anomalies the guarded TryAcquire refuses to create
// (e.g. a releasing lease on a pending owner) — never for normal lifecycle
// setups, which must use acquireWithLifecycle.
func seedOrphanReservation(t *testing.T, ctx *app.Context, tenantID int, serverID, protocol string, port uint32, txID, status string, lease time.Duration, now time.Time) {
	t.Helper()
	require.NoError(t, dbAll(ctx).Create(&models.PortReservation{
		TenantID:      tenantID,
		ServerID:      serverID,
		Protocol:      protocol,
		RemotePort:    port,
		TransactionID: txID,
		Status:        status,
		ExpiresAt:     now.Add(lease),
	}).Error)
}

// proxySpec describes one typed proxy for clientConfigWithProxies. The
// physical key of the marshalled rule is (protocol, remotePort) — the SAME
// tuple the port reservation unique index uses, so a test can build a real
// client config whose physical key provably matches a reservation key.
type proxySpec struct {
	name       string
	protocol   string // tcp / udp only
	remotePort uint32
	localPort  uint32
}

// clientConfigWithProxies builds a real, serializable v1 client config
// carrying the given typed TCP/UDP proxies with EXPLICIT remote ports — the
// physical keys the CommitUpdate S0 check derives from Client.ConfigContent
// must match the reservation keys, so fixtures can never silently run a
// different port than they lock.
func clientConfigWithProxies(t *testing.T, proxies ...proxySpec) []byte {
	t.Helper()
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	for _, p := range proxies {
		var proxy v1.ProxyConfigurer
		switch p.protocol {
		case "tcp":
			tcp := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: p.name, Type: "tcp"}}
			tcp.RemotePort = int(p.remotePort)
			tcp.LocalIP = "127.0.0.1"
			tcp.LocalPort = int(p.localPort)
			proxy = tcp
		case "udp":
			udp := &v1.UDPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: p.name, Type: "udp"}}
			udp.RemotePort = int(p.remotePort)
			udp.LocalIP = "127.0.0.1"
			udp.LocalPort = int(p.localPort)
			proxy = udp
		default:
			require.FailNow(t, "clientConfigWithProxies: unsupported protocol %q", p.protocol)
		}
		cfg.Proxies = append(cfg.Proxies, v1.TypedProxyConfig{Type: p.protocol, ProxyConfigurer: proxy})
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	return raw
}
