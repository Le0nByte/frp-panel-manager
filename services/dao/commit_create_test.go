package dao_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var commitDSNCounter uint64

// commitTestDSN isolates every CommitCreate test with a run-unique shared
// in-memory SQLite database (monotonic counter + test name — the *testing.T
// pointer alone is NOT unique across runs, see the dao test-helper note).
func commitTestDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&commitDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:commit-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

// newCommitTestEnv builds the full port-manager schema (clients +
// proxy_config included — setupTestApp migrates only the port tables).
func newCommitTestEnv(t *testing.T) (*app.Context, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(commitTestDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.Client{},
		&models.ProxyConfig{},
		&models.PortReservation{},
		&models.ChangeTransaction{},
		&models.ConfigSnapshot{},
		&models.AuditEvent{},
	))
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst), db
}

// clientConfigBytes builds a serializable client config carrying the named
// TCP proxies (same shape SetConfigContent produces).
func clientConfigBytes(t *testing.T, proxyNames ...string) []byte {
	t.Helper()
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	for _, name := range proxyNames {
		p := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: name, Type: "tcp"}, RemotePort: 8000}
		p.LocalIP = "127.0.0.1"
		p.LocalPort = 8080
		cfg.Proxies = append(cfg.Proxies, v1.TypedProxyConfig{ProxyConfigurer: p})
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	return raw
}

// TestCommitCreateExistingChildMissingFailsAndRollsBack is the I1 RED/GREEN
// contract: when the target child row disappeared between plan and commit
// (concurrent legacy hard delete, I1 in the round-8 quality review),
// CommitCreate must return a stable error and the WHOLE transaction must
// roll back — origin/client rows and proxy rows byte-identical, the
// reservation still pending, the transaction still in verifying, and no
// after snapshot or audit rows. A silent 0-row UPDATE that "succeeds"
// would leave orphan proxy rows and a permanently blocked port.
func TestCommitCreateExistingChildMissingFailsAndRollsBack(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	user := testUserInfo()
	now := time.Now()

	const (
		originID = "cli-origin"
		childID  = "cli-child"
		txID     = "tx-existing"
		srvID    = "srv-1"
	)

	// origin: already shadowed, config cleared (existing-child flow)
	origin := &models.ClientEntity{
		ClientID: originID, IsShadow: true, ConnectSecret: "origin-secret",
	}
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, origin))

	// child: existing row carrying the before config + one proxy row
	child := &models.ClientEntity{
		ClientID: childID, OriginClientID: originID, ServerID: srvID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	child.ConfigContent = clientConfigBytes(t, "ssh")
	require.NoError(t, dao.NewMutation(ctx).CreateClient(user, child))
	require.NoError(t, db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: childID, Name: "ssh", TenantID: user.GetTenantID(), UserID: user.GetUserID(),
	}}).Error)

	// the transaction row is mid-flight (verifying) with a pending lease
	require.NoError(t, db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: "idem-existing", RequestHash: "h",
		Operation: models.TxOperationCreate, Status: models.TxStatusVerifying,
		ActorUserID: user.GetUserID(), TenantID: user.GetTenantID(), StartedAt: now,
	}).Error)
	require.NoError(t, db.Create(&models.PortReservation{
		TenantID: user.GetTenantID(), ServerID: srvID, Protocol: "tcp", RemotePort: 8000,
		TransactionID: txID, Status: models.ReservationStatusPending, ExpiresAt: now.Add(time.Minute),
	}).Error)

	beforeOrigin := captureClientRow(t, db, originID)
	beforeProxies := captureCommitProxies(t, db)

	// the concurrent legacy DeleteClientHandler hard-deletes the child row
	// (and its proxies) between plan and commit
	require.NoError(t, db.Unscoped().Where("client_id = ?", childID).Delete(&models.Client{}).Error)

	in := &dao.CommitCreateInput{
		TxID:      txID,
		ProxyName: "web",
		Client: &models.ClientEntity{
			ClientID: childID, OriginClientID: originID, ServerID: srvID,
			TenantID: user.GetTenantID(), UserID: user.GetUserID(),
			IsShadow: false, ConnectSecret: "child-secret",
			ConfigContent: clientConfigBytes(t, "ssh", "web"),
		},
		Shadow: &dao.ShadowCommit{FirstShadow: false, ChildCreated: false},
		Snapshot: dao.SnapshotInput{
			Side: models.SnapshotSideAfter, NodeType: models.SnapshotNodeTypeClient,
			NodeID: childID, Plaintext: []byte(`{"proxies":["ssh","web"]}`),
			Key: make([]byte, snapshot.KeySize),
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID, EventType: models.AuditEventTypeCommit,
			Summary: "created proxy web", Success: true,
		},
	}

	_, err := dao.NewMutation(ctx).CommitCreate(user, in)

	// 1. stable error: the missing child must never look like a success
	require.Error(t, err, "CommitCreate must fail when the existing child row is gone")
	require.True(t, errors.Is(err, defs.ErrClientNotFound),
		"missing child must surface the stable ErrClientNotFound sentinel, got: %v", err)

	// 2. the whole DB transaction rolled back: the origin row is
	// byte-identical and the concurrently-deleted child stays ABSENT (the
	// aborted commit neither resurrected nor half-updated it)
	require.Equal(t, beforeOrigin, captureClientRow(t, db, originID), "origin row must be unchanged")
	var childCount int64
	require.NoError(t, db.Model(&models.Client{}).Where("client_id = ?", childID).Count(&childCount).Error)
	require.Zero(t, childCount, "the deleted child must stay absent — the commit must not recreate it")
	require.Equal(t, beforeProxies, captureCommitProxies(t, db), "proxy rows must be unchanged")

	// 3. reservation still pending (never committed, never released)
	var res models.PortReservation
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&res).Error)
	require.Equal(t, models.ReservationStatusPending, res.Status)

	// 4. transaction still verifying (the committed transition never ran)
	var tx models.ChangeTransaction
	require.NoError(t, db.Where("transaction_id = ?", txID).First(&tx).Error)
	require.Equal(t, models.TxStatusVerifying, tx.Status)
	require.Nil(t, tx.FinishedAt, "a failed commit must not stamp finished_at")

	// 5. no after snapshot, no audit rows from the aborted commit
	var snapCount int64
	require.NoError(t, db.Model(&models.ConfigSnapshot{}).Where("transaction_id = ?", txID).Count(&snapCount).Error)
	require.Zero(t, snapCount, "no after snapshot may survive the aborted commit")
	var auditCount int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Where("transaction_id = ?", txID).Count(&auditCount).Error)
	require.Zero(t, auditCount, "no audit event may survive the aborted commit")
}

func captureClientRow(t *testing.T, db *gorm.DB, id string) models.Client {
	t.Helper()
	var c models.Client
	require.NoError(t, db.Where("client_id = ?", id).First(&c).Error)
	return c
}

func captureCommitProxies(t *testing.T, db *gorm.DB) []models.ProxyConfig {
	t.Helper()
	var proxies []models.ProxyConfig
	require.NoError(t, db.Find(&proxies).Error)
	return proxies
}
