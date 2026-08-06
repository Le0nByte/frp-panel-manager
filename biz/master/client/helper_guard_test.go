package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
	"gorm.io/gorm"
)

// ---- doubles ---------------------------------------------------------------

var guardHelperDSNCounter uint64

// auditRecorder is the rpcguard.SystemAuditWriter test double for this
// package: it collects the fixed-structure system rows drained by Flush.
type auditRecorder struct {
	events []*models.AuditEvent
}

func (w *auditRecorder) LogSystem(ev *models.AuditEvent) error {
	w.events = append(w.events, ev)
	return nil
}

// guardHelperEnv wires a guard + (optionally) a real in-memory SQLite DB
// into an app, mirroring the production layering: peer ctx -> guard ctx ->
// app ctx.
type guardHelperEnv struct {
	t     *testing.T
	app   app.Application
	db    *gorm.DB
	guard *rpcguard.Guard
}

func newGuardHelperEnv(t *testing.T, failThreshold int) *guardHelperEnv {
	t.Helper()
	n := atomic.AddUint64(&guardHelperDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:guard-helper-client-%d-%s?mode=memory&cache=shared", n, name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.Client{}, &models.Server{}))
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)

	a := app.NewApp()
	a.SetDBManager(dbm)

	cfg := rpcguard.DefaultConfig()
	cfg.SecretFailureThreshold = failThreshold
	cfg.DistinctSecretFailThreshold = 100 // single node under test: never IP-lock
	cfg.SecretKeyLockDuration = 15 * time.Minute
	g := rpcguard.NewGuard(rpcguard.WithConfig(cfg))

	return &guardHelperEnv{t: t, app: a, db: db, guard: g}
}

// noDBEnv builds an app WITHOUT a DB manager: any DAO touch panics on
// GetDefaultDB, so tests prove the guard rejects before the DAO.
func noDBEnv(t *testing.T, failThreshold int) *guardHelperEnv {
	t.Helper()
	cfg := rpcguard.DefaultConfig()
	cfg.SecretFailureThreshold = failThreshold
	cfg.DistinctSecretFailThreshold = 100
	g := rpcguard.NewGuard(rpcguard.WithConfig(cfg))
	return &guardHelperEnv{t: t, app: app.NewApp(), guard: g}
}

// guardedClientCtx wraps peerCtx with the guard and the app, exactly like
// the master service does per inbound RPC.
func (e *guardHelperEnv) ctx(ip string) *app.Context {
	p := &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 7000}}
	base := peer.NewContext(context.Background(), p)
	return app.NewContext(rpcguard.ContextWithGuard(base, e.guard), e.app)
}

// seedClient inserts a client row directly (no DAO needed).
func (e *guardHelperEnv) seedClient(id, secret string) {
	e.t.Helper()
	require.NoError(e.t, e.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID:      id,
		ConnectSecret: secret,
	}}).Error)
}

// flush counts the secret-failure audit rows drained from the guard.
func (e *guardHelperEnv) flush() (clientFailures, total int) {
	e.t.Helper()
	w := &auditRecorder{}
	require.NoError(e.t, e.guard.Flush(w))
	for _, ev := range w.events {
		if ev.EventType == models.AuditEventTypeRPCSecretFailure {
			total++
			if strings.Contains(ev.Summary, "layer=client") {
				clientFailures++
			}
		}
	}
	return
}

// ---- behavior --------------------------------------------------------------

// TestValidateClientRequestEmptyRecordsFailure: an empty id/secret is a
// real secret-validation failure — typed error AND a guard failure record
// (layer=client), never a generic fmt error.
func TestValidateClientRequestEmptyRecordsFailure(t *testing.T) {
	e := newGuardHelperEnv(t, 5)
	ctx := e.ctx("203.0.113.10")

	_, err := ValidateClientRequest(ctx, &pb.ClientBase{})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "empty base must return the typed sentinel")
	cf, total := e.flush()
	require.Equal(t, 1, cf, "empty secret must be recorded as a client-layer failure")
	require.Equal(t, 1, total)

	// id present, secret empty: failure recorded against that node.
	_, err = ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a"})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret)
	cf, _ = e.flush()
	require.Equal(t, 1, cf, "empty secret with id must record the node failure")
}

// TestValidateClientRequestNilRecordsFailure: a nil request records a
// failure with an empty node and returns the typed sentinel.
func TestValidateClientRequestNilRecordsFailure(t *testing.T) {
	e := newGuardHelperEnv(t, 5)
	ctx := e.ctx("203.0.113.10")

	_, err := ValidateClientRequest(ctx, nil)
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret)
	cf, total := e.flush()
	require.Equal(t, 1, cf)
	require.Equal(t, 1, total)
}

// TestValidateClientRequestMismatchLocksKeyAndBlocksBeforeDAO: with
// threshold 1 a mismatch locks the (IP,node) key; the next attempt is
// rejected by the guard BEFORE any DAO access — an app without a DB
// manager would panic if the DAO were reached.
func TestValidateClientRequestMismatchLocksKeyAndBlocksBeforeDAO(t *testing.T) {
	e := newGuardHelperEnv(t, 1)
	e.seedClient("cli-a", "good-secret")
	ctx := e.ctx("203.0.113.11")

	_, err := ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "mismatch must be the typed sentinel")
	require.False(t, e.guard.AllowSecretAttempt("203.0.113.11:7000", "cli-a"), "threshold 1 must lock the key")

	// Now the key is locked. A CORRECT secret from the same peer must be
	// rejected before the DAO (no DB manager => panic iff touched).
	e2 := noDBEnv(t, 1)
	// Pre-lock the guard the same way the mismatch path does.
	e2.guard.RecordSecretFailure("203.0.113.11:7000", "cli-a", rpcguard.LayerClient)
	ctx2 := e2.ctx("203.0.113.11")
	require.NotPanics(t, func() {
		_, err = ValidateClientRequest(ctx2, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "good-secret"})
	}, "locked key must be rejected before any DAO access")
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "locked attempt must return the typed sentinel")
	require.False(t, e2.guard.AllowSecretAttempt("203.0.113.11:7000", "cli-a"))
}

// TestValidateClientRequestNotFoundCountsFailure: a missing row is
// gorm.ErrRecordNotFound and counts as a secret failure.
func TestValidateClientRequestNotFoundCountsFailure(t *testing.T) {
	e := newGuardHelperEnv(t, 5)
	ctx := e.ctx("203.0.113.12")

	_, err := ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "ghost", ClientSecret: "x"})
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "missing row must pass through as record-not-found")
	cf, _ := e.flush()
	require.Equal(t, 1, cf, "not-found must count as a failure")
}

// TestValidateClientRequestTransientDBErrorNotCounted: a closed database
// ("sql: database is closed") is a transient DB error — the raw error is
// returned but NEVER counted as a secret failure and never locks.
func TestValidateClientRequestTransientDBErrorNotCounted(t *testing.T) {
	e := newGuardHelperEnv(t, 1)
	e.seedClient("cli-a", "good-secret")
	sqlDB, err := e.db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	ctx := e.ctx("203.0.113.13")
	_, err = ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "good-secret"})
	require.Error(t, err)
	require.NotErrorIs(t, err, dao.ErrInvalidClientSecret, "transient DB error is not a secret failure")

	cf, total := e.flush()
	require.Zero(t, cf, "transient DB errors must never count as secret failures")
	require.Zero(t, total)
	require.True(t, e.guard.AllowSecretAttempt("203.0.113.13:7000", "cli-a"), "no failure => no lock")
}

// TestValidateClientRequestSuccessClearsFailureState: a success clears
// the failure counter (RecordSecretSuccess), so an earlier failure never
// carries into a later streak and never triggers a lock.
func TestValidateClientRequestSuccessClearsFailureState(t *testing.T) {
	e := newGuardHelperEnv(t, 2)
	e.seedClient("cli-a", "good-secret")
	ctx := e.ctx("203.0.113.14")

	_, err := ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret)
	require.True(t, e.guard.AllowSecretAttempt("203.0.113.14:7000", "cli-a"), "count 1 < threshold 2: not locked yet")

	cli, err := ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "good-secret"})
	require.NoError(t, err)
	require.Equal(t, "cli-a", cli.ClientID)

	cf, _ := e.flush()
	require.Equal(t, 1, cf, "only the single failure is audited")

	// The success cleared the counter: a new failure restarts at 1 and
	// does NOT lock (if it had carried, count would be 2 => lock).
	_, err = ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret)
	require.True(t, e.guard.AllowSecretAttempt("203.0.113.14:7000", "cli-a"), "cleared failure state must not accumulate into a lock")
}

// TestValidateClientRequestNoGuardKeepsTypedBehavior: without a guard in
// the context the helper keeps functional behavior (typed errors) and
// never panics.
func TestValidateClientRequestNoGuardKeepsTypedBehavior(t *testing.T) {
	e := newGuardHelperEnv(t, 1)
	e.seedClient("cli-a", "good-secret")
	ctx := app.NewContext(context.Background(), e.app) // no guard, no peer

	_, err := ValidateClientRequest(ctx, &pb.ClientBase{})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret, "no guard: empty still returns the typed sentinel")

	_, err = ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "cli-a", ClientSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidClientSecret)

	_, err = ValidateClientRequest(ctx, &pb.ClientBase{ClientId: "ghost", ClientSecret: "x"})
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound), "not-found must pass through unchanged")
}
