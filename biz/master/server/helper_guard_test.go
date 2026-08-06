package server

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

type auditRecorder struct {
	events []*models.AuditEvent
}

func (w *auditRecorder) LogSystem(ev *models.AuditEvent) error {
	w.events = append(w.events, ev)
	return nil
}

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
	dsn := fmt.Sprintf("file:guard-helper-server-%d-%s?mode=memory&cache=shared", n, name)
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
	cfg.DistinctSecretFailThreshold = 100
	cfg.SecretKeyLockDuration = 15 * time.Minute
	g := rpcguard.NewGuard(rpcguard.WithConfig(cfg))

	return &guardHelperEnv{t: t, app: a, db: db, guard: g}
}

func noDBEnv(t *testing.T, failThreshold int) *guardHelperEnv {
	t.Helper()
	cfg := rpcguard.DefaultConfig()
	cfg.SecretFailureThreshold = failThreshold
	cfg.DistinctSecretFailThreshold = 100
	g := rpcguard.NewGuard(rpcguard.WithConfig(cfg))
	return &guardHelperEnv{t: t, app: app.NewApp(), guard: g}
}

func (e *guardHelperEnv) ctx(ip string) *app.Context {
	p := &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 7000}}
	base := peer.NewContext(context.Background(), p)
	return app.NewContext(rpcguard.ContextWithGuard(base, e.guard), e.app)
}

func (e *guardHelperEnv) seedServer(id, secret string) {
	e.t.Helper()
	require.NoError(e.t, e.db.Create(&models.Server{ServerEntity: &models.ServerEntity{
		ServerID:      id,
		ConnectSecret: secret,
	}}).Error)
}

// flush counts the secret-failure audit rows drained from the guard.
func (e *guardHelperEnv) flush() (serverFailures, total int) {
	e.t.Helper()
	w := &auditRecorder{}
	require.NoError(e.t, e.guard.Flush(w))
	for _, ev := range w.events {
		if ev.EventType == models.AuditEventTypeRPCSecretFailure {
			total++
			if strings.Contains(ev.Summary, "layer=server") {
				serverFailures++
			}
		}
	}
	return
}

// ---- behavior --------------------------------------------------------------

func TestValidateServerRequestEmptyRecordsFailure(t *testing.T) {
	e := newGuardHelperEnv(t, 5)
	ctx := e.ctx("203.0.113.20")

	_, err := ValidateServerRequest(ctx, &pb.ServerBase{})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret, "empty base must return the typed sentinel")
	sf, total := e.flush()
	require.Equal(t, 1, sf, "empty secret must be recorded as a server-layer failure")
	require.Equal(t, 1, total)

	_, err = ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a"})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
	sf, _ = e.flush()
	require.Equal(t, 1, sf, "empty secret with id must record the node failure")
}

func TestValidateServerRequestNilRecordsFailure(t *testing.T) {
	e := newGuardHelperEnv(t, 5)
	ctx := e.ctx("203.0.113.20")

	_, err := ValidateServerRequest(ctx, nil)
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
	sf, total := e.flush()
	require.Equal(t, 1, sf)
	require.Equal(t, 1, total)
}

func TestValidateServerRequestMismatchLocksKeyAndBlocksBeforeDAO(t *testing.T) {
	e := newGuardHelperEnv(t, 1)
	e.seedServer("srv-a", "good-secret")
	ctx := e.ctx("203.0.113.21")

	_, err := ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
	require.False(t, e.guard.AllowSecretAttempt("203.0.113.21:7000", "srv-a"), "threshold 1 must lock the key")

	e2 := noDBEnv(t, 1)
	e2.guard.RecordSecretFailure("203.0.113.21:7000", "srv-a", rpcguard.LayerServer)
	ctx2 := e2.ctx("203.0.113.21")
	require.NotPanics(t, func() {
		_, err = ValidateServerRequest(ctx2, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "good-secret"})
	}, "locked key must be rejected before any DAO access")
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
	require.False(t, e2.guard.AllowSecretAttempt("203.0.113.21:7000", "srv-a"))
}

func TestValidateServerRequestNotFoundCountsFailure(t *testing.T) {
	e := newGuardHelperEnv(t, 5)
	ctx := e.ctx("203.0.113.22")

	_, err := ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "ghost", ServerSecret: "x"})
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	sf, _ := e.flush()
	require.Equal(t, 1, sf, "not-found must count as a failure")
}

func TestValidateServerRequestTransientDBErrorNotCounted(t *testing.T) {
	e := newGuardHelperEnv(t, 1)
	e.seedServer("srv-a", "good-secret")
	sqlDB, err := e.db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	ctx := e.ctx("203.0.113.23")
	_, err = ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "good-secret"})
	require.Error(t, err)
	require.NotErrorIs(t, err, dao.ErrInvalidServerSecret, "transient DB error is not a secret failure")

	sf, total := e.flush()
	require.Zero(t, sf, "transient DB errors must never count as secret failures")
	require.Zero(t, total)
	require.True(t, e.guard.AllowSecretAttempt("203.0.113.23:7000", "srv-a"))
}

func TestValidateServerRequestSuccessClearsFailureState(t *testing.T) {
	e := newGuardHelperEnv(t, 2)
	e.seedServer("srv-a", "good-secret")
	ctx := e.ctx("203.0.113.24")

	_, err := ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
	require.True(t, e.guard.AllowSecretAttempt("203.0.113.24:7000", "srv-a"), "count 1 < threshold 2: not locked yet")

	srv, err := ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "good-secret"})
	require.NoError(t, err)
	require.Equal(t, "srv-a", srv.ServerID)

	sf, _ := e.flush()
	require.Equal(t, 1, sf, "only the single failure is audited")

	_, err = ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)
	require.True(t, e.guard.AllowSecretAttempt("203.0.113.24:7000", "srv-a"), "cleared failure state must not accumulate into a lock")
}

func TestValidateServerRequestNoGuardKeepsTypedBehavior(t *testing.T) {
	e := newGuardHelperEnv(t, 1)
	e.seedServer("srv-a", "good-secret")
	ctx := app.NewContext(context.Background(), e.app) // no guard, no peer

	_, err := ValidateServerRequest(ctx, &pb.ServerBase{})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret, "no guard: empty still returns the typed sentinel")

	_, err = ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "srv-a", ServerSecret: "wrong"})
	require.ErrorIs(t, err, dao.ErrInvalidServerSecret)

	_, err = ValidateServerRequest(ctx, &pb.ServerBase{ServerId: "ghost", ServerSecret: "x"})
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound), "not-found must pass through unchanged")
}
