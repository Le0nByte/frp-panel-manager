package master

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---- environment -----------------------------------------------------------

var serverSendDSNCounter uint64

// fakeClientsManager is the ClientsManager replacement for ServerSend
// tests: Set stores the connector the rpc.Recv pump reads, Get returns it.
type fakeClientsManager struct {
	mu    sync.Mutex
	conns map[string]*defs.Connector
}

func (m *fakeClientsManager) Get(cliID string) *defs.Connector {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conns[cliID]
}

func (m *fakeClientsManager) Set(cliID, clientType string, sender pb.Master_ServerSendServer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[cliID] = &defs.Connector{CliID: cliID, Conn: sender, CliType: clientType}
}

func (m *fakeClientsManager) Remove(cliID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, cliID)
}

func (m *fakeClientsManager) ClientAddr(cliID string) string { return "" }
func (m *fakeClientsManager) ConnectTime(cliID string) (time.Time, bool) {
	return time.Time{}, false
}
func (m *fakeClientsManager) UpdateLastSeenAt(cliID string) {}
func (m *fakeClientsManager) GetLastSeenAt(cliID string) (time.Time, bool) {
	return time.Time{}, false
}

// serverSendEnv wires a server + guard + (optionally) a real SQLite DB.
// withDB=false leaves the app WITHOUT a DB manager, so any DAO touch
// panics — the failpoint that proves admission rejects happen first.
type serverSendEnv struct {
	t       *testing.T
	s       *server
	guard   *rpcguard.Guard
	app     app.Application
	db      *gorm.DB
	manager *fakeClientsManager
}

func newServerSendEnv(t *testing.T, withDB bool, cfg rpcguard.Config) *serverSendEnv {
	t.Helper()
	a := app.NewApp()
	a.SetClientRecvMap(&sync.Map{})
	mgr := &fakeClientsManager{conns: map[string]*defs.Connector{}}
	a.SetClientsManager(mgr)

	var db *gorm.DB
	if withDB {
		n := atomic.AddUint64(&serverSendDSNCounter, 1)
		dsn := fmt.Sprintf("file:master-serversend-%d-%s?mode=memory&cache=shared", n, strings.ReplaceAll(t.Name(), "/", "_"))
		var err error
		db, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{})
		require.NoError(t, err)
		require.NoError(t, db.AutoMigrate(&models.Client{}, &models.Server{}))
		if sqlDB, err := db.DB(); err == nil {
			t.Cleanup(func() { _ = sqlDB.Close() })
		}
		dbm := models.NewDBManager(defs.DBTypeSQLite3)
		dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
		a.SetDBManager(dbm)
	}

	g := rpcguard.NewGuard(rpcguard.WithConfig(cfg))
	return &serverSendEnv{t: t, s: newServer(a, g), guard: g, app: a, db: db, manager: mgr}
}

// serverSendGuardCfg returns defaults tuned for ServerSend tests.
func serverSendGuardCfg() rpcguard.Config {
	c := rpcguard.DefaultConfig()
	c.DistinctSecretFailThreshold = 100 // one node under test: never IP-lock
	return c
}

const serverSendPeer = "203.0.113.40"

// newStream builds a ServerSend fake with the shared test peer.
func newStream() *fakeServerSendStream {
	return &fakeServerSendStream{
		ctx:  peerCtx(serverSendPeer),
		recv: make(chan *pb.ClientMessage, 4),
		sent: make(chan *pb.ServerMessage, 8),
	}
}

func registerMsg(id, secret string) *pb.ClientMessage {
	return &pb.ClientMessage{Event: pb.Event_EVENT_REGISTER_CLIENT, ClientId: id, Secret: secret}
}

// nextSent returns the next ServerMessage with a timeout.
func (e *serverSendEnv) nextSent(stream *fakeServerSendStream) *pb.ServerMessage {
	e.t.Helper()
	select {
	case m := <-stream.sent:
		return m
	case <-time.After(5 * time.Second):
		e.t.Fatal("no ServerMessage sent")
		return nil
	}
}

// requireReject asserts the single, byte-identical auth reject message.
func (e *serverSendEnv) requireReject(stream *fakeServerSendStream, event pb.Event) {
	e.t.Helper()
	m := e.nextSent(stream)
	require.Equal(e.t, event, m.GetEvent())
	require.Equal(e.t, []byte("rpc auth token is invalid"), m.GetData(), "reject Data must match the legacy bytes")
}

func (e *serverSendEnv) flushEvent(eventType string) []string {
	e.t.Helper()
	w := &auditRecorder{}
	require.NoError(e.t, e.guard.Flush(w))
	var out []string
	for _, ev := range w.events {
		if ev.EventType == eventType {
			out = append(out, ev.Summary)
		}
	}
	return out
}

// ---- register admission order ----------------------------------------------

// TestServerSendRegisterRateBlockedZeroDAO: a pre-warmed (IP,node) rate
// counter denies registration BEFORE the empty-secret check and BEFORE
// any DAO access — the reject is byte-identical and only the
// register_rate_blocked bucket is recorded.
func TestServerSendRegisterRateBlockedZeroDAO(t *testing.T) {
	cfg := serverSendGuardCfg()
	cfg.RegisterRatePerMin = 1
	cfg.RegisterWindow = time.Minute
	e := newServerSendEnv(t, false, cfg) // no DB: panic iff DAO touched

	// Pre-warm: the first register from this peer+node consumed the quota.
	require.True(t, e.guard.AllowRegister(serverSendPeer+":7000", "cli-1"))

	stream := newStream()
	stream.recv <- registerMsg("cli-1", "even-an-empty-secret-would-lose")
	close(stream.recv)

	require.NotPanics(t, func() {
		err := e.s.ServerSend(stream)
		require.ErrorIs(t, err, errRPCAuthInvalid, "rate-blocked register returns the unified auth error")
	})
	e.requireReject(stream, pb.Event_EVENT_REGISTER_CLIENT)

	blocks := e.flushEvent(models.AuditEventTypeRPCRegisterRateBlocked)
	require.Len(t, blocks, 1, "rate block must be audited")
	require.Contains(t, blocks[0], "ip="+serverSendPeer)
	require.Contains(t, blocks[0], "node=cli-1")
	require.Empty(t, e.flushEvent(models.AuditEventTypeRPCSecretFailure),
		"rate gate must fire before the secret checks")
}

// TestServerSendKeyLockedZeroDAO: a locked (IP,node) key rejects the
// register BEFORE the DAO, records the failure and never leaks the
// reason.
func TestServerSendKeyLockedZeroDAO(t *testing.T) {
	cfg := serverSendGuardCfg()
	cfg.SecretFailureThreshold = 1
	cfg.SecretKeyLockDuration = 15 * time.Minute
	e := newServerSendEnv(t, false, cfg)

	// Pre-lock the key the way the mismatch path does.
	e.guard.RecordSecretFailure(serverSendPeer+":7000", "cli-1", rpcguard.LayerClient)
	require.False(t, e.guard.AllowSecretAttempt(serverSendPeer+":7000", "cli-1"))

	stream := newStream()
	stream.recv <- registerMsg("cli-1", "good-secret")
	close(stream.recv)

	require.NotPanics(t, func() {
		err := e.s.ServerSend(stream)
		require.ErrorIs(t, err, errRPCAuthInvalid)
	})
	e.requireReject(stream, pb.Event_EVENT_REGISTER_CLIENT)

	fails := e.flushEvent(models.AuditEventTypeRPCSecretFailure)
	require.Len(t, fails, 1, "the locked attempt failure must be audited")
	require.Contains(t, fails[0], "layer=client")
	require.Empty(t, e.flushEvent(models.AuditEventTypeRPCRegisterRateBlocked))
}

// TestServerSendEmptySecretRejected: an empty secret is rejected with the
// same bytes and counted as a client-layer failure — before any DAO.
func TestServerSendEmptySecretRejected(t *testing.T) {
	e := newServerSendEnv(t, false, serverSendGuardCfg())

	stream := newStream()
	stream.recv <- registerMsg("cli-1", "")
	close(stream.recv)

	require.NotPanics(t, func() {
		err := e.s.ServerSend(stream)
		require.ErrorIs(t, err, errRPCAuthInvalid)
	})
	e.requireReject(stream, pb.Event_EVENT_REGISTER_CLIENT)

	fails := e.flushEvent(models.AuditEventTypeRPCSecretFailure)
	require.Len(t, fails, 1)
	require.Contains(t, fails[0], "node=cli-1")
	require.Contains(t, fails[0], "layer=client")
}

// TestServerSendUnknownIDCountsFailure: a missing row is a failure; the
// response stays byte-identical and the returned error never leaks the
// "record not found" reason.
func TestServerSendUnknownIDCountsFailure(t *testing.T) {
	e := newServerSendEnv(t, true, serverSendGuardCfg())

	stream := newStream()
	stream.recv <- registerMsg("ghost", "x")
	close(stream.recv)

	err := e.s.ServerSend(stream)
	require.ErrorIs(t, err, errRPCAuthInvalid)
	require.NotContains(t, err.Error(), "record not found", "the unified error must not leak the DAO reason")
	e.requireReject(stream, pb.Event_EVENT_REGISTER_CLIENT)

	fails := e.flushEvent(models.AuditEventTypeRPCSecretFailure)
	require.Len(t, fails, 1, "not-found must count as a failure")
}

// TestServerSendTransientDBErrorNotCounted: a closed database is a
// transient DB error — rejected with the same shape but NEVER counted as
// a failure and never rate-blocked.
func TestServerSendTransientDBErrorNotCounted(t *testing.T) {
	e := newServerSendEnv(t, true, serverSendGuardCfg())
	require.NoError(t, e.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID: "cli-ok", ConnectSecret: "good-secret",
	}}).Error)
	sqlDB, err := e.db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	stream := newStream()
	stream.recv <- registerMsg("cli-ok", "good-secret")
	close(stream.recv)

	err = e.s.ServerSend(stream)
	require.ErrorIs(t, err, errRPCAuthInvalid)
	e.requireReject(stream, pb.Event_EVENT_REGISTER_CLIENT)

	require.Empty(t, e.flushEvent(models.AuditEventTypeRPCSecretFailure),
		"transient DB errors must never count as secret failures")
	require.Empty(t, e.flushEvent(models.AuditEventTypeRPCRegisterRateBlocked))
}

// TestServerSendMismatchCountsSuccessClears: failures accumulate per
// (IP,node); a successful register clears them and completes the legacy
// register path (Set + Recv pump + success message).
func TestServerSendMismatchCountsSuccessClears(t *testing.T) {
	cfg := serverSendGuardCfg()
	cfg.SecretFailureThreshold = 3
	cfg.SecretKeyLockDuration = 15 * time.Minute
	e := newServerSendEnv(t, true, cfg)
	require.NoError(t, e.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID: "cli-a", ConnectSecret: "good-secret",
	}}).Error)

	// Two mismatches: count reaches 2, no lock yet (threshold 3).
	for i := 0; i < 2; i++ {
		stream := newStream()
		stream.recv <- registerMsg("cli-a", "wrong-"+string(rune('1'+i)))
		close(stream.recv)
		err := e.s.ServerSend(stream)
		require.ErrorIs(t, err, errRPCAuthInvalid)
		e.requireReject(stream, pb.Event_EVENT_REGISTER_CLIENT)
	}

	// Third attempt with the correct secret: success clears the state and
	// completes the register path.
	stream := newStream()
	stream.recv <- registerMsg("cli-a", "good-secret")
	done := make(chan error, 1)
	go func() { done <- e.s.ServerSend(stream) }()

	m := e.nextSent(stream)
	require.Equal(t, pb.Event_EVENT_REGISTER_CLIENT, m.GetEvent())
	require.Equal(t, "cli-a", m.GetClientId())
	require.Equal(t, "cli-a", m.GetSessionId())
	require.Equal(t, defs.CliTypeClient, e.manager.Get("cli-a").CliType, "Set must register the client type")

	// Close the stream so the rpc.Recv pump sees EOF and releases ServerSend.
	close(stream.recv)
	select {
	case err := <-done:
		require.NoError(t, err, "successful register must terminate cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("ServerSend did not return after the stream closed")
	}

	fails := e.flushEvent(models.AuditEventTypeRPCSecretFailure)
	require.Len(t, fails, 1, "both mismatches aggregate into one bucket")
	require.Contains(t, fails[0], "count=2")
	require.True(t, e.guard.AllowSecretAttempt(serverSendPeer+":7000", "cli-a"),
		"success must clear the failure state")
}

// TestServerSendLogsNeverContainSecret: no log line emitted during
// register attempts may contain the secret material (success log shows
// only event/clientID/type; error logs never print the request or the
// secret).
func TestServerSendLogsNeverContainSecret(t *testing.T) {
	e := newServerSendEnv(t, true, serverSendGuardCfg())
	require.NoError(t, e.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID: "cli-a", ConnectSecret: "good-secret",
	}}).Error)

	hook := &captureHook{}
	logger.LoggerInstance.AddHook(hook)
	t.Cleanup(func() {
		removeHook(logger.LoggerInstance, hook)
	})

	probeSecret := "LOGPROBE-SECRET-42"

	// Mismatch attempt (logs the "invalid secret" path).
	stream := newStream()
	stream.recv <- registerMsg("cli-a", probeSecret)
	close(stream.recv)
	err := e.s.ServerSend(stream)
	require.ErrorIs(t, err, errRPCAuthInvalid)

	// Success attempt (logs "register success").
	stream2 := newStream()
	stream2.recv <- registerMsg("cli-a", "good-secret")
	done := make(chan error, 1)
	go func() { done <- e.s.ServerSend(stream2) }()
	_ = e.nextSent(stream2)
	close(stream2.recv)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ServerSend did not return")
	}

	for _, entry := range hook.snapshot() {
		require.NotContains(t, entry.Message, probeSecret, "log line must never contain the secret")
		require.NotContains(t, entry.Message, "good-secret", "log line must never contain a valid secret either")
	}
	require.NotEmpty(t, hook.snapshot(), "the attempts must have produced log lines to audit")
}

// captureHook collects every logrus entry emitted while attached.
type captureHook struct {
	mu      sync.Mutex
	entries []*logrus.Entry
}

func (h *captureHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *captureHook) Fire(e *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
	return nil
}

func (h *captureHook) snapshot() []*logrus.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*logrus.Entry, len(h.entries))
	copy(out, h.entries)
	return out
}

// removeHook detaches a hook from a logrus logger (logrus has no
// RemoveHook API).
func removeHook(l *logrus.Logger, target logrus.Hook) {
	for lvl, hooks := range l.Hooks {
		out := hooks[:0]
		for _, h := range hooks {
			if h != target {
				out = append(out, h)
			}
		}
		l.Hooks[lvl] = out
	}
}
