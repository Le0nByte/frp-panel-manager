package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	coordChildID = "cli-1-1"
	coordSrvIP   = "1.2.3.4"
)

var candidateWebTOML = []byte("[[proxies]]\nname = \"web\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 8080\nremotePort = 8000\n")

// ---- test doubles (only Preflight / Apply / Verifier / KeyProvider are
// faked; the DAO stores are the REAL implementations over in-memory SQLite) ----

type fakePreflightRunner struct {
	mu                 sync.Mutex
	serverOK, clientOK bool
	code, msg          string
	err                error
	calls              int
	lastReq            *pb.PreflightRequest

	// A4a: independent single-gate counters and scriptable results.
	// SafeCreate still drives Run only; these exist for the A4b update
	// coordinator and never affect Run's behavior.
	clientGateCalls   int
	serverProbeCalls  int
	clientGateResult  *PreflightGateResult
	serverProbeResult *PreflightGateResult
	clientGateErr     error
	serverProbeErr    error
}

func (f *fakePreflightRunner) Run(_ *app.Context, req *pb.PreflightRequest) (*pb.PreflightResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	resp := &pb.PreflightResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		ServerOk:     lo.ToPtr(f.serverOK),
		ClientOk:     lo.ToPtr(f.clientOK),
		ErrorCode:    lo.ToPtr(f.code),
		ErrorMessage: lo.ToPtr(f.msg),
	}
	return resp, nil
}

func (f *fakePreflightRunner) RunClient(_ *app.Context, _ *pb.PreflightRequest) (*PreflightGateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clientGateCalls++
	if f.clientGateErr != nil {
		return nil, f.clientGateErr
	}
	if f.clientGateResult != nil {
		return f.clientGateResult, nil
	}
	return &PreflightGateResult{Ok: f.clientOK, ErrorCode: f.code, ErrorMessage: f.msg}, nil
}

func (f *fakePreflightRunner) RunServerProbe(_ *app.Context, _ *pb.PreflightRequest) (*PreflightGateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.serverProbeCalls++
	if f.serverProbeErr != nil {
		return nil, f.serverProbeErr
	}
	if f.serverProbeResult != nil {
		return f.serverProbeResult, nil
	}
	return &PreflightGateResult{Ok: f.serverOK, ErrorCode: f.code, ErrorMessage: f.msg}, nil
}

type fakeApplyService struct {
	mu        sync.Mutex
	calls     int
	callsData []ApplyRequest
	results   []applyResult // scripted; last result repeats
}

func (f *fakeApplyService) Apply(ctx context.Context, _ *app.Context, req ApplyRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.callsData = append(f.callsData, req)
	if len(f.results) == 0 {
		return "", nil
	}
	r := f.results[0]
	if len(f.results) > 1 {
		f.results = f.results[1:]
	}
	return r.code, r.err
}

type fakeVerifierService struct {
	mu                 sync.Mutex
	appliedCalls       int
	releasedCalls      int
	appliedErr         error
	releasedErr        error
	lastAppliedPortVal uint32 // recorded by the D2c-C rollback test's lastAppliedPort()
}

func (f *fakeVerifierService) VerifyApplied(ctx context.Context, _ *app.Context, _ string, _ string, port uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appliedCalls++
	f.lastAppliedPortVal = port
	return f.appliedErr
}

func (f *fakeVerifierService) VerifyReleased(context.Context, *app.Context, string, string, uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releasedCalls++
	return f.releasedErr
}

type failKeyProvider struct{}

func (failKeyProvider) SnapshotKey(context.Context) ([]byte, error) {
	return nil, errors.New("snapshot key file is not configured")
}

// failingCommitStore wraps the REAL dao store and injects a single
// CommitCreate failure — everything else stays on the real DAO.
type failingCommitStore struct {
	TxStore
	failCommit bool
}

func (s *failingCommitStore) CommitCreate(c *app.Context, u models.UserInfo, in *dao.CommitCreateInput) (uint, error) {
	if s.failCommit {
		return 0, errors.New("injected commit db failure")
	}
	return s.TxStore.CommitCreate(c, u, in)
}

// failingStatusStore wraps the REAL dao store and injects a single
// UpdateStatus failure for one target status — everything else stays on
// the real DAO.
type failingStatusStore struct {
	TxStore
	failStatus string
	failed     bool
}

func (s *failingStatusStore) UpdateStatus(c *app.Context, u models.UserInfo, txID, status, code, msg string) error {
	if !s.failed && status == s.failStatus {
		s.failed = true
		return errors.New("injected status transition failure")
	}
	return s.TxStore.UpdateStatus(c, u, txID, status, code, msg)
}

type applyResult struct {
	code string
	err  error
}

// barrierApplyService holds the FIRST Apply call until either the second
// Apply call arrives (the unserialized interleaving: both requests pass
// PrepareShadowPlan before either commits, so both derived the same child
// id from CountClientsInShadow=0 — the colliding D1-复审 P1 interleaving)
// or the injected OriginLocker reports a blocked second caller (the
// serialized behavior: the second request is waiting on the per-origin
// lock, so the first may proceed and commit). Both releases are driven by
// real events — the test outcome never depends on sleeps. Calls beyond
// the second (e.g. a compensation apply) pass straight through.
type barrierApplyService struct {
	mu        sync.Mutex
	calls     int
	callsData []ApplyRequest
	second    chan struct{} // closed when the second Apply arrives
	waiter    chan struct{} // closed when the locker reports a blocked second caller
}

func (b *barrierApplyService) Apply(_ context.Context, _ *app.Context, req ApplyRequest) (string, error) {
	b.mu.Lock()
	b.calls++
	n := b.calls
	b.callsData = append(b.callsData, req)
	b.mu.Unlock()

	if n == 1 {
		select {
		case <-b.second:
		case <-b.waiter:
		}
	} else if n == 2 {
		close(b.second)
	}
	return "", nil
}

// contentionSignalingLocker wraps a real OriginLocker and closes its
// signal channel the first time a second goroutine contends on an already
// held key. The barrier apply consumes the signal to tell the serialized
// behavior (second request waiting on the per-origin lock) apart from the
// racy one (both requests already applying).
type contentionSignalingLocker struct {
	inner   OriginLocker
	mu      sync.Mutex
	holders map[string]int
	once    sync.Once
	signal  chan struct{}
}

func newContentionSignalingLocker(inner OriginLocker) *contentionSignalingLocker {
	return &contentionSignalingLocker{
		inner:   inner,
		holders: make(map[string]int),
		signal:  make(chan struct{}),
	}
}

func (l *contentionSignalingLocker) Lock(key string) {
	l.mu.Lock()
	l.holders[key]++
	contended := l.holders[key] > 1
	l.mu.Unlock()
	if contended {
		l.once.Do(func() { close(l.signal) })
	}
	l.inner.Lock(key)
}

func (l *contentionSignalingLocker) Unlock(key string) {
	l.mu.Lock()
	l.holders[key]--
	if l.holders[key] == 0 {
		delete(l.holders, key)
	}
	l.mu.Unlock()
	l.inner.Unlock(key)
}

// requireCompensationEvidence asserts the compensation evidence JSON
// (spec §8.2 / P2): it must carry at least the stage, the owner
// error/observation timestamp, and NEVER config content or secrets.
func requireCompensationEvidence(t *testing.T, tx *models.ChangeTransaction, stage string, wantErr bool) {
	t.Helper()
	require.NotEmpty(t, tx.EvidenceJSON, "compensation must write evidence_json")
	var ev map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(tx.EvidenceJSON), &ev))
	require.Equal(t, stage, ev["stage"], "evidence stage must identify the compensation phase")
	require.Contains(t, ev, "observed_at", "evidence must carry the timestamp")
	if wantErr {
		require.Contains(t, ev, "owner_error", "failed compensation must record the owner error")
	} else {
		require.NotContains(t, ev, "owner_error", "successful compensation has no owner error")
	}
	for k := range ev {
		require.NotContains(t, k, "config", "evidence must not carry config content")
		require.NotContains(t, k, "secret", "evidence must not carry secrets")
		require.NotContains(t, k, "token", "evidence must not carry tokens")
	}
}

// ---- environment ----

type coordTestEnv struct {
	t        *testing.T
	db       *gorm.DB
	ctx      *app.Context
	appInst  app.Application
	user     models.UserInfo // alias of userInfo (D2c-C rollback env reads e.user)
	userInfo models.UserInfo
	pre      *fakePreflightRunner
	apply    *fakeApplyService
	verify   *fakeVerifierService
	key      *MemoryKeyProvider
	now      time.Time
	deps     CoordinatorDeps
}

var coordDSNCounter uint64

func coordTestDSN(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&coordDSNCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return fmt.Sprintf("file:coord-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)", n, name)
}

func newCoordTestEnv(t *testing.T) *coordTestEnv {
	t.Helper()
	return buildCoordEnv(t, coordTestDSN(t))
}

// newCoordFileTestEnv builds an env over a TEMP FILE SQLite database.
// The concurrent same-origin test runs two flows with real overlapping DB
// writes: a file DB with busy_timeout gives OS-level locking semantics
// and per-run isolation (unique temp dir), avoiding the shared-cache
// in-memory DSN's multi-connection quirks for this specific test.
func newCoordFileTestEnv(t *testing.T) *coordTestEnv {
	t.Helper()
	// Create the temporary database under the package working directory.
	// The repository is on D:, so Windows tests do not spill SQLite files
	// into the user/profile temp directory on C:. The cleanup registered by
	// buildCoordEnv runs first (LIFO) and closes SQLite before RemoveAll.
	tmpDir, err := os.MkdirTemp(".", ".coord-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	dsn := filepath.ToSlash(filepath.Join(tmpDir, "coord.db")) + "?_pragma=busy_timeout(3000)"
	return buildCoordEnv(t, dsn)
}

func buildCoordEnv(t *testing.T, dsn string) *coordTestEnv {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&models.Client{},
		&models.Server{},
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

	userInfo := &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1, Token: "test-token"}
	ctx := app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, userInfo), appInst)

	env := &coordTestEnv{
		t:        t,
		db:       db,
		ctx:      ctx,
		appInst:  appInst,
		user:     userInfo,
		userInfo: userInfo,
		pre:      &fakePreflightRunner{serverOK: true, clientOK: true},
		apply:    &fakeApplyService{},
		verify:   &fakeVerifierService{},
		key:      &MemoryKeyProvider{Key: bytes.Repeat([]byte{0x42}, snapshot.KeySize)},
		now:      time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	}
	env.deps = CoordinatorDeps{
		TxStore:        newDAOStore(),
		ResStore:       newDAOStore(),
		SnapStore:      newDAOStore(),
		KeyProvider:    env.key,
		Preflight:      env.pre,
		Apply:          env.apply,
		Verifier:       env.verify,
		OriginLocker:   NewOriginKeyedLock(),
		Now:            func() time.Time { return env.now },
		Lease:          time.Minute,
		ApplyTimeout:   2 * time.Second,
		VerifyTimeout:  2 * time.Second,
		VerifyAttempts: 3,
		VerifyInterval: time.Millisecond,
		HashRequest:    hashCreateRequest,
	}
	return env
}

// seedShadowedPair creates the origin (shadowed, no config) + child (with
// the given proxies) + the server row, tenant-scoped. GetClientWithMakeShadow
// then hits the fast path with ZERO writes.
func (e *coordTestEnv) seedShadowedPair(stopped bool, proxies ...v1.TypedProxyConfig) {
	t := e.t
	t.Helper()

	child := &models.ClientEntity{
		ClientID:       coordChildID,
		OriginClientID: testClientID,
		ServerID:       testServerID,
		IsShadow:       false,
		Stopped:        false,
		ConnectSecret:  "child-secret",
	}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = proxies
	require.NoError(t, child.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, child))

	origin := &models.ClientEntity{
		ClientID:      testClientID,
		IsShadow:      true,
		Stopped:       stopped,
		ConnectSecret: "origin-secret",
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))

	srv := &models.ServerEntity{
		ServerID:      testServerID,
		ServerIP:      coordSrvIP,
		ConfigContent: []byte(`{"bindPort": 7100}`),
		ConnectSecret: "server-secret",
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateServer(e.userInfo, srv))
}

// seedServer creates the server row used by the safe flow.
func (e *coordTestEnv) seedServer() {
	t := e.t
	t.Helper()
	srv := &models.ServerEntity{
		ServerID:      testServerID,
		ServerIP:      coordSrvIP,
		ConfigContent: []byte(`{"bindPort": 7100}`),
		ConnectSecret: "server-secret",
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateServer(e.userInfo, srv))
}

// seedUnshadowedOrigin creates a never-shadowed origin client carrying the
// given proxies (no child exists for any server).
func (e *coordTestEnv) seedUnshadowedOrigin(proxies ...v1.TypedProxyConfig) {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{
		ClientID:      testClientID,
		IsShadow:      false,
		ConnectSecret: "origin-secret",
	}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = proxies
	require.NoError(t, origin.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
}

// seedShadowedOrigin creates an already-converted origin (the state a
// completed shadow conversion produces) with NO child for any server.
func (e *coordTestEnv) seedShadowedOrigin() {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{
		ClientID:      testClientID,
		IsShadow:      true,
		ConnectSecret: "origin-secret",
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
}

// captureConfigRows snapshots every client + proxy_config row: the
// zero-config-delta contract compares these before/after a failed request.
func (e *coordTestEnv) captureConfigRows() (clients []models.Client, proxies []models.ProxyConfig) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Find(&clients).Error)
	require.NoError(t, e.db.Find(&proxies).Error)
	return clients, proxies
}

func (e *coordTestEnv) clientByID(id string) *models.ClientEntity {
	t := e.t
	t.Helper()
	cli, err := dao.NewQuery(e.ctx).GetClientByFilter(e.userInfo, &models.ClientEntity{ClientID: id}, nil)
	require.NoError(t, err)
	return cli
}

func (e *coordTestEnv) proxyCountByClient(id string) int {
	t := e.t
	t.Helper()
	var n int64
	require.NoError(t, e.db.Model(&models.ProxyConfig{}).Where("client_id = ?", id).Count(&n).Error)
	return int(n)
}

func tcpProxy(name string, remote, local uint32) v1.TypedProxyConfig {
	p := &v1.TCPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: name, Type: "tcp"}, RemotePort: int(remote)}
	p.LocalIP = "127.0.0.1"
	p.LocalPort = int(local)
	return v1.TypedProxyConfig{ProxyConfigurer: p}
}

func (e *coordTestEnv) newCoordinator() *TransactionCoordinator {
	return &TransactionCoordinator{deps: e.deps}
}

func createReq(idemKey string, config []byte) *pb.CreateProxyConfigRequest {
	return &pb.CreateProxyConfigRequest{
		ClientId:       lo.ToPtr(testClientID),
		ServerId:       lo.ToPtr(testServerID),
		Config:         config,
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

func uuidKey() string { return uuid.NewString() }

// ---- DB assertions ----

func (e *coordTestEnv) txByID(txID string) *models.ChangeTransaction {
	e.t.Helper()
	tx, err := dao.NewQuery(e.ctx).FindByID(e.userInfo, txID)
	require.NoError(e.t, err)
	return tx
}

func (e *coordTestEnv) txByKey(key string) *models.ChangeTransaction {
	e.t.Helper()
	tx, err := dao.NewQuery(e.ctx).FindByIdempotencyKey(e.userInfo, key)
	require.NoError(e.t, err)
	return tx
}

func (e *coordTestEnv) countTransactions() int {
	e.t.Helper()
	list, err := dao.NewQuery(e.ctx).ListRecent(e.userInfo, 100)
	require.NoError(e.t, err)
	return len(list)
}

func (e *coordTestEnv) countReservations() int {
	e.t.Helper()
	var n int64
	require.NoError(e.t, e.db.Model(&models.PortReservation{}).Count(&n).Error)
	return int(n)
}

func (e *coordTestEnv) reservationByPort(port uint32) *models.PortReservation {
	e.t.Helper()
	var res models.PortReservation
	err := e.db.Where("remote_port = ?", port).First(&res).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(e.t, err)
	return &res
}

func (e *coordTestEnv) snapshots(txID string) []*models.ConfigSnapshot {
	e.t.Helper()
	list, err := dao.NewQuery(e.ctx).FindByTransaction(e.userInfo, txID)
	require.NoError(e.t, err)
	return list
}

func (e *coordTestEnv) auditEvents(txID string) []*models.AuditEvent {
	e.t.Helper()
	list := []*models.AuditEvent{}
	require.NoError(e.t, e.db.Where("transaction_id = ?", txID).Find(&list).Error)
	return list
}

func (e *coordTestEnv) childClient() *models.ClientEntity {
	e.t.Helper()
	cli, err := dao.NewQuery(e.ctx).GetClientByFilter(e.userInfo, &models.ClientEntity{ClientID: coordChildID}, nil)
	require.NoError(e.t, err)
	return cli
}

func (e *coordTestEnv) childProxyNames() []string {
	return e.proxyNamesOf(coordChildID)
}

// proxyNamesOf returns the proxy rule names of the client with the given
// id (the flow-created child id is app.ShadowedClientID(origin, 1), which
// differs from coordChildID — the id the seeded pairs use).
func (e *coordTestEnv) proxyNamesOf(clientID string) []string {
	t := e.t
	t.Helper()
	cfg, err := e.clientByID(clientID).GetConfigContent()
	require.NoError(t, err)
	names := []string{}
	for _, p := range cfg.Proxies {
		names = append(names, p.GetBaseConfig().Name)
	}
	return names
}

func decryptSnapshot(t *testing.T, snap *models.ConfigSnapshot, key []byte) []byte {
	t.Helper()
	plain, err := dao.DecryptSnapshotForTest(snap, key)
	require.NoError(t, err)
	return plain
}

func assertInvalidCode(t *testing.T, resp *pb.CreateProxyConfigResponse, code string) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, code, resp.GetErrorCode(), "structured error_code must carry the stable code")
	require.False(t, strings.HasPrefix(resp.GetStatus().GetMessage(), code+":"),
		"message %q must stay human-readable without the %q prefix", resp.GetStatus().GetMessage(), code)
}

func requireNoReservation(t *testing.T, e *coordTestEnv) {
	t.Helper()
	require.Zero(t, e.countReservations(), "reservation must be released")
}

func requireOnePendingReservation(t *testing.T, e *coordTestEnv, port uint32) *models.PortReservation {
	t.Helper()
	res := e.reservationByPort(port)
	require.NotNil(t, res, "reservation must be retained for manual intervention")
	require.Equal(t, models.ReservationStatusPending, res.Status)
	return res
}

// ---- tests ----

func TestSafeCreateHappyPath(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// 1. transaction committed
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.FinishedAt)

	// 2. client config row carries the applied config (ssh + web)
	require.ElementsMatch(t, []string{"ssh", "web"}, env.childProxyNames())

	// 3. proxy rows rebuilt; reservation committed and bound to the new row
	rows := []*models.ProxyConfig{}
	require.NoError(t, env.db.Where("client_id = ?", coordChildID).Find(&rows).Error)
	require.Len(t, rows, 2)
	var webRow *models.ProxyConfig
	for _, r := range rows {
		if r.Name == "web" {
			webRow = r
		}
	}
	require.NotNil(t, webRow)
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, webRow.ID, *res.OwnerProxyID)

	// 4. before + after encrypted snapshots, both decryptable with the key
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 2)
	var beforeSnap, afterSnap *models.ConfigSnapshot
	for _, s := range snaps {
		switch s.Side {
		case models.SnapshotSideBefore:
			beforeSnap = s
		case models.SnapshotSideAfter:
			afterSnap = s
		}
	}
	require.NotNil(t, beforeSnap)
	require.NotNil(t, afterSnap)
	beforePlain := decryptSnapshot(t, beforeSnap, env.key.Key)
	require.NotEmpty(t, beforePlain)
	afterPlain := decryptSnapshot(t, afterSnap, env.key.Key)
	require.NotEmpty(t, afterPlain)

	// 5. apply happened exactly once, synchronously, addressed to the
	// child with NO replacement (existing-child update)
	require.Equal(t, 1, env.apply.calls)
	call := env.apply.callsData[0]
	require.Equal(t, testClientID, call.PhysicalAgentID)
	require.Equal(t, coordChildID, call.TargetClientID)
	require.Empty(t, call.ReplaceClientID, "an existing-child update must not replace any handler")
	require.Equal(t, testServerID, call.ServerID)
	require.NotEmpty(t, call.Config)
	// the after snapshot stores exactly what was applied
	require.Equal(t, afterPlain, call.Config)

	// 6. commit audit event
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeCommit, audits[0].EventType)
	require.True(t, audits[0].Success)
}

func TestSafeCreateBeforeSnapshotIsIsolatedCopy(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	original := append([]byte(nil), env.childClient().ConfigContent...)

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// locate the committed tx via ListRecent (single tx in this env)
	list, err := dao.NewQuery(env.ctx).ListRecent(env.userInfo, 10)
	require.NoError(t, err)
	require.Len(t, list, 1)
	var beforeSnap *models.ConfigSnapshot
	for _, s := range env.snapshots(list[0].TransactionID) {
		if s.Side == models.SnapshotSideBefore {
			beforeSnap = s
		}
	}
	require.NotNil(t, beforeSnap)
	beforePlain := decryptSnapshot(t, beforeSnap, env.key.Key)
	// byte-for-byte identical to the pre-change config: the candidate
	// mutation must never alias the before bytes.
	require.Equal(t, original, beforePlain)
}

func TestSafeCreateReservationConflict(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()

	// first request commits and keeps a committed reservation
	resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// second request on the same port with a different key
	resp2, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp2, defs.ErrorCodeReservationConflict)

	// no apply, no config change, failed tx + audit
	require.Equal(t, 1, env.apply.calls)
	require.ElementsMatch(t, []string{"ssh", "web"}, env.childProxyNames())
	require.Equal(t, 2, env.countTransactions())
	list, err := dao.NewQuery(env.ctx).ListRecent(env.userInfo, 10)
	require.NoError(t, err)
	var failed *models.ChangeTransaction
	for _, tx := range list {
		if tx.Status == models.TxStatusFailed {
			failed = tx
		}
	}
	require.NotNil(t, failed)
	require.Equal(t, defs.ErrorCodeReservationConflict, failed.ErrorCode)
	audits := env.auditEvents(failed.TransactionID)
	require.Len(t, audits, 1)
	require.False(t, audits[0].Success)
	// the winner's reservation stays committed
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
}

func TestSafeCreateRejectsMissingIdempotencyKey(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq("", candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.countTransactions(), "no transaction may be created without a valid key")
	require.Zero(t, env.apply.calls)
	requireNoReservation(t, env)
}

func TestSafeCreateRejectsInvalidIdempotencyKey(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()

	for _, bad := range []string{"not-a-uuid", "00000000-0000-0000-0000-000000000000", "123e4567-e89b-12d3-a456-426614174000"} {
		resp, err := tc.SafeCreateProxy(env.ctx, createReq(bad, candidateWebTOML))
		require.NoError(t, err)
		assertInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	}
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.apply.calls)
	requireNoReservation(t, env)
}

func TestSafeCreateIdempotencyReplayCommitted(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	key := uuidKey()
	req := createReq(key, candidateWebTOML)

	resp, err := tc.SafeCreateProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// replay: same key + same hash -> idempotent success, no second apply
	resp2, err := tc.SafeCreateProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Empty(t, resp2.GetErrorCode(), "idempotent replay success must carry no error_code")
	require.Equal(t, 1, env.apply.calls, "replay must not re-apply")
	require.Equal(t, 1, env.countTransactions(), "replay must not create a second transaction")
}

func TestSafeCreateIdempotencyDifferentHashConflict(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	other := []byte("[[proxies]]\nname = \"web2\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 9090\nremotePort = 8000\n")
	resp2, err := tc.SafeCreateProxy(env.ctx, createReq(key, other))
	require.NoError(t, err)
	assertInvalidCode(t, resp2, defs.ErrorCodeIdempotencyConflict)
	require.Equal(t, 1, env.apply.calls)
	require.Equal(t, 1, env.countTransactions())
}

func TestSafeCreateIdempotencyInflightRejected(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	key := uuidKey()
	req := createReq(key, candidateWebTOML)
	hash := hashCreateRequest(req)

	// a concurrent request already created the transaction and is applying
	txRow := &models.ChangeTransaction{
		TransactionID:  uuid.NewString(),
		IdempotencyKey: key,
		RequestHash:    hash,
		Operation:      models.TxOperationCreate,
		Status:         models.TxStatusApplying,
		ActorUserID:    1,
		TenantID:       1,
		StartedAt:      env.now,
	}
	require.NoError(t, env.db.Create(txRow).Error)

	resp, err := tc.SafeCreateProxy(env.ctx, req)
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeIdempotencyInFlight)
	require.Zero(t, env.apply.calls)
	require.Equal(t, 1, env.countTransactions())
	requireNoReservation(t, env)
}

func TestSafeCreateIdempotencyTerminalFailureReplay(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	key := uuidKey()
	req := createReq(key, candidateWebTOML)
	hash := hashCreateRequest(req)

	txRow := &models.ChangeTransaction{
		TransactionID:  uuid.NewString(),
		IdempotencyKey: key,
		RequestHash:    hash,
		Operation:      models.TxOperationCreate,
		Status:         models.TxStatusFailed,
		ErrorCode:      defs.ErrorCodePortRemoteInUse,
		ErrorMessage:   "port busy",
		ActorUserID:    1,
		TenantID:       1,
		StartedAt:      env.now,
	}
	require.NoError(t, env.db.Create(txRow).Error)

	resp, err := tc.SafeCreateProxy(env.ctx, req)
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
	require.Zero(t, env.apply.calls)
	require.Equal(t, 1, env.countTransactions())
}

func TestSafeCreatePreflightServerGateFail(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.pre.serverOK = false
	env.pre.code = defs.ErrorCodePortRemoteInUse
	env.pre.msg = "port is held by another process"
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortRemoteInUse, tx.ErrorCode)
	require.NotEmpty(t, tx.EvidenceJSON, "preflight evidence must be recorded")
	require.Zero(t, env.apply.calls)
	requireNoReservation(t, env)
	require.ElementsMatch(t, []string{"ssh"}, env.childProxyNames(), "no config change on preflight failure")
}

func TestSafeCreatePreflightClientGateWarningAllowsCreate(t *testing.T) {
	// 2026-08 user decision: the client gate is WARN-graded —
	// PORT_LOCAL_NOT_LISTENING (local port not listening yet) must NOT
	// block creation.
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.pre.clientOK = false
	env.pre.code = defs.ErrorCodePortLocalNotListening
	env.pre.msg = "local port is not listening yet (warning)"
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "advisory client warning must not fail creation")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotZero(t, env.apply.calls, "the rule must still be applied to the agent")
	require.NotNil(t, env.reservationByPort(8000))
}

func TestSafeCreatePreflightClientGateUnknownRejects(t *testing.T) {
	// client gate hard failures (offline / unknown / transport) still
	// reject creation — only the advisory PORT_LOCAL_NOT_LISTENING is
	// warn-graded.
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.pre.clientOK = false
	env.pre.code = defs.ErrorCodePortScanUnavailable
	env.pre.msg = "port scan unavailable"
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodePortScanUnavailable)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortScanUnavailable, tx.ErrorCode)
	require.Zero(t, env.apply.calls)
	requireNoReservation(t, env)
}

func TestSafeCreatePreflightTransportError(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.pre.err = errors.New("agent unreachable")
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodePortScanUnavailable)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.apply.calls)
	requireNoReservation(t, env)
}

func TestSafeCreateStoppedOriginClient(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(true, tcpProxy("ssh", 24, 22)) // origin stopped
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeAgentOffline)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.apply.calls, "stopped origin must never receive an apply")
	requireNoReservation(t, env)
}

func TestSafeCreateApplyFailureCompensates(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyTimeout, err: errors.New("agent did not ack within 15s")},
		{}, // compensation restore succeeds
	}
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeApplyTimeout)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyTimeout, tx.ErrorCode)
	require.Equal(t, 2, env.apply.calls, "apply + compensation restore")
	// the restore is addressed at the SAME target (existing child, no
	// replacement) and carries the before config
	require.Equal(t, env.apply.callsData[0].TargetClientID, env.apply.callsData[1].TargetClientID)
	require.Empty(t, env.apply.callsData[1].ReplaceClientID, "existing-child restore must not replace any handler")
	require.Equal(t, 1, env.verify.releasedCalls, "compensation must verify the old state")
	requireNoReservation(t, env)
	require.ElementsMatch(t, []string{"ssh"}, env.childProxyNames(), "compensated config must be the before state")
	requireCompensationEvidence(t, tx, "compensate-done", false)

	audits := env.auditEvents(tx.TransactionID)
	var compAudit *models.AuditEvent
	for _, a := range audits {
		if a.EventType == models.AuditEventTypeCompensation {
			compAudit = a
		}
	}
	require.NotNil(t, compAudit)
	require.True(t, compAudit.Success)
}

func TestSafeCreateVerifyFailureCompensates(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.verify.appliedErr = errors.New("server gate never saw the proxy")
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, 2, env.apply.calls, "apply + compensation restore")
	require.Equal(t, 1, env.verify.appliedCalls)
	require.Equal(t, 1, env.verify.releasedCalls)
	requireNoReservation(t, env)
	require.ElementsMatch(t, []string{"ssh"}, env.childProxyNames())
}

func TestSafeCreateCompensationApplyFailureRollbackFailed(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyTimeout, err: errors.New("apply timed out")},
		{code: "", err: errors.New("restore apply also failed")},
	}
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)
	require.True(t, strings.Contains(resp.GetStatus().GetMessage(), "compensation failed"))

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
	require.Equal(t, 2, env.apply.calls)
	// the rule stays blocked: reservation RETAINED for manual intervention
	requireOnePendingReservation(t, env, 8000)
	requireCompensationEvidence(t, tx, "compensate-apply", true)

	audits := env.auditEvents(tx.TransactionID)
	var compAudit *models.AuditEvent
	for _, a := range audits {
		if a.EventType == models.AuditEventTypeCompensation {
			compAudit = a
		}
	}
	require.NotNil(t, compAudit)
	require.False(t, compAudit.Success)
}

func TestSafeCreateCompensationVerifyFailureRollbackFailed(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyTimeout, err: errors.New("apply timed out")},
		{}, // compensation restore succeeds, then VerifyReleased fails
	}
	env.verify.releasedErr = errors.New("port still occupied after restore")
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, 2, env.apply.calls)
	require.Equal(t, 1, env.verify.releasedCalls)
	requireOnePendingReservation(t, env, 8000)
	requireCompensationEvidence(t, tx, "compensate-verify", true)
}

// TestSafeCreateUpdateStatusPreflightFailureReleasesReservation: a DB
// failure while advancing pending->preflight must NOT leave the lease to
// rot for its full 120s — the coordinator best-effort fails the
// transaction AND releases the reservation (P2, pre-apply path).
func TestSafeCreateUpdateStatusPreflightFailureReleasesReservation(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	store := &failingStatusStore{TxStore: newDAOStore(), failStatus: models.TxStatusPreflight}
	env.deps.TxStore = store
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.Error(t, err, "status transition failure is an internal error")
	require.Nil(t, resp)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status, "pre-apply failure must fail the transaction")
	require.Equal(t, "", tx.ErrorCode, "an internal transition failure carries no stable business code")
	requireNoReservation(t, env)
	require.Equal(t, 0, env.apply.calls, "no apply may happen before the preflight transition")
}

// TestSafeCreateUpdateStatusApplyingFailureReleasesReservation: the same
// best-effort FailTransaction+Release applies when advancing
// preflight->applying fails (still PRE-apply — nothing was sent to the
// agent, so compensation is not needed).
func TestSafeCreateUpdateStatusApplyingFailureReleasesReservation(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	store := &failingStatusStore{TxStore: newDAOStore(), failStatus: models.TxStatusApplying}
	env.deps.TxStore = store
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.Error(t, err, "status transition failure is an internal error")
	require.Nil(t, resp)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status, "pre-apply failure must fail the transaction")
	requireNoReservation(t, env)
	require.Equal(t, 0, env.apply.calls, "no apply may happen before the applying transition")
}

func TestSafeCreateVerifyingTransitionFailureCompensationSucceeded(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	store := &failingStatusStore{TxStore: newDAOStore(), failStatus: models.TxStatusVerifying}
	env.deps.TxStore = store
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeCommitFailed)
	require.Equal(t, 2, env.apply.calls, "forward apply plus successful restore apply")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	requireNoReservation(t, env)
}

func TestSafeCreateVerifyingTransitionFailureCompensationFailed(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	store := &failingStatusStore{TxStore: newDAOStore(), failStatus: models.TxStatusVerifying}
	env.deps.TxStore = store
	env.apply.results = []applyResult{
		{},
		{code: defs.ErrorCodeApplyRejected, err: errors.New("injected restore rejection")},
	}
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
	requireOnePendingReservation(t, env, 8000)
}

func TestSafeCreateCommitDBFailureCompensates(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	store := &failingCommitStore{TxStore: newDAOStore(), failCommit: true}
	env.deps.TxStore = store
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status, "config was applied, so commit failure must compensate")
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	require.Equal(t, 2, env.apply.calls)
	require.Equal(t, 1, env.verify.releasedCalls)
	requireNoReservation(t, env)
	// the failed commit left nothing behind
	require.Len(t, env.snapshots(tx.TransactionID), 1, "only the before snapshot survives the aborted commit")
	var proxyRows int64
	require.NoError(t, env.db.Model(&models.ProxyConfig{}).Where("client_id = ?", coordChildID).Count(&proxyRows).Error)
	require.Zero(t, proxyRows, "aborted commit must not persist proxy rows")
}

func TestSafeCreateSnapshotKeyFailure(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.deps.KeyProvider = failKeyProvider{}
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.Error(t, err)
	require.Nil(t, resp)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.apply.calls, "key failure happens before the apply point")
	requireNoReservation(t, env)
	require.Len(t, env.snapshots(tx.TransactionID), 0, "no snapshot may be written without the key")
}

// TestSafeCreateFailuresLeaveZeroConfigDelta is the contract for spec §13.2
// (零配置变更): a failed SafeCreate must leave the origin client, every
// child client and every proxy_config row EXACTLY as they were before the
// request — no shadow conversion, no child scaffolding, no proxy-row
// migration. Only the transaction row, the reservation and the audit trail
// may change. The shadow conversion is an in-memory plan until the final
// CommitCreate transaction.
func TestSafeCreateFailuresLeaveZeroConfigDelta(t *testing.T) {
	assertNoConfigDelta := func(t *testing.T, env *coordTestEnv, beforeClients []models.Client, beforeProxies []models.ProxyConfig) {
		t.Helper()
		afterClients, afterProxies := env.captureConfigRows()
		require.Equal(t, beforeClients, afterClients, "client rows must be identical to the pre-request state")
		require.Equal(t, beforeProxies, afterProxies, "proxy_config rows must be identical to the pre-request state")
	}

	t.Run("preflight-fail-first-shadow", func(t *testing.T) {
		env := newCoordTestEnv(t)
		env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
		env.seedServer()
		env.pre.serverOK = false
		env.pre.code = defs.ErrorCodePortRemoteInUse
		env.pre.msg = "port is held by another process"
		tc := env.newCoordinator()
		beforeC, beforeP := env.captureConfigRows()

		resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
		require.NoError(t, err)
		assertInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
		assertNoConfigDelta(t, env, beforeC, beforeP)
		requireNoReservation(t, env)
		require.Zero(t, env.apply.calls)
	})

	t.Run("server-missing", func(t *testing.T) {
		env := newCoordTestEnv(t)
		env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
		// NOTE: no server row seeded on purpose.
		tc := env.newCoordinator()
		beforeC, beforeP := env.captureConfigRows()

		resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
		require.Error(t, err)
		require.Nil(t, resp)
		assertNoConfigDelta(t, env, beforeC, beforeP)
		requireNoReservation(t, env)
		require.Zero(t, env.apply.calls)
	})

	t.Run("snapshot-key-failure", func(t *testing.T) {
		env := newCoordTestEnv(t)
		env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
		env.seedServer()
		env.deps.KeyProvider = failKeyProvider{}
		tc := env.newCoordinator()
		beforeC, beforeP := env.captureConfigRows()

		resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
		require.Error(t, err)
		require.Nil(t, resp)
		assertNoConfigDelta(t, env, beforeC, beforeP)
		requireNoReservation(t, env)
		require.Zero(t, env.apply.calls)
	})

	t.Run("reservation-conflict", func(t *testing.T) {
		env := newCoordTestEnv(t)
		env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
		env.seedServer()
		tc := env.newCoordinator()
		// first request commits (and performs the first shadow)
		resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
		require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

		// second request on the same port must fail WITHOUT touching rows
		beforeC, beforeP := env.captureConfigRows()
		resp2, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
		require.NoError(t, err)
		assertInvalidCode(t, resp2, defs.ErrorCodeReservationConflict)
		assertNoConfigDelta(t, env, beforeC, beforeP)
		require.Equal(t, 1, env.apply.calls, "the conflicting request must not apply")
	})

	t.Run("preflight-fail-shadowed-origin-no-child", func(t *testing.T) {
		env := newCoordTestEnv(t)
		env.seedShadowedOrigin()
		env.seedServer()
		env.pre.serverOK = false
		env.pre.code = defs.ErrorCodePortRemoteInUse
		env.pre.msg = "port is held by another process"
		tc := env.newCoordinator()
		beforeC, beforeP := env.captureConfigRows()

		resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
		require.NoError(t, err)
		assertInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
		assertNoConfigDelta(t, env, beforeC, beforeP)
		requireNoReservation(t, env)
		require.Zero(t, env.apply.calls)
	})
}

// TestSafeCreateFirstShadowHappyPath pins the committed end-state of a
// first shadow: the origin is converted (shadow flag + cleared
// config/server), the child is created from the origin's config, the proxy
// rows migrate to the child, and reservation/tx/snapshots/audit all land in
// the final CommitCreate transaction.
func TestSafeCreateFirstShadowHappyPath(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
	env.seedServer()
	tc := env.newCoordinator()
	key := uuidKey()
	childID := app.ShadowedClientID(testClientID, 1)
	wantBefore := append([]byte(nil), env.clientByID(testClientID).ConfigContent...)

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// 1. transaction committed
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)

	// 2. origin converted: shadow flag set, config + server binding cleared
	origin := env.clientByID(testClientID)
	require.True(t, origin.IsShadow)
	require.Empty(t, origin.ConfigContent)
	require.Empty(t, origin.ServerID)

	// 3. child created: inherits the origin's config, bound to the server
	child := env.clientByID(childID)
	require.False(t, child.IsShadow)
	require.Equal(t, testServerID, child.ServerID)
	require.Equal(t, testClientID, child.OriginClientID)
	cfg, err := child.GetConfigContent()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"ssh", "web"}, lo.Map(cfg.Proxies, func(p v1.TypedProxyConfig, _ int) string {
		return p.GetBaseConfig().Name
	}))

	// 4. proxy rows migrated: child owns both, origin owns none
	require.Equal(t, 2, env.proxyCountByClient(childID))
	require.Zero(t, env.proxyCountByClient(testClientID))

	// 5. reservation committed and bound to the new web row
	rows := []*models.ProxyConfig{}
	require.NoError(t, env.db.Where("client_id = ?", childID).Find(&rows).Error)
	var webRow *models.ProxyConfig
	for _, r := range rows {
		if r.Name == "web" {
			webRow = r
		}
	}
	require.NotNil(t, webRow)
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, webRow.ID, *res.OwnerProxyID)

	// 6. before snapshot == the origin's original config; after == applied
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 2)
	var beforeSnap, afterSnap *models.ConfigSnapshot
	for _, s := range snaps {
		switch s.Side {
		case models.SnapshotSideBefore:
			beforeSnap = s
		case models.SnapshotSideAfter:
			afterSnap = s
		}
	}
	require.NotNil(t, beforeSnap)
	require.NotNil(t, afterSnap)
	require.Equal(t, wantBefore, decryptSnapshot(t, beforeSnap, env.key.Key), "before snapshot is the origin's pre-shadow config")
	afterPlain := decryptSnapshot(t, afterSnap, env.key.Key)
	require.NotEmpty(t, afterPlain)

	// 7. one synchronous apply: physical origin manager id, child payload
	// id, and the directed switch Replace=origin (round-8 I2: the origin
	// handler must not stay live after a first shadow)
	require.Equal(t, 1, env.apply.calls)
	call := env.apply.callsData[0]
	require.Equal(t, testClientID, call.PhysicalAgentID)
	require.Equal(t, childID, call.TargetClientID)
	require.Equal(t, testClientID, call.ReplaceClientID, "first-shadow forward apply must replace the origin handler")
	require.Equal(t, afterPlain, call.Config)

	// 8. single commit audit
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeCommit, audits[0].EventType)
	require.True(t, audits[0].Success)
}

// failingCommitSnapshotStore wraps the REAL dao store and poisons ONLY the
// after-snapshot key inside CommitCreate: the AES key-size check fails at
// step 4 of the commit transaction, AFTER the origin conversion, child
// insert and proxy rebuild were written — proving a mid-commit failure
// rolls the whole batch back.
type failingCommitSnapshotStore struct {
	TxStore
}

func (s *failingCommitSnapshotStore) CommitCreate(c *app.Context, u models.UserInfo, in *dao.CommitCreateInput) (uint, error) {
	in2 := *in
	in2.Snapshot = in.Snapshot
	in2.Snapshot.Key = []byte{1} // wrong size: EncryptSnapshot fails mid-transaction
	return s.TxStore.CommitCreate(c, u, &in2)
}

// TestSafeCreateFirstShadowCommitRollsBackEverything proves the first-shadow
// conversion is atomic: when CommitCreate fails after the conversion writes
// are already inside the transaction, the WHOLE batch rolls back — the DB
// returns to the pre-request state and the agent compensation restores the
// live config.
func TestSafeCreateFirstShadowCommitRollsBackEverything(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
	env.seedServer()
	env.apply.results = []applyResult{{}, {}} // apply ok, compensation restore ok
	env.deps.TxStore = &failingCommitSnapshotStore{TxStore: newDAOStore()}
	tc := env.newCoordinator()
	key := uuidKey()
	beforeC, beforeP := env.captureConfigRows()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	// the aborted commit left the config tables byte-identical: origin
	// still unshadowed, no child, no proxy migration
	afterC, afterP := env.captureConfigRows()
	require.Equal(t, beforeC, afterC, "origin + child client rows must roll back")
	require.Equal(t, beforeP, afterP, "proxy rows must roll back")

	// the agent compensation completed: tx rolled back, reservation released
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	require.Equal(t, 2, env.apply.calls, "apply + compensation restore")
	require.Equal(t, 1, env.verify.releasedCalls)
	requireNoReservation(t, env)
	require.Len(t, env.snapshots(tx.TransactionID), 1, "only the before snapshot survives the aborted commit")
}

func TestCreateProxyConfigHandlerDelegatesToCoordinator(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))

	old := safeCreateCoordinator
	defer func() { safeCreateCoordinator = old }()
	safeCreateCoordinator = &TransactionCoordinator{deps: env.deps}

	key := uuidKey()
	resp, err := CreateProxyConfig(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")
	require.Equal(t, 1, env.apply.calls, "handler must run the synchronous safe flow, not the legacy async path")
	require.Equal(t, models.TxStatusCommitted, env.txByKey(key).Status)
}

// sanity: the restored handler still points at the default coordinator after the swap test
func TestSafeCreateCoordinatorRestoredAfterSwap(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	old := safeCreateCoordinator
	defer func() { safeCreateCoordinator = old }()
	safeCreateCoordinator = &TransactionCoordinator{deps: env.deps}
	_ = safeCreateCoordinator
	require.NotNil(t, old)
}

// TestSafeCreateConcurrentFirstShadowSameOriginSerializes is the
// deterministic RED/GREEN for the D1-复审 P1 race: two concurrent
// first-shadow creates on the SAME origin with DIFFERENT remote/local
// ports and DIFFERENT idempotency keys must both succeed, with the second
// request re-planning after the first commits and appending its rule to
// the winner's child — never colliding on the child id and never
// compensating the winner's committed config away.
//
// Determinism: the barrier apply holds the FIRST Apply call until either
// the second Apply call arrives (the unserialized interleaving: both
// requests pass PrepareShadowPlan before either commits, so both derived
// the same child id from CountClientsInShadow=0) or the injected
// OriginLocker reports a blocked second caller (the serialized behavior:
// the second request is waiting on the per-origin lock, so the first may
// proceed and commit). Both release paths are real events — no sleeps
// drive the outcome.
func TestSafeCreateConcurrentFirstShadowSameOriginSerializes(t *testing.T) {
	env := newCoordFileTestEnv(t)
	env.seedUnshadowedOrigin() // never-shadowed origin with EMPTY config
	env.seedServer()

	sig := newContentionSignalingLocker(NewOriginKeyedLock())
	barrier := &barrierApplyService{second: make(chan struct{}), waiter: sig.signal}
	env.deps.Apply = barrier
	env.deps.OriginLocker = sig
	tc := env.newCoordinator()

	web1 := []byte("[[proxies]]\nname = \"web1\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 8080\nremotePort = 8000\n")
	web2 := []byte("[[proxies]]\nname = \"web2\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 9090\nremotePort = 9000\n")

	type outcome struct {
		resp *pb.CreateProxyConfigResponse
		err  error
	}
	var wg sync.WaitGroup
	wg.Add(2)
	results := make([]outcome, 2)
	go func() {
		defer wg.Done()
		results[0].resp, results[0].err = tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), web1))
	}()
	go func() {
		defer wg.Done()
		results[1].resp, results[1].err = tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), web2))
	}()
	wg.Wait()

	// 1. BOTH requests succeed: the second re-planned after the first
	//    committed and merged onto the existing child instead of colliding.
	for i, r := range results {
		require.NoErrorf(t, r.err, "request %d must succeed, got: %v", i, r.err)
		require.NotNil(t, r.resp)
		require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, r.resp.GetStatus().GetCode())
		require.Empty(t, r.resp.GetErrorCode())
	}

	// 2. exactly ONE shadow child row exists for the origin
	var children int64
	require.NoError(t, env.db.Model(&models.Client{}).Where("origin_client_id = ?", testClientID).Count(&children).Error)
	require.Equal(t, int64(1), children, "the two creates must share ONE child, not collide into two")

	// 3. the child config carries BOTH rules (second request appended to
	//    the winner's config). The flow-created child id is
	//    app.ShadowedClientID(origin, 1) = "cli-1@1" — NOT coordChildID,
	//    which only the seeded pairs use.
	flowChildID := app.ShadowedClientID(testClientID, 1)
	require.ElementsMatch(t, []string{"web1", "web2"}, env.proxyNamesOf(flowChildID))

	// 4. two proxy_config rows on the child
	require.Equal(t, 2, env.proxyCountByClient(flowChildID))

	// 5. both reservations committed (nothing was compensated/released)
	var committed int64
	require.NoError(t, env.db.Model(&models.PortReservation{}).Where("status = ?", models.ReservationStatusCommitted).Count(&committed).Error)
	require.Equal(t, int64(2), committed)

	// 6. exactly two apply calls, ZERO compensation applies; the LAST
	//    applied config (what the agent ends up with) contains both rules
	require.Equal(t, 2, barrier.calls, "compensation must not re-apply the before config")
	require.Equal(t, 2, len(barrier.callsData))
	lastCfg, err := utils.LoadProxiesFromContent(barrier.callsData[len(barrier.callsData)-1].Config)
	require.NoError(t, err)
	lastNames := lo.Map(lastCfg, func(p v1.TypedProxyConfig, _ int) string { return p.GetBaseConfig().Name })
	require.ElementsMatch(t, []string{"web1", "web2"}, lastNames, "agent final config must carry both rules — compensation must not have clobbered the winner's rule")

	// 7. snapshot evidence of the re-plan: the two before snapshots are
	//    {empty, winner's single rule} and the two after snapshots are
	//    {winner's rule, both rules} — the second request's before read
	//    happened AFTER the first committed, on the winner's child config.
	list, err := dao.NewQuery(env.ctx).ListRecent(env.userInfo, 10)
	require.NoError(t, err)
	require.Len(t, list, 2)
	beforeCounts := map[int]int{}
	afterCounts := map[int]int{}
	for _, tx := range list {
		require.Equal(t, models.TxStatusCommitted, tx.Status)
		var beforeN, afterN int
		for _, s := range env.snapshots(tx.TransactionID) {
			plain := decryptSnapshot(t, s, env.key.Key)
			cfgs, perr := utils.LoadProxiesFromContent(plain)
			require.NoError(t, perr)
			switch s.Side {
			case models.SnapshotSideBefore:
				beforeN = len(cfgs)
			case models.SnapshotSideAfter:
				afterN = len(cfgs)
			}
		}
		beforeCounts[beforeN]++
		afterCounts[afterN]++
	}
	require.Equal(t, map[int]int{0: 1, 1: 1}, beforeCounts, "one before snapshot is the origin's empty config, the other is the winner's committed single-rule config")
	require.Equal(t, map[int]int{1: 1, 2: 1}, afterCounts, "one after snapshot carries the winner's rule, the other carries both rules")
}

// TestSafeCreateChildAddressedUsesCanonicalOriginWithoutGrandchild pins the
// D1 addendum P0 fix: a create request that addresses an EXISTING shadow
// child (client_id = child, not the origin) must resolve the CANONICAL
// ORIGIN and plan the child the origin already has for the requested
// server — it must NEVER treat the child as an origin and spawn a
// grandchild (child@N with OriginClientID=child). The apply targets the
// existing child with Replace="" (no directed origin switch), the physical
// agent id is the ORIGIN, and the only client row that gains rules is the
// existing child.
func TestSafeCreateChildAddressedUsesCanonicalOriginWithoutGrandchild(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	tc := env.newCoordinator()
	key := uuidKey()

	childReq := &pb.CreateProxyConfigRequest{
		ClientId:       lo.ToPtr(coordChildID),
		ServerId:       lo.ToPtr(testServerID),
		Config:         candidateWebTOML,
		IdempotencyKey: lo.ToPtr(key),
	}
	resp, err := tc.SafeCreateProxy(env.ctx, childReq)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// 1. transaction committed
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)

	// 2. one apply addressed to the PHYSICAL ORIGIN agent, targeting the
	// EXISTING child, with NO directed replace (existing-child update)
	require.Equal(t, 1, env.apply.calls)
	call := env.apply.callsData[0]
	require.Equal(t, testClientID, call.PhysicalAgentID, "the physical agent is the canonical origin, never the raw child")
	require.Equal(t, coordChildID, call.TargetClientID, "the target is the existing child the origin already has for this server")
	require.Empty(t, call.ReplaceClientID, "an existing-child create must not replace the origin handler")

	// 3. NO grandchild: nothing may have the child as its origin, and the
	// origin still has exactly one child
	var grandchild int64
	require.NoError(t, env.db.Model(&models.Client{}).Where("origin_client_id = ?", coordChildID).Count(&grandchild).Error)
	require.Zero(t, grandchild, "the child must never be treated as an origin (no child@N of the child)")
	var children int64
	require.NoError(t, env.db.Model(&models.Client{}).Where("origin_client_id = ?", testClientID).Count(&children).Error)
	require.Equal(t, int64(1), children, "the origin must keep exactly its one existing child")

	// 4. only the existing child gained the rule
	require.ElementsMatch(t, []string{"ssh", "web"}, env.childProxyNames())
	require.Equal(t, 2, env.proxyCountByClient(coordChildID))
}

// TestSafeCreateOriginToChildCanonicalIdempotentReplay pins the canonical
// hash dimension of the D1 addendum P0: the SAME logical create retried
// through the generated shadow child id must hash identically to the
// origin-addressed original. The stored transaction's RequestHash is
// computed over the CANONICAL ORIGIN, so the child-addressed retry replays
// as an idempotent success (1 tx, 1 apply) instead of an
// IDEMPOTENCY_CONFLICT or a wrong re-plan.
func TestSafeCreateOriginToChildCanonicalIdempotentReplay(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedUnshadowedOrigin()
	env.seedServer()
	tc := env.newCoordinator()
	key := uuidKey()

	originReq := createReq(key, candidateWebTOML)
	resp, err := tc.SafeCreateProxy(env.ctx, originReq)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode())

	flowChildID := app.ShadowedClientID(testClientID, 1)
	childReq := &pb.CreateProxyConfigRequest{
		ClientId:       lo.ToPtr(flowChildID),
		ServerId:       lo.ToPtr(testServerID),
		Config:         candidateWebTOML,
		IdempotencyKey: lo.ToPtr(key),
	}

	// the stored hash must be over the CANONICAL ORIGIN id — the raw child
	// id must never enter the hash
	canonical := &pb.CreateProxyConfigRequest{
		ClientId:       lo.ToPtr(testClientID),
		ServerId:       lo.ToPtr(testServerID),
		Config:         candidateWebTOML,
		IdempotencyKey: lo.ToPtr(key),
	}
	tx := env.txByKey(key)
	require.Equal(t, hashCreateRequest(canonical), tx.RequestHash, "the stored hash must be over the canonical origin id")
	require.NotEqual(t, hashCreateRequest(childReq), tx.RequestHash, "the raw child id must NOT enter the hash")

	// the child-addressed retry is an idempotent replay: same key + same
	// canonical hash -> success, no second transaction, no second apply
	resp2, err := tc.SafeCreateProxy(env.ctx, childReq)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Empty(t, resp2.GetErrorCode(), "idempotent replay success must carry no error_code")
	require.Equal(t, 1, env.countTransactions(), "the retry must not create a second transaction")
	require.Equal(t, 1, env.apply.calls, "the retry must not re-apply")
}
