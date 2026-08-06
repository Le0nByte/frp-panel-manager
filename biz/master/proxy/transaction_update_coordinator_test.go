package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---- candidate TOML payloads (each is exactly one tcp/udp rule) ----

// updateWebTOML keeps the same physical key (tcp/8000) as the seeded old
// rule and only changes the local target port — a real config change.
var updateWebTOML = []byte("[[proxies]]\nname = \"web\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 9090\nremotePort = 8000\n")

// updateWebPort9000TOML moves the rule to a NEW physical key (tcp/9000).
var updateWebPort9000TOML = []byte("[[proxies]]\nname = \"web\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 8080\nremotePort = 9000\n")

// updateWebUDPTOML changes the protocol on the SAME port (tcp/8000 ->
// udp/8000): the unique index makes this a DIFFERENT physical key.
var updateWebUDPTOML = []byte("[[proxies]]\nname = \"web\"\ntype = \"udp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 8080\nremotePort = 8000\n")

// updateWeb2TOML renames the rule while keeping the physical key (tcp/8000).
var updateWeb2TOML = []byte("[[proxies]]\nname = \"web2\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 8080\nremotePort = 8000\n")

// updOldCreateTx is the ORIGINAL create transaction id bound to the seeded
// committed reservation: SameKeyOwned compensation must never delete it.
const updOldCreateTx = "tx-create-web"

// ---- test doubles (DAO stores stay REAL; only the RPC-bound parts are
// faked, mirroring the SafeCreate suite) ----

// acquireCountingResStore wraps the real reservation store and counts
// TryAcquire calls — SameKeyOwned must never acquire. (Named distinctly
// from switch_apply_test.go's countingResStore, which counts Release.)
type acquireCountingResStore struct {
	ResStore
	acquires int64
}

func (s *acquireCountingResStore) TryAcquire(c *app.Context, u models.UserInfo, serverID, protocol string, port uint32, txID string, lease time.Duration, now time.Time) (*models.PortReservation, error) {
	atomic.AddInt64(&s.acquires, 1)
	return s.ResStore.TryAcquire(c, u, serverID, protocol, port, txID, lease, now)
}

func (s *acquireCountingResStore) acquireCalls() int64 { return atomic.LoadInt64(&s.acquires) }

// failingUpdateCommitStore wraps the REAL dao store and injects a single
// CommitUpdate failure — everything else stays on the real DAO.
type failingUpdateCommitStore struct {
	UpdateTxStore
	fail bool
}

func (s *failingUpdateCommitStore) CommitUpdate(c *app.Context, u models.UserInfo, in *dao.CommitUpdateInput) (uint, error) {
	if s.fail {
		return 0, errors.New("injected commit update db failure")
	}
	return s.UpdateTxStore.CommitUpdate(c, u, in)
}

// failingUpdateStatusStore injects a single UpdateStatus failure for one
// target status, mirroring failingStatusStore for the update interface.
type failingUpdateStatusStore struct {
	UpdateTxStore
	failStatus string
	failed     bool
}

func (s *failingUpdateStatusStore) UpdateStatus(c *app.Context, u models.UserInfo, txID, status, code, msg string) error {
	if !s.failed && status == s.failStatus {
		s.failed = true
		return errors.New("injected status transition failure")
	}
	return s.UpdateTxStore.UpdateStatus(c, u, txID, status, code, msg)
}

// failingSnapStore injects a single SaveEncrypted failure.
type failingSnapStore struct {
	SnapStore
	fail bool
}

func (s *failingSnapStore) SaveEncrypted(c *app.Context, userInfo models.UserInfo, txID, side, nodeType, nodeID string, plaintext, key []byte) (*models.ConfigSnapshot, error) {
	if s.fail {
		return nil, errors.New("injected snapshot save failure")
	}
	return s.SnapStore.SaveEncrypted(c, userInfo, txID, side, nodeType, nodeID, plaintext, key)
}

// scriptedVerifier returns scripted results per call (the last result
// repeats), so a test can make the post-apply verify fail while the
// compensation verify succeeds — impossible with the shared
// fakeVerifierService whose errors apply to every call.
type scriptedVerifier struct {
	mu            sync.Mutex
	applied       []error
	released      []error
	appliedCalls  int
	releasedCalls int
}

func (v *scriptedVerifier) VerifyApplied(context.Context, *app.Context, string, string, uint32) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.appliedCalls++
	if len(v.applied) == 0 {
		return nil
	}
	e := v.applied[0]
	if len(v.applied) > 1 {
		v.applied = v.applied[1:]
	}
	return e
}

func (v *scriptedVerifier) VerifyReleased(context.Context, *app.Context, string, string, uint32) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.releasedCalls++
	if len(v.released) == 0 {
		return nil
	}
	e := v.released[0]
	if len(v.released) > 1 {
		v.released = v.released[1:]
	}
	return e
}

// snapshotCapturingApply records how many BEFORE snapshots exist at the
// moment the forward Apply runs (the timing contract: both before rows are
// saved before any agent contact) and captures the applied config bytes.
type snapshotCapturingApply struct {
	inner      ApplyService
	db         *gorm.DB
	mu         sync.Mutex
	beforeRows int
	applied    []byte
}

func (s *snapshotCapturingApply) Apply(ctx context.Context, c *app.Context, req ApplyRequest) (string, error) {
	var n int64
	_ = s.db.Model(&models.ConfigSnapshot{}).Where("side = ?", models.SnapshotSideBefore).Count(&n)
	s.mu.Lock()
	s.beforeRows = int(n)
	s.applied = append([]byte(nil), req.Config...)
	s.mu.Unlock()
	return s.inner.Apply(ctx, c, req)
}

// ---- environment ----

type updateCoordEnv struct {
	*coordTestEnv
	udeps UpdateCoordinatorDeps
	res   *acquireCountingResStore
}

func newUpdateCoordEnv(t *testing.T) *updateCoordEnv {
	t.Helper()
	base := newCoordTestEnv(t)
	e := &updateCoordEnv{
		coordTestEnv: base,
		res:          &acquireCountingResStore{ResStore: base.deps.ResStore},
	}
	e.udeps = UpdateCoordinatorDeps{
		Query:            newDAOStore(),
		Tx:               newDAOStore(),
		Res:              e.res,
		Snap:             base.deps.SnapStore,
		KeyProvider:      base.key,
		Preflight:        base.pre,
		Apply:            base.apply,
		Verifier:         base.verify,
		OriginLocker:     NewOriginKeyedLock(),
		Now:              base.deps.Now,
		Lease:            base.deps.Lease,
		PreflightTimeout: 2 * time.Second,
		ApplyTimeout:     base.deps.ApplyTimeout,
		VerifyTimeout:    base.deps.VerifyTimeout,
		VerifyAttempts:   base.deps.VerifyAttempts,
		VerifyInterval:   base.deps.VerifyInterval,
		HashRequest:      hashUpdateRequest,
	}
	return e
}

func (e *updateCoordEnv) newUpdateCoordinator() *UpdateTransactionCoordinator {
	return &UpdateTransactionCoordinator{deps: e.udeps}
}

// oldRuleSeed describes the ACTIVE old rule row a test world seeds.
type oldRuleSeed struct {
	name     string
	protocol string
	remote   uint32
	local    uint32
}

func typedProxy(name, protocol string, remote, local uint32) v1.TypedProxyConfig {
	if protocol == "udp" {
		p := &v1.UDPProxyConfig{ProxyBaseConfig: v1.ProxyBaseConfig{Name: name, Type: "udp"}, RemotePort: int(remote)}
		p.LocalIP = "127.0.0.1"
		p.LocalPort = int(local)
		return v1.TypedProxyConfig{ProxyConfigurer: p}
	}
	return tcpProxy(name, remote, local)
}

func (e *updateCoordEnv) seedServer() {
	e.coordTestEnv.seedServer()
}

// seedExistingChildWorld builds the standard shadowed world: origin +
// child carrying the old rule + server + the old ACTIVE proxy_config row
// (on the child). When seedRes is true a COMMITTED reservation owned by
// the old row (transaction_id = updOldCreateTx) exists at the old key.
func (e *updateCoordEnv) seedExistingChildWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))

	oldTyped := typedProxy(old.name, old.protocol, old.remote, old.local)
	child := &models.ClientEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = []v1.TypedProxyConfig{oldTyped}
	require.NoError(t, child.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, child))
	e.seedServer()

	content, err := oldTyped.MarshalJSON()
	require.NoError(t, err)
	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: old.name, Type: old.protocol,
		TenantID: e.userInfo.GetTenantID(), UserID: e.userInfo.GetUserID(),
		Content: content,
	}}
	require.NoError(t, e.db.Create(oldRow).Error)

	if seedRes {
		require.NoError(t, e.db.Create(&models.PortReservation{
			TenantID: e.userInfo.GetTenantID(), ServerID: testServerID,
			Protocol: old.protocol, RemotePort: old.remote,
			TransactionID: updOldCreateTx, Status: models.ReservationStatusCommitted,
			OwnerProxyID: &oldRow.ID,
		}).Error)
	}
	return oldRow
}

// seedFirstShadowWorld builds the never-shadowed origin world: origin
// client with the old rule in its config + server + the old ACTIVE
// proxy_config row ON the origin (client_id=origin, no origin link).
// stoppedSibling optionally seeds a same-server STOPPED sibling row on the
// origin (the migration contract: it must survive the first shadow).
func (e *updateCoordEnv) seedFirstShadowWorld(stoppedSibling bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	oldTyped := typedProxy(old.name, old.protocol, old.remote, old.local)
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: false, ConnectSecret: "origin-secret"}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = []v1.TypedProxyConfig{oldTyped}
	require.NoError(t, origin.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
	e.seedServer()

	content, err := oldTyped.MarshalJSON()
	require.NoError(t, err)
	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: testClientID, ServerID: testServerID,
		Name: old.name, Type: old.protocol,
		TenantID: e.userInfo.GetTenantID(), UserID: e.userInfo.GetUserID(),
		Content: content,
	}}
	require.NoError(t, e.db.Create(oldRow).Error)

	if stoppedSibling {
		sibTyped := typedProxy("stop-sib", "tcp", 7000, 7000)
		sib, err := sibTyped.MarshalJSON()
		require.NoError(t, err)
		require.NoError(t, e.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: testClientID, ServerID: testServerID,
			Name: "stop-sib", Type: "tcp", Stopped: true,
			TenantID: e.userInfo.GetTenantID(), UserID: e.userInfo.GetUserID(),
			Content: sib,
		}}).Error)
	}
	return oldRow
}

// proxyRowByID loads a proxy_config row by id (nil when missing).
func (e *updateCoordEnv) proxyRowByID(id uint) *models.ProxyConfig {
	t := e.t
	t.Helper()
	var row models.ProxyConfig
	err := e.db.Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(t, err)
	return &row
}

// proxyRowByName finds a proxy_config row by (client_id, name).
func (e *updateCoordEnv) proxyRowByName(clientID, name string) *models.ProxyConfig {
	t := e.t
	t.Helper()
	var row models.ProxyConfig
	err := e.db.Where("client_id = ? AND name = ?", clientID, name).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	require.NoError(t, err)
	return &row
}

func updateReq(idemKey, clientID, name string, config []byte) *pb.UpdateProxyConfigRequest {
	return &pb.UpdateProxyConfigRequest{
		ClientId:       lo.ToPtr(clientID),
		ServerId:       lo.ToPtr(testServerID),
		Name:           lo.ToPtr(name),
		Config:         config,
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

func assertUpdateInvalidCode(t *testing.T, resp *pb.UpdateProxyConfigResponse, code string) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, code, resp.GetErrorCode(), "structured error_code must carry the stable code")
	require.False(t, stringsHasPrefix(resp.GetStatus().GetMessage(), code+":"),
		"message %q must stay human-readable without the %q prefix", resp.GetStatus().GetMessage(), code)
}

func stringsHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// seedIdemTx plants a transaction row for idempotency replay tests.
func (e *updateCoordEnv) seedIdemTx(txID, key, status, code, msg string, req *pb.UpdateProxyConfigRequest) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: key,
		RequestHash:  hashUpdateRequest(req, testClientID),
		Operation:    models.TxOperationUpdate,
		Status:       status,
		ErrorCode:    code,
		ErrorMessage: msg,
		ActorUserID:  e.userInfo.GetUserID(),
		TenantID:     e.userInfo.GetTenantID(),
		StartedAt:    e.now,
	}).Error)
}

func (e *updateCoordEnv) snapByType(snaps []*models.ConfigSnapshot, side, nodeType string) *models.ConfigSnapshot {
	for _, s := range snaps {
		if s.Side == side && s.NodeType == nodeType {
			return s
		}
	}
	return nil
}

// ---- tests ----

func TestSafeUpdateSameKeyOwnedHappy(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// reservation mode: SameKeyOwned -> NO TryAcquire, no bind probe,
	// owner-proof (VerifyApplied) + post-apply VerifyApplied
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must never TryAcquire")
	require.Zero(t, env.pre.serverProbeCalls, "same-key owner path must not run the server bind probe")
	require.Equal(t, 1, env.pre.clientGateCalls, "the client local gate must still run")
	require.Equal(t, 2, env.verify.appliedCalls, "owner-proof + post-apply verify")
	require.Zero(t, env.verify.releasedCalls)

	// tx committed, operation=update, ProxyID = the OLD id (same-name
	// same-key rebuild reuses the row id — the lease identity)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, models.TxOperationUpdate, tx.Operation)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, oldRow.ID, *tx.ProxyID)

	// the committed reservation is untouched: owner stays old id and the
	// transaction_id remains the ORIGINAL create tx (compensation must
	// never be able to delete it)
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite the old committed transaction_id")

	// one synchronous apply to the child, no replacement
	require.Equal(t, 1, env.apply.calls)
	call := env.apply.callsData[0]
	require.Equal(t, testClientID, call.PhysicalAgentID)
	require.Equal(t, coordChildID, call.TargetClientID)
	require.Empty(t, call.ReplaceClientID, "an existing-child update must not replace any handler")
	require.Equal(t, testServerID, call.ServerID)
	require.NotEmpty(t, call.Config)

	// the child config really changed (local target 8080 -> 9090)
	childCfg, err := env.childClient().GetConfigContent()
	require.NoError(t, err)
	require.Len(t, childCfg.Proxies, 1)
	web, ok := childCfg.Proxies[0].ProxyConfigurer.(*v1.TCPProxyConfig)
	require.True(t, ok)
	require.Equal(t, 9090, web.LocalPort)

	// snapshots: 2 before (client+proxy) + 2 after (client+proxy)
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 4)
	require.NotNil(t, env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeClient))
	require.NotNil(t, env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy))
	require.NotNil(t, env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeClient))
	require.NotNil(t, env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy))

	// update audit, never config plaintext
	audits := env.auditEvents(tx.TransactionID)
	require.NotEmpty(t, audits)
	for _, a := range audits {
		require.NotContains(t, a.Summary, "[[proxies]]")
		require.NotContains(t, a.Summary, "remotePort")
	}
}

func TestSafeUpdateSameKeyLegacyPending(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, int64(1), env.res.acquireCalls(), "legacy same-key world must TryAcquire exactly once")
	require.Zero(t, env.pre.serverProbeCalls, "owner-proof still replaces the bind probe")
	require.Equal(t, 1, env.pre.clientGateCalls)
	require.Equal(t, 2, env.verify.appliedCalls, "owner-proof + post-apply verify")

	// the pending lease flipped to committed with owner=final
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.NotNil(t, res.OwnerProxyID)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, env.txByKey(key).TransactionID, res.TransactionID)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
}

func TestSafeUpdateDifferentKeyOldExpected(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, int64(1), env.res.acquireCalls(), "different key must TryAcquire the new key")
	require.Equal(t, 1, env.pre.serverProbeCalls, "different key runs the full bind probe")
	require.Equal(t, 1, env.pre.clientGateCalls)
	require.Equal(t, 1, env.verify.appliedCalls, "post-apply verify only (no owner-proof)")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)

	// new key committed and owned by the final row; old committed HARD-deleted
	newRes := env.reservationByPort(9000)
	require.NotNil(t, newRes)
	require.Equal(t, models.ReservationStatusCommitted, newRes.Status)
	require.NotNil(t, newRes.OwnerProxyID)
	require.Equal(t, *tx.ProxyID, *newRes.OwnerProxyID)
	require.Nil(t, env.reservationByPort(8000), "old committed reservation must be released")

	// the child config carries the new port
	childCfg, err := env.childClient().GetConfigContent()
	require.NoError(t, err)
	web, ok := childCfg.Proxies[0].ProxyConfigurer.(*v1.TCPProxyConfig)
	require.True(t, ok)
	require.Equal(t, 9000, web.RemotePort)
}

func TestSafeUpdateProtocolChangeSamePort(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebUDPTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// protocol is inside the unique index: tcp/8000 -> udp/8000 is a
	// DIFFERENT key — old tcp row deleted, new udp row committed
	require.Equal(t, int64(1), env.res.acquireCalls())
	var tcpRows int64
	require.NoError(t, env.db.Model(&models.PortReservation{}).Where("protocol = ? AND remote_port = ?", "tcp", 8000).Count(&tcpRows).Error)
	require.Zero(t, tcpRows, "the old tcp reservation must be gone")
	udpRes := env.reservationByPort(8000)
	require.NotNil(t, udpRes)
	require.Equal(t, "udp", udpRes.Protocol)
	require.Equal(t, models.ReservationStatusCommitted, udpRes.Status)

	// the child config really changed to udp
	childCfg, err := env.childClient().GetConfigContent()
	require.NoError(t, err)
	web, ok := childCfg.Proxies[0].ProxyConfigurer.(*v1.UDPProxyConfig)
	require.True(t, ok)
	require.Equal(t, "udp", web.GetBaseConfig().Type)
}

func TestSafeUpdateFirstShadow(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedFirstShadowWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// origin converted to shadow in the commit transaction
	origin := env.clientByID(testClientID)
	require.True(t, origin.IsShadow)
	require.Empty(t, origin.ConfigContent)
	require.Empty(t, origin.ServerID)

	// the generated child exists with the applied rule
	childID := app.ShadowedClientID(testClientID, 1)
	child := env.clientByID(childID)
	require.Equal(t, testClientID, child.OriginClientID)
	require.Equal(t, testServerID, child.ServerID)
	childCfg, err := child.GetConfigContent()
	require.NoError(t, err)
	require.Len(t, childCfg.Proxies, 1)

	// active origin rows deleted; the stopped sibling migrated with its id
	var originRows int64
	require.NoError(t, env.db.Model(&models.ProxyConfig{}).Where("client_id = ?", testClientID).Count(&originRows).Error)
	require.Zero(t, originRows, "no active row may survive on the shadow origin")
	require.Nil(t, env.proxyRowByID(oldRow.ID), "the old active row is deleted by the first shadow")
	sib := env.proxyRowByName(childID, "stop-sib")
	require.NotNil(t, sib)
	require.Equal(t, testClientID, sib.OriginClientID)
	require.True(t, sib.Stopped)

	// forward apply: directed switch origin -> child
	require.Equal(t, 1, env.apply.calls)
	call := env.apply.callsData[0]
	require.Equal(t, testClientID, call.PhysicalAgentID)
	require.Equal(t, childID, call.TargetClientID)
	require.Equal(t, testClientID, call.ReplaceClientID, "first shadow must switch the origin handler away")

	// legacy world (no reservation row): SameKeyPending -> pending committed
	require.Equal(t, int64(1), env.res.acquireCalls())
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.NotEqual(t, oldRow.ID, *tx.ProxyID, "first-shadow rebuild creates a fresh row id")
	require.Equal(t, *tx.ProxyID, *res.OwnerProxyID)
}

func TestSafeUpdateRename(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWeb2TOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotNil(t, tx.ProxyID)
	require.NotEqual(t, oldRow.ID, *tx.ProxyID, "rename rebuilds a fresh row id")

	// old row gone, new row present; owner CAS migrated to the new id and
	// the transaction_id of the committed lease is untouched
	require.Nil(t, env.proxyRowByID(oldRow.ID))
	newRow := env.proxyRowByName(coordChildID, "web2")
	require.NotNil(t, newRow)
	require.Equal(t, *tx.ProxyID, newRow.ID)
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, newRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID)

	// the child config filters the OLD name and carries the new one
	childCfg, err := env.childClient().GetConfigContent()
	require.NoError(t, err)
	require.Len(t, childCfg.Proxies, 1)
	require.Equal(t, "web2", childCfg.Proxies[0].GetBaseConfig().Name)
	require.Equal(t, 1, env.apply.calls)
	require.Contains(t, string(env.apply.callsData[0].Config), "web2")
	require.NotContains(t, string(env.apply.callsData[0].Config), `"name":"web"`)
}

func TestSafeUpdateRenameConflictPrecheck(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// another ACTIVE row already occupies the new name in the origin scope
	conflictTyped := typedProxy("web2", "tcp", 9001, 8080)
	conflictContent, err := conflictTyped.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: "web2", Type: "tcp",
		TenantID: env.userInfo.GetTenantID(), UserID: env.userInfo.GetUserID(),
		Content: conflictContent,
	}}).Error)

	uc := env.newUpdateCoordinator()
	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWeb2TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls, "rename conflict must fail before any apply")
	require.Zero(t, env.res.acquireCalls())
	require.Zero(t, env.countTransactions(), "the conflict is detected before any tx row is created")
}

func TestSafeUpdateSameKeyForeignListenerOwnerProofFails(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.verify.appliedErr = errors.New("port not served by the embedded frps (owner: pid=123 service=frps-phone)")
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
	require.Zero(t, env.apply.calls, "owner-proof failure must stop before any apply")
	require.Zero(t, env.pre.serverProbeCalls, "the same-key path never runs the bind probe")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortRemoteInUse, tx.ErrorCode)
	// SameKeyOwned has no pending lease: the old committed row survives
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
}

func TestSafeUpdateDifferentKeyServerGateBusinessFail(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.serverProbeResult = &PreflightGateResult{Ok: false, ErrorCode: defs.ErrorCodePortRemoteInUse, ErrorMessage: "port occupied by another process"}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
	require.Zero(t, env.apply.calls, "a failed gate must never reach Apply")
	require.Zero(t, env.pre.clientGateCalls, "the client gate must not run after the server gate rejected")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortRemoteInUse, tx.ErrorCode)
	require.Nil(t, env.reservationByPort(9000), "the new-key pending lease must be released")
	require.NotNil(t, env.reservationByPort(8000), "the old committed lease stays")
}

func TestSafeUpdateClientGateFail(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.clientGateResult = &PreflightGateResult{Ok: false, ErrorCode: defs.ErrorCodePortLocalNotListening, ErrorMessage: "no listener on 127.0.0.1:8080"}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodePortLocalNotListening)
	require.Zero(t, env.apply.calls)
	require.Equal(t, 1, env.pre.serverProbeCalls, "the server gate ran first and passed")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortLocalNotListening, tx.ErrorCode)
	require.Nil(t, env.reservationByPort(9000))
}

func TestSafeUpdateServerGateTransportFail(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.serverProbeErr = errors.New("cannot get client")
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeAgentOffline)
	require.Zero(t, env.apply.calls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeAgentOffline, tx.ErrorCode)
}

func TestSafeUpdateIdempotencyCommittedReplay(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()
	req := updateReq(key, testClientID, "web", updateWebTOML)

	resp, err := uc.SafeUpdateProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	resp2, err := uc.SafeUpdateProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls, "a committed replay must never re-apply")
	require.Equal(t, 1, env.countTransactions())
	require.Equal(t, models.TxStatusCommitted, env.txByKey(key).Status)
}

func TestSafeUpdateIdempotencyOriginToChildReplay(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedFirstShadowWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()
	reqOrigin := updateReq(key, testClientID, "web", updateWebTOML)

	resp, err := uc.SafeUpdateProxy(env.ctx, reqOrigin)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls)

	// the SAME logical request retried through the generated child id: the
	// canonical hash resolves to the origin, so it must replay the committed
	// tx instead of running a second update.
	childID := app.ShadowedClientID(testClientID, 1)
	reqChild := updateReq(key, childID, "web", updateWebTOML)
	resp2, err := uc.SafeUpdateProxy(env.ctx, reqChild)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls, "origin->child canonical replay must not re-apply")
	require.Equal(t, 1, env.countTransactions())
	require.Equal(t, models.TxStatusCommitted, env.txByKey(key).Status)
}

func TestSafeUpdateIdempotencyDifferentHash(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	_, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeIdempotencyConflict)
	require.Equal(t, 1, env.apply.calls, "the conflicting replay must not run")
	require.Equal(t, 1, env.countTransactions())
}

func TestSafeUpdateIdempotencyInflight(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()
	req := updateReq(key, testClientID, "web", updateWebTOML)
	env.seedIdemTx("tx-inflight", key, models.TxStatusPending, "", "", req)

	resp, err := uc.SafeUpdateProxy(env.ctx, req)
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeIdempotencyInFlight)
	require.Zero(t, env.apply.calls)
	require.Equal(t, 1, env.countTransactions())
}

func TestSafeUpdateIdempotencyTerminalReplay(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()
	req := updateReq(key, testClientID, "web", updateWebTOML)
	env.seedIdemTx("tx-failed", key, models.TxStatusFailed, defs.ErrorCodePortRemoteInUse, "remote port is busy", req)

	resp, err := uc.SafeUpdateProxy(env.ctx, req)
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
	require.Equal(t, "remote port is busy", resp.GetStatus().GetMessage())
	require.Zero(t, env.apply.calls)
}

func TestSafeUpdateCommitFailureCompensationSameKey(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStore := &failingUpdateCommitStore{UpdateTxStore: newDAOStore(), fail: true}
	env.udeps.Tx = failStore
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	// compensation: restore apply + old-applied verify; rolled_back
	require.Equal(t, 2, env.apply.calls, "forward apply + restore apply")
	restore := env.apply.callsData[1]
	require.Equal(t, testClientID, restore.PhysicalAgentID)
	require.Equal(t, coordChildID, restore.TargetClientID)
	require.Empty(t, restore.ReplaceClientID, "existing-child restore never replaces")
	require.Equal(t, 3, env.verify.appliedCalls, "owner-proof + post-apply + restore-verify")
	require.Zero(t, env.verify.releasedCalls, "same-key compensation verifies applied, never released")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)

	// the OLD committed reservation is untouched (SameKeyOwned: no pending
	// to release; the old create tx keeps ownership)
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID)

	var compFound bool
	for _, a := range env.auditEvents(tx.TransactionID) {
		if a.EventType == models.AuditEventTypeCompensation && a.Success {
			compFound = true
		}
		require.NotContains(t, a.Summary, "[[proxies]]")
	}
	require.True(t, compFound, "a successful compensation audit event must exist")
}

func TestSafeUpdateCommitFailureCompensationDifferentKey(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStore := &failingUpdateCommitStore{UpdateTxStore: newDAOStore(), fail: true}
	env.udeps.Tx = failStore
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls)
	require.Equal(t, 2, env.verify.appliedCalls, "post-apply + restore-old")
	require.Equal(t, 1, env.verify.releasedCalls, "different-key compensation must verify the new port released")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.NotNil(t, env.reservationByPort(8000), "old committed lease survives")
	require.Equal(t, oldRow.ID, *env.reservationByPort(8000).OwnerProxyID)
	require.Nil(t, env.reservationByPort(9000), "the new-key pending lease is released")
}

func TestSafeUpdateCompensationReleaseFailRetainsPending(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStore := &failingUpdateCommitStore{UpdateTxStore: newDAOStore(), fail: true}
	env.udeps.Tx = failStore
	env.verify.releasedErr = errors.New("new port still occupied")
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	// the current tx's pending lease is RETAINED for manual intervention
	res := env.reservationByPort(9000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusPending, res.Status)
	require.Equal(t, tx.TransactionID, res.TransactionID)
	require.NotNil(t, env.reservationByPort(8000), "old committed lease survives")
}

func TestSafeUpdateApplyFailureCompensates(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyRejected, err: errors.New("agent rejected config")},
		{}, // the compensation restore apply must succeed (last result repeats)
	}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeApplyRejected)

	require.Equal(t, 2, env.apply.calls, "forward apply (rejected) + restore apply")
	require.Equal(t, 2, env.verify.appliedCalls, "owner-proof + restore-verify")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyRejected, tx.ErrorCode)
}

func TestSafeUpdatePostVerifyFailureCompensates(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	verifier := &scriptedVerifier{
		applied:  []error{errors.New("verify failed: port not served by the embedded frps"), nil},
		released: []error{nil},
	}
	env.udeps.Verifier = verifier
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)

	require.Equal(t, 2, verifier.appliedCalls, "post-apply verify failed + restore verify succeeded")
	require.Equal(t, 1, verifier.releasedCalls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeVerifyFailed, tx.ErrorCode)
}

func TestSafeUpdateStatusTransitionFailurePreApply(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStore := &failingUpdateStatusStore{UpdateTxStore: newDAOStore(), failStatus: models.TxStatusApplying}
	env.udeps.Tx = failStore
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	_, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.Error(t, err, "a status-transition failure before apply surfaces as an internal error")
	require.Zero(t, env.apply.calls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.countReservations(), "the pending lease must be released on the pre-apply failure")
}

func TestSafeUpdateVerifyingTransitionFailureCompensationSucceeded(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStore := &failingUpdateStatusStore{UpdateTxStore: newDAOStore(), failStatus: models.TxStatusVerifying}
	env.udeps.Tx = failStore
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)
	require.Equal(t, 2, env.apply.calls, "forward apply plus successful restore apply")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	var res models.PortReservation
	require.NoError(t, env.db.Where("server_id = ? AND protocol = ? AND remote_port = ?",
		testServerID, "tcp", 8000).First(&res).Error)
	require.Equal(t, updOldCreateTx, res.TransactionID, "old committed reservation must survive compensation")
}

func TestSafeUpdateVerifyingTransitionFailureCompensationFailed(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStore := &failingUpdateStatusStore{UpdateTxStore: newDAOStore(), failStatus: models.TxStatusVerifying}
	env.udeps.Tx = failStore
	env.apply.results = []applyResult{
		{},
		{code: defs.ErrorCodeApplyRejected, err: errors.New("injected restore rejection")},
	}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
}

func TestSafeUpdateSnapshotKeyFailurePreApply(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.udeps.KeyProvider = &failKeyProvider{}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	_, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.Error(t, err, "a missing snapshot key is a hard error")
	require.Zero(t, env.apply.calls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.countReservations(), "the pending lease must be released (pre-apply failure)")
}

func TestSafeUpdateSnapshotSaveFailurePreApply(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	snapStore := &failingSnapStore{SnapStore: env.udeps.Snap, fail: true}
	env.udeps.Snap = snapStore
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	_, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.Error(t, err)
	require.Zero(t, env.apply.calls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.countReservations())
}

func TestSafeUpdateSnapshotsBeforeApplyAndAfter(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	cap := &snapshotCapturingApply{inner: env.apply, db: env.db}
	env.udeps.Apply = cap
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// timing contract: BOTH before rows exist the moment Apply runs
	cap.mu.Lock()
	require.Equal(t, 2, cap.beforeRows, "client-before + proxy-before must exist before any Apply")
	applied := append([]byte(nil), cap.applied...)
	cap.mu.Unlock()

	tx := env.txByKey(key)
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 4)
	beforeClient := env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeClient)
	beforeProxy := env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy)
	afterClient := env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeClient)
	afterProxy := env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
	require.NotNil(t, beforeClient)
	require.NotNil(t, beforeProxy)
	require.NotNil(t, afterClient)
	require.NotNil(t, afterProxy)

	// node identity: client-before binds the child (existing flow),
	// proxy-before binds the OLD rule row, after rows bind child/final row
	require.Equal(t, coordChildID, beforeClient.NodeID)
	require.Equal(t, strconv.FormatUint(uint64(oldRow.ID), 10), beforeProxy.NodeID)
	require.Equal(t, coordChildID, afterClient.NodeID)
	require.Equal(t, strconv.FormatUint(uint64(*tx.ProxyID), 10), afterProxy.NodeID)

	// the client-after snapshot stores exactly what was applied
	afterPlain := decryptSnapshot(t, afterClient, env.key.Key)
	require.Equal(t, applied, afterPlain)

	// the proxy-before envelope is the versioned typed-rule state
	proxyPlain := decryptSnapshot(t, beforeProxy, env.key.Key)
	var envp updateProxyEnvelope
	require.NoError(t, json.Unmarshal(proxyPlain, &envp))
	require.Equal(t, 1, envp.Version)
	require.Equal(t, oldRow.ID, envp.ProxyID)
	require.Equal(t, "web", envp.Name)
	require.Equal(t, testServerID, envp.ServerID)
	require.Equal(t, coordChildID, envp.ClientID)
	require.Equal(t, testClientID, envp.OriginClientID)
	require.False(t, envp.Stopped)
	require.Equal(t, oldRow.Content, envp.Typed)

	// no plaintext config anywhere in the trail
	for _, s := range snaps {
		require.NotContains(t, string(s.Ciphertext), "remotePort", "snapshot rows store ciphertext only")
	}
	for _, a := range env.auditEvents(tx.TransactionID) {
		require.NotContains(t, a.Summary, "[[proxies]]")
		require.NotContains(t, a.Summary, "remotePort")
	}
}

func TestSafeUpdateCrossTenantReservationConflict(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// another tenant already owns the physical new key (the unique index is
	// GLOBAL): the tenant-scoped plan sees nothing, TryAcquire fails closed
	foreignOwner := uint(999)
	require.NoError(t, env.db.Create(&models.PortReservation{
		TenantID: 2, ServerID: testServerID, Protocol: "tcp", RemotePort: 9000,
		TransactionID: "tx-other-tenant", Status: models.ReservationStatusCommitted,
		OwnerProxyID: &foreignOwner,
	}).Error)
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebPort9000TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeReservationConflict)
	require.Zero(t, env.apply.calls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeReservationConflict, tx.ErrorCode)
	require.Equal(t, 2, env.countReservations(), "both the old lease and the foreign lease survive")
}

func TestSafeUpdateCrossTenantRuleNotFound(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedServer()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.userInfo, origin))
	// the only "web" row belongs to ANOTHER tenant: the tenant-scoped old
	// rule lookup must fail closed (not found), never leak the row
	foreignTyped := typedProxy("web", "tcp", 8000, 8080)
	content, err := foreignTyped.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: "cli-other", OriginClientID: testClientID, ServerID: testServerID,
		Name: "web", Type: "tcp", TenantID: 2, UserID: 99, Content: content,
	}}).Error)
	uc := env.newUpdateCoordinator()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

func TestSafeUpdateOldRuleMissing(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	require.NoError(t, env.db.Delete(&models.ProxyConfig{}, "id = ?", oldRow.ID).Error)
	uc := env.newUpdateCoordinator()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

func TestSafeUpdateAmbiguousOldRuleFailsClosed(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedServer()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.userInfo, origin))
	ambTyped := typedProxy("web", "tcp", 8000, 8080)
	content, err := ambTyped.MarshalJSON()
	require.NoError(t, err)
	// TWO rows match the same natural key in the origin scope: one legacy
	// shape (on the origin) and one shadow shape (on the child)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: testClientID, ServerID: testServerID,
		Name: "web", Type: "tcp",
		TenantID: env.userInfo.GetTenantID(), UserID: env.userInfo.GetUserID(), Content: content,
	}}).Error)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: "cli-1-1", OriginClientID: testClientID, ServerID: testServerID,
		Name: "web", Type: "tcp",
		TenantID: env.userInfo.GetTenantID(), UserID: env.userInfo.GetUserID(), Content: content,
	}}).Error)
	uc := env.newUpdateCoordinator()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

func TestSafeUpdateSameKeyReservationConflict(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// another transaction holds a LIVE pending lease at the same key
	require.NoError(t, env.db.Create(&models.PortReservation{
		TenantID: env.userInfo.GetTenantID(), ServerID: testServerID,
		Protocol: "tcp", RemotePort: 8000,
		TransactionID: "tx-other", Status: models.ReservationStatusPending,
		ExpiresAt: env.now.Add(time.Hour),
	}).Error)
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeReservationConflict)
	require.Zero(t, env.apply.calls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeReservationConflict, tx.ErrorCode)
	// the other transaction's lease survives (released only by its owner)
	other := env.reservationByPort(8000)
	require.NotNil(t, other)
	require.Equal(t, "tx-other", other.TransactionID)
	require.Equal(t, models.ReservationStatusPending, other.Status)
}

// TestSafeUpdateStoppedLegacyOriginShapeRejected lives in
// transaction_update_stopped_coordinator_test.go (A4c: the stopped branch
// routes valid stopped shapes; the unsafe first-shadow shape is rejected
// there with zero tx/apply). This placeholder was the A4b guard that
// stopped rules are NOT routed here as active updates — replaced by the
// A4c stopped-branch suite.

func TestSafeUpdateValidationErrors(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()

	cases := []struct {
		name string
		req  *pb.UpdateProxyConfigRequest
		code string
	}{
		{"missing client_id", &pb.UpdateProxyConfigRequest{ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), Config: updateWebTOML, IdempotencyKey: lo.ToPtr(uuidKey())}, defs.ErrorCodeConfigValidationFailed},
		{"missing name", &pb.UpdateProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Config: updateWebTOML, IdempotencyKey: lo.ToPtr(uuidKey())}, defs.ErrorCodeConfigValidationFailed},
		{"missing config", &pb.UpdateProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}, defs.ErrorCodeConfigValidationFailed},
		{"missing idempotency key", &pb.UpdateProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), Config: updateWebTOML}, defs.ErrorCodeConfigValidationFailed},
		{"two rules in config", &pb.UpdateProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), Config: []byte("[[proxies]]\nname = \"a\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 1\nremotePort = 1\n\n[[proxies]]\nname = \"b\"\ntype = \"tcp\"\nlocalIP = \"127.0.0.1\"\nlocalPort = 2\nremotePort = 2\n"), IdempotencyKey: lo.ToPtr(uuidKey())}, defs.ErrorCodeConfigValidationFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := uc.SafeUpdateProxy(env.ctx, tc.req)
			require.NoError(t, err)
			assertUpdateInvalidCode(t, resp, tc.code)
		})
	}
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

func TestSafeUpdateRenameReservationModeIsSameKeyOwned(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	// rename keeps the physical key: reservation mode stays SameKeyOwned
	// (no TryAcquire) and the owner CAS moves old -> new row id
	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWeb2TOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Zero(t, env.res.acquireCalls(), "a same-key rename must not TryAcquire")

	tx := env.txByKey(key)
	newRow := env.proxyRowByName(coordChildID, "web2")
	require.NotNil(t, newRow)
	require.Equal(t, newRow.ID, *tx.ProxyID)
	res := env.reservationByPort(8000)
	require.Equal(t, newRow.ID, *res.OwnerProxyID)
	require.Equal(t, oldRow.ID, newRow.ID-1, "sanity: the fresh row got a new id")
	require.Equal(t, updOldCreateTx, res.TransactionID)
}
