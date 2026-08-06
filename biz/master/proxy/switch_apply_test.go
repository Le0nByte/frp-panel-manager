package proxy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Controller-backed Apply fake: it executes the SAME directed-switch
// sequence the real agent handler performs (biz/client.UpdateFrpcHander,
// whose behavior is pinned by update_tunnel_test.go in that package)
// against a miniature agent controller. The coordinator tests can then
// assert the FINAL agent handler state after a full apply + compensation
// chain — "only the origin running with the before config", "only the
// child running, no stale origin handler" (round-8 I2).
// ---------------------------------------------------------------------------

type agentHandler struct {
	mu      sync.Mutex
	cfg     []byte
	running bool
}

type agentController struct {
	mu       sync.Mutex
	handlers map[string]map[string]*agentHandler // clientID -> serverID -> handler
}

func newAgentController() *agentController {
	return &agentController{handlers: map[string]map[string]*agentHandler{}}
}

// stopDelete mirrors the agent's replace step: stop and delete ONLY the
// (clientID, serverID) pair. A missing target is a no-op (idempotent).
func (c *agentController) stopDelete(clientID, serverID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		return
	}
	if h, ok := c.handlers[clientID][serverID]; ok {
		h.mu.Lock()
		h.running = false
		h.mu.Unlock()
		delete(c.handlers[clientID], serverID)
	}
}

// addRun mirrors the agent's target step: update in place when the target
// handler already exists, otherwise create it — then ensure it runs.
func (c *agentController) addRun(clientID, serverID string, cfg []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		c.handlers[clientID] = map[string]*agentHandler{}
	}
	h, ok := c.handlers[clientID][serverID]
	if !ok {
		h = &agentHandler{}
		c.handlers[clientID][serverID] = h
	}
	h.mu.Lock()
	h.cfg = append([]byte(nil), cfg...)
	h.running = true
	h.mu.Unlock()
}

func (c *agentController) get(clientID, serverID string) *agentHandler {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers[clientID] == nil {
		return nil
	}
	return c.handlers[clientID][serverID]
}

func (h *agentHandler) config() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]byte(nil), h.cfg...)
}

func (h *agentHandler) isRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

// switchApplyService is an ApplyService that performs the directed switch
// on an agentController. It never fails (failures are injected via the
// Verifier / TxStore fakes instead).
type switchApplyService struct {
	mu    sync.Mutex
	calls []ApplyRequest
	ctrl  *agentController
}

func newSwitchApplyService() *switchApplyService {
	return &switchApplyService{ctrl: newAgentController()}
}

func (s *switchApplyService) Apply(_ context.Context, _ *app.Context, req ApplyRequest) (string, error) {
	s.mu.Lock()
	s.calls = append(s.calls, req)
	s.mu.Unlock()
	// directed switch (mirrors biz/client.UpdateFrpcHander)
	if req.ReplaceClientID != "" && req.ReplaceClientID != req.TargetClientID {
		s.ctrl.stopDelete(req.ReplaceClientID, req.ServerID)
	}
	s.ctrl.addRun(req.TargetClientID, req.ServerID, req.Config)
	return "", nil
}

// clientConfigNames extracts the proxy rule names from an applied config.
func clientConfigNames(t *testing.T, cfg []byte) []string {
	t.Helper()
	cfgs, err := utils.LoadProxiesFromContent(cfg)
	require.NoError(t, err)
	return lo.Map(cfgs, func(p v1.TypedProxyConfig, _ int) string { return p.GetBaseConfig().Name })
}

// configMetadataClientID extracts the FRPClientIDKey metadata stamped into
// an applied config (the identity the agent handler registers under).
func configMetadataClientID(t *testing.T, cfg []byte) string {
	t.Helper()
	parsed, err := utils.LoadClientConfigNormal(cfg, true)
	require.NoError(t, err)
	return parsed.ClientCommonConfig.Metadatas[defs.FRPClientIDKey]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestSafeCreateFirstShadowAgentEndStateOnlyChildRunning is the round-8 I2
// happy path at the agent level: after a successful first-shadow create the
// fake agent controller holds ONLY the child handler (running) — the origin
// handler was replaced, never left stale.
func TestSafeCreateFirstShadowAgentEndStateOnlyChildRunning(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
	env.seedServer()
	sw := newSwitchApplyService()
	env.deps.Apply = sw
	tc := env.newCoordinator()
	childID := app.ShadowedClientID(testClientID, 1)

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(uuidKey(), candidateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// forward apply: Target=child, Replace=origin
	require.Len(t, sw.calls, 1)
	require.Equal(t, testClientID, sw.calls[0].PhysicalAgentID)
	require.Equal(t, childID, sw.calls[0].TargetClientID)
	require.Equal(t, testClientID, sw.calls[0].ReplaceClientID)

	// final agent state: ONLY the child runs, origin handler is gone
	require.Nil(t, sw.ctrl.get(testClientID, testServerID), "no stale origin handler may survive")
	child := sw.ctrl.get(childID, testServerID)
	require.NotNil(t, child)
	require.True(t, child.isRunning())
	require.ElementsMatch(t, []string{"ssh", "web"}, clientConfigNames(t, child.config()))
	require.Equal(t, childID, configMetadataClientID(t, child.config()), "the child handler registers under its own identity")
}

// TestSafeCreateFirstShadowVerifyFailureRestoresOrigin is the round-8 I2
// compensation inverse: when verify fails after a first shadow, the restore
// apply must be the EXACT inverse (Target=origin, Replace=child) with the
// origin's own identity/metadata, and the final agent state must be only
// the origin running with the before config.
func TestSafeCreateFirstShadowVerifyFailureRestoresOrigin(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
	env.seedServer()
	env.verify.appliedErr = errors.New("server gate never saw the proxy")
	sw := newSwitchApplyService()
	env.deps.Apply = sw
	tc := env.newCoordinator()
	key := uuidKey()
	childID := app.ShadowedClientID(testClientID, 1)
	wantBefore := append([]byte(nil), env.clientByID(testClientID).ConfigContent...)

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Len(t, sw.calls, 2, "forward apply + compensation restore")

	forward, restore := sw.calls[0], sw.calls[1]
	require.Equal(t, testClientID, forward.ReplaceClientID)
	require.Equal(t, childID, forward.TargetClientID)
	// the true inverse
	require.Equal(t, testClientID, restore.TargetClientID, "compensation must restore the ORIGIN handler")
	require.Equal(t, childID, restore.ReplaceClientID, "compensation must stop the child handler")
	require.Equal(t, testServerID, restore.ServerID)

	// the restore config carries the ORIGIN identity and only the before rules
	require.Equal(t, testClientID, configMetadataClientID(t, restore.Config))
	require.ElementsMatch(t, []string{"ssh"}, clientConfigNames(t, restore.Config), "restore payload must be the before config, not the candidate")

	// final agent state: only the origin running with the before config
	origin := sw.ctrl.get(testClientID, testServerID)
	require.NotNil(t, origin)
	require.True(t, origin.isRunning())
	require.Nil(t, sw.ctrl.get(childID, testServerID), "the child handler must be gone after compensation")
	require.Equal(t, testClientID, configMetadataClientID(t, origin.config()))

	// the before snapshot is the origin's original config
	snaps := env.snapshots(tx.TransactionID)
	var beforeSnap *models.ConfigSnapshot
	for _, s := range snaps {
		if s.Side == models.SnapshotSideBefore {
			beforeSnap = s
		}
	}
	require.NotNil(t, beforeSnap)
	require.Equal(t, wantBefore, decryptSnapshot(t, beforeSnap, env.key.Key))
}

// TestSafeCreateFirstShadowCommitFailureRestoresOrigin: same inverse
// contract when the DB commit itself fails after the apply+verify.
func TestSafeCreateFirstShadowCommitFailureRestoresOrigin(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedUnshadowedOrigin(tcpProxy("ssh", 24, 22))
	env.seedServer()
	env.deps.TxStore = &failingCommitSnapshotStore{TxStore: newDAOStore()}
	sw := newSwitchApplyService()
	env.deps.Apply = sw
	tc := env.newCoordinator()
	key := uuidKey()
	childID := app.ShadowedClientID(testClientID, 1)

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	require.Len(t, sw.calls, 2)

	restore := sw.calls[1]
	require.Equal(t, testClientID, restore.TargetClientID)
	require.Equal(t, childID, restore.ReplaceClientID)
	require.Equal(t, testClientID, configMetadataClientID(t, restore.Config))

	origin := sw.ctrl.get(testClientID, testServerID)
	require.NotNil(t, origin)
	require.True(t, origin.isRunning())
	require.Nil(t, sw.ctrl.get(childID, testServerID), "no stale child handler after first-shadow compensation")
}

// deletingChildCommitStore simulates the round-8 I1 race: the legacy
// DeleteClientHandler hard-deletes the child row between plan and commit,
// right before the REAL CommitCreate runs.
type deletingChildCommitStore struct {
	TxStore
	childID string
}

func (s *deletingChildCommitStore) CommitCreate(c *app.Context, u models.UserInfo, in *dao.CommitCreateInput) (uint, error) {
	if s.childID != "" {
		db := c.GetApp().GetDBManager().GetDefaultDB()
		if err := db.Unscoped().Where("client_id = ?", s.childID).Delete(&models.Client{}).Error; err != nil {
			return 0, err
		}
	}
	return s.TxStore.CommitCreate(c, u, in)
}

// TestSafeCreateExistingChildDeletedAtCommitCompensates: when CommitCreate
// fails because the existing child row vanished (I1 — the guarded UPDATE
// now fails with ErrClientNotFound instead of silently "succeeding"), the
// coordinator must still compensate the agent and land on rolled_back, with
// the restore addressed at the child and no replacement.
func TestSafeCreateExistingChildDeletedAtCommitCompensates(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.deps.TxStore = &deletingChildCommitStore{TxStore: newDAOStore(), childID: coordChildID}
	sw := newSwitchApplyService()
	env.deps.Apply = sw
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status, "commit failure after apply must compensate to rolled_back")
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	require.Len(t, sw.calls, 2, "forward apply + compensation restore")

	forward, restore := sw.calls[0], sw.calls[1]
	require.Equal(t, coordChildID, forward.TargetClientID)
	require.Empty(t, forward.ReplaceClientID, "existing-child forward apply replaces nothing")
	require.Equal(t, coordChildID, restore.TargetClientID, "existing-child restore re-applies the child config")
	require.Empty(t, restore.ReplaceClientID)
	require.Equal(t, coordChildID, configMetadataClientID(t, restore.Config), "the restore config keeps the CHILD identity")

	// the child row stays absent (the concurrent delete is outside the
	// transaction; the commit rolled back)
	_, qerr := dao.NewQuery(env.ctx).GetClientByFilter(env.userInfo, &models.ClientEntity{ClientID: coordChildID}, nil)
	require.Error(t, qerr, "child row must stay deleted")
	requireNoReservation(t, env)
}

// countingResStore counts Release calls (the round-8 Minor: FailTransaction
// releases the reservation atomically, so the coordinator must NOT call
// ResStore.Release again after failTx — the extra call always failed with
// ErrReservationNotFound and swallowed it).
type countingResStore struct {
	ResStore
	releaseCalls int
}

func (s *countingResStore) Release(c *app.Context, u models.UserInfo, txID string) error {
	s.releaseCalls++
	return s.ResStore.Release(c, u, txID)
}

// TestSafeCreatePreApplyFailureDoesNotCallResStoreRelease pins the cleanup:
// a pre-apply failure path must rely on FailTransaction's atomic release —
// zero explicit Release calls, zero swallowed errors.
func TestSafeCreatePreApplyFailureDoesNotCallResStoreRelease(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.pre.serverOK = false
	env.pre.code = defs.ErrorCodePortRemoteInUse
	env.pre.msg = "port is held by another process"
	res := &countingResStore{ResStore: newDAOStore()}
	env.deps.ResStore = res
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, res.releaseCalls, "FailTransaction already released atomically — the redundant Release call is gone")
	requireNoReservation(t, env)
	require.Zero(t, env.apply.calls)
}

// TestSafeCreateApplyRejectionIsNotDisguisedAsTimeout: an agent explicit
// rejection must surface as APPLY_REJECTED on the response AND on the
// transaction row (round-8 Minor), and the flow compensates like any other
// post-apply failure.
func TestSafeCreateApplyRejectionIsNotDisguisedAsTimeout(t *testing.T) {
	env := newCoordTestEnv(t)
	env.seedShadowedPair(false, tcpProxy("ssh", 24, 22))
	env.apply.results = []applyResult{
		{code: defs.ErrorCodeApplyRejected, err: errors.New("agent rejected config: remote port conflict")},
		{}, // compensation restore succeeds
	}
	tc := env.newCoordinator()
	key := uuidKey()

	resp, err := tc.SafeCreateProxy(env.ctx, createReq(key, candidateWebTOML))
	require.NoError(t, err)
	assertInvalidCode(t, resp, defs.ErrorCodeApplyRejected)
	require.False(t, strings.Contains(strings.ToLower(resp.GetStatus().GetMessage()), "timeout"), "a rejection is not a timeout")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyRejected, tx.ErrorCode)
	require.Equal(t, 2, env.apply.calls, "apply + compensation restore")
	requireCompensationEvidence(t, tx, "compensate-done", false)
}
