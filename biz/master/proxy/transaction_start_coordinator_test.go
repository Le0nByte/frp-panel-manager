package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/originlock"
	"github.com/VaalaCat/frp-panel/utils"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---- B3b: the SAFE START coordinator (D2b B3b). The tests prove the
// COORDINATOR side: request normalization, canonical-origin lock, stable
// idempotency, old-rule + physical-key resolution, fast (DB-only, target
// already present) vs full (gates + Apply) branch selection, the two-sided
// gate contract (server bind probe AVAILABLE + client local target
// LISTENING/ACTIVE), fast→full upgrade when the owner proof fails,
// compensation with both success/failure outcomes, SameKeyOwned /
// SameKeyPending reservation handling and the no-secret evidence/audit
// contract. The DAO commit side is D2b B2 (CommitLifecycle/CommitStart).
// The formal StartProxy route is NOT switched — only SafeStartProxy /
// safeStartCoordinator are exported.

// decodeStartEvidence decodes the committed start transaction's evidence_json.
func decodeStartEvidence(t *testing.T, tx *models.ChangeTransaction) map[string]interface{} {
	t.Helper()
	require.NotEmpty(t, tx.EvidenceJSON, "the start commit must write evidence_json")
	var ev map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(tx.EvidenceJSON), &ev))
	return ev
}

// ---- test doubles (DAO stores stay REAL; only the RPC-bound parts are
// faked, mirroring the B3a stop suite) ----

// failingStartCommitStore wraps the REAL dao store and injects a single
// CommitStart failure — everything else stays on the real DAO.
type failingStartCommitStore struct {
	StartTxStore
	fail bool
}

func (s *failingStartCommitStore) CommitStart(c *app.Context, u models.UserInfo, in *dao.CommitLifecycleInput) (uint, error) {
	if s.fail {
		return 0, errors.New("injected commit start db failure")
	}
	return s.StartTxStore.CommitStart(c, u, in)
}

// failingStartStatusStore injects a single UpdateStatus failure for one
// target status (the post-apply verifying transition).
type failingStartStatusStore struct {
	StartTxStore
	failStatus string
	failed     bool
}

func (s *failingStartStatusStore) UpdateStatus(c *app.Context, u models.UserInfo, txID, status, code, msg string) error {
	if !s.failed && status == s.failStatus {
		s.failed = true
		return errors.New("injected status transition failure")
	}
	return s.StartTxStore.UpdateStatus(c, u, txID, status, code, msg)
}

// ---- environment ----

type startCoordEnv struct {
	*coordTestEnv
	sdeps StartCoordinatorDeps
	res   *acquireCountingResStore
}

func newStartCoordEnv(t *testing.T) *startCoordEnv {
	t.Helper()
	base := newCoordTestEnv(t)
	e := &startCoordEnv{
		coordTestEnv: base,
		res:          &acquireCountingResStore{ResStore: base.deps.ResStore},
	}
	e.sdeps = StartCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Res:            e.res,
		Snap:           base.deps.SnapStore,
		KeyProvider:    base.key,
		Preflight:      base.pre,
		Apply:          base.apply,
		Verifier:       base.verify,
		OriginLocker:   NewOriginKeyedLock(),
		Now:            base.deps.Now,
		Lease:          base.deps.Lease,
		ApplyTimeout:   base.deps.ApplyTimeout,
		VerifyTimeout:  base.deps.VerifyTimeout,
		VerifyAttempts: base.deps.VerifyAttempts,
		VerifyInterval: base.deps.VerifyInterval,
		HashRequest:    hashStartRequest,
	}
	return e
}

func (e *startCoordEnv) newStartCoordinator() *StartTransactionCoordinator {
	return &StartTransactionCoordinator{deps: e.sdeps}
}

// proxyRowByID loads a proxy_config row by id (nil when missing).
func (e *startCoordEnv) proxyRowByID(id uint) *models.ProxyConfig {
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

// clientRowByID loads the full client row (including gorm timestamps) by
// client id.
func (e *startCoordEnv) clientRowByID(id string) models.Client {
	t := e.t
	t.Helper()
	var row models.Client
	require.NoError(t, e.db.Where("client_id = ?", id).First(&row).Error)
	return row
}

// snapByType picks the snapshot row for a side/node type.
func (e *startCoordEnv) snapByType(snaps []*models.ConfigSnapshot, side, nodeType string) *models.ConfigSnapshot {
	for _, s := range snaps {
		if s.Side == side && s.NodeType == nodeType {
			return s
		}
	}
	return nil
}

// startReq builds a canonical start request.
func startReq(idemKey, clientID, name string) *pb.StartProxyRequest {
	return &pb.StartProxyRequest{
		ClientId:       lo.ToPtr(clientID),
		ServerId:       lo.ToPtr(testServerID),
		Name:           lo.ToPtr(name),
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

func assertStartInvalidCode(t *testing.T, resp *pb.StartProxyResponse, code string) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, code, resp.GetErrorCode(), "structured error_code must carry the stable code")
	require.False(t, stringsHasPrefix(resp.GetStatus().GetMessage(), code+":"),
		"message %q must stay human-readable without the %q prefix", resp.GetStatus().GetMessage(), code)
}

// seedStartRow creates the proxy_config row for the old rule.
func (e *startCoordEnv) seedStartRow(clientID, originID string, old oldRuleSeed, stopped bool) *models.ProxyConfig {
	t := e.t
	t.Helper()
	oldTyped := typedProxy(old.name, old.protocol, old.remote, old.local)
	content, err := oldTyped.MarshalJSON()
	require.NoError(t, err)
	row := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: clientID, OriginClientID: originID, ServerID: testServerID,
		Name: old.name, Type: old.protocol, Stopped: stopped,
		TenantID: e.userInfo.GetTenantID(), UserID: e.userInfo.GetUserID(),
		Content: content,
	}}
	require.NoError(t, e.db.Create(row).Error)
	return row
}

func (e *startCoordEnv) seedStartCommittedRes(owner uint, protocol string, port uint32) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.PortReservation{
		TenantID: e.userInfo.GetTenantID(), ServerID: testServerID,
		Protocol: protocol, RemotePort: port,
		TransactionID: updOldCreateTx, Status: models.ReservationStatusCommitted,
		OwnerProxyID: &owner,
	}).Error)
}

// seedStartChildClient creates the shadowed pair: origin (shadowed, no
// config) + child (with the given proxies) + the server row.
func (e *startCoordEnv) seedStartChildClient(proxies []v1.TypedProxyConfig) {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
	child := &models.ClientEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = proxies
	require.NoError(t, child.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, child))
	e.seedServer()
}

// seedStartStoppedChildTargetAbsentWorld is the NORMAL stopped start: the
// DB row is stopped AND the target is absent from the child's applied
// config — the FULL branch (server bind probe + client gate + Apply +
// VerifyApplied).
func (e *startCoordEnv) seedStartStoppedChildTargetAbsentWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStartChildClient(nil)
	row := e.seedStartRow(coordChildID, testClientID, old, true)
	if seedRes {
		e.seedStartCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStartStoppedChildWithTargetWorld is the STOPPED-DRIFT world: the DB
// row is stopped but the target rule is STILL present in the child's
// applied config — the FAST branch (client gate + VerifyApplied owner
// proof, DB-only commit).
func (e *startCoordEnv) seedStartStoppedChildWithTargetWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStartChildClient([]v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)})
	row := e.seedStartRow(coordChildID, testClientID, old, true)
	if seedRes {
		e.seedStartCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStartActiveChildWithTargetWorld is the ALREADY-STATE world: the DB
// row is active AND the target is present — the fast no-op commit.
func (e *startCoordEnv) seedStartActiveChildWithTargetWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStartChildClient([]v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)})
	row := e.seedStartRow(coordChildID, testClientID, old, false)
	if seedRes {
		e.seedStartCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStartActiveChildTargetAbsentWorld is the ACTIVE RUNTIME DRIFT world:
// the DB row is active but the target is absent from the child's applied
// config — the full self-heal branch re-adds it.
func (e *startCoordEnv) seedStartActiveChildTargetAbsentWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStartChildClient(nil)
	row := e.seedStartRow(coordChildID, testClientID, old, false)
	if seedRes {
		e.seedStartCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStartOriginTargetAbsentWorld builds the LEGACY origin-shaped world:
// a never-shadowed origin whose own config does NOT carry the target and
// whose proxy row is stopped ON the origin (no origin link) — the full
// branch with a legacy SameKeyPending claim.
func (e *startCoordEnv) seedStartOriginTargetAbsentWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: false, ConnectSecret: "origin-secret"}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	require.NoError(t, origin.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
	e.seedServer()
	row := e.seedStartRow(testClientID, "", old, true)
	if seedRes {
		e.seedStartCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStartActiveOriginWorld builds the LEGACY origin-shaped ACTIVE world:
// the origin's own config carries the target and the row is active on the
// origin — the fast branch over an origin-shaped rule.
func (e *startCoordEnv) seedStartActiveOriginWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: false, ConnectSecret: "origin-secret"}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = []v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)}
	require.NoError(t, origin.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
	e.seedServer()
	row := e.seedStartRow(testClientID, "", old, false)
	if seedRes {
		e.seedStartCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStartIdemTx plants a transaction row for idempotency replay tests.
func (e *startCoordEnv) seedStartIdemTx(txID, key, status, code, msg string, req *pb.StartProxyRequest) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: key,
		RequestHash:  hashStartRequest(req, testClientID),
		Operation:    models.TxOperationStart,
		Status:       status,
		ErrorCode:    code,
		ErrorMessage: msg,
		ActorUserID:  e.userInfo.GetUserID(),
		TenantID:     e.userInfo.GetTenantID(),
		StartedAt:    e.now,
	}).Error)
}

// ---- tests ----

// TestSafeStartStoppedTargetAbsentFullHappy is the NORMAL stopped start:
// the target is absent from the persisted config, so SafeStart runs the
// two-sided gate (server bind probe AVAILABLE + client local target
// LISTENING), applies the candidate (config + target) to the PHYSICAL
// origin agent (TargetClientID = the row's client, Replace empty), verifies
// the port is served, commits. The before snapshots must already exist at
// the moment the forward Apply runs.
func TestSafeStartStoppedTargetAbsentFullHappy(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartStoppedChildTargetAbsentWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	capture := &snapshotCapturingApply{inner: env.apply, db: env.db}
	env.sdeps.Apply = capture
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// full branch: one server bind probe + one client gate + exactly one
	// Apply (physical origin, target = row client, replace empty) + one
	// VerifyApplied; ZERO VerifyReleased on the forward path
	require.Equal(t, 1, env.pre.serverProbeCalls, "the full start must run the server bind probe once")
	require.Equal(t, 1, env.pre.clientGateCalls, "the full start must run the client local gate once")
	require.Equal(t, 1, env.apply.calls, "the full branch must apply exactly once")
	require.Equal(t, 1, env.verify.appliedCalls, "the full branch must VerifyApplied exactly once")
	require.Zero(t, env.verify.releasedCalls, "no VerifyReleased on the forward start path")
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")
	require.Equal(t, 2, capture.beforeRows, "both before snapshots must exist BEFORE the forward Apply")
	require.Contains(t, string(capture.applied), `"name":"web"`, "the applied config must carry the started target")
	applied := env.apply.callsData[0]
	require.Equal(t, testClientID, applied.PhysicalAgentID, "the physical agent is the canonical origin, never the raw child")
	require.Equal(t, coordChildID, applied.TargetClientID, "the apply targets the rule's real client row")
	require.Equal(t, "", applied.ReplaceClientID, "start is never a directed switch")

	// proxy row: stopped flips to false, identity untouched
	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.False(t, row.Stopped)
	require.Equal(t, "web", row.Name)
	require.Equal(t, oldRow.Content, row.Content, "the rule content itself never changes on start")

	// client row: the target rule is back in the applied config, sibling
	// fields (secret) untouched
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "web", cfgs[0].GetBaseConfig().Name)

	// committed reservation: owner + historical transaction_id PRESERVED
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite the historical transaction_id")

	// tx: committed, bound to the old row, full-branch evidence
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	ev := decodeStartEvidence(t, tx)
	require.Equal(t, "full", ev["branch"])
	require.Equal(t, true, ev["applied_to_agent"])
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Equal(t, float64(1), ev["verify_calls"])

	// 4 snapshots: client/proxy before (coordinator) + client/proxy after
	// (commit), node ids bound to the child + old row
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
	require.Equal(t, coordChildID, beforeClient.NodeID)
	require.Equal(t, strconv.FormatUint(uint64(oldRow.ID), 10), beforeProxy.NodeID)
	require.Equal(t, coordChildID, afterClient.NodeID)
	require.Equal(t, strconv.FormatUint(uint64(oldRow.ID), 10), afterProxy.NodeID)

	// proxy-after: the RUNNING envelope of the unchanged rule
	proxyPlain := decryptSnapshot(t, afterProxy, env.key.Key)
	var envp updateProxyEnvelope
	require.NoError(t, json.Unmarshal(proxyPlain, &envp))
	require.Equal(t, 1, envp.Version)
	require.Equal(t, oldRow.ID, envp.ProxyID, "proxy-after envelope must stay bound to the committed proxy row")
	require.Equal(t, "web", envp.Name)
	require.Equal(t, coordChildID, envp.ClientID)
	require.Equal(t, testClientID, envp.OriginClientID)
	require.False(t, envp.Stopped, "the started proxy after envelope carries Stopped=false")
	require.Equal(t, oldRow.Content, envp.Typed, "the started proxy after envelope carries the unchanged rule")

	// audit: exactly one start event, no plaintext
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeStart, audits[0].EventType)
	require.True(t, audits[0].Success)
	require.NotContains(t, audits[0].Summary, "[[proxies]]")
	require.NotContains(t, audits[0].Summary, "child-secret")
	require.NotContains(t, audits[0].Summary, "origin-secret")
}

// TestSafeStartTargetPresentDBStoppedFast is the STOPPED-DRIFT fast branch:
// the DB row is stopped but the target rule IS present in the applied
// config. The coordinator runs the client local-listen gate + VerifyApplied
// owner proof, then commits DB-only — ZERO Apply, ZERO server bind probe.
func TestSafeStartTargetPresentDBStoppedFast(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()
	key := uuidKey()

	childBefore := env.clientRowByID(coordChildID)
	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// zero-Apply contract: one client gate, one VerifyApplied owner proof,
	// NO server bind probe, no VerifyReleased
	require.Zero(t, env.apply.calls, "the fast branch never applies anything")
	require.Zero(t, env.pre.serverProbeCalls, "the fast branch never runs the server bind probe")
	require.Equal(t, 1, env.pre.clientGateCalls, "the fast branch runs the client local gate once")
	require.Equal(t, 1, env.verify.appliedCalls, "the fast branch runs the VerifyApplied owner proof once")
	require.Zero(t, env.verify.releasedCalls)
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")

	// client row: WHOLE row incl. UpdatedAt byte-identical (fast never
	// rewrites an identical config)
	require.Equal(t, childBefore, env.clientRowByID(coordChildID), "the fast branch must not touch the child row")

	// proxy row: stopped flips false, content untouched
	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.False(t, row.Stopped)
	require.Equal(t, oldRow.Content, row.Content)

	// SameKeyOwned reservation preserved
	res := env.reservationByPort(8000)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned transaction_id preserved")
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	ev := decodeStartEvidence(t, tx)
	require.Equal(t, "fast", ev["branch"])
	require.Equal(t, false, ev["applied_to_agent"])
	require.Equal(t, float64(0), ev["apply_calls"])
	require.Equal(t, float64(1), ev["verify_calls"])

	// client-after snapshot byte-identical to client-before
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 4)
	beforePlain := decryptSnapshot(t, env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeClient), env.key.Key)
	afterPlain := decryptSnapshot(t, env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeClient), env.key.Key)
	require.Equal(t, beforePlain, afterPlain)
}

// TestSafeStartTargetPresentActiveAlreadyStateFast is the ALREADY-STATE
// fast branch: the DB row is active AND the target is present — a no-op
// commit that only leaves the audit/snapshot trail.
func TestSafeStartTargetPresentActiveAlreadyStateFast(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartActiveChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()
	key := uuidKey()

	childBefore := env.clientRowByID(coordChildID)
	rowBefore := env.proxyRowByID(oldRow.ID)
	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls, "the already-state fast branch never applies anything")
	require.Zero(t, env.pre.serverProbeCalls)
	require.Equal(t, 1, env.pre.clientGateCalls)
	require.Equal(t, 1, env.verify.appliedCalls)

	// no-op commit: BOTH rows byte-identical (stopped already false, config
	// already present)
	require.Equal(t, childBefore, env.clientRowByID(coordChildID), "the no-op commit must not touch the child row")
	rowAfter := env.proxyRowByID(oldRow.ID)
	require.Equal(t, rowBefore.Content, rowAfter.Content)
	require.Equal(t, rowBefore.Stopped, rowAfter.Stopped)
	require.False(t, rowAfter.Stopped)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeStartEvidence(t, tx)
	require.Equal(t, "fast", ev["branch"])
	require.Equal(t, float64(0), ev["apply_calls"])
	// the already-state commit still leaves a trace
	require.Len(t, env.auditEvents(tx.TransactionID), 1)
	require.Len(t, env.snapshots(tx.TransactionID), 4)
}

// TestSafeStartActiveTargetAbsentFullSelfHeal: the DB row is ACTIVE but the
// target is absent from the applied config (a drifted runtime state) — the
// coordinator must run the FULL branch to re-add the rule to the agent, not
// trust the DB.
func TestSafeStartActiveTargetAbsentFullSelfHeal(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartActiveChildTargetAbsentWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, 1, env.pre.serverProbeCalls, "the self-heal must run the server bind probe")
	require.Equal(t, 1, env.pre.clientGateCalls, "the self-heal must run the client gate")
	require.Equal(t, 1, env.apply.calls, "the self-heal must apply the target-added config to the agent")
	require.Equal(t, 1, env.verify.appliedCalls)
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "web", cfgs[0].GetBaseConfig().Name, "the self-heal must persist the target back into the config")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeStartEvidence(t, tx)
	require.Equal(t, "full", ev["branch"])
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Equal(t, oldRow.ID, *tx.ProxyID)
}

// TestSafeStartTargetPresentVerifyAppliedFailUpgradeFull: the fast branch's
// VerifyApplied owner proof fails (the agent does not serve the rule
// despite the config) — the coordinator must UPGRADE to the full branch:
// re-run BOTH gates (server bind probe + client gate), then applying →
// Apply → verifying → VerifyApplied, with full-branch evidence.
func TestSafeStartTargetPresentVerifyAppliedFailUpgradeFull(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	scripted := &scriptedVerifier{applied: []error{errors.New("the agent does not serve the port despite the config"), nil}}
	env.sdeps.Verifier = scripted
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, 1, env.apply.calls, "the failed fast owner proof must upgrade to a real Apply")
	require.Equal(t, 1, env.pre.serverProbeCalls, "the upgrade runs the server bind probe exactly once")
	require.Equal(t, 2, env.pre.clientGateCalls, "the client gate runs once in fast and once in the upgrade")
	require.Equal(t, 2, scripted.appliedCalls, "one failed fast owner proof + one post-apply verify")
	require.Zero(t, env.verify.releasedCalls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeStartEvidence(t, tx)
	require.Equal(t, "full", ev["branch"], "an upgraded start DID touch the agent")
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Equal(t, float64(2), ev["verify_calls"])
}

// TestSafeStartLegacyNoReservationPendingToCommitted covers the legacy
// origin-shaped world: no committed reservation exists, so the coordinator
// TryAcquires a pending lease and the commit flips it to committed owned by
// the old row.
func TestSafeStartLegacyNoReservationPendingToCommitted(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartOriginTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, int64(1), env.res.acquireCalls(), "the legacy world must acquire one pending lease")
	require.Equal(t, 1, env.apply.calls, "origin-shaped stopped start is the full branch")
	require.Equal(t, testClientID, env.apply.callsData[0].PhysicalAgentID)
	require.Equal(t, testClientID, env.apply.callsData[0].TargetClientID, "origin-shaped rules apply to the origin handler itself")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, tx.TransactionID, res.TransactionID)
	require.Equal(t, 1, env.countReservations())
}

// TestSafeStartServerProbeRejectFull: the server bind probe does not come
// back AVAILABLE — fail closed with 0 Apply, the client gate must NOT run,
// the pending lease is released.
func TestSafeStartServerProbeRejectFull(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.serverProbeResult = &PreflightGateResult{
		Ok: false, ErrorCode: defs.ErrorCodePortRemoteInUse, ErrorMessage: "remote port is taken by another process",
	}
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.pre.clientGateCalls, "a failed server gate must not run the client gate")
	// the tx row failed and the legacy pending lease was released
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortRemoteInUse, tx.ErrorCode)
	require.Equal(t, 1, env.countTransactions())
	require.Zero(t, env.countReservations(), "the pending lease must be released on the gate rejection")
	require.Equal(t, int64(1), env.res.acquireCalls(), "the legacy world claims one pending lease before the gate rejects")
}

// TestSafeStartClientGateRejectFull: the server probe passes but the client
// local target is not listening — PORT_LOCAL_NOT_LISTENING, 0 Apply,
// pending released.
func TestSafeStartClientGateRejectFull(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.clientGateResult = &PreflightGateResult{
		Ok: false, ErrorCode: defs.ErrorCodePortLocalNotListening, ErrorMessage: "no local service is listening",
	}
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodePortLocalNotListening)
	require.Zero(t, env.apply.calls)
	require.Equal(t, 1, env.pre.serverProbeCalls, "the server gate runs before the client gate")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodePortLocalNotListening, tx.ErrorCode)
	require.Zero(t, env.countReservations(), "the pending lease must be released on the gate rejection")
}

// TestSafeStartClientGateRejectFast: the fast branch's client local gate
// fails — the start must be REJECTED with PORT_LOCAL_NOT_LISTENING (spec:
// the two-sided gate is mandatory even when the rule is already in the
// config), never a DB-only commit.
func TestSafeStartClientGateRejectFast(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildWithTargetWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.clientGateResult = &PreflightGateResult{
		Ok: false, ErrorCode: defs.ErrorCodePortLocalNotListening, ErrorMessage: "local service went away",
	}
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodePortLocalNotListening)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.pre.serverProbeCalls, "the fast branch never runs the server bind probe")
	require.Zero(t, env.verify.appliedCalls, "no owner proof after a rejected client gate")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.countReservations(), "the pending lease must be released on the gate rejection")
}

// TestSafeStartApplyRejectCompensates: the full-branch Apply is rejected by
// the agent — the coordinator compensates (restore the before config, verify
// the port released) and lands on rolled_back with APPLY_REJECTED, releasing
// the current pending lease.
func TestSafeStartApplyRejectCompensates(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: errors.New("agent rejected the config")},
		{}, // compensation restore succeeds
	}
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeApplyRejected)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, testClientID, env.apply.callsData[1].PhysicalAgentID, "compensation restores on the physical origin")
	require.Equal(t, coordChildID, env.apply.callsData[1].TargetClientID)
	require.Equal(t, 1, env.verify.releasedCalls, "compensation verifies the added rule is gone")
	require.Zero(t, env.verify.appliedCalls, "no forward VerifyApplied after a rejected apply")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyRejected, tx.ErrorCode)
	require.Equal(t, 0, env.countReservations(), "a successfully compensated start releases the current pending lease")
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeCompensation, audits[0].EventType)
	require.True(t, audits[0].Success)
}

// TestSafeStartApplyTimeoutCompensates: an Apply deadline uses the
// APPLY_TIMEOUT fallback code.
func TestSafeStartApplyTimeoutCompensates(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: context.DeadlineExceeded},
		{}, // compensation restore succeeds
	}
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeApplyTimeout)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyTimeout, tx.ErrorCode)
}

// TestSafeStartPostApplyVerifyFailureCompensates: VerifyApplied fails after
// a successful Apply — full compensation, rolled_back + VERIFY_FAILED.
func TestSafeStartPostApplyVerifyFailureCompensates(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.verify.appliedErr = errors.New("the agent does not serve the port after apply")
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, 1, env.verify.releasedCalls, "compensation verifies the port is released")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeVerifyFailed, tx.ErrorCode)
	require.Zero(t, env.countReservations(), "the legacy pending lease is released after a successful compensation")
}

// TestSafeStartVerifyingTransitionFailureCompensates: the post-apply
// applying->verifying DB transition fails — the config IS applied, so the
// coordinator compensates with CONFIG_COMMIT_FAILED.
func TestSafeStartVerifyingTransitionFailureCompensates(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStatus := &failingStartStatusStore{StartTxStore: env.sdeps.Tx, failStatus: models.TxStatusVerifying}
	env.sdeps.Tx = failStatus
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, 1, env.verify.releasedCalls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
}

// TestSafeStartCommitFailureCompensates: the full branch's CommitStart fails
// after the config was applied and verified — the coordinator compensates
// (never a bare failed) with CONFIG_COMMIT_FAILED.
func TestSafeStartCommitFailureCompensates(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failCommit := &failingStartCommitStore{StartTxStore: env.sdeps.Tx, fail: true}
	env.sdeps.Tx = failCommit
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
}

// TestSafeStartCompensationApplyFailureRollbackFailed: the compensation's
// own restore Apply fails — rollback_failed, ROLLBACK_FAILED, and the
// current pending lease is RETAINED for manual intervention.
func TestSafeStartCompensationApplyFailureRollbackFailed(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: errors.New("forward rejected")},
		{code: "", err: errors.New("restore rejected")},
	}
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	require.Equal(t, 2, env.apply.calls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
	// the pending lease must be RETAINED (rule blocked for manual rollback)
	require.Equal(t, 1, env.countReservations())
	res := env.reservationByPort(8000)
	require.Equal(t, models.ReservationStatusPending, res.Status)
	require.Equal(t, tx.TransactionID, res.TransactionID)
	requireCompensationEvidence(t, tx, "compensate-apply", true)
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.False(t, audits[0].Success)
}

// TestSafeStartCompensationVerifyFailureRollbackFailed: the compensation
// restore applies but the released-state verify fails — rollback_failed with
// the lease retained.
func TestSafeStartCompensationVerifyFailureRollbackFailed(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	scripted := &scriptedVerifier{
		applied:  []error{errors.New("post-apply verify failed")},
		released: []error{errors.New("port still held after restore")},
	}
	env.sdeps.Verifier = scripted
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
	require.Equal(t, 1, env.countReservations(), "the pending lease is retained on rollback_failed")
	requireCompensationEvidence(t, tx, "compensate-verify-old", true)
}

// TestSafeStartFastCommitFailureNoPseudoRollback: the FAST branch (zero
// Apply) CommitStart fails — the transaction lands on failed +
// CONFIG_COMMIT_FAILED. There was never an agent apply, so rolled_back /
// ROLLBACK_FAILED would be a lie.
func TestSafeStartFastCommitFailureNoPseudoRollback(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failCommit := &failingStartCommitStore{StartTxStore: env.sdeps.Tx, fail: true}
	env.sdeps.Tx = failCommit
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Zero(t, env.apply.calls, "the fast branch never applied anything")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status, "fast commit failure is failed, never rolled_back")
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	// SameKeyOwned: nothing to release — the historical committed lease stays
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, updOldCreateTx, res.TransactionID)
}

// TestSafeStartDuplicateTargetRejected: the target name appears more than
// once in the applied config — fail closed with zero Apply and zero
// transaction rows (the rejection happens before any write).
func TestSafeStartDuplicateTargetRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient([]v1.TypedProxyConfig{
		typedProxy("web", "tcp", 8000, 8080),
		typedProxy("web", "tcp", 8000, 9090),
	})
	env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions(), "rejections before any tx row is created")
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeStartPhysicalKeyDriftRejected: the applied config carries the
// target name on a DIFFERENT physical key than the rule row — the
// coordinator must never start a same-name drifted rule.
func TestSafeStartPhysicalKeyDriftRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient([]v1.TypedProxyConfig{typedProxy("web", "tcp", 9000, 8080)})
	env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeStartUnparseableConfigRejected: the persisted client config cannot
// be parsed — fail closed with CONFIG_VALIDATION_FAILED, zero tx, zero Apply.
func TestSafeStartUnparseableConfigRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient(nil)
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", coordChildID).
		Update("config_content", []byte("{not-json")).Error)
	env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStartNilBaseConfigRejected: a malformed typed entry WITHOUT a base
// config makes the physical key unprovable — fail closed.
func TestSafeStartNilBaseConfigRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient(nil)
	// a proxies entry with no base config at all (nil ProxyConfigurer)
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", coordChildID).
		Update("config_content", []byte(`{"proxies":[null]}`)).Error)
	env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStartClientShapeMismatchRejected: the child client the rule lives
// on does not match the request's canonical origin — fail closed.
func TestSafeStartClientShapeMismatchRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	// a child whose origin link points at a DIFFERENT origin
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: true, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.userInfo, origin))
	child := &models.ClientEntity{
		ClientID: coordChildID, OriginClientID: "other-origin", ServerID: testServerID,
		IsShadow: false, ConnectSecret: "child-secret",
	}
	cfg := &v1.ClientConfig{}
	cfg.Complete()
	cfg.Proxies = []v1.TypedProxyConfig{typedProxy("web", "tcp", 8000, 8080)}
	require.NoError(t, child.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.userInfo, child))
	env.seedServer()
	env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStartOriginStoppedRejected: the physical origin agent is stopped —
// it cannot receive the update, fail closed with AGENT_OFFLINE, zero Apply.
func TestSafeStartOriginStoppedRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	origin := env.clientByID(testClientID)
	origin.Stopped = true
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.userInfo, origin))
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeAgentOffline)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStartServerMismatchRejected: the rule does not exist for the
// requested server — CONFIG_VALIDATION_FAILED, zero Apply.
func TestSafeStartServerMismatchRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, &pb.StartProxyRequest{
		ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr("srv-other"),
		Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey()),
	})
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStartUnsupportedTypeRejected: an HTTP/HTTPS/STCP/XTCP rule is
// read-only per spec §1.2.5 — a start request must be rejected with
// UNSUPPORTED_PROXY_TYPE before any write or agent contact.
func TestSafeStartUnsupportedTypeRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient(nil)
	// seed an HTTP rule row: valid typed content with type=http (read-only
	// per spec §1.2.5 — HTTP configs carry no remotePort)
	content, err := json.Marshal(map[string]interface{}{
		"name": "web", "type": "http",
		"localIP": "127.0.0.1", "localPort": 8080,
	})
	require.NoError(t, err)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: "web", Type: "http",
		TenantID: env.userInfo.GetTenantID(), UserID: env.userInfo.GetUserID(),
		Content: content,
	}}).Error)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeUnsupportedProxyType)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeStartValidationRejected covers the request-shape contract: missing
// client/server/name and a malformed idempotency key all fail with the
// structured CONFIG_VALIDATION_FAILED before any lock/write.
func TestSafeStartValidationRejected(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()

	cases := []struct {
		name string
		req  *pb.StartProxyRequest
	}{
		{"missing client", &pb.StartProxyRequest{ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing server", &pb.StartProxyRequest{ClientId: lo.ToPtr(testClientID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing name", &pb.StartProxyRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing idem key", &pb.StartProxyRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web")}},
		{"malformed idem key", &pb.StartProxyRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr("not-a-uuid")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := sc.SafeStartProxy(env.ctx, tc.req)
			require.NoError(t, err)
			assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
		})
	}
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStartChildAddressedLocksCanonicalOriginKeyOnce: a child-addressed
// start locks the (tenant, ORIGIN) key exactly once — no double lock, no
// deadlock, and the key is canonical (never the raw child).
func TestSafeStartChildAddressedLocksCanonicalOriginKeyOnce(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	rec := &proxyRecordingLocker{}
	env.sdeps.OriginLocker = rec
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(uuidKey(), coordChildID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.keys, 1, "the coordinator must lock exactly once")
	require.Equal(t, originlock.Key(1, testClientID), rec.keys[0],
		"child-addressed requests must lock the canonical (tenant, origin) key")
	require.Equal(t, rec.keys, rec.unlocks, "every Lock must be paired with an Unlock")
	require.Equal(t, testClientID, env.apply.callsData[0].PhysicalAgentID,
		"the physical agent is the canonical origin, never the raw child")
}

// TestSafeStartHoldsSharedOriginLockBlockingLegacyLock: while the safe start
// holds the origin lock mid-apply, a concurrent LockLegacyOrigin — the exact
// lock body every legacy writer uses — must NOT acquire; after the release
// it acquires and resolves the canonical origin.
func TestSafeStartHoldsSharedOriginLockBlockingLegacyLock(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sig := newContentionSignalingLocker(NewOriginKeyedLock())
	env.sdeps.OriginLocker = sig
	oldLive := liveOriginLocker
	liveOriginLocker = sig
	t.Cleanup(func() { liveOriginLocker = oldLive })

	barrier := &barrierReleaseApply{inner: env.apply, entered: make(chan struct{}), release: make(chan struct{})}
	env.sdeps.Apply = barrier
	sc := env.newStartCoordinator()
	key := uuidKey()

	startDone := make(chan error, 1)
	go func() {
		_, err := sc.SafeStartProxy(env.ctx, startReq(key, coordChildID, "web"))
		startDone <- err
	}()
	<-barrier.entered // SafeStart provably holds the lock (mid-apply)

	legacyDone := make(chan struct{})
	var (
		legacyOrigin string
		legacyErr    error
	)
	go func() {
		defer close(legacyDone)
		var unlock func()
		legacyOrigin, unlock, legacyErr = LockLegacyOrigin(env.ctx, coordChildID)
		if unlock != nil {
			unlock()
		}
	}()

	select {
	case <-sig.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("LockLegacyOrigin never contended on the shared origin lock")
	}

	close(barrier.release)
	require.NoError(t, <-startDone)
	<-legacyDone
	require.NoError(t, legacyErr)
	require.Equal(t, testClientID, legacyOrigin, "child-addressed LockLegacyOrigin must resolve the canonical origin")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, int64(1), atomic.LoadInt64(&barrier.calls))
}

// TestSafeStartIdempotentCommittedReplaySingleTxSingleApply: replaying the
// same idempotency key after a committed start reuses the SAME transaction
// and performs NO second Apply / Verify.
func TestSafeStartIdempotentCommittedReplaySingleTxSingleApply(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls)

	// replay: committed -> idempotent success, zero new side effects
	resp2, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls, "a committed replay must not re-apply")
	require.Equal(t, 1, env.verify.appliedCalls, "a committed replay must not re-verify")
	require.Equal(t, 1, env.countTransactions(), "the replay must reuse the same transaction")
	require.Equal(t, 1, env.countReservations())
}

// TestSafeStartIdempotencyConflictDifferentHash: the same idempotency key
// replayed with a DIFFERENT start target is a stable IDEMPOTENCY_CONFLICT.
func TestSafeStartIdempotencyConflictDifferentHash(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildTargetAbsentWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStartCoordinator()
	key := uuidKey()

	req := startReq(key, testClientID, "web")
	resp, err := sc.SafeStartProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// same key, different name -> different canonical hash
	otherReq := startReq(key, testClientID, "web2")
	env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web2", protocol: "tcp", remote: 9000, local: 9090}, false)
	resp2, err := sc.SafeStartProxy(env.ctx, otherReq)
	require.NoError(t, err)
	assertStartInvalidCode(t, resp2, defs.ErrorCodeIdempotencyConflict)
	require.Equal(t, 1, env.countTransactions(), "the conflicting replay must not create a second transaction")
}

// TestSafeStartIdempotencyInFlightReplay: replaying a key whose transaction
// is still running returns the stable IDEMPOTENCY_IN_FLIGHT.
func TestSafeStartIdempotencyInFlightReplay(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient(nil)
	req := startReq("00000000-0000-4000-8000-000000000001", testClientID, "web")
	env.seedStartIdemTx("tx-start-inflight", "00000000-0000-4000-8000-000000000001", models.TxStatusApplying, "", "", req)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, req)
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeIdempotencyInFlight)
	require.Zero(t, env.apply.calls)
}

// TestSafeStartTerminalFailureReplay: replaying a key whose transaction
// terminally failed returns the ORIGINAL failure result.
func TestSafeStartTerminalFailureReplay(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient(nil)
	req := startReq("00000000-0000-4000-8000-000000000002", testClientID, "web")
	env.seedStartIdemTx("tx-start-failed", "00000000-0000-4000-8000-000000000002", models.TxStatusFailed,
		defs.ErrorCodeVerifyFailed, "verify failed earlier", req)
	sc := env.newStartCoordinator()

	resp, err := sc.SafeStartProxy(env.ctx, req)
	require.NoError(t, err)
	assertStartInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)
	require.Zero(t, env.apply.calls)
}

// TestSafeStartEvidenceAuditNoSecretSiblingUntouched: the committed
// evidence + audit never carry config/secret plaintext, and a sibling rule
// (row + applied config) is left byte-identical by the start.
func TestSafeStartEvidenceAuditNoSecretSiblingUntouched(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartChildClient([]v1.TypedProxyConfig{
		typedProxy("sib", "tcp", 7000, 7000),
	})
	oldRow := env.seedStartRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, true)
	sibTyped := typedProxy("sib", "tcp", 7000, 7000)
	sibContent, err := sibTyped.MarshalJSON()
	require.NoError(t, err)
	sibRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: "sib", Type: "tcp",
		TenantID: env.userInfo.GetTenantID(), UserID: env.userInfo.GetUserID(),
		Content: sibContent,
	}}
	require.NoError(t, env.db.Create(sibRow).Error)
	env.seedStartCommittedRes(oldRow.ID, "tcp", 8000)
	sc := env.newStartCoordinator()
	key := uuidKey()

	beforeSibRow := *sibRow
	resp, err := sc.SafeStartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// sibling proxy row untouched (content + stopped + id)
	afterSib := env.proxyRowByID(sibRow.ID)
	require.Equal(t, beforeSibRow.Content, afterSib.Content)
	require.False(t, afterSib.Stopped)

	// sibling still in the applied config, and the target was added
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Len(t, cfgs, 2)
	names := []string{cfgs[0].GetBaseConfig().Name, cfgs[1].GetBaseConfig().Name}
	require.Contains(t, names, "sib")
	require.Contains(t, names, "web")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.NotContains(t, tx.EvidenceJSON, "[[proxies]]")
	require.NotContains(t, tx.EvidenceJSON, "remotePort")
	require.NotContains(t, tx.EvidenceJSON, "child-secret")
	require.NotContains(t, tx.EvidenceJSON, "origin-secret")
	require.NotContains(t, tx.EvidenceJSON, "test-token")
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.NotContains(t, audits[0].Summary, "[[proxies]]")
	require.NotContains(t, audits[0].Summary, "child-secret")
	require.NotContains(t, audits[0].Summary, "origin-secret")
	require.NotContains(t, audits[0].Summary, "test-token")
}
