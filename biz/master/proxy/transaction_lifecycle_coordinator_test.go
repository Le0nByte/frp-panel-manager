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

// ---- B3a: the SAFE STOP coordinator (D2b B3a). The tests prove the
// COORDINATOR side: request normalization, canonical-origin lock, stable
// idempotency, old-rule + physical-key resolution, fast (DB-only) vs full
// (Apply) branch selection, before snapshots before any Verify/Apply,
// compensation with both success/failure outcomes, SameKeyOwned /
// SameKeyPending reservation handling and the no-secret evidence/audit
// contract. The DAO commit side is D2b B2 (CommitLifecycle/CommitStop).

// decodeStopEvidence decodes the committed stop transaction's evidence_json.
func decodeStopEvidence(t *testing.T, tx *models.ChangeTransaction) map[string]interface{} {
	t.Helper()
	require.NotEmpty(t, tx.EvidenceJSON, "the stop commit must write evidence_json")
	var ev map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(tx.EvidenceJSON), &ev))
	return ev
}

// ---- test doubles (DAO stores stay REAL; only the RPC-bound parts are
// faked, mirroring the SafeCreate/SafeUpdate suites) ----

// failingStopCommitStore wraps the REAL dao store and injects a single
// CommitStop failure — everything else stays on the real DAO.
type failingStopCommitStore struct {
	StopTxStore
	fail bool
}

func (s *failingStopCommitStore) CommitStop(c *app.Context, u models.UserInfo, in *dao.CommitLifecycleInput) (uint, error) {
	if s.fail {
		return 0, errors.New("injected commit stop db failure")
	}
	return s.StopTxStore.CommitStop(c, u, in)
}

// failingStopStatusStore injects a single UpdateStatus failure for one
// target status (the post-apply verifying transition).
type failingStopStatusStore struct {
	StopTxStore
	failStatus string
	failed     bool
}

func (s *failingStopStatusStore) UpdateStatus(c *app.Context, u models.UserInfo, txID, status, code, msg string) error {
	if !s.failed && status == s.failStatus {
		s.failed = true
		return errors.New("injected status transition failure")
	}
	return s.StopTxStore.UpdateStatus(c, u, txID, status, code, msg)
}

// ---- environment ----

type stopCoordEnv struct {
	*coordTestEnv
	sdeps StopCoordinatorDeps
	res   *acquireCountingResStore
}

func newStopCoordEnv(t *testing.T) *stopCoordEnv {
	t.Helper()
	base := newCoordTestEnv(t)
	e := &stopCoordEnv{
		coordTestEnv: base,
		res:          &acquireCountingResStore{ResStore: base.deps.ResStore},
	}
	e.sdeps = StopCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Res:            e.res,
		Snap:           base.deps.SnapStore,
		KeyProvider:    base.key,
		Apply:          base.apply,
		Verifier:       base.verify,
		OriginLocker:   NewOriginKeyedLock(),
		Now:            base.deps.Now,
		Lease:          base.deps.Lease,
		ApplyTimeout:   base.deps.ApplyTimeout,
		VerifyTimeout:  base.deps.VerifyTimeout,
		VerifyAttempts: base.deps.VerifyAttempts,
		VerifyInterval: base.deps.VerifyInterval,
		HashRequest:    hashStopRequest,
	}
	return e
}

func (e *stopCoordEnv) newStopCoordinator() *StopTransactionCoordinator {
	return &StopTransactionCoordinator{deps: e.sdeps}
}

// proxyRowByID loads a proxy_config row by id (nil when missing).
func (e *stopCoordEnv) proxyRowByID(id uint) *models.ProxyConfig {
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
func (e *stopCoordEnv) clientRowByID(id string) models.Client {
	t := e.t
	t.Helper()
	var row models.Client
	require.NoError(t, e.db.Where("client_id = ?", id).First(&row).Error)
	return row
}

// snapByType picks the snapshot row for a side/node type.
func (e *stopCoordEnv) snapByType(snaps []*models.ConfigSnapshot, side, nodeType string) *models.ConfigSnapshot {
	for _, s := range snaps {
		if s.Side == side && s.NodeType == nodeType {
			return s
		}
	}
	return nil
}

// stopReq builds a canonical stop request.
func stopReq(idemKey, clientID, name string) *pb.StopProxyRequest {
	return &pb.StopProxyRequest{
		ClientId:       lo.ToPtr(clientID),
		ServerId:       lo.ToPtr(testServerID),
		Name:           lo.ToPtr(name),
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

func assertStopInvalidCode(t *testing.T, resp *pb.StopProxyResponse, code string) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, code, resp.GetErrorCode(), "structured error_code must carry the stable code")
	require.False(t, stringsHasPrefix(resp.GetStatus().GetMessage(), code+":"),
		"message %q must stay human-readable without the %q prefix", resp.GetStatus().GetMessage(), code)
}

// seedStopRow creates the proxy_config row for the old rule.
func (e *stopCoordEnv) seedStopRow(clientID, originID string, old oldRuleSeed, stopped bool) *models.ProxyConfig {
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

func (e *stopCoordEnv) seedStopCommittedRes(owner uint, protocol string, port uint32) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.PortReservation{
		TenantID: e.userInfo.GetTenantID(), ServerID: testServerID,
		Protocol: protocol, RemotePort: port,
		TransactionID: updOldCreateTx, Status: models.ReservationStatusCommitted,
		OwnerProxyID: &owner,
	}).Error)
}

func (e *stopCoordEnv) seedStopChildClient(proxies []v1.TypedProxyConfig) {
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

// seedStopActiveChildWorld builds the child-shaped ACTIVE world: the target
// rule lives in the child's applied config and the proxy row is active on
// the child. seedRes adds a committed reservation owned by the old row.
func (e *stopCoordEnv) seedStopActiveChildWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStopChildClient([]v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)})
	row := e.seedStopRow(coordChildID, testClientID, old, false)
	if seedRes {
		e.seedStopCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStopStoppedChildWithTargetWorld is the SELF-HEAL world: the DB row is
// stopped but the target rule is STILL present in the child's applied config
// (the agent still runs it) — the full branch must remove it from the agent.
func (e *stopCoordEnv) seedStopStoppedChildWithTargetWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStopChildClient([]v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)})
	row := e.seedStopRow(coordChildID, testClientID, old, true)
	if seedRes {
		e.seedStopCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStopStoppedChildWorld is the already-stopped world: the DB row is
// stopped AND the target is absent from the child's applied config — the
// fast DB-only branch.
func (e *stopCoordEnv) seedStopStoppedChildWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedStopChildClient(nil)
	row := e.seedStopRow(coordChildID, testClientID, old, true)
	if seedRes {
		e.seedStopCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStopActiveOriginWorld builds the LEGACY origin-shaped ACTIVE world:
// a never-shadowed origin whose own config carries the target and whose
// proxy row is active ON the origin (no origin link).
func (e *stopCoordEnv) seedStopActiveOriginWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
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
	row := e.seedStopRow(testClientID, "", old, false)
	if seedRes {
		e.seedStopCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedStopIdemTx plants a transaction row for idempotency replay tests.
func (e *stopCoordEnv) seedStopIdemTx(txID, key, status, code, msg string, req *pb.StopProxyRequest) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: key,
		RequestHash:  hashStopRequest(req, testClientID),
		Operation:    models.TxOperationStop,
		Status:       status,
		ErrorCode:    code,
		ErrorMessage: msg,
		ActorUserID:  e.userInfo.GetUserID(),
		TenantID:     e.userInfo.GetTenantID(),
		StartedAt:    e.now,
	}).Error)
}

// ---- tests ----

// TestSafeStopActiveFullHappy is the active normal full branch: the target
// rule is in the child config and the row is active, so SafeStop applies
// the target-removed config to the PHYSICAL origin agent (TargetClientID =
// the row's client, Replace empty), verifies the port released, commits.
// Stop never runs the client local-listen gate.
func TestSafeStopActiveFullHappy(t *testing.T) {
	env := newStopCoordEnv(t)
	oldRow := env.seedStopActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// full branch: exactly one Apply (physical origin, target = row client,
	// replace empty) and one VerifyReleased; ZERO client local-listen gates
	require.Equal(t, 1, env.apply.calls, "the full branch must apply exactly once")
	require.Equal(t, 1, env.verify.releasedCalls, "the full branch must VerifyReleased exactly once")
	require.Zero(t, env.verify.appliedCalls, "no VerifyApplied on the forward stop path")
	require.Zero(t, env.pre.clientGateCalls, "stop must never run the client local-listen gate")
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")
	applied := env.apply.callsData[0]
	require.Equal(t, testClientID, applied.PhysicalAgentID, "the physical agent is the canonical origin, never the raw child")
	require.Equal(t, coordChildID, applied.TargetClientID, "the apply targets the rule's real client row")
	require.Equal(t, "", applied.ReplaceClientID, "stop is never a directed switch")
	require.NotContains(t, string(applied.Config), `"name":"web"`, "the applied config must have the target removed")
	require.NotContains(t, string(applied.Config), `name = "web"`, "the applied config must have the target removed (toml form)")

	// proxy row: stopped flips to true, identity untouched
	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.True(t, row.Stopped)
	require.Equal(t, "web", row.Name)
	require.Equal(t, oldRow.Content, row.Content, "the rule content itself never changes on stop")

	// client row: the target rule is gone from the applied config, sibling
	// fields (secret) untouched
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Empty(t, cfgs, "the stopped target must be removed from the child config")

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
	ev := decodeStopEvidence(t, tx)
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

	// proxy-after: the STOPPED envelope of the unchanged rule
	proxyPlain := decryptSnapshot(t, afterProxy, env.key.Key)
	var envp updateProxyEnvelope
	require.NoError(t, json.Unmarshal(proxyPlain, &envp))
	require.Equal(t, 1, envp.Version)
	require.Equal(t, oldRow.ID, envp.ProxyID, "proxy-after envelope must stay bound to the committed proxy row")
	require.Equal(t, "web", envp.Name)
	require.Equal(t, coordChildID, envp.ClientID)
	require.Equal(t, testClientID, envp.OriginClientID)
	require.True(t, envp.Stopped)
	require.Equal(t, oldRow.Content, envp.Typed, "the stopped proxy after envelope carries the unchanged rule")

	// audit: exactly one stop event, no plaintext
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeStop, audits[0].EventType)
	require.True(t, audits[0].Success)
	require.NotContains(t, audits[0].Summary, "[[proxies]]")
	require.NotContains(t, audits[0].Summary, "child-secret")
	require.NotContains(t, audits[0].Summary, "origin-secret")
}

// TestSafeStopAlreadyStoppedTargetAbsentFast is the fast DB-only branch:
// the row is already stopped AND the target is absent from the applied
// config — the coordinator only verifies the port is released and commits.
// ZERO Apply.
func TestSafeStopAlreadyStoppedTargetAbsentFast(t *testing.T) {
	env := newStopCoordEnv(t)
	oldRow := env.seedStopStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()
	key := uuidKey()

	childBefore := env.clientRowByID(coordChildID)
	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// zero-Apply contract, one VerifyReleased, no client gate
	require.Zero(t, env.apply.calls, "the fast branch never applies anything")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.pre.clientGateCalls, "stop must never run the client local-listen gate")
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")

	// client row: WHOLE row incl. UpdatedAt byte-identical (fast never
	// rewrites an identical config)
	require.Equal(t, childBefore, env.clientRowByID(coordChildID), "the fast branch must not touch the child row")

	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.True(t, row.Stopped)

	res := env.reservationByPort(8000)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned transaction_id preserved")
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	ev := decodeStopEvidence(t, tx)
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

// TestSafeStopStoppedDBTargetStillInChildConfigFullSelfHeal: the DB row is
// stopped but the target is STILL in the child config (a drifted runtime
// state) — the coordinator must run the FULL branch to remove the rule from
// the agent, not trust the DB-only fast path.
func TestSafeStopStoppedDBTargetStillInChildConfigFullSelfHeal(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, 1, env.apply.calls, "the self-heal must apply the target-removed config to the agent")
	require.Equal(t, 1, env.verify.releasedCalls)
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Empty(t, cfgs, "the self-heal must persist the target removal")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeStopEvidence(t, tx)
	require.Equal(t, "full", ev["branch"])
	require.Equal(t, float64(1), ev["apply_calls"])
}

// TestSafeStopActiveDBTargetAbsentFast: the DB row is ACTIVE but the target
// is already absent from the applied config — a DB-only stop heals the row
// without any agent contact (fast branch).
func TestSafeStopActiveDBTargetAbsentFast(t *testing.T) {
	env := newStopCoordEnv(t)
	oldRow := env.seedStopActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// remove the target from the child config after seeding (simulate drift)
	cli := env.clientByID(coordChildID)
	cfg, err := cli.GetConfigContent()
	require.NoError(t, err)
	cfg.Proxies = nil
	require.NoError(t, cli.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.userInfo, cli))
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls, "target-absent active stop is DB-only")
	require.Equal(t, 1, env.verify.releasedCalls)
	row := env.proxyRowByID(oldRow.ID)
	require.True(t, row.Stopped)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeStopEvidence(t, tx)
	require.Equal(t, "fast", ev["branch"])
}

// TestSafeStopLegacyNoReservationPendingToCommitted covers the legacy world
// on the ORIGIN-shaped rule: no committed reservation exists, so the
// coordinator TryAcquires a pending lease and the commit flips it to
// committed owned by the old row.
func TestSafeStopLegacyNoReservationPendingToCommitted(t *testing.T) {
	env := newStopCoordEnv(t)
	oldRow := env.seedStopActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, int64(1), env.res.acquireCalls(), "the legacy world must acquire one pending lease")
	require.Equal(t, 1, env.apply.calls, "origin-shaped active stop is the full branch")
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

// TestSafeStopFastVerifyReleasedFailureUpgradesToFull: the fast branch's
// VerifyReleased fails (the agent still runs the rule despite the DB) — the
// coordinator must UPGRADE to the full branch and apply, NEVER report the
// verifier failure as success.
func TestSafeStopFastVerifyReleasedFailureUpgradesToFull(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	scripted := &scriptedVerifier{released: []error{errors.New("port still occupied by the stale agent rule"), nil}}
	env.sdeps.Verifier = scripted
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, 1, env.apply.calls, "the failed fast verify must upgrade to a real Apply")
	require.Equal(t, 2, scripted.releasedCalls, "one failed fast verify + one post-apply verify")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeStopEvidence(t, tx)
	require.Equal(t, "full", ev["branch"], "an upgraded stop DID touch the agent")
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Equal(t, float64(2), ev["verify_calls"])
}

// TestSafeStopApplyRejectCompensates: the full-branch Apply is rejected by
// the agent — the coordinator compensates (restore before config, verify
// old state) and lands on rolled_back with APPLY_REJECTED (the empty-code
// fallback), releasing the current pending lease.
func TestSafeStopApplyRejectCompensates(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: errors.New("agent rejected the config")},
		{}, // compensation restore succeeds
	}
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeApplyRejected)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, testClientID, env.apply.callsData[1].PhysicalAgentID, "compensation restores on the physical origin")
	require.Equal(t, testClientID, env.apply.callsData[1].TargetClientID)
	require.Equal(t, 1, env.verify.appliedCalls, "compensation verifies the old rule is back")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyRejected, tx.ErrorCode)
	require.Equal(t, 0, env.countReservations(), "a successfully compensated stop releases the current pending lease")
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeCompensation, audits[0].EventType)
	require.True(t, audits[0].Success)
}

// TestSafeStopApplyTimeoutCompensates: an Apply deadline uses the
// APPLY_TIMEOUT fallback code.
func TestSafeStopApplyTimeoutCompensates(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: context.DeadlineExceeded},
		{}, // compensation restore succeeds
	}
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeApplyTimeout)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyTimeout, tx.ErrorCode)
}

// TestSafeStopPostApplyVerifyFailureCompensates: VerifyReleased fails after
// a successful Apply — full compensation, rolled_back + VERIFY_FAILED.
func TestSafeStopPostApplyVerifyFailureCompensates(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.verify.releasedErr = errors.New("port still held after apply")
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, 1, env.verify.appliedCalls, "compensation verifies the old state")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeVerifyFailed, tx.ErrorCode)
	require.Zero(t, env.countReservations(), "the legacy pending lease is released after a successful compensation")
}

// TestSafeStopVerifyingTransitionFailureCompensates: the post-apply
// applying->verifying DB transition fails — the config IS applied, so the
// coordinator compensates with CONFIG_COMMIT_FAILED.
func TestSafeStopVerifyingTransitionFailureCompensates(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStatus := &failingStopStatusStore{StopTxStore: env.sdeps.Tx, failStatus: models.TxStatusVerifying}
	env.sdeps.Tx = failStatus
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, 1, env.verify.appliedCalls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
}

// TestSafeStopCommitFailureCompensates: the full branch's CommitStop fails
// after the config was applied and verified — the coordinator compensates
// (never a bare failed) with CONFIG_COMMIT_FAILED.
func TestSafeStopCommitFailureCompensates(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failCommit := &failingStopCommitStore{StopTxStore: env.sdeps.Tx, fail: true}
	env.sdeps.Tx = failCommit
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
}

// TestSafeStopCompensationApplyFailureRollbackFailed: the compensation's own
// restore Apply fails — rollback_failed, ROLLBACK_FAILED, and the current
// pending lease is RETAINED for manual intervention.
func TestSafeStopCompensationApplyFailureRollbackFailed(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: errors.New("forward rejected")},
		{code: "", err: errors.New("restore rejected")},
	}
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

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

// TestSafeStopCompensationVerifyFailureRollbackFailed: the compensation
// restore applies but the old-state verify fails — rollback_failed with the
// lease retained.
func TestSafeStopCompensationVerifyFailureRollbackFailed(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	scripted := &scriptedVerifier{released: []error{errors.New("post-apply verify failed")}, applied: []error{errors.New("old rule did not come back")}}
	env.sdeps.Verifier = scripted
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
	require.Equal(t, 1, env.countReservations(), "the pending lease is retained on rollback_failed")
	requireCompensationEvidence(t, tx, "compensate-verify-old", true)
}

// TestSafeStopFastCommitFailureNoPseudoRollback: the FAST branch (zero
// Apply) CommitStop fails — the transaction lands on failed +
// CONFIG_COMMIT_FAILED. There was never an agent apply, so rolled_back /
// ROLLBACK_FAILED would be a lie.
func TestSafeStopFastCommitFailureNoPseudoRollback(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failCommit := &failingStopCommitStore{StopTxStore: env.sdeps.Tx, fail: true}
	env.sdeps.Tx = failCommit
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

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

// TestSafeStopDuplicateTargetRejected: the target name appears more than
// once in the applied config — fail closed with zero Apply and zero
// transaction rows (the rejection happens before any write).
func TestSafeStopDuplicateTargetRejected(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopChildClient([]v1.TypedProxyConfig{
		typedProxy("web", "tcp", 8000, 8080),
		typedProxy("web", "tcp", 8000, 9090),
	})
	env.seedStopRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions(), "rejections before any tx row is created")
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeStopPhysicalKeyDriftRejected: the applied config carries the
// target name on a DIFFERENT physical key than the rule row — the
// coordinator must never delete a same-name drifted rule.
func TestSafeStopPhysicalKeyDriftRejected(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopChildClient([]v1.TypedProxyConfig{typedProxy("web", "tcp", 9000, 8080)})
	env.seedStopRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeStopClientShapeMismatchRejected: the child client the rule lives
// on does not match the request's canonical origin — fail closed.
func TestSafeStopClientShapeMismatchRejected(t *testing.T) {
	env := newStopCoordEnv(t)
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
	env.seedStopRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStopOriginStoppedRejected: the physical origin agent is stopped —
// it cannot receive the update, fail closed with AGENT_OFFLINE, zero Apply.
func TestSafeStopOriginStoppedRejected(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	origin := env.clientByID(testClientID)
	origin.Stopped = true
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.userInfo, origin))
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeAgentOffline)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStopServerMismatchRejected: the rule does not exist for the
// requested server — CONFIG_VALIDATION_FAILED, zero Apply.
func TestSafeStopServerMismatchRejected(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, &pb.StopProxyRequest{
		ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr("srv-other"),
		Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey()),
	})
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStopUnsupportedTypeRejected: an HTTP/HTTPS/STCP/XTCP rule is
// read-only per spec §1.2.5 — a stop request must be rejected with
// UNSUPPORTED_PROXY_TYPE before any write or agent contact.
func TestSafeStopUnsupportedTypeRejected(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopChildClient(nil)
	// seed an HTTP rule row: valid typed content with type=http (read-only
	// per spec §1.2.5 — the typed parser resolves it as an HTTPProxyConfig;
	// HTTP configs carry no remotePort)
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
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeUnsupportedProxyType)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeStopValidationRejected covers the request-shape contract: missing
// client/server/name and a malformed idempotency key all fail with the
// structured CONFIG_VALIDATION_FAILED before any lock/write.
func TestSafeStopValidationRejected(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()

	cases := []struct {
		name string
		req  *pb.StopProxyRequest
	}{
		{"missing client", &pb.StopProxyRequest{ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing server", &pb.StopProxyRequest{ClientId: lo.ToPtr(testClientID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing name", &pb.StopProxyRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing idem key", &pb.StopProxyRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web")}},
		{"malformed idem key", &pb.StopProxyRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr("not-a-uuid")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := sc.SafeStopProxy(env.ctx, tc.req)
			require.NoError(t, err)
			assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
		})
	}
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeStopChildAddressedLocksCanonicalOriginKeyOnce: a child-addressed
// stop locks the (tenant, ORIGIN) key exactly once — no double lock, no
// deadlock, and the key is canonical (never the raw child).
func TestSafeStopChildAddressedLocksCanonicalOriginKeyOnce(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	rec := &proxyRecordingLocker{}
	env.sdeps.OriginLocker = rec
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), coordChildID, "web"))
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

// TestSafeStopHoldsSharedOriginLockBlockingLegacyLock: while the safe stop
// holds the origin lock mid-apply, a concurrent LockLegacyOrigin — the
// exact lock body every legacy writer uses — must NOT acquire; after the
// release it acquires and resolves the canonical origin.
func TestSafeStopHoldsSharedOriginLockBlockingLegacyLock(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sig := newContentionSignalingLocker(NewOriginKeyedLock())
	env.sdeps.OriginLocker = sig
	oldLive := liveOriginLocker
	liveOriginLocker = sig
	t.Cleanup(func() { liveOriginLocker = oldLive })

	barrier := &barrierReleaseApply{inner: env.apply, entered: make(chan struct{}), release: make(chan struct{})}
	env.sdeps.Apply = barrier
	sc := env.newStopCoordinator()
	key := uuidKey()

	stopDone := make(chan error, 1)
	go func() {
		_, err := sc.SafeStopProxy(env.ctx, stopReq(key, coordChildID, "web"))
		stopDone <- err
	}()
	<-barrier.entered // SafeStop provably holds the lock (mid-apply)

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
	require.NoError(t, <-stopDone)
	<-legacyDone
	require.NoError(t, legacyErr)
	require.Equal(t, testClientID, legacyOrigin, "child-addressed LockLegacyOrigin must resolve the canonical origin")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, int64(1), atomic.LoadInt64(&barrier.calls))
}

// TestSafeStopIdempotentCommittedReplaySingleTxSingleApply: replaying the
// same idempotency key after a committed stop reuses the SAME transaction
// and performs NO second Apply / Verify.
func TestSafeStopIdempotentCommittedReplaySingleTxSingleApply(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()
	key := uuidKey()

	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls)

	// replay: committed -> idempotent success, zero new side effects
	resp2, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls, "a committed replay must not re-apply")
	require.Equal(t, 1, env.verify.releasedCalls, "a committed replay must not re-verify")
	require.Equal(t, 1, env.countTransactions(), "the replay must reuse the same transaction")
	require.Equal(t, 1, env.countReservations())
}

// TestSafeStopIdempotencyConflictDifferentHash: the same idempotency key
// replayed with a DIFFERENT stop target is a stable IDEMPOTENCY_CONFLICT.
func TestSafeStopIdempotencyConflictDifferentHash(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newStopCoordinator()
	key := uuidKey()

	req := stopReq(key, testClientID, "web")
	resp, err := sc.SafeStopProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// same key, different name -> different canonical hash
	otherReq := stopReq(key, testClientID, "web2")
	env.seedStopRow(coordChildID, testClientID, oldRuleSeed{name: "web2", protocol: "tcp", remote: 9000, local: 9090}, false)
	resp2, err := sc.SafeStopProxy(env.ctx, otherReq)
	require.NoError(t, err)
	assertStopInvalidCode(t, resp2, defs.ErrorCodeIdempotencyConflict)
	require.Equal(t, 1, env.countTransactions(), "the conflicting replay must not create a second transaction")
}

// TestSafeStopIdempotencyInFlightReplay: replaying a key whose transaction
// is still running returns the stable IDEMPOTENCY_IN_FLIGHT.
func TestSafeStopIdempotencyInFlightReplay(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopChildClient(nil)
	req := stopReq("00000000-0000-4000-8000-000000000001", testClientID, "web")
	env.seedStopIdemTx("tx-stop-inflight", "00000000-0000-4000-8000-000000000001", models.TxStatusApplying, "", "", req)
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, req)
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeIdempotencyInFlight)
	require.Zero(t, env.apply.calls)
}

// TestSafeStopTerminalFailureReplay: replaying a key whose transaction
// terminally failed returns the ORIGINAL failure result.
func TestSafeStopTerminalFailureReplay(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopChildClient(nil)
	req := stopReq("00000000-0000-4000-8000-000000000002", testClientID, "web")
	env.seedStopIdemTx("tx-stop-failed", "00000000-0000-4000-8000-000000000002", models.TxStatusFailed,
		defs.ErrorCodeVerifyFailed, "verify failed earlier", req)
	sc := env.newStopCoordinator()

	resp, err := sc.SafeStopProxy(env.ctx, req)
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)
	require.Zero(t, env.apply.calls)
}

// TestSafeStopEvidenceAuditNoSecretSiblingUntouched: the committed
// evidence + audit never carry config/secret plaintext, and a sibling rule
// (row + applied config) is left byte-identical by the stop.
func TestSafeStopEvidenceAuditNoSecretSiblingUntouched(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopChildClient([]v1.TypedProxyConfig{
		typedProxy("web", "tcp", 8000, 8080),
		typedProxy("sib", "tcp", 7000, 7000),
	})
	oldRow := env.seedStopRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
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
	env.seedStopCommittedRes(oldRow.ID, "tcp", 8000)
	sc := env.newStopCoordinator()
	key := uuidKey()

	beforeSibRow := *sibRow
	resp, err := sc.SafeStopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// sibling proxy row untouched (content + stopped + id)
	afterSib := env.proxyRowByID(sibRow.ID)
	require.Equal(t, beforeSibRow.Content, afterSib.Content)
	require.False(t, afterSib.Stopped)

	// sibling still in the applied config
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "sib", cfgs[0].GetBaseConfig().Name)

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
