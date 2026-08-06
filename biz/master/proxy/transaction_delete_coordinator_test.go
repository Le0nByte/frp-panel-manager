package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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

// ---- B3c: the SAFE DELETE coordinator (D2b B3c). The tests prove the
// COORDINATOR side: request normalization, canonical-origin lock, stable
// idempotency (committed replay must succeed even AFTER the row is gone),
// old-rule + physical-key resolution, fast (DB-only) vs full (Apply) branch
// selection, before snapshots before any Verify/Apply, delete-shaped
// compensation (restore before config + VerifyApplied — the old rule must
// come back), SameKeyOwned (historical committed lease NEVER released by
// txID) / SameKeyPending reservation handling and the no-secret
// evidence/audit contract. The DAO commit side is D2b B2
// (CommitLifecycle/CommitDelete: row hard-delete + reservation release by
// owner). Delete NEVER runs a client local-listen gate (spec §8.3 删除:
// 快照 → 下发无目标规则的候选配置 → 验证公网端口释放 → 数据库删除).

// decodeDeleteEvidence decodes the committed delete transaction's
// evidence_json.
func decodeDeleteEvidence(t *testing.T, tx *models.ChangeTransaction) map[string]interface{} {
	t.Helper()
	require.NotEmpty(t, tx.EvidenceJSON, "the delete commit must write evidence_json")
	var ev map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(tx.EvidenceJSON), &ev))
	return ev
}

// ---- test doubles (DAO stores stay REAL; only the RPC-bound parts are
// faked, mirroring the SafeCreate/SafeUpdate/SafeStop suites) ----

// failingDeleteCommitStore wraps the REAL dao store and injects a single
// CommitDelete failure — everything else stays on the real DAO.
type failingDeleteCommitStore struct {
	DeleteTxStore
	fail bool
}

func (s *failingDeleteCommitStore) CommitDelete(c *app.Context, u models.UserInfo, in *dao.CommitLifecycleInput) (uint, error) {
	if s.fail {
		return 0, errors.New("injected commit delete db failure")
	}
	return s.DeleteTxStore.CommitDelete(c, u, in)
}

// failingDeleteStatusStore injects a single UpdateStatus failure for one
// target status (the post-apply verifying transition).
type failingDeleteStatusStore struct {
	DeleteTxStore
	failStatus string
	failed     bool
}

func (s *failingDeleteStatusStore) UpdateStatus(c *app.Context, u models.UserInfo, txID, status, code, msg string) error {
	if !s.failed && status == s.failStatus {
		s.failed = true
		return errors.New("injected status transition failure")
	}
	return s.DeleteTxStore.UpdateStatus(c, u, txID, status, code, msg)
}

// racingDeleteCreateStore simulates the idempotency-key insert race: the
// first Find returns not-found, then Create reports a duplicate-key
// violation (another request won the race), and the follow-up Find returns
// the winner's committed transaction — the coordinator must REPLAY it, not
// apply anything.
type racingDeleteCreateStore struct {
	DeleteTxStore
	findCalls int
	winner    *models.ChangeTransaction
}

func (s *racingDeleteCreateStore) FindByIdempotencyKey(c *app.Context, u models.UserInfo, key string) (*models.ChangeTransaction, error) {
	s.findCalls++
	if s.findCalls == 1 {
		return nil, defs.ErrTransactionNotFound
	}
	return s.winner, nil
}

func (s *racingDeleteCreateStore) Create(c *app.Context, u models.UserInfo, tx *models.ChangeTransaction) error {
	return gorm.ErrDuplicatedKey
}

// ---- environment ----

type deleteCoordEnv struct {
	*coordTestEnv
	sdeps DeleteCoordinatorDeps
	res   *acquireCountingResStore
}

func newDeleteCoordEnv(t *testing.T) *deleteCoordEnv {
	t.Helper()
	base := newCoordTestEnv(t)
	e := &deleteCoordEnv{
		coordTestEnv: base,
		res:          &acquireCountingResStore{ResStore: base.deps.ResStore},
	}
	e.sdeps = DeleteCoordinatorDeps{
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
		HashRequest:    hashDeleteRequest,
	}
	return e
}

func (e *deleteCoordEnv) newDeleteCoordinator() *DeleteTransactionCoordinator {
	return &DeleteTransactionCoordinator{deps: e.sdeps}
}

// proxyRowByID loads a proxy_config row by id (nil when missing).
func (e *deleteCoordEnv) proxyRowByID(id uint) *models.ProxyConfig {
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
func (e *deleteCoordEnv) clientRowByID(id string) models.Client {
	t := e.t
	t.Helper()
	var row models.Client
	require.NoError(t, e.db.Where("client_id = ?", id).First(&row).Error)
	return row
}

// snapByType picks the snapshot row for a side/node type.
func (e *deleteCoordEnv) snapByType(snaps []*models.ConfigSnapshot, side, nodeType string) *models.ConfigSnapshot {
	for _, s := range snaps {
		if s.Side == side && s.NodeType == nodeType {
			return s
		}
	}
	return nil
}

// deleteReq builds a canonical delete request.
func deleteReq(idemKey, clientID, name string) *pb.DeleteProxyConfigRequest {
	return &pb.DeleteProxyConfigRequest{
		ClientId:       lo.ToPtr(clientID),
		ServerId:       lo.ToPtr(testServerID),
		Name:           lo.ToPtr(name),
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

func assertDeleteInvalidCode(t *testing.T, resp *pb.DeleteProxyConfigResponse, code string) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, code, resp.GetErrorCode(), "structured error_code must carry the stable code")
	require.False(t, stringsHasPrefix(resp.GetStatus().GetMessage(), code+":"),
		"message %q must stay human-readable without the %q prefix", resp.GetStatus().GetMessage(), code)
}

// seedDeleteRow creates the proxy_config row for the old rule.
func (e *deleteCoordEnv) seedDeleteRow(clientID, originID string, old oldRuleSeed, stopped bool) *models.ProxyConfig {
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

func (e *deleteCoordEnv) seedDeleteCommittedRes(owner uint, protocol string, port uint32) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.PortReservation{
		TenantID: e.userInfo.GetTenantID(), ServerID: testServerID,
		Protocol: protocol, RemotePort: port,
		TransactionID: updOldCreateTx, Status: models.ReservationStatusCommitted,
		OwnerProxyID: &owner,
	}).Error)
}

func (e *deleteCoordEnv) seedDeleteChildClient(proxies []v1.TypedProxyConfig) {
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

// seedDeleteActiveChildWorld builds the child-shaped ACTIVE world: the
// target rule lives in the child's applied config and the proxy row is
// active on the child. seedRes adds a committed reservation owned by the
// old row.
func (e *deleteCoordEnv) seedDeleteActiveChildWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedDeleteChildClient([]v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)})
	row := e.seedDeleteRow(coordChildID, testClientID, old, false)
	if seedRes {
		e.seedDeleteCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedDeleteStoppedChildWithTargetWorld is the SELF-HEAL world: the DB row
// is stopped but the target rule is STILL present in the child's applied
// config (the agent still runs it) — the full branch must remove it from
// the agent.
func (e *deleteCoordEnv) seedDeleteStoppedChildWithTargetWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedDeleteChildClient([]v1.TypedProxyConfig{typedProxy(old.name, old.protocol, old.remote, old.local)})
	row := e.seedDeleteRow(coordChildID, testClientID, old, true)
	if seedRes {
		e.seedDeleteCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedDeleteStoppedChildWorld is the already-stopped world: the DB row is
// stopped AND the target is absent from the child's applied config — the
// fast DB-only branch.
func (e *deleteCoordEnv) seedDeleteStoppedChildWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	e.seedDeleteChildClient(nil)
	row := e.seedDeleteRow(coordChildID, testClientID, old, true)
	if seedRes {
		e.seedDeleteCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedDeleteActiveOriginWorld builds the LEGACY origin-shaped ACTIVE world:
// a never-shadowed origin whose own config carries the target and whose
// proxy row is active ON the origin (no origin link).
func (e *deleteCoordEnv) seedDeleteActiveOriginWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
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
	row := e.seedDeleteRow(testClientID, "", old, false)
	if seedRes {
		e.seedDeleteCommittedRes(row.ID, old.protocol, old.remote)
	}
	return row
}

// seedDeleteIdemTx plants a transaction row for idempotency replay tests.
func (e *deleteCoordEnv) seedDeleteIdemTx(txID, key, status, code, msg string, req *pb.DeleteProxyConfigRequest) {
	t := e.t
	t.Helper()
	require.NoError(t, e.db.Create(&models.ChangeTransaction{
		TransactionID: txID, IdempotencyKey: key,
		RequestHash:  hashDeleteRequest(req, testClientID),
		Operation:    models.TxOperationDelete,
		Status:       status,
		ErrorCode:    code,
		ErrorMessage: msg,
		ActorUserID:  e.userInfo.GetUserID(),
		TenantID:     e.userInfo.GetTenantID(),
		StartedAt:    e.now,
	}).Error)
}

// ---- tests ----

// TestSafeDeleteActiveFullHappy is the active normal full branch: the
// target rule is in the child config, so SafeDelete applies the
// target-removed config to the PHYSICAL origin agent (TargetClientID = the
// row's client, Replace empty), verifies the port released, then the
// commit HARD-DELETES the proxy row AND releases the committed reservation
// by owner. Delete never runs the client local-listen gate.
func TestSafeDeleteActiveFullHappy(t *testing.T) {
	env := newDeleteCoordEnv(t)
	oldRow := env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// full branch: exactly one Apply (physical origin, target = row client,
	// replace empty) and one VerifyReleased; ZERO client local-listen gates
	require.Equal(t, 1, env.apply.calls, "the full branch must apply exactly once")
	require.Equal(t, 1, env.verify.releasedCalls, "the full branch must VerifyReleased exactly once")
	require.Zero(t, env.verify.appliedCalls, "no VerifyApplied on the forward delete path")
	require.Zero(t, env.pre.clientGateCalls, "delete must never run the client local-listen gate")
	require.Zero(t, env.pre.serverProbeCalls, "delete must never run the server bind probe")
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")
	applied := env.apply.callsData[0]
	require.Equal(t, testClientID, applied.PhysicalAgentID, "the physical agent is the canonical origin, never the raw child")
	require.Equal(t, coordChildID, applied.TargetClientID, "the apply targets the rule's real client row")
	require.Equal(t, "", applied.ReplaceClientID, "delete is never a directed switch")
	require.NotContains(t, string(applied.Config), `"name":"web"`, "the applied config must have the target removed")
	require.NotContains(t, string(applied.Config), `name = "web"`, "the applied config must have the target removed (toml form)")

	// proxy row: HARD-DELETED by the commit
	require.Nil(t, env.proxyRowByID(oldRow.ID), "the delete commit must hard-delete the proxy row")

	// client row: the target rule is gone from the applied config, sibling
	// fields (secret) untouched
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Empty(t, cfgs, "the deleted target must be removed from the child config")

	// committed reservation: RELEASED BY OWNER inside the commit — zero
	// reservation rows remain
	require.Zero(t, env.countReservations(), "delete releases the committed lease inside the commit")
	require.Nil(t, env.reservationByPort(8000))

	// tx: committed, bound to the old row, full-branch evidence
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID, "the commit returns the old proxy id even though the row is gone")
	ev := decodeDeleteEvidence(t, tx)
	require.Equal(t, "full", ev["branch"])
	require.Equal(t, true, ev["applied_to_agent"])
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Equal(t, float64(1), ev["verify_calls"])

	// 3 snapshots: client/proxy before (coordinator) + client after
	// (commit). Delete writes NO proxy-after snapshot — the row is gone.
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 3)
	beforeClient := env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeClient)
	beforeProxy := env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy)
	afterClient := env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeClient)
	afterProxy := env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy)
	require.NotNil(t, beforeClient)
	require.NotNil(t, beforeProxy)
	require.NotNil(t, afterClient)
	require.Nil(t, afterProxy, "delete never writes a proxy-after snapshot")
	require.Equal(t, coordChildID, beforeClient.NodeID)
	require.Equal(t, strconv.FormatUint(uint64(oldRow.ID), 10), beforeProxy.NodeID)
	require.Equal(t, coordChildID, afterClient.NodeID)

	// the proxy-before envelope is the rollback source: stopped value +
	// typed content of the OLD row
	proxyPlain := decryptSnapshot(t, beforeProxy, env.key.Key)
	var envp updateProxyEnvelope
	require.NoError(t, json.Unmarshal(proxyPlain, &envp))
	require.Equal(t, 1, envp.Version)
	require.Equal(t, oldRow.ID, envp.ProxyID)
	require.Equal(t, "web", envp.Name)
	require.Equal(t, coordChildID, envp.ClientID)
	require.Equal(t, testClientID, envp.OriginClientID)
	require.False(t, envp.Stopped)
	require.Equal(t, oldRow.Content, envp.Typed, "the before envelope carries the unchanged rule")

	// audit: exactly one delete event, no plaintext
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeDelete, audits[0].EventType)
	require.True(t, audits[0].Success)
	require.NotContains(t, audits[0].Summary, "[[proxies]]")
	require.NotContains(t, audits[0].Summary, "child-secret")
	require.NotContains(t, audits[0].Summary, "origin-secret")
}

// TestSafeDeleteStoppedTargetAbsentFast is the fast DB-only branch: the
// row is already stopped AND the target is absent from the applied config
// — the coordinator only verifies the port is released and commits.
// ZERO Apply.
func TestSafeDeleteStoppedTargetAbsentFast(t *testing.T) {
	env := newDeleteCoordEnv(t)
	oldRow := env.seedDeleteStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	childBefore := env.clientRowByID(coordChildID)
	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// zero-Apply contract, one VerifyReleased, no gates
	require.Zero(t, env.apply.calls, "the fast branch never applies anything")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.pre.clientGateCalls, "delete must never run the client local-listen gate")
	require.Zero(t, env.pre.serverProbeCalls)
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")

	// client row: WHOLE row incl. UpdatedAt byte-identical (fast never
	// rewrites an identical config)
	require.Equal(t, childBefore, env.clientRowByID(coordChildID), "the fast branch must not touch the child row")

	// row hard-deleted, reservation released by owner
	require.Nil(t, env.proxyRowByID(oldRow.ID))
	require.Zero(t, env.countReservations())
	require.Nil(t, env.reservationByPort(8000))

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	ev := decodeDeleteEvidence(t, tx)
	require.Equal(t, "fast", ev["branch"])
	require.Equal(t, false, ev["applied_to_agent"])
	require.Equal(t, float64(0), ev["apply_calls"])
	require.Equal(t, float64(1), ev["verify_calls"])

	// client-after snapshot byte-identical to client-before; no proxy after
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 3)
	beforePlain := decryptSnapshot(t, env.snapByType(snaps, models.SnapshotSideBefore, models.SnapshotNodeTypeClient), env.key.Key)
	afterPlain := decryptSnapshot(t, env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeClient), env.key.Key)
	require.Equal(t, beforePlain, afterPlain)
	require.Nil(t, env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy))
}

// TestSafeDeleteStoppedDBTargetStillInChildConfigFullSelfHeal: the DB row
// is stopped but the target is STILL in the child config (a drifted runtime
// state) — the coordinator must run the FULL branch to remove the rule from
// the agent, not trust the DB-only fast path.
func TestSafeDeleteStoppedDBTargetStillInChildConfigFullSelfHeal(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
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
	ev := decodeDeleteEvidence(t, tx)
	require.Equal(t, "full", ev["branch"])
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Zero(t, env.countReservations())
}

// TestSafeDeleteActiveDBTargetAbsentFast: the DB row is ACTIVE but the
// target is already absent from the applied config — a DB-only delete heals
// the row without any agent contact (fast branch).
func TestSafeDeleteActiveDBTargetAbsentFast(t *testing.T) {
	env := newDeleteCoordEnv(t)
	oldRow := env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// remove the target from the child config after seeding (simulate drift)
	cli := env.clientByID(coordChildID)
	cfg, err := cli.GetConfigContent()
	require.NoError(t, err)
	cfg.Proxies = nil
	require.NoError(t, cli.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.userInfo, cli))
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls, "target-absent active delete is DB-only")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Nil(t, env.proxyRowByID(oldRow.ID))
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeDeleteEvidence(t, tx)
	require.Equal(t, "fast", ev["branch"])
}

// TestSafeDeleteLegacyNoReservationPendingAcquiredThenDeleted covers the
// legacy world on the ORIGIN-shaped rule: no committed reservation exists,
// so the coordinator TryAcquires a pending lease and the commit DELETES it
// (by current tx) together with the row — zero reservations after.
func TestSafeDeleteLegacyNoReservationPendingAcquiredThenDeleted(t *testing.T) {
	env := newDeleteCoordEnv(t)
	oldRow := env.seedDeleteActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, int64(1), env.res.acquireCalls(), "the legacy world must acquire one pending lease")
	require.Equal(t, 1, env.apply.calls, "origin-shaped active delete is the full branch")
	require.Equal(t, testClientID, env.apply.callsData[0].PhysicalAgentID)
	require.Equal(t, testClientID, env.apply.callsData[0].TargetClientID, "origin-shaped rules apply to the origin handler itself")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	require.Nil(t, env.proxyRowByID(oldRow.ID))
	require.Zero(t, env.countReservations(), "the pending lease acquired by this tx is deleted by the commit")
	require.Nil(t, env.reservationByPort(8000))
}

// TestSafeDeleteFastVerifyReleasedFailureUpgradesToFull: the fast branch's
// VerifyReleased fails (the agent still runs the rule despite the DB) — the
// coordinator must UPGRADE to the full branch and apply, NEVER report the
// verifier failure as success.
func TestSafeDeleteFastVerifyReleasedFailureUpgradesToFull(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	scripted := &scriptedVerifier{released: []error{errors.New("port still occupied by the stale agent rule"), nil}}
	env.sdeps.Verifier = scripted
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, 1, env.apply.calls, "the failed fast verify must upgrade to a real Apply")
	require.Equal(t, 2, scripted.releasedCalls, "one failed fast verify + one post-apply verify")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	ev := decodeDeleteEvidence(t, tx)
	require.Equal(t, "full", ev["branch"], "an upgraded delete DID touch the agent")
	require.Equal(t, float64(1), ev["apply_calls"])
	require.Equal(t, float64(2), ev["verify_calls"])
	require.Zero(t, env.countReservations(), "the SameKeyOwned lease is released by owner at commit")
}

// TestSafeDeleteApplyRejectCompensates: the full-branch Apply is rejected
// by the agent — the coordinator compensates (restore before config, verify
// the old rule is back) and lands on rolled_back with APPLY_REJECTED (the
// empty-code fallback), releasing the current pending lease.
func TestSafeDeleteApplyRejectCompensates(t *testing.T) {
	env := newDeleteCoordEnv(t)
	oldRow := env.seedDeleteActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: errors.New("agent rejected the config")},
		{}, // compensation restore succeeds
	}
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeApplyRejected)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, testClientID, env.apply.callsData[1].PhysicalAgentID, "compensation restores on the physical origin")
	require.Equal(t, testClientID, env.apply.callsData[1].TargetClientID)
	require.Equal(t, 1, env.verify.appliedCalls, "compensation verifies the old rule is back (delete direction)")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyRejected, tx.ErrorCode)
	require.Equal(t, 0, env.countReservations(), "a successfully compensated delete releases the current pending lease")
	// the row survives a compensated delete
	require.NotNil(t, env.proxyRowByID(oldRow.ID))
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.Equal(t, models.AuditEventTypeCompensation, audits[0].EventType)
	require.True(t, audits[0].Success)
}

// TestSafeDeleteApplyTimeoutCompensates: an Apply deadline uses the
// APPLY_TIMEOUT fallback code.
func TestSafeDeleteApplyTimeoutCompensates(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: context.DeadlineExceeded},
		{}, // compensation restore succeeds
	}
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeApplyTimeout)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeApplyTimeout, tx.ErrorCode)
}

// TestSafeDeletePostApplyVerifyFailureCompensates: VerifyReleased fails
// after a successful Apply — full compensation, rolled_back + VERIFY_FAILED.
func TestSafeDeletePostApplyVerifyFailureCompensates(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.verify.releasedErr = errors.New("port still held after apply")
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, 1, env.verify.appliedCalls, "compensation verifies the old state")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeVerifyFailed, tx.ErrorCode)
	require.Zero(t, env.countReservations(), "the legacy pending lease is released after a successful compensation")
}

// TestSafeDeleteVerifyingTransitionFailureCompensates: the post-apply
// applying->verifying DB transition fails — the config IS applied, so the
// coordinator compensates with CONFIG_COMMIT_FAILED.
func TestSafeDeleteVerifyingTransitionFailureCompensates(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failStatus := &failingDeleteStatusStore{DeleteTxStore: env.sdeps.Tx, failStatus: models.TxStatusVerifying}
	env.sdeps.Tx = failStatus
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	require.Equal(t, 1, env.verify.appliedCalls)
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
}

// TestSafeDeleteCommitFailureCompensatesKeepsHistoricalCommittedLease: the
// full branch's CommitDelete fails on a SameKeyOwned world (the committed
// lease belongs to the ORIGINAL create transaction) — the coordinator
// compensates with CONFIG_COMMIT_FAILED, lands on rolled_back, and the
// historical committed lease is NEVER released by the current txID.
func TestSafeDeleteCommitFailureCompensatesKeepsHistoricalCommittedLease(t *testing.T) {
	env := newDeleteCoordEnv(t)
	oldRow := env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failCommit := &failingDeleteCommitStore{DeleteTxStore: env.sdeps.Tx, fail: true}
	env.sdeps.Tx = failCommit
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Equal(t, 2, env.apply.calls, "forward apply + compensation restore")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRolledBack, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	// SameKeyOwned: the historical committed lease (transaction_id of the
	// ORIGINAL create) must SURVIVE — FailTransaction releases only the
	// current tx's pending, and the commit rolled back.
	require.Equal(t, 1, env.countReservations())
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, updOldCreateTx, res.TransactionID, "the historical committed lease is never deleted by txID")
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	// the row survives a compensated delete
	require.NotNil(t, env.proxyRowByID(oldRow.ID))
}

// TestSafeDeleteCompensationApplyFailureRollbackFailed: the compensation's
// own restore Apply fails — rollback_failed, ROLLBACK_FAILED, and the
// current pending lease is RETAINED for manual intervention.
func TestSafeDeleteCompensationApplyFailureRollbackFailed(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.apply.results = []applyResult{
		{code: "", err: errors.New("forward rejected")},
		{code: "", err: errors.New("restore rejected")},
	}
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

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

// TestSafeDeleteCompensationVerifyFailureRollbackFailed: the compensation
// restore applies but the old-state verify fails — rollback_failed with the
// lease retained.
func TestSafeDeleteCompensationVerifyFailureRollbackFailed(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveOriginWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	scripted := &scriptedVerifier{released: []error{errors.New("post-apply verify failed")}, applied: []error{errors.New("old rule did not come back")}}
	env.sdeps.Verifier = scripted
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeRollbackFailed)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusRollbackFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeRollbackFailed, tx.ErrorCode)
	require.Equal(t, 1, env.countReservations(), "the pending lease is retained on rollback_failed")
	requireCompensationEvidence(t, tx, "compensate-verify-old", true)
}

// TestSafeDeleteFastCommitFailureNoPseudoRollback: the FAST branch (zero
// Apply) CommitDelete fails — the transaction lands on failed +
// CONFIG_COMMIT_FAILED. There was never an agent apply, so rolled_back /
// ROLLBACK_FAILED would be a lie. The SameKeyOwned committed lease stays
// (no current pending exists to release).
func TestSafeDeleteFastCommitFailureNoPseudoRollback(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	failCommit := &failingDeleteCommitStore{DeleteTxStore: env.sdeps.Tx, fail: true}
	env.sdeps.Tx = failCommit
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeCommitFailed)

	require.Zero(t, env.apply.calls, "the fast branch never applied anything")
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status, "fast commit failure is failed, never rolled_back")
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	// SameKeyOwned: nothing to release — the historical committed lease stays
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, updOldCreateTx, res.TransactionID)
	require.NotNil(t, env.proxyRowByID(*tx.ProxyID), "the failed fast delete must not remove the row")
}

// TestSafeDeleteDuplicateTargetRejected: the target name appears more than
// once in the applied config — fail closed with zero Apply and zero
// transaction rows (the rejection happens before any write).
func TestSafeDeleteDuplicateTargetRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient([]v1.TypedProxyConfig{
		typedProxy("web", "tcp", 8000, 8080),
		typedProxy("web", "tcp", 8000, 9090),
	})
	env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions(), "rejections before any tx row is created")
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeDeletePhysicalKeyDriftRejected: the applied config carries the
// target name on a DIFFERENT physical key than the rule row — the
// coordinator must never delete a same-name drifted rule.
func TestSafeDeletePhysicalKeyDriftRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient([]v1.TypedProxyConfig{typedProxy("web", "tcp", 9000, 8080)})
	env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeDeleteUnparseableConfigRejected: the persisted client config
// cannot be parsed — fail closed with CONFIG_VALIDATION_FAILED, zero tx,
// zero Apply.
func TestSafeDeleteUnparseableConfigRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient(nil)
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", coordChildID).
		Update("config_content", []byte("{not-json")).Error)
	env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeDeleteNilBaseConfigRejected: a malformed typed entry WITHOUT a
// base config makes the physical key unprovable — fail closed (unlike the
// stop filter which silently skips nil-base entries, delete must reject).
func TestSafeDeleteNilBaseConfigRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient(nil)
	// a proxies entry with no base config at all (nil ProxyConfigurer)
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", coordChildID).
		Update("config_content", []byte(`{"proxies":[null]}`)).Error)
	env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeDeleteClientShapeMismatchRejected: the child client the rule
// lives on does not match the request's canonical origin — fail closed.
func TestSafeDeleteClientShapeMismatchRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
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
	env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeDeleteOriginStoppedRejected: the physical origin agent is stopped
// — it cannot receive the update (and the fast branch may upgrade to an
// agent apply), fail closed with AGENT_OFFLINE, zero Apply.
func TestSafeDeleteOriginStoppedRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	origin := env.clientByID(testClientID)
	origin.Stopped = true
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.userInfo, origin))
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeAgentOffline)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeDeleteServerMismatchRejected: the rule does not exist for the
// requested server — CONFIG_VALIDATION_FAILED, zero Apply.
func TestSafeDeleteServerMismatchRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, &pb.DeleteProxyConfigRequest{
		ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr("srv-other"),
		Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey()),
	})
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeDeleteUnsupportedTypeRejected: an HTTP/HTTPS/STCP/XTCP rule is
// read-only per spec §1.2.5 — a delete request must be rejected with
// UNSUPPORTED_PROXY_TYPE before any write or agent contact.
func TestSafeDeleteUnsupportedTypeRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient(nil)
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
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeUnsupportedProxyType)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
	require.Zero(t, env.res.acquireCalls())
}

// TestSafeDeleteValidationRejected covers the request-shape contract:
// missing client/server/name and a malformed idempotency key all fail with
// the structured CONFIG_VALIDATION_FAILED before any lock/write.
func TestSafeDeleteValidationRejected(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()

	cases := []struct {
		name string
		req  *pb.DeleteProxyConfigRequest
	}{
		{"missing client", &pb.DeleteProxyConfigRequest{ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing server", &pb.DeleteProxyConfigRequest{ClientId: lo.ToPtr(testClientID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing name", &pb.DeleteProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), IdempotencyKey: lo.ToPtr(uuidKey())}},
		{"missing idem key", &pb.DeleteProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web")}},
		{"malformed idem key", &pb.DeleteProxyConfigRequest{ClientId: lo.ToPtr(testClientID), ServerId: lo.ToPtr(testServerID), Name: lo.ToPtr("web"), IdempotencyKey: lo.ToPtr("not-a-uuid")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := sc.SafeDeleteProxy(env.ctx, tc.req)
			require.NoError(t, err)
			assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
		})
	}
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// TestSafeDeleteChildAddressedLocksCanonicalOriginKeyOnce: a child-addressed
// delete locks the (tenant, ORIGIN) key exactly once — no double lock, no
// deadlock, and the key is canonical (never the raw child).
func TestSafeDeleteChildAddressedLocksCanonicalOriginKeyOnce(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	rec := &proxyRecordingLocker{}
	env.sdeps.OriginLocker = rec
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(uuidKey(), coordChildID, "web"))
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

// TestSafeDeleteHoldsSharedOriginLockBlockingLegacyLock: while the safe
// delete holds the origin lock mid-apply, a concurrent LockLegacyOrigin —
// the exact lock body every legacy writer uses — must NOT acquire; after
// the release it acquires and resolves the canonical origin.
func TestSafeDeleteHoldsSharedOriginLockBlockingLegacyLock(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sig := newContentionSignalingLocker(NewOriginKeyedLock())
	env.sdeps.OriginLocker = sig
	oldLive := liveOriginLocker
	liveOriginLocker = sig
	t.Cleanup(func() { liveOriginLocker = oldLive })

	barrier := &barrierReleaseApply{inner: env.apply, entered: make(chan struct{}), release: make(chan struct{})}
	env.sdeps.Apply = barrier
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	deleteDone := make(chan error, 1)
	go func() {
		_, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, coordChildID, "web"))
		deleteDone <- err
	}()
	<-barrier.entered // SafeDelete provably holds the lock (mid-apply)

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
	require.NoError(t, <-deleteDone)
	<-legacyDone
	require.NoError(t, legacyErr)
	require.Equal(t, testClientID, legacyOrigin, "child-addressed LockLegacyOrigin must resolve the canonical origin")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
}

// TestSafeDeleteIdempotentCommittedReplayAfterRowGone: replaying the same
// idempotency key after a committed delete (whose row is ALREADY GONE)
// still returns success with ZERO side effects — the replay runs before any
// re-resolution of the deleted rule.
func TestSafeDeleteIdempotentCommittedReplayAfterRowGone(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls)
	// the row is gone after the first delete
	require.Nil(t, env.proxyRowByID(*env.txByKey(key).ProxyID))

	// replay: committed -> idempotent success even though the row is gone
	resp2, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp2.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls, "a committed replay must not re-apply")
	require.Equal(t, 1, env.verify.releasedCalls, "a committed replay must not re-verify")
	require.Equal(t, 1, env.countTransactions(), "the replay must reuse the same transaction")
	require.Zero(t, env.countReservations(), "the committed replay must not re-create any lease")
}

// TestSafeDeleteIdempotencyConflictDifferentHash: the same idempotency key
// replayed with a DIFFERENT delete target is a stable IDEMPOTENCY_CONFLICT.
func TestSafeDeleteIdempotencyConflictDifferentHash(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	req := deleteReq(key, testClientID, "web")
	resp, err := sc.SafeDeleteProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// same key, different name -> different canonical hash
	otherReq := deleteReq(key, testClientID, "web2")
	env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web2", protocol: "tcp", remote: 9000, local: 9090}, false)
	resp2, err := sc.SafeDeleteProxy(env.ctx, otherReq)
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp2, defs.ErrorCodeIdempotencyConflict)
	require.Equal(t, 1, env.countTransactions(), "the conflicting replay must not create a second transaction")
}

// TestSafeDeleteIdempotencyInFlightReplay: replaying a key whose
// transaction is still running returns the stable IDEMPOTENCY_IN_FLIGHT.
func TestSafeDeleteIdempotencyInFlightReplay(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient(nil)
	req := deleteReq("00000000-0000-4000-8000-000000000001", testClientID, "web")
	env.seedDeleteIdemTx("tx-delete-inflight", "00000000-0000-4000-8000-000000000001", models.TxStatusApplying, "", "", req)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, req)
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeIdempotencyInFlight)
	require.Zero(t, env.apply.calls)
}

// TestSafeDeleteTerminalFailureReplay: replaying a key whose transaction
// terminally failed returns the ORIGINAL failure result.
func TestSafeDeleteTerminalFailureReplay(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient(nil)
	req := deleteReq("00000000-0000-4000-8000-000000000002", testClientID, "web")
	env.seedDeleteIdemTx("tx-delete-failed", "00000000-0000-4000-8000-000000000002", models.TxStatusFailed,
		defs.ErrorCodeVerifyFailed, "verify failed earlier", req)
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, req)
	require.NoError(t, err)
	assertDeleteInvalidCode(t, resp, defs.ErrorCodeVerifyFailed)
	require.Zero(t, env.apply.calls)
}

// TestSafeDeleteIdempotencyUniqueRaceReplays: two concurrent requests with
// the same key — the loser's Create hits the unique idempotency index and
// must REPLAY the winner's committed transaction, never apply.
func TestSafeDeleteIdempotencyUniqueRaceReplays(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	req := deleteReq("00000000-0000-4000-8000-0000000000aa", testClientID, "web")
	winner := &models.ChangeTransaction{
		TransactionID: "tx-delete-winner", IdempotencyKey: "00000000-0000-4000-8000-0000000000aa",
		RequestHash: hashDeleteRequest(req, testClientID),
		Operation:   models.TxOperationDelete,
		Status:      models.TxStatusCommitted,
		ActorUserID: env.userInfo.GetUserID(),
		TenantID:    env.userInfo.GetTenantID(),
		StartedAt:   env.now,
	}
	race := &racingDeleteCreateStore{DeleteTxStore: env.sdeps.Tx, winner: winner}
	env.sdeps.Tx = race
	sc := env.newDeleteCoordinator()

	resp, err := sc.SafeDeleteProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Zero(t, env.apply.calls, "the loser of the insert race replays, never applies")
	require.Zero(t, env.verify.releasedCalls)
	require.Equal(t, 2, race.findCalls, "the race must re-find the winner after the insert failure")
}

// TestSafeDeleteEvidenceAuditNoSecretSiblingUntouched: the committed
// evidence + audit never carry config/secret plaintext, and a sibling rule
// (row + applied config) is left byte-identical by the delete.
func TestSafeDeleteEvidenceAuditNoSecretSiblingUntouched(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.seedDeleteChildClient([]v1.TypedProxyConfig{
		typedProxy("web", "tcp", 8000, 8080),
		typedProxy("sib", "tcp", 7000, 7000),
	})
	oldRow := env.seedDeleteRow(coordChildID, testClientID, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080}, false)
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
	env.seedDeleteCommittedRes(oldRow.ID, "tcp", 8000)
	env.seedDeleteCommittedRes(sibRow.ID, "tcp", 7000)
	sc := env.newDeleteCoordinator()
	key := uuidKey()

	beforeSibRow := *sibRow
	resp, err := sc.SafeDeleteProxy(env.ctx, deleteReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// target row gone, sibling proxy row untouched (content + stopped + id)
	require.Nil(t, env.proxyRowByID(oldRow.ID))
	afterSib := env.proxyRowByID(sibRow.ID)
	require.Equal(t, beforeSibRow.Content, afterSib.Content)
	require.False(t, afterSib.Stopped)

	// sibling still in the applied config, target gone
	cli := env.clientByID(coordChildID)
	cfgs, err := utils.LoadProxiesFromContent(cli.ConfigContent)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	require.Equal(t, "sib", cfgs[0].GetBaseConfig().Name)

	// the sibling's committed lease (different port) is untouched; only the
	// target port lease was released by the commit
	require.Equal(t, 1, env.countReservations(), "only the target's lease is released; the sibling's survives")
	sibRes := env.reservationByPort(7000)
	require.NotNil(t, sibRes, "the sibling's committed lease must survive the delete")
	require.Equal(t, models.ReservationStatusCommitted, sibRes.Status)
	require.Equal(t, sibRow.ID, *sibRes.OwnerProxyID)
	targetRes := env.reservationByPort(8000)
	require.Nil(t, targetRes, "the target's committed lease is released by owner at commit")

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
