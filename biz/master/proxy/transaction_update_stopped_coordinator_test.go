package proxy

import (
	"context"
	"encoding/json"
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
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// ---- A4c: the STOPPED branch of the safe update (zero-Apply by
// construction). The DAO commit side is A3b; these tests prove the
// COORDINATOR side: branch routing, reservation reuse, child guard,
// before/after snapshots with byte-identical client state, DB-only
// applying/verifying transitions, the zero-Apply/zero-Verify contract and
// the shared origin lock.

// seedStoppedChildWorld builds the standard stopped world: shadowed origin
// + child whose APPLIED config does NOT contain the stopped rule (StopProxy
// semantics — a stopped rule is absent from the applied config) + server +
// the old STOPPED proxy_config row on the child. When seedRes is true a
// COMMITTED reservation owned by the old row (transaction_id =
// updOldCreateTx) exists at the old key.
func (e *updateCoordEnv) seedStoppedChildWorld(seedRes bool, old oldRuleSeed) *models.ProxyConfig {
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
	cfg.Proxies = []v1.TypedProxyConfig{}
	require.NoError(t, child.SetConfigContent(*cfg))
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, child))
	e.seedServer()

	oldTyped := typedProxy(old.name, old.protocol, old.remote, old.local)
	content, err := oldTyped.MarshalJSON()
	require.NoError(t, err)
	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: old.name, Type: old.protocol, Stopped: true,
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

// seedStoppedOriginWorld builds the UNSAFE stopped shape: a stopped rule
// that still lives on the NEVER-SHADOWED origin (client_id==origin,
// origin_client_id empty). The stopped branch may not create a child, so
// this shape is rejected up front.
func (e *updateCoordEnv) seedStoppedOriginWorld(old oldRuleSeed) *models.ProxyConfig {
	t := e.t
	t.Helper()
	origin := &models.ClientEntity{ClientID: testClientID, IsShadow: false, ConnectSecret: "origin-secret"}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.userInfo, origin))
	e.seedServer()

	oldTyped := typedProxy(old.name, old.protocol, old.remote, old.local)
	content, err := oldTyped.MarshalJSON()
	require.NoError(t, err)
	oldRow := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: testClientID, ServerID: testServerID,
		Name: old.name, Type: old.protocol, Stopped: true,
		TenantID: e.userInfo.GetTenantID(), UserID: e.userInfo.GetUserID(),
		Content: content,
	}}
	require.NoError(t, e.db.Create(oldRow).Error)
	return oldRow
}

// clientRowByID loads the full client row (including gorm timestamps) by
// client id — the zero-client-delta contract compares the WHOLE row.
func (e *updateCoordEnv) clientRowByID(id string) models.Client {
	t := e.t
	t.Helper()
	var row models.Client
	require.NoError(t, e.db.Where("client_id = ?", id).First(&row).Error)
	return row
}

// stoppedEvidence decodes the committed transaction's evidence_json.
func stoppedEvidence(t *testing.T, tx *models.ChangeTransaction) map[string]interface{} {
	t.Helper()
	require.NotEmpty(t, tx.EvidenceJSON, "the stopped commit must write evidence_json")
	var ev map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(tx.EvidenceJSON), &ev))
	return ev
}

// TestSafeUpdateStoppedSameKeyOwnedHappy is the stopped-branch happy path:
// SameKeyOwned (no TryAcquire), server+client gates run, ZERO Apply and
// ZERO Verifier calls, the child row (incl. UpdatedAt) byte-identical, the
// proxy row updated IN PLACE (id preserved, stopped stays true, content
// updated), the old committed reservation owner+transaction_id untouched,
// the tx committed with ProxyID=old, 4 snapshots and an audit + evidence
// that explicitly say "db-only; zero agent apply".
func TestSafeUpdateStoppedSameKeyOwnedHappy(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	childBefore := env.clientRowByID(coordChildID)
	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	// zero-Apply / zero-Verify contract
	require.Zero(t, env.apply.calls, "the stopped branch never applies anything to an agent")
	require.Zero(t, env.verify.appliedCalls, "no owner-proof VerifyApplied on the stopped branch")
	require.Zero(t, env.verify.releasedCalls, "no VerifyReleased on the stopped branch")
	// both gates DO run (server probe on the NEW key + client gate)
	require.Equal(t, 1, env.pre.serverProbeCalls)
	require.Equal(t, 1, env.pre.clientGateCalls)
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")

	// child row: WHOLE row incl. UpdatedAt byte-identical
	require.Equal(t, childBefore, env.clientRowByID(coordChildID), "the stopped branch must not touch the child row")

	// proxy row: in-place, id preserved, stopped stays true, content updated
	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.Equal(t, oldRow.ID, row.ID, "the stopped branch must keep the row id (the lease identity)")
	require.True(t, row.Stopped)
	require.Equal(t, "web", row.Name)
	require.Equal(t, testServerID, row.ServerID)
	wantTyped := typedProxy("web", "tcp", 8000, 9090)
	wantJSON, err := wantTyped.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, wantJSON, row.Content)

	// old committed reservation: owner + transaction_id untouched
	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID)

	// tx: committed, bound to the OLD row id, with stopped evidence
	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
	ev := stoppedEvidence(t, tx)
	require.Equal(t, "stopped", ev["branch"])
	require.Equal(t, false, ev["applied_to_agent"])
	require.Equal(t, float64(0), ev["apply_calls"])
	require.Equal(t, float64(0), ev["verify_calls"])

	// 4 snapshots: client-before/proxy-before (coordinator) + client-after/
	// proxy-after (commit), node ids bound to child + old row
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

	// client-after plaintext == client-before plaintext EXACTLY (the child
	// was never changed); both == the real child applied config bytes
	beforePlain := decryptSnapshot(t, beforeClient, env.key.Key)
	afterPlain := decryptSnapshot(t, afterClient, env.key.Key)
	require.Equal(t, beforePlain, afterPlain, "the client-after snapshot must be byte-identical to before")
	child := env.clientByID(coordChildID)
	realCfg, err := child.MarshalJSONConfig()
	require.NoError(t, err)
	require.Equal(t, realCfg, beforePlain, "client-before must be the real applied child config")

	// proxy-after: the STOPPED candidate envelope
	proxyPlain := decryptSnapshot(t, afterProxy, env.key.Key)
	var envp updateProxyEnvelope
	require.NoError(t, json.Unmarshal(proxyPlain, &envp))
	require.Equal(t, 1, envp.Version)
	require.Equal(t, "web", envp.Name)
	require.Equal(t, coordChildID, envp.ClientID)
	require.Equal(t, testClientID, envp.OriginClientID)
	require.True(t, envp.Stopped)
	require.Equal(t, wantJSON, envp.Typed)

	// audit: one update event, summary explicitly db-only, no plaintext
	audits := env.auditEvents(tx.TransactionID)
	require.Len(t, audits, 1)
	require.True(t, audits[0].Success)
	require.Contains(t, audits[0].Summary, "db-only; zero agent apply")
	require.NotEmpty(t, audits[0].EvidenceRef, "append-only audit must reference the zero-Apply evidence")
	require.Equal(t, "change_transaction:"+tx.TransactionID+"#evidence_json", audits[0].EvidenceRef)
	require.NotContains(t, audits[0].Summary, "[[proxies]]")
	require.NotContains(t, audits[0].Summary, "remotePort")
}

// TestSafeUpdateStoppedSameKeyLegacyPending covers the legacy world: NO
// committed reservation exists, so the coordinator acquires a NEW pending
// lease (SameKeyPending) and the commit flips it to committed owned by the
// old row, with transaction_id = the update tx. Still zero Apply/Verify.
func TestSafeUpdateStoppedSameKeyLegacyPending(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Equal(t, int64(1), env.res.acquireCalls(), "the legacy world must acquire one pending lease")
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)
	require.Equal(t, 1, env.pre.serverProbeCalls)
	require.Equal(t, 1, env.pre.clientGateCalls)

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

// TestSafeUpdateStoppedDifferentKeyProtocolChange moves the stopped rule
// tcp/8000 -> udp/8000 (protocol is inside the unique index, so it is a
// DIFFERENT key): the new-key pending commits owned by the old row id and
// the old committed row is deleted; zero Apply/Verify.
func TestSafeUpdateStoppedDifferentKeyProtocolChange(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebUDPTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)
	require.Equal(t, 1, env.pre.serverProbeCalls, "all stopped scenarios run the server probe on the NEW key")
	require.Equal(t, 1, env.pre.clientGateCalls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)

	var oldTcpRes []models.PortReservation
	require.NoError(t, env.db.Where("protocol = ? AND remote_port = ?", "tcp", 8000).Find(&oldTcpRes).Error)
	require.Empty(t, oldTcpRes, "the old committed tcp/8000 row must be deleted on a different-key stopped update")
	var udpRes []models.PortReservation
	require.NoError(t, env.db.Where("protocol = ? AND remote_port = ?", "udp", 8000).Find(&udpRes).Error)
	require.Len(t, udpRes, 1)
	require.Equal(t, models.ReservationStatusCommitted, udpRes[0].Status)
	require.Equal(t, oldRow.ID, *udpRes[0].OwnerProxyID)
	require.Equal(t, tx.TransactionID, udpRes[0].TransactionID)
	require.Equal(t, 1, env.countReservations())

	row := env.proxyRowByID(oldRow.ID)
	wantTyped := typedProxy("web", "udp", 8000, 8080)
	wantJSON, err := wantTyped.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, wantJSON, row.Content)
}

// TestSafeUpdateStoppedRename renames the stopped rule in place: the row id
// is preserved (the lease identity) and the child applied config must not
// contain either the old or the new name.
func TestSafeUpdateStoppedRename(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWeb2TOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls)
	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.Equal(t, oldRow.ID, row.ID, "a stopped rename must keep the row id")
	require.Equal(t, "web2", row.Name)
	require.True(t, row.Stopped)
	res := env.reservationByPort(8000)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
}

// TestSafeUpdateStoppedRenameConflictPrecheck: the rename pre-check runs on
// the stopped branch too — a second row already carrying the target name in
// the origin scope fails closed BEFORE any transaction is created.
func TestSafeUpdateStoppedRenameConflictPrecheck(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// seed a second row named web2 in the origin scope (on the child)
	otherTyped := typedProxy("web2", "tcp", 9000, 9000)
	content, err := otherTyped.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: "web2", Type: "tcp", Stopped: true,
		TenantID: env.userInfo.GetTenantID(), UserID: env.userInfo.GetUserID(),
		Content: content,
	}}).Error)
	uc := env.newUpdateCoordinator()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWeb2TOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions(), "the rename pre-check must fail before any tx row is created")
	require.Zero(t, env.res.acquireCalls())
	require.Equal(t, oldRow.ID, env.proxyRowByID(oldRow.ID).ID, "the old row must stay untouched")
}

// TestSafeUpdateStoppedLegacyOriginShapeRejected: a stopped rule that still
// lives on the NEVER-SHADOWED origin (client_id==origin) is the unsafe
// first-shadow state — the stopped branch may not create a child, so the
// request fails with CONFIG_VALIDATION_FAILED, zero client/proxy/res delta
// and zero transaction.
func TestSafeUpdateStoppedLegacyOriginShapeRejected(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedOriginWorld(oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()

	beforeClients, beforeProxies := env.captureConfigRows()
	beforeRes := env.countReservations()
	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.countTransactions(), "the shape rejection happens before any tx row is created")
	require.Zero(t, env.res.acquireCalls())
	afterClients, afterProxies := env.captureConfigRows()
	require.Equal(t, beforeClients, afterClients, "zero client delta on the unsafe shape")
	require.Equal(t, beforeProxies, afterProxies, "zero proxy delta on the unsafe shape")
	require.Equal(t, beforeRes, env.countReservations(), "zero reservation delta")
	require.Equal(t, oldRow.ID, env.proxyRowByID(oldRow.ID).ID)
}

// TestSafeUpdateStoppedTargetStillInChildConfigRejected: the stopped branch
// must never update a rule that is still present in the child's APPLIED
// config (it may still be live on the agent). Fail closed with
// CONFIG_VALIDATION_FAILED, the tx failed + released, zero Apply.
func TestSafeUpdateStoppedTargetStillInChildConfigRejected(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	// put the stopped rule back into the child's applied config (the
	// unsafe state: the rule may still be live on the agent)
	bad := &v1.ClientConfig{}
	bad.Complete()
	bad.Proxies = []v1.TypedProxyConfig{typedProxy("web", "tcp", 8000, 8080)}
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", coordChildID).Update("config_content", mustMarshalClientConfig(t, bad)).Error)
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status, "the rejection must fail the pending tx")
	// SameKeyOwned: the old committed reservation is untouched
	res := env.reservationByPort(8000)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID)
	require.Equal(t, oldRow.ID, env.proxyRowByID(oldRow.ID).ID, "the proxy row must stay untouched")
}

func mustMarshalClientConfig(t *testing.T, cfg *v1.ClientConfig) []byte {
	t.Helper()
	ent := &models.ClientEntity{}
	require.NoError(t, ent.SetConfigContent(*cfg))
	raw, err := ent.MarshalJSONConfig()
	require.NoError(t, err)
	return raw
}

// TestSafeUpdateStoppedServerGateFail: the server probe runs on ALL stopped
// scenarios; a busy public port fails the tx and releases the pending lease
// (SameKeyPending variant) while the old committed row (if any) is
// untouched — zero Apply.
func TestSafeUpdateStoppedServerGateFail(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.serverProbeResult = &PreflightGateResult{Ok: false, ErrorCode: defs.ErrorCodePortRemoteInUse, ErrorMessage: "port busy"}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebUDPTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodePortRemoteInUse)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)
	require.Equal(t, 1, env.pre.serverProbeCalls)
	require.Zero(t, env.pre.clientGateCalls, "a failed server gate must not run the client gate")

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	// DifferentKey: the new udp/8000 pending lease is released
	var udpRes []models.PortReservation
	require.NoError(t, env.db.Where("protocol = ? AND remote_port = ?", "udp", 8000).Find(&udpRes).Error)
	require.Empty(t, udpRes, "the failed tx's pending lease must be released")
	// the old committed tcp/8000 row survives
	oldRes := env.reservationByPort(8000)
	require.Equal(t, models.ReservationStatusCommitted, oldRes.Status)
	require.Equal(t, updOldCreateTx, oldRes.TransactionID)
}

// TestSafeUpdateStoppedClientGateFail: the client gate runs on the physical
// origin; a non-listening local target fails the tx + releases the pending
// lease, zero Apply.
func TestSafeUpdateStoppedClientGateFail(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedStoppedChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.pre.clientGateResult = &PreflightGateResult{Ok: false, ErrorCode: defs.ErrorCodePortLocalNotListening, ErrorMessage: "nothing listening"}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodePortLocalNotListening)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)
	require.Equal(t, 1, env.pre.serverProbeCalls)
	require.Equal(t, 1, env.pre.clientGateCalls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Zero(t, env.countReservations(), "SameKeyPending: the pending lease must be released")
	require.Zero(t, env.apply.calls)
}

// TestSafeUpdateStoppedCommitFailureNoCompensationSameKey: a CommitUpdate
// failure on SameKeyOwned rolls the commit back (zero agent side effects by
// construction — the stopped branch never applied anything), so there is
// NOTHING to compensate: the tx goes straight to failed with
// CONFIG_COMMIT_FAILED, the old committed reservation transaction_id is
// never rewritten, and no rolled_back status is ever used.
func TestSafeUpdateStoppedCommitFailureNoCompensationSameKey(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.udeps.Tx = &failingUpdateCommitStore{UpdateTxStore: newDAOStore(), fail: true}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status, "commit failure on the stopped branch is failed, never rolled_back")
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	res := env.reservationByPort(8000)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned: the old committed txID is never rewritten")
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	// the proxy row is untouched (the commit rolled back)
	require.Equal(t, oldRow.Content, env.proxyRowByID(oldRow.ID).Content)
	// no commit audit event for the failed commit (the fail audit is the
	// only event besides the tx-level record)
	require.Len(t, env.auditEvents(tx.TransactionID), 1)
	require.False(t, env.auditEvents(tx.TransactionID)[0].Success)
}

// TestSafeUpdateStoppedCommitFailureNoCompensationDifferentKey: same on the
// DifferentKey path — the NEW pending lease acquired by the tx is released,
// the old committed row survives, zero Apply/Verify.
func TestSafeUpdateStoppedCommitFailureNoCompensationDifferentKey(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.udeps.Tx = &failingUpdateCommitStore{UpdateTxStore: newDAOStore(), fail: true}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebUDPTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	var udpRes []models.PortReservation
	require.NoError(t, env.db.Where("protocol = ? AND remote_port = ?", "udp", 8000).Find(&udpRes).Error)
	require.Empty(t, udpRes, "the DifferentKey pending lease must be released")
	oldRes := env.reservationByPort(8000)
	require.Equal(t, models.ReservationStatusCommitted, oldRes.Status)
	require.Equal(t, updOldCreateTx, oldRes.TransactionID)
	require.Equal(t, oldRow.ID, *oldRes.OwnerProxyID)
	require.Equal(t, oldRow.Content, env.proxyRowByID(oldRow.ID).Content)
}

// TestSafeUpdateStoppedStatusTransitionFailure: a DB-only applying/verifying
// transition failure (zero agent side effects) fails the tx with the stable
// CONFIG_COMMIT_FAILED, releases the pending lease and never calls Apply or
// the Verifier.
func TestSafeUpdateStoppedStatusTransitionFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failStatus string
		seedRes    bool
		config     []byte
	}{
		{"applying-same-key", models.TxStatusApplying, true, updateWebTOML},
		{"verifying-different-key", models.TxStatusVerifying, true, updateWebUDPTOML},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newUpdateCoordEnv(t)
			env.seedStoppedChildWorld(tc.seedRes, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
			env.udeps.Tx = &failingUpdateStatusStore{UpdateTxStore: newDAOStore(), failStatus: tc.failStatus}
			uc := env.newUpdateCoordinator()
			key := uuidKey()

			resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", tc.config))
			require.NoError(t, err)
			assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)
			require.Zero(t, env.apply.calls, "a DB-only transition failure must never touch an agent")
			require.Zero(t, env.verify.appliedCalls)
			require.Zero(t, env.verify.releasedCalls)
			require.Equal(t, 1, env.pre.serverProbeCalls)
			require.Equal(t, 1, env.pre.clientGateCalls)

			tx := env.txByKey(key)
			require.Equal(t, models.TxStatusFailed, tx.Status)
			require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
			var udpRes []models.PortReservation
			require.NoError(t, env.db.Where("protocol = ? AND remote_port = ?", "udp", 8000).Find(&udpRes).Error)
			require.Empty(t, udpRes, "the pending lease must be released")
			oldRes := env.reservationByPort(8000)
			require.Equal(t, models.ReservationStatusCommitted, oldRes.Status)
			require.Equal(t, updOldCreateTx, oldRes.TransactionID)
		})
	}
}

func TestSafeUpdateStoppedPreflightTransitionFailure(t *testing.T) {
	env := newUpdateCoordEnv(t)
	oldRow := env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	env.udeps.Tx = &failingUpdateStatusStore{UpdateTxStore: newDAOStore(), failStatus: models.TxStatusPreflight}
	uc := env.newUpdateCoordinator()
	key := uuidKey()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(key, testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeCommitFailed)
	require.Zero(t, env.pre.serverProbeCalls)
	require.Zero(t, env.pre.clientGateCalls)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls)

	tx := env.txByKey(key)
	require.Equal(t, models.TxStatusFailed, tx.Status)
	require.Equal(t, defs.ErrorCodeCommitFailed, tx.ErrorCode)
	oldRes := env.reservationByPort(8000)
	require.NotNil(t, oldRes)
	require.Equal(t, updOldCreateTx, oldRes.TransactionID)
	require.Equal(t, oldRow.ID, *oldRes.OwnerProxyID)
}

// TestSafeUpdateStoppedIdempotentCommittedReplay: the stopped branch
// participates in the shared idempotency replay — a committed stopped
// update replays as idempotent success and no second tx is created.
func TestSafeUpdateStoppedIdempotentCommittedReplay(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	uc := env.newUpdateCoordinator()
	key := uuidKey()
	req := updateReq(key, testClientID, "web", updateWebTOML)

	resp, err := uc.SafeUpdateProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.countTransactions())

	resp, err = uc.SafeUpdateProxy(env.ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.countTransactions(), "the replay must not create a second tx")
	require.Zero(t, env.apply.calls)
}

// ---- route delegation (D.13): the /update_config route is a THIN delegate
// to the safe update coordinator; both branches route through it and
// business rejections surface (resp, nil) with the real error_code. ----

// swapSafeCoordinator replaces the package-global route coordinator with an
// injected one and restores it on cleanup.
func swapSafeCoordinator(t *testing.T, uc *UpdateTransactionCoordinator) {
	t.Helper()
	old := safeUpdateCoordinator
	safeUpdateCoordinator = uc
	t.Cleanup(func() { safeUpdateCoordinator = old })
}

func TestUpdateRouteDelegatesToSafeCoordinatorStopped(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapSafeCoordinator(t, env.newUpdateCoordinator())

	resp, err := UpdateProxyConfig(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Zero(t, env.apply.calls, "the route must land on the stopped branch: zero agent apply")
	tx := env.latestUpdateTx()
	require.Equal(t, models.TxStatusCommitted, tx.Status)
}

func TestUpdateRouteDelegatesToSafeCoordinatorActive(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapSafeCoordinator(t, env.newUpdateCoordinator())

	resp, err := UpdateProxyConfig(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, 1, env.apply.calls, "the active branch applies exactly once")
	tx := env.latestUpdateTx()
	require.Equal(t, models.TxStatusCommitted, tx.Status)
}

// latestUpdateTx returns the newest update transaction row.
func (e *updateCoordEnv) latestUpdateTx() *models.ChangeTransaction {
	t := e.t
	t.Helper()
	var txs []models.ChangeTransaction
	require.NoError(t, e.db.Order("id desc").Find(&txs).Error)
	require.NotEmpty(t, txs)
	return &txs[0]
}

// TestUpdateRouteBusinessRejectionStructured: the route returns (resp, nil)
// with the stable error_code for business rejections — never a bare Go
// error.
func TestUpdateRouteBusinessRejectionStructured(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapSafeCoordinator(t, env.newUpdateCoordinator())

	resp, err := UpdateProxyConfig(env.ctx, &pb.UpdateProxyConfigRequest{
		ClientId: lo.ToPtr(testClientID),
		ServerId: lo.ToPtr(testServerID),
		Name:     lo.ToPtr("web"),
		Config:   updateWebTOML,
	})
	require.NoError(t, err, "a business rejection is (resp, nil), never a Go error")
	assertUpdateInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env.apply.calls)
	require.Zero(t, env.countTransactions())
}

// ---- shared origin lock (D.14): the SafeUpdate coordinator locks the SAME
// (tenant, origin) key the legacy writers use. ----

// barrierReleaseApply holds the FIRST Apply call until release closes —
// SafeUpdate is provably mid-flight (and mid-lock) while another goroutine
// contends on the same origin key.
type barrierReleaseApply struct {
	inner   ApplyService
	calls   int64
	entered chan struct{}
	release chan struct{}
}

func (a *barrierReleaseApply) Apply(ctx context.Context, c *app.Context, req ApplyRequest) (string, error) {
	if atomic.AddInt64(&a.calls, 1) == 1 {
		close(a.entered)
		<-a.release
	}
	return a.inner.Apply(ctx, c, req)
}

// TestSafeUpdateHoldsSharedOriginLockBlockingLegacyLock: while the safe
// update (active branch) holds the origin lock mid-apply, a concurrent
// LockLegacyOrigin — the exact lock body every legacy writer uses — must
// NOT acquire; after the release it acquires and resolves the canonical
// origin. Deterministic via channels, no sleeps.
func TestSafeUpdateHoldsSharedOriginLockBlockingLegacyLock(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedExistingChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	sig := newContentionSignalingLocker(NewOriginKeyedLock())
	env.udeps.OriginLocker = sig
	oldLive := liveOriginLocker
	liveOriginLocker = sig
	t.Cleanup(func() { liveOriginLocker = oldLive })

	barrier := &barrierReleaseApply{inner: env.apply, entered: make(chan struct{}), release: make(chan struct{})}
	env.udeps.Apply = barrier
	uc := env.newUpdateCoordinator()

	beforeClients, beforeProxies := env.captureConfigRows()
	updateDone := make(chan error, 1)
	go func() {
		_, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), testClientID, "web", updateWebTOML))
		updateDone <- err
	}()
	<-barrier.entered // SafeUpdate provably holds the lock (mid-apply)

	legacyDone := make(chan struct{})
	var (
		legacyOrigin string
		legacyUnlock func()
		legacyErr    error
	)
	go func() {
		defer close(legacyDone)
		legacyOrigin, legacyUnlock, legacyErr = LockLegacyOrigin(env.ctx, coordChildID)
	}()

	// the legacy lock body is provably blocked at the lock
	select {
	case <-sig.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("LockLegacyOrigin never contended on the shared origin lock")
	}
	afterClients, afterProxies := env.captureConfigRows()
	require.Equal(t, beforeClients, afterClients, "the blocked legacy lock must not mutate any client row")
	require.Equal(t, beforeProxies, afterProxies, "the blocked legacy lock must not mutate any proxy row")

	close(barrier.release)
	require.NoError(t, <-updateDone)
	<-legacyDone
	require.NoError(t, legacyErr)
	require.Equal(t, testClientID, legacyOrigin, "child-addressed LockLegacyOrigin must resolve the canonical origin")
	require.NotNil(t, legacyUnlock)
	legacyUnlock()

	tx := env.latestUpdateTx()
	require.Equal(t, models.TxStatusCommitted, tx.Status, "the safe update must commit after the release")
	require.Equal(t, int64(1), atomic.LoadInt64(&barrier.calls))
}

// TestSafeUpdateChildAddressedLocksCanonicalOriginKeyOnce: a child-
// addressed safe update locks the (tenant, ORIGIN) key exactly once — no
// double lock, no deadlock, and the key is canonical.
func TestSafeUpdateChildAddressedLocksCanonicalOriginKeyOnce(t *testing.T) {
	env := newUpdateCoordEnv(t)
	env.seedStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	rec := &proxyRecordingLocker{}
	env.udeps.OriginLocker = rec
	uc := env.newUpdateCoordinator()

	resp, err := uc.SafeUpdateProxy(env.ctx, updateReq(uuidKey(), coordChildID, "web", updateWebTOML))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.keys, 1, "the coordinator must lock exactly once")
	require.Equal(t, originlock.Key(1, testClientID), rec.keys[0],
		"child-addressed requests must lock the canonical (tenant, origin) key")
	require.Equal(t, rec.keys, rec.unlocks, "every Lock must be paired with an Unlock")
}
