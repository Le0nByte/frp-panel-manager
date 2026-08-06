package proxy

import (
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/stretchr/testify/require"
)

// B4c route-switch tests: the live /delete_config route (DeleteProxyConfig,
// handler.go /api/v1/proxy/delete_config) must be a THIN DELEGATE to the
// B3c safe delete coordinator (the package-global safeDeleteCoordinator).
// The handler itself must not validate-by-legacy-contract, must not acquire
// the legacy origin lock (LockLegacyOrigin / liveOriginLocker) and must not
// run the legacy DB-first + agent flow.
//
// All tests swap the package-global safeDeleteCoordinator for a coordinator
// with injected deps (the real fake-DB env + fake apply/verify services —
// never string assertions) and restore it on cleanup; they MUST NOT run
// t.Parallel (the swap is package-global).

// swapDeleteCoordinator replaces the package-global route coordinator with
// an injected one and restores it on cleanup. Route-swap tests MUST NOT run
// t.Parallel: the swap is package-global.
func swapDeleteCoordinator(t *testing.T, sc *DeleteTransactionCoordinator) {
	t.Helper()
	old := safeDeleteCoordinator
	safeDeleteCoordinator = sc
	t.Cleanup(func() { safeDeleteCoordinator = old })
}

// TestDeleteProxyConfigRouteValidationContract: a request with a MISSING or
// INVALID idempotency_key (or a missing mandatory field) must return the
// SafeDelete STRUCTURED business response: (resp, nil) with
// error_code=CONFIG_VALIDATION_FAILED. The legacy handler contract was a
// BARE error (or — for a missing key — no idempotency gate at all and the
// legacy flow ran). The validation fails closed BEFORE any lock/DB/agent.
func TestDeleteProxyConfigRouteValidationContract(t *testing.T) {
	env := newDeleteCoordEnv(t)
	swapDeleteCoordinator(t, env.newDeleteCoordinator())

	cases := []struct {
		name string
		req  *pb.DeleteProxyConfigRequest
	}{
		{
			name: "missing idempotency_key",
			req:  deleteReq("", testClientID, "web"),
		},
		{
			name: "invalid idempotency_key",
			req:  deleteReq("not-a-uuid", testClientID, "web"),
		},
		{
			name: "missing client_id",
			req:  deleteReq(uuidKey(), "", "web"),
		},
		{
			name: "missing name",
			req:  deleteReq(uuidKey(), testClientID, ""),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := DeleteProxyConfig(env.ctx, tc.req)
			require.NoError(t, err, "a structured business failure must never surface as a bare error")
			require.NotNil(t, resp)
			assertDeleteInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
		})
	}
}

// TestDeleteProxyConfigRouteDelegatesSafeDeleteActiveFull: an ACTIVE rule
// (target still in the child's applied config) deleted through the ROUTE
// must run the safe FULL branch — exactly one Apply to the physical origin
// with the target removed, one VerifyReleased, a committed delete
// transaction bound to the old row, the proxy row HARD-DELETED and the
// committed lease released (delete releases, unlike stop/start). The legacy
// route would have run the async/DB-first body instead (no transaction at
// all) and surfaced any agent failure as a bare error.
func TestDeleteProxyConfigRouteDelegatesSafeDeleteActiveFull(t *testing.T) {
	env := newDeleteCoordEnv(t)
	// an (empty) clients manager keeps the RED legacy path a fast, clean
	// bare error instead of a nil-manager panic
	env.appInst.SetClientsManager(&mapClientsManager{conns: map[string]*defs.Connector{}})
	oldRow := env.seedDeleteActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapDeleteCoordinator(t, env.newDeleteCoordinator())
	key := uuidKey()

	resp, err := DeleteProxyConfig(env.ctx, deleteReq(key, coordChildID, "web"))
	require.NoError(t, err, "the route must return the safe flow's result, not a legacy bare error")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// full-branch evidence, reached THROUGH the route
	require.Equal(t, 1, env.apply.calls, "the route must run the synchronous safe full branch (one Apply)")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Equal(t, testClientID, env.apply.callsData[0].PhysicalAgentID, "the physical agent is the canonical origin")
	require.Equal(t, coordChildID, env.apply.callsData[0].TargetClientID)

	require.Nil(t, env.proxyRowByID(oldRow.ID), "the delete commit must hard-delete the proxy row")
	require.Zero(t, env.countReservations(), "delete releases the committed lease inside the commit")
	require.Nil(t, env.reservationByPort(8000))

	tx := env.txByKey(key)
	require.Equal(t, models.TxOperationDelete, tx.Operation, "the route must create the delete transaction")
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID, "the commit returns the old proxy id even though the row is gone")

	// 3 snapshots: client/proxy before (coordinator) + client after
	// (commit). Delete writes NO proxy-after snapshot — the row is gone.
	snaps := env.snapshots(tx.TransactionID)
	require.Len(t, snaps, 3)
	require.Nil(t, env.snapByType(snaps, models.SnapshotSideAfter, models.SnapshotNodeTypeProxy), "delete never writes a proxy-after snapshot")
}

// TestDeleteProxyConfigRouteDelegatesSafeDeleteStoppedFast: an ALREADY
// DELETED-from-config rule (row stopped + target absent from the applied
// config) deleted through the ROUTE must run the safe FAST (DB-only) branch
// — ZERO Apply, one VerifyReleased, a committed delete transaction and the
// row gone. The legacy body would have pushed the config to the agent
// regardless.
func TestDeleteProxyConfigRouteDelegatesSafeDeleteStoppedFast(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.appInst.SetClientsManager(&mapClientsManager{conns: map[string]*defs.Connector{}})
	oldRow := env.seedDeleteStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapDeleteCoordinator(t, env.newDeleteCoordinator())
	key := uuidKey()

	resp, err := DeleteProxyConfig(env.ctx, deleteReq(key, coordChildID, "web"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls, "the fast branch must never apply anything through the route")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Zero(t, env.verify.appliedCalls)

	require.Nil(t, env.proxyRowByID(oldRow.ID), "the delete commit must hard-delete the proxy row")
	require.Zero(t, env.countReservations(), "delete releases the committed lease inside the commit")

	tx := env.txByKey(key)
	require.Equal(t, models.TxOperationDelete, tx.Operation)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
}

// TestDeleteProxyConfigRouteDoesNotTakeLegacyOriginLock: the route must NOT
// go through LockLegacyOrigin / liveOriginLocker — the safe coordinator
// owns the (tenant, origin) keyed lock (its injected OriginLocker), and the
// handler is a thin delegate. The recording locker proves ZERO Lock calls
// on the legacy locker while the safe flow completes (no double lock, no
// deadlock: the coordinator locks exactly once on its own injected
// instance).
func TestDeleteProxyConfigRouteDoesNotTakeLegacyOriginLock(t *testing.T) {
	env := newDeleteCoordEnv(t)
	env.appInst.SetClientsManager(&mapClientsManager{conns: map[string]*defs.Connector{}})
	env.seedDeleteStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapDeleteCoordinator(t, env.newDeleteCoordinator())

	rec := &proxyRecordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	resp, err := DeleteProxyConfig(env.ctx, deleteReq(uuidKey(), coordChildID, "web"))

	rec.mu.Lock()
	keys := append([]string(nil), rec.keys...)
	rec.mu.Unlock()
	require.Empty(t, keys, "the /delete_config route must never acquire the legacy origin lock (LockLegacyOrigin/liveOriginLocker)")
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
}

// TestSafeDeleteCoordinatorRestoredAfterSwap is the sanity twin of the
// SafeStop/SafeStart ones: after the swap test, the package-global
// coordinator must be the default again (t.Cleanup restore in
// swapDeleteCoordinator), so the production route is never left pointing at
// a test instance.
func TestSafeDeleteCoordinatorRestoredAfterSwap(t *testing.T) {
	old := safeDeleteCoordinator
	defer func() { safeDeleteCoordinator = old }()
	safeDeleteCoordinator = NewDefaultDeleteCoordinator()
	require.NotNil(t, old)
}
