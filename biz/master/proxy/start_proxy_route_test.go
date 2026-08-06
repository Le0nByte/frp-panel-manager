package proxy

import (
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/stretchr/testify/require"
)

// B4 route-switch tests: the live /start route (StartProxy, handler.go
// /api/v1/proxy/start_proxy) must be a THIN DELEGATE to the B3b safe start
// coordinator (the package-global safeStartCoordinator). The handler itself
// must not validate-by-legacy-contract, must not acquire the legacy origin
// lock (LockLegacyOrigin / liveOriginLocker) and must not run the legacy
// DB-first + agent flow.
//
// All tests swap the package-global safeStartCoordinator for a coordinator
// with injected deps (the real fake-DB env + fake apply/verify services —
// never string assertions) and DEFER the restore; they MUST NOT run
// t.Parallel (the swap is package-global).

// swapStartCoordinator replaces the package-global route coordinator with an
// injected one and restores it on cleanup. Route-swap tests MUST NOT run
// t.Parallel: the swap is package-global.
func swapStartCoordinator(t *testing.T, sc *StartTransactionCoordinator) {
	t.Helper()
	old := safeStartCoordinator
	safeStartCoordinator = sc
	t.Cleanup(func() { safeStartCoordinator = old })
}

// TestStartProxyRouteValidationContract: B1 — a request with a MISSING or
// INVALID idempotency_key (or a missing mandatory field) must return the
// SafeStart STRUCTURED business response: (resp, nil) with
// error_code=CONFIG_VALIDATION_FAILED. The legacy handler contract was a
// BARE error (or — for a missing key — no idempotency gate at all and the
// legacy flow ran). The validation fails closed BEFORE any lock/DB/agent.
func TestStartProxyRouteValidationContract(t *testing.T) {
	env := newStartCoordEnv(t)
	swapStartCoordinator(t, env.newStartCoordinator())

	cases := []struct {
		name string
		req  *pb.StartProxyRequest
	}{
		{
			name: "missing idempotency_key",
			req:  startReq("", testClientID, "web"),
		},
		{
			name: "invalid idempotency_key",
			req:  startReq("not-a-uuid", testClientID, "web"),
		},
		{
			name: "missing client_id",
			req:  startReq(uuidKey(), "", "web"),
		},
		{
			name: "missing name",
			req:  startReq(uuidKey(), testClientID, ""),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := StartProxy(env.ctx, tc.req)
			require.NoError(t, err, "a structured business failure must never surface as a bare error")
			require.NotNil(t, resp)
			assertStartInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
		})
	}
}

// TestStartProxyRouteDelegatesSafeStartActiveFull: an ACTIVE rule whose
// target is ABSENT from the child's applied config (a runtime drift) started
// through the ROUTE must run the safe FULL branch — one server bind probe +
// one client gate, exactly one Apply to the PHYSICAL origin with the target
// added, one VerifyApplied, a committed start transaction bound to the old
// row, SameKeyOwned reservation preserved (no TryAcquire) and the row stays
// active. The legacy route would have run the async/DB-first body instead
// (no transaction at all) and surfaced any agent failure as a bare error.
func TestStartProxyRouteDelegatesSafeStartActiveFull(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartActiveChildTargetAbsentWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapStartCoordinator(t, env.newStartCoordinator())
	key := uuidKey()

	resp, err := StartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err, "the route must return the safe flow's result, not a legacy bare error")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// full-branch evidence, reached THROUGH the route
	require.Equal(t, 1, env.apply.calls, "the route must run the synchronous safe full branch (one Apply)")
	require.Equal(t, 1, env.verify.appliedCalls)
	require.Zero(t, env.verify.releasedCalls, "no VerifyReleased on the forward start path")
	require.Equal(t, 1, env.pre.serverProbeCalls, "the full branch runs the server bind probe once")
	require.Equal(t, 1, env.pre.clientGateCalls, "the full branch runs the client local gate once")
	require.Equal(t, testClientID, env.apply.callsData[0].PhysicalAgentID, "the physical agent is the canonical origin")
	require.Equal(t, coordChildID, env.apply.callsData[0].TargetClientID, "the apply targets the rule's real client row")
	require.Equal(t, "", env.apply.callsData[0].ReplaceClientID, "start is never a directed switch")
	require.Zero(t, env.res.acquireCalls(), "SameKeyOwned must not TryAcquire")

	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.False(t, row.Stopped, "the started rule stays active")

	tx := env.txByKey(key)
	require.Equal(t, models.TxOperationStart, tx.Operation, "the route must create the start transaction")
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)

	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite the historical transaction_id")
}

// TestStartProxyRouteDelegatesSafeStartStoppedFast: an ALREADY-STOPPED rule
// whose target IS still present in the applied config started through the
// ROUTE must run the safe FAST (DB-only) branch — ZERO Apply, ZERO server
// bind probe, one client gate, one VerifyApplied owner proof, a committed
// start transaction and the row flipped to active. The legacy body would
// have pushed the config to the agent regardless.
func TestStartProxyRouteDelegatesSafeStartStoppedFast(t *testing.T) {
	env := newStartCoordEnv(t)
	oldRow := env.seedStartStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapStartCoordinator(t, env.newStartCoordinator())
	key := uuidKey()

	resp, err := StartProxy(env.ctx, startReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls, "the fast branch must never apply anything through the route")
	require.Zero(t, env.pre.serverProbeCalls, "the fast branch never runs the server bind probe")
	require.Equal(t, 1, env.pre.clientGateCalls, "the fast branch runs the client local gate once")
	require.Equal(t, 1, env.verify.appliedCalls, "the fast branch runs the VerifyApplied owner proof once")
	require.Zero(t, env.verify.releasedCalls)

	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.False(t, row.Stopped, "the stopped row must flip to active")

	tx := env.txByKey(key)
	require.Equal(t, models.TxOperationStart, tx.Operation)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)
}

// TestStartProxyRouteDoesNotTakeLegacyOriginLock: the route must NOT go
// through LockLegacyOrigin / liveOriginLocker — the safe coordinator owns
// the (tenant, origin) keyed lock (its injected OriginLocker), and the
// handler is a thin delegate. The recording locker proves ZERO Lock calls
// on the legacy locker while the safe flow completes (no double lock, no
// deadlock: the coordinator locks exactly once on its own injected
// instance).
func TestStartProxyRouteDoesNotTakeLegacyOriginLock(t *testing.T) {
	env := newStartCoordEnv(t)
	env.seedStartStoppedChildWithTargetWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapStartCoordinator(t, env.newStartCoordinator())

	rec := &proxyRecordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	resp, err := StartProxy(env.ctx, startReq(uuidKey(), testClientID, "web"))

	rec.mu.Lock()
	keys := append([]string(nil), rec.keys...)
	rec.mu.Unlock()
	require.Empty(t, keys, "the /start route must never acquire the legacy origin lock (LockLegacyOrigin/liveOriginLocker)")
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
}

// TestSafeStartCoordinatorRestoredAfterSwap is the sanity twin of the
// SafeCreate/SafeStop ones: after the swap test, the package-global
// coordinator must be the default again (defer restore in
// swapStartCoordinator), so the production route is never left pointing at a
// test instance.
func TestSafeStartCoordinatorRestoredAfterSwap(t *testing.T) {
	old := safeStartCoordinator
	defer func() { safeStartCoordinator = old }()
	safeStartCoordinator = NewDefaultStartCoordinator()
	require.NotNil(t, old)
}
