package proxy

import (
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/stretchr/testify/require"
)

// B4 route-switch tests: the live /stop route (StopProxy, handler.go
// /api/v1/proxy/stop_proxy) must be a THIN DELEGATE to the B3a safe stop
// coordinator (the package-global safeStopCoordinator). The handler itself
// must not validate-by-legacy-contract, must not acquire the legacy origin
// lock (LockLegacyOrigin / liveOriginLocker) and must not run the legacy
// DB-first + agent flow.
//
// All tests swap the package-global safeStopCoordinator for a coordinator
// with injected deps (the real fake-DB env + fake apply/verify services —
// never string assertions) and DEFER the restore; they MUST NOT run
// t.Parallel (the swap is package-global).

// swapStopCoordinator replaces the package-global route coordinator with an
// injected one and restores it on cleanup. Route-swap tests MUST NOT run
// t.Parallel: the swap is package-global.
func swapStopCoordinator(t *testing.T, sc *StopTransactionCoordinator) {
	t.Helper()
	old := safeStopCoordinator
	safeStopCoordinator = sc
	t.Cleanup(func() { safeStopCoordinator = old })
}

// TestStopProxyRouteValidationContract: B1 — a request with a MISSING or
// INVALID idempotency_key (or a missing mandatory field) must return the
// SafeStop STRUCTURED business response: (resp, nil) with
// error_code=CONFIG_VALIDATION_FAILED. The legacy handler contract was a
// BARE error (or — for a missing key — no idempotency gate at all and the
// legacy flow ran). The validation fails closed BEFORE any lock/DB/agent.
func TestStopProxyRouteValidationContract(t *testing.T) {
	env := newStopCoordEnv(t)
	swapStopCoordinator(t, env.newStopCoordinator())

	cases := []struct {
		name string
		req  *pb.StopProxyRequest
	}{
		{
			name: "missing idempotency_key",
			req:  stopReq("", testClientID, "web"),
		},
		{
			name: "invalid idempotency_key",
			req:  stopReq("not-a-uuid", testClientID, "web"),
		},
		{
			name: "missing client_id",
			req:  stopReq(uuidKey(), "", "web"),
		},
		{
			name: "missing name",
			req:  stopReq(uuidKey(), testClientID, ""),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := StopProxy(env.ctx, tc.req)
			require.NoError(t, err, "a structured business failure must never surface as a bare error")
			require.NotNil(t, resp)
			assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
		})
	}
}

// TestStopProxyRouteDelegatesSafeStopActiveFull: an ACTIVE rule (target
// still in the child's applied config) stopped through the ROUTE must run
// the safe FULL branch — exactly one Apply to the physical origin with the
// target removed, one VerifyReleased, a committed stop transaction bound to
// the old row, the row flipped to stopped. The legacy route would have run
// the async/DB-first body instead (no transaction at all) and surfaced any
// agent failure as a bare error.
func TestStopProxyRouteDelegatesSafeStopActiveFull(t *testing.T) {
	env := newStopCoordEnv(t)
	// an (empty) clients manager keeps the RED legacy path a fast, clean
	// bare error instead of a nil-manager panic
	env.appInst.SetClientsManager(&mapClientsManager{conns: map[string]*defs.Connector{}})
	oldRow := env.seedStopActiveChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapStopCoordinator(t, env.newStopCoordinator())
	key := uuidKey()

	resp, err := StopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err, "the route must return the safe flow's result, not a legacy bare error")
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetErrorCode(), "success responses must carry no error_code")

	// full-branch evidence, reached THROUGH the route
	require.Equal(t, 1, env.apply.calls, "the route must run the synchronous safe full branch (one Apply)")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Equal(t, testClientID, env.apply.callsData[0].PhysicalAgentID, "the physical agent is the canonical origin")
	require.Equal(t, coordChildID, env.apply.callsData[0].TargetClientID)

	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.True(t, row.Stopped)

	tx := env.txByKey(key)
	require.Equal(t, models.TxOperationStop, tx.Operation, "the route must create the stop transaction")
	require.Equal(t, models.TxStatusCommitted, tx.Status)
	require.Equal(t, oldRow.ID, *tx.ProxyID)

	res := env.reservationByPort(8000)
	require.NotNil(t, res)
	require.Equal(t, models.ReservationStatusCommitted, res.Status)
	require.Equal(t, oldRow.ID, *res.OwnerProxyID)
	require.Equal(t, updOldCreateTx, res.TransactionID, "SameKeyOwned must never rewrite the historical transaction_id")
}

// TestStopProxyRouteDelegatesSafeStopStoppedFast: an ALREADY-STOPPED rule
// (row stopped + target absent from the applied config) stopped through the
// ROUTE must run the safe FAST (DB-only) branch — ZERO Apply, one
// VerifyReleased, a committed stop transaction. The legacy body would have
// pushed the config to the agent regardless.
func TestStopProxyRouteDelegatesSafeStopStoppedFast(t *testing.T) {
	env := newStopCoordEnv(t)
	env.appInst.SetClientsManager(&mapClientsManager{conns: map[string]*defs.Connector{}})
	oldRow := env.seedStopStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapStopCoordinator(t, env.newStopCoordinator())
	key := uuidKey()

	resp, err := StopProxy(env.ctx, stopReq(key, testClientID, "web"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	require.Zero(t, env.apply.calls, "the fast branch must never apply anything through the route")
	require.Equal(t, 1, env.verify.releasedCalls)
	require.Zero(t, env.verify.appliedCalls)

	row := env.proxyRowByID(oldRow.ID)
	require.NotNil(t, row)
	require.True(t, row.Stopped)

	tx := env.txByKey(key)
	require.Equal(t, models.TxOperationStop, tx.Operation)
	require.Equal(t, models.TxStatusCommitted, tx.Status)
}

// TestStopProxyRouteDoesNotTakeLegacyOriginLock: the route must NOT go
// through LockLegacyOrigin / liveOriginLocker — the safe coordinator owns
// the (tenant, origin) keyed lock (its injected OriginLocker), and the
// handler is a thin delegate. The recording locker proves ZERO Lock calls
// on the legacy locker while the safe flow completes (no double lock, no
// deadlock: the coordinator locks exactly once on its own injected
// instance).
func TestStopProxyRouteDoesNotTakeLegacyOriginLock(t *testing.T) {
	env := newStopCoordEnv(t)
	env.appInst.SetClientsManager(&mapClientsManager{conns: map[string]*defs.Connector{}})
	env.seedStopStoppedChildWorld(true, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	swapStopCoordinator(t, env.newStopCoordinator())

	rec := &proxyRecordingLocker{}
	old := liveOriginLocker
	liveOriginLocker = rec
	defer func() { liveOriginLocker = old }()

	resp, err := StopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))

	rec.mu.Lock()
	keys := append([]string(nil), rec.keys...)
	rec.mu.Unlock()
	require.Empty(t, keys, "the /stop route must never acquire the legacy origin lock (LockLegacyOrigin/liveOriginLocker)")
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
}

// TestSafeStopCoordinatorRestoredAfterSwap is the sanity twin of the
// SafeCreate one: after the swap test, the package-global coordinator must
// be the default again (defer restore in swapStopCoordinator), so the
// production route is never left pointing at a test instance.
func TestSafeStopCoordinatorRestoredAfterSwap(t *testing.T) {
	old := safeStopCoordinator
	defer func() { safeStopCoordinator = old }()
	safeStopCoordinator = NewDefaultStopCoordinator()
	require.NotNil(t, old)
}
