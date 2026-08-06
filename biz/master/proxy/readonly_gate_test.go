package proxy

// I1-B L2 write-entry gates (biz-internal callers cannot bypass the HTTP
// middleware): every managed proxy write entry returns its pb INVALID
// response with ErrorCode READ_ONLY (err == nil) while blocked, and every
// legacy/typed entry returns readonly.ErrReadOnly BEFORE validation,
// locking or any DB work. "Zero downstream" is proven by construction:
// READ_ONLY can only be produced by the entry gate — the coordinators and
// legacy bodies never reference it — and the unblocked twin of every
// assertion shows the REAL downstream answering (validation-first).

import (
	"context"
	"testing"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/stretchr/testify/require"
)

// newReadonlyGateCtx builds a minimal app context carrying a valid user
// (no DB is needed: the entry gates fire before any coordinator work, and
// the unblocked controls fail in the coordinators' validation-first step).
func newReadonlyGateCtx(t *testing.T) *app.Context {
	t.Helper()
	userInfo := &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1, Token: "t"}
	return app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, userInfo), app.NewApp())
}

// TestReadOnlyManagedEntriesReturnREAD_ONLYZeroCoordinator: all six
// managed write entries answer (resp, nil) with Status INVALID + ErrorCode
// READ_ONLY while blocked. The requests are deliberately empty: had the
// coordinator run it would have answered CONFIG_VALIDATION_FAILED, so the
// READ_ONLY shape is proof the coordinator was never reached.
func TestReadOnlyManagedEntriesReturnREAD_ONLYZeroCoordinator(t *testing.T) {
	readonly.Set(true, "snapshot key fingerprint deadbeef42 does not match")
	t.Cleanup(readonly.ResetForTest)
	ctx := newReadonlyGateCtx(t)

	t.Run("CreateProxyConfig", func(t *testing.T) {
		resp, err := CreateProxyConfig(ctx, &pb.CreateProxyConfigRequest{})
		require.NoError(t, err, "a blocked write is a structured response, not an error")
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	})

	t.Run("UpdateProxyConfig", func(t *testing.T) {
		resp, err := UpdateProxyConfig(ctx, &pb.UpdateProxyConfigRequest{})
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	})

	t.Run("DeleteProxyConfig", func(t *testing.T) {
		resp, err := DeleteProxyConfig(ctx, &pb.DeleteProxyConfigRequest{})
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	})

	t.Run("StartProxy", func(t *testing.T) {
		resp, err := StartProxy(ctx, &pb.StartProxyRequest{})
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	})

	t.Run("StopProxy", func(t *testing.T) {
		resp, err := StopProxy(ctx, &pb.StopProxyRequest{})
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	})

	t.Run("RollbackProxy", func(t *testing.T) {
		resp, err := RollbackProxy(ctx, &pb.RollbackRequest{})
		require.NoError(t, err)
		require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
		require.Equal(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	})
}

// TestReadOnlyManagedEntriesUnblockedReachCoordinator: the unblocked twin —
// the very same minimal requests are answered by the REAL coordinator with
// CONFIG_VALIDATION_FAILED, proving READ_ONLY in the blocked test can only
// have come from the entry gate.
func TestReadOnlyManagedEntriesUnblockedReachCoordinator(t *testing.T) {
	readonly.ResetForTest() // default unblocked
	t.Cleanup(readonly.ResetForTest)
	ctx := newReadonlyGateCtx(t)

	resp, err := CreateProxyConfig(ctx, &pb.CreateProxyConfigRequest{})
	require.NoError(t, err)
	require.NotEqual(t, defs.ErrorCodeReadOnly, resp.GetErrorCode())
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode(),
		"unblocked: the coordinator's validation-first response must surface")

	respU, err := UpdateProxyConfig(ctx, &pb.UpdateProxyConfigRequest{})
	require.NoError(t, err)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, respU.GetErrorCode())

	respD, err := DeleteProxyConfig(ctx, &pb.DeleteProxyConfigRequest{})
	require.NoError(t, err)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, respD.GetErrorCode())

	respStart, err := StartProxy(ctx, &pb.StartProxyRequest{})
	require.NoError(t, err)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, respStart.GetErrorCode())

	respStop, err := StopProxy(ctx, &pb.StopProxyRequest{})
	require.NoError(t, err)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, respStop.GetErrorCode())

	respRB, err := RollbackProxy(ctx, &pb.RollbackRequest{})
	require.NoError(t, err)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, respRB.GetErrorCode())
}

// TestReadOnlyLegacyEntriesReturnErrReadOnlyBeforeValidation: the worker
// legacy create/delete entries (and the locked bodies) return
// readonly.ErrReadOnly BEFORE any validation, locking or DB work — the
// parameters are deliberately garbage, so any other outcome would prove
// the gate is not first.
func TestReadOnlyLegacyEntriesReturnErrReadOnlyBeforeValidation(t *testing.T) {
	readonly.Set(true, "test: snapshot key mismatch")
	t.Cleanup(readonly.ResetForTest)
	ctx := newReadonlyGateCtx(t)

	err := CreateProxyConfigWithTypedConfig(ctx, CreateProxyConfigWithTypedConfigParam{})
	require.ErrorIs(t, err, readonly.ErrReadOnly,
		"the legacy create entry must gate before its request validation")

	err = CreateProxyConfigWithTypedConfigLocked(ctx, CreateProxyConfigWithTypedConfigParam{})
	require.ErrorIs(t, err, readonly.ErrReadOnly,
		"the worker-ingress locked create entry must gate before validation/DB")

	_, err = DeleteProxyConfigLegacy(ctx, &pb.DeleteProxyConfigRequest{})
	require.ErrorIs(t, err, readonly.ErrReadOnly,
		"the legacy delete entry must gate before its request validation")

	// The lock-free legacy delete body is production-callable (worker
	// ingress teardown and the legacy create cleanup path): it must gate
	// too, before any DB access.
	_, err = deleteProxyConfigLegacyLocked(ctx, &pb.DeleteProxyConfigRequest{})
	require.ErrorIs(t, err, readonly.ErrReadOnly,
		"the legacy locked delete body must gate before any DB work")

	require.NotContains(t, err.Error(), "deadbeef42")
}

// TestReadOnlyLegacyEntriesUnblockedValidationFirst: unblocked, the same
// garbage requests fail in the entries' own validation (never with
// ErrReadOnly), proving the blocked outcome came from the gate.
func TestReadOnlyLegacyEntriesUnblockedValidationFirst(t *testing.T) {
	readonly.ResetForTest() // default unblocked
	t.Cleanup(readonly.ResetForTest)
	ctx := newReadonlyGateCtx(t)

	err := CreateProxyConfigWithTypedConfig(ctx, CreateProxyConfigWithTypedConfigParam{})
	require.Error(t, err)
	require.NotErrorIs(t, err, readonly.ErrReadOnly)
	require.Contains(t, err.Error(), "request invalid")

	err = CreateProxyConfigWithTypedConfigLocked(ctx, CreateProxyConfigWithTypedConfigParam{})
	require.Error(t, err)
	require.NotErrorIs(t, err, readonly.ErrReadOnly)

	_, err = DeleteProxyConfigLegacy(ctx, &pb.DeleteProxyConfigRequest{})
	require.Error(t, err)
	require.NotErrorIs(t, err, readonly.ErrReadOnly)
	require.Contains(t, err.Error(), "request invalid")
}
