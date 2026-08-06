package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/VaalaCat/frp-panel/biz/master/proxy"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// D2c-C: the AUDIT /rollback handler. The handler is a THIN BOUNDARY
// DELEGATE — it fail-closed checks the user and the two mandatory request
// fields, then hands the request to the transactional rollback coordinator
// (proxy.RollbackProxy). These tests prove the boundary gate, the
// delegation (a valid request reaches the INJECTED coordinator and its
// outcome passes through untouched) and the passthrough of the
// coordinator's structured and internal errors. The coordinator's own deep
// validation (replay/conflict/inflight, committed-only gate, snapshots,
// lock, apply/verify/compensation) is covered by the proxy package tests;
// the handler never duplicates it.
//
// The coordinator swap is package-global (SetDefaultRollbackCoordinator),
// so these tests must never run in parallel — they don't.
// ---------------------------------------------------------------------------

// swapRollbackCoordinator installs the REAL default rollback coordinator
// (the exact instance RollbackProxy uses in production) and restores the
// previous one via t.Cleanup. With a live coordinator installed, any
// accidental delegation would be observable — the boundary tests below
// assert the handler's own rejection shape and messages.
func swapRollbackCoordinator(t *testing.T) {
	t.Helper()
	old := proxy.SetDefaultRollbackCoordinator(proxy.NewDefaultRollbackCoordinator())
	require.NotNil(t, old, "a previous coordinator must exist to restore")
	t.Cleanup(func() { proxy.SetDefaultRollbackCoordinator(old) })
}

// rollbackRequestHash mirrors the coordinator's canonical idempotency hash
// (hashRollbackRequest): SHA-256 over the SOURCE transaction id. A replay
// row seeded with this hash is recognized by the coordinator as the SAME
// request.
func rollbackRequestHash(sourceTxID string) string {
	sum := sha256.Sum256([]byte(sourceTxID))
	return hex.EncodeToString(sum[:])
}

// seedCommittedRollbackReplay writes a COMMITTED rollback transaction for
// the given idempotency key through the REAL DAO. The coordinator's replay
// gate short-circuits BEFORE any snapshot/agent work and returns the SAME
// transaction id — the deterministic proof that a valid request reached the
// injected coordinator.
func seedCommittedRollbackReplay(t *testing.T, ctx *app.Context, user models.UserInfo, idemKey, sourceTxID, rbTxID string) {
	t.Helper()
	require.NoError(t, dao.NewMutation(ctx).Create(user, &models.ChangeTransaction{
		TransactionID:  rbTxID,
		IdempotencyKey: idemKey,
		RequestHash:    rollbackRequestHash(sourceTxID),
		Operation:      models.TxOperationRollback,
		Status:         models.TxStatusCommitted,
	}))
}

// assertNoRollbackTxCreated proves the boundary rejection had ZERO side
// effects: no rollback transaction exists for the idempotency key.
func assertNoRollbackTxCreated(t *testing.T, ctx *app.Context, user models.UserInfo, idemKey string) {
	t.Helper()
	_, err := dao.NewQuery(ctx).FindByIdempotencyKey(user, idemKey)
	require.ErrorIs(t, err, defs.ErrTransactionNotFound, "a boundary rejection must never create a rollback transaction")
}

func rollbackReq(txID, idemKey string) *pb.RollbackRequest {
	return &pb.RollbackRequest{
		TransactionId:  lo.ToPtr(txID),
		IdempotencyKey: lo.ToPtr(idemKey),
	}
}

// TestRollbackUnauthenticatedFailClosed: an unauthenticated context is
// rejected by the handler with INVALID + CONFIG_VALIDATION_FAILED BEFORE
// any coordinator work — the handler's own "invalid user" message (the
// coordinator would say "authentication required") proves the gate ran at
// the boundary.
func TestRollbackUnauthenticatedFailClosed(t *testing.T) {
	swapRollbackCoordinator(t)
	ctx, _ := newAuditTestEnv(t)
	anon := newAuditTestEnvAnonymous(ctx)

	resp, err := Rollback(anon, rollbackReq("tx-src", uuid.NewString()))
	require.NoError(t, err)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
	require.Equal(t, "invalid user", resp.GetStatus().GetMessage())
	require.Empty(t, resp.GetNewTransactionId())
}

// TestRollbackTransactionIDRequired: transaction_id is mandatory. The
// rejection carries INVALID + CONFIG_VALIDATION_FAILED and creates no
// rollback transaction (zero RPC, zero writes).
func TestRollbackTransactionIDRequired(t *testing.T) {
	swapRollbackCoordinator(t)
	ctx, user := newAuditTestEnv(t)

	idem := uuid.NewString()
	resp, err := Rollback(ctx, rollbackReq("", idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
	require.Equal(t, "transaction_id is required", resp.GetStatus().GetMessage())
	require.Empty(t, resp.GetNewTransactionId())
	assertNoRollbackTxCreated(t, ctx, user, idem)
}

// TestRollbackInvalidIdempotencyKey: the idempotency key must be present
// AND a UUID v4 (empty, non-UUID and a real v1 UUID all fail). The
// handler's own "idempotency_key must be a UUID v4" message differs from
// the coordinator's wording for the same input, proving the boundary gate
// rejected before delegation; nothing was created.
func TestRollbackInvalidIdempotencyKey(t *testing.T) {
	for name, key := range map[string]string{
		"empty key":  "",
		"not a uuid": "not-a-uuid",
		"uuid v1":    "550e8400-e29b-11d4-a716-446655440000",
		"uuid v5":    "6ba7b810-9dad-51d1-80b4-00c04fd430c8",
	} {
		t.Run(name, func(t *testing.T) {
			swapRollbackCoordinator(t)
			ctx, user := newAuditTestEnv(t)

			resp, err := Rollback(ctx, rollbackReq("tx-src", key))
			require.NoError(t, err)
			require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
			require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
			require.Equal(t, "idempotency_key must be a UUID v4", resp.GetStatus().GetMessage())
			require.Empty(t, resp.GetNewTransactionId())
			assertNoRollbackTxCreated(t, ctx, user, key)
		})
	}
}

// TestRollbackValidRequestReachesInjectedCoordinator: a valid request
// (non-empty transaction_id + UUID v4 idempotency key) is delegated to the
// INJECTED coordinator, which replays the seeded committed rollback
// transaction and returns SUCCESS with the SAME rollback transaction id.
// The handler alone can never fabricate SUCCESS + a new transaction id, so
// this proves the delegation is real and the coordinator's response passes
// through untouched.
func TestRollbackValidRequestReachesInjectedCoordinator(t *testing.T) {
	swapRollbackCoordinator(t)
	ctx, user := newAuditTestEnv(t)

	const (
		sourceTxID = "tx-src-replay-1"
		rbTxID     = "rb-tx-replay-1"
	)
	idem := uuid.NewString()
	seedCommittedRollbackReplay(t, ctx, user, idem, sourceTxID, rbTxID)

	resp, err := Rollback(ctx, rollbackReq(sourceTxID, idem))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, rbTxID, resp.GetNewTransactionId(),
		"a committed replay returns the SAME rollback transaction id")
}

// TestRollbackCoordinatorNotFoundPassthrough: a well-formed request for a
// MISSING source transaction reaches the coordinator, which answers with
// its own NOT_FOUND business rejection. The handler has no DB access and
// can never produce NOT_FOUND — the response proves the delegation and the
// untouched passthrough.
func TestRollbackCoordinatorNotFoundPassthrough(t *testing.T) {
	swapRollbackCoordinator(t)
	ctx, _ := newAuditTestEnv(t)

	resp, err := Rollback(ctx, rollbackReq("tx-ghost", uuid.NewString()))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_NOT_FOUND, resp.GetStatus().GetCode())
	require.Equal(t, "source transaction not found", resp.GetStatus().GetMessage())
	require.Empty(t, resp.GetNewTransactionId())
}

// TestRollbackCoordinatorStructuredErrorPassthrough: a committed source
// transaction whose operation is itself a rollback is not rollback-able —
// the coordinator rejects it with INVALID + CONFIG_VALIDATION_FAILED and
// the deep "not rollback-able" message (deeper than anything the handler
// boundary can produce). The structured error code travels through the
// handler in the dedicated error_code field unchanged.
func TestRollbackCoordinatorStructuredErrorPassthrough(t *testing.T) {
	swapRollbackCoordinator(t)
	ctx, user := newAuditTestEnv(t)

	require.NoError(t, dao.NewMutation(ctx).Create(user, &models.ChangeTransaction{
		TransactionID:  "tx-rollback-of-rollback",
		IdempotencyKey: "idem-src-badop",
		RequestHash:    "h",
		Operation:      models.TxOperationRollback,
		Status:         models.TxStatusCommitted,
	}))

	resp, err := Rollback(ctx, rollbackReq("tx-rollback-of-rollback", uuid.NewString()))
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, resp.GetErrorCode())
	require.Contains(t, resp.GetStatus().GetMessage(), "not rollback-able",
		"the coordinator's deep source-gate message must pass through")
	require.Empty(t, resp.GetNewTransactionId())
}

// TestRollbackCoordinatorInternalErrorPropagates: an internal coordinator
// failure (here: the snapshot key is not configured, so the coordinator
// cannot even decrypt the source snapshots) surfaces as (nil, err) — the
// handler never masks an internal error as a fabricated business response.
func TestRollbackCoordinatorInternalErrorPropagates(t *testing.T) {
	swapRollbackCoordinator(t)
	ctx, user := newAuditTestEnv(t)

	// a committed, rollback-able CREATE source transaction; its snapshots
	// are irrelevant because the coordinator must read the snapshot key
	// first and the test app has no MASTER_SNAPSHOT_KEY_FILE configured.
	proxyID := uint(7)
	seedCommittedTx(t, ctx, user, "tx-src-snapkey", &proxyID, `{"stage":"commit"}`)

	resp, err := Rollback(ctx, rollbackReq("tx-src-snapkey", uuid.NewString()))
	require.Nil(t, resp, "an internal coordinator error must not be wrapped into a business response")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot key")
}
