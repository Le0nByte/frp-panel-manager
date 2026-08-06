package audit

import (
	"strings"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestGetAuditCommittedTxDetail: a committed transaction returns the newest
// audit event (with the tx's operation/status/proxy_id stamped on it), the
// REDACTED evidence JSON from the tx row, and the snapshot METADATA list —
// never ciphertext/nonce, never tenant/actor identity.
func TestGetAuditCommittedTxDetail(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	proxyID := uint(42)
	evidence := `{"stage":"verify","ok":true,"observed":{"pid":123}}`
	seedCommittedTx(t, ctx, user, "tx-detail-1", &proxyID, evidence)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-detail-1",
		EventType:     models.AuditEventTypePreflight,
		Summary:       "preflight tcp 8000 ok",
		NodeID:        "aliyun-1",
		Success:       true,
	})
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-detail-1",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "commit tcp 8000 ok",
		NodeID:        "aliyun-1",
		Success:       true,
	})
	seedSnapshot(t, ctx, user, "tx-detail-1", models.SnapshotSideBefore, models.SnapshotNodeTypeClient, "cli-1")
	seedSnapshot(t, ctx, user, "tx-detail-1", models.SnapshotSideAfter, models.SnapshotNodeTypeClient, "cli-1")

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-detail-1")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())

	ev := resp.GetEvent()
	require.NotNil(t, ev)
	require.Equal(t, "tx-detail-1", ev.GetTransactionId())
	require.Equal(t, models.AuditEventTypeCommit, ev.GetEventType(), "newest event wins")
	require.Equal(t, "commit tcp 8000 ok", ev.GetSummary())
	require.Equal(t, "aliyun-1", ev.GetNodeId())
	require.True(t, ev.GetSuccess())
	require.NotZero(t, ev.GetCreatedAt())
	// tx details stamped from the ChangeTransaction row
	require.Equal(t, models.TxOperationCreate, ev.GetOperation())
	require.Equal(t, models.TxStatusCommitted, ev.GetStatus())
	require.Equal(t, int64(42), ev.GetProxyId())

	// evidence = the tx row's redacted EvidenceJSON, byte-identical
	require.Equal(t, evidence, string(resp.GetEvidenceJson()))

	// snapshots: metadata only, ordered by id ASC (before first)
	snaps := resp.GetSnapshots()
	require.Len(t, snaps, 2)
	require.Equal(t, models.SnapshotSideBefore, snaps[0].GetSide())
	require.Equal(t, models.SnapshotSideAfter, snaps[1].GetSide())
	require.Equal(t, models.SnapshotNodeTypeClient, snaps[0].GetNodeType())
	require.Equal(t, "cli-1", snaps[0].GetNodeId())
	require.NotEmpty(t, snaps[0].GetSha256())
	require.NotZero(t, snaps[0].GetCreatedAt())

	// no tenant/actor/ciphertext/nonce may exist anywhere on the wire
	raw, err := proto.Marshal(resp)
	require.NoError(t, err)
	body := string(raw)
	for _, forbidden := range []string{"tenant_id", "tenantID", "actor_user_id", "actorUserID", "ciphertext", "nonce"} {
		require.False(t, strings.Contains(body, forbidden), "response must not carry %q", forbidden)
	}
}

// TestGetAuditNilProxyIDSafe: a transaction without a proxy_id must not
// panic and the event proxy_id stays unset (optional pointer safety).
func TestGetAuditNilProxyIDSafe(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedCommittedTx(t, ctx, user, "tx-noproxy-1", nil, `{"stage":"commit"}`)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-noproxy-1",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "commit ok",
		Success:       true,
	})

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-noproxy-1")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.NotNil(t, resp.GetEvent())
	require.Equal(t, int64(0), resp.GetEvent().GetProxyId())
	require.Nil(t, resp.GetEvent().ProxyId, "nil proxy_id must stay unset on the wire")
}

// TestGetAuditTxIDRequired: an empty transaction_id is INVALID, no lookup.
func TestGetAuditTxIDRequired(t *testing.T) {
	ctx, _ := newAuditTestEnv(t)
	resp, err := GetAudit(ctx, &pb.GetAuditRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Nil(t, resp.GetEvent())
}

// TestGetAuditRowMissing: a missing transaction is NOT_FOUND with no event
// and no evidence — the caller cannot distinguish it from a cross-tenant
// row (no existence leak).
func TestGetAuditRowMissing(t *testing.T) {
	ctx, _ := newAuditTestEnv(t)
	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-ghost")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_NOT_FOUND, resp.GetStatus().GetCode())
	require.Nil(t, resp.GetEvent())
	require.Empty(t, resp.GetEvidenceJson())
}

// TestGetAuditCrossTenantSameNotFound: tenant 2 asking for tenant 1's
// transaction gets the IDENTICAL NOT_FOUND response.
func TestGetAuditCrossTenantSameNotFound(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedCommittedTx(t, ctx, user, "tx-t1-secret", nil, `{"stage":"commit"}`)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-t1-secret",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "tenant 1 secret",
		Success:       true,
	})

	otherCtx, _ := newAuditTestEnvTenant(t, ctx, 2)
	resp, err := GetAudit(otherCtx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-t1-secret")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_NOT_FOUND, resp.GetStatus().GetCode())
	require.Nil(t, resp.GetEvent())
	require.Empty(t, resp.GetEvidenceJson())
}

// TestGetAuditTxWithoutEvents: a transaction row exists but its audit chain
// is empty — INVALID + CONFIG_VALIDATION_FAILED, and NO fabricated event.
func TestGetAuditTxWithoutEvents(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedCommittedTx(t, ctx, user, "tx-noevents-1", nil, `{"stage":"commit"}`)

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-noevents-1")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Contains(t, resp.GetStatus().GetMessage(), defs.ErrorCodeConfigValidationFailed)
	require.Nil(t, resp.GetEvent(), "no audit event must never be fabricated")
	require.Empty(t, resp.GetEvidenceJson())
	require.Empty(t, resp.GetSnapshots())
}

// TestGetAuditMalformedUserFailClosed: an unauthenticated context is
// rejected with INVALID.
func TestGetAuditMalformedUserFailClosed(t *testing.T) {
	ctx, _ := newAuditTestEnv(t)
	anon := newAuditTestEnvAnonymous(ctx)
	resp, err := GetAudit(anon, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-x")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Nil(t, resp.GetEvent())
}

// TestGetAuditEvidenceSensitiveKeys: evidence JSON whose keys carry a
// sensitive marker (secret/password/token/config/plaintext/ciphertext, at
// any nesting depth) fails closed: INVALID + CONFIG_VALIDATION_FAILED with
// EMPTY evidence — the original text never reaches the caller.
func TestGetAuditEvidenceSensitiveKeys(t *testing.T) {
	for name, dirty := range map[string]string{
		"top-level secret key":  `{"secret":"abc123"}`,
		"nested password key":   `{"data":{"password":"hunter2"}}`,
		"top-level token key":   `{"token":"jwt-xyz"}`,
		"nested config key":     `{"data":{"config":"plain"}}`,
		"plaintext key":         `{"plaintext":"secret"}`,
		"ciphertext key":        `{"ciphertext":"AAAA"}`,
		"mixed safe plus dirty": `{"stage":"verify","auth":{"token":"t"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, user := newAuditTestEnv(t)
			seedCommittedTx(t, ctx, user, "tx-dirty-ev", nil, dirty)
			seedAuditEvent(t, ctx, user, &models.AuditEvent{
				TransactionID: "tx-dirty-ev",
				EventType:     models.AuditEventTypeCommit,
				Summary:       "commit ok",
				Success:       true,
			})

			resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-dirty-ev")})
			require.NoError(t, err)
			require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
			require.Contains(t, resp.GetStatus().GetMessage(), defs.ErrorCodeConfigValidationFailed)
			require.Empty(t, resp.GetEvidenceJson(), "dirty evidence must never reach the caller")
			require.Nil(t, resp.GetEvent(), "fail-closed detail carries no partial body")
		})
	}
}

// TestGetAuditEvidenceInvalidJSON: non-JSON evidence fails closed.
func TestGetAuditEvidenceInvalidJSON(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedCommittedTx(t, ctx, user, "tx-badjson", nil, "this is not json at all")
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-badjson",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "commit ok",
		Success:       true,
	})

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-badjson")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Contains(t, resp.GetStatus().GetMessage(), defs.ErrorCodeConfigValidationFailed)
	require.Empty(t, resp.GetEvidenceJson())
}

// TestGetAuditEvidenceTooLarge: evidence over the 64KB guard fails closed.
func TestGetAuditEvidenceTooLarge(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	big := `{"stage":"verify","blob":"` + strings.Repeat("a", 65*1024) + `"}`
	seedCommittedTx(t, ctx, user, "tx-big-ev", nil, big)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-big-ev",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "commit ok",
		Success:       true,
	})

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-big-ev")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Contains(t, resp.GetStatus().GetMessage(), defs.ErrorCodeConfigValidationFailed)
	require.Empty(t, resp.GetEvidenceJson())
}

// TestGetAuditSummaryRedaction: dirty historical summaries are redacted in
// the detail event too, mirroring the list view.
func TestGetAuditSummaryRedaction(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedCommittedTx(t, ctx, user, "tx-dirty-sum", nil, `{"stage":"commit"}`)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-dirty-sum",
		EventType:     models.AuditEventTypeCreate,
		Summary:       "password=sup3rs3cret rotated",
		Success:       true,
	})

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-dirty-sum")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, "[redacted]", resp.GetEvent().GetSummary())
}

// TestGetAuditEvidenceValidLegitPasses: safe structured evidence (stage/
// error/observed keys only) is returned byte-identical.
func TestGetAuditEvidenceValidLegitPasses(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	evidence := `{"stage":"compensation","error":"apply failed","observed":{"pid":7}}`
	seedCommittedTx(t, ctx, user, "tx-legit-ev", nil, evidence)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-legit-ev",
		EventType:     models.AuditEventTypeCompensation,
		Summary:       "compensation ok",
		Success:       false,
	})

	resp, err := GetAudit(ctx, &pb.GetAuditRequest{TransactionId: lo.ToPtr("tx-legit-ev")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Equal(t, evidence, string(resp.GetEvidenceJson()))
}
