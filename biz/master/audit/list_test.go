package audit

import (
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

// TestListAuditEmptySuccess: an authenticated user with no audit rows gets
// SUCCESS with an empty (non-nil) event list and total 0 — not an error.
func TestListAuditEmptySuccess(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	resp, err := ListAudit(ctx, &pb.ListAuditRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.GetStatus())
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetEvents())
	require.Equal(t, int32(0), resp.GetTotal())
	_ = user
}

// TestListAuditMapsFields: every redacted AuditEvent column maps onto the
// wire event exactly — and the transaction-detail fields (operation/status/
// proxy_id) stay UNSET in the list view (they belong to GetAudit).
func TestListAuditMapsFields(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-list-1",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "create tcp 8000 -> 127.0.0.1:22 ok",
		NodeID:        "aliyun-1",
		EvidenceRef:   "change_transaction:tx-list-1#evidence_json",
		ErrorCode:     "",
		Success:       true,
	})

	resp, err := ListAudit(ctx, &pb.ListAuditRequest{Page: lo.ToPtr(int32(1)), PageSize: lo.ToPtr(int32(20))})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Len(t, resp.GetEvents(), 1)
	ev := resp.GetEvents()[0]
	require.Equal(t, "tx-list-1", ev.GetTransactionId())
	require.Equal(t, models.AuditEventTypeCommit, ev.GetEventType())
	require.Equal(t, "create tcp 8000 -> 127.0.0.1:22 ok", ev.GetSummary())
	require.Equal(t, "aliyun-1", ev.GetNodeId())
	require.True(t, ev.GetSuccess())
	require.NotZero(t, ev.GetCreatedAt())
	require.Equal(t, "change_transaction:tx-list-1#evidence_json", ev.GetEvidenceRef())
	// detail fields are a GetAudit concern, never the list view
	require.Equal(t, "", ev.GetOperation())
	require.Equal(t, "", ev.GetStatus())
	require.Equal(t, int64(0), ev.GetProxyId())
}

// TestListAuditPageSizeAndTxFilter: page/page_size flow to the DAO (which
// clamps pageSize to 1..200) and the optional transaction_id filter is
// honored, with total reflecting the FILTERED count.
func TestListAuditPageSizeAndTxFilter(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	for _, txID := range []string{"tx-a", "tx-a", "tx-b"} {
		seedAuditEvent(t, ctx, user, &models.AuditEvent{
			TransactionID: txID,
			EventType:     models.AuditEventTypePreflight,
			Summary:       "preflight ok",
			Success:       true,
		})
	}

	// page 2 of size 2 over 3 rows -> exactly the newest row
	resp, err := ListAudit(ctx, &pb.ListAuditRequest{Page: lo.ToPtr(int32(2)), PageSize: lo.ToPtr(int32(2))})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Len(t, resp.GetEvents(), 1)
	require.Equal(t, int32(3), resp.GetTotal())

	// tx filter
	resp, err = ListAudit(ctx, &pb.ListAuditRequest{TransactionId: lo.ToPtr("tx-a")})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Len(t, resp.GetEvents(), 2)
	require.Equal(t, int32(2), resp.GetTotal())
	for _, ev := range resp.GetEvents() {
		require.Equal(t, "tx-a", ev.GetTransactionId())
	}
}

// TestListAuditTenantIsolation: a tenant-2 caller never sees tenant-1 rows
// (real SQLite DAO, tenant scoping cannot be bypassed through the handler).
func TestListAuditTenantIsolation(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-t1",
		EventType:     models.AuditEventTypeCommit,
		Summary:       "tenant 1 secret row",
		Success:       true,
	})

	otherCtx, otherUser := newAuditTestEnvTenant(t, ctx, 2)
	resp, err := ListAudit(otherCtx, &pb.ListAuditRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetEvents(), "cross-tenant rows must never surface")
	require.Equal(t, int32(0), resp.GetTotal())
	_ = otherUser
}

// TestListAuditMalformedUserFailClosed: an unauthenticated context is
// rejected with INVALID — never an empty-but-successful leak.
func TestListAuditMalformedUserFailClosed(t *testing.T) {
	ctx, _ := newAuditTestEnv(t)
	anon := newAuditTestEnvAnonymous(ctx)
	resp, err := ListAudit(anon, &pb.ListAuditRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Empty(t, resp.GetEvents())
}

// TestListAuditSummaryRedaction: dirty historical summaries carrying
// sensitive markers are replaced by the generic "[redacted]" placeholder;
// legitimate known summaries pass through untouched.
func TestListAuditSummaryRedaction(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-dirty",
		EventType:     models.AuditEventTypeCreate,
		Summary:       "rotated secret: password=sup3rs3cret",
		Success:       true,
	})
	seedAuditEvent(t, ctx, user, &models.AuditEvent{
		TransactionID: "tx-clean",
		EventType:     models.AuditEventTypeCreate,
		Summary:       "create tcp 8000 -> 127.0.0.1:22 ok",
		Success:       true,
	})

	resp, err := ListAudit(ctx, &pb.ListAuditRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	byTx := map[string]string{}
	for _, ev := range resp.GetEvents() {
		byTx[ev.GetTransactionId()] = ev.GetSummary()
	}
	require.Equal(t, "[redacted]", byTx["tx-dirty"], "sensitive summary must be redacted")
	require.Equal(t, "create tcp 8000 -> 127.0.0.1:22 ok", byTx["tx-clean"], "legit summary must pass through")
}

// TestListAuditUnclampedPageSizeDefaultsToDAO: DAO pageSize defaults
// (1..200, default 20) apply when the request carries no page/size — the
// handler must not invent its own pagination.
func TestListAuditUnclampedPageSizeDefaultsToDAO(t *testing.T) {
	ctx, user := newAuditTestEnv(t)
	for i := 0; i < 25; i++ {
		seedAuditEvent(t, ctx, user, &models.AuditEvent{
			TransactionID: "tx-default-page",
			EventType:     models.AuditEventTypeCommit,
			Summary:       "ok",
			Success:       true,
		})
	}
	resp, err := ListAudit(ctx, &pb.ListAuditRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RespCode_RESP_CODE_SUCCESS, resp.GetStatus().GetCode())
	require.Len(t, resp.GetEvents(), 20, "DAO default pageSize is 20")
	require.Equal(t, int32(25), resp.GetTotal())
}
