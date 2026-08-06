// Package audit implements the Master-side audit API (spec §7.1:
// POST /api/v1/audit/list, POST /api/v1/audit/get and POST
// /api/v1/audit/rollback). The list/detail handlers are strictly
// read-only and tenant-scoped; every summary and evidence payload passes
// the redaction guard (redact.go) before it reaches the UI. The rollback
// handler is a thin boundary-validation delegate to the transactional
// rollback coordinator (biz/master/proxy/transaction_rollback_coordinator.go)
// and never touches the DB itself.
package audit

import (
	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
)

// toProtoAuditEvent maps ONE redacted AuditEvent column to its wire
// representation. The list view exposes ONLY the redacted public columns —
// TenantID/ActorUserID/RequestRedacted/Ciphertext have no wire field and
// cannot leak by construction. The summary passes the redaction guard so a
// dirty historical row can never smuggle plaintext into the UI.
func toProtoAuditEvent(ev *models.AuditEvent) *pb.AuditEvent {
	return &pb.AuditEvent{
		TransactionId: lo.ToPtr(ev.TransactionID),
		EventType:     lo.ToPtr(ev.EventType),
		Summary:       lo.ToPtr(sanitizeSummary(ev.Summary)),
		NodeId:        lo.ToPtr(ev.NodeID),
		Success:       lo.ToPtr(ev.Success),
		CreatedAt:     lo.ToPtr(ev.CreatedAt.Unix()),
		ErrorCode:     lo.ToPtr(ev.ErrorCode),
		EvidenceRef:   lo.ToPtr(ev.EvidenceRef),
	}
}

// ListAudit returns the caller's audit trail (spec §7.1 POST
// /api/v1/audit/list): newest first, page/page_size forwarded to the
// tenant-scoped DAO (which clamps pageSize to 1..200), optional
// transaction_id filter. Every summary passes the redaction guard. The
// response is a structured business result: internal DAO failures are
// returned as errors for the Wrapper, business rejections as (resp, nil).
func ListAudit(c *app.Context, req *pb.ListAuditRequest) (*pb.ListAuditResponse, error) {
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() {
		return &pb.ListAuditResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
		}, nil
	}

	events, total, err := dao.NewQuery(c).List(userInfo, int(req.GetPage()), int(req.GetPageSize()), req.GetTransactionId())
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot list audit events")
		return nil, err
	}

	out := make([]*pb.AuditEvent, 0, len(events))
	for _, ev := range events {
		out = append(out, toProtoAuditEvent(ev))
	}
	return &pb.ListAuditResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Events: out,
		Total:  lo.ToPtr(int32(total)),
	}, nil
}
