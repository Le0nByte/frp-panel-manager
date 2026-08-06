package audit

import (
	"errors"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
)

// auditEventChainLimit caps the event chain fetched for one transaction
// detail (newest first; the DAO clamps pageSize to 200 anyway).
const auditEventChainLimit = 200

// invalidEvidenceResp is the structured fail-closed detail response: the
// caller must never see a partial body (no event/snapshots/evidence) when
// the evidence guard or the event chain is unusable.
func invalidEvidenceResp(msg string) *pb.GetAuditResponse {
	return &pb.GetAuditResponse{
		Status: &pb.Status{
			Code:    pb.RespCode_RESP_CODE_INVALID,
			Message: defs.ErrorCodeConfigValidationFailed + ": " + msg,
		},
	}
}

// toSnapshotMetadata maps ONE ConfigSnapshot row to its wire metadata. Only
// side/node_type/node_id/sha256/created_at exist on the wire type —
// Ciphertext and Nonce have no field and cannot leak by construction
// (spec §6.3 encrypted snapshots stay server-side).
func toSnapshotMetadata(s *models.ConfigSnapshot) *pb.SnapshotMetadata {
	return &pb.SnapshotMetadata{
		Side:      lo.ToPtr(s.Side),
		NodeType:  lo.ToPtr(s.NodeType),
		NodeId:    lo.ToPtr(s.NodeID),
		Sha256:    lo.ToPtr(s.SHA256),
		CreatedAt: lo.ToPtr(s.CreatedAt.Unix()),
	}
}

// GetAudit returns one transaction's audit detail (spec §7.1 POST
// /api/v1/audit/get): the newest audit event (stamped with the owning
// ChangeTransaction's operation/status/proxy_id), the transaction's
// redacted EvidenceJSON (guarded), and the snapshot METADATA list. The
// transaction lookup is tenant-scoped: a missing row and a cross-tenant
// row are the SAME NOT_FOUND (no existence leak). Business rejections
// return (resp, nil); internal DAO failures return an error for the
// Wrapper.
func GetAudit(c *app.Context, req *pb.GetAuditRequest) (*pb.GetAuditResponse, error) {
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() {
		return &pb.GetAuditResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
		}, nil
	}

	txID := req.GetTransactionId()
	if txID == "" {
		return &pb.GetAuditResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "transaction_id is required"},
		}, nil
	}

	q := dao.NewQuery(c)

	tx, err := q.FindByID(userInfo, txID)
	if err != nil {
		if errors.Is(err, defs.ErrTransactionNotFound) {
			// missing and cross-tenant are indistinguishable
			return &pb.GetAuditResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_NOT_FOUND, Message: "transaction not found"},
			}, nil
		}
		logger.Logger(c).WithError(err).Errorf("cannot load transaction for audit detail, tx: [%s]", txID)
		return nil, err
	}

	events, _, err := q.List(userInfo, 1, auditEventChainLimit, txID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot load audit event chain, tx: [%s]", txID)
		return nil, err
	}
	if len(events) == 0 {
		// a transaction without its audit chain is unusable as a detail:
		// never fabricate an event (spec D2c-B ruling)
		return invalidEvidenceResp("transaction has no audit events"), nil
	}

	snaps, err := q.FindByTransaction(userInfo, txID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot load snapshots, tx: [%s]", txID)
		return nil, err
	}

	ev := toProtoAuditEvent(events[0]) // newest event wins
	ev.Operation = lo.ToPtr(tx.Operation)
	ev.Status = lo.ToPtr(tx.Status)
	if tx.ProxyID != nil {
		ev.ProxyId = lo.ToPtr(int64(*tx.ProxyID))
	}

	if !sanitizeEvidence([]byte(tx.EvidenceJSON)) {
		return invalidEvidenceResp("evidence_json failed the redaction guard"), nil
	}

	snapshots := make([]*pb.SnapshotMetadata, 0, len(snaps))
	for _, s := range snaps {
		snapshots = append(snapshots, toSnapshotMetadata(s))
	}
	return &pb.GetAuditResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Event:        ev,
		EvidenceJson: []byte(tx.EvidenceJSON),
		Snapshots:    snapshots,
	}, nil
}
