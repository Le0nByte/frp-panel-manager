package audit

import (
	"github.com/VaalaCat/frp-panel/biz/master/proxy"
	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/google/uuid"
	"github.com/samber/lo"
)

// Rollback is the live /rollback route entry (spec §7.1 POST
// /api/v1/audit/rollback). It is a THIN BOUNDARY DELEGATE: it fail-closed
// checks the authenticated user and the two mandatory request fields
// (transaction_id, idempotency_key as a UUID v4), then hands the request
// to the transactional rollback coordinator (proxy.RollbackProxy). The
// handler itself never reads or writes the DB, never acquires the
// canonical (tenant, origin) lock and never sends agent RPCs — idempotency
// replay, the committed-only source gate, snapshot decryption, plan
// building, apply/verify, commit and compensation all run inside the
// coordinator. Business rejections return (resp, nil) with Status INVALID
// and the stable CONFIG_VALIDATION_FAILED code in the dedicated error_code
// field; every coordinator outcome (structured or internal error) passes
// through unchanged.
func Rollback(c *app.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() {
		return invalidRollbackReq("invalid user"), nil
	}
	if len(req.GetTransactionId()) == 0 {
		return invalidRollbackReq("transaction_id is required"), nil
	}
	if !isValidV4UUID(req.GetIdempotencyKey()) {
		return invalidRollbackReq("idempotency_key must be a UUID v4"), nil
	}
	// real transactional delegation: the coordinator re-validates the
	// source (committed-only), replays idempotency, resolves and locks the
	// canonical origin, applies the source-before config and commits.
	return proxy.RollbackProxy(c, req)
}

// invalidRollbackReq builds the handler-level business rejection: Status
// INVALID with the stable structured error code (spec §7.3) in the
// dedicated error_code field. Status.Message stays human-readable and
// carries no request material.
func invalidRollbackReq(msg string) *pb.RollbackResponse {
	return &pb.RollbackResponse{
		Status:    &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
		ErrorCode: lo.ToPtr(defs.ErrorCodeConfigValidationFailed),
	}
}

// isValidV4UUID enforces the fail-closed idempotency contract (gate 7):
// the key must be present AND a UUID v4. An empty key would occupy the
// unique idempotency index and collide across unrelated requests; a
// non-v4 UUID is a client bug — reject before any coordinator work.
func isValidV4UUID(key string) bool {
	if key == "" {
		return false
	}
	u, err := uuid.Parse(key)
	if err != nil {
		return false
	}
	return u.Version() == 4
}
