package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// A4c: the STOPPED branch of the safe update.
//
// A stopped rule is ABSENT from the child's applied config (StopProxy
// removes it), so updating it is a pure metadata change: the proxy_config
// row is updated IN PLACE (name/type/content, stopped stays true, the row
// id — the lease identity — is preserved) and the child client row is only
// GUARDED, never written. There is therefore NOTHING to apply: the flow
// runs the two-sided preflight gates for the candidate (server probe on the
// NEW key + client gate on the physical origin — the stopped branch has no
// owner-proof, that is an active same-key exclusive), then moves the
// transaction through preflight -> applying -> verifying as DB-ONLY
// transitions and commits. Apply/Verify are NEVER called (zero agent side
// effects by construction).
//
// A stopped rule that still lives on the NEVER-SHADOWED origin (client_id
// == origin or an empty origin link) is the unsafe first-shadow state and
// is rejected at the branch point (CONFIG_VALIDATION_FAILED) before any
// transaction exists: the stopped branch may not create a child.
//
// Crash recovery note (P1): Recovery A2 treats a stopped transaction found
// crashed in applying/verifying conservatively as rollback_failed — the
// same fail-closed path as the active branch (the lease is retained and
// the rule blocked for manual intervention). Because the stopped branch
// has zero agent side effects, the candidate was provably never applied,
// so this recovery is overly conservative but never unsafe; a dedicated
// stopped-aware recovery (failed, release) is tracked as P1.
// ---------------------------------------------------------------------------

// stoppedUpdateEvidence is the structured evidence of a committed stopped
// update: it records the branch and the explicit "zero agent contact"
// proof. It never carries config/secret material.
type stoppedUpdateEvidence struct {
	Branch         string `json:"branch"`
	AppliedToAgent bool   `json:"applied_to_agent"`
	ApplyCalls     int    `json:"apply_calls"`
	VerifyCalls    int    `json:"verify_calls"`
}

func (e stoppedUpdateEvidence) JSON() string {
	raw, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(raw)
}

// buildStoppedProxyAfterEnvelope builds the candidate's versioned typed
// envelope for the AFTER snapshot of the stopped branch: the row id never
// changes (in-place update), so the envelope carries no ProxyID and the
// DAO forces node_id = the old row id. Stopped=true is the distinguishing
// field vs the active envelope.
func buildStoppedProxyAfterEnvelope(target preflightTarget, serverID, childID, origin string, candidate v1.TypedProxyConfig) ([]byte, error) {
	typedJSON, err := candidate.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return marshalProxyEnvelope(updateProxyEnvelope{
		Name: target.ProxyName, ServerID: serverID,
		ClientID: childID, OriginClientID: origin,
		Stopped: true, Typed: typedJSON,
	})
}

// safeUpdateStoppedLocked implements the stopped branch of the safe
// update. The caller (SafeUpdateProxy) holds the (tenant, origin) lock and
// has already resolved: the origin id, the old STOPPED row (child-shaped:
// ClientID != origin, OriginClientID == origin), its physical key, the
// candidate target, the canonical request hash and the idempotency key,
// and ran the rename pre-check. Every failure after the transaction row
// exists marks it failed and releases its pending lease; the whole flow
// NEVER calls Apply, VerifyApplied or VerifyReleased.
func (uc *UpdateTransactionCoordinator) safeUpdateStoppedLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.UpdateProxyConfigRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	target preflightTarget,
	reqHash, idemKey string,
	candidate v1.TypedProxyConfig,
) (*pb.UpdateProxyConfigResponse, error) {
	d := uc.deps
	now := d.Now()

	// ---- 1. pending tx bound to the OLD row id (the lease identity);
	// same canonical reqHash/idem, redacted request (SHA only) ----
	txID := uuid.NewString()
	redacted, err := redactUpdateRequest(req, target, origin)
	if err != nil {
		return nil, err
	}
	tx := &models.ChangeTransaction{
		TransactionID:   txID,
		IdempotencyKey:  idemKey,
		RequestHash:     reqHash,
		Operation:       models.TxOperationUpdate,
		Status:          models.TxStatusPending,
		ProxyID:         lo.ToPtr(old.ID),
		RequestRedacted: redacted,
		StartedAt:       now,
	}
	if err := d.Tx.Create(c, userInfo, tx); err != nil {
		if dao.IsUniqueViolation(err) {
			if existing, ferr := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey); ferr == nil {
				return uc.replayUpdate(existing, reqHash)
			}
		}
		return nil, err
	}

	// ---- 2. reservation mode decision + acquisition (shared with the
	// active branch): SameKeyOwned / SameKeyPending / DifferentKey. ----
	newKey := dao.ReservationKey{ServerID: req.GetServerId(), Protocol: target.Protocol, RemotePort: target.RemotePort}
	resPlan, err := uc.planUpdateReservation(c, userInfo, old.ID, txID, oldKey, newKey, now)
	if err != nil {
		msg := fmt.Sprintf("port %d/%s on %s cannot be reserved: %v", target.RemotePort, target.Protocol, req.GetServerId(), err)
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeReservationConflict, msg, "")
		return invalidUpdateResp(defs.ErrorCodeReservationConflict, msg), nil
	}

	// ---- 3. child guard (tenant-scoped): the stopped rule lives on the
	// child; its applied config must NOT contain the old or the renamed
	// target (a present rule may still be live on the agent — a DB-only
	// update would silently diverge from runtime state). The child is
	// never written here. ----
	child, err := d.Query.GetClientByClientID(c, userInfo, old.ClientID)
	if err != nil {
		code := defs.ErrorCodeConfigValidationFailed
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			code = "" // internal failure: surface the raw error
		}
		msg := fmt.Sprintf("cannot resolve the stopped rule's child client %s: %v", old.ClientID, err)
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		if code == "" {
			return nil, err
		}
		return invalidUpdateResp(code, msg), nil
	}
	childEntity := child.ClientEntity
	if childEntity.ServerID != req.GetServerId() || childEntity.OriginClientID != origin {
		msg := fmt.Sprintf("stopped rule's child client %s shape (server=%s origin=%s) differs from the request (server=%s origin=%s)",
			old.ClientID, childEntity.ServerID, childEntity.OriginClientID, req.GetServerId(), origin)
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, msg, "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, msg), nil
	}
	childCfgs, err := utils.LoadProxiesFromContent(childEntity.ConfigContent)
	if err != nil {
		msg := fmt.Sprintf("cannot parse the child client config: %v", err)
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, msg, "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, msg), nil
	}
	for _, p := range childCfgs {
		if base := p.GetBaseConfig(); base != nil && (base.Name == old.Name || base.Name == target.ProxyName) {
			msg := fmt.Sprintf("stopped target %q is still present in the child's applied config — zero-Apply update refused", base.Name)
			_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, msg, "")
			return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, msg), nil
		}
	}

	// ---- 4. snapshot key once; 2 BEFORE rows (client = the REAL child
	// applied bytes, proxy = the old versioned envelope) + the AFTER
	// proxy envelope. The client-after plaintext must be byte-identical
	// to the before (the child is never changed). ----
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot load snapshot key")
		_ = uc.failUpdateTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	beforeClient, err := childEntity.MarshalJSONConfig()
	if err != nil {
		msg := fmt.Sprintf("cannot capture the child config: %v", err)
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, msg, "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, msg), nil
	}
	proxyBeforePlain, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: old.ID, Name: old.Name, ServerID: old.ServerID,
		ClientID: old.ClientID, OriginClientID: old.OriginClientID,
		Stopped: old.Stopped, Typed: old.Content,
	})
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy before envelope: %v", err), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, old.ClientID, beforeClient, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the client before snapshot")
		_ = uc.failUpdateTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(old.ID), 10), proxyBeforePlain, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the proxy before snapshot")
		_ = uc.failUpdateTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	proxyAfterPlain, err := buildStoppedProxyAfterEnvelope(target, req.GetServerId(), old.ClientID, origin, candidate)
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy after envelope: %v", err), "")
		return nil, err
	}

	// ---- 5. preflight: ALL stopped scenarios (same/different key) run
	// the server bind probe on the NEW key + the client gate on the
	// PHYSICAL origin. There is NO owner-proof Verifier — that is the
	// active same-key exclusive. Gate failures fail the tx + release. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the db-only stopped update to preflight: %v", err)
		if ftErr := uc.failUpdateTx(c, userInfo, txID, code, msg, ""); ftErr != nil {
			return nil, ftErr
		}
		return invalidUpdateResp(code, msg), nil
	}
	preReq := &pb.PreflightRequest{
		ClientId:   lo.ToPtr(origin),
		ServerId:   lo.ToPtr(req.GetServerId()),
		Protocol:   lo.ToPtr(target.Protocol),
		RemotePort: lo.ToPtr(target.RemotePort),
		LocalIp:    lo.ToPtr(target.LocalIP),
		LocalPort:  lo.ToPtr(target.LocalPort),
		ProxyName:  lo.ToPtr(target.ProxyName),
	}
	serverRes, gErr := d.Preflight.RunServerProbe(c, preReq)
	if gErr != nil {
		code := classifyAgentError(gErr)
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		msg := fmt.Sprintf("server preflight failed: %v", gErr)
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		return invalidUpdateResp(code, msg), nil
	}
	if !serverRes.Ok {
		code := serverRes.ErrorCode
		if code == "" {
			code = defs.ErrorCodePortRemoteInUse
		}
		msg := serverRes.ErrorMessage
		if msg == "" {
			msg = "remote port is not available on the server"
		}
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		return invalidUpdateResp(code, msg), nil
	}
	clientRes, gErr := d.Preflight.RunClient(c, preReq)
	if gErr != nil {
		code := classifyAgentError(gErr)
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		msg := fmt.Sprintf("client preflight failed: %v", gErr)
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		return invalidUpdateResp(code, msg), nil
	}
	if !clientRes.Ok {
		code := clientRes.ErrorCode
		if code == "" {
			code = defs.ErrorCodePortLocalNotListening
		}
		msg := clientRes.ErrorMessage
		if msg == "" {
			msg = "no local service is listening on the client"
		}
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		return invalidUpdateResp(code, msg), nil
	}

	// ---- 6. DB-ONLY applying -> verifying: the status transitions
	// happen (the audit trail records the lifecycle) but Apply /
	// VerifyApplied / VerifyReleased are NEVER invoked — zero agent side
	// effects by construction. A transition failure is a control-plane DB
	// failure: failed + CONFIG_COMMIT_FAILED + release, and the response
	// must never claim an agent was touched. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the db-only stopped update to applying: %v", err)
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		return invalidUpdateResp(code, msg), nil
	}
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the db-only stopped update to verifying: %v", err)
		_ = uc.failUpdateTx(c, userInfo, txID, code, msg, "")
		return invalidUpdateResp(code, msg), nil
	}

	// ---- 7. single-transaction commit: BranchStopped, Client/Shadow
	// nil, the in-place stopped payload, the reservation step, the
	// byte-identical client-after snapshot, the stopped proxy envelope,
	// and an audit + evidence that explicitly state "db-only; zero agent
	// apply". ----
	candidateTypedJSON, err := candidate.MarshalJSON()
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	summary := fmt.Sprintf("updated stopped proxy %q (%s port %d) on %s for %s (db-only; zero agent apply)",
		target.ProxyName, target.Protocol, target.RemotePort, req.GetServerId(), origin)
	evidence := stoppedUpdateEvidence{Branch: "stopped", AppliedToAgent: false, ApplyCalls: 0, VerifyCalls: 0}.JSON()
	evidenceRef := "change_transaction:" + txID + "#evidence_json"
	commitIn := &dao.CommitUpdateInput{
		TxID:           txID,
		OldProxyID:     old.ID,
		OldName:        old.Name,
		OldServerID:    req.GetServerId(),
		OriginClientID: origin,
		Branch:         dao.UpdateBranchStopped,
		StoppedUpdate: &dao.StoppedUpdatePayload{
			ChildClientID: old.ClientID,
			NewName:       target.ProxyName,
			NewType:       target.Protocol,
			NewServerID:   req.GetServerId(),
			NewTypedJSON:  candidateTypedJSON,
		},
		Reservation: dao.ReservationStep{
			Mode:                   resPlan.mode,
			OldKey:                 resPlan.oldKey,
			NewKey:                 resPlan.newKey,
			OldReservationExpected: resPlan.oldReservationExpected,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeClient,
			NodeID:    old.ClientID,
			Plaintext: beforeClient,
			Key:       snapKey,
		},
		ProxyAfterSnapshot: dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeProxy,
			Plaintext: proxyAfterPlain,
			Key:       snapKey,
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID,
			EventType:     models.AuditEventTypeUpdate,
			Summary:       summary,
			NodeID:        old.ClientID,
			EvidenceRef:   evidenceRef,
			Success:       true,
		},
		EvidenceJSON: evidence,
	}
	if _, err := d.Tx.CommitUpdate(c, userInfo, commitIn); err != nil {
		// The commit's single DB transaction rolled back and the stopped
		// branch never touched an agent — there is NOTHING to compensate.
		// Mark the tx failed with the stable CONFIG_COMMIT_FAILED (the
		// agent side is fine; only the control plane failed) and release
		// the current pending lease. SameKeyOwned's committed lease is
		// untouched (its transaction_id was never rewritten); Different /
		// SameKeyPending release by txID. rolled_back is NEVER used here:
		// no compensation ran.
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		if ftErr := uc.failUpdateTx(c, userInfo, txID, code, msg, ""); ftErr != nil {
			return nil, ftErr
		}
		return invalidUpdateResp(code, msg), nil
	}

	return &pb.UpdateProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}
