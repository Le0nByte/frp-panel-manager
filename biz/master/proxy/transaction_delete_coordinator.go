package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tiendc/go-deepcopy"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// B3c: the SAFE DELETE coordinator (spec §8.3 删除).
//
// SafeDeleteProxy is the transactional delete for a tcp/udp managed rule. It
// resolves the CANONICAL ORIGIN client id (child- or origin-addressed),
// acquires the (tenant, origin) keyed lock — the SAME originlock.Shared
// instance the legacy writers and SafeCreate/SafeUpdate/SafeStart/SafeStop
// lock on — and runs the WHOLE flow under it: idempotency replay (a
// committed replay succeeds even AFTER the row is gone), old-rule +
// physical-key resolution, reservation planning (SameKeyOwned / SameKeyPending
// only — delete never changes the port; the commit releases the lease BY
// OWNER / BY CURRENT TX atomically), before snapshots (saved before ANY
// Verify/Apply), branch selection, Apply/VerifyReleased, CommitDelete and
// compensation.
//
//   - fast branch (the target is ALREADY absent from the persisted client
//     config): DB-only — the coordinator verifies the port is released and
//     commits WITHOUT contacting the agent. A failed fast VerifyReleased
//     UPGRADES to the full branch (the agent still runs the rule) — the
//     verifier failure is never reported as success.
//   - full branch (the target is still in the persisted config): apply the
//     target-removed config to the PHYSICAL origin agent
//     (TargetClientID = the rule's real client row, Replace empty), verify
//     the port released, commit (row hard-delete + reservation release by
//     owner / by current tx). Every post-apply failure compensates: restore
//     the before config, verify the old rule is BACK (VerifyApplied — the
//     delete direction), then mark rolled_back and release only the current
//     pending lease; a failed compensation lands on rollback_failed and
//     RETAINS the lease. The historical SameKeyOwned committed lease (whose
//     transaction_id belongs to the ORIGINAL create) is never deleted by the
//     current tx id.
//
// Delete NEVER runs the client local-listen gate and NEVER runs the server
// bind probe (spec §8.3 删除: 快照 → 下发无目标规则的候选配置 → 验证公网端口释放
// → 数据库删除) — preflight is DB-only status transitions.
//
// The live /delete route IS switched in B4: DeleteProxyConfig is the thin
// delegate to SafeDeleteProxy / safeDeleteCoordinator.
// ---------------------------------------------------------------------------

// DeleteTxStore is the transaction persistence surface SafeDelete needs.
type DeleteTxStore interface {
	FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error
	UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	CommitDelete(c *app.Context, userInfo models.UserInfo, in *dao.CommitLifecycleInput) (uint, error)
	FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error
}

// DeleteQueryStore is the read surface SafeDelete needs (all tenant-scoped;
// the locked body re-queries every entity through this interface instead of
// caching pre-lock reads).
type DeleteQueryStore interface {
	GetOriginClientID(c *app.Context, userInfo models.UserInfo, clientID string) (string, error)
	GetClientByClientID(c *app.Context, userInfo models.UserInfo, clientID string) (*models.Client, error)
	GetProxyConfigByOriginOrClientAndName(c *app.Context, userInfo models.UserInfo, originID, serverID, name string) (*models.ProxyConfig, error)
	GetByServerFilter(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error)
}

// daoStore adapter method for the delete surface. It is ADDITIVE: the
// SafeCreate/Update/Stop/Start interface sets are untouched, so no existing
// wrapper stops compiling.
func (s *daoStore) CommitDelete(c *app.Context, userInfo models.UserInfo, in *dao.CommitLifecycleInput) (uint, error) {
	return dao.NewMutation(c).CommitDelete(userInfo, in)
}

// DeleteCoordinatorDeps carries every external dependency of the safe delete
// flow. Fields mirror StopCoordinatorDeps; HashRequest takes the CANONICAL
// RESOLVED ORIGIN id so the same logical request hashes the same before and
// after a first-shadow conversion. There is deliberately NO PreflightRunner:
// delete never runs a preflight gate (spec §8.3 lists 快照 → 下发 → 验证 →
// 提交 only).
type DeleteCoordinatorDeps struct {
	Query          DeleteQueryStore
	Tx             DeleteTxStore
	Res            ResStore
	Snap           SnapStore
	KeyProvider    KeyProvider
	Apply          ApplyService
	Verifier       Verifier
	OriginLocker   OriginLocker
	Now            func() time.Time
	Lease          time.Duration
	ApplyTimeout   time.Duration
	VerifyTimeout  time.Duration
	VerifyAttempts int
	VerifyInterval time.Duration
	HashRequest    func(req *pb.DeleteProxyConfigRequest, originID string) string
}

func defaultDeleteDeps() DeleteCoordinatorDeps {
	return DeleteCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Res:            newDAOStore(),
		Snap:           newDAOStore(),
		KeyProvider:    ConfigKeyProvider{},
		Apply:          rpcApplyService{},
		Verifier:       &rpcVerifier{Attempts: defaultVerifyAttempts, Interval: defaultVerifyInterval},
		OriginLocker:   sharedOriginLocker,
		Now:            time.Now,
		Lease:          defaultReservationLease,
		ApplyTimeout:   defaultApplyTimeout,
		VerifyTimeout:  defaultVerifyTimeout,
		VerifyAttempts: defaultVerifyAttempts,
		VerifyInterval: defaultVerifyInterval,
		HashRequest:    hashDeleteRequest,
	}
}

// DeleteTransactionCoordinator orchestrates the safe delete lifecycle.
type DeleteTransactionCoordinator struct {
	deps DeleteCoordinatorDeps
}

// NewDefaultDeleteCoordinator wires the REAL implementations (DAO, RPC,
// configured key file, the SHARED origin lock).
func NewDefaultDeleteCoordinator() *DeleteTransactionCoordinator {
	return &DeleteTransactionCoordinator{deps: defaultDeleteDeps()}
}

// safeDeleteCoordinator is the package-wide default coordinator the B4
// /delete route will delegate to. Tests swap it for a coordinator with
// injected deps and restore it afterwards.
var safeDeleteCoordinator = NewDefaultDeleteCoordinator()

// SafeDeleteProxy is the exported transactional entry used by the B4 route.
// It delegates to the package-global safeDeleteCoordinator.
func SafeDeleteProxy(c *app.Context, req *pb.DeleteProxyConfigRequest) (*pb.DeleteProxyConfigResponse, error) {
	return safeDeleteCoordinator.SafeDeleteProxy(c, req)
}

// hashDeleteRequest computes the canonical idempotency hash for a delete:
// SHA-256 over the operation discriminator ("delete"), the CANONICAL
// RESOLVED ORIGIN id (never the raw request client id — a retry through the
// generated shadow child must hash identically), the server and the rule
// name — the full semantic identity of a delete (the request carries no
// config body). idempotency_key is deliberately excluded (it is the lookup
// dimension).
func hashDeleteRequest(req *pb.DeleteProxyConfigRequest, originID string) string {
	h := sha256.New()
	h.Write([]byte("delete"))
	h.Write([]byte{0})
	h.Write([]byte(originID))
	h.Write([]byte{0})
	h.Write([]byte(req.GetServerId()))
	h.Write([]byte{0})
	h.Write([]byte(req.GetName()))
	return hex.EncodeToString(h.Sum(nil))
}

// deleteEvidence is the structured evidence of a committed delete: it
// records the branch (full = an agent apply happened, fast = db-only),
// whether the agent was touched and the exact Apply/Verify call counts. It
// never carries config/secret material.
type deleteEvidence struct {
	Branch         string `json:"branch"` // "full" | "fast"
	AppliedToAgent bool   `json:"applied_to_agent"`
	ApplyCalls     int    `json:"apply_calls"`
	VerifyCalls    int    `json:"verify_calls"`
}

func (e deleteEvidence) JSON() string {
	raw, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(raw)
}

func invalidDeleteResp(code, msg string) *pb.DeleteProxyConfigResponse {
	resp := &pb.DeleteProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
	}
	if code != "" {
		resp.ErrorCode = lo.ToPtr(code)
	}
	return resp
}

// redactDeleteRequest builds the redacted request summary stored on the
// transaction: ids and the rule name only (no config body exists on a delete
// request).
func redactDeleteRequest(req *pb.DeleteProxyConfigRequest, origin string) (string, error) {
	summary := map[string]interface{}{
		"client_id":  origin,
		"server_id":  req.GetServerId(),
		"proxy_name": req.GetName(),
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// deleteTargetMatches parses the persisted client config and locates the
// target rule: the target name must appear exactly 0 or 1 times, and a
// present entry must carry the SAME physical key as the rule row — a
// same-name rule on a different key is drift and may never be deleted. A
// malformed typed entry WITHOUT a base config fails closed (its physical key
// is unprovable — unlike the stop filter, which silently skips nil-base
// entries, delete must REJECT). The returned slice is the full-branch
// trigger (1 entry) vs the fast-branch trigger (0 entries).
func deleteTargetMatches(cli *models.ClientEntity, name string, key dao.ReservationKey) ([]v1.TypedProxyConfig, error) {
	if len(cli.ConfigContent) == 0 {
		return nil, nil
	}
	cfg, err := cli.GetConfigContent()
	if err != nil {
		return nil, fmt.Errorf("cannot parse the client config: %v", err)
	}
	for _, p := range cfg.Proxies {
		if p.GetBaseConfig() == nil {
			return nil, fmt.Errorf("client config contains a malformed proxy entry without a base config")
		}
	}
	matches := lo.Filter(cfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name == name
	})
	if len(matches) > 1 {
		return nil, fmt.Errorf("target rule %q appears %d times in the client config (must be 0 or 1)", name, len(matches))
	}
	if len(matches) == 1 {
		matchKey, kerr := updateTypedPhysicalKey(key.ServerID, matches[0])
		if kerr != nil || matchKey != key {
			return nil, fmt.Errorf("target rule %q in the client config drifted from its physical key (same name, different port/protocol)", name)
		}
	}
	return matches, nil
}

// resolveDeleteOriginAndLock resolves the ORIGIN client id for a delete
// request that may address the origin itself or one of its shadow children,
// acquires the (tenant, origin) keyed lock on the SAME injected locker
// instance the legacy writers and SafeCreate/SafeUpdate/SafeStart/SafeStop
// use, and re-resolves UNDER the lock: if the mapping changed while waiting,
// the key is released and the resolve+lock cycle retried (bounded,
// fail-closed). The caller MUST defer the unlock exactly once. The whole
// flow — idempotency replay, resolution, snapshots, verify, apply, commit
// AND compensation — runs under this lock.
func (dc *DeleteTransactionCoordinator) resolveDeleteOriginAndLock(c *app.Context, userInfo models.UserInfo, reqClientID string) (string, func(), error) {
	d := dc.deps
	if d.OriginLocker == nil {
		panic("proxy: DeleteTransactionCoordinator.OriginLocker is nil — same-origin SafeDelete serialization is disabled")
	}
	const maxOriginLockAttempts = 3
	for attempt := 0; attempt < maxOriginLockAttempts; attempt++ {
		resolved, err := d.Query.GetOriginClientID(c, userInfo, reqClientID)
		if err != nil {
			return "", nil, err
		}
		key := originLockKey(userInfo.GetTenantID(), resolved)
		d.OriginLocker.Lock(key)
		reResolved, err := d.Query.GetOriginClientID(c, userInfo, reqClientID)
		if err != nil {
			d.OriginLocker.Unlock(key)
			return "", nil, err
		}
		if reResolved != resolved {
			d.OriginLocker.Unlock(key)
			continue
		}
		return resolved, func() { d.OriginLocker.Unlock(key) }, nil
	}
	return "", nil, fmt.Errorf("origin mapping of client [%s] kept changing; giving up after %d attempts", reqClientID, maxOriginLockAttempts)
}

// SafeDeleteProxy is the transactional delete (spec §8.3). It returns
// (resp, nil) for structured business failures (the response carries the
// stable error code/message) and (nil, err) for internal errors.
func (dc *DeleteTransactionCoordinator) SafeDeleteProxy(c *app.Context, req *pb.DeleteProxyConfigRequest) (*pb.DeleteProxyConfigResponse, error) {
	d := dc.deps
	userInfo := common.GetUserInfo(c)
	if userInfo == nil {
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, "authentication required"), nil
	}

	// ---- a. request normalization (fail-closed BEFORE any lock or write):
	// client/server/name and a UUID-v4 idempotency key are mandatory. ----
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetName()) == 0 {
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, "client_id, server_id and name are required"), nil
	}
	idemKey := req.GetIdempotencyKey()
	if !isValidIdempotencyKey(idemKey) {
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed,
			"idempotency_key is required and must be a UUID v4"), nil
	}

	// ---- b. resolve the CANONICAL ORIGIN + per-(tenant, origin)
	// serialization (D1 P0): the lock key, the idempotency hash and the
	// physical agent are anchored on the resolved origin, NEVER the raw
	// request client id. The lock spans the WHOLE flow. ----
	origin, unlock, err := dc.resolveDeleteOriginAndLock(c, userInfo, req.GetClientId())
	if err != nil {
		return nil, err
	}
	defer unlock()

	// ---- c. idempotency replay (gate 7): the lookup runs on the CANONICAL
	// hash and always before any plan or DB write; a committed/terminal
	// replay never re-applies or re-verifies — it must succeed even after
	// the deleted row is gone (the replay never re-resolves the rule). ----
	reqHash := d.HashRequest(req, origin)
	existing, err := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey)
	if err == nil {
		return dc.replayDelete(existing, reqHash)
	}
	if !errors.Is(err, defs.ErrTransactionNotFound) {
		return nil, err
	}

	// ---- d. resolve the OLD rule (origin- or child-shaped) and its real
	// typed physical key; HTTP/HTTPS/STCP/XTCP rules are read-only
	// (UNSUPPORTED_PROXY_TYPE, spec §1.2.5). ----
	old, err := d.Query.GetProxyConfigByOriginOrClientAndName(c, userInfo, origin, req.GetServerId(), req.GetName())
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("proxy rule %q not found", req.GetName())), nil
		}
		if errors.Is(err, defs.ErrAmbiguousProxyTarget) {
			return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, "ambiguous proxy target: more than one row matches the rule"), nil
		}
		return nil, err
	}
	oldKey, err := stopPhysicalKeyOfRow(old)
	if err != nil {
		if errors.Is(err, defs.ErrUnsupportedProxyType) {
			return invalidDeleteResp(defs.ErrorCodeUnsupportedProxyType, err.Error()), nil
		}
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the old rule's physical key: %v", err)), nil
	}

	// ---- e. the client row the rule lives on + shape guard (the B3a stop
	// shape contract is identical for delete — reuse it, zero drift). ----
	cli, err := d.Query.GetClientByClientID(c, userInfo, old.ClientID)
	if err != nil {
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the rule's client %s: %v", old.ClientID, err)), nil
	}
	clientEntity := cli.ClientEntity
	if err := validateStopClientShape(old, clientEntity, origin, req.GetServerId()); err != nil {
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- f. parse the persisted config: target exactly 0 or 1 with a
	// matching physical key (drift rules are never deleted blindly); a
	// malformed nil-base entry fails closed. ----
	matches, err := deleteTargetMatches(clientEntity, old.Name, oldKey)
	if err != nil {
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- g. the physical origin agent must be able to receive the
	// update — a stopped/unavailable origin fails closed (AGENT_OFFLINE);
	// the fast branch may upgrade to an agent apply, so the gate is
	// mandatory for both branches. ----
	originCli, err := d.Query.GetClientByClientID(c, userInfo, origin)
	if err != nil {
		return nil, err
	}
	if originCli.ClientEntity.Stopped {
		return invalidDeleteResp(defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error()), nil
	}

	// ---- h. create the transaction (pending) FIRST, bound to the OLD row
	// id (the lease identity): every later DB write happens under a live
	// transaction row. ----
	txID := uuid.NewString()
	now := d.Now()
	redacted, err := redactDeleteRequest(req, origin)
	if err != nil {
		return nil, err
	}
	tx := &models.ChangeTransaction{
		TransactionID:   txID,
		IdempotencyKey:  idemKey,
		RequestHash:     reqHash,
		Operation:       models.TxOperationDelete,
		Status:          models.TxStatusPending,
		ProxyID:         lo.ToPtr(old.ID),
		RequestRedacted: redacted,
		StartedAt:       now,
	}
	if err := d.Tx.Create(c, userInfo, tx); err != nil {
		// concurrent request with the same key won the insert race
		if dao.IsUniqueViolation(err) {
			if existing, ferr := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey); ferr == nil {
				return dc.replayDelete(existing, reqHash)
			}
		}
		return nil, err
	}

	// ---- i. reservation: same physical key only — SameKeyOwned (an
	// existing committed lease owned by the old rule, NO TryAcquire) or
	// SameKeyPending (legacy: TryAcquire THIS tx's pending lease). Delete
	// never changes the port; failures release only the current pending.
	// The historical committed lease's transaction_id is NEVER rewritten,
	// so compensation can never delete it by txID. ----
	resPlan, err := dc.planDeleteReservation(c, userInfo, old.ID, txID, oldKey, now)
	if err != nil {
		msg := fmt.Sprintf("port %d/%s on %s cannot be reserved: %v", oldKey.RemotePort, oldKey.Protocol, req.GetServerId(), err)
		_ = dc.failDeleteTx(c, userInfo, txID, defs.ErrorCodeReservationConflict, msg, "")
		return invalidDeleteResp(defs.ErrorCodeReservationConflict, msg), nil
	}

	// ---- j. before snapshots — saved BEFORE any Verify/Apply
	// (requirement 7): client physical before (MarshalJSONConfig) + proxy
	// versioned before envelope (the rollback source; delete writes NO
	// proxy-after — the row is gone). Key/store failures fail closed. ----
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot load snapshot key")
		_ = dc.failDeleteTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	beforeClient, err := clientEntity.MarshalJSONConfig()
	if err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err), "")
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err)), nil
	}
	proxyBeforePlain, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: old.ID, Name: old.Name, ServerID: old.ServerID,
		ClientID: old.ClientID, OriginClientID: old.OriginClientID,
		Stopped: old.Stopped, Typed: old.Content,
	})
	if err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy before envelope: %v", err), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, old.ClientID, beforeClient, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the client before snapshot")
		_ = dc.failDeleteTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(old.ID), 10), proxyBeforePlain, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the proxy before snapshot")
		_ = dc.failDeleteTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}

	// ---- k. branch: the target is present in the persisted config ->
	// full (Apply + verify + commit); absent -> fast DB-only (verify +
	// commit, zero Apply, may upgrade). ----
	if len(matches) == 0 {
		return dc.safeDeleteFastLocked(c, userInfo, req, origin, old, oldKey, resPlan, clientEntity, beforeClient, snapKey, txID)
	}
	return dc.safeDeleteFullLocked(c, userInfo, req, origin, old, oldKey, resPlan, clientEntity, beforeClient, snapKey, txID)
}

// safeDeleteFastLocked implements the fast DB-only branch. The caller holds
// the (tenant, origin) lock; the target is already absent from the persisted
// config, so the coordinator only verifies the port is released and commits
// — ZERO Apply. A failed VerifyReleased means the agent still runs the rule:
// the branch UPGRADES to a real Apply (the same target-absent bytes) and
// re-verifies — the verifier failure is never reported as success. A fast
// CommitDelete failure is `failed` + CONFIG_COMMIT_FAILED (nothing was ever
// applied, so rolled_back would be a lie).
func (dc *DeleteTransactionCoordinator) safeDeleteFastLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.DeleteProxyConfigRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	clientEntity *models.ClientEntity,
	beforeClient, snapKey []byte,
	txID string,
) (*pb.DeleteProxyConfigResponse, error) {
	d := dc.deps
	verifyCalls := 0

	// Enter preflight before checking whether the public port is already
	// released. Do NOT jump directly to verifying: the transaction state
	// machine requires preflight -> applying -> verifying even for a DB-only
	// fast path. We defer those two transitions until we know whether this
	// remains fast or upgrades to a real Apply.
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the db-only delete to preflight: %v", err)
		_ = dc.failDeleteTx(c, userInfo, txID, code, msg, "")
		return invalidDeleteResp(code, msg), nil
	}

	// VerifyReleased (the port must be free already)
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyReleased(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	verifyCalls++
	if verifyErr != nil {
		// UPGRADE to full: the agent still runs the rule. The candidate is
		// the SAME persisted bytes (the target is absent), re-applied to the
		// physical origin agent so the stale handler is removed.
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		logger.Logger(c).Warnf("fast delete [%s] verify failed; upgrading to full apply: %s", txID, msg)
		if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			amsg := fmt.Sprintf("cannot advance the upgraded delete to applying: %v", err)
			_ = dc.failDeleteTx(c, userInfo, txID, code, amsg, "")
			return invalidDeleteResp(code, amsg), nil
		}
		finalRaw, buildErr := buildFrpcApplyConfig(c, clientEntity, req.GetServerId(), beforeClient)
		if buildErr != nil {
			code := defs.ErrorCodeCommitFailed
			bmsg := fmt.Sprintf("cannot build the upgrade config: %v", buildErr)
			_ = dc.failDeleteTx(c, userInfo, txID, code, bmsg, "")
			return invalidDeleteResp(code, bmsg), nil
		}
		applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
		applyCode, applyErr := d.Apply.Apply(applyCtx, c, ApplyRequest{
			PhysicalAgentID: origin,
			TargetClientID:  old.ClientID,
			ServerID:        req.GetServerId(),
			Config:          finalRaw,
		})
		cancelApply()
		if applyErr != nil {
			if applyCode == "" {
				if errors.Is(applyErr, context.DeadlineExceeded) || errors.Is(applyErr, context.Canceled) {
					applyCode = defs.ErrorCodeApplyTimeout
				} else {
					applyCode = defs.ErrorCodeApplyRejected
				}
			}
			amsg := fmt.Sprintf("apply failed: %v", applyErr)
			compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, applyCode, amsg)
			if compensated {
				return invalidDeleteResp(applyCode, amsg), nil
			}
			return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", amsg)), nil
		}
		if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			vmsg := fmt.Sprintf("cannot advance the upgraded delete to verifying: %v", err)
			compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, vmsg)
			if compensated {
				return invalidDeleteResp(code, vmsg), nil
			}
			return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", vmsg)), nil
		}
		verifyCtx2, cancelVerify2 := context.WithTimeout(c, d.VerifyTimeout)
		verifyErr = d.Verifier.VerifyReleased(verifyCtx2, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
		cancelVerify2()
		verifyCalls++
		if verifyErr != nil {
			vmsg := fmt.Sprintf("verify failed: %v", verifyErr)
			compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, defs.ErrorCodeVerifyFailed, vmsg)
			if compensated {
				return invalidDeleteResp(defs.ErrorCodeVerifyFailed, vmsg), nil
			}
			return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", vmsg)), nil
		}
		// upgraded delete DID touch the agent: evidence branch=full
		commitIn := dc.deleteCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, clientEntity, beforeClient, snapKey,
			"full", true, 1, verifyCalls)
		if _, err := d.Tx.CommitDelete(c, userInfo, commitIn); err != nil {
			code := defs.ErrorCodeCommitFailed
			cmsg := fmt.Sprintf("commit failed: %v", err)
			compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, cmsg)
			if compensated {
				return invalidDeleteResp(code, cmsg), nil
			}
			return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", cmsg)), nil
		}
		return &pb.DeleteProxyConfigResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
	}

	// fast DB-only commit: the client bytes are unchanged (candidate ==
	// before); a commit failure is failed + CONFIG_COMMIT_FAILED, NEVER
	// rolled_back (no compensation ran).
	for _, status := range []string{models.TxStatusApplying, models.TxStatusVerifying} {
		if err := d.Tx.UpdateStatus(c, userInfo, txID, status, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			msg := fmt.Sprintf("cannot advance the db-only delete to %s: %v", status, err)
			_ = dc.failDeleteTx(c, userInfo, txID, code, msg, "")
			return invalidDeleteResp(code, msg), nil
		}
	}
	commitIn := dc.deleteCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, clientEntity, beforeClient, snapKey,
		"fast", false, 0, verifyCalls)
	if _, err := d.Tx.CommitDelete(c, userInfo, commitIn); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		_ = dc.failDeleteTx(c, userInfo, txID, code, msg, "")
		return invalidDeleteResp(code, msg), nil
	}
	return &pb.DeleteProxyConfigResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
}

// safeDeleteFullLocked implements the full branch: build the candidate
// (persisted config MINUS the target), apply it to the PHYSICAL origin agent
// (TargetClientID = the rule's real client row, ReplaceClientID empty),
// verify the port released, commit (row hard-delete + reservation release by
// owner / by current tx). Every failure after the Apply point compensates
// (restore the before config, verify the old rule is back, rolled_back +
// release current pending; a failed compensation lands on rollback_failed
// and RETAINS the lease).
func (dc *DeleteTransactionCoordinator) safeDeleteFullLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.DeleteProxyConfigRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	clientEntity *models.ClientEntity,
	beforeClient, snapKey []byte,
	txID string,
) (*pb.DeleteProxyConfigResponse, error) {
	d := dc.deps

	// ---- 1. build the candidate: the persisted config minus the target.
	// The client-after entity carries the same identity; only ConfigContent
	// changes (the DAO commit writes nothing else). ----
	oldCfg, err := clientEntity.GetConfigContent()
	if err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, "cannot read the current client config", "")
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, "cannot read the current client config"), nil
	}
	oldCfg.Proxies = lo.Filter(oldCfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name != old.Name
	})
	afterEntity := &models.ClientEntity{}
	if err := deepcopy.Copy(afterEntity, clientEntity); err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, "", fmt.Sprintf("cannot copy the client entity: %v", err), "")
		return nil, err
	}
	if err := afterEntity.SetConfigContent(*oldCfg); err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	candidate, err := afterEntity.MarshalJSONConfig()
	if err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	finalRaw, err := buildFrpcApplyConfig(c, afterEntity, req.GetServerId(), candidate)
	if err != nil {
		_ = dc.failDeleteTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err), "")
		return invalidDeleteResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err)), nil
	}

	// ---- 2. preflight -> applying are DB-only transitions (delete never
	// runs a preflight gate); a transition failure BEFORE the apply point
	// is failed + CONFIG_COMMIT_FAILED + release. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the delete to preflight: %v", err)
		_ = dc.failDeleteTx(c, userInfo, txID, code, msg, "")
		return invalidDeleteResp(code, msg), nil
	}
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the delete to applying: %v", err)
		_ = dc.failDeleteTx(c, userInfo, txID, code, msg, "")
		return invalidDeleteResp(code, msg), nil
	}

	// ---- 3. synchronous apply with ACK (15s budget). The physical agent
	// is the canonical origin; the target is the rule's real client row;
	// Replace is EMPTY — delete is never a directed switch. ----
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	applyCode, applyErr := d.Apply.Apply(applyCtx, c, ApplyRequest{
		PhysicalAgentID: origin,
		TargetClientID:  old.ClientID,
		ServerID:        req.GetServerId(),
		Config:          finalRaw,
	})
	cancelApply()
	if applyErr != nil {
		if applyCode == "" {
			if errors.Is(applyErr, context.DeadlineExceeded) || errors.Is(applyErr, context.Canceled) {
				applyCode = defs.ErrorCodeApplyTimeout
			} else {
				applyCode = defs.ErrorCodeApplyRejected
			}
		}
		msg := fmt.Sprintf("apply failed: %v", applyErr)
		compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, applyCode, msg)
		if compensated {
			return invalidDeleteResp(applyCode, msg), nil
		}
		return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 4. applying -> verifying (POST-apply: a failure compensates — the
	// config IS applied). ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the applied delete to verifying: %v", err)
		compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, msg)
		if compensated {
			return invalidDeleteResp(code, msg), nil
		}
		return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 5. verify the port is released (30s budget). ----
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyReleased(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, defs.ErrorCodeVerifyFailed, msg)
		if compensated {
			return invalidDeleteResp(defs.ErrorCodeVerifyFailed, msg), nil
		}
		return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 6. single-transaction commit (CommitDelete): row hard-delete +
	// client config CAS + same-key reservation release (by owner / by
	// current tx) + client-after snapshot (NO proxy after) + delete audit +
	// evidence. A commit failure compensates (the agent side is fine; only
	// the control plane failed). ----
	commitIn := dc.deleteCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, afterEntity, candidate, snapKey,
		"full", true, 1, 1)
	if _, err := d.Tx.CommitDelete(c, userInfo, commitIn); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		compensated := dc.compensateDelete(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, msg)
		if compensated {
			return invalidDeleteResp(code, msg), nil
		}
		return invalidDeleteResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	return &pb.DeleteProxyConfigResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
}

// deleteCommitInput builds the CommitLifecycleInput for a delete commit:
// ExpectedStopped = the old row's stopped value, ExpectedClientConfigContent
// = the ORIGINAL DB bytes, ClientAfter carries only the changed
// ConfigContent, Reservation old==new (SameKeyOwned/SameKeyPending — the DAO
// releases by owner / by current tx INSIDE the commit), client-after physical
// snapshot, NO proxy-after snapshot (the row is deleted — the before
// envelope is the rollback source), a delete audit event and evidence that
// explicitly records branch / applied_to_agent / apply_calls / verify_calls.
// The DAO re-checks tenant/actor/op on every row it touches and returns the
// old proxy id even though the row is gone.
func (dc *DeleteTransactionCoordinator) deleteCommitInput(
	txID string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	origin string,
	expectedClientConfig []byte,
	clientAfter *models.ClientEntity,
	clientAfterPlain, snapKey []byte,
	branch string,
	appliedToAgent bool,
	applyCalls, verifyCalls int,
) *dao.CommitLifecycleInput {
	ev := deleteEvidence{Branch: branch, AppliedToAgent: appliedToAgent, ApplyCalls: applyCalls, VerifyCalls: verifyCalls}.JSON()
	return &dao.CommitLifecycleInput{
		TxID:                        txID,
		ProxyID:                     old.ID,
		Name:                        old.Name,
		ServerID:                    old.ServerID,
		OriginClientID:              origin,
		ExpectedStopped:             old.Stopped,
		ClientAfter:                 clientAfter,
		ExpectedClientConfigContent: expectedClientConfig,
		Reservation: dao.ReservationStep{
			Mode:                   resPlan.mode,
			OldKey:                 resPlan.oldKey,
			NewKey:                 resPlan.newKey,
			OldReservationExpected: resPlan.oldReservationExpected,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeClient,
			Plaintext: clientAfterPlain,
			Key:       snapKey,
		},
		// ProxyAfterSnapshot is nil for delete: the row is gone, the "after"
		// state is nonexistence and the before envelope is the rollback
		// source (design §4).
		AuditEvent: &models.AuditEvent{
			TransactionID: txID,
			EventType:     models.AuditEventTypeDelete,
			Summary:       fmt.Sprintf("deleted proxy %q (%s port %d) on %s for %s", old.Name, oldKey.Protocol, oldKey.RemotePort, old.ServerID, origin),
			NodeID:        old.ClientID,
			EvidenceRef:   "change_transaction:" + txID + "#evidence_json",
			Success:       true,
		},
		EvidenceJSON: ev,
	}
}

// replayDelete implements the idempotency replay semantics (gate 7):
// same key + same hash + committed -> idempotent success (no re-Apply /
// re-Verify — this must work even after the deleted row is gone); same key +
// different hash (or a different operation) -> stable IDEMPOTENCY_CONFLICT;
// terminal failure -> the original failure result; in-flight -> stable
// IDEMPOTENCY_IN_FLIGHT.
func (dc *DeleteTransactionCoordinator) replayDelete(existing *models.ChangeTransaction, reqHash string) (*pb.DeleteProxyConfigResponse, error) {
	if existing.Operation != models.TxOperationDelete || existing.RequestHash != reqHash {
		return invalidDeleteResp(defs.ErrorCodeIdempotencyConflict,
			"idempotency key was already used with a different request"), nil
	}
	switch existing.Status {
	case models.TxStatusCommitted:
		return &pb.DeleteProxyConfigResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		}, nil
	case models.TxStatusFailed, models.TxStatusRolledBack, models.TxStatusRollbackFailed:
		code := existing.ErrorCode
		if code == "" {
			code = defs.ErrorCodeConfigValidationFailed
		}
		msg := existing.ErrorMessage
		if msg == "" {
			msg = "the original request failed"
		}
		return invalidDeleteResp(code, msg), nil
	default:
		return invalidDeleteResp(defs.ErrorCodeIdempotencyInFlight,
			"a request with this idempotency key is already in progress"), nil
	}
}

// failDeleteTx marks the transaction failed (pre-apply failures) and audits
// it. The pending reservation owned by the transaction is released
// (SameKeyOwned has none — the historical committed lease is never touched).
func (dc *DeleteTransactionCoordinator) failDeleteTx(c *app.Context, userInfo models.UserInfo, txID, code, msg, evidence string) error {
	audit := &models.AuditEvent{
		TransactionID: txID,
		EventType:     models.AuditEventTypeDelete,
		Summary:       "delete failed: " + msg,
		ErrorCode:     code,
		Success:       false,
	}
	return dc.deps.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:               txID,
		Status:             models.TxStatusFailed,
		ErrorCode:          code,
		ErrorMessage:       msg,
		EvidenceJSON:       evidence,
		ReleaseReservation: true,
		AuditEvent:         audit,
	})
}

// planDeleteReservation decides the same-key reservation mode (delete never
// changes the port):
//
//	committed row owned by the old rule -> SameKeyOwned (NO TryAcquire);
//	no row (legacy world)             -> SameKeyPending (TryAcquire THIS tx);
//	any other row                     -> RESERVATION_CONFLICT.
//
// TryAcquire requires the transaction row to EXIST and be pending — the
// caller creates it first. The commit releases the lease: SameKeyOwned BY
// OWNER, SameKeyPending BY CURRENT TX — inside the single commit transaction.
func (dc *DeleteTransactionCoordinator) planDeleteReservation(c *app.Context, userInfo models.UserInfo, oldProxyID uint, txID string, key dao.ReservationKey, now time.Time) (*updateReservationPlan, error) {
	d := dc.deps
	plan := &updateReservationPlan{oldKey: key, newKey: key, sameKey: true}
	rows, err := d.Query.GetByServerFilter(c, userInfo, key.ServerID, key.Protocol, key.RemotePort)
	if err != nil {
		return nil, err
	}
	switch {
	case len(rows) == 0:
		// legacy world: this origin owns the port without any lease row;
		// acquire a fresh pending lease for THIS transaction at the key.
		if _, err := d.Res.TryAcquire(c, userInfo, key.ServerID, key.Protocol, key.RemotePort, txID, d.Lease, now); err != nil {
			return nil, err
		}
		plan.mode = dao.ReservationModeSameKeyPending
		return plan, nil
	case len(rows) == 1 && reservationCommittedOwnedBy(rows[0], oldProxyID):
		plan.mode = dao.ReservationModeSameKeyOwned
		plan.oldReservationExpected = true
		return plan, nil
	default:
		return nil, defs.ErrReservationConflict
	}
}

// compensateDelete performs the automatic rollback after the delete apply
// point (spec §8.2): re-apply the immutable before config to the SAME client
// handler on the physical origin agent, verify the old rule is held by frps
// again (VerifyApplied — the delete direction: the rule must be BACK), then
// atomically mark rolled_back and release ONLY the current tx's pending
// lease (SameKeyOwned has none; the historical committed lease's
// transaction_id is never rewritten, so it can never be deleted by txID).
// Any step failure moves the transaction to rollback_failed and RETAINS the
// current pending lease for manual intervention. Returns true when the
// rollback completed. Evidence never contains config/secret material.
func (dc *DeleteTransactionCoordinator) compensateDelete(
	c *app.Context,
	userInfo models.UserInfo,
	txID, serverID, origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	cli *models.ClientEntity,
	beforeClient []byte,
	origCode, origMsg string,
) bool {
	d := dc.deps
	logger.Logger(c).Warnf("compensating delete transaction [%s]: %s", txID, origMsg)

	// 1. applying/verifying -> rolling_back
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusRollingBack, "", ""); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot move transaction [%s] to rolling_back", txID)
		return false
	}

	rollbackAudit := func(success bool, code, msg string) *models.AuditEvent {
		summary := "compensation failed: " + msg
		if success {
			summary = "compensation succeeded: " + msg
		}
		return &models.AuditEvent{
			TransactionID: txID,
			EventType:     models.AuditEventTypeCompensation,
			Summary:       summary,
			ErrorCode:     code,
			Success:       success,
		}
	}
	failRollback := func(stage string, code, msg string) bool {
		if err := d.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
			TxID: txID, Status: models.TxStatusRollbackFailed,
			ErrorCode: defs.ErrorCodeRollbackFailed, ErrorMessage: msg,
			EvidenceJSON: compensationEvidence(stage, errors.New(msg), d.Now()),
			AuditEvent:   rollbackAudit(false, code, msg),
		}); err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot persist rollback_failed for delete transaction [%s]", txID)
		}
		return false
	}

	// 2. build the inverse apply: re-apply the before config to the SAME
	// handler (TargetClientID = the rule's client row) on the physical
	// origin agent; Replace is empty — the restore is never a switch.
	restore := ApplyRequest{
		PhysicalAgentID: origin,
		TargetClientID:  old.ClientID,
		ServerID:        serverID,
	}
	beforeFinal, err := buildFrpcApplyConfig(c, cli, serverID, beforeClient)
	if err != nil {
		return failRollback("compensate-build", defs.ErrorCodeRollbackFailed, fmt.Sprintf("compensation build failed: %v", err))
	}
	restore.Config = beforeFinal

	// 3. re-apply the before snapshot (synchronous ACK, ApplyTimeout budget)
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	_, applyErr := d.Apply.Apply(applyCtx, c, restore)
	cancelApply()
	if applyErr != nil {
		return failRollback("compensate-apply", defs.ErrorCodeRollbackFailed, fmt.Sprintf("compensation apply failed: %v", applyErr))
	}

	// 4. verify the old rule is held by our embedded frps again (the rule
	// must be BACK — the delete direction)
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyApplied(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		return failRollback("compensate-verify-old", defs.ErrorCodeVerifyFailed, fmt.Sprintf("compensation verify old state failed: %v", verifyErr))
	}

	// 5. atomically mark rolled_back + release ONLY the current tx's pending
	// lease + audit
	if err := d.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:               txID,
		Status:             models.TxStatusRolledBack,
		ErrorCode:          origCode,
		ErrorMessage:       origMsg,
		ReleaseReservation: true,
		EvidenceJSON:       compensationEvidence("compensate-done", nil, d.Now()),
		AuditEvent:         rollbackAudit(true, "", "before config restored"),
	}); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot finalize compensation for transaction [%s]", txID)
		return false
	}
	return true
}
