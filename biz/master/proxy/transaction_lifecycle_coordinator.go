package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
// B3a: the SAFE STOP coordinator (spec §8.3 停止).
//
// SafeStopProxy is the transactional stop for a tcp/udp managed rule. It
// resolves the CANONICAL ORIGIN client id (child- or origin-addressed),
// acquires the (tenant, origin) keyed lock — the SAME originlock.Shared
// instance the legacy writers and SafeCreate/SafeUpdate lock on — and runs
// the WHOLE flow under it: idempotency replay, old-rule + physical-key
// resolution, reservation planning (SameKeyOwned / SameKeyPending only —
// stop never changes the port), before snapshots (saved before ANY
// Verify/Apply), branch selection, Apply/VerifyReleased, CommitStop and
// compensation.
//
//   - fast branch (the target is already absent from the persisted client
//     config): DB-only — the coordinator verifies the port is released and
//     commits WITHOUT contacting the agent. A failed fast VerifyReleased
//     UPGRADES to the full branch (the agent still runs the rule) — the
//     verifier failure is never reported as success.
//   - full branch (the target is still in the persisted config): apply the
//     target-removed config to the PHYSICAL origin agent
//     (TargetClientID = the rule's real client row, Replace empty), verify
//     the port released, commit. Every post-apply failure compensates:
//     restore the before config, verify the old rule is back, then mark
//     rolled_back and release only the current pending lease; a failed
//     compensation lands on rollback_failed and RETAINS the lease.
//
// Stop NEVER runs the client local-listen gate (a stopped rule does not
// require the local service to still listen) and never calls RunClient.
//
// The live /stop route IS switched in B4: StopProxy is now the thin
// delegate to this transactional coordinator.
// ---------------------------------------------------------------------------

// StopTxStore is the transaction persistence surface SafeStop needs.
type StopTxStore interface {
	FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error
	UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	CommitStop(c *app.Context, userInfo models.UserInfo, in *dao.CommitLifecycleInput) (uint, error)
	FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error
}

// StopQueryStore is the read surface SafeStop needs (all tenant-scoped; the
// locked body re-queries every entity through this interface instead of
// caching pre-lock reads).
type StopQueryStore interface {
	GetOriginClientID(c *app.Context, userInfo models.UserInfo, clientID string) (string, error)
	GetClientByClientID(c *app.Context, userInfo models.UserInfo, clientID string) (*models.Client, error)
	GetProxyConfigByOriginOrClientAndName(c *app.Context, userInfo models.UserInfo, originID, serverID, name string) (*models.ProxyConfig, error)
	GetByServerFilter(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error)
}

// daoStore adapter methods for the stop surface. They are ADDITIVE: the
// SafeCreate/Update interface sets are untouched, so no existing wrapper
// stops compiling.
func (s *daoStore) CommitStop(c *app.Context, userInfo models.UserInfo, in *dao.CommitLifecycleInput) (uint, error) {
	return dao.NewMutation(c).CommitStop(userInfo, in)
}

// StopCoordinatorDeps carries every external dependency of the safe stop
// flow. Fields mirror CoordinatorDeps (SafeCreate); HashRequest takes the
// CANONICAL RESOLVED ORIGIN id so the same logical request hashes the same
// before and after a first-shadow conversion.
type StopCoordinatorDeps struct {
	Query          StopQueryStore
	Tx             StopTxStore
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
	HashRequest    func(req *pb.StopProxyRequest, originID string) string
}

func defaultStopDeps() StopCoordinatorDeps {
	return StopCoordinatorDeps{
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
		HashRequest:    hashStopRequest,
	}
}

// StopTransactionCoordinator orchestrates the safe stop lifecycle.
type StopTransactionCoordinator struct {
	deps StopCoordinatorDeps
}

// NewDefaultStopCoordinator wires the REAL implementations (DAO, RPC,
// configured key file, the SHARED origin lock).
func NewDefaultStopCoordinator() *StopTransactionCoordinator {
	return &StopTransactionCoordinator{deps: defaultStopDeps()}
}

// safeStopCoordinator is the package-wide default coordinator the B4 /stop
// route will delegate to. Tests swap it for a coordinator with injected
// deps and restore it afterwards.
var safeStopCoordinator = NewDefaultStopCoordinator()

// SafeStopProxy is the exported transactional entry used by the B4 route.
// It delegates to the package-global safeStopCoordinator.
func SafeStopProxy(c *app.Context, req *pb.StopProxyRequest) (*pb.StopProxyResponse, error) {
	return safeStopCoordinator.SafeStopProxy(c, req)
}

// hashStopRequest computes the canonical idempotency hash for a stop:
// SHA-256 over the operation discriminator ("stop"), the CANONICAL RESOLVED
// ORIGIN id (never the raw request client id — a retry through the
// generated shadow child must hash identically), the server and the rule
// name — the full semantic identity of a stop (the request carries no
// config body). idempotency_key is deliberately excluded (it is the lookup
// dimension).
func hashStopRequest(req *pb.StopProxyRequest, originID string) string {
	h := sha256.New()
	h.Write([]byte("stop"))
	h.Write([]byte{0})
	h.Write([]byte(originID))
	h.Write([]byte{0})
	h.Write([]byte(req.GetServerId()))
	h.Write([]byte{0})
	h.Write([]byte(req.GetName()))
	return hex.EncodeToString(h.Sum(nil))
}

// stopEvidence is the structured evidence of a committed stop: it records
// the branch (full = an agent apply happened, fast = db-only), whether the
// agent was touched and the exact Apply/Verify call counts. It never
// carries config/secret material.
type stopEvidence struct {
	Branch         string `json:"branch"` // "full" | "fast"
	AppliedToAgent bool   `json:"applied_to_agent"`
	ApplyCalls     int    `json:"apply_calls"`
	VerifyCalls    int    `json:"verify_calls"`
}

func (e stopEvidence) JSON() string {
	raw, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(raw)
}

func invalidStopResp(code, msg string) *pb.StopProxyResponse {
	resp := &pb.StopProxyResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
	}
	if code != "" {
		resp.ErrorCode = lo.ToPtr(code)
	}
	return resp
}

// redactStopRequest builds the redacted request summary stored on the
// transaction: ids and the rule name only (no config body exists on a stop
// request).
func redactStopRequest(req *pb.StopProxyRequest, origin string) (string, error) {
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

// stopPhysicalKeyOfRow parses a proxy_config row's typed content into its
// physical reservation key. Only tcp/udp rules are managed (spec §1.2.5:
// HTTP/HTTPS/STCP/XTCP are read-only) — a non-tcp/udp type fails with
// defs.ErrUnsupportedProxyType; an unparseable row fails closed.
func stopPhysicalKeyOfRow(row *models.ProxyConfig) (dao.ReservationKey, error) {
	typed, err := row.GetTypedProxyConfig()
	if err != nil {
		return dao.ReservationKey{}, fmt.Errorf("old rule content is not a parseable typed config: %v", err)
	}
	if base := typed.GetBaseConfig(); base != nil {
		switch strings.ToLower(strings.TrimSpace(base.Type)) {
		case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		default:
			return dao.ReservationKey{}, fmt.Errorf("%w: %s", defs.ErrUnsupportedProxyType, base.Type)
		}
	}
	return updateTypedPhysicalKey(row.ServerID, typed)
}

// validateStopClientShape enforces the rule/client shape contract: the rule
// row must live on the canonical origin (origin-shaped legacy) or on a
// child that matches the request's server + origin (child-shaped), and the
// client row must agree with the row's shape (spec §6 / P0: no cross-origin
// drift).
func validateStopClientShape(old *models.ProxyConfig, cli *models.ClientEntity, origin, serverID string) error {
	if old.OriginClientID == "" {
		// origin-shaped: the never-shadowed origin hosts the row directly
		if old.ClientID != origin {
			return fmt.Errorf("origin-shaped rule %q lives on client %s, not the origin %s", old.Name, old.ClientID, origin)
		}
		if cli.OriginClientID != "" {
			return fmt.Errorf("origin client %s has an origin link %q", cli.ClientID, cli.OriginClientID)
		}
		if cli.ServerID != "" && cli.ServerID != serverID {
			return fmt.Errorf("origin client %s server %q differs from the rule server %q", cli.ClientID, cli.ServerID, serverID)
		}
		return nil
	}
	// child-shaped: the origin link must match the canonical origin
	if old.OriginClientID != origin {
		return fmt.Errorf("rule %q origin %q differs from the canonical origin %q", old.Name, old.OriginClientID, origin)
	}
	if cli.OriginClientID != origin || cli.ServerID != serverID {
		return fmt.Errorf("child client %s shape (server=%s origin=%s) differs from the request (server=%s origin=%s)",
			cli.ClientID, cli.ServerID, cli.OriginClientID, serverID, origin)
	}
	return nil
}

// stopTargetMatches parses the persisted client config and locates the
// target rule: the target name must appear exactly 0 or 1 times, and a
// present entry must carry the SAME physical key as the rule row — a
// same-name rule on a different key is drift and may never be deleted. The
// returned slice is the full-branch trigger (1 entry) vs the fast-branch
// trigger (0 entries).
func stopTargetMatches(cli *models.ClientEntity, name string, key dao.ReservationKey) ([]v1.TypedProxyConfig, error) {
	if len(cli.ConfigContent) == 0 {
		return nil, nil
	}
	cfg, err := cli.GetConfigContent()
	if err != nil {
		return nil, fmt.Errorf("cannot parse the client config: %v", err)
	}
	matches := lo.Filter(cfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		base := p.GetBaseConfig()
		return base != nil && base.Name == name
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

// resolveStopOriginAndLock resolves the ORIGIN client id for a stop request
// that may address the origin itself or one of its shadow children,
// acquires the (tenant, origin) keyed lock on the SAME injected locker
// instance the legacy writers and SafeCreate/SafeUpdate use, and
// re-resolves UNDER the lock: if the mapping changed while waiting, the key
// is released and the resolve+lock cycle retried (bounded, fail-closed).
// The caller MUST defer the unlock exactly once. The whole flow —
// idempotency replay, resolution, snapshots, verify, apply, commit AND
// compensation — runs under this lock.
func (sc *StopTransactionCoordinator) resolveStopOriginAndLock(c *app.Context, userInfo models.UserInfo, reqClientID string) (string, func(), error) {
	d := sc.deps
	if d.OriginLocker == nil {
		panic("proxy: StopTransactionCoordinator.OriginLocker is nil — same-origin SafeStop serialization is disabled")
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

// SafeStopProxy is the transactional stop (spec §8.3). It returns (resp,
// nil) for structured business failures (the response carries the stable
// error code/message) and (nil, err) for internal errors.
func (sc *StopTransactionCoordinator) SafeStopProxy(c *app.Context, req *pb.StopProxyRequest) (*pb.StopProxyResponse, error) {
	d := sc.deps
	userInfo := common.GetUserInfo(c)
	if userInfo == nil {
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, "authentication required"), nil
	}

	// ---- a. request normalization (fail-closed BEFORE any lock or write):
	// client/server/name and a UUID-v4 idempotency key are mandatory. ----
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetName()) == 0 {
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, "client_id, server_id and name are required"), nil
	}
	idemKey := req.GetIdempotencyKey()
	if !isValidIdempotencyKey(idemKey) {
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed,
			"idempotency_key is required and must be a UUID v4"), nil
	}

	// ---- b. resolve the CANONICAL ORIGIN + per-(tenant, origin)
	// serialization (D1 P0): the lock key, the idempotency hash and the
	// physical agent are anchored on the resolved origin, NEVER the raw
	// request client id. The lock spans the WHOLE flow. ----
	origin, unlock, err := sc.resolveStopOriginAndLock(c, userInfo, req.GetClientId())
	if err != nil {
		return nil, err
	}
	defer unlock()

	// ---- c. idempotency replay (gate 7): the lookup runs on the CANONICAL
	// hash and always before any plan or DB write; a committed/terminal
	// replay never re-applies or re-verifies. ----
	reqHash := d.HashRequest(req, origin)
	existing, err := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey)
	if err == nil {
		return sc.replayStop(existing, reqHash)
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
			return invalidStopResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("proxy rule %q not found", req.GetName())), nil
		}
		if errors.Is(err, defs.ErrAmbiguousProxyTarget) {
			return invalidStopResp(defs.ErrorCodeConfigValidationFailed, "ambiguous proxy target: more than one row matches the rule"), nil
		}
		return nil, err
	}
	oldKey, err := stopPhysicalKeyOfRow(old)
	if err != nil {
		if errors.Is(err, defs.ErrUnsupportedProxyType) {
			return invalidStopResp(defs.ErrorCodeUnsupportedProxyType, err.Error()), nil
		}
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the old rule's physical key: %v", err)), nil
	}

	// ---- e. the client row the rule lives on + shape guard. ----
	cli, err := d.Query.GetClientByClientID(c, userInfo, old.ClientID)
	if err != nil {
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the rule's client %s: %v", old.ClientID, err)), nil
	}
	clientEntity := cli.ClientEntity
	if err := validateStopClientShape(old, clientEntity, origin, req.GetServerId()); err != nil {
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- f. parse the persisted config: target exactly 0 or 1 with a
	// matching physical key (drift rules are never deleted). ----
	matches, err := stopTargetMatches(clientEntity, old.Name, oldKey)
	if err != nil {
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- g. the physical origin agent must be able to receive the
	// update — a stopped/unavailable origin fails closed (AGENT_OFFLINE). ----
	originCli, err := d.Query.GetClientByClientID(c, userInfo, origin)
	if err != nil {
		return nil, err
	}
	if originCli.ClientEntity.Stopped {
		return invalidStopResp(defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error()), nil
	}

	// ---- h. create the transaction (pending) FIRST, bound to the OLD row
	// id (the lease identity): every later DB write happens under a live
	// transaction row. ----
	txID := uuid.NewString()
	now := d.Now()
	redacted, err := redactStopRequest(req, origin)
	if err != nil {
		return nil, err
	}
	tx := &models.ChangeTransaction{
		TransactionID:   txID,
		IdempotencyKey:  idemKey,
		RequestHash:     reqHash,
		Operation:       models.TxOperationStop,
		Status:          models.TxStatusPending,
		ProxyID:         lo.ToPtr(old.ID),
		RequestRedacted: redacted,
		StartedAt:       now,
	}
	if err := d.Tx.Create(c, userInfo, tx); err != nil {
		// concurrent request with the same key won the insert race
		if dao.IsUniqueViolation(err) {
			if existing, ferr := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey); ferr == nil {
				return sc.replayStop(existing, reqHash)
			}
		}
		return nil, err
	}

	// ---- i. reservation: same physical key only — SameKeyOwned (an
	// existing committed lease owned by the old rule, NO TryAcquire) or
	// SameKeyPending (legacy: TryAcquire THIS tx's pending lease). Stop
	// never changes the port; failures release only the current pending. ----
	resPlan, err := sc.planStopReservation(c, userInfo, old.ID, txID, oldKey, now)
	if err != nil {
		msg := fmt.Sprintf("port %d/%s on %s cannot be reserved: %v", oldKey.RemotePort, oldKey.Protocol, req.GetServerId(), err)
		_ = sc.failStopTx(c, userInfo, txID, defs.ErrorCodeReservationConflict, msg, "")
		return invalidStopResp(defs.ErrorCodeReservationConflict, msg), nil
	}

	// ---- j. before snapshots — saved BEFORE any Verify/Apply (requirement
	// 7): client physical before (MarshalJSONConfig) + proxy versioned
	// before envelope. Key/store failures fail closed. ----
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot load snapshot key")
		_ = sc.failStopTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	beforeClient, err := clientEntity.MarshalJSONConfig()
	if err != nil {
		_ = sc.failStopTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err), "")
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err)), nil
	}
	proxyBeforePlain, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: old.ID, Name: old.Name, ServerID: old.ServerID,
		ClientID: old.ClientID, OriginClientID: old.OriginClientID,
		Stopped: old.Stopped, Typed: old.Content,
	})
	if err != nil {
		_ = sc.failStopTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy before envelope: %v", err), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, old.ClientID, beforeClient, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the client before snapshot")
		_ = sc.failStopTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(old.ID), 10), proxyBeforePlain, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the proxy before snapshot")
		_ = sc.failStopTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	// the stopped proxy after envelope: the rule content is unchanged, only
	// Stopped flips to true.
	proxyAfterPlain, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: old.ID, Name: old.Name, ServerID: old.ServerID,
		ClientID: old.ClientID, OriginClientID: old.OriginClientID,
		Stopped: true, Typed: old.Content,
	})
	if err != nil {
		_ = sc.failStopTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy after envelope: %v", err), "")
		return nil, err
	}

	// ---- k. branch: the target is absent from the persisted config ->
	// fast DB-only (verify + commit, no agent contact); present -> full
	// (Apply + verify + commit). ----
	if len(matches) == 0 {
		return sc.safeStopFastLocked(c, userInfo, req, origin, old, oldKey, resPlan, clientEntity, beforeClient, proxyAfterPlain, snapKey, txID)
	}
	return sc.safeStopFullLocked(c, userInfo, req, origin, old, oldKey, resPlan, clientEntity, beforeClient, proxyAfterPlain, snapKey, txID)
}

// safeStopFastLocked implements the fast DB-only branch. The caller holds
// the (tenant, origin) lock; the target is already absent from the
// persisted config, so the coordinator only verifies the port is released
// and commits — ZERO Apply. A failed VerifyReleased means the agent still
// runs the rule: the branch UPGRADES to a real Apply (the same
// target-removed bytes) and re-verifies — the verifier failure is never
// reported as success. A fast CommitStop failure is `failed` +
// CONFIG_COMMIT_FAILED (nothing was ever applied, so rolled_back would be
// a lie).
func (sc *StopTransactionCoordinator) safeStopFastLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.StopProxyRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	clientEntity *models.ClientEntity,
	beforeClient, proxyAfterPlain, snapKey []byte,
	txID string,
) (*pb.StopProxyResponse, error) {
	d := sc.deps
	verifyCalls := 0

	// Enter preflight before checking whether the public port is already
	// released. Do NOT jump directly to verifying: the transaction state
	// machine requires preflight -> applying -> verifying even for a DB-only
	// fast path. We defer those two transitions until we know whether this
	// remains fast or upgrades to a real Apply.
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the db-only stop to preflight: %v", err)
		_ = sc.failStopTx(c, userInfo, txID, code, msg, "")
		return invalidStopResp(code, msg), nil
	}

	// VerifyReleased (the port must be free again)
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyReleased(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	verifyCalls++
	if verifyErr != nil {
		// UPGRADE to full: the agent still runs the rule. The candidate is
		// the SAME persisted bytes (the target is absent), re-applied to the
		// physical origin agent so the stale handler is removed.
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		logger.Logger(c).Warnf("fast stop [%s] verify failed; upgrading to full apply: %s", txID, msg)
		if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			amsg := fmt.Sprintf("cannot advance the upgraded stop to applying: %v", err)
			_ = sc.failStopTx(c, userInfo, txID, code, amsg, "")
			return invalidStopResp(code, amsg), nil
		}
		finalRaw, buildErr := buildFrpcApplyConfig(c, clientEntity, req.GetServerId(), beforeClient)
		if buildErr != nil {
			code := defs.ErrorCodeCommitFailed
			bmsg := fmt.Sprintf("cannot build the upgrade config: %v", buildErr)
			_ = sc.failStopTx(c, userInfo, txID, code, bmsg, "")
			return invalidStopResp(code, bmsg), nil
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
			compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, applyCode, amsg)
			if compensated {
				return invalidStopResp(applyCode, amsg), nil
			}
			return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", amsg)), nil
		}
		if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			vmsg := fmt.Sprintf("cannot advance the upgraded stop to verifying: %v", err)
			compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, vmsg)
			if compensated {
				return invalidStopResp(code, vmsg), nil
			}
			return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", vmsg)), nil
		}
		verifyCtx2, cancelVerify2 := context.WithTimeout(c, d.VerifyTimeout)
		verifyErr = d.Verifier.VerifyReleased(verifyCtx2, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
		cancelVerify2()
		verifyCalls++
		if verifyErr != nil {
			vmsg := fmt.Sprintf("verify failed: %v", verifyErr)
			compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, defs.ErrorCodeVerifyFailed, vmsg)
			if compensated {
				return invalidStopResp(defs.ErrorCodeVerifyFailed, vmsg), nil
			}
			return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", vmsg)), nil
		}
		// upgraded stop DID touch the agent: evidence branch=full
		commitIn := sc.stopCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, clientEntity, beforeClient, proxyAfterPlain, snapKey,
			"full", true, 1, verifyCalls)
		if _, err := d.Tx.CommitStop(c, userInfo, commitIn); err != nil {
			code := defs.ErrorCodeCommitFailed
			cmsg := fmt.Sprintf("commit failed: %v", err)
			compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, cmsg)
			if compensated {
				return invalidStopResp(code, cmsg), nil
			}
			return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", cmsg)), nil
		}
		return &pb.StopProxyResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
	}

	// fast DB-only commit: the client bytes are unchanged (candidate ==
	// before); a commit failure is failed + CONFIG_COMMIT_FAILED, NEVER
	// rolled_back (no compensation ran).
	for _, status := range []string{models.TxStatusApplying, models.TxStatusVerifying} {
		if err := d.Tx.UpdateStatus(c, userInfo, txID, status, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			msg := fmt.Sprintf("cannot advance the db-only stop to %s: %v", status, err)
			_ = sc.failStopTx(c, userInfo, txID, code, msg, "")
			return invalidStopResp(code, msg), nil
		}
	}
	commitIn := sc.stopCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, clientEntity, beforeClient, proxyAfterPlain, snapKey,
		"fast", false, 0, verifyCalls)
	if _, err := d.Tx.CommitStop(c, userInfo, commitIn); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		_ = sc.failStopTx(c, userInfo, txID, code, msg, "")
		return invalidStopResp(code, msg), nil
	}
	return &pb.StopProxyResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
}

// safeStopFullLocked implements the full branch: apply the target-removed
// config to the PHYSICAL origin agent (TargetClientID = the rule's real
// client row, ReplaceClientID empty), verify the port released, commit.
// Every failure after the Apply point compensates (restore the before
// config, verify the old rule is back, rolled_back + release current
// pending; a failed compensation lands on rollback_failed and RETAINS the
// lease).
func (sc *StopTransactionCoordinator) safeStopFullLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.StopProxyRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	clientEntity *models.ClientEntity,
	beforeClient, proxyAfterPlain, snapKey []byte,
	txID string,
) (*pb.StopProxyResponse, error) {
	d := sc.deps

	// ---- 1. build the candidate: the persisted config minus the target.
	// The client-after entity carries the same identity; only ConfigContent
	// changes (the DAO commit writes nothing else). ----
	oldCfg, err := clientEntity.GetConfigContent()
	if err != nil {
		_ = sc.failStopTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, "cannot read the current client config", "")
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, "cannot read the current client config"), nil
	}
	oldCfg.Proxies = lo.Filter(oldCfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name != old.Name
	})
	afterEntity := &models.ClientEntity{}
	if err := deepcopy.Copy(afterEntity, clientEntity); err != nil {
		_ = sc.failStopTx(c, userInfo, txID, "", fmt.Sprintf("cannot copy the client entity: %v", err), "")
		return nil, err
	}
	if err := afterEntity.SetConfigContent(*oldCfg); err != nil {
		_ = sc.failStopTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	candidate, err := afterEntity.MarshalJSONConfig()
	if err != nil {
		_ = sc.failStopTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	finalRaw, err := buildFrpcApplyConfig(c, afterEntity, req.GetServerId(), candidate)
	if err != nil {
		_ = sc.failStopTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err), "")
		return invalidStopResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err)), nil
	}

	// ---- 2. preflight -> applying are DB-only transitions (stop never
	// runs the client local-listen gate); a transition failure BEFORE the
	// apply point is failed + CONFIG_COMMIT_FAILED + release. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the stop to preflight: %v", err)
		_ = sc.failStopTx(c, userInfo, txID, code, msg, "")
		return invalidStopResp(code, msg), nil
	}
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the stop to applying: %v", err)
		_ = sc.failStopTx(c, userInfo, txID, code, msg, "")
		return invalidStopResp(code, msg), nil
	}

	// ---- 3. synchronous apply with ACK (15s budget). The physical agent
	// is the canonical origin; the target is the rule's real client row;
	// Replace is EMPTY — stop is never a directed switch. ----
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
		compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, applyCode, msg)
		if compensated {
			return invalidStopResp(applyCode, msg), nil
		}
		return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 4. applying -> verifying (POST-apply: a failure compensates — the
	// config IS applied). ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the applied stop to verifying: %v", err)
		compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, msg)
		if compensated {
			return invalidStopResp(code, msg), nil
		}
		return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 5. verify the port is released (30s budget). ----
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyReleased(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, defs.ErrorCodeVerifyFailed, msg)
		if compensated {
			return invalidStopResp(defs.ErrorCodeVerifyFailed, msg), nil
		}
		return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 6. single-transaction commit (CommitStop): stopped flip +
	// client config CAS + same-key reservation migration + after snapshots +
	// stop audit + evidence. A commit failure compensates (the agent side
	// is fine; only the control plane failed). ----
	commitIn := sc.stopCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, afterEntity, candidate, proxyAfterPlain, snapKey,
		"full", true, 1, 1)
	if _, err := d.Tx.CommitStop(c, userInfo, commitIn); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		compensated := sc.compensateStop(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, msg)
		if compensated {
			return invalidStopResp(code, msg), nil
		}
		return invalidStopResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	return &pb.StopProxyResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
}

// stopCommitInput builds the CommitLifecycleInput for a stop commit:
// ExpectedStopped = the old row's stopped value, ExpectedClientConfigContent
// = the ORIGINAL DB bytes, ClientAfter carries only the changed
// ConfigContent, Reservation old==new (SameKeyOwned/SameKeyPending),
// client-after physical snapshot, stopped proxy after envelope, a stop
// audit event and evidence that explicitly records branch / applied_to_agent
// / apply_calls / verify_calls. The DAO re-checks tenant/actor/op on every
// row it touches.
func (sc *StopTransactionCoordinator) stopCommitInput(
	txID string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	origin string,
	expectedClientConfig []byte,
	clientAfter *models.ClientEntity,
	clientAfterPlain, proxyAfterPlain, snapKey []byte,
	branch string,
	appliedToAgent bool,
	applyCalls, verifyCalls int,
) *dao.CommitLifecycleInput {
	ev := stopEvidence{Branch: branch, AppliedToAgent: appliedToAgent, ApplyCalls: applyCalls, VerifyCalls: verifyCalls}.JSON()
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
		ProxyAfterSnapshot: &dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeProxy,
			Plaintext: proxyAfterPlain,
			Key:       snapKey,
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID,
			EventType:     models.AuditEventTypeStop,
			Summary:       fmt.Sprintf("stopped proxy %q (%s port %d) on %s for %s", old.Name, oldKey.Protocol, oldKey.RemotePort, old.ServerID, origin),
			NodeID:        old.ClientID,
			EvidenceRef:   "change_transaction:" + txID + "#evidence_json",
			Success:       true,
		},
		EvidenceJSON: ev,
	}
}

// replayStop implements the idempotency replay semantics (gate 7):
// same key + same hash + committed -> idempotent success (no re-Apply /
// re-Verify); same key + different hash (or a different operation) ->
// stable IDEMPOTENCY_CONFLICT; terminal failure -> the original failure
// result; in-flight -> stable IDEMPOTENCY_IN_FLIGHT.
func (sc *StopTransactionCoordinator) replayStop(existing *models.ChangeTransaction, reqHash string) (*pb.StopProxyResponse, error) {
	if existing.Operation != models.TxOperationStop || existing.RequestHash != reqHash {
		return invalidStopResp(defs.ErrorCodeIdempotencyConflict,
			"idempotency key was already used with a different request"), nil
	}
	switch existing.Status {
	case models.TxStatusCommitted:
		return &pb.StopProxyResponse{
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
		return invalidStopResp(code, msg), nil
	default:
		return invalidStopResp(defs.ErrorCodeIdempotencyInFlight,
			"a request with this idempotency key is already in progress"), nil
	}
}

// failStopTx marks the transaction failed (pre-apply failures) and audits
// it. The pending reservation owned by the transaction is released
// (SameKeyOwned has none — the historical committed lease is never
// touched).
func (sc *StopTransactionCoordinator) failStopTx(c *app.Context, userInfo models.UserInfo, txID, code, msg, evidence string) error {
	audit := &models.AuditEvent{
		TransactionID: txID,
		EventType:     models.AuditEventTypeStop,
		Summary:       "stop failed: " + msg,
		ErrorCode:     code,
		Success:       false,
	}
	return sc.deps.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:               txID,
		Status:             models.TxStatusFailed,
		ErrorCode:          code,
		ErrorMessage:       msg,
		EvidenceJSON:       evidence,
		ReleaseReservation: true,
		AuditEvent:         audit,
	})
}

// planStopReservation decides the same-key reservation mode (stop never
// changes the port):
//
//	committed row owned by the old rule -> SameKeyOwned (NO TryAcquire);
//	no row (legacy world)             -> SameKeyPending (TryAcquire THIS tx);
//	any other row                     -> RESERVATION_CONFLICT.
//
// TryAcquire requires the transaction row to EXIST and be pending — the
// caller creates it first.
func (sc *StopTransactionCoordinator) planStopReservation(c *app.Context, userInfo models.UserInfo, oldProxyID uint, txID string, key dao.ReservationKey, now time.Time) (*updateReservationPlan, error) {
	d := sc.deps
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

// compensateStop performs the automatic rollback after the stop apply point
// (spec §8.2): re-apply the immutable before config to the SAME client
// handler on the physical origin agent, verify the old rule is held by frps
// again (VerifyApplied), then atomically mark rolled_back and release ONLY
// the current tx's pending lease (SameKeyOwned has none; the historical
// committed lease's transaction_id is never rewritten, so it can never be
// deleted by txID). Any step failure moves the transaction to
// rollback_failed and RETAINS the current pending lease for manual
// intervention. Returns true when the rollback completed. Evidence never
// contains config/secret material.
func (sc *StopTransactionCoordinator) compensateStop(
	c *app.Context,
	userInfo models.UserInfo,
	txID, serverID, origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	cli *models.ClientEntity,
	beforeClient []byte,
	origCode, origMsg string,
) bool {
	d := sc.deps
	logger.Logger(c).Warnf("compensating stop transaction [%s]: %s", txID, origMsg)

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
			logger.Logger(c).WithError(err).Errorf("cannot persist rollback_failed for stop transaction [%s]", txID)
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

	// 4. verify the old rule is held by our embedded frps again
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
