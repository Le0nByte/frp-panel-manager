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
// B3b: the SAFE START coordinator (spec §8.3 启动).
//
// SafeStartProxy is the transactional start for a tcp/udp managed rule. It
// resolves the CANONICAL ORIGIN client id (child- or origin-addressed),
// acquires the (tenant, origin) keyed lock — the SAME originlock.Shared
// instance the legacy writers and SafeCreate/SafeUpdate/SafeStop lock on —
// and runs the WHOLE flow under it: idempotency replay, old-rule +
// physical-key resolution, reservation planning (SameKeyOwned /
// SameKeyPending only — start never changes the port), before snapshots
// (saved before ANY Verify/Apply), branch selection, gates, Apply /
// VerifyApplied, CommitStart and compensation.
//
//   - fast branch (the target is ALREADY present in the persisted client
//     config — an active rule or a stopped-drift rule): DB-only. The
//     coordinator runs the client local-listen gate (NEVER the server bind
//     probe) plus the VerifyApplied owner proof, then commits WITHOUT
//     contacting the agent. A failed client gate REJECTS
//     (PORT_LOCAL_NOT_LISTENING — spec: the two-sided gate is mandatory);
//     a failed VerifyApplied owner proof UPGRADES to the full branch (the
//     agent may not serve the rule) — server bind probe + client gate +
//     Apply + re-verify.
//   - full branch (the target is ABSENT from the persisted config — a
//     normal stopped start or an active runtime drift): the two-sided gate
//     runs first (RunServerProbe bind probe must be AVAILABLE, RunClient on
//     the rule's local ip:port must be LISTENING or ACTIVE), then apply the
//     candidate (persisted config + the old row's typed rule) to the
//     PHYSICAL origin agent (TargetClientID = the rule's real client row,
//     Replace empty), verify the port served (VerifyApplied), commit. Every
//     post-apply failure compensates: re-apply the before config, verify the
//     port released (VerifyReleased — the create direction), then mark
//     rolled_back and release only the current pending lease; a failed
//     compensation lands on rollback_failed and RETAINS the lease.
//
// The live /start route IS switched in B4b: StartProxy is the thin delegate
// to SafeStartProxy / safeStartCoordinator.
// ---------------------------------------------------------------------------

// StartTxStore is the transaction persistence surface SafeStart needs.
type StartTxStore interface {
	FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error
	UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	CommitStart(c *app.Context, userInfo models.UserInfo, in *dao.CommitLifecycleInput) (uint, error)
	FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error
}

// StartQueryStore is the read surface SafeStart needs (all tenant-scoped;
// the locked body re-queries every entity through this interface instead of
// caching pre-lock reads).
type StartQueryStore interface {
	GetOriginClientID(c *app.Context, userInfo models.UserInfo, clientID string) (string, error)
	GetClientByClientID(c *app.Context, userInfo models.UserInfo, clientID string) (*models.Client, error)
	GetProxyConfigByOriginOrClientAndName(c *app.Context, userInfo models.UserInfo, originID, serverID, name string) (*models.ProxyConfig, error)
	GetByServerFilter(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error)
}

// daoStore adapter methods for the start surface. They are ADDITIVE: the
// SafeCreate/Update/Stop interface sets are untouched, so no existing
// wrapper stops compiling.
func (s *daoStore) CommitStart(c *app.Context, userInfo models.UserInfo, in *dao.CommitLifecycleInput) (uint, error) {
	return dao.NewMutation(c).CommitStart(userInfo, in)
}

// StartCoordinatorDeps carries every external dependency of the safe start
// flow. Fields mirror CoordinatorDeps / StopCoordinatorDeps; HashRequest
// takes the CANONICAL RESOLVED ORIGIN id so the same logical request hashes
// the same before and after a first-shadow conversion.
type StartCoordinatorDeps struct {
	Query          StartQueryStore
	Tx             StartTxStore
	Res            ResStore
	Snap           SnapStore
	KeyProvider    KeyProvider
	Preflight      PreflightRunner
	Apply          ApplyService
	Verifier       Verifier
	OriginLocker   OriginLocker
	Now            func() time.Time
	Lease          time.Duration
	ApplyTimeout   time.Duration
	VerifyTimeout  time.Duration
	VerifyAttempts int
	VerifyInterval time.Duration
	HashRequest    func(req *pb.StartProxyRequest, originID string) string
}

func defaultStartDeps() StartCoordinatorDeps {
	return StartCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Res:            newDAOStore(),
		Snap:           newDAOStore(),
		KeyProvider:    ConfigKeyProvider{},
		Preflight:      preflightRunner{},
		Apply:          rpcApplyService{},
		Verifier:       &rpcVerifier{Attempts: defaultVerifyAttempts, Interval: defaultVerifyInterval},
		OriginLocker:   sharedOriginLocker,
		Now:            time.Now,
		Lease:          defaultReservationLease,
		ApplyTimeout:   defaultApplyTimeout,
		VerifyTimeout:  defaultVerifyTimeout,
		VerifyAttempts: defaultVerifyAttempts,
		VerifyInterval: defaultVerifyInterval,
		HashRequest:    hashStartRequest,
	}
}

// StartTransactionCoordinator orchestrates the safe start lifecycle.
type StartTransactionCoordinator struct {
	deps StartCoordinatorDeps
}

// NewDefaultStartCoordinator wires the REAL implementations (DAO, RPC,
// configured key file, the SHARED origin lock).
func NewDefaultStartCoordinator() *StartTransactionCoordinator {
	return &StartTransactionCoordinator{deps: defaultStartDeps()}
}

// safeStartCoordinator is the package-wide default coordinator used by the
// B4b /start route. Tests swap it for a coordinator with injected deps
// and restore it afterwards.
var safeStartCoordinator = NewDefaultStartCoordinator()

// SafeStartProxy is the exported transactional entry used by the B4 route.
// It delegates to the package-global safeStartCoordinator.
func SafeStartProxy(c *app.Context, req *pb.StartProxyRequest) (*pb.StartProxyResponse, error) {
	return safeStartCoordinator.SafeStartProxy(c, req)
}

// hashStartRequest computes the canonical idempotency hash for a start:
// SHA-256 over the operation discriminator ("start"), the CANONICAL RESOLVED
// ORIGIN id (never the raw request client id — a retry through the generated
// shadow child must hash identically), the server and the rule name — the
// full semantic identity of a start (the request carries no config body).
// idempotency_key is deliberately excluded (it is the lookup dimension).
func hashStartRequest(req *pb.StartProxyRequest, originID string) string {
	h := sha256.New()
	h.Write([]byte("start"))
	h.Write([]byte{0})
	h.Write([]byte(originID))
	h.Write([]byte{0})
	h.Write([]byte(req.GetServerId()))
	h.Write([]byte{0})
	h.Write([]byte(req.GetName()))
	return hex.EncodeToString(h.Sum(nil))
}

// startEvidence is the structured evidence of a committed start: it records
// the branch (full = an agent apply happened, fast = db-only), whether the
// agent was touched and the exact Apply/Verify call counts. It never
// carries config/secret material.
type startEvidence struct {
	Branch         string `json:"branch"` // "full" | "fast"
	AppliedToAgent bool   `json:"applied_to_agent"`
	ApplyCalls     int    `json:"apply_calls"`
	VerifyCalls    int    `json:"verify_calls"`
}

func (e startEvidence) JSON() string {
	raw, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(raw)
}

func invalidStartResp(code, msg string) *pb.StartProxyResponse {
	resp := &pb.StartProxyResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
	}
	if code != "" {
		resp.ErrorCode = lo.ToPtr(code)
	}
	return resp
}

// redactStartRequest builds the redacted request summary stored on the
// transaction: ids and the rule name only (no config body exists on a start
// request).
func redactStartRequest(req *pb.StartProxyRequest, origin string) (string, error) {
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

// startRowTyped parses a proxy_config row's typed content into the typed
// rule AND its physical reservation key. Only tcp/udp rules are managed
// (spec §1.2.5: HTTP/HTTPS/STCP/XTCP are read-only) — a non-tcp/udp type
// fails with defs.ErrUnsupportedProxyType; an unparseable row or a row
// without a base config fails closed (can't prove tcp/udp ⇒ can't start).
func startRowTyped(row *models.ProxyConfig) (v1.TypedProxyConfig, dao.ReservationKey, error) {
	typed, err := row.GetTypedProxyConfig()
	if err != nil {
		return v1.TypedProxyConfig{}, dao.ReservationKey{}, fmt.Errorf("old rule content is not a parseable typed config: %v", err)
	}
	if base := typed.GetBaseConfig(); base != nil {
		switch strings.ToLower(strings.TrimSpace(base.Type)) {
		case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		default:
			return v1.TypedProxyConfig{}, dao.ReservationKey{}, fmt.Errorf("%w: %s", defs.ErrUnsupportedProxyType, base.Type)
		}
	}
	key, err := updateTypedPhysicalKey(row.ServerID, typed)
	if err != nil {
		return v1.TypedProxyConfig{}, dao.ReservationKey{}, err
	}
	return typed, key, nil
}

// startLocalTarget extracts the local bind address of the old rule — the
// client-side gate probes this on the PHYSICAL origin agent.
func startLocalTarget(typed v1.TypedProxyConfig) (string, uint32, error) {
	switch p := typed.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		return p.LocalIP, uint32(p.LocalPort), nil
	case *v1.UDPProxyConfig:
		return p.LocalIP, uint32(p.LocalPort), nil
	default:
		name := ""
		if base := typed.GetBaseConfig(); base != nil {
			name = base.Name
		}
		return "", 0, fmt.Errorf("proxy %q has no local target", name)
	}
}

// startPreflightRequest builds the two-sided gate request. The client id is
// the PHYSICAL origin agent (never the logical child); the server side is a
// bind probe on the rule's remote port and the client side is the rule's
// local ip:port.
func startPreflightRequest(origin, serverID, name string, key dao.ReservationKey, localIP string, localPort uint32) *pb.PreflightRequest {
	return &pb.PreflightRequest{
		ClientId:   lo.ToPtr(origin),
		ServerId:   lo.ToPtr(serverID),
		Protocol:   lo.ToPtr(key.Protocol),
		RemotePort: lo.ToPtr(key.RemotePort),
		LocalIp:    lo.ToPtr(localIP),
		LocalPort:  lo.ToPtr(localPort),
		ProxyName:  lo.ToPtr(name),
	}
}

// startTargetMatches parses the persisted client config and locates the
// target rule: the target name must appear exactly 0 or 1 times; a present
// entry must carry the SAME physical key as the rule row — a same-name rule
// on a different key is drift and may never be started blindly. A malformed
// typed entry WITHOUT a base config fails closed (its physical key is
// unprovable). The returned slice is the fast-branch trigger (1 entry) vs
// the full-branch trigger (0 entries).
func startTargetMatches(cli *models.ClientEntity, name string, key dao.ReservationKey) ([]v1.TypedProxyConfig, error) {
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

// resolveStartOriginAndLock resolves the ORIGIN client id for a start
// request that may address the origin itself or one of its shadow children,
// acquires the (tenant, origin) keyed lock on the SAME injected locker
// instance the legacy writers and SafeCreate/SafeUpdate/SafeStop use, and
// re-resolves UNDER the lock: if the mapping changed while waiting, the key
// is released and the resolve+lock cycle retried (bounded, fail-closed).
// The caller MUST defer the unlock exactly once. The whole flow —
// idempotency replay, resolution, snapshots, gates, verify, apply, commit
// AND compensation — runs under this lock.
func (sc *StartTransactionCoordinator) resolveStartOriginAndLock(c *app.Context, userInfo models.UserInfo, reqClientID string) (string, func(), error) {
	d := sc.deps
	if d.OriginLocker == nil {
		panic("proxy: StartTransactionCoordinator.OriginLocker is nil — same-origin SafeStart serialization is disabled")
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

// SafeStartProxy is the transactional start (spec §8.3). It returns (resp,
// nil) for structured business failures (the response carries the stable
// error code/message) and (nil, err) for internal errors.
func (sc *StartTransactionCoordinator) SafeStartProxy(c *app.Context, req *pb.StartProxyRequest) (*pb.StartProxyResponse, error) {
	d := sc.deps
	userInfo := common.GetUserInfo(c)
	if userInfo == nil {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, "authentication required"), nil
	}

	// ---- a. request normalization (fail-closed BEFORE any lock or write):
	// client/server/name and a UUID-v4 idempotency key are mandatory. ----
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetName()) == 0 {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, "client_id, server_id and name are required"), nil
	}
	idemKey := req.GetIdempotencyKey()
	if !isValidIdempotencyKey(idemKey) {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed,
			"idempotency_key is required and must be a UUID v4"), nil
	}

	// ---- b. resolve the CANONICAL ORIGIN + per-(tenant, origin)
	// serialization (D1 P0): the lock key, the idempotency hash and the
	// physical agent are anchored on the resolved origin, NEVER the raw
	// request client id. The lock spans the WHOLE flow. ----
	origin, unlock, err := sc.resolveStartOriginAndLock(c, userInfo, req.GetClientId())
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
		return sc.replayStart(existing, reqHash)
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
			return invalidStartResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("proxy rule %q not found", req.GetName())), nil
		}
		if errors.Is(err, defs.ErrAmbiguousProxyTarget) {
			return invalidStartResp(defs.ErrorCodeConfigValidationFailed, "ambiguous proxy target: more than one row matches the rule"), nil
		}
		return nil, err
	}
	startTyped, oldKey, err := startRowTyped(old)
	if err != nil {
		if errors.Is(err, defs.ErrUnsupportedProxyType) {
			return invalidStartResp(defs.ErrorCodeUnsupportedProxyType, err.Error()), nil
		}
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the old rule's physical key: %v", err)), nil
	}
	localIP, localPort, err := startLocalTarget(startTyped)
	if err != nil {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- e. the client row the rule lives on + shape guard (the B3a stop
	// shape contract is identical for start — reuse it, zero drift). ----
	cli, err := d.Query.GetClientByClientID(c, userInfo, old.ClientID)
	if err != nil {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the rule's client %s: %v", old.ClientID, err)), nil
	}
	clientEntity := cli.ClientEntity
	if err := validateStopClientShape(old, clientEntity, origin, req.GetServerId()); err != nil {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- f. parse the persisted config: target exactly 0 or 1 with a
	// matching physical key (drift rules are never started blindly); a
	// malformed nil-base entry fails closed. ----
	matches, err := startTargetMatches(clientEntity, old.Name, oldKey)
	if err != nil {
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- g. the physical origin agent must be able to receive the
	// update — a stopped/unavailable origin fails closed (AGENT_OFFLINE). ----
	originCli, err := d.Query.GetClientByClientID(c, userInfo, origin)
	if err != nil {
		return nil, err
	}
	if originCli.ClientEntity.Stopped {
		return invalidStartResp(defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error()), nil
	}

	// ---- h. create the transaction (pending) FIRST, bound to the OLD row
	// id (the lease identity): every later DB write happens under a live
	// transaction row. ----
	txID := uuid.NewString()
	now := d.Now()
	redacted, err := redactStartRequest(req, origin)
	if err != nil {
		return nil, err
	}
	tx := &models.ChangeTransaction{
		TransactionID:   txID,
		IdempotencyKey:  idemKey,
		RequestHash:     reqHash,
		Operation:       models.TxOperationStart,
		Status:          models.TxStatusPending,
		ProxyID:         lo.ToPtr(old.ID),
		RequestRedacted: redacted,
		StartedAt:       now,
	}
	if err := d.Tx.Create(c, userInfo, tx); err != nil {
		// concurrent request with the same key won the insert race
		if dao.IsUniqueViolation(err) {
			if existing, ferr := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey); ferr == nil {
				return sc.replayStart(existing, reqHash)
			}
		}
		return nil, err
	}

	// ---- i. reservation: same physical key only — SameKeyOwned (an
	// existing committed lease owned by the old rule, NO TryAcquire) or
	// SameKeyPending (legacy: TryAcquire THIS tx's pending lease). Start
	// never changes the port; failures release only the current pending. ----
	resPlan, err := sc.planStartReservation(c, userInfo, old.ID, txID, oldKey, now)
	if err != nil {
		msg := fmt.Sprintf("port %d/%s on %s cannot be reserved: %v", oldKey.RemotePort, oldKey.Protocol, req.GetServerId(), err)
		_ = sc.failStartTx(c, userInfo, txID, defs.ErrorCodeReservationConflict, msg, "")
		return invalidStartResp(defs.ErrorCodeReservationConflict, msg), nil
	}

	// ---- j. before snapshots — saved BEFORE any Verify/Apply
	// (requirement 7): client physical before (MarshalJSONConfig) + proxy
	// versioned before envelope. Key/store failures fail closed. ----
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot load snapshot key")
		_ = sc.failStartTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	beforeClient, err := clientEntity.MarshalJSONConfig()
	if err != nil {
		_ = sc.failStartTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err), "")
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err)), nil
	}
	proxyBeforePlain, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: old.ID, Name: old.Name, ServerID: old.ServerID,
		ClientID: old.ClientID, OriginClientID: old.OriginClientID,
		Stopped: old.Stopped, Typed: old.Content,
	})
	if err != nil {
		_ = sc.failStartTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy before envelope: %v", err), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, old.ClientID, beforeClient, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the client before snapshot")
		_ = sc.failStartTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(old.ID), 10), proxyBeforePlain, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the proxy before snapshot")
		_ = sc.failStartTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	// the started proxy after envelope: the rule content is unchanged, only
	// Stopped flips to false.
	proxyAfterPlain, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: old.ID, Name: old.Name, ServerID: old.ServerID,
		ClientID: old.ClientID, OriginClientID: old.OriginClientID,
		Stopped: false, Typed: old.Content,
	})
	if err != nil {
		_ = sc.failStartTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy after envelope: %v", err), "")
		return nil, err
	}

	// ---- k. branch: the target is ABSENT from the persisted config ->
	// full (gates + Apply + verify + commit); PRESENT -> fast DB-only
	// (client gate + owner proof + commit, zero Apply). ----
	preReq := startPreflightRequest(origin, req.GetServerId(), old.Name, oldKey, localIP, localPort)
	if len(matches) == 0 {
		return sc.safeStartFullLocked(c, userInfo, req, origin, old, oldKey, resPlan, clientEntity, startTyped, beforeClient, proxyAfterPlain, snapKey, preReq, txID)
	}
	return sc.safeStartFastLocked(c, userInfo, req, origin, old, oldKey, resPlan, clientEntity, beforeClient, proxyAfterPlain, snapKey, preReq, txID)
}

// safeStartFastLocked implements the fast DB-only branch. The caller holds
// the (tenant, origin) lock; the target is ALREADY present in the persisted
// config, so the coordinator only runs the client local-listen gate (NEVER
// the server bind probe) plus the VerifyApplied owner proof and commits —
// ZERO Apply. A failed client gate REJECTS (PORT_LOCAL_NOT_LISTENING —
// spec: the two-sided gate is mandatory even for the already-present
// state). A failed VerifyApplied means the agent does NOT serve the rule
// despite the config: the branch UPGRADES to a real Apply (re-running BOTH
// gates, applying the same bytes from applying) and re-verifies — the
// verifier failure is never reported as success. A fast CommitStart failure
// is `failed` + CONFIG_COMMIT_FAILED (nothing was ever applied, so
// rolled_back would be a lie).
func (sc *StartTransactionCoordinator) safeStartFastLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.StartProxyRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	clientEntity *models.ClientEntity,
	beforeClient, proxyAfterPlain, snapKey []byte,
	preReq *pb.PreflightRequest,
	txID string,
) (*pb.StartProxyResponse, error) {
	d := sc.deps
	verifyCalls := 0

	// Enter preflight before the client local gate. Do NOT jump directly
	// to verifying: the transaction state machine requires preflight ->
	// applying -> verifying even for a DB-only fast path. We defer those
	// two transitions until we know whether this remains fast or upgrades
	// to a real Apply.
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the db-only start to preflight: %v", err)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
	}

	// client local gate (the fast branch NEVER runs the server bind probe)
	clientRes, gErr := d.Preflight.RunClient(c, preReq)
	if gErr != nil {
		code := classifyAgentError(gErr)
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		msg := fmt.Sprintf("client preflight failed: %v", gErr)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
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
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
	}

	// VerifyApplied(oldKey) owner proof: the embedded frps must already
	// hold the port (the rule is in the config — this proves the DB state
	// matches the agent state).
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyApplied(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	verifyCalls++
	if verifyErr != nil {
		// UPGRADE to full: the agent does not serve the rule despite the
		// config. Re-run BOTH gates (server bind probe + client local
		// listen), then applying -> Apply (the SAME persisted bytes — the
		// target is already present) -> verifying -> VerifyApplied. The
		// Apply never runs from the verifying state.
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		logger.Logger(c).Warnf("fast start [%s] verify failed; upgrading to full apply: %s", txID, msg)

		serverRes, gErr := d.Preflight.RunServerProbe(c, preReq)
		if gErr != nil {
			code := classifyAgentError(gErr)
			if code == "" {
				code = defs.ErrorCodePortScanUnavailable
			}
			smsg := fmt.Sprintf("server preflight failed: %v", gErr)
			_ = sc.failStartTx(c, userInfo, txID, code, smsg, "")
			return invalidStartResp(code, smsg), nil
		}
		if !serverRes.Ok {
			code := serverRes.ErrorCode
			if code == "" {
				code = defs.ErrorCodePortRemoteInUse
			}
			smsg := serverRes.ErrorMessage
			if smsg == "" {
				smsg = "remote port is not available on the server"
			}
			_ = sc.failStartTx(c, userInfo, txID, code, smsg, "")
			return invalidStartResp(code, smsg), nil
		}
		clientRes2, gErr := d.Preflight.RunClient(c, preReq)
		if gErr != nil {
			code := classifyAgentError(gErr)
			if code == "" {
				code = defs.ErrorCodePortScanUnavailable
			}
			cmsg := fmt.Sprintf("client preflight failed: %v", gErr)
			_ = sc.failStartTx(c, userInfo, txID, code, cmsg, "")
			return invalidStartResp(code, cmsg), nil
		}
		if !clientRes2.Ok {
			code := clientRes2.ErrorCode
			if code == "" {
				code = defs.ErrorCodePortLocalNotListening
			}
			cmsg := clientRes2.ErrorMessage
			if cmsg == "" {
				cmsg = "no local service is listening on the client"
			}
			_ = sc.failStartTx(c, userInfo, txID, code, cmsg, "")
			return invalidStartResp(code, cmsg), nil
		}

		if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			amsg := fmt.Sprintf("cannot advance the upgraded start to applying: %v", err)
			_ = sc.failStartTx(c, userInfo, txID, code, amsg, "")
			return invalidStartResp(code, amsg), nil
		}
		finalRaw, buildErr := buildFrpcApplyConfig(c, clientEntity, req.GetServerId(), beforeClient)
		if buildErr != nil {
			code := defs.ErrorCodeCommitFailed
			bmsg := fmt.Sprintf("cannot build the upgrade config: %v", buildErr)
			_ = sc.failStartTx(c, userInfo, txID, code, bmsg, "")
			return invalidStartResp(code, bmsg), nil
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
			compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, applyCode, amsg)
			if compensated {
				return invalidStartResp(applyCode, amsg), nil
			}
			return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", amsg)), nil
		}
		if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			vmsg := fmt.Sprintf("cannot advance the upgraded start to verifying: %v", err)
			compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, vmsg)
			if compensated {
				return invalidStartResp(code, vmsg), nil
			}
			return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", vmsg)), nil
		}
		verifyCtx2, cancelVerify2 := context.WithTimeout(c, d.VerifyTimeout)
		verifyErr = d.Verifier.VerifyApplied(verifyCtx2, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
		cancelVerify2()
		verifyCalls++
		if verifyErr != nil {
			vmsg := fmt.Sprintf("verify failed: %v", verifyErr)
			compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, defs.ErrorCodeVerifyFailed, vmsg)
			if compensated {
				return invalidStartResp(defs.ErrorCodeVerifyFailed, vmsg), nil
			}
			return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", vmsg)), nil
		}
		// upgraded start DID touch the agent: evidence branch=full
		commitIn := sc.startCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, clientEntity, beforeClient, proxyAfterPlain, snapKey,
			"full", true, 1, verifyCalls)
		if _, err := d.Tx.CommitStart(c, userInfo, commitIn); err != nil {
			code := defs.ErrorCodeCommitFailed
			cmsg := fmt.Sprintf("commit failed: %v", err)
			compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, cmsg)
			if compensated {
				return invalidStartResp(code, cmsg), nil
			}
			return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", cmsg)), nil
		}
		return &pb.StartProxyResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
	}

	// fast DB-only commit: the client bytes are unchanged (candidate ==
	// before); a commit failure is failed + CONFIG_COMMIT_FAILED, NEVER
	// rolled_back (no compensation ran).
	for _, status := range []string{models.TxStatusApplying, models.TxStatusVerifying} {
		if err := d.Tx.UpdateStatus(c, userInfo, txID, status, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			msg := fmt.Sprintf("cannot advance the db-only start to %s: %v", status, err)
			_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
			return invalidStartResp(code, msg), nil
		}
	}
	commitIn := sc.startCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, clientEntity, beforeClient, proxyAfterPlain, snapKey,
		"fast", false, 0, verifyCalls)
	if _, err := d.Tx.CommitStart(c, userInfo, commitIn); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
	}
	return &pb.StartProxyResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
}

// safeStartFullLocked implements the full branch: build the candidate
// (persisted config PLUS the old row's typed rule), run the two-sided gate
// (server bind probe must be AVAILABLE, client local target must be
// LISTENING or ACTIVE), apply to the PHYSICAL origin agent (TargetClientID
// = the rule's real client row, ReplaceClientID EMPTY — start is never a
// directed switch), verify the port served, commit. Every failure after the
// Apply point compensates (restore the before config, verify the port
// released, rolled_back + release current pending; a failed compensation
// lands on rollback_failed and RETAINS the lease).
func (sc *StartTransactionCoordinator) safeStartFullLocked(
	c *app.Context,
	userInfo models.UserInfo,
	req *pb.StartProxyRequest,
	origin string,
	old *models.ProxyConfig,
	oldKey dao.ReservationKey,
	resPlan *updateReservationPlan,
	clientEntity *models.ClientEntity,
	startTyped v1.TypedProxyConfig,
	beforeClient, proxyAfterPlain, snapKey []byte,
	preReq *pb.PreflightRequest,
	txID string,
) (*pb.StartProxyResponse, error) {
	d := sc.deps

	// ---- 1. build the candidate: the persisted config PLUS the target
	// (the old row's typed rule is the authoritative truth). The
	// client-after entity carries the same identity; only ConfigContent
	// changes (the DAO commit writes nothing else). No duplicate can be
	// generated: this branch only runs when the target name is absent. ----
	oldCfg, err := clientEntity.GetConfigContent()
	if err != nil {
		_ = sc.failStartTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, "cannot read the current client config", "")
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, "cannot read the current client config"), nil
	}
	oldCfg.Proxies = append(oldCfg.Proxies, startTyped)
	afterEntity := &models.ClientEntity{}
	if err := deepcopy.Copy(afterEntity, clientEntity); err != nil {
		_ = sc.failStartTx(c, userInfo, txID, "", fmt.Sprintf("cannot copy the client entity: %v", err), "")
		return nil, err
	}
	if err := afterEntity.SetConfigContent(*oldCfg); err != nil {
		_ = sc.failStartTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	candidate, err := afterEntity.MarshalJSONConfig()
	if err != nil {
		_ = sc.failStartTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	finalRaw, err := buildFrpcApplyConfig(c, afterEntity, req.GetServerId(), candidate)
	if err != nil {
		_ = sc.failStartTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err), "")
		return invalidStartResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err)), nil
	}

	// ---- 2. pending -> preflight (DB-only transition); a transition
	// failure BEFORE the gates is failed + CONFIG_COMMIT_FAILED + release. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the start to preflight: %v", err)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
	}

	// ---- 3. server gate: RunServerProbe (bind probe) on the remote port
	// must come back AVAILABLE. ----
	serverRes, gErr := d.Preflight.RunServerProbe(c, preReq)
	if gErr != nil {
		code := classifyAgentError(gErr)
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		msg := fmt.Sprintf("server preflight failed: %v", gErr)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
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
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
	}

	// ---- 4. client gate: the rule's local ip:port must be LISTENING or
	// ACTIVE on the physical origin agent. ----
	clientRes, gErr := d.Preflight.RunClient(c, preReq)
	if gErr != nil {
		code := classifyAgentError(gErr)
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		msg := fmt.Sprintf("client preflight failed: %v", gErr)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
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
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
	}

	// ---- 5. applying: synchronous apply with ACK (15s budget). The
	// physical agent is the canonical origin; the target is the rule's real
	// client row; Replace is EMPTY — start is never a directed switch. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the start to applying: %v", err)
		_ = sc.failStartTx(c, userInfo, txID, code, msg, "")
		return invalidStartResp(code, msg), nil
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
		msg := fmt.Sprintf("apply failed: %v", applyErr)
		compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, applyCode, msg)
		if compensated {
			return invalidStartResp(applyCode, msg), nil
		}
		return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 6. applying -> verifying (POST-apply: a failure compensates —
	// the config IS applied). ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the applied start to verifying: %v", err)
		compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, msg)
		if compensated {
			return invalidStartResp(code, msg), nil
		}
		return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 7. verify the port is served by the embedded frps (30s budget). ----
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyApplied(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, defs.ErrorCodeVerifyFailed, msg)
		if compensated {
			return invalidStartResp(defs.ErrorCodeVerifyFailed, msg), nil
		}
		return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- 8. single-transaction commit (CommitStart): stopped flip +
	// client config CAS + same-key reservation migration + after snapshots +
	// start audit + evidence. A commit failure compensates (the agent side
	// is fine; only the control plane failed). ----
	commitIn := sc.startCommitInput(txID, old, oldKey, resPlan, origin, clientEntity.ConfigContent, afterEntity, candidate, proxyAfterPlain, snapKey,
		"full", true, 1, 1)
	if _, err := d.Tx.CommitStart(c, userInfo, commitIn); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		compensated := sc.compensateStart(c, userInfo, txID, req.GetServerId(), origin, old, oldKey, clientEntity, beforeClient, code, msg)
		if compensated {
			return invalidStartResp(code, msg), nil
		}
		return invalidStartResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	return &pb.StartProxyResponse{Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"}}, nil
}

// startCommitInput builds the CommitLifecycleInput for a start commit:
// ExpectedStopped = the old row's stopped value, ExpectedClientConfigContent
// = the ORIGINAL DB bytes (NOT the candidate — the S4 CAS compares against
// what is persisted), ClientAfter carries only the changed ConfigContent,
// Reservation old==new (SameKeyOwned/SameKeyPending), client-after physical
// snapshot, running proxy after envelope, a start audit event and evidence
// that explicitly records branch / applied_to_agent / apply_calls /
// verify_calls. The DAO re-checks tenant/actor/op on every row it touches.
func (sc *StartTransactionCoordinator) startCommitInput(
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
	ev := startEvidence{Branch: branch, AppliedToAgent: appliedToAgent, ApplyCalls: applyCalls, VerifyCalls: verifyCalls}.JSON()
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
			EventType:     models.AuditEventTypeStart,
			Summary:       fmt.Sprintf("started proxy %q (%s port %d) on %s for %s", old.Name, oldKey.Protocol, oldKey.RemotePort, old.ServerID, origin),
			NodeID:        old.ClientID,
			EvidenceRef:   "change_transaction:" + txID + "#evidence_json",
			Success:       true,
		},
		EvidenceJSON: ev,
	}
}

// replayStart implements the idempotency replay semantics (gate 7):
// same key + same hash + committed -> idempotent success (no re-Apply /
// re-Verify); same key + different hash (or a different operation) ->
// stable IDEMPOTENCY_CONFLICT; terminal failure -> the original failure
// result; in-flight -> stable IDEMPOTENCY_IN_FLIGHT.
func (sc *StartTransactionCoordinator) replayStart(existing *models.ChangeTransaction, reqHash string) (*pb.StartProxyResponse, error) {
	if existing.Operation != models.TxOperationStart || existing.RequestHash != reqHash {
		return invalidStartResp(defs.ErrorCodeIdempotencyConflict,
			"idempotency key was already used with a different request"), nil
	}
	switch existing.Status {
	case models.TxStatusCommitted:
		return &pb.StartProxyResponse{
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
		return invalidStartResp(code, msg), nil
	default:
		return invalidStartResp(defs.ErrorCodeIdempotencyInFlight,
			"a request with this idempotency key is already in progress"), nil
	}
}

// failStartTx marks the transaction failed (pre-apply failures) and audits
// it. The pending reservation owned by the transaction is released
// (SameKeyOwned has none — the historical committed lease is never
// touched).
func (sc *StartTransactionCoordinator) failStartTx(c *app.Context, userInfo models.UserInfo, txID, code, msg, evidence string) error {
	audit := &models.AuditEvent{
		TransactionID: txID,
		EventType:     models.AuditEventTypeStart,
		Summary:       "start failed: " + msg,
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

// planStartReservation decides the same-key reservation mode (start never
// changes the port):
//
//	committed row owned by the old rule -> SameKeyOwned (NO TryAcquire);
//	no row (legacy world)             -> SameKeyPending (TryAcquire THIS tx);
//	any other row                     -> RESERVATION_CONFLICT.
//
// TryAcquire requires the transaction row to EXIST and be pending — the
// caller creates it first.
func (sc *StartTransactionCoordinator) planStartReservation(c *app.Context, userInfo models.UserInfo, oldProxyID uint, txID string, key dao.ReservationKey, now time.Time) (*updateReservationPlan, error) {
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

// compensateStart performs the automatic rollback after the start apply
// point (spec §8.2): re-apply the immutable before config to the SAME
// client handler on the physical origin agent, verify the port is RELEASED
// again (VerifyReleased — the create direction: a started rule is removed
// by restoring the before config), then atomically mark rolled_back and
// release ONLY the current tx's pending lease (SameKeyOwned has none; the
// historical committed lease's transaction_id is never rewritten, so it can
// never be deleted by txID). Any step failure moves the transaction to
// rollback_failed and RETAINS the current pending lease for manual
// intervention. Returns true when the rollback completed. Evidence never
// contains config/secret material.
func (sc *StartTransactionCoordinator) compensateStart(
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
	logger.Logger(c).Warnf("compensating start transaction [%s]: %s", txID, origMsg)

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
			logger.Logger(c).WithError(err).Errorf("cannot persist rollback_failed for start transaction [%s]", txID)
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

	// 4. verify the port is released by the embedded frps again (the added
	// rule must be gone)
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyReleased(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		return failRollback("compensate-verify-old", defs.ErrorCodeVerifyFailed, fmt.Sprintf("compensation verify released state failed: %v", verifyErr))
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
