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
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// A4b/A4c: the SAFE UPDATE coordinator.
//
// This file implements the ACTIVE branch of the safe update (spec §8.1
// applied to updates): resolve+lock, idempotency, old-rule resolution,
// reservation-mode planning, shadow plan, candidate build, before
// snapshots, mode-aware preflight (owner-proof for same physical key, bind
// probe for a new key), Apply/Verify/CommitUpdate and the full dual-verify
// compensation. The STOPPED branch (zero-Apply in-place metadata update)
// lives in transaction_update_stopped_coordinator.go and is routed here
// from the branch point in SafeUpdateProxy (A4c).
//
// The dependency surface mirrors SafeCreate (CoordinatorDeps) but stays a
// SEPARATE small interface set (UpdateTxStore / UpdateQueryStore): the
// existing SafeCreate TxStore is not forced to grow, and every wrapper used
// by tests still compiles. daoStore gains the extra adapter methods below.
// ---------------------------------------------------------------------------

// UpdateTxStore is the transaction persistence surface SafeUpdate needs.
type UpdateTxStore interface {
	FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error
	UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	CommitUpdate(c *app.Context, userInfo models.UserInfo, in *dao.CommitUpdateInput) (uint, error)
	FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error
}

// UpdateQueryStore is the read surface SafeUpdate needs (all tenant-scoped;
// the locked body re-queries every entity through this interface instead of
// caching pre-lock reads).
type UpdateQueryStore interface {
	GetOriginClientID(c *app.Context, userInfo models.UserInfo, clientID string) (string, error)
	GetClientByClientID(c *app.Context, userInfo models.UserInfo, clientID string) (*models.Client, error)
	GetProxyConfigByOriginOrClientAndName(c *app.Context, userInfo models.UserInfo, originID, serverID, name string) (*models.ProxyConfig, error)
	GetByServerFilter(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error)
}

// daoStore adapter methods for the update surface. They are ADDITIVE: the
// SafeCreate TxStore interface is untouched, so no existing wrapper stops
// compiling.
func (s *daoStore) CommitUpdate(c *app.Context, userInfo models.UserInfo, in *dao.CommitUpdateInput) (uint, error) {
	return dao.NewMutation(c).CommitUpdate(userInfo, in)
}
func (s *daoStore) GetOriginClientID(c *app.Context, userInfo models.UserInfo, clientID string) (string, error) {
	return dao.NewQuery(c).GetOriginClientID(userInfo, clientID)
}
func (s *daoStore) GetClientByClientID(c *app.Context, userInfo models.UserInfo, clientID string) (*models.Client, error) {
	return dao.NewQuery(c).GetClientByClientID(userInfo, clientID)
}
func (s *daoStore) GetProxyConfigByOriginOrClientAndName(c *app.Context, userInfo models.UserInfo, originID, serverID, name string) (*models.ProxyConfig, error) {
	return dao.NewQuery(c).GetProxyConfigByOriginOrClientAndName(userInfo, originID, serverID, name)
}
func (s *daoStore) GetByServerFilter(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error) {
	return dao.NewQuery(c).GetByServerFilter(userInfo, serverID, protocol, port)
}

// UpdateCoordinatorDeps carries every external dependency of the safe update
// flow. Fields mirror CoordinatorDeps (SafeCreate); HashRequest takes the
// CANONICAL RESOLVED ORIGIN id so the same logical request hashes the same
// before and after a first-shadow conversion.
type UpdateCoordinatorDeps struct {
	Query            UpdateQueryStore
	Tx               UpdateTxStore
	Res              ResStore
	Snap             SnapStore
	KeyProvider      KeyProvider
	Preflight        PreflightRunner
	Apply            ApplyService
	Verifier         Verifier
	OriginLocker     OriginLocker
	Now              func() time.Time
	Lease            time.Duration
	PreflightTimeout time.Duration // owner-proof VerifyApplied budget (like a gate)
	ApplyTimeout     time.Duration
	VerifyTimeout    time.Duration
	VerifyAttempts   int
	VerifyInterval   time.Duration
	HashRequest      func(req *pb.UpdateProxyConfigRequest, originID string) string
}

func defaultUpdateDeps() UpdateCoordinatorDeps {
	return UpdateCoordinatorDeps{
		Query:            newDAOStore(),
		Tx:               newDAOStore(),
		Res:              newDAOStore(),
		Snap:             newDAOStore(),
		KeyProvider:      ConfigKeyProvider{},
		Preflight:        preflightRunner{},
		Apply:            rpcApplyService{},
		Verifier:         &rpcVerifier{Attempts: defaultVerifyAttempts, Interval: defaultVerifyInterval},
		OriginLocker:     sharedOriginLocker,
		Now:              time.Now,
		Lease:            defaultReservationLease,
		PreflightTimeout: preflightTimeout,
		ApplyTimeout:     defaultApplyTimeout,
		VerifyTimeout:    defaultVerifyTimeout,
		VerifyAttempts:   defaultVerifyAttempts,
		VerifyInterval:   defaultVerifyInterval,
		HashRequest:      hashUpdateRequest,
	}
}

// UpdateTransactionCoordinator orchestrates the safe update lifecycle.
type UpdateTransactionCoordinator struct {
	deps UpdateCoordinatorDeps
}

// NewDefaultUpdateCoordinator wires the REAL implementations (DAO, RPC,
// configured key file, the SHARED origin lock).
func NewDefaultUpdateCoordinator() *UpdateTransactionCoordinator {
	return &UpdateTransactionCoordinator{deps: defaultUpdateDeps()}
}

// safeUpdateCoordinator is the package-wide default coordinator behind the
// /update_config route (A4c: UpdateProxyConfig is a thin delegate). Tests
// swap it for a coordinator with injected deps and restore it afterwards.
var safeUpdateCoordinator = NewDefaultUpdateCoordinator()

// hashUpdateRequest computes the canonical idempotency hash: SHA-256 over
// the CANONICAL RESOLVED ORIGIN id (never the raw request client id — a
// retry through the generated shadow child must hash identically), server,
// old rule name and the candidate config bytes. idempotency_key and
// preflight_token are deliberately excluded (the key is the lookup
// dimension, the token is a UI-only hint).
func hashUpdateRequest(req *pb.UpdateProxyConfigRequest, originID string) string {
	h := sha256.New()
	h.Write([]byte(originID))
	h.Write([]byte{0})
	h.Write([]byte(req.GetServerId()))
	h.Write([]byte{0})
	h.Write([]byte(req.GetName()))
	h.Write([]byte{0})
	h.Write(req.GetConfig())
	return hex.EncodeToString(h.Sum(nil))
}

// updateProxyEnvelope is the versioned typed-rule snapshot envelope stored
// encrypted as node_type=proxy (spec §6.3): the full rule-level state a
// one-click rollback needs, never plaintext in the audit trail.
type updateProxyEnvelope struct {
	Version        int    `json:"version"`
	ProxyID        uint   `json:"proxy_id,omitempty"` // before: old row id; after: omitted (final id forced by the DAO node_id)
	Name           string `json:"name"`
	ServerID       string `json:"server_id"`
	ClientID       string `json:"client_id"`
	OriginClientID string `json:"origin_client_id"`
	Stopped        bool   `json:"stopped"`
	Typed          []byte `json:"typed"` // typed rule JSON (the row Content)
}

func marshalProxyEnvelope(env updateProxyEnvelope) ([]byte, error) {
	env.Version = 1
	return json.Marshal(env)
}

// redactUpdateRequest builds the redacted request summary stored on the
// transaction: ids, name and the config SHA-256 — NEVER the config bytes
// (they may embed auth tokens).
func redactUpdateRequest(req *pb.UpdateProxyConfigRequest, target preflightTarget, originID string) (string, error) {
	sum := sha256.Sum256(req.GetConfig())
	summary := map[string]interface{}{
		"client_id":     originID,
		"server_id":     req.GetServerId(),
		"proxy_name":    target.ProxyName,
		"proxy_type":    target.Protocol,
		"remote_port":   target.RemotePort,
		"local_ip":      target.LocalIP,
		"local_port":    target.LocalPort,
		"config_sha256": hex.EncodeToString(sum[:]),
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func invalidUpdateResp(code, msg string) *pb.UpdateProxyConfigResponse {
	resp := &pb.UpdateProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
	}
	if code != "" {
		resp.ErrorCode = lo.ToPtr(code)
	}
	return resp
}

// updateTypedPhysicalKey extracts the physical reservation key of a typed
// v1 proxy config: (server, normalized-lowercase protocol, remote port).
// This is the coordinator-side mirror of the DAO's typedProxyPhysicalKey
// (the DAO helper is unexported): only tcp/udp are accepted and the remote
// port must be positive.
func updateTypedPhysicalKey(serverID string, cfg v1.TypedProxyConfig) (dao.ReservationKey, error) {
	base := cfg.GetBaseConfig()
	if base == nil {
		return dao.ReservationKey{}, fmt.Errorf("typed proxy config has no base config")
	}
	protocol := strings.ToLower(strings.TrimSpace(base.Type))
	switch protocol {
	case "tcp", "tcp4", "tcp6":
		protocol = "tcp"
	case "udp", "udp4", "udp6":
		protocol = "udp"
	default:
		return dao.ReservationKey{}, fmt.Errorf("unsupported proxy type %q (only tcp/udp)", base.Type)
	}
	var remotePort int
	switch p := cfg.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		remotePort = p.RemotePort
	case *v1.UDPProxyConfig:
		remotePort = p.RemotePort
	default:
		return dao.ReservationKey{}, fmt.Errorf("proxy %q has no remote port", base.Name)
	}
	if remotePort <= 0 {
		return dao.ReservationKey{}, fmt.Errorf("proxy %q has invalid remote port %d", base.Name, remotePort)
	}
	return dao.ReservationKey{ServerID: serverID, Protocol: protocol, RemotePort: uint32(remotePort)}, nil
}

// updatePhysicalKeyOfRow parses a proxy_config row's typed content into its
// physical reservation key. An unparseable or non-tcp/udp old rule fails
// closed (legacy non-port rules are read-only, spec §1.2.5).
func updatePhysicalKeyOfRow(row *models.ProxyConfig) (dao.ReservationKey, error) {
	typed, err := row.GetTypedProxyConfig()
	if err != nil {
		return dao.ReservationKey{}, fmt.Errorf("old rule content is not a parseable typed config: %v", err)
	}
	return updateTypedPhysicalKey(row.ServerID, typed)
}

// updateReservationPlan is the outcome of the mode decision (design §3):
// the reservation mode plus the physical keys the commit must migrate.
type updateReservationPlan struct {
	mode                   dao.ReservationMode
	oldKey                 dao.ReservationKey
	newKey                 dao.ReservationKey
	oldReservationExpected bool
	sameKey                bool
}

// reservationCommittedOwnedBy reports whether the row is a committed lease
// owned by the given proxy row id.
func reservationCommittedOwnedBy(row *models.PortReservation, proxyID uint) bool {
	return row.Status == models.ReservationStatusCommitted &&
		row.OwnerProxyID != nil && *row.OwnerProxyID == proxyID
}

// planUpdateReservation decides the reservation mode and acquires what the
// mode needs (design §3, fail closed everywhere):
//
//	same key: committed row owned by old -> SameKeyOwned (NO TryAcquire);
//	          no row (legacy world) -> SameKeyPending (TryAcquire current tx);
//	          any other row -> RESERVATION_CONFLICT.
//	different: old committed owned by old -> OldReservationExpected=true;
//	           no old row -> false; any other old row -> conflict.
//	           ANY tenant-scoped new-key row -> conflict; else TryAcquire
//	           (cross-tenant collisions are caught by the GLOBAL unique
//	           index inside TryAcquire).
//
// TryAcquire requires the transaction row to EXIST and be pending — the
// caller creates it first.
func (uc *UpdateTransactionCoordinator) planUpdateReservation(c *app.Context, userInfo models.UserInfo, oldProxyID uint, txID string, oldKey, newKey dao.ReservationKey, now time.Time) (*updateReservationPlan, error) {
	d := uc.deps
	plan := &updateReservationPlan{oldKey: oldKey, newKey: newKey, sameKey: oldKey == newKey}

	if plan.sameKey {
		rows, err := d.Query.GetByServerFilter(c, userInfo, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
		if err != nil {
			return nil, err
		}
		switch {
		case len(rows) == 0:
			// legacy world: this origin owns the port without any lease row;
			// acquire a fresh pending lease for THIS transaction at the key.
			if _, err := d.Res.TryAcquire(c, userInfo, newKey.ServerID, newKey.Protocol, newKey.RemotePort, txID, d.Lease, now); err != nil {
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

	// different keys
	oldRows, err := d.Query.GetByServerFilter(c, userInfo, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	if err != nil {
		return nil, err
	}
	switch {
	case len(oldRows) == 0:
		plan.oldReservationExpected = false
	case len(oldRows) == 1 && reservationCommittedOwnedBy(oldRows[0], oldProxyID):
		plan.oldReservationExpected = true
	default:
		return nil, defs.ErrReservationConflict
	}
	newRows, err := d.Query.GetByServerFilter(c, userInfo, newKey.ServerID, newKey.Protocol, newKey.RemotePort)
	if err != nil {
		return nil, err
	}
	if len(newRows) > 0 {
		return nil, defs.ErrReservationConflict
	}
	if _, err := d.Res.TryAcquire(c, userInfo, newKey.ServerID, newKey.Protocol, newKey.RemotePort, txID, d.Lease, now); err != nil {
		return nil, err
	}
	plan.mode = dao.ReservationModeDifferentKey
	return plan, nil
}

// resolveOriginAndLock resolves the ORIGIN client id for a request that may
// address the origin itself or one of its shadow children, acquires the
// (tenant, origin) keyed lock on the SAME injected locker instance the
// legacy writers and SafeCreate use, and re-resolves UNDER the lock: if the
// mapping changed while waiting, the key is released and the cycle retried
// (bounded). The caller MUST defer the unlock exactly once. The whole flow
// — idempotency replay, plan, snapshots, apply, verify, commit AND
// compensation — runs under this lock.
func (uc *UpdateTransactionCoordinator) resolveOriginAndLock(c *app.Context, userInfo models.UserInfo, reqClientID string) (string, func(), error) {
	d := uc.deps
	if d.OriginLocker == nil {
		panic("proxy: UpdateTransactionCoordinator.OriginLocker is nil — same-origin SafeUpdate serialization is disabled")
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

// SafeUpdateProxy is the transactional safe update (spec §8.1): the ACTIVE
// branch (Apply + dual-verify + compensation) for live rules and the
// STOPPED branch (zero-Apply in-place metadata update, A4c) for stopped
// rules. It returns (resp, nil) for structured business failures and
// (nil, err) for internal errors. The /update_config route delegates to
// this entry (A4c); the whole flow runs under the single (tenant, origin)
// lock acquired here.
func (uc *UpdateTransactionCoordinator) SafeUpdateProxy(c *app.Context, req *pb.UpdateProxyConfigRequest) (*pb.UpdateProxyConfigResponse, error) {
	d := uc.deps
	userInfo := common.GetUserInfo(c)
	if userInfo == nil {
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, "authentication required"), nil
	}

	// ---- a. field validation ----
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetName()) == 0 || len(req.GetConfig()) == 0 {
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, "client_id, server_id, name and config are required"), nil
	}

	// ---- b. resolve origin + lock (per-(tenant, origin) serialization).
	// Every DB read below happens UNDER the lock; nothing from before the
	// lock is trusted. ----
	origin, unlock, err := uc.resolveOriginAndLock(c, userInfo, req.GetClientId())
	if err != nil {
		return nil, err
	}
	defer unlock()

	// ---- c. idempotency key / request hash replay (gate 7) ----
	idemKey := req.GetIdempotencyKey()
	if !isValidIdempotencyKey(idemKey) {
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed,
			"idempotency_key is required and must be a UUID v4"), nil
	}
	reqHash := d.HashRequest(req, origin)
	existing, err := d.Tx.FindByIdempotencyKey(c, userInfo, idemKey)
	if err == nil {
		return uc.replayUpdate(existing, reqHash)
	}
	if !errors.Is(err, defs.ErrTransactionNotFound) {
		return nil, err
	}

	// ---- d. parse candidate (exactly one tcp/udp rule) ----
	cfgs, err := utils.LoadProxiesFromContent(req.GetConfig())
	if err != nil {
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot parse candidate config: %v", err)), nil
	}
	if len(cfgs) != 1 {
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("exactly one proxy rule is required, got %d", len(cfgs))), nil
	}
	target, err := targetFromProxyConfig(cfgs[0])
	if err != nil {
		code := defs.ErrorCodeConfigValidationFailed
		if errors.Is(err, defs.ErrUnsupportedProxyType) {
			code = defs.ErrorCodeUnsupportedProxyType
		}
		return invalidUpdateResp(code, err.Error()), nil
	}

	// ---- e. resolve the OLD rule under the lock (0 rows / ambiguous =
	// CONFIG_VALIDATION_FAILED) and its real typed physical key. ----
	old, err := d.Query.GetProxyConfigByOriginOrClientAndName(c, userInfo, origin, req.GetServerId(), req.GetName())
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("proxy rule %q not found", req.GetName())), nil
		}
		if errors.Is(err, defs.ErrAmbiguousProxyTarget) {
			return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, "ambiguous proxy target: more than one row matches the old rule"), nil
		}
		return nil, err
	}
	oldKey, err := updatePhysicalKeyOfRow(old)
	if err != nil {
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot resolve the old rule's physical key: %v", err)), nil
	}

	// ---- f. rename pre-check (fast fail; runs for BOTH branches; the
	// commit's S2 guard stays the authoritative TOCTOU re-check) ----
	if target.ProxyName != old.Name {
		if _, clashErr := d.Query.GetProxyConfigByOriginOrClientAndName(c, userInfo, origin, req.GetServerId(), target.ProxyName); clashErr == nil || errors.Is(clashErr, defs.ErrAmbiguousProxyTarget) {
			return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("proxy name %q is already taken", target.ProxyName)), nil
		} else if !errors.Is(clashErr, gorm.ErrRecordNotFound) {
			return nil, clashErr
		}
	}

	// ---- f2. branch point (A4c): a stopped rule routes to the STOPPED
	// branch (zero-Apply in-place metadata update). Only the child-shaped
	// rule is accepted: a stopped rule that still lives on the
	// never-shadowed origin (client_id==origin or an empty origin link)
	// is the unsafe first-shadow state the stopped branch may never
	// convert (it must not create a child) — CONFIG_VALIDATION_FAILED
	// with zero tx / zero agent contact. ----
	if old.Stopped {
		if old.ClientID == "" || old.ClientID == origin || old.OriginClientID == "" || old.OriginClientID != origin {
			return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed,
				"updating a stopped rule that still lives on the unshadowed origin is not supported (the stopped branch may not create a child)"), nil
		}
		return uc.safeUpdateStoppedLocked(c, userInfo, req, origin, old, oldKey, target, reqHash, idemKey, cfgs[0])
	}

	// ---- g. create the transaction (pending) FIRST: every later DB write
	// happens under a live transaction row, so no failure can silently
	// leave unrecoverable side effects. ProxyID is bound to the OLD rule
	// from birth (spec §6.2 update semantics). ----
	txID := uuid.NewString()
	now := d.Now()
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

	// ---- h. reservation mode decision + acquisition (tx must already be
	// pending). ANY planning/acquire failure -> failed + release +
	// structured RESERVATION_CONFLICT. ----
	newKey := dao.ReservationKey{ServerID: req.GetServerId(), Protocol: target.Protocol, RemotePort: target.RemotePort}
	resPlan, err := uc.planUpdateReservation(c, userInfo, old.ID, txID, oldKey, newKey, now)
	if err != nil {
		msg := fmt.Sprintf("port %d/%s on %s cannot be reserved: %v", target.RemotePort, target.Protocol, req.GetServerId(), err)
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeReservationConflict, msg, "")
		return invalidUpdateResp(defs.ErrorCodeReservationConflict, msg), nil
	}

	// ---- i. read-only shadow plan (zero config writes before the commit) ----
	plan, err := PrepareShadowPlan(c, origin, req.GetServerId())
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve client, id: [%s]", origin)
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot resolve client: %v", err), "")
		return nil, err
	}

	// ---- j. build the candidate on the plan's WORKING child. The old rule
	// must appear EXACTLY ONCE in the plan config (first-shadow: the origin
	// config the child inherits; existing: the child's own config) and its
	// real typed physical key must equal the old reservation key — missing,
	// duplicated or drifted rules fail closed. ----
	var oldCfg *v1.ClientConfig
	if len(plan.Child.ConfigContent) == 0 {
		oldCfg = &v1.ClientConfig{}
	} else {
		oldCfg, err = plan.Child.GetConfigContent()
		if err != nil {
			_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, "cannot read the current client config", "")
			return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, "cannot read the current client config"), nil
		}
	}
	oldMatches := lo.Filter(oldCfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name == old.Name
	})
	if len(oldMatches) != 1 {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed,
			fmt.Sprintf("old rule %q must appear exactly once in the client config (found %d)", old.Name, len(oldMatches)), "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed,
			fmt.Sprintf("old rule %q must appear exactly once in the client config (found %d)", old.Name, len(oldMatches))), nil
	}
	matchKey, kerr := updateTypedPhysicalKey(req.GetServerId(), oldMatches[0])
	if kerr != nil || matchKey != oldKey {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, "old rule physical key drifted from its reservation key", "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, "old rule physical key drifted from its reservation key"), nil
	}
	// filter the OLD name (never the candidate name) and append the candidate
	oldCfg.Proxies = lo.Filter(oldCfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name != old.Name
	})
	oldCfg.Proxies = append(oldCfg.Proxies, cfgs[0])
	if err := plan.Child.SetConfigContent(*oldCfg); err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	candidate, err := plan.Child.MarshalJSONConfig()
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	finalRaw, err := buildFrpcApplyConfig(c, plan.Child, req.GetServerId(), candidate)
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err), "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err)), nil
	}

	// the physical origin agent must be able to receive the update
	originCli, err := d.Query.GetClientByClientID(c, userInfo, origin)
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot get origin client: %v", err), "")
		return nil, err
	}
	if originCli.ClientEntity.Stopped {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error(), "")
		return invalidUpdateResp(defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error()), nil
	}

	// ---- k. before snapshots: client-before (the config the agent runs
	// NOW, from the plan's IMMUTABLE before copies — never the mutated
	// working child) + proxy-before (versioned encrypted envelope of the
	// old rule). Any save failure is a PRE-APPLY failure: failed + release,
	// zero agent contact. ----
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot load snapshot key")
		_ = uc.failUpdateTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	beforeClientNode := plan.Child.ClientID
	beforeEntity := plan.ChildBefore
	if plan.FirstShadow {
		beforeClientNode = origin
		beforeEntity = plan.OriginBefore
	}
	beforeClient, err := beforeEntity.MarshalJSONConfig()
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err), "")
		return invalidUpdateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot capture the before config: %v", err)), nil
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
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, beforeClientNode, beforeClient, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the client before snapshot")
		_ = uc.failUpdateTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	if _, err := d.Snap.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(old.ID), 10), proxyBeforePlain, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save the proxy before snapshot")
		_ = uc.failUpdateTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	// the after proxy envelope is built NOW (pre-apply): the final row id is
	// unknown for a rename and is forced by the DAO node_id at commit time.
	proxyAfterPlain, err := buildProxyAfterEnvelope(target, req.GetServerId(), plan.Child.ClientID, origin, cfgs[0])
	if err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot build the proxy after envelope: %v", err), "")
		return nil, err
	}

	// ---- l. preflight (mode-aware, design §3):
	// same physical key -> owner-proof: DB evidence (the reservation mode)
	// AND VerifyApplied(oldKey) proving the embedded frps holds the port —
	// NO bind probe, and a foreign listener fails; then the client gate.
	// different key -> full server bind probe on the NEW key, then the
	// client gate. The client request always addresses the PHYSICAL origin
	// agent, never the logical child. ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot advance to preflight: %v", err), "")
		return nil, err
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
	if resPlan.sameKey {
		proofCtx, cancelProof := context.WithTimeout(c, d.PreflightTimeout)
		proofErr := d.Verifier.VerifyApplied(proofCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
		cancelProof()
		if proofErr != nil {
			msg := fmt.Sprintf("remote port is not served by the panel frps: %v", proofErr)
			_ = uc.failUpdateTx(c, userInfo, txID, defs.ErrorCodePortRemoteInUse, msg, "")
			return invalidUpdateResp(defs.ErrorCodePortRemoteInUse, msg), nil
		}
	} else {
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

	// ---- m. applying: synchronous apply with ACK (directed switch for a
	// first shadow: Target=child, Replace=origin) ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
		_ = uc.failUpdateTx(c, userInfo, txID, "", fmt.Sprintf("cannot advance to applying: %v", err), "")
		return nil, err
	}
	replaceID := ""
	if plan.FirstShadow {
		replaceID = origin
	}
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	applyCode, applyErr := d.Apply.Apply(applyCtx, c, ApplyRequest{
		PhysicalAgentID: origin,
		TargetClientID:  plan.Child.ClientID,
		ReplaceClientID: replaceID,
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
		compensated := uc.compensateUpdate(c, userInfo, txID, req.GetServerId(), plan, oldKey, newKey, !resPlan.sameKey, beforeClient, applyCode, msg)
		if compensated {
			return invalidUpdateResp(applyCode, msg), nil
		}
		return invalidUpdateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- n. verifying: the new key must be served by the embedded frps ----
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
		// The candidate may already be applied. This is a control-plane DB
		// transition failure (not an Agent rejection), so compensate and honor
		// its terminal result exactly like verify/commit failures.
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance applied update to verifying: %v", err)
		compensated := uc.compensateUpdate(c, userInfo, txID, req.GetServerId(), plan,
			oldKey, newKey, !resPlan.sameKey, beforeClient, code, msg)
		if compensated {
			return invalidUpdateResp(code, msg), nil
		}
		return invalidUpdateResp(defs.ErrorCodeRollbackFailed,
			fmt.Sprintf("%s (compensation failed)", msg)), nil
	}
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyApplied(verifyCtx, c, newKey.ServerID, newKey.Protocol, newKey.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		compensated := uc.compensateUpdate(c, userInfo, txID, req.GetServerId(), plan, oldKey, newKey, !resPlan.sameKey, beforeClient, defs.ErrorCodeVerifyFailed, msg)
		if compensated {
			return invalidUpdateResp(defs.ErrorCodeVerifyFailed, msg), nil
		}
		return invalidUpdateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- o. single-transaction commit (active branch: shadow conversion,
	// child persistence, rebuild, reservation migration, after snapshots,
	// update audit) ----
	commitIn := &dao.CommitUpdateInput{
		TxID:           txID,
		OldProxyID:     old.ID,
		OldName:        old.Name,
		OldServerID:    req.GetServerId(),
		OriginClientID: origin,
		Branch:         dao.UpdateBranchActive,
		Client:         plan.Child,
		Shadow: &dao.ShadowCommit{
			FirstShadow:  plan.FirstShadow,
			OriginAfter:  plan.OriginAfter,
			ChildCreated: plan.ChildCreated,
		},
		ProxyName: target.ProxyName,
		Reservation: dao.ReservationStep{
			Mode:                   resPlan.mode,
			OldKey:                 resPlan.oldKey,
			NewKey:                 resPlan.newKey,
			OldReservationExpected: resPlan.oldReservationExpected,
		},
		ClientAfterSnapshot: dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeClient,
			NodeID:    plan.Child.ClientID,
			Plaintext: finalRaw,
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
			Summary:       fmt.Sprintf("updated proxy %q (%s port %d) on %s for %s", target.ProxyName, target.Protocol, target.RemotePort, req.GetServerId(), origin),
			NodeID:        plan.Child.ClientID,
			Success:       true,
		},
	}
	if _, err := d.Tx.CommitUpdate(c, userInfo, commitIn); err != nil {
		// the config IS applied and verified but the DB commit failed: the
		// stable CONFIG_COMMIT_FAILED code (never APPLY_REJECTED/
		// VERIFY_FAILED — the agent side is fine) and full compensation.
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		compensated := uc.compensateUpdate(c, userInfo, txID, req.GetServerId(), plan, oldKey, newKey, !resPlan.sameKey, beforeClient, code, msg)
		if compensated {
			return invalidUpdateResp(code, msg), nil
		}
		return invalidUpdateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	return &pb.UpdateProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}

// buildProxyAfterEnvelope builds the candidate's versioned typed envelope
// for the AFTER snapshot. The final proxy row id is unknown for a rename
// and is intentionally NOT prefilled — the DAO forces it into node_id.
func buildProxyAfterEnvelope(target preflightTarget, serverID, childID, origin string, candidate v1.TypedProxyConfig) ([]byte, error) {
	typedJSON, err := candidate.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return marshalProxyEnvelope(updateProxyEnvelope{
		Name: target.ProxyName, ServerID: serverID,
		ClientID: childID, OriginClientID: origin,
		Stopped: false, Typed: typedJSON,
	})
}

// replayUpdate implements the idempotency replay semantics (gate 7):
// committed -> idempotent success; different hash -> IDEMPOTENCY_CONFLICT;
// terminal failure -> the original failure result; in-flight -> stable
// IDEMPOTENCY_IN_FLIGHT.
func (uc *UpdateTransactionCoordinator) replayUpdate(existing *models.ChangeTransaction, reqHash string) (*pb.UpdateProxyConfigResponse, error) {
	if existing.RequestHash != reqHash {
		return invalidUpdateResp(defs.ErrorCodeIdempotencyConflict,
			"idempotency key was already used with a different request"), nil
	}
	switch existing.Status {
	case models.TxStatusCommitted:
		return &pb.UpdateProxyConfigResponse{
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
		return invalidUpdateResp(code, msg), nil
	default:
		return invalidUpdateResp(defs.ErrorCodeIdempotencyInFlight,
			"a request with this idempotency key is already in progress"), nil
	}
}

// failUpdateTx marks the transaction failed (pre-apply failures) and audits
// it. The pending reservation owned by the transaction is released.
func (uc *UpdateTransactionCoordinator) failUpdateTx(c *app.Context, userInfo models.UserInfo, txID, code, msg, evidence string) error {
	audit := &models.AuditEvent{
		TransactionID: txID,
		EventType:     models.AuditEventTypeUpdate,
		Summary:       "update failed: " + msg,
		ErrorCode:     code,
		Success:       false,
	}
	return uc.deps.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:               txID,
		Status:             models.TxStatusFailed,
		ErrorCode:          code,
		ErrorMessage:       msg,
		EvidenceJSON:       evidence,
		ReleaseReservation: true,
		AuditEvent:         audit,
	})
}

// compensateUpdate performs the automatic rollback after the apply point
// (spec §8.2): re-apply the immutable before config with the TRUE INVERSE
// switch (first shadow: Target=origin, Replace=child; existing:
// Target=child, Replace=""), verify the OLD state is applied again and —
// for a DIFFERENT key — that the NEW port was released. Success moves the
// transaction to rolled_back and releases ONLY the current tx's pending
// lease (SameKeyOwned has none; the old committed lease's transaction_id
// is never rewritten, so it can never be deleted by txID). Any step
// failure moves the transaction to rollback_failed and RETAINS the current
// pending lease for manual intervention. Returns true when the rollback
// completed. Evidence never contains config/secret material.
func (uc *UpdateTransactionCoordinator) compensateUpdate(
	c *app.Context,
	userInfo models.UserInfo,
	txID, serverID string,
	plan *ShadowPlan,
	oldKey, newKey dao.ReservationKey,
	differentKey bool,
	beforeClient []byte,
	origCode, origMsg string,
) bool {
	d := uc.deps
	logger.Logger(c).Warnf("compensating update transaction [%s]: %s", txID, origMsg)

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
			logger.Logger(c).WithError(err).Errorf("cannot persist rollback_failed for update transaction [%s]", txID)
		}
		return false
	}

	// 1. applying/verifying -> rolling_back
	if err := d.Tx.UpdateStatus(c, userInfo, txID, models.TxStatusRollingBack, "", ""); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot move transaction [%s] to rolling_back", txID)
		return false
	}

	// 2. build the inverse apply from the plan's IMMUTABLE before state
	restore := ApplyRequest{
		PhysicalAgentID: plan.Child.OriginClientID,
		ServerID:        serverID,
	}
	restoreEntity := plan.ChildBefore
	if plan.FirstShadow {
		restore.TargetClientID = plan.OriginBefore.ClientID
		restore.ReplaceClientID = plan.Child.ClientID
		restoreEntity = plan.OriginBefore
	} else {
		restore.TargetClientID = plan.Child.ClientID
	}
	beforeFinal, err := buildFrpcApplyConfig(c, restoreEntity, serverID, beforeClient)
	if err != nil {
		return failRollback("compensate-build", defs.ErrorCodeRollbackFailed, fmt.Sprintf("compensation build failed: %v", err))
	}
	restore.Config = beforeFinal

	// 3. re-apply the before config (synchronous ACK, ApplyTimeout budget)
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	_, applyErr := d.Apply.Apply(applyCtx, c, restore)
	cancelApply()
	if applyErr != nil {
		return failRollback("compensate-apply", defs.ErrorCodeRollbackFailed, fmt.Sprintf("compensation apply failed: %v", applyErr))
	}

	// 4. verify the OLD state is applied again (the restored rule holds the
	// port via our embedded frps) — the reverse direction of create's
	// VerifyReleased.
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	oldErr := d.Verifier.VerifyApplied(verifyCtx, c, oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort)
	cancelVerify()
	if oldErr != nil {
		return failRollback("compensate-verify-old", defs.ErrorCodeVerifyFailed, fmt.Sprintf("compensation verify old state failed: %v", oldErr))
	}

	// 5. different key: the NEW port must be released again
	if differentKey {
		relCtx, cancelRel := context.WithTimeout(c, d.VerifyTimeout)
		relErr := d.Verifier.VerifyReleased(relCtx, c, newKey.ServerID, newKey.Protocol, newKey.RemotePort)
		cancelRel()
		if relErr != nil {
			return failRollback("compensate-verify-new-released", defs.ErrorCodeVerifyFailed, fmt.Sprintf("compensation verify new port released failed: %v", relErr))
		}
	}

	// 6. atomically mark rolled_back + release ONLY the current tx's pending
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
