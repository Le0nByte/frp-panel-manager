package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
)

// ---------------------------------------------------------------------------
// Small dependency interfaces (gate 5: struct + small injected interfaces,
// no global stubs). The defaults are REAL implementations backed by the DAO,
// the RPC layer and the configured key file; tests inject fakes for the
// RPC-bound parts while keeping the DAO real.
//
// Every method takes the *app.Context explicitly: the DAO adapters need it to
// reach the DB manager, and tests hand over their fake app context.
// ---------------------------------------------------------------------------

// TxStore is the transaction persistence surface SafeCreate needs.
type TxStore interface {
	FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error
	UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	CommitCreate(c *app.Context, userInfo models.UserInfo, in *dao.CommitCreateInput) (uint, error)
	FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error
}

// ResStore is the port reservation surface.
type ResStore interface {
	TryAcquire(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32, txID string, lease time.Duration, now time.Time) (*models.PortReservation, error)
	Release(c *app.Context, userInfo models.UserInfo, txID string) error
}

// SnapStore is the encrypted snapshot surface.
type SnapStore interface {
	SaveEncrypted(c *app.Context, userInfo models.UserInfo, txID, side, nodeType, nodeID string, plaintext, key []byte) (*models.ConfigSnapshot, error)
}

// PreflightRunner runs the two-sided gate (the production default is the
// package Preflight function) and, since A4a, each single side of it.
// RunServerProbe / RunClient contact exactly one agent each; callers
// compose the gates explicitly (SafeUpdate) instead of paying for the
// other side.
type PreflightRunner interface {
	Run(c *app.Context, req *pb.PreflightRequest) (*pb.PreflightResponse, error)
	RunClient(c *app.Context, req *pb.PreflightRequest) (*PreflightGateResult, error)
	RunServerProbe(c *app.Context, req *pb.PreflightRequest) (*PreflightGateResult, error)
}

// daoStore adapts dao.NewQuery/NewMutation to the coordinator interfaces
// using the context passed per call.
type daoStore struct{}

func newDAOStore() *daoStore { return &daoStore{} }

func (s *daoStore) FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error) {
	return dao.NewQuery(c).FindByIdempotencyKey(userInfo, key)
}
func (s *daoStore) Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error {
	return dao.NewMutation(c).Create(userInfo, tx)
}
func (s *daoStore) UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error {
	return dao.NewMutation(c).UpdateStatus(userInfo, txID, status, errorCode, errorMessage)
}
func (s *daoStore) CommitCreate(c *app.Context, userInfo models.UserInfo, in *dao.CommitCreateInput) (uint, error) {
	return dao.NewMutation(c).CommitCreate(userInfo, in)
}
func (s *daoStore) FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error {
	return dao.NewMutation(c).FailTransaction(userInfo, in)
}
func (s *daoStore) TryAcquire(c *app.Context, userInfo models.UserInfo, serverID, protocol string, port uint32, txID string, lease time.Duration, now time.Time) (*models.PortReservation, error) {
	return dao.NewMutation(c).TryAcquire(userInfo, serverID, protocol, port, txID, lease, now)
}
func (s *daoStore) Release(c *app.Context, userInfo models.UserInfo, txID string) error {
	return dao.NewMutation(c).Release(userInfo, txID)
}
func (s *daoStore) SaveEncrypted(c *app.Context, userInfo models.UserInfo, txID, side, nodeType, nodeID string, plaintext, key []byte) (*models.ConfigSnapshot, error) {
	return dao.NewMutation(c).SaveEncrypted(userInfo, txID, side, nodeType, nodeID, plaintext, key)
}

// preflightRunner adapts the package Preflight function.
type preflightRunner struct{}

func (preflightRunner) Run(c *app.Context, req *pb.PreflightRequest) (*pb.PreflightResponse, error) {
	return Preflight(c, req)
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

const (
	defaultReservationLease = 120 * time.Second
	defaultApplyTimeout     = 15 * time.Second
	defaultVerifyTimeout    = 30 * time.Second
	defaultVerifyAttempts   = 30 // spec §8.4: poll every 1s, at most 30 tries, inside the 30s ctx budget
	defaultVerifyInterval   = 1 * time.Second
)

// CoordinatorDeps carries every external dependency of the safe create flow.
type CoordinatorDeps struct {
	TxStore        TxStore
	ResStore       ResStore
	SnapStore      SnapStore
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
	HashRequest    func(req *pb.CreateProxyConfigRequest) string
}

func defaultDeps() CoordinatorDeps {
	return CoordinatorDeps{
		TxStore:        newDAOStore(),
		ResStore:       newDAOStore(),
		SnapStore:      newDAOStore(),
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
		HashRequest:    hashCreateRequest,
	}
}

// TransactionCoordinator orchestrates the safe change lifecycle (spec §8):
// preflight -> before snapshot -> synchronous apply with ACK -> verify ->
// single-transaction commit, with automatic compensation on every failure
// after the apply point.
type TransactionCoordinator struct {
	deps CoordinatorDeps
}

// NewDefaultCoordinator returns a coordinator wired to the REAL
// implementations (DAO, RPC, configured key file). Tests build their own
// coordinator with injected deps instead of stubbing globals.
func NewDefaultCoordinator() *TransactionCoordinator {
	return &TransactionCoordinator{deps: defaultDeps()}
}

// hashCreateRequest computes the request hash for idempotency replay
// (gate 1/7): SHA-256 over the SEMANTIC request fields (client, server,
// config, overwrite). The idempotency key itself is deliberately excluded —
// it is the lookup dimension, not part of the request identity.
func hashCreateRequest(req *pb.CreateProxyConfigRequest) string {
	h := sha256.New()
	h.Write([]byte(req.GetClientId()))
	h.Write([]byte{0})
	h.Write([]byte(req.GetServerId()))
	h.Write([]byte{0})
	h.Write(req.GetConfig())
	h.Write([]byte{0})
	if req.GetOverwrite() {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// resolveCreateOriginAndLock resolves the ORIGIN client id for a create
// request that may address the origin itself or one of its shadow
// children, acquires the (tenant, origin) keyed lock on the SAME injected
// locker instance the legacy writers and the safe update use, and
// re-resolves UNDER the lock: if the mapping changed while waiting for the
// key, the lock is released and the resolve+lock cycle retried (bounded,
// fail-closed) — the caller never operates under a stale key. The caller
// MUST defer the unlock exactly once. The whole flow — idempotency replay,
// plan, snapshots, apply, verify, commit AND compensation — runs under
// this lock. Mirrors UpdateTransactionCoordinator.resolveOriginAndLock.
func (tc *TransactionCoordinator) resolveCreateOriginAndLock(c *app.Context, userInfo models.UserInfo, reqClientID string) (string, func(), error) {
	d := tc.deps
	if d.OriginLocker == nil {
		panic("proxy: TransactionCoordinator.OriginLocker is nil — same-origin SafeCreate serialization is disabled")
	}
	const maxOriginLockAttempts = 3
	for attempt := 0; attempt < maxOriginLockAttempts; attempt++ {
		resolved, err := dao.NewQuery(c).GetOriginClientID(userInfo, reqClientID)
		if err != nil {
			return "", nil, err
		}
		key := originLockKey(userInfo.GetTenantID(), resolved)
		d.OriginLocker.Lock(key)
		reResolved, err := dao.NewQuery(c).GetOriginClientID(userInfo, reqClientID)
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

// SafeCreateProxy is the transactional create (spec §8.1). It returns
// (resp, nil) for structured business failures (the response carries the
// error code/message) and (nil, err) for internal errors.
func (tc *TransactionCoordinator) SafeCreateProxy(c *app.Context, req *pb.CreateProxyConfigRequest) (*pb.CreateProxyConfigResponse, error) {
	d := tc.deps
	userInfo := common.GetUserInfo(c)
	if userInfo == nil {
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, "authentication required"), nil
	}

	// ---- a. validate request + parse candidate (TCP/UDP only) ----
	if len(req.GetClientId()) == 0 || len(req.GetServerId()) == 0 || len(req.GetConfig()) == 0 {
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, "client_id, server_id and config are required"), nil
	}
	cfgs, err := utils.LoadProxiesFromContent(req.GetConfig())
	if err != nil {
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot parse candidate config: %v", err)), nil
	}
	if len(cfgs) != 1 {
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("exactly one proxy rule is required, got %d", len(cfgs))), nil
	}
	target, err := targetFromProxyConfig(cfgs[0])
	if err != nil {
		code := defs.ErrorCodeConfigValidationFailed
		if errors.Is(err, defs.ErrUnsupportedProxyType) {
			code = defs.ErrorCodeUnsupportedProxyType
		}
		return invalidCreateResp(code, err.Error()), nil
	}

	// ---- a2. idempotency key format (gate 7, fail-closed BEFORE any lock
	// or resolution): the key is mandatory and must be a UUID v4 — an
	// empty or malformed key would either occupy the unique idempotency
	// index (blocking legitimate retries) or collide across unrelated
	// requests. Required-field validation precedes the canonical-origin
	// resolution. ----
	idemKey := req.GetIdempotencyKey()
	if !isValidIdempotencyKey(idemKey) {
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed,
			"idempotency_key is required and must be a UUID v4"), nil
	}

	// ---- a3. resolve the CANONICAL ORIGIN + per-(tenant, origin)
	// serialization. The request may address the origin itself or one of
	// its shadow children; EVERY dimension that must be stable across
	// retries — the lock key, the idempotency hash, the shadow plan, the
	// physical agent id, the audit node — is anchored on the resolved
	// origin, NEVER the raw request client id (D1 addendum P0). A
	// child-addressed request that locked (tenant, child) would split from
	// the legacy writers' (tenant, origin) key and, by planning against
	// the child as if it were an origin, spawn a grandchild (child@N).
	// The per-origin lock spans the WHOLE flow — idempotency replay, plan,
	// before snapshot, apply, verify, commit AND compensation — so the
	// second request re-plans against the winner's committed state and
	// appends to the existing child instead of colliding. Nothing derived
	// from the shadow count is cached outside the lock. Different
	// origins/tenants proceed in parallel; see OriginLocker for the
	// single-Master/HA caveat. The resolution is re-run UNDER the lock:
	// if the mapping changed while waiting, release and retry (bounded,
	// fail-closed). ----
	originID, unlock, err := tc.resolveCreateOriginAndLock(c, userInfo, req.GetClientId())
	if err != nil {
		return nil, err
	}
	defer unlock()

	// the CANONICAL request: a shallow copy of the caller's request with
	// only ClientId replaced by the resolved origin (the caller's request
	// object is never mutated). Everything downstream — hash, redacted
	// summary, shadow plan, preflight physical id, apply target, audit
	// node — reads the canonical id; server/config/overwrite/idem stay
	// untouched.
	canonical := &pb.CreateProxyConfigRequest{
		ClientId:       lo.ToPtr(originID),
		ServerId:       req.ServerId,
		Config:         req.Config,
		Overwrite:      req.Overwrite,
		IdempotencyKey: req.IdempotencyKey,
		PreflightToken: req.PreflightToken,
	}

	// ---- b. idempotency key / request hash replay (gate 7): the lookup
	// runs on the CANONICAL hash and always before any plan or DB write;
	// a committed replay must not re-query rules or re-apply. ----
	reqHash := d.HashRequest(canonical)
	existing, err := d.TxStore.FindByIdempotencyKey(c, userInfo, idemKey)
	if err == nil {
		return tc.replayCreate(existing, reqHash)
	}
	if !errors.Is(err, defs.ErrTransactionNotFound) {
		return nil, err
	}

	// ---- c. create the transaction (pending) FIRST: every later DB write
	// (shadow scaffolding included) happens under a live transaction row, so
	// no failure can silently leave unrecoverable side effects. ----
	txID := uuid.NewString()
	now := d.Now()
	redacted, err := redactCreateRequest(canonical, target)
	if err != nil {
		return nil, err
	}
	tx := &models.ChangeTransaction{
		TransactionID:   txID,
		IdempotencyKey:  idemKey,
		RequestHash:     reqHash,
		Operation:       models.TxOperationCreate,
		Status:          models.TxStatusPending,
		RequestRedacted: redacted,
		StartedAt:       now,
	}
	if err := d.TxStore.Create(c, userInfo, tx); err != nil {
		// concurrent request with the same key won the insert race
		if dao.IsUniqueViolation(err) {
			if existing, ferr := d.TxStore.FindByIdempotencyKey(c, userInfo, idemKey); ferr == nil {
				return tc.replayCreate(existing, reqHash)
			}
		}
		return nil, err
	}

	// ---- d. reserve the public port (120s lease), fail fast: a conflicting
	// request performs no client writes at all ----
	if _, err := d.ResStore.TryAcquire(c, userInfo, canonical.GetServerId(), target.Protocol, target.RemotePort, txID, d.Lease, now); err != nil {
		if errors.Is(err, defs.ErrReservationConflict) {
			_ = tc.failTx(c, userInfo, txID, defs.ErrorCodeReservationConflict,
				fmt.Sprintf("port %d/%s on %s is already reserved by another change", target.RemotePort, target.Protocol, canonical.GetServerId()), "")
			return invalidCreateResp(defs.ErrorCodeReservationConflict,
				fmt.Sprintf("port %d/%s on %s is already reserved by another change", target.RemotePort, target.Protocol, canonical.GetServerId())), nil
		}
		_ = tc.failTx(c, userInfo, txID, "", fmt.Sprintf("cannot reserve port: %v", err), "")
		return nil, err
	}

	// ---- e. resolve target child + origin + server, tenant-scoped.
	// PrepareShadowPlan is READ-ONLY: the shadow conversion (origin flag,
	// child row, proxy migration) is deferred into the final CommitCreate
	// transaction, so every failure before the apply point leaves the
	// config tables byte-identical (spec §13.2 零配置变更). ----
	plan, err := PrepareShadowPlan(c, canonical.GetClientId(), canonical.GetServerId())
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve client, id: [%s]", canonical.GetClientId())
		_ = tc.failTx(c, userInfo, txID, "", fmt.Sprintf("cannot resolve client: %v", err), "")
		return nil, err
	}
	clientEntity := plan.Child
	if _, err := dao.NewQuery(c).GetServerByServerID(userInfo, canonical.GetServerId()); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get server, id: [%s]", canonical.GetServerId())
		_ = tc.failTx(c, userInfo, txID, "", fmt.Sprintf("cannot get server: %v", err), "")
		return nil, err
	}
	originEntity, err := dao.NewQuery(c).GetClientByFilter(userInfo, &models.ClientEntity{ClientID: canonical.GetClientId()}, nil)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get origin client, id: [%s]", canonical.GetClientId())
		_ = tc.failTx(c, userInfo, txID, "", fmt.Sprintf("cannot get origin client: %v", err), "")
		return nil, err
	}

	// capture the BEFORE config bytes now with a defensive copy from the
	// plan's IMMUTABLE before-state (never the working child, which the
	// candidate build below mutates). The before snapshot represents the
	// TARGET CHILD's pre-request config: for a first shadow that is the
	// origin config the child inherits (plan.OriginBefore); for an
	// existing child it is the child's own pre-request config
	// (plan.ChildBefore); a brand-new child of an already-shadowed
	// origin is empty.
	var beforePlain []byte
	if plan.FirstShadow {
		beforePlain = append([]byte(nil), plan.OriginBefore.ConfigContent...)
	} else {
		beforePlain = append([]byte(nil), plan.ChildBefore.ConfigContent...)
	}
	// ---- f. read + encrypt the before snapshot ----
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot load snapshot key")
		_ = tc.failTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}
	if _, err := d.SnapStore.SaveEncrypted(c, userInfo, txID, models.SnapshotSideBefore, models.SnapshotNodeTypeClient, clientEntity.ClientID, beforePlain, snapKey); err != nil {
		logger.Logger(c).WithError(err).Error("cannot save before snapshot")
		_ = tc.failTx(c, userInfo, txID, "", err.Error(), "")
		return nil, err
	}

	// ---- f. two-sided preflight (both gates strict) ----
	if err := d.TxStore.UpdateStatus(c, userInfo, txID, models.TxStatusPreflight, "", ""); err != nil {
		// pre-apply failure: the lease must not rot until it expires —
		// best-effort fail the transaction and release the reservation.
		_ = tc.failTx(c, userInfo, txID, "", fmt.Sprintf("cannot advance to preflight: %v", err), "")
		return nil, err
	}
	preResp, err := d.Preflight.Run(c, &pb.PreflightRequest{
		ClientId:   lo.ToPtr(canonical.GetClientId()),
		ServerId:   lo.ToPtr(canonical.GetServerId()),
		Protocol:   lo.ToPtr(target.Protocol),
		RemotePort: lo.ToPtr(target.RemotePort),
		LocalIp:    lo.ToPtr(target.LocalIP),
		LocalPort:  lo.ToPtr(target.LocalPort),
		ProxyName:  lo.ToPtr(target.ProxyName),
	})
	if err != nil {
		code := defs.ErrorCodePortScanUnavailable
		_ = tc.failTx(c, userInfo, txID, code, fmt.Sprintf("preflight transport failure: %v", err), "")
		return invalidCreateResp(code, fmt.Sprintf("preflight failed: %v", err)), nil
	}
	if !preResp.GetServerOk() {
		// server gate stays STRICT: the public port must be bindable.
		code := preResp.GetErrorCode()
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		evidence, _ := json.Marshal(preResp)
		_ = tc.failTx(c, userInfo, txID, code, preResp.GetErrorMessage(), string(evidence))
		return invalidCreateResp(code, preResp.GetErrorMessage()), nil
	}
	if !preResp.GetClientOk() && preResp.GetErrorCode() != defs.ErrorCodePortLocalNotListening {
		// client gate is WARN-graded: PORT_LOCAL_NOT_LISTENING (local port
		// not listening yet) is advisory and MUST NOT block creation. Any
		// other client failure (offline / unknown / conflict / transport)
		// still rejects.
		code := preResp.GetErrorCode()
		if code == "" {
			code = defs.ErrorCodePortScanUnavailable
		}
		evidence, _ := json.Marshal(preResp)
		_ = tc.failTx(c, userInfo, txID, code, preResp.GetErrorMessage(), string(evidence))
		return invalidCreateResp(code, preResp.GetErrorMessage()), nil
	}

	// ---- g. state -> applying ----
	if err := d.TxStore.UpdateStatus(c, userInfo, txID, models.TxStatusApplying, "", ""); err != nil {
		// still PRE-apply (nothing was sent to the agent yet): best-effort
		// fail the transaction and release the reservation instead of
		// letting the lease rot until it expires.
		_ = tc.failTx(c, userInfo, txID, "", fmt.Sprintf("cannot advance to applying: %v", err), "")
		return nil, err
	}

	// ---- h. build the candidate client config (replace same-name proxy) ----
	var oldCfg *v1.ClientConfig
	if len(clientEntity.ConfigContent) == 0 {
		// fresh child without a config yet: start from an empty client config
		oldCfg = &v1.ClientConfig{}
	} else {
		oldCfg, err = clientEntity.GetConfigContent()
		if err != nil {
			_ = tc.failTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, "cannot read current client config", "")
			return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, "cannot read current client config"), nil
		}
	}
	oldCfg.Proxies = lo.Filter(oldCfg.Proxies, func(p v1.TypedProxyConfig, _ int) bool {
		return p.GetBaseConfig().Name != target.ProxyName
	})
	oldCfg.Proxies = append(oldCfg.Proxies, cfgs[0])
	if err := clientEntity.SetConfigContent(*oldCfg); err != nil {
		_ = tc.failTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	candidate, err := clientEntity.MarshalJSONConfig()
	if err != nil {
		_ = tc.failTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, err.Error(), "")
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}
	finalRaw, err := buildFrpcApplyConfig(c, clientEntity, canonical.GetServerId(), candidate)
	if err != nil {
		_ = tc.failTx(c, userInfo, txID, defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err), "")
		return invalidCreateResp(defs.ErrorCodeConfigValidationFailed, fmt.Sprintf("cannot build candidate config: %v", err)), nil
	}

	// a stopped origin client cannot receive the update
	if originEntity.Stopped {
		_ = tc.failTx(c, userInfo, txID, defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error(), "")
		return invalidCreateResp(defs.ErrorCodeAgentOffline, defs.ErrClientStopped.Error()), nil
	}

	// ---- i. synchronous apply with ACK (15s budget). The directed
	// switch (round-8 I2): a first shadow REPLACES the origin handler on
	// the agent (Target=child, Replace=origin) so no duplicate/stale
	// origin registration survives; an existing-child update leaves every
	// other handler untouched (Replace empty). ----
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	replaceID := ""
	if plan.FirstShadow {
		replaceID = originEntity.ClientID
	}
	applyCode, applyErr := d.Apply.Apply(applyCtx, c, ApplyRequest{
		PhysicalAgentID: originEntity.ClientID,
		TargetClientID:  clientEntity.ClientID,
		ReplaceClientID: replaceID,
		ServerID:        canonical.GetServerId(),
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
		compensated := tc.compensate(c, userInfo, txID, canonical.GetServerId(), target, plan, beforePlain, applyCode, msg)
		if compensated {
			return invalidCreateResp(applyCode, msg), nil
		}
		return invalidCreateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- j. verify the remote port is now served by frps (30s budget,
	// 1s polls, ≤30 tries — spec §8.4) ----
	if err := d.TxStore.UpdateStatus(c, userInfo, txID, models.TxStatusVerifying, "", ""); err != nil {
		// state machine rejected the move (e.g. concurrent failure): the
		// config may already be applied — compensate to be safe.
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance applied create to verifying: %v", err)
		compensated := tc.compensate(c, userInfo, txID, canonical.GetServerId(), target, plan, beforePlain, code, msg)
		if compensated {
			return invalidCreateResp(code, msg), nil
		}
		return invalidCreateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyApplied(verifyCtx, c, canonical.GetServerId(), target.Protocol, target.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		msg := fmt.Sprintf("verify failed: %v", verifyErr)
		compensated := tc.compensate(c, userInfo, txID, canonical.GetServerId(), target, plan, beforePlain, defs.ErrorCodeVerifyFailed, msg)
		if compensated {
			return invalidCreateResp(defs.ErrorCodeVerifyFailed, msg), nil
		}
		return invalidCreateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- k. single-transaction commit (shadow conversion included) ----
	commitIn := &dao.CommitCreateInput{
		TxID:      txID,
		Client:    clientEntity,
		ProxyName: target.ProxyName,
		Shadow: &dao.ShadowCommit{
			FirstShadow:  plan.FirstShadow,
			OriginAfter:  plan.OriginAfter,
			ChildCreated: plan.ChildCreated,
		},
		Snapshot: dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeClient,
			NodeID:    clientEntity.ClientID,
			Plaintext: finalRaw,
			Key:       snapKey,
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: txID,
			EventType:     models.AuditEventTypeCommit,
			Summary:       fmt.Sprintf("created proxy %q (%s port %d) on %s for %s", target.ProxyName, target.Protocol, target.RemotePort, canonical.GetServerId(), canonical.GetClientId()),
			NodeID:        canonical.GetClientId(),
			Success:       true,
		},
	}
	if _, err := d.TxStore.CommitCreate(c, userInfo, commitIn); err != nil {
		// the config IS applied and verified, but the DB commit failed:
		// compensate (restore before config, verify old state, release).
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("commit failed: %v", err)
		compensated := tc.compensate(c, userInfo, txID, canonical.GetServerId(), target, plan, beforePlain, code, msg)
		if compensated {
			return invalidCreateResp(code, msg), nil
		}
		return invalidCreateResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- l. success ----
	return &pb.CreateProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}

// replayCreate implements the idempotency replay semantics (gate 7):
//   - same key + same hash + committed       -> idempotent success
//   - same key + different hash              -> stable IDEMPOTENCY_CONFLICT
//   - same key + same hash + terminal failure -> the original failure result
//   - same key + same hash + in-flight       -> stable in-flight error
func (tc *TransactionCoordinator) replayCreate(existing *models.ChangeTransaction, reqHash string) (*pb.CreateProxyConfigResponse, error) {
	if existing.RequestHash != reqHash {
		return invalidCreateResp(defs.ErrorCodeIdempotencyConflict,
			"idempotency key was already used with a different request"), nil
	}
	switch existing.Status {
	case models.TxStatusCommitted:
		return &pb.CreateProxyConfigResponse{
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
		return invalidCreateResp(code, msg), nil
	default:
		// pending/preflight/applying/verifying/rolling_back
		return invalidCreateResp(defs.ErrorCodeIdempotencyInFlight,
			"a request with this idempotency key is already in progress"), nil
	}
}

// failTx marks the transaction failed (pre-apply failures) and audits it.
func (tc *TransactionCoordinator) failTx(c *app.Context, userInfo models.UserInfo, txID, code, msg, evidence string) error {
	audit := &models.AuditEvent{
		TransactionID: txID,
		EventType:     models.AuditEventTypeCreate,
		Summary:       "create failed: " + msg,
		ErrorCode:     code,
		Success:       false,
	}
	return tc.deps.TxStore.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:               txID,
		Status:             models.TxStatusFailed,
		ErrorCode:          code,
		ErrorMessage:       msg,
		EvidenceJSON:       evidence,
		ReleaseReservation: true,
		AuditEvent:         audit,
	})
}

// compensationEvidence builds the evidence JSON for one compensation step
// (spec §8.2 / P2): it records the stage, the owner observation/error and
// the timestamp — NEVER config content or secrets (no request body, no
// snapshot plaintext, no credentials are ever included).
func compensationEvidence(stage string, ownerErr error, observedAt time.Time) string {
	ev := map[string]interface{}{
		"stage":       stage,
		"observed_at": observedAt.UTC().Format(time.RFC3339),
	}
	if ownerErr != nil {
		ev["owner_error"] = ownerErr.Error()
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return ""
	}
	return string(raw)
}

// compensate performs the automatic rollback after the apply point (spec
// §8.2): re-apply the before snapshot with ACK, verify the old state, then
// atomically release the reservation and mark the transaction rolled_back.
// When any compensation step fails the transaction is marked rollback_failed
// and the reservation is RETAINED (the rule stays blocked for manual
// intervention, spec §8.2). Every step writes evidence_json (stage +
// owner error/observation + time, never config/secret). Returns true when
// the rollback completed.
//
// The restore is the TRUE INVERSE of the forward apply (round-8 I2): a
// first shadow re-adds the ORIGIN handler with the ORIGIN's own
// identity/metadata (Target=origin, Replace=child) — the reverse of the
// forward Target=child/Replace=origin switch — while an existing-child
// restore re-applies the child's before config and touches no other
// handler (Replace empty). Both the payload bytes and the identity come
// from the plan's immutable before-copies, never from the mutated working
// child. The agent treats a missing replace target as a no-op, so the
// inverse stays idempotent when the forward apply timed out before its
// switch ever ran.
func (tc *TransactionCoordinator) compensate(
	c *app.Context,
	userInfo models.UserInfo,
	txID, serverID string,
	target preflightTarget,
	plan *ShadowPlan,
	beforeCandidate []byte,
	origCode, origMsg string,
) bool {
	d := tc.deps
	logger.Logger(c).Warnf("compensating transaction [%s]: %s", txID, origMsg)

	// 1. applying/verifying -> rolling_back
	if err := d.TxStore.UpdateStatus(c, userInfo, txID, models.TxStatusRollingBack, "", ""); err != nil {
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

	// 2. build the inverse apply: restore target/replace + the entity
	// whose identity (FRPClientIDKey metadata) the restore config carries.
	restore := ApplyRequest{
		PhysicalAgentID: plan.Child.OriginClientID, // the physical origin agent key
		TargetClientID:  plan.Child.ClientID,
		ServerID:        serverID,
	}
	restoreEntity := plan.ChildBefore
	if plan.FirstShadow {
		restore.TargetClientID = plan.OriginBefore.ClientID
		restore.ReplaceClientID = plan.Child.ClientID
		restoreEntity = plan.OriginBefore
	}

	// 3. re-apply the before snapshot (synchronous ACK, 15s budget)
	beforeFinal, err := buildFrpcApplyConfig(c, restoreEntity, serverID, beforeCandidate)
	if err != nil {
		_ = d.TxStore.FailTransaction(c, userInfo, &dao.FailTransactionInput{
			TxID: txID, Status: models.TxStatusRollbackFailed,
			ErrorCode: defs.ErrorCodeRollbackFailed, ErrorMessage: fmt.Sprintf("compensation build failed: %v", err),
			EvidenceJSON: compensationEvidence("compensate-build", err, d.Now()),
			AuditEvent:   rollbackAudit(false, defs.ErrorCodeRollbackFailed, err.Error()),
		})
		return false
	}
	restore.Config = beforeFinal
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	applyCode, applyErr := d.Apply.Apply(applyCtx, c, restore)
	cancelApply()
	if applyErr != nil {
		code := applyCode
		if code == "" {
			code = defs.ErrorCodeRollbackFailed
		}
		_ = d.TxStore.FailTransaction(c, userInfo, &dao.FailTransactionInput{
			TxID: txID, Status: models.TxStatusRollbackFailed,
			ErrorCode: defs.ErrorCodeRollbackFailed, ErrorMessage: fmt.Sprintf("compensation apply failed: %v", applyErr),
			EvidenceJSON: compensationEvidence("compensate-apply", applyErr, d.Now()),
			AuditEvent:   rollbackAudit(false, code, applyErr.Error()),
		})
		return false
	}

	// 3. verify the old state (port free again)
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	verifyErr := d.Verifier.VerifyReleased(verifyCtx, c, serverID, target.Protocol, target.RemotePort)
	cancelVerify()
	if verifyErr != nil {
		_ = d.TxStore.FailTransaction(c, userInfo, &dao.FailTransactionInput{
			TxID: txID, Status: models.TxStatusRollbackFailed,
			ErrorCode: defs.ErrorCodeRollbackFailed, ErrorMessage: fmt.Sprintf("compensation verify failed: %v", verifyErr),
			EvidenceJSON: compensationEvidence("compensate-verify", verifyErr, d.Now()),
			AuditEvent:   rollbackAudit(false, defs.ErrorCodeVerifyFailed, verifyErr.Error()),
		})
		return false
	}

	// 4. atomically release the reservation + mark rolled_back + audit
	err = d.TxStore.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:               txID,
		Status:             models.TxStatusRolledBack,
		ErrorCode:          origCode,
		ErrorMessage:       origMsg,
		ReleaseReservation: true,
		EvidenceJSON:       compensationEvidence("compensate-done", nil, d.Now()),
		AuditEvent:         rollbackAudit(true, "", "before config restored"),
	})
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot finalize compensation for transaction [%s]", txID)
		return false
	}
	return true
}

// redactCreateRequest builds the redacted request summary stored on the
// transaction (never the config content — it may embed auth tokens).
func redactCreateRequest(req *pb.CreateProxyConfigRequest, target preflightTarget) (string, error) {
	summary := map[string]interface{}{
		"client_id":   req.GetClientId(),
		"server_id":   req.GetServerId(),
		"proxy_name":  target.ProxyName,
		"proxy_type":  target.Protocol,
		"remote_port": target.RemotePort,
		"local_ip":    target.LocalIP,
		"local_port":  target.LocalPort,
		"overwrite":   req.GetOverwrite(),
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// targetFromProxyConfig extracts the candidate rule from a parsed proxy.
func targetFromProxyConfig(cfg v1.TypedProxyConfig) (preflightTarget, error) {
	t := preflightTarget{}
	switch p := cfg.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		t.Protocol = "tcp"
		t.RemotePort = uint32(p.RemotePort)
		t.LocalIP = p.LocalIP
		t.LocalPort = uint32(p.LocalPort)
		t.ProxyName = p.Name
	case *v1.UDPProxyConfig:
		t.Protocol = "udp"
		t.RemotePort = uint32(p.RemotePort)
		t.LocalIP = p.LocalIP
		t.LocalPort = uint32(p.LocalPort)
		t.ProxyName = p.Name
	default:
		return t, fmt.Errorf("%w: %s", defs.ErrUnsupportedProxyType, cfg.Type)
	}
	return normalizeTarget(t)
}

// isValidIdempotencyKey enforces the fail-closed idempotency contract
// (gate 7): the key must be present AND a UUID v4. An empty key would
// occupy the unique idempotency index and collide across unrelated
// requests; a non-v4 UUID would be a client bug — reject before creating
// any transaction row.
func isValidIdempotencyKey(key string) bool {
	if key == "" {
		return false
	}
	u, err := uuid.Parse(key)
	if err != nil {
		return false
	}
	return u.Version() == 4
}

func invalidCreateResp(code, msg string) *pb.CreateProxyConfigResponse {
	// The stable structured error code (spec §7.3) travels in the dedicated
	// error_code field; Status.Message stays purely human-readable. The
	// same code lands on the transaction row for audit.
	resp := &pb.CreateProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
	}
	if code != "" {
		resp.ErrorCode = lo.ToPtr(code)
	}
	return resp
}
