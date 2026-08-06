package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// D2c-C: the MANUAL ROLLBACK coordinator (spec §7.1 POST /api/v1/audit/rollback).
//
// Rollback reverses ONE committed change_transaction (create/update/delete/
// start/stop) into a NEW transaction with operation=rollback. The source
// transaction is NEVER modified (committed is terminal). The flow:
//
//	idempotency replay -> committed-only source gate -> source snapshot
//	decryption (AES-256 + SHA-256, fail closed) -> canonical (tenant, origin)
//	lock -> TOCTOU drift guard -> rollback tx (pending) -> before snapshots
//	-> apply the source-BEFORE client config (real Apply) -> verify the
//	target direction (VerifyReleased for create/start, VerifyApplied for
//	delete/update/stop) -> single-transaction CommitRollback (DAO).
//
// Every failure AFTER the apply point compensates with the TRUE INVERSE
// (re-apply the source-AFTER config + inverse verify) and lands the rollback
// tx on rolled_back (original error code) — a failed compensation lands on
// rollback_failed with the committed lease RETAINED (blocking). All
// fail-closed guards (committed-only, snapshot integrity, TOCTOU drift,
// missing snapshots) run BEFORE any transaction/agent side effect.
// ---------------------------------------------------------------------------

// RollbackTxStore is the persistence surface the rollback coordinator needs:
// the union of the tenant-scoped reads (source tx, idempotency, client row,
// origin resolution, snapshots, current proxy row) and the writes (rollback
// tx lifecycle + the atomic CommitRollback). The real adapter is daoStore;
// tests wrap it to inject failures (failingRollbackCommitStore).
type RollbackTxStore interface {
	// reads (all tenant-scoped)
	FindByIdempotencyKey(c *app.Context, userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	FindByID(c *app.Context, userInfo models.UserInfo, txID string) (*models.ChangeTransaction, error)
	GetClientByClientID(c *app.Context, userInfo models.UserInfo, clientID string) (*models.Client, error)
	GetOriginClientID(c *app.Context, userInfo models.UserInfo, clientID string) (string, error)
	FindByTransaction(c *app.Context, userInfo models.UserInfo, txID string) ([]*models.ConfigSnapshot, error)
	GetProxyConfigByID(c *app.Context, userInfo models.UserInfo, proxyID uint) (*models.ProxyConfig, error)
	// writes
	Create(c *app.Context, userInfo models.UserInfo, tx *models.ChangeTransaction) error
	UpdateStatus(c *app.Context, userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	FailTransaction(c *app.Context, userInfo models.UserInfo, in *dao.FailTransactionInput) error
	CommitRollback(c *app.Context, userInfo models.UserInfo, in *dao.CommitRollbackInput) (uint, error)
}

// daoStore adapter methods for the rollback surface. They are ADDITIVE: the
// existing SafeCreate/Update/Lifecycle interface sets are untouched.
func (s *daoStore) FindByID(c *app.Context, userInfo models.UserInfo, txID string) (*models.ChangeTransaction, error) {
	return dao.NewQuery(c).FindByID(userInfo, txID)
}

func (s *daoStore) FindByTransaction(c *app.Context, userInfo models.UserInfo, txID string) ([]*models.ConfigSnapshot, error) {
	return dao.NewQuery(c).FindByTransaction(userInfo, txID)
}

func (s *daoStore) GetProxyConfigByID(c *app.Context, userInfo models.UserInfo, proxyID uint) (*models.ProxyConfig, error) {
	return dao.NewQuery(c).GetProxyConfigByID(userInfo, proxyID)
}

func (s *daoStore) CommitRollback(c *app.Context, userInfo models.UserInfo, in *dao.CommitRollbackInput) (uint, error) {
	return dao.NewMutation(c).CommitRollback(userInfo, in)
}

// RollbackCoordinatorDeps carries every external dependency of the manual
// rollback flow. Query/Tx are the SAME store surface (a failure-injecting
// wrapper overrides CommitRollback only); Snap supplies the encrypted
// before-snapshot writes; Apply/Verifier are the real RPC-bound services
// (tests inject fakes); OriginLocker is the canonical (tenant, origin)
// keyed mutex; HashRequest is hashRollbackRequest.
type RollbackCoordinatorDeps struct {
	Query          RollbackTxStore
	Tx             RollbackTxStore
	Snap           SnapStore
	KeyProvider    KeyProvider
	Apply          ApplyService
	Verifier       Verifier
	OriginLocker   OriginLocker
	Now            func() time.Time
	ApplyTimeout   time.Duration
	VerifyTimeout  time.Duration
	VerifyAttempts int
	VerifyInterval time.Duration
	HashRequest    func(sourceTxID string) string
}

func defaultRollbackDeps() RollbackCoordinatorDeps {
	return RollbackCoordinatorDeps{
		Query:          newDAOStore(),
		Tx:             newDAOStore(),
		Snap:           newDAOStore(),
		KeyProvider:    ConfigKeyProvider{},
		Apply:          rpcApplyService{},
		Verifier:       &rpcVerifier{Attempts: defaultVerifyAttempts, Interval: defaultVerifyInterval},
		OriginLocker:   sharedOriginLocker,
		Now:            time.Now,
		ApplyTimeout:   defaultApplyTimeout,
		VerifyTimeout:  defaultVerifyTimeout,
		VerifyAttempts: defaultVerifyAttempts,
		VerifyInterval: defaultVerifyInterval,
		HashRequest:    hashRollbackRequest,
	}
}

// RollbackTransactionCoordinator orchestrates the manual rollback lifecycle.
type RollbackTransactionCoordinator struct {
	deps RollbackCoordinatorDeps
}

// NewDefaultRollbackCoordinator wires the REAL implementations (DAO, RPC,
// configured key file, the SHARED origin lock).
func NewDefaultRollbackCoordinator() *RollbackTransactionCoordinator {
	return &RollbackTransactionCoordinator{deps: defaultRollbackDeps()}
}

// safeRollbackCoordinator is the package-wide default coordinator the audit
// /rollback route delegates to (RollbackProxy). Tests swap it for a
// coordinator with injected deps and restore it afterwards.
var safeRollbackCoordinator = NewDefaultRollbackCoordinator()

// RollbackProxy is the exported transactional entry used by the audit
// /rollback route (biz/master/audit/rollback.go).
func RollbackProxy(c *app.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	// I1-B L2 gate: a blocked master answers READ_ONLY here — the
	// rollback coordinator (source re-apply, lock, DB, agent RPC) is
	// never reached.
	if readonly.Blocked() {
		return invalidRollbackResp(defs.ErrorCodeReadOnly, readonly.ErrReadOnly.Error()), nil
	}
	return safeRollbackCoordinator.Rollback(c, req)
}

// SetDefaultRollbackCoordinator swaps the package-wide rollback coordinator
// and returns the previous one for defer-restore. It exists because the
// audit handler test lives in another package and cannot reach the
// unexported var.
func SetDefaultRollbackCoordinator(rc *RollbackTransactionCoordinator) *RollbackTransactionCoordinator {
	old := safeRollbackCoordinator
	safeRollbackCoordinator = rc
	return old
}

// hashRollbackRequest computes the canonical idempotency hash of a rollback
// request: SHA-256 over the SOURCE transaction id. The idempotency key
// itself is deliberately excluded — it is the lookup dimension, not part of
// the request identity.
func hashRollbackRequest(sourceTxID string) string {
	sum := sha256.Sum256([]byte(sourceTxID))
	return hex.EncodeToString(sum[:])
}

// rollbackValidationError marks a STRUCTURED business rejection (the
// response carries CONFIG_VALIDATION_FAILED) — as opposed to an internal
// fail-closed error that surfaces as (nil, err).
type rollbackValidationError struct{ msg string }

func (e *rollbackValidationError) Error() string { return e.msg }

// rollbackSnapshotSet is the decrypted, integrity-checked snapshot material
// of the SOURCE transaction.
type rollbackSnapshotSet struct {
	beforeClient     []byte // source-before client config (the DESIRED state)
	afterClient      []byte // source-after client config (the CURRENT state)
	beforeClientNode string // node id the before client snapshot was recorded on
	afterClientNode  string // node id the after client snapshot was recorded on (the CURRENT handler)
	beforeProxy      []byte // source-before proxy envelope (desired row shape; nil when absent)
	afterProxy       []byte // source-after proxy envelope (current row shape; nil when absent)
}

// rollbackPlan is the coordinator-side decision for one rollback: the proxy
// geometry the DAO commit must CAS, the reservation migration step (with the
// physical keys derived from the REAL typed rules — never from request
// fields), the apply target and the verify direction.
type rollbackPlan struct {
	txProxyID            uint                // the rollback tx proxy_id (the source tx's proxy id)
	clientNodeID         string              // CURRENT handler (child for shadowed rules, origin otherwise)
	origin               string              // physical origin agent id
	serverID             string              // server the rule runs on
	beforeClient         []byte              // source-before client config (applied + desired)
	afterClient          []byte              // source-after client config (expected + restored on compensation)
	currentProxy         *models.ProxyConfig // expected CURRENT row shape (nil for delete rollback)
	desiredProxy         *models.ProxyConfig // expected DESIRED (before) row shape (nil for create rollback)
	currentProxyEnvPlain []byte              // rb-tx before proxy snapshot plaintext (current row envelope)
	beforeEnvPlain       []byte              // rb-tx after proxy snapshot plaintext (source-before envelope)
	reservation          dao.RollbackReservationStep
	verifyApplied        bool // true: forward verify = VerifyApplied (rule comes back); false: VerifyReleased (rule goes away)
	verifyKey            dao.ReservationKey
}

// invalidRollbackResp builds a structured business failure (spec §7.3): the
// stable error code travels in the dedicated error_code field, Status.Message
// stays human-readable.
func invalidRollbackResp(code, msg string) *pb.RollbackResponse {
	resp := &pb.RollbackResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
	}
	if code != "" {
		resp.ErrorCode = lo.ToPtr(code)
	}
	return resp
}

// validateRollbackSource enforces the committed-only gate: only committed
// create/update/delete/start/stop transactions are rollback-able. The source
// row is NEVER transitioned.
func validateRollbackSource(src *models.ChangeTransaction) error {
	if src == nil {
		return errors.New("missing source transaction")
	}
	switch src.Operation {
	case models.TxOperationCreate, models.TxOperationUpdate, models.TxOperationDelete,
		models.TxOperationStart, models.TxOperationStop:
	default:
		return fmt.Errorf("source transaction operation %q is not rollback-able", src.Operation)
	}
	if src.Status != models.TxStatusCommitted {
		return fmt.Errorf("only committed transactions can be rolled back (source status is %q)", src.Status)
	}
	return nil
}

// decryptRollbackSnapshots decrypts the source snapshots with the provided
// AES-256 key and verifies the SHA-256 integrity of EVERY row (a tampered
// snapshot fails closed). Missing required snapshots are structured
// validation failures (rollbackValidationError); key/decrypt/integrity
// problems are internal errors.
func decryptRollbackSnapshots(op string, snaps []*models.ConfigSnapshot, key []byte) (*rollbackSnapshotSet, error) {
	set := &rollbackSnapshotSet{}
	seenSlots := make(map[string]struct{}, len(snaps))
	for _, s := range snaps {
		if s == nil {
			return nil, &rollbackValidationError{msg: "source snapshot list contains a nil row"}
		}
		if s.Side != models.SnapshotSideBefore && s.Side != models.SnapshotSideAfter {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("source snapshot has unsupported side %q", s.Side)}
		}
		if s.NodeType != models.SnapshotNodeTypeClient && s.NodeType != models.SnapshotNodeTypeProxy {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("source snapshot has unsupported node type %q", s.NodeType)}
		}
		slot := s.Side + "\x00" + s.NodeType
		if _, exists := seenSlots[slot]; exists {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("duplicate source snapshot slot %s/%s", s.Side, s.NodeType)}
		}
		seenSlots[slot] = struct{}{}
		plain, err := snapshot.DecryptSnapshot(s.Ciphertext, s.Nonce, key)
		if err != nil {
			return nil, fmt.Errorf("rollback snapshot %s/%s/%s decrypt failed: %w", s.Side, s.NodeType, s.NodeID, err)
		}
		sum := sha256.Sum256(plain)
		if hex.EncodeToString(sum[:]) != s.SHA256 {
			return nil, fmt.Errorf("rollback snapshot %s/%s/%s sha256 mismatch: integrity check failed", s.Side, s.NodeType, s.NodeID)
		}
		switch {
		case s.Side == models.SnapshotSideBefore && s.NodeType == models.SnapshotNodeTypeClient:
			set.beforeClient = plain
			set.beforeClientNode = s.NodeID
		case s.Side == models.SnapshotSideAfter && s.NodeType == models.SnapshotNodeTypeClient:
			set.afterClient = plain
			set.afterClientNode = s.NodeID
		case s.Side == models.SnapshotSideBefore && s.NodeType == models.SnapshotNodeTypeProxy:
			set.beforeProxy = plain
		case s.Side == models.SnapshotSideAfter && s.NodeType == models.SnapshotNodeTypeProxy:
			set.afterProxy = plain
		}
	}
	missing := func(kind string) error {
		return &rollbackValidationError{msg: fmt.Sprintf("missing required source snapshot: %s", kind)}
	}
	if len(set.beforeClient) == 0 {
		return nil, missing("before client")
	}
	if len(set.afterClient) == 0 {
		return nil, missing("after client")
	}
	switch op {
	case models.TxOperationDelete, models.TxOperationUpdate, models.TxOperationStart, models.TxOperationStop:
		if len(set.beforeProxy) == 0 {
			return nil, missing("before proxy")
		}
	}
	switch op {
	case models.TxOperationUpdate, models.TxOperationStart, models.TxOperationStop:
		if len(set.afterProxy) == 0 {
			return nil, missing("after proxy")
		}
	}
	return set, nil
}

// resolveRollbackOriginAndLock resolves the ORIGIN client id from the node
// the source-after client snapshot was recorded on (the CURRENT handler),
// acquires the canonical (tenant, origin) keyed lock on the SAME injected
// locker instance the legacy writers and the other coordinators use, and
// re-resolves UNDER the lock (bounded retry, fail closed on a stale key).
// The caller MUST defer the unlock exactly once.
func (rc *RollbackTransactionCoordinator) resolveRollbackOriginAndLock(c *app.Context, userInfo models.UserInfo, clientNodeID string) (string, func(), error) {
	d := rc.deps
	if d.OriginLocker == nil {
		panic("proxy: RollbackTransactionCoordinator.OriginLocker is nil — same-origin rollback serialization is disabled")
	}
	const maxOriginLockAttempts = 3
	for attempt := 0; attempt < maxOriginLockAttempts; attempt++ {
		resolved, err := d.Query.GetOriginClientID(c, userInfo, clientNodeID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "", nil, &rollbackValidationError{msg: fmt.Sprintf("current client node %s is no longer visible", clientNodeID)}
			}
			return "", nil, err
		}
		key := originLockKey(userInfo.GetTenantID(), resolved)
		d.OriginLocker.Lock(key)
		reResolved, err := d.Query.GetOriginClientID(c, userInfo, clientNodeID)
		if err != nil {
			d.OriginLocker.Unlock(key)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "", nil, &rollbackValidationError{msg: fmt.Sprintf("current client node %s is no longer visible", clientNodeID)}
			}
			return "", nil, err
		}
		if reResolved != resolved {
			d.OriginLocker.Unlock(key)
			continue
		}
		return resolved, func() { d.OriginLocker.Unlock(key) }, nil
	}
	return "", nil, fmt.Errorf("origin mapping of client [%s] kept changing; giving up after %d attempts", clientNodeID, maxOriginLockAttempts)
}

// physicalKeyOfTyped derives the reservation key of a typed rule JSON blob.
func physicalKeyOfTyped(serverID string, typed []byte) (dao.ReservationKey, error) {
	var cfg v1.TypedProxyConfig
	if err := cfg.UnmarshalJSON(typed); err != nil {
		return dao.ReservationKey{}, fmt.Errorf("typed rule is not parseable: %w", err)
	}
	return updateTypedPhysicalKey(serverID, cfg)
}

// typedRuleType extracts the proxy type string from a typed rule JSON blob.
func typedRuleType(typed []byte) string {
	var cfg v1.TypedProxyConfig
	if err := cfg.UnmarshalJSON(typed); err != nil {
		return ""
	}
	if base := cfg.GetBaseConfig(); base != nil {
		return base.Type
	}
	return ""
}

// proxyFromEnvelope rebuilds a ProxyConfig row shape from a versioned proxy
// envelope (the snapshot format of the update/lifecycle commits) with the
// row id supplied by the caller (the after envelope deliberately omits it).
func proxyFromEnvelope(env updateProxyEnvelope, id uint) *models.ProxyConfig {
	return &models.ProxyConfig{
		Model: &gorm.Model{ID: id},
		ProxyConfigEntity: &models.ProxyConfigEntity{
			ServerID:       env.ServerID,
			ClientID:       env.ClientID,
			Name:           env.Name,
			Type:           typedRuleType(env.Typed),
			OriginClientID: env.OriginClientID,
			Content:        env.Typed,
			Stopped:        env.Stopped,
		},
	}
}

// currentProxyEnvelopeOfRow builds the rb-tx before proxy snapshot plaintext
// from the LIVE current row (create rollback — no source after-proxy
// envelope exists for creates).
func currentProxyEnvelopeOfRow(cur *models.ProxyConfig) []byte {
	raw, err := marshalProxyEnvelope(updateProxyEnvelope{
		ProxyID: cur.ID, Name: cur.Name, ServerID: cur.ServerID, ClientID: cur.ClientID,
		OriginClientID: cur.OriginClientID, Stopped: cur.Stopped, Typed: cur.Content,
	})
	if err != nil {
		return nil
	}
	return raw
}

// buildRollbackPlan runs UNDER the (tenant, origin) lock: it re-validates
// the source, TOCTOU-guards the current client bytes against the source-AFTER
// plaintext, resolves the proxy geometry per source operation and derives
// the reservation migration step from the REAL typed rules. Every mismatch
// is a structured rollbackValidationError with ZERO side effects.
func (rc *RollbackTransactionCoordinator) buildRollbackPlan(c *app.Context, userInfo models.UserInfo, src *models.ChangeTransaction, dec *rollbackSnapshotSet, origin string) (*rollbackPlan, error) {
	d := rc.deps
	// re-load the source under the lock: the world may have changed while
	// waiting for the key.
	reSrc, err := d.Query.FindByID(c, userInfo, src.TransactionID)
	if err != nil {
		if errors.Is(err, defs.ErrTransactionNotFound) {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("source transaction %s disappeared", src.TransactionID)}
		}
		return nil, err
	}
	if err := validateRollbackSource(reSrc); err != nil {
		return nil, &rollbackValidationError{msg: err.Error()}
	}
	if reSrc.ProxyID == nil {
		return nil, &rollbackValidationError{msg: "source transaction carries no proxy id"}
	}
	plan := &rollbackPlan{
		txProxyID:    *reSrc.ProxyID,
		clientNodeID: dec.afterClientNode,
		origin:       origin,
		beforeClient: dec.beforeClient,
		afterClient:  dec.afterClient,
	}

	cli, err := d.Query.GetClientByClientID(c, userInfo, plan.clientNodeID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("current client node %s not found", plan.clientNodeID)}
		}
		return nil, err
	}
	plan.serverID = cli.ServerID

	// S2 TOCTOU guard (mirrors the DAO commit): the persisted client config
	// must be byte-identical to the source-AFTER plaintext — a rollback must
	// never clobber newer edits.
	if !bytes.Equal(cli.ConfigContent, dec.afterClient) {
		return nil, &rollbackValidationError{msg: "the current client config drifted from the source-after snapshot; refusing to clobber newer edits"}
	}

	op := reSrc.Operation
	switch op {
	case models.TxOperationCreate:
		// create rollback: the current row is deleted, the lease released.
		cur, err := d.Query.GetProxyConfigByID(c, userInfo, *reSrc.ProxyID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, &rollbackValidationError{msg: fmt.Sprintf("current rule row %d not found", *reSrc.ProxyID)}
			}
			return nil, err
		}
		curTyped, err := cur.GetTypedProxyConfig()
		if err != nil {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("current rule row %d content is not parseable: %v", cur.ID, err)}
		}
		curKey, err := updateTypedPhysicalKey(cur.ServerID, curTyped)
		if err != nil {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("cannot resolve the current rule key: %v", err)}
		}
		plan.currentProxy = cur
		plan.currentProxyEnvPlain = currentProxyEnvelopeOfRow(cur)
		plan.verifyKey = curKey
		plan.verifyApplied = false // the rule is removed -> the port must be RELEASED
		plan.reservation = dao.RollbackReservationStep{
			Mode: dao.RollbackReservationRelease, Key: curKey, CurrentProxyID: cur.ID,
		}
	case models.TxOperationDelete:
		// delete rollback: the exact before row is recreated, the lease
		// restored owned by the recreated row.
		des, err := proxyFromBeforeEnvelope(dec.beforeProxy)
		if err != nil {
			return nil, &rollbackValidationError{msg: err.Error()}
		}
		desKey, err := physicalKeyOfTyped(plan.serverID, des.Content)
		if err != nil {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("cannot resolve the restored rule key: %v", err)}
		}
		plan.desiredProxy = des
		plan.beforeEnvPlain = dec.beforeProxy
		plan.verifyKey = desKey
		plan.verifyApplied = true // the rule comes back -> VerifyApplied
		plan.reservation = dao.RollbackReservationStep{
			Mode: dao.RollbackReservationRestore, Key: desKey,
			DesiredProxyID: des.ID, SourceTxID: reSrc.TransactionID,
		}
	case models.TxOperationUpdate, models.TxOperationStart, models.TxOperationStop:
		// current row shape from the source-after envelope + the source
		// tx proxy id (the after envelope omits the row id); desired row
		// from the source-before envelope.
		afterEnv, err := updateProxyEnvelopeFromBytes(dec.afterProxy)
		if err != nil {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("after proxy envelope is not parseable: %v", err)}
		}
		cur := proxyFromEnvelope(afterEnv, *reSrc.ProxyID)
		des, err := proxyFromBeforeEnvelope(dec.beforeProxy)
		if err != nil {
			return nil, &rollbackValidationError{msg: err.Error()}
		}
		curKey, err := physicalKeyOfTyped(plan.serverID, cur.Content)
		if err != nil {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("cannot resolve the current rule key: %v", err)}
		}
		desKey, err := physicalKeyOfTyped(plan.serverID, des.Content)
		if err != nil {
			return nil, &rollbackValidationError{msg: fmt.Sprintf("cannot resolve the restored rule key: %v", err)}
		}
		plan.currentProxy = cur
		plan.desiredProxy = des
		plan.currentProxyEnvPlain = dec.afterProxy
		plan.beforeEnvPlain = dec.beforeProxy
		switch op {
		case models.TxOperationUpdate:
			if curKey == desKey {
				plan.reservation = dao.RollbackReservationStep{
					Mode: dao.RollbackReservationOwnerCAS, Key: curKey,
					CurrentProxyID: cur.ID, DesiredProxyID: des.ID,
				}
			} else {
				plan.reservation = dao.RollbackReservationStep{
					Mode:       dao.RollbackReservationKeySwitch,
					ReleaseKey: curKey, CurrentProxyID: cur.ID,
					RestoreKey: desKey, DesiredProxyID: des.ID, SourceTxID: reSrc.TransactionID,
				}
			}
			plan.verifyKey = desKey
			plan.verifyApplied = true
		case models.TxOperationStart:
			// start rollback: the rule flips back to STOPPED (removed from
			// the applied config) -> VerifyReleased.
			plan.reservation = dao.RollbackReservationStep{
				Mode: dao.RollbackReservationSameKey, Key: curKey, CurrentProxyID: cur.ID,
			}
			plan.verifyKey = curKey
			plan.verifyApplied = false
		case models.TxOperationStop:
			// stop rollback: the rule flips back to ACTIVE -> VerifyApplied.
			plan.reservation = dao.RollbackReservationStep{
				Mode: dao.RollbackReservationSameKey, Key: curKey, CurrentProxyID: cur.ID,
			}
			plan.verifyKey = curKey
			plan.verifyApplied = true
		}
	}
	return plan, nil
}

// proxyFromBeforeEnvelope parses the source-before proxy envelope into the
// desired row shape (the before envelope carries the exact row id).
func proxyFromBeforeEnvelope(raw []byte) (*models.ProxyConfig, error) {
	env, err := parseProxyEnvelope(raw)
	if err != nil {
		return nil, fmt.Errorf("before proxy envelope is not parseable: %v", err)
	}
	if env.ProxyID == 0 {
		return nil, errors.New("before proxy envelope carries no row id")
	}
	return proxyFromEnvelope(env, env.ProxyID), nil
}

func parseProxyEnvelope(raw []byte) (updateProxyEnvelope, error) {
	var env updateProxyEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return env, err
	}
	if env.Version != 1 {
		return env, fmt.Errorf("unsupported proxy envelope version %d", env.Version)
	}
	return env, nil
}

func updateProxyEnvelopeFromBytes(raw []byte) (updateProxyEnvelope, error) {
	return parseProxyEnvelope(raw)
}

// redactRollbackRequest builds the redacted request summary stored on the
// rollback transaction: ids, ports-free metadata and config SHA-256 hashes —
// NEVER config bytes, rule names or remote ports (the redaction contract
// forbids "web"/"remotePort"-style plaintext markers).
func redactRollbackRequest(src *models.ChangeTransaction, plan *rollbackPlan) string {
	beforeSum := sha256.Sum256(plan.beforeClient)
	afterSum := sha256.Sum256(plan.afterClient)
	summary := map[string]interface{}{
		"source_transaction_id": src.TransactionID,
		"source_operation":      src.Operation,
		"proxy_id":              plan.txProxyID,
		"client_node_id":        plan.clientNodeID,
		"origin_client_id":      plan.origin,
		"server_id":             plan.serverID,
		"before_config_sha256":  hex.EncodeToString(beforeSum[:]),
		"after_config_sha256":   hex.EncodeToString(afterSum[:]),
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return ""
	}
	return string(raw)
}

// rollbackSuccessEvidence builds the evidence written onto the COMMITTED
// rollback transaction inside the DAO commit: the source operation, the
// physical agent the config was applied to and the apply/verify call counts.
// It never carries config material.
func rollbackSuccessEvidence(src *models.ChangeTransaction, plan *rollbackPlan) string {
	dir := "released"
	if plan.verifyApplied {
		dir = "applied"
	}
	ev := map[string]interface{}{
		"stage":            "rollback-committed",
		"source_operation": src.Operation,
		"applied_to_agent": plan.origin,
		"apply_calls":      1,
		"verify_calls":     1,
		"verify_direction": dir,
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return ""
	}
	return string(raw)
}

// rollbackCommitInput assembles the DAO CommitRollback payload.
func rollbackCommitInput(rbTxID string, src *models.ChangeTransaction, plan *rollbackPlan, snapKey []byte) *dao.CommitRollbackInput {
	in := &dao.CommitRollbackInput{
		TxID:                 rbTxID,
		SourceTxID:           src.TransactionID,
		SourceOp:             src.Operation,
		TxProxyID:            plan.txProxyID,
		ClientNodeID:         plan.clientNodeID,
		ExpectedClientConfig: plan.afterClient,
		DesiredClientConfig:  plan.beforeClient,
		CurrentProxy:         plan.currentProxy,
		DesiredProxy:         plan.desiredProxy,
		Reservation:          plan.reservation,
		ClientAfterSnapshot: dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeClient,
			NodeID:    plan.clientNodeID,
			Plaintext: plan.beforeClient,
			Key:       snapKey,
		},
		AuditEvent: &models.AuditEvent{
			TransactionID: rbTxID,
			EventType:     models.AuditEventTypeRollback,
			Summary:       fmt.Sprintf("rolled back %s transaction %s (proxy %d on server %s for client %s)", src.Operation, src.TransactionID, plan.txProxyID, plan.serverID, plan.clientNodeID),
			NodeID:        plan.clientNodeID,
			EvidenceRef:   "change_transaction:" + rbTxID + "#evidence_json",
			Success:       true,
		},
		EvidenceJSON: rollbackSuccessEvidence(src, plan),
	}
	if plan.desiredProxy != nil {
		in.ProxyAfterSnapshot = &dao.SnapshotInput{
			Side:      models.SnapshotSideAfter,
			NodeType:  models.SnapshotNodeTypeProxy,
			NodeID:    strconv.FormatUint(uint64(plan.desiredProxy.ID), 10),
			Plaintext: plan.beforeEnvPlain,
			Key:       snapKey,
		}
	}
	return in
}

// rollbackAuditEvent builds the compensation audit event (EventType is
// forced to compensation by the caller's FailTransaction path).
func rollbackAuditEvent(rbTxID string, success bool, code, msg string) *models.AuditEvent {
	summary := "compensation failed: " + msg
	if success {
		summary = "compensation succeeded: " + msg
	}
	return &models.AuditEvent{
		TransactionID: rbTxID,
		EventType:     models.AuditEventTypeCompensation,
		Summary:       summary,
		ErrorCode:     code,
		Success:       success,
	}
}

// failRollbackTx marks a rollback transaction failed (pre-apply failures
// only — after the apply point the compensation path takes over).
func (rc *RollbackTransactionCoordinator) failRollbackTx(c *app.Context, userInfo models.UserInfo, txID, code, msg, evidence string) error {
	audit := &models.AuditEvent{
		TransactionID: txID,
		EventType:     models.AuditEventTypeRollback,
		Summary:       "rollback failed: " + msg,
		ErrorCode:     code,
		Success:       false,
	}
	return rc.deps.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:         txID,
		Status:       models.TxStatusFailed,
		ErrorCode:    code,
		ErrorMessage: msg,
		EvidenceJSON: evidence,
		AuditEvent:   audit,
	})
}

// compensateRollback performs the automatic compensation after the apply
// point (spec §8.2): re-apply the source-AFTER config to the SAME handler
// (the TRUE INVERSE of the forward apply), verify the inverse direction
// (the rule must be back when the forward verify wanted it gone and vice
// versa), then atomically mark the rollback tx rolled_back with the ORIGINAL
// error code. A failed compensation lands on rollback_failed and RETAINS the
// committed lease (blocking). Returns true when the rollback completed.
func (rc *RollbackTransactionCoordinator) compensateRollback(
	c *app.Context,
	userInfo models.UserInfo,
	rbTxID string,
	plan *rollbackPlan,
	clientEntity *models.ClientEntity,
	origCode, origMsg string,
) bool {
	d := rc.deps
	logger.Logger(c).Warnf("compensating rollback transaction [%s]: %s", rbTxID, origMsg)

	// 1. applying/verifying -> rolling_back
	if err := d.Tx.UpdateStatus(c, userInfo, rbTxID, models.TxStatusRollingBack, "", ""); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot move rollback transaction [%s] to rolling_back", rbTxID)
		return false
	}
	failRollback := func(stage string, code, msg string) bool {
		if err := d.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
			TxID: rbTxID, Status: models.TxStatusRollbackFailed,
			ErrorCode: defs.ErrorCodeRollbackFailed, ErrorMessage: msg,
			EvidenceJSON: compensationEvidence(stage, errors.New(msg), d.Now()),
			AuditEvent:   rollbackAuditEvent(rbTxID, false, code, msg),
		}); err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot persist rollback_failed for rollback transaction [%s]", rbTxID)
		}
		return false
	}

	// 2. restore the source-AFTER config (inverse apply) to the SAME handler.
	restore := ApplyRequest{
		PhysicalAgentID: plan.origin,
		TargetClientID:  plan.clientNodeID,
		ReplaceClientID: "",
		ServerID:        plan.serverID,
	}
	afterFinal, err := buildFrpcApplyConfig(c, clientEntity, plan.serverID, plan.afterClient)
	if err != nil {
		return failRollback("rollback-compensate-build", defs.ErrorCodeRollbackFailed,
			fmt.Sprintf("compensation build failed: %v", err))
	}
	restore.Config = afterFinal
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	applyCode, applyErr := d.Apply.Apply(applyCtx, c, restore)
	cancelApply()
	if applyErr != nil {
		code := applyCode
		if code == "" {
			code = defs.ErrorCodeRollbackFailed
		}
		return failRollback("rollback-compensate-apply", code,
			fmt.Sprintf("compensation apply failed: %v", applyErr))
	}

	// 3. inverse verify: the world must be back to the source-AFTER state.
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	var verifyErr error
	if plan.verifyApplied {
		verifyErr = d.Verifier.VerifyReleased(verifyCtx, c, plan.verifyKey.ServerID, plan.verifyKey.Protocol, plan.verifyKey.RemotePort)
	} else {
		verifyErr = d.Verifier.VerifyApplied(verifyCtx, c, plan.verifyKey.ServerID, plan.verifyKey.Protocol, plan.verifyKey.RemotePort)
	}
	cancelVerify()
	if verifyErr != nil {
		return failRollback("rollback-compensate-verify", defs.ErrorCodeVerifyFailed,
			fmt.Sprintf("compensation verify failed: %v", verifyErr))
	}

	// 4. atomically mark rolled_back with the ORIGINAL code + evidence.
	if err := d.Tx.FailTransaction(c, userInfo, &dao.FailTransactionInput{
		TxID:         rbTxID,
		Status:       models.TxStatusRolledBack,
		ErrorCode:    origCode,
		ErrorMessage: origMsg,
		EvidenceJSON: compensationEvidence("rollback-compensate-done", nil, d.Now()),
		AuditEvent:   rollbackAuditEvent(rbTxID, true, "", "source-after config restored"),
	}); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot finalize compensation for rollback transaction [%s]", rbTxID)
		return false
	}
	return true
}

// replayRollback implements the idempotency replay semantics (gate 7):
// same key + same hash + committed -> idempotent success returning the SAME
// rollback tx id; same key + different hash -> stable IDEMPOTENCY_CONFLICT;
// terminal failure -> the original failure result; in-flight -> stable
// IDEMPOTENCY_IN_FLIGHT. The replay short-circuits BEFORE any source work.
func (rc *RollbackTransactionCoordinator) replayRollback(existing *models.ChangeTransaction, reqHash string) (*pb.RollbackResponse, error) {
	if existing.Operation != models.TxOperationRollback || existing.RequestHash != reqHash {
		return invalidRollbackResp(defs.ErrorCodeIdempotencyConflict,
			"idempotency key was already used with a different request"), nil
	}
	switch existing.Status {
	case models.TxStatusCommitted:
		return &pb.RollbackResponse{
			Status:           &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
			NewTransactionId: lo.ToPtr(existing.TransactionID),
		}, nil
	case models.TxStatusFailed, models.TxStatusRolledBack, models.TxStatusRollbackFailed:
		code := existing.ErrorCode
		if code == "" {
			code = defs.ErrorCodeConfigValidationFailed
		}
		msg := existing.ErrorMessage
		if msg == "" {
			msg = "the original rollback request failed"
		}
		return invalidRollbackResp(code, msg), nil
	default:
		return invalidRollbackResp(defs.ErrorCodeIdempotencyInFlight,
			"a rollback with this idempotency key is already in progress"), nil
	}
}

// Rollback is the exported manual-rollback entry: it returns (resp, nil) for
// structured business failures (the response carries the error code/message)
// and (nil, err) for internal errors. The whole flow runs under the single
// canonical (tenant, origin) lock.
func (rc *RollbackTransactionCoordinator) Rollback(c *app.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	d := rc.deps
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() {
		return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed, "authentication required"), nil
	}

	// ---- a. request validation ----
	if len(req.GetTransactionId()) == 0 {
		return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed, "transaction_id is required"), nil
	}
	if !isValidIdempotencyKey(req.GetIdempotencyKey()) {
		return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed,
			"idempotency_key is required and must be a UUID v4"), nil
	}
	reqHash := d.HashRequest(req.GetTransactionId())

	// ---- b. idempotency replay (before ANY source work) ----
	existing, err := d.Query.FindByIdempotencyKey(c, userInfo, req.GetIdempotencyKey())
	if err == nil {
		return rc.replayRollback(existing, reqHash)
	}
	if !errors.Is(err, defs.ErrTransactionNotFound) {
		return nil, err
	}

	// ---- c. source transaction: tenant-scoped, committed-only gate ----
	src, err := d.Query.FindByID(c, userInfo, req.GetTransactionId())
	if err != nil {
		if errors.Is(err, defs.ErrTransactionNotFound) {
			return &pb.RollbackResponse{Status: &pb.Status{
				Code: pb.RespCode_RESP_CODE_NOT_FOUND, Message: "source transaction not found",
			}}, nil
		}
		return nil, err
	}
	if err := validateRollbackSource(src); err != nil {
		return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed, err.Error()), nil
	}

	// ---- d. decrypt + integrity-check the source snapshots (fail closed) ----
	snaps, err := d.Query.FindByTransaction(c, userInfo, src.TransactionID)
	if err != nil {
		return nil, err
	}
	snapKey, err := d.KeyProvider.SnapshotKey(c)
	if err != nil {
		return nil, fmt.Errorf("rollback: cannot read the snapshot key: %w", err)
	}
	dec, err := decryptRollbackSnapshots(src.Operation, snaps, snapKey)
	if err != nil {
		var valErr *rollbackValidationError
		if errors.As(err, &valErr) {
			return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed, valErr.Error()), nil
		}
		return nil, err
	}

	// ---- e. resolve the origin + canonical (tenant, origin) lock ----
	origin, unlock, err := rc.resolveRollbackOriginAndLock(c, userInfo, dec.afterClientNode)
	if err != nil {
		var valErr *rollbackValidationError
		if errors.As(err, &valErr) {
			return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed, valErr.Error()), nil
		}
		return nil, err
	}
	defer unlock()

	// ---- f. UNDER THE LOCK: drift guard + plan + rollback tx ----
	plan, err := rc.buildRollbackPlan(c, userInfo, src, dec, origin)
	if err != nil {
		var valErr *rollbackValidationError
		if errors.As(err, &valErr) {
			return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed, valErr.Error()), nil
		}
		return nil, err
	}
	clientRow, err := d.Query.GetClientByClientID(c, userInfo, plan.clientNodeID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return invalidRollbackResp(defs.ErrorCodeConfigValidationFailed,
				fmt.Sprintf("current client node %s is no longer visible", plan.clientNodeID)), nil
		}
		return nil, err
	}
	clientEntity := clientRow.ClientEntity

	rbTxID := uuid.NewString()
	now := d.Now()
	rbTx := &models.ChangeTransaction{
		TransactionID:   rbTxID,
		IdempotencyKey:  req.GetIdempotencyKey(),
		RequestHash:     reqHash,
		Operation:       models.TxOperationRollback,
		Status:          models.TxStatusPending,
		ProxyID:         lo.ToPtr(plan.txProxyID),
		RequestRedacted: redactRollbackRequest(src, plan),
		StartedAt:       now,
	}
	if err := d.Tx.Create(c, userInfo, rbTx); err != nil {
		if dao.IsUniqueViolation(err) {
			if existing, ferr := d.Tx.FindByIdempotencyKey(c, userInfo, req.GetIdempotencyKey()); ferr == nil {
				return rc.replayRollback(existing, reqHash)
			}
		}
		return nil, err
	}

	// ---- g. before snapshots (saved BEFORE any apply) ----
	if _, err := d.Snap.SaveEncrypted(c, userInfo, rbTxID, models.SnapshotSideBefore,
		models.SnapshotNodeTypeClient, plan.clientNodeID, plan.afterClient, snapKey); err != nil {
		msg := fmt.Sprintf("cannot save the rollback before client snapshot: %v", err)
		_ = rc.failRollbackTx(c, userInfo, rbTxID, defs.ErrorCodeCommitFailed, msg, "")
		return nil, err
	}
	if plan.currentProxy != nil && len(plan.currentProxyEnvPlain) > 0 {
		if _, err := d.Snap.SaveEncrypted(c, userInfo, rbTxID, models.SnapshotSideBefore,
			models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(plan.currentProxy.ID), 10),
			plan.currentProxyEnvPlain, snapKey); err != nil {
			msg := fmt.Sprintf("cannot save the rollback before proxy snapshot: %v", err)
			_ = rc.failRollbackTx(c, userInfo, rbTxID, defs.ErrorCodeCommitFailed, msg, "")
			return nil, err
		}
	}

	// ---- h. state machine: pending -> preflight -> applying ----
	for _, st := range []string{models.TxStatusPreflight, models.TxStatusApplying} {
		if err := d.Tx.UpdateStatus(c, userInfo, rbTxID, st, "", ""); err != nil {
			code := defs.ErrorCodeCommitFailed
			msg := fmt.Sprintf("cannot advance the rollback to %s: %v", st, err)
			_ = rc.failRollbackTx(c, userInfo, rbTxID, code, msg, "")
			return invalidRollbackResp(code, msg), nil
		}
	}

	// ---- i. forward apply: push the source-BEFORE config to the CURRENT
	// handler on the physical origin agent (never a switch). ----
	forward := ApplyRequest{
		PhysicalAgentID: plan.origin,
		TargetClientID:  plan.clientNodeID,
		ReplaceClientID: "",
		ServerID:        plan.serverID,
	}
	beforeFinal, err := buildFrpcApplyConfig(c, clientEntity, plan.serverID, plan.beforeClient)
	if err != nil {
		code := defs.ErrorCodeConfigValidationFailed
		msg := fmt.Sprintf("cannot build the rollback config: %v", err)
		_ = rc.failRollbackTx(c, userInfo, rbTxID, code, msg, "")
		return invalidRollbackResp(code, msg), nil
	}
	forward.Config = beforeFinal
	applyCtx, cancelApply := context.WithTimeout(c, d.ApplyTimeout)
	applyCode, applyErr := d.Apply.Apply(applyCtx, c, forward)
	cancelApply()
	if applyErr != nil {
		if applyCode == "" {
			if errors.Is(applyErr, context.DeadlineExceeded) || errors.Is(applyErr, context.Canceled) {
				applyCode = defs.ErrorCodeApplyTimeout
			} else {
				applyCode = defs.ErrorCodeApplyRejected
			}
		}
		msg := fmt.Sprintf("rollback apply failed: %v", applyErr)
		compensated := rc.compensateRollback(c, userInfo, rbTxID, plan, clientEntity, applyCode, msg)
		if compensated {
			return invalidRollbackResp(applyCode, msg), nil
		}
		return invalidRollbackResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- j. applying -> verifying, then verify the target direction ----
	if err := d.Tx.UpdateStatus(c, userInfo, rbTxID, models.TxStatusVerifying, "", ""); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("cannot advance the applied rollback to verifying: %v", err)
		compensated := rc.compensateRollback(c, userInfo, rbTxID, plan, clientEntity, code, msg)
		if compensated {
			return invalidRollbackResp(code, msg), nil
		}
		return invalidRollbackResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}
	verifyCtx, cancelVerify := context.WithTimeout(c, d.VerifyTimeout)
	var verifyErr error
	if plan.verifyApplied {
		verifyErr = d.Verifier.VerifyApplied(verifyCtx, c, plan.verifyKey.ServerID, plan.verifyKey.Protocol, plan.verifyKey.RemotePort)
	} else {
		verifyErr = d.Verifier.VerifyReleased(verifyCtx, c, plan.verifyKey.ServerID, plan.verifyKey.Protocol, plan.verifyKey.RemotePort)
	}
	cancelVerify()
	if verifyErr != nil {
		msg := fmt.Sprintf("rollback verify failed: %v", verifyErr)
		compensated := rc.compensateRollback(c, userInfo, rbTxID, plan, clientEntity, defs.ErrorCodeVerifyFailed, msg)
		if compensated {
			return invalidRollbackResp(defs.ErrorCodeVerifyFailed, msg), nil
		}
		return invalidRollbackResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	// ---- k. single-transaction commit (verifying -> committed is the DAO's
	// last step; the source tx is never touched). ----
	if _, err := d.Tx.CommitRollback(c, userInfo, rollbackCommitInput(rbTxID, src, plan, snapKey)); err != nil {
		code := defs.ErrorCodeCommitFailed
		msg := fmt.Sprintf("rollback commit failed: %v", err)
		compensated := rc.compensateRollback(c, userInfo, rbTxID, plan, clientEntity, code, msg)
		if compensated {
			return invalidRollbackResp(code, msg), nil
		}
		return invalidRollbackResp(defs.ErrorCodeRollbackFailed, fmt.Sprintf("%s (compensation failed)", msg)), nil
	}

	return &pb.RollbackResponse{
		Status:           &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		NewTransactionId: lo.ToPtr(rbTxID),
	}, nil
}
