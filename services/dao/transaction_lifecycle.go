package dao

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// LifecycleOp is the operation a CommitLifecycle commit persists. The value
// is the same string the change_transaction.operation column carries, and
// the S1 owner guard requires an EXACT match — an update transaction can
// never be used to commit a delete (and vice versa).
type LifecycleOp string

const (
	LifecycleOpDelete LifecycleOp = "delete"
	LifecycleOpStart  LifecycleOp = "start"
	LifecycleOpStop   LifecycleOp = "stop"
)

// maxEvidenceJSONBytes caps the optional evidence payload written onto the
// transaction inside the commit (S8.5). The evidence is metadata only —
// never config or secret material.
const maxEvidenceJSONBytes = 64 * 1024

// CommitLifecycleInput is the complete payload of one delete/start/stop
// commit (spec §8.3). All values are tenant-scoped by the caller; the DAO
// re-checks the tenant, the operation, the rule identity, the client config
// bytes and the reservation state on every row it touches.
//
// The lifecycle NEVER changes the physical key: Reservation must be
// SameKeyOwned or SameKeyPending with OldKey == NewKey. All paths — the
// no-op fast paths included — run the reservation migration, so there is no
// "mode none".
type CommitLifecycleInput struct {
	TxID string
	Op   LifecycleOp

	// old rule identity (plan result; re-verified against the DB in S1)
	ProxyID  uint
	Name     string
	ServerID string

	// OriginClientID is the never-shadowed origin; the rule row must live
	// on it (origin-shaped) or on its child with this origin link
	// (child-shaped).
	OriginClientID string

	// ExpectedStopped is REQUIRED for every op (including delete): the
	// caller passes the plan's OLD stopped value and S1 fails closed when
	// the row drifted — a delete must never be able to race a start/stop.
	ExpectedStopped bool

	// ClientAfter is the client row state AFTER the lifecycle op: the rule
	// re-added (start) or removed (stop/delete). The DAO only ever writes
	// its ConfigContent (S4) — secret/comment/stopped/identity fields are
	// never touched by this batch.
	ClientAfter *models.ClientEntity
	// ExpectedClientConfigContent is the EXACT persisted DB bytes of the
	// client's config_content before the commit (the raw DB CAS bytes, NOT
	// the agent snapshot plaintext). nil/empty is a legal value (a client
	// row may carry NULL/empty config) and the S4 CAS matches it
	// explicitly — never `= NULL`.
	ExpectedClientConfigContent []byte

	// Reservation is the same-key migration. SameKeyOwned requires
	// OldReservationExpected=true (committed row owned by the old proxy);
	// SameKeyPending requires it false (this tx's own pending lease).
	// DifferentKey/unknown modes are rejected in S0.
	Reservation ReservationStep

	// AFTER-only encrypted snapshots. ClientAfterSnapshot is always
	// written (node_id forced to ClientAfter.ClientID); ProxyAfterSnapshot
	// is REQUIRED for start/stop (node_id forced to the old proxy row id)
	// and must be nil for delete. A wrong-size AES key passes S0 (non-empty)
	// and fails inside S7, rolling back the whole commit — the atomicity
	// contract.
	ClientAfterSnapshot SnapshotInput
	ProxyAfterSnapshot  *SnapshotInput

	// lifecycle audit event (TransactionID/TenantID/ActorUserID/EventType
	// forced by the DAO per op; summary must not embed plaintext config).
	// EvidenceRef may be carried by the caller.
	AuditEvent *models.AuditEvent

	// EvidenceJSON is optional metadata written onto the transaction INSIDE
	// the commit transaction (before the committed transition). It must be
	// valid JSON, at most 64KiB, and must never carry config/secret material.
	EvidenceJSON string
}

// CommitLifecycle atomically persists the final state of a delete/start/stop
// lifecycle operation (spec §8.3). Returns the OLD proxy row id (the
// reservation owner before the commit).
//
// Transaction order (any failure rolls the whole commit back):
//
//	S0  static validation — zero DB writes, stable ErrInvalidCommitInput
//	S1  owner tx guard (tenant+actor+TxID, operation == Op, status ==
//	    verifying, proxy_id nil-or-target; write-first lock on SQLite,
//	    SELECT ... FOR UPDATE elsewhere) + old rule row guard (id+tenant+
//	    user; name/server/Stopped==ExpectedStopped, legal shape, real typed
//	    physical key == Reservation.OldKey → ErrProxyConfigNotFound /
//	    ErrProxyConfigChanged)
//	S2  client guard (tenant+user+client_id; origin link must equal the
//	    proxy row's; child clients enforce server+origin, origin clients
//	    tolerate empty server/origin; persisted config_content must equal
//	    ExpectedClientConfigContent → ErrClientNotFound / ErrProxyConfigChanged)
//	S3  proxy write: delete → guarded Unscoped hard delete (exact id+tenant+
//	    user+name+server+stopped, RowsAffected==1); start/stop → guarded
//	    UPDATE stopped only; target == ExpectedStopped skips the write
//	    (already-state fast path — S1 already guarded; a value-identical
//	    UPDATE's MySQL RowsAffected=0 is never misread as a CAS miss)
//	S4  client write: only config_content; bytes.Equal(expected, after)
//	    skips (whole-row UpdatedAt preserved); otherwise guarded UPDATE with
//	    the exact expected bytes in the WHERE (nil matches SQL NULL; a
//	    non-nil empty slice matches only a zero-length blob), RowsAffected==1
//	S5  setTransactionProxyID (idempotent; delete's row is already gone and
//	    the old id is still the right backfill; prebound-identical succeeds)
//	S6  reservation migration: SameKeyOwned start/stop → owner shape check
//	    only (transaction_id NEVER rewritten); SameKeyOwned delete → exact
//	    key+owner hard delete (must be 1 row); SameKeyPending start/stop →
//	    pending→committed owner=old id; SameKeyPending delete → exact
//	    tx+key+pending hard delete (must be 1 row; never the old committed)
//	S7  encrypted AFTER snapshots: client always (node_id=ClientAfter
//	    ClientID), proxy for start/stop (node_id=old proxy id), never for
//	    delete
//	S8  audit event (TxID/tenant/actor/EventType forced per op)
//	S8.5 evidence (optional; same transaction, verifying CAS)
//	S9  transition tx → committed (last step)
func (m *transactionMutation) CommitLifecycle(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return 0, err
	}
	// S0 static validation — zero DB writes; every violation is the stable
	// ErrInvalidCommitInput BEFORE the transaction begins.
	if err := validateCommitLifecycleInput(in); err != nil {
		return 0, err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	tenantID := userInfo.GetTenantID()
	userID := userInfo.GetUserID()

	err := db.Transaction(func(tx *gorm.DB) error {
		// S1 owner tx guard (write-first lock / FOR UPDATE) then old rule
		// row guard.
		if err := guardLifecycleTxInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		old, err := loadLifecycleProxyRowInTx(tx, tenantID, userID, in)
		if err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		// S2 client guard (shape + persisted config bytes)
		if err := guardLifecycleClientInTx(tx, tenantID, userID, old, in); err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		// S3 proxy write (delete hard delete / start-stop stopped flip with
		// the already-state fast path)
		if err := writeLifecycleProxyInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		// S4 client write (config_content only; skip value-identical)
		if err := writeLifecycleClientInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		// S5 backfill change_transaction.proxy_id (verifying CAS; the old
		// id is the right value for every op — delete's row is already gone)
		if err := setTransactionProxyID(tx, tenantID, in.TxID, in.ProxyID); err != nil {
			return fmt.Errorf("commit lifecycle: backfill transaction proxy id: %w", err)
		}
		// S6 reservation migration (write-first CAS per mode; the DAO
		// re-validates the mode against the DB instead of trusting the
		// caller)
		if err := migrateLifecycleReservationInTx(tx, tenantID, in); err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		// S7 encrypted AFTER snapshots (before rows are the coordinator's
		// pre-apply job and are never written here)
		if err := saveAfterSnapshotInTx(tx, tenantID, in.TxID, in.ClientAfterSnapshot,
			models.SnapshotNodeTypeClient, in.ClientAfter.ClientID); err != nil {
			return fmt.Errorf("commit lifecycle: save client after snapshot: %w", err)
		}
		if in.ProxyAfterSnapshot != nil {
			if err := saveAfterSnapshotInTx(tx, tenantID, in.TxID, *in.ProxyAfterSnapshot,
				models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(in.ProxyID), 10)); err != nil {
				return fmt.Errorf("commit lifecycle: save proxy after snapshot: %w", err)
			}
		}
		// S8 audit (TxID/tenant/actor/EventType forced; summary redaction
		// was validated in S0)
		if err := saveLifecycleAuditInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit lifecycle: %w", err)
		}
		// S8.5 evidence (optional; lands atomically with the commit)
		if in.EvidenceJSON != "" {
			if err := writeLifecycleEvidenceInTx(tx, tenantID, userID, in); err != nil {
				return fmt.Errorf("commit lifecycle: write evidence: %w", err)
			}
		}
		// S9 tx -> committed (last step)
		return transitionTxStatus(tx, tenantID, in.TxID, models.TxStatusCommitted, "", "")
	})
	if err != nil {
		return 0, err
	}
	return in.ProxyID, nil
}

// CommitDelete/CommitStart/CommitStop are thin op-binding wrappers over
// CommitLifecycle for coordinator ergonomics — they bind Op themselves, so
// callers can build one input shape and route by wrapper.
func (m *transactionMutation) CommitDelete(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error) {
	return m.CommitLifecycle(userInfo, lifecycleInputWithOp(in, LifecycleOpDelete))
}

func (m *transactionMutation) CommitStart(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error) {
	return m.CommitLifecycle(userInfo, lifecycleInputWithOp(in, LifecycleOpStart))
}

func (m *transactionMutation) CommitStop(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error) {
	return m.CommitLifecycle(userInfo, lifecycleInputWithOp(in, LifecycleOpStop))
}

func lifecycleInputWithOp(in *CommitLifecycleInput, op LifecycleOp) *CommitLifecycleInput {
	if in == nil {
		return nil
	}
	copy := *in
	copy.Op = op
	return &copy
}

// validateCommitLifecycleInput enforces the static shape contract (S0) with
// ZERO DB writes: op/identity completeness, same-key reservation geometry,
// per-op snapshot requirements, audit presence/redaction and evidence
// validity. Every violation is the stable defs.ErrInvalidCommitInput. The
// AES-256 KEY SIZE is deliberately NOT checked here: a wrong-size key must
// fail inside the transaction at S7 to prove the atomic rollback contract.
func validateCommitLifecycleInput(in *CommitLifecycleInput) error {
	invalid := func(format string, args ...interface{}) error {
		return fmt.Errorf("%w: %s", defs.ErrInvalidCommitInput, fmt.Sprintf(format, args...))
	}
	if in == nil {
		return invalid("nil input")
	}
	switch in.Op {
	case LifecycleOpStart, LifecycleOpStop, LifecycleOpDelete:
	default:
		return invalid("op %q is not a valid lifecycle operation", in.Op)
	}
	if in.TxID == "" {
		return invalid("missing transaction id")
	}
	if in.ProxyID == 0 {
		return invalid("missing proxy id")
	}
	if in.Name == "" {
		return invalid("missing proxy name")
	}
	if in.ServerID == "" {
		return invalid("missing server id")
	}
	if in.OriginClientID == "" {
		return invalid("missing origin client id")
	}
	if in.ClientAfter == nil {
		return invalid("client after state is required")
	}
	if in.ClientAfter.ClientID == "" {
		return invalid("missing client after id")
	}
	// reservation key geometry: valid tcp/udp key on the rule's server,
	// identical old/new keys, only the two same-key modes with their exact
	// old-row expectations (spec §3 #6-8 + lifecycle ruling: OldKey==NewKey
	// always, mode none does not exist).
	if err := validateReservationKey(invalid, in.Reservation.OldKey); err != nil {
		return err
	}
	if in.Reservation.OldKey.ServerID != in.ServerID {
		return invalid("reservation key server %q differs from rule server %q", in.Reservation.OldKey.ServerID, in.ServerID)
	}
	switch in.Reservation.Mode {
	case ReservationModeSameKeyOwned:
		if !in.Reservation.OldReservationExpected {
			return invalid("same_key_owned implies an existing owned reservation")
		}
	case ReservationModeSameKeyPending:
		if in.Reservation.OldReservationExpected {
			return invalid("same_key_pending never expects an old committed row")
		}
	default:
		return invalid("reservation mode %q is not valid for lifecycle commits (only same_key_owned/same_key_pending)", in.Reservation.Mode)
	}
	if in.Reservation.OldKey != in.Reservation.NewKey {
		return invalid("lifecycle commits never change the physical key (old %s/%s/%d differs from new %s/%s/%d)",
			in.Reservation.OldKey.ServerID, in.Reservation.OldKey.Protocol, in.Reservation.OldKey.RemotePort,
			in.Reservation.NewKey.ServerID, in.Reservation.NewKey.Protocol, in.Reservation.NewKey.RemotePort)
	}
	// after snapshots: client always required with key+plaintext; proxy
	// required for start/stop, FORBIDDEN for delete
	if len(in.ClientAfterSnapshot.Key) == 0 {
		return invalid("client after snapshot key must not be empty")
	}
	if len(in.ClientAfterSnapshot.Plaintext) == 0 {
		return invalid("client after snapshot plaintext must not be empty")
	}
	switch in.Op {
	case LifecycleOpStart, LifecycleOpStop:
		if in.ProxyAfterSnapshot == nil {
			return invalid("%s requires the proxy after snapshot", in.Op)
		}
		if len(in.ProxyAfterSnapshot.Key) == 0 {
			return invalid("proxy after snapshot key must not be empty")
		}
		if len(in.ProxyAfterSnapshot.Plaintext) == 0 {
			return invalid("proxy after snapshot plaintext must not be empty")
		}
	case LifecycleOpDelete:
		if in.ProxyAfterSnapshot != nil {
			return invalid("delete never writes a proxy after snapshot")
		}
	}
	// audit: required, and the summary must not embed the plaintext
	// snapshots (the audit trail is redacted by construction, spec §6.4)
	if in.AuditEvent == nil {
		return invalid("audit event is required")
	}
	proxyPlain := []byte(nil)
	if in.ProxyAfterSnapshot != nil {
		proxyPlain = in.ProxyAfterSnapshot.Plaintext
	}
	secretPlain := []byte(nil)
	if in.ClientAfter.ConnectSecret != "" {
		secretPlain = []byte(in.ClientAfter.ConnectSecret)
	}
	if summaryContainsPlaintext(in.AuditEvent.Summary,
		in.ClientAfterSnapshot.Plaintext,
		proxyPlain,
		in.ExpectedClientConfigContent,
		secretPlain,
	) {
		return invalid("audit summary must not contain plaintext config material")
	}
	// evidence: optional, valid JSON, capped, and never config/secret
	// material (S8.5)
	if in.EvidenceJSON != "" {
		if len(in.EvidenceJSON) > maxEvidenceJSONBytes {
			return invalid("evidence json exceeds the %d byte cap", maxEvidenceJSONBytes)
		}
		if !json.Valid([]byte(in.EvidenceJSON)) {
			return invalid("evidence json is not valid json")
		}
		if lifecycleEvidenceContainsPlaintext(in) {
			return invalid("evidence must not contain config/secret material")
		}
	}
	return nil
}

// lifecycleEvidenceContainsPlaintext reports whether the evidence payload
// embeds any plaintext config material or the client connect secret — the
// evidence column is metadata only and must never become a secret leak.
func lifecycleEvidenceContainsPlaintext(in *CommitLifecycleInput) bool {
	ev := in.EvidenceJSON
	check := func(p []byte) bool {
		return len(p) > 0 && strings.Contains(ev, string(p))
	}
	if check(in.ClientAfterSnapshot.Plaintext) {
		return true
	}
	if check(in.ExpectedClientConfigContent) {
		return true
	}
	if in.ProxyAfterSnapshot != nil && check(in.ProxyAfterSnapshot.Plaintext) {
		return true
	}
	if in.ClientAfter != nil {
		if check(in.ClientAfter.ConfigContent) {
			return true
		}
		if in.ClientAfter.ConnectSecret != "" && strings.Contains(ev, in.ClientAfter.ConnectSecret) {
			return true
		}
	}
	return false
}

// guardLifecycleTxInTx validates AND locks the owning change_transaction row
// at the head of the commit (S1): it must exist, belong to tenant+actor,
// carry operation == the committed Op (an update tx can never commit a
// delete), still be verifying, and its proxy_id must be nil or the target.
//
// The lock discipline mirrors lockOwnerTransactionForAcquire:
//   - SQLite: WRITE-FIRST value-neutral touch UPDATE whose WHERE clause
//     re-validates transaction_id+tenant+actor+operation+status+proxy_id —
//     the single-writer model serializes this against any concurrent
//     status change, and the loser re-evaluates against the committed state.
//   - PostgreSQL/MySQL: SELECT ... FOR UPDATE takes the row lock for the
//     rest of the transaction; a concurrent guarded UPDATE blocks, then
//     re-evaluates against the committed state when this commits.
//
// Error contract: missing/cross-scope row → defs.ErrTransactionNotFound (no
// existence leak); wrong operation / wrong status / wrong proxy_id →
// defs.ErrInvalidTxTransition (stable sentinel).
func guardLifecycleTxInTx(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) error {
	scopeErr := func() error {
		return fmt.Errorf("%w: lifecycle transaction %s", defs.ErrTransactionNotFound, in.TxID)
	}
	stateErr := func(detail string) error {
		return fmt.Errorf("%w: %s", defs.ErrInvalidTxTransition, detail)
	}
	if tx.Dialector.Name() == "sqlite" {
		res := tx.Model(&models.ChangeTransaction{}).
			Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ? AND operation = ? AND status = ? AND (proxy_id IS NULL OR proxy_id = ?)",
				in.TxID, tenantID, userID, string(in.Op), models.TxStatusVerifying, in.ProxyID).
			Update("started_at", gorm.Expr("started_at"))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 1 {
			return nil
		}
		// miss: classify (scope vs state)
		var row models.ChangeTransaction
		err := tx.Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ?",
			in.TxID, tenantID, userID).First(&row).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return scopeErr()
			}
			return err
		}
		return classifyLifecycleTxState(stateErr, &row, in)
	}
	// PostgreSQL/MySQL: row-lock the owner row for the rest of this
	// transaction.
	var row models.ChangeTransaction
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ?",
			in.TxID, tenantID, userID).
		First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return scopeErr()
		}
		return err
	}
	return classifyLifecycleTxState(stateErr, &row, in)
}

func classifyLifecycleTxState(stateErr func(string) error, row *models.ChangeTransaction, in *CommitLifecycleInput) error {
	if row.Operation != string(in.Op) {
		return stateErr(fmt.Sprintf("transaction %s operation %q cannot be committed by lifecycle op %q",
			in.TxID, row.Operation, in.Op))
	}
	if row.Status != models.TxStatusVerifying {
		return stateErr(fmt.Sprintf("transaction %s is %s, only verifying may commit", in.TxID, row.Status))
	}
	if row.ProxyID != nil && *row.ProxyID != in.ProxyID {
		return stateErr(fmt.Sprintf("transaction %s proxy_id %d differs from the target %d",
			in.TxID, *row.ProxyID, in.ProxyID))
	}
	return nil
}

// lifecycleRowGuardQuery locks lifecycle-owned proxy/client rows until the
// commit transaction ends. SQLite already holds the single-writer lock from
// guardLifecycleTxInTx's write-first touch and does not support FOR UPDATE;
// PostgreSQL/MySQL need an explicit row lock, especially on fast no-op paths
// where S3/S4 intentionally issue no guarded UPDATE.
func lifecycleRowGuardQuery(tx *gorm.DB) *gorm.DB {
	if tx.Dialector.Name() == "sqlite" {
		return tx
	}
	return tx.Clauses(clause.Locking{Strength: "UPDATE"})
}

// loadLifecycleProxyRowInTx re-verifies the old-rule identity the plan
// resolved (S1): the row must exist in tenant+user scope, carry the planned
// name/server AND Stopped == ExpectedStopped (the state TOCTOU guard that is
// mandatory for start/stop/delete alike), sit in one of the two legal
// shapes, and its REAL typed physical key (parsed from its content) must
// equal Reservation.OldKey — a commit must never flip/delete a lock for a
// port the rule does not run on. An unparseable row fails closed too.
//
// A missing row is ErrProxyConfigNotFound; any divergence is
// ErrProxyConfigChanged (fail closed, whole commit rolls back).
func loadLifecycleProxyRowInTx(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) (*models.ProxyConfig, error) {
	var old models.ProxyConfig
	err := lifecycleRowGuardQuery(tx).
		Where("id = ? AND tenant_id = ? AND user_id = ?", in.ProxyID, tenantID, userID).
		First(&old).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: proxy config row %d", defs.ErrProxyConfigNotFound, in.ProxyID)
		}
		return nil, err
	}
	if old.ServerID != in.ServerID || old.Name != in.Name {
		return nil, fmt.Errorf("%w: row %d is %s/%s, plan said %s/%s",
			defs.ErrProxyConfigChanged, in.ProxyID, old.ServerID, old.Name, in.ServerID, in.Name)
	}
	if old.Stopped != in.ExpectedStopped {
		return nil, fmt.Errorf("%w: row %d stopped=%v, plan expected %v",
			defs.ErrProxyConfigChanged, in.ProxyID, old.Stopped, in.ExpectedStopped)
	}
	// P0 physical-key binding: the row's REAL typed key must equal the old
	// reservation key.
	oldKey, err := proxyRowPhysicalKey(&old)
	if err != nil {
		return nil, fmt.Errorf("%w: row %d physical key: %v", defs.ErrProxyConfigChanged, in.ProxyID, err)
	}
	if oldKey != in.Reservation.OldKey {
		return nil, fmt.Errorf("%w: row %d physical key %s/%s/%d differs from the old reservation key %s/%s/%d",
			defs.ErrProxyConfigChanged, in.ProxyID,
			oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort,
			in.Reservation.OldKey.ServerID, in.Reservation.OldKey.Protocol, in.Reservation.OldKey.RemotePort)
	}
	// shape: both legal shapes require row.ClientID == ClientAfter.ClientID
	// (the lifecycle never moves the rule across clients); the origin link
	// then picks the shape.
	if old.ClientID != in.ClientAfter.ClientID {
		return nil, fmt.Errorf("%w: row %d is on client %s, plan said client %s",
			defs.ErrProxyConfigChanged, in.ProxyID, old.ClientID, in.ClientAfter.ClientID)
	}
	if old.OriginClientID == "" {
		// origin-shaped: the never-shadowed origin hosts the row directly
		if old.ClientID != in.OriginClientID {
			return nil, fmt.Errorf("%w: row %d is origin-shaped but its client %s differs from the origin %s",
				defs.ErrProxyConfigChanged, in.ProxyID, old.ClientID, in.OriginClientID)
		}
	} else if old.OriginClientID != in.OriginClientID {
		// child-shaped: the origin link must match the plan
		return nil, fmt.Errorf("%w: row %d origin %q differs from the planned origin %q",
			defs.ErrProxyConfigChanged, in.ProxyID, old.OriginClientID, in.OriginClientID)
	}
	return &old, nil
}

// guardLifecycleClientInTx validates the client row the rule lives on (S2):
// it must exist in tenant+user scope, its origin link must equal the proxy
// row's (a child row pointing at a different origin — or an origin-shaped
// rule on a child client — is drift), child clients must carry the planned
// server+origin while origin clients tolerate empty server/origin (legacy
// leniency), and the persisted config_content must be byte-identical to
// ExpectedClientConfigContent (the DB CAS bytes — the agent snapshot
// plaintext is a different value and never compared here).
//
// Missing → ErrClientNotFound; any divergence → ErrProxyConfigChanged.
func guardLifecycleClientInTx(tx *gorm.DB, tenantID, userID int, old *models.ProxyConfig, in *CommitLifecycleInput) error {
	var client models.Client
	err := lifecycleRowGuardQuery(tx).
		Where("client_id = ? AND tenant_id = ? AND user_id = ?",
			in.ClientAfter.ClientID, tenantID, userID).
		First(&client).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: lifecycle client %s", defs.ErrClientNotFound, in.ClientAfter.ClientID)
		}
		return err
	}
	if client.OriginClientID != old.OriginClientID {
		return fmt.Errorf("%w: client %s origin %q differs from proxy row %d origin %q",
			defs.ErrProxyConfigChanged, in.ClientAfter.ClientID, client.OriginClientID, old.ID, old.OriginClientID)
	}
	if client.OriginClientID != "" {
		// child-shaped client: server + origin must match the plan
		if client.ServerID != in.ServerID || client.OriginClientID != in.OriginClientID {
			return fmt.Errorf("%w: child client %s shape (server=%s origin=%s) differs from the plan (server=%s origin=%s)",
				defs.ErrProxyConfigChanged, in.ClientAfter.ClientID, client.ServerID, client.OriginClientID, in.ServerID, in.OriginClientID)
		}
	} else if client.ServerID != "" && client.ServerID != in.ServerID {
		// origin-shaped client: server may be empty, but a SET server must
		// not contradict the rule's server
		return fmt.Errorf("%w: origin client %s server %q differs from the rule server %q",
			defs.ErrProxyConfigChanged, in.ClientAfter.ClientID, client.ServerID, in.ServerID)
	}
	if !exactNullableBytesEqual(client.ConfigContent, in.ExpectedClientConfigContent) {
		return fmt.Errorf("%w: client %s config drifted (expected %d bytes, found %d bytes)",
			defs.ErrProxyConfigChanged, in.ClientAfter.ClientID, len(in.ExpectedClientConfigContent), len(client.ConfigContent))
	}
	return nil
}

// exactNullableBytesEqual preserves the persisted SQL distinction between
// NULL and a zero-length BLOB. bytes.Equal(nil, []byte{}) is true, which is
// correct for payload equality but too weak for a TOCTOU/CAS guard: changing
// NULL to X” (or back) is still a concurrent database mutation.
func exactNullableBytesEqual(left, right []byte) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return bytes.Equal(left, right)
}

// writeLifecycleProxyInTx implements the S3 proxy write.
//   - delete: guarded Unscoped hard delete of the exact row
//     (id+tenant+user+name+server+stopped) — RowsAffected must be 1.
//   - start/stop: guarded UPDATE of stopped ONLY (content/name/type/id and
//     every sibling stay untouched; UpdatedAt is GORM's normal handling).
//     When target == ExpectedStopped the row is already in the final state
//     and NO UPDATE statement is issued at all — S1 already guarded it, and
//     a value-identical UPDATE's MySQL RowsAffected=0 can never be misread
//     as a CAS miss.
//
// A 0-row guarded write is NEVER a success: the row is reloaded and
// classified (missing → ErrProxyConfigNotFound, changed → ErrProxyConfigChanged).
func writeLifecycleProxyInTx(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) error {
	switch in.Op {
	case LifecycleOpDelete:
		res := tx.Unscoped().
			Where("id = ? AND tenant_id = ? AND user_id = ? AND name = ? AND server_id = ? AND stopped = ?",
				in.ProxyID, tenantID, userID, in.Name, in.ServerID, in.ExpectedStopped).
			Delete(&models.ProxyConfig{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return lifecycleProxyWriteMiss(tx, tenantID, userID, in)
		}
		return nil
	default: // start / stop
		target := in.Op == LifecycleOpStop // stop -> true, start -> false
		if target == in.ExpectedStopped {
			// already-state fast path: skip the write entirely
			return nil
		}
		res := tx.Model(&models.ProxyConfig{}).
			Where("id = ? AND tenant_id = ? AND user_id = ? AND name = ? AND server_id = ? AND stopped = ?",
				in.ProxyID, tenantID, userID, in.Name, in.ServerID, in.ExpectedStopped).
			Update("stopped", target)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return lifecycleProxyWriteMiss(tx, tenantID, userID, in)
		}
		return nil
	}
}

func lifecycleProxyWriteMiss(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) error {
	var row models.ProxyConfig
	err := tx.Where("id = ? AND tenant_id = ? AND user_id = ?", in.ProxyID, tenantID, userID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: proxy config row %d", defs.ErrProxyConfigNotFound, in.ProxyID)
		}
		return err
	}
	return fmt.Errorf("%w: row %d changed between guard and write", defs.ErrProxyConfigChanged, in.ProxyID)
}

// writeLifecycleClientInTx implements the S4 client write: ONLY
// config_content may change — secret/comment/stopped/identity are never
// written by the lifecycle commit. When the after bytes equal the expected
// bytes no UPDATE is issued (value-identical skip preserves the whole-row
// UpdatedAt). Otherwise the guarded UPDATE re-verifies the exact expected
// bytes in its WHERE (the CAS double-guard on top of S2); nil maps only to
// SQL NULL while a non-nil empty slice maps only to a zero-length blob.
func writeLifecycleClientInTx(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) error {
	if exactNullableBytesEqual(in.ExpectedClientConfigContent, in.ClientAfter.ConfigContent) {
		return nil
	}
	q := tx.Model(&models.Client{}).
		Where("client_id = ? AND tenant_id = ? AND user_id = ?",
			in.ClientAfter.ClientID, tenantID, userID)
	if in.ExpectedClientConfigContent == nil {
		q = q.Where("config_content IS NULL")
	} else {
		q = q.Where("config_content = ?", in.ExpectedClientConfigContent)
	}
	res := q.Update("config_content", in.ClientAfter.ConfigContent)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		var row models.Client
		err := tx.Where("client_id = ? AND tenant_id = ? AND user_id = ?",
			in.ClientAfter.ClientID, tenantID, userID).First(&row).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: lifecycle client %s", defs.ErrClientNotFound, in.ClientAfter.ClientID)
			}
			return err
		}
		return fmt.Errorf("%w: client %s config drifted between guard and write",
			defs.ErrProxyConfigChanged, in.ClientAfter.ClientID)
	}
	return nil
}

// migrateLifecycleReservationInTx implements the S6 reservation migration.
// The physical key NEVER changes; the mode is re-validated against the DB
// instead of trusting the caller:
//   - SameKeyOwned start/stop: the row id never changes, so the owner CAS
//     is a shape check only — transaction_id is NEVER rewritten.
//   - SameKeyOwned delete: the committed row is hard-deleted BY OWNER at
//     the exact key (must be 1 row).
//   - SameKeyPending start/stop: THIS tx's pending lease flips to
//     committed with owner = the old proxy id.
//   - SameKeyPending delete: THIS tx's pending lease is hard-deleted by
//     exact tx+key+pending (must be 1 row) — the delete tx id can never
//     touch an old committed row.
func migrateLifecycleReservationInTx(tx *gorm.DB, tenantID int, in *CommitLifecycleInput) error {
	key := in.Reservation.OldKey
	switch in.Reservation.Mode {
	case ReservationModeSameKeyOwned:
		if in.Op == LifecycleOpDelete {
			if err := releaseOldCommittedInTx(tx, tenantID, key, in.ProxyID); err != nil {
				return fmt.Errorf("lifecycle delete: %w", err)
			}
			return nil
		}
		if err := casReservationOwnerInTx(tx, tenantID, key, in.ProxyID, in.ProxyID); err != nil {
			return fmt.Errorf("lifecycle %s: same-key owned reservation CAS: %w", in.Op, err)
		}
		return nil
	case ReservationModeSameKeyPending:
		if in.Op == LifecycleOpDelete {
			return deleteLifecyclePendingInTx(tx, tenantID, key, in.TxID)
		}
		return commitPendingByKeyInTx(tx, tenantID, key, in.TxID, in.ProxyID)
	default:
		return fmt.Errorf("%w: lifecycle reservation mode %q is not supported",
			defs.ErrInvalidCommitInput, in.Reservation.Mode)
	}
}

// deleteLifecyclePendingInTx hard-deletes the pending lease THIS transaction
// acquired at the key. The scope is exact: tenant + key + transaction_id +
// status=pending, so a delete commit can never delete a committed row whose
// transaction_id is an older create tx. 0 rows is NOT tolerated — the
// reservation must exist and be the caller's own pending lease.
func deleteLifecyclePendingInTx(tx *gorm.DB, tenantID int, key ReservationKey, txID string) error {
	res := tx.Unscoped().
		Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ? AND transaction_id = ? AND status = ?",
			tenantID, key.ServerID, key.Protocol, key.RemotePort, txID, models.ReservationStatusPending).
		Delete(&models.PortReservation{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 1 {
		return nil
	}
	var row models.PortReservation
	err := tx.Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ?",
		tenantID, key.ServerID, key.Protocol, key.RemotePort).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: pending reservation %s/%s/%d for tx %s",
				defs.ErrReservationNotFound, key.ServerID, key.Protocol, key.RemotePort, txID)
		}
		return err
	}
	return fmt.Errorf("%w: reservation %s/%s/%d is not a pending lease of tx %s (status=%s tx=%s)",
		defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort, txID, row.Status, row.TransactionID)
}

// saveLifecycleAuditInTx persists the commit audit event with
// TransactionID, TenantID, ActorUserID and the op-specific EventType FORCED
// (spec §6.4): delete→delete, start→start, stop→stop. The summary redaction
// guard ran in S0; EvidenceRef and the other caller-supplied fields are kept.
func saveLifecycleAuditInTx(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) error {
	audit := in.AuditEvent
	audit.TransactionID = in.TxID
	audit.TenantID = tenantID
	audit.ActorUserID = userID
	switch in.Op {
	case LifecycleOpDelete:
		audit.EventType = models.AuditEventTypeDelete
	case LifecycleOpStart:
		audit.EventType = models.AuditEventTypeStart
	case LifecycleOpStop:
		audit.EventType = models.AuditEventTypeStop
	}
	return tx.Create(audit).Error
}

// writeLifecycleEvidenceInTx persists the optional evidence onto the
// change_transaction row INSIDE the commit transaction (S8.5). The guarded
// UPDATE re-verifies tenant+actor+operation+verifying; a miss is
// classified — an already-verifying row carrying the exact same evidence is
// an idempotent success (MySQL value-identical UPDATE reports 0 rows),
// anything else fails closed.
func writeLifecycleEvidenceInTx(tx *gorm.DB, tenantID, userID int, in *CommitLifecycleInput) error {
	res := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ? AND operation = ? AND status = ?",
			in.TxID, tenantID, userID, string(in.Op), models.TxStatusVerifying).
		Update("evidence_json", in.EvidenceJSON)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 1 {
		return nil
	}
	var row models.ChangeTransaction
	err := tx.Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ?",
		in.TxID, tenantID, userID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: transaction %s", defs.ErrTransactionNotFound, in.TxID)
		}
		return err
	}
	if row.Status == models.TxStatusVerifying && row.EvidenceJSON == in.EvidenceJSON {
		return nil
	}
	return fmt.Errorf("%w: transaction %s evidence write missed", defs.ErrTxStatusConflict, in.TxID)
}
