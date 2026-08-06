package dao

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// RollbackReservationMode describes how the port reservation migrates during
// a manual rollback commit (D2c-C). Every mode is re-validated against the
// DB inside the transaction; the rollback transaction itself NEVER acquires
// a pending lease and committed leases are ONLY ever touched by exact
// physical key + owner_proxy_id — never by the rollback transaction id.
type RollbackReservationMode string

const (
	// RollbackReservationRelease deletes the CURRENT committed lease at the
	// key owned by the current rule row (create rollback: the rule is being
	// removed, its lease must go with it BY OWNER).
	RollbackReservationRelease RollbackReservationMode = "release"
	// RollbackReservationRestore recreates (or retains) a committed lease at
	// the key owned by the RESTORED rule row, stamped with the SOURCE
	// transaction id (delete rollback: the rule comes back, its port lock
	// must come back with it — but never owned by the rollback tx).
	RollbackReservationRestore RollbackReservationMode = "restore"
	// RollbackReservationSameKey shape-checks the committed lease at the key
	// owned by the (unchanged) rule row id (start/stop rollback: the row id
	// never changes, so neither does the owner; transaction_id NEVER
	// rewritten).
	RollbackReservationSameKey RollbackReservationMode = "same_key"
	// RollbackReservationOwnerCAS migrates the committed lease owner from
	// the current rule row to the restored rule row at the same key (update
	// rollback, same physical key, different row ids).
	RollbackReservationOwnerCAS RollbackReservationMode = "owner_cas"
	// RollbackReservationKeySwitch releases the CURRENT (after) key lease
	// owned by the current row and restores the BEFORE key lease owned by
	// the restored row (update rollback that changed the physical key).
	RollbackReservationKeySwitch RollbackReservationMode = "key_switch"
)

// RollbackReservationStep is the reservation migration instruction of one
// rollback commit. Mode determines which fields are meaningful:
//
//	release:     Key + CurrentProxyID
//	restore:     Key + DesiredProxyID + SourceTxID
//	same_key:    Key + CurrentProxyID
//	owner_cas:   Key + CurrentProxyID + DesiredProxyID
//	key_switch:  ReleaseKey + CurrentProxyID + RestoreKey + DesiredProxyID
//	             + SourceTxID
type RollbackReservationStep struct {
	Mode           RollbackReservationMode
	Key            ReservationKey // release / restore / same_key / owner_cas
	ReleaseKey     ReservationKey // key_switch: the current (after) key
	RestoreKey     ReservationKey // key_switch: the before key
	CurrentProxyID uint
	DesiredProxyID uint
	SourceTxID     string // stamped on recreated committed leases (NEVER the rollback tx id)
}

// CommitRollbackInput is the complete payload of ONE manual rollback commit
// (D2c-C). It atomically: guards the rollback transaction (tenant+actor+
// operation=rollback+verifying) and the SOURCE transaction (tenant+actor+
// operation+committed, READ ONLY — the source row never transitions),
// CAS-writes the current client config back to the source-before plaintext,
// writes the proxy rows per the source operation (delete current /
// recreate exact before row / in-place restore / stopped flip), migrates the
// reservation by exact physical key + owner_proxy_id, saves the encrypted
// AFTER snapshots (client always, proxy when a desired row exists), forces
// the rollback audit event + evidence, and commits the rollback transaction
// LAST.
//
// All values are tenant-scoped by the caller; the DAO re-checks the tenant,
// the operation, the client bytes and the reservation state on every row it
// touches. The source transaction is NEVER modified.
type CommitRollbackInput struct {
	// TxID is the ROLLBACK transaction (operation=rollback).
	TxID string
	// SourceTxID is the committed transaction being rolled back; its row is
	// guarded read-only (operation == SourceOp, status == committed) and
	// never transitions.
	SourceTxID string
	// SourceOp is the source transaction's operation (create/update/delete/
	// start/stop) — it selects the proxy write shape.
	SourceOp string
	// TxProxyID is the rollback transaction's proxy_id (the current rule
	// row id) — the S1 guard requires the rollback tx to carry it.
	TxProxyID uint

	// current client state (S2): the row ClientNodeID must currently hold
	// ExpectedClientConfig (the source-AFTER plaintext — the TOCTOU CAS
	// bytes) and is written to DesiredClientConfig (the source-BEFORE
	// plaintext).
	ClientNodeID         string
	ExpectedClientConfig []byte
	DesiredClientConfig  []byte

	// proxy state (S3). CurrentProxy describes the row as it must exist NOW
	// (nil = must be absent); DesiredProxy is the row to persist AFTER (nil
	// = the current row must be deleted). Same id = in-place update; a
	// different id = delete current + insert desired with the exact id.
	CurrentProxy *models.ProxyConfig
	DesiredProxy *models.ProxyConfig

	// reservation migration (S4)
	Reservation RollbackReservationStep

	// AFTER-only encrypted snapshots (S5). ClientAfterSnapshot is always
	// written (node_id forced to ClientNodeID); ProxyAfterSnapshot is
	// required exactly when a desired proxy row exists (node_id forced to
	// its id). Before snapshots for the rollback transaction are saved by
	// the coordinator BEFORE apply and are never written here.
	ClientAfterSnapshot SnapshotInput
	ProxyAfterSnapshot  *SnapshotInput

	// rollback audit event (S6): TransactionID/TenantID/ActorUserID/
	// EventType forced by the DAO; summary must not embed plaintext.
	AuditEvent *models.AuditEvent
	// EvidenceJSON is optional metadata written onto the rollback
	// transaction INSIDE the commit (before the committed transition); it
	// must never carry config/secret material.
	EvidenceJSON string
}

// CommitRollback atomically persists the final state of a manual rollback
// (D2c-C). Returns the CURRENT proxy row id (the reservation owner before
// the commit; 0 for a delete rollback whose current row is absent).
//
// Transaction order (any failure rolls the whole commit back):
//
//	S0  static validation — zero DB writes, stable ErrInvalidCommitInput
//	S1  rollback tx guard (tenant+actor+TxID, operation=rollback,
//	    status=verifying, proxy_id == TxProxyID; write-first lock on
//	    SQLite, SELECT ... FOR UPDATE elsewhere) + SOURCE tx guard
//	    (tenant+actor+SourceTxID, operation == SourceOp, status ==
//	    committed — read-only, never transitioned)
//	S2  client guard + CAS write (persisted config_content must equal
//	    ExpectedClientConfig → ErrClientNotFound / ErrProxyConfigChanged;
//	    then write DesiredClientConfig, guarded)
//	S3  proxy write (create: guarded hard delete of the current row;
//	    delete: guarded INSERT of the exact before row; update: in-place
//	    update when ids match else delete current + insert before;
//	    start/stop: in-place stopped flip)
//	S4  reservation migration (exact key + owner_proxy_id; re-validated)
//	S5  encrypted AFTER snapshots (client always; proxy iff desired row)
//	S6  rollback audit event (forced tenant/actor/EventType)
//	S6.5 evidence (optional; verifying CAS)
//	S7  transition rollback tx → committed (last step)
func (m *transactionMutation) CommitRollback(userInfo models.UserInfo, in *CommitRollbackInput) (uint, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return 0, err
	}
	// S0 static validation — zero DB writes; every violation is the stable
	// ErrInvalidCommitInput BEFORE the transaction begins.
	if err := validateCommitRollbackInput(in); err != nil {
		return 0, err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	tenantID := userInfo.GetTenantID()
	userID := userInfo.GetUserID()

	currentProxyID := uint(0)
	if in.CurrentProxy != nil {
		currentProxyID = in.CurrentProxy.ID
	}
	// The reservation-relevant row id: the current row for a create
	// rollback (the row being deleted), the DESIRED row for every restore
	// shape (delete/update/start/stop — the row that owns the lease after).
	ownerID := currentProxyID
	if in.CurrentProxy == nil && in.DesiredProxy != nil {
		ownerID = in.DesiredProxy.ID
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		// S1 rollback tx guard (write-first lock / FOR UPDATE) + source tx
		// guard (read-only, committed).
		if err := guardRollbackTxInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit rollback: %w", err)
		}
		if err := guardRollbackSourceTxInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit rollback: %w", err)
		}
		// S2 client guard + CAS write (mirrors the lifecycle commit).
		if err := writeRollbackClientInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit rollback: %w", err)
		}
		// S3 proxy write per source operation.
		if err := writeRollbackProxyInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit rollback: %w", err)
		}
		// S4 reservation migration (exact key + owner, re-validated).
		if err := migrateRollbackReservationInTx(tx, tenantID, in); err != nil {
			return fmt.Errorf("commit rollback: %w", err)
		}
		// S5 encrypted AFTER snapshots (client always; proxy iff desired row).
		if err := saveAfterSnapshotInTx(tx, tenantID, in.TxID, in.ClientAfterSnapshot,
			models.SnapshotNodeTypeClient, in.ClientNodeID); err != nil {
			return fmt.Errorf("commit rollback: save client after snapshot: %w", err)
		}
		if in.ProxyAfterSnapshot != nil {
			if err := saveAfterSnapshotInTx(tx, tenantID, in.TxID, *in.ProxyAfterSnapshot,
				models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(in.DesiredProxy.ID), 10)); err != nil {
				return fmt.Errorf("commit rollback: save proxy after snapshot: %w", err)
			}
		}
		// S6 rollback audit (TxID/tenant/actor/EventType forced).
		if err := saveRollbackAuditInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit rollback: %w", err)
		}
		// S6.5 evidence (optional; lands atomically with the commit).
		if in.EvidenceJSON != "" {
			if err := writeRollbackEvidenceInTx(tx, tenantID, userID, in); err != nil {
				return fmt.Errorf("commit rollback: write evidence: %w", err)
			}
		}
		// S7 rollback tx -> committed (last step). The source tx row is
		// never touched.
		return transitionTxStatus(tx, tenantID, in.TxID, models.TxStatusCommitted, "", "")
	})
	if err != nil {
		return 0, err
	}
	return ownerID, nil
}

// validateCommitRollbackInput enforces the static shape contract (S0) with
// ZERO DB writes: op-consistent proxy geometry, reservation mode/key
// geometry with the physical-key binding (the CURRENT row's real typed key
// must equal the release/current key and the DESIRED row's real typed key
// must equal the restore key — a rollback must never touch a lock for a
// port the rule does not run on), after-snapshot material and audit
// presence/redaction. The AES-256 KEY SIZE is deliberately NOT checked
// here: a wrong-size key must fail inside the transaction at S5 to prove
// the atomic rollback contract.
func validateCommitRollbackInput(in *CommitRollbackInput) error {
	invalid := func(format string, args ...interface{}) error {
		return fmt.Errorf("%w: %s", defs.ErrInvalidCommitInput, fmt.Sprintf(format, args...))
	}
	if in == nil {
		return invalid("nil input")
	}
	if in.TxID == "" {
		return invalid("missing transaction id")
	}
	if in.SourceTxID == "" {
		return invalid("missing source transaction id")
	}
	if in.TxProxyID == 0 {
		return invalid("missing rollback transaction proxy id")
	}
	switch in.SourceOp {
	case models.TxOperationCreate, models.TxOperationUpdate, models.TxOperationDelete,
		models.TxOperationStart, models.TxOperationStop:
	default:
		return invalid("source operation %q is not rollback-able", in.SourceOp)
	}
	if in.ClientNodeID == "" {
		return invalid("missing client node id")
	}
	if len(in.ExpectedClientConfig) == 0 {
		return invalid("missing expected client config")
	}
	if len(in.DesiredClientConfig) == 0 {
		return invalid("missing desired client config")
	}

	// proxy geometry per source operation
	cur, des := in.CurrentProxy, in.DesiredProxy
	switch in.SourceOp {
	case models.TxOperationCreate:
		if cur == nil || des != nil {
			return invalid("create rollback requires a current proxy row and NO desired row")
		}
		if in.Reservation.Mode != RollbackReservationRelease {
			return invalid("create rollback reservation mode must be release")
		}
	case models.TxOperationDelete:
		if cur != nil || des == nil {
			return invalid("delete rollback requires a desired proxy row and NO current row")
		}
		if in.Reservation.Mode != RollbackReservationRestore {
			return invalid("delete rollback reservation mode must be restore")
		}
	case models.TxOperationUpdate:
		if cur == nil || des == nil {
			return invalid("update rollback requires current AND desired proxy rows")
		}
		switch in.Reservation.Mode {
		case RollbackReservationOwnerCAS, RollbackReservationKeySwitch:
		default:
			return invalid("update rollback reservation mode must be owner_cas or key_switch")
		}
	case models.TxOperationStart, models.TxOperationStop:
		if cur == nil || des == nil {
			return invalid("%s rollback requires current AND desired proxy rows", in.SourceOp)
		}
		if des.ID != cur.ID {
			return invalid("%s rollback never changes the proxy row id (current %d, desired %d)", in.SourceOp, cur.ID, des.ID)
		}
		if in.Reservation.Mode != RollbackReservationSameKey {
			return invalid("%s rollback reservation mode must be same_key", in.SourceOp)
		}
	}

	// reservation geometry + physical-key binding
	step := in.Reservation
	switch step.Mode {
	case RollbackReservationRelease:
		if err := validateReservationKey(invalid, step.Key); err != nil {
			return err
		}
		if step.CurrentProxyID == 0 || step.CurrentProxyID != cur.ID {
			return invalid("release requires the current proxy id to match the current row")
		}
		if err := requireProxyKeyMatches(invalid, cur, step.Key); err != nil {
			return err
		}
	case RollbackReservationRestore:
		if err := validateReservationKey(invalid, step.Key); err != nil {
			return err
		}
		if step.DesiredProxyID == 0 || step.DesiredProxyID != des.ID {
			return invalid("restore requires the desired proxy id to match the desired row")
		}
		if step.SourceTxID != in.SourceTxID {
			return invalid("restore must stamp the source transaction id")
		}
		if err := requireProxyKeyMatches(invalid, des, step.Key); err != nil {
			return err
		}
	case RollbackReservationSameKey:
		if err := validateReservationKey(invalid, step.Key); err != nil {
			return err
		}
		if step.CurrentProxyID == 0 || step.CurrentProxyID != cur.ID {
			return invalid("same_key requires the current proxy id to match the current row")
		}
		if err := requireProxyKeyMatches(invalid, cur, step.Key); err != nil {
			return err
		}
	case RollbackReservationOwnerCAS:
		if err := validateReservationKey(invalid, step.Key); err != nil {
			return err
		}
		if step.CurrentProxyID == 0 || step.CurrentProxyID != cur.ID {
			return invalid("owner_cas requires the current proxy id to match the current row")
		}
		if step.DesiredProxyID == 0 || step.DesiredProxyID != des.ID {
			return invalid("owner_cas requires the desired proxy id to match the desired row")
		}
		if err := requireProxyKeyMatches(invalid, cur, step.Key); err != nil {
			return err
		}
		if err := requireProxyKeyMatches(invalid, des, step.Key); err != nil {
			return err
		}
	case RollbackReservationKeySwitch:
		if err := validateReservationKey(invalid, step.ReleaseKey); err != nil {
			return err
		}
		if err := validateReservationKey(invalid, step.RestoreKey); err != nil {
			return err
		}
		if step.ReleaseKey == step.RestoreKey {
			return invalid("key_switch requires different release/restore keys")
		}
		if step.CurrentProxyID == 0 || step.CurrentProxyID != cur.ID {
			return invalid("key_switch requires the current proxy id to match the current row")
		}
		if step.DesiredProxyID == 0 || step.DesiredProxyID != des.ID {
			return invalid("key_switch requires the desired proxy id to match the desired row")
		}
		if step.SourceTxID != in.SourceTxID {
			return invalid("key_switch must stamp the source transaction id")
		}
		if err := requireProxyKeyMatches(invalid, cur, step.ReleaseKey); err != nil {
			return err
		}
		if err := requireProxyKeyMatches(invalid, des, step.RestoreKey); err != nil {
			return err
		}
	default:
		return invalid("reservation mode %q is not valid for rollback commits", step.Mode)
	}

	// after snapshots: client always required with key+plaintext; proxy
	// exactly when a desired row exists.
	if len(in.ClientAfterSnapshot.Key) == 0 {
		return invalid("client after snapshot key must not be empty")
	}
	if len(in.ClientAfterSnapshot.Plaintext) == 0 {
		return invalid("client after snapshot plaintext must not be empty")
	}
	if des != nil {
		if in.ProxyAfterSnapshot == nil {
			return invalid("a desired proxy row requires the proxy after snapshot")
		}
		if len(in.ProxyAfterSnapshot.Key) == 0 {
			return invalid("proxy after snapshot key must not be empty")
		}
		if len(in.ProxyAfterSnapshot.Plaintext) == 0 {
			return invalid("proxy after snapshot plaintext must not be empty")
		}
	} else if in.ProxyAfterSnapshot != nil {
		return invalid("no desired proxy row means no proxy after snapshot")
	}

	// audit: required, and the summary must not embed the plaintext
	// snapshots (the audit trail is redacted by construction, spec §6.4).
	if in.AuditEvent == nil {
		return invalid("audit event is required")
	}
	proxyPlain := []byte(nil)
	if in.ProxyAfterSnapshot != nil {
		proxyPlain = in.ProxyAfterSnapshot.Plaintext
	}
	if summaryContainsPlaintext(in.AuditEvent.Summary,
		in.ClientAfterSnapshot.Plaintext,
		proxyPlain,
		in.ExpectedClientConfig,
		in.DesiredClientConfig,
	) {
		return invalid("audit summary must not contain plaintext config material")
	}

	// evidence: optional, valid JSON, capped, and never config/secret
	// material (S6.5)
	if in.EvidenceJSON != "" {
		if err := validateRollbackEvidence(invalid, in); err != nil {
			return err
		}
	}
	return nil
}

// requireProxyKeyMatches binds a proxy row's REAL typed physical key to the
// reservation key the commit plans to touch — a malformed/nil-base row or a
// rule that runs a different port than the lock fails closed at S0.
func requireProxyKeyMatches(invalid func(string, ...interface{}) error, row *models.ProxyConfig, key ReservationKey) error {
	if row == nil {
		return invalid("missing proxy row for key binding")
	}
	rowKey, err := proxyRowPhysicalKey(row)
	if err != nil {
		return invalid("proxy row %d typed content: %v", row.ID, err)
	}
	if rowKey != key {
		return invalid("proxy row %d physical key %s/%s/%d differs from the reservation key %s/%s/%d",
			row.ID, rowKey.ServerID, rowKey.Protocol, rowKey.RemotePort,
			key.ServerID, key.Protocol, key.RemotePort)
	}
	return nil
}

// validateRollbackEvidence enforces the S6.5 evidence contract: valid JSON,
// at most 64KiB, and free of every plaintext payload the commit carries.
func validateRollbackEvidence(invalid func(string, ...interface{}) error, in *CommitRollbackInput) error {
	if len(in.EvidenceJSON) > maxEvidenceJSONBytes {
		return invalid("evidence json exceeds the %d byte cap", maxEvidenceJSONBytes)
	}
	if !json.Valid([]byte(in.EvidenceJSON)) {
		return invalid("evidence json is not valid json")
	}
	check := func(p []byte) bool {
		return len(p) > 0 && bytes.Contains([]byte(in.EvidenceJSON), p)
	}
	if check(in.ClientAfterSnapshot.Plaintext) || check(in.ExpectedClientConfig) || check(in.DesiredClientConfig) {
		return invalid("evidence must not contain config/secret material")
	}
	if in.ProxyAfterSnapshot != nil && check(in.ProxyAfterSnapshot.Plaintext) {
		return invalid("evidence must not contain config/secret material")
	}
	return nil
}

// guardRollbackTxInTx validates AND locks the rollback change_transaction
// row at the head of the commit (S1): it must exist, belong to
// tenant+actor, carry operation=rollback, still be verifying, and its
// proxy_id must equal TxProxyID. The lock discipline mirrors
// guardLifecycleTxInTx (write-first touch on SQLite, SELECT ... FOR UPDATE
// elsewhere); a miss is classified (missing/cross-scope →
// ErrTransactionNotFound, wrong state → ErrInvalidTxTransition).
func guardRollbackTxInTx(tx *gorm.DB, tenantID, userID int, in *CommitRollbackInput) error {
	scopeErr := func() error {
		return fmt.Errorf("%w: rollback transaction %s", defs.ErrTransactionNotFound, in.TxID)
	}
	stateErr := func(detail string) error {
		return fmt.Errorf("%w: %s", defs.ErrInvalidTxTransition, detail)
	}
	if tx.Dialector.Name() == "sqlite" {
		res := tx.Model(&models.ChangeTransaction{}).
			Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ? AND operation = ? AND status = ? AND proxy_id = ?",
				in.TxID, tenantID, userID, models.TxOperationRollback, models.TxStatusVerifying, in.TxProxyID).
			Update("started_at", gorm.Expr("started_at"))
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
				return scopeErr()
			}
			return err
		}
		return classifyRollbackTxState(stateErr, &row, in)
	}
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
	return classifyRollbackTxState(stateErr, &row, in)
}

func classifyRollbackTxState(stateErr func(string) error, row *models.ChangeTransaction, in *CommitRollbackInput) error {
	if row.Operation != models.TxOperationRollback {
		return stateErr(fmt.Sprintf("transaction %s operation %q is not rollback", in.TxID, row.Operation))
	}
	if row.Status != models.TxStatusVerifying {
		return stateErr(fmt.Sprintf("transaction %s is %s, only verifying may commit a rollback", in.TxID, row.Status))
	}
	if row.ProxyID == nil || *row.ProxyID != in.TxProxyID {
		return stateErr(fmt.Sprintf("transaction %s proxy_id %v differs from the target %d", in.TxID, row.ProxyID, in.TxProxyID))
	}
	return nil
}

// guardRollbackSourceTxInTx validates the SOURCE transaction (S1) WITHOUT
// ever transitioning it: it must exist in tenant+actor scope, carry the
// declared source operation and still be committed. A committed transaction
// is terminal and never changes, so the guard is a plain read — the row is
// deliberately never locked or written.
func guardRollbackSourceTxInTx(tx *gorm.DB, tenantID, userID int, in *CommitRollbackInput) error {
	var src models.ChangeTransaction
	err := tx.Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ?",
		in.SourceTxID, tenantID, userID).First(&src).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: source transaction %s", defs.ErrTransactionNotFound, in.SourceTxID)
		}
		return err
	}
	if src.Operation != in.SourceOp || src.Status != models.TxStatusCommitted {
		return fmt.Errorf("%w: source transaction %s is %s/%s, only committed %s may be rolled back",
			defs.ErrInvalidTxTransition, in.SourceTxID, src.Operation, src.Status, in.SourceOp)
	}
	return nil
}

// writeRollbackClientInTx implements S2: the client row ClientNodeID must
// exist in tenant+user scope and its persisted config_content must be
// byte-identical to ExpectedClientConfig (the source-AFTER plaintext — the
// TOCTOU guard: a rollback must never clobber newer edits). When the
// desired bytes differ, the guarded UPDATE re-verifies the exact expected
// bytes in its WHERE (nil maps only to SQL NULL). A missing row is
// ErrClientNotFound; any divergence is ErrProxyConfigChanged.
func writeRollbackClientInTx(tx *gorm.DB, tenantID, userID int, in *CommitRollbackInput) error {
	var client models.Client
	err := tx.Where("client_id = ? AND tenant_id = ? AND user_id = ?",
		in.ClientNodeID, tenantID, userID).First(&client).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: rollback client %s", defs.ErrClientNotFound, in.ClientNodeID)
		}
		return err
	}
	if !exactNullableBytesEqual(client.ConfigContent, in.ExpectedClientConfig) {
		return fmt.Errorf("%w: client %s config drifted (expected %d bytes, found %d bytes)",
			defs.ErrProxyConfigChanged, in.ClientNodeID, len(in.ExpectedClientConfig), len(client.ConfigContent))
	}
	if exactNullableBytesEqual(in.ExpectedClientConfig, in.DesiredClientConfig) {
		return nil // value-identical: the guard already proved the state
	}
	q := tx.Model(&models.Client{}).
		Where("client_id = ? AND tenant_id = ? AND user_id = ?",
			in.ClientNodeID, tenantID, userID)
	if in.ExpectedClientConfig == nil {
		q = q.Where("config_content IS NULL")
	} else {
		q = q.Where("config_content = ?", in.ExpectedClientConfig)
	}
	res := q.Update("config_content", in.DesiredClientConfig)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("%w: client %s config drifted between guard and write",
			defs.ErrProxyConfigChanged, in.ClientNodeID)
	}
	return nil
}

// writeRollbackProxyInTx implements S3. The shape is selected by the
// CurrentProxy/DesiredProxy pair:
//
//	current set, desired nil  -> create rollback: guarded hard delete
//	current nil, desired set  -> delete rollback: guarded INSERT of the
//	                             exact before row (id must be free, no
//	                             same-name row in scope)
//	both set, same id          -> in-place UPDATE to the desired shape
//	                             (start/stop flip, update same-id restore)
//	both set, different ids    -> update rollback: guarded hard delete of
//	                             the current row + INSERT of the before row
//
// Every non-nil expectation row is shape-compared against the DB (name/
// server/client/origin/stopped/content) before the write; a 0-row guarded
// write is NEVER a success.
func writeRollbackProxyInTx(tx *gorm.DB, tenantID, userID int, in *CommitRollbackInput) error {
	cur, des := in.CurrentProxy, in.DesiredProxy
	switch {
	case cur != nil && des == nil:
		if err := guardRollbackProxyShapeInTx(tx, tenantID, userID, cur); err != nil {
			return err
		}
		res := tx.Unscoped().
			Where("id = ? AND tenant_id = ? AND user_id = ?", cur.ID, tenantID, userID).
			Delete(&models.ProxyConfig{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return rollbackProxyWriteMiss(tx, tenantID, userID, cur.ID)
		}
		return nil
	case cur == nil && des != nil:
		if err := guardRollbackProxyIDFreeInTx(tx, tenantID, userID, des); err != nil {
			return err
		}
		row := &models.ProxyConfig{
			Model:             &gorm.Model{ID: des.ID},
			ProxyConfigEntity: des.ProxyConfigEntity,
		}
		row.TenantID = tenantID
		row.UserID = userID
		return tx.Create(row).Error
	case cur != nil && des != nil && des.ID == cur.ID:
		if err := guardRollbackProxyShapeInTx(tx, tenantID, userID, cur); err != nil {
			return err
		}
		if rollbackProxyShapeEqual(cur, des) {
			return nil // value-identical in-place restore: guard already proved the state
		}
		res := tx.Model(&models.ProxyConfig{}).
			Where("id = ? AND tenant_id = ? AND user_id = ?", cur.ID, tenantID, userID).
			Updates(map[string]interface{}{
				"server_id":        des.ServerID,
				"client_id":        des.ClientID,
				"name":             des.Name,
				"type":             des.Type,
				"origin_client_id": des.OriginClientID,
				"content":          des.Content,
				"stopped":          des.Stopped,
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return rollbackProxyWriteMiss(tx, tenantID, userID, cur.ID)
		}
		return nil
	case cur != nil && des != nil:
		if err := guardRollbackProxyShapeInTx(tx, tenantID, userID, cur); err != nil {
			return err
		}
		res := tx.Unscoped().
			Where("id = ? AND tenant_id = ? AND user_id = ?", cur.ID, tenantID, userID).
			Delete(&models.ProxyConfig{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return rollbackProxyWriteMiss(tx, tenantID, userID, cur.ID)
		}
		if err := guardRollbackProxyIDFreeInTx(tx, tenantID, userID, des); err != nil {
			return err
		}
		row := &models.ProxyConfig{
			Model:             &gorm.Model{ID: des.ID},
			ProxyConfigEntity: des.ProxyConfigEntity,
		}
		row.TenantID = tenantID
		row.UserID = userID
		return tx.Create(row).Error
	default:
		return fmt.Errorf("%w: rollback proxy shape is inconsistent with the source operation", defs.ErrInvalidCommitInput)
	}
}

// guardRollbackProxyShapeInTx loads the current row and byte-compares every
// identity/state field against the expected row — a rollback must never
// delete or overwrite a rule that drifted from the source-after state.
func guardRollbackProxyShapeInTx(tx *gorm.DB, tenantID, userID int, expected *models.ProxyConfig) error {
	var row models.ProxyConfig
	err := tx.Where("id = ? AND tenant_id = ? AND user_id = ?",
		expected.ID, tenantID, userID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: rollback proxy row %d", defs.ErrProxyConfigNotFound, expected.ID)
		}
		return err
	}
	if row.Name != expected.Name || row.ServerID != expected.ServerID ||
		row.ClientID != expected.ClientID || row.OriginClientID != expected.OriginClientID ||
		row.Stopped != expected.Stopped || !bytes.Equal(row.Content, expected.Content) {
		return fmt.Errorf("%w: rollback proxy row %d drifted from the source after state", defs.ErrProxyConfigChanged, expected.ID)
	}
	return nil
}

// guardRollbackProxyIDFreeInTx fails closed when the desired (before) row id
// is already taken or another row in the same tenant+user+server scope
// carries the desired name — recreating the exact before row must never
// collide with a phantom duplicate.
func guardRollbackProxyIDFreeInTx(tx *gorm.DB, tenantID, userID int, des *models.ProxyConfig) error {
	var byID int64
	if err := tx.Model(&models.ProxyConfig{}).
		Where("id = ? AND tenant_id = ? AND user_id = ?", des.ID, tenantID, userID).
		Count(&byID).Error; err != nil {
		return err
	}
	if byID > 0 {
		return fmt.Errorf("%w: rollback proxy row %d already exists", defs.ErrProxyConfigChanged, des.ID)
	}
	var byName int64
	if err := tx.Model(&models.ProxyConfig{}).
		Where("tenant_id = ? AND user_id = ? AND server_id = ? AND name = ?",
			tenantID, userID, des.ServerID, des.Name).
		Count(&byName).Error; err != nil {
		return err
	}
	if byName > 0 {
		return fmt.Errorf("%w: a rule named %q already exists on server %s", defs.ErrProxyConfigChanged, des.Name, des.ServerID)
	}
	return nil
}

func rollbackProxyWriteMiss(tx *gorm.DB, tenantID, userID int, proxyID uint) error {
	var row models.ProxyConfig
	err := tx.Where("id = ? AND tenant_id = ? AND user_id = ?", proxyID, tenantID, userID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: rollback proxy config row %d", defs.ErrProxyConfigNotFound, proxyID)
		}
		return err
	}
	return fmt.Errorf("%w: rollback proxy row %d changed between guard and write", defs.ErrProxyConfigChanged, proxyID)
}

// rollbackProxyShapeEqual reports whether the desired row equals the current
// row field-by-field (the in-place value-identical skip).
func rollbackProxyShapeEqual(cur, des *models.ProxyConfig) bool {
	return cur.Name == des.Name && cur.ServerID == des.ServerID &&
		cur.ClientID == des.ClientID && cur.OriginClientID == des.OriginClientID &&
		cur.Type == des.Type && cur.Stopped == des.Stopped &&
		bytes.Equal(cur.Content, des.Content)
}

// migrateRollbackReservationInTx implements S4. Every mode re-validates the
// DB state instead of trusting the caller; committed leases are ONLY ever
// touched by exact physical key + owner_proxy_id — the rollback transaction
// id is never a release key and never stamped on a restored lease.
func migrateRollbackReservationInTx(tx *gorm.DB, tenantID int, in *CommitRollbackInput) error {
	step := in.Reservation
	switch step.Mode {
	case RollbackReservationRelease:
		if err := releaseRollbackCommittedInTx(tx, tenantID, step.Key, step.CurrentProxyID); err != nil {
			return fmt.Errorf("rollback release: %w", err)
		}
		return nil
	case RollbackReservationRestore:
		if err := restoreRollbackCommittedInTx(tx, tenantID, step.Key, step.DesiredProxyID, step.SourceTxID); err != nil {
			return fmt.Errorf("rollback restore: %w", err)
		}
		return nil
	case RollbackReservationSameKey:
		// the row id never changes: the owner CAS is a shape check only
		// (transaction_id NEVER rewritten — reuse the shared helper).
		if err := casReservationOwnerInTx(tx, tenantID, step.Key, step.CurrentProxyID, step.CurrentProxyID); err != nil {
			return fmt.Errorf("rollback same-key: %w", err)
		}
		return nil
	case RollbackReservationOwnerCAS:
		if err := casReservationOwnerInTx(tx, tenantID, step.Key, step.CurrentProxyID, step.DesiredProxyID); err != nil {
			return fmt.Errorf("rollback owner CAS: %w", err)
		}
		return nil
	case RollbackReservationKeySwitch:
		if err := releaseRollbackCommittedInTx(tx, tenantID, step.ReleaseKey, step.CurrentProxyID); err != nil {
			return fmt.Errorf("rollback key switch release: %w", err)
		}
		if err := restoreRollbackCommittedInTx(tx, tenantID, step.RestoreKey, step.DesiredProxyID, step.SourceTxID); err != nil {
			return fmt.Errorf("rollback key switch restore: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("%w: rollback reservation mode %q is not supported", defs.ErrInvalidCommitInput, step.Mode)
	}
}

// releaseRollbackCommittedInTx hard-deletes the committed lease at the exact
// key owned by the given rule row (create rollback / key-switch release).
// 0 rows is NOT tolerated — the lease must exist and be the current rule's.
func releaseRollbackCommittedInTx(tx *gorm.DB, tenantID int, key ReservationKey, ownerID uint) error {
	res := tx.Unscoped().
		Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ? AND status = ? AND owner_proxy_id = ?",
			tenantID, key.ServerID, key.Protocol, key.RemotePort, models.ReservationStatusCommitted, ownerID).
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
			return fmt.Errorf("%w: committed reservation %s/%s/%d for release", defs.ErrReservationNotFound, key.ServerID, key.Protocol, key.RemotePort)
		}
		return err
	}
	return fmt.Errorf("%w: reservation %s/%s/%d is not committed and owned by proxy %d (status=%s owner=%v)",
		defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort, ownerID, row.Status, row.OwnerProxyID)
}

// restoreRollbackCommittedInTx recreates (or retains) the committed lease
// at the key owned by the restored rule row, stamped with the SOURCE
// transaction id — a restored lease can never be released by the rollback
// transaction id. An existing committed lease already owned by the restored
// row is a retain (shape check); ANY other row at the key fails closed
// (silent double-lock guard).
func restoreRollbackCommittedInTx(tx *gorm.DB, tenantID int, key ReservationKey, ownerID uint, sourceTxID string) error {
	var row models.PortReservation
	err := tx.Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ?",
		tenantID, key.ServerID, key.Protocol, key.RemotePort).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		owner := ownerID
		return tx.Create(&models.PortReservation{
			TenantID:      tenantID,
			ServerID:      key.ServerID,
			Protocol:      key.Protocol,
			RemotePort:    key.RemotePort,
			OwnerProxyID:  &owner,
			TransactionID: sourceTxID,
			Status:        models.ReservationStatusCommitted,
		}).Error
	}
	if err != nil {
		return err
	}
	if row.Status == models.ReservationStatusCommitted && row.OwnerProxyID != nil && *row.OwnerProxyID == ownerID {
		return nil
	}
	return fmt.Errorf("%w: key %s/%s/%d is held by another lease (status=%s owner=%v)",
		defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort, row.Status, row.OwnerProxyID)
}

// saveRollbackAuditInTx persists the commit audit event with TransactionID,
// TenantID, ActorUserID and EventType=rollback FORCED (spec §6.4); the
// summary redaction guard ran in S0.
func saveRollbackAuditInTx(tx *gorm.DB, tenantID, userID int, in *CommitRollbackInput) error {
	audit := in.AuditEvent
	audit.TransactionID = in.TxID
	audit.TenantID = tenantID
	audit.ActorUserID = userID
	audit.EventType = models.AuditEventTypeRollback
	return tx.Create(audit).Error
}

// writeRollbackEvidenceInTx persists the optional evidence onto the
// rollback transaction INSIDE the commit (S6.5): the guarded UPDATE
// re-verifies tenant+actor+operation=rollback+verifying; a miss is
// classified — an already-verifying row carrying the exact same evidence is
// an idempotent success, anything else fails closed.
func writeRollbackEvidenceInTx(tx *gorm.DB, tenantID, userID int, in *CommitRollbackInput) error {
	res := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ? AND operation = ? AND status = ?",
			in.TxID, tenantID, userID, models.TxOperationRollback, models.TxStatusVerifying).
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
	return fmt.Errorf("%w: rollback transaction %s evidence write missed", defs.ErrTxStatusConflict, in.TxID)
}
