package dao

import (
	"errors"
	"fmt"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"gorm.io/gorm"
)

type TransactionQuery interface {
	FindByID(userInfo models.UserInfo, txID string) (*models.ChangeTransaction, error)
	FindByIdempotencyKey(userInfo models.UserInfo, key string) (*models.ChangeTransaction, error)
	ListByStatus(userInfo models.UserInfo, status string, limit int) ([]*models.ChangeTransaction, error)
	ListRecent(userInfo models.UserInfo, limit int) ([]*models.ChangeTransaction, error)
}

type TransactionMutation interface {
	Create(userInfo models.UserInfo, tx *models.ChangeTransaction) error
	// UpdateStatus advances the transaction to status and records error
	// details. It loads the current row inside one DB transaction, enforces
	// the state machine (CanTransitionTo) and CAS-updates with
	// WHERE transaction_id=? AND tenant_id=? AND status=<loaded>. Illegal
	// transitions return defs.ErrInvalidTxTransition; a CAS miss (the row
	// changed between load and update) returns defs.ErrTxStatusConflict.
	// Terminal statuses stamp FinishedAt.
	UpdateStatus(userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error
	Update(userInfo models.UserInfo, tx *models.ChangeTransaction) error
	// CommitCreate atomically persists the final state of a safe create:
	// client config row, rebuilt proxy config rows, committed reservation,
	// encrypted after snapshot, commit audit event and the committed status —
	// all inside ONE DB transaction (spec §8.1 steps 10-11). Returns the id
	// of the newly created proxy row (the reservation owner).
	CommitCreate(userInfo models.UserInfo, in *CommitCreateInput) (uint, error)
	// CommitUpdate atomically persists the final state of a safe update
	// (spec §8.1): old-rule guards, the branch write (active rebuild /
	// stopped in-place), reservation migration, encrypted AFTER snapshots
	// (client + proxy), the update audit event and the committed status —
	// all inside ONE DB transaction. Returns the final proxy row id (the
	// reservation owner). Before snapshots are saved by the coordinator
	// BEFORE apply and are never written here.
	CommitUpdate(userInfo models.UserInfo, in *CommitUpdateInput) (uint, error)
	// CommitLifecycle atomically persists the final state of a
	// delete/start/stop operation (spec §8.3): owner-tx guard, old-rule
	// guard (name/server/stopped/shape + real physical key), the proxy
	// stopped flip or guarded hard delete, the client config_content CAS
	// update, the same-key reservation migration (SameKeyOwned /
	// SameKeyPending only — the physical key never changes), the encrypted
	// AFTER snapshots (client always; proxy for start/stop only), the
	// forced lifecycle audit event, optional evidence and the committed
	// status — all inside ONE DB transaction. Returns the OLD proxy row id
	// (the reservation owner before the commit). Before snapshots are
	// saved by the coordinator BEFORE apply and are never written here.
	CommitLifecycle(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error)
	// CommitDelete/CommitStart/CommitStop are thin op-binding wrappers
	// over CommitLifecycle for coordinator ergonomics.
	CommitDelete(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error)
	CommitStart(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error)
	CommitStop(userInfo models.UserInfo, in *CommitLifecycleInput) (uint, error)
	// CommitRollback atomically persists the final state of a MANUAL
	// rollback (D2c-C): guard the rollback tx (tenant+actor+operation=
	// rollback+verifying) and the SOURCE tx (tenant+actor+operation+
	// committed, read-only — never transitioned), CAS the current client
	// config back to the source-before plaintext, write the proxy rows per
	// the source operation (delete current / recreate the exact before row /
	// in-place restore / stopped flip), migrate the reservation by exact
	// physical key + owner_proxy_id (the rollback tx id is never a release
	// key and never stamped on a restored lease), save the encrypted AFTER
	// snapshots (client always; proxy iff a desired row exists), force the
	// rollback audit event + evidence and commit the rollback tx LAST.
	// Returns the CURRENT proxy row id (the reservation owner before the
	// commit; 0 when the current row is absent).
	CommitRollback(userInfo models.UserInfo, in *CommitRollbackInput) (uint, error)
	// FailTransaction atomically moves a transaction to a terminal failure
	// state (failed / rolled_back / rollback_failed) and logs the audit event
	// in one DB transaction. When ReleaseReservation is set the pending
	// reservation owned by the transaction is released in the same
	// transaction. rollback_failed callers must NOT release (the rule stays
	// blocked for manual intervention, spec §8.2).
	FailTransaction(userInfo models.UserInfo, in *FailTransactionInput) error
}

// CommitCreateInput carries everything the atomic commit of a safe create
// needs. All values are tenant-scoped by the caller; the DAO re-checks the
// tenant on every row it touches.
type CommitCreateInput struct {
	TxID       string
	Client     *models.ClientEntity // child client carrying the applied (final) config
	ProxyName  string               // name of the newly created proxy (row id captured)
	Shadow     *ShadowCommit        // in-memory shadow conversion to persist atomically (nil = no origin change)
	Snapshot   SnapshotInput
	AuditEvent *models.AuditEvent // commit audit event (TenantID/ActorUserID forced by DAO)
	// NewKey is the physical key (server/protocol/remotePort) the created rule
	// runs on. The commit needs it to reconcile a committed lease inherited
	// from an overwritten rule: exactly one committed lease may survive per
	// proxy row, the one for the port that row actually serves.
	NewKey ReservationKey
	// Overwrite mirrors the request's explicit overwrite intent. A create may
	// only reuse an existing ACTIVE row (same name → same row id, the silent
	// upsert) when this is true; otherwise the commit fails closed with
	// defs.ErrProxyConfigNameConflict.
	Overwrite bool
	// AdoptLease is the same-key overwrite contract: the rule being replaced
	// already holds a committed lease at exactly NewKey, so this transaction
	// acquired NO pending lease of its own and the commit adopts the existing
	// committed one instead of flipping a pending row.
	AdoptLease bool
}

// ShadowCommit describes the shadow conversion a safe create persists
// atomically with its commit (spec §13.2 零配置变更: zero config side
// effects before the final transaction). It is the DAO-side view of the
// coordinator's in-memory shadow plan (biz/master/proxy.ShadowPlan — the
// DAO package cannot import biz, so the coordinator maps its plan into this
// struct). A nil ShadowCommit means the child already existed and no origin
// row may change.
type ShadowCommit struct {
	// FirstShadow: convert the origin row (IsShadow=true, cleared
	// config/server) and hard-delete the origin's proxy_config rows inside
	// the commit transaction; they migrate to the child rebuild below.
	FirstShadow bool
	// OriginAfter is the origin row's post-conversion state; ClientID
	// identifies the row to convert. Non-nil only when FirstShadow.
	OriginAfter *models.ClientEntity
	// ChildCreated: INSERT the child row (brand-new) instead of updating
	// the existing child in place.
	ChildCreated bool
}

// FailTransactionInput carries a terminal failure transition plus optional
// reservation release and audit event.
type FailTransactionInput struct {
	TxID               string
	Status             string // failed / rolled_back / rollback_failed
	ErrorCode          string
	ErrorMessage       string
	EvidenceJSON       string
	ReleaseReservation bool
	AuditEvent         *models.AuditEvent
}

// ReservationKey identifies a physical port reservation.
type ReservationKey struct {
	ServerID   string
	Protocol   string
	RemotePort uint32
}

// IsZero reports whether the key carries no physical identity at all (legacy
// callers that predate the key-aware commit paths).
func (k ReservationKey) IsZero() bool {
	return k.ServerID == "" && k.Protocol == "" && k.RemotePort == 0
}

// SnapshotInput describes one encrypted config snapshot row.
type SnapshotInput struct {
	Side      string // before / after
	NodeType  string // client / server
	NodeID    string
	Plaintext []byte
	Key       []byte // AES-256 key; never logged, never persisted
}

type transactionQuery struct{ *queryImpl }
type transactionMutation struct{ *mutationImpl }

func newTransactionQuery(base *queryImpl) TransactionQuery { return &transactionQuery{base} }
func newTransactionMutation(base *mutationImpl) TransactionMutation {
	return &transactionMutation{base}
}

func requireUserInfo(userInfo models.UserInfo) error {
	if userInfo == nil {
		return defs.ErrNilUserInfo
	}
	return nil
}

func (q *transactionQuery) FindByID(userInfo models.UserInfo, txID string) (*models.ChangeTransaction, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	tx := &models.ChangeTransaction{}
	err := db.Where("transaction_id = ? AND tenant_id = ?", txID, userInfo.GetTenantID()).First(tx).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, defs.ErrTransactionNotFound
		}
		return nil, err
	}
	return tx, nil
}

func (q *transactionQuery) FindByIdempotencyKey(userInfo models.UserInfo, key string) (*models.ChangeTransaction, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	tx := &models.ChangeTransaction{}
	err := db.Where("idempotency_key = ? AND tenant_id = ? AND actor_user_id = ?",
		key, userInfo.GetTenantID(), userInfo.GetUserID()).First(tx).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, defs.ErrTransactionNotFound
		}
		return nil, err
	}
	return tx, nil
}

func (q *transactionQuery) ListByStatus(userInfo models.UserInfo, status string, limit int) ([]*models.ChangeTransaction, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	list := []*models.ChangeTransaction{}
	err := db.Where("status = ? AND tenant_id = ?", status, userInfo.GetTenantID()).
		Order("id DESC").Limit(limit).Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (q *transactionQuery) ListRecent(userInfo models.UserInfo, limit int) ([]*models.ChangeTransaction, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	list := []*models.ChangeTransaction{}
	err := db.Where("tenant_id = ?", userInfo.GetTenantID()).
		Order("id DESC").Limit(limit).Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *transactionMutation) Create(userInfo models.UserInfo, tx *models.ChangeTransaction) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	// Tenant and actor are forced from the authenticated user: callers can
	// never create a transaction owned by another tenant/actor.
	tx.TenantID = userInfo.GetTenantID()
	tx.ActorUserID = userInfo.GetUserID()
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	return db.Create(tx).Error
}

func (m *transactionMutation) UpdateStatus(userInfo models.UserInfo, txID, status, errorCode, errorMessage string) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	return db.Transaction(func(tx *gorm.DB) error {
		return transitionTxStatus(tx, userInfo.GetTenantID(), txID, status, errorCode, errorMessage)
	})
}

// transitionTxStatus is the shared CAS state-machine step used by
// UpdateStatus and by the atomic commit (inside the bigger transaction).
//
// It is WRITE-FIRST: the state machine is encoded in the UPDATE's WHERE
// clause (status IN allowed-from-set for the target), so the statement either
// matches the current row or misses. A miss is classified by a follow-up read:
//   - row missing              -> defs.ErrTransactionNotFound
//   - row already at target    -> defs.ErrTxStatusConflict (concurrent winner)
//   - row at an allowed source -> defs.ErrTxStatusConflict (changed mid-flight)
//   - row anywhere else        -> defs.ErrInvalidTxTransition (stable sentinel)
//
// Write-first avoids the read-then-write lock upgrade that deadlocks SQLite
// when two transactions validate the same row concurrently; on every dialect
// the loser deterministically observes the winner's committed state.
func transitionTxStatus(tx *gorm.DB, tenantID int, txID, status, errorCode, errorMessage string) error {
	fromSet := models.AllowedSourcesFor(status)
	if len(fromSet) == 0 {
		return fmt.Errorf("%w: no state may transition to %s", defs.ErrInvalidTxTransition, status)
	}
	fromList := make([]string, 0, len(fromSet))
	for src := range fromSet {
		fromList = append(fromList, src)
	}
	updates := map[string]interface{}{
		"status":        status,
		"error_code":    errorCode,
		"error_message": errorMessage,
	}
	if models.IsTerminalTxStatus(status) {
		updates["finished_at"] = time.Now()
	}
	res := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND tenant_id = ? AND status IN ?", txID, tenantID, fromList).
		Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 1 {
		return nil
	}

	// miss: classify
	var row models.ChangeTransaction
	err := tx.Where("transaction_id = ? AND tenant_id = ?", txID, tenantID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return defs.ErrTransactionNotFound
		}
		return err
	}
	if row.Status == status || fromSet[row.Status] {
		return defs.ErrTxStatusConflict
	}
	return fmt.Errorf("%w: %s -> %s", defs.ErrInvalidTxTransition, row.Status, status)
}

// setTransactionProxyID writes the owning proxy row id onto a change
// transaction that is still in a committable state. It is the same
// WRITE-FIRST CAS discipline as transitionTxStatus, encoded as a guarded
// UPDATE — the row must still be in a state from which committed is legal
// (verifying); a miss fails the whole enclosing transaction, so a commit
// can never finish with a transaction pointing at a rule row that did not
// survive. Shared by CommitCreate and (later) CommitUpdate.
func setTransactionProxyID(tx *gorm.DB, tenantID int, txID string, proxyID uint) error {
	fromSet := models.AllowedSourcesFor(models.TxStatusCommitted)
	fromList := make([]string, 0, len(fromSet))
	for src := range fromSet {
		fromList = append(fromList, src)
	}
	res := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND tenant_id = ? AND status IN ?", txID, tenantID, fromList).
		Update("proxy_id", proxyID)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 1 {
		return nil
	}
	// miss: classify (mirrors transitionTxStatus)
	var row models.ChangeTransaction
	err := tx.Where("transaction_id = ? AND tenant_id = ?", txID, tenantID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: transaction %s", defs.ErrTransactionNotFound, txID)
		}
		return err
	}
	// MySQL commonly reports RowsAffected=0 for a value-identical UPDATE.
	// Update transactions are born with ProxyID=oldProxyID, so same-name and
	// stopped updates legitimately backfill the same value. If the row is
	// still committable and already carries the target id, the guarded shape
	// has been proven and this is an idempotent success. A different value in
	// a committable row is a real CAS conflict, not an invalid transition.
	if fromSet[row.Status] {
		if row.ProxyID != nil && *row.ProxyID == proxyID {
			return nil
		}
		return fmt.Errorf("%w: transaction %s proxy_id changed concurrently",
			defs.ErrTxStatusConflict, txID)
	}
	return fmt.Errorf("%w: %s is not a committable state for proxy_id backfill", defs.ErrInvalidTxTransition, row.Status)
}

func (m *transactionMutation) Update(userInfo models.UserInfo, tx *models.ChangeTransaction) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	res := db.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND tenant_id = ?", tx.TransactionID, userInfo.GetTenantID()).
		Select("Status", "ProxyID", "EvidenceJSON", "ErrorCode", "ErrorMessage", "FinishedAt").
		Updates(tx)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return defs.ErrTransactionNotFound
	}
	return nil
}

// CommitCreate implements the single-transaction commit of a safe create
// (spec §8.1 steps 10-11). Order inside the transaction:
//  0. if this is a FIRST shadow: convert the origin row (shadow flag +
//     cleared config/server, tenant/user scoped) and hard-delete the
//     origin's proxy_config rows (they migrate to the child rebuild);
//  1. persist the child client row (INSERT when brand-new, UPDATE in place
//     otherwise) with the applied config;
//  2. rebuild proxy_config rows from that config (delete + save), capturing
//     the id of the proxy named in.ProxyName;
//     2b. backfill change_transaction.proxy_id with that id (spec §6.2: the
//     field is nullable only until the rule row exists) — same CAS
//     discipline as the final transition, inside this transaction;
//  3. commit the pending reservation (CAS on status=pending);
//  4. save the encrypted after snapshot;
//  5. insert the commit audit event;
//  6. move the transaction to committed (CAS state machine).
//
// Any failure rolls back the whole commit — including the shadow
// conversion and the proxy_id backfill, so a failed commit never leaves a
// half-converted origin or a transaction pointing at a rule that never
// committed.
func (m *transactionMutation) CommitCreate(userInfo models.UserInfo, in *CommitCreateInput) (uint, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return 0, err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	tenantID := userInfo.GetTenantID()
	userID := userInfo.GetUserID()

	var proxyID uint
	err := db.Transaction(func(tx *gorm.DB) error {
		// rows this commit is about to hard-delete. Their committed leases are
		// reconciled against the rebuilt rows after the rebuild: a lease whose
		// rule survived keeps the port (owner transferred to the new row id),
		// a lease whose rule did not survive is released. Without this the port
		// stays reserved forever by a row that no longer exists.
		var replacedRows []models.ProxyConfig
		// 0. first shadow: convert the origin row and migrate its proxy
		// rows (tenant/user scoped; a missing origin row fails the commit)
		if in.Shadow != nil && in.Shadow.FirstShadow {
			if in.Shadow.OriginAfter == nil {
				return fmt.Errorf("commit create: shadow conversion without origin-after state")
			}
			if err := guardNoOtherServerStoppedRowsInTx(tx, tenantID, userID,
				in.Shadow.OriginAfter.ClientID, in.Client.ServerID); err != nil {
				return fmt.Errorf("commit create: %w", err)
			}
			if err := convertOriginToShadowInTx(tx, tenantID, userID, in.Shadow.OriginAfter.ClientID); err != nil {
				return fmt.Errorf("commit create: %w", err)
			}
			// capture the ACTIVE origin rows BEFORE they are deleted: the
			// rebuild re-creates them on the child under new ids
			originRows, err := captureActiveProxyRowsInTx(tx, tenantID, userID, in.Shadow.OriginAfter.ClientID)
			if err != nil {
				return fmt.Errorf("commit create: capture origin proxy rows: %w", err)
			}
			replacedRows = append(replacedRows, originRows...)
			// Active rows are rebuilt from the child's applied config. Stopped
			// rows are absent from that config, but they remain user-visible and
			// keep committed reservations. Move the SAME-SERVER stopped rows to
			// the generated child with the same row id before deleting only the
			// active origin rows; merely leaving them on the now-shadow origin
			// would make StartProxy unable to find them by child_id. Stopped
			// rows of OTHER servers are never migrated (they must not surface
			// under the wrong server).
			if err := migrateStoppedProxyRowsInTx(tx, tenantID, userID,
				in.Shadow.OriginAfter.ClientID, in.Client.ClientID, in.Client.ServerID); err != nil {
				return fmt.Errorf("commit create: migrate stopped origin proxy rows: %w", err)
			}
			if err := deleteActiveProxyRowsInTx(tx, tenantID, userID,
				in.Shadow.OriginAfter.ClientID); err != nil {
				return fmt.Errorf("commit create: delete active origin proxy rows: %w", err)
			}
		}

		// 1. child client row (same semantics as clientMutation.UpdateClient
		// / CreateClient, tenant-scoped and inside this transaction)
		if err := persistChildClientInTx(tx, tenantID, userID, in.Client,
			in.Shadow != nil && in.Shadow.ChildCreated); err != nil {
			return fmt.Errorf("commit create: %w", err)
		}

		// 2. rebuild proxy config rows from the applied config
		//
		// The rebuild deletes the child's ACTIVE rows first, so they must be
		// captured before it runs: their committed leases are reconciled
		// against the rebuilt rows right after the rebuild resolved the final
		// row id.
		childRows, err := captureActiveProxyRowsInTx(tx, tenantID, userID, in.Client.ClientID)
		if err != nil {
			return fmt.Errorf("commit create: capture child proxy rows: %w", err)
		}
		replacedRows = append(replacedRows, childRows...)
		id, err := rebuildProxyConfigsInTx(tx, tenantID, userID, in.Client, in.ProxyName)
		if err != nil {
			return fmt.Errorf("commit create: rebuild proxy configs: %w", err)
		}
		proxyID = id

		// 2c. a create never silently rebinds an existing ACTIVE rule: the
		// rebuild preserves the row id of an existing same-name row on the
		// same client, which turns the "create" into an in-place overwrite of
		// that tunnel — its public port moves with it and the old port keeps a
		// committed lease nobody can see. Only an explicit overwrite may do
		// that; anything else fails closed (the coordinator rejects it before
		// the apply, this is the TOCTOU re-check).
		if !in.Overwrite && reusedActiveRow(childRows, id) {
			return fmt.Errorf("%w: create would overwrite the active rule %q (row %d)",
				defs.ErrProxyConfigNameConflict, in.ProxyName, id)
		}

		// 2d. reconcile the leases of every row the rebuild replaced: a rule
		// that survived on the same port keeps its lease (owner transferred to
		// the new row id), a rule that did not survive releases it.
		if _, err := reconcileRebuiltReservationsInTx(tx, tenantID, replacedRows, in.Client.ClientID, id); err != nil {
			return fmt.Errorf("commit create: reconcile reservations: %w", err)
		}

		// 2e. one row, one lease: release any committed lease still owned by
		// the final row but keyed on a different port (the same-name overwrite
		// leak: remotePort changed, the old lease was never reclaimed).
		if !in.NewKey.IsZero() {
			if _, err := releaseStaleOwnerReservationsInTx(tx, tenantID, id, in.NewKey); err != nil {
				return fmt.Errorf("commit create: release stale reservations: %w", err)
			}
		}

		// 2b. backfill change_transaction.proxy_id (spec §6.2: nullable
		// only until the rule row exists). Runs BEFORE the reservation
		// commit and the final transition so the whole commit is one
		// atomic unit — a later failure rolls the backfill back with it.
		if err := setTransactionProxyID(tx, tenantID, in.TxID, id); err != nil {
			return fmt.Errorf("commit create: backfill transaction proxy id: %w", err)
		}

		// 3. commit the reservation (CAS: only the still-pending lease owned
		// by this transaction may be committed). In same-key overwrite mode the
		// transaction never acquired a lease: the committed lease the replaced
		// rule already holds is adopted instead (and must belong to the final
		// row id by now).
		if in.AdoptLease {
			if err := adoptCommittedLeaseInTx(tx, tenantID, in.NewKey, id); err != nil {
				return fmt.Errorf("commit create: adopt reservation: %w", err)
			}
		} else {
			res := tx.Model(&models.PortReservation{}).
				Where("transaction_id = ? AND tenant_id = ? AND status = ?",
					in.TxID, tenantID, models.ReservationStatusPending).
				Updates(map[string]interface{}{
					"status":         models.ReservationStatusCommitted,
					"owner_proxy_id": id,
					"expires_at":     nil,
				})
			if res.Error != nil {
				return fmt.Errorf("commit create: commit reservation: %w", res.Error)
			}
			if res.RowsAffected == 0 {
				return fmt.Errorf("%w: reservation for tx %s", defs.ErrReservationNotFound, in.TxID)
			}
		}

		// 4. encrypted after snapshot
		snap := &models.ConfigSnapshot{
			TransactionID: in.TxID,
			TenantID:      tenantID,
			Side:          in.Snapshot.Side,
			NodeType:      in.Snapshot.NodeType,
			NodeID:        in.Snapshot.NodeID,
		}
		ciphertext, nonce, sum, err := snapshot.EncryptSnapshot(in.Snapshot.Plaintext, in.Snapshot.Key)
		if err != nil {
			return fmt.Errorf("commit create: encrypt after snapshot: %w", err)
		}
		snap.Ciphertext = ciphertext
		snap.Nonce = nonce
		snap.SHA256 = sum
		if err := tx.Create(snap).Error; err != nil {
			return fmt.Errorf("commit create: save after snapshot: %w", err)
		}

		// 5. commit audit event
		if in.AuditEvent != nil {
			in.AuditEvent.TenantID = tenantID
			in.AuditEvent.ActorUserID = userID
			if err := tx.Create(in.AuditEvent).Error; err != nil {
				return fmt.Errorf("commit create: audit: %w", err)
			}
		}

		// 6. tx -> committed (last step, spec §8.1 step 11)
		return transitionTxStatus(tx, tenantID, in.TxID, models.TxStatusCommitted, "", "")
	})
	if err != nil {
		return 0, err
	}
	return proxyID, nil
}

// rebuildProxyConfigsInTx mirrors proxyMutation.RebuildProxyConfigFromClient
// semantics (skip-stopped, preserve the row id of an existing proxy with the
// same name) but runs inside the given transaction with explicit tenant
// scoping. Returns the id of the row whose name equals wantName.
func rebuildProxyConfigsInTx(tx *gorm.DB, tenantID, userID int, client *models.ClientEntity, wantName string) (uint, error) {
	pxyCfgs, err := utils.LoadProxiesFromContent(client.ConfigContent)
	if err != nil {
		return 0, err
	}

	entities := make([]*models.ProxyConfig, 0, len(pxyCfgs))
	for _, pxyCfg := range pxyCfgs {
		proxyCfg := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{}}
		// preserve the id of an existing row for the same rule (overwrite)
		var old models.ProxyConfig
		err := tx.Where("client_id = ? AND name = ? AND tenant_id = ?",
			client.ClientID, pxyCfg.GetBaseConfig().Name, tenantID).First(&old).Error
		if err == nil {
			proxyCfg.Model = old.Model
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, err
		}
		if err := proxyCfg.FillClientConfig(client); err != nil {
			return 0, err
		}
		if err := proxyCfg.FillTypedProxyConfig(pxyCfg); err != nil {
			return 0, err
		}
		entities = append(entities, proxyCfg)
	}

	// remove the previous ACTIVE rows for this client (hard delete:
	// soft-deleted rows would linger). The explicit
	// (stopped IS NULL OR stopped = false) filter mirrors the legacy
	// DeleteProxyConfigsByClientIDOrOriginClientID semantics: stopped
	// siblings (rules the user stopped, which are NOT in the applied
	// config) must survive the rebuild — their rows and committed
	// reservations stay intact. NOTE the old-row id reuse above
	// deliberately does NOT exclude stopped rows: when the applied config
	// contains a proxy whose name matches a stopped row, the stopped row's
	// id is reused in place (single row, no duplicate), which is exactly
	// the legacy RebuildProxyConfigFromClient behavior.
	if err := deleteActiveProxyRowsInTx(tx, tenantID, userID, client.ClientID); err != nil {
		return 0, err
	}

	if len(entities) == 0 {
		return 0, fmt.Errorf("applied client config contains no proxies")
	}
	if err := tx.Save(entities).Error; err != nil {
		return 0, err
	}

	// capture the id of the row for the requested proxy (fresh rows only get
	// their id assigned by Save, so resolve it after the write)
	var want models.ProxyConfig
	if err := tx.Where("client_id = ? AND name = ? AND tenant_id = ?",
		client.ClientID, wantName, tenantID).First(&want).Error; err != nil {
		return 0, fmt.Errorf("applied client config has no proxy named %q", wantName)
	}
	return want.ID, nil
}

// convertOriginToShadowInTx converts a never-shadowed origin client row in
// place (shadow flag + cleared config/server binding) inside the commit
// transaction. The guarded UPDATE (tenant+user+client_id) fails closed with
// defs.ErrClientNotFound when the origin row vanished between plan and
// commit. Shared by CommitCreate and CommitUpdate first-shadow commits.
func convertOriginToShadowInTx(tx *gorm.DB, tenantID, userID int, originID string) error {
	res := tx.Model(&models.Client{}).
		Where("client_id = ? AND tenant_id = ? AND user_id = ?", originID, tenantID, userID).
		Updates(map[string]interface{}{
			"is_shadow":      true,
			"config_content": nil,
			"server_id":      "",
		})
	if res.Error != nil {
		return fmt.Errorf("shadow origin client: %w", res.Error)
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("%w: shadow origin client %s", defs.ErrClientNotFound, originID)
	}
	return nil
}

// persistChildClientInTx persists the applied child client row inside the
// commit transaction: INSERT for a brand-new child, otherwise an explicit
// tenant+user+client_id UPDATE of every mutable content field. GORM Save is
// FORBIDDEN for the update path — with a Where clause it returns nil even
// when 0 rows match, so a concurrently hard-deleted child would make the
// commit silently "succeed" against nothing (orphan proxy rows + a
// permanently blocked port, round-8 I1). The RowsAffected==1 guard fails
// the whole commit with defs.ErrClientNotFound instead. Shared by
// CommitCreate and CommitUpdate.
func persistChildClientInTx(tx *gorm.DB, tenantID, userID int, c *models.ClientEntity, childCreated bool) error {
	if childCreated {
		child := &models.Client{ClientEntity: c}
		child.UserID = userID
		child.TenantID = tenantID
		if err := tx.Create(child).Error; err != nil {
			return fmt.Errorf("create child client: %w", err)
		}
		return nil
	}
	res := tx.Model(&models.Client{}).
		Where("client_id = ? AND tenant_id = ? AND user_id = ?", c.ClientID, tenantID, userID).
		Updates(map[string]interface{}{
			"server_id":        c.ServerID,
			"config_content":   c.ConfigContent,
			"connect_secret":   c.ConnectSecret,
			"stopped":          c.Stopped,
			"comment":          c.Comment,
			"is_shadow":        c.IsShadow,
			"origin_client_id": c.OriginClientID,
			"frps_url":         c.FrpsUrl,
			"ephemeral":        c.Ephemeral,
		})
	if res.Error != nil {
		return fmt.Errorf("persist client: %w", res.Error)
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("persist client: %w: %s", defs.ErrClientNotFound, c.ClientID)
	}
	return nil
}

// migrateStoppedProxyRowsInTx preserves the SAME-SERVER stopped rules across
// a first-shadow conversion. They are not present in Client.ConfigContent,
// so the active rebuild cannot recreate them. Updating client_id/
// origin_client_id in place keeps the proxy row id — and therefore every
// committed reservation's owner_proxy_id — stable while making the rule
// discoverable through the per-server child used by StartProxy. The server
// scope is mandatory: stopped rows of OTHER servers must never be migrated
// onto the current child (they would surface under the wrong server and
// block a future start there).
func migrateStoppedProxyRowsInTx(tx *gorm.DB, tenantID, userID int, originID, childID, serverID string) error {
	return tx.Model(&models.ProxyConfig{}).
		Where("client_id = ? AND tenant_id = ? AND user_id = ? AND stopped = ? AND server_id = ?",
			originID, tenantID, userID, true, serverID).
		Updates(map[string]interface{}{
			"client_id":        childID,
			"origin_client_id": originID,
		}).Error
}

// guardNoOtherServerStoppedRowsInTx rejects a historical shape that cannot
// be migrated safely during first-shadow. Moving another server's stopped
// row to the current child would attach it to the wrong frps; leaving it on
// the origin would strand it because the origin becomes a shadow and
// StartProxy subsequently queries the per-server child id. Fail before the
// first client/proxy write so the whole database stays byte-identical.
func guardNoOtherServerStoppedRowsInTx(tx *gorm.DB, tenantID, userID int, originID, serverID string) error {
	var count int64
	if err := tx.Model(&models.ProxyConfig{}).
		Where("client_id = ? AND tenant_id = ? AND user_id = ? AND stopped = ?",
			originID, tenantID, userID, true).
		Where("server_id IS NULL OR server_id <> ?", serverID).
		Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("%w: origin %s has stopped rules for another server",
			defs.ErrStoppedFirstShadow, originID)
	}
	return nil
}

// deleteActiveProxyRowsInTx hard-deletes only rows represented by the
// applied client config. Stopped rows are deliberately excluded because
// they live only in proxy_config and retain their port reservations.
func deleteActiveProxyRowsInTx(tx *gorm.DB, tenantID, userID int, clientID string) error {
	return tx.Unscoped().
		Where("client_id = ? AND tenant_id = ? AND user_id = ?", clientID, tenantID, userID).
		Where("stopped IS NULL OR stopped = ?", false).
		Delete(&models.ProxyConfig{}).Error
}

// FailTransaction atomically fails/rolls-back a transaction and audits it.
func (m *transactionMutation) FailTransaction(userInfo models.UserInfo, in *FailTransactionInput) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	tenantID := userInfo.GetTenantID()
	userID := userInfo.GetUserID()
	return db.Transaction(func(tx *gorm.DB) error {
		if err := transitionTxStatus(tx, tenantID, in.TxID, in.Status, in.ErrorCode, in.ErrorMessage); err != nil {
			return err
		}
		if in.EvidenceJSON != "" {
			res := tx.Model(&models.ChangeTransaction{}).
				Where("transaction_id = ? AND tenant_id = ?", in.TxID, tenantID).
				Update("evidence_json", in.EvidenceJSON)
			if res.Error != nil {
				return res.Error
			}
		}
		if in.ReleaseReservation {
			res := tx.Unscoped().
				Where("transaction_id = ? AND tenant_id = ?", in.TxID, tenantID).
				Delete(&models.PortReservation{})
			if res.Error != nil {
				return res.Error
			}
		}
		if in.AuditEvent != nil {
			in.AuditEvent.TenantID = tenantID
			in.AuditEvent.ActorUserID = userID
			if err := tx.Create(in.AuditEvent).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
