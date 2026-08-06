package dao

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PortReservationQuery interface {
	GetByServerFilter(userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error)
	GetByTransactionID(userInfo models.UserInfo, txID string) ([]*models.PortReservation, error)
	ListExpiredPending(userInfo models.UserInfo, now time.Time) ([]*models.PortReservation, error)
}

type PortReservationMutation interface {
	// TryAcquire inserts a new pending lease for the key, evicting a
	// previously-expired pending lease ONLY when its owning transaction is
	// provably safe to forget: same tenant AND owner in pending/preflight
	// (stale pre-apply: marked failed with RESERVATION_EXPIRED first) or
	// failed/rolled_back (terminal). Every eviction is audited (spec
	// §6.1.5). Owners that may have applied the config (applying/
	// verifying/rolling_back), blocked (rollback_failed), committed,
	// orphaned (no transaction row) or belonging to ANOTHER tenant are
	// NEVER evicted — the acquire fails with defs.ErrReservationConflict
	// and the lease survives. The unique index (server_id, protocol,
	// remote_port) is GLOBAL per physical port — one port, one owner,
	// regardless of tenant.
	//
	// Owner transaction guard (Recovery A2): the caller's transaction must
	// EXIST, belong to the same tenant+actor, and still be pending — it is
	// validated AND locked (write-first touch on SQLite, SELECT ... FOR
	// UPDATE elsewhere) inside the same transaction before any eviction or
	// insert, so the concurrent recovery sweep can never fail the owner
	// between validation and insert (a lease owned by a failed transaction
	// would be unrecoverable until lease expiry). Missing/cross-scope owner
	// → defs.ErrTransactionNotFound (no existence leak); non-pending owner →
	// defs.ErrInvalidTxTransition. An empty txID is rejected outright.
	TryAcquire(userInfo models.UserInfo, serverID, protocol string, port uint32, txID string, lease time.Duration, now time.Time) (*models.PortReservation, error)
	// CommitReservation marks a pending lease committed and binds it to the
	// persisted proxy row. Committed leases never expire.
	CommitReservation(userInfo models.UserInfo, txID string, proxyID uint) error
	// Release removes the reservation owned by txID.
	Release(userInfo models.UserInfo, txID string) error
	DeleteExpiredPending(userInfo models.UserInfo, now time.Time) (int64, error)
}

type portReservationQuery struct{ *queryImpl }
type portReservationMutation struct{ *mutationImpl }

func newPortReservationQuery(base *queryImpl) PortReservationQuery {
	return &portReservationQuery{base}
}
func newPortReservationMutation(base *mutationImpl) PortReservationMutation {
	return &portReservationMutation{base}
}

func (q *portReservationQuery) GetByServerFilter(userInfo models.UserInfo, serverID, protocol string, port uint32) ([]*models.PortReservation, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	list := []*models.PortReservation{}
	err := db.Where(&models.PortReservation{
		TenantID:   userInfo.GetTenantID(),
		ServerID:   serverID,
		Protocol:   protocol,
		RemotePort: port,
	}).Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (q *portReservationQuery) GetByTransactionID(userInfo models.UserInfo, txID string) ([]*models.PortReservation, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	list := []*models.PortReservation{}
	err := db.Where("transaction_id = ? AND tenant_id = ?", txID, userInfo.GetTenantID()).Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (q *portReservationQuery) ListExpiredPending(userInfo models.UserInfo, now time.Time) ([]*models.PortReservation, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	list := []*models.PortReservation{}
	err := db.Where("status = ? AND expires_at IS NOT NULL AND expires_at < ? AND tenant_id = ?",
		models.ReservationStatusPending, now, userInfo.GetTenantID()).Find(&list).Error
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *portReservationMutation) TryAcquire(userInfo models.UserInfo, serverID, protocol string, port uint32, txID string, lease time.Duration, now time.Time) (*models.PortReservation, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, err
	}
	if txID == "" {
		return nil, fmt.Errorf("TryAcquire: empty transaction id")
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	res := &models.PortReservation{
		TenantID:      userInfo.GetTenantID(),
		ServerID:      serverID,
		Protocol:      protocol,
		RemotePort:    port,
		TransactionID: txID,
		Status:        models.ReservationStatusPending,
		ExpiresAt:     now.Add(lease),
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		// Owner transaction guard FIRST (spec §8.1 step 3): the lease may
		// only ever be created for a transaction that exists, belongs to
		// this tenant+actor and is still pending. Locking the owner row
		// here closes the TOCTOU window against the concurrent recovery
		// sweep: without it the sweep could fail the transaction between
		// our validation and the insert, leaving a lease owned by a failed
		// transaction (the A2 root cause: recovery must never race a live
		// acquire into a failed-owner reservation).
		if err := lockOwnerTransactionForAcquire(tx, userInfo.GetTenantID(), userInfo.GetUserID(), txID); err != nil {
			return err
		}
		// The unique index allows at most one row per key; a conflicting
		// row decides whether the new lease may evict it (owner-aware, see
		// below) or must fail with RESERVATION_CONFLICT.
		var existing []*models.PortReservation
		if err := tx.Where(
			"server_id = ? AND protocol = ? AND remote_port = ?",
			serverID, protocol, port,
		).Find(&existing).Error; err != nil {
			return err
		}
		for _, ex := range existing {
			// Only an EXPIRED PENDING lease is a candidate for eviction.
			// Live pending / committed / releasing rows fall through to the
			// unique-index conflict on insert below.
			if ex.Status != models.ReservationStatusPending || !ex.IsExpired(now) {
				continue
			}
			// The owning transaction's tenant + state decide whether the
			// port is provably safe to release (P0-2: never silently steal
			// a lease whose config may already be live).
			var owner models.ChangeTransaction
			err := tx.Where("transaction_id = ?", ex.TransactionID).First(&owner).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// orphan: no owner state to prove safety
					return defs.ErrReservationConflict
				}
				return err
			}
			if owner.TenantID != userInfo.GetTenantID() {
				// cross-tenant: the physical port is global, but only its
				// owner's recovery path may release it
				return defs.ErrReservationConflict
			}
			switch owner.Status {
			case models.TxStatusPending, models.TxStatusPreflight:
				// Stale PRE-APPLY owner: nothing was ever sent to the
				// agent, so eviction is safe. Mark the owner failed with
				// the stable code first (the lease expiry is the crash
				// signal), then release below.
				upd := tx.Model(&models.ChangeTransaction{}).
					Where("transaction_id = ? AND status = ?", ex.TransactionID, owner.Status).
					Updates(map[string]interface{}{
						"status":        models.TxStatusFailed,
						"error_code":    defs.ErrorCodeReservationExpired,
						"error_message": "pending reservation lease expired and was evicted by a new acquire",
						"finished_at":   now,
					})
				if upd.Error != nil {
					return upd.Error
				}
				if upd.RowsAffected == 0 {
					return defs.ErrTxStatusConflict
				}
				if err := tx.Create(&models.AuditEvent{
					TransactionID: ex.TransactionID,
					TenantID:      ex.TenantID,
					EventType:     models.AuditEventTypeReservationExpired,
					Summary:       "transaction failed: pending reservation lease expired and was evicted by a new acquire",
					ErrorCode:     defs.ErrorCodeReservationExpired,
					Success:       false,
				}).Error; err != nil {
					return err
				}
				fallthrough
			case models.TxStatusFailed, models.TxStatusRolledBack:
				// Terminal AND proven safe: hard-delete the lease (a
				// soft-deleted row would still occupy the unique index)
				// and audit the eviction (spec §6.1.5: no lock disappears
				// without a reason).
				if err := tx.Unscoped().Where("id = ?", ex.ID).Delete(&models.PortReservation{}).Error; err != nil {
					return err
				}
				if err := tx.Create(&models.AuditEvent{
					TransactionID: ex.TransactionID,
					TenantID:      ex.TenantID,
					EventType:     models.AuditEventTypeReservationExpired,
					Summary:       "expired pending reservation evicted by a new acquire",
					Success:       true,
				}).Error; err != nil {
					return err
				}
			default:
				// applying/verifying/rolling_back/rollback_failed/
				// committed: the config may be live or the rule blocked —
				// never evict.
				return defs.ErrReservationConflict
			}
		}
		return tx.Create(res).Error
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, defs.ErrReservationConflict
		}
		return nil, err
	}
	return res, nil
}

// lockOwnerTransactionForAcquire validates and locks the owning
// change_transaction row at the head of the TryAcquire transaction (spec
// §8.1 step 3: the lease is only ever created for a transaction that
// exists, belongs to the calling tenant+actor and is still pending).
//
// It must close the TOCTOU window between "owner is pending" and "lease
// inserted": a concurrent recovery sweep could fail the transaction in
// between, leaving a lease owned by a failed transaction (Recovery A2 root
// cause — recovery must never race a live acquire into a failed-owner
// reservation). The mechanism is dialect-dependent:
//
//   - PostgreSQL/MySQL: SELECT ... FOR UPDATE takes the row lock on the
//     owner row for the rest of this transaction. If the sweep's guarded
//     UPDATE is mid-flight it blocks until this transaction commits, then
//     re-evaluates its WHERE (including NOT EXISTS reservation) against the
//     committed state and misses, aborting the whole sweep pass. If the
//     sweep committed first, this SELECT reads the terminal state and the
//     status check below rejects the acquire.
//
//   - SQLite: no FOR UPDATE support, so the write lock is taken WRITE-FIRST
//     with a value-neutral touch UPDATE (started_at = started_at; GORM also
//     bumps updated_at, which is harmless) whose WHERE clause re-validates
//     transaction_id + tenant_id + actor_user_id + status=pending.
//     SQLite's single-writer model serializes this against the sweep's
//     guarded UPDATE: whoever writes first wins, and the loser's statement
//     re-evaluates against the committed state when the winner commits
//     (busy_timeout), so either the acquire sees the sweep's terminal state
//     (miss → reject) or the sweep's CAS misses on NOT EXISTS (pass
//     aborts). This is the same write-first discipline as
//     transitionTxStatus — never a bare unlocked SELECT.
//
// Error contract: missing row, cross-tenant or cross-actor owner →
// defs.ErrTransactionNotFound (no existence leak); owner in any state other
// than pending → defs.ErrInvalidTxTransition (stable sentinel).
func lockOwnerTransactionForAcquire(tx *gorm.DB, tenantID, actorUserID int, txID string) error {
	scopeErr := func() error {
		return fmt.Errorf("%w: owner transaction %s", defs.ErrTransactionNotFound, txID)
	}
	stateErr := func(status string) error {
		return fmt.Errorf("%w: owner transaction %s is %s, only pending may acquire a reservation", defs.ErrInvalidTxTransition, txID, status)
	}

	if tx.Dialector.Name() == "sqlite" {
		res := tx.Model(&models.ChangeTransaction{}).
			Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ? AND status = ?",
				txID, tenantID, actorUserID, models.TxStatusPending).
			Update("started_at", gorm.Expr("started_at"))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 1 {
			return nil
		}
		// miss: classify (row missing or wrong scope → NotFound; wrong
		// status → invalid transition)
		var owner models.ChangeTransaction
		err := tx.Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ?",
			txID, tenantID, actorUserID).First(&owner).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return scopeErr()
			}
			return err
		}
		return stateErr(owner.Status)
	}

	// PostgreSQL/MySQL: row-lock the owner row for the rest of this
	// transaction.
	var owner models.ChangeTransaction
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("transaction_id = ? AND tenant_id = ? AND actor_user_id = ?",
			txID, tenantID, actorUserID).
		First(&owner).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return scopeErr()
		}
		return err
	}
	if owner.Status != models.TxStatusPending {
		return stateErr(owner.Status)
	}
	return nil
}

func (m *portReservationMutation) CommitReservation(userInfo models.UserInfo, txID string, proxyID uint) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	res := db.Model(&models.PortReservation{}).
		Where("transaction_id = ? AND tenant_id = ? AND status = ?", txID, userInfo.GetTenantID(), models.ReservationStatusPending).
		Updates(map[string]interface{}{
			"status":         models.ReservationStatusCommitted,
			"owner_proxy_id": proxyID,
			"expires_at":     nil,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return defs.ErrReservationNotFound
	}
	return nil
}

func (m *portReservationMutation) Release(userInfo models.UserInfo, txID string) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	res := db.Unscoped().Where("transaction_id = ? AND tenant_id = ?", txID, userInfo.GetTenantID()).
		Delete(&models.PortReservation{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return defs.ErrReservationNotFound
	}
	return nil
}

func (m *portReservationMutation) DeleteExpiredPending(userInfo models.UserInfo, now time.Time) (int64, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return 0, err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	res := db.Unscoped().Where(
		"status = ? AND expires_at IS NOT NULL AND expires_at < ? AND tenant_id = ?",
		models.ReservationStatusPending, now, userInfo.GetTenantID(),
	).Delete(&models.PortReservation{})
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// isUniqueViolation detects unique constraint errors across the supported
// SQL dialects (SQLite via glebarez, MySQL, PostgreSQL). GORM's translated
// sentinel is checked first, then driver message text.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "duplicate entry") ||
		strings.Contains(msg, "duplicate key")
}

// IsUniqueViolation is the exported form used by the transaction coordinator
// to detect idempotency-key insert races.
func IsUniqueViolation(err error) bool {
	return isUniqueViolation(err)
}
