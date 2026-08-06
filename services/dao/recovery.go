package dao

import (
	"errors"
	"fmt"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"gorm.io/gorm"
)

// SystemRecoveryResult reports what the startup/periodic cleanup did. The
// Retained/Blocked counters let monitoring see leases that could NOT be
// proven safe to release and transactions that now need manual intervention.
type SystemRecoveryResult struct {
	// ReleasedReservations: leases hard-released because the owning
	// transaction is proven safe to forget (pending/preflight failed by
	// this pass, or already failed/rolled_back).
	ReleasedReservations int64
	// RetainedReservations: leases KEPT because releasing them cannot be
	// proven safe (mid-apply crash, terminal-retain owners, orphans).
	RetainedReservations int64
	// FailedTransactions: non-terminal pre-apply owners marked failed —
	// both expired-pending reservation owners (phase A) and stale
	// no-reservation transactions (phase B).
	FailedTransactions int64
	// BlockedTransactions: mid-apply owners marked rollback_failed
	// (needs-intervention, spec §8.2) — phase A and phase B combined.
	BlockedTransactions int64
	AuditedEvents       int64
}

// DefaultStaleTransactionAfter is the age past which a change transaction
// with NO port reservation of its own is considered crashed and recovered
// by the system sweep (phase B of SystemRecoverExpiredPending). Four
// minutes covers the upper bound of preflight (two ~5s scans) + apply (15s
// timeout) + verify (30s poll budget) + compensation with margin, and is
// deliberately far beyond the 120s reservation lease, so a live coordinator
// is never misclassified. The cutoff is StartedAt-based (never CreatedAt).
const DefaultStaleTransactionAfter = 4 * time.Minute

// staleNoReservationStatuses are the non-terminal transaction states that
// phase B may recover. Terminal states are never candidates: committed is
// done, failed/rolled_back/rollback_failed are already closed.
var staleNoReservationStatuses = []string{
	models.TxStatusPending,
	models.TxStatusPreflight,
	models.TxStatusApplying,
	models.TxStatusVerifying,
	models.TxStatusRollingBack,
}

// SystemRecoverExpiredPending is the SYSTEM-ONLY recovery sweep (spec
// §6.1.3/§6.1.4). It is intentionally NOT part of the user-facing
// Query/Mutation composites: it operates across ALL tenants in one
// transaction, so it must never be reached through a user-scoped interface.
//
// The pass has two phases, both inside the SAME database transaction:
//
// Phase A — expired pending reservations (the original sweep). For every
// expired pending lease the owning transaction's state decides the outcome
// (P0-2 fail-closed):
//
//   - pending/preflight: the candidate config was NEVER sent to the agent,
//     so the lease is provably safe to release. The transaction is CAS-ed
//     to failed (RESERVATION_EXPIRED + finished_at) and audited, then the
//     lease is hard-deleted and the release audited.
//
//   - applying/verifying/rolling_back: the candidate config MAY already be
//     live on the agent. The lease is NEVER released. The transaction is
//     CAS-ed to rollback_failed (needs manual intervention, spec §8.2) and
//     audited, and the reservation is moved to the non-evictable
//     `releasing` state (expires_at cleared) so neither this sweep nor
//     TryAcquire's lazy eviction can touch it again.
//
//   - rollback_failed/committed: terminal-retain owners. The lease stays,
//     the transaction row is untouched and the anomaly is audited.
//
//   - failed/rolled_back: terminal AND proven safe (rolled_back is only
//     reached after a verified compensation), so the expired lease is
//     hard-released and audited.
//
//   - orphan (no transaction row): no owner state exists to prove the port
//     is safe to release — the lease is retained and the anomaly audited.
//     The audit TenantID always comes from the reservation row itself,
//     never a fabricated zero.
//
// Phase B — stale transactions with NO reservation (Recovery A2). A
// transaction that crashed between row creation and its own TryAcquire (or,
// for updates, reused the old committed reservation without creating a
// current lease) never owns an expired lease, so phase A can never see it —
// it would stay in-flight forever. Any transaction older than
// DefaultStaleTransactionAfter (StartedAt-based) that owns NO live
// reservation (pending/committed/releasing — soft-deleted rows excluded) is
// recovered:
//
//   - pending/preflight: nothing was ever sent to the agent → CAS to failed
//     (RESERVATION_EXPIRED + finished_at) + transaction_stale audit.
//
//   - applying/verifying/rolling_back: the candidate config MAY be live →
//     CAS to rollback_failed (ROLLBACK_FAILED + finished_at) +
//     transaction_stale audit (manual intervention).
//
//   - The guarded CAS re-encodes transaction_id, tenant_id, the loaded
//     status, the stale cutoff and NOT EXISTS reservation, so a lease that
//     appeared after the candidate query (a live coordinator won the
//     acquire race) aborts the pass with ErrTxStatusConflict. No
//     reservation is ever touched by phase B; old committed reservations of
//     previous transactions stay byte-identical.
//
// Committed reservations and live pending leases are never touched. The
// whole sweep runs in ONE database transaction: a conflict or audit failure
// mid-pass aborts the pass (the 30s ticker retries), so no partial cleanup
// is ever observable.
func SystemRecoverExpiredPending(ctx *app.Context, now time.Time) (*SystemRecoveryResult, error) {
	if ctx == nil || ctx.GetApp() == nil || ctx.GetApp().GetDBManager() == nil {
		return nil, errors.New("system recovery: missing app/db context")
	}
	db := ctx.GetApp().GetDBManager().GetDefaultDB()
	result := &SystemRecoveryResult{}

	err := db.Transaction(func(tx *gorm.DB) error {
		// ---- phase A: expired pending reservations ----
		var expired []*models.PortReservation
		if err := tx.Where(
			"status = ? AND expires_at IS NOT NULL AND expires_at < ?",
			models.ReservationStatusPending, now,
		).Find(&expired).Error; err != nil {
			return err
		}

		for _, res := range expired {
			var txRow models.ChangeTransaction
			err := tx.Where("transaction_id = ?", res.TransactionID).First(&txRow).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			orphan := errors.Is(err, gorm.ErrRecordNotFound)

			// The audit tenant always comes from the reservation row: an
			// orphan has no transaction row to read, and writing a zero
			// tenant would corrupt the audit trail.
			tenantID := res.TenantID

			switch {
			case orphan:
				// No owner state to prove the port is safe: retain + audit.
				if err := auditRecovery(tx, tenantID, res.TransactionID,
					models.AuditEventTypeReservationExpired,
					"orphan expired pending reservation retained: no owning transaction proves the port is safe to release; manual intervention required",
					false, defs.ErrorCodeReservationExpired); err != nil {
					return err
				}
				result.AuditedEvents++
				result.RetainedReservations++

			case txRow.Status == models.TxStatusPending || txRow.Status == models.TxStatusPreflight:
				// Pre-apply owner: nothing was ever sent to the agent.
				// CAS to failed, then release.
				if err := failExpiredOwner(tx, res, txRow, now); err != nil {
					return err
				}
				result.FailedTransactions++
				if err := auditRecovery(tx, tenantID, res.TransactionID,
					models.AuditEventTypeReservationExpired,
					"transaction failed: pending reservation lease expired", false, defs.ErrorCodeReservationExpired); err != nil {
					return err
				}
				result.AuditedEvents++
				if err := hardReleaseReservation(tx, res); err != nil {
					return err
				}
				result.ReleasedReservations++
				if err := auditRecovery(tx, tenantID, res.TransactionID,
					models.AuditEventTypeReservationExpired,
					"expired pending reservation released", true, ""); err != nil {
					return err
				}
				result.AuditedEvents++

			case txRow.Status == models.TxStatusApplying ||
				txRow.Status == models.TxStatusVerifying ||
				txRow.Status == models.TxStatusRollingBack:
				// The candidate config may already be live: NEVER release.
				// Block for manual intervention and move the lease out of
				// the expired-pending sweep (releasing, no expiry).
				if err := blockExpiredOwner(tx, res, txRow, now); err != nil {
					return err
				}
				result.BlockedTransactions++
				if err := auditRecovery(tx, tenantID, res.TransactionID,
					models.AuditEventTypeReservationExpired,
					"transaction crashed mid-apply: lease retained for manual intervention; automatic compensation was not possible",
					false, defs.ErrorCodeRollbackFailed); err != nil {
					return err
				}
				result.AuditedEvents++
				result.RetainedReservations++

			case txRow.Status == models.TxStatusRollbackFailed || txRow.Status == models.TxStatusCommitted:
				// Terminal-retain owners: lease stays, row untouched.
				if err := auditRecovery(tx, tenantID, res.TransactionID,
					models.AuditEventTypeReservationExpired,
					"expired pending reservation retained: owning transaction is terminal ("+txRow.Status+"); manual intervention required",
					false, defs.ErrorCodeReservationExpired); err != nil {
					return err
				}
				result.AuditedEvents++
				result.RetainedReservations++

			case txRow.Status == models.TxStatusFailed || txRow.Status == models.TxStatusRolledBack:
				// Terminal AND proven safe: hard release + audit.
				if err := hardReleaseReservation(tx, res); err != nil {
					return err
				}
				result.ReleasedReservations++
				if err := auditRecovery(tx, tenantID, res.TransactionID,
					models.AuditEventTypeReservationExpired,
					"expired pending reservation released (owning transaction already terminal)", true, ""); err != nil {
					return err
				}
				result.AuditedEvents++

			default:
				return fmt.Errorf("system recovery: unexpected transaction status %q for %s", txRow.Status, res.TransactionID)
			}
		}

		// ---- phase B: stale transactions with no reservation of their own.
		// Same transaction, same pass — a phase-B conflict rolls back phase A's
		// work with it (no partial cleanup is ever observable). ----
		return recoverStaleTransactionsPhase(tx, now, result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// recoverStaleTransactionsPhase is phase B of the system recovery pass
// (Recovery A2): transactions that crashed before ever creating a port
// reservation would otherwise stay in-flight forever — the expired-pending
// sweep can never see them because they own no lease. Candidates are
// non-terminal transactions older than now - DefaultStaleTransactionAfter
// (StartedAt-based) with NO live reservation row (pending/committed/
// releasing — soft-deleted rows excluded): a current transaction's lease is
// handled by phase A / the reservation sweep, while an OLD committed
// reservation of a PREVIOUS transaction on the same port never shields the
// current transaction (the subquery keys on the candidate's own
// transaction_id).
func recoverStaleTransactionsPhase(tx *gorm.DB, now time.Time, result *SystemRecoveryResult) error {
	cutoff := now.Add(-DefaultStaleTransactionAfter)
	var stale []*models.ChangeTransaction
	if err := tx.Where(
		"status IN ? AND started_at < ? AND NOT EXISTS ("+
			"SELECT 1 FROM port_reservation WHERE transaction_id = change_transaction.transaction_id AND deleted_at IS NULL)",
		staleNoReservationStatuses, cutoff,
	).Find(&stale).Error; err != nil {
		return err
	}
	for _, txRow := range stale {
		if err := recoverStaleNoReservationTx(tx, txRow, cutoff, now, result); err != nil {
			return err
		}
	}
	return nil
}

// recoverStaleNoReservationTx applies the phase-B fork to ONE stale
// candidate. The CAS UPDATE re-encodes everything that made it a candidate
// — transaction_id, tenant_id, the LOADED status, the stale cutoff and
// NOT EXISTS reservation — so a state or lease change after the candidate
// query (a live coordinator winning the acquire race, the owner advancing
// status) deterministically misses: RowsAffected == 0 → ErrTxStatusConflict
// and the whole pass rolls back (the ticker retries from fresh state).
//
// Fork (spec §8.2 fail-closed):
//   - pending/preflight: nothing was ever sent to the agent → failed,
//     RESERVATION_EXPIRED (stable code), finished_at = now.
//   - applying/verifying/rolling_back: the candidate config MAY be live →
//     rollback_failed, ROLLBACK_FAILED, finished_at = now (manual
//     intervention).
//
// No reservation is ever touched here: the candidate owns none by
// definition, and old committed reservations of previous transactions stay
// byte-identical. Each recovery writes one transaction_stale audit whose
// summary carries no config or secret content.
func recoverStaleNoReservationTx(tx *gorm.DB, txRow *models.ChangeTransaction, cutoff, now time.Time, result *SystemRecoveryResult) error {
	var (
		targetStatus string
		errorCode    string
		errorMessage string
		summary      string
	)
	switch txRow.Status {
	case models.TxStatusPending, models.TxStatusPreflight:
		targetStatus = models.TxStatusFailed
		errorCode = defs.ErrorCodeReservationExpired
		errorMessage = "transaction became stale without ever creating a port reservation; nothing was applied to the agent"
		summary = "stale transaction recovered: no port reservation was ever created, nothing was applied; marked failed (RESERVATION_EXPIRED)"
	case models.TxStatusApplying, models.TxStatusVerifying, models.TxStatusRollingBack:
		targetStatus = models.TxStatusRollbackFailed
		errorCode = defs.ErrorCodeRollbackFailed
		errorMessage = "transaction became stale without a port reservation while mid-apply; automatic compensation was not possible, manual intervention required"
		summary = "stale transaction recovered: crashed mid-apply before creating a port reservation; blocked for manual intervention (ROLLBACK_FAILED)"
	default:
		return fmt.Errorf("system recovery: unexpected phase-B status %q for %s", txRow.Status, txRow.TransactionID)
	}

	upd := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND tenant_id = ? AND status = ? AND started_at < ? AND NOT EXISTS ("+
			"SELECT 1 FROM port_reservation WHERE transaction_id = ? AND deleted_at IS NULL)",
			txRow.TransactionID, txRow.TenantID, txRow.Status, cutoff, txRow.TransactionID).
		Updates(map[string]interface{}{
			"status":        targetStatus,
			"error_code":    errorCode,
			"error_message": errorMessage,
			"finished_at":   now,
		})
	if upd.Error != nil {
		return upd.Error
	}
	if upd.RowsAffected == 0 {
		// The row moved or a reservation appeared between query and CAS: a
		// live coordinator owns the transaction now. Abort the whole pass so
		// the next sweep re-evaluates from the fresh state.
		return fmt.Errorf("%w: stale transaction %s changed concurrently", defs.ErrTxStatusConflict, txRow.TransactionID)
	}

	if err := auditRecovery(tx, txRow.TenantID, txRow.TransactionID,
		models.AuditEventTypeTransactionStale, summary, false, errorCode); err != nil {
		return err
	}
	result.AuditedEvents++
	if targetStatus == models.TxStatusFailed {
		result.FailedTransactions++
	} else {
		result.BlockedTransactions++
	}
	return nil
}

// failExpiredOwner CAS-es a pre-apply owner (pending/preflight) to failed
// with the stable RESERVATION_EXPIRED code. A CAS miss means the row moved
// concurrently (a live coordinator owns it now): abort the pass so the next
// sweep re-evaluates from the fresh state.
func failExpiredOwner(tx *gorm.DB, res *models.PortReservation, txRow models.ChangeTransaction, now time.Time) error {
	upd := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND status = ?", res.TransactionID, txRow.Status).
		Updates(map[string]interface{}{
			"status":        models.TxStatusFailed,
			"error_code":    defs.ErrorCodeReservationExpired,
			"error_message": "pending reservation lease expired before the transaction finished",
			"finished_at":   now,
		})
	if upd.Error != nil {
		return upd.Error
	}
	if upd.RowsAffected == 0 {
		return defs.ErrTxStatusConflict
	}
	return nil
}

// blockExpiredOwner CAS-es a mid-apply owner (applying/verifying/
// rolling_back) to rollback_failed — the crash-recovery transition pinned
// in the model — and moves its lease to the non-evictable `releasing`
// state with no expiry, so neither the sweep nor TryAcquire's lazy
// eviction can touch it again.
func blockExpiredOwner(tx *gorm.DB, res *models.PortReservation, txRow models.ChangeTransaction, now time.Time) error {
	upd := tx.Model(&models.ChangeTransaction{}).
		Where("transaction_id = ? AND status = ?", res.TransactionID, txRow.Status).
		Updates(map[string]interface{}{
			"status":        models.TxStatusRollbackFailed,
			"error_code":    defs.ErrorCodeRollbackFailed,
			"error_message": "transaction crashed mid-apply; automatic compensation was not possible, manual intervention required",
			"finished_at":   now,
		})
	if upd.Error != nil {
		return upd.Error
	}
	if upd.RowsAffected == 0 {
		return defs.ErrTxStatusConflict
	}
	lease := tx.Model(&models.PortReservation{}).
		Where("id = ? AND status = ?", res.ID, models.ReservationStatusPending).
		Updates(map[string]interface{}{
			"status":     models.ReservationStatusReleasing,
			"expires_at": nil,
		})
	if lease.Error != nil {
		return lease.Error
	}
	if lease.RowsAffected == 0 {
		return defs.ErrTxStatusConflict
	}
	return nil
}

// hardReleaseReservation removes the lease row (hard delete: a
// soft-deleted row would still occupy the global unique index).
func hardReleaseReservation(tx *gorm.DB, res *models.PortReservation) error {
	return tx.Unscoped().Where("id = ?", res.ID).Delete(&models.PortReservation{}).Error
}

func auditRecovery(tx *gorm.DB, tenantID int, txID, eventType, summary string, success bool, errorCode string) error {
	return tx.Create(&models.AuditEvent{
		TransactionID: txID,
		TenantID:      tenantID,
		EventType:     eventType,
		Summary:       summary,
		ErrorCode:     errorCode,
		Success:       success,
	}).Error
}
