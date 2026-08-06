package dao

import (
	"errors"
	"fmt"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"gorm.io/gorm"
)

// migrateReservationInTx executes the reservation migration (S6). Every
// mode re-validates the DB state instead of trusting the caller; any
// divergence fails the whole commit.
func migrateReservationInTx(tx *gorm.DB, tenantID int, in *CommitUpdateInput, finalProxyID uint) error {
	switch in.Reservation.Mode {
	case ReservationModeSameKeyOwned:
		if err := casReservationOwnerInTx(tx, tenantID, in.Reservation.OldKey, in.OldProxyID, finalProxyID); err != nil {
			return fmt.Errorf("same-key owned reservation CAS: %w", err)
		}
	case ReservationModeSameKeyPending:
		if err := commitPendingByKeyInTx(tx, tenantID, in.Reservation.NewKey, in.TxID, finalProxyID); err != nil {
			return err
		}
	case ReservationModeDifferentKey:
		if err := commitPendingByKeyInTx(tx, tenantID, in.Reservation.NewKey, in.TxID, finalProxyID); err != nil {
			return err
		}
		if in.Reservation.OldReservationExpected {
			if err := releaseOldCommittedInTx(tx, tenantID, in.Reservation.OldKey, in.OldProxyID); err != nil {
				return err
			}
		} else if err := guardNoReservationAtKeyInTx(tx, tenantID, in.Reservation.OldKey); err != nil {
			return err
		}
	}
	return nil
}

// casReservationOwnerInTx implements SameKeyOwned. The row must exist, be
// committed and owned by the OLD proxy id. When the final id equals the old
// id the owner CAS update would be a value-identical no-op (MySQL reports
// RowsAffected=0 for such an UPDATE), so the shape check alone is the
// success path — success NEVER depends on a no-op RowsAffected. When the id
// changes, the guarded UPDATE re-verifies status+owner in its WHERE.
// transaction_id is NEVER rewritten: compensation releases by txID must not
// be able to delete a committed lease owned by an earlier create
// transaction.
func casReservationOwnerInTx(tx *gorm.DB, tenantID int, key ReservationKey, oldProxyID, finalProxyID uint) error {
	var row models.PortReservation
	err := tx.Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ?",
		tenantID, key.ServerID, key.Protocol, key.RemotePort).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: same-key reservation %s/%s/%d missing",
				defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort)
		}
		return err
	}
	if row.Status != models.ReservationStatusCommitted || row.OwnerProxyID == nil || *row.OwnerProxyID != oldProxyID {
		return fmt.Errorf("%w: same-key reservation %s/%s/%d is not committed and owned by proxy %d (status=%s owner=%v)",
			defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort, oldProxyID, row.Status, row.OwnerProxyID)
	}
	if finalProxyID == oldProxyID {
		return nil
	}
	res := tx.Model(&models.PortReservation{}).
		Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ? AND status = ? AND owner_proxy_id = ?",
			tenantID, key.ServerID, key.Protocol, key.RemotePort, models.ReservationStatusCommitted, oldProxyID).
		Update("owner_proxy_id", finalProxyID)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		// CAS miss inside the transaction: the row changed between the read
		// and the update — fail closed.
		return fmt.Errorf("%w: same-key reservation %s/%s/%d owner CAS miss",
			defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort)
	}
	return nil
}

// commitPendingByKeyInTx implements the new-key commit used by
// SameKeyPending and DifferentKey: the pending lease at the key, owned by
// THIS transaction (transaction_id=TxID), flips to committed with
// owner=final and a cleared expiry. A CAS miss is classified: no row at all
// → ErrReservationNotFound; a row owned by someone/something else →
// ErrReservationConflict.
func commitPendingByKeyInTx(tx *gorm.DB, tenantID int, key ReservationKey, txID string, finalProxyID uint) error {
	res := tx.Model(&models.PortReservation{}).
		Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ? AND transaction_id = ? AND status = ?",
			tenantID, key.ServerID, key.Protocol, key.RemotePort, txID, models.ReservationStatusPending).
		Updates(map[string]interface{}{
			"status":         models.ReservationStatusCommitted,
			"owner_proxy_id": finalProxyID,
			"expires_at":     nil,
		})
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
	return fmt.Errorf("%w: pending reservation %s/%s/%d is not owned by tx %s (status=%s tx=%s)",
		defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort, txID, row.Status, row.TransactionID)
}

// releaseOldCommittedInTx implements DifferentKey + OldReservationExpected:
// the old committed row (owner=old proxy) MUST exist and is hard-deleted
// (a soft-deleted row would still occupy the unique index). 0 rows is NOT
// tolerated — silently keeping the old lock while committing the new key
// would leave a double lock the coordinator cannot see.
func releaseOldCommittedInTx(tx *gorm.DB, tenantID int, key ReservationKey, oldProxyID uint) error {
	res := tx.Unscoped().
		Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ? AND status = ? AND owner_proxy_id = ?",
			tenantID, key.ServerID, key.Protocol, key.RemotePort, models.ReservationStatusCommitted, oldProxyID).
		Delete(&models.PortReservation{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 1 {
		return nil
	}
	return fmt.Errorf("%w: old committed reservation %s/%s/%d owned by proxy %d",
		defs.ErrReservationNotFound, key.ServerID, key.Protocol, key.RemotePort, oldProxyID)
}

// guardNoReservationAtKeyInTx implements DifferentKey + legacy
// (OldReservationExpected=false): the old key must be empty for this
// tenant. ANY row (any status, any owner) fails closed — silently
// committing the new key while an unseen old lock survives would leave a
// double lock. 0 rows is tolerated (the legacy world never had an old
// committed row).
func guardNoReservationAtKeyInTx(tx *gorm.DB, tenantID int, key ReservationKey) error {
	var count int64
	err := tx.Model(&models.PortReservation{}).
		Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ?",
			tenantID, key.ServerID, key.Protocol, key.RemotePort).Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("%w: old key %s/%s/%d unexpectedly holds a reservation",
			defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort)
	}
	return nil
}
