package dao

import (
	"fmt"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// Reservation ↔ proxy-row reconciliation (post-rebuild).
//
// A rebuild (rebuildProxyConfigsInTx) hard-deletes the ACTIVE rows of a client
// and saves a fresh set. That is fine for the proxy_config table, but the
// port_reservation table is keyed on owner_proxy_id: a committed lease never
// expires and is only released by an explicit lifecycle operation. Whenever the
// rebuild replaces a row — because it reuses the id for a same-name rule on a
// different port (the silent same-name overwrite) or because it re-creates the
// rule under a new id (first-shadow conversion, rename) — the old committed
// lease either points at a port the rule no longer runs on, or at a row that no
// longer exists.
//
// Both shapes are the same defect: a lease that is invisible in the panel (the
// list joins on proxy_config) and that permanently blocks the old port with
// RESERVATION_CONFLICT for every future create. The helpers below are the
// commit-time fix:
//
//   - a lease whose RULE survived the rebuild (same name, same physical key on
//     the same child) TRANSFERS to the rebuilt row id — the port stays owned by
//     the rule that still serves it;
//   - a lease whose rule did not survive is RELEASED (hard delete, so the
//     unique index frees the port);
//   - any committed lease still owned by the final row but keyed on another
//     port is RELEASED (the same-name-overwrite leak).
//
// Everything runs inside the caller's DB transaction, so a failed commit rolls
// the reconciliation back with the rest of the change.
// ---------------------------------------------------------------------------

// captureActiveProxyRowsInTx snapshots the ACTIVE rows of a client BEFORE a
// rebuild deletes them. Stopped rows are excluded on purpose: they are not
// deleted by rebuildProxyConfigsInTx and keep their leases untouched.
func captureActiveProxyRowsInTx(tx *gorm.DB, tenantID, userID int, clientID string) ([]models.ProxyConfig, error) {
	var rows []models.ProxyConfig
	err := tx.Where("client_id = ? AND tenant_id = ? AND user_id = ?", clientID, tenantID, userID).
		Where("stopped IS NULL OR stopped = ?", false).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// reusedActiveRow reports whether the rebuild resolved `id` to an ACTIVE row
// that already existed before it ran (the silent same-name upsert).
func reusedActiveRow(before []models.ProxyConfig, id uint) bool {
	for i := range before {
		if before[i].ID == id && !before[i].Stopped {
			return true
		}
	}
	return false
}

// committedLeasesOfOwnerInTx loads every committed lease owned by one proxy
// row. Pending leases (owner NULL, owned by a transaction) are never touched.
func committedLeasesOfOwnerInTx(tx *gorm.DB, tenantID int, ownerID uint) ([]models.PortReservation, error) {
	var rows []models.PortReservation
	err := tx.Where("tenant_id = ? AND owner_proxy_id = ? AND status = ?",
		tenantID, ownerID, models.ReservationStatusCommitted).Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// hardDeleteLeaseInTx removes ONE lease permanently. A soft delete would keep
// occupying the unique index (server_id, protocol, remote_port) and the port
// would stay blocked, which is exactly the bug this file fixes.
func hardDeleteLeaseInTx(tx *gorm.DB, tenantID int, leaseID uint) error {
	res := tx.Unscoped().Where("id = ? AND tenant_id = ?", leaseID, tenantID).
		Delete(&models.PortReservation{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("%w: lease %d vanished during reconciliation", defs.ErrReservationNotFound, leaseID)
	}
	return nil
}

// reconcileRebuiltReservationsInTx transfers or releases the committed leases
// of the rows a rebuild just replaced.
//
// skipOldOwnerID excludes ONE old row whose lease is managed by the caller's own
// reservation migration (the safe-update target rule: CommitUpdate migrates it
// by physical key in S6). Pass 0 to reconcile every captured row.
//
// The successor lookup is (same client, same rule name, same physical key): a
// rule that still runs on the same port keeps its lease, everything else is
// released. A row whose content cannot be parsed cannot be proven to still serve
// its port, so its lease is released too — never silently kept.
func reconcileRebuiltReservationsInTx(
	tx *gorm.DB,
	tenantID int,
	replaced []models.ProxyConfig,
	childClientID string,
	skipOldOwnerID uint,
) (int, error) {
	if len(replaced) == 0 {
		return 0, nil
	}
	var rebuilt []models.ProxyConfig
	if err := tx.Where("client_id = ? AND tenant_id = ?", childClientID, tenantID).
		Find(&rebuilt).Error; err != nil {
		return 0, err
	}
	// name → rebuilt rows (a name is unique per child, but a rebuild may
	// transiently hold both an active and a stopped row with that name)
	byName := make(map[string][]models.ProxyConfig, len(rebuilt))
	for _, row := range rebuilt {
		byName[row.Name] = append(byName[row.Name], row)
	}

	released := 0
	for i := range replaced {
		old := replaced[i]
		if skipOldOwnerID != 0 && old.ID == skipOldOwnerID {
			continue
		}
		leases, err := committedLeasesOfOwnerInTx(tx, tenantID, old.ID)
		if err != nil {
			return released, err
		}
		if len(leases) == 0 {
			continue
		}
		oldKey, keyErr := proxyRowPhysicalKey(&old)
		for _, lease := range leases {
			if keyErr == nil {
				if succ, ok := rebuiltSuccessor(byName[old.Name], lease, oldKey); ok {
					if succ.ID != old.ID {
						res := tx.Model(&models.PortReservation{}).
							Where("id = ? AND tenant_id = ? AND status = ? AND owner_proxy_id = ?",
								lease.ID, tenantID, models.ReservationStatusCommitted, old.ID).
							Update("owner_proxy_id", succ.ID)
						if res.Error != nil {
							return released, res.Error
						}
						if res.RowsAffected != 1 {
							return released, fmt.Errorf("%w: lease %d owner transfer to row %d",
								defs.ErrReservationConflict, lease.ID, succ.ID)
						}
						continue
					}
					continue // same row id: the rule kept its own lease
				}
			}
			if err := hardDeleteLeaseInTx(tx, tenantID, lease.ID); err != nil {
				return released, err
			}
			released++
		}
	}
	return released, nil
}

// rebuiltSuccessor reports whether one of the rebuilt rows still carries the
// replaced rule: same name AND same physical key as the lease being migrated.
func rebuiltSuccessor(candidates []models.ProxyConfig, lease models.PortReservation, oldKey ReservationKey) (models.ProxyConfig, bool) {
	// The lease key is authoritative: it is the port the rule actually held.
	if lease.ServerID != oldKey.ServerID || lease.Protocol != oldKey.Protocol || lease.RemotePort != oldKey.RemotePort {
		return models.ProxyConfig{}, false
	}
	for i := range candidates {
		key, err := proxyRowPhysicalKey(&candidates[i])
		if err != nil {
			continue
		}
		if key != oldKey {
			continue
		}
		// Prefer the active row when a stopped sibling shares the name.
		if candidates[i].Stopped {
			continue
		}
		return candidates[i], true
	}
	return models.ProxyConfig{}, false
}

// releaseStaleOwnerReservationsInTx releases every committed lease owned by
// ownerID whose physical key differs from keep.
//
// This is the same-name-overwrite invariant at its narrowest: one proxy row
// owns at most ONE committed lease, the one for the port it currently serves.
// The commit calls it after the rebuild resolved the final row id, so a rule
// that moved from 39130 to 39131 can never leave 39130 reserved.
func releaseStaleOwnerReservationsInTx(tx *gorm.DB, tenantID int, ownerID uint, keep ReservationKey) (int, error) {
	leases, err := committedLeasesOfOwnerInTx(tx, tenantID, ownerID)
	if err != nil {
		return 0, err
	}
	released := 0
	for _, lease := range leases {
		if lease.ServerID == keep.ServerID && lease.Protocol == keep.Protocol && lease.RemotePort == keep.RemotePort {
			continue
		}
		if err := hardDeleteLeaseInTx(tx, tenantID, lease.ID); err != nil {
			return released, err
		}
		released++
	}
	return released, nil
}

// adoptCommittedLeaseInTx verifies the same-key overwrite contract: the rule
// being overwritten already holds a committed lease at exactly this key, so the
// transaction acquired no pending lease of its own. After reconciliation the
// lease must belong to the final row id — otherwise the create would commit a
// rule whose port is owned by somebody else (or by nobody), and the next write
// would either steal the port or block it forever.
func adoptCommittedLeaseInTx(tx *gorm.DB, tenantID int, key ReservationKey, finalProxyID uint) error {
	var rows []models.PortReservation
	err := tx.Where("tenant_id = ? AND server_id = ? AND protocol = ? AND remote_port = ? AND status = ?",
		tenantID, key.ServerID, key.Protocol, key.RemotePort, models.ReservationStatusCommitted).
		Find(&rows).Error
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("%w: %d committed leases at %s/%s/%d", defs.ErrReservationConflict,
			len(rows), key.ServerID, key.Protocol, key.RemotePort)
	}
	if rows[0].OwnerProxyID == nil || *rows[0].OwnerProxyID != finalProxyID {
		return fmt.Errorf("%w: lease at %s/%s/%d is not owned by proxy %d",
			defs.ErrReservationConflict, key.ServerID, key.Protocol, key.RemotePort, finalProxyID)
	}
	return nil
}
