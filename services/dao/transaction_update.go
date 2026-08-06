package dao

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"gorm.io/gorm"
)

// UpdateBranch selects which commit branch persists the update.
type UpdateBranch string

const (
	// UpdateBranchActive persists the applied config on the child: the
	// child client row is updated, the proxy_config rows are rebuilt from
	// the applied config (A3a).
	UpdateBranchActive UpdateBranch = "active"
	// UpdateBranchStopped updates a stopped rule IN PLACE (name/type/
	// content, stopped stays true) with ZERO client delta — the child row
	// is only guarded, never written, and no Apply runs (A3b).
	UpdateBranchStopped UpdateBranch = "stopped"
)

// ReservationMode describes how the port reservation migrates during a safe
// update commit.
type ReservationMode string

const (
	// ReservationModeSameKeyOwned: same key, an existing committed row owned
	// by the old proxy — the commit CAS-moves owner_proxy_id old→final and
	// NEVER rewrites transaction_id (compensation releases by txID must not
	// be able to delete a committed lease owned by an earlier create).
	ReservationModeSameKeyOwned ReservationMode = "same_key_owned"
	// ReservationModeSameKeyPending: same key, legacy path where the
	// coordinator acquired a NEW pending lease at the (old) key; the commit
	// flips pending→committed owner=final.
	ReservationModeSameKeyPending ReservationMode = "same_key_pending"
	// ReservationModeDifferentKey: the key changed (port AND/OR protocol —
	// protocol is inside the unique index); the new-key pending commits and,
	// when OldReservationExpected, the old committed row (owner=old) is
	// hard-deleted; otherwise the old key must be empty (fail closed — never
	// leave a silent double lock).
	ReservationModeDifferentKey ReservationMode = "different_key"
)

// ReservationStep carries the physical reservation keys and the expectation
// for the old committed row. The DAO never trusts the caller: every mode is
// re-validated against the DB inside the commit transaction.
type ReservationStep struct {
	Mode   ReservationMode
	OldKey ReservationKey // physical key before the update
	NewKey ReservationKey // physical key after the update
	// DifferentKey only: true = an old committed row (owner=old proxy) must
	// exist and be deleted (0 rows fails); false = legacy, the old key is
	// expected to be empty (ANY row at all fails closed).
	OldReservationExpected bool
}

// StoppedUpdatePayload carries the stopped-branch payload (A3b). The
// stopped branch never touches the child client row: ChildClientID is the
// child the rule lives on (only GUARDED), NewName may equal OldName (keep)
// or rename in place (id preserved), NewTypedJSON is the new typed rule
// (frp v1 JSON) written to the row.
type StoppedUpdatePayload struct {
	ChildClientID string // must equal the old row's client_id (stopped rules live on the child)
	NewName       string // may equal OldName (keep) or rename in place (id preserved)
	NewType       string
	NewServerID   string // must equal OldServerID (no cross-server moves)
	NewTypedJSON  []byte // new typed rule (frp v1 JSON) written to the row
}

// CommitUpdateInput is the complete payload of one safe-update commit. All
// values are tenant-scoped by the caller; the DAO re-checks the tenant and
// the old-rule identity on every row it touches.
type CommitUpdateInput struct {
	TxID string
	// old rule identity (plan result; re-verified against the DB in S1)
	OldProxyID     uint
	OldName        string
	OldServerID    string
	OriginClientID string

	// branch (enum, not a boolean pair; DAO validates exactly one)
	Branch UpdateBranch

	// active payload (Branch==active, required)
	Client    *models.ClientEntity // child carrying the applied (final) config
	Shadow    *ShadowCommit        // first-shadow conversion; ChildCreated=false for existing child
	ProxyName string               // final name of the rule (rebuild want)

	// stopped payload (branch==stopped, required)
	StoppedUpdate *StoppedUpdatePayload

	// reservation migration (enum + expectation; DAO re-validates)
	Reservation ReservationStep

	// AFTER-only snapshots. The before rows are saved by the coordinator
	// BEFORE apply (spec §8.1 step 4); the commit never writes before rows.
	ClientAfterSnapshot SnapshotInput
	ProxyAfterSnapshot  SnapshotInput

	// update audit event (TransactionID/TenantID/ActorUserID/EventType
	// forced by the DAO; summary must not embed plaintext config)
	AuditEvent *models.AuditEvent

	// EvidenceJSON is written onto the transaction INSIDE the commit
	// transaction (before the committed transition). The coordinator uses
	// it for the stopped branch's explicit "db-only; zero agent apply"
	// proof; it must never carry config/secret material.
	EvidenceJSON string
}

// CommitUpdate atomically persists the final state of a safe update.
//
// Transaction order (any failure rolls the whole commit back):
//
//	S0 static validation — zero DB writes, stable ErrInvalidCommitInput
//	S1 guard the old rule row (tenant+user+OldProxyID; name/server/
//	   stopped/shape re-check → ErrProxyConfigNotFound / ErrProxyConfigChanged)
//	S2 rename second guard (same origin scope, TOCTOU →
//	   ErrProxyConfigNameConflict)
//	S3 branch write: active first-shadow converts the origin, migrates its
//	   same-server stopped siblings and persists/rebuilds the child; stopped
//	   updates guard the child and update only the stopped proxy row in place
//	S4 active rebuild → finalProxyID; stopped keeps OldProxyID without rebuild
//	S5 backfill change_transaction.proxy_id (verifying CAS)
//	S6 reservation migration (write-first CAS per mode; the DAO re-validates
//	   every expectation instead of trusting the caller)
//	S7 save the two encrypted AFTER snapshots (client + proxy) — before
//	   rows are the coordinator's job pre-apply and are never written here
//	S8 audit event (TxID/tenant/actor/EventType forced)
//	S9 transition tx → committed (last step)
func (m *transactionMutation) CommitUpdate(userInfo models.UserInfo, in *CommitUpdateInput) (uint, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return 0, err
	}
	// S0 static validation — zero DB writes; every violation is the stable
	// ErrInvalidCommitInput BEFORE the transaction begins.
	if err := validateCommitUpdateInput(in); err != nil {
		return 0, err
	}
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	tenantID := userInfo.GetTenantID()
	userID := userInfo.GetUserID()

	var finalProxyID uint
	err := db.Transaction(func(tx *gorm.DB) error {
		// S1 guard the old rule row (tenant+user scoped, re-verified; branch
		// aware — returns the loaded row for the stopped in-place update)
		old, err := loadOldProxyRowInTx(tx, tenantID, userID, in)
		if err != nil {
			return fmt.Errorf("commit update: %w", err)
		}
		// S2 rename second guard (TOCTOU; single WHERE expression)
		if err := guardRenameConflictInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit update: %w", err)
		}
		switch in.Branch {
		case UpdateBranchActive:
			// S3 active branch writes
			if in.Shadow.FirstShadow {
				if err := guardNoOtherServerStoppedRowsInTx(tx, tenantID, userID,
					in.OriginClientID, in.Client.ServerID); err != nil {
					return fmt.Errorf("commit update: %w", err)
				}
				if err := convertOriginToShadowInTx(tx, tenantID, userID, in.OriginClientID); err != nil {
					return fmt.Errorf("commit update: %w", err)
				}
				// stopped rules are absent from the applied config; move the
				// same-server stopped siblings to the generated child with their
				// original row id so reservations never dangle and StartProxy can
				// still find them by child_id. Other-server stopped rows are NOT
				// migrated (they must never surface under the wrong server).
				if err := migrateStoppedProxyRowsInTx(tx, tenantID, userID,
					in.OriginClientID, in.Client.ClientID, in.Client.ServerID); err != nil {
					return fmt.Errorf("commit update: migrate stopped origin proxy rows: %w", err)
				}
				if err := deleteActiveProxyRowsInTx(tx, tenantID, userID, in.OriginClientID); err != nil {
					return fmt.Errorf("commit update: delete active origin proxy rows: %w", err)
				}
			}
			if err := persistChildClientInTx(tx, tenantID, userID, in.Client, in.Shadow.ChildCreated); err != nil {
				return fmt.Errorf("commit update: %w", err)
			}
			// S4 rebuild proxy_config rows from the applied config
			id, err := rebuildProxyConfigsInTx(tx, tenantID, userID, in.Client, in.ProxyName)
			if err != nil {
				return fmt.Errorf("commit update: rebuild proxy configs: %w", err)
			}
			// existing-child same-name MUST keep the old row id (the row id is
			// the lease identity): a different id means the rebuild resolved a
			// different row than the plan's old rule — fail closed.
			if !in.Shadow.FirstShadow && in.ProxyName == in.OldName && id != in.OldProxyID {
				return fmt.Errorf("%w: same-name update resolved proxy row %d, expected old row %d",
					defs.ErrProxyConfigChanged, id, in.OldProxyID)
			}
			finalProxyID = id
		case UpdateBranchStopped:
			// S3 stopped: the child is only GUARDED (zero client delta), the
			// old row is updated IN PLACE (never rebuilt — the row id IS the
			// lease identity and every reservation owner must stay stable).
			if err := persistStoppedProxyRowInTx(tx, tenantID, userID, old, in); err != nil {
				return fmt.Errorf("commit update: %w", err)
			}
			finalProxyID = old.ID
		}
		// S5 backfill change_transaction.proxy_id (verifying CAS) — before
		// the reservation migration so the whole commit is one atomic unit
		if err := setTransactionProxyID(tx, tenantID, in.TxID, finalProxyID); err != nil {
			return fmt.Errorf("commit update: backfill transaction proxy id: %w", err)
		}
		// S6 reservation migration (write-first CAS per mode)
		if err := migrateReservationInTx(tx, tenantID, in, finalProxyID); err != nil {
			return fmt.Errorf("commit update: %w", err)
		}
		// S7 save the two encrypted AFTER snapshots only — before rows are
		// the coordinator's pre-apply job and are never written here. The
		// client snapshot binds to the child (the stopped branch never
		// touches the client, but the AFTER client state still belongs to
		// it — and in.Client is nil on that branch).
		clientNodeID := ""
		if in.Branch == UpdateBranchActive {
			clientNodeID = in.Client.ClientID
		} else {
			clientNodeID = in.StoppedUpdate.ChildClientID
		}
		if err := saveAfterSnapshotInTx(tx, tenantID, in.TxID, in.ClientAfterSnapshot,
			models.SnapshotNodeTypeClient, clientNodeID); err != nil {
			return fmt.Errorf("commit update: save client after snapshot: %w", err)
		}
		if err := saveAfterSnapshotInTx(tx, tenantID, in.TxID, in.ProxyAfterSnapshot,
			models.SnapshotNodeTypeProxy, strconv.FormatUint(uint64(finalProxyID), 10)); err != nil {
			return fmt.Errorf("commit update: save proxy after snapshot: %w", err)
		}
		// S8 audit (TxID/tenant/actor/EventType forced; summary redaction
		// was validated in S0)
		if err := saveUpdateAuditInTx(tx, tenantID, userID, in); err != nil {
			return fmt.Errorf("commit update: %w", err)
		}
		// S8.5 evidence (optional; the stopped branch writes its db-only
		// proof here so the evidence lands atomically with the commit)
		if in.EvidenceJSON != "" {
			if err := tx.Model(&models.ChangeTransaction{}).
				Where("transaction_id = ? AND tenant_id = ?", in.TxID, tenantID).
				Update("evidence_json", in.EvidenceJSON).Error; err != nil {
				return fmt.Errorf("commit update: write evidence: %w", err)
			}
		}
		// S9 tx -> committed (last step)
		return transitionTxStatus(tx, tenantID, in.TxID, models.TxStatusCommitted, "", "")
	})
	if err != nil {
		return 0, err
	}
	return finalProxyID, nil
}

// loadOldProxyRowInTx re-verifies the old-rule identity the plan resolved
// (S1) and returns the loaded row: the row must exist in tenant+user scope
// and carry the planned name/server. The branch then re-checks its row
// shape:
//   - active:       stopped=false; first-shadow rows live on the
//     never-shadowed origin (client_id==origin, origin_client_id empty),
//     existing-child rows on the planned child (client_id==child,
//     origin_client_id==origin)
//   - stopped:      stopped=true; client_id==StoppedUpdate.ChildClientID;
//     origin_client_id==OriginClientID (non-empty); a row that still lives
//     on the origin (client_id==origin or origin_client_id empty) is an
//     unsafe first-shadow state → ErrStoppedFirstShadow
//
// Both branches additionally require the row's REAL typed physical key
// (parsed from its content) to equal Reservation.OldKey — a commit must
// never keep or release a lock for a port the rule does not run on.
//
// A missing row is ErrProxyConfigNotFound; any divergence is
// ErrProxyConfigChanged (fail closed, whole commit rolls back).
func loadOldProxyRowInTx(tx *gorm.DB, tenantID, userID int, in *CommitUpdateInput) (*models.ProxyConfig, error) {
	var old models.ProxyConfig
	err := tx.Where("id = ? AND tenant_id = ? AND user_id = ?", in.OldProxyID, tenantID, userID).First(&old).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: proxy config row %d", defs.ErrProxyConfigNotFound, in.OldProxyID)
		}
		return nil, err
	}
	if old.ServerID != in.OldServerID || old.Name != in.OldName {
		return nil, fmt.Errorf("%w: row %d is %s/%s, plan said %s/%s",
			defs.ErrProxyConfigChanged, in.OldProxyID, old.ServerID, old.Name, in.OldServerID, in.OldName)
	}
	// P0 physical-key binding (OLD side): the row's REAL typed key must
	// equal the planned old reservation key. An unparseable row fails
	// closed too.
	oldKey, err := proxyRowPhysicalKey(&old)
	if err != nil {
		return nil, fmt.Errorf("%w: row %d physical key: %v", defs.ErrProxyConfigChanged, in.OldProxyID, err)
	}
	if oldKey != in.Reservation.OldKey {
		return nil, fmt.Errorf("%w: row %d physical key %s/%s/%d differs from the old reservation key %s/%s/%d",
			defs.ErrProxyConfigChanged, in.OldProxyID,
			oldKey.ServerID, oldKey.Protocol, oldKey.RemotePort,
			in.Reservation.OldKey.ServerID, in.Reservation.OldKey.Protocol, in.Reservation.OldKey.RemotePort)
	}
	switch in.Branch {
	case UpdateBranchActive:
		if old.Stopped {
			return nil, fmt.Errorf("%w: row %d is stopped; active updates never touch stopped rules", defs.ErrProxyConfigChanged, in.OldProxyID)
		}
		if in.Shadow.FirstShadow {
			if old.ClientID != in.OriginClientID || old.OriginClientID != "" {
				return nil, fmt.Errorf("%w: row %d is not on the never-shadowed origin (client_id=%s origin_client_id=%s)",
					defs.ErrProxyConfigChanged, in.OldProxyID, old.ClientID, old.OriginClientID)
			}
			return &old, nil
		}
		if old.ClientID != in.Client.ClientID || old.OriginClientID != in.OriginClientID {
			return nil, fmt.Errorf("%w: row %d is not on the planned child (client_id=%s origin_client_id=%s)",
				defs.ErrProxyConfigChanged, in.OldProxyID, old.ClientID, old.OriginClientID)
		}
		return &old, nil
	case UpdateBranchStopped:
		if !old.Stopped {
			return nil, fmt.Errorf("%w: row %d is active; stopped updates never touch active rules", defs.ErrProxyConfigChanged, in.OldProxyID)
		}
		// a rule that still lives on the unshadowed origin (client_id==origin
		// or no origin link at all) cannot be updated in place — converting
		// it would be a first-shadow the stopped branch is forbidden to do
		if old.ClientID == in.OriginClientID || old.OriginClientID == "" {
			return nil, fmt.Errorf("%w: row %d still lives on the unshadowed origin (client_id=%s origin_client_id=%s)",
				defs.ErrStoppedFirstShadow, in.OldProxyID, old.ClientID, old.OriginClientID)
		}
		if old.ClientID != in.StoppedUpdate.ChildClientID {
			return nil, fmt.Errorf("%w: row %d is on client %s, plan said child %s",
				defs.ErrProxyConfigChanged, in.OldProxyID, old.ClientID, in.StoppedUpdate.ChildClientID)
		}
		if old.OriginClientID != in.OriginClientID {
			return nil, fmt.Errorf("%w: row %d origin %q differs from the planned origin %q",
				defs.ErrProxyConfigChanged, in.OldProxyID, old.OriginClientID, in.OriginClientID)
		}
		return &old, nil
	}
	return nil, fmt.Errorf("%w: branch %q is not valid", defs.ErrInvalidCommitInput, in.Branch)
}

// guardRenameConflictInTx re-checks rename collisions inside the commit
// transaction (S2, TOCTOU guard): when the final name differs from the old
// name, ANY other row in the same origin scope — tenant+user+server AND
// (origin_client_id=origin OR client_id=origin), written as ONE SQL
// expression so the scope can never be lost — already carrying the final
// name fails the commit. The final name is branch-aware: the active branch
// rebuilds under ProxyName, the stopped branch renames in place under
// StoppedUpdate.NewName. GORM's chained .Or() must NOT be used here: it
// would escape the WHERE scope.
func guardRenameConflictInTx(tx *gorm.DB, tenantID, userID int, in *CommitUpdateInput) error {
	finalName := in.ProxyName
	if in.Branch == UpdateBranchStopped {
		finalName = in.StoppedUpdate.NewName
	}
	if finalName == in.OldName {
		return nil
	}
	var count int64
	err := tx.Model(&models.ProxyConfig{}).
		Where("tenant_id = ? AND user_id = ? AND server_id = ? AND name = ? AND (origin_client_id = ? OR client_id = ?) AND id != ?",
			tenantID, userID, in.OldServerID, finalName, in.OriginClientID, in.OriginClientID, in.OldProxyID).
		Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("%w: name %q already exists in the origin scope", defs.ErrProxyConfigNameConflict, finalName)
	}
	return nil
}

// saveAfterSnapshotInTx persists ONE encrypted AFTER snapshot row with the
// side/node-type/node-id FORCED by the commit (side=after; node_id is the
// child client id or the final proxy row id). Before snapshots are saved by
// the coordinator before apply and are never written here. A wrong-size key
// makes EncryptSnapshot fail INSIDE the transaction, rolling back the
// client-after row written moments earlier.
func saveAfterSnapshotInTx(tx *gorm.DB, tenantID int, txID string, in SnapshotInput, nodeType, nodeID string) error {
	ciphertext, nonce, sum, err := snapshot.EncryptSnapshot(in.Plaintext, in.Key)
	if err != nil {
		return err
	}
	snap := &models.ConfigSnapshot{
		TransactionID: txID,
		TenantID:      tenantID,
		Side:          models.SnapshotSideAfter,
		NodeType:      nodeType,
		NodeID:        nodeID,
		Ciphertext:    ciphertext,
		Nonce:         nonce,
		SHA256:        sum,
	}
	return tx.Create(snap).Error
}

// saveUpdateAuditInTx persists the commit audit event with TransactionID,
// TenantID, ActorUserID and EventType FORCED (spec §6.4); the summary
// redaction guard ran in S0.
func saveUpdateAuditInTx(tx *gorm.DB, tenantID, userID int, in *CommitUpdateInput) error {
	audit := in.AuditEvent
	audit.TransactionID = in.TxID
	audit.TenantID = tenantID
	audit.ActorUserID = userID
	audit.EventType = models.AuditEventTypeUpdate
	return tx.Create(audit).Error
}
