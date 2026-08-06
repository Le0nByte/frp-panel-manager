package models

import (
	"time"

	"gorm.io/gorm"
)

// change_transaction lifecycle statuses.
const (
	TxStatusPending        = "pending"
	TxStatusPreflight      = "preflight"
	TxStatusApplying       = "applying"
	TxStatusVerifying      = "verifying"
	TxStatusCommitted      = "committed"
	TxStatusRollingBack    = "rolling_back"
	TxStatusRolledBack     = "rolled_back"
	TxStatusFailed         = "failed"
	TxStatusRollbackFailed = "rollback_failed"
)

// change_transaction operations.
const (
	TxOperationCreate   = "create"
	TxOperationUpdate   = "update"
	TxOperationDelete   = "delete"
	TxOperationStart    = "start"
	TxOperationStop     = "stop"
	TxOperationRollback = "rollback"
)

// ChangeTransaction records the full lifecycle of one safe configuration
// change: preflight evidence, apply, verify, commit, compensation and rollback.
type ChangeTransaction struct {
	gorm.Model
	TransactionID   string     `gorm:"uniqueIndex:idx_tx_id;size:64;not null"`
	IdempotencyKey  string     `gorm:"uniqueIndex:idx_tx_idem;size:128;not null"`
	RequestHash     string     `gorm:"size:64;not null"` // SHA-256 of the canonical request (idempotency replay guard)
	ActorUserID     int        `gorm:"uniqueIndex:idx_tx_idem;index"`
	TenantID        int        `gorm:"uniqueIndex:idx_tx_idem;index"`
	Operation       string     `gorm:"size:32;not null"`
	ProxyID         *uint      `gorm:"index"`
	Status          string     `gorm:"index;size:32;not null"`
	RequestRedacted string     `gorm:"type:text"`
	EvidenceJSON    string     `gorm:"type:text"`
	ErrorCode       string     `gorm:"size:64"`
	ErrorMessage    string     `gorm:"type:text"`
	StartedAt       time.Time  `gorm:"index"`
	FinishedAt      *time.Time `gorm:"index"`
}

func (*ChangeTransaction) TableName() string {
	return "change_transaction"
}

// allowedTransitions maps each state to the set of states it may move to.
// committed/failed/rolled_back/rollback_failed are TERMINAL: no outgoing
// edges. A manual rollback of a committed transaction is a NEW transaction
// with operation=rollback (spec §6.2), never a transition of the committed
// row itself.
var allowedTransitions = map[string]map[string]bool{
	TxStatusPending: {
		TxStatusPreflight: true,
		TxStatusFailed:    true,
	},
	TxStatusPreflight: {
		TxStatusApplying: true,
		TxStatusFailed:   true,
	},
	TxStatusApplying: {
		TxStatusVerifying:      true,
		TxStatusFailed:         true,
		TxStatusRollingBack:    true,
		TxStatusRollbackFailed: true, // crash recovery ONLY (spec §6.1.3): the candidate config may already be applied, so the lease must be retained and the rule blocked for manual intervention
	},
	TxStatusVerifying: {
		TxStatusCommitted:      true,
		TxStatusFailed:         true,
		TxStatusRollingBack:    true,
		TxStatusRollbackFailed: true, // crash recovery ONLY: same rationale as applying
	},
	TxStatusRollingBack: {
		TxStatusRolledBack:     true,
		TxStatusRollbackFailed: true,
	},
	// terminal states (no outgoing edges): committed, failed, rolled_back, rollback_failed
}

// CanTransitionTo reports whether the transaction may move from its current
// status to target. Terminal states (committed/failed/rolled_back/
// rollback_failed) accept no further transitions.
func (t *ChangeTransaction) CanTransitionTo(target string) bool {
	from := t.Status
	allowed, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	return allowed[target]
}

// AllowedSourcesFor returns the set of source states that may transition to
// target (the inverse of allowedTransitions). The DAO encodes this set into
// its CAS UPDATE WHERE clause so the state machine is enforced atomically.
func AllowedSourcesFor(target string) map[string]bool {
	from := map[string]bool{}
	for src, targets := range allowedTransitions {
		if targets[target] {
			from[src] = true
		}
	}
	return from
}

// IsTerminalTxStatus reports whether status is a terminal state.
func IsTerminalTxStatus(status string) bool {
	switch status {
	case TxStatusCommitted, TxStatusFailed, TxStatusRolledBack, TxStatusRollbackFailed:
		return true
	}
	return false
}
