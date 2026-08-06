package models_test

import (
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/stretchr/testify/assert"
)

func TestTransactionStateMachine(t *testing.T) {
	cases := []struct {
		name   string
		from   string
		to     string
		expect bool
	}{
		// happy path
		{"pending->preflight", models.TxStatusPending, models.TxStatusPreflight, true},
		{"preflight->applying", models.TxStatusPreflight, models.TxStatusApplying, true},
		{"applying->verifying", models.TxStatusApplying, models.TxStatusVerifying, true},
		{"verifying->committed", models.TxStatusVerifying, models.TxStatusCommitted, true},
		// failures before any config apply
		{"pending->failed", models.TxStatusPending, models.TxStatusFailed, true},
		{"preflight->failed", models.TxStatusPreflight, models.TxStatusFailed, true},
		// compensation after apply
		{"applying->rolling_back", models.TxStatusApplying, models.TxStatusRollingBack, true},
		{"verifying->rolling_back", models.TxStatusVerifying, models.TxStatusRollingBack, true},
		{"rolling_back->rolled_back", models.TxStatusRollingBack, models.TxStatusRolledBack, true},
		{"rolling_back->rollback_failed", models.TxStatusRollingBack, models.TxStatusRollbackFailed, true},
		// committed is TERMINAL: manual rollback is a NEW transaction with
		// operation=rollback, never a transition of the committed row.
		{"committed->rolling_back rejected", models.TxStatusCommitted, models.TxStatusRollingBack, false},
		// invalid transitions
		{"pending->committed skip", models.TxStatusPending, models.TxStatusCommitted, false},
		{"preflight->committed skip", models.TxStatusPreflight, models.TxStatusCommitted, false},
		{"committed->failed", models.TxStatusCommitted, models.TxStatusFailed, false},
		{"failed->rolled_back", models.TxStatusFailed, models.TxStatusRolledBack, false},
		{"rolled_back->rolling_back", models.TxStatusRolledBack, models.TxStatusRollingBack, false},
		{"rollback_failed->rolled_back", models.TxStatusRollbackFailed, models.TxStatusRolledBack, false},
		{"committed->committed", models.TxStatusCommitted, models.TxStatusCommitted, false},
		{"unknown->pending", "bogus", models.TxStatusPending, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &models.ChangeTransaction{Status: tc.from}
			assert.Equal(t, tc.expect, tx.CanTransitionTo(tc.to), "from %s to %s", tc.from, tc.to)
		})
	}
}

func TestTransactionConstants(t *testing.T) {
	assert.Equal(t, "create", models.TxOperationCreate)
	assert.Equal(t, "rollback", models.TxOperationRollback)
	assert.Equal(t, "rollback_failed", models.TxStatusRollbackFailed)
	assert.Equal(t, "change_transaction", (&models.ChangeTransaction{}).TableName())
}

// TestCrashRecoveryTransitionsToRollbackFailed pins the crash-recovery
// edges (spec §6.1.3 + P0-2): a transaction that crashed while the
// candidate config may already have been applied (applying/verifying) or
// while compensation was running (rolling_back) must be movable to
// rollback_failed so the system recovery can block the rule for manual
// intervention WITHOUT ever releasing its lease. States before the apply
// point (pending/preflight) must NOT jump to rollback_failed — they fail
// cleanly instead.
func TestCrashRecoveryTransitionsToRollbackFailed(t *testing.T) {
	cases := []struct {
		name   string
		from   string
		expect bool
	}{
		{"applying->rollback_failed (crash recovery)", models.TxStatusApplying, true},
		{"verifying->rollback_failed (crash recovery)", models.TxStatusVerifying, true},
		{"rolling_back->rollback_failed", models.TxStatusRollingBack, true},
		{"pending->rollback_failed rejected", models.TxStatusPending, false},
		{"preflight->rollback_failed rejected", models.TxStatusPreflight, false},
		{"failed->rollback_failed rejected (terminal)", models.TxStatusFailed, false},
		{"rolled_back->rollback_failed rejected (terminal)", models.TxStatusRolledBack, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &models.ChangeTransaction{Status: tc.from}
			assert.Equal(t, tc.expect, tx.CanTransitionTo(models.TxStatusRollbackFailed), "from %s", tc.from)
		})
	}
}
