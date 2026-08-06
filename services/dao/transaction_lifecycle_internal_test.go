package dao

import (
	"fmt"
	"strings"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestClassifyLifecycleTxStateAcceptsValidLockedRow(t *testing.T) {
	proxyID := uint(42)
	row := &models.ChangeTransaction{
		TransactionID: "tx-lifecycle-valid",
		Operation:     models.TxOperationStart,
		Status:        models.TxStatusVerifying,
		ProxyID:       &proxyID,
	}
	in := &CommitLifecycleInput{
		TxID:    row.TransactionID,
		Op:      LifecycleOpStart,
		ProxyID: proxyID,
	}
	stateErr := func(detail string) error {
		return fmt.Errorf("%w: %s", defs.ErrInvalidTxTransition, detail)
	}

	if err := classifyLifecycleTxState(stateErr, row, in); err != nil {
		t.Fatalf("valid FOR UPDATE row rejected: %v", err)
	}
}

func TestLifecycleThinWrappersDoNotMutateCallerInput(t *testing.T) {
	m := &transactionMutation{}
	tests := []struct {
		name string
		call func(*CommitLifecycleInput)
	}{
		{"delete", func(in *CommitLifecycleInput) { _, _ = m.CommitDelete(nil, in) }},
		{"start", func(in *CommitLifecycleInput) { _, _ = m.CommitStart(nil, in) }},
		{"stop", func(in *CommitLifecycleInput) { _, _ = m.CommitStop(nil, in) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := &CommitLifecycleInput{Op: LifecycleOp("caller-owned-value")}
			tc.call(in)
			if in.Op != LifecycleOp("caller-owned-value") {
				t.Fatalf("wrapper mutated caller input op to %q", in.Op)
			}
		})
	}
}

func TestLifecycleRowGuardUsesForUpdateOutsideSQLite(t *testing.T) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  "host=127.0.0.1 user=test dbname=test sslmode=disable",
		PreferSimpleProtocol: true,
	}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("open dry-run postgres dialector: %v", err)
	}
	var row models.ProxyConfig
	stmt := lifecycleRowGuardQuery(db).
		Where("id = ?", 1).
		First(&row).Statement
	if sql := stmt.SQL.String(); !strings.Contains(strings.ToUpper(sql), "FOR UPDATE") {
		t.Fatalf("non-SQLite lifecycle row guard lacks FOR UPDATE: %s", sql)
	}
}
