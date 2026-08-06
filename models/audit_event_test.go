package models_test

import (
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestAuditEventModel(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(uniqueDSN(t)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.AuditEvent{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	ev := &models.AuditEvent{
		TransactionID: "tx-1",
		EventType:     "proxy.create",
		Summary:       "create proxy tcp 8000 -> 127.0.0.1:22 [REDACTED]",
		NodeID:        "aliyun-1",
		ErrorCode:     "",
		Success:       true,
	}
	require.NoError(t, db.Create(ev).Error)

	var loaded models.AuditEvent
	require.NoError(t, db.First(&loaded, "transaction_id = ?", "tx-1").Error)
	assert.Equal(t, "proxy.create", loaded.EventType)
	assert.True(t, loaded.Success)
	assert.Equal(t, "audit_event", (&models.AuditEvent{}).TableName())
}

// TestAuditEventTypeRollback pins the rollback event kind constant
// (D2c-B contract; the rollback handler that writes it lands in D2c-C).
func TestAuditEventTypeRollback(t *testing.T) {
	if models.AuditEventTypeRollback != "rollback" {
		t.Fatalf("AuditEventTypeRollback = %q, want rollback", models.AuditEventTypeRollback)
	}
}

// TestAuditEventRPCTypeConstants pins the I3-A system-audit event kinds.
// These are the ONLY event types AuditMutation.LogSystem accepts (rpc.*
// prefix); the strings are part of the guard→audit contract, so drift is a
// contract break rather than a cosmetic change.
func TestAuditEventRPCTypeConstants(t *testing.T) {
	want := map[string]string{
		"AuditEventTypeRPCSecretFailure":        "rpc.secret_failure",
		"AuditEventTypeRPCHandshakeFailure":     "rpc.handshake_failure",
		"AuditEventTypeRPCConnRateBlocked":      "rpc.conn_rate_blocked",
		"AuditEventTypeRPCRegisterRateBlocked":  "rpc.register_rate_blocked",
		"AuditEventTypeRPCAuditFloodSuppressed": "rpc.audit_flood_suppressed",
	}
	got := map[string]string{
		"AuditEventTypeRPCSecretFailure":        models.AuditEventTypeRPCSecretFailure,
		"AuditEventTypeRPCHandshakeFailure":     models.AuditEventTypeRPCHandshakeFailure,
		"AuditEventTypeRPCConnRateBlocked":      models.AuditEventTypeRPCConnRateBlocked,
		"AuditEventTypeRPCRegisterRateBlocked":  models.AuditEventTypeRPCRegisterRateBlocked,
		"AuditEventTypeRPCAuditFloodSuppressed": models.AuditEventTypeRPCAuditFloodSuppressed,
	}
	for name, wantVal := range want {
		if got[name] != wantVal {
			t.Errorf("%s = %q, want %q", name, got[name], wantVal)
		}
	}
}
