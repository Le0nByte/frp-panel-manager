package models

import (
	"errors"

	"gorm.io/gorm"
)

// ErrNilAuditEvent guards the append-only trail against nil inserts.
var ErrNilAuditEvent = errors.New("audit event is nil")

// AuditEventType* are the event kinds written by the transaction coordinator
// and the system recovery paths.
const (
	AuditEventTypeReservationExpired = "reservation_expired"
	// AuditEventTypeTransactionStale is written by the phase-B stale sweep
	// (Recovery A2) for transactions recovered without any port reservation.
	// It is a plain string column value, not a schema enum.
	AuditEventTypeTransactionStale = "transaction_stale"
	AuditEventTypePreflight        = "preflight"
	AuditEventTypeApply            = "apply"
	AuditEventTypeVerify           = "verify"
	AuditEventTypeCommit           = "commit"
	AuditEventTypeCompensation     = "compensation"
	AuditEventTypeCreate           = "create"
	// AuditEventTypeUpdate is written by the safe-update commit (A3):
	// one event per committed update, carrying only redacted evidence.
	AuditEventTypeUpdate = "update"
	// AuditEventTypeDelete/Start/Stop are the lifecycle operation kinds
	// written by the delete/start/stop commit paths (D2b B1).
	AuditEventTypeDelete = "delete"
	AuditEventTypeStart  = "start"
	AuditEventTypeStop   = "stop"
	// AuditEventTypeRollback is the event kind written by the manual
	// rollback path (D2c-C); the constant is part of the D2c-B contract
	// so the rollback coordinator can reference it without string literals.
	AuditEventTypeRollback = "rollback"
	// AuditEventTypeResetPassword is written by the local-only admin
	// reset-password command (spec §10.7.8). The summary is a fixed
	// marker-free constant; the row never carries a password or token.
	AuditEventTypeResetPassword = "admin.reset_password"
	// AuditEventTypeRPC* are the system-audit kinds written by the RPC
	// guard (I3-A) through AuditMutation.LogSystem. LogSystem ONLY accepts
	// the rpc.* prefix so ordinary authenticated flows can never forge a
	// system row (TenantID=0/ActorUserID=0).
	AuditEventTypeRPCSecretFailure        = "rpc.secret_failure"
	AuditEventTypeRPCHandshakeFailure     = "rpc.handshake_failure"
	AuditEventTypeRPCConnRateBlocked      = "rpc.conn_rate_blocked"
	AuditEventTypeRPCRegisterRateBlocked  = "rpc.register_rate_blocked"
	AuditEventTypeRPCAuditFloodSuppressed = "rpc.audit_flood_suppressed"
)

// AuditEvent is the append-only audit trail. It only stores redacted
// summaries, evidence references and results — never secrets or plaintext
// configs.
type AuditEvent struct {
	gorm.Model
	TransactionID string `gorm:"index;size:64"`
	TenantID      int    `gorm:"index"`
	EventType     string `gorm:"index;size:64;not null"`
	Summary       string `gorm:"type:text;not null"`
	NodeID        string `gorm:"size:128"`
	EvidenceRef   string `gorm:"size:256"`
	ErrorCode     string `gorm:"size:64"`
	ActorUserID   int    `gorm:"index"`
	Success       bool   `gorm:"index"`
}

func (*AuditEvent) TableName() string {
	return "audit_event"
}
