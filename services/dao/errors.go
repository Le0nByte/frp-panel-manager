package dao

import "errors"

// Typed secret-validation sentinels (I3-A). ValidateClientSecret and
// ValidateServerSecret return these for empty/mismatch inputs so callers
// branch with errors.Is instead of string matching; gorm.ErrRecordNotFound
// and other DB errors are passed through unchanged.
var (
	// ErrInvalidClientSecret marks a client ConnectSecret validation
	// failure: empty client id/secret or a secret that does not match the
	// stored row.
	ErrInvalidClientSecret = errors.New("invalid client secret")
	// ErrInvalidServerSecret marks a server ConnectSecret validation
	// failure: empty server id/secret or a secret that does not match the
	// stored row.
	ErrInvalidServerSecret = errors.New("invalid server secret")
	// ErrSystemAuditEventTypeForbidden marks a LogSystem call whose event
	// type does not carry the reserved rpc.* prefix. System rows are the
	// ONLY rows with TenantID=0/ActorUserID=0, so the prefix restriction
	// stops authenticated flows from forging them via LogSystem.
	ErrSystemAuditEventTypeForbidden = errors.New("system audit event type must use the rpc.* prefix")
	// ErrSystemAuditDBUnavailable marks a LogSystem call with no registered
	// default database (defensive; GetDefaultDB returns nil only when the
	// DB manager was never configured).
	ErrSystemAuditDBUnavailable = errors.New("system audit database unavailable")
)
