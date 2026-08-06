package defs

import "errors"

// Structured error codes exposed by the port-manager API (spec §7.3).
// Frontends must render the specific node/protocol/port/owner from evidence
// rather than a generic "save failed".
const (
	ErrorCodePortRemoteInUse        = "PORT_REMOTE_IN_USE"
	ErrorCodePortLocalNotListening  = "PORT_LOCAL_NOT_LISTENING"
	ErrorCodeConfigValidationFailed = "CONFIG_VALIDATION_FAILED"
	ErrorCodePortScanUnavailable    = "PORT_SCAN_UNAVAILABLE"
	ErrorCodeAgentOffline           = "AGENT_OFFLINE"
	ErrorCodeReservationConflict    = "RESERVATION_CONFLICT"
	ErrorCodeReservationExpired     = "RESERVATION_EXPIRED"
	ErrorCodeUnsupportedProxyType   = "UNSUPPORTED_PROXY_TYPE"
	ErrorCodeApplyTimeout           = "APPLY_TIMEOUT"
	// ErrorCodeApplyRejected distinguishes an agent EXPLICIT rejection (or
	// an unparseable ACK) from APPLY_TIMEOUT — a rejection is a real
	// answer, not a missing one, and must never be disguised as a timeout.
	ErrorCodeApplyRejected  = "APPLY_REJECTED"
	ErrorCodeVerifyFailed   = "VERIFY_FAILED"
	ErrorCodeRollbackFailed = "ROLLBACK_FAILED"
	// ErrorCodeCommitFailed is the stable code for a safe-update COMMIT
	// whose internal DB transaction failed (spec §8.2: the config IS
	// applied and verified, so the transaction compensates and reports
	// this code — never APPLY_REJECTED/VERIFY_FAILED, which would lie
	// about the agent side).
	ErrorCodeCommitFailed        = "CONFIG_COMMIT_FAILED"
	ErrorCodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	ErrorCodeIdempotencyInFlight = "IDEMPOTENCY_IN_FLIGHT"
	// ErrorCodeReadOnly is the stable code returned by every write entry
	// (HTTP gate, managed proxy entries and legacy writers) while the
	// master is blocked in read-only mode because the snapshot key
	// fingerprint is missing or mismatched (spec §6.3.2). Frontends must
	// render it as "master is read-only" — a retry cannot succeed until
	// the operator restores the key file.
	ErrorCodeReadOnly = "READ_ONLY"
)

// Sentinel errors for port reservation lifecycle.
var (
	// ErrReservationConflict is returned when the unique key
	// (server_id, protocol, remote_port) is already held by another
	// pending or committed reservation.
	ErrReservationConflict = errors.New("port reservation conflict")
	// ErrReservationNotFound is returned when releasing/committing a
	// reservation that does not exist (already released or never acquired).
	ErrReservationNotFound = errors.New("port reservation not found")
	// ErrTransactionNotFound is returned when a change transaction lookup
	// misses (including cross-tenant lookups, which must never leak rows).
	ErrTransactionNotFound = errors.New("change transaction not found")
	// ErrUnsupportedProxyType is returned when a proxy type other than
	// tcp/udp is submitted for a port-managed rule (spec §7.3).
	ErrUnsupportedProxyType = errors.New("unsupported proxy type")
	// ErrNilUserInfo is returned by port-manager DAO methods when the
	// caller passes a nil user (tenant scoping is mandatory).
	ErrNilUserInfo = errors.New("user info is required for tenant scoping")
	// ErrInvalidTxTransition is the stable sentinel for illegal change
	// transaction status transitions (gate 3).
	ErrInvalidTxTransition = errors.New("invalid transaction status transition")
	// ErrTxStatusConflict is returned when the CAS status update misses
	// (the row changed between load and update: concurrent coordinator).
	ErrTxStatusConflict = errors.New("transaction status changed concurrently")
	// ErrIdempotencyConflict is returned when the same idempotency key is
	// replayed with a DIFFERENT request body (gate 7).
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrIdempotencyInFlight is returned when the same idempotency key is
	// replayed while the original transaction is still running.
	ErrIdempotencyInFlight = errors.New("request with this idempotency key is already in progress")
	// ErrClientStopped is returned when the target client is stopped and
	// cannot receive config updates.
	ErrClientStopped = errors.New("client is stopped")
	// ErrClientNotFound is returned when a tenant-scoped client row is
	// missing at commit time (concurrently deleted between plan and
	// commit) — the stable sentinel for a 0-row guarded UPDATE.
	ErrClientNotFound = errors.New("client not found")
	// ErrAmbiguousProxyTarget is returned when a legacy-or-shadow old-rule
	// lookup matches MORE than one proxy_config row for the same
	// (server_id, name) — the caller must fail closed rather than guess
	// which row is the real old rule.
	ErrAmbiguousProxyTarget = errors.New("ambiguous proxy target: more than one matching row")
	// ErrProxyConfigNotFound is returned when the old proxy_config row of a
	// safe update is missing at commit time (concurrently deleted between
	// plan and commit).
	ErrProxyConfigNotFound = errors.New("proxy config not found")
	// ErrProxyConfigChanged is returned when the old proxy_config row no
	// longer matches the plan (name/server/stopped/shape diverged) or when
	// the commit rebuild resolved a different row id than the plan's old id.
	ErrProxyConfigChanged = errors.New("proxy config changed concurrently")
	// ErrProxyConfigNameConflict is returned when the rename target name is
	// already taken by another row in the same origin scope at commit time
	// (the TOCTOU re-check).
	ErrProxyConfigNameConflict = errors.New("proxy config name conflict")
	// ErrInvalidCommitInput is returned when a commit payload violates the
	// static shape contract (branch/mode/key geometry, missing payloads,
	// snapshot material) before any DB write.
	ErrInvalidCommitInput = errors.New("invalid commit input")
	// ErrStoppedFirstShadow is returned when a first-shadow conversion cannot
	// reconcile stopped rows safely: the stopped-update branch may not create
	// a child at all, and an active conversion must reject stopped rows owned
	// by another server rather than strand them on the shadow origin or attach
	// them to the wrong child.
	ErrStoppedFirstShadow = errors.New("unsafe stopped first-shadow state")
)
