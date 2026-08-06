package conf

import (
	"time"

	"github.com/VaalaCat/frp-panel/services/rpcguard"
)

// rpcGuardConfigFromAppConfig maps the MASTER_RPC_GUARD_* config block
// (I3-C) onto the rpcguard tuning and returns the enable flag. Every
// duration is derived from the documented integer knobs:
//
//   - connection ceiling/window  → MaxConnsPerIPPerMin / 1 minute
//   - handshake threshold/window → MaxFailConnsPerIPPerMin / 1 minute
//   - IP lock duration           → LockoutMinutes
//   - register ceiling/window    → MaxRegisterPerIPClientPerMin / 1 minute
//   - secret threshold/window    → MaxSecretFailures / LockoutMinutes
//   - secret key lock duration   → LockoutMinutes
//   - audit flush interval/cap   → AuditFlushSeconds / AuditMaxRowsPerFlush
//
// Knobs without an env (DistinctSecretFailThreshold, MaxTrackedEntries,
// the fixed 1-minute connection/handshake/register windows) are left zero:
// rpcguard.NewGuard's normalizeConfig fills every non-positive field from
// the production defaults, so a mis-typed env degrades to documented
// tuning instead of disabling a ceiling. The raw values are deliberately
// NOT sanitized here — the fallback lives in one place, the guard.
func rpcGuardConfigFromAppConfig(cfg Config) (rpcguard.Config, bool) {
	g := cfg.Master.RPCGuard
	// Config is a value type, so its bool zero value cannot distinguish an
	// omitted block from an explicit false. Treat false as an explicit disable
	// only when the operator supplied a meaningful positive tuning value; an
	// all-zero or invalid-only block keeps the fail-closed default enabled.
	enabled := g.Enable
	if !g.Enable && g.MaxConnsPerIPPerMin <= 0 && g.MaxFailConnsPerIPPerMin <= 0 &&
		g.MaxRegisterPerIPClientPerMin <= 0 && g.MaxSecretFailures <= 0 &&
		g.LockoutMinutes <= 0 && g.AuditFlushSeconds <= 0 && g.AuditMaxRowsPerFlush <= 0 {
		enabled = true
	}
	d := rpcguard.DefaultConfig()
	connectionRate := g.MaxConnsPerIPPerMin
	if connectionRate <= 0 {
		connectionRate = d.ConnectionRatePerMin
	}
	handshakeRate := g.MaxFailConnsPerIPPerMin
	if handshakeRate <= 0 {
		handshakeRate = d.HandshakeFailureThreshold
	}
	registerRate := g.MaxRegisterPerIPClientPerMin
	if registerRate <= 0 {
		registerRate = d.RegisterRatePerMin
	}
	secretFailures := g.MaxSecretFailures
	if secretFailures <= 0 {
		secretFailures = d.SecretFailureThreshold
	}
	lockout := g.LockoutMinutes
	if lockout <= 0 {
		lockout = int(d.IPLockDuration / time.Minute)
	}
	flushSeconds := g.AuditFlushSeconds
	if flushSeconds <= 0 {
		flushSeconds = int(d.AuditFlushInterval / time.Second)
	}
	maxRows := g.AuditMaxRowsPerFlush
	if maxRows <= 0 {
		maxRows = d.MaxAuditRowsPerFlush
	}
	return rpcguard.Config{
		ConnectionRatePerMin:      connectionRate,
		ConnectionWindow:          time.Minute,
		HandshakeFailureThreshold: handshakeRate,
		HandshakeFailureWindow:    time.Minute,
		IPLockDuration:            time.Duration(lockout) * time.Minute,
		RegisterRatePerMin:        registerRate,
		RegisterWindow:            time.Minute,
		SecretFailureThreshold:    secretFailures,
		SecretFailureWindow:       time.Duration(lockout) * time.Minute,
		SecretKeyLockDuration:     time.Duration(lockout) * time.Minute,
		AuditFlushInterval:        time.Duration(flushSeconds) * time.Second,
		MaxAuditRowsPerFlush:      maxRows,
	}, enabled
}

// RPCGuardConfig exposes the normalized guard configuration to the runtime
// provider while keeping the mapping implementation private to conf.
func RPCGuardConfig(cfg Config) (rpcguard.Config, bool) {
	return rpcGuardConfigFromAppConfig(cfg)
}
