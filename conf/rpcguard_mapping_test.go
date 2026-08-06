package conf

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/stretchr/testify/require"
)

// TestRPCGuardConfigFromAppConfigDefaults (I3-C): a zero config block maps
// to the I3-A production tuning and ENABLED — the public 9101 listener is
// rate-limited and audited out of the box.
func TestRPCGuardConfigFromAppConfigDefaults(t *testing.T) {
	cfg, enabled := rpcGuardConfigFromAppConfig(Config{})
	require.True(t, enabled, "the guard must default to enabled (fail-closed)")

	d := rpcguard.DefaultConfig()
	require.Equal(t, d.ConnectionRatePerMin, cfg.ConnectionRatePerMin)
	require.Equal(t, time.Minute, cfg.ConnectionWindow)
	require.Equal(t, d.HandshakeFailureThreshold, cfg.HandshakeFailureThreshold)
	require.Equal(t, time.Minute, cfg.HandshakeFailureWindow)
	require.Equal(t, d.IPLockDuration, cfg.IPLockDuration)
	require.Equal(t, d.RegisterRatePerMin, cfg.RegisterRatePerMin)
	require.Equal(t, time.Minute, cfg.RegisterWindow)
	require.Equal(t, d.SecretFailureThreshold, cfg.SecretFailureThreshold)
	require.Equal(t, d.SecretFailureWindow, cfg.SecretFailureWindow)
	require.Equal(t, d.SecretKeyLockDuration, cfg.SecretKeyLockDuration)
	require.Equal(t, d.AuditFlushInterval, cfg.AuditFlushInterval)
	require.Equal(t, d.MaxAuditRowsPerFlush, cfg.MaxAuditRowsPerFlush)
}

// TestRPCGuardConfigFromAppConfigOverride (I3-C): every documented env knob
// must map onto exactly the rpcguard field it tunes.
func TestRPCGuardConfigFromAppConfigOverride(t *testing.T) {
	cfg := Config{}
	cfg.Master.RPCGuard.Enable = false
	cfg.Master.RPCGuard.MaxConnsPerIPPerMin = 120
	cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin = 40
	cfg.Master.RPCGuard.MaxRegisterPerIPClientPerMin = 60
	cfg.Master.RPCGuard.MaxSecretFailures = 10
	cfg.Master.RPCGuard.LockoutMinutes = 30
	cfg.Master.RPCGuard.AuditFlushSeconds = 120
	cfg.Master.RPCGuard.AuditMaxRowsPerFlush = 1000

	gc, enabled := rpcGuardConfigFromAppConfig(cfg)
	require.False(t, enabled)
	require.Equal(t, 120, gc.ConnectionRatePerMin)
	require.Equal(t, 40, gc.HandshakeFailureThreshold)
	require.Equal(t, 60, gc.RegisterRatePerMin)
	require.Equal(t, 10, gc.SecretFailureThreshold)
	require.Equal(t, 30*time.Minute, gc.IPLockDuration)
	require.Equal(t, 30*time.Minute, gc.SecretFailureWindow)
	require.Equal(t, 30*time.Minute, gc.SecretKeyLockDuration)
	require.Equal(t, 120*time.Second, gc.AuditFlushInterval)
	require.Equal(t, 1000, gc.MaxAuditRowsPerFlush)
}

// TestRPCGuardConfigFromAppConfigFallback (I3-C): a zero/negative knob is
// mapped raw (0/-5 survive the mapping) and the GUARD falls back to the
// safe production ceiling — a mis-typed env degrades to documented tuning,
// never to "no ceiling".
func TestRPCGuardConfigFromAppConfigFallback(t *testing.T) {
	cfg := Config{}
	cfg.Master.RPCGuard.MaxConnsPerIPPerMin = 0
	cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin = -5

	gc, enabled := rpcGuardConfigFromAppConfig(cfg)
	require.True(t, enabled)
	require.Equal(t, rpcguard.DefaultConfig().ConnectionRatePerMin, gc.ConnectionRatePerMin, "zero env knob must map to the safe default")
	require.Equal(t, rpcguard.DefaultConfig().HandshakeFailureThreshold, gc.HandshakeFailureThreshold, "negative env knob must map to the safe default")

	// The guard normalizes non-positive fields back to production defaults.
	g := rpcguard.NewGuard(rpcguard.WithConfig(gc))
	peer := "203.0.113.60:7000"
	for i := 0; i < rpcguard.DefaultConnectionRatePerMin; i++ {
		require.True(t, g.AllowConnection(peer), "attempt %d must be allowed", i+1)
	}
	require.False(t, g.AllowConnection(peer),
		"a zero env knob must fall back to the default ceiling, not disable it")

	// The -5 handshake threshold must also normalize to the default 20.
	for i := 0; i < rpcguard.DefaultHandshakeFailureThreshold; i++ {
		g.RecordHandshakeFailureLayer(peer, rpcguard.LayerPreTLS)
	}
	require.False(t, g.AllowConnection(peer),
		"the IP must be locked after the default handshake-failure threshold")
}

// TestRPCGuardConfigFromAppConfigDisabled (I3-C): the enable flag is the
// guard's master switch — false must be carried through so the provider can
// call WithEnabled(false) and keep the wiring in place while the guard does
// nothing.
func TestRPCGuardConfigFromAppConfigDisabled(t *testing.T) {
	cfg := Config{}
	cfg.Master.RPCGuard.Enable = false
	cfg.Master.RPCGuard.MaxConnsPerIPPerMin = 999

	_, enabled := rpcGuardConfigFromAppConfig(cfg)
	require.False(t, enabled)
}
