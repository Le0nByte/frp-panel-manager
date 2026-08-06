package conf

import (
	"os"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
)

// rpcGuardEnvVars lists every MASTER_RPC_GUARD_* knob (I3-C). The names
// are part of the deployment contract — the env template and runbook
// reference them verbatim.
var rpcGuardEnvVars = []string{
	"MASTER_RPC_GUARD_ENABLE",
	"MASTER_RPC_GUARD_MAX_CONNS_PER_IP_PER_MIN",
	"MASTER_RPC_GUARD_MAX_FAIL_CONNS_PER_IP_PER_MIN",
	"MASTER_RPC_GUARD_MAX_REGISTER_PER_IP_CLIENT_PER_MIN",
	"MASTER_RPC_GUARD_MAX_SECRET_FAILURES",
	"MASTER_RPC_GUARD_LOCKOUT_MINUTES",
	"MASTER_RPC_GUARD_AUDIT_FLUSH_SECONDS",
	"MASTER_RPC_GUARD_AUDIT_MAX_ROWS_PER_FLUSH",
}

func unsetRPCGuardEnvs(t *testing.T) {
	t.Helper()
	for _, k := range rpcGuardEnvVars {
		old, existed := os.LookupEnv(k)
		os.Unsetenv(k)
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(k, old)
			} else {
				_ = os.Unsetenv(k)
			}
		})
	}
}

// TestRPCGuardDefaults (I3-C): with every MASTER_RPC_GUARD_* env unset the
// guard must default to ENABLED with the I3-A production tuning — the
// public 9101 listener is rate-limited and audited out of the box.
func TestRPCGuardDefaults(t *testing.T) {
	unsetRPCGuardEnvs(t)

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}

	if !cfg.Master.RPCGuard.Enable {
		t.Error("MASTER_RPC_GUARD_ENABLE must default to true (fail-closed for the public listener)")
	}
	if cfg.Master.RPCGuard.MaxConnsPerIPPerMin != 60 {
		t.Errorf("MASTER_RPC_GUARD_MAX_CONNS_PER_IP_PER_MIN default = %d, want 60", cfg.Master.RPCGuard.MaxConnsPerIPPerMin)
	}
	if cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin != 20 {
		t.Errorf("MASTER_RPC_GUARD_MAX_FAIL_CONNS_PER_IP_PER_MIN default = %d, want 20", cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin)
	}
	if cfg.Master.RPCGuard.MaxRegisterPerIPClientPerMin != 30 {
		t.Errorf("MASTER_RPC_GUARD_MAX_REGISTER_PER_IP_CLIENT_PER_MIN default = %d, want 30", cfg.Master.RPCGuard.MaxRegisterPerIPClientPerMin)
	}
	if cfg.Master.RPCGuard.MaxSecretFailures != 5 {
		t.Errorf("MASTER_RPC_GUARD_MAX_SECRET_FAILURES default = %d, want 5", cfg.Master.RPCGuard.MaxSecretFailures)
	}
	if cfg.Master.RPCGuard.LockoutMinutes != 15 {
		t.Errorf("MASTER_RPC_GUARD_LOCKOUT_MINUTES default = %d, want 15", cfg.Master.RPCGuard.LockoutMinutes)
	}
	if cfg.Master.RPCGuard.AuditFlushSeconds != 60 {
		t.Errorf("MASTER_RPC_GUARD_AUDIT_FLUSH_SECONDS default = %d, want 60", cfg.Master.RPCGuard.AuditFlushSeconds)
	}
	if cfg.Master.RPCGuard.AuditMaxRowsPerFlush != 500 {
		t.Errorf("MASTER_RPC_GUARD_AUDIT_MAX_ROWS_PER_FLUSH default = %d, want 500", cfg.Master.RPCGuard.AuditMaxRowsPerFlush)
	}
}

// TestRPCGuardExplicitOverride (I3-C): operators must be able to raise
// every ceiling and tune the flush explicitly via the exact documented
// env names.
func TestRPCGuardExplicitOverride(t *testing.T) {
	unsetRPCGuardEnvs(t)

	os.Setenv("MASTER_RPC_GUARD_ENABLE", "false")
	os.Setenv("MASTER_RPC_GUARD_MAX_CONNS_PER_IP_PER_MIN", "120")
	os.Setenv("MASTER_RPC_GUARD_MAX_FAIL_CONNS_PER_IP_PER_MIN", "40")
	os.Setenv("MASTER_RPC_GUARD_MAX_REGISTER_PER_IP_CLIENT_PER_MIN", "60")
	os.Setenv("MASTER_RPC_GUARD_MAX_SECRET_FAILURES", "10")
	os.Setenv("MASTER_RPC_GUARD_LOCKOUT_MINUTES", "30")
	os.Setenv("MASTER_RPC_GUARD_AUDIT_FLUSH_SECONDS", "120")
	os.Setenv("MASTER_RPC_GUARD_AUDIT_MAX_ROWS_PER_FLUSH", "1000")
	t.Cleanup(func() {
		for _, k := range rpcGuardEnvVars {
			os.Unsetenv(k)
		}
	})

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}

	if cfg.Master.RPCGuard.Enable {
		t.Error("MASTER_RPC_GUARD_ENABLE=false must be readable (explicit disable)")
	}
	if cfg.Master.RPCGuard.MaxConnsPerIPPerMin != 120 {
		t.Errorf("MaxConnsPerIPPerMin = %d, want 120", cfg.Master.RPCGuard.MaxConnsPerIPPerMin)
	}
	if cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin != 40 {
		t.Errorf("MaxFailConnsPerIPPerMin = %d, want 40", cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin)
	}
	if cfg.Master.RPCGuard.MaxRegisterPerIPClientPerMin != 60 {
		t.Errorf("MaxRegisterPerIPClientPerMin = %d, want 60", cfg.Master.RPCGuard.MaxRegisterPerIPClientPerMin)
	}
	if cfg.Master.RPCGuard.MaxSecretFailures != 10 {
		t.Errorf("MaxSecretFailures = %d, want 10", cfg.Master.RPCGuard.MaxSecretFailures)
	}
	if cfg.Master.RPCGuard.LockoutMinutes != 30 {
		t.Errorf("LockoutMinutes = %d, want 30", cfg.Master.RPCGuard.LockoutMinutes)
	}
	if cfg.Master.RPCGuard.AuditFlushSeconds != 120 {
		t.Errorf("AuditFlushSeconds = %d, want 120", cfg.Master.RPCGuard.AuditFlushSeconds)
	}
	if cfg.Master.RPCGuard.AuditMaxRowsPerFlush != 1000 {
		t.Errorf("AuditMaxRowsPerFlush = %d, want 1000", cfg.Master.RPCGuard.AuditMaxRowsPerFlush)
	}
}

// TestRPCGuardZeroNegativeKeptForProviderFallback (I3-C): the config layer
// stores whatever the env says — a 0 or negative value must NOT silently
// become a default here. The rpcguard provider (rpcGuardConfigFromAppConfig)
// owns the fallback to safe defaults, so a mis-typed env degrades to the
// documented production tuning instead of disabling a ceiling.
func TestRPCGuardZeroNegativeKeptForProviderFallback(t *testing.T) {
	unsetRPCGuardEnvs(t)

	os.Setenv("MASTER_RPC_GUARD_MAX_CONNS_PER_IP_PER_MIN", "0")
	os.Setenv("MASTER_RPC_GUARD_MAX_FAIL_CONNS_PER_IP_PER_MIN", "-5")
	t.Cleanup(func() {
		os.Unsetenv("MASTER_RPC_GUARD_MAX_CONNS_PER_IP_PER_MIN")
		os.Unsetenv("MASTER_RPC_GUARD_MAX_FAIL_CONNS_PER_IP_PER_MIN")
	})

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}
	if cfg.Master.RPCGuard.MaxConnsPerIPPerMin != 0 {
		t.Errorf("MaxConnsPerIPPerMin = %d, want raw 0 (provider falls back)", cfg.Master.RPCGuard.MaxConnsPerIPPerMin)
	}
	if cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin != -5 {
		t.Errorf("MaxFailConnsPerIPPerMin = %d, want raw -5 (provider falls back)", cfg.Master.RPCGuard.MaxFailConnsPerIPPerMin)
	}
}
