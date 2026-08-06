package conf

import (
	"os"
	"strings"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
)

func TestSecurityDefaultsAndMasterSecretValidation(t *testing.T) {
	const (
		globalSecretEnv = "APP_GLOBAL_SECRET"
		cookieSecureEnv = "APP_COOKIE_SECURE"
	)
	for _, key := range []string{globalSecretEnv, cookieSecureEnv} {
		old, existed := os.LookupEnv(key)
		os.Unsetenv(key)
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}
	if cfg.App.GlobalSecret != "" {
		t.Fatalf("APP_GLOBAL_SECRET must have no predictable default, got non-empty value")
	}
	if !cfg.App.CookieSecure {
		t.Fatal("APP_COOKIE_SECURE must default to true")
	}

	for _, secret := range []string{"", "frp-panel", strings.Repeat("x", 31)} {
		cfg.App.GlobalSecret = secret
		if err := ValidateMasterSecurity(cfg); err == nil {
			t.Fatalf("ValidateMasterSecurity must reject a secret of length %d", len(secret))
		}
	}
	cfg.App.GlobalSecret = strings.Repeat("x", 32)
	if err := ValidateMasterSecurity(cfg); err != nil {
		t.Fatalf("ValidateMasterSecurity rejected an explicit 32-character secret: %v", err)
	}
}

func TestPrintStrRedactsSensitiveValues(t *testing.T) {
	const (
		sentinelGlobalSecret = "SENTINEL_GLOBAL_SECRET_1a2b3c"
		sentinelClientSecret = "SENTINEL_CLIENT_SECRET_4d5e6f"
		sentinelDSN          = "SENTINEL_DSN_7g8h9i"
		sentinelProxy        = "SENTINEL_HTTP_PROXY_j0k1l2"
	)

	cfg := Config{}
	cfg.App.GlobalSecret = sentinelGlobalSecret
	cfg.Client.Secret = sentinelClientSecret
	cfg.DB.DSN = sentinelDSN
	cfg.HTTP_PROXY = sentinelProxy

	// non-sensitive values that must survive redaction (spec §10.9 keeps
	// the log useful while masking credentials)
	cfg.Master.RPCHost = "127.0.0.1"
	cfg.Master.APIPort = 9000
	cfg.Client.ID = "keep-me-client-id"
	cfg.Client.RPCUrl = "grpc://127.0.0.1:9000"

	out := cfg.PrintStr()

	for _, leaked := range []string{
		sentinelGlobalSecret,
		sentinelClientSecret,
		sentinelDSN,
		sentinelProxy,
	} {
		if strings.Contains(out, leaked) {
			t.Errorf("PrintStr() output leaks sensitive value %q\noutput:\n%s", leaked, out)
		}
	}

	if !strings.Contains(out, "[REDACTED]") {
		t.Errorf("PrintStr() output has no [REDACTED] marker\noutput:\n%s", out)
	}

	for _, kept := range []string{"keep-me-client-id", "grpc://127.0.0.1:9000", "127.0.0.1", "9000"} {
		if !strings.Contains(out, kept) {
			t.Errorf("PrintStr() dropped non-sensitive value %q\noutput:\n%s", kept, out)
		}
	}
}

func TestRedactJSONMasksUnknownNesting(t *testing.T) {
	const (
		sentinelToken = "SENTINEL_TOKEN_deadbeef"
		sentinelPass  = "SENTINEL_PASSWORD_cafe"
	)
	raw := []byte(`{
		"server_id": "srv-1",
		"auth": {"token": "SENTINEL_TOKEN_deadbeef"},
		"plugin": {"nested": {"unknown": {"password": "SENTINEL_PASSWORD_cafe"}}},
		"keep_me": "visible-value",
		"ports": [{"port": 8080}]
	}`)

	redacted, err := RedactJSON(raw)
	if err != nil {
		t.Fatalf("RedactJSON() unexpected error: %v", err)
	}
	out := string(redacted)

	for _, leaked := range []string{sentinelToken, sentinelPass} {
		if strings.Contains(out, leaked) {
			t.Errorf("RedactJSON() leaks %q in output:\n%s", leaked, out)
		}
	}

	if strings.Count(out, "[REDACTED]") < 2 {
		t.Errorf("RedactJSON() expected >=2 [REDACTED] markers, got output:\n%s", out)
	}

	for _, kept := range []string{"srv-1", "visible-value", "8080"} {
		if !strings.Contains(out, kept) {
			t.Errorf("RedactJSON() dropped non-sensitive value %q in output:\n%s", kept, out)
		}
	}
}

// TestTLSInsecureSkipVerifyDefaultFalse pins spec §10.5: production defaults
// CLIENT_TLS_INSECURE_SKIP_VERIFY to false so the agent gRPC credentials
// verify the master's server TLS certificate. Only test environments may
// explicitly opt in to skipping verification.
func TestTLSInsecureSkipVerifyDefaultFalse(t *testing.T) {
	os.Unsetenv("CLIENT_TLS_INSECURE_SKIP_VERIFY")

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}
	if cfg.Client.TLSInsecureSkipVerify {
		t.Error("CLIENT_TLS_INSECURE_SKIP_VERIFY must default to false (spec §10.5)")
	}
}

// TestDangerousFeaturesDefaultFalse (S5a): every dangerous feature flag —
// the three independent master toggles (PTY / Worker / WireGuard, no single
// master switch) and the three client toggles (Functions / RemoteShell /
// WireGuard) — must default to false so the panel starts fail-closed. The
// master flags live under MASTER_FEATURES_ and the client flags under
// CLIENT_FEATURES_ (client WireGuard is reserved for S5b).
func TestDangerousFeaturesDefaultFalse(t *testing.T) {
	featureEnvVars := []string{
		"MASTER_FEATURES_ENABLE_PTY",
		"MASTER_FEATURES_ENABLE_WORKER",
		"MASTER_FEATURES_ENABLE_WIREGUARD",
		"CLIENT_FEATURES_ENABLE_FUNCTIONS",
		"CLIENT_FEATURES_ENABLE_REMOTE_SHELL",
		"CLIENT_FEATURES_ENABLE_WIREGUARD",
	}
	for _, k := range featureEnvVars {
		os.Unsetenv(k)
		t.Cleanup(func() { os.Unsetenv(k) })
	}

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}

	checks := []struct {
		name string
		got  bool
	}{
		{"MASTER_FEATURES_ENABLE_PTY", cfg.Master.Features.EnablePTY},
		{"MASTER_FEATURES_ENABLE_WORKER", cfg.Master.Features.EnableWorker},
		{"MASTER_FEATURES_ENABLE_WIREGUARD", cfg.Master.Features.EnableWireGuard},
		{"CLIENT_FEATURES_ENABLE_FUNCTIONS", cfg.Client.Features.EnableFunctions},
		{"CLIENT_FEATURES_ENABLE_REMOTE_SHELL", cfg.Client.Features.EnableRemoteShell},
		{"CLIENT_FEATURES_ENABLE_WIREGUARD", cfg.Client.Features.EnableWireGuard},
	}
	for _, c := range checks {
		if c.got {
			t.Errorf("%s must default to false (fail-closed)", c.name)
		}
	}
}

// TestDangerousFeaturesExplicitTrueReadable (S5a): the same flags must be
// readable when operators explicitly opt back in, per feature. A
// representative subset is enough: one master flag and one client flag.
func TestDangerousFeaturesExplicitTrueReadable(t *testing.T) {
	const (
		masterPTY = "MASTER_FEATURES_ENABLE_PTY"
		clientWG  = "CLIENT_FEATURES_ENABLE_WIREGUARD"
	)
	os.Setenv(masterPTY, "true")
	os.Setenv(clientWG, "true")
	t.Cleanup(func() {
		os.Unsetenv(masterPTY)
		os.Unsetenv(clientWG)
	})

	cfg := Config{}
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		t.Fatalf("cleanenv.ReadEnv: %v", err)
	}
	if !cfg.Master.Features.EnablePTY {
		t.Errorf("%s=true must be readable", masterPTY)
	}
	if !cfg.Client.Features.EnableWireGuard {
		t.Errorf("%s=true must be readable", clientWG)
	}
}
