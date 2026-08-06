package conf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/gin-gonic/gin"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
	"github.com/tidwall/pretty"
)

type Config struct {
	App struct {
		UseGvisorNet   bool   `env:"USE_GVISOR_NET" env-default:"false" env-description:"use gvisor netstack for TUN device"`
		GlobalSecret   string `env:"GLOBAL_SECRET" env-default:"" env-description:"global secret, must be explicitly set to at least 32 characters"`
		CookieAge      int    `env:"COOKIE_AGE" env-default:"86400" env-description:"cookie age in second, default is 1 day"`
		CookieName     string `env:"COOKIE_NAME" env-default:"frp-panel-cookie" env-description:"cookie name"`
		CookiePath     string `env:"COOKIE_PATH" env-default:"/" env-description:"cookie path"`
		CookieDomain   string `env:"COOKIE_DOMAIN" env-default:"" env-description:"cookie domain"`
		CookieSecure   bool   `env:"COOKIE_SECURE" env-default:"true" env-description:"cookie secure"`
		CookieHTTPOnly bool   `env:"COOKIE_HTTP_ONLY" env-default:"true" env-description:"cookie http only"`
		EnableRegister bool   `env:"ENABLE_REGISTER" env-default:"false" env-description:"enable register, only allow the first admin to register"`
		GithubProxyUrl string `env:"GITHUB_PROXY_URL" env-default:"https://ghfast.top/" env-description:"github proxy url"`
	} `env-prefix:"APP_"`
	Master struct {
		APIPort               int    `env:"API_PORT" env-default:"9100" env-description:"master api port"`
		APIHost               string `env:"API_HOST" env-description:"master host, can behind proxy like cdn"`
		APIScheme             string `env:"API_SCHEME" env-default:"http" env-description:"master api scheme"`
		CacheSize             int    `env:"CACHE_SIZE" env-default:"10" env-description:"cache size in MB"`
		RPCHost               string `env:"RPC_HOST" env-default:"127.0.0.1" env-description:"master host, is a public ip or domain"`
		RPCPort               int    `env:"RPC_PORT" env-default:"9101" env-description:"master rpc port"`
		InternalFRPServerHost string `env:"INTERNAL_FRP_SERVER_HOST" env-description:"internal frp server host, used for client connection"`
		// SnapshotKeyFile is the root-only secret file (0600) holding the
		// 32-byte AES-256 key used to encrypt config snapshots (spec §6.3).
		// Key material is never read into logs or the database; only its
		// fingerprint is stored.
		SnapshotKeyFile string `env:"SNAPSHOT_KEY_FILE" env-default:"/opt/frp-panel/secrets/snapshot.key" env-description:"path of the root-only snapshot encryption key file"`
		// Features are independent master-side toggles for the dangerous
		// surfaces (S5a). Each defaults to false (fail-closed): routes are
		// only registered while on, and a request-time gate 403s on a
		// runtime flip. Three separate switches, not one master switch.
		Features struct {
			EnablePTY       bool `env:"ENABLE_PTY" env-default:"false" env-description:"enable pty remote shell (dangerous, default off)"`
			EnableWorker    bool `env:"ENABLE_WORKER" env-default:"false" env-description:"enable worker management (dangerous, default off)"`
			EnableWireGuard bool `env:"ENABLE_WIREGUARD" env-default:"false" env-description:"enable wireguard management (dangerous, default off)"`
		} `env-prefix:"FEATURES_" env-description:"master feature toggles"`
		// RPCGuard tunes the I3-A/I3-B admission guard on the PUBLIC 9101
		// mux (spec §10.5): per-IP connection rate, TLS handshake failures,
		// register throttling, secret-failure key locks and the bounded
		// system audit flush. All knobs are code-defaulted here and the
		// env names below are the deployment contract (env template +
		// runbook reference them verbatim). Zero/negative ceilings are
		// NOT sanitized at this layer: the rpcguard provider falls back
		// to the safe production defaults, so a mis-typed env degrades
		// to documented tuning instead of disabling a ceiling.
		RPCGuard struct {
			Enable                       bool `env:"ENABLE" env-default:"true" env-description:"enable rpc guard rate limiting and audit on the public 9101 listener"`
			MaxConnsPerIPPerMin          int  `env:"MAX_CONNS_PER_IP_PER_MIN" env-default:"60" env-description:"max accepted connections per IP per minute"`
			MaxFailConnsPerIPPerMin      int  `env:"MAX_FAIL_CONNS_PER_IP_PER_MIN" env-default:"20" env-description:"max TLS/pre-TLS failed connections per IP per minute before IP lockout"`
			MaxRegisterPerIPClientPerMin int  `env:"MAX_REGISTER_PER_IP_CLIENT_PER_MIN" env-default:"30" env-description:"max register attempts per (IP, client) per minute"`
			MaxSecretFailures            int  `env:"MAX_SECRET_FAILURES" env-default:"5" env-description:"max ConnectSecret failures per (IP, client) in the lockout window before key lockout"`
			LockoutMinutes               int  `env:"LOCKOUT_MINUTES" env-default:"15" env-description:"lockout window/duration in minutes for secret-failure and IP locks"`
			AuditFlushSeconds            int  `env:"AUDIT_FLUSH_SECONDS" env-default:"60" env-description:"system audit flush interval in seconds"`
			AuditMaxRowsPerFlush         int  `env:"AUDIT_MAX_ROWS_PER_FLUSH" env-default:"500" env-description:"max aggregated system audit rows per flush"`
		} `env-prefix:"RPC_GUARD_" env-description:"rpc guard rate limits and audit tuning"`
	} `env-prefix:"MASTER_"`
	Server struct {
		APIPort int `env:"API_PORT" env-default:"8999" env-description:"server api port"`
	} `env-prefix:"SERVER_"`
	DB struct {
		Type string `env:"TYPE" env-default:"sqlite3" env-description:"db type, mysql or sqlite3 and so on"`
		DSN  string `env:"DSN" env-default:"/data/data.db?_pragma=journal_mode(WAL)" env-description:"db dsn, for sqlite is path, other is dsn, look at https://github.com/go-sql-driver/mysql#dsn-data-source-name"`
	} `env-prefix:"DB_"`
	Client struct {
		ID                    string `env:"ID" env-description:"client id"`
		Secret                string `env:"SECRET" env-description:"client secret"`
		TLSRpc                bool   `env:"TLS_RPC" env-default:"true" env-description:"use tls for rpc connection"`
		RPCUrl                string `env:"RPC_URL" env-description:"rpc url, support grpc scheme, eg: grpc://PUBLIC_IP_OR_DNS:9101"`
		APIUrl                string `env:"API_URL" env-description:"api url, support https scheme, eg: https://PUBLIC_IP_OR_DNS:9443"`
		TLSInsecureSkipVerify bool   `env:"TLS_INSECURE_SKIP_VERIFY" env-default:"false" env-description:"skip tls verify, only for test environment"`
		Worker                struct {
			WorkerdBinaryPath  string `env:"WORKERD_BINARY_PATH" env-description:"workerd binary path"`
			WorkerdWorkDir     string `env:"WORKERD_WORK_DIR" env-default:"/tmp/frpp/workerd" env-description:"workerd work dir"`
			WorkerdDownloadURL struct {
				UseProxy   bool   `env:"USE_PROXY" env-default:"true" env-description:"use proxy"`
				LinuxArm64 string `env:"LINUX_ARM64" env-default:"https://github.com/cloudflare/workerd/releases/download/v1.20250505.0/workerd-linux-arm64.gz"`
				LinuxX8664 string `env:"LINUX_X86_64" env-default:"https://github.com/cloudflare/workerd/releases/download/v1.20250505.0/workerd-linux-64.gz"`
			} `env-prefix:"WORKERD_DOWNLOAD_URL_" env-description:"workerd download url"`
		} `env-prefix:"WORKER_" env-description:"worker's config"`
		Features struct {
			EnableFunctions   bool `env:"ENABLE_FUNCTIONS" env-default:"false" env-description:"enable functions (dangerous, default off)"`
			EnableRemoteShell bool `env:"ENABLE_REMOTE_SHELL" env-default:"false" env-description:"enable remote shell (dangerous, default off)"`
			EnableWireGuard   bool `env:"ENABLE_WIREGUARD" env-default:"false" env-description:"enable wireguard (dangerous, default off, reserved for S5b)"`
		} `env-prefix:"FEATURES_" env-description:"features config"`
	} `env-prefix:"CLIENT_"`
	IsDebug bool `env:"IS_DEBUG" env-default:"false" env-description:"is debug mode"`
	Debug   struct {
		ProfilerEnabled bool `env:"PROFILER_ENABLED" env-default:"false" env-description:"enable profiler"`
		ProfilerPort    int  `env:"PROFILER_PORT" env-default:"6961" env-description:"profiler port"`
	} `env-prefix:"DEBUG_"`
	Logger struct {
		DefaultLoggerLevel string `env:"DEFAULT_LOGGER_LEVEL" env-default:"info" env-description:"frp-panel internal default logger level"`
		FRPLoggerLevel     string `env:"FRP_LOGGER_LEVEL" env-default:"info" env-description:"frp logger level"`
	} `env-prefix:"LOGGER_"`
	HTTP_PROXY string `env:"HTTP_PROXY" env-description:"http proxy"`
}

func NewConfig() Config {
	var (
		err        error
		useEnvFile bool
		ctx        = context.Background()
	)

	// 越前面优先级越高，后面的不会覆盖前面的
	envFiles := []string{
		defs.CurEnvPath,
		defs.SysEnvPath,
	}

	for _, envFile := range envFiles {
		if err = godotenv.Load(envFile); err == nil {
			logger.Logger(ctx).Infof("load env file success: %s", envFile)
			useEnvFile = true
		}
	}

	if !useEnvFile {
		logger.Logger(ctx).Info("use runtime env variables")
	}

	cfg := Config{}
	if err = cleanenv.ReadEnv(&cfg); err != nil {
		logger.Logger(ctx).Panic(err)
	}
	cfg.Complete()

	if !cfg.IsDebug {
		gin.SetMode(gin.ReleaseMode)
	}

	return cfg
}

func (cfg *Config) Complete() {
	if len(cfg.Master.InternalFRPServerHost) == 0 {
		cfg.Master.InternalFRPServerHost = cfg.Master.RPCHost
	}

	if len(cfg.Master.APIHost) == 0 {
		cfg.Master.APIHost = cfg.Master.RPCHost
	}

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if len(cfg.Client.ID) == 0 {
		cfg.Client.ID = hostname
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("failed to get current working directory:", err)
		os.Exit(1)
	}

	if len(cfg.Client.Worker.WorkerdBinaryPath) == 0 {
		w, _ := utils.FindExecutableNames(func(name string) bool {
			return strings.HasPrefix(name, "workerd")
		}, cwd, "/")
		if len(w) > 0 {
			cfg.Client.Worker.WorkerdBinaryPath = w[0]
		}
	}
}

func (cfg Config) PrintStr() string {
	raw, err := json.Marshal(cfg)
	if err != nil {
		// Never fall back to the plaintext config: a partial serialization
		// could still carry secrets (spec §10.9).
		return fmt.Sprintf("[REDACTED] failed to serialize config: %v", err)
	}
	redacted, err := RedactJSON(raw)
	if err != nil {
		return fmt.Sprintf("[REDACTED] failed to redact config: %v", err)
	}
	return string(pretty.Pretty(redacted))
}

// sensitiveKeySubstrings lists normalized key fragments whose values must
// never appear in logs: credentials, tokens, DSNs (may embed passwords),
// cookies and proxies (may embed user:pass) (spec §10.9).
var sensitiveKeySubstrings = []string{
	"secret",
	"token",
	"password",
	"authorization",
	"apikey",
	"privatekey",
	"dsn",
	"cookie",
	"proxy",
}

// RedactJSON returns a copy of raw JSON with the value of every sensitive key
// replaced by [REDACTED]. Keys are matched case-insensitively after
// normalizing to [a-z0-9] (so secret, Secret, SECRET and _secret_ all match).
// Unknown nested objects and arrays are traversed recursively, so secrets at
// any depth are masked.
func RedactJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	redactValue(v)
	return json.Marshal(v)
}

// redactValue recursively replaces the value of sensitive keys with
// [REDACTED] and descends into every nested object and array.
func redactValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if isSensitiveKey(k) {
				t[k] = "[REDACTED]"
			} else {
				t[k] = redactValue(val)
			}
		}
	case []any:
		for i, val := range t {
			t[i] = redactValue(val)
		}
	}
	return v
}

// isSensitiveKey reports whether a JSON object key names sensitive data.
// Matching is substring-based on the normalized (lowercase, [a-z0-9] only)
// key, so camelCase, snake_case and upper-case variants all match.
func isSensitiveKey(key string) bool {
	norm := normalizeKey(key)
	for _, s := range sensitiveKeySubstrings {
		if strings.Contains(norm, s) {
			return true
		}
	}
	return false
}

func normalizeKey(key string) string {
	var b strings.Builder
	b.Grow(len(key))
	for _, r := range strings.ToLower(key) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
