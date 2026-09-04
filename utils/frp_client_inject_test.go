package utils

import (
	"encoding/json"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/stretchr/testify/require"
)

// candidateUninjected is the shape the transactional coordinators persist into
// clients.config_content: the proxies are real, but the connection identity
// (serverAddr/serverPort/user/metadatas) is still at frp's Complete() defaults.
// An agent that receives THIS config dials 0.0.0.0:7000 with no auth token.
const candidateUninjected = `{
  "serverAddr": "0.0.0.0",
  "serverPort": 7000,
  "loginFailExit": true,
  "proxies": [
    {"name": "mt3000-ssh", "type": "tcp", "localIP": "127.0.0.1", "localPort": 22, "remotePort": 322}
  ]
}`

func parseInjected(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	out := map[string]any{}
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func baseInjection() FRPClientInjection {
	return FRPClientInjection{
		ServerAddr: "203.0.113.10",
		BindPort:   7100,
		User:       "root",
		AuthToken:  "token-abc",
		ClientID:   "root.c.openWrt@1",
	}
}

func TestInjectFRPClientConfigFillsConnectionIdentity(t *testing.T) {
	raw, err := InjectFRPClientConfig([]byte(candidateUninjected), baseInjection())
	require.NoError(t, err)

	got := parseInjected(t, raw)
	require.Equal(t, "203.0.113.10", got["serverAddr"])
	require.Equal(t, float64(7100), got["serverPort"])
	require.Equal(t, "root", got["user"])

	md, ok := got["metadatas"].(map[string]any)
	require.True(t, ok, "metadatas must be present")
	require.Equal(t, "token-abc", md[defs.FRPAuthTokenKey])
	require.Equal(t, "root.c.openWrt@1", md[defs.FRPClientIDKey])

	// the proxies must survive untouched — injection only rewrites the
	// connection identity, never the user's tunnel rules.
	proxies, ok := got["proxies"].([]any)
	require.True(t, ok)
	require.Len(t, proxies, 1)
	p := proxies[0].(map[string]any)
	require.Equal(t, "mt3000-ssh", p["name"])
	require.Equal(t, float64(322), p["remotePort"])
	require.Equal(t, float64(22), p["localPort"])
}

// A pull-config caller re-injects on EVERY poll (every 30s) and the agent
// compares the parsed common config to decide whether to recreate the frpc
// handler. A non-idempotent injection would churn the handler forever.
func TestInjectFRPClientConfigIsIdempotent(t *testing.T) {
	first, err := InjectFRPClientConfig([]byte(candidateUninjected), baseInjection())
	require.NoError(t, err)
	second, err := InjectFRPClientConfig(first, baseInjection())
	require.NoError(t, err)
	require.Equal(t, string(first), string(second), "re-injecting an injected config must be a no-op")
}

// Already-injected rows exist in production (they were written by the legacy
// write path). Injecting them again must not change a single byte of their
// connection identity.
func TestInjectFRPClientConfigLeavesAlreadyInjectedRowUnchanged(t *testing.T) {
	injected, err := InjectFRPClientConfig([]byte(candidateUninjected), baseInjection())
	require.NoError(t, err)

	again, err := InjectFRPClientConfig(injected, baseInjection())
	require.NoError(t, err)

	a := parseInjected(t, injected)
	b := parseInjected(t, again)
	require.Equal(t, a["serverAddr"], b["serverAddr"])
	require.Equal(t, a["serverPort"], b["serverPort"])
	require.Equal(t, a["user"], b["user"])
	require.Equal(t, a["metadatas"], b["metadatas"])
}

func TestInjectFRPClientConfigPicksPortByTransportProtocol(t *testing.T) {
	cases := []struct {
		protocol string
		want     float64
	}{
		{"tcp", 7100},
		{"kcp", 7101},
		{"quic", 7102},
		{"websocket", 7100}, // anything else falls back to the TCP bind port
	}
	for _, tc := range cases {
		t.Run(tc.protocol, func(t *testing.T) {
			candidate := `{"serverAddr":"0.0.0.0","serverPort":7000,` +
				`"transport":{"protocol":"` + tc.protocol + `"},"proxies":[]}`
			in := baseInjection()
			in.KCPBindPort = 7101
			in.QUICBindPort = 7102

			raw, err := InjectFRPClientConfig([]byte(candidate), in)
			require.NoError(t, err)
			got := parseInjected(t, raw)
			require.Equal(t, tc.want, got["serverPort"])
		})
	}
}

func TestInjectFRPClientConfigFrpsURLOverrideWins(t *testing.T) {
	in := baseInjection()
	in.FrpsURLHost = "frps.example.com"
	in.FrpsURLPort = 8443
	in.FrpsURLProtocol = "quic"

	raw, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
	require.NoError(t, err)

	got := parseInjected(t, raw)
	require.Equal(t, "frps.example.com", got["serverAddr"])
	require.Equal(t, float64(8443), got["serverPort"])
	transport, ok := got["transport"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "quic", transport["protocol"])
}

// A caller must never be able to hand the agent a config that authenticates
// as somebody else: an empty client id would let frps index the connection
// under the wrong handler.
func TestInjectFRPClientConfigRejectsEmptyClientID(t *testing.T) {
	in := baseInjection()
	in.ClientID = ""
	_, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
	require.Error(t, err)
}

func TestInjectFRPClientConfigRejectsUnparseableCandidate(t *testing.T) {
	_, err := InjectFRPClientConfig([]byte("{not json"), baseInjection())
	require.Error(t, err)
}

// frps' multiuser auth plugin refuses a login whose user or meta token is
// empty. A config missing either parses fine and dials the right address but
// can never log in — exactly the silent-failure class this injector exists to
// prevent, so both paths must refuse to produce it.
func TestInjectFRPClientConfigRejectsEmptyIdentity(t *testing.T) {
	t.Run("empty user", func(t *testing.T) {
		in := baseInjection()
		in.User = ""
		_, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
		require.Error(t, err)
	})
	t.Run("empty auth token", func(t *testing.T) {
		in := baseInjection()
		in.AuthToken = ""
		_, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
		require.Error(t, err)
	})
}

// A server row that does not carry the port for the candidate's transport
// would otherwise produce serverPort: 0 — parseable, authenticated, and
// permanently unconnectable.
func TestInjectFRPClientConfigRejectsUndialableTarget(t *testing.T) {
	t.Run("transport has no bind port on the server", func(t *testing.T) {
		candidate := `{"serverAddr":"0.0.0.0","serverPort":7000,` +
			`"transport":{"protocol":"kcp"},"proxies":[]}`
		in := baseInjection() // KCPBindPort left at 0
		_, err := InjectFRPClientConfig([]byte(candidate), in)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no dialable port")
	})
	t.Run("server row has no address", func(t *testing.T) {
		in := baseInjection()
		in.ServerAddr = ""
		_, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
		require.Error(t, err)
	})
	t.Run("frps url override with zero port", func(t *testing.T) {
		in := baseInjection()
		in.FrpsURLHost = "frps.example.com"
		in.FrpsURLPort = 0
		in.FrpsURLProtocol = "tcp"
		_, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
		require.Error(t, err)
	})
}

// The error text travels to the agent over gRPC and into logs on both ends —
// it must never carry the frps auth token (spec §10.9 redaction).
func TestInjectFRPClientConfigErrorsCarryNoSecret(t *testing.T) {
	in := baseInjection()
	in.ServerAddr = ""
	_, err := InjectFRPClientConfig([]byte(candidateUninjected), in)
	require.Error(t, err)
	require.NotContains(t, err.Error(), in.AuthToken)
}

// The injected bytes must round-trip through the loader the agent itself
// uses, otherwise the agent rejects the config it just pulled.
func TestInjectFRPClientConfigRoundTripsThroughLoader(t *testing.T) {
	raw, err := InjectFRPClientConfig([]byte(candidateUninjected), baseInjection())
	require.NoError(t, err)

	common, proxies, _, err := LoadClientConfig(raw, true)
	require.NoError(t, err)
	require.Equal(t, "203.0.113.10", common.ServerAddr)
	require.Equal(t, 7100, common.ServerPort)
	require.Equal(t, "root", common.User)
	require.Equal(t, "token-abc", common.Metadatas[defs.FRPAuthTokenKey])
	require.Len(t, proxies, 1)
	// frp's own Complete() prefixes every proxy name with the user, which is
	// why frps logs the tunnel as "root.mt3000-ssh". Injecting the user is
	// therefore what makes the agent's proxy names match the master's.
	require.Equal(t, "root.mt3000-ssh", proxies[0].GetBaseConfig().Name)
}

// Guard the assumption the whole fix rests on: frp's own Complete() turns an
// empty config into 0.0.0.0:7000, which is exactly what the broken agent dialed.
func TestFRPCompleteDefaultsAreTheBrokenValues(t *testing.T) {
	cfg := &v1.ClientCommonConfig{}
	require.NoError(t, cfg.Complete())
	require.Equal(t, "0.0.0.0", cfg.ServerAddr)
	require.Equal(t, 7000, cfg.ServerPort)
	// ...including loginFailExit=true, which is why a candidate config that
	// never set the field arrives at the agent as "exit on first login
	// failure". See TestInjectFRPClientConfigForcesLoginRetry.
	require.Equal(t, true, *cfg.LoginFailExit)
}

// A master-managed agent must NEVER exit because one login failed.
//
// frp's Complete() defaults loginFailExit to true, so every candidate config
// that does not set the field explicitly tells the agent: if the first login
// fails, cancel the service and stop retrying (client/service.go:212 →
// loopLoginUntilSuccess(..., firstLoginExit=true) → svr.cancel()). Under
// frp-panel that is the wrong policy: the master hands out the connection
// identity, so a transient failure (master restarting, frps bindPort briefly
// taken, network blip, a config the master has since fixed) permanently kills
// a tunnel that would have worked seconds later. Recovery then depends on
// something noticing the dead handler.
//
// With false, the same call site keeps retrying with exponential backoff
// (1s × 2, jitter 0.1, capped at 10s), and once a control is established
// keepControllerWorking() reconnects forever. Self-healing, no supervision
// needed.
//
// This is a POLICY the injector owns, not a user preference: the field is not
// exposed in the tunnel UI, and both the pull path and the apply path go
// through here, so pinning it once covers every agent-facing config.
func TestInjectFRPClientConfigForcesLoginRetry(t *testing.T) {
	t.Run("candidate that never set the field", func(t *testing.T) {
		// candidateUninjected carries loginFailExit:true explicitly, mirroring
		// what Complete() bakes into every stored candidate.
		raw, err := InjectFRPClientConfig([]byte(candidateUninjected), baseInjection())
		require.NoError(t, err)
		require.Equal(t, false, parseInjected(t, raw)["loginFailExit"],
			"a managed agent must retry, never exit on a failed login")
	})
	t.Run("candidate that explicitly asked to exit", func(t *testing.T) {
		candidate := `{"serverAddr":"0.0.0.0","serverPort":7000,` +
			`"loginFailExit":true,"proxies":[]}`
		raw, err := InjectFRPClientConfig([]byte(candidate), baseInjection())
		require.NoError(t, err)
		require.Equal(t, false, parseInjected(t, raw)["loginFailExit"])
	})
	t.Run("survives the loader the agent uses", func(t *testing.T) {
		raw, err := InjectFRPClientConfig([]byte(candidateUninjected), baseInjection())
		require.NoError(t, err)
		common, _, _, err := LoadClientConfig(raw, true)
		require.NoError(t, err)
		require.NotNil(t, common.LoginFailExit)
		require.False(t, *common.LoginFailExit,
			"Complete() must not flip an explicit false back to true")
	})
}
