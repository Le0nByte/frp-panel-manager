package client

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
)

// ---- helpers ----------------------------------------------------------

// agentCtx builds the context an AGENT rpc actually runs in: authenticated by
// client secret, with NO JWT user in the context. Using the harness ctx (which
// carries defs.UserInfoKey) would hide a nil-pointer panic in production, so
// every pull-config test MUST use this one.
func agentCtx(e *masterClientEnv) *app.Context {
	return app.NewContext(context.Background(), e.appInst)
}

// seedUser creates the owning account row so the pull path can resolve the frp
// auth token. The shared harness migrates Client/Server/ProxyConfig only, so
// the User table is migrated here — idempotent per test, and tests that do not
// need a user do not pay for the extra table.
func seedUser(t *testing.T, e *masterClientEnv) {
	t.Helper()
	require.NoError(t, e.db.AutoMigrate(&models.User{}))
	require.NoError(t, e.db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID: 1, UserName: "tester", Email: "tester@example.com",
		TenantID: 1, Token: "test-token", SessionVersion: 1,
	}}).Error)
}

// candidateConfig is what the transactional coordinators persist into
// clients.config_content: real proxies, connection identity still at frp's
// Complete() defaults (0.0.0.0:7000, no user, no auth metadata).
func candidateConfig(t *testing.T) []byte {
	t.Helper()
	return []byte(`{
  "serverAddr": "0.0.0.0",
  "serverPort": 7000,
  "loginFailExit": true,
  "proxies": [
    {"name": "mt3000-ssh", "type": "tcp", "localIP": "127.0.0.1", "localPort": 22, "remotePort": 322}
  ]
}`)
}

// seedCandidateChild seeds the exact production shape that broke the OpenWrt
// agent: shadow origin + child whose stored config is the UNINJECTED candidate.
func seedCandidateChild(t *testing.T, e *masterClientEnv) {
	t.Helper()
	seedUser(t, e)

	child := &models.ClientEntity{
		ClientID:       "cli-child",
		OriginClientID: "cli-origin",
		ServerID:       "srv-1",
		ConnectSecret:  "child-secret",
		ConfigContent:  candidateConfig(t),
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.user, child))

	origin := &models.ClientEntity{
		ClientID:      "cli-origin",
		IsShadow:      true,
		ConnectSecret: "origin-secret",
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateClient(e.user, origin))

	srv := &models.ServerEntity{
		ServerID:      "srv-1",
		ServerIP:      "203.0.113.10",
		ConfigContent: []byte(`{"bindPort": 7100}`),
		ConnectSecret: "server-secret",
	}
	require.NoError(t, dao.NewMutation(e.ctx).CreateServer(e.user, srv))
}

func pullReq(clientID, secret string) *pb.PullClientConfigReq {
	return &pb.PullClientConfigReq{Base: &pb.ClientBase{
		ClientId: clientID, ClientSecret: secret,
	}}
}

func pulledConfig(t *testing.T, resp *pb.PullClientConfigResp) map[string]any {
	t.Helper()
	raw := resp.GetClient().GetConfig()
	require.NotEmpty(t, raw, "pull returned an empty config")
	out := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	return out
}

// ---- tests ------------------------------------------------------------

// The regression this whole change exists for: an agent polling pull-config
// must receive a DIALABLE config, not the stored candidate. Before the fix the
// OpenWrt agent got serverAddr=0.0.0.0/serverPort=7000 with no auth metadata,
// dialed a foreign frps on 7000, and exited within seconds (loginFailExit).
func TestRPCPullConfigInjectsConnectionIdentity(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)

	got := pulledConfig(t, resp)
	require.Equal(t, "203.0.113.10", got["serverAddr"], "must be the server row IP, never 0.0.0.0")
	require.Equal(t, float64(7100), got["serverPort"], "must be the frps bindPort, never frp's 7000 default")
	require.Equal(t, "tester", got["user"])

	md, ok := got["metadatas"].(map[string]any)
	require.True(t, ok, "frps auth metadata must be present")
	require.Equal(t, "test-token", md[defs.FRPAuthTokenKey])
	require.Equal(t, "cli-child", md[defs.FRPClientIDKey])
}

// Injection rewrites the connection identity only; the user's tunnel rules must
// survive byte-for-byte or the agent would silently lose tunnels on every poll.
func TestRPCPullConfigKeepsProxiesIntact(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)

	got := pulledConfig(t, resp)
	proxies, ok := got["proxies"].([]any)
	require.True(t, ok)
	require.Len(t, proxies, 1)
	p := proxies[0].(map[string]any)
	require.Equal(t, "mt3000-ssh", p["name"])
	require.Equal(t, float64(322), p["remotePort"])
	require.Equal(t, float64(22), p["localPort"])
	require.Equal(t, "127.0.0.1", p["localIP"])
}

// The agent recreates its frpc handler whenever the pulled common config
// differs from the running one. Injection must therefore be stable across
// polls, otherwise every 30s tick tears the tunnels down and rebuilds them.
func TestRPCPullConfigIsStableAcrossPolls(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)

	first, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)
	second, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)

	require.Equal(t, first.GetClient().GetConfig(), second.GetClient().GetConfig(),
		"two consecutive polls must return byte-identical configs")
}

// Injection is a READ-side transform: it must never write the injected bytes
// (with the plaintext frps token) back into clients.config_content.
func TestRPCPullConfigDoesNotPersistInjectedBytes(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)
	before := append([]byte(nil), candidateConfig(t)...)

	_, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)

	row := &models.Client{}
	require.NoError(t, env.db.Where("client_id = ?", "cli-child").First(row).Error)
	var stored, orig map[string]any
	require.NoError(t, json.Unmarshal(row.ConfigContent, &stored))
	require.NoError(t, json.Unmarshal(before, &orig))
	require.Equal(t, orig["serverAddr"], stored["serverAddr"], "stored candidate must stay untouched")
	require.Equal(t, orig["serverPort"], stored["serverPort"])
	require.NotContains(t, string(row.ConfigContent), "test-token",
		"the frps token must never be persisted into the client row by a read")
}

// A brand-new client with no tunnels yet has an empty config. That must stay
// empty so the agent logs "config is empty, wait for server init" instead of
// being handed something unusable.
func TestRPCPullConfigEmptyConfigStaysEmpty(t *testing.T) {
	env := newMasterClientEnv(t)
	seedUser(t, env)
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-fresh", ServerID: "srv-1", ConnectSecret: "fresh-secret",
	}))

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-fresh", "fresh-secret"))
	require.NoError(t, err)
	require.Empty(t, resp.GetClient().GetConfig())
}

// Fail-safe, not fail-open: when the server row cannot supply an address the
// pull must NOT hand back the candidate's 0.0.0.0:7000 (which would make the
// agent dial a wrong, possibly unauthenticated frps). It must also not report
// "empty", which is indistinguishable from a healthy brand-new client — the
// fault has to surface as an error on both sides.
func TestRPCPullConfigServerNotPreparedFailsLoudly(t *testing.T) {
	env := newMasterClientEnv(t)
	seedUser(t, env)
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-missing",
		ConnectSecret: "child-secret", ConfigContent: candidateConfig(t),
	}))

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.Error(t, err, "a client with a config the master cannot make dialable is a fault, not 'empty'")
	require.Empty(t, resp.GetClient().GetConfig(),
		"never hand the agent an un-injected config")
}

// Tenant isolation: the server row is resolved with the OWNER's UserInfo, not
// an admin-scoped lookup. A client row pointing at another tenant's server
// must NOT be able to pull that tenant's frps address through the agent RPC
// (which carries no JWT of its own).
func TestRPCPullConfigDoesNotLeakForeignTenantServer(t *testing.T) {
	env := newMasterClientEnv(t)
	seedUser(t, env)

	// a server owned by a DIFFERENT tenant/user
	require.NoError(t, env.db.Create(&models.Server{ServerEntity: &models.ServerEntity{
		ServerID: "srv-foreign", ServerIP: "198.51.100.7",
		ConfigContent: []byte(`{"bindPort": 7999}`),
		ConnectSecret: "foreign-secret", UserID: 42, TenantID: 42,
	}}).Error)

	// our client (user 1) wrongly points at it
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-foreign",
		ConnectSecret: "child-secret", ConfigContent: candidateConfig(t),
	}))

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.Error(t, err, "a cross-tenant server row must not resolve")
	cfg := resp.GetClient().GetConfig()
	require.Empty(t, cfg, "a cross-tenant server row must not be injectable")
	require.NotContains(t, cfg, "198.51.100.7")
}

// An unparseable stored config is a fault too — it must not degrade to
// "empty" and hide behind the healthy brand-new-client log line.
func TestRPCPullConfigUnparseableStoredConfigFailsLoudly(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", "cli-child").
		Update("config_content", []byte("{not json")).Error)

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.Error(t, err)
	require.Empty(t, resp.GetClient().GetConfig())
}

// The error must NOT carry the frps auth token: it travels to the agent over
// gRPC and lands in logs on both sides (spec §10.9 redaction).
func TestRPCPullConfigErrorCarriesNoSecret(t *testing.T) {
	env := newMasterClientEnv(t)
	seedUser(t, env)
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-missing",
		ConnectSecret: "child-secret", ConfigContent: candidateConfig(t),
	}))

	_, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "test-token", "the frps token must never travel in an error")
	require.NotContains(t, err.Error(), "child-secret", "the client secret must never travel in an error")
}

// Production shape: the built-in `default` server row is owned by user_id 0
// while the client belongs to the admin account (user_id 1). The owner-scoped
// lookup must still resolve it through GetServerByServerID's default-server
// branch, or every real deployment would silently stop serving configs.
func TestRPCPullConfigResolvesBuiltinDefaultServer(t *testing.T) {
	env := newMasterClientEnv(t)
	seedUser(t, env)

	// the built-in default server: user_id/tenant_id 0, not the admin's own row
	require.NoError(t, env.db.Create(&models.Server{ServerEntity: &models.ServerEntity{
		ServerID: "default", ServerIP: "203.0.113.10",
		ConfigContent: []byte(`{"bindPort": 7100}`),
		ConnectSecret: "server-secret", UserID: 0, TenantID: 0,
	}}).Error)

	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "default",
		ConnectSecret: "child-secret", ConfigContent: candidateConfig(t),
	}))

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)

	got := pulledConfig(t, resp)
	require.Equal(t, "203.0.113.10", got["serverAddr"])
	require.Equal(t, float64(7100), got["serverPort"])
}

// An explicit frps URL on the client row overrides the server-derived address
// (same precedence as the write path's buildFrpcApplyConfig).
func TestRPCPullConfigFrpsURLOverrideWins(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)
	require.NoError(t, env.db.Model(&models.Client{}).
		Where("client_id = ?", "cli-child").
		Update("frps_url", "quic://frps.example.com:8443").Error)

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.NoError(t, err)

	got := pulledConfig(t, resp)
	require.Equal(t, "frps.example.com", got["serverAddr"])
	require.Equal(t, float64(8443), got["serverPort"])
	transport, ok := got["transport"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "quic", transport["protocol"])
}

// A shadow origin carries no config of its own: the response must keep listing
// the child ids so the agent recurses, and must not blow up on injection.
func TestRPCPullConfigShadowOriginListsChildren(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)
	require.NoError(t, env.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		Name: "mt3000-ssh", ClientID: "cli-child", OriginClientID: "cli-origin",
		ServerID: "srv-1", UserID: 1, TenantID: 1,
	}}).Error)

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-origin", "origin-secret"))
	require.NoError(t, err)
	require.Contains(t, resp.GetClient().GetClientIds(), "cli-child")
	require.Empty(t, resp.GetClient().GetConfig(), "a shadow row has no config of its own")
}

// The kcp/quic transports pull their port from a DIFFERENT field of the server
// row than tcp does. This pins the read path's wiring of those fields into the
// injector — the injector's own unit test cannot catch a mis-wired caller.
func TestRPCPullConfigWiresTransportSpecificBindPorts(t *testing.T) {
	for _, tc := range []struct {
		protocol string
		want     float64
	}{
		{"tcp", 7100},
		{"kcp", 7101},
		{"quic", 7102},
	} {
		t.Run(tc.protocol, func(t *testing.T) {
			env := newMasterClientEnv(t)
			seedUser(t, env)
			require.NoError(t, env.db.Create(&models.Server{ServerEntity: &models.ServerEntity{
				ServerID: "srv-multi", ServerIP: "203.0.113.10",
				ConfigContent: []byte(`{"bindPort":7100,"kcpBindPort":7101,"quicBindPort":7102}`),
				ConnectSecret: "server-secret", UserID: 1, TenantID: 1,
			}}).Error)
			require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
				ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-multi",
				ConnectSecret: "child-secret",
				ConfigContent: []byte(`{"serverAddr":"0.0.0.0","serverPort":7000,` +
					`"transport":{"protocol":"` + tc.protocol + `"},"proxies":[]}`),
			}))

			resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
			require.NoError(t, err)
			got := pulledConfig(t, resp)
			require.Equal(t, "203.0.113.10", got["serverAddr"])
			require.Equal(t, tc.want, got["serverPort"],
				"the %s bind port from the server row must reach the agent", tc.protocol)
		})
	}
}

// A server row missing the port for the client's transport yields serverPort 0
// — parseable, authenticated, permanently unconnectable. Fail loudly instead.
func TestRPCPullConfigRejectsServerWithoutPortForTransport(t *testing.T) {
	env := newMasterClientEnv(t)
	seedUser(t, env)
	require.NoError(t, env.db.Create(&models.Server{ServerEntity: &models.ServerEntity{
		ServerID: "srv-tcponly", ServerIP: "203.0.113.10",
		ConfigContent: []byte(`{"bindPort":7100}`), // no kcpBindPort
		ConnectSecret: "server-secret", UserID: 1, TenantID: 1,
	}}).Error)
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-tcponly",
		ConnectSecret: "child-secret",
		ConfigContent: []byte(`{"serverAddr":"0.0.0.0","serverPort":7000,` +
			`"transport":{"protocol":"kcp"},"proxies":[]}`),
	}))

	resp, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "child-secret"))
	require.Error(t, err)
	require.Empty(t, resp.GetClient().GetConfig())
}

// Authentication must stay exactly as strict as before the change.
func TestRPCPullConfigRejectsWrongSecret(t *testing.T) {
	env := newMasterClientEnv(t)
	seedCandidateChild(t, env)

	_, err := RPCPullConfig(agentCtx(env), pullReq("cli-child", "wrong-secret"))
	require.Error(t, err)
}
