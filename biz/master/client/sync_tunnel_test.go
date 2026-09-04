package client

import (
	"context"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/stretchr/testify/require"
)

// syncTunnelEnv seeds a user-owned client whose stored config is the canonical
// CANDIDATE shape (no identity) plus one row that still carries a legacy
// injected identity, so both directions of the invariant can be asserted.
func syncTunnelCtx(t *testing.T, e *masterClientEnv) (*app.Context, models.UserInfo) {
	t.Helper()
	seedUser(t, e)
	user := &models.UserEntity{
		UserID: 1, UserName: "renamed", Email: "tester@example.com",
		TenantID: 1, Token: "rotated-token", SessionVersion: 1,
	}
	return app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), e.appInst), user
}

// SyncTunnel used to write the frps auth token in PLAINTEXT into
// clients.config_content whenever a user was renamed or their token rotated.
// Both consumers of that column now inject the identity at use time
// (utils.InjectFRPClientConfig, shared by the pull path and the transactional
// apply path), so persisting a credential there buys nothing and violates the
// "no plaintext token at rest in the client row" invariant (spec §10.9).
func TestSyncTunnelDoesNotPersistPlaintextToken(t *testing.T) {
	env := newMasterClientEnv(t)
	ctx, user := syncTunnelCtx(t, env)

	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-1",
		ConnectSecret: "child-secret", ConfigContent: candidateConfig(t),
	}))

	require.NoError(t, SyncTunnel(ctx, user))

	var stored models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-child").First(&stored).Error)
	require.NotContains(t, string(stored.ConfigContent), "rotated-token",
		"the frps token must never be written into clients.config_content")
	require.NotContains(t, string(stored.ConfigContent), "renamed",
		"the frp user must never be written into clients.config_content")
}

// The old implementation replaced the whole metadatas map with a single token
// entry, silently dropping x-vaala-frp-client-id. A row that legitimately
// carries client-id metadata must survive a user rename untouched.
func TestSyncTunnelPreservesExistingClientIDMetadata(t *testing.T) {
	env := newMasterClientEnv(t)
	ctx, user := syncTunnelCtx(t, env)

	// a legacy row that still holds an injected identity
	legacy, err := utils.InjectFRPClientConfig(candidateConfig(t), utils.FRPClientInjection{
		ServerAddr: "203.0.113.10", BindPort: 7100,
		User: "tester", AuthToken: "old-token", ClientID: "cli-child",
	})
	require.NoError(t, err)
	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-1",
		ConnectSecret: "child-secret", ConfigContent: legacy,
	}))

	require.NoError(t, SyncTunnel(ctx, user))

	var stored models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-child").First(&stored).Error)
	cfg, err := stored.GetConfigContent()
	require.NoError(t, err)
	require.Equal(t, "cli-child", cfg.Metadatas[defs.FRPClientIDKey],
		"the client id metadata must not be destroyed by a user rename")
}

// The tunnel rules themselves are the user's data — a user rename must never
// touch them.
func TestSyncTunnelLeavesProxiesUntouched(t *testing.T) {
	env := newMasterClientEnv(t)
	ctx, user := syncTunnelCtx(t, env)

	require.NoError(t, dao.NewMutation(env.ctx).CreateClient(env.user, &models.ClientEntity{
		ClientID: "cli-child", OriginClientID: "cli-origin", ServerID: "srv-1",
		ConnectSecret: "child-secret", ConfigContent: candidateConfig(t),
	}))

	require.NoError(t, SyncTunnel(ctx, user))

	var stored models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-child").First(&stored).Error)
	cfg, err := stored.GetConfigContent()
	require.NoError(t, err)
	require.Len(t, cfg.Proxies, 1)
	require.Equal(t, "mt3000-ssh", cfg.Proxies[0].GetBaseConfig().Name)
}
