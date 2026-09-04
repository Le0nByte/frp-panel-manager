package client

import (
	"encoding/json"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/stretchr/testify/require"
)

// storedRow reads a client row's raw config bytes back from the DB.
func storedRow(t *testing.T, env *masterClientEnv, clientID string) map[string]any {
	t.Helper()
	var row models.Client
	require.NoError(t, env.db.Where("client_id = ?", clientID).First(&row).Error)
	if len(row.ConfigContent) == 0 {
		return nil
	}
	var m map[string]any
	require.NoError(t, json.Unmarshal(row.ConfigContent, &m))
	return m
}

// pollutedConfig is what the pre-v0.4.0 writers persisted: a candidate config
// with the frp user and the PLAINTEXT frps auth token baked in.
func pollutedConfig(t *testing.T) []byte {
	t.Helper()
	return []byte(`{
  "user": "root",
  "serverAddr": "203.0.113.10",
  "serverPort": 7100,
  "loginFailExit": true,
  "metadatas": {"token": "leaked-frps-token", "x-vaala-frp-client-id": "cli-child"},
  "transport": {"protocol": "tcp"},
  "proxies": [
    {"name": "mt3000-ssh", "type": "tcp", "localIP": "127.0.0.1", "localPort": 22, "remotePort": 322},
    {"name": "mt3000-luci", "type": "tcp", "localIP": "127.0.0.1", "localPort": 443, "remotePort": 3443}
  ]
}`)
}

func seedPolluted(t *testing.T, env *masterClientEnv, clientID string, cfg []byte) {
	t.Helper()
	require.NoError(t, env.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID: clientID, ServerID: "srv-1", UserID: 1, TenantID: 1,
		ConnectSecret: "child-secret", ConfigContent: cfg,
	}}).Error)
}

// Rows written before v0.4.0 keep a plaintext frps token at rest in a column
// get_client/list_client hand to the WebUI. The new code no longer writes it,
// but existing rows do not self-heal — this startup pass cleans them.
func TestStripStoredCredentialsRemovesTokenAndUser(t *testing.T) {
	env := newMasterClientEnv(t)
	seedPolluted(t, env, "cli-polluted", pollutedConfig(t))

	scanned, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, scanned)
	require.Equal(t, 1, cleaned)

	got := storedRow(t, env, "cli-polluted")
	require.NotContains(t, got, "user", "the frp user must be gone")
	md, _ := got["metadatas"].(map[string]any)
	require.NotContains(t, md, defs.FRPAuthTokenKey, "the frps auth token must be gone")
	require.Equal(t, "cli-child", md[defs.FRPClientIDKey],
		"the client-id metadata identifies the handler and must survive")
}

// Everything that is NOT the identity must survive byte-for-byte in meaning:
// stripping a credential must never cost a tunnel.
func TestStripStoredCredentialsPreservesProxiesAndTransport(t *testing.T) {
	env := newMasterClientEnv(t)
	seedPolluted(t, env, "cli-polluted", pollutedConfig(t))

	_, _, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)

	got := storedRow(t, env, "cli-polluted")
	proxies, ok := got["proxies"].([]any)
	require.True(t, ok)
	require.Len(t, proxies, 2, "both tunnels must survive")
	first, _ := proxies[0].(map[string]any)
	require.Equal(t, "mt3000-ssh", first["name"])
	require.Equal(t, float64(322), first["remotePort"])
	transport, _ := got["transport"].(map[string]any)
	require.Equal(t, "tcp", transport["protocol"], "transport must survive")
	require.Equal(t, float64(7100), got["serverPort"], "non-identity fields untouched")
	require.Equal(t, true, got["loginFailExit"])
}

// Idempotent: a second pass must find nothing to do and must not rewrite rows.
func TestStripStoredCredentialsIsIdempotent(t *testing.T) {
	env := newMasterClientEnv(t)
	seedPolluted(t, env, "cli-polluted", pollutedConfig(t))

	_, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, cleaned)

	var before models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-polluted").First(&before).Error)

	scanned, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, scanned)
	require.Equal(t, 0, cleaned, "a clean row must not be rewritten")

	var after models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-polluted").First(&after).Error)
	require.Equal(t, string(before.ConfigContent), string(after.ConfigContent))
}

// A row that is already canonical (candidate shape) must be left completely
// alone — no write, no metadata churn.
func TestStripStoredCredentialsLeavesCleanRowsUntouched(t *testing.T) {
	env := newMasterClientEnv(t)
	seedPolluted(t, env, "cli-clean", candidateConfig(t))

	var before models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-clean").First(&before).Error)

	scanned, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, scanned)
	require.Equal(t, 0, cleaned)

	var after models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-clean").First(&after).Error)
	require.Equal(t, string(before.ConfigContent), string(after.ConfigContent))
}

// Unparseable JSON must not abort the whole pass: one bad row cannot block
// cleaning every other row (and must not be silently rewritten either).
func TestStripStoredCredentialsSkipsUnparseableRow(t *testing.T) {
	env := newMasterClientEnv(t)
	seedPolluted(t, env, "cli-bad", []byte("{not json"))
	seedPolluted(t, env, "cli-polluted", pollutedConfig(t))

	scanned, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err, "a single malformed row must not fail the pass")
	require.Equal(t, 2, scanned)
	require.Equal(t, 1, cleaned)

	var bad models.Client
	require.NoError(t, env.db.Where("client_id = ?", "cli-bad").First(&bad).Error)
	require.Equal(t, "{not json", string(bad.ConfigContent), "a row we cannot parse is left as-is")

	got := storedRow(t, env, "cli-polluted")
	require.NotContains(t, got, "user")
}

// The pass must never touch rows with no config at all (brand-new clients,
// shadow origins) — no write, no error.
func TestStripStoredCredentialsIgnoresEmptyConfigRows(t *testing.T) {
	env := newMasterClientEnv(t)
	require.NoError(t, env.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID: "cli-empty", UserID: 1, TenantID: 1, ConnectSecret: "child-secret",
	}}).Error)

	scanned, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)
	require.Equal(t, 0, scanned, "rows without a config are not scanned")
	require.Equal(t, 0, cleaned)
}

// Cross-tenant: the pass is admin-wide by design (it is a data-hygiene
// migration, not a user-facing operation), so a row owned by another tenant
// must be cleaned too.
func TestStripStoredCredentialsCleansEveryTenant(t *testing.T) {
	env := newMasterClientEnv(t)
	require.NoError(t, env.db.Create(&models.Client{ClientEntity: &models.ClientEntity{
		ClientID: "cli-foreign", ServerID: "srv-1", UserID: 42, TenantID: 42,
		ConnectSecret: "foreign-secret", ConfigContent: pollutedConfig(t),
	}}).Error)

	scanned, cleaned, err := StripStoredCredentials(env.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, scanned)
	require.Equal(t, 1, cleaned)

	got := storedRow(t, env, "cli-foreign")
	require.NotContains(t, got, "user")
}
