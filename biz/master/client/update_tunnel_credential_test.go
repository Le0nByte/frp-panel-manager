package client

import (
	"encoding/json"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// UpdateFrpcHander is a live legacy writer (reached from the worker-ingress
// create path and the legacy proxy delete cleanup). It used to persist the
// INJECTED bytes — frp user + plaintext frps auth token — into
// clients.config_content, a column get_client/list_client hand to the WebUI.
//
// Both consumers of that column now inject the identity at use time
// (utils.InjectFRPClientConfig, shared by the agent pull path and the
// transactional apply path), so the credential must not be at rest there
// (spec §10.9). Invariant: what is STORED is the candidate, what is PUSHED
// carries the identity. These two tests pin both halves.
func TestUpdateFrpcHanderStoresCandidateWithoutCredential(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedUnshadowedOrigin()

	result := make(chan error, 1)
	go func() {
		_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-origin", "srv-1", []byte(validFrpcTOML)))
		result <- err
	}()
	wire := env.waitForEvent(stream)
	env.ackUpdateFRPC(stream, wire, pb.RespCode_RESP_CODE_SUCCESS, "ok")
	require.NoError(t, <-result)

	var update pb.UpdateFRPCRequest
	require.NoError(t, proto.Unmarshal(wire.GetData(), &update))

	var stored models.Client
	require.NoError(t, env.db.Where("client_id = ?", update.GetClientId()).First(&stored).Error)

	raw := string(stored.ConfigContent)
	require.NotContains(t, raw, env.user.GetToken(),
		"the frps auth token must never be persisted into clients.config_content")

	cfg, err := stored.GetConfigContent()
	require.NoError(t, err)
	require.Empty(t, cfg.Metadatas[defs.FRPAuthTokenKey], "no auth-token metadata at rest")
	require.Empty(t, cfg.User, "no frp user at rest")
	require.Equal(t, update.GetClientId(), cfg.Metadatas[defs.FRPClientIDKey],
		"the client-id metadata identifies the handler and must stay")
}

// The other half: the bytes actually pushed to the agent MUST carry the
// identity, otherwise the agent dials frps unauthenticated.
func TestUpdateFrpcHanderPushesInjectedConfig(t *testing.T) {
	env := newMasterClientEnv(t)
	stream := env.registerAgent("cli-origin")
	env.seedUnshadowedOrigin()

	result := make(chan error, 1)
	go func() {
		_, err := UpdateFrpcHander(env.ctx, updateFrpcReq("cli-origin", "srv-1", []byte(validFrpcTOML)))
		result <- err
	}()
	wire := env.waitForEvent(stream)
	env.ackUpdateFRPC(stream, wire, pb.RespCode_RESP_CODE_SUCCESS, "ok")
	require.NoError(t, <-result)

	var update pb.UpdateFRPCRequest
	require.NoError(t, proto.Unmarshal(wire.GetData(), &update))

	var payload struct {
		User       string            `json:"user"`
		ServerAddr string            `json:"serverAddr"`
		Metadatas  map[string]string `json:"metadatas"`
	}
	require.NoError(t, json.Unmarshal(update.GetConfig(), &payload))
	require.Equal(t, env.user.GetToken(), payload.Metadatas[defs.FRPAuthTokenKey],
		"the pushed config must carry the frps auth token")
	require.Equal(t, env.user.GetUserName(), payload.User,
		"the pushed config must carry the frp user")
	require.NotEqual(t, "0.0.0.0", payload.ServerAddr,
		"the pushed config must carry the real frps address")
}
