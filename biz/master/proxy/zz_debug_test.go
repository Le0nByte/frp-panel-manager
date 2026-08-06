package proxy

import (
	"encoding/json"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
)

// TestSafeStopValidationOrderingOriginStoppedAndMalformedContent pins two
// fail-closed validation-order cases that were originally found during B3a:
// a valid managed rule on a stopped physical origin reports AGENT_OFFLINE,
// while malformed typed content is rejected as CONFIG_VALIDATION_FAILED
// before the coordinator can classify its nominal HTTP type.
func TestSafeStopValidationOrderingOriginStoppedAndMalformedContent(t *testing.T) {
	env := newStopCoordEnv(t)
	env.seedStopActiveChildWorld(false, oldRuleSeed{name: "web", protocol: "tcp", remote: 8000, local: 8080})
	origin := env.clientByID(testClientID)
	origin.Stopped = true
	require.NoError(t, dao.NewMutation(env.ctx).UpdateClient(env.userInfo, origin))
	sc := env.newStopCoordinator()
	resp, err := sc.SafeStopProxy(env.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeAgentOffline)
	require.Zero(t, env.countTransactions(), "origin-stopped rejection must happen before transaction creation")

	env2 := newStopCoordEnv(t)
	env2.seedStopChildClient(nil)
	content, err2 := json.Marshal(map[string]interface{}{
		"name": "web", "type": "http",
		"localIP": "127.0.0.1", "localPort": 8080, "remotePort": 8000,
	})
	require.NoError(t, err2)
	require.NoError(t, env2.db.Create(&models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
		ClientID: coordChildID, OriginClientID: testClientID, ServerID: testServerID,
		Name: "web", Type: "http",
		TenantID: env2.userInfo.GetTenantID(), UserID: env2.userInfo.GetUserID(),
		Content: content,
	}}).Error)
	sc2 := env2.newStopCoordinator()
	resp, err = sc2.SafeStopProxy(env2.ctx, stopReq(uuidKey(), testClientID, "web"))
	require.NoError(t, err)
	assertStopInvalidCode(t, resp, defs.ErrorCodeConfigValidationFailed)
	require.Zero(t, env2.countTransactions(), "malformed-content rejection must happen before transaction creation")
}
