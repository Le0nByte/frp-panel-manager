package proxy

import (
	"fmt"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
)

// Proxy working status values the panel renders. They answer "why is this
// tunnel not working" and therefore must stay distinct:
//
//	stopped       - the rule is paused on purpose (nothing to fix)
//	agent_offline - the master holds no session for the agent, i.e. the frpc
//	                process on that host is not running (or is not connected);
//	                the fix is to start the client, not to touch the rule
//	error         - the agent answered but reported a broken tunnel, or the
//	                call failed while the session still exists
//	unknown       - the agent answered with no phase at all
const (
	proxyStatusStopped      = "stopped"
	proxyStatusAgentOffline = "agent_offline"
	proxyStatusError        = "error"
	proxyStatusUnknown      = "unknown"
)

// agentOfflineHint is the operator-facing explanation for
// proxyStatusAgentOffline. It carries the client id only, never secret
// material, so the panel can show it as-is.
const agentOfflineHint = "the frp client process is not connected to the master: start it on the host that runs client [%s]"

func GetProxyConfig(c *app.Context, req *pb.GetProxyConfigRequest) (*pb.GetProxyConfigResponse, error) {
	return getProxyConfigWithSessionCheck(c, req.GetClientId(), req.GetServerId(), req.GetName())
}

// getProxyConfigWithSessionCheck reports one tunnel's working status.
//
// The session existence check is what separates "the frpc process is not
// running" from "the agent answered with a broken tunnel". Without it the
// panel spent the full 30s RPC budget and then flattened every cause into
// "error", which told the operator nothing about the next action.
//
// Only a MISSING session counts as agent_offline. A session that exists but
// does not answer stays "error": the process may still be running (half-open
// stream), and reporting it as offline would invite a needless restart.
func getProxyConfigWithSessionCheck(c *app.Context, clientID, serverID, proxyName string) (*pb.GetProxyConfigResponse, error) {
	userInfo := common.GetUserInfo(c)

	proxyConfig, err := dao.NewQuery(c).GetProxyConfigByFilter(userInfo, &models.ProxyConfigEntity{
		ClientID: clientID,
		ServerID: serverID,
		Name:     proxyName,
	})
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get proxy config, client: [%s], server: [%s], proxy name: [%s]", clientID, serverID, proxyName)
		return nil, err
	}

	workingStatus := &pb.ProxyWorkingStatus{Status: lo.ToPtr(proxyStatusStopped)}
	if !proxyConfig.Stopped {
		workingStatus = probeProxyWorkingStatus(c, userInfo, proxyConfig, proxyName)
	}

	return &pb.GetProxyConfigResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "success"},
		ProxyConfig: &pb.ProxyConfig{
			Id:             lo.ToPtr(uint32(proxyConfig.ID)),
			Name:           lo.ToPtr(proxyConfig.Name),
			Type:           lo.ToPtr(proxyConfig.Type),
			ClientId:       lo.ToPtr(proxyConfig.ClientID),
			ServerId:       lo.ToPtr(proxyConfig.ServerID),
			Config:         lo.ToPtr(string(proxyConfig.Content)),
			OriginClientId: lo.ToPtr(proxyConfig.OriginClientID),
			Stopped:        lo.ToPtr(proxyConfig.Stopped),
		},
		WorkingStatus: workingStatus,
	}, nil
}

// probeProxyWorkingStatus resolves the live phase of one active rule. The agent
// session is checked BEFORE any RPC so an offline agent is reported
// immediately instead of after the RPC timeout.
func probeProxyWorkingStatus(c *app.Context, userInfo models.UserInfo, proxyConfig *models.ProxyConfig, proxyName string) *pb.ProxyWorkingStatus {
	if c.GetApp().GetClientsManager().Get(proxyConfig.OriginClientID) == nil {
		logger.Logger(c).Warnf("proxy agent is offline, client: [%s], server: [%s], proxy name: [%s]",
			proxyConfig.OriginClientID, proxyConfig.ServerID, proxyConfig.Name)
		return agentOfflineStatus(proxyConfig.OriginClientID)
	}

	resp := &pb.GetProxyConfigResponse{}
	if err := rpc.CallClientWrapper(c, proxyConfig.OriginClientID, pb.Event_EVENT_GET_PROXY_INFO, &pb.GetProxyConfigRequest{
		ClientId: lo.ToPtr(proxyConfig.ClientID),
		ServerId: lo.ToPtr(proxyConfig.ServerID),
		Name:     lo.ToPtr(fmt.Sprintf("%s.%s", userInfo.GetUserName(), proxyName)),
	}, resp); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get proxy config, client: [%s], server: [%s], proxy name: [%s]", proxyConfig.OriginClientID, proxyConfig.ServerID, proxyConfig.Name)
		// The session may have been torn down between the check and the
		// call; that is still an offline agent.
		if c.GetApp().GetClientsManager().Get(proxyConfig.OriginClientID) == nil {
			return agentOfflineStatus(proxyConfig.OriginClientID)
		}
		return &pb.ProxyWorkingStatus{
			Status: lo.ToPtr(proxyStatusError),
			Err:    lo.ToPtr(err.Error()),
		}
	}

	if len(resp.GetWorkingStatus().GetStatus()) == 0 {
		return &pb.ProxyWorkingStatus{Status: lo.ToPtr(proxyStatusUnknown)}
	}
	return resp.GetWorkingStatus()
}

func agentOfflineStatus(originClientID string) *pb.ProxyWorkingStatus {
	return &pb.ProxyWorkingStatus{
		Status: lo.ToPtr(proxyStatusAgentOffline),
		Err:    lo.ToPtr(fmt.Sprintf(agentOfflineHint, originClientID)),
	}
}
