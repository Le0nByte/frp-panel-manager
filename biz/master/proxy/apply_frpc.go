package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/proto"
)

// ApplyRequest is the structured apply payload (round-8 I2: the old
// positional originID/childID/serverID/config parameter string is gone).
// PhysicalAgentID is the ClientsManager key of the agent PROCESS (the
// origin client id). The handler switch happens on the agent: it stops and
// deletes the (ReplaceClientID, ServerID) handler when ReplaceClientID is
// set and differs from TargetClientID, then adds/updates the
// TargetClientID handler — a directed switch, never StopAll.
type ApplyRequest struct {
	PhysicalAgentID string
	TargetClientID  string
	ReplaceClientID string
	ServerID        string
	Config          []byte
}

// ApplyService pushes a candidate frpc config to the real client agent and
// waits for its ACK. It is the ONLY sanctioned way to apply configs inside a
// safe transaction: synchronous, bounded by ctx (spec §8.4: config apply
// 15s), no goroutine fire-and-forget, no DB writes before the ACK.
//
// The first return value is the structured error code ("" on success):
// AGENT_OFFLINE when the agent is not connected, APPLY_TIMEOUT when the
// agent did not ACK within the budget, APPLY_REJECTED when the agent
// answered but rejected the config (or sent an unparseable ACK).
type ApplyService interface {
	Apply(ctx context.Context, c *app.Context, req ApplyRequest) (string, error)
}

type rpcApplyService struct{}

// buildUpdateFRPCRequest maps the structured apply into the wire payload.
// ReplaceClientID always travels as a present (possibly empty) field so the
// agent's GetReplaceClientId is deterministic.
func buildUpdateFRPCRequest(req ApplyRequest) *pb.UpdateFRPCRequest {
	return &pb.UpdateFRPCRequest{
		ClientId:        lo.ToPtr(req.TargetClientID),
		ServerId:        lo.ToPtr(req.ServerID),
		Config:          req.Config,
		ReplaceClientId: lo.ToPtr(req.ReplaceClientID),
	}
}

// classifyApplyResponse turns the raw RPC outcome into the stable error
// code. Transport failures classify via classifyApplyError; an agent answer
// that is not a clean success — explicit rejection or an unparseable ACK —
// is APPLY_REJECTED (a real answer, never a timeout).
func classifyApplyResponse(resp *pb.ClientMessage, err error) (string, error) {
	if err != nil {
		return classifyApplyError(err), fmt.Errorf("apply frpc config: %w", err)
	}
	if resp == nil {
		return defs.ErrorCodeAgentOffline, fmt.Errorf("apply frpc config: empty agent reply")
	}
	out := &pb.UpdateFRPCResponse{}
	if err := proto.Unmarshal(resp.GetData(), out); err != nil {
		return defs.ErrorCodeApplyRejected, fmt.Errorf("apply frpc config: cannot unmarshal agent ack: %w", err)
	}
	if out.GetStatus() == nil || out.GetStatus().GetCode() != pb.RespCode_RESP_CODE_SUCCESS {
		msg := "agent rejected config"
		if out.GetStatus() != nil {
			msg = out.GetStatus().GetMessage()
		}
		return defs.ErrorCodeApplyRejected, fmt.Errorf("apply frpc config: %s", msg)
	}
	return "", nil
}

// Apply sends EVENT_UPDATE_FRPC addressed to the physical agent (origin
// client id — the ClientsManager key) with the child (shadow) id in the
// payload and the directed replace, and blocks until the agent replies.
func (rpcApplyService) Apply(ctx context.Context, c *app.Context, req ApplyRequest) (string, error) {
	if req.PhysicalAgentID == "" {
		return defs.ErrorCodeAgentOffline, fmt.Errorf("apply: origin client id is empty")
	}
	resp, err := rpc.CallClientWithContext(ctx, c.GetApp(), req.PhysicalAgentID, pb.Event_EVENT_UPDATE_FRPC, buildUpdateFRPCRequest(req))
	return classifyApplyResponse(resp, err)
}

func classifyApplyError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return defs.ErrorCodeApplyTimeout
	}
	if strings.Contains(err.Error(), "cannot get client") {
		return defs.ErrorCodeAgentOffline
	}
	// connection broken mid-send or agent returned EVENT_ERROR: the apply did
	// not complete, so the transaction must compensate.
	return defs.ErrorCodeAgentOffline
}

// buildFrpcApplyConfig is the PURE config-building half of the legacy
// master-side UpdateFrpcHander (the other half was DB-first + async RPC and
// is NOT reused). It normalizes the candidate client config into the final
// payload sent to the agent: server address/port from the server row, frps
// URL override, user, and auth metadata. It performs no DB writes — the
// caller persists only after the synchronous ACK (spec §8.1 step 8/10).
//
// cli is the resolved child (shadow) client entity: it provides FrpsUrl and
// the child id stamped into the metadata; its ConfigContent is NOT read, the
// candidate bytes are authoritative.
func buildFrpcApplyConfig(c *app.Context, cli *models.ClientEntity, serverID string, candidate []byte) ([]byte, error) {
	userInfo := common.GetUserInfo(c)
	cliCfg, err := utils.LoadClientConfigNormal(candidate, true)
	if err != nil {
		return nil, fmt.Errorf("cannot load candidate client config: %w", err)
	}

	srv, err := dao.NewQuery(c).GetServerByServerID(userInfo, serverID)
	if err != nil || srv == nil || len(srv.ServerIP) == 0 || len(srv.ConfigContent) == 0 {
		return nil, fmt.Errorf("cannot get server, server is not prepared, id: [%s]", serverID)
	}
	srvConf, err := srv.GetConfigContent()
	if err != nil || srvConf == nil {
		return nil, fmt.Errorf("cannot get server config, id: [%s]", serverID)
	}

	cliCfg.ServerAddr = srv.ServerIP
	switch cliCfg.Transport.Protocol {
	case "kcp":
		cliCfg.ServerPort = srvConf.KCPBindPort
	case "quic":
		cliCfg.ServerPort = srvConf.QUICBindPort
	default:
		cliCfg.ServerPort = srvConf.BindPort
	}

	if len(cli.FrpsUrl) > 0 {
		parsed, err := validateFrpsURL(cli.FrpsUrl)
		if err != nil {
			return nil, fmt.Errorf("invalid frps url: %w", err)
		}
		cliCfg.ServerAddr = parsed.Hostname()
		cliCfg.ServerPort = cast.ToInt(parsed.Port())
		cliCfg.Transport.Protocol = parsed.Scheme
	}

	cliCfg.User = userInfo.GetUserName()
	if cliCfg.Metadatas == nil {
		cliCfg.Metadatas = make(map[string]string)
	}
	cliCfg.Metadatas[defs.FRPAuthTokenKey] = userInfo.GetToken()
	cliCfg.Metadatas[defs.FRPClientIDKey] = cli.ClientID

	newCfg := struct {
		v1.ClientCommonConfig
		Proxies  []v1.ProxyConfigurer   `json:"proxies,omitempty"`
		Visitors []v1.VisitorConfigurer `json:"visitors,omitempty"`
	}{
		ClientCommonConfig: cliCfg.ClientCommonConfig,
		Proxies:            lo.Map(cliCfg.Proxies, func(item v1.TypedProxyConfig, _ int) v1.ProxyConfigurer { return item.ProxyConfigurer }),
		Visitors:           lo.Map(cliCfg.Visitors, func(item v1.TypedVisitorConfig, _ int) v1.VisitorConfigurer { return item.VisitorConfigurer }),
	}
	return json.Marshal(newCfg)
}

// validateFrpsURL mirrors biz/master/client.ValidateFrpsUrl without pulling
// the client package into the apply path (keeps the builder dependency-light).
func validateFrpsURL(urlStr string) (*url.URL, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if parsed.Hostname() == "" {
		return nil, fmt.Errorf("frps url has no host")
	}
	return parsed, nil
}
