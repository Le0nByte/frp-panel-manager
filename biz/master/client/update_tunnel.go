package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// ErrLegacyFRPCUpdateDisabled is returned by the public compatibility route.
// Raw client JSON cannot be mapped safely to the transactional proxy API: it
// can contain multiple rules and unsupported proxy types. Internal callers
// that already hold the origin lock continue to use UpdateFrpcHanderLocked.
var ErrLegacyFRPCUpdateDisabled = errors.New("legacy raw frpc update endpoint is disabled; use the managed proxy API")

// RejectLegacyFRPCUpdate is deliberately fail-closed. Keeping the route
// shape avoids a breaking router change for old clients, while preventing a
// public caller from bypassing Preflight, reservation, snapshot, ACK and
// rollback requirements.
func RejectLegacyFRPCUpdate(_ *app.Context, _ *pb.UpdateFRPCRequest) (*pb.UpdateFRPCResponse, error) {
	return &pb.UpdateFRPCResponse{
		Status: &pb.Status{
			Code:    pb.RespCode_RESP_CODE_INVALID,
			Message: "legacy raw frpc update endpoint is disabled; use the managed proxy API",
		},
	}, ErrLegacyFRPCUpdateDisabled
}

// UpdateFrpcHander is an internal compatibility entry. It acquires the
// (tenant, origin) keyed lock — the SAME originlock.Shared instance the
// managed coordinators use — and delegates to UpdateFrpcHanderLocked. The
// public /frpc/update route is bound to RejectLegacyFRPCUpdate.
// The lock spans the whole operation: reads, DB writes and the
// synchronous agent ACK (round-8 I3: legacy writers bypassed the
// SafeCreate lock and silently lost updates).
//
// NOTE for internal callers: the proxy package handlers (Update/Delete/
// Start/Stop/CreateWorkerIngress) already hold the lock when they push
// the new client config to the agent — they MUST call
// UpdateFrpcHanderLocked, never this entry, or the same goroutine would
// deadlock on its own key.
func UpdateFrpcHander(c *app.Context, req *pb.UpdateFRPCRequest) (*pb.UpdateFRPCResponse, error) {
	_, unlock, err := lockClientOrigin(c, req.GetClientId())
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve origin client, id: [%s]", req.GetClientId())
		return nil, err
	}
	defer unlock()
	return UpdateFrpcHanderLocked(c, req)
}

// UpdateFrpcHanderLocked is the lock-free body of UpdateFrpcHander. The
// caller MUST already hold the (tenant, origin) keyed lock for the
// request's resolved origin (the proxy live writers hold it across their
// whole flow and call this directly).
func UpdateFrpcHanderLocked(c *app.Context, req *pb.UpdateFRPCRequest) (*pb.UpdateFRPCResponse, error) {
	// REDACTED: never log the raw request — req.Config embeds the FRP auth
	// token metadata (spec §10.9). Only ids and the config size are logged.
	logger.Logger(c).Infof("update frpc, client: [%s], server: [%s], config bytes: [%d]",
		req.GetClientId(), req.GetServerId(), len(req.GetConfig()))
	var (
		content     = req.GetConfig()
		serverID    = req.GetServerId()
		reqClientID = req.GetClientId() // may be shadow or child
		userInfo    = common.GetUserInfo(c)
	)
	q := dao.NewQuery(c)
	m := dao.NewMutation(c)

	cliCfg, err := utils.LoadClientConfigNormal(content, true)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot load config")
		return &pb.UpdateFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
		}, err
	}

	cliRecord, err := q.GetClientByClientID(userInfo, reqClientID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get client, id: [%s]", reqClientID)
		return &pb.UpdateFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "cannot get client"},
		}, fmt.Errorf("cannot get client")
	}

	cli := cliRecord.ClientEntity

	if cli.IsShadow {
		cli, _, err = ChildClientForServer(c, serverID, cli)
		if err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot get child client, id: [%s]", reqClientID)
			return &pb.UpdateFRPCResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "cannot get child client"},
			}, fmt.Errorf("cannot get child client")
		}
	}

	if cli.IsShadow && len(cli.ConfigContent) == 0 {
		logger.Logger(c).Warnf("client is shadowed, cannot update, id: [%s]", reqClientID)
		return nil, fmt.Errorf("client is shadowed, cannot update")
	}

	if !cli.IsShadow && len(cli.OriginClientID) == 0 {
		logger.Logger(c).Warnf("client is not shadowed, make it shadow, id: [%s]", reqClientID)
		cli, err = MakeClientShadowed(c, serverID, cli)
		if err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot make client shadowed, id: [%s]", reqClientID)
			return &pb.UpdateFRPCResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "cannot make client shadowed"},
			}, fmt.Errorf("cannot make client shadowed")
		}
	}

	srv, err := q.GetServerByServerID(userInfo, req.GetServerId())
	if err != nil || srv == nil || len(srv.ServerIP) == 0 || len(srv.ConfigContent) == 0 {
		logger.Logger(c).WithError(err).Errorf("cannot get server, server is not prepared, id: [%s]", req.GetServerId())
		return &pb.UpdateFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "cannot get server"},
		}, fmt.Errorf("cannot get server")
	}

	if len(srv.ConfigContent) == 0 {
		logger.Logger(c).Errorf("cannot get server, server is not prepared, id: [%s]", req.GetServerId())
		return &pb.UpdateFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "server is not prepared, pls update server config first"},
		}, fmt.Errorf("server is not prepared, pls update server config first")
	}

	srvConf, err := srv.GetConfigContent()
	if srvConf == nil || err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get server, id: [%s]", serverID)
		return nil, err
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

	if len(req.GetFrpsUrl()) > 0 || len(cli.FrpsUrl) > 0 {
		// 有一个有就需要覆盖，优先请求的url
		var (
			parsedFrpsUrl *url.URL
			err           error
			urlToParse    string
		)
		if len(req.GetFrpsUrl()) > 0 {
			parsedFrpsUrl, err = ValidateFrpsUrl(req.GetFrpsUrl())
			if err != nil {
				logger.Logger(c).WithError(err).Errorf("invalid frps url, url: [%s]", req.GetFrpsUrl())
				return &pb.UpdateFRPCResponse{
					Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
				}, err
			}
			urlToParse = req.GetFrpsUrl()
		}

		if len(cli.FrpsUrl) > 0 && parsedFrpsUrl == nil {
			parsedFrpsUrl, err = ValidateFrpsUrl(cli.FrpsUrl)
			if err != nil {
				logger.Logger(c).WithError(err).Errorf("invalid old frps url, url: [%s]", cli.FrpsUrl)
				return &pb.UpdateFRPCResponse{
					Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
				}, err
			}
			urlToParse = cli.FrpsUrl
		}

		cliCfg.ServerAddr = parsedFrpsUrl.Hostname()
		cliCfg.ServerPort = cast.ToInt(parsedFrpsUrl.Port())
		cliCfg.Transport.Protocol = parsedFrpsUrl.Scheme
		cli.FrpsUrl = urlToParse
	}

	cliCfg.User = userInfo.GetUserName()

	if cliCfg.Metadatas == nil {
		cliCfg.Metadatas = make(map[string]string)
	}

	cliCfg.Metadatas[defs.FRPAuthTokenKey] = userInfo.GetToken()
	// The wire target may differ from the route request: an origin-addressed
	// first update creates a shadow child. The FRP metadata must identify the
	// actual target handler, otherwise the Agent indexes it as the child while
	// frps observes the stale origin identity.
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

	rawCliConf, err := json.Marshal(newCfg)
	if err != nil {
		logger.Logger(c).WithError(err).Error("cannot marshal config")
		return nil, err
	}

	cli.ConfigContent = rawCliConf
	cli.ServerID = serverID
	if req.Comment != nil {
		cli.Comment = req.GetComment()
	}

	if err := m.UpdateClient(userInfo, cli); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot update client, id: [%s]", cli.ClientID)
		return nil, err
	}

	if err := m.RebuildProxyConfigFromClient(userInfo, &models.Client{ClientEntity: cli}); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot rebuild proxy config from client, id: [%s]", cli.ClientID)
		return nil, err
	}

	cliReq := &pb.UpdateFRPCRequest{
		ClientId: lo.ToPtr(cli.ClientID),
		ServerId: lo.ToPtr(serverID),
		Config:   rawCliConf,
	}
	if cli.OriginClientID != "" {
		// Every child update idempotently replaces the origin handler. This is
		// required for a first shadow and also cleans historical stale origin
		// handlers left by the old fire-and-forget path. Agent-side replacement
		// treats a missing origin handler as a no-op.
		cliReq.ReplaceClientId = lo.ToPtr(cli.OriginClientID)
	}

	// Synchronous agent ACK (round-8 I3): the previous go-func returned
	// SUCCESS before the agent had applied anything — silent DB/agent
	// drift. The handler now blocks until the physical agent ACKs (or
	// the agentAckTimeout budget expires), validates the ACK payload and
	// surfaces every failure as an error. The DB-first write above stays
	// a KNOWN legacy debt (the D2 migration makes the commit atomic);
	// the interim per-origin lock covers the whole span — read, DB write
	// AND the ACK — so no other writer can interleave in between.
	agentCtx := c.Background() // *app.Context for the DAO reads below
	cliToUpdate, err := dao.NewQuery(agentCtx).GetClientByFilter(userInfo, &models.ClientEntity{ClientID: cli.OriginClientID}, nil)
	if err != nil {
		logger.Logger(agentCtx).WithError(err).Errorf("cannot get origin client, id: [%s]", cli.OriginClientID)
		return nil, fmt.Errorf("cannot get origin client, id: [%s]: %w", cli.OriginClientID, err)
	}

	if cliToUpdate.Stopped {
		logger.Logger(agentCtx).Infof("client [%s] is stopped, do not send update event", cliToUpdate.ClientID)
	} else {
		rpcCtx, cancel := context.WithTimeout(agentCtx, agentAckTimeout)
		defer cancel()
		resp, err := rpc.CallClientWithContext(rpcCtx, c.GetApp(), cliToUpdate.ClientID, pb.Event_EVENT_UPDATE_FRPC, cliReq)
		if err != nil {
			logger.Logger(agentCtx).WithError(err).Errorf("update event send to client error, server: [%s], origin client: [%s]", serverID, cliToUpdate.ClientID)
			return nil, err
		}
		if err := requireAckSuccess(resp, &pb.UpdateFRPCResponse{}); err != nil {
			logger.Logger(agentCtx).WithError(err).Errorf("update frpc rejected by agent, server: [%s], origin client: [%s]", serverID, cliToUpdate.ClientID)
			return nil, err
		}
	}

	logger.Logger(c).Infof("update frpc success, client id: [%s]", reqClientID)
	return &pb.UpdateFRPCResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}
