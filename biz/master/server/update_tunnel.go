package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/samber/lo"
)

var ErrLegacyFRPSUpdateDisabled = errors.New("legacy raw frps update endpoint is disabled; use the managed proxy API")

// RejectLegacyFRPSUpdate keeps the public route fail-closed. Raw FRPS JSON
// performs a DB-first, asynchronous Agent update and cannot satisfy the
// transaction/snapshot/ACK/rollback contract. Agent-side EVENT_UPDATE_FRPS
// handling remains available to managed internal flows.
func RejectLegacyFRPSUpdate(_ *app.Context, _ *pb.UpdateFRPSRequest) (*pb.UpdateFRPSResponse, error) {
	return &pb.UpdateFRPSResponse{
		Status: &pb.Status{
			Code:    pb.RespCode_RESP_CODE_INVALID,
			Message: "legacy raw frps update endpoint is disabled; use the managed proxy API",
		},
	}, ErrLegacyFRPSUpdateDisabled
}

func UpdateFrpsHander(c *app.Context, req *pb.UpdateFRPSRequest) (*pb.UpdateFRPSResponse, error) {
	logger.Logger(c).Info(UpdateFRPSReqSummary(req.GetServerId(), req.GetConfig()))
	var (
		serverID  = req.GetServerId()
		configStr = req.GetConfig()
		userInfo  = common.GetUserInfo(c)
	)

	if len(configStr) == 0 || len(serverID) == 0 {
		return nil, fmt.Errorf("request invalid")
	}

	q := dao.NewQuery(c)
	srv, err := q.GetServerByServerID(userInfo, serverID)
	if srv == nil || err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot get server, id: [%s]", serverID)
		return nil, err
	}

	srvCfg, err := utils.LoadServerConfig(configStr, true)
	if srvCfg == nil || err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot load server config")
		return nil, err
	}

	lo.Filter(srvCfg.HTTPPlugins, func(item v1.HTTPPluginOptions, _ int) bool {
		return item.Name != defs.FRP_Plugin_Multiuser
	})
	srvCfg.HTTPPlugins = append(srvCfg.HTTPPlugins, conf.FRPsAuthOption(c.GetApp().GetConfig()))

	if err := srv.SetConfigContent(srvCfg); err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot set server config")
		return nil, err
	}

	srv.Comment = req.GetComment()
	if len(req.GetServerIp()) > 0 {
		srv.ServerIP = req.GetServerIp()
	}

	if len(req.GetFrpsUrls()) > 0 {
		srv.FrpsUrls = req.GetFrpsUrls()
	}

	if err := dao.NewMutation(c).UpdateServer(userInfo, srv); err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot update server, id: [%s]", serverID)
		return nil, err
	}

	go func() {
		resp, err := rpc.CallClient(app.NewContext(context.Background(), c.GetApp()), req.GetServerId(), pb.Event_EVENT_UPDATE_FRPS, req)
		if err != nil {
			logger.Logger(context.Background()).WithError(err).Errorf("update event send to server error, server id: [%s]", req.GetServerId())
		}
		if resp == nil {
			logger.Logger(c).Errorf("cannot get response, server id: [%s]", req.GetServerId())
		}
	}()

	logger.Logger(c).Infof("update frps success, id: [%s]", serverID)
	return &pb.UpdateFRPSResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}

// UpdateFRPSReqSummary returns a safe log summary of an update-request:
// only the server ID and the config byte length are included. The raw FRPS
// config bytes (which embed the FRP auth token) are never logged (spec
// §10.9). The signature is request-agnostic so sibling handlers (e.g.
// UpdateFRPC) can reuse it.
func UpdateFRPSReqSummary(serverID string, config []byte) string {
	return fmt.Sprintf("update frps, req: {server_id: %q, config_size: %d bytes}", serverID, len(config))
}
