package server

import (
	"context"
	"fmt"
	"reflect"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/server"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
)

func UpdateFrpsHander(ctx *app.Context, req *pb.UpdateFRPSRequest) (*pb.UpdateFRPSResponse, error) {
	logger.Logger(ctx).Info(UpdateFRPSReqSummary(req.GetServerId(), req.GetConfig()))

	content := req.GetConfig()

	s, err := utils.LoadServerConfig(content, true)
	if err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot load config")
		return &pb.UpdateFRPSResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
		}, err
	}

	InjectAuthPlugin(ctx, s)

	serverID := req.GetServerId()
	if cli := ctx.GetApp().GetServerController().Get(serverID); cli != nil {
		if !reflect.DeepEqual(cli.GetCommonCfg(), s) {
			cli.Stop()
			ctx.GetApp().GetServerController().Delete(serverID)
			logger.Logger(ctx).Infof("server %s config changed, will recreate it", serverID)
		} else {
			logger.Logger(ctx).Infof("server %s config not changed", serverID)
			return &pb.UpdateFRPSResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
			}, nil
		}
	}

	ctx.GetApp().GetServerController().Add(serverID, server.NewServerHandler(s))
	ctx.GetApp().GetServerController().Run(serverID)

	return &pb.UpdateFRPSResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}

// UpdateFRPSReqSummary returns a safe log summary of an update-request:
// only the server ID and the config byte length are included. The raw FRPS
// config bytes (which embed the FRP auth token) are never logged (spec
// §10.9). Package-local mirror of the master-side helper so the agent
// binary does not need to import master business code.
func UpdateFRPSReqSummary(serverID string, config []byte) string {
	return fmt.Sprintf("update frps, req: {server_id: %q, config_size: %d bytes}", serverID, len(config))
}
