package client

import (
	"reflect"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/client"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
)

// newClientHandler is the handler constructor used by UpdateFrpcHander.
// Injectable so tests can substitute a recording fake (constructing the
// real frp client Service would dial the configured server on Run).
var newClientHandler = client.NewClientHandler

func UpdateFrpcHander(ctx *app.Context, req *pb.UpdateFRPCRequest) (*pb.UpdateFRPCResponse, error) {
	// REDACTED: never log the raw request — req.Config embeds the FRP auth
	// token metadata (spec §10.9). Only ids and the config size are logged.
	logger.Logger(ctx).Infof("update frpc, client: [%s], server: [%s], replace: [%s], config bytes: [%d]",
		req.GetClientId(), req.GetServerId(), req.GetReplaceClientId(), len(req.GetConfig()))
	content := req.GetConfig()
	c, p, v, err := utils.LoadClientConfig(content, false)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot load config")
		return &pb.UpdateFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: err.Error()},
		}, err
	}

	ctrl := ctx.GetApp().GetClientController()

	// Directed handler switch (atomic shadow, round-8 I2): the candidate
	// was fully parsed/validated above; only now may the controller
	// change. When replace_client_id is set and differs from client_id,
	// stop and delete ONLY the (replace_client_id, server_id) handler —
	// never StopAll, never os.Exit. A missing replace target is tolerated
	// (idempotent): the forward apply may have timed out before the
	// switch, so the compensation inverse must be able to run against an
	// agent that never performed the original switch.
	replaceID := req.GetReplaceClientId()
	serverID := req.GetServerId()
	targetID := req.GetClientId()
	if replaceID != "" && replaceID != targetID {
		if old := ctrl.Get(replaceID, serverID); old != nil {
			old.Stop()
			ctrl.Delete(replaceID, serverID)
			logger.Logger(ctx).Infof("replaced client handler, id: [%s], server: [%s]", replaceID, serverID)
		} else {
			logger.Logger(ctx).Warnf("replace target not running, id: [%s], server: [%s] (idempotent)", replaceID, serverID)
		}
	}

	cli := ctrl.Get(targetID, serverID)
	if cli != nil {
		if reflect.DeepEqual(c, cli.GetCommonCfg()) {
			logger.Logger(ctx).Warnf("client common config not changed")
			cli.Update(p, v)
		} else {
			cli.Stop()
			ctrl.Delete(targetID, serverID)
			ctrl.Add(targetID, serverID, newClientHandler(c, p, v))
			ctrl.Run(targetID, serverID)
		}
		logger.Logger(ctx).Infof("update client, id: [%s] success, running", targetID)
	} else {
		ctrl.Add(targetID, serverID, newClientHandler(c, p, v))
		ctrl.Run(targetID, serverID)
		logger.Logger(ctx).Infof("add new client, id: [%s], running", targetID)
	}

	return &pb.UpdateFRPCResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}
