package client

import (
	"context"
	"fmt"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils/logger"
)

func DeleteClientHandler(ctx *app.Context, req *pb.DeleteClientRequest) (*pb.DeleteClientResponse, error) {
	logger.Logger(ctx).Infof("delete physical client, id: [%s]", req.GetClientId())

	userInfo := common.GetUserInfo(ctx)
	clientID := req.GetClientId()

	if !userInfo.Valid() {
		return &pb.DeleteClientResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
		}, nil
	}

	if len(clientID) == 0 {
		return &pb.DeleteClientResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid client id"},
		}, nil
	}

	originID, unlock, err := lockClientOrigin(ctx, clientID)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot resolve origin client, id: [%s]", clientID)
		return nil, err
	}
	defer unlock()

	// EVENT_REMOVE_FRPC terminates the physical Agent process after its ACK.
	// A shadow child is only one logical handler in that process, so routing a
	// child id through this endpoint would incorrectly take every sibling down.
	// D2 will provide transactional child/rule deletion; this legacy endpoint
	// therefore accepts physical origin ids only and fails closed otherwise.
	if originID != clientID {
		return &pb.DeleteClientResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "shadow child cannot be deleted through the physical client endpoint"},
		}, fmt.Errorf("cannot delete shadow child [%s] through physical client endpoint", clientID)
	}

	if err := dao.NewMutation(ctx).DeleteClient(userInfo, clientID); err != nil {
		return nil, err
	}

	// The pull-path failure memo is keyed by client id; drop it here so a
	// deleted client cannot leave a permanent entry behind (see
	// pullFailureLog in rpc_pull_config.go).
	clearPullFailure(clientID)

	if err := dao.NewMutation(ctx).DeleteProxyConfigsByClientIDOrOriginClientID(userInfo, clientID); err != nil {
		return nil, err
	}

	// Keep the shared origin lock until the physical Agent ACKs. This closes
	// the old fire-and-forget window where another writer could start while the
	// Agent still served a configuration whose DB rows had already been deleted.
	agentCtx := app.NewContext(context.Background(), ctx.GetApp())
	rpcCtx, cancel := context.WithTimeout(agentCtx, agentAckTimeout)
	defer cancel()
	resp, err := rpc.CallClientWithContext(rpcCtx, ctx.GetApp(), originID, pb.Event_EVENT_REMOVE_FRPC, req)
	if err != nil {
		logger.Logger(agentCtx).WithError(err).Errorf("remove event send to client error, client id: [%s]", originID)
		return nil, err
	}
	if err := requireAckSuccess(resp, &pb.RemoveFRPCResponse{}); err != nil {
		logger.Logger(agentCtx).WithError(err).Errorf("remove frpc rejected by agent, client id: [%s]", originID)
		return nil, err
	}

	return &pb.DeleteClientResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}
