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

func StartFRPCHandler(ctx *app.Context, req *pb.StartFRPCRequest) (*pb.StartFRPCResponse, error) {
	logger.Logger(ctx).Infof("master get a start client request, client id: [%s]", req.GetClientId())

	userInfo := common.GetUserInfo(ctx)
	clientID := req.GetClientId()

	if !userInfo.Valid() {
		return &pb.StartFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
		}, nil
	}

	if len(clientID) == 0 {
		return &pb.StartFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid client id"},
		}, nil
	}

	originID, unlock, err := lockClientOrigin(ctx, clientID)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot resolve origin client, id: [%s]", clientID)
		return nil, err
	}
	defer unlock()

	// EVENT_START_FRPC is a physical-Agent-level StartAll: a shadow child is
	// only ONE logical handler inside that process, so a child-addressed
	// request must never start the whole process (and would otherwise be
	// indistinguishable from the child's own rules). This legacy physical
	// lifecycle endpoint accepts origin ids only and fails closed before any
	// DB mutation.
	if originID != clientID {
		return &pb.StartFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "shadow child cannot be started through the physical client endpoint"},
		}, fmt.Errorf("cannot start shadow child [%s] through physical client endpoint", clientID)
	}

	cli, err := dao.NewQuery(ctx).GetClientByClientID(userInfo, originID)
	if err != nil {
		return nil, err
	}

	client := cli.ClientEntity

	client.Stopped = false

	if err = dao.NewMutation(ctx).UpdateClient(userInfo, client); err != nil {
		return nil, err
	}

	// DB-first ordering remains KNOWN legacy debt (the D2 transaction
	// migration makes it atomic); the per-origin lock + synchronous ACK
	// below only close the concurrency and silent-drift windows in the
	// interim. The lock is held across the write AND the agent ACK.
	startReq := &pb.StartFRPCRequest{ClientId: &originID}
	rpcCtx, cancel := context.WithTimeout(context.Background(), agentAckTimeout)
	defer cancel()
	resp, err := rpc.CallClientWithContext(rpcCtx, ctx.GetApp(), originID, pb.Event_EVENT_START_FRPC, startReq)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("start client event send to client error, client id: [%s]", originID)
		return nil, err
	}
	if err := requireAckSuccess(resp, &pb.StartFRPCResponse{}); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("start frpc rejected by agent, client id: [%s]", originID)
		return nil, err
	}

	return &pb.StartFRPCResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}
