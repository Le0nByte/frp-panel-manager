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

func RemoveFrpcHandler(c *app.Context, req *pb.RemoveFRPCRequest) (*pb.RemoveFRPCResponse, error) {
	logger.Logger(c).Infof("remove physical frpc, id: [%s]", req.GetClientId())

	var (
		clientID = req.GetClientId()
		userInfo = common.GetUserInfo(c)
	)

	if !userInfo.Valid() {
		return &pb.RemoveFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
		}, nil
	}

	if len(clientID) == 0 {
		logger.Logger(c).Errorf("invalid client id")
		return nil, fmt.Errorf("invalid client id")
	}

	originID, unlock, err := lockClientOrigin(c, clientID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot resolve origin client, id: [%s]", clientID)
		return nil, err
	}
	defer unlock()
	if originID != clientID {
		return &pb.RemoveFRPCResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "shadow child cannot be removed through the physical client endpoint"},
		}, fmt.Errorf("cannot remove shadow child [%s] through physical client endpoint", clientID)
	}

	if _, err := dao.NewQuery(c).GetClientByClientID(userInfo, originID); err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot get client, id: [%s]", originID)
		return nil, err
	}

	err = dao.NewMutation(c).DeleteClient(userInfo, originID)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("cannot delete client, id: [%s]", originID)
		return nil, err
	}

	// Hold the shared origin lock until the physical Agent acknowledges. The
	// old goroutine returned SUCCESS before REMOVE_FRPC completed, allowing
	// another writer to race with a still-running client process.
	removeReq := &pb.RemoveFRPCRequest{ClientId: &originID}
	rpcCtx, cancel := context.WithTimeout(context.Background(), agentAckTimeout)
	defer cancel()
	resp, err := rpc.CallClientWithContext(rpcCtx, c.GetApp(), originID, pb.Event_EVENT_REMOVE_FRPC, removeReq)
	if err != nil {
		logger.Logger(c).WithError(err).Errorf("remove event send to client error, client id: [%s]", originID)
		return nil, err
	}
	if err := requireAckSuccess(resp, &pb.RemoveFRPCResponse{}); err != nil {
		logger.Logger(c).WithError(err).Errorf("remove frpc rejected by agent, client id: [%s]", originID)
		return nil, err
	}

	logger.Logger(c).Infof("remove frpc success, client id: [%s]", originID)
	return &pb.RemoveFRPCResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
	}, nil
}
