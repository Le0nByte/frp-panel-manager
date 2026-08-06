package rpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func CallClientWrapper[R common.RespType](c *app.Context, clientID string, event pb.Event, req proto.Message, resp *R) error {
	cresp, err := CallClient(c, clientID, event, req)
	if err != nil {
		return err
	}

	protoMsgRef, ok := any(resp).(protoreflect.ProtoMessage)
	if !ok {
		return fmt.Errorf("type does not implement protoreflect.ProtoMessage")
	}

	return proto.Unmarshal(cresp.GetData(), protoMsgRef)
}

func CallClient(ctx *app.Context, clientID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error) {
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return CallClientWithContext(ctx2, ctx.GetApp(), clientID, event, msg)
}

// CallClientWithContext sends an event to an agent and waits for the
// response, bounded by ctx. On timeout, cancellation or send failure the
// session is removed from ClientRecvMap (spec §8.4).
//
// The session channel is deliberately NOT closed during cleanup: Recv may
// already hold a reference and a concurrent close would panic on a late
// send. The channel is buffered (1) and deleted from the map, which makes
// late responses safe by construction: either the map lookup misses (the
// response is dropped) or the send lands in the buffer and is GC'd.
func CallClientWithContext(ctx context.Context, appInstance app.Application, clientID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error) {
	sender := appInstance.GetClientsManager().Get(clientID)
	if sender == nil {
		logger.Logger(ctx).Errorf("cannot get client, id: [%s]", clientID)
		return nil, fmt.Errorf("cannot get client, id: [%s]", clientID)
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot marshal")
		return nil, err
	}

	req := &pb.ServerMessage{
		Event:     event,
		Data:      data,
		SessionId: uuid.New().String(),
		ClientId:  clientID,
	}

	respCh := make(chan *pb.ClientMessage, 1)
	appInstance.GetClientRecvMap().Store(req.SessionId, respCh)
	defer func() {
		appInstance.GetClientRecvMap().Delete(req.SessionId)
	}()

	err = sender.Conn.Send(req)
	if err != nil {
		logger.Logger(context.Background()).WithError(err).Errorf("cannot send")
		appInstance.GetClientsManager().Remove(clientID)
		return nil, err
	}

	select {
	case <-ctx.Done():
		logger.Logger(ctx).WithError(ctx.Err()).Errorf("rpc call to client [%s] event [%s] timed out or cancelled", clientID, event)
		return nil, fmt.Errorf("rpc call to client [%s] event [%s] timeout or cancelled: %w", clientID, event, ctx.Err())
	case resp := <-respCh:
		if resp.Event == pb.Event_EVENT_ERROR {
			return nil, fmt.Errorf("client return error: %s", resp.Data)
		}
		return resp, nil
	}
}

// recvRetryDelay bounds the poll rate of Recv while the target agent is
// not registered (offline): the pump waits between attempts instead of
// busy-looping on the manager. Overridable in tests.
var recvRetryDelay = 100 * time.Millisecond

func Recv(appInstance app.Application, clientID string) chan bool {
	done := make(chan bool)
	go func() {
		c := context.Background()
		log := logger.Logger(c).WithField("clientID", clientID)
		for {
			reciver := appInstance.GetClientsManager().Get(clientID)
			if reciver == nil {
				log.Errorf("cannot get client")
				// Back off before retrying; also stoppable via the
				// context so a future cancellation never strands the
				// pump in a bare sleep.
				select {
				case <-time.After(recvRetryDelay):
				case <-c.Done():
					return
				}
				continue
			}
			resp, err := reciver.Conn.Recv()
			if err == io.EOF {
				log.Infof("finish client recv")
				done <- true
				return
			}
			if err != nil {
				log.WithError(err).Errorf("cannot recv, usually means client disconnect")
				done <- true
				return
			}

			respChAny, ok := appInstance.GetClientRecvMap().Load(resp.SessionId)
			if !ok {
				// session already cleaned up (timeout/cancel): late
				// response is discarded safely
				log.Debugf("drop late response for unknown session [%s]", resp.SessionId)
				continue
			}

			respCh, ok := respChAny.(chan *pb.ClientMessage)
			if !ok {
				log.Errorf("cannot cast")
				continue
			}
			log.Debugf("recv success, resp: %+v", resp)
			select {
			case respCh <- resp:
			default:
				// channel full or nobody reading: drop to avoid blocking
				log.Debugf("drop response for full session channel [%s]", resp.SessionId)
			}
		}
	}()
	return done
}
