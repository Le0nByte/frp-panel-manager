package master

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/VaalaCat/frp-panel/biz/master/client"
	masterserver "github.com/VaalaCat/frp-panel/biz/master/server"
	"github.com/VaalaCat/frp-panel/biz/master/shell"
	"github.com/VaalaCat/frp-panel/biz/master/streamlog"
	"github.com/VaalaCat/frp-panel/biz/master/wg"
	"github.com/VaalaCat/frp-panel/biz/master/worker"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gorm.io/gorm"
)

type server struct {
	pb.UnimplementedMasterServer
	appInstance app.Application
	// guard is the process-wide I3-A/I3-B admission guard injected by fx.
	// Nil keeps the legacy behavior for standalone callers/tests: the
	// helpers and ServerSend simply skip admission gating.
	guard *rpcguard.Guard
}

// newServer builds the MasterServer implementation. guards is variadic so
// legacy call sites keep compiling without one; production fx always
// passes the SAME guard instance to every MasterService.
func newServer(appInstance app.Application, guards ...*rpcguard.Guard) *server {
	s := &server{appInstance: appInstance}
	if len(guards) > 0 {
		s.guard = guards[0]
	}
	return s
}

// errRPCAuthInvalid is the unified generic error returned for every
// rejected register/secret attempt. It never leaks whether the node
// exists, why the attempt failed, or any secret material.
var errRPCAuthInvalid = errors.New("rpc auth token is invalid")

// appCtx wraps the ORIGINAL request context (peer preserved) with the
// guard and the app instance. Inbound methods MUST use this instead of
// app.NewContext(context.Background(), ...) so the transport peer survives
// into the guard helpers (I3-B).
func (s *server) appCtx(ctx context.Context) *app.Context {
	return app.NewContext(rpcguard.ContextWithGuard(ctx, s.guard), s.appInstance)
}

func (s *server) ReportWireGuardRuntimeInfo(ctx context.Context, req *pb.ReportWireGuardRuntimeInfoReq) (*pb.ReportWireGuardRuntimeInfoResp, error) {
	logger.Logger(ctx).Debugf("report wireguard runtime info, clientID: [%s], interfaceName: [%s]", req.GetBase().GetClientId(), req.GetInterfaceName())
	appCtx := s.appCtx(ctx)

	if client, err := client.ValidateClientRequest(appCtx, req.GetBase()); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot validate client request")
		return nil, err
	} else if client.Stopped {
		logger.Logger(appCtx).Infof("report wireguard runtime info, client [%s] is stopped", req.GetBase().GetClientId())
		return &pb.ReportWireGuardRuntimeInfoResp{
			Status: &pb.Status{
				Code:    pb.RespCode_RESP_CODE_NOT_FOUND,
				Message: "client stopped",
			},
		}, nil
	}
	logger.Logger(appCtx).Debugf("validate client success, clientID: [%s], interfaceName: [%s]", req.GetBase().GetClientId(), req.GetInterfaceName())

	return wg.ReportWireGuardRuntimeInfo(appCtx, req)
}

// ListClientWireGuards implements pb.MasterServer.
func (s *server) ListClientWireGuards(ctx context.Context, req *pb.ListClientWireGuardsRequest) (*pb.ListClientWireGuardsResponse, error) {
	logger.Logger(ctx).Debugf("list client wire guards, clientID: [%s]", req.GetBase().GetClientId())
	// S5b gate: fail closed with a successful empty answer BEFORE request
	// validation or any DAO access while the master feature is off.
	if !s.appInstance.GetConfig().Master.Features.EnableWireGuard {
		return &pb.ListClientWireGuardsResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS},
		}, nil
	}
	appCtx := s.appCtx(ctx)

	if client, err := client.ValidateClientRequest(appCtx, req.GetBase()); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot validate client request")
		return nil, err
	} else if client.Stopped {
		logger.Logger(appCtx).Infof("list client wire guards, client [%s] is stopped", req.GetBase().GetClientId())
		return &pb.ListClientWireGuardsResponse{
			Status: &pb.Status{
				Code:    pb.RespCode_RESP_CODE_NOT_FOUND,
				Message: "client stopped",
			},
		}, nil
	}

	resp, err := wg.ListClientWireGuards(appCtx, req)
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot list client wire guards")
		return nil, err
	}

	return resp, nil
}

// ListClientWorkers implements pb.MasterServer.
func (s *server) ListClientWorkers(ctx context.Context, req *pb.ListClientWorkersRequest) (*pb.ListClientWorkersResponse, error) {
	logger.Logger(ctx).Debugf("list client workers, clientID: [%s]", req.GetBase().GetClientId())
	// S5b gate: fail closed with a successful empty answer BEFORE request
	// validation or any DAO access while the master feature is off.
	if !s.appInstance.GetConfig().Master.Features.EnableWorker {
		return &pb.ListClientWorkersResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS},
		}, nil
	}
	appCtx := s.appCtx(ctx)

	if client, err := client.ValidateClientRequest(appCtx, req.GetBase()); err != nil {
		logger.Logger(ctx).WithError(err).Errorf("cannot validate client request")
		return nil, err
	} else if client.Stopped {
		logger.Logger(appCtx).Infof("list client workers, client [%s] is stopped", req.GetBase().GetClientId())
		return &pb.ListClientWorkersResponse{
			Status: &pb.Status{
				Code:    pb.RespCode_RESP_CODE_NOT_FOUND,
				Message: "client stopped",
			},
		}, nil
	}

	logger.Logger(appCtx).Debugf("validate client success, clientID: [%s]", req.GetBase().GetClientId())

	return worker.ListClientWorkers(appCtx, req)
}

func newRpcServer(appInstance app.Application, creds credentials.TransportCredentials, guards ...*rpcguard.Guard) *grpc.Server {
	// s := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	// s := grpc.NewServer(grpc.Creds(creds))
	s := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterMasterServer(s, newServer(appInstance, guards...))
	return s
}

func runRpcServer(appInstance app.Application, s *grpc.Server) {
	ctx := context.Background()

	lis, err := net.Listen("tcp", conf.RPCListenAddr(appInstance.GetConfig()))
	if err != nil {
		logger.Logger(ctx).Fatalf("rpc server failed to listen: %v", err)
	}

	logger.Logger(ctx).Infof("start server")
	if err := s.Serve(lis); err != nil {
		logger.Logger(ctx).Fatalf("failed to serve: %v", err)
	}
}

// PullClientConfig implements pb.MasterServer.
func (s *server) PullClientConfig(ctx context.Context, req *pb.PullClientConfigReq) (*pb.PullClientConfigResp, error) {
	logger.Logger(ctx).Debugf("pull client config, clientID: [%+v]", req.GetBase().GetClientId())
	return client.RPCPullConfig(s.appCtx(ctx), req)
}

// PullServerConfig implements pb.MasterServer.
func (s *server) PullServerConfig(ctx context.Context, req *pb.PullServerConfigReq) (*pb.PullServerConfigResp, error) {
	logger.Logger(ctx).Debugf("pull server config, serverID: [%+v]", req.GetBase().GetServerId())
	return masterserver.RPCPullConfig(s.appCtx(ctx), req)
}

// FRPCAuth implements pb.MasterServer.
func (s *server) FRPCAuth(ctx context.Context, req *pb.FRPAuthRequest) (*pb.FRPAuthResponse, error) {
	logger.Logger(ctx).Debugf("frpc auth, user: [%+v] ,serverID: [%+v]", req.GetUser(), req.GetBase().GetServerId())
	return masterserver.FRPAuth(s.appCtx(ctx), req)
}

// ServerSend implements pb.MasterServer.
func (s *server) ServerSend(sender pb.Master_ServerSendServer) error {
	// The app context is built from sender.Context() — NOT
	// context.Background() — so the transport peer survives into the
	// guard admission checks (I3-B).
	ctx := s.appCtx(sender.Context())

	logger.Logger(ctx).Infof("server get a client connected")
	var done chan bool
	for {
		req, err := sender.Recv()
		if err == io.EOF {
			logger.Logger(ctx).Infof("finish server send, client id: [%s]", req.GetClientId())
			return nil
		}

		if err != nil {
			logger.Logger(ctx).WithError(err).Errorf("cannot recv from client, id: [%s]", req.GetClientId())
			return err
		}

		cliType := ""

		if req.GetEvent() == pb.Event_EVENT_REGISTER_CLIENT || req.GetEvent() == pb.Event_EVENT_REGISTER_SERVER {
			peer := rpcguard.PeerFromContext(sender.Context())
			node := req.GetClientId()
			layer := rpcguard.LayerClient
			if req.GetEvent() == pb.Event_EVENT_REGISTER_SERVER {
				layer = rpcguard.LayerServer
			}

			// I3-B admission, in strict order and BEFORE any DAO access.
			// Every rejection sends the byte-identical legacy message and
			// returns the unified generic error — the reason (rate, lock,
			// missing row, DB trouble) is never leaked.
			if s.guard != nil && !s.guard.AllowRegister(peer, node) {
				_ = sender.Send(&pb.ServerMessage{
					Event: req.GetEvent(),
					Data:  []byte("rpc auth token is invalid"),
				})
				return errRPCAuthInvalid
			}
			if s.guard != nil && !s.guard.AllowSecretAttempt(peer, node) {
				s.guard.RecordSecretFailure(peer, node, layer)
				_ = sender.Send(&pb.ServerMessage{
					Event: req.GetEvent(),
					Data:  []byte("rpc auth token is invalid"),
				})
				return errRPCAuthInvalid
			}
			if len(req.GetSecret()) == 0 {
				if s.guard != nil {
					s.guard.RecordSecretFailure(peer, node, layer)
				}
				logger.Logger(ctx).Errorf("rpc auth token is empty")
				_ = sender.Send(&pb.ServerMessage{
					Event: req.GetEvent(),
					Data:  []byte("rpc auth token is invalid"),
				})
				return errRPCAuthInvalid
			}

			var secret string
			switch req.GetEvent() {
			case pb.Event_EVENT_REGISTER_CLIENT:
				cli, err := dao.NewQuery(ctx).AdminGetClientByClientID(req.GetClientId())
				if err != nil {
					if s.guard != nil && errors.Is(err, gorm.ErrRecordNotFound) {
						s.guard.RecordSecretFailure(peer, node, layer)
					}
					logger.Logger(ctx).WithError(err).Errorf("cannot get client, %s id: [%s]", req.GetEvent().String(), req.GetClientId())
					_ = sender.Send(&pb.ServerMessage{
						Event: req.GetEvent(),
						Data:  []byte("rpc auth token is invalid"),
					})
					return errRPCAuthInvalid
				}
				secret = cli.ConnectSecret
				cliType = defs.CliTypeClient
			case pb.Event_EVENT_REGISTER_SERVER:
				srv, err := dao.NewQuery(ctx).AdminGetServerByServerID(req.GetClientId())
				if err != nil {
					if s.guard != nil && errors.Is(err, gorm.ErrRecordNotFound) {
						s.guard.RecordSecretFailure(peer, node, layer)
					}
					logger.Logger(ctx).WithError(err).Errorf("cannot get server, %s id: [%s]", req.GetEvent().String(), req.GetClientId())
					_ = sender.Send(&pb.ServerMessage{
						Event: req.GetEvent(),
						Data:  []byte("rpc auth token is invalid"),
					})
					return errRPCAuthInvalid
				}
				secret = srv.ConnectSecret
				cliType = defs.CliTypeServer
			}

			if secret != req.GetSecret() {
				if s.guard != nil {
					s.guard.RecordSecretFailure(peer, node, layer)
				}
				logger.Logger(ctx).Errorf("invalid secret, %s id: [%s]", req.GetEvent().String(), req.GetClientId())
				_ = sender.Send(&pb.ServerMessage{
					Event: req.GetEvent(),
					Data:  []byte("rpc auth token is invalid"),
				})
				return errRPCAuthInvalid
			}
			if s.guard != nil {
				s.guard.RecordSecretSuccess(peer, node)
			}

			if cliType == defs.CliTypeClient {
				if err := dao.NewMutation(ctx).AdminUpdateClientLastSeen(req.GetClientId()); err != nil {
					logger.Logger(ctx).Errorf("cannot update client last seen, %s id: [%s]", req.GetEvent().String(), req.GetClientId())
				}
			}

			s.appInstance.GetClientsManager().Set(req.GetClientId(), cliType, sender)
			done = rpc.Recv(s.appInstance, req.GetClientId())
			_ = sender.Send(&pb.ServerMessage{
				Event:     req.GetEvent(),
				ClientId:  req.GetClientId(),
				SessionId: req.GetClientId(),
			})
			// Success log carries only event/clientID/type — never the
			// request (which contains the ConnectSecret).
			logger.Logger(ctx).Infof("register success, event: [%s], clientID: [%s], type: [%s]", req.GetEvent().String(), req.GetClientId(), cliType)
			break
		}
	}
	<-done
	return nil
}

// PushProxyInfo implements pb.MasterServer.
func (s *server) PushProxyInfo(ctx context.Context, req *pb.PushProxyInfoReq) (*pb.PushProxyInfoResp, error) {
	logger.Logger(ctx).Debugf("push proxy info, req server: [%+v]", req.GetProxyInfos())
	return masterserver.PushProxyInfo(s.appCtx(ctx), req)
}

func (s *server) PushClientStreamLog(sender pb.Master_PushClientStreamLogServer) error {
	return streamlog.PushClientStreamLog(s.appCtx(sender.Context()), sender)
}

func (s *server) PushServerStreamLog(sender pb.Master_PushServerStreamLogServer) error {
	return streamlog.PushServerStreamLog(s.appCtx(sender.Context()), sender)
}

func (s *server) PTYConnect(sender pb.Master_PTYConnectServer) error {
	return shell.PTYConnect(s.appCtx(sender.Context()), sender)
}
