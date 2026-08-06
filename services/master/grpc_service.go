package master

import (
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type MasterService interface {
	Run()
	Stop()
	GetServer() *grpc.Server
}

type master struct {
	grpcServer  *grpc.Server
	appInstance app.Application
	// guard mirrors the guard injected into the registered server
	// implementation; kept on the wrapper for wiring tests and I3-C.
	guard *rpcguard.Guard
}

// NewMasterService builds a MasterService. guards is variadic for legacy
// callers; production fx always passes the SAME guard instance (via
// cmd/frpp/shared NewRPCGuard) so every TLS/HTTP/WS MasterService shares
// one admission state machine.
func NewMasterService(appInstance app.Application, creds credentials.TransportCredentials, guards ...*rpcguard.Guard) MasterService {
	s := newRpcServer(appInstance, creds, guards...)
	m := &master{
		grpcServer:  s,
		appInstance: appInstance,
	}
	if len(guards) > 0 {
		m.guard = guards[0]
	}
	return m
}

func (s *master) Run() {
	runRpcServer(s.appInstance, s.grpcServer)
}

func (s *master) Stop() {
	s.grpcServer.Stop()
}

func (s *master) GetServer() *grpc.Server {
	return s.grpcServer
}
