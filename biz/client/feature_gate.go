package client

import (
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/pb"
)

// featureGateBlock decides whether a server event must fail closed while its
// feature is disabled (S5b). It returns the exact EVENT_ERROR message the
// client must answer with, or nil to let the legacy dispatch run.
//
// Feature mapping (by pb.Event enum name, never magic numbers):
//   - PTY class (EVENT_START_PTY_CONNECT)            -> Client.Features.EnableRemoteShell
//   - Worker class (CREATE/REMOVE/GET_STATUS/INSTALL_WORKERD)
//     -> Client.Features.EnableFunctions
//   - WireGuard class (CREATE/DELETE/UPDATE/GET_RUNTIME_INFO/RESTART)
//     -> Client.Features.EnableWireGuard
//
// Every other event (frpc/frps lifecycle + ACK, PING/PONG, stream log, proxy
// info, upgrade, port inspector/preflight) is never intercepted.
func featureGateBlock(cfg conf.Config, event pb.Event) *pb.ClientMessage {
	var feature string

	switch event {
	case pb.Event_EVENT_START_PTY_CONNECT:
		feature = "remote shell"
		if cfg.Client.Features.EnableRemoteShell {
			return nil
		}
	case pb.Event_EVENT_CREATE_WORKER, pb.Event_EVENT_REMOVE_WORKER,
		pb.Event_EVENT_GET_WORKER_STATUS, pb.Event_EVENT_INSTALL_WORKERD:
		feature = "functions"
		if cfg.Client.Features.EnableFunctions {
			return nil
		}
	case pb.Event_EVENT_CREATE_WIREGUARD, pb.Event_EVENT_DELETE_WIREGUARD,
		pb.Event_EVENT_UPDATE_WIREGUARD, pb.Event_EVENT_GET_WIREGUARD_RUNTIME_INFO,
		pb.Event_EVENT_RESTART_WIREGUARD:
		feature = "wireguard"
		if cfg.Client.Features.EnableWireGuard {
			return nil
		}
	default:
		return nil
	}

	return &pb.ClientMessage{
		Event: pb.Event_EVENT_ERROR,
		Data:  []byte("feature disabled: " + feature),
	}
}
