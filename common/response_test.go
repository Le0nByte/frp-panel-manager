package common

import (
	"testing"

	"github.com/VaalaCat/frp-panel/pb"
)

// TestGetEventServerACKMapping is a regression test for the Server ACK event
// mapping bug: UpdateFRPSResponse / RemoveFRPSResponse must map to the FRPS
// events (EVENT_UPDATE_FRPS / EVENT_REMOVE_FRPS), not the FRPC events.
func TestGetEventServerACKMapping(t *testing.T) {
	cases := []struct {
		name string
		msg  interface{}
		want pb.Event
	}{
		{"UpdateFRPCResponse", &pb.UpdateFRPCResponse{}, pb.Event_EVENT_UPDATE_FRPC},
		{"RemoveFRPCResponse", &pb.RemoveFRPCResponse{}, pb.Event_EVENT_REMOVE_FRPC},
		{"UpdateFRPSResponse", &pb.UpdateFRPSResponse{}, pb.Event_EVENT_UPDATE_FRPS},
		{"RemoveFRPSResponse", &pb.RemoveFRPSResponse{}, pb.Event_EVENT_REMOVE_FRPS},
		{"StartFRPCResponse", &pb.StartFRPCResponse{}, pb.Event_EVENT_START_FRPC},
		{"StopFRPCResponse", &pb.StopFRPCResponse{}, pb.Event_EVENT_STOP_FRPC},
		{"StartFRPSResponse", &pb.StartFRPSResponse{}, pb.Event_EVENT_START_FRPS},
		{"StopFRPSResponse", &pb.StopFRPSResponse{}, pb.Event_EVENT_STOP_FRPS},
		{"CreateWireGuardResponse", &pb.CreateWireGuardResponse{}, pb.Event_EVENT_CREATE_WIREGUARD},
		{"GetWireGuardRuntimeInfoResponse", &pb.GetWireGuardRuntimeInfoResponse{}, pb.Event_EVENT_GET_WIREGUARD_RUNTIME_INFO},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := getEvent(tc.msg)
			if err != nil {
				t.Fatalf("getEvent(%T) unexpected error: %v", tc.msg, err)
			}
			if got != tc.want {
				t.Fatalf("getEvent(%T) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// TestGetEventNewPortEvents verifies the new port-manager response types map
// to their agent events (spec §4.2).
func TestGetEventNewPortEvents(t *testing.T) {
	cases := []struct {
		name string
		msg  interface{}
		want pb.Event
	}{
		{"InspectPortsResponse", &pb.InspectPortsResponse{}, pb.Event_EVENT_INSPECT_PORTS},
		{"ValidatePortResponse", &pb.ValidatePortResponse{}, pb.Event_EVENT_VALIDATE_PORT},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := getEvent(tc.msg)
			if err != nil {
				t.Fatalf("getEvent(%T) unexpected error: %v", tc.msg, err)
			}
			if got != tc.want {
				t.Fatalf("getEvent(%T) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// TestGetEventUnknownType verifies unknown response types are rejected loudly
// instead of silently mapping to a wrong event.
func TestGetEventUnknownType(t *testing.T) {
	if _, _, err := getEvent(&pb.CommonRequest{}); err == nil {
		t.Fatal("expected error for unregistered response type")
	}
}
