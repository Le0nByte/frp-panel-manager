package server

import (
	"os"
	"testing"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/utils/portinspector"
	"github.com/stretchr/testify/assert"
)

func obs(pid int32, state pb.PortState) *pb.PortObservation {
	return &pb.PortObservation{
		Pid:    pid,
		State:  state,
		NodeId: "srv-1",
	}
}

// TestMarkEmbeddedFRPSOnlySelfPID: only sockets owned by THIS process (the
// embedded fatedier frps runs inside the Server Agent process) in
// LISTENING/ACTIVE state may be marked as the panel's frps. External
// processes — including another frps binary — must never be marked.
func TestMarkEmbeddedFRPSOnlySelfPID(t *testing.T) {
	self := int32(os.Getpid())

	cases := []struct {
		name   string
		o      *pb.PortObservation
		expect bool
	}{
		{"self pid listening", obs(self, pb.PortState_STATE_LISTENING), true},
		{"self pid active", obs(self, pb.PortState_STATE_ACTIVE), true},
		{"self pid conflict not marked", obs(self, pb.PortState_STATE_CONFLICT), false},
		{"self pid available not marked", obs(self, pb.PortState_STATE_AVAILABLE), false},
		{"self pid unknown not marked", obs(self, pb.PortState_STATE_UNKNOWN), false},
		{"foreign pid listening never marked", obs(self+1000, pb.PortState_STATE_LISTENING), false},
		{"foreign pid active never marked", obs(self+1000, pb.PortState_STATE_ACTIVE), false},
		{"pid zero listening never marked", obs(0, pb.PortState_STATE_LISTENING), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			markEmbeddedFRPS(tc.o)
			if tc.expect {
				assert.Equal(t, portinspector.ServiceEmbeddedFRPS, tc.o.GetServiceName())
			} else {
				assert.NotEqual(t, portinspector.ServiceEmbeddedFRPS, tc.o.GetServiceName(), "external/other-state sockets must not be marked as the panel frps")
			}
		})
	}
}

// TestMarkEmbeddedFRPSAll: the batch helper marks exactly the qualifying
// observations and leaves the rest untouched.
func TestMarkEmbeddedFRPSAll(t *testing.T) {
	self := int32(os.Getpid())
	list := []*pb.PortObservation{
		obs(self, pb.PortState_STATE_LISTENING),
		obs(self+1, pb.PortState_STATE_LISTENING),
		obs(self, pb.PortState_STATE_UNKNOWN),
	}
	markEmbeddedFRPSAll(list)
	assert.Equal(t, portinspector.ServiceEmbeddedFRPS, list[0].GetServiceName())
	assert.NotEqual(t, portinspector.ServiceEmbeddedFRPS, list[1].GetServiceName())
	assert.NotEqual(t, portinspector.ServiceEmbeddedFRPS, list[2].GetServiceName())
}
