package client

import (
	"fmt"
	"time"

	"github.com/VaalaCat/frp-panel/pb"
	"google.golang.org/protobuf/proto"
)

// agentAckTimeout bounds every synchronous agent ACK wait of the legacy
// live writers (frpc update / remove / start / stop + client delete).
// Overridable in tests (short budgets prove the timeout path).
var agentAckTimeout = 15 * time.Second

// statusCarrier is the common surface of the agent response types
// (UpdateFRPCResponse / RemoveFRPCResponse / StartFRPCResponse /
// StopFRPCResponse): every one carries a Status.
type statusCarrier interface {
	GetStatus() *pb.Status
}

// requireAckSuccess validates the agent's ACK payload: the response must
// exist, parse into the expected response type, and carry
// Status.Code == RESP_CODE_SUCCESS. Anything else — a transport error,
// an unparseable body, an explicit rejection — is an error the caller
// MUST surface (round-8 I3: fire-and-forget agent calls returned SUCCESS
// while the agent never applied the change — silent DB/agent drift).
func requireAckSuccess[T proto.Message](resp *pb.ClientMessage, out T) error {
	if resp == nil {
		return fmt.Errorf("agent returned no response")
	}
	if err := proto.Unmarshal(resp.GetData(), out); err != nil {
		return fmt.Errorf("cannot unmarshal agent ack: %w", err)
	}
	carrier, ok := any(out).(statusCarrier)
	if !ok {
		return fmt.Errorf("agent ack type %T carries no status", out)
	}
	st := carrier.GetStatus()
	if st == nil || st.GetCode() != pb.RespCode_RESP_CODE_SUCCESS {
		msg := "agent rejected the change"
		if st != nil && st.GetMessage() != "" {
			msg = st.GetMessage()
		}
		return fmt.Errorf("agent rejected the change: %s", msg)
	}
	return nil
}
