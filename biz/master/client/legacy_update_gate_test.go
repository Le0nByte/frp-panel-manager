package client

import (
	"testing"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRejectLegacyFRPCUpdateIsFailClosed(t *testing.T) {
	resp, err := RejectLegacyFRPCUpdate(nil, &pb.UpdateFRPCRequest{
		ClientId: proto.String("client"),
		Config:   []byte(`{"proxies":[{"type":"tcp","remotePort":80}]}`),
	})
	require.ErrorIs(t, err, ErrLegacyFRPCUpdateDisabled)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Contains(t, resp.GetStatus().GetMessage(), "disabled")
}
