package server

import (
	"testing"

	"github.com/VaalaCat/frp-panel/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRejectLegacyFRPSUpdateIsFailClosed(t *testing.T) {
	resp, err := RejectLegacyFRPSUpdate(nil, &pb.UpdateFRPSRequest{
		ServerId: proto.String("server"),
		Config:   []byte(`{"bindPort":7000}`),
	})
	require.ErrorIs(t, err, ErrLegacyFRPSUpdateDisabled)
	require.NotNil(t, resp)
	require.Equal(t, pb.RespCode_RESP_CODE_INVALID, resp.GetStatus().GetCode())
	require.Contains(t, resp.GetStatus().GetMessage(), "disabled")
}
