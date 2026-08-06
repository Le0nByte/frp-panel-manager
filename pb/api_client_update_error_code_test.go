package pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestUpdateProxyConfigResponseErrorCodeRoundTrip pins the §7.3 structured
// error-code field on UpdateProxyConfigResponse (the mirror of
// CreateProxyConfigResponse.error_code — the D2a design's only API-surface
// gap): the field must survive a marshal/unmarshal round trip, keep the
// status, and expose the generated getter. Field number 2, like Create.
func TestUpdateProxyConfigResponseErrorCodeRoundTrip(t *testing.T) {
	resp := &UpdateProxyConfigResponse{
		Status:    &Status{Code: RespCode_RESP_CODE_INVALID, Message: "port busy"},
		ErrorCode: proto.String("PORT_REMOTE_IN_USE"),
	}
	raw, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got UpdateProxyConfigResponse
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetErrorCode() != "PORT_REMOTE_IN_USE" {
		t.Fatalf("error_code = %q, want PORT_REMOTE_IN_USE", got.GetErrorCode())
	}
	if got.GetStatus() == nil || got.GetStatus().GetCode() != RespCode_RESP_CODE_INVALID {
		t.Fatalf("status must survive the round trip")
	}
}
