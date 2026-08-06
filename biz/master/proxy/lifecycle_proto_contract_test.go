package proxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Lifecycle proto contract (spec §7.3, D2b B1): the three lifecycle
// requests (delete/stop/start) mirror Create/Update with an idempotency_key
// on wire field 12, and their responses carry a structured error_code on
// wire field 2. These tests pin the generated Go surface, the wire numbers
// and a marshal/unmarshal round trip (a getter-only fake cannot pass).
func TestLifecycleDeleteRequestIdempotencyKey(t *testing.T) {
	req := &pb.DeleteProxyConfigRequest{
		ClientId:      proto.String("c1"),
		ServerId:      proto.String("s1"),
		Name:          proto.String("ssh"),
		IdempotencyKey: proto.String("idem-delete-1"),
	}
	if got := req.GetIdempotencyKey(); got != "idem-delete-1" {
		t.Fatalf("GetIdempotencyKey() = %q, want idem-delete-1", got)
	}
	assertWireFieldNumber(t, req.ProtoReflect().Descriptor(), "idempotency_key", 12)
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.DeleteProxyConfigRequest
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetIdempotencyKey() != "idem-delete-1" {
		t.Fatalf("round trip lost idempotency_key = %q", got.GetIdempotencyKey())
	}
	if got.GetName() != "ssh" {
		t.Fatalf("round trip lost name = %q", got.GetName())
	}
}

func TestLifecycleStartRequestIdempotencyKey(t *testing.T) {
	req := &pb.StartProxyRequest{
		ClientId:       proto.String("c1"),
		ServerId:       proto.String("s1"),
		Name:           proto.String("ssh"),
		IdempotencyKey: proto.String("idem-start-1"),
	}
	if got := req.GetIdempotencyKey(); got != "idem-start-1" {
		t.Fatalf("GetIdempotencyKey() = %q, want idem-start-1", got)
	}
	assertWireFieldNumber(t, req.ProtoReflect().Descriptor(), "idempotency_key", 12)
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.StartProxyRequest
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetIdempotencyKey() != "idem-start-1" {
		t.Fatalf("round trip lost idempotency_key = %q", got.GetIdempotencyKey())
	}
}

func TestLifecycleStopRequestIdempotencyKey(t *testing.T) {
	req := &pb.StopProxyRequest{
		ClientId:       proto.String("c1"),
		ServerId:       proto.String("s1"),
		Name:           proto.String("ssh"),
		IdempotencyKey: proto.String("idem-stop-1"),
	}
	if got := req.GetIdempotencyKey(); got != "idem-stop-1" {
		t.Fatalf("GetIdempotencyKey() = %q, want idem-stop-1", got)
	}
	assertWireFieldNumber(t, req.ProtoReflect().Descriptor(), "idempotency_key", 12)
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.StopProxyRequest
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetIdempotencyKey() != "idem-stop-1" {
		t.Fatalf("round trip lost idempotency_key = %q", got.GetIdempotencyKey())
	}
}

func TestLifecycleDeleteResponseErrorCode(t *testing.T) {
	resp := &pb.DeleteProxyConfigResponse{
		Status:    &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "port busy"},
		ErrorCode: proto.String("PORT_REMOTE_IN_USE"),
	}
	if got := resp.GetErrorCode(); got != "PORT_REMOTE_IN_USE" {
		t.Fatalf("GetErrorCode() = %q, want PORT_REMOTE_IN_USE", got)
	}
	assertWireFieldNumber(t, resp.ProtoReflect().Descriptor(), "error_code", 2)
	raw, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.DeleteProxyConfigResponse
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetErrorCode() != "PORT_REMOTE_IN_USE" {
		t.Fatalf("round trip lost error_code = %q", got.GetErrorCode())
	}
	if got.GetStatus() == nil || got.GetStatus().GetCode() != pb.RespCode_RESP_CODE_INVALID {
		t.Fatalf("status must survive the round trip")
	}
}

func TestLifecycleStartResponseErrorCode(t *testing.T) {
	resp := &pb.StartProxyResponse{
		Status:    &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "gate denied"},
		ErrorCode: proto.String("PORT_REMOTE_IN_USE"),
	}
	if got := resp.GetErrorCode(); got != "PORT_REMOTE_IN_USE" {
		t.Fatalf("GetErrorCode() = %q, want PORT_REMOTE_IN_USE", got)
	}
	assertWireFieldNumber(t, resp.ProtoReflect().Descriptor(), "error_code", 2)
	raw, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.StartProxyResponse
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetErrorCode() != "PORT_REMOTE_IN_USE" {
		t.Fatalf("round trip lost error_code = %q", got.GetErrorCode())
	}
}

func TestLifecycleStopResponseErrorCode(t *testing.T) {
	resp := &pb.StopProxyResponse{
		Status:    &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "not running"},
		ErrorCode: proto.String("PORT_NOT_RUNNING"),
	}
	if got := resp.GetErrorCode(); got != "PORT_NOT_RUNNING" {
		t.Fatalf("GetErrorCode() = %q, want PORT_NOT_RUNNING", got)
	}
	assertWireFieldNumber(t, resp.ProtoReflect().Descriptor(), "error_code", 2)
	raw, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.StopProxyResponse
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetErrorCode() != "PORT_NOT_RUNNING" {
		t.Fatalf("round trip lost error_code = %q", got.GetErrorCode())
	}
}

// TestLifecycleGeneratedTSContract proves the regenerated TypeScript contract
// carries idempotencyKey on the three lifecycle requests and errorCode on the
// three lifecycle responses — block-scoped, so Create/Update hits elsewhere in
// the file cannot satisfy it, and preflight_token is not smuggled in.
func TestLifecycleGeneratedTSContract(t *testing.T) {
	path := filepath.Join("..", "..", "..", "www", "lib", "pb", "api_client.ts")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated TS contract: %v", err)
	}
	blocks := splitTSMessageBlocks(string(raw))
	for name, want := range map[string][2]string{
		"DeleteProxyConfigRequest": {"idempotencyKey?: string", "optional string idempotency_key = 12;"},
		"StartProxyRequest":        {"idempotencyKey?: string", "optional string idempotency_key = 12;"},
		"StopProxyRequest":         {"idempotencyKey?: string", "optional string idempotency_key = 12;"},
		"DeleteProxyConfigResponse": {"errorCode?: string", "optional string error_code = 2;"},
		"StartProxyResponse":        {"errorCode?: string", "optional string error_code = 2;"},
		"StopProxyResponse":         {"errorCode?: string", "optional string error_code = 2;"},
	} {
		block, ok := blocks[name]
		if !ok {
			t.Fatalf("message block %q not found in %s", name, path)
		}
		if !strings.Contains(block, want[1]) {
			t.Fatalf("%s block missing field marker %q", name, want[1])
		}
		if !strings.Contains(block, want[0]) {
			t.Fatalf("%s block missing declaration %q", name, want[0])
		}
	}
	// Anti-smear: the Delete block must not pick up Create/Update fields.
	if strings.Contains(blocks["DeleteProxyConfigRequest"], "preflightToken") {
		t.Fatalf("DeleteProxyConfigRequest must not gain preflight_token (D2b B1 scope)")
	}
}

func TestLifecycleAuditEventTypeDelete(t *testing.T) {
	if models.AuditEventTypeDelete != "delete" {
		t.Fatalf("AuditEventTypeDelete = %q, want delete", models.AuditEventTypeDelete)
	}
}

func TestLifecycleAuditEventTypeStart(t *testing.T) {
	if models.AuditEventTypeStart != "start" {
		t.Fatalf("AuditEventTypeStart = %q, want start", models.AuditEventTypeStart)
	}
}

func TestLifecycleAuditEventTypeStop(t *testing.T) {
	if models.AuditEventTypeStop != "stop" {
		t.Fatalf("AuditEventTypeStop = %q, want stop", models.AuditEventTypeStop)
	}
}

func assertWireFieldNumber(t *testing.T, md protoreflect.MessageDescriptor, name string, want protoreflect.FieldNumber) {
	t.Helper()
	fd := md.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		t.Fatalf("descriptor of %s has no field %q (wire contract missing)", md.FullName(), name)
	}
	if fd.Number() != want {
		t.Fatalf("field %s.%s = %d, want wire number %d", md.FullName(), name, fd.Number(), want)
	}
}

// splitTSMessageBlocks splits a protobuf-ts generated file on
// "export interface <Name> {" markers and returns each message body keyed by
// its message name. Class/$Type blocks are excluded because they do not start
// with the export-interface marker.
func splitTSMessageBlocks(content string) map[string]string {
	blocks := map[string]string{}
	for _, seg := range strings.Split(content, "export interface ") {
		head, body, found := strings.Cut(seg, " {\n")
		if !found {
			continue
		}
		blocks[strings.TrimSpace(head)] = body
	}
	return blocks
}
