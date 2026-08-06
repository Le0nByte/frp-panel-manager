package pb

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// D2c-B audit detail wire contract (spec §7.1 POST /api/v1/audit/list,
// POST /api/v1/audit/get). The audit detail view must surface the
// ChangeTransaction details (operation/status/proxy_id) and the snapshot
// metadata (identity + digest only — never ciphertext/nonce), and the
// future rollback endpoint must be retry-safe and structured. These tests
// pin the generated Go surface, the wire numbers and a marshal/unmarshal
// round trip (a getter-only fake cannot pass).

// TestAuditEventDetailFieldsDescriptorAndRoundTrip pins the additive
// detail fields on common.AuditEvent: operation=9, status=10, proxy_id=11
// (fields 1-8 unchanged). The GetAudit handler maps the ChangeTransaction
// row onto the event so the UI can render the transaction detail.
func TestAuditEventDetailFieldsDescriptorAndRoundTrip(t *testing.T) {
	ev := &AuditEvent{
		TransactionId: proto.String("tx-1"),
		EventType:     proto.String("commit"),
		Summary:       proto.String("create tcp 8000 ok"),
		NodeId:        proto.String("n1"),
		Success:       proto.Bool(true),
		CreatedAt:     proto.Int64(1700000000),
		ErrorCode:     proto.String(""),
		EvidenceRef:   proto.String("change_transaction:tx-1#evidence_json"),
		Operation:     proto.String("create"),
		Status:        proto.String("committed"),
		ProxyId:       proto.Int64(42),
	}
	assertWireFieldNumber(t, ev.ProtoReflect().Descriptor(), "operation", 9)
	assertWireFieldNumber(t, ev.ProtoReflect().Descriptor(), "status", 10)
	assertWireFieldNumber(t, ev.ProtoReflect().Descriptor(), "proxy_id", 11)

	raw, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got AuditEvent
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetOperation() != "create" {
		t.Fatalf("round trip lost operation = %q", got.GetOperation())
	}
	if got.GetStatus() != "committed" {
		t.Fatalf("round trip lost status = %q", got.GetStatus())
	}
	if got.GetProxyId() != 42 {
		t.Fatalf("round trip lost proxy_id = %d", got.GetProxyId())
	}
	// the pre-existing fields survive alongside the additions
	if got.GetTransactionId() != "tx-1" || !got.GetSuccess() || got.GetCreatedAt() != 1700000000 {
		t.Fatalf("round trip lost pre-existing fields: tx=%q success=%v created_at=%d",
			got.GetTransactionId(), got.GetSuccess(), got.GetCreatedAt())
	}
}

// TestSnapshotMetadataContract pins the SnapshotMetadata message: it is the
// ONLY snapshot surface the GetAudit handler may expose (identity + digest
// metadata — side/node_type/node_id/sha256/created_at; ciphertext and nonce
// have NO wire fields, so they cannot leak by construction).
func TestSnapshotMetadataContract(t *testing.T) {
	sm := &SnapshotMetadata{
		Side:      proto.String("before"),
		NodeType:  proto.String("client"),
		NodeId:    proto.String("cli-1"),
		Sha256:    proto.String("abcdef0123456789"),
		CreatedAt: proto.Int64(1700000001),
	}
	md := sm.ProtoReflect().Descriptor()
	assertWireFieldNumber(t, md, "side", 1)
	assertWireFieldNumber(t, md, "node_type", 2)
	assertWireFieldNumber(t, md, "node_id", 3)
	assertWireFieldNumber(t, md, "sha256", 4)
	assertWireFieldNumber(t, md, "created_at", 5)
	// no ciphertext/nonce field may ever exist on the wire type
	if md.Fields().ByName("ciphertext") != nil || md.Fields().ByName("nonce") != nil {
		t.Fatalf("SnapshotMetadata must never carry ciphertext/nonce fields")
	}

	raw, err := proto.Marshal(sm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got SnapshotMetadata
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetSide() != "before" || got.GetNodeType() != "client" || got.GetNodeId() != "cli-1" {
		t.Fatalf("round trip lost metadata: side=%q node_type=%q node_id=%q",
			got.GetSide(), got.GetNodeType(), got.GetNodeId())
	}
	if got.GetSha256() != "abcdef0123456789" || got.GetCreatedAt() != 1700000001 {
		t.Fatalf("round trip lost digest/timestamp: sha256=%q created_at=%d", got.GetSha256(), got.GetCreatedAt())
	}
}

// TestGetAuditResponseSnapshotsContract pins the additive snapshots field
// (repeated SnapshotMetadata, wire field 4) on GetAuditResponse.
func TestGetAuditResponseSnapshotsContract(t *testing.T) {
	resp := &GetAuditResponse{
		Status: &Status{Code: RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Event:  &AuditEvent{TransactionId: proto.String("tx-1")},
		Snapshots: []*SnapshotMetadata{
			{Side: proto.String("before"), NodeId: proto.String("cli-1"), Sha256: proto.String("sum1")},
			{Side: proto.String("after"), NodeId: proto.String("cli-1"), Sha256: proto.String("sum2")},
		},
	}
	assertWireFieldNumber(t, resp.ProtoReflect().Descriptor(), "snapshots", 4)

	raw, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got GetAuditResponse
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.GetSnapshots()) != 2 {
		t.Fatalf("round trip lost snapshots: got %d", len(got.GetSnapshots()))
	}
	if got.GetSnapshots()[0].GetSide() != "before" || got.GetSnapshots()[1].GetSha256() != "sum2" {
		t.Fatalf("round trip lost snapshot metadata: side=%q sha256=%q",
			got.GetSnapshots()[0].GetSide(), got.GetSnapshots()[1].GetSha256())
	}
	if got.GetStatus() == nil || got.GetStatus().GetCode() != RespCode_RESP_CODE_SUCCESS {
		t.Fatalf("status must survive the round trip")
	}
}

// TestRollbackRequestIdempotencyKeyContract pins the additive
// idempotency_key (wire field 2) on RollbackRequest so D2c-C's rollback
// endpoint can be retry-safe (a network retry must never roll back twice).
func TestRollbackRequestIdempotencyKeyContract(t *testing.T) {
	req := &RollbackRequest{
		TransactionId:  proto.String("tx-1"),
		IdempotencyKey: proto.String("idem-rollback-1"),
	}
	assertWireFieldNumber(t, req.ProtoReflect().Descriptor(), "idempotency_key", 2)

	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RollbackRequest
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetIdempotencyKey() != "idem-rollback-1" {
		t.Fatalf("round trip lost idempotency_key = %q", got.GetIdempotencyKey())
	}
	if got.GetTransactionId() != "tx-1" {
		t.Fatalf("round trip lost transaction_id = %q", got.GetTransactionId())
	}
}

// TestRollbackResponseErrorCodeContract pins the additive error_code
// (wire field 3) on RollbackResponse for structured terminal failures
// (spec §7.3; D2c-C replays the stable code, never a Status-only string).
func TestRollbackResponseErrorCodeContract(t *testing.T) {
	resp := &RollbackResponse{
		Status:           &Status{Code: RespCode_RESP_CODE_INVALID, Message: "cannot roll back"},
		ErrorCode:        proto.String("ROLLBACK_FAILED"),
		NewTransactionId: proto.String("tx-rollback-1"),
	}
	assertWireFieldNumber(t, resp.ProtoReflect().Descriptor(), "error_code", 3)

	raw, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RollbackResponse
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetErrorCode() != "ROLLBACK_FAILED" {
		t.Fatalf("round trip lost error_code = %q", got.GetErrorCode())
	}
	if got.GetNewTransactionId() != "tx-rollback-1" {
		t.Fatalf("round trip lost new_transaction_id = %q", got.GetNewTransactionId())
	}
}

// TestAuditGeneratedTSContracts proves the regenerated TypeScript contract
// carries the detail fields — block-scoped, so identical fields elsewhere
// in the files cannot satisfy it.
func TestAuditGeneratedTSContracts(t *testing.T) {
	commonPath := filepath.Join("..", "www", "lib", "pb", "common.ts")
	commonRaw, err := os.ReadFile(commonPath)
	if err != nil {
		t.Fatalf("read generated TS contract: %v", err)
	}
	auditPath := filepath.Join("..", "www", "lib", "pb", "api_audit.ts")
	auditRaw, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read generated TS contract: %v", err)
	}
	commonBlocks := splitTSMessageBlocks(string(commonRaw))
	auditBlocks := splitTSMessageBlocks(string(auditRaw))

	// Each declaration is asserted individually: protobuf-ts emits a doc
	// comment block between consecutive fields, so a contiguous multi-field
	// string would never match.
	for name, wants := range map[string]struct {
		marker string
		decls  []string
	}{
		"AuditEvent": {
			marker: "optional string operation = 9;",
			decls:  []string{"operation?: string;", "status?: string;", "proxyId?: bigint;"},
		},
	} {
		block, ok := commonBlocks[name]
		if !ok {
			t.Fatalf("message block %q not found in %s", name, commonPath)
		}
		if !strings.Contains(block, wants.marker) {
			t.Fatalf("%s block missing field marker %q", name, wants.marker)
		}
		for _, decl := range wants.decls {
			if !strings.Contains(block, decl) {
				t.Fatalf("%s block missing declaration %q", name, decl)
			}
		}
	}

	for name, wants := range map[string]struct {
		marker string
		decls  []string
	}{
		"GetAuditResponse": {
			marker: "repeated api_audit.SnapshotMetadata snapshots = 4;",
			decls:  []string{"snapshots: SnapshotMetadata[];"},
		},
		"SnapshotMetadata": {
			marker: "optional int64 created_at = 5;",
			decls:  []string{"side?: string;", "nodeType?: string;", "nodeId?: string;", "sha256?: string;", "createdAt?: bigint;"},
		},
		"RollbackRequest": {
			marker: "optional string idempotency_key = 2;",
			decls:  []string{"idempotencyKey?: string;"},
		},
		"RollbackResponse": {
			marker: "optional string error_code = 3;",
			decls:  []string{"errorCode?: string;"},
		},
	} {
		block, ok := auditBlocks[name]
		if !ok {
			t.Fatalf("message block %q not found in %s", name, auditPath)
		}
		if !strings.Contains(block, wants.marker) {
			t.Fatalf("%s block missing field marker %q", name, wants.marker)
		}
		for _, decl := range wants.decls {
			if !strings.Contains(block, decl) {
				t.Fatalf("%s block missing declaration %q", name, decl)
			}
		}
	}

	// Anti-smear: the audit detail surface must not expose ciphertext/nonce
	// as WIRE FIELDS (doc comments may legitimately mention them).
	for _, block := range auditBlocks {
		if strings.Contains(block, "ciphertext?:") || strings.Contains(block, "nonce?:") ||
			strings.Contains(block, "ciphertext = ") || strings.Contains(block, "nonce = ") {
			t.Fatalf("api_audit.ts must not carry ciphertext/nonce fields anywhere")
		}
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
