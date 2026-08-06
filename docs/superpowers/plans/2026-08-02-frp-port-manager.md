# FRP 端口穿透管理平台实施计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 FRP-Panel 开源底座上扩展端口可视化、双端预检、安全事务、配置快照和回滚，管理阿里云 + K30 Pro 的 FRP TCP/UDP 规则。

**Architecture:** 复用现有双向流 Agent 通道，新增 Port Inspector（端口探测器）和 Transaction Coordinator（事务协调器），改造 CreateProxyConfig 为同步 ACK 闭环，加密快照为回滚提供证据。

**Tech Stack:** Go 1.24+, gRPC/Gin, gopsutil v4, GORM SQLite, Next.js 14, TypeScript, Tailwind, Protobuf.

**Source Spec:** `docs/superpowers/specs/2026-08-02-frp-port-manager-design.md`

**Baseline:** `main@1a58b85`; FRP kernel v0.65.0; Go 1.26.4 / Node v22.20.0 / pnpm 10.26.0 on Windows

---

## Chunk 0: 环境与分支隔离

### Task 0.1: 创建 worktree 并切换 feature 分支

**Files:**
- 无新建文件（Git 分支操作）

- [ ] **Step 1: 创建隔离 worktree**
```bash
cd D:/frp-panel-manager
git worktree add -b feat/port-manager ../frp-panel-manager-feat main
cd ../frp-panel-manager-feat
```
Expected: 在 `D:/frp-panel-manager-feat` 获得独立工作目录。

- [ ] **Step 2: 验证前端构建可用**
```bash
cd www && pnpm install --no-frozen-lockfile && pnpm build
```
Expected: `pnpm build` 退出码 0，生成 `www/out/`。

- [ ] **Step 3: 验证后端构建可用（仅 Windows 平台）**
```bash
bash ./build.sh --platform windows --bintype full --arch amd64 --skip-frontend
```
Expected: `dist/frp-panel-windows-amd64.exe` 存在。

- [ ] **Step 4: 验证 Go 测试通过**
```bash
go test ./utils/... ./conf/... -v
```
Expected: 所有已有测试 PASS。

- [ ] **Step 5: Commit**
```bash
git add -A && git commit -m "chore: initialize feat/port-manager worktree"
```

---

## Chunk 1: 数据模型与基础代码生成

### Task 1.1: Proto 定义与代码生成

**Files:**
- Create: `idl/types_port.proto`
- Create: `idl/api_port.proto`
- Modify: `idl/rpc_master.proto` — add `EVENT_INSPECT_PORTS`, `EVENT_VALIDATE_PORT`
- Generate: `pb/types_port.pb.go`, `pb/api_port.pb.go`, `pb/rpc_master.pb.go`
- Generate: `www/lib/pb/types_port.ts`, `www/lib/pb/api_port.ts`

- [ ] **Step 1: 定义 PortObservation 消息**
```protobuf
// idl/types_port.proto
syntax = "proto3";
package types_port;
option go_package = "../pb";

enum PortState {
  STATE_UNSPECIFIED = 0;
  STATE_AVAILABLE = 1;
  STATE_LISTENING = 2;
  STATE_ACTIVE = 3;
  STATE_CONFLICT = 4;
  STATE_UNKNOWN = 5;
}

message PortObservation {
  string node_id = 1;
  string protocol = 2;
  string ip_family = 3;
  string local_address = 4;
  uint32 local_port = 5;
  PortState state = 6;
  int32 pid = 7;
  string process_name = 8;
  string executable = 9;
  string service_name = 10;
  string docker_container_id = 11;
  string docker_container_name = 12;
  string frp_proxy_name = 13;
  int32 active_connections = 14;
  int64 observed_at = 15;
  string source = 16;
}
```

- [ ] **Step 2: 定义端口 API 消息**
```protobuf
// idl/api_port.proto
syntax = "proto3";
package api_port;
import "common.proto";
import "types_port.proto";
option go_package = "../pb";

message ListPortsRequest {
  optional string node_id = 1;
  optional string protocol = 2;
  optional uint32 port_min = 3;
  optional uint32 port_max = 4;
}

message ListPortsResponse {
  optional common.Status status = 1;
  repeated types_port.PortObservation observations = 2;
}

message PreflightRequest {
  optional string client_id = 1;
  optional string server_id = 2;
  optional string proxy_type = 3;
  optional bytes config = 4;
}

message PreflightResponse {
  optional common.Status status = 1;
  repeated types_port.PortObservation server_checks = 2;
  repeated types_port.PortObservation client_checks = 3;
  optional bool server_ok = 4;
  optional bool client_ok = 5;
  optional string error_code = 6;
}

message InspectPortsRequest {
  optional string client_id = 1;
  optional string server_id = 2;
  optional string protocol = 3;
  optional uint32 port_min = 4;
  optional uint32 port_max = 5;
  optional bool include_docker = 6;
}

message InspectPortsResponse {
  optional common.Status status = 1;
  repeated types_port.PortObservation observations = 2;
}
```

- [ ] **Step 2b: 补充 CreateProxyConfigRequest.idempotency_key 字段**
```protobuf
// idl/api_client.proto - CreateProxyConfigRequest 追加：
optional string idempotency_key = 12;
```

- [ ] **Step 2c: 定义审计 API 消息**
```protobuf
// idl/api_audit.proto
syntax = "proto3";
package api_audit;
import "common.proto";
option go_package = "../pb";

message ListAuditRequest {
  optional int32 page = 1;
  optional int32 page_size = 2;
  optional string transaction_id = 3;
}

message ListAuditResponse {
  optional common.Status status = 1;
  repeated common.AuditEvent events = 2;
  optional int32 total = 3;
}

message GetAuditRequest {
  optional string transaction_id = 1;
}

message GetAuditResponse {
  optional common.Status status = 1;
  optional common.AuditEvent event = 2;
  optional bytes evidence_json = 3;
}

message RollbackRequest {
  optional string transaction_id = 1;
}

message RollbackResponse {
  optional common.Status status = 1;
  optional string new_transaction_id = 2;
}
```

- [ ] **Step 2d: 定义 AuditEvent 通用消息**
```protobuf
// idl/common.proto 追加：
message AuditEvent {
  optional string transaction_id = 1;
  optional string event_type = 2;
  optional string summary = 3;
  optional string node_id = 4;
  optional bool success = 5;
  optional int64 created_at = 6;
  optional string error_code = 7;
}
```


- [ ] **Step 3: 在 rpc_master.proto 的 Event enum 追加**
```protobuf
EVENT_INSPECT_PORTS = 30;
EVENT_VALIDATE_PORT = 31;
```

- [ ] **Step 4: 运行代码生成**
```bash
bash ./codegen.sh
```
Expected: 退出码 0，pb 和 www/lib/pb 目录生成新文件。

- [ ] **Step 5: 修复 common/response.go Server Event 映射**
```go
// common/response.go:47 — 修复 BUG: UpdateFRPSResponse 原有映射为 FRPC Event
case *pb.UpdateFRPSResponse:
    return pb.Event_EVENT_UPDATE_FRPS, ptr, nil  // 原是 FRPC，修正
case *pb.RemoveFRPSResponse:
    return pb.Event_EVENT_REMOVE_FRPS, ptr, nil  // 原是 FRPC，修正
```

- [ ] **Step 6: 注册新 Proto 类型到 common/request.go 和 response.go**
```go
// common/request.go ReqType 追加:
pb.ListPortsRequest | pb.PreflightRequest | pb.InspectPortsRequest |
pb.ListAuditRequest | pb.GetAuditRequest | pb.RollbackRequest |

// common/response.go RespType 追加:
pb.ListPortsResponse | pb.PreflightResponse | pb.InspectPortsResponse |
pb.ListAuditResponse | pb.GetAuditResponse | pb.RollbackResponse |
```

- [ ] **Step 7: 构建验证**
```bash
go build ./...
```
Expected: 无编译错误。

- [ ] **Step 8: Commit**
```bash
git add idl/ pb/ www/lib/pb/ common/request.go common/response.go
git commit -m "feat: add port, audit, and idempotency proto; fix Server ACK event mapping"
```

### Task 1.2: 数据库模型

**Files:**
- Create: `models/port_reservation.go`
- Create: `models/change_transaction.go`
- Create: `models/config_snapshot.go`
- Create: `models/audit_event.go`
- Modify: `models/db.go` — add AutoMigrate
- Modify: `services/dao/query.go` — add new query interfaces
- Create: `services/dao/port_reservation.go`
- Create: `services/dao/transaction.go`
- Create: `services/dao/snapshot.go`
- Create: `services/dao/audit.go`

- [ ] **Step 1: 编写 models/port_reservation.go 测试**
```go
func TestPortReservationUniqueConstraint(t *testing.T) {
    db := setupTestDB(t)
    r1 := &models.PortReservation{...}
    r2 := &models.PortReservation{...}  // 同 server/protocol/port
    db.Create(r1)
    err := db.Create(r2)
    if err == nil {
        t.Fatal("expected unique constraint violation")
    }
}
```

- [ ] **Step 2: 实现 port_reservation 模型**
```go
type PortReservation struct {
    gorm.Model
    ServerID      string     `gorm:"uniqueIndex:idx_port_res"`
    Protocol      string     `gorm:"uniqueIndex:idx_port_res;size:16"`
    RemotePort    uint32     `gorm:"uniqueIndex:idx_port_res"`
    OwnerProxyID  *uint
    TransactionID string     `gorm:"index"`
    Status        string     `gorm:"default:pending;size:32"`
    ExpiresAt     time.Time
}
```
包含 `FillFromRequest` 和 `IsExpired` 方法。

- [ ] **Step 3: 编写 change_transaction 状态转换测试**
```go
func TestTransactionStateMachine(t *testing.T) {
    // pending → preflight → applying → verifying → committed
    // pending → failed (rollback at preflight)
    // committed → rolling_back → rolled_back
    // committed → rolling_back → rollback_failed
}
```

- [ ] **Step 4: 实现 change_transaction 模型**
- Status enum: pending, preflight, applying, verifying, committed, rolling_back, rolled_back, failed, rollback_failed
- Fields: TransactionID, IdempotencyKey(unique), ActorUserID, Operation, ProxyID, Status, ErrorCode, EvidenceJSON
- `CanTransitionTo(target) bool` 方法

- [ ] **Step 5: 实现 config_snapshot 模型**
```go
type ConfigSnapshot struct {
    gorm.Model
    TransactionID string
    Side          string // before/after
    NodeType      string // client/server
    NodeID        string
    Ciphertext    []byte
    Nonce         []byte
    SHA256        string // gorm:"uniqueIndex"
    CreatedAt     time.Time
}
```

- [ ] **Step 6: 实现 audit_event 模型**
精简字段：EventType, Summary(脱敏), TransactionID, NodeID, EvidenceRef, Success, CreatedAt。

- [ ] **Step 7: 在 models/db.go 注册迁移**
```go
db.AutoMigrate(
    &PortReservation{}, &ChangeTransaction{},
    &ConfigSnapshot{}, &AuditEvent{},
)
```

- [ ] **Step 8: 实现 DAO 层**
- `services/dao/port_reservation.go`: TryAcquire(serverID, protocol, port, txID), Release, GetByServerFilter
- `services/dao/transaction.go`: Create, FindByID, FindByIdempotencyKey, UpdateStatus
- `services/dao/snapshot.go`: SaveEncrypted(txID, side, nodeType, nodeID, plaintext, key), FindByTransaction
- `services/dao/audit.go`: Log(eventType, summary, txID, success)

- [ ] **Step 9: 注册新 DAO 接口**
在 `services/dao/query.go` 增加 `PortReservationQuery/PortReservationMutation/TransactionQuery/TransactionMutation/SnapshotQuery/SnapshotMutation/AuditMutation` 并更新 compositeQuery。

- [ ] **Step 10: 运行测试**
```bash
go test ./models/... -v
go test ./services/dao/... -v -run "PortReservation|Transaction|Snapshot|Audit"
```

- [ ] **Step 11: Commit**
```bash
git add models/*.go services/dao/*.go
git commit -m "feat: add port reservation, transaction, snapshot, and audit models"
```

---

## Chunk 2: Port Inspector (端口探测器)

### Task 2.1: Linux/Android 端口扫描

**Files:**
- Create: `utils/portinspector/inspector.go`
- Create: `utils/portinspector/inspector_linux.go`
- Create: `utils/portinspector/inspector_other.go`
- Create: `utils/portinspector/inspector_test.go`
- Modify: `go.mod` — ensure gopsutil v4

- [ ] **Step 1: 编写 inspector 单元测试（文件解析）**
```go
func TestParseProcNetTCP(t *testing.T) {
    sample := " 0: 0100007F:0016 00000000:0000 0A ..."
    obs := parseProcNetLine("tcp", sample)
    assertEqual(t, "tcp", obs.Protocol)
    assertEqual(t, "127.0.0.1", obs.LocalAddress)
    assertEqual(t, uint32(22), obs.LocalPort)
    assertEqual(t, "LISTENING", obs.State)
}
```

- [ ] **Step 2: 实现 inspector 核心接口**
```go
type Inspector interface {
    ScanPorts(ctx context.Context, opts ScanOptions) ([]PortObservation, error)
}
type ScanOptions struct {
    Protocol string // tcp, udp, ""
    PortMin  uint32
    PortMax  uint32
}
```

- [ ] **Step 3: 实现 Linux 平台扫描**
- 首选 `gopsutil/v4/net.Connections` 获取结构化 socket
- 回退读取 `/proc/net/tcp`, `/proc/net/tcp6`, `/proc/net/udp`, `/proc/net/udp6`
- 按 socket inode 关联 `/proc/<pid>/fd` 和 `/proc/<pid>/exe`
- Docker 关联通过 Unix Socket API `/var/run/docker.sock`

- [ ] **Step 4: 实现端口绑定探测**
```go
func ProbeBind(network, address string) (bool, error) {
    // TCP: net.Listen → 立即 Close
    // UDP: net.ListenPacket → 立即 Close
}
```

- [ ] **Step 5: 实现 IPv4/IPv6 冲突检测**
通配 `0.0.0.0` 和 `[::]` 均绑定同一端口时判为冲突。

- [ ] **Step 6: 编写完整集成测试**
用 `net.Listen` 创建已知监听端口，验证 inspector 正确识别。

- [ ] **Step 7: 编译 Linux/Android 目标验证**
```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ./utils/portinspector/...
GOOS=android GOARCH=arm64 CGO_ENABLED=0 go build ./utils/portinspector/...
```

- [ ] **Step 8: Commit**
```bash
git add utils/portinspector/
git commit -m "feat: add Port Inspector with /proc and gopsutil scanning"
```

### Task 2.2: Agent 端事件处理

**Files:**
- Create: `biz/client/inspect_ports.go`
- Create: `biz/server/inspect_ports.go`
- Modify: `biz/client/rpc_handler.go` — add EVENT_INSPECT_PORTS case
- Modify: `biz/server/rpc_handler.go` — add EVENT_INSPECT_PORTS case

- [ ] **Step 1: 实现 Client InspectPortsHandler**
```go
func InspectPortsHandler(ctx *app.Context, req *pb.InspectPortsRequest) (*pb.InspectPortsResponse, error) {
    inspector := portinspector.New()
    opts := portinspector.ScanOptions{...}
    obs, err := inspector.ScanPorts(ctx, opts)
    // ...
}
```

- [ ] **Step 2: 实现 Server InspectPortsHandler**
相同接口，Linux 宿主机扫描。

- [ ] **Step 3: 在 RPC dispatcher 注册事件**
```go
// biz/client/rpc_handler.go:
case pb.Event_EVENT_INSPECT_PORTS:
    return app.WrapperServerMsg(appInstance, req, InspectPortsHandler)

// biz/server/rpc_handler.go:
case pb.Event_EVENT_INSPECT_PORTS:
    return app.WrapperServerMsg(appInstance, req, InspectPortsHandler)
```

- [ ] **Step 4: 验证编译**
```bash
go build ./biz/client/... ./biz/server/...
```

- [ ] **Step 5: Commit**
```bash
git add biz/client/inspect_ports.go biz/server/inspect_ports.go biz/client/rpc_handler.go biz/server/rpc_handler.go
git commit -m "feat: add Agent-side port inspection event handlers"
```

---

## Chunk 3: RPC 超时与可靠性修复

### Task 3.1: 修复 CallClient 超时和 Session 泄漏

**Files:**
- Modify: `services/rpc/client.go`

- [ ] **Step 1: 编写超时测试**
```go
func TestCallClientTimeout(t *testing.T) {
    // 启动 mock server 不响应
    // 验证 context 超时后 err != nil
    // 验证 Session Map 已清理
}
```

- [ ] **Step 2: 新增 CallClientWithContext**
```go
func CallClientWithContext(ctx context.Context, appInstance app.Application,
    clientID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error) {
    // select case <-ctx.Done(): cleanup and return error
    // select case resp := <-respCh: return resp
    // defer: delete from ClientRecvMap, close channel
}
```
默认超时：端口扫描 5s，配置应用 15s，状态验证 30s。

- [ ] **Step 3: 修改现有 CallClient 为薄包装**
```go
func CallClient(...) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    return CallClientWithContext(ctx, ...)
}
```

- [ ] **Step 4: 运行测试**
```bash
go test ./services/rpc/ -v -run "Timeout|Leak"
```

- [ ] **Step 5: Commit**
```bash
git add services/rpc/client.go services/rpc/client_test.go
git commit -m "fix: add RPC timeout and Session Map cleanup to CallClient"
```

---

## Chunk 4: 安全事务编排

### Task 4.1: Transaction Coordinator

**Files:**
- Create: `biz/master/proxy/transaction_coordinator.go`
- Create: `biz/master/proxy/transaction_coordinator_test.go`
- Modify: `biz/master/proxy/create_proxy_config.go`
- Modify: `biz/master/proxy/update_proxy_config.go`
- Modify: `biz/master/proxy/delete_proxy_config.go`

- [ ] **Step 1: 编写 transaction coordinator 测试**
```go
func TestCreateProxyWithValidation(t *testing.T) {
    // mock server/client agents online
    // mock server port scan: AVAILABLE
    // mock client port scan: LISTENING
    // 验证: transaction committed, audit logged, snapshot saved
}
func TestCreateProxyServerPortConflict(t *testing.T) {
    // mock server port scan: CONFLICT (other process)
    // 验证: transaction failed, no config change, audit logged
}
func TestCreateProxyClientOffline(t *testing.T) {
    // mock client agent offline
    // 验证: transaction failed, agent offline error
}
```

- [ ] **Step 2: 实现事务协调器核心流程**
```go
func SafeCreateProxy(ctx *app.Context, req *pb.CreateProxyConfigRequest, idempotencyKey string) (*pb.CreateProxyConfigResponse, error) {
    // 1. 幂等键去重
    // 2. 解析并校验 TCP/UDP；HTTP/其他 reject
    // 3. 创建 change_transaction
    // 4. 申请 port_reservation (DB unique constraint)
    // 5. 加密 before 快照
    // 6. Server real-time scan + bind probe
    // 7. Client real-time scan (本地监听)
    // 8. 生成候选配置
    // 9. 同步下发 Client (EVENT_UPDATE_FRPC, wait ACK, 15s timeout)
    // 10. 验证公网监听 (retry * 3, 30s total)
    // 11. DB transaction: 持久化 ProxyConfig + reservation committed + audit
    // 12. return success
    // ANY failure → auto compensation → mark failed
}
```

- [ ] **Step 3: 改造 CreateProxyConfig handler**
```go
func CreateProxyConfig(c *app.Context, req *pb.CreateProxyConfigRequest) (*pb.CreateProxyConfigResponse, error) {
    return SafeCreateProxy(c, req, req.GetIdempotencyKey())
}
```

- [ ] **Step 4: 改造 UpdateProxyConfig handler**
同样走事务协调器。原端口由当前规则占用时 `port_reservation` 不判冲突。

- [ ] **Step 5: 改造 DeleteProxyConfig handler**
```go
func DeleteProxyConfig(c *app.Context, req *pb.DeleteProxyConfigRequest) {
    // 1. 加密 before 快照
    // 2. 下发无目标规则的配置
    // 3. 验证公网端口释放
    // 4. DB 删除
    // 5. audit
}
```

- [ ] **Step 6: 实现自动补偿**
```go
func compensate(ctx *app.Context, tx *Transaction) error {
    // 1. 下发 before snapshot 到 Client → wait ACK
    // 2. 验证旧规则恢复
    // 3. 恢复 DB before 状态
    // 4. 释放 port_reservation
    // if compensation fail → mark ROLLBACK_FAILED, alert
}
```

- [ ] **Step 7: 修改 async goroutine 为同步等待**
`biz/master/client/update_tunnel.go` 中 `go func()` 改为同步 `CallClientWithContext`。

- [ ] **Step 8: 运行测试**
```bash
go test ./biz/master/proxy/... -v -run "Safe|Transaction|Compensate"
```

- [ ] **Step 9: Commit**
```bash
git add biz/master/proxy/ biz/master/client/update_tunnel.go
git commit -m "feat: add safe transaction coordinator with ACK and compensation"
```

---

## Chunk 5: API 路由与 HTTP Handler

### Task 5.1: 新增 port/audit/preflight 路由

**Files:**
- Modify: `biz/master/handler.go`
- Create: `biz/master/port/list_ports.go`
- Create: `biz/master/proxy/preflight.go`
- Create: `biz/master/audit/list.go`
- Create: `biz/master/audit/get.go`
- Create: `biz/master/audit/rollback.go`

- [ ] **Step 1: 实现 ListPorts Handler**
```go
func ListPorts(ctx *app.Context, req *pb.ListPortsRequest) (*pb.ListPortsResponse, error) {
    // 1. Validate node_id points to registered client/server
    // 2. Call Agent via EVENT_INSPECT_PORTS (with context timeout 5s)
    // 3. Parse response, filter by protocol/port range
    // 4. Return PortObservations
}
```

- [ ] **Step 2: 实现 Preflight Handler**
```go
func Preflight(ctx *app.Context, req *pb.PreflightRequest) (*pb.PreflightResponse, error) {
    // 1. Server port scan + bind probe
    // 2. Client port scan (local listener)
    // 3. Return evidence without modifying any config
}
```

- [ ] **Step 3: 实现 Audit Handlers**
```go
func ListAudit(ctx *app.Context, req *pb.ListAuditRequest) (*pb.ListAuditResponse, error)
func GetAudit(ctx *app.Context, req *pb.GetAuditRequest) (*pb.GetAuditResponse, error)
func Rollback(ctx *app.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error)
```

- [ ] **Step 4: 注册路由**
```go
// biz/master/handler.go 追加：
portRouter := v1.Group("/port")
portRouter.POST("/list", app.Wrapper(appInstance, port.ListPorts))

proxyRouter.POST("/preflight", app.Wrapper(appInstance, proxy.Preflight))

auditRouter := v1.Group("/audit")
auditRouter.POST("/list", app.Wrapper(appInstance, audit.ListAudit))
auditRouter.POST("/get", app.Wrapper(appInstance, audit.GetAudit))
auditRouter.POST("/rollback", app.Wrapper(appInstance, audit.Rollback))
```

- [ ] **Step 4b: 编写 ListPorts handler 测试**
```go
func TestListPortsHandler(t *testing.T) {
    // 启动 mock agent 返回预定义端口列表
    // 调用 handler
    // 验证响应包含正确的 PortObservation
}
```

- [ ] **Step 5: 验证编译**
```bash
go build ./biz/master/...
```

- [ ] **Step 6: Commit**
```bash
git add biz/master/handler.go biz/master/port/ biz/master/audit/
git commit -m "feat: add port list, preflight, and audit API routes"
```

---

## Chunk 6: 前端 — 端口中心与安全编辑

### Task 6.1: 前端 API 层

**Files:**
- Create: `www/api/port.ts`
- Create: `www/api/audit.ts`
- Modify: `www/api/proxy.ts` — add preflight

- [ ] **Step 1: 实现 port API**
```typescript
export const listPorts = async (req: ListPortsRequest) => {
  const res = await http.post(API_PATH + '/port/list', ListPortsRequest.toJson(req))
  return ListPortsResponse.fromJson((res.data as BaseResponse).body)
}
```

- [ ] **Step 2: 实现 audit API**
```typescript
export const listAudit = async (req: ListAuditRequest) => ...
export const getAudit = async (req: GetAuditRequest) => ...
export const rollbackAudit = async (req: RollbackRequest) => ...
```

- [ ] **Step 3: 实现 preflight API**
```typescript
export const preflightProxy = async (req: PreflightRequest) => ...
```

- [ ] **Step 4: Commit**
```bash
git add www/api/port.ts www/api/audit.ts www/api/proxy.ts
git commit -m "feat: add port, audit, and preflight frontend API layers"
```

### Task 6.2: 端口中心页面

**Files:**
- Create: `www/pages/port-center.tsx`
- Create: `www/components/port/port_table.tsx`
- Create: `www/components/port/port_filters.tsx`

- [ ] **Step 1: 实现 PortItem 行组件**
显示：端口、协议、监听地址、状态标签、PID、进程、容器、FRP 规则、连接数。

- [ ] **Step 2: 实现 PortFilters 组件**
按节点、TCP/UDP、状态、端口范围过滤。

- [ ] **Step 3: 实现端口中心页面**
- 节点选择器切换 Server/Client
- 自动刷新（30 秒）
- 状态颜色：绿(AVAILABLE/LISTENING)、黄(ACTIVE)、红(CONFLICT)、灰(UNKNOWN)

- [ ] **Step 4: 前端 typecheck**
```bash
cd www && pnpm run lint
```

- [ ] **Step 5: Commit**
```bash
git add www/pages/port-center.tsx www/components/port/
git commit -m "feat: add port center page with observation table"
```

### Task 6.3: 安全代理创建/编辑

**Files:**
- Modify: `www/components/proxy/mutate_proxy_config.tsx`
- Create: `www/components/proxy/port_check_panel.tsx`

- [ ] **Step 1: 实现 PortCheckPanel 组件**
- 显示公网端口状态 + 占用证据
- 显示本地端口状态 + 进程/容器证据
- 只有两项均绿色时才启用保存按钮
- "重新检查"按钮触发 preflight API

- [ ] **Step 2: 集成到 MutateProxyConfig**
- 选择 Client/Server/端口后自动显示预检按钮
- Preflight 前先做前端基本校验（端口范围、整数）
- 预检失败显示结构化错误（端口、占用者、时间）
- 提交时后端重新检查（TOCTOU防护）

- [ ] **Step 3: 错误展示格式**
```tsx
<Alert variant="destructive">
  <AlertTitle>公网端口被占用</AlertTitle>
  <AlertDescription>
    TCP 8000 已被 frps(pid=24872) 使用
    节点: 47.109.18.109 | 检测时间: 2026-08-02 21:45:00
  </AlertDescription>
</Alert>
```

- [ ] **Step 4: 提交时发送 idempotency_key**
```typescript
const idempotencyKey = crypto.randomUUID()  // 服务端通过 change_transaction 唯一约束去重
createProxyConfig({
  ...originalReq,
  idempotencyKey,
})
```

- [ ] **Step 5: 前端 typecheck + build**
```bash
cd www && pnpm run lint && pnpm build
```
Expected: 无 lint 错误，build 成功。

- [ ] **Step 6: Commit**
```bash
git add www/components/proxy/
git commit -m "feat: add port check panel and safe proxy creation UI"
```

### Task 6.4: 导航与总体布局

**Frontend tests:**
- Create: `www/__tests__/api/audit.test.ts`
- Create: `www/__tests__/api/port.test.ts`

**Files:**
- Modify: `www/config/nav.ts`
- Create: `www/pages/audit.tsx`
- Create: `www/pages/dashboard.tsx` (replace index.tsx)

- [ ] **Step 1: 更新导航**
隐藏 Worker、WireGuard、控制台入口；新增"端口中心"和"审计与回滚"。

- [ ] **Step 2: 实现仪表盘页面**
在线节点、活跃隧道、监听端口、冲突计数、事务失败告警。

- [ ] **Step 3: 实现审计列表页面**
事务 ID、操作类型、时间、状态、操作者、回滚按钮。

- [ ] **Step 4: Commit**
```bash
git add www/config/nav.ts www/pages/
git commit -m "feat: add dashboard, audit pages and updated navigation"
```

---

## Chunk 7: 配置快照与加密

### Task 7.1: 快照加密服务

**Files:**
- Create: `utils/snapshot/snapshot.go`
- Create: `utils/snapshot/snapshot_test.go`

- [ ] **Step 1: 编写加解密测试**
```go
func EncryptSnapshot(plaintext []byte, key []byte) (ciphertext, nonce []byte, sha256sum string, err error)
func DecryptSnapshot(ciphertext, nonce []byte, key []byte) ([]byte, error)
```

- [ ] **Step 2: 测试加解密一致性**
- 加密 → 解密 → 原文一致
- 错误 nonce → 解密失败
- 错误密钥 → 解密失败
- 大配置 (10MB) 加解密性能

- [ ] **Step 3: 密钥加载**
从环境变量 `SNAPSHOT_ENCRYPTION_KEY` 加载。未设置时启动失败。

- [ ] **Step 4: Commit**
```bash
git add utils/snapshot/
git commit -m "feat: add AES-GCM snapshot encryption service"
```


```bash
git add biz/master/proxy/transaction_coordinator.go
git commit -m "feat: integrate encrypted snapshots into transaction system"
```

---

## Chunk 8: 迁移脚本与部署

### Task 8.1: 备份与恢复脚本

**Files:**
- Create: `deploy/backup-aliyun.sh`
- Create: `deploy/backup-k30pro.sh`
- Create: `deploy/rollback-aliyun.sh`
- Create: `deploy/rollback-k30pro.sh`
- Create: `deploy/migrate.sh`

- [ ] **Step 1: 编写阿里云备份脚本**
```bash
#!/bin/bash
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
BACKUP_DIR="/opt/frp-panel/migration/${TIMESTAMP}"
mkdir -p "$BACKUP_DIR"

cp /opt/frp/frp_0.65.0_linux_amd64/frps-phone.toml "$BACKUP_DIR/"
cp /etc/systemd/system/frps-phone.service "$BACKUP_DIR/"
sha256sum /opt/frp/frp_0.65.0_linux_amd64/frps > "$BACKUP_DIR/sha256.txt"
sha256sum "$BACKUP_DIR"/* > "$BACKUP_DIR/manifest.txt"
```

- [ ] **Step 2: 编写 K30 Pro 备份脚本**
备份 `/data/frp/frpc`, `/data/frp/frpc.toml`, `/data/adb/service.d/frpc.sh`，记录权限和属主。

- [ ] **Step 3: 编写阿里云回滚脚本**
停止新服务 → 从 manifest 恢复旧文件和权限 → 启动 frps-phone → 验证端口监听。

- [ ] **Step 4: 编写整体迁移编排脚本**
按规格 12.2 的 5 个阶段编排；每个阶段可独立执行和回滚。

- [ ] **Step 5: Commit**
```bash
echo 'deploy/env/*.env' >> .gitignore
git add .gitignore deploy/backup-*.sh deploy/rollback-*.sh deploy/migrate.sh
git commit -m "feat: add backup, rollback, and migration orchestration scripts"
```

### Task 8.2: 阿里云部署配置

**Files:**
- Create: `deploy/docker-compose.yml` (阿里云)
- Create: `deploy/env/master.env` (环境变量模板)
- Create: `deploy/env/client-phone.env` (手机 Agent 环境变量)

- [ ] **Step 1: 编写 docker-compose.yml**
- Master 9100 (仅回环) + 9101 (公网)
- 数据卷 `/opt/frp-panel/data`
- 必要环境变量

- [ ] **Step 2: 编写 master.env**
```env
MASTER_API_PORT=9100
MASTER_RPC_HOST=47.109.18.109
MASTER_RPC_PORT=9101
APP_GLOBAL_SECRET=<generate-256bit>
APP_ENABLE_REGISTER=true
DB_DSN=/data/data.db?_pragma=journal_mode(WAL)
```

- [ ] **Step 3: 编写手机 client.env**
```env
CLIENT_ID=k30pro-lmi
CLIENT_RPC_URL=grpc://47.109.18.109:9101
CLIENT_API_URL=https://47.109.18.109:9443
CLIENT_TLS_INSECURE_SKIP_VERIFY=false
```

- [ ] **Step 4: Commit**
```bash
echo 'deploy/env/*.env' >> .gitignore
git add .gitignore deploy/
git commit -m "feat: add deployment configs for Aliyun and K30 Pro"
```

---

## Chunk 9: 安全加固

### Task 9.1: TLS 与日志脱敏

**Files:**
- Modify: `conf/helper.go` — GetCertTemplate IP SAN
- Modify: `cmd/frpp/shared/providers.go` — TLS + 日志脱敏
- Modify: `conf/settings.go` — Config.PrintStr 脱敏
- Modify: `biz/master/handler.go` — CheckOrigin

- [ ] **Step 1: 修复证书 IP SAN**
```go
// conf/helper.go GetCertTemplate:
func GetCertTemplate(cfg Config) *x509.Certificate {
    // 识别 MASTER_API_HOST 是 IP 还是域名
    ip := net.ParseIP(cfg.Master.APIHost)
    if ip != nil {
        cert.IPAddresses = append([]net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, ip)
    } else {
        cert.DNSNames = append([]string{"localhost"}, cfg.Master.APIHost)
    }
}
```

- [ ] **Step 2: 接入严格 TLS 校验**
```go
// NewClientCred/NewServerCred:
if cfg.Client.TLSInsecureSkipVerify {
    cred, err = utils.TLSClientCertNoValidate(...) // 仅测试
} else {
    cred, err = utils.TLSClientCert(...) // 生产
}
```

- [ ] **Step 3: 脱敏敏感日志**
```go
// conf/settings.go PrintStr():
func (cfg Config) PrintStr() string {
    safe := cfg
    safe.App.GlobalSecret = "[REDACTED]"
    safe.Client.Secret = "[REDACTED]"
    // 不输出完整密文
}
```
UpdateFRPC/UpdateFRPS 和 AutoJoin 日志同理。

- [ ] **Step 4: 限制 WebSocket CheckOrigin**
```go
// biz/master/handler.go — wsGrpcHandler:
CheckOrigin: func(r *http.Request) bool {
    origin := r.Header.Get("Origin")
    return origin == "https://47.109.18.109:9443"
}
```

- [ ] **Step 5: 验证编译和已有测试**
```bash
go build ./... && go test ./...
```

- [ ] **Step 6: Commit**
```bash
git add conf/ cmd/ biz/master/handler.go
git commit -m "security: fix TLS cert SAN, enforce strict verification, redact secrets in logs"
```

---

## Chunk 10: 集成测试与验收

### Task 10.1: 集成测试套件

**Files:**
- Create: `tests/integration/port_inspector_test.go`
- Create: `tests/integration/transaction_test.go`
- Create: `tests/integration/migration_test.go`

- [ ] **Step 1: Port Inspector 集成测试**
- 在 Docker 容器中启动 Nginx，验证端口识别
- 同一端口被两个进程监听的冲突检测
- IPv4/IPv6 通配处理

- [ ] **Step 2: 事务集成测试**
- 完整 happy path: 创建 → 预检 → 提交 → 公网可达
- 端口冲突拒绝
- Agent 离线拒绝
- 并发同端口只有一个成功
- 回滚成功后规则恢复
- 回滚失败后标记 + 告警

- [ ] **Step 3: 迁移集成测试**
- 备份脚本产生有效 manifest
- 回滚脚本可恢复原配置
- 60 秒 RTO 验证

- [ ] **Step 4: 运行全量测试**
```bash
go test ./... -v -count=1
cd www && pnpm run lint && pnpm build
```

- [ ] **Step 5: Commit**
```bash
git add tests/
git commit -m "test: add integration tests for port inspector, transaction, and migration"
```

---

## Chunk 11: 兼容性门禁与演示环境部署

### Task 11.1: Android ARM64 兼容性门禁

- [ ] **Step 1: 构建 Android/ARM64 Client**
```bash
GOOS=android GOARCH=arm64 CGO_ENABLED=0 go build -o dist/frp-panel-android-arm64 ./cmd/frppc/
```

- [ ] **Step 2: 传输到 K30 Pro 并启动测试**
```bash
adb push dist/frp-panel-android-arm64 /data/frp-panel/frp-panel-client
adb shell "chmod 755 /data/frp-panel/frp-panel-client"
adb shell "/data/frp-panel/frp-panel-client client --join-token <token>"
```
Expected: Client 注册到 Master，Start/Restart/Scan/Docker 识别均成功。

- [ ] **Step 3: 在干净 Android 环境测试（如失败则回退方案）**
失败 → 记录兼容性问题详情 → 切换到"官方 Android frpc + 独立探测 Agent" → 更新设计规格 → 重新评审。

- [ ] **Step 4: 阿里云部署与影子验证**
按 `deploy/migrate.sh` 顺序执行前 3 阶段。

- [ ] **Step 5: 24 小时监控后提交切流决策**
资源指标、RPC 重连、规则稳定性均正常 → 执行切流。

---

## 文件变更总览

| 新增文件 | 修改文件 |
|---|---|
| `idl/types_port.proto`, `idl/api_port.proto` | `idl/rpc_master.proto` |
| `models/port_reservation.go`, `models/change_transaction.go`, `models/config_snapshot.go`, `models/audit_event.go` | `models/db.go` |
| `services/dao/port_reservation.go`, `services/dao/transaction.go`, `services/dao/snapshot.go`, `services/dao/audit.go` | `services/dao/query.go` |
| `utils/portinspector/*.go` | `utils/port.go` |
| `utils/snapshot/*.go` | - |
| `biz/client/inspect_ports.go`, `biz/server/inspect_ports.go` | `biz/client/rpc_handler.go`, `biz/server/rpc_handler.go` |
| `biz/master/proxy/transaction_coordinator.go` | `biz/master/proxy/create_proxy_config.go`, `update_proxy_config.go`, `delete_proxy_config.go` |
| `biz/master/port/*.go`, `biz/master/audit/*.go` | `biz/master/handler.go` |
| `www/api/port.ts`, `www/api/audit.ts` | `www/api/proxy.ts` |
| `www/pages/port-center.tsx`, `www/pages/audit.tsx`, `www/pages/dashboard.tsx` | `www/config/nav.ts` |
| `www/components/port/*.tsx`, `www/components/proxy/port_check_panel.tsx` | `www/components/proxy/mutate_proxy_config.tsx` |
| `deploy/*.sh`, `deploy/docker-compose.yml`, `deploy/env/*.env` | - |
| `tests/integration/*_test.go` | - |
| - | `common/request.go`, `common/response.go` |
| - | `services/rpc/client.go` |
| - | `biz/master/client/update_tunnel.go` |
| - | `conf/helper.go`, `conf/settings.go`, `cmd/frpp/shared/providers.go` |
