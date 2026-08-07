# 端口配额配置与 firewalld 联动 设计

> 定位：在 FRP-Panel 服务端配置界面开放端口配额（`maxPortsPerClient` / `allowPorts`）的 Web 配置能力，并在创建隧道时支持一键放行服务器 firewalld 端口。
> 日期：2026-08-07
> 状态：已批准（用户确认：firewalld 开关默认开启；删除隧道不自动关闭防火墙端口）

## 背景

线上事故：`maxPortsPerClient=7` 导致第 8 条隧道创建失败（`exceed the max_ports_per_client`），且失败路径因 `VerifyTimeout=30s` 让前端等待约 40 秒。修复为服务器直改配置后，用户要求将端口配额配置能力暴露到 Web 界面，并支持创建隧道时联动放行 firewalld。

## 功能 1：服务端端口限制配置（前端扩展）

### 位置

`www/components/frps/frps_form.tsx`（服务端编辑表单，已有 bindPort 等字段）。

### 新增字段

| 字段 | 控件 | 校验 |
|---|---|---|
| `maxPortsPerClient` | 数字输入 | 1–65535，整数 |
| `allowPorts` | 规则列表编辑器 | 见下 |

### 端口规则编辑器

支持两种条目：

- 单个端口：`5700` → `{"single": 5700}`
- 端口范围：`10000-20000` → `{"start": 10000, "end": 20000}`

UI：条目列表（输入框 + 删除按钮）+「添加单个端口」「添加范围」按钮。保存时转换为 frp v1 `PortRange` 数组写入 config。

### 后端

**零 Go 改动**。`UpdateFrpsHandlerSafe` 已通过 `utils.LoadServerConfig(configStr, true)` 解析完整 frps v1 ServerConfig，`allowPorts` / `maxPortsPerClient` 是标准字段（已用 API 实测验证）。

## 功能 2：创建隧道时开放 firewalld

### 前端

新建代理表单（`mutate_proxy_config.tsx`）增加 Switch「开放服务器防火墙端口（firewalld）」：

- 默认**开启**（用户决策 2026-08-07）
- 仅 tcp/udp 单规则可勾选；创建成功后调用防火墙 API
- 防火墙 API 失败：显示警告「隧道已创建，但防火墙未开放」，不阻断

### 后端新 API

```text
POST /api/v1/firewall/open
{ "server_id": "default", "port": 5700, "protocol": "tcp" }
```

- 路由：`v1.Group("/firewall")` + `middleware.RequireWritable()`，挂到现有 JWT/RBAC 链下
- Handler 用 gin `BindJSON` 自定义结构（同 `auth.HandleLogin` 模式，**不改 proto/pb**——pb 源文件不在仓库、无 protoc 工具链）
- 校验：server_id 存在且属于当前用户、port 1–65535、protocol ∈ {tcp, udp}
- 执行：`firewall-cmd --permanent --add-port=<port>/<protocol>` → `firewall-cmd --reload`
- 审计：通过 `biz/master/audit` 记录（含 server_id/port/protocol，不含命令输出）
- 失败：返回结构化错误（命令不存在 / 执行失败 / 非法参数）

### 运行前提（已确认）

- frp-panel 以 **root** 运行（`ps` 实测 USER=root），无需 sudoers
- 服务器为 CentOS 7 + firewalld（`/usr/bin/firewall-cmd` 存在）

### 不做（YAGNI，用户决策）

- 删除隧道**不自动关闭**防火墙端口（避免误伤同端口其它服务）
- 不做防火墙端口关闭 API（本期无需求）

## 错误处理与审计

| 场景 | 行为 |
|---|---|
| 隧道创建成功 + 防火墙放行成功 | 正常 toast，公网立即可达 |
| 隧道创建成功 + 防火墙放行失败 | 警告 toast「隧道已创建，但防火墙未开放」，附错误信息 |
| 防火墙 API 参数非法 | 400，不入审计 |
| `firewall-cmd` 不存在 | 结构化错误「服务器未安装 firewalld」 |

## 测试

- 后端：`biz/master/firewall/open_test.go` — 参数校验、server 不存在、firewall-cmd 成功/失败（mock exec 或注入命令执行器）
- 前端：tsc + next build 门禁（现有 CI）
- 集成：CI 部署后 curl 实测 `/api/v1/firewall/open`

## 涉及文件

```
biz/master/firewall/open.go          新
biz/master/firewall/open_test.go     新
biz/master/handler.go                路由 +1
www/components/frps/frps_form.tsx    端口限制区块
www/components/proxy/mutate_proxy_config.tsx  firewalld 开关 + 调用
www/api/firewall.ts                  新
www/types/server.ts                  ServerConfig 扩展
www/i18n/locales/{zh,en,fr,zh-tw}.json  文案
```
