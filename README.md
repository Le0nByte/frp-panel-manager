# FRP-Panel-Manager

FRP-Panel-Manager（frp-panel-manager）是一个基于 [FRP](https://github.com/fatedier/frp)（快速反向代理）的**端口转发集中管理平台**。它提供 Web 管理界面，让你在一台机器（Master）上统一管理多个节点的内网穿透：公网服务端（frps）负责接入流量，内网客户端（frpc）负责转发，端口分配、隧道配置、节点状态全部可视化。

> 本项目 fork 自 [VaalaCat/frp-panel](https://github.com/VaalaCat/frp-panel)，在保留原生多节点管理能力的基础上，针对生产环境做了端口管理、事务安全与审计强化（详见下文「与上游的差异」）。

## 核心功能

| 功能 | 说明 |
|:--|:--|
| 集中化管理 | 所有 frps（FRP 服务端）与 frpc（FRP 客户端）配置由 Master 统一管理，无需逐台手改配置文件 |
| 多节点监控 | 任意数量的服务端/客户端在线状态、实时日志、连接统计一屏可见 |
| 可视化隧道 | Web UI 创建、编辑、启停 TCP/UDP/HTTP 隧道，实时查看状态 |
| 端口预约与事务 | 端口分配走「预检 → 预约 → 提交 → 回滚」事务流程，避免并发冲突与残留状态 |
| 审计与安全门禁 | 管理操作全程审计；关键写入面 fail-closed（默认拒绝），未配置的安全功能一律不开放 |
| 凭证自动分发 | 在 Web UI 创建节点后自动生成启动命令，复制到目标机器即可接入 |

## 架构

```
┌─────────────┐   HTTPS 9443 (Web/API)     ┌─────────────┐
│  浏览器 / PWA │ ────────────────────────▶ │             │
└─────────────┘                            │   Master    │ 控制面（管理端）
                                           │  9100 API   │
┌─────────────┐   gRPC/TLS 9101 (公网)     │  9101 RPC   │
│ frpc 客户端  │ ◀──────────────────────── │             │
└─────────────┘                            └─────────────┘
        │ 内网穿透隧道（动态端口）
        ▼
┌─────────────┐
│ frps 服务端  │ 公网入口，转发流量到内网客户端
└─────────────┘
```

- **Master（控制面）**：集中管理与认证中心，所有 Server/Client 都需要能连到它。API 默认回环监听 `127.0.0.1:9100`，对外通过反向代理（HTTPS 9443）暴露；RPC 监听 `0.0.0.0:9101` 供 Agent 公网接入。
- **Server（服务端 / frps）**：公网入口，负责接收外部流量并转发给 Client。
- **Client（客户端 / frpc）**：内网一侧的代理，向 Master 注册隧道，将内网服务暴露到公网。

## 快速开始

完整的面向小白的图文步骤见 [使用指南](./docs/usage-guide.md)。核心三步：

1. **部署 Master**：在公网服务器上安装二进制 + systemd 服务（详见 [部署 Master](./docs/deploy-master.md)），配置 `MASTER_API_HOST` 为公网 IP/DNS，放行 `9101/tcp`（Agent 接入）、`9443/tcp`（Web 管理入口）。
2. **注册管理员**：浏览器打开 `https://<公网IP>:9443`，首次注册的用户自动成为管理员。
3. **接入节点**：在 Web UI「客户端」页新建客户端 ID，复制启动命令，在目标机器执行（Android 手机可放 `service.d` 开机自启，详见 [部署客户端](./docs/deploy-client.md)）。

## 文档

| 文档 | 内容 |
|:--|:--|
| [使用指南](./docs/usage-guide.md) | 面向新手的完整操作流程：部署 → 注册 → 绑定/配置服务端 → 绑定/配置客户端 → 常见问题 |
| [部署 Master](./docs/deploy-master.md) | Master 部署方式比较（systemd / Docker host network / 反向代理）与环境变量 |
| [部署 Server](./docs/deploy-server.md) | frps 服务端部署 |
| [部署 Client](./docs/deploy-client.md) | frpc 客户端部署（Linux / Windows / Docker / Android） |
| [全部配置项](./docs/all-configs.md) | 环境变量完整参考 |
| [运维手册](./docs/ops-runbook.md) | 备份、恢复、升级、证书、故障排查 |

## 技术栈

- 后端：Go（gRPC + gRPC-Web + HTTP/2）、SQLite（WAL）
- 前端：Next.js + TypeScript（静态导出，可作 PWA）
- 协议：FRP v0.65.0 内核，Master ↔ Agent 走 TLS gRPC

## 与上游的差异

在 [VaalaCat/frp-panel](https://github.com/VaalaCat/frp-panel) 基础上，本仓库新增/强化：

- **端口管理扩展**：端口预约事务（预检 → 预约 → 提交 → 回滚）、快照与恢复、端口检查器
- **安全强化**：默认关闭高风险功能（远程 Shell / Worker / WireGuard），管理写入面 fail-closed，RPC 审计与限流
- **部署体系**：systemd 单元、备份/迁移/回滚脚本、GitHub Actions 自动构建部署、离线测试套件
- **端口约定**：控制面 API `9100`（回环）、RPC `9101`（公网）、Web 管理入口 `9443`（HTTPS 反代）

## License

开源许可证沿用上游 [Apache-2.0](./LICENSE)。感谢 [VaalaCat/frp-panel](https://github.com/VaalaCat/frp-panel) 与 [FRP](https://github.com/fatedier/frp) 项目。