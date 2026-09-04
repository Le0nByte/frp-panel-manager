# FRP-Panel-Manager 使用指南（小白版）

这篇指南面向**第一次使用**的人，按步骤带你完成：部署管理端 → 注册登录 → 绑定服务端 → 配置服务端 → 绑定客户端 → 配置客户端 → 日常使用。

全程大约 30 分钟。看不懂的名词先看下面的「基础概念」。

---

## 0. 基础概念（先花 2 分钟看懂）

FRP-Panel-Manager 帮你管理 FRP 内网穿透。先认识三个角色：

| 角色 | 是什么 | 比喻 |
|:--|:--|:--|
| **Master** | 管理平台本体（控制面），跑在一台公网服务器上，提供网页界面 | 总控制台 |
| **Server（服务端 / frps）** | 公网入口，把外部流量接收下来转给内网 | 大门口的信箱 |
| **Client（客户端 / frpc）** | 跑在内网机器上，把内网服务（如 NAS、远程桌面、网站）通过隧道暴露出去 | 你家里的门卫 |

一个典型场景：

```
你家的 NAS（内网，没有公网 IP）
        │  frpc 客户端 主动连接
        ▼
阿里云服务器（Master + frps 服务端，有公网 IP）
        ▲
外部访问：手机/电脑 → http://公网IP:端口 → 隧道 → NAS
```

关键点：**frpc 主动连出来**，所以你内网机器不需要公网 IP，也不需要路由器端口映射。

---

## 1. 部署 Master（管理平台）

### 1.1 你需要准备

- 一台**公网服务器**（推荐 2C2G 以上，本文以 Linux 为例）
- 一个域名或直接用公网 IP（后续 Agent 接入用）
- 目标内网机器若干（要暴露服务的机器）

### 1.2 方式 A：GitHub Actions 自动部署（推荐，已配置好 CI 时）

仓库已内置 `.github/workflows/build-deploy.yml`，流水线会自动完成「测试 → 构建 → SSH 部署」。你只需要：

1. 在 GitHub 仓库 Settings → Secrets and variables → Actions 添加以下 Secrets：

| Secret | 值 | 说明 |
|:--|:--|:--|
| `DEPLOY_HOST` | 服务器公网 IP | 例：`47.109.18.109` |
| `DEPLOY_PORT` | SSH 端口 | 默认 `22` |
| `DEPLOY_USER` | SSH 用户 | 例：`root` |
| `DEPLOY_SSH_KEY` | SSH 私钥全文 | 能登录服务器的私钥内容 |
| `MASTER_GLOBAL_SECRET` | 随机字符串（≥32 位） | 生成：`openssl rand -hex 32` |
| `MASTER_API_HOST` | 公网 IP 或域名 | 进入 TLS 证书 SAN，Agent 实际连接地址 |

2. 在 Actions 页面手动触发 `Build & Deploy FRP Panel` 的 `Run workflow`（或推送 `v*` tag）。

3. 流水线会在服务器上自动：准备目录 → 生成 `snapshot.key` → 生成 `/etc/frp-panel/frp-panel.env` → 安装 systemd 服务 → 启动 → 健康检查。

### 1.3 方式 B：手动部署（理解原理）

把 `frp-panel` 二进制（Linux amd64/arm64）上传到服务器。二进制来源有两种：

- 推 `v*` tag 后从本仓库 Release 下载（产物名 `frp-panel-<版本>-linux-amd64` / `-linux-arm64`）；仓库为私有，需要有仓库权限才能下载；
- 本地自行交叉编译：`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o frp-panel-linux-amd64 ./cmd/frpp`（前端需先 `cd www && pnpm build` 生成 `cmd/frpp/out`）。

> 不要用上游 `VaalaCat/frp-panel` 的 install.sh / install.ps1 一键脚本或其 Release 二进制：那是上游版本，不含本仓库的端口中心、事务安全与审计能力，客户端还会缺 `EVENT_INSPECT_PORTS` 等本仓库新增的 RPC，端口扫描将不可用。

```bash
sudo mkdir -p /opt/frp-panel/bin /opt/frp-panel/data /opt/frp-panel/secrets /etc/frp-panel
sudo cp frp-panel /opt/frp-panel/bin/frp-panel
sudo chmod 0755 /opt/frp-panel/bin/frp-panel
```

创建环境文件 `/etc/frp-panel/frp-panel.env`（**权限必须 0600**）：

```ini
APP_GLOBAL_SECRET=<openssl rand -hex 32 生成>
APP_ENABLE_REGISTER=false
MASTER_API_HOST=<公网IP或域名>
MASTER_API_PORT=9100
MASTER_RPC_HOST=127.0.0.1
MASTER_RPC_PORT=9101
CLIENT_TLS_INSECURE_SKIP_VERIFY=true
DB_TYPE=sqlite3
DB_DSN=/opt/frp-panel/data/data.db?_pragma=journal_mode(WAL)
SNAPSHOT_KEY_FILE=/opt/frp-panel/secrets/snapshot.key
```

生成快照密钥并安装 systemd 服务（参考 `deploy/systemd/frp-panel.service`）：

```bash
head -c 32 /dev/urandom > /opt/frp-panel/secrets/snapshot.key
chmod 0600 /opt/frp-panel/secrets/snapshot.key
sudo cp deploy/systemd/frp-panel.service /etc/systemd/system/frp-panel.service
sudo systemctl daemon-reload && sudo systemctl enable --now frp-panel
```

### 1.4 开放防火墙 / 安全组

| 端口 | 方向 | 用途 |
|:--|:--|:--|
| `9101/tcp` | 公网 → Master | Agent（frpc/frps）接入 RPC |
| `9443/tcp` | 公网 → Master | Web 管理界面（HTTPS 反代） |
| `9100/tcp` | 仅本机 | 管理 API（回环，不要对公网开放） |

如果用了阿里云/腾讯云，记得在**安全组**也放行 9101 和 9443。

### 1.5 配 HTTPS 反代（可选但推荐）

Master API 只监听回环 `127.0.0.1:9100`，需要 Nginx 反代出去：

```nginx
server {
    listen 9443 ssl;
    server_name _;
    ssl_certificate     /opt/frp-panel/tls/frp-panel.crt;
    ssl_certificate_key /opt/frp-panel/tls/frp-panel.key;
    location / {
        proxy_pass http://127.0.0.1:9100;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

证书可以自签发（浏览器会提示风险，选择继续访问即可）：

```bash
sudo mkdir -p /opt/frp-panel/tls
sudo openssl req -x509 -nodes -newkey rsa:2048 \
  -keyout /opt/frp-panel/tls/frp-panel.key -out /opt/frp-panel/tls/frp-panel.crt \
  -days 825 -subj "/CN=<公网IP>" \
  -addext "subjectAltName=IP:<公网IP>"
```

> 有域名的话建议签 Let's Encrypt 正式证书（见 `docs/ops-runbook.md`）。

### 1.6 验证 Master 已启动

```bash
systemctl status frp-panel        # 应显示 active (running)
curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9100/   # 应输出 200
ss -lntp | grep -E ':(9100|9101)'  # 应看到 9100(回环) 和 9101(公网) 在监听
```

---

## 2. 注册管理员并登录

1. 浏览器打开 `https://<公网IP>:9443`
2. 首次访问会看到自签证书警告 → 点击「高级」→「继续前往」
3. 点击注册，填写**用户名 / 密码 / 邮箱**
4. **第一个注册的用户自动成为管理员**（之后注册功能默认关闭，不会再有人能注册）

> 如果注册提示「register is disabled」，说明已存在用户。忘记密码需联系管理员在服务器处理（见常见问题）。

---

## 3. 绑定服务端（frps）

### 3.1 用内置默认服务端（最简单）

Master 自带一个**默认服务端（ID 为 `default`）**，它随 Master 一起启动，公网入口即时可用。**绝大多数场景不需要新建服务端**，直接跳到第 5 步绑定客户端即可。

> 总览页「服务端总数」会显示 1（default），代表这个内置服务端。

### 3.2 新建额外服务端（需要多台公网入口机时）

1. 左侧菜单进入「服务端」页面
2. 点「新建」，输入唯一 ID（如 `srv.beijing`），保存
3. 列表中出现该服务端，点击「密钥/启动命令」列，复制启动命令
4. 在**那台公网机器**上执行（Linux 安装为服务）：

```bash
sudo ./frp-panel server -s <SECRET> -i srv.beijing \
  --api-url https://<公网IP>:9443 \
  --rpc-url grpc://<公网IP>:9101
```

> 注意：`--api-url` / `--rpc-url` 必须填 Agent 能连到的**公网地址**，不要填 127.0.0.1（那是它自己）。

---

## 4. 配置服务端（frps 参数）

在「服务端」页面点击该服务端进入详情，可配置：

| 参数 | 说明 | 示例 |
|:--|:--|:--|
| `bindPort` | frps 对外监听端口（客户端隧道入口） | `7002` |
| `bindAddr` | 监听地址 | `0.0.0.0` |
| `auth.token` | frps 与 frpc 之间校验令牌 | 随机长字符串 |
| `allowPorts` | 允许客户端使用的公网端口范围 | `7000-7020` |
| `maxPortsPerClient` | 每个客户端最多占用端口数 | `10` |
| 带宽/日志 | 限速与日志级别 | 按需 |

配置说明：

- **端口范围要和你安全组/防火墙放行的一致**（例如放行 `7000-7020/tcp`）。
- 修改后保存，Master 会推送新配置给对应 frps 并热更新。
- 内置 default 服务端同样可以在这里配置（首次配置后它才从「未配置」变为「已配置」）。

---

## 5. 绑定客户端（frpc）

1. 左侧菜单进入「客户端」页面
2. 点「新建」，输入唯一 ID（建议带机器名，如 `client.nas.home`），保存
3. 列表出现该客户端，点击「密钥/启动命令」列，复制启动命令
4. 在**内网目标机器**上执行启动命令

### 5.1 Linux 客户端（安装为 systemd 服务）

推荐把身份和地址写进 `/etc/frpp/.env`（密钥不进服务文件，权限 600）：

```bash
cd ~/Downloads   # 换成下载文件实际所在目录
sudo install -m 0755 frp-panel-<版本>-linux-amd64 /usr/local/bin/frp-panel
sudo mkdir -p /etc/frpp
sudo tee /etc/frpp/.env > /dev/null <<'EOF'
CLIENT_ID=client.nas.home
CLIENT_SECRET=<SECRET>
CLIENT_API_URL=https://<公网IP>:9443
CLIENT_RPC_URL=grpc://<公网IP>:9101
CLIENT_TLS_INSECURE_SKIP_VERIFY=true
EOF
sudo chmod 600 /etc/frpp/.env

sudo frp-panel install client   # 只写 unit + enable，不会启动
sudo frp-panel start            # 必须再执行一次才真正运行
```

要点（均已实测）：

| 事实 | 说明 |
|:--|:--|
| 服务名是 `frpp` | `defs.DefaultServiceName`；用 `systemctl status frpp` 查看 |
| `install` 不会启动服务 | kardianos/service 的 systemd 分支只做「写 unit + `systemctl enable` + `daemon-reload`」；执行后 `is-enabled=enabled` 但 `is-active=inactive` |
| `CLIENT_TLS_INSECURE_SKIP_VERIFY` 没有命令行参数 | 只能通过 env / `.env` 文件传入；默认 `false`，自签证书必须显式设 `true` |
| 配置优先级 | 进程环境变量（`/etc/sysconfig/frpp`）> 二进制同目录 `.env` > `/etc/frpp/.env`；`godotenv.Load` 不覆盖已存在的变量 |
| 崩溃重启间隔默认 120s | unit 模板固定 `Restart=always` + `RestartSec=120`；要更快就加 drop-in（见下方代码块），`install` 不会影响 drop-in |
| `install` 遇已存在的 unit 会报错 | kardianos/service 先 `os.Stat` unit 文件，存在即返回 `Init already exists`，不覆盖；换二进制路径要先 `uninstall` 再 `install` |
| unit 权限 644 | 带 `-s <SECRET>` 安装会把密钥明文写进 `ExecStart`，同机任何用户可读；多用户机器请用上面的 `.env` 方式 |
| `uninstall` 保留二进制 | 只删 unit（`disable` + 删文件）；二进制需自己删 |
| 非 root 安装会失败 | `open /etc/systemd/system/frpp.service: permission denied`，必须 `sudo` |

也可以直接带参数安装（快，但密钥进 unit）：

```bash
sudo frp-panel install client -s <SECRET> -i client.nas.home \
  --api-url https://<公网IP>:9443 \
  --rpc-url grpc://<公网IP>:9101
sudo frp-panel start
```

无 systemd 的环境（老容器 / 精简系统）用前台方式，自己接守护：

```bash
nohup env CLIENT_TLS_INSECURE_SKIP_VERIFY=true \
  /usr/local/bin/frp-panel client -s <SECRET> -i client.nas.home \
  --api-url https://<公网IP>:9443 \
  --rpc-url grpc://<公网IP>:9101 \
  > /var/log/frpp-client.log 2>&1 &
```

> 这种写法密钥会出现在进程命令行（同机任何用户 `ps` 可见）与 shell 历史中，仅建议单用户/可信机器使用；多用户机器请用 systemd + `/etc/frpp/.env`（600）。

需要更快的崩溃自动拉起时，加一个 drop-in（不改动 `install` 生成的 unit）：

```bash
sudo mkdir -p /etc/systemd/system/frpp.service.d
sudo tee /etc/systemd/system/frpp.service.d/override.conf > /dev/null <<'EOF'
[Service]
RestartSec=5
EOF
sudo systemctl daemon-reload
sudo systemctl restart frpp
```

### 5.2 Windows 客户端

在 PowerShell 中，二进制同目录执行：

```powershell
$env:CLIENT_TLS_INSECURE_SKIP_VERIFY="true"
.\frp-panel.exe client -s <SECRET> -i client.nas.home --api-url https://<公网IP>:9443 --rpc-url grpc://<公网IP>:9101
```

开机自启可参考 `docs/deploy-client.md` 的 Windows 服务方式。

### 5.3 Android 客户端（Magisk 环境，开机自启）

把二进制放到 `/data/frp-panel/bin/frp-panel`，新建 `/data/adb/service.d/frpp-client.sh`：

```sh
#!/system/bin/sh
sleep 30
FRPP_DIR=/data/frp-panel
if pidof frp-panel >/dev/null 2>&1; then exit 0; fi
nohup env CLIENT_TLS_INSECURE_SKIP_VERIFY=true "$FRPP_DIR/bin/frp-panel" client \
  -s <SECRET> -i client.phone \
  --api-url https://<公网IP>:9443 \
  --rpc-url grpc://<公网IP>:9101 \
  > "$FRPP_DIR/logs/client.log" 2>&1 &
```

```bash
chmod 755 /data/adb/service.d/frpp-client.sh
```

### 5.4 验证客户端已上线

- Web UI「客户端」列表该客户端显示在线；
- Master 总览「客户端总数」+1；
- Linux systemd：`systemctl is-active frpp` → `active`，`systemctl is-enabled frpp` → `enabled`；
- 客户端日志（`journalctl -u frpp -n 40 --no-pager`）依次出现：
  - `load env file success: /etc/frpp/.env`（配置读到了）
  - `start to run client`
  - `client get server register envent success`（Master 已认；`envent` 是上游源码里的拼写，搜索时用 `register` 更稳）

---

## 6. 配置客户端（创建隧道）

1. 在「客户端」页面点击在线客户端进入详情
2. 进入「代理/隧道」列表，点「新建代理」
3. 选择协议并填写：

| 字段 | 说明 | 示例（把内网 NAS 的 5000 端口暴露出去） |
|:--|:--|:--|
| 代理名称 | 唯一标识 | `nas-web` |
| 类型 | tcp / udp / http / https | `tcp` |
| 本地 IP | 内网服务地址 | `192.168.1.10` |
| 本地端口 | 内网服务端口 | `5000` |
| 远程端口 | 公网入口端口（需在服务端允许范围内） | `7050` |

4. 保存后 Master 通过 RPC 推送配置给该客户端，frpc 自动启动隧道

### 6.1 验证隧道

在**任意公网机器**上访问：

```bash
curl http://<公网IP>:7050    # 应能访问到内网 NAS 的 5000 端口
```

Web UI 中该代理状态显示运行中，可查看实时日志。

---

## 7. 日常使用

| 操作 | 位置 |
|:--|:--|
| 查看所有节点在线状态 | 总览页（服务端/客户端计数、在线状态） |
| 启停隧道 | 客户端详情 → 代理列表 → 启动/停止 |
| 查看实时日志 | 节点详情 → 日志 |
| 修改隧道配置 | 客户端详情 → 代理 → 编辑（会热更新） |
| 端口占用检查 | 新建代理时系统会自动预检端口是否可预约 |

---

## 8. 常见问题（FAQ）

**Q1：浏览器提示证书不安全？**
自签证书的正常现象。点「高级 → 继续前往」即可。要消除提示，请换 Let's Encrypt 正式证书（见 `docs/ops-runbook.md`）。

**Q2：总览显示服务端/客户端数为 0，但明明有节点？**
先刷新页面。若仍为 0，检查节点是否真的在线（客户端列表是否显示在线）。最新版本已修复 default 服务端计数问题。

**Q3：Agent 日志报 `certificate signed by unknown authority`？**
自签证书部署必须设置环境变量 `CLIENT_TLS_INSECURE_SKIP_VERIFY=true`（Linux 前台运行加在命令前，systemd 写入 EnvironmentFile）。有正式证书后才可以改为 false。

**Q4：Agent 日志报 `not 0.0.0.0` 之类的 x509 错误？**
Master 的 `MASTER_RPC_HOST` 应设为 `127.0.0.1`（内部自连地址），不要设 `0.0.0.0`。

**Q5：公网访问隧道不通？**
依次排查：① 服务端 `bindPort` 和客户端 `remotePort` 是否一致；② 云安全组/防火墙是否放行了该端口；③ 服务端 `allowPorts` 是否包含该端口；④ 客户端日志是否有报错。

**Q6：注册提示 disabled，怎么加用户？**
`APP_ENABLE_REGISTER` 默认 false 且已有用户。临时改为 true 重启后可注册新用户，注册完立即改回 false 并重启。

**Q7：如何更新到新版本？**
推送代码后手动触发 GitHub Actions 流水线（或打 tag），会自动构建并 SSH 部署；`ci-deploy-master.sh` 会保留服务器上已有配置。

**Q8：如何备份？**
使用 `deploy/backup-aliyun.sh` / `deploy/rollback-aliyun.sh`（含 SQLite 快照、manifest 校验），详见 `docs/ops-runbook.md`。

**Q9：Linux 上 `frp-panel install client` 执行成功，但服务没跑起来？**
预期行为。`install` 只写 unit 文件 + `systemctl enable`（开机自启），不会启动；必须再执行 `sudo frp-panel start`（或 `systemctl start frpp`）。

**Q10：Linux 改了 `/etc/frpp/.env` 但不生效？**
配置只在进程启动时读一次，改完要 `sudo frp-panel restart`。若仍是旧值，检查是否被更高优先级来源覆盖：进程环境变量（`/etc/sysconfig/frpp`）> 二进制同目录 `.env` > `/etc/frpp/.env`。

**Q11：Linux 服务 `start` 后仍是 `inactive`，日志也没有输出？**
看 `systemctl status frpp`。若出现 `Condition: start condition failed ... ConditionFileIsExecutable`，说明 unit 里记录的二进制路径已不存在或丢了执行权限（常见于移动/替换二进制后）。注意直接重新 `install` 会报 `Init already exists`（kardianos/service 在 unit 已存在时不覆盖），必须先卸载再装：

```bash
sudo frp-panel uninstall
sudo frp-panel install client   # 在新二进制所在目录执行，unit 会按其真实路径重建
sudo frp-panel start
```

---

## 9. 安全建议

- `MASTER_GLOBAL_SECRET`、节点 secret 属于凭据，**不要提交到仓库或日志**；
- 管理 API（9100）保持回环监听，不要直接暴露公网；
- 生产环境建议用正式域名证书，并把 `CLIENT_TLS_INSECURE_SKIP_VERIFY` 改为 `false`；
- 高风险功能（远程 Shell / Worker / WireGuard）默认关闭，按需开启并做好授权审计；
- 定期用备份脚本做快照，升级前先备份。

---

*更详细的部署与运维参考：`docs/deploy-master.md`、`docs/deploy-client.md`、`docs/deploy-server.md`、`docs/all-configs.md`、`docs/ops-runbook.md`。*