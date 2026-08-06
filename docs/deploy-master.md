# Master 部署

Master 推荐使用 docker 部署！不推荐直接安装到服务器中

会给出三种部署方式，任选一种即可

部署后没有默认用户，注册的第一个用户即为管理员，为了安全，默认不开启多用户注册

程序的默认存储数据路径和程序文件同目录，如需修改请参考配置表格

重要！：如果你想只部署 master，同时作为 server 运行，请不要忘记启动 master 后，在 webui 的配置服务端中修改 default 的配置

## 前期准备

### 服务器开放公网端口：

- **WEBUI 端口**: 默认 `TCP 9100`（仅回环监听）
- **RPC 端口**: 默认 `TCP 9101`
- **frps 的API端口**：没有默认，请随意预留，例子使用 `TCP/UDP 7000`
- **frps 对外开放的服务端口**：没有默认，请随意预留，例子使用 `TCP/UDP 26999-27050`

API 9100 只绑定回环地址，不能直接暴露公网；公网管理访问由反向代理转发到 `127.0.0.1:9100`。

Agent TLS gRPC 9101 必须保持公网开放，不能因为使用 API 反向代理而关闭或复用。生产环境不把 API、h2c 和 Agent gRPC 混用。

如果你想要使用安全的方式（反向代理），请参考下图设置环境变量「`CLIENT_RPC_URL`和`CLIENT_API_URL`」。

注意⚠️：请首先使用普通部署的方式部署成功！然后再来调整这两个变量！！！！

橙色是不安全，绿色是安全。你需要保证两个环境变量都有设置，才能正常工作

![](./public/images/frp-panel-platform-connection-env.svg)

> 测试端口是否开放的方法（以8080为例），在服务器上运行：
> ```shell
> python3 -m http.server 8080
> ```
> 然后在另一台电脑/服务器上执行：
> ```shell
> curl http://服务器公网IP/域名:8080 -I
> ```
> 成功的话，输出类似
> ```
> HTTP/1.0 200 OK
> Server: SimpleHTTP/0.6 Python/3.11.0
> Date: Sat, 12 Apr 2025 17:12:15 GMT
> Content-type: text/html; charset=utf-8
> Content-Length: 8225
> ```

## 在 Linux 上部署

### 方式一：Docker Compose 部署

> `MASTER_API_HOST` 必须填写 Agent 实际连接使用的公网 IP 或 DNS；该值会进入 TLS 证书 SAN。Master API 仍只监听 `127.0.0.1:9100`，公网管理入口由 HTTPS 反向代理提供。

服务器需要安装docker和docker compose

首先创建一个`docker-compose.yaml`文件，写入以下内容

```yaml
services:
  frpp-master:
    image: vaalacat/frp-panel:latest
    network_mode: host
    environment:
      APP_GLOBAL_SECRET: your_secret # 随便输入一些随机字符，不要泄露
      MASTER_RPC_HOST: 1.2.3.4 # 服务器的外部IP或域名
      MASTER_RPC_PORT: 9101 # 公网 Agent TLS gRPC 监听端口
      MASTER_API_HOST: 1.2.3.4 # 服务器的外部IP或域名
      MASTER_API_PORT: 9100 # loopback API/WebUI port; public HTTPS is provided by the reverse proxy
      # CLIENT_RPC_URL和CLIENT_API_URL请根据实际情况设置，设置为外部可以通过url访问到master的形式
      # Client 连接 master RPC 的 URL，如果使用反向代理，请设置为通过反向代理访问的 URL（如grpc://PUBLIC_IP_OR_DNS:9101）
      CLIENT_RPC_URL: grpc://1.2.3.4:9101
      # Client 连接 master API/WebUI 的 URL，如果使用反向代理，请设置为通过反向代理访问的 URL（如https://example.com:443）
      CLIENT_API_URL: https://1.2.3.4:9443
    volumes:
      - ./data:/data # 数据存储位置
    restart: unless-stopped
    command: master
```

### 方式二：Docker 命令部署

服务器需要安装 docker，我们推荐使用 host 网络模式部署 `Master`

```bash
# 推荐
# MASTER_RPC_HOST 是 master 内部自连地址（RPC 监听本身绑定所有接口），填 127.0.0.1 即可；
# MASTER_API_HOST 必须填写 Agent 实际连接的公网 IP 或 DNS（进入 TLS 证书 SAN）
# APP_GLOBAL_SECRET注意不要泄漏，客户端和服务端的是通过Master生成的
# CLIENT_RPC_URL和CLIENT_API_URL请根据实际情况设置
# 如果使用反向代理，请设置为通过反向代理访问的 URL，也就是外部如何访问master
# 如 443端口代理example.com到回环 9100端口
# CLIENT_RPC_URL=grpc://PUBLIC_IP_OR_DNS:9101
# CLIENT_API_URL=https://example.com:443
docker run -d \
	--network=host \
	--restart=unless-stopped \
	-v /opt/frp-panel:/data \
	-e APP_GLOBAL_SECRET=your_secret \
	-e MASTER_RPC_HOST=127.0.0.1 \
	-e MASTER_API_HOST=PUBLIC_IP_OR_DNS \
	-e CLIENT_RPC_URL=grpc://PUBLIC_IP_OR_DNS:9101 \
	-e CLIENT_API_URL=https://PUBLIC_IP_OR_DNS:9443 \
	vaalacat/frp-panel
```

> 本 fork 不支持使用 Docker bridge 端口映射部署 Master：应用内 API 固定绑定容器回环地址，docker-proxy 无法转发到该地址。请使用上面的 `--network=host` 方式。

### 方式三：宿主机 HTTPS 反向代理（推荐）

Master API 固定监听 `127.0.0.1:9100`。反向代理必须运行在宿主机网络命名空间中（宿主机服务，或 Linux 上显式使用 `network_mode: host`），不得使用 bridge 网络中的容器通过容器 IP 访问 9100。

部署要求：

1. Master 使用 `network_mode: host`，并设置 `MASTER_API_HOST=PUBLIC_IP_OR_DNS`。
2. Nginx、Caddy 或 Traefik 在宿主机网络中监听公网 HTTPS `9443`（或 `443`），上游为 `http://127.0.0.1:9100`。
3. Agent TLS gRPC `9101` 保持公网开放，不通过 API 反向代理复用。
4. Agent 使用 `CLIENT_API_URL=https://PUBLIC_IP_OR_DNS:9443` 与 `CLIENT_RPC_URL=grpc://PUBLIC_IP_OR_DNS:9101`。

生产配置、证书、验证和回滚步骤以 [`ops-runbook.md`](ops-runbook.md) 为准。

## 在 Windows 上部署

### 直接运行

在下载的可执行文件同名文件夹下创建一个 `.env` 文件(注意不要有后缀名)，然后输入以下内容保存后运行对应命令

```
APP_GLOBAL_SECRET=your_secret
DB_DSN=data.db
MASTER_API_HOST=IP
CLIENT_RPC_URL=grpc://IP:9101
CLIENT_API_URL=https://IP:9443
```

- master: `frp-panel-amd64.exe master`
