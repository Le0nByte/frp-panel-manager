# Client 部署

Client 推荐使用 docker 部署

但直接部署在客户机中，可以通过远程终端直接在客户机以 root 权限执行命令，方便升级和管理。

注意 ⚠️：client 和 server 的启动指令可能会随着项目更新而改变，虽然在项目迭代时会注意前后兼容，但仍难以完全适配，因此 client 和 server 的启动指令以 master 生成为准

## 准备

打开 Master 的 webui 并登录，如果没有账号，请直接注册，第一个用户即为管理员

在侧边栏跳转到 `服务端`，点击上方的 `新建` 并输入 客户端 的唯一识别ID然后点击保存

![](./public/images/cn_client_list.png)

刷新后，新的客户端会出现在列表中。

部署client之前，需要修改服务端的配置，否则客户端无法正常连接

## 在 Linux 上部署

> ⚠️ **不要使用上游 `VaalaCat/frp-panel` 的 `install.sh` / `install.ps1` 一键脚本，也不要用上游 Release 的二进制。**
> 那是上游版本，不含本仓库的端口中心、事务安全与审计能力，客户端还会缺 `EVENT_INSPECT_PORTS` 等本仓库新增的 RPC，端口扫描将不可用。
> 本仓库的 WebUI 已移除一键安装按钮，改为在「操作指南」页给出完整步骤。二进制请从本仓库 Release（需仓库权限）获取或自行交叉编译，详见 [usage-guide.md §1.3](./usage-guide.md) 与 WebUI 侧边栏的「操作指南」。

### 直接运行

首先在系统上创建一个专用目录，把本仓库构建出的 `frp-panel` 二进制放进去并赋予执行权限：

```bash
sudo mkdir -p /opt/frp-panel && cd /opt/frp-panel
sudo cp ~/Downloads/frp-panel-linux-amd64 ./frp-panel
sudo chmod +x ./frp-panel
```

启动命令从 WebUI 获取：点击对应客户端的 `密钥 (点击查看启动命令)` 一列，复制出来的命令形如：

```bash
./frp-panel client -s abc -i user.c.client1 --api-url https://frpp.example.com --rpc-url grpc://frpp-rpc.example.com:9101
```

注意，如果你使用 反向代理 TLS，需要以 http 上游的形式，外部 443 端口代理 `master` 的 9100(API) 端口，并把 `--api-url` 改为带 443 端口的形式：

```bash
./frp-panel client -s abc -i user.c.client1 --api-url https://frpp.example.com:443 --rpc-url grpc://frpp-rpc.example.com:9101
```

命令行直接带 `-s <密钥>` 会让密钥出现在 `ps` 输出和 shell 历史里。生产环境请改用 `/etc/frpp/.env`（权限 600）承载密钥，做法见「操作指南」的 Linux 页签。

### Docker Compose 部署

点击对应客户端的 `密钥 (点击查看启动命令)` 一列中的隐藏字段，复制类似的启动命令如下备用：

```bash
./frp-panel client -s abc -i user.c.client1 --api-url https://frpp.example.com --rpc-url grpc://frpp-rpc.example.com:9101
```

注意，如果你使用 反向代理 TLS，需要修改这行命令类似如下：

```bash
./frp-panel client -s abc -i user.c.client1 --api-url https://frpp.example.com:443 --rpc-url grpc://frpp-rpc.example.com:9101
```

docker-compose.yaml

```yaml
version: '3'
services:
  frp-panel-client:
    image: vaalacat/frp-panel
    container_name: frp-panel-client
    network_mode: host
    restart: unless-stopped
    command: client -s abc -i user.c.client1 --api-url https://frpp.example.com:443 --rpc-url grpc://frpp-rpc.example.com:9101
```

### 安装为 systemd 服务

frp-panel 拥有管理 systemd 服务的能力，服务名为 `frpp`，内置了很多命令，请使用 `frp-panel --help` 查看支持的命令。这里给出一些例子：

- 安装特定参数的 client 到 systemd （支持任意的参数，包括server）
```bash
sudo ./frp-panel install [client 参数]
# eg. frp-panel install client -s abc -i user.c.client1 --api-url https://frpp.example.com:443 --rpc-url grpc://frpp-rpc.example.com:9101
```

- 卸载 frpp 服务
```bash
sudo ./frp-panel uninstall
```

- 启动 frpp 服务
```bash
sudo ./frp-panel start
```

- 停止 frpp 服务
```bash
sudo ./frp-panel stop
```

- 重启 frpp 服务
```bash
sudo ./frp-panel restart
```

## 在 Windows 上部署

### 直接运行

在 powershell 中，可执行文件的同目录下运行 WebUI 中复制的启动命令

```powershell
.\frp-panel.exe client -s abc -i user.c.client1 --api-url https://frpp.example.com:443 --rpc-url grpc://frpp-rpc.example.com:9101
```

### 安装为服务

与上方 Linux 的命令一致，修改文件名，去掉sudo执行即可

Windows 安装后使用示例：

```
C:/frpp/frpp.exe stop
C:/frpp/frpp.exe start
C:/frpp/frpp.exe uninstall
```
