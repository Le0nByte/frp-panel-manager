'use client'

import { Providers } from '@/components/providers'
import { RootLayout } from '@/components/layout'
import { Header } from '@/components/header'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useTranslation } from 'react-i18next'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Terminal, Download, Monitor, CheckCircle, Info, ShieldAlert, Wrench } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { getPlatformInfo, getMasterVersion } from '@/api/platform'

const StepTitle = ({ n, children }: { n: number; children: React.ReactNode }) => (
  <h3 className="flex items-center gap-2 text-lg font-semibold">
    <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">
      {n}
    </span>
    {children}
  </h3>
)

const Code = ({ children, tone }: { children: React.ReactNode; tone?: 'good' }) => (
  <pre
    className={
      'overflow-x-auto rounded p-3 font-mono text-xs leading-relaxed ' +
      (tone === 'good' ? 'bg-green-50 text-green-800' : 'bg-muted')
    }
  >
    {children}
  </pre>
)

const PLACEHOLDER = '…'

// 本指南不得写死任何属于某一次部署的值（IP、端口、客户端 ID、安装路径、仓库地址）。
// 读者跑的是自己的 Master；写死的地址会把人带到错误端口、错误仓库或连不上的 127.0.0.1。
// 所有环境相关的值一律从 Master 自身的 platform info 与浏览器当前 origin 读取 ——
// 这也正是平台下发给客户端的同一份地址。
function useGuideEnv() {
  const { data: info } = useQuery({ queryKey: ['getPlatformInfo'], queryFn: getPlatformInfo })
  const { data: version } = useQuery({ queryKey: ['getMasterVersion'], queryFn: getMasterVersion })

  const apiUrl = info?.clientApiUrl || PLACEHOLDER
  const rpcUrl = info?.clientRpcUrl || PLACEHOLDER

  // 读者此刻正在访问的控制台地址：它的 origin 一定是本部署对外可达的地址。
  const consoleUrl =
    typeof window !== 'undefined' && window.location.origin !== 'null' ? window.location.origin : PLACEHOLDER

  return { version, apiUrl, rpcUrl, consoleUrl }
}

// GuideBody 必须在 <Providers> 内部渲染：useQuery 依赖 QueryClientProvider，
// 若在 Provider 之外调用，`next build` 的静态预渲染会因缺少 QueryClient 直接失败。
function GuideBody() {
  const { t } = useTranslation()
  const { version, apiUrl, rpcUrl, consoleUrl } = useGuideEnv()

  return (
    <RootLayout mainHeader={<Header title={t('nav.guide')} />}>
      <div className="mx-auto max-w-4xl space-y-6 p-4">
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>前置条件（两种系统通用）</AlertTitle>
          <AlertDescription>
            Master 管理端已部署并运行，且已注册管理员账号。如果还没有，请先完成部署和注册。
            <div className="mt-2 space-y-1 font-mono text-xs">
              <div>Master 控制台：{consoleUrl}</div>
              <div>客户端 API 地址：{apiUrl}</div>
              <div>客户端 RPC 地址：{rpcUrl}</div>
            </div>
            <div className="mt-2 text-xs">
              以上三个地址由本 Master 实时给出，直接照抄。不要改成 <code>127.0.0.1</code>——内网机器连不上。
              {version?.gitVersion && (
                <>
                  {' '}
                  当前 Master 版本：<code>{version.gitVersion}</code>
                </>
              )}
            </div>
          </AlertDescription>
        </Alert>

        <Tabs defaultValue="linux" className="w-full">
          <TabsList className="h-10">
            <TabsTrigger value="linux" className="gap-1.5">
              <Terminal className="h-4 w-4" />
              Linux
            </TabsTrigger>
            <TabsTrigger value="windows" className="gap-1.5">
              <Monitor className="h-4 w-4" />
              Windows
            </TabsTrigger>
          </TabsList>

          {/* ==================== Linux ==================== */}
          <TabsContent value="linux" className="mt-4 space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Download className="h-5 w-5" />
                  添加内网机器为客户端（Linux 版）
                </CardTitle>
                <CardDescription>
                  把内网 Linux 机器（NAS、树莓派、办公室服务器等，无公网 IP）接入 frp-panel，安装为 systemd
                  服务并开机自启。全程约 5 分钟。
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Step 1 */}
                <div className="space-y-2">
                  <StepTitle n={1}>下载客户端二进制</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <p>先确认 CPU 架构，决定下载哪个文件：</p>
                    <Code>{`uname -m
# x86_64  → 下载 linux-amd64（绝大多数服务器 / PC）
# aarch64 → 下载 linux-arm64（树莓派 4/5、ARM 云主机、Android）`}</Code>
                    <p>到本平台的 Release 页面下载对应架构的文件（仓库为私有，需要仓库权限；没有权限请找管理员取）。</p>
                    <Code>{`本仓库 Releases 页面 → 下载与 uname -m 匹配的 linux-amd64 / linux-arm64`}</Code>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ 不要用上游一键安装脚本</AlertTitle>
                      <AlertDescription>
                        网上流传的 <code>curl .../VaalaCat/frp-panel/main/install.sh | bash</code>{' '}
                        能跑通，但装的是上游版本，不含本平台的端口中心、事务安全与审计能力，端口扫描（
                        <code>EVENT_INSPECT_PORTS</code>）也会失效。没有仓库权限时，请找管理员取二进制。
                      </AlertDescription>
                    </Alert>
                    <p>
                      文件名形如 <code>frp-panel-&lt;版本&gt;-linux-amd64</code>，在下载文件所在目录（浏览器下载通常是{' '}
                      <code>~/Downloads</code>）执行，安装到 <code>/usr/local/bin</code> 并确认能运行：
                    </p>
                    <Code>{`cd ~/Downloads   # 换成你实际存放下载文件的目录
sudo install -m 0755 frp-panel-<版本>-linux-amd64 /usr/local/bin/frp-panel
frp-panel version`}</Code>
                    <p>
                      <code>version</code> 会打印 <code>BinVersion</code> / <code>GitCommit</code> /{' '}
                      <code>Platform</code>，用来核对版本与架构：
                    </p>
                    <Code>{`BinVersion: v1.1.1              # 应与你下载的版本一致
GitCommit: fa8486ae...           # 空值说明这不是正式发布包
Platform: linux/amd64           # 要与本机 uname -m 对应`}</Code>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ 显示 dev-build 说明包不对</AlertTitle>
                      <AlertDescription>
                        若 <code>BinVersion</code> 是 <code>dev-build</code>、<code>GitCommit</code> 为空、
                        <code>BuildDate</code> 为 <code>1970-01-01</code>，说明这个二进制是本地直接{' '}
                        <code>go build</code> 的、没注入版本号，不是 Release 包。
                        功能可能一样，但升级和排障时无法确认版本，请换用 Release 页的包。
                      </AlertDescription>
                    </Alert>
                  </div>
                </div>

                {/* Step 2 */}
                <div className="space-y-2">
                  <StepTitle n={2}>创建客户端 ID</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <ol className="list-decimal space-y-1 pl-5">
                      <li>
                        浏览器打开 <code className="mx-1">{consoleUrl}</code>
                        （自签证书警告 → 继续访问）
                      </li>
                      <li>用管理员账号登录</li>
                      <li>
                        左侧菜单点击 <strong>客户端</strong>
                      </li>
                      <li>
                        页面顶部，点击 <strong>新建客户端</strong> 按钮
                      </li>
                      <li>
                        弹窗中输入客户端唯一 ID，只能用小写字母、数字、<code>-</code>、<code>_</code>，例如{' '}
                        <code>home-nas</code>
                      </li>
                      <li>
                        点击 <strong>提交</strong> 按钮
                      </li>
                      <li>
                        在该行右侧点 <strong>⋮</strong> 操作菜单 → <strong>复制启动命令</strong>，从中取出{' '}
                        <code>-s</code> 后的密钥与 <code>-i</code> 后的客户端 ID 备用
                      </li>
                    </ol>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ 命令里的地址不能直接用</AlertTitle>
                      <AlertDescription>
                        复制出来的命令，<code>--api-url</code> / <code>--rpc-url</code> 可能是 <code>127.0.0.1</code>
                        （内网机器连不上）。下一步统一填本页顶部给出的地址：
                        <code className="mx-1">{apiUrl}</code> 和 <code className="mx-1">{rpcUrl}</code>。
                      </AlertDescription>
                    </Alert>
                  </div>
                </div>

                {/* Step 3 */}
                <div className="space-y-2">
                  <StepTitle n={3}>写配置文件（推荐：密钥不进服务文件）</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <p>
                      把身份和地址写进 <code>/etc/frpp/.env</code>，权限设为 600（仅 root 可读）：
                    </p>
                    <Code>{`sudo mkdir -p /etc/frpp
sudo tee /etc/frpp/.env > /dev/null <<'EOF'
CLIENT_ID=<第2步复制命令里的 -i 值>
CLIENT_SECRET=<第2步复制的密钥>
CLIENT_API_URL=${apiUrl}
CLIENT_RPC_URL=${rpcUrl}
CLIENT_TLS_INSECURE_SKIP_VERIFY=true
EOF
sudo chmod 600 /etc/frpp/.env`}</Code>
                    <p>
                      <code>CLIENT_TLS_INSECURE_SKIP_VERIFY</code> 默认是 <code>false</code>，自签证书部署必须显式写{' '}
                      <code>true</code>，否则连接会报 <code>certificate signed by unknown authority</code>。
                    </p>
                  </div>
                </div>

                {/* Step 4 */}
                <div className="space-y-2">
                  <StepTitle n={4}>安装为 systemd 服务并启动</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <Code>{`sudo frp-panel install client
sudo frp-panel start`}</Code>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ install 不会自动启动</AlertTitle>
                      <AlertDescription>
                        <code>install</code> 只做两件事：写 <code>/etc/systemd/system/frpp.service</code> +{' '}
                        <code>systemctl enable</code>（开机自启）。服务此时仍是 <code>inactive</code>，必须再执行一次{' '}
                        <code>start</code> 才会真正跑起来。
                      </AlertDescription>
                    </Alert>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ start 可能卡住一到两分钟</AlertTitle>
                      <AlertDescription>
                        实测 <code>start</code> 会等 systemd 把服务拉起来再返回，慢的时候约 2 分钟才打印{' '}
                        <code>controller start service success</code>。这不是卡死，不要中途 Ctrl+C；等它返回后再做第 5
                        步验证。
                      </AlertDescription>
                    </Alert>
                    <p>
                      服务名是 <code>frpp</code>（不是 frp-panel）。因为配置已在 <code>/etc/frpp/.env</code>
                      ，这里不需要带任何参数。
                    </p>
                  </div>
                </div>

                {/* Step 5 */}
                <div className="space-y-2">
                  <StepTitle n={5}>
                    <span className="flex items-center gap-1">
                      <CheckCircle className="h-4 w-4 text-green-600" /> 验证已上线
                    </span>
                  </StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <Code>{`systemctl is-active frpp     # 期望：active
systemctl is-enabled frpp    # 期望：enabled
sudo journalctl -u frpp -n 40 --no-pager`}</Code>
                    <p>日志里应能看到这几条（说明配置读到了、客户端起来了、握手完成了）：</p>
                    <Code>{`load env file success: /etc/frpp/.env
start to run client
client get server register envent success`}</Code>
                    <p>
                      最后一条的 <code>envent</code> 是上游源码里的拼写，不是笔误 —— 按 <code>register</code>{' '}
                      搜索最稳妥。
                    </p>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ register 成功 ≠ 客户端已上线</AlertTitle>
                      <AlertDescription>
                        这一行之后如果还有 <code>rpc auth token is invalid</code> 或{' '}
                        <code>cannot pull client config ... record not found</code>，说明握手通了但身份不被认可。
                        常见原因：<code>CLIENT_ID</code> 与 WebUI 里的名字不一致（漏了用户名前缀）、密钥复制不全、
                        或这台机器上还有旧进程占着同一个 ID。必须看到 WebUI 里显示<strong>在线</strong>才算成功。
                      </AlertDescription>
                    </Alert>
                    <ul className="list-disc space-y-1 pl-5">
                      <li>
                        WebUI 刷新 <strong>客户端</strong> 页面，该客户端应显示<strong>在线</strong>（绿色圆点）
                      </li>
                      <li>总览页「客户端总数」应 +1</li>
                      <li>
                        PowerShell 窗口里出现 <code>rpc auth token is invalid</code> 或 <code>record not found</code>{' '}
                        时客户端<strong>没有</strong>上线，回到第 3 步核对 ID 与密钥
                      </li>
                    </ul>
                  </div>
                </div>

                {/* Step 6 */}
                <div className="space-y-2">
                  <StepTitle n={6}>
                    <span className="flex items-center gap-1">
                      <Wrench className="h-4 w-4" /> 日常运维命令
                    </span>
                  </StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <div className="overflow-x-auto">
                      <table className="w-full text-left text-sm">
                        <thead>
                          <tr className="border-b">
                            <th className="py-2 pr-4 font-medium">目的</th>
                            <th className="py-2 font-medium">命令</th>
                          </tr>
                        </thead>
                        <tbody className="font-mono text-xs">
                          <tr className="border-b">
                            <td className="py-1.5 pr-4 font-sans">启动</td>
                            <td className="py-1.5">sudo frp-panel start</td>
                          </tr>
                          <tr className="border-b">
                            <td className="py-1.5 pr-4 font-sans">停止</td>
                            <td className="py-1.5">sudo frp-panel stop</td>
                          </tr>
                          <tr className="border-b">
                            <td className="py-1.5 pr-4 font-sans">重启（改完配置必做）</td>
                            <td className="py-1.5">sudo frp-panel restart</td>
                          </tr>
                          <tr className="border-b">
                            <td className="py-1.5 pr-4 font-sans">实时看日志</td>
                            <td className="py-1.5">sudo journalctl -u frpp -f</td>
                          </tr>
                          <tr>
                            <td className="py-1.5 pr-4 font-sans">卸载服务（保留二进制）</td>
                            <td className="py-1.5">sudo frp-panel uninstall</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                    <p>
                      这些子命令就是 <code>systemctl</code> 的包装，也可以直接用{' '}
                      <code>sudo systemctl restart frpp</code>，效果一致。
                    </p>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ uninstall 不会停掉已运行的进程</AlertTitle>
                      <AlertDescription>
                        实测 <code>uninstall</code> 只删 unit 文件，<strong>正在跑的进程会变成孤儿继续运行</strong>，
                        仍占着该客户端 ID。重新安装后会出现两个实例抢同一个 ID，表现为日志里反复{' '}
                        <code>rpc auth token is invalid</code>。卸载后请确认没有残留：
                      </AlertDescription>
                    </Alert>
                    <Code>{`sudo frp-panel uninstall
pgrep -a frp-panel                  # 有输出说明进程还在
sudo pkill -f 'frp-panel client'    # 需要时手动结束
systemctl is-active frpp            # 期望：inactive / unknown`}</Code>
                  </div>
                </div>

                {/* Step 7 */}
                <div className="space-y-2">
                  <StepTitle n={7}>崩溃后更快自动拉起（可选，建议做）</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <p>
                      默认 unit 是 <code>Restart=always</code> + <code>RestartSec=120</code>，进程异常退出后要等{' '}
                      <strong>2 分钟</strong>才重启。加个 drop-in 改成 5 秒：
                    </p>
                    <Code>{`sudo mkdir -p /etc/systemd/system/frpp.service.d
sudo tee /etc/systemd/system/frpp.service.d/override.conf > /dev/null <<'EOF'
[Service]
RestartSec=5
EOF
sudo systemctl daemon-reload
sudo systemctl restart frpp`}</Code>
                    <p>
                      drop-in 是独立文件，不会被 <code>frp-panel install</code> 影响（<code>install</code> 遇到已存在的
                      unit 会直接报 <code>Init already exists</code> 而不覆盖），升级二进制后依然有效。
                    </p>
                    <p>WebUI 客户端列表每 10 秒刷新一次，服务重启后无需手动刷新页面就能看到状态变化。</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Alternative install */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <ShieldAlert className="h-4 w-4" />
                  另一种装法：直接带参数安装（快，但密钥会暴露）
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm text-muted-foreground">
                <Code>{`sudo frp-panel install client -s <密钥> -i <第2步的客户端ID> \\
  --api-url ${apiUrl} \\
  --rpc-url ${rpcUrl}
sudo frp-panel start`}</Code>
                <Alert className="border-amber-300 bg-amber-50">
                  <AlertTitle>⚠ 两个副作用</AlertTitle>
                  <AlertDescription>
                    <ol className="mt-1 list-decimal space-y-1 pl-5">
                      <li>
                        密钥会以明文写进 <code>/etc/systemd/system/frpp.service</code> 的 <code>ExecStart</code>
                        ，而该文件权限是 <code>644</code>（机器上任何用户都能读到）。
                      </li>
                      <li>
                        命令行参数里没法传 <code>CLIENT_TLS_INSECURE_SKIP_VERIFY</code>，自签证书场景仍然要额外建{' '}
                        <code>/etc/frpp/.env</code> 写这一行。
                      </li>
                    </ol>
                    多用户机器建议用上面第 3 步的 <code>.env</code> 方式。
                  </AlertDescription>
                </Alert>
              </CardContent>
            </Card>

            {/* Linux FAQ */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Info className="h-4 w-4" />
                  Linux 常见问题
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm text-muted-foreground">
                <div>
                  <p className="font-medium text-foreground">
                    安装报 <code>permission denied</code>
                  </p>
                  <p>
                    写 <code>/etc/systemd/system/</code> 需要 root。命令前加 <code>sudo</code>，或切到 root 再执行。
                    如果 <code>sudo</code> 需要密码而你在非交互环境（脚本、CI、没有 tty 的会话）里执行，
                    它会一直等输入、看起来像卡死 —— 先确认 <code>sudo -n true</code> 能过，或改用 root 会话。
                  </p>
                </div>
                <div>
                  <p className="font-medium text-foreground">
                    日志报 <code>certificate signed by unknown authority</code>
                  </p>
                  <p>
                    自签证书没放行。检查 <code>/etc/frpp/.env</code> 里有{' '}
                    <code>CLIENT_TLS_INSECURE_SKIP_VERIFY=true</code>，然后 <code>sudo frp-panel restart</code>
                    。日志里那段配置打印会显示 <code>TLSInsecureSkipVerify: true</code> 才算生效。
                  </p>
                </div>
                <div>
                  <p className="font-medium text-foreground">
                    <code>start</code> 之后仍然 <code>inactive</code>，日志却什么都没有
                  </p>
                  <p>
                    看 <code>systemctl status frpp</code>，如果出现{' '}
                    <code>Condition: start condition failed ... ConditionFileIsExecutable</code>，说明 unit
                    里记录的二进制路径已经不存在或没有执行权限（常见于把二进制挪了位置）。注意直接重新{' '}
                    <code>install</code> 会报 <code>Init already exists</code>（unit 已存在时不覆盖），必须先卸载再装：
                  </p>
                  <Code>{`sudo frp-panel uninstall
pgrep -a frp-panel        # 确认没有残留进程
sudo frp-panel install client
sudo frp-panel start`}</Code>
                  <p>
                    在新二进制所在目录执行（或让它在 <code>PATH</code> 里），<code>install</code>{' '}
                    会按当前可执行文件的真实路径重建 unit。
                  </p>
                </div>
                <div>
                  <p className="font-medium text-foreground">改了 .env 但不生效</p>
                  <p>
                    配置只在启动时读一次，改完必须 <code>sudo frp-panel restart</code>。另外优先级是：系统环境变量（
                    <code>/etc/sysconfig/frpp</code>）&gt; 二进制同目录的 <code>.env</code> &gt;{' '}
                    <code>/etc/frpp/.env</code>；同名变量以优先级高的为准。注意 <code>install</code> 会把 unit 的{' '}
                    <code>WorkingDirectory</code> 设成二进制所在目录，如果那里有历史遗留的 <code>.env</code>，先删掉。
                  </p>
                </div>
                <div>
                  <p className="font-medium text-foreground">
                    日志出现 <code>rpc auth token is invalid</code>
                  </p>
                  <p>
                    握手通了但密钥不被认可。按顺序查：<code>CLIENT_ID</code> 是否与 WebUI
                    里的名字完全一致（含用户名前缀）、 密钥是否复制完整、这台机器上是否有另一个进程用同一个 ID
                    在抢。该错误每 3 秒重试一次，会刷很多行，属同一次失败。
                  </p>
                </div>
                <div>
                  <p className="font-medium text-foreground">这台机器没有 systemd（老容器 / 精简系统）</p>
                  <p>用前台方式跑，配合自己的守护手段（supervisor、Docker restart policy 等）：</p>
                  <Code>{`nohup env CLIENT_TLS_INSECURE_SKIP_VERIFY=true \\
  /usr/local/bin/frp-panel client -s <密钥> -i <客户端ID> \\
  --api-url ${apiUrl} \\
  --rpc-url ${rpcUrl} \\
  > /var/log/frpp-client.log 2>&1 &`}</Code>
                  <p>
                    注意：这种写法密钥会出现在进程命令行（同机任何用户 <code>ps</code> 可见）和 shell
                    历史里，只建议单用户/可信机器用。多用户机器优先用上面的 systemd + <code>/etc/frpp/.env</code>
                    （600）方式。
                  </p>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* ==================== Windows ==================== */}
          <TabsContent value="windows" className="mt-4 space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Download className="h-5 w-5" />
                  添加内网机器为客户端（Windows 版）
                </CardTitle>
                <CardDescription>
                  把家里的电脑（内网，无公网 IP）接入 frp-panel，将内网服务暴露到公网。全程约 5 分钟。
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Step 1 */}
                <div className="space-y-2">
                  <StepTitle n={1}>下载客户端二进制</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <p>
                      到本平台的 Release 页面下载 Windows 版客户端（仓库为私有，需要仓库权限；没有权限请找管理员取）。
                    </p>
                    <Code>{`本仓库 Releases 页面 → 下载 frp-panel-<版本>-windows-amd64.exe`}</Code>
                    <p>
                      下载 <code>frp-panel-&lt;版本&gt;-windows-amd64.exe</code>，放到一个固定目录。 下面示例用{' '}
                      <code>C:\frp-panel\</code>，你可以换成任何有写权限的目录，记得把后续命令里的路径一起改。
                    </p>
                    <p>
                      重命名并确认版本（<code>BinVersion</code> 不应是 <code>dev-build</code>）：
                    </p>
                    <Code>{`mkdir C:\frp-panel
move frp-panel-<版本>-windows-amd64.exe C:\frp-panel\frp-panel.exe
C:\frp-panel\frp-panel.exe version`}</Code>
                  </div>
                </div>

                {/* Step 2 */}
                <div className="space-y-2">
                  <StepTitle n={2}>创建客户端 ID</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <ol className="list-decimal space-y-1 pl-5">
                      <li>
                        浏览器打开 <code className="mx-1">{consoleUrl}</code>
                        （自签证书警告 → 继续访问）
                      </li>
                      <li>用管理员账号登录</li>
                      <li>
                        左侧菜单点击 <strong>客户端</strong>
                      </li>
                      <li>
                        页面顶部，点击 <strong>新建客户端</strong> 按钮
                      </li>
                      <li>
                        弹窗中输入客户端唯一 ID，只能用小写字母、数字、<code>-</code>、<code>_</code>，例如{' '}
                        <code>home-pc</code>
                      </li>
                      <li>
                        点击 <strong>提交</strong> 按钮
                      </li>
                    </ol>
                    <p>
                      提交成功后列表里会出现一行。显示的名字会带用户名前缀（如 <code>&lt;用户名&gt;.c.home-pc</code>
                      ），这是多租户命名空间，属正常现象。
                    </p>
                  </div>
                </div>

                {/* Step 3 */}
                <div className="space-y-2">
                  <StepTitle n={3}>复制并修改启动命令</StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <ol className="list-decimal space-y-1 pl-5">
                      <li>
                        在该行右侧，点击 <strong>⋮</strong>（三个点）操作菜单按钮
                      </li>
                      <li>
                        菜单中点击 <strong>复制启动命令</strong>
                      </li>
                      <li>粘贴到记事本，命令格式类似：</li>
                    </ol>
                    <p className="font-medium text-foreground">复制出来的原始命令（地址部分不能直接用）：</p>
                    <Code>{`frp-panel.exe client -s <密钥> -i <客户端ID> --api-url http://127.0.0.1:<端口> --rpc-url grpc://127.0.0.1:<端口>`}</Code>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ 必须修改地址</AlertTitle>
                      <AlertDescription>
                        命令里的 <code>--api-url</code> 和 <code>--rpc-url</code> 是
                        127.0.0.1（内网机器无法连接），必须改为本页顶部给出的地址：
                        <code className="mx-1">{apiUrl}</code> 和 <code className="mx-1">{rpcUrl}</code>。
                      </AlertDescription>
                    </Alert>
                    <p className="font-medium text-foreground">改好的命令（用这个）：</p>
                    <Code tone="good">{`frp-panel.exe client -s <密钥> -i <客户端ID> --api-url ${apiUrl} --rpc-url ${rpcUrl}`}</Code>
                  </div>
                </div>

                {/* Step 4 */}
                <div className="space-y-2">
                  <StepTitle n={4}>
                    <span className="flex items-center gap-1">
                      <Terminal className="h-4 w-4" /> 在目标电脑上运行
                    </span>
                  </StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <p>
                      打开 <strong>PowerShell</strong>，依次执行：
                    </p>
                    <Code>{`cd C:\\frp-panel
$env:CLIENT_TLS_INSECURE_SKIP_VERIFY="true"
.\\frp-panel.exe client -s <密钥> -i <客户端ID> --api-url ${apiUrl} --rpc-url ${rpcUrl}`}</Code>
                    <p>
                      保持 PowerShell 窗口打开（不要关闭）。用正式证书时可去掉 <code>TLS_INSECURE_SKIP_VERIFY</code>{' '}
                      那行。
                    </p>
                  </div>
                </div>

                {/* Step 5 */}
                <div className="space-y-2">
                  <StepTitle n={5}>
                    <span className="flex items-center gap-1">
                      <CheckCircle className="h-4 w-4 text-green-600" /> 验证已上线
                    </span>
                  </StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <ul className="list-disc space-y-1 pl-5">
                      <li>
                        在 WebUI 刷新 <strong>客户端</strong> 页面，该客户端应显示
                        <strong>在线</strong>（绿色圆点）
                      </li>
                      <li>总览页「客户端总数」应 +1</li>
                    </ul>
                  </div>
                </div>

                {/* Step 6 */}
                <div className="space-y-2">
                  <StepTitle n={6}>
                    <span className="flex items-center gap-1">
                      <Monitor className="h-4 w-4" /> 配置开机自启（可选）
                    </span>
                  </StepTitle>
                  <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                    <p>
                      创建 <code>C:\frp-panel\start-client.ps1</code>（路径、ID、地址都按你的实际值填）：
                    </p>
                    <Code>{`$env:CLIENT_TLS_INSECURE_SKIP_VERIFY="true"
$LogFile = "C:\\frp-panel\\client.log"
$Bin = "C:\\frp-panel\\frp-panel.exe"
$Args = @(
    "client"
    "-s", "<密钥>"
    "-i", "<客户端ID>"
    "--api-url", "${apiUrl}"
    "--rpc-url", "${rpcUrl}"
)
Start-Process -NoNewWindow -FilePath $Bin -ArgumentList $Args -RedirectStandardOutput $LogFile -RedirectStandardError $LogFile`}</Code>
                    <Alert className="border-amber-300 bg-amber-50">
                      <AlertTitle>⚠ 密钥留在脚本里，别写进计划任务参数</AlertTitle>
                      <AlertDescription>
                        下面的 <code>schtasks</code> 只引用脚本文件。<code>start-client.ps1</code>{' '}
                        里是明文密钥，建议收紧权限：
                      </AlertDescription>
                    </Alert>
                    <Code>{`icacls C:\\frp-panel\\start-client.ps1 /inheritance:r /grant:r "SYSTEM:(R)" /grant:r "Administrators:(R)"`}</Code>
                    <p>
                      然后用管理员身份运行（脚本必须对 <code>/ru</code> 指定的账号可读）：
                    </p>
                    <Code>{`schtasks /create /tn "frp-panel-client" /tr "powershell -ExecutionPolicy Bypass -File C:\\frp-panel\\start-client.ps1" /sc onstart /delay 0001:01 /ru SYSTEM`}</Code>
                    <p className="text-xs">
                      <code>/sc onstart</code> 需要管理员权限创建；<code>/ru SYSTEM</code>{' '}
                      表示开机即以系统账户运行，适合无人值守机器。 若希望登录后才启动，改用 <code>/sc onlogon</code>{' '}
                      并去掉 <code>/ru SYSTEM</code>。
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        {/* Notes */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Info className="h-4 w-4" />
              速查（两种系统通用）
            </CardTitle>
            <CardDescription>以下每条在上面对应步骤里都有完整说明，这里只做提醒。</CardDescription>
          </CardHeader>
          <CardContent>
            <ul className="list-disc space-y-1.5 pl-5 text-sm text-muted-foreground">
              <li>
                自签证书：<code>CLIENT_TLS_INSECURE_SKIP_VERIFY=true</code>（换正式证书后改回 <code>false</code>）
              </li>
              <li>
                地址：<code>--api-url</code> / <code>--rpc-url</code> 一律填本页顶部给出的地址，不填{' '}
                <code>127.0.0.1</code>，也不要自己拼域名或漏端口
              </li>
              <li>内网机器不需要公网 IP，也不需要路由器端口映射——frpc 主动连出去</li>
              <li>防火墙只需放行 frp-panel 出站，无需入站规则（Linux 出站默认放行）</li>
              <li>创建隧道：WebUI 进入客户端详情 → 代理列表 → 新建代理，填本地端口和公网端口</li>
              <li>密钥等同口令：不要贴进聊天工具或提交到仓库</li>
              <li>
                <code>version</code> 里 <code>BinVersion</code> 为 <code>dev-build</code> 说明不是 Release
                包，排障前先换包
              </li>
            </ul>
          </CardContent>
        </Card>
      </div>
    </RootLayout>
  )
}

export default function GuidePage() {
  return (
    <Providers>
      <GuideBody />
    </Providers>
  )
}
