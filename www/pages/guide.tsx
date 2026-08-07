'use client'

import { Providers } from '@/components/providers'
import { RootLayout } from '@/components/layout'
import { Header } from '@/components/header'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useTranslation } from 'react-i18next'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Terminal, Download, Monitor, CheckCircle, Info } from 'lucide-react'

export default function GuidePage() {
  const { t } = useTranslation()

  return (
    <Providers>
      <RootLayout mainHeader={<Header title={t('nav.guide')} />}>
        <div className="mx-auto max-w-4xl space-y-6 p-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Download className="h-5 w-5" />
                添加内网机器为客户端（Windows 版）
              </CardTitle>
              <CardDescription>
                把家里的电脑（内网，无公网 IP）接入 frp-panel，将内网服务暴露到公网。
                全程约 5 分钟。
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <Alert>
                <Info className="h-4 w-4" />
                <AlertTitle>前置条件</AlertTitle>
                <AlertDescription>
                  Master 管理端已部署并运行在 <code>47.109.18.109:9443</code>，
                  且已注册管理员账号。如果还没有，请先完成部署和注册。
                </AlertDescription>
              </Alert>

              {/* Step 1 */}
              <div className="space-y-2">
                <h3 className="flex items-center gap-2 text-lg font-semibold">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">1</span>
                  下载客户端二进制
                </h3>
                <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                  <p>打开 Release 页面下载 Windows 版客户端：</p>
                  <div className="rounded bg-muted p-3 font-mono text-xs">
                    https://github.com/NeitherCandiDa/frp-panel-manager/releases
                  </div>
                  <p>下载 <code>frp-panel-v0.2.0-windows-amd64.exe</code>，将其重命名为 <code>frp-panel.exe</code>，放在 <code>D:\frp-panel\</code> 目录。</p>
                </div>
              </div>

              {/* Step 2 */}
              <div className="space-y-2">
                <h3 className="flex items-center gap-2 text-lg font-semibold">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">2</span>
                  创建客户端 ID
                </h3>
                <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                  <ol className="list-decimal space-y-1 pl-5">
                    <li>浏览器打开 <code className="mx-1">https://47.109.18.109:9443/</code>（自签证书警告 → 继续访问）</li>
                    <li>用管理员账号登录</li>
                    <li>左侧菜单点击 <strong>客户端</strong></li>
                    <li>页面顶部，点击 <strong>新建客户端</strong> 按钮</li>
                    <li>弹窗中输入客户端唯一 ID，例如 <code>client.home-pc</code></li>
                    <li>点击 <strong>提交</strong> 按钮</li>
                  </ol>
                  <p>提示「创建成功」后，列表中会出现 <code>client.home-pc</code> 行。</p>
                </div>
              </div>

              {/* Step 3 */}
              <div className="space-y-2">
                <h3 className="flex items-center gap-2 text-lg font-semibold">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">3</span>
                  复制并修改启动命令
                </h3>
                <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                  <ol className="list-decimal space-y-1 pl-5">
                    <li>在 <code>client.home-pc</code> 行右侧，点击 <strong>⋮</strong>（三个点）操作菜单按钮</li>
                    <li>菜单中点击 <strong>复制启动命令</strong></li>
                    <li>粘贴到记事本，命令格式类似：</li>
                  </ol>
                  <div className="rounded bg-muted p-3 font-mono text-xs">
                    frp-panel.exe client -s 84cc2fd7-xxxx -i client.home-pc --api-url http://127.0.0.1:9100 --rpc-url grpc://127.0.0.1:9101
                  </div>
                  <Alert className="border-amber-300 bg-amber-50">
                    <AlertTitle>⚠ 必须修改地址</AlertTitle>
                    <AlertDescription>
                      命令中的 <code>--api-url</code> 和 <code>--rpc-url</code> 的地址是 127.0.0.1（内网机器无法连接），
                      必须改为公网地址：
                    </AlertDescription>
                  </Alert>
                  <div className="rounded bg-green-50 p-3 font-mono text-xs text-green-800">
                    frp-panel.exe client -s 84cc2fd7-xxxx -i client.home-pc --api-url https://47.109.18.109:9443 --rpc-url grpc://47.109.18.109:9101
                  </div>
                </div>
              </div>

              {/* Step 4 */}
              <div className="space-y-2">
                <h3 className="flex items-center gap-2 text-lg font-semibold">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">4</span>
                  <span className="flex items-center gap-1"><Terminal className="h-4 w-4" /> 在家电脑上运行</span>
                </h3>
                <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                  <p>打开 <strong>PowerShell</strong>，依次执行：</p>
                  <div className="rounded bg-muted p-3 font-mono text-xs">
                    <div>cd D:\frp-panel</div>
                    <div className="mt-1">$env:CLIENT_TLS_INSECURE_SKIP_VERIFY=&quot;true&quot;</div>
                    <div className="mt-1">.\frp-panel.exe client -s 84cc2fd7-xxxx -i client.home-pc --api-url https://47.109.18.109:9443 --rpc-url grpc://47.109.18.109:9101</div>
                  </div>
                  <p>保持 PowerShell 窗口打开（不要关闭）。</p>
                </div>
              </div>

              {/* Step 5 */}
              <div className="space-y-2">
                <h3 className="flex items-center gap-2 text-lg font-semibold">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">5</span>
                  <span className="flex items-center gap-1"><CheckCircle className="h-4 w-4 text-green-600" /> 验证已上线</span>
                </h3>
                <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                  <ul className="list-disc space-y-1 pl-5">
                    <li>在 WebUI 刷新 <strong>客户端</strong> 页面，<code>client.home-pc</code> 应显示<strong>在线</strong>（绿色圆点）</li>
                    <li>总览页「客户端总数」应 +1</li>
                  </ul>
                </div>
              </div>

              {/* Step 6 */}
              <div className="space-y-2">
                <h3 className="flex items-center gap-2 text-lg font-semibold">
                  <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">6</span>
                  <span className="flex items-center gap-1"><Monitor className="h-4 w-4" /> 配置开机自启（可选）</span>
                </h3>
                <div className="ml-9 space-y-2 text-sm text-muted-foreground">
                  <p>创建 <code>D:\frp-panel\start-client.ps1</code>：</p>
                  <div className="rounded bg-muted p-3 font-mono text-xs">
                    <div>$env:CLIENT_TLS_INSECURE_SKIP_VERIFY=&quot;true&quot;</div>
                    <div>$LogFile = &quot;D:\frp-panel\client.log&quot;</div>
                    <div>$Bin = &quot;D:\frp-panel\frp-panel.exe&quot;</div>
                    <div>$Args = @(</div>
                    <div>&nbsp;&nbsp;&nbsp;&nbsp;&quot;client&quot;</div>
                    <div>&nbsp;&nbsp;&nbsp;&nbsp;&quot;-s&quot;, &quot;84cc2fd7-xxxx&quot;</div>
                    <div>&nbsp;&nbsp;&nbsp;&nbsp;&quot;-i&quot;, &quot;client.home-pc&quot;</div>
                    <div>&nbsp;&nbsp;&nbsp;&nbsp;&quot;--api-url&quot;, &quot;https://47.109.18.109:9443&quot;</div>
                    <div>&nbsp;&nbsp;&nbsp;&nbsp;&quot;--rpc-url&quot;, &quot;grpc://47.109.18.109:9101&quot;</div>
                    <div>)</div>
                    <div>Start-Process -NoNewWindow -FilePath $Bin -ArgumentList $Args -RedirectStandardOutput $LogFile -RedirectStandardError $LogFile</div>
                  </div>
                  <p>然后用管理员身份运行：</p>
                  <div className="rounded bg-muted p-3 font-mono text-xs">
                    schtasks /create /tn &quot;frp-panel-client&quot; /tr &quot;powershell -ExecutionPolicy Bypass -File D:\frp-panel\start-client.ps1&quot; /sc onstart /delay 0001:00 /ru SYSTEM
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Notes */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base">
                <Info className="h-4 w-4" />
                注意事项
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="list-disc space-y-1.5 pl-5 text-sm text-muted-foreground">
                <li>
                  <code>CLIENT_TLS_INSECURE_SKIP_VERIFY=true</code> 必须设置——自签证书部署的硬性要求；
                  换用正式证书后可改为 <code>false</code>
                </li>
                <li>
                  <code>--api-url</code> 和 <code>--rpc-url</code> 必须填公网地址（<code>47.109.18.109</code>），不能填 <code>127.0.0.1</code>
                </li>
                <li>内网机器不需要公网 IP，也不需要路由器端口映射——frpc 主动连出去</li>
                <li>如果家电脑装了防火墙，放行 <code>frp-panel.exe</code> 出站即可（不需要入站规则）</li>
                <li>
                  创建隧道（把家电脑的服务暴露到公网）：在 WebUI 进入客户端详情 → 代理列表 → 新建代理，
                  填本地端口和公网端口
                </li>
                <li>如果家电脑是 Linux，下载 <code>frp-panel-v0.2.0-linux-amd64</code>，启动命令改为 <code>CLIENT_TLS_INSECURE_SKIP_VERIFY=true ./frp-panel ...</code>，开机自启用 systemd 或 crontab</li>
              </ul>
            </CardContent>
          </Card>
        </div>
      </RootLayout>
    </Providers>
  )
}