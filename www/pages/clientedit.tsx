import Link from 'next/link'

export default function ClientEditPage() {
  return (
    <main className="min-h-screen flex items-center justify-center p-8">
      <section className="max-w-2xl space-y-4 rounded border p-6">
        <h1 className="text-xl font-semibold">客户端配置由隧道管理自动生成</h1>
        <p className="text-muted-foreground">
          frp-panel-manager 通过「端口中心」管理客户端隧道：新建隧道时会自动生成完整的
          frpc 配置（服务端地址、认证、代理条目），不需要手动编辑 JSON。
        </p>
        <ul className="list-disc space-y-1 pl-5 text-sm">
          <li>新建隧道：端口中心 → 新建代理 → 填写本地端口与公网端口</li>
          <li>修改/启停隧道：端口中心或客户端详情 → 代理列表</li>
          <li>所有操作均经过双端预检、事务提交与回滚保护</li>
        </ul>
        <div className="space-x-4">
          <Link className="underline" href="/ports">打开端口中心</Link>
          <Link className="underline" href="/clients">返回客户端列表</Link>
        </div>
      </section>
    </main>
  )
}