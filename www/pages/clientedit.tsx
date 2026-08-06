import Link from 'next/link'

export default function ClientEditPage() {
  return (
    <main className="min-h-screen flex items-center justify-center p-8">
      <section className="max-w-lg space-y-4 rounded border p-6">
        <h1 className="text-xl font-semibold">Raw FRPC editing is disabled</h1>
        <p className="text-muted-foreground">
          Use the managed proxy workflow. It performs both-side preflight,
          transactional persistence, acknowledgement and rollback checks.
        </p>
        <Link className="underline" href="/ports">Open Port Center</Link>
      </section>
    </main>
  )
}
