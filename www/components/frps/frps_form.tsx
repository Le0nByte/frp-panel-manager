import { ServerConfig } from '@/types/server'
import { PortsRange } from '@/types/common'
import { useEffect, useState } from 'react'
import { zodResolver } from '@hookform/resolvers/zod'
import { useForm } from 'react-hook-form'
import * as z from 'zod'
import { Button } from '@/components/ui/button'
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { ZodIPSchema, ZodPortSchema, ZodStringSchema } from '@/lib/consts'
import { RespCode, Server } from '@/lib/pb/common'
import { updateFRPS } from '@/api/frp'
import { useMutation } from '@tanstack/react-query'
import { Label } from '@radix-ui/react-label'
import { useTranslation } from 'react-i18next'
import { toast } from 'sonner'
import { HostField, PortField } from '../base/form-field'
import { Trash2, Plus, Hash, Brackets } from 'lucide-react'

const ServerConfigSchema = z.object({
  bindAddr: ZodIPSchema.default('0.0.0.0').optional(),
  bindPort: ZodPortSchema.default(7000),
  proxyBindAddr: ZodIPSchema.optional(),
  vhostHTTPPort: ZodPortSchema.optional(),
  subDomainHost: ZodStringSchema.optional(),
  publicHost: ZodStringSchema.optional(),
  quicBindPort: ZodPortSchema.optional(),
  kcpBindPort: ZodPortSchema.optional(),
  maxPortsPerClient: z.coerce
    .number()
    .int('整数')
    .min(1, '1-65535')
    .max(65535, '1-65535')
    .optional(),
})

export const ServerConfigZodSchema = ServerConfigSchema

// A port rule editor entry: either a single port or an inclusive range.
// "5700"       -> { single: 5700 }
// "10000-20000"-> { start: 10000, end: 20000 }
type PortRuleDraft = {
  id: string
  text: string
}

const isValidSingle = (v: number) => Number.isInteger(v) && v >= 1 && v <= 65535

const parsePortRule = (text: string): PortsRange | null => {
  const t = text.trim()
  if (t === '') return null
  if (t.includes('-')) {
    const parts = t.split('-')
    if (parts.length !== 2) return null
    const start = Number(parts[0].trim())
    const end = Number(parts[1].trim())
    if (!isValidSingle(start) || !isValidSingle(end) || start > end) return null
    return { start, end }
  }
  const single = Number(t)
  if (!isValidSingle(single)) return null
  return { single }
}

// Serialize a PortsRange back to the editor text form.
const rangeToText = (r: PortsRange): string => {
  if (r.single != null) return String(r.single)
  if (r.start != null && r.end != null) return `${r.start}-${r.end}`
  return ''
}

const newRuleDraft = (): PortRuleDraft => ({ id: Math.random().toString(36).slice(2), text: '' })

export interface FRPSFormProps {
  serverID: string
  server: Server
  frpsUrls: string[]
}

const FRPSForm: React.FC<FRPSFormProps> = ({ serverID, server, frpsUrls }) => {
  const { t } = useTranslation()
  const form = useForm<z.infer<typeof ServerConfigZodSchema>>({
    resolver: zodResolver(ServerConfigZodSchema),
  })

  const updateFrps = useMutation({ mutationFn: updateFRPS })

  // Port rule editor state — initialized from the server config and kept in
  // sync when the server row changes.
  const [portRules, setPortRules] = useState<PortRuleDraft[]>([])

  useEffect(() => {
    form.reset({})
  }, [])

  useEffect(() => {
    const cfg = JSON.parse(server?.config || '{}') as ServerConfig
    form.reset(cfg as z.infer<typeof ServerConfigZodSchema>)
    setPortRules(
      (cfg.allowPorts ?? []).map((r) => ({
        id: Math.random().toString(36).slice(2),
        text: rangeToText(r),
      })),
    )
  }, [server])

  const addSingleRule = () => setPortRules((prev) => [...prev, newRuleDraft()])
  const addRangeRule = () => setPortRules((prev) => [...prev, newRuleDraft()])
  const updateRule = (id: string, text: string) =>
    setPortRules((prev) => prev.map((r) => (r.id === id ? { ...r, text } : r)))
  const removeRule = (id: string) => setPortRules((prev) => prev.filter((r) => r.id !== id))

  const onSubmit = async (values: z.infer<typeof ServerConfigZodSchema>) => {
    // Parse + validate port rules; invalid rules block the save with a toast.
    const allowPorts: PortsRange[] = []
    for (const rule of portRules) {
      const parsed = parsePortRule(rule.text)
      if (rule.text.trim() !== '' && parsed == null) {
        toast(t('server.form.port_rule_invalid'), {
          description: rule.text,
        })
        return
      }
      if (parsed != null) allowPorts.push(parsed)
    }
    try {
      const { publicHost, ...rest } = values
      let resp = await updateFrps.mutateAsync({
        serverIp: publicHost,
        serverId: serverID,
        frpsUrls: frpsUrls,
        // @ts-ignore
        config: Buffer.from(
          JSON.stringify({
            ...rest,
            allowPorts,
          } as ServerConfig),
        ),
      })
      toast(resp.status?.code === RespCode.SUCCESS ? t('server.operation.update_success') : t('server.operation.update_failed'), {
        description: resp.status?.message,
      })
    } catch (error) {
      console.error(error)
      toast(t('server.operation.update_title'), {
        description: t('server.operation.update_failed')
      })
    }
  }

  return (
    <div className="flex flex-col w-full pt-2">
      <Label className="text-sm font-medium">{t('server.form.comment_title', { id: serverID })}</Label>
      <p className="text-sm text-muted-foreground">{t('server.form.comment_hint')}</p>
      <p className="text-sm border rounded p-2 my-2">
        {server?.comment == undefined || server?.comment === '' ? t('server.form.comment_empty') : server?.comment}
      </p>
      {serverID && (
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4 px-0.5">
            <HostField name="publicHost" label={t('server.form.public_host')} placeholder='8.8.8.8' control={form.control} defaultValue={server?.ip}/>
            <PortField name="bindPort" label={t('server.form.bind_port')} control={form.control} />
            <HostField name="bindAddr" label={t('server.form.bind_addr')} control={form.control} />
            <HostField name="proxyBindAddr" label={t('server.form.proxy_bind_addr')} control={form.control} />
            <PortField name="vhostHTTPPort" label={t('server.form.vhost_http_port')} control={form.control} />
            <HostField name="subDomainHost" label={t('server.form.subdomain_host')} control={form.control} />
            <PortField name="quicBindPort" label={t('server.form.quic_bind_port')} control={form.control} />
            <PortField name="kcpBindPort" label={t('server.form.kcp_bind_port')} control={form.control} />

            {/* 2026-08-07: port quota — maxPortsPerClient + allowPorts */}
            <div className="space-y-3 rounded-md border p-3">
              <p className="text-sm font-medium">{t('server.form.port_quota_title')}</p>
              <FormField
                control={form.control}
                name="maxPortsPerClient"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>{t('server.form.max_ports_per_client')}</FormLabel>
                    <FormControl>
                      <Input
                        type="number"
                        min={1}
                        max={65535}
                        placeholder="64"
                        value={field.value ?? ''}
                        onChange={(e) => field.onChange(e.target.value === '' ? undefined : Number(e.target.value))}
                      />
                    </FormControl>
                    <p className="text-xs text-muted-foreground">{t('server.form.max_ports_per_client_hint')}</p>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <FormLabel>{t('server.form.allow_ports')}</FormLabel>
                  <div className="flex gap-1.5">
                    <Button type="button" size="sm" variant="outline" onClick={addSingleRule}>
                      <Hash className="mr-1 h-3.5 w-3.5" />
                      {t('server.form.add_single_port')}
                    </Button>
                    <Button type="button" size="sm" variant="outline" onClick={addRangeRule}>
                      <Brackets className="mr-1 h-3.5 w-3.5" />
                      {t('server.form.add_port_range')}
                    </Button>
                  </div>
                </div>
                <p className="text-xs text-muted-foreground">{t('server.form.allow_ports_hint')}</p>
                {portRules.length === 0 && (
                  <p className="text-xs text-muted-foreground">{t('server.form.allow_ports_empty')}</p>
                )}
                {portRules.map((rule) => (
                  <div key={rule.id} className="flex items-center gap-2">
                    <Input
                      value={rule.text}
                      placeholder="5700 或 10000-20000"
                      onChange={(e) => updateRule(rule.id, e.target.value)}
                      className="font-mono"
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8 shrink-0"
                      onClick={() => removeRule(rule.id)}
                    >
                      <Trash2 className="h-4 w-4 text-destructive" />
                    </Button>
                  </div>
                ))}
              </div>
            </div>

            <Button type="submit">{t('common.submit')}</Button>
          </form>
        </Form>
      )}
    </div>
  )
}

export default FRPSForm
