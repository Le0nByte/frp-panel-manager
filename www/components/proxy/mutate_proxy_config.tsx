'use client'

import { useEffect, useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { useTranslation } from 'react-i18next'
import { ServerSelector } from '../base/server-selector'
import { ClientSelector } from '../base/client-selector'
import { TypedProxyForm } from '../frpc/proxy_form'
import { ProxyType, TypedProxyConfig } from '@/types/proxy'
import { BaseSelector } from '../base/selector'
import {
  SUPPORTED_PROXY_TYPES,
  canPreflightCandidate,
  extractPreflightCandidate,
} from '@/lib/preflight-gate'
import { createProxyConfig } from '@/api/proxy'
import { openFirewallPort } from '@/api/firewall'
import { preflight } from '@/api/port'
import { PreflightResponse } from '@/lib/pb/api_port'
import { PortObservation, PortState } from '@/lib/pb/types_port'
import { ClientConfig } from '@/types/client'
import { ObjToUint8Array } from '@/lib/utils'
import { VisitPreview } from '../base/visit-preview'
import { ProxyConfig, Server } from '@/lib/pb/common'
import { TypedProxyConfigValid } from '@/lib/consts'
import { toast } from 'sonner'
import { $proxyTableRefetchTrigger } from '@/store/refetch-trigger'
import { Switch } from '../ui/switch'
import { Textarea } from '../ui/textarea'
import { CheckCircle2, XCircle, Search, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'

export type ProxyConfigMutateDialogProps = {
  overwrite?: boolean
  defaultProxyConfig?: TypedProxyConfig
  defaultOriginalProxyConfig?: ProxyConfig
  disableChangeProxyName?: boolean
  onSuccess?: () => void
}

export const ProxyConfigMutateDialog = ({ ...props }: ProxyConfigMutateDialogProps) => {
  const { t } = useTranslation()
  // Controlled dialog: closes automatically after a successful submit
  // (2026-08-08 fix — previously the dialog stayed open forever).
  const [open, setOpen] = useState(false)

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" className="w-fit">
          {t('proxy.config.create')}
        </Button>
      </DialogTrigger>
      <DialogContent className="max-h-[90vh] overflow-auto">
        <DialogHeader>
          <DialogTitle>{t('proxy.config.create_proxy')}</DialogTitle>
          <DialogDescription>{t('proxy.config.create_proxy_description')}</DialogDescription>
        </DialogHeader>
        <ProxyConfigMutateForm
          {...props}
          onSuccess={() => {
            setOpen(false)
            props.onSuccess?.()
          }}
        />
      </DialogContent>
    </Dialog>
  )
}

// The candidate fields the dual-side preflight can reason about. Only single
// tcp/udp rules are gateable (see lib/preflight-gate.ts); http/https and
// multi-rule configs are rejected there so no mode can bypass the gate.

type PreflightState = {
  sig: string
  status: 'loading' | 'success' | 'error'
  data?: PreflightResponse
  error?: string
}

const stateLabel = (state: PortState): string => {
  switch (state) {
    case PortState.STATE_AVAILABLE:
      return 'port.state.available'
    case PortState.STATE_LISTENING:
      return 'port.state.listening'
    case PortState.STATE_ACTIVE:
      return 'port.state.active'
    case PortState.STATE_CONFLICT:
      return 'port.state.conflict'
    case PortState.STATE_UNKNOWN:
      return 'port.state.unknown'
    default:
      return 'port.state.unspecified'
  }
}

const CheckSidePanel = ({
  title,
  ok,
  checks,
  t,
}: {
  title: string
  ok: boolean
  checks: PortObservation[]
  t: (key: string) => string
}) => (
  <div
    className={cn(
      'flex-1 rounded-md border p-2',
      ok ? 'border-green-200 bg-green-50/50' : 'border-red-200 bg-red-50/50',
    )}
  >
    <div className="flex items-center gap-1.5 text-sm font-medium">
      {ok ? (
        <CheckCircle2 className="h-4 w-4 text-green-600" />
      ) : (
        <XCircle className="h-4 w-4 text-red-600" />
      )}
      {title}
      <Badge variant={ok ? 'default' : 'destructive'} className="ml-auto">
        {ok ? t('proxy.config.preflight_ok') : t('proxy.config.preflight_fail')}
      </Badge>
    </div>
    {checks.length === 0 && (
      <div className="mt-1 text-xs text-muted-foreground">{t('proxy.config.preflight_no_evidence')}</div>
    )}
    {checks.map((o, i) => (
      <div key={i} className="mt-1 font-mono text-xs break-all">
        {o.protocol}/{o.localPort} · {o.localAddress} · {t(stateLabel(o.state))}
        {o.processName ? ` · ${o.processName}` : ''}
        {o.frpProxyName ? ` · ${o.frpProxyName}` : ''}
      </div>
    ))}
  </div>
)

export const ProxyConfigMutateForm = ({
  overwrite,
  defaultProxyConfig,
  defaultOriginalProxyConfig,
  disableChangeProxyName,
  onSuccess,
}: ProxyConfigMutateDialogProps) => {
  const { t } = useTranslation()
  const [newClientID, setNewClientID] = useState<string | undefined>()
  const [newServerID, setNewServerID] = useState<string | undefined>()
  const [proxyConfigs, setProxyConfigs] = useState<TypedProxyConfig[]>([])
  const [proxyName, setProxyName] = useState<string | undefined>('')
  const [proxyType, setProxyType] = useState<ProxyType>('tcp')
  const [selectedServer, setSelectedServer] = useState<Server | undefined>()
  // First-version safe support: only tcp/udp can pass the dual-side preflight
  // gate. http/https have no single remotePort to check (C1: never substitute
  // localPort for the public remotePort) and stay out of the gated flow.
  const supportedProxyTypes: ProxyType[] = [...SUPPORTED_PROXY_TYPES]
  // advanced mode toggle
  const [advancedMode, setAdvancedMode] = useState<boolean>(false)
  const [rawConfig, setRawConfig] = useState<string>('{}')
  // dual-side preflight gate (spec §9.4): only a fresh, passing preflight of
  // the exact current candidate may unlock submit. The backend re-validates on
  // commit — this UI gate never substitutes for the server-side check.
  const [preflightState, setPreflightState] = useState<PreflightState | null>(null)
  // 2026-08-07: after a successful create, optionally open the server's
  // firewalld port so the tunnel is immediately reachable from the public
  // internet (default ON per user decision).
  const [openFirewall, setOpenFirewall] = useState<boolean>(true)

  const createProxyConfigMutation = useMutation({
    mutationKey: ['createProxyConfig', newClientID, newServerID],
    mutationFn: () =>
      createProxyConfig({
        clientId: newClientID!,
        serverId: newServerID!,
        config: ObjToUint8Array({
          proxies: proxyConfigs,
        } as ClientConfig),
        overwrite,
      }),
    onSuccess: () => {
      toast(t('proxy.config.create_success'))
      if (openFirewall && cand && newServerID && cand.remotePort) {
        openFirewallPort({
          server_id: newServerID,
          port: cand.remotePort,
          protocol: cand.type,
        })
          .then((resp) => {
            if (resp.code === 200) {
              toast(t('proxy.config.firewall_opened'), {
                description: `${cand.remotePort}/${cand.type}`,
              })
            } else {
              toast(t('proxy.config.firewall_failed'), {
                description: resp.msg,
              })
            }
          })
          .catch((e) => {
            toast(t('proxy.config.firewall_failed'), {
              description: String(e),
            })
          })
      }
      $proxyTableRefetchTrigger.set(Math.random())
      onSuccess?.()
    },
    onError: (e) => {
      toast(t('proxy.config.create_failed'), {
        description: JSON.stringify(e),
      })
      $proxyTableRefetchTrigger.set(Math.random())
    },
  })

  useEffect(() => {
    if (proxyName && proxyType) {
      setProxyConfigs([{ ...defaultProxyConfig, name: proxyName, type: proxyType }])
    }
  }, [proxyName, proxyType])

  useEffect(() => {
    if (proxyConfigs) {
      setRawConfig(JSON.stringify(proxyConfigs, null, 2))
    }
  }, [proxyConfigs, setRawConfig])

  useEffect(() => {
    if (defaultProxyConfig && defaultOriginalProxyConfig) {
      setProxyConfigs([defaultProxyConfig])
      setProxyType(defaultProxyConfig.type)
      setProxyName(defaultProxyConfig.name)
      setNewClientID(defaultOriginalProxyConfig.originClientId)
      setNewServerID(defaultOriginalProxyConfig.serverId)
      setRawConfig(JSON.stringify([defaultProxyConfig], null, 2))
    }
  }, [defaultProxyConfig, defaultOriginalProxyConfig])

  const cand = useMemo(() => extractPreflightCandidate(proxyConfigs), [proxyConfigs])

  // Any change to server/client/name/ports must invalidate a previous
  // preflight — evidence for an older candidate must never unlock submit.
  const candidateSig = useMemo(
    () =>
      JSON.stringify({
        clientId: newClientID,
        serverId: newServerID,
        name: cand?.name,
        type: cand?.type,
        remotePort: cand?.remotePort,
        localIp: cand?.localIP,
        localPort: cand?.localPort,
      }),
    [newClientID, newServerID, cand],
  )

  useEffect(() => {
    setPreflightState(null)
  }, [candidateSig])

  // §9.4 gate (UI half): single tcp/udp candidate, integer ports in 1..65535,
  // client & server selected. Same gate in both normal and advanced mode.
  const canPreflight = canPreflightCandidate(cand, !!newClientID, !!newServerID)

  const preflightMutation = useMutation({
    mutationKey: ['preflight', candidateSig],
    mutationFn: () => {
      if (!cand || !newClientID || !newServerID) {
        throw new Error('missing candidate')
      }
      return preflight({
        clientId: newClientID,
        serverId: newServerID,
        // tcp/udp only (http/https never reach here): the public remotePort is
        // always a real, user-entered value — never localPort in disguise.
        proxyType: cand.type,
        protocol: cand.type,
        remotePort: cand.remotePort,
        localIp: cand.localIP,
        localPort: cand.localPort,
        proxyName: overwrite === true ? cand.name : undefined,
      })
    },
    onSuccess: (data) => {
      setPreflightState({ sig: candidateSig, status: 'success', data })
    },
    onError: (e) => {
      setPreflightState({ sig: candidateSig, status: 'error', error: String(e) })
    },
  })

  // Both ends must be green on a preflight of the exact current candidate.
  const preflightGatePassed =
    preflightState?.status === 'success' &&
    preflightState.sig === candidateSig &&
    preflightState.data?.serverOk === true &&
    preflightState.data?.clientOk === true

  // Submit NEVER bypasses the gate: advanced mode used to enable submit with
  // only `proxyConfigs.length > 0`, which skipped the dual-side preflight
  // (§9.4). A fresh, passing preflight of the exact candidate is mandatory in
  // both modes; the backend re-validates on commit regardless.
  const submitDisabled = !preflightGatePassed

  return (
    <>
      <Label>{t('proxy.config.select_server')} </Label>
      <ServerSelector setServerID={setNewServerID} serverID={newServerID} setServer={setSelectedServer} />
      <Label>{t('proxy.config.select_client')} </Label>
      <ClientSelector setClientID={setNewClientID} clientID={newClientID} />
      <div className="flex items-center space-x-2 my-2">
        <Label>{t('proxy.config.advanced_mode')}</Label>
        <Switch onCheckedChange={setAdvancedMode} />
      </div>
      {!advancedMode && (
        <>
          <Label>{t('proxy.config.select_proxy_type')} </Label>
          <BaseSelector
            dataList={supportedProxyTypes.map((type) => ({ value: type, label: type }))}
            value={proxyType}
            setValue={(value) => {
              setProxyType(value as ProxyType)
            }}
          />
          {proxyConfigs &&
            selectedServer &&
            proxyConfigs.length > 0 &&
            proxyConfigs[0] &&
            TypedProxyConfigValid(proxyConfigs[0]) && (
              <div className="flex flex-row w-full overflow-auto">
                <div className="flex flex-col">
                  <VisitPreview server={selectedServer} typedProxyConfig={proxyConfigs[0]} />
                </div>
              </div>
            )}
          <Label>{t('proxy.config.proxy_name')} </Label>
          <Input
            className="text-sm"
            defaultValue={proxyName}
            onChange={(e) => setProxyName(e.target.value)}
            disabled={disableChangeProxyName}
          />
          {proxyName && newClientID && newServerID && (
            <TypedProxyForm
              serverID={newServerID}
              clientID={newClientID}
              proxyName={proxyName}
              defaultProxyConfig={proxyConfigs && proxyConfigs.length > 0 ? proxyConfigs[0] : undefined}
              clientProxyConfigs={proxyConfigs}
              setClientProxyConfigs={setProxyConfigs}
              enablePreview={false}
            />
          )}
        </>
      )}
      {advancedMode && (
        <>
          <Label>{t('proxy.config.raw_json')}</Label>
          <Textarea
            className="w-full h-64 font-mono text-sm p-2 border"
            value={rawConfig}
            onChange={(e) => setRawConfig(e.target.value)}
          />
          <Button
            onClick={() => {
              try {
                const parsed = JSON.parse(rawConfig) as TypedProxyConfig[]
                setProxyConfigs(parsed)
              } catch {
                toast(t('proxy.config.invalid_json'))
                return
              }
            }}
          >
            {t('proxy.config.draft')}
          </Button>
        </>
      )}

      {/* Dual-side preflight gate — visible in BOTH normal and advanced mode.
          Advanced multi-rule / http(s) configs fail extractPreflightCandidate,
          so canPreflight stays false and submit stays locked with the hint
          below (spec §9.4: submit only unlocks when both sides pass). */}
      <div className="flex flex-col gap-2 my-2 rounded-md border p-2">
        <div className="flex items-center justify-between gap-2">
          <Label className="text-sm">{t('proxy.config.preflight_title')}</Label>
          <Button
            variant="outline"
            className="h-8 gap-1.5 text-sm"
            disabled={!canPreflight || preflightMutation.isPending}
            onClick={() => preflightMutation.mutate()}
          >
            {preflightMutation.isPending ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Search className="h-3.5 w-3.5" />
            )}
            {preflightMutation.isPending
              ? t('proxy.config.preflight_pending')
              : t('proxy.config.preflight_check')}
          </Button>
        </div>

        {!canPreflight && (
          <p className="text-xs text-muted-foreground">
            {advancedMode && (!cand || proxyConfigs.length !== 1)
              ? t('proxy.config.preflight_advanced_unsupported')
              : t('proxy.config.preflight_hint')}
          </p>
        )}

        {preflightState?.status === 'error' && (
          <p className="text-xs text-destructive break-all">
            {t('proxy.config.preflight_failed')}: {preflightState.error}
          </p>
        )}

        {preflightState?.status === 'success' && preflightState.sig === candidateSig && (
          <div className="flex flex-col gap-2">
            <div className="flex flex-col sm:flex-row gap-2">
              <CheckSidePanel
                title={t('proxy.config.preflight_server')}
                ok={preflightState.data?.serverOk === true}
                checks={preflightState.data?.serverChecks ?? []}
                t={t}
              />
              <CheckSidePanel
                title={t('proxy.config.preflight_client')}
                ok={preflightState.data?.clientOk === true}
                checks={preflightState.data?.clientChecks ?? []}
                t={t}
              />
            </div>
            {(!preflightState.data?.serverOk || !preflightState.data?.clientOk) &&
              (preflightState.data?.errorCode || preflightState.data?.errorMessage) && (
                <p className="text-xs text-destructive break-all">
                  {preflightState.data.errorCode}
                  {preflightState.data.errorMessage
                    ? `: ${preflightState.data.errorMessage}`
                    : ''}
                </p>
              )}
            {/* 2026-08: the client gate is WARN-graded — a local port that
                is not listening yet passes the gate but carries
                PORT_LOCAL_NOT_LISTENING as an advisory warning. */}
            {preflightState.data?.serverOk === true &&
              preflightState.data?.clientOk === true &&
              preflightState.data?.errorCode === 'PORT_LOCAL_NOT_LISTENING' && (
                <p className="text-xs text-amber-600 break-all">
                  {t('proxy.config.preflight_client_warning')}
                  {preflightState.data.errorMessage
                    ? `: ${preflightState.data.errorMessage}`
                    : ''}
                </p>
              )}
            {preflightState.data?.serverOk === true &&
              preflightState.data?.clientOk === true &&
              !preflightState.data?.errorCode && (
                <p className="text-xs text-green-600">{t('proxy.config.preflight_all_ok')}</p>
              )}
          </div>
        )}
        {/* 2026-08-07: open the server firewalld port after creation
            (only meaningful for a single tcp/udp rule that passed preflight). */}
        {canPreflight && (
          <div className="flex items-center justify-between rounded-md border p-3">
            <div className="space-y-0.5">
              <Label className="text-sm">{t('proxy.config.open_firewall')}</Label>
              <p className="text-xs text-muted-foreground">{t('proxy.config.open_firewall_desc')}</p>
            </div>
            <Switch checked={openFirewall} onCheckedChange={setOpenFirewall} />
          </div>
        )}
      </div>
      <Button
        disabled={submitDisabled || preflightMutation.isPending}
        onClick={() => {
          if (advancedMode) {
            if (proxyConfigs.length === 0) {
              toast(t('proxy.config.invalid_config'))
              return
            }
          } else {
            if (!TypedProxyConfigValid(proxyConfigs[0])) {
              toast(t('proxy.config.invalid_config'))
              return
            }
          }
          // Defense in depth: the button is disabled, but a gate check here
          // guarantees no path submits without a passing preflight.
          if (!preflightGatePassed) {
            toast(t('proxy.config.preflight_required'))
            return
          }
          createProxyConfigMutation.mutate()
        }}
      >
        {preflightMutation.isPending
          ? t('proxy.config.submit_pending')
          : t('proxy.config.submit')}
      </Button>
      {!preflightGatePassed && (
        <p className="text-xs text-muted-foreground">{t('proxy.config.preflight_required')}</p>
      )}
    </>
  )
}
