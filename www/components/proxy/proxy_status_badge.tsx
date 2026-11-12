'use client'

import React, { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery } from '@tanstack/react-query'
import { Badge } from '../ui/badge'
import { Button } from '../ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { getPlatformInfo } from '@/api/platform'
import { getClient } from '@/api/client'
import { ExecCommandStr } from '@/lib/consts'
import { toast } from 'sonner'
import { Copy, PowerOff } from 'lucide-react'

// AgentOfflineBadge renders the tunnel status and, for the one status the
// operator can act on, opens the start-up guide.
//
// The distinction matters: "error" means the agent answered and the tunnel (or
// the local service) is broken, while "agent_offline" means the master holds no
// session at all, i.e. the frpc process is not running. Telling someone to
// restart the client on an "error" would be wrong, and telling them to edit the
// tunnel config on "agent_offline" is useless.
export function statusColor(status: string): string {
  switch (status) {
    case 'new':
      return 'text-blue-500'
    case 'wait start':
      return 'text-yellow-400'
    case 'start error':
      return 'text-red-500'
    case 'running':
      return 'text-green-500'
    case 'check failed':
      return 'text-orange-500'
    // Amber, not red: the tunnel config is fine, only the client is down.
    case 'agent_offline':
      return 'text-amber-500'
    case 'error':
      return 'text-red-600'
    default:
      return 'text-gray-500'
  }
}

export interface ProxyStatusBadgeProps {
  status: string
  clientID: string
  serverID: string
  err?: string
}

export function ProxyStatusBadge({ status, clientID, serverID, err }: ProxyStatusBadgeProps) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)

  // The status is a wire literal; translate it but never lose the raw value.
  const label = status === 'agent_offline' ? t('status.agent_offline') : status || 'loading'

  const badge = (
    <Badge
      variant={'secondary'}
      className={`p-2 border rounded font-mono w-fit ${statusColor(status)} text-nowrap rounded-full h-6`}
      title={err || undefined}
    >
      {label}
    </Badge>
  )

  if (status !== 'agent_offline') {
    return <div className="flex items-center gap-2 flex-row text-nowrap">{badge}</div>
  }

  return (
    <>
      <button
        type="button"
        className="flex items-center gap-1 cursor-pointer"
        onClick={() => setOpen(true)}
        aria-label={t('proxy.status.agent_offline_title')}
      >
        {badge}
      </button>
      <AgentOfflineGuide open={open} onOpenChange={setOpen} clientID={clientID} serverID={serverID} />
    </>
  )
}

function AgentOfflineGuide({
  open,
  onOpenChange,
  clientID,
  serverID,
}: {
  open: boolean
  onOpenChange: (v: boolean) => void
  clientID: string
  serverID: string
}) {
  const { t } = useTranslation()

  // Only fetched when the dialog actually opens: the list view polls every 10s
  // and must not pay for this.
  const { data: platformInfo } = useQuery({
    queryKey: ['getPlatformInfo'],
    queryFn: getPlatformInfo,
    enabled: open,
  })
  const { data: client } = useQuery({
    queryKey: ['getClient', clientID, serverID],
    queryFn: () => getClient({ clientId: clientID, serverId: serverID }),
    enabled: open && !!clientID && !!serverID,
  })

  const command =
    client?.client && platformInfo ? ExecCommandStr('client', client.client, platformInfo) : ''

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <PowerOff className="h-4 w-4 text-amber-500" />
            {t('proxy.status.agent_offline_title')}
          </DialogTitle>
          <DialogDescription>{t('proxy.status.agent_offline_hint')}</DialogDescription>
        </DialogHeader>

        <div className="space-y-3">
          <p className="text-sm text-muted-foreground">{t('proxy.status.agent_offline_desc')}</p>
          <div className="rounded-md border bg-muted p-3 font-mono text-xs break-all whitespace-pre-wrap">
            {command || '…'}
          </div>
          <div className="text-xs text-muted-foreground font-mono">
            {t('proxy.item.client_id')}: {clientID}
          </div>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            disabled={!command}
            onClick={() => {
              navigator.clipboard.writeText(command)
              toast(t('common.copy'))
            }}
          >
            <Copy className="h-4 w-4 mr-1" />
            {t('common.copy')}
          </Button>
          <Button onClick={() => onOpenChange(false)}>{t('proxy.status.close')}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
