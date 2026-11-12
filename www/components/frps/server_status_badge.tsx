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
import { getServer } from '@/api/server'
import { ExecCommandStr } from '@/lib/consts'
import { ClientStatus_Status } from '@/lib/pb/api_master'
import { ClientDetail } from '../base/client_detail'
import { toast } from 'sonner'
import { Copy, PowerOff } from 'lucide-react'

// ServerStatusBadge mirrors the tunnel-side agent_offline treatment for the frps
// side: when the master holds no session for the server agent, the frps process
// is not running, and the operator needs the command that starts it — not a red
// "offline" label with no next step.
//
// "error" still means the agent answered and something inside it is broken, so
// the two cases stay visually and behaviourally distinct.

export interface ServerStatusBadgeProps {
  serverID: string
  stopped: boolean
  status?: ClientStatus_Status
  ping?: number
  clientStatus?: Parameters<typeof ClientDetail>[0]['clientStatus']
}

function statusColor(status?: ClientStatus_Status, stopped?: boolean): string {
  if (status === ClientStatus_Status.ONLINE) {
    return stopped ? 'text-yellow-500' : 'text-green-500'
  }
  if (status === ClientStatus_Status.OFFLINE) {
    return 'text-amber-500'
  }
  if (status === ClientStatus_Status.ERROR) {
    return 'text-red-500'
  }
  return 'text-gray-500'
}

export function ServerStatusBadge({
  serverID,
  stopped,
  status,
  ping,
  clientStatus,
}: ServerStatusBadgeProps) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)

  const label = (() => {
    if (status === undefined) return t('server.status_unknown')
    if (status === ClientStatus_Status.ONLINE) {
      return stopped ? t('server.status_pause') : t('server.status_online')
    }
    if (status === ClientStatus_Status.OFFLINE) return t('server.status_offline')
    if (status === ClientStatus_Status.ERROR) return t('server.status_error')
    return t('server.status_unknown')
  })()

  const offline = status === ClientStatus_Status.OFFLINE

  const badge = (
    <Badge
      variant={'secondary'}
      className={`p-2 border rounded font-mono w-fit ${statusColor(status, stopped)} text-nowrap rounded-full h-6`}
    >
      {status === undefined ? label : `${ping ?? 0}ms,${label}`}
    </Badge>
  )

  return (
    <div className="flex items-center gap-2 flex-row">
      {offline ? (
        <button
          type="button"
          className="cursor-pointer"
          onClick={() => setOpen(true)}
          aria-label={t('server.offline_title')}
        >
          {badge}
        </button>
      ) : (
        badge
      )}
      {clientStatus?.version && <ClientDetail clientStatus={clientStatus} />}
      {offline && (
        <ServerOfflineGuide open={open} onOpenChange={setOpen} serverID={serverID} />
      )}
    </div>
  )
}

function ServerOfflineGuide({
  open,
  onOpenChange,
  serverID,
}: {
  open: boolean
  onOpenChange: (v: boolean) => void
  serverID: string
}) {
  const { t } = useTranslation()

  // Only fetched when the dialog opens: the servers list polls and must not pay
  // for this on every render.
  const { data: platformInfo } = useQuery({
    queryKey: ['getPlatformInfo'],
    queryFn: getPlatformInfo,
    enabled: open,
  })
  const { data: server } = useQuery({
    queryKey: ['getServer', serverID],
    queryFn: () => getServer({ serverId: serverID }),
    enabled: open && !!serverID,
  })

  const command =
    server?.server && platformInfo ? ExecCommandStr('server', server.server, platformInfo) : ''

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <PowerOff className="h-4 w-4 text-amber-500" />
            {t('server.offline_title')}
          </DialogTitle>
          <DialogDescription>{t('server.offline_hint')}</DialogDescription>
        </DialogHeader>

        <div className="space-y-3">
          <p className="text-sm text-muted-foreground">{t('server.offline_desc')}</p>
          <div className="rounded-md border bg-muted p-3 font-mono text-xs break-all whitespace-pre-wrap">
            {command || '…'}
          </div>
          <div className="text-xs text-muted-foreground font-mono">
            {t('server.id')}: {serverID}
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
          <Button onClick={() => onOpenChange(false)}>{t('server.offline_close')}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
