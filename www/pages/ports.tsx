'use client'

import { Providers } from '@/components/providers'
import { RootLayout } from '@/components/layout'
import { Header } from '@/components/header'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useState } from 'react'
import { listPorts } from '@/api/port'
import { PortObservation, PortState } from '@/lib/pb/types_port'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Card, CardContent } from '@/components/ui/card'
import { BaseSelector } from '@/components/base/selector'
import LoadingCircle from '@/components/base/status'
import { Search, RefreshCcw, TriangleAlert, Inbox } from 'lucide-react'
import { cn } from '@/lib/utils'

const stateMeta: Record<PortState, { label: string; className: string; prominent?: boolean }> = {
  [PortState.STATE_UNSPECIFIED]: {
    label: 'port.state.unspecified',
    className: 'bg-gray-100 text-gray-600',
  },
  [PortState.STATE_AVAILABLE]: {
    label: 'port.state.available',
    className: 'bg-green-100 text-green-700',
  },
  [PortState.STATE_LISTENING]: {
    label: 'port.state.listening',
    className: 'bg-blue-100 text-blue-700',
  },
  [PortState.STATE_ACTIVE]: {
    label: 'port.state.active',
    className: 'bg-blue-100 text-blue-700',
  },
  [PortState.STATE_CONFLICT]: {
    label: 'port.state.conflict',
    className: 'bg-red-100 text-red-700',
    prominent: true,
  },
  [PortState.STATE_UNKNOWN]: {
    label: 'port.state.unknown',
    className: 'bg-amber-100 text-amber-700',
    prominent: true,
  },
}

const formatObservedAt = (v: bigint | undefined): string => {
  if (v === undefined) return '-'
  const n = Number(v)
  if (!n) return '-'
  return new Date(n * 1000).toLocaleString()
}

const emptyText = (v: string | undefined | null): string => (v && v.length > 0 ? v : '-')

// Port-range filters must stay integers in 1..65535. Number('') is 0 and
// Number('abc') is NaN — neither may ever reach the query payload.
const parsePortFilter = (v: string): number | undefined => {
  if (!v) return undefined
  const n = Number(v)
  return Number.isInteger(n) && n >= 1 && n <= 65535 ? n : undefined
}

type StatusFilter = 'all' | 'available' | 'listening' | 'active' | 'conflict' | 'unknown'

const statusFilterOptions = (
  t: (key: string) => string,
): { value: StatusFilter; label: string }[] => [
  { value: 'all', label: t('port.filter.all') },
  { value: 'available', label: t('port.state.available') },
  { value: 'listening', label: t('port.state.listening') },
  { value: 'active', label: t('port.state.active') },
  { value: 'conflict', label: t('port.state.conflict') },
  { value: 'unknown', label: t('port.state.unknown') },
]

const matchesStatusFilter = (o: PortObservation, f: StatusFilter): boolean => {
  if (f === 'all') return true
  const want =
    f === 'available'
      ? PortState.STATE_AVAILABLE
      : f === 'listening'
        ? PortState.STATE_LISTENING
        : f === 'active'
          ? PortState.STATE_ACTIVE
          : f === 'conflict'
            ? PortState.STATE_CONFLICT
            : PortState.STATE_UNKNOWN
  return o.state === want
}

export default function PortsPage() {
  const { t } = useTranslation()
  return (
    <Providers>
      <RootLayout mainHeader={<Header title={t('nav.ports')} />}>
        <PortsContent />
      </RootLayout>
    </Providers>
  )
}

// Data hooks live under <Providers> (QueryClientProvider): the page component
// itself is mounted by _app.tsx above the per-page Providers tree.
function PortsContent() {
  const { t } = useTranslation()
  const [nodeId, setNodeId] = useState('')
  const [protocol, setProtocol] = useState('')
  const [portMin, setPortMin] = useState('')
  const [portMax, setPortMax] = useState('')
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [applied, setApplied] = useState<{
    nodeId: string
    protocol: string
    portMin?: number
    portMax?: number
  }>({ nodeId: '', protocol: '' })

  const applyFilters = () => {
    setApplied({
      nodeId: nodeId.trim(),
      protocol: protocol === 'all' ? '' : protocol,
      portMin: parsePortFilter(portMin),
      portMax: parsePortFilter(portMax),
    })
  }

  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['listPorts', applied.nodeId, applied.protocol, applied.portMin, applied.portMax],
    queryFn: () =>
      listPorts({
        nodeId: applied.nodeId || undefined,
        protocol: applied.protocol || undefined,
        portMin: applied.portMin,
        portMax: applied.portMax,
      }),
    // Port centre auto-refresh: 30s background poll keeps conflicts current.
    refetchInterval: 30_000,
  })

  const observations: PortObservation[] = data?.observations ?? []
  // Status filtering happens client-side on the fetched observations.
  const filtered = observations.filter((o) => matchesStatusFilter(o, statusFilter))
  const conflictingCount = filtered.filter(
    (o) => o.state === PortState.STATE_CONFLICT || o.state === PortState.STATE_UNKNOWN,
  ).length

  return (
    <div className="space-y-4">
          <Card>
            <CardContent className="p-3">
              <div className="flex flex-wrap items-end gap-2">
                <div className="flex flex-col gap-1">
                  <Label className="text-xs">{t('port.filter.node')}</Label>
                  <Input
                    className="h-8 w-48 text-sm"
                    value={nodeId}
                    placeholder={t('port.filter.node_placeholder')}
                    onChange={(e) => setNodeId(e.target.value)}
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <Label className="text-xs">{t('port.filter.protocol')}</Label>
                  <BaseSelector
                    className="h-8 w-32"
                    value={protocol}
                    setValue={setProtocol}
                    dataList={[
                      { value: 'all', label: t('port.filter.all') },
                      { value: 'tcp', label: 'tcp' },
                      { value: 'udp', label: 'udp' },
                    ]}
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <Label className="text-xs">{t('port.filter.state')}</Label>
                  <BaseSelector
                    className="h-8 w-36"
                    value={statusFilter}
                    setValue={(v) => setStatusFilter(v as StatusFilter)}
                    dataList={statusFilterOptions(t)}
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <Label className="text-xs">{t('port.filter.port_min')}</Label>
                  <Input
                    className="h-8 w-24 text-sm"
                    type="number"
                    min={1}
                    max={65535}
                    value={portMin}
                    onChange={(e) => setPortMin(e.target.value)}
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <Label className="text-xs">{t('port.filter.port_max')}</Label>
                  <Input
                    className="h-8 w-24 text-sm"
                    type="number"
                    min={1}
                    max={65535}
                    value={portMax}
                    onChange={(e) => setPortMax(e.target.value)}
                  />
                </div>
                <Button className="h-8 gap-1.5" onClick={applyFilters}>
                  <Search className="h-3.5 w-3.5" />
                  {t('port.filter.apply')}
                </Button>
                <Button
                  variant="outline"
                  className="h-8 w-8 p-0"
                  onClick={() => refetch()}
                  disabled={isFetching}
                >
                  <RefreshCcw className={cn('h-3.5 w-3.5', isFetching && 'animate-spin')} />
                </Button>
                <div className="flex items-center gap-1.5 ml-auto">
                  <LoadingCircle status={isLoading ? 'loading' : isError ? 'error' : 'success'} />
                  {conflictingCount > 0 && (
                    <Badge variant="destructive" className="gap-1">
                      <TriangleAlert className="h-3 w-3" />
                      {t('port.conflict_badge', { count: conflictingCount })}
                    </Badge>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>

          {isError && (
            <Alert variant="destructive">
              <TriangleAlert className="h-4 w-4" />
              <AlertTitle>{t('port.load_failed')}</AlertTitle>
              <AlertDescription className="break-all">{String(error)}</AlertDescription>
            </Alert>
          )}

          {!isError && isLoading && (
            <div className="flex items-center justify-center py-16 text-muted-foreground">
              {t('common.loading')}
            </div>
          )}

          {!isError && !isLoading && filtered.length === 0 && (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-muted-foreground">
              <Inbox className="h-8 w-8" />
              <p>{t('port.empty')}</p>
            </div>
          )}

          {!isError && !isLoading && filtered.length > 0 && (
            <Card>
              <CardContent className="p-0 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{t('port.col.node')}</TableHead>
                      <TableHead>{t('port.col.port')}</TableHead>
                      <TableHead>{t('port.col.protocol')}</TableHead>
                      <TableHead>{t('port.col.address')}</TableHead>
                      <TableHead>{t('port.col.state')}</TableHead>
                      <TableHead>{t('port.col.process')}</TableHead>
                      <TableHead>{t('port.col.docker')}</TableHead>
                      <TableHead>{t('port.col.frp_rule')}</TableHead>
                      <TableHead>{t('port.col.connections')}</TableHead>
                      <TableHead>{t('port.col.observed_at')}</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filtered.map((o, idx) => {
                      const meta = stateMeta[o.state] ?? stateMeta[PortState.STATE_UNSPECIFIED]
                      const prominent = meta.prominent === true
                      return (
                        <TableRow key={`${o.nodeId}-${o.protocol}-${o.localPort}-${idx}`} className={cn(prominent && 'bg-red-50/60 dark:bg-red-950/20')}>
                          <TableCell className="font-mono text-xs">{emptyText(o.nodeId)}</TableCell>
                          <TableCell className="font-mono text-xs">{o.localPort}</TableCell>
                          <TableCell className="text-xs uppercase">{emptyText(o.protocol)}</TableCell>
                          <TableCell className="font-mono text-xs">{emptyText(o.localAddress)}</TableCell>
                          <TableCell>
                            <Badge className={cn('font-medium', meta.className, prominent && 'animate-pulse')}>
                              {t(meta.label)}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-xs">
                            {o.pid > 0 || o.processName ? (
                              <span className="font-mono">
                                {o.pid > 0 ? o.pid : '-'}
                                {o.processName ? ` · ${o.processName}` : ''}
                              </span>
                            ) : (
                              '-'
                            )}
                          </TableCell>
                          <TableCell className="text-xs">
                            {o.dockerContainerName || o.dockerContainerId ? (
                              <span className="font-mono">
                                {emptyText(o.dockerContainerName) ||
                                  (o.dockerContainerId.length > 12
                                    ? o.dockerContainerId.slice(0, 12)
                                    : o.dockerContainerId)}
                              </span>
                            ) : (
                              '-'
                            )}
                          </TableCell>
                          <TableCell className="font-mono text-xs">{emptyText(o.frpProxyName)}</TableCell>
                          <TableCell className="text-xs">{o.activeConnections}</TableCell>
                          <TableCell className="text-xs">{formatObservedAt(o.observedAt)}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}
        </div>
  )
}
