'use client'

import { Providers } from '@/components/providers'
import { RootLayout } from '@/components/layout'
import { Header } from '@/components/header'
import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useEffect, useMemo, useState } from 'react'
import { getAudit, listAudit, rollback } from '@/api/audit'
import { AuditEvent, RespCode } from '@/lib/pb/common'
import { RollbackResponse, SnapshotMetadata } from '@/lib/pb/api_audit'
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import LoadingCircle from '@/components/base/status'
import { Search, RefreshCcw, TriangleAlert, Inbox, Undo2, FileClock } from 'lucide-react'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'

const formatTs = (v: bigint | undefined): string => {
  if (v === undefined) return '-'
  const n = Number(v)
  if (!n) return '-'
  const ms = n > 1e12 ? n : n * 1000
  return new Date(ms).toLocaleString()
}

const emptyText = (v: string | undefined | null): string => (v && v.length > 0 ? v : '-')

const decodeEvidence = (bytes: Uint8Array | undefined): string => {
  if (!bytes || bytes.length === 0) return ''
  const text = new TextDecoder().decode(bytes)
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return text
  }
}

// Only events that were committed by a non-rollback operation may be rolled back.
const canRollback = (e: AuditEvent): boolean =>
  e.success === true && e.operation !== 'rollback' && (e.status || '').toLowerCase() === 'committed'

export default function AuditPage() {
  const { t } = useTranslation()
  return (
    <Providers>
      <RootLayout mainHeader={<Header title={t('nav.audit')} />}>
        <AuditContent />
      </RootLayout>
    </Providers>
  )
}

// Data hooks live under <Providers> (QueryClientProvider): the page component
// itself is mounted by _app.tsx above the per-page Providers tree.
function AuditContent() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const [page, setPage] = useState(1)
  const pageSize = 20
  const [txFilter, setTxFilter] = useState('')
  const [appliedTxId, setAppliedTxId] = useState('')
  const [detailTxId, setDetailTxId] = useState<string | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)

  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['listAudit', page, pageSize, appliedTxId],
    queryFn: () => listAudit({ page, pageSize, transactionId: appliedTxId || undefined }),
    placeholderData: keepPreviousData,
  })

  const events: AuditEvent[] = data?.events ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / pageSize))

  const { data: detail, isFetching: detailFetching } = useQuery({
    queryKey: ['getAudit', detailTxId],
    queryFn: () => getAudit({ transactionId: detailTxId! }),
    enabled: !!detailTxId && detailOpen,
  })

  const evidenceText = useMemo(() => decodeEvidence(detail?.evidenceJson), [detail])

  // A rollback failed when the payload carries a stable errorCode OR the
  // status code is not SUCCESS — never infer success from absence alone.
  const rollbackFailed = (res: RollbackResponse | undefined): boolean =>
    !!res?.errorCode || (!!res?.status && res.status.code !== RespCode.SUCCESS)

  const rollbackMutation = useMutation({
    mutationFn: (transactionId: string) => rollback({ transactionId }),
    onSuccess: (res) => {
      if (rollbackFailed(res)) {
        toast(t('audit.rollback_failed'), {
          description:
            [res.errorCode, res.status?.message].filter((s) => !!s && s.length > 0).join(' · ') ||
            undefined,
        })
      } else {
        toast(t('audit.rollback_success'), {
          description: res.newTransactionId ? `tx: ${res.newTransactionId}` : undefined,
        })
      }
      queryClient.invalidateQueries({ queryKey: ['listAudit'] })
      if (detailTxId) {
        queryClient.invalidateQueries({ queryKey: ['getAudit', detailTxId] })
      }
    },
    onError: (e) => {
      toast(t('audit.rollback_failed'), { description: String(e) })
    },
  })

  // Rollback is destructive and irreversible: require an explicit confirmation
  // naming the exact transaction + operation. Cancel → no mutation at all.
  const requestRollback = (event: AuditEvent) => {
    const tx = event.transactionId
    if (!tx) return
    const confirmed = window.confirm(
      t('audit.rollback_confirm', { tx, operation: event.operation || '-' }),
    )
    if (!confirmed) return
    rollbackMutation.mutate(tx)
  }

  // A rollback result belongs to one transaction only: clear it whenever the
  // inspected event changes. (rollbackMutation is a fresh object each render,
  // so only detailTxId belongs in the dependency array.)
  useEffect(() => {
    rollbackMutation.reset()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [detailTxId])

  const applyTxFilter = () => {
    setPage(1)
    setAppliedTxId(txFilter.trim())
  }

  return (
    <div className="space-y-4">
          <Card>
            <CardContent className="p-3">
              <div className="flex flex-wrap items-end gap-2">
                <div className="flex flex-col gap-1">
                  <Label className="text-xs">{t('audit.filter.transaction')}</Label>
                  <Input
                    className="h-8 w-64 text-sm font-mono"
                    value={txFilter}
                    placeholder={t('audit.filter.transaction_placeholder')}
                    onChange={(e) => setTxFilter(e.target.value)}
                  />
                </div>
                <Button className="h-8 gap-1.5" onClick={applyTxFilter}>
                  <Search className="h-3.5 w-3.5" />
                  {t('audit.filter.apply')}
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
                  <span className="text-xs text-muted-foreground">
                    {t('audit.total', { total })}
                  </span>
                </div>
              </div>
            </CardContent>
          </Card>

          {isError && (
            <Alert variant="destructive">
              <TriangleAlert className="h-4 w-4" />
              <AlertTitle>{t('audit.load_failed')}</AlertTitle>
              <AlertDescription className="break-all">{String(error)}</AlertDescription>
            </Alert>
          )}

          {!isError && isLoading && (
            <div className="flex items-center justify-center py-16 text-muted-foreground">
              {t('common.loading')}
            </div>
          )}

          {!isError && !isLoading && events.length === 0 && (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-muted-foreground">
              <Inbox className="h-8 w-8" />
              <p>{t('audit.empty')}</p>
            </div>
          )}

          {!isError && !isLoading && events.length > 0 && (
            <Card>
              <CardContent className="p-0 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{t('audit.col.time')}</TableHead>
                      <TableHead>{t('audit.col.event')}</TableHead>
                      <TableHead>{t('audit.col.operation')}</TableHead>
                      <TableHead>{t('audit.col.status')}</TableHead>
                      <TableHead>{t('audit.col.proxy')}</TableHead>
                      <TableHead>{t('audit.col.node')}</TableHead>
                      <TableHead>{t('audit.col.result')}</TableHead>
                      <TableHead>{t('audit.col.summary')}</TableHead>
                      <TableHead></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {events.map((e) => (
                      <TableRow
                        key={e.transactionId || `${e.createdAt}-${e.eventType}`}
                        className="cursor-pointer"
                        onClick={() => {
                          setDetailTxId(e.transactionId || null)
                          setDetailOpen(true)
                        }}
                      >
                        <TableCell className="text-xs">{formatTs(e.createdAt)}</TableCell>
                        <TableCell className="font-mono text-xs">{emptyText(e.eventType)}</TableCell>
                        <TableCell className="font-mono text-xs">{emptyText(e.operation)}</TableCell>
                        <TableCell className="font-mono text-xs">
                          <Badge variant={statusBadgeVariant(e.status, e.success)}>{emptyText(e.status)}</Badge>
                        </TableCell>
                        <TableCell className="font-mono text-xs">
                          {e.proxyId !== undefined && e.proxyId !== 0n ? String(e.proxyId) : '-'}
                        </TableCell>
                        <TableCell className="font-mono text-xs">{emptyText(e.nodeId)}</TableCell>
                        <TableCell>
                          <Badge variant={e.success ? 'default' : 'destructive'}>
                            {e.success ? t('audit.result.success') : t('audit.result.failed')}
                          </Badge>
                        </TableCell>
                        <TableCell className="max-w-[220px] truncate text-xs">{emptyText(e.summary)}</TableCell>
                        <TableCell>
                          <Button variant="ghost" className="h-7 gap-1 px-2 text-xs" onClick={(ev) => {
                            ev.stopPropagation()
                            setDetailTxId(e.transactionId || null)
                            setDetailOpen(true)
                          }}>
                            <FileClock className="h-3.5 w-3.5" />
                            {t('audit.detail')}
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
              <div className="flex items-center justify-between p-2">
                <span className="text-xs text-muted-foreground">
                  {t('audit.page_info', { page, totalPages })}
                </span>
                <div className="flex gap-1.5">
                  <Button variant="outline" className="h-8 px-3 text-xs" disabled={page <= 1} onClick={() => setPage((p) => Math.max(1, p - 1))}>
                    {t('audit.prev')}
                  </Button>
                  <Button
                    variant="outline"
                    className="h-8 px-3 text-xs"
                    disabled={page >= totalPages}
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  >
                    {t('audit.next')}
                  </Button>
                </div>
              </div>
            </Card>
          )}

          <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
            <DialogContent className="max-h-[90vh] overflow-auto max-w-3xl">
              <DialogHeader>
                <DialogTitle>{t('audit.detail_title')}</DialogTitle>
                <DialogDescription className="font-mono text-xs break-all">
                  {detailTxId}
                </DialogDescription>
              </DialogHeader>
              {detailFetching && <div className="py-8 text-center text-muted-foreground">{t('common.loading')}</div>}
              {!detailFetching && detail?.event && (
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <DetailItem label={t('audit.detail.event')} value={emptyText(detail.event.eventType)} mono />
                    <DetailItem label={t('audit.detail.operation')} value={emptyText(detail.event.operation)} mono />
                    <DetailItem label={t('audit.detail.status')} value={emptyText(detail.event.status)} mono />
                    <DetailItem label={t('audit.detail.result')} value={detail.event.success ? t('audit.result.success') : t('audit.result.failed')} />
                    <DetailItem label={t('audit.detail.proxy')} value={detail.event.proxyId !== undefined && detail.event.proxyId !== 0n ? String(detail.event.proxyId) : '-'} mono />
                    <DetailItem label={t('audit.detail.node')} value={emptyText(detail.event.nodeId)} mono />
                    <DetailItem label={t('audit.detail.time')} value={formatTs(detail.event.createdAt)} />
                    <DetailItem label={t('audit.detail.error_code')} value={emptyText(detail.event.errorCode)} mono error={!!detail.event.errorCode} />
                  </div>
                  <div className="text-sm">
                    <Label className="text-xs text-muted-foreground">{t('audit.detail.summary')}</Label>
                    <p className="mt-1 break-all">{emptyText(detail.event.summary)}</p>
                  </div>

                  {detail.snapshots.length > 0 && (
                    <div>
                      <Label className="text-xs text-muted-foreground">{t('audit.detail.snapshots')}</Label>
                      <Table className="mt-1">
                        <TableHeader>
                          <TableRow>
                            <TableHead>{t('audit.detail.snapshot_side')}</TableHead>
                            <TableHead>{t('audit.detail.snapshot_node_type')}</TableHead>
                            <TableHead>{t('audit.detail.snapshot_node')}</TableHead>
                            <TableHead>{t('audit.detail.snapshot_sha256')}</TableHead>
                            <TableHead>{t('audit.detail.snapshot_time')}</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {detail.snapshots.map((s: SnapshotMetadata, idx: number) => (
                            <TableRow key={idx}>
                              <TableCell className="font-mono text-xs">{emptyText(s.side)}</TableCell>
                              <TableCell className="font-mono text-xs">{emptyText(s.nodeType)}</TableCell>
                              <TableCell className="font-mono text-xs">{emptyText(s.nodeId)}</TableCell>
                              <TableCell className="font-mono text-xs">{emptyText(s.sha256)}</TableCell>
                              <TableCell className="text-xs">{formatTs(s.createdAt)}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  )}

                  {evidenceText && (
                    <div>
                      <Label className="text-xs text-muted-foreground">{t('audit.detail.evidence')}</Label>
                      <pre className="mt-1 max-h-64 overflow-auto rounded-md bg-muted p-2 font-mono text-xs whitespace-pre-wrap break-all">
                        {evidenceText}
                      </pre>
                    </div>
                  )}

                  {detail.event && canRollback(detail.event) && (
                    <div className="flex items-center justify-end gap-2 pt-2">
                      {rollbackMutation.isPending && (
                        <span className="text-xs text-muted-foreground">{t('audit.rollback_pending')}</span>
                      )}
                      {rollbackMutation.data && (
                        <span className="text-xs">
                          {rollbackFailed(rollbackMutation.data) ? (
                            <span className="text-destructive">
                              {t('audit.rollback_failed')} · {rollbackMutation.data.errorCode}
                            </span>
                          ) : (
                            <span className="text-green-600">
                              {t('audit.rollback_success')} · {rollbackMutation.data.newTransactionId}
                            </span>
                          )}
                        </span>
                      )}
                      <Button
                        variant="outline"
                        className="h-8 gap-1.5"
                        disabled={rollbackMutation.isPending}
                        onClick={() => detail.event && requestRollback(detail.event)}
                      >
                        <Undo2 className="h-3.5 w-3.5" />
                        {t('audit.rollback')}
                      </Button>
                    </div>
                  )}
                </div>
              )}
              {!detailFetching && !detail?.event && (
                <div className="py-8 text-center text-muted-foreground">{t('audit.detail_not_found')}</div>
              )}
            </DialogContent>
          </Dialog>
        </div>
  )
}

const statusBadgeVariant = (status: string | undefined, success: boolean | undefined) => {
  const s = (status || '').toLowerCase()
  if (s === 'committed') return 'default'
  if (s === 'rolled_back') return 'secondary'
  if (s === 'rollback_failed' || s === 'failed') return 'destructive'
  return success === false ? 'destructive' : 'secondary'
}

const DetailItem = ({
  label,
  value,
  mono,
  error,
}: {
  label: string
  value: string
  mono?: boolean
  error?: boolean
}) => (
  <div className="flex flex-col gap-0.5">
    <Label className="text-xs text-muted-foreground">{label}</Label>
    <span className={error ? 'text-destructive' : undefined}>
      <span className={mono ? 'font-mono text-xs break-all' : 'text-xs break-all'}>{value}</span>
    </span>
  </div>
)
