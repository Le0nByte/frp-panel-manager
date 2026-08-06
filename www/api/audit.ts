import http from '@/api/http'
import { API_PATH } from '@/lib/consts'
import { v4 as uuidv4 } from 'uuid'
import {
  GetAuditRequest,
  GetAuditResponse,
  ListAuditRequest,
  ListAuditResponse,
  RollbackRequest,
  RollbackResponse,
} from '@/lib/pb/api_audit'
import { BaseResponse } from '@/types/api'

export const listAudit = async (req: ListAuditRequest) => {
  const res = await http.post(API_PATH + '/audit/list', ListAuditRequest.toJson(req))
  return ListAuditResponse.fromJson((res.data as BaseResponse).body)
}

export const getAudit = async (req: GetAuditRequest) => {
  const res = await http.post(API_PATH + '/audit/get', GetAuditRequest.toJson(req))
  return GetAuditResponse.fromJson((res.data as BaseResponse).body)
}

// rollback is retry-safe: a network retry must never roll a transaction back
// twice. Callers may pass their own key; otherwise one is generated here.
export const rollback = async (req: RollbackRequest) => {
  const res = await http.post(
    API_PATH + '/audit/rollback',
    RollbackRequest.toJson({ ...req, idempotencyKey: req.idempotencyKey ?? uuidv4() }),
  )
  return RollbackResponse.fromJson((res.data as BaseResponse).body)
}
