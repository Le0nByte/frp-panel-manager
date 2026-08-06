import http from '@/api/http'
import { API_PATH } from '@/lib/consts'
import {
  ListPortsRequest,
  ListPortsResponse,
  PreflightRequest,
  PreflightResponse,
} from '@/lib/pb/api_port'
import { BaseResponse } from '@/types/api'

export const listPorts = async (req: ListPortsRequest) => {
  const res = await http.post(API_PATH + '/port/list', ListPortsRequest.toJson(req))
  return ListPortsResponse.fromJson((res.data as BaseResponse).body)
}

export const preflight = async (req: PreflightRequest) => {
  const res = await http.post(API_PATH + '/proxy/preflight', PreflightRequest.toJson(req))
  return PreflightResponse.fromJson((res.data as BaseResponse).body)
}
