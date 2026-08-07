import http from '@/api/http'
import { API_PATH } from '@/lib/consts'

// 2026-08-07: open a firewalld port on the server (control-plane write).
// The endpoint returns the standard {code, msg, body} envelope — not a
// protobuf type — so the raw response is returned as-is.
export interface OpenFirewallRequest {
  server_id: string
  port: number
  protocol: string
}

export interface OpenFirewallResponse {
  code: number
  msg: string
  body?: {
    status?: string
    port?: string
  }
}

export const openFirewallPort = async (req: OpenFirewallRequest): Promise<OpenFirewallResponse> => {
  const res = await http.post(API_PATH + '/firewall/open', req)
  return res.data as OpenFirewallResponse
}
