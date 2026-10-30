import http from '@/api/http'
import { API_PATH } from '@/lib/consts'
import { GetClientsStatusRequest, GetClientsStatusResponse } from '@/lib/pb/api_master'
import { GetPlatformInfoResponse } from '@/lib/pb/api_user'
import { BaseResponse } from '@/types/api'

export const getPlatformInfo = async () => {
  const res = await http.get(API_PATH + '/platform/baseinfo')
  return GetPlatformInfoResponse.fromJson((res.data as BaseResponse).body)
}

// Master 二进制自身的版本信息（后端 conf.GetVersion() 直出 JSON，不走 protobuf）。
// 版本号由 CI 构建时 -ldflags 注入；本地 go build 会是 "dev-build"。
export interface MasterVersionInfo {
  gitVersion: string
  gitCommit: string
  gitBranch: string
  buildDate: string
  goVersion: string
  compiler: string
  platform: string
}

export const getMasterVersion = async (): Promise<MasterVersionInfo> => {
  const res = await http.get(API_PATH + '/platform/version')
  return (res.data as BaseResponse).body as MasterVersionInfo
}

export const getClientsStatus = async (req: GetClientsStatusRequest) => {
  const res = await http.post(API_PATH + '/platform/clientsstatus', GetClientsStatusRequest.toJson(req))
  return GetClientsStatusResponse.fromJson((res.data as BaseResponse).body)
}
