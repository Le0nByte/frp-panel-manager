// Dual-side preflight gate for the port-manager create/edit form (spec §9.4).
//
// Pure logic, no React deps, so it is directly unit-testable with a plain
// node script (see scripts/preflight-gate.test.ts). The UI component and the
// backend both enforce this gate; this module is the single front-end source
// of truth for "which candidate may be preflighted".
//
// NOTE: this file is intentionally written as v0 mirroring the ORIGINAL inline
// logic of mutate_proxy_config.tsx (http allowed, ports only checked > 0).
// The TDD cycle fixes it to the safe contract below:
//   - only a single tcp/udp rule may be preflighted (no http/https/multi-rule)
//   - local & remote ports must be integers in 1..65535 (no NaN, no overflow)
import type { TypedProxyConfig } from '../types/proxy'

export type PreflightCandidate = {
  type: 'tcp' | 'udp'
  remotePort?: number
  localIP?: string
  localPort?: number
  name?: string
}

export const SUPPORTED_PROXY_TYPES = ['tcp', 'udp'] as const
export type SupportedProxyType = (typeof SUPPORTED_PROXY_TYPES)[number]

// Ports must be integers in the valid TCP/UDP range (1..65535). NaN, 0,
// fractions and >65535 are all rejected — never forwarded to the backend.
export const isIntegerPort = (v: number | undefined): v is number =>
  typeof v === 'number' && Number.isInteger(v) && v >= 1 && v <= 65535

// Only exactly one rule may be preflighted. A multi-rule or http/https/stcp
// etc. config has no single remotePort to check and is rejected here so the
// advanced (raw JSON) mode cannot bypass the gate.
export const extractPreflightCandidate = (
  configs: TypedProxyConfig[] | undefined,
): PreflightCandidate | null => {
  if (!configs || configs.length !== 1) return null
  const c = configs[0]
  if (c.type !== 'tcp' && c.type !== 'udp') return null
  return {
    type: c.type,
    localIP: c.localIP,
    localPort: c.localPort,
    name: c.name,
    remotePort: c.remotePort,
  }
}

// The UI-side half of the §9.4 gate: client & server selected, candidate is a
// single tcp/udp rule, both ports are integers in 1..65535.
export const canPreflightCandidate = (
  cand: PreflightCandidate | null | undefined,
  hasClient: boolean,
  hasServer: boolean,
): boolean =>
  !!cand &&
  hasClient &&
  hasServer &&
  (cand.type === 'tcp' || cand.type === 'udp') &&
  isIntegerPort(cand.localPort) &&
  isIntegerPort(cand.remotePort)
