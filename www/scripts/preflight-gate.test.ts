// Real, dependency-free regression tests for the dual-side preflight gate
// (www/lib/preflight-gate.ts). No vitest/jest — a plain node assertion script.
//
// Run:
//   cd www
//   node_modules/.bin/tsc -p <tmp-tsconfig>   # compiles gate + this test
//   node <out>/scripts/preflight-gate.test.js
//
// Covers the independent-review findings:
//   C1  http must never be preflightable (default proxyType=http deadlock)
//   I1  multi-rule / empty configs must not yield a preflightable candidate
//   M3  ports must be integers in 1..65535 (no NaN, no overflow, no fractions)
//   §9.4  client & server must both be selected
import assert from 'node:assert'
import {
  SUPPORTED_PROXY_TYPES,
  canPreflightCandidate,
  extractPreflightCandidate,
  isIntegerPort,
} from '../lib/preflight-gate'
import type { PreflightCandidate } from '../lib/preflight-gate'
import type { TypedProxyConfig } from '../types/proxy'

let failures = 0
const check = (name: string, fn: () => void) => {
  try {
    fn()
    console.log(`  ok: ${name}`)
  } catch (e) {
    failures++
    console.error(`  FAIL: ${name}\n    ${(e as Error).message}`)
  }
}

const cand = (over: Partial<PreflightCandidate> = {}): PreflightCandidate => ({
  type: 'tcp',
  remotePort: 8080,
  localPort: 8081,
  ...over,
})

const tcpRule = (over: Partial<TypedProxyConfig> = {}): TypedProxyConfig =>
  ({ type: 'tcp', name: 'r1', remotePort: 8080, localPort: 8081, ...over }) as TypedProxyConfig

console.log('preflight-gate tests:')

// --- C1: http must never be preflightable -------------------------------
check('SUPPORTED_PROXY_TYPES is exactly [tcp, udp]', () => {
  assert.deepStrictEqual([...SUPPORTED_PROXY_TYPES], ['tcp', 'udp'])
})
check('extractPreflightCandidate rejects an http rule', () => {
  assert.strictEqual(
    extractPreflightCandidate([{ ...tcpRule(), type: 'http' } as TypedProxyConfig]),
    null,
  )
})
check('extractPreflightCandidate rejects an https rule', () => {
  assert.strictEqual(
    extractPreflightCandidate([{ ...tcpRule(), type: 'https' } as TypedProxyConfig]),
    null,
  )
})
check('canPreflightCandidate rejects an http candidate', () => {
  // cast: the type system now forbids http candidates outright — this guards
  // against a future loosening of the gate.
  const httpCand = { type: 'http', remotePort: 8080, localPort: 8081 } as unknown as PreflightCandidate
  assert.strictEqual(canPreflightCandidate(httpCand, true, true), false)
})

// --- I1: only a single rule may be preflighted --------------------------
check('extractPreflightCandidate rejects empty config list', () => {
  assert.strictEqual(extractPreflightCandidate([]), null)
})
check('extractPreflightCandidate rejects undefined config list', () => {
  assert.strictEqual(extractPreflightCandidate(undefined), null)
})
check('extractPreflightCandidate rejects multi-rule config', () => {
  assert.strictEqual(extractPreflightCandidate([tcpRule(), tcpRule({ name: 'r2' })]), null)
})
check('extractPreflightCandidate accepts exactly one tcp rule', () => {
  const c = extractPreflightCandidate([tcpRule()])
  assert.ok(c)
  assert.strictEqual(c.type, 'tcp')
  assert.strictEqual(c.remotePort, 8080)
})

// --- M3: integer port range 1..65535 ------------------------------------
check('isIntegerPort bounds', () => {
  assert.strictEqual(isIntegerPort(1), true)
  assert.strictEqual(isIntegerPort(65535), true)
  assert.strictEqual(isIntegerPort(0), false)
  assert.strictEqual(isIntegerPort(65536), false)
  assert.strictEqual(isIntegerPort(1.5), false)
  assert.strictEqual(isIntegerPort(Number.NaN), false)
  assert.strictEqual(isIntegerPort(undefined), false)
})
check('canPreflightCandidate rejects remotePort 0', () => {
  assert.strictEqual(canPreflightCandidate(cand({ remotePort: 0 }), true, true), false)
})
check('canPreflightCandidate rejects remotePort 65536 (upper bound)', () => {
  assert.strictEqual(canPreflightCandidate(cand({ remotePort: 65536 }), true, true), false)
})
check('canPreflightCandidate rejects fractional localPort', () => {
  assert.strictEqual(canPreflightCandidate(cand({ localPort: 80.5 }), true, true), false)
})
check('canPreflightCandidate rejects NaN localPort', () => {
  assert.strictEqual(canPreflightCandidate(cand({ localPort: Number.NaN }), true, true), false)
})
check('canPreflightCandidate rejects missing remotePort', () => {
  assert.strictEqual(canPreflightCandidate(cand({ remotePort: undefined }), true, true), false)
})

// --- §9.4: client & server must both be selected ------------------------
check('canPreflightCandidate requires client', () => {
  assert.strictEqual(canPreflightCandidate(cand(), false, true), false)
})
check('canPreflightCandidate requires server', () => {
  assert.strictEqual(canPreflightCandidate(cand(), true, false), false)
})
check('canPreflightCandidate requires a candidate', () => {
  assert.strictEqual(canPreflightCandidate(null, true, true), false)
})

// --- happy path ----------------------------------------------------------
check('canPreflightCandidate accepts a valid single tcp rule', () => {
  assert.strictEqual(canPreflightCandidate(cand(), true, true), true)
})
check('canPreflightCandidate accepts a valid single udp rule', () => {
  assert.strictEqual(canPreflightCandidate(cand({ type: 'udp' }), true, true), true)
})

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURE(S)`)
process.exit(failures === 0 ? 0 : 1)
