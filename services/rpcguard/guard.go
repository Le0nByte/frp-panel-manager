// Package rpcguard implements the I3-A RPC admission state machine for the
// frp control plane: per-peer connection rate limiting, handshake-failure
// IP locks, per-(IP,node) register throttling, secret-failure key locks
// with distinct-node escalation to an IP lock, and an in-memory audit
// aggregator that flushes bounded, fixed-structure system rows through
// dao.AuditMutation.LogSystem.
//
// Scope & deployment notes:
//   - PROCESS-LOCAL: all state lives in this process. In an HA deployment
//     each instance keeps its own windows/locks, so aggregate limits are
//     per-instance, not global. Cross-instance enforcement is a later
//     batch (distributed/Redis-backed guard) and is out of scope here.
//   - NAT: peer normalization strips the port and keeps the transport
//     host. Behind NAT many legitimate clients share one public IP, so
//     per-IP limits intentionally aggregate them; operators raising the
//     per-IP ceilings must account for this.
//   - The guard never touches the database and never classifies DB vs
//     transient errors: it only records counts in memory and hands
//     ready-made system rows to the injected writer.
package rpcguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/VaalaCat/frp-panel/models"
)

// Stable error codes carried by flushed system audit rows. They are part
// of the guard→audit contract and must not change once consumers exist.
const (
	ErrorCodeSecretFailure        = "RPC_SECRET_FAILURE"
	ErrorCodeHandshakeFailure     = "RPC_HANDSHAKE_FAILURE"
	ErrorCodeConnRateBlocked      = "RPC_CONN_RATE_BLOCKED"
	ErrorCodeRegisterRateBlocked  = "RPC_REGISTER_RATE_BLOCKED"
	ErrorCodeAuditFloodSuppressed = "RPC_AUDIT_FLOOD_SUPPRESSED"
)

// Layer values for RecordSecretFailure. The layer is carried in the audit
// summary so operators can tell client vs server secret failures apart.
const (
	LayerClient = "client"
	LayerServer = "server"
)

// Layer values for RecordHandshakeFailureLayer (I3-C). The layer is
// carried in the audit summary so operators can tell TCP-only probes from
// attempted-but-failed TLS handshakes. These strings are part of the
// guard→audit contract.
const (
	// LayerPreTLS marks a connection that was accepted and closed without
	// a single byte being read (no ClientHello): a plain TCP probe.
	LayerPreTLS = "pre_tls"
	// LayerTLSHandshake marks a connection where bytes were read (a
	// ClientHello was attempted) but no HTTP request ever arrived — the
	// TLS handshake failed or was abandoned mid-way.
	LayerTLSHandshake = "tls_handshake"
)

// Default tuning (I3-A spec). All windows are wall-clock sliding windows;
// thresholds are per-window ceilings.
const (
	DefaultConnectionRatePerMin        = 60
	DefaultConnectionWindow            = time.Minute
	DefaultHandshakeFailureThreshold   = 20
	DefaultHandshakeFailureWindow      = time.Minute
	DefaultIPLockDuration              = 15 * time.Minute
	DefaultRegisterRatePerMin          = 30
	DefaultRegisterWindow              = time.Minute
	DefaultSecretFailureThreshold      = 5
	DefaultSecretFailureWindow         = 15 * time.Minute
	DefaultSecretKeyLockDuration       = 15 * time.Minute
	DefaultDistinctSecretFailThreshold = 30
	DefaultAuditFlushInterval          = 60 * time.Second
	// DefaultMaxAuditRowsPerFlush caps the NORMAL rows per flush. A flood of
	// buckets beyond the cap — and any buckets evicted by the boundedness
	// trim before they could be flushed — collapses into at most ONE extra
	// rpc.audit_flood_suppressed row, so a flush writes N+1 rows at worst.
	DefaultMaxAuditRowsPerFlush = 500
	DefaultMaxTrackedEntries    = 10000
)

// Config tunes a Guard. Zero fields are filled from DefaultConfig at
// construction, so callers may override only what they care about.
type Config struct {
	ConnectionRatePerMin        int
	ConnectionWindow            time.Duration
	HandshakeFailureThreshold   int
	HandshakeFailureWindow      time.Duration
	IPLockDuration              time.Duration
	RegisterRatePerMin          int
	RegisterWindow              time.Duration
	SecretFailureThreshold      int
	SecretFailureWindow         time.Duration
	SecretKeyLockDuration       time.Duration
	DistinctSecretFailThreshold int
	AuditFlushInterval          time.Duration
	// MaxAuditRowsPerFlush caps the normal rows per flush; a flood beyond
	// the cap (and eviction-derived suppression) adds at most one extra
	// rpc.audit_flood_suppressed row.
	MaxAuditRowsPerFlush int
	MaxTrackedEntries    int
}

// DefaultConfig returns the production tuning.
func DefaultConfig() Config {
	return Config{
		ConnectionRatePerMin:        DefaultConnectionRatePerMin,
		ConnectionWindow:            DefaultConnectionWindow,
		HandshakeFailureThreshold:   DefaultHandshakeFailureThreshold,
		HandshakeFailureWindow:      DefaultHandshakeFailureWindow,
		IPLockDuration:              DefaultIPLockDuration,
		RegisterRatePerMin:          DefaultRegisterRatePerMin,
		RegisterWindow:              DefaultRegisterWindow,
		SecretFailureThreshold:      DefaultSecretFailureThreshold,
		SecretFailureWindow:         DefaultSecretFailureWindow,
		SecretKeyLockDuration:       DefaultSecretKeyLockDuration,
		DistinctSecretFailThreshold: DefaultDistinctSecretFailThreshold,
		AuditFlushInterval:          DefaultAuditFlushInterval,
		MaxAuditRowsPerFlush:        DefaultMaxAuditRowsPerFlush,
		MaxTrackedEntries:           DefaultMaxTrackedEntries,
	}
}

// Clock provides the current time; injectable so tests advance time
// deterministically without sleeping.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Option configures a Guard.
type Option func(*Guard)

// WithClock injects the time source (tests).
func WithClock(c Clock) Option { return func(g *Guard) { g.clock = c } }

// WithEnabled toggles the whole guard. The DEFAULT is enabled (true) so
// the public listener is protected out of the box; MASTER_RPC_GUARD_ENABLE
// maps to it. A disabled guard passes every Allow* check, records nothing
// and never writes audit rows — the wiring stays in place, the guard just
// does nothing (I3-C).
func WithEnabled(enabled bool) Option { return func(g *Guard) { g.enabled = enabled } }

// WithConfig replaces the tuning; zero fields fall back to defaults.
func WithConfig(cfg Config) Option { return func(g *Guard) { g.cfg = cfg } }

// WithErrorSink wires a callback that Run invokes for every failed Flush
// (ticker and cancel-final alike). Records are still requeued inside Flush;
// the sink is observability only. A panicking sink is recovered and never
// takes down Run.
func WithErrorSink(sink func(error)) Option { return func(g *Guard) { g.errorSink = sink } }

// SystemAuditWriter receives aggregated system rows. dao.AuditMutation
// satisfies it via LogSystem.
type SystemAuditWriter interface {
	LogSystem(event *models.AuditEvent) error
}

// ErrNilAuditWriter is returned by Flush/Run when no writer is wired.
var ErrNilAuditWriter = errors.New("rpcguard: audit writer is nil")

// Guard is a thread-safe, process-local admission state machine plus audit
// aggregator. All methods are safe for concurrent use; internal maps are
// bounded by MaxTrackedEntries (oldest-idle entries are evicted first, and
// active locks are evicted last, soonest-expiring first).
type Guard struct {
	mu sync.Mutex

	clock Clock
	cfg   Config

	// enabled gates the whole admission/audit state machine (I3-C). It is
	// fixed at construction via WithEnabled; a disabled guard short-circuits
	// every public method to the permissive answer.
	enabled bool

	// Rate-limit / lock state. Counter keys: IP for connection/handshake,
	// "ip\x00node" for register/secret. Lock values are deadlines.
	connRate   map[string]*windowCounter   // ip
	handshake  map[string]*windowCounter   // ip
	register   map[string]*windowCounter   // ip\x00node
	secretFail map[string]*windowCounter   // ip\x00node
	distinct   map[string]*distinctCounter // ip -> failed node-key set
	ipLock     map[string]time.Time        // ip -> lockedUntil
	keyLock    map[string]time.Time        // ip\x00node -> lockedUntil

	// Audit aggregation buckets: one in-memory counter per
	// (eventType, normalizedIP, node, layer); flushed by Flush/Run.
	buckets map[bucketKey]*auditBucket

	// Eviction bookkeeping: when the boundedness trim forces an audit
	// bucket out before it was flushed, the loss is counted here and
	// reported as an rpc.audit_flood_suppressed row at the next drain.
	// These are plain fields — they deliberately consume no tracked-map
	// slots, and they are reset as soon as the drain converts them into
	// a pending row (so a failed flush preserves them via requeue).
	evictedBuckets int
	evictedRecords int

	// errorSink receives flush failures from Run (observability only).
	errorSink func(error)
}

// NewGuard builds a guard with production defaults.
func NewGuard(opts ...Option) *Guard {
	g := &Guard{
		clock:      systemClock{},
		cfg:        DefaultConfig(),
		enabled:    true,
		connRate:   make(map[string]*windowCounter),
		handshake:  make(map[string]*windowCounter),
		register:   make(map[string]*windowCounter),
		secretFail: make(map[string]*windowCounter),
		distinct:   make(map[string]*distinctCounter),
		ipLock:     make(map[string]time.Time),
		keyLock:    make(map[string]time.Time),
		buckets:    make(map[bucketKey]*auditBucket),
	}
	for _, opt := range opts {
		opt(g)
	}
	g.cfg = normalizeConfig(g.cfg)
	return g
}

func normalizeConfig(c Config) Config {
	d := DefaultConfig()
	if c.ConnectionRatePerMin <= 0 {
		c.ConnectionRatePerMin = d.ConnectionRatePerMin
	}
	if c.ConnectionWindow <= 0 {
		c.ConnectionWindow = d.ConnectionWindow
	}
	if c.HandshakeFailureThreshold <= 0 {
		c.HandshakeFailureThreshold = d.HandshakeFailureThreshold
	}
	if c.HandshakeFailureWindow <= 0 {
		c.HandshakeFailureWindow = d.HandshakeFailureWindow
	}
	if c.IPLockDuration <= 0 {
		c.IPLockDuration = d.IPLockDuration
	}
	if c.RegisterRatePerMin <= 0 {
		c.RegisterRatePerMin = d.RegisterRatePerMin
	}
	if c.RegisterWindow <= 0 {
		c.RegisterWindow = d.RegisterWindow
	}
	if c.SecretFailureThreshold <= 0 {
		c.SecretFailureThreshold = d.SecretFailureThreshold
	}
	if c.SecretFailureWindow <= 0 {
		c.SecretFailureWindow = d.SecretFailureWindow
	}
	if c.SecretKeyLockDuration <= 0 {
		c.SecretKeyLockDuration = d.SecretKeyLockDuration
	}
	if c.DistinctSecretFailThreshold <= 0 {
		c.DistinctSecretFailThreshold = d.DistinctSecretFailThreshold
	}
	if c.AuditFlushInterval <= 0 {
		c.AuditFlushInterval = d.AuditFlushInterval
	}
	if c.MaxAuditRowsPerFlush <= 0 {
		c.MaxAuditRowsPerFlush = d.MaxAuditRowsPerFlush
	}
	if c.MaxTrackedEntries <= 0 {
		c.MaxTrackedEntries = d.MaxTrackedEntries
	}
	return c
}

// ---- normalization ---------------------------------------------------------

// NormalizePeer reduces a transport peer ("host:port", "[v6]:port" or a
// bare IP) to its host for keying. Anything that is neither is "unknown"
// so a malformed peer can never produce an empty/aliased key. IPs are
// canonicalized via net.ParseIP — an IPv4-mapped IPv6 (::ffff:a.b.c.d)
// collapses to its IPv4 form — and hostnames are lowercased.
func NormalizePeer(peer string) string {
	p := strings.TrimSpace(peer)
	if p == "" {
		return "unknown"
	}
	if host, _, err := net.SplitHostPort(p); err == nil {
		return normalizePeerHost(host)
	}
	if ip := net.ParseIP(strings.Trim(p, "[]")); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4.String()
		}
		return ip.String()
	}
	return "unknown"
}

// normalizePeerHost reduces a SplitHostPort host part to its canonical key
// form: IPs (including IPv4-mapped IPv6) normalize via net.ParseIP,
// everything else (e.g. "localhost") is lowercased.
func normalizePeerHost(host string) string {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if host == "" {
		return "unknown"
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4.String()
		}
		return ip.String()
	}
	return strings.ToLower(host)
}

// NormalizeNode strips control characters and unicode format characters
// (bidi/zero-width), trims whitespace and caps the result at 128 runes so
// node keys stay bounded and attack-inert.
func NormalizeNode(node string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(node) {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			continue
		}
		b.WriteRune(r)
	}
	s := strings.TrimSpace(b.String())
	if r := []rune(s); len(r) > 128 {
		s = string(r[:128])
	}
	return s
}

func joinKey(ip, node string) string { return ip + "\x00" + node }

// ---- public API -------------------------------------------------------------

// AllowConnection reports whether a connection from peer may proceed
// under the per-IP connection rate ceiling. Denied attempts (rate or IP
// lock) are recorded in the rpc.conn_rate_blocked audit bucket.
func (g *Guard) AllowConnection(peer string) bool {
	if !g.enabled {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	// trim on EVERY return path: deny paths record buckets and must not
	// bypass the tracked-state bound.
	defer g.trimLocked(now)

	ip := NormalizePeer(peer)
	if g.ipLockedLocked(ip, now) {
		g.recordBucketLocked(now, models.AuditEventTypeRPCConnRateBlocked, ip, "", "")
		return false
	}
	e := g.windowCounterLocked(g.connRate, ip, now, g.cfg.ConnectionWindow)
	if e.count >= g.cfg.ConnectionRatePerMin {
		g.recordBucketLocked(now, models.AuditEventTypeRPCConnRateBlocked, ip, "", "")
		return false
	}
	e.count++
	return true
}

// RecordHandshakeFailure records one failed TLS/handshake attempt from
// peer with the legacy empty layer. It is kept for callers that predate
// the layer split; new callers should use RecordHandshakeFailureLayer.
func (g *Guard) RecordHandshakeFailure(peer string) {
	g.RecordHandshakeFailureLayer(peer, "")
}

// RecordHandshakeFailureLayer records one failed connection from peer at
// the given layer ("", LayerPreTLS or LayerTLSHandshake — I3-C). Beyond
// HandshakeFailureThreshold failures in the handshake window the IP is
// locked for IPLockDuration. Every failure is audited (the layer is part
// of the summary); a locked IP is never extended by further failures.
func (g *Guard) RecordHandshakeFailureLayer(peer, layer string) {
	if !g.enabled {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	defer g.trimLocked(now)

	ip := NormalizePeer(peer)
	g.recordBucketLocked(now, models.AuditEventTypeRPCHandshakeFailure, ip, "", layer)
	if g.ipLockedLocked(ip, now) {
		return // locked: audited above, never extended
	}
	e := g.windowCounterLocked(g.handshake, ip, now, g.cfg.HandshakeFailureWindow)
	e.count++
	if e.count >= g.cfg.HandshakeFailureThreshold {
		g.ipLock[ip] = now.Add(g.cfg.IPLockDuration)
		e.count = 0
		e.windowStart = now
	}
}

// AllowRegister reports whether node may register from peer under the
// per-(IP,node) ceiling. A locked IP denies registration too; denials are
// recorded in the rpc.register_rate_blocked audit bucket.
func (g *Guard) AllowRegister(peer, node string) bool {
	if !g.enabled {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	defer g.trimLocked(now)

	ip := NormalizePeer(peer)
	node = NormalizeNode(node)
	if g.ipLockedLocked(ip, now) {
		g.recordBucketLocked(now, models.AuditEventTypeRPCRegisterRateBlocked, ip, node, "")
		return false
	}
	key := joinKey(ip, node)
	e := g.windowCounterLocked(g.register, key, now, g.cfg.RegisterWindow)
	if e.count >= g.cfg.RegisterRatePerMin {
		g.recordBucketLocked(now, models.AuditEventTypeRPCRegisterRateBlocked, ip, node, "")
		return false
	}
	e.count++
	return true
}

// RecordSecretFailure records one failed ConnectSecret validation for
// (peer, node) at the given layer. SecretFailureThreshold failures in the
// secret window lock the (IP,node) key for SecretKeyLockDuration, and
// DistinctSecretFailThreshold distinct failed node keys from one IP in the
// window escalate to an IP lock. Every failure is audited; the API takes
// no secret material, so nothing secret can reach the audit rows.
func (g *Guard) RecordSecretFailure(peer, node, layer string) {
	if !g.enabled {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	defer g.trimLocked(now)

	ip := NormalizePeer(peer)
	node = NormalizeNode(node)
	g.recordBucketLocked(now, models.AuditEventTypeRPCSecretFailure, ip, node, layer)

	key := joinKey(ip, node)
	if until, ok := g.keyLock[key]; ok && now.Before(until) {
		return // key locked: audited above, never extended
	}
	e := g.windowCounterLocked(g.secretFail, key, now, g.cfg.SecretFailureWindow)
	e.count++

	// Distinct failed node-key tracking with windowed pruning: stale keys
	// never count toward a fresh window.
	d := g.distinctCounterLocked(ip, now)
	for n, first := range d.nodes {
		if now.Sub(first) >= g.cfg.SecretFailureWindow {
			delete(d.nodes, n)
		}
	}
	if _, seen := d.nodes[node]; !seen {
		d.nodes[node] = now
		d.lastSeen = now
	}

	if e.count >= g.cfg.SecretFailureThreshold {
		g.keyLock[key] = now.Add(g.cfg.SecretKeyLockDuration)
		e.count = 0
		e.windowStart = now
	}
	if len(d.nodes) >= g.cfg.DistinctSecretFailThreshold {
		g.ipLock[ip] = now.Add(g.cfg.IPLockDuration)
		delete(g.distinct, ip) // reset the distinct window so a stale lock is not instantly re-armed
	}
}

// RecordSecretSuccess clears the (peer,node) secret-failure state: the key
// lock, the failure counter and the node's distinct-failure membership.
// A legitimate success therefore never carries earlier failures forward.
func (g *Guard) RecordSecretSuccess(peer, node string) {
	if !g.enabled {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	defer g.trimLocked(now)

	ip := NormalizePeer(peer)
	node = NormalizeNode(node)
	key := joinKey(ip, node)
	delete(g.secretFail, key)
	delete(g.keyLock, key)
	if d, ok := g.distinct[ip]; ok {
		delete(d.nodes, node)
		d.lastSeen = now
		if len(d.nodes) == 0 {
			delete(g.distinct, ip)
		}
	}
}

// AllowSecretAttempt reports whether a ConnectSecret attempt from peer for
// node may proceed right now: it is denied while the IP is locked or the
// (IP,node) key is locked. It is a pure check under the same mutex — a
// denial never extends a lock and records nothing; callers that want the
// attempt audited call RecordSecretFailure themselves (which also advances
// the lock state). Expired locks encountered here are removed by the
// trailing trim, so a lock never lingers past its deadline. I3-B enforces
// this before touching the DAO.
func (g *Guard) AllowSecretAttempt(peer, node string) bool {
	if !g.enabled {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	defer g.trimLocked(now)

	ip := NormalizePeer(peer)
	node = NormalizeNode(node)
	if g.ipLockedLocked(ip, now) {
		return false
	}
	key := joinKey(ip, node)
	if until, ok := g.keyLock[key]; ok && now.Before(until) {
		return false
	}
	return true
}

// Flush drains the audit buckets into at most MaxAuditRowsPerFlush normal
// system rows (one per bucket, fixed structure, Success=false,
// Tenant/Actor 0) plus at most one rpc.audit_flood_suppressed row, and
// hands them to writer. Buckets evicted by the boundedness trim since the
// last drain are folded into that suppressed row instead of vanishing
// silently. On the first writer error the remaining buckets are requeued
// and the error is returned — a failed flush is never silently dropped.
func (g *Guard) Flush(writer SystemAuditWriter) error {
	if !g.enabled {
		return nil
	}
	if writer == nil {
		return ErrNilAuditWriter
	}
	g.mu.Lock()
	if len(g.buckets) == 0 && g.evictedBuckets == 0 && g.evictedRecords == 0 {
		g.mu.Unlock()
		return nil
	}
	pending := g.drainBucketsLocked()
	g.mu.Unlock()

	for i, p := range pending {
		if err := writer.LogSystem(p.event()); err != nil {
			g.requeue(pending[i:])
			return err
		}
	}
	return nil
}

// Run drives Flush on the audit-flush interval until ctx is cancelled,
// then drains once more before returning (no goroutine leak). Writer
// errors are requeued inside Flush and reported to the error sink (if
// wired); Run itself keeps the loop alive — a failing writer never loses
// records. A panicking sink is recovered by notifyError, so Run survives.
func (g *Guard) Run(ctx context.Context, writer SystemAuditWriter) {
	if !g.enabled {
		return // disabled: no ticker, no final drain, nothing to do
	}
	interval := g.cfg.AuditFlushInterval
	if interval <= 0 {
		interval = DefaultAuditFlushInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := g.Flush(writer); err != nil { // drain on cancel
				g.notifyError(err)
			}
			return
		case <-ticker.C:
			if err := g.Flush(writer); err != nil {
				g.notifyError(err)
			}
		}
	}
}

// notifyError delivers a flush failure to the error sink. Sink panics are
// recovered: a misbehaving sink can never take down Run (the records were
// already requeued inside Flush regardless of this call).
func (g *Guard) notifyError(err error) {
	if g.errorSink == nil {
		return
	}
	defer func() { _ = recover() }()
	g.errorSink(err)
}

// ---- internal state ---------------------------------------------------------

type windowCounter struct {
	count       int
	windowStart time.Time
	lastSeen    time.Time
}

type distinctCounter struct {
	nodes    map[string]time.Time // node -> first failed at (window start)
	lastSeen time.Time
}

type bucketKey struct {
	eventType string
	ip        string
	node      string
	layer     string
}

type auditBucket struct {
	count        int
	floodBuckets int // >0 only for rpc.audit_flood_suppressed rows: how many distinct buckets are collapsed into this one
	firstSeen    time.Time
	lastSeen     time.Time
}

// windowCounterLocked returns the counter for key, resetting it when the
// window expired. lastSeen refreshes on every access so idle entries are
// the natural eviction victims.
func (g *Guard) windowCounterLocked(m map[string]*windowCounter, key string, now time.Time, window time.Duration) *windowCounter {
	e, ok := m[key]
	if !ok {
		e = &windowCounter{windowStart: now, lastSeen: now}
		m[key] = e
		return e
	}
	if now.Sub(e.windowStart) >= window {
		e.count = 0
		e.windowStart = now
	}
	e.lastSeen = now
	return e
}

func (g *Guard) distinctCounterLocked(ip string, now time.Time) *distinctCounter {
	d, ok := g.distinct[ip]
	if !ok {
		d = &distinctCounter{nodes: make(map[string]time.Time), lastSeen: now}
		g.distinct[ip] = d
	}
	return d
}

func (g *Guard) ipLockedLocked(ip string, now time.Time) bool {
	until, ok := g.ipLock[ip]
	return ok && now.Before(until)
}

func (g *Guard) recordBucketLocked(now time.Time, eventType, ip, node, layer string) {
	k := bucketKey{eventType: eventType, ip: ip, node: node, layer: layer}
	b, ok := g.buckets[k]
	if !ok {
		b = &auditBucket{firstSeen: now, lastSeen: now}
		g.buckets[k] = b
	}
	b.count++
	b.lastSeen = now
}

// pending is one drained bucket awaiting its write. All aggregation state —
// including the flood suppression counts — lives on the auditBucket itself,
// so a requeued pending row never loses information.
type pending struct {
	key bucketKey
	b   *auditBucket
}

// event renders the fixed-structure system row for the bucket. NodeID
// carries the node, or the IP for IP-level events; the IP/node/layer/count
// always appear in the summary. No free-form input reaches the row.
func (p pending) event() *models.AuditEvent {
	if p.key.eventType == models.AuditEventTypeRPCAuditFloodSuppressed {
		return &models.AuditEvent{
			EventType: models.AuditEventTypeRPCAuditFloodSuppressed,
			Summary: fmt.Sprintf("rpc audit flood suppressed: buckets=%d records=%d",
				p.b.floodBuckets, p.b.count),
			ErrorCode:   ErrorCodeAuditFloodSuppressed,
			Success:     false,
			TenantID:    0,
			ActorUserID: 0,
		}
	}
	node := p.key.node
	if node == "" {
		node = p.key.ip
	}
	return &models.AuditEvent{
		EventType: p.key.eventType,
		Summary: fmt.Sprintf("%s: ip=%s node=%s layer=%s count=%d",
			p.key.eventType, p.key.ip, p.key.node, p.key.layer, p.b.count),
		NodeID:      node,
		ErrorCode:   errorCodeFor(p.key.eventType),
		Success:     false,
		TenantID:    0,
		ActorUserID: 0,
	}
}

func errorCodeFor(eventType string) string {
	switch eventType {
	case models.AuditEventTypeRPCSecretFailure:
		return ErrorCodeSecretFailure
	case models.AuditEventTypeRPCHandshakeFailure:
		return ErrorCodeHandshakeFailure
	case models.AuditEventTypeRPCConnRateBlocked:
		return ErrorCodeConnRateBlocked
	case models.AuditEventTypeRPCRegisterRateBlocked:
		return ErrorCodeRegisterRateBlocked
	default:
		return ""
	}
}

// drainBucketsLocked converts the buckets into a deterministic, capped
// pending list and clears them. Buckets beyond MaxAuditRowsPerFlush — plus
// any buckets the boundedness trim evicted since the last drain — collapse
// into one rpc.audit_flood_suppressed row. Eviction counters are consumed
// here (reset), so a failed flush preserves the report via requeue.
func (g *Guard) drainBucketsLocked() []pending {
	keys := make([]bucketKey, 0, len(g.buckets))
	for k := range g.buckets {
		keys = append(keys, k)
	}
	sortBucketKeys(keys)

	out := make([]pending, 0, min(len(keys), g.cfg.MaxAuditRowsPerFlush)+1)
	overflowBuckets, overflowRecords := 0, 0
	for _, k := range keys {
		if len(out) >= g.cfg.MaxAuditRowsPerFlush {
			overflowBuckets++
			overflowRecords += g.buckets[k].count
			continue
		}
		out = append(out, pending{key: k, b: g.buckets[k]})
	}
	if g.evictedBuckets > 0 || g.evictedRecords > 0 {
		overflowBuckets += g.evictedBuckets
		overflowRecords += g.evictedRecords
		g.evictedBuckets, g.evictedRecords = 0, 0
	}
	if overflowBuckets > 0 {
		now := g.clock.Now()
		out = append(out, pending{
			key: bucketKey{eventType: models.AuditEventTypeRPCAuditFloodSuppressed},
			b: &auditBucket{
				count:        overflowRecords,
				floodBuckets: overflowBuckets,
				firstSeen:    now,
				lastSeen:     now,
			},
		})
	}
	g.buckets = make(map[bucketKey]*auditBucket)
	return out
}

// requeue puts drained buckets back (re-aggregating counts AND flood
// suppression counts so a concurrent record can never be lost or
// double-counted). Called on writer failure.
func (g *Guard) requeue(ps []pending) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	for _, p := range ps {
		if existing, ok := g.buckets[p.key]; ok {
			existing.count += p.b.count
			existing.floodBuckets += p.b.floodBuckets
			if existing.firstSeen.IsZero() || p.b.firstSeen.Before(existing.firstSeen) {
				existing.firstSeen = p.b.firstSeen
			}
			if p.b.lastSeen.After(existing.lastSeen) {
				existing.lastSeen = p.b.lastSeen
			}
			continue
		}
		g.buckets[p.key] = p.b
	}
	g.trimLocked(now)
}

// ---- boundedness -------------------------------------------------------------

// totalTrackedLocked counts every in-memory entry the guard owns. Tests
// (white-box) use it to pin the boundedness invariant.
func (g *Guard) totalTrackedLocked() int {
	return len(g.connRate) + len(g.handshake) + len(g.register) + len(g.secretFail) +
		len(g.distinct) + len(g.ipLock) + len(g.keyLock) + len(g.buckets)
}

// trimLocked keeps the total tracked state at or below MaxTrackedEntries.
// Expired windows/locks are dropped first; then the oldest-idle counter,
// distinct or bucket entry; active locks are evicted last, soonest-to-
// expire first — so flooding can never erase a fresh lock cheaply.
func (g *Guard) trimLocked(now time.Time) {
	g.evictExpiredLocked(now)
	for g.totalTrackedLocked() > g.cfg.MaxTrackedEntries {
		if !g.evictOldestLocked() {
			return
		}
	}
}

// evictExpiredLocked drops window-expired counters, empty distinct sets
// and expired locks. Returns true if anything was removed.
func (g *Guard) evictExpiredLocked(now time.Time) bool {
	evicted := false
	evictCounter := func(m map[string]*windowCounter, window time.Duration) {
		for k, e := range m {
			if now.Sub(e.windowStart) >= window {
				delete(m, k)
				evicted = true
			}
		}
	}
	evictCounter(g.connRate, g.cfg.ConnectionWindow)
	evictCounter(g.handshake, g.cfg.HandshakeFailureWindow)
	evictCounter(g.register, g.cfg.RegisterWindow)
	evictCounter(g.secretFail, g.cfg.SecretFailureWindow)
	for k, d := range g.distinct {
		for node, first := range d.nodes {
			if now.Sub(first) >= g.cfg.SecretFailureWindow {
				delete(d.nodes, node)
			}
		}
		if len(d.nodes) == 0 {
			delete(g.distinct, k)
			evicted = true
		}
	}
	for k, until := range g.ipLock {
		if !now.Before(until) {
			delete(g.ipLock, k)
			evicted = true
		}
	}
	for k, until := range g.keyLock {
		if !now.Before(until) {
			delete(g.keyLock, k)
			evicted = true
		}
	}
	return evicted
}

// evictOldestLocked removes the single least-relevant entry: the oldest
// idle counter/distinct/bucket, or — when none remain — the soonest-
// expiring lock. Active locks are therefore the last thing evicted.
// Returns false when there is nothing left to evict.
func (g *Guard) evictOldestLocked() bool {
	type cand struct {
		ts   time.Time
		kind int // 0 counter, 1 distinct, 2 bucket
		ck   string
		cmap map[string]*windowCounter
		dk   string
		bk   bucketKey
		b    *auditBucket
	}
	var best cand
	have := false
	better := func(ts time.Time) bool { return !have || ts.Before(best.ts) }

	for _, m := range []map[string]*windowCounter{g.connRate, g.handshake, g.register, g.secretFail} {
		for k, e := range m {
			if better(e.lastSeen) {
				have = true
				best = cand{ts: e.lastSeen, kind: 0, ck: k, cmap: m}
			}
		}
	}
	for k, d := range g.distinct {
		if better(d.lastSeen) {
			have = true
			best = cand{ts: d.lastSeen, kind: 1, dk: k}
		}
	}
	for k, b := range g.buckets {
		if better(b.lastSeen) {
			have = true
			best = cand{ts: b.lastSeen, kind: 2, bk: k, b: b}
		}
	}
	if have {
		switch best.kind {
		case 0:
			delete(best.cmap, best.ck)
		case 1:
			delete(g.distinct, best.dk)
		case 2:
			// A bucket evicted before its flush must not vanish silently:
			// count the loss for the next drain's suppressed row. If the
			// victim is itself a flood row, merge its suppression counts
			// (the original buckets and records it stood for) — exactly,
			// with no extra increment for the row itself, so repeated
			// flood-eviction cycles cannot drift the totals.
			delete(g.buckets, best.bk)
			g.evictedRecords += best.b.count
			if best.bk.eventType == models.AuditEventTypeRPCAuditFloodSuppressed {
				g.evictedBuckets += best.b.floodBuckets
			} else {
				g.evictedBuckets++
			}
		}
		return true
	}

	// No counters/distinct/buckets left: evict the soonest-expiring lock.
	bestUntil := time.Time{}
	bestLockKey, bestLockMap := "", map[string]time.Time(nil)
	for k, until := range g.ipLock {
		if bestUntil.IsZero() || until.Before(bestUntil) {
			bestUntil, bestLockKey, bestLockMap = until, k, g.ipLock
		}
	}
	for k, until := range g.keyLock {
		if bestUntil.IsZero() || until.Before(bestUntil) {
			bestUntil, bestLockKey, bestLockMap = until, k, g.keyLock
		}
	}
	if bestLockMap != nil {
		delete(bestLockMap, bestLockKey)
		return true
	}
	return false
}

// ---- small helpers -----------------------------------------------------------

func sortBucketKeys(keys []bucketKey) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && bucketLess(keys[j], keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

func bucketLess(a, b bucketKey) bool {
	if a.eventType != b.eventType {
		return a.eventType < b.eventType
	}
	if a.ip != b.ip {
		return a.ip < b.ip
	}
	if a.node != b.node {
		return a.node < b.node
	}
	return a.layer < b.layer
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
