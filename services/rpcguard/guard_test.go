package rpcguard

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixed summaries of flushed rows (see eventCounts).
var (
	floodSummaryRe = regexp.MustCompile(`buckets=(\d+) records=(\d+)`)
	countSummaryRe = regexp.MustCompile(`count=(\d+)`)
)

// ---- test doubles ---------------------------------------------------------

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t0 time.Time) *fakeClock { return &fakeClock{now: t0} }
func (f *fakeClock) Now() time.Time        { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// stubWriter is the SystemAuditWriter test double. failLeft>0 makes the
// next failLeft LogSystem calls return failErr (requeue probe).
type stubWriter struct {
	mu       sync.Mutex
	events   []*models.AuditEvent
	failErr  error
	failLeft int
}

func (w *stubWriter) LogSystem(ev *models.AuditEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failLeft > 0 {
		w.failLeft--
		return w.failErr
	}
	w.events = append(w.events, ev)
	return nil
}

func (w *stubWriter) snapshot() []*models.AuditEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*models.AuditEvent, len(w.events))
	copy(out, w.events)
	return out
}

// toggleWriter fails every write while fail is true, then records normally.
type toggleWriter struct {
	mu     sync.Mutex
	fail   bool
	events []*models.AuditEvent
}

func (w *toggleWriter) LogSystem(ev *models.AuditEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail {
		return errors.New("db down")
	}
	w.events = append(w.events, ev)
	return nil
}

func (w *toggleWriter) setFail(f bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fail = f
}

func (w *toggleWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events)
}

// blockingFailWriter blocks inside its first LogSystem call until the test
// releases it, then returns failErr — so tests can record concurrently
// while a flush is in flight.
type blockingFailWriter struct {
	mu      sync.Mutex
	first   chan struct{} // closed when the first LogSystem call arrives
	release chan struct{} // test closes this to let the writer fail
	events  []*models.AuditEvent
}

func (w *blockingFailWriter) LogSystem(ev *models.AuditEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.first != nil {
		close(w.first)
		w.first = nil
		<-w.release
		return errors.New("db down")
	}
	w.events = append(w.events, ev)
	return nil
}

func testClock() *fakeClock { return newFakeClock(time.Unix(1_700_000_000, 0)) }

// smallConfig returns the default config with compact thresholds so tests
// exercise full state machines without looping hundreds of times.
func smallConfig() Config {
	c := DefaultConfig()
	c.ConnectionRatePerMin = 5
	c.HandshakeFailureThreshold = 3
	c.RegisterRatePerMin = 4
	c.SecretFailureThreshold = 3
	c.DistinctSecretFailThreshold = 4
	return c
}

// ---- normalization ----------------------------------------------------------

func TestNormalizePeer(t *testing.T) {
	cases := []struct{ in, want string }{
		{"1.2.3.4:9000", "1.2.3.4"},
		{"[::1]:8080", "::1"},
		{"1.2.3.4", "1.2.3.4"},
		{"::1", "::1"},
		{"localhost:9101", "localhost"},
		{"LOCALHOST:9101", "localhost"},       // hostnames lowercased
		{"[2001:db8::1]:8080", "2001:db8::1"}, // canonical v6 form
		{"::ffff:203.0.113.7", "203.0.113.7"}, // IPv4-mapped IPv6 collapses to IPv4
		{"[::ffff:203.0.113.7]:9000", "203.0.113.7"},
		{"", "unknown"},
		{"   ", "unknown"},
		{":9000", "unknown"}, // empty host
		{"not an ip at all", "unknown"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, NormalizePeer(c.in), "NormalizePeer(%q)", c.in)
	}
}

func TestNormalizeNode(t *testing.T) {
	// control characters stripped, whitespace trimmed
	require.Equal(t, "node-1", NormalizeNode("  node-1\x00\x01\x1f  "))
	require.Equal(t, "n\u00e9mo", NormalizeNode("n\u00e9mo")) // non-ASCII kept
	// bidi / zero-width format characters (unicode.Cf) stripped too
	require.Equal(t, "abc", NormalizeNode("a\u200bb\u200ec")) // ZWSP + LRM
	require.Equal(t, "abc", NormalizeNode("a\u200fb\u202ac")) // RLM + LRE
	require.Equal(t, "abc", NormalizeNode("a\u202eb\u200dc")) // PDF + ZWJ
	require.Equal(t, "ab", NormalizeNode("a\u00adb"))         // soft hyphen
	require.Equal(t, "", NormalizeNode("\u200b\u200e\u202a"))
	require.Equal(t, "", NormalizeNode(""))
	require.Equal(t, "", NormalizeNode("  \x00\x07  "))
	// capped at 128 runes
	long := strings.Repeat("x", 300)
	got := NormalizeNode(long)
	require.Equal(t, 128, len([]rune(got)))
	require.True(t, strings.HasPrefix(got, long[:128]))
}

// ---- connection rate limit -------------------------------------------------

func TestAllowConnectionRateLimit(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk)) // defaults: 60/min/IP
	for i := 0; i < 60; i++ {
		require.True(t, g.AllowConnection("203.0.113.7:1000"), "call %d must be allowed", i+1)
	}
	require.False(t, g.AllowConnection("203.0.113.7:1001"), "61st call in window must be denied")

	// a different IP is not affected
	require.True(t, g.AllowConnection("203.0.113.8:1000"))

	// the denied call is recorded in the conn_rate_blocked audit bucket
	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 1)
	assert.Equal(t, models.AuditEventTypeRPCConnRateBlocked, evs[0].EventType)
	assert.Equal(t, "203.0.113.7", evs[0].NodeID)
	assert.False(t, evs[0].Success)
}

func TestAllowConnectionWindowReset(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 60; i++ {
		require.True(t, g.AllowConnection("203.0.113.9:1000"))
	}
	require.False(t, g.AllowConnection("203.0.113.9:1001"))

	clk.advance(61 * time.Second) // window (60s) fully expired
	require.True(t, g.AllowConnection("203.0.113.9:1001"), "window reset must allow again")
}

// ---- handshake failures → IP lock ------------------------------------------

func TestHandshakeFailureLocksIP(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 20; i++ {
		g.RecordHandshakeFailure("203.0.113.10:9101")
	}
	require.False(t, g.AllowConnection("203.0.113.10:1000"), "IP locked after 20 handshake failures")
	require.False(t, g.AllowRegister("203.0.113.10:1000", "node-x"), "register also denied for locked IP")

	// other IPs unaffected
	require.True(t, g.AllowConnection("203.0.113.11:1000"))

	// lock expires after 15m
	clk.advance(15*time.Minute + time.Second)
	require.True(t, g.AllowConnection("203.0.113.10:1000"), "IP lock must expire after 15m")
}

func TestHandshakeFailureWindowReset(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 19; i++ {
		g.RecordHandshakeFailure("203.0.113.12:9101")
	}
	require.True(t, g.AllowConnection("203.0.113.12:1000"), "19 failures < 20 must not lock")

	clk.advance(61 * time.Second) // handshake window (60s) expires
	g.RecordHandshakeFailure("203.0.113.12:9101")
	require.True(t, g.AllowConnection("203.0.113.12:1000"), "window reset: 1 failure after expiry must not lock")
}

// ---- register rate limit ----------------------------------------------------

func TestAllowRegisterRateLimit(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 30; i++ {
		require.True(t, g.AllowRegister("203.0.113.13:1000", "node-a"), "register call %d", i+1)
	}
	require.False(t, g.AllowRegister("203.0.113.13:1000", "node-a"), "31st register in window denied")
	// (IP,node) keyed: a different node from the same IP still allowed
	require.True(t, g.AllowRegister("203.0.113.13:1000", "node-b"))

	// denied register is recorded in the register_rate_blocked bucket
	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 1)
	assert.Equal(t, models.AuditEventTypeRPCRegisterRateBlocked, evs[0].EventType)
	assert.Equal(t, "node-a", evs[0].NodeID)
}

func TestAllowRegisterWindowReset(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 30; i++ {
		require.True(t, g.AllowRegister("203.0.113.14:1000", "node-a"))
	}
	require.False(t, g.AllowRegister("203.0.113.14:1000", "node-a"))
	clk.advance(61 * time.Second)
	require.True(t, g.AllowRegister("203.0.113.14:1000", "node-a"), "register window reset")
}

// ---- secret failures → key lock, success clears -----------------------------

func TestSecretFailureLocksKeyAndSuccessClears(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	key := "203.0.113.15\x00node-s1"

	for i := 0; i < 5; i++ {
		g.RecordSecretFailure("203.0.113.15:1000", "node-s1", "client")
	}
	until, locked := g.keyLock[key]
	require.True(t, locked, "5 secret failures must lock the key")
	firstDeadline := until

	// while locked, failures do NOT extend the lock
	g.RecordSecretFailure("203.0.113.15:1000", "node-s1", "client")
	until, locked = g.keyLock[key]
	require.True(t, locked)
	require.Equal(t, firstDeadline, until, "locked key must not extend on further failures")

	// success clears the key: fresh counting restarts
	g.RecordSecretSuccess("203.0.113.15:1000", "node-s1")
	_, locked = g.keyLock[key]
	require.False(t, locked, "success must clear the key lock")
	require.NotContains(t, g.secretFail, key, "success must clear the failure counter")

	for i := 0; i < 4; i++ {
		g.RecordSecretFailure("203.0.113.15:1000", "node-s1", "client")
	}
	_, locked = g.keyLock[key]
	require.False(t, locked, "4 fresh failures must not lock")
	g.RecordSecretFailure("203.0.113.15:1000", "node-s1", "client")
	_, locked = g.keyLock[key]
	require.True(t, locked, "5th fresh failure locks again")

	// key lock is NOT an IP lock: other IP traffic unaffected, same IP can still connect
	require.True(t, g.AllowConnection("203.0.113.15:2000"))
}

// ---- distinct failed node keys → IP lock --------------------------------------

func TestDistinctSecretFailuresLockIP(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 30; i++ {
		g.RecordSecretFailure("203.0.113.16:1000", fmt.Sprintf("node-%02d", i), "server")
	}
	_, locked := g.ipLock["203.0.113.16"]
	require.True(t, locked, "30 distinct failed node keys within 15m must lock the IP")
	require.False(t, g.AllowConnection("203.0.113.16:1000"))
	require.False(t, g.AllowRegister("203.0.113.16:1000", "node-zz"))
}

func TestDistinctSecretFailuresWindowReset(t *testing.T) {
	clk := testClock()
	// default config: distinct threshold is 30, window 15m
	g := NewGuard(WithClock(clk))
	for i := 0; i < 29; i++ {
		g.RecordSecretFailure("203.0.113.17:1000", fmt.Sprintf("node-%02d", i), "client")
	}
	_, locked := g.ipLock["203.0.113.17"]
	require.False(t, locked, "29 distinct keys must not lock")

	clk.advance(16 * time.Minute) // all keys aged out of the 15m window
	g.RecordSecretFailure("203.0.113.17:1000", "node-fresh", "client")
	_, locked = g.ipLock["203.0.113.17"]
	require.False(t, locked, "stale distinct keys must not count toward a fresh window")
}

func TestSecretSuccessRemovesDistinctKey(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 3; i++ {
		g.RecordSecretFailure("203.0.113.18:1000", fmt.Sprintf("node-%02d", i), "client")
	}
	require.Len(t, g.distinct["203.0.113.18"].nodes, 3)
	g.RecordSecretSuccess("203.0.113.18:1000", "node-01")
	require.Len(t, g.distinct["203.0.113.18"].nodes, 2, "success must remove the key from distinct tracking")
}

// ---- audit aggregation --------------------------------------------------------

func TestFlushAggregatesSameBucket(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 100; i++ {
		g.RecordSecretFailure("203.0.113.20:1000", "node-agg", "client")
	}

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 1, "100 identical failures must flush as ONE aggregated row")
	ev := evs[0]
	assert.Equal(t, models.AuditEventTypeRPCSecretFailure, ev.EventType)
	assert.False(t, ev.Success, "system rows never claim success")
	assert.Zero(t, ev.TenantID)
	assert.Zero(t, ev.ActorUserID)
	assert.Equal(t, "node-agg", ev.NodeID)
	assert.Equal(t, ErrorCodeSecretFailure, ev.ErrorCode)
	assert.Equal(t, "rpc.secret_failure: ip=203.0.113.20 node=node-agg layer=client count=100", ev.Summary)

	// buckets drained: a second flush writes nothing
	w2 := &stubWriter{}
	require.NoError(t, g.Flush(w2))
	assert.Empty(t, w2.snapshot(), "flush must drain buckets")
}

func TestFlushSummaryFixedStructureNoSecretMaterial(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	g.RecordSecretFailure("203.0.113.21:1000", "node-fixed", "server")
	g.RecordSecretFailure("203.0.113.21:1000", "node-fixed", "server")
	g.RecordSecretFailure("203.0.113.21:1000", "node-fixed", "server")

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 1)
	// fixed structure: no free-form input can smuggle a secret into the row
	assert.Equal(t, "rpc.secret_failure: ip=203.0.113.21 node=node-fixed layer=server count=3", evs[0].Summary)
	assert.Equal(t, "node-fixed", evs[0].NodeID)
	assert.NotContains(t, evs[0].Summary, "secret=")
	assert.NotContains(t, evs[0].Summary, "password")
}

func TestFlushCapsBucketsAndSuppresses(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.MaxAuditRowsPerFlush = 5
	g := NewGuard(WithClock(clk), WithConfig(cfg))
	for i := 0; i < 6; i++ {
		g.RecordSecretFailure("203.0.113.22:1000", fmt.Sprintf("node-%02d", i), "client")
	}

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 6, "5 capped rows + 1 flood-suppressed row")

	var normal, suppressed int
	for _, ev := range evs {
		if ev.EventType == models.AuditEventTypeRPCAuditFloodSuppressed {
			suppressed++
			assert.Equal(t, ErrorCodeAuditFloodSuppressed, ev.ErrorCode)
			assert.False(t, ev.Success)
			assert.Zero(t, ev.TenantID)
			assert.Contains(t, ev.Summary, "buckets=1")
		} else {
			normal++
			assert.Equal(t, models.AuditEventTypeRPCSecretFailure, ev.EventType)
		}
	}
	assert.Equal(t, 5, normal)
	assert.Equal(t, 1, suppressed)
}

func TestFlushWriterFailureRequeues(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 100; i++ {
		g.RecordSecretFailure("203.0.113.23:1000", "node-rq", "client")
	}

	w := &stubWriter{failErr: errors.New("db down"), failLeft: 1}
	err := g.Flush(w)
	require.Error(t, err, "writer failure must surface")
	assert.Empty(t, w.snapshot(), "failed write must not be silently dropped")

	// requeued: a healthy flush writes the full aggregated count exactly once
	w2 := &stubWriter{}
	require.NoError(t, g.Flush(w2))
	evs := w2.snapshot()
	require.Len(t, evs, 1)
	assert.Contains(t, evs[0].Summary, "count=100", "requeue must not lose or double-count records")
}

// ---- concurrency & boundedness ------------------------------------------------

func TestConcurrentAllowConnectionNoOverAllow(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.ConnectionRatePerMin = 10
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	const n = 100
	var wg sync.WaitGroup
	allowed := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			allowed[i] = g.AllowConnection("203.0.113.30:1000")
		}(i)
	}
	wg.Wait()

	got := 0
	for _, ok := range allowed {
		if ok {
			got++
		}
	}
	require.Equal(t, 10, got, "concurrent calls must never exceed the limit")
}

func TestConcurrentSecretFailuresAggregate(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.RecordSecretFailure("203.0.113.31:1000", "node-conc", "client")
		}()
	}
	wg.Wait()

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 1)
	assert.Contains(t, evs[0].Summary, "count=100", "all 100 concurrent failures must be aggregated")
}

func TestTrackedEntriesBounded(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.MaxTrackedEntries = 50
	cfg.ConnectionRatePerMin = 1 // one allowed connection per IP then blocked
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	for i := 0; i < 1000; i++ {
		// advance per insert so lastSeen timestamps are strictly ordered
		// (as in production): the newest entry is never the eviction victim.
		clk.advance(time.Millisecond)
		g.AllowConnection(fmt.Sprintf("10.0.%d.%d:1000", i/250, i%250))
	}
	total := g.totalTrackedLocked()
	require.LessOrEqual(t, total, cfg.MaxTrackedEntries, "attackers must not grow the state map unbounded")
	require.LessOrEqual(t, len(g.connRate), cfg.MaxTrackedEntries)

	// eviction keeps the guard functional: a brand-new IP is still rate limited
	clk.advance(time.Millisecond)
	require.True(t, g.AllowConnection("10.9.9.9:1000"))
	require.False(t, g.AllowConnection("10.9.9.9:1001"), "eviction must not disable rate limiting")
}

// ---- Run lifecycle -------------------------------------------------------------

func TestRunCancelDrains(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	g.RecordSecretFailure("203.0.113.40:1000", "node-run", "client")
	g.RecordSecretFailure("203.0.113.40:1000", "node-run", "client")
	g.RecordSecretFailure("203.0.113.40:1000", "node-run", "client")

	ctx, cancel := context.WithCancel(context.Background())
	w := &stubWriter{}
	done := make(chan struct{})
	go func() {
		g.Run(ctx, w)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel (goroutine leak?)")
	}
	evs := w.snapshot()
	require.Len(t, evs, 1, "cancel must drain pending buckets")
	assert.Contains(t, evs[0].Summary, "count=3")
}

func TestRunTickerFlushes(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.AuditFlushInterval = 20 * time.Millisecond
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &stubWriter{}
	go g.Run(ctx, w)

	g.RecordSecretFailure("203.0.113.41:1000", "node-tick", "client")
	require.Eventually(t, func() bool {
		return len(w.snapshot()) == 1
	}, 2*time.Second, 10*time.Millisecond, "ticker must flush pending buckets")
}

// ---- bucket eviction reporting (I3-A quality #1) ----------------------------

// TestBucketEvictionReportsSuppressed: when the boundedness trim evicts an
// audit bucket before it could be flushed, the next drain MUST emit an
// rpc.audit_flood_suppressed row with the exact evicted bucket/record counts
// instead of dropping the loss silently.
func TestBucketEvictionReportsSuppressed(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.SecretFailureThreshold = 100000      // no key locks in this test
	cfg.DistinctSecretFailThreshold = 100000 // no IP locks in this test
	cfg.MaxTrackedEntries = 5
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	const n = 100
	for i := 0; i < n; i++ {
		clk.advance(time.Millisecond) // strictly ordered lastSeen
		g.RecordSecretFailure("203.0.113.50:1000", fmt.Sprintf("node-%02d", i), "client")
	}

	g.mu.Lock()
	evB, evR := g.evictedBuckets, g.evictedRecords
	require.True(t, evB > 0, "the tight bound must have evicted audit buckets")
	require.True(t, evR > 0, "evicted buckets must carry records")
	require.LessOrEqual(t, g.totalTrackedLocked(), cfg.MaxTrackedEntries,
		"eviction counters are plain fields and must not consume tracked slots")
	g.mu.Unlock()

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()

	var suppressed *models.AuditEvent
	normal := 0
	for _, ev := range evs {
		if ev.EventType == models.AuditEventTypeRPCAuditFloodSuppressed {
			suppressed = ev
			continue
		}
		normal++
	}
	require.NotNil(t, suppressed, "bucket eviction must not be silent")
	assert.Contains(t, suppressed.Summary, fmt.Sprintf("buckets=%d", evB), "suppressed row must report the exact evicted bucket count")
	assert.Contains(t, suppressed.Summary, fmt.Sprintf("records=%d", evR), "suppressed row must report the exact evicted record count")
	assert.Equal(t, n, normal+evB, "every recorded bucket is either flushed or reported evicted")
	assert.Equal(t, ErrorCodeAuditFloodSuppressed, suppressed.ErrorCode)
	assert.False(t, suppressed.Success)

	// a successful flush clears the eviction counters
	g.mu.Lock()
	assert.Zero(t, g.evictedBuckets, "successful flush must clear eviction counters")
	assert.Zero(t, g.evictedRecords, "successful flush must clear eviction counters")
	g.mu.Unlock()
}

// TestBucketEvictionSurvivesFailedFlush: the eviction report must survive a
// failed flush via requeue — a second, healthy flush still emits a
// suppressed row, and the global accounting (every recorded bucket and
// record flushed or reported) holds exactly. Exact eviction counts are not
// pinned here because the failed flush's requeue-trim can legitimately
// evict further entries.
func TestBucketEvictionSurvivesFailedFlush(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.SecretFailureThreshold = 100000
	cfg.DistinctSecretFailThreshold = 100000
	cfg.MaxTrackedEntries = 5
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	for i := 0; i < 100; i++ {
		clk.advance(time.Millisecond)
		g.RecordSecretFailure("203.0.113.54:1000", fmt.Sprintf("node-%02d", i), "client")
	}
	g.mu.Lock()
	require.True(t, g.evictedBuckets > 0 && g.evictedRecords > 0, "precondition: buckets must have been evicted")
	g.mu.Unlock()

	w := &stubWriter{failErr: errors.New("db down"), failLeft: 1}
	require.Error(t, g.Flush(w), "first flush must fail so the report is requeued")

	w2 := &stubWriter{}
	require.NoError(t, g.Flush(w2))
	evs := w2.snapshot()

	var suppressed *models.AuditEvent
	totalBuckets, totalRecords := 0, 0
	for _, ev := range evs {
		b, r := eventCounts(t, ev)
		totalRecords += r
		if ev.EventType == models.AuditEventTypeRPCAuditFloodSuppressed {
			suppressed = ev
			require.True(t, b > 0, "suppressed report must not be zeroed")
			totalBuckets += b
		} else {
			totalBuckets++
		}
	}
	require.NotNil(t, suppressed, "eviction report must survive a failed flush")
	require.Equal(t, 100, totalRecords, "no record may be lost or double-counted across the failed flush")
	require.Equal(t, 100, totalBuckets, "every bucket is either flushed or reported in the suppressed row")
}

// eventCounts parses the fixed summary of a flushed row: normal rows carry
// count=N, flood rows carry buckets=N records=M.
func eventCounts(t *testing.T, ev *models.AuditEvent) (buckets, records int) {
	t.Helper()
	if m := floodSummaryRe.FindStringSubmatch(ev.Summary); m != nil {
		b, err1 := strconv.Atoi(m[1])
		r, err2 := strconv.Atoi(m[2])
		require.NoError(t, err1)
		require.NoError(t, err2)
		return b, r
	}
	if m := countSummaryRe.FindStringSubmatch(ev.Summary); m != nil {
		c, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		return 0, c
	}
	t.Fatalf("unrecognized summary format: %q", ev.Summary)
	return 0, 0
}

// TestFloodBucketEvictionMerges: evicting a bucket that is itself a flood
// row must merge its floodBuckets and records into the next report instead
// of losing them.
func TestFloodBucketEvictionMerges(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.SecretFailureThreshold = 100000
	cfg.DistinctSecretFailThreshold = 100000
	cfg.MaxTrackedEntries = 3
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	t0 := clk.Now().Add(-2 * time.Hour)
	g.mu.Lock()
	g.buckets[bucketKey{eventType: models.AuditEventTypeRPCAuditFloodSuppressed}] =
		&auditBucket{count: 3, floodBuckets: 7, firstSeen: t0, lastSeen: t0}
	g.mu.Unlock()

	// one public call adds counter+distinct+bucket (3 entries) → total 4 > 3,
	// so trim evicts the oldest entry: the flood bucket.
	g.RecordSecretFailure("203.0.113.52:1000", "node-fresh", "client")

	g.mu.Lock()
	require.Equal(t, 7, g.evictedBuckets, "flood row eviction must merge its floodBuckets exactly")
	require.Equal(t, 3, g.evictedRecords, "flood row eviction must merge its records")
	g.mu.Unlock()

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	var suppressed *models.AuditEvent
	for _, ev := range w.snapshot() {
		if ev.EventType == models.AuditEventTypeRPCAuditFloodSuppressed {
			suppressed = ev
		}
	}
	require.NotNil(t, suppressed)
	assert.Contains(t, suppressed.Summary, "buckets=7")
	assert.Contains(t, suppressed.Summary, "records=3")
}

// ---- flood row requeue (I3-A quality #2) ------------------------------------

// TestFloodRowRequeueKeepsBucketsRecords: a flood row that fails its first
// write and is requeued must still report buckets=N records=M on the second
// flush — the suppression counts live on the bucket, not the pending write.
func TestFloodRowRequeueKeepsBucketsRecords(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.MaxAuditRowsPerFlush = 2
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	for i := 0; i < 5; i++ {
		g.RecordSecretFailure("203.0.113.51:1000", fmt.Sprintf("node-%02d", i), "client")
	}

	w := &stubWriter{failErr: errors.New("db down"), failLeft: 1}
	require.Error(t, g.Flush(w), "first write of the flood row must fail")

	w2 := &stubWriter{}
	require.NoError(t, g.Flush(w2))
	var suppressed *models.AuditEvent
	for _, ev := range w2.snapshot() {
		if ev.EventType == models.AuditEventTypeRPCAuditFloodSuppressed &&
			strings.Contains(ev.Summary, "buckets=3") {
			suppressed = ev
		}
	}
	require.NotNil(t, suppressed, "requeued flood row must keep its suppression counts")
	assert.Contains(t, suppressed.Summary, "records=3", "requeued flood row must keep its record count")
}

// ---- AllowSecretAttempt (I3-A quality #3) -----------------------------------

func TestAllowSecretAttemptKeyLockAndSuccess(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))

	// 5 failures lock the (IP,node) key
	for i := 0; i < 5; i++ {
		g.RecordSecretFailure("203.0.113.60:1000", "node-sec", "client")
	}
	require.False(t, g.AllowSecretAttempt("203.0.113.60:1000", "node-sec"), "key-locked pair must be denied")
	require.True(t, g.AllowSecretAttempt("203.0.113.60:1000", "node-other"), "different node from same IP must be allowed")
	require.True(t, g.AllowSecretAttempt("203.0.113.61:1000", "node-sec"), "different IP must be allowed")

	// a denial is a pure check: it must not extend the lock
	g.mu.Lock()
	before := g.keyLock[joinKey("203.0.113.60", "node-sec")]
	g.mu.Unlock()
	require.False(t, g.AllowSecretAttempt("203.0.113.60:1000", "node-sec"))
	g.mu.Lock()
	after := g.keyLock[joinKey("203.0.113.60", "node-sec")]
	g.mu.Unlock()
	require.Equal(t, before, after, "denied attempt must not extend the lock")

	// success clears the key: allowed again
	g.RecordSecretSuccess("203.0.113.60:1000", "node-sec")
	require.True(t, g.AllowSecretAttempt("203.0.113.60:1000", "node-sec"), "success must clear the key lock")

	// expired key locks neither block nor linger in state
	for i := 0; i < 5; i++ {
		g.RecordSecretFailure("203.0.113.60:1000", "node-sec", "client")
	}
	require.False(t, g.AllowSecretAttempt("203.0.113.60:1000", "node-sec"))
	clk.advance(16 * time.Minute) // beyond SecretKeyLockDuration (15m)
	require.True(t, g.AllowSecretAttempt("203.0.113.60:1000", "node-sec"), "expired key lock must not block")
	g.mu.Lock()
	_, still := g.keyLock[joinKey("203.0.113.60", "node-sec")]
	g.mu.Unlock()
	require.False(t, still, "expired key lock must be removed from state")
}

func TestAllowSecretAttemptIPLockDeniesAllNodes(t *testing.T) {
	clk := testClock()
	cfg := smallConfig() // DistinctSecretFailThreshold=4
	g := NewGuard(WithClock(clk), WithConfig(cfg))

	for i := 0; i < 4; i++ {
		g.RecordSecretFailure("203.0.113.62:1000", fmt.Sprintf("node-%d", i), "server")
	}
	_, locked := g.ipLock["203.0.113.62"]
	require.True(t, locked, "precondition: distinct failures must lock the IP")

	require.False(t, g.AllowSecretAttempt("203.0.113.62:1000", "node-anything"), "IP lock must deny every node")
	require.False(t, g.AllowSecretAttempt("203.0.113.62:1000", "node-0"))
	require.True(t, g.AllowSecretAttempt("203.0.113.63:1000", "node-0"), "other IP unaffected")
}

// ---- error sink (I3-A quality #4) -------------------------------------------

func TestRunErrorSinkReportsFlushFailures(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.AuditFlushInterval = 20 * time.Millisecond

	var sinkMu sync.Mutex
	var sinkErrs []error
	g := NewGuard(WithClock(clk), WithConfig(cfg), WithErrorSink(func(err error) {
		sinkMu.Lock()
		sinkErrs = append(sinkErrs, err)
		sinkMu.Unlock()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	w := &toggleWriter{fail: true}
	done := make(chan struct{})
	go func() { g.Run(ctx, w); close(done) }()

	g.RecordSecretFailure("203.0.113.71:1000", "node-sink", "client")
	require.Eventually(t, func() bool {
		sinkMu.Lock()
		defer sinkMu.Unlock()
		return len(sinkErrs) >= 1
	}, 2*time.Second, 10*time.Millisecond, "ticker flush failure must reach the error sink")

	// healthy writer: the next ticker flush succeeds and the sink stays quiet
	w.setFail(false)
	g.RecordSecretFailure("203.0.113.71:1000", "node-sink", "client")
	require.Eventually(t, func() bool { return w.count() >= 1 }, 2*time.Second, 10*time.Millisecond,
		"healthy flush must write the row")
	sinkMu.Lock()
	quiet := len(sinkErrs)
	sinkMu.Unlock()
	time.Sleep(80 * time.Millisecond) // several ticker intervals
	sinkMu.Lock()
	require.Equal(t, quiet, len(sinkErrs), "successful flush must not call the sink")
	sinkMu.Unlock()

	// failing writer again + cancel: the cancel-final drain failure reaches the sink
	w.setFail(true)
	g.RecordSecretFailure("203.0.113.71:1000", "node-sink", "client")
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel (goroutine leak?)")
	}
	sinkMu.Lock()
	require.GreaterOrEqual(t, len(sinkErrs), quiet+1, "cancel-final flush failure must reach the sink")
	sinkMu.Unlock()

	// nothing lost through the failures: the requeued rows flush exactly once
	w2 := &stubWriter{}
	require.NoError(t, g.Flush(w2))
	evs := w2.snapshot()
	require.Len(t, evs, 1)
	assert.Contains(t, evs[0].Summary, "count=1", "requeued records must be flushed exactly once")
	require.Equal(t, 1, w.count(), "the healthy-window write must not be duplicated")
}

func TestRunErrorSinkPanicDoesNotKillRun(t *testing.T) {
	clk := testClock()
	cfg := smallConfig()
	cfg.AuditFlushInterval = 20 * time.Millisecond

	var mu sync.Mutex
	calls := 0
	g := NewGuard(WithClock(clk), WithConfig(cfg), WithErrorSink(func(err error) {
		mu.Lock()
		calls++
		mu.Unlock()
		panic("sink boom")
	}))

	ctx, cancel := context.WithCancel(context.Background())
	w := &toggleWriter{fail: true}
	done := make(chan struct{})
	go func() { g.Run(ctx, w); close(done) }()

	g.RecordSecretFailure("203.0.113.72:1000", "node-panic", "client")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 1
	}, 2*time.Second, 10*time.Millisecond, "sink must be invoked despite panicking")

	// Run survived the panic: it keeps flushing and notifying
	g.RecordSecretFailure("203.0.113.72:1000", "node-panic", "client")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	}, 2*time.Second, 10*time.Millisecond, "Run must stay alive after a sink panic")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// ---- concurrent requeue merge (I3-A quality minor) --------------------------

// TestConcurrentRecordDuringFailedFlushRequeuesWithoutLoss: records landing
// while a flush is failing in-flight must be merged on requeue — the total
// count is neither lost nor double-counted.
func TestConcurrentRecordDuringFailedFlushRequeuesWithoutLoss(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	for i := 0; i < 50; i++ {
		g.RecordSecretFailure("203.0.113.73:1000", "node-conc-rq", "client")
	}

	w := &blockingFailWriter{first: make(chan struct{}), release: make(chan struct{})}
	flushErr := make(chan error, 1)
	go func() { flushErr <- g.Flush(w) }()
	<-w.first // the flush drained 50 and is blocked inside the failing write

	// concurrent records land in the fresh buckets while the flush is in flight
	for i := 0; i < 30; i++ {
		g.RecordSecretFailure("203.0.113.73:1000", "node-conc-rq", "client")
	}
	close(w.release)
	require.Error(t, <-flushErr, "blocked write must fail and requeue")

	w2 := &stubWriter{}
	require.NoError(t, g.Flush(w2))
	evs := w2.snapshot()
	require.Len(t, evs, 1)
	assert.Contains(t, evs[0].Summary, "count=80", "requeue must merge concurrent records: 50 + 30 exactly once")
}

// ---- layer-separated aggregation (I3-A quality minor) -----------------------

func TestFlushSeparatesClientServerLayers(t *testing.T) {
	clk := testClock()
	g := NewGuard(WithClock(clk))
	g.RecordSecretFailure("203.0.113.53:1000", "node-l", "client")
	g.RecordSecretFailure("203.0.113.53:1000", "node-l", "server")

	w := &stubWriter{}
	require.NoError(t, g.Flush(w))
	evs := w.snapshot()
	require.Len(t, evs, 2, "client and server layers must aggregate separately, not last-write-overwrite")

	client, server := false, false
	for _, ev := range evs {
		assert.Contains(t, ev.Summary, "count=1")
		if strings.Contains(ev.Summary, "layer=client") {
			client = true
		}
		if strings.Contains(ev.Summary, "layer=server") {
			server = true
		}
	}
	require.True(t, client && server, "both layer summaries must be present")
}
