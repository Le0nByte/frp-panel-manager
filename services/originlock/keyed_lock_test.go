package originlock

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The keyed origin lock is the serialization primitive behind the
// per-(tenant, origin) mutual exclusion shared by SafeCreateProxy (D1
// 复审 P1) and every live legacy writer (round-8 I3). These tests pin
// its contract: the same key serializes, different keys never block
// each other, map entries are deleted after the last release (no growth
// over a long-lived process), the defer-unlock pattern stays correct
// when the guarded flow panics, and Unlock-without-Lock fails closed.

func TestKeyedLockSerializesSameKey(t *testing.T) {
	lk := NewKeyedLock()
	const key = "origin-key"

	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone) // runs AFTER the deferred Unlock below
		lk.Lock(key)
		defer lk.Unlock(key)
		close(entered)
		<-release
	}()
	<-entered // first goroutine provably holds the key

	acquired := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone) // runs AFTER the deferred Unlock below
		lk.Lock(key)
		defer lk.Unlock(key)
		close(acquired)
	}()

	// while the first holder is still inside its critical section the
	// second Lock MUST stay blocked (it cannot acquire: the key is held)
	select {
	case <-acquired:
		t.Fatal("second Lock acquired while the first holder is still inside its critical section")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-firstDone
	<-secondDone
	require.Empty(t, lk.locks, "map entry must be deleted after the last release")
}

func TestKeyedLockDifferentKeysDoNotBlock(t *testing.T) {
	lk := NewKeyedLock()

	firstIn := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone) // runs AFTER the deferred Unlock below
		lk.Lock("key-a")
		defer lk.Unlock("key-a")
		close(firstIn)
		<-releaseFirst
	}()
	<-firstIn // "key-a" is provably held

	entered := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone) // runs AFTER the deferred Unlock below
		lk.Lock("key-b")
		defer lk.Unlock("key-b")
		close(entered)
	}()

	// a different key must be acquirable while "key-a" is still held
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("different keys blocked each other")
	}
	close(releaseFirst)
	<-firstDone
	<-secondDone
	require.Empty(t, lk.locks)
}

func TestKeyedLockReleasesMapEntryAfterLastUnlock(t *testing.T) {
	lk := NewKeyedLock()

	lk.Lock("k")
	lk.Unlock("k")
	require.Empty(t, lk.locks, "entry must be deleted once no holder remains")

	// reuse + more keys: the map must not accumulate entries
	lk.Lock("k")
	lk.Unlock("k")
	lk.Lock("other")
	lk.Unlock("other")
	require.Empty(t, lk.locks)

	// concurrent churn over a small key set, then everything is gone
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%4)
			lk.Lock(key)
			defer lk.Unlock(key)
		}(i)
	}
	wg.Wait()
	require.Empty(t, lk.locks, "map must be empty after concurrent churn")
}

func TestKeyedLockDeferredUnlockRunsAfterPanic(t *testing.T) {
	lk := NewKeyedLock()
	func() {
		defer func() { _ = recover() }()
		lk.Lock("k")
		defer lk.Unlock("k")
		panic("boom")
	}()

	// the deferred Unlock ran despite the panic: the key is free again
	done := make(chan struct{})
	go func() {
		defer close(done) // runs AFTER the deferred Unlock below
		lk.Lock("k")
		defer lk.Unlock("k")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("key stayed locked after a panic in the guarded flow — deferred Unlock did not run")
	}
	require.Empty(t, lk.locks)
}

// TestKeyedLockUnlockWithoutLockPanics pins the fail-closed contract: an
// unbalanced Unlock is a programming error and must panic loudly instead
// of corrupting the refcount silently.
func TestKeyedLockUnlockWithoutLockPanics(t *testing.T) {
	lk := NewKeyedLock()
	require.Panics(t, func() { lk.Unlock("never-locked") }, "Unlock without a matching Lock must panic")

	// double Unlock of the same key must also panic (refcount would go negative)
	lk.Lock("k")
	lk.Unlock("k")
	require.Panics(t, func() { lk.Unlock("k") }, "double Unlock must panic")
	require.Empty(t, lk.locks)
}

// TestKeyedLockTurnoverNoOverlap is the refcount-turnover stress: many
// goroutines churn the SAME key while a fresh holder keeps arriving at
// the exact moment the previous holder releases. The map-mutex-held
// unlock+delete ordering must guarantee that at most one goroutine is
// ever inside the critical section — a torn ref (new entry created
// before the old ref was released) would let two goroutines in at once.
func TestKeyedLockTurnoverNoOverlap(t *testing.T) {
	lk := NewKeyedLock()
	var inCS int32

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lk.Lock("turnover-key")
			if n := atomic.AddInt32(&inCS, 1); n != 1 {
				t.Errorf("turnover race: %d goroutines inside one critical section", n)
			}
			// tiny real work window so the release/acquire handoff
			// actually overlaps across goroutines
			time.Sleep(time.Microsecond)
			atomic.AddInt32(&inCS, -1)
			lk.Unlock("turnover-key")
		}()
	}
	wg.Wait()
	require.Empty(t, lk.locks, "map must be empty after turnover stress")
	require.Zero(t, atomic.LoadInt32(&inCS), "no goroutine may still be inside a critical section")
}

func TestKey(t *testing.T) {
	// same tenant + same origin string: identical key
	require.Equal(t, Key(1, "cli-origin"), Key(1, "cli-origin"))

	// different tenants, same origin string: MUST NOT collide
	require.NotEqual(t, Key(1, "cli-origin"), Key(2, "cli-origin"))

	// same tenant, different origins: MUST NOT collide
	require.NotEqual(t, Key(1, "cli-origin"), Key(1, "cli-other"))

	// the key must embed the origin string verbatim: a child id that
	// resolves to the origin uses the same key, so the string must not
	// be transformed (hash, prefix, ...) by Key itself.
	require.Equal(t, Key(7, "cli-origin"), Key(7, "cli-origin"))
	require.NotEqual(t, Key(7, "cli-origin"), Key(7, "cli-origin@2"))
}
