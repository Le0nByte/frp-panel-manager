// Package originlock provides the process-local, tenant+origin keyed
// lock shared by EVERY live config writer in the Master (the SafeCreate
// coordinator AND the legacy DB-first writers). It has NO business
// dependencies: it is a pure concurrency primitive.
//
// SINGLE-MASTER CAVEAT: the default wiring (Shared) is a PROCESS-LOCAL
// keyed mutex. It serializes same-origin flows within this Master
// process, which is exactly what the current single-Master architecture
// requires. It is NOT a cross-process lock. A future HA deployment with
// multiple Master instances MUST replace it with a DB-backed
// advisory/lease lock keyed on (tenant, origin); until that lands, this
// package MUST NOT be claimed HA-safe.
package originlock

import (
	"fmt"
	"sync"
)

// Locker serializes flows per key. Every Lock MUST be paired with
// exactly one Unlock; callers pair them with defer so the release also
// runs when the guarded flow panics.
type Locker interface {
	// Lock blocks until the key is acquired.
	Lock(key string)
	// Unlock releases the key acquired by the matching Lock.
	Unlock(key string)
}

// Shared is the process-wide default Locker used by the SafeCreate
// coordinator AND every live legacy writer. All of them MUST reference
// this same instance — a second instance would silently split the
// mutual exclusion the lock exists to provide. Tests may build their
// own KeyedLock and inject it instead.
var Shared = NewKeyedLock()

// keyLockRef is one key's entry in KeyedLock: the per-key mutex plus
// the number of goroutines that hold or wait on it.
type keyLockRef struct {
	mu   sync.Mutex
	refs int
}

// KeyedLock is a process-local, reference-counted keyed mutex
// implementing Locker. Lock/Unlock on the same key serialize; different
// keys never interact. The map entry for a key is deleted once its last
// holder unlocks, so long-lived processes never accumulate entries for
// past origins.
//
// It MUST be used through a *KeyedLock — never copied by value (it
// embeds sync.Mutex values).
type KeyedLock struct {
	mu    sync.Mutex
	locks map[string]*keyLockRef
}

// NewKeyedLock returns an empty keyed lock.
func NewKeyedLock() *KeyedLock {
	return &KeyedLock{locks: make(map[string]*keyLockRef)}
}

// Lock acquires the per-key lock, creating the map entry when the key is
// seen for the first time. A goroutine whose key is held blocks until
// the holder unlocks — or until the holder's deferred Unlock runs after
// a panic, which makes the coordinator's defer pattern panic-safe.
func (l *KeyedLock) Lock(key string) {
	l.mu.Lock()
	ref, ok := l.locks[key]
	if !ok {
		ref = &keyLockRef{}
		l.locks[key] = ref
	}
	ref.refs++
	l.mu.Unlock()

	ref.mu.Lock()
}

// Unlock releases the per-key lock. When the last holder releases, the
// map entry is deleted so the map cannot grow with the set of keys seen
// over the process lifetime. Unlock without a matching Lock is a
// programming error and panics (loud fail-closed instead of corrupting
// the refcount).
//
// ORDERING CONTRACT: the map mutex is held across refs-- → per-key
// Unlock → last-entry delete → map unlock. If the last entry were
// deleted before the per-key mutex was released, a concurrent Lock could
// create a second ref for the same key and enter while the old holder
// still owns the first ref, briefly allowing two critical sections for
// one key. This ordering closes that turnover window; do not "optimize"
// it.
func (l *KeyedLock) Unlock(key string) {
	l.mu.Lock()
	ref, ok := l.locks[key]
	if !ok || ref.refs == 0 {
		l.mu.Unlock()
		panic("originlock: KeyedLock.Unlock without a matching Lock")
	}
	ref.refs--
	last := ref.refs == 0
	// Release the per-key mutex while the map mutex is still held, then
	// delete the entry if this was the last holder.
	ref.mu.Unlock()
	if last {
		delete(l.locks, key)
	}
	l.mu.Unlock()
}

// Key builds the serialization key: the tenant and the ORIGIN client id
// (requests may arrive addressed to a child; callers resolve the origin
// BEFORE locking, then re-query under the lock). The tenant is part of
// the key so two tenants operating on the same client id string never
// block each other; the origin client id is the dimension whose shadow
// plan collides. Different origins proceed in parallel.
//
// The origin string is embedded verbatim (NUL-separated): the resolver
// guarantees both origin- and child-addressed requests map to the same
// origin string, so they contend on this exact key.
func Key(tenantID int, originClientID string) string {
	return fmt.Sprintf("%d\x00%s", tenantID, originClientID)
}
