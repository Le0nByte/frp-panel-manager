package proxy

import (
	"fmt"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/originlock"
)

// LockLegacyOrigin resolves the ORIGIN client id for a live legacy writer
// request (the request may address the origin itself or one of its shadow
// children), acquires the (tenant, origin) keyed lock — the SAME
// originlock.Shared instance SafeCreateProxy and the master client
// handlers lock on (round-8 I3) — and returns the resolved origin id plus
// the unlock function. The caller MUST defer the unlock exactly once.
//
// The lock is acquired BEFORE the operation's first read or DB write and
// spans the whole operation (reads, DB writes AND the synchronous agent
// ACK). The resolution is re-run under the lock: if the client's origin
// mapping changed while we waited for the key (e.g. a concurrent shadow
// conversion), the lock is released and the resolve+lock cycle is retried
// (bounded) — the caller never operates under a stale key. A missing or
// invalid user, or an empty client id, fails closed before any lock is
// taken.
//
// NOTE for internal callers: any helper invoked while this lock is held
// MUST NOT re-acquire the same key itself (client.UpdateFrpcHanderLocked,
// deleteProxyConfigLegacyLocked, CreateProxyConfigWithTypedConfigLocked, ...):
// the keyed mutex is non-reentrant and the same goroutine would deadlock
// on its own key. The locked bodies re-query the rows they operate on
// instead of reusing pre-lock cached entities.
func LockLegacyOrigin(c *app.Context, requestedClientID string) (originID string, unlock func(), err error) {
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() || len(requestedClientID) == 0 {
		return "", nil, fmt.Errorf("invalid user or client id")
	}
	q := dao.NewQuery(c)
	const maxOriginLockAttempts = 3
	for attempt := 0; attempt < maxOriginLockAttempts; attempt++ {
		resolved, err := q.GetOriginClientID(userInfo, requestedClientID)
		if err != nil {
			return "", nil, err
		}
		key := originLockKey(userInfo.GetTenantID(), resolved)
		liveOriginLocker.Lock(key)
		// Re-resolve under the lock: the mapping may have changed while we
		// waited for the key. Operating under a stale key would serialize
		// against the wrong origin — release and retry instead.
		reResolved, err := q.GetOriginClientID(userInfo, requestedClientID)
		if err != nil {
			liveOriginLocker.Unlock(key)
			return "", nil, err
		}
		if reResolved != resolved {
			liveOriginLocker.Unlock(key)
			continue
		}
		return resolved, func() { liveOriginLocker.Unlock(key) }, nil
	}
	return "", nil, fmt.Errorf("origin mapping of client [%s] kept changing; giving up after %d attempts", requestedClientID, maxOriginLockAttempts)
}

// OriginLocker serializes SafeCreateProxy flows per (tenant, origin
// client) so the shadow plan — whose child id is derived from
// CountClientsInShadow OUTSIDE the commit transaction — is always
// computed against committed state.
//
// The default wiring is a PROCESS-LOCAL keyed mutex (originlock.Shared):
// it serializes same-origin creates within this Master process, which is
// exactly what the current single-Master architecture requires. It is
// NOT a cross-process lock. A future HA deployment with multiple Master
// instances MUST replace it with a DB-backed advisory/lease lock keyed
// on the origin; until that lands, this code MUST NOT be claimed HA-safe
// (see originlock.Shared).
type OriginLocker = originlock.Locker

// OriginKeyedLock is the concrete keyed-mutex implementation. The
// implementation lives in services/originlock (shared with the live
// legacy writers in biz/master/client, which cannot import this package);
// the alias keeps the SafeCreate tests compiling unchanged.
type OriginKeyedLock = originlock.KeyedLock

// NewOriginKeyedLock builds an independent keyed lock — used by tests
// that inject a private locker. Production wiring MUST use
// originlock.Shared (via sharedOriginLocker), never a second instance:
// two instances would silently split the mutual exclusion.
var NewOriginKeyedLock = originlock.NewKeyedLock

// sharedOriginLocker is the process-wide default OriginLocker used by
// NewDefaultCoordinator. It IS originlock.Shared — the SAME instance the
// live legacy writers lock on — so a legacy update/delete/start/stop can
// never interleave with a SafeCreate flow on the same origin.
var sharedOriginLocker = originlock.Shared

// liveOriginLocker is the (tenant, origin) keyed lock every live legacy
// writer in this package acquires at its outermost route entry, held
// across the WHOLE operation: reads, DB writes AND the synchronous agent
// ACK. The production default IS originlock.Shared — the SAME process-wide
// instance the SafeCreate coordinator locks on (round-8 I3: legacy
// writers bypassed the SafeCreate lock and silently lost updates). Tests
// swap this var for an independent locker (defer-restore); the SafeCreate
// coordinator injects its own locker independently.
var liveOriginLocker originlock.Locker = originlock.Shared

// originLockKey builds the SafeCreate serialization key: the tenant and
// the ORIGIN client id (req.GetClientId is always the origin — the child
// is derived from it). The tenant is part of the key so two tenants
// operating on the same client id string never block each other; the
// origin client id is the dimension whose shadow plan collides. Different
// origins proceed in parallel.
func originLockKey(tenantID int, originClientID string) string {
	return originlock.Key(tenantID, originClientID)
}
