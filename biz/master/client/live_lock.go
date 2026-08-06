package client

import (
	"fmt"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/originlock"
)

// liveOriginLocker is the (tenant, origin) keyed lock every live legacy
// writer in this package acquires at its outermost route entry, held
// across the WHOLE operation: reads, DB writes AND the synchronous agent
// ACK. The production default IS originlock.Shared — the SAME process-wide
// instance the SafeCreate coordinator locks on — so a legacy update /
// delete / start / stop can never interleave with a SafeCreate flow on
// the same origin (round-8 I3: legacy writers bypassed the SafeCreate
// lock and silently lost updates / drifted from the agent).
//
// Tests swap this var for an independent locker (defer-restore); the
// SafeCreate coordinator injects its own locker independently. This var
// is the only place the legacy writers touch the lock, so a single swap
// covers every handler in the package.
var liveOriginLocker originlock.Locker = originlock.Shared

// lockClientOrigin resolves a request id (origin or shadow child) to its
// tenant-scoped physical origin, acquires the shared keyed lock, then resolves
// again while holding it. If a concurrent shadow conversion changed the
// mapping while this goroutine waited, the stale key is released and the
// operation retries. Callers MUST defer the returned unlock exactly once.
func lockClientOrigin(c *app.Context, requestedClientID string) (originID string, unlock func(), err error) {
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() || requestedClientID == "" {
		return "", nil, fmt.Errorf("invalid user or client id")
	}

	q := dao.NewQuery(c)
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resolved, err := q.GetOriginClientID(userInfo, requestedClientID)
		if err != nil {
			return "", nil, err
		}
		key := originlock.Key(userInfo.GetTenantID(), resolved)
		liveOriginLocker.Lock(key)

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
	return "", nil, fmt.Errorf("origin mapping of client [%s] kept changing", requestedClientID)
}
