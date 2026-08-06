package proxy

import (
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
)

// StartProxy is the live /start route entry (handler.go
// POST /api/v1/proxy/start_proxy). It is a THIN DELEGATE to SafeStartProxy
// (the B3b transactional coordinator): the handler itself never resolves
// origins, never acquires locks, never reads or writes the DB and never
// sends agent RPCs. All of that — canonical-origin resolution, the
// (tenant, origin) keyed lock, idempotency replay, before-snapshots,
// branch selection (fast DB-only vs full gates+Apply), apply/verify,
// commit and compensation — runs inside the coordinator via the
// package-global safeStartCoordinator.
func StartProxy(ctx *app.Context, req *pb.StartProxyRequest) (*pb.StartProxyResponse, error) {
	// I1-B L2 gate: a blocked master answers READ_ONLY here — the
	// coordinator (lock, DB, snapshots, agent RPC) is never reached.
	if readonly.Blocked() {
		return invalidStartResp(defs.ErrorCodeReadOnly, readonly.ErrReadOnly.Error()), nil
	}
	return SafeStartProxy(ctx, req)
}
