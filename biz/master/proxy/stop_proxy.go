package proxy

import (
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
)

// StopProxy is the live /stop route entry (B4, handler.go
// POST /api/v1/proxy/stop_proxy). It is a THIN DELEGATE to SafeStopProxy
// (the B3a transactional coordinator): the handler itself never resolves
// origins, never acquires locks, never reads or writes the DB and never
// sends agent RPCs. All of that — canonical-origin resolution, the
// (tenant, origin) keyed lock, idempotency replay, before-snapshots,
// apply/verify, commit and compensation — runs inside the coordinator via
// the package-global safeStopCoordinator.
func StopProxy(ctx *app.Context, req *pb.StopProxyRequest) (*pb.StopProxyResponse, error) {
	// I1-B L2 gate: a blocked master answers READ_ONLY here — the
	// coordinator (lock, DB, snapshots, agent RPC) is never reached.
	if readonly.Blocked() {
		return invalidStopResp(defs.ErrorCodeReadOnly, readonly.ErrReadOnly.Error()), nil
	}
	return SafeStopProxy(ctx, req)
}
