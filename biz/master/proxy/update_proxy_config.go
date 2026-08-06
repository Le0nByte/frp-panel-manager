package proxy

import (
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
)

// UpdateProxyConfig is the live /update_config route entry (A4c). It is a
// THIN DELEGATE to the transactional safe update coordinator: the
// coordinator resolves the ORIGIN client id (child- or origin-addressed)
// and acquires the (tenant, origin) keyed lock — the SAME originlock.Shared
// instance the legacy writers lock on — exactly ONCE for the whole
// operation (reads, DB writes AND the synchronous agent ACK).
//
// The handler itself must NOT lock again: the keyed mutex is non-reentrant
// and a second acquisition on the same key would deadlock this goroutine.
// Business rejections return (resp, nil) with the stable error_code field
// (spec §7.3); HTTP/HTTPS/STCP/XTCP rules are read-only per spec §1.2.5 and
// are rejected by the coordinator's target resolution
// (UNSUPPORTED_PROXY_TYPE) before any agent contact.
func UpdateProxyConfig(c *app.Context, req *pb.UpdateProxyConfigRequest) (*pb.UpdateProxyConfigResponse, error) {
	// I1-B L2 gate: a blocked master answers READ_ONLY here — the
	// coordinator (lock, DB, snapshots, agent RPC) is never reached.
	if readonly.Blocked() {
		return invalidUpdateResp(defs.ErrorCodeReadOnly, readonly.ErrReadOnly.Error()), nil
	}
	return safeUpdateCoordinator.SafeUpdateProxy(c, req)
}
