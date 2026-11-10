package proxy

import (
	"fmt"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// rowPhysicalKey resolves the physical key (server + protocol + public port)
// that a STORED proxy_config row occupies right now. It is the identity of the
// port_reservation lease the row owns, and the only thing the create path may
// compare against a request: two rules with different names but the same key
// are the same physical port.
func rowPhysicalKey(row *models.ProxyConfig) (dao.ReservationKey, error) {
	if row == nil {
		return dao.ReservationKey{}, fmt.Errorf("nil proxy row")
	}
	cfg, err := row.GetTypedProxyConfig()
	if err != nil {
		return dao.ReservationKey{}, fmt.Errorf("proxy %q: content is not a parseable typed proxy config: %w", row.Name, err)
	}
	key := dao.ReservationKey{ServerID: row.ServerID}
	switch typed := cfg.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		key.Protocol, key.RemotePort = "tcp", uint32(typed.RemotePort)
	case *v1.UDPProxyConfig:
		key.Protocol, key.RemotePort = "udp", uint32(typed.RemotePort)
	default:
		return dao.ReservationKey{}, fmt.Errorf("%w: %s", defs.ErrUnsupportedProxyType, cfg.Type)
	}
	return key, nil
}

// committedLeaseOwnedBy reports whether the committed lease at key is already
// owned by ownerID. It gates the same-key overwrite adoption: only the rule
// that is being replaced may hand its own lease to the new transaction.
func committedLeaseOwnedBy(c *app.Context, userInfo models.UserInfo, key dao.ReservationKey, ownerID uint) (bool, error) {
	rows, err := dao.NewQuery(c).GetByServerFilter(userInfo, key.ServerID, key.Protocol, key.RemotePort)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if row.Status != models.ReservationStatusCommitted {
			continue
		}
		if row.OwnerProxyID != nil && *row.OwnerProxyID == ownerID {
			return true, nil
		}
	}
	return false, nil
}
