package dao

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils"
	"gorm.io/gorm"
)

// persistStoppedProxyRowInTx implements the stopped-branch write (S3): the
// child client row is only GUARDED — never updated, never rebuilt, never
// shadowed (zero client delta by construction) — and the old proxy row is
// updated IN PLACE (name/type/content/stopped=true; server/client/origin/
// tenant/user untouched). The row id is the lease identity, so
// finalProxyID always stays OldProxyID.
//
// When the new values are byte-identical to the current row, no UPDATE is
// issued at all: a value-identical UPDATE reports RowsAffected=0 on MySQL
// and must never be misread as a CAS miss. Otherwise the guarded UPDATE
// re-verifies the row identity (id+tenant+user+OLD name/server+stopped=true)
// in its WHERE clause — RowsAffected must be exactly 1, anything else fails
// closed with ErrProxyConfigChanged.
func persistStoppedProxyRowInTx(tx *gorm.DB, tenantID, userID int, old *models.ProxyConfig, in *CommitUpdateInput) error {
	// 1. child existence + shape guard (tenant+user scoped SELECT; the
	// stopped branch never writes the client)
	var child models.Client
	err := tx.Where("client_id = ? AND tenant_id = ? AND user_id = ?",
		in.StoppedUpdate.ChildClientID, tenantID, userID).First(&child).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: stopped branch child client %s", defs.ErrClientNotFound, in.StoppedUpdate.ChildClientID)
		}
		return err
	}
	if child.ServerID != in.OldServerID || child.OriginClientID != in.OriginClientID {
		return fmt.Errorf("%w: stopped branch child client %s shape (server=%s origin=%s) differs from the plan (server=%s origin=%s)",
			defs.ErrProxyConfigChanged, in.StoppedUpdate.ChildClientID,
			child.ServerID, child.OriginClientID, in.OldServerID, in.OriginClientID)
	}
	// A stopped rule must be absent from the child's applied config. If the
	// old or renamed target is still present, it may still be live on the
	// Agent; a DB-only update would then silently diverge from runtime state.
	// Treat an unreadable child config the same way: fail closed.
	proxies, err := utils.LoadProxiesFromContent(child.ConfigContent)
	if err != nil {
		return fmt.Errorf("%w: cannot parse stopped branch child config: %v",
			defs.ErrProxyConfigChanged, err)
	}
	for _, proxy := range proxies {
		base := proxy.GetBaseConfig()
		if base != nil && (base.Name == in.OldName || base.Name == in.StoppedUpdate.NewName) {
			return fmt.Errorf("%w: stopped target %q is still present in child %s applied config",
				defs.ErrProxyConfigChanged, base.Name, in.StoppedUpdate.ChildClientID)
		}
	}
	// 2. value-identical short circuit: the row is already in the final
	// state — no UPDATE statement, so MySQL's no-op RowsAffected=0 can
	// never be misread as a CAS miss
	if old.Name == in.StoppedUpdate.NewName &&
		old.Type == in.StoppedUpdate.NewType &&
		bytes.Equal(old.Content, in.StoppedUpdate.NewTypedJSON) &&
		old.Stopped {
		return nil
	}
	// 3. guarded in-place UPDATE (WHERE re-verifies the S1 identity CAS)
	res := tx.Model(&models.ProxyConfig{}).
		Where("id = ? AND tenant_id = ? AND user_id = ? AND name = ? AND server_id = ? AND stopped = ?",
			old.ID, tenantID, userID, in.OldName, in.OldServerID, true).
		Updates(map[string]interface{}{
			"name":    in.StoppedUpdate.NewName,
			"type":    in.StoppedUpdate.NewType,
			"content": in.StoppedUpdate.NewTypedJSON,
			"stopped": true,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("%w: stopped row %d vanished or changed between plan and commit", defs.ErrProxyConfigChanged, old.ID)
	}
	return nil
}
