package dao_test

import (
	"errors"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestGetProxyConfigByOriginOrClientAndName is the legacy/shadow old-rule
// locator contract (D2a foundation): given the ORIGIN client id, server id
// and rule name, the lookup must hit BOTH row shapes — legacy rows
// (client_id = origin, origin_client_id empty) and shadowed rows
// (client_id = child, origin_client_id = origin) — while staying strictly
// tenant/user/server scoped, and must fail CLOSED with
// defs.ErrAmbiguousProxyTarget when both shapes match instead of guessing.
func TestGetProxyConfigByOriginOrClientAndName(t *testing.T) {
	ctx, db := newCommitTestEnv(t)
	user := testUserInfo()
	otherTenant := &models.UserEntity{UserID: 2, UserName: "other", Email: "other@example.com", TenantID: 2}

	const (
		originID = "cli-origin"
		srv1     = "srv-1"
		srv2     = "srv-2"
	)

	seed := func(clientID, originClientID, serverID, name string, tenantID, userID int) *models.ProxyConfig {
		row := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{
			ClientID: clientID, OriginClientID: originClientID,
			ServerID: serverID, Name: name, TenantID: tenantID, UserID: userID,
		}}
		require.NoError(t, db.Create(row).Error)
		return row
	}

	// legacy row: origin id lives in client_id, origin_client_id empty
	legacySSH := seed(originID, "", srv1, "ssh", user.GetTenantID(), user.GetUserID())
	// shadow row: origin id lives in origin_client_id, client_id = child
	shadowHTTP := seed("cli-child", originID, srv1, "http", user.GetTenantID(), user.GetUserID())
	// same rule name on a DIFFERENT server must stay invisible
	_ = seed(originID, "", srv2, "ssh", user.GetTenantID(), user.GetUserID())
	// other tenant must stay invisible
	_ = seed(originID, "", srv1, "ftp", otherTenant.GetTenantID(), otherTenant.GetUserID())
	// same tenant, different user must stay invisible
	_ = seed(originID, "", srv1, "sftp", user.GetTenantID(), 2)
	// legacy + shadow pair sharing (server, name) → ambiguous
	_ = seed(originID, "", srv1, "vpn", user.GetTenantID(), user.GetUserID())
	_ = seed("cli-child-2", originID, srv1, "vpn", user.GetTenantID(), user.GetUserID())

	q := dao.NewQuery(ctx)

	tests := []struct {
		name     string
		originID string
		serverID string
		proxy    string
		wantID   uint
		wantErr  error
	}{
		{
			name: "legacy row hits by client_id", originID: originID, serverID: srv1, proxy: "ssh",
			wantID: legacySSH.ID,
		},
		{
			name: "shadow row hits by origin_client_id", originID: originID, serverID: srv1, proxy: "http",
			wantID: shadowHTTP.ID,
		},
		{
			name: "server mismatch is not found", originID: originID, serverID: srv2, proxy: "http",
			wantErr: gorm.ErrRecordNotFound,
		},
		{
			name: "other tenant is not found", originID: originID, serverID: srv1, proxy: "ftp",
			wantErr: gorm.ErrRecordNotFound,
		},
		{
			name: "same tenant other user is not found", originID: originID, serverID: srv1, proxy: "sftp",
			wantErr: gorm.ErrRecordNotFound,
		},
		{
			name: "legacy plus shadow double match fails closed", originID: originID, serverID: srv1, proxy: "vpn",
			wantErr: defs.ErrAmbiguousProxyTarget,
		},
		{
			name: "absent rule is not found", originID: originID, serverID: srv1, proxy: "absent",
			wantErr: gorm.ErrRecordNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.GetProxyConfigByOriginOrClientAndName(user, tt.originID, tt.serverID, tt.proxy)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tt.wantErr),
					"error = %v, want %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, tt.wantID, got.ID, "must return exactly the seeded row")
		})
	}

	t.Run("empty args are rejected", func(t *testing.T) {
		_, err := q.GetProxyConfigByOriginOrClientAndName(user, "", srv1, "ssh")
		require.Error(t, err)
		_, err = q.GetProxyConfigByOriginOrClientAndName(user, originID, "", "ssh")
		require.Error(t, err)
		_, err = q.GetProxyConfigByOriginOrClientAndName(user, originID, srv1, "")
		require.Error(t, err)
	})
}
