package auth

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/glebarez/sqlite"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---- session-version token claim (spec §10.7.8) ----

var sessionVersionEnvCounter uint64

// seedUserDBWithVersion is seedUserDB plus an explicit session version.
// A version of 0 simulates a legacy row written before the session-version
// feature existed (gorm would default 0 to 1, so 0 is forced via UPDATE).
func seedUserDBWithVersion(t *testing.T, version int64) app.Application {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:login-version-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)",
		atomic.AddUint64(&sessionVersionEnvCounter, 1), name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, db.AutoMigrate(&models.User{}))

	hash, err := bcrypt.GenerateFromPassword([]byte("correct-password"), bcrypt.MinCost)
	require.NoError(t, err)
	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID:   1,
		UserName: "admin",
		Email:    "admin@example.com",
		Password: string(hash),
		Role:     defs.UserRole_Admin,
		TenantID: 1,
	}}).Error)
	if version != 1 {
		require.NoError(t, db.Model(&models.User{}).Where("user_id = ?", 1).Update("session_version", version).Error)
	}

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	cfg := appInst.GetConfig()
	cfg.App.CookieName = "frp-panel-cookie"
	cfg.App.CookieAge = 3600
	appInst.SetConfig(cfg)
	return appInst
}

// loginTokenClaims logs in and returns the raw JWT claims of the issued
// token (real handler, real router, real DB, real login guard).
func loginTokenClaims(t *testing.T, appInst app.Application, username, password string) map[string]interface{} {
	t.Helper()
	router := loginRouter(appInst)
	swapLoginGuard(t)

	w := postLogin(t, router, username, password)
	out := parseLogin(t, w)
	require.Equal(t, int(pb.RespCode_RESP_CODE_SUCCESS), out.Body.Status.Code, out.Body.Status.Message)
	require.NotEmpty(t, out.Body.Token)

	claims, err := utils.ParseToken(conf.JWTSecret(appInst.GetConfig()), out.Body.Token)
	require.NoError(t, err)
	return claims
}

// TestLoginTokenCarriesSessionVersion: the login token must carry the
// user's CURRENT session version so AuthCtx can reject stale tokens after a
// local admin reset-password (spec §10.7.8).
func TestLoginTokenCarriesSessionVersion(t *testing.T) {
	appInst := seedUserDBWithVersion(t, 2)
	claims := loginTokenClaims(t, appInst, "admin", "correct-password")
	require.Equal(t, int64(2), cast.ToInt64(claims[defs.SessionVersionKey]),
		"token must carry the user's current session version")
}

// TestLoginTokenLegacyZeroUserGetsVersionOne: a legacy row with
// session_version 0 must be treated as version 1 in the issued claim.
func TestLoginTokenLegacyZeroUserGetsVersionOne(t *testing.T) {
	appInst := seedUserDBWithVersion(t, 0)
	claims := loginTokenClaims(t, appInst, "admin", "correct-password")
	require.Equal(t, int64(1), cast.ToInt64(claims[defs.SessionVersionKey]),
		"legacy 0-version row must be treated as version 1")
}

// TestLoginTokenStillCarriesPermissions: the permissions claim must survive
// the switch to GetJWTWithPayload (RBAC regression guard).
func TestLoginTokenStillCarriesPermissions(t *testing.T) {
	appInst := seedUserDBWithVersion(t, 1)
	claims := loginTokenClaims(t, appInst, "admin", "correct-password")
	perms, ok := claims[defs.TokenPayloadKey_Permissions].([]interface{})
	require.True(t, ok, "permissions claim must be present")
	require.NotEmpty(t, perms, "permissions claim must not be empty")
}
