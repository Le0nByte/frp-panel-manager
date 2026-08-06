package user

import (
	"net/http/httptest"
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

func TestSignTokenCarriesCurrentSessionVersion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := conf.Config{}
	cfg.App.GlobalSecret = "sign-token-test-secret"
	appInstance := app.NewApp()
	appInstance.SetConfig(cfg)

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Set(defs.UserInfoKey, &models.UserEntity{
		UserID:         7,
		UserName:       "admin",
		Role:           defs.UserRole_Admin,
		SessionVersion: 4,
	})
	ctx := app.NewContext(ginCtx, appInstance)

	expiresIn := int64(60)
	path := "/api/v1/port/list"
	method := "POST"
	resp, err := SignTokenHandler(ctx, &pb.SignTokenRequest{
		ExpiresIn: &expiresIn,
		Permissions: []*pb.APIPermission{{
			Path:   &path,
			Method: &method,
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetToken())

	claims, err := utils.ParseToken(conf.JWTSecret(cfg), resp.GetToken())
	require.NoError(t, err)
	require.Equal(t, int64(4), cast.ToInt64(claims[defs.SessionVersionKey]))
}
