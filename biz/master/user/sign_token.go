package user

import (
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

func SignTokenHandler(ctx *app.Context, req *pb.SignTokenRequest) (*pb.SignTokenResponse, error) {
	var (
		userInfo    = common.GetUserInfo(ctx)
		permissions = req.GetPermissions()
		expiresIn   = req.GetExpiresIn()
		cfg         = ctx.GetApp().GetConfig()
	)

	token, err := utils.GetJwtTokenFromMap(conf.JWTSecret(cfg),
		time.Now().Unix(),
		int64(expiresIn),
		map[string]interface{}{
			defs.UserIDKey:                   userInfo.GetUserID(),
			defs.TokenPayloadKey_Permissions: permissions,
			// spec §10.7.8: minted tokens MUST carry the caller's session
			// version, otherwise AuthCtx rejects every sign token once the
			// user's stored version has ever been bumped by a local admin
			// reset-password. The value is taken from the authenticated
			// entity in the context (freshly loaded by AuthCtx from the DB,
			// so it always reflects the latest reset) — never from the
			// request. See currentSessionVersion for the fallback chain.
			defs.SessionVersionKey: currentSessionVersion(ctx, userInfo),
		})
	if err != nil {
		logger.Logger(ctx).WithError(err).Errorf("get jwt token failed, userID: [%d], expiresIn: [%d], permissionCount: [%d]",
			userInfo.GetUserID(), expiresIn, len(permissions))
		return nil, err
	}

	logger.Logger(ctx).Infof("get jwt token success, userID: [%d], expiresIn: [%d], permissionCount: [%d]",
		userInfo.GetUserID(), expiresIn, len(permissions))

	return &pb.SignTokenResponse{
		Token: lo.ToPtr(token),
		Status: &pb.Status{
			Code:    pb.RespCode_RESP_CODE_SUCCESS,
			Message: "ok",
		},
	}, nil
}

// currentSessionVersion returns the session version to stamp on a freshly
// minted sign token (spec §10.7.8). The authoritative source is the
// authenticated user entity in the context: AuthCtx loads it from the DB on
// every request, so it always carries the CURRENT stored version (bumped by
// any local admin reset-password). When no entity is present it falls back
// to the already-verified JWT claim set by JWTAuth, and finally to 1 (the
// legacy initial generation). The request is never consulted. Without this
// claim every sign token would be rejected by AuthCtx after the first reset.
func currentSessionVersion(ctx *app.Context, userInfo models.UserInfo) int64 {
	if ue, ok := userInfo.(*models.UserEntity); ok {
		return ue.EffectiveSessionVersion()
	}
	if raw, ok := ctx.GetGinCtx().Get(defs.SessionVersionKey); ok {
		return cast.ToInt64(raw)
	}
	return 1
}
