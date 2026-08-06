package auth

import (
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/middleware"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
)

// loginGuard is the process-local login attempt guard. It is process-only
// by design (spec §10.7); HA deployments need a shared store.
var loginGuard = NewLoginGuard()

func LoginHandler(ctx *app.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	username := req.GetUsername()
	password := req.GetPassword()

	// spec §10.7: rate limit + lockout BEFORE the expensive password check.
	// The key binds the normalized username to the TCP peer (RemoteAddr host,
	// review I1) so failures for one account/source cannot block other users
	// or other sources, and client-spoofable forwarding headers (X-Forwarded-
	// For via gin ClientIP) cannot mint fresh keys.
	key := LoginKey(username, ClientHostKey(ctx.GetGinCtx().Request))
	if !loginGuard.Allow(key) {
		return &pb.LoginResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: rateLimitedMessage},
		}, nil
	}

	ok, user, err := dao.NewQuery(ctx).CheckUserPassword(username, password)
	if err != nil {
		// Unified failure path: a lookup error (unknown user, DB failure)
		// must not leak whether the account exists, and counts as a failed
		// attempt for the lockout.
		loginGuard.RecordFailure(key)
		return &pb.LoginResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: invalidCredentialsMessage},
		}, nil
	}

	if !ok {
		loginGuard.RecordFailure(key)
		return &pb.LoginResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: invalidCredentialsMessage},
		}, nil
	}

	loginGuard.RecordSuccess(key)

	userCount, err := dao.NewQuery(ctx).AdminCountUsers()
	if err != nil {
		logger.Logger(ctx).WithError(err).Error("get user count failed")
	}

	if userCount == 1 && user.GetSafeUserInfo().Role != defs.UserRole_Admin {
		userEntity, ok := user.(models.User)
		if !ok {
			logger.Logger(ctx).Errorf("trans user entity failed, invalid user entity")
			return &pb.LoginResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
			}, nil
		}

		userEntity.Role = defs.UserRole_Admin

		dao.NewMutation(ctx).AdminUpdateUser(&models.UserEntity{
			UserID: user.GetUserID(),
		}, userEntity.UserEntity)
	}

	// spec §10.7.8: the login token carries the user's CURRENT session
	// version so AuthCtx can reject every token issued before a local admin
	// reset-password (which bumps the stored version). Permissions ride
	// along unchanged for RBAC.
	tokenStr, err := conf.GetJWTWithPayload(ctx.GetApp().GetConfig(), user.GetUserID(), map[string]interface{}{
		defs.TokenPayloadKey_Permissions: conf.AllPermission(),
		defs.SessionVersionKey:           sessionVersionOf(user),
	})
	if err != nil {
		logger.Logger(ctx).WithError(err).Error("get jwt token failed")
		return nil, err
	}

	ginCtx := ctx.GetGinCtx()
	middleware.PushTokenStr(ginCtx, ctx.GetApp(), tokenStr)

	return &pb.LoginResponse{
		Status: &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
		Token:  &tokenStr,
	}, nil
}

// sessionVersionOf returns the user's effective session version for the
// login token claim; legacy rows (0) count as 1. The value is read from the
// real entity so the claim always matches what AuthCtx compares against.
// The UserInfo interface deliberately has no session accessor, so a
// type-assert back to the concrete wrapper is used (same pattern as the
// first-user admin promotion above).
func sessionVersionOf(user models.UserInfo) int64 {
	if ue, ok := user.(models.User); ok && ue.UserEntity != nil {
		return ue.UserEntity.EffectiveSessionVersion()
	}
	return 1
}
