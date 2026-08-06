package master

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestAuditRoutesRegistered: the live router must expose
// POST /api/v1/audit/list, POST /api/v1/audit/get and
// POST /api/v1/audit/rollback (spec §7.1).
func TestAuditRoutesRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	ConfigureRouter(app.NewApp(), router)

	registered := map[string]bool{} // "METHOD /path"
	for _, r := range router.Routes() {
		registered[r.Method+" "+r.Path] = true
	}

	require.True(t, registered["POST /api/v1/audit/list"], "audit list route must be registered")
	require.True(t, registered["POST /api/v1/audit/get"], "audit get route must be registered")
	require.True(t, registered["POST /api/v1/audit/rollback"], "audit rollback route must be registered")

	// existing paths are unchanged
	require.True(t, registered["POST /api/v1/port/list"])
	require.True(t, registered["POST /api/v1/proxy/preflight"])
}

// TestAuditListRouteRealHit: a real HTTP POST to /api/v1/audit/list through
// the full JWT/RBAC middleware + registered route reaches the ListAudit
// handler and returns SUCCESS with an empty event list (no audit rows) —
// proving the handler is genuinely wired, not just named.
func TestAuditListRouteRealHit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	w := postJSON(t, router, token, "/api/v1/audit/list", `{}`)
	require.Equal(t, http.StatusOK, w.Code, "a structured business response is an HTTP 200 body")

	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
			Events []interface{} `json:"events"`
			Total  int32         `json:"total"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_SUCCESS), out.Body.Status.Code)
	require.Empty(t, out.Body.Events)
	require.Zero(t, out.Body.Total)
}

// TestAuditGetRouteRealHit: a real HTTP POST to /api/v1/audit/get with an
// empty body reaches the GetAudit handler and returns INVALID
// (transaction_id required) — the handler is wired and its validation runs.
func TestAuditGetRouteRealHit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	w := postJSON(t, router, token, "/api/v1/audit/get", `{}`)
	require.Equal(t, http.StatusOK, w.Code)

	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_INVALID), out.Body.Status.Code)
}

// rollbackRouteBody is the wire shape of POST /api/v1/audit/rollback.
type rollbackRouteBody struct {
	TransactionID  string `json:"transaction_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

// seedRouteRollbackReplay writes a COMMITTED rollback transaction through
// the REAL DAO on the route test DB so a real HTTP request that reaches the
// coordinator replays it (same request hash) and returns the same
// transaction id — the strongest proof that the route is wired end-to-end.
func seedRouteRollbackReplay(t *testing.T, appInst *routeTestApp, idemKey, sourceTxID, rbTxID string) {
	t.Helper()
	user := &models.UserEntity{UserID: 1, UserName: "tester", Email: "tester@example.com", TenantID: 1}
	ctx := app.NewContext(context.WithValue(context.Background(), defs.UserInfoKey, user), appInst)
	sum := sha256.Sum256([]byte(sourceTxID))
	require.NoError(t, dao.NewMutation(ctx).Create(user, &models.ChangeTransaction{
		TransactionID:  rbTxID,
		IdempotencyKey: idemKey,
		RequestHash:    hex.EncodeToString(sum[:]),
		Operation:      models.TxOperationRollback,
		Status:         models.TxStatusCommitted,
	}))
}

// TestAuditRollbackRouteRealHit: a real HTTP POST to /api/v1/audit/rollback
// through the full JWT/RBAC middleware + registered route reaches the
// Rollback handler, whose boundary validation rejects an empty body with
// INVALID + CONFIG_VALIDATION_FAILED (zero RPC) — the handler is genuinely
// wired behind the middleware, not just named.
func TestAuditRollbackRouteRealHit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	w := postJSON(t, router, token, "/api/v1/audit/rollback", `{}`)
	require.Equal(t, http.StatusOK, w.Code, "a structured business failure is an HTTP 200 body")

	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
			ErrorCode    string `json:"error_code"`
			ErrorMessage string `json:"error_message"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_INVALID), out.Body.Status.Code)
	require.Equal(t, defs.ErrorCodeConfigValidationFailed, out.Body.ErrorCode)
}

// TestAuditRollbackRouteReachesCoordinator: a real HTTP POST with a valid
// request shape (transaction_id + UUID v4 idempotency key) travels through
// JWT/RBAC and the registered route INTO the rollback coordinator, which
// replays the seeded committed rollback transaction and answers SUCCESS
// with the same rollback transaction id. Only the coordinator can produce
// that id — the route is proven wired end-to-end.
func TestAuditRollbackRouteReachesCoordinator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appInst, token := newRouteEnv(t)
	router := gin.New()
	ConfigureRouter(appInst, router)

	const (
		sourceTxID = "tx-src-route"
		rbTxID     = "rb-tx-route"
	)
	idem := uuid.NewString()
	seedRouteRollbackReplay(t, appInst, idem, sourceTxID, rbTxID)

	body, err := json.Marshal(rollbackRouteBody{TransactionID: sourceTxID, IdempotencyKey: idem})
	require.NoError(t, err)
	w := postJSON(t, router, token, "/api/v1/audit/rollback", string(body))
	require.Equal(t, http.StatusOK, w.Code)

	var out struct {
		Code int `json:"code"`
		Body struct {
			Status struct {
				Code int `json:"code"`
			} `json:"status"`
			NewTransactionId string `json:"new_transaction_id"`
		} `json:"body"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 200, out.Code)
	require.Equal(t, int(pb.RespCode_RESP_CODE_SUCCESS), out.Body.Status.Code)
	require.Equal(t, rbTxID, out.Body.NewTransactionId,
		"the coordinator's committed replay returns the SAME rollback transaction id")
}
