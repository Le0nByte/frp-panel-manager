package dao_test

import (
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionCreateAndFind(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()

	tx := &models.ChangeTransaction{
		TransactionID:   "tx-abc",
		IdempotencyKey:  "idem-1",
		ActorUserID:     user.GetUserID(),
		Operation:       models.TxOperationCreate,
		Status:          models.TxStatusPending,
		RequestRedacted: `{"proxy_type":"tcp"}`,
		StartedAt:       now,
	}
	require.NoError(t, dao.NewMutation(ctx).Create(user, tx))

	loaded, err := dao.NewQuery(ctx).FindByID(user, "tx-abc")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusPending, loaded.Status)

	byKey, err := dao.NewQuery(ctx).FindByIdempotencyKey(user, "idem-1")
	require.NoError(t, err)
	assert.Equal(t, "tx-abc", byKey.TransactionID)
}

func TestIdempotencyKeyUnique(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	now := time.Now()

	base := func(txID string) *models.ChangeTransaction {
		return &models.ChangeTransaction{
			TransactionID:  txID,
			IdempotencyKey: "idem-same",
			ActorUserID:    user.GetUserID(),
			Operation:      models.TxOperationCreate,
			Status:         models.TxStatusPending,
			StartedAt:      now,
		}
	}
	require.NoError(t, dao.NewMutation(ctx).Create(user, base("tx-1")))
	require.Error(t, dao.NewMutation(ctx).Create(user, base("tx-2")))
}

func TestTransactionUpdateStatus(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()

	tx := &models.ChangeTransaction{
		TransactionID:  "tx-flow",
		IdempotencyKey: "idem-flow",
		ActorUserID:    user.GetUserID(),
		Operation:      models.TxOperationCreate,
		Status:         models.TxStatusPending,
		StartedAt:      time.Now(),
	}
	require.NoError(t, dao.NewMutation(ctx).Create(user, tx))

	m := dao.NewMutation(ctx)
	require.NoError(t, m.UpdateStatus(user, "tx-flow", models.TxStatusPreflight, "", ""))
	require.NoError(t, m.UpdateStatus(user, "tx-flow", models.TxStatusApplying, "", ""))
	require.NoError(t, m.UpdateStatus(user, "tx-flow", models.TxStatusVerifying, "", ""))
	require.NoError(t, m.UpdateStatus(user, "tx-flow", models.TxStatusCommitted, "", ""))

	loaded, err := dao.NewQuery(ctx).FindByID(user, "tx-flow")
	require.NoError(t, err)
	assert.Equal(t, models.TxStatusCommitted, loaded.Status)
	require.NotNil(t, loaded.FinishedAt, "terminal status must set finished_at")
}

func TestTransactionUpdateStatusWithError(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()

	tx := &models.ChangeTransaction{
		TransactionID:  "tx-err",
		IdempotencyKey: "idem-err",
		ActorUserID:    user.GetUserID(),
		Operation:      models.TxOperationCreate,
		Status:         models.TxStatusVerifying,
		StartedAt:      time.Now(),
	}
	require.NoError(t, dao.NewMutation(ctx).Create(user, tx))
	require.NoError(t, dao.NewMutation(ctx).UpdateStatus(user, "tx-err", models.TxStatusFailed, "VERIFY_FAILED", "health check failed"))

	loaded, err := dao.NewQuery(ctx).FindByID(user, "tx-err")
	require.NoError(t, err)
	assert.Equal(t, "VERIFY_FAILED", loaded.ErrorCode)
	assert.Equal(t, "health check failed", loaded.ErrorMessage)
}
