package dao_test

import (
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLogAndList(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()

	m := dao.NewMutation(ctx)
	require.NoError(t, m.Log(user, &models.AuditEvent{
		TransactionID: "tx-a1",
		EventType:     "proxy.create",
		Summary:       "create tcp 8000 -> 127.0.0.1:22 ok",
		NodeID:        "aliyun-1",
		Success:       true,
	}))
	require.NoError(t, m.Log(user, &models.AuditEvent{
		TransactionID: "tx-a2",
		EventType:     "proxy.create",
		Summary:       "create tcp 8000 -> 127.0.0.1:22 conflict",
		NodeID:        "aliyun-1",
		ErrorCode:     "PORT_REMOTE_IN_USE",
		Success:       false,
	}))
	require.NoError(t, m.Log(user, &models.AuditEvent{
		TransactionID: "tx-a3",
		EventType:     "rollback",
		Summary:       "rollback tx-a1",
		Success:       true,
	}))

	events, total, err := dao.NewQuery(ctx).List(user, 1, 10, "")
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, events, 3)

	// filter by transaction
	events, total, err = dao.NewQuery(ctx).List(user, 1, 10, "tx-a1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, "tx-a1", events[0].TransactionID)

	// pagination
	events, total, err = dao.NewQuery(ctx).List(user, 2, 2, "")
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, events, 1)
}
