package dao_test

import (
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSaveEncryptedRoundTrip(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	plaintext := []byte(`{"serverAddr":"47.109.18.109","auth":{"token":"[REDACTED]"}}`)

	snap, err := dao.NewMutation(ctx).SaveEncrypted(user, "tx-snap", models.SnapshotSideBefore, models.SnapshotNodeTypeClient, "k30pro", plaintext, key)
	require.NoError(t, err)
	assert.NotEmpty(t, snap.SHA256)
	assert.NotEmpty(t, snap.Ciphertext)
	assert.NotEmpty(t, snap.Nonce)
	// ciphertext must not contain the plaintext
	assert.NotContains(t, string(snap.Ciphertext), "47.109.18.109")

	loaded, err := dao.NewQuery(ctx).FindByTransaction(user, "tx-snap")
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, models.SnapshotSideBefore, loaded[0].Side)

	dec, err := dao.DecryptSnapshotForTest(loaded[0], key)
	require.NoError(t, err)
	assert.Equal(t, plaintext, dec)

	// wrong key must fail
	_, err = dao.DecryptSnapshotForTest(loaded[0], make([]byte, 32))
	require.Error(t, err)
}

func TestSnapshotTamperDetected(t *testing.T) {
	ctx := setupTestApp(t)
	user := testUserInfo()
	key := make([]byte, 32)

	_, err := dao.NewMutation(ctx).SaveEncrypted(user, "tx-snap2", models.SnapshotSideAfter, models.SnapshotNodeTypeServer, "aliyun-1", []byte("cfg"), key)
	require.NoError(t, err)
	loaded, err := dao.NewQuery(ctx).FindByTransaction(user, "tx-snap2")
	require.NoError(t, err)
	require.Len(t, loaded, 1)

	loaded[0].Ciphertext[0] ^= 0xff
	_, err = dao.DecryptSnapshotForTest(loaded[0], key)
	require.Error(t, err)
}
