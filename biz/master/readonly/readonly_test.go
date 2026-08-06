package readonly

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var fileCounter uint64

// tmpDir returns the project-local .tmp test directory
// (D:\frp-panel-manager\.tmp), creating it if needed. Tests must never
// write to C:/tmp.
func tmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "repo root not found above %s", dir)
		dir = parent
	}
	td := filepath.Join(dir, ".tmp")
	require.NoError(t, os.MkdirAll(td, 0o755))
	return td
}

// uniqueFile returns a unique path under the project .tmp dir. The name is
// a monotonic counter + the test name, NOT the *testing.T pointer: Go
// reuses T addresses across tests, so a pointer-based name can collide
// with a still-alive file from the previous test.
func uniqueFile(t *testing.T, ext string) string {
	t.Helper()
	n := atomic.AddUint64(&fileCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return filepath.Join(tmpDir(t), fmt.Sprintf("readonly-%d-%s.%s", n, name, ext))
}

// setupDB opens a real file-backed SQLite database with a unique DSN and
// cleans the file up on test end. busy_timeout makes concurrent TOFU
// writers wait for the winner instead of failing with SQLITE_BUSY.
func setupDB(t *testing.T) *gorm.DB {
	t.Helper()
	path := uniqueFile(t, "db")
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.SystemMeta{}, &models.ConfigSnapshot{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		_ = os.Remove(path)
		_ = os.Remove(path + "-wal")
		_ = os.Remove(path + "-shm")
	})
	return db
}

// writeKeyFile writes a snapshot key file under .tmp and removes it on
// test end.
func writeKeyFile(t *testing.T, key []byte) string {
	t.Helper()
	path := uniqueFile(t, "key")
	require.NoError(t, os.WriteFile(path, key, 0o600))
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

func randomKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, snapshot.KeySize)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

// seedSnapshots inserts n encrypted config_snapshot rows.
func seedSnapshots(t *testing.T, db *gorm.DB, key []byte, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		plaintext := []byte(fmt.Sprintf("rule-%05d-%s", i, strings.Repeat("x", 50)))
		ct, nonce, sum, err := snapshot.EncryptSnapshot(plaintext, key)
		require.NoError(t, err)
		require.NoError(t, db.Create(&models.ConfigSnapshot{
			TransactionID: fmt.Sprintf("tx-%05d", i),
			TenantID:      1,
			Side:          "before",
			NodeType:      "proxy",
			NodeID:        fmt.Sprintf("node-%05d", i),
			Ciphertext:    ct,
			Nonce:         nonce,
			SHA256:        sum,
		}).Error)
	}
}

func metaRow(t *testing.T, db *gorm.DB) (models.SystemMeta, error) {
	t.Helper()
	var meta models.SystemMeta
	err := db.First(&meta, "key = ?", models.SnapshotKeyFingerprintMetaKey).Error
	return meta, err
}

func TestReadonlySingletonState(t *testing.T) {
	ResetForTest()
	t.Cleanup(ResetForTest)

	assert.False(t, Blocked())
	assert.Equal(t, "", Reason())

	Set(true, "snapshot key mismatch")
	assert.True(t, Blocked())
	assert.Equal(t, "snapshot key mismatch", Reason())

	ResetForTest()
	assert.False(t, Blocked())
	assert.Equal(t, "", Reason())
	assert.Error(t, ErrReadOnly)
}

func TestSnapshotKeyFingerprintRecorded(t *testing.T) {
	db := setupDB(t)

	recorded, err := SnapshotKeyFingerprintRecorded(db)
	require.NoError(t, err)
	assert.False(t, recorded, "a fresh database has no fingerprint row yet")

	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)
	_, _, err = VerifySnapshotKey(db, pathA)
	require.NoError(t, err)

	recorded, err = SnapshotKeyFingerprintRecorded(db)
	require.NoError(t, err)
	assert.True(t, recorded, "after TOFU the fingerprint row must exist")

	// DB failures surface as errors (never silently "not recorded").
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	_, err = SnapshotKeyFingerprintRecorded(db)
	require.Error(t, err)
}

func TestVerifySnapshotKeyBytesPinsTheExactLoadedKey(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	keyB := randomKey(t)
	require.NotEqual(t, keyA, keyB)

	blocked, reason, err := VerifySnapshotKeyBytes(db, keyA)
	require.NoError(t, err)
	assert.False(t, blocked, reason)

	blocked, reason, err = VerifySnapshotKeyBytes(db, keyB)
	require.NoError(t, err)
	assert.True(t, blocked)
	assert.NotEmpty(t, reason)

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)
}

func TestVerifySnapshotKeyTOFUNoSnapshots(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)

	blocked, reason, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)
	assert.False(t, blocked, "first sight of a valid key with no history must be writable")
	assert.Empty(t, reason)

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)
	assert.Equal(t, "system_metadata", (&models.SystemMeta{}).TableName())
}

func TestVerifySnapshotKeySameKeyRestart(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)

	_, _, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)

	blocked, _, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)
	assert.False(t, blocked, "the same key on restart must stay writable")

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)
}

func TestVerifySnapshotKeyWrongKeyBlockedRowUnchanged(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	keyB := randomKey(t)
	require.NotEqual(t, keyA, keyB)
	pathA := writeKeyFile(t, keyA)
	pathB := writeKeyFile(t, keyB)

	_, _, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)

	blocked, reason, err := VerifySnapshotKey(db, pathB)
	require.NoError(t, err)
	assert.True(t, blocked)
	assert.NotEmpty(t, reason)
	assert.NotContains(t, reason, snapshot.Fingerprint(keyA))
	assert.NotContains(t, reason, snapshot.Fingerprint(keyB))

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value,
		"a mismatched key must never overwrite the stored fingerprint")
}

func TestVerifySnapshotKeyMissingFileBlockedNoTOFU(t *testing.T) {
	db := setupDB(t)
	missing := uniqueFile(t, "key")
	_, err := os.Stat(missing)
	require.True(t, os.IsNotExist(err), "fixture path must not exist")

	blocked, reason, err := VerifySnapshotKey(db, missing)
	require.NoError(t, err, "a missing key blocks read-only; it is not a startup error")
	assert.True(t, blocked)
	assert.NotEmpty(t, reason)

	_, err = metaRow(t, db)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "a missing key must never be TOFU'd into metadata")
}

func TestVerifySnapshotKeyBadSizeBlockedNoTOFU(t *testing.T) {
	db := setupDB(t)
	bad := writeKeyFile(t, make([]byte, 16)) // wrong size, not 32 bytes

	blocked, reason, err := VerifySnapshotKey(db, bad)
	require.NoError(t, err)
	assert.True(t, blocked)
	assert.NotEmpty(t, reason)

	_, err = metaRow(t, db)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "a bad-size key must never be TOFU'd into metadata")
}

func TestVerifySnapshotKeyBackfillsFingerprintAfterHistoricalVerify(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)
	// More than one FindInBatches page (verifyBatchSize=200): the whole
	// history must be verified across batch boundaries before backfilling.
	seedSnapshots(t, db, keyA, verifyBatchSize+5)

	blocked, reason, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)
	assert.False(t, blocked, reason)
	assert.Empty(t, reason)

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)

	var total int64
	require.NoError(t, db.Model(&models.ConfigSnapshot{}).Count(&total).Error)
	assert.Equal(t, int64(verifyBatchSize+5), total, "verification must not alter snapshot rows")
}

func TestVerifySnapshotKeyHistoricalWrongKeyBlockedNoBackfill(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	keyB := randomKey(t)
	require.NotEqual(t, keyA, keyB)
	pathB := writeKeyFile(t, keyB)
	seedSnapshots(t, db, keyA, 3)

	blocked, reason, err := VerifySnapshotKey(db, pathB)
	require.NoError(t, err)
	assert.True(t, blocked, "a key that cannot decrypt history must block")
	assert.NotEmpty(t, reason)
	assert.NotContains(t, reason, snapshot.Fingerprint(keyB))

	_, err = metaRow(t, db)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound,
		"failed historical verification must not backfill the fingerprint")
}

func TestVerifySnapshotKeyHistoricalTamperedSHA256BlockedNoBackfill(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)
	seedSnapshots(t, db, keyA, 3)

	// Tamper the stored SHA-256 of one row: decryption succeeds but the
	// plaintext hash no longer matches the recorded evidence.
	require.NoError(t, db.Model(&models.ConfigSnapshot{}).
		Where("transaction_id = ?", "tx-00001").
		Update("sha256", strings.Repeat("0", 64)).Error)

	blocked, reason, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)
	assert.True(t, blocked, "a tampered snapshot must block verification")
	assert.NotEmpty(t, reason)

	_, err = metaRow(t, db)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound,
		"failed integrity verification must not backfill the fingerprint")
}

func TestVerifySnapshotKeyConcurrentTOFUSingleRowSingleFingerprint(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)

	const n = 16
	results := make([]struct {
		blocked bool
		reason  string
		err     error
	}, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].blocked, results[i].reason, results[i].err = VerifySnapshotKey(db, pathA)
		}(i)
	}
	wg.Wait()

	for i := range results {
		require.NoError(t, results[i].err)
		assert.False(t, results[i].blocked, results[i].reason,
			"a same-key TOFU race must be writable for every goroutine")
	}

	var count int64
	require.NoError(t, db.Model(&models.SystemMeta{}).Count(&count).Error)
	assert.Equal(t, int64(1), count, "a TOFU race must converge to exactly one metadata row")
	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value,
		"the final state must hold exactly one fingerprint")
}

func TestVerifySnapshotKeyConcurrentTOFUTwoKeysSingleWinner(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	keyB := randomKey(t)
	require.NotEqual(t, keyA, keyB)
	pathA := writeKeyFile(t, keyA)
	pathB := writeKeyFile(t, keyB)
	fpA := snapshot.Fingerprint(keyA)
	fpB := snapshot.Fingerprint(keyB)

	const n = 16
	type result struct {
		fp      string
		blocked bool
		err     error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path, fp := pathA, fpA
			if i%2 == 1 {
				path, fp = pathB, fpB
			}
			results[i].fp = fp
			results[i].blocked, _, results[i].err = VerifySnapshotKey(db, path)
		}(i)
	}
	wg.Wait()

	var count int64
	require.NoError(t, db.Model(&models.SystemMeta{}).Count(&count).Error)
	require.Equal(t, int64(1), count, "a two-key TOFU race must converge to exactly one row")

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	require.True(t, meta.Value == fpA || meta.Value == fpB,
		"the stored fingerprint must be one of the racing keys")

	for i := range results {
		require.NoError(t, results[i].err)
		if results[i].fp == meta.Value {
			assert.False(t, results[i].blocked, "the winning-key goroutine must be writable")
		} else {
			assert.True(t, results[i].blocked, "the losing-key goroutine must be blocked")
		}
	}
}

func TestVerifySnapshotKeySwitchBackAndForth(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	keyB := randomKey(t)
	require.NotEqual(t, keyA, keyB)
	pathA := writeKeyFile(t, keyA)
	pathB := writeKeyFile(t, keyB)

	blocked, _, err := VerifySnapshotKey(db, pathA)
	require.NoError(t, err)
	assert.False(t, blocked)

	blocked, _, err = VerifySnapshotKey(db, pathB)
	require.NoError(t, err)
	assert.True(t, blocked)

	blocked, _, err = VerifySnapshotKey(db, pathA)
	require.NoError(t, err)
	assert.False(t, blocked, "the original key must remain writable after a wrong-key attempt")

	meta, err := metaRow(t, db)
	require.NoError(t, err)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)
}

func TestVerifySnapshotKeyDBErrorReturnsErr(t *testing.T) {
	db := setupDB(t)
	keyA := randomKey(t)
	pathA := writeKeyFile(t, keyA)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	blocked, _, err := VerifySnapshotKey(db, pathA)
	assert.True(t, blocked, "DB failures must still fail closed")
	require.Error(t, err, "DB failures must surface as errors so the startup layer can abort")
	assert.NotContains(t, err.Error(), string(keyA), "errors must never carry key material")
}
