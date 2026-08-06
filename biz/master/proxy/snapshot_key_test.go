package proxy

// I1-B ConfigKeyProvider fingerprint re-verification: after reading the
// key file, the production provider re-verifies the key against the
// system_metadata fingerprint via the app-context default DB before
// handing the key out. A mismatched key file is rejected with
// readonly.ErrReadOnly (fail closed, never the key), DB failures are
// wrapped without key/fingerprint material, and the stored fingerprint
// row is never overwritten. MemoryKeyProvider (tests only) is untouched.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var snapshotKeyFileCounter uint64

// snapshotKeyTmpDir returns the project-local .tmp test directory (repo
// convention: tests never write to C:/tmp).
func snapshotKeyTmpDir(t *testing.T) string {
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

func snapshotKeyUniqueFile(t *testing.T, ext string) string {
	t.Helper()
	n := atomic.AddUint64(&snapshotKeyFileCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return filepath.Join(snapshotKeyTmpDir(t), fmt.Sprintf("snapkey-%d-%s.%s", n, name, ext))
}

// newSnapshotKeyEnv wires an app context whose default DB carries a
// seeded system_metadata fingerprint and whose config points at a key
// file, plus the file-backed sqlite handle.
func newSnapshotKeyEnv(t *testing.T, metaFP string, keyPath string) (*app.Context, *gorm.DB) {
	t.Helper()
	dbPath := snapshotKeyUniqueFile(t, "db")
	dsn := "file:" + filepath.ToSlash(dbPath) + "?_pragma=busy_timeout(5000)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.SystemMeta{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
		_ = os.Remove(dbPath + "-shm")
	})
	if metaFP != "" {
		require.NoError(t, db.Create(&models.SystemMeta{
			Key:   models.SnapshotKeyFingerprintMetaKey,
			Value: metaFP,
		}).Error)
	}

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	appInst := app.NewApp()
	appInst.SetDBManager(dbm)
	cfg := conf.Config{}
	cfg.Master.SnapshotKeyFile = keyPath
	appInst.SetConfig(cfg)
	return app.NewContext(context.Background(), appInst), db
}

func writeSnapshotKeyFile(t *testing.T, key []byte) string {
	t.Helper()
	path := snapshotKeyUniqueFile(t, "key")
	require.NoError(t, os.WriteFile(path, key, 0o600))
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

func TestConfigKeyProviderMetadataAFileBRejected(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	keyA := make([]byte, snapshot.KeySize)
	keyB := make([]byte, snapshot.KeySize)
	for i := range keyA {
		keyA[i] = byte('A')
		keyB[i] = byte('B')
	}
	pathB := writeSnapshotKeyFile(t, keyB)
	fpA := snapshot.Fingerprint(keyA)
	ctx, db := newSnapshotKeyEnv(t, fpA, pathB)

	_, err := (ConfigKeyProvider{}).SnapshotKey(ctx)
	require.ErrorIs(t, err, readonly.ErrReadOnly,
		"a key file whose fingerprint does not match the database must be rejected")

	var meta models.SystemMeta
	require.NoError(t, db.First(&meta, "key = ?", models.SnapshotKeyFingerprintMetaKey).Error)
	require.Equal(t, fpA, meta.Value,
		"a mismatched key must never overwrite the stored fingerprint")

	require.NotContains(t, err.Error(), fpA, "errors must not leak the stored fingerprint")
	require.NotContains(t, err.Error(), snapshot.Fingerprint(keyB), "errors must not leak the candidate fingerprint")
	require.NotContains(t, err.Error(), string(keyB), "errors must not leak key material")
}

func TestConfigKeyProviderMetadataAFileAAllowed(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	keyA := make([]byte, snapshot.KeySize)
	for i := range keyA {
		keyA[i] = byte('A')
	}
	pathA := writeSnapshotKeyFile(t, keyA)
	ctx, _ := newSnapshotKeyEnv(t, snapshot.Fingerprint(keyA), pathA)

	got, err := (ConfigKeyProvider{}).SnapshotKey(ctx)
	require.NoError(t, err)
	require.Equal(t, keyA, got, "a matching key must be handed out unchanged")
}

func TestConfigKeyProviderDBErrorWrappedWithoutLeak(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	keyA := make([]byte, snapshot.KeySize)
	for i := range keyA {
		keyA[i] = byte('A')
	}
	pathA := writeSnapshotKeyFile(t, keyA)
	ctx, db := newSnapshotKeyEnv(t, snapshot.Fingerprint(keyA), pathA)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = (ConfigKeyProvider{}).SnapshotKey(ctx)
	require.Error(t, err, "a DB failure must surface as an error so the caller can abort")
	require.NotErrorIs(t, err, readonly.ErrReadOnly,
		"a DB failure is an internal error, not the readonly sentinel")
	require.NotContains(t, err.Error(), snapshot.Fingerprint(keyA), "errors must not leak the fingerprint")
	require.NotContains(t, err.Error(), string(keyA), "errors must not leak key material")
}

func TestConfigKeyProviderMissingFileErrorsWithoutLeak(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	missing := snapshotKeyUniqueFile(t, "key")
	_, err := (ConfigKeyProvider{}).SnapshotKey(&app.Context{})
	require.Error(t, err, "a non-app context is rejected")
	require.NotErrorIs(t, err, readonly.ErrReadOnly)

	ctx, _ := newSnapshotKeyEnv(t, "", missing)
	_, err = (ConfigKeyProvider{}).SnapshotKey(ctx)
	require.Error(t, err, "a missing key file is a hard error (never a fallback)")
	require.NotErrorIs(t, err, readonly.ErrReadOnly)
	require.NotContains(t, err.Error(), "fingerprint", "file errors must not reference fingerprint material")
}

func TestConfigKeyProviderNoDBConfiguredErrors(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	keyA := make([]byte, snapshot.KeySize)
	for i := range keyA {
		keyA[i] = byte('A')
	}
	pathA := writeSnapshotKeyFile(t, keyA)
	cfg := conf.Config{}
	cfg.Master.SnapshotKeyFile = pathA
	appInst := app.NewApp()
	appInst.SetConfig(cfg)
	ctx := app.NewContext(context.Background(), appInst)

	_, err := (ConfigKeyProvider{}).SnapshotKey(ctx)
	require.Error(t, err, "without a DB the fingerprint cannot be verified — fail closed")
	require.NotErrorIs(t, err, readonly.ErrReadOnly)
}

// TestMemoryKeyProviderUnaffected pins that the test-only provider keeps
// its exact contract after the ConfigKeyProvider change.
func TestMemoryKeyProviderUnaffected(t *testing.T) {
	key := make([]byte, snapshot.KeySize)
	got, err := (&MemoryKeyProvider{Key: key}).SnapshotKey(context.Background())
	require.NoError(t, err)
	require.Equal(t, key, got)

	_, err = (&MemoryKeyProvider{Key: make([]byte, 16)}).SnapshotKey(context.Background())
	require.ErrorIs(t, err, snapshot.ErrInvalidKeySize)
}
