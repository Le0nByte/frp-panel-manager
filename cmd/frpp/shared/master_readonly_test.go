package shared

// I1-C Master startup wiring tests: snapshot-key fingerprint verification
// BEFORE recovery/listeners, the TOFU warning, and the read-only recovery
// degradation rule. Real SQLite (SystemMeta + ConfigSnapshot migration)
// and key files under D:\frp-panel-manager\.tmp — no real listeners or
// servers are started.

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/biz/master/proxy"
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/VaalaCat/frp-panel/utils/snapshot"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var masterTestFileCounter uint64

// masterTmpDir returns the project-local .tmp test directory
// (D:\frp-panel-manager\.tmp), creating it if needed.
func masterTmpDir(t *testing.T) string {
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

func masterUniqueFile(t *testing.T, ext string) string {
	t.Helper()
	n := atomic.AddUint64(&masterTestFileCounter, 1)
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return filepath.Join(masterTmpDir(t), fmt.Sprintf("master-%d-%s.%s", n, name, ext))
}

func masterSetupDB(t *testing.T) *gorm.DB {
	t.Helper()
	path := masterUniqueFile(t, "db")
	dsn := "file:" + filepath.ToSlash(path) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
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

func masterWriteKeyFile(t *testing.T, key []byte) string {
	t.Helper()
	path := masterUniqueFile(t, "key")
	require.NoError(t, os.WriteFile(path, key, 0o600))
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

func masterRandomKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, snapshot.KeySize)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

// masterHarness builds an app.Application whose config points at the key
// file and whose DBManager serves the given database — the same wiring
// applySnapshotReadOnlyMode consumes in runMaster.
func masterHarness(t *testing.T, keyPath string, dbm app.DBManager) (*app.Context, app.Application) {
	t.Helper()
	appInst := app.NewApp()
	cfg := conf.Config{}
	cfg.Master.SnapshotKeyFile = keyPath
	appInst.SetConfig(cfg)
	appInst.SetDBManager(dbm)
	return app.NewContext(context.Background(), appInst), appInst
}

// masterDBManager wraps a *gorm.DB in a minimal app.DBManager, or returns
// a manager whose GetDefaultDB is nil when withDB is false.
func masterDBManager(db *gorm.DB, withDB bool) app.DBManager {
	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	if withDB {
		dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)
	}
	return dbm
}

// recoveryCallRecorder proves the startup ordering: it records whether
// read-only mode was already active when the recovery pass ran, and can
// be scripted to fail so both degradation paths are testable.
type recoveryCallRecorder struct {
	mu            sync.Mutex
	blockedAtCall bool
	calls         int
	fail          bool
}

func (r *recoveryCallRecorder) RunOnce(_ *app.Context, _ time.Time) (*dao.SystemRecoveryResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.blockedAtCall = readonly.Blocked()
	if r.fail {
		return nil, fmt.Errorf("injected startup recovery failure")
	}
	return &dao.SystemRecoveryResult{}, nil
}

func (r *recoveryCallRecorder) observed() (calls int, blockedAtCall bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.blockedAtCall
}

func TestApplySnapshotReadOnlyModeValidFirstKeyWritableAndRecorded(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	db := masterSetupDB(t)
	keyA := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	dbm := masterDBManager(db, true)
	ctx, appInst := masterHarness(t, pathA, dbm)

	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))
	assert.False(t, readonly.Blocked(), "a valid first key must leave the master writable")
	assert.Equal(t, "", readonly.Reason())

	var meta models.SystemMeta
	require.NoError(t, db.First(&meta, "key = ?", models.SnapshotKeyFingerprintMetaKey).Error)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)
}

func TestApplySnapshotReadOnlyModeSameKeyRestartWritable(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	db := masterSetupDB(t)
	keyA := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	dbm := masterDBManager(db, true)

	ctx, appInst := masterHarness(t, pathA, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))

	// restart: same process state must end up writable again
	readonly.ResetForTest()
	ctx, appInst = masterHarness(t, pathA, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))
	assert.False(t, readonly.Blocked(), "the same key on restart must stay writable")
	assert.Equal(t, "", readonly.Reason())
}

func TestApplySnapshotReadOnlyModeWrongKeyBlockedNoErr(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	db := masterSetupDB(t)
	keyA := masterRandomKey(t)
	keyB := masterRandomKey(t)
	require.NotEqual(t, keyA, keyB)
	pathA := masterWriteKeyFile(t, keyA)
	pathB := masterWriteKeyFile(t, keyB)
	dbm := masterDBManager(db, true)

	ctx, appInst := masterHarness(t, pathA, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))

	readonly.ResetForTest()
	ctx, appInst = masterHarness(t, pathB, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm),
		"a mismatched key blocks read-only; it is not a startup error")
	assert.True(t, readonly.Blocked())
	reason := readonly.Reason()
	assert.NotEmpty(t, reason)
	assert.NotContains(t, reason, snapshot.Fingerprint(keyA))
	assert.NotContains(t, reason, snapshot.Fingerprint(keyB))
	assert.NotContains(t, reason, fmt.Sprintf("%x", keyA))
	assert.NotContains(t, reason, fmt.Sprintf("%x", keyB))

	// the recorded fingerprint must stay the FIRST key's
	var meta models.SystemMeta
	require.NoError(t, db.First(&meta, "key = ?", models.SnapshotKeyFingerprintMetaKey).Error)
	assert.Equal(t, snapshot.Fingerprint(keyA), meta.Value)
}

func TestApplySnapshotReadOnlyModeMissingKeyBlockedNoErr(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	db := masterSetupDB(t)
	missing := masterUniqueFile(t, "key")
	_, err := os.Stat(missing)
	require.True(t, os.IsNotExist(err), "fixture key path must not exist")

	dbm := masterDBManager(db, true)
	ctx, appInst := masterHarness(t, missing, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm),
		"a missing key blocks read-only; it is not a startup error")
	assert.True(t, readonly.Blocked())
	assert.NotEmpty(t, readonly.Reason())

	recorded, err := readonly.SnapshotKeyFingerprintRecorded(db)
	require.NoError(t, err)
	assert.False(t, recorded, "a missing key must never be TOFU'd into metadata")
}

func TestApplySnapshotReadOnlyModeNilDBManagerErrorStaysBlocked(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	keyA := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	ctx, appInst := masterHarness(t, pathA, nil)

	require.Error(t, applySnapshotReadOnlyMode(ctx, appInst, nil),
		"a nil DB manager must abort startup, never pretend read-only is available")
	assert.True(t, readonly.Blocked(), "DB trouble must never leave the master unblocked")
}

func TestApplySnapshotReadOnlyModeNilDBErrorStaysBlocked(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	keyA := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	dbm := masterDBManager(nil, false) // GetDefaultDB() == nil

	ctx, appInst := masterHarness(t, pathA, dbm)
	require.Error(t, applySnapshotReadOnlyMode(ctx, appInst, dbm),
		"a nil default database must abort startup")
	assert.True(t, readonly.Blocked(), "DB trouble must never leave the master unblocked")
}

func TestApplySnapshotReadOnlyModeDBErrorStaysBlocked(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	db := masterSetupDB(t)
	keyA := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	dbm := masterDBManager(db, true)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	ctx, appInst := masterHarness(t, pathA, dbm)
	applyErr := applySnapshotReadOnlyMode(ctx, appInst, dbm)
	require.Error(t, applyErr, "a verification DB error must abort startup")
	assert.True(t, readonly.Blocked(), "DB trouble must never leave the master unblocked")
	assert.NotContains(t, applyErr.Error(), fmt.Sprintf("%x", keyA), "errors must never carry key material")
}

func TestApplySnapshotReadOnlyModeTOFUWarningFiresOnceAndHidesFingerprint(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	// Capture logrus entries for the duration of this test.
	lg := logger.Instance()
	oldOut := lg.Out
	hook := test.NewLocal(lg)
	defer func() { lg.Out = oldOut }()

	db := masterSetupDB(t)
	keyA := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	dbm := masterDBManager(db, true)

	// First call: no fingerprint recorded before → TOFU warning fires.
	ctx, appInst := masterHarness(t, pathA, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))
	require.False(t, readonly.Blocked())

	var tofuEntries int
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "TOFU") {
			tofuEntries++
			assert.NotContains(t, e.Message, snapshot.Fingerprint(keyA), "TOFU warning must never print the fingerprint")
			assert.NotContains(t, e.Message, fmt.Sprintf("%x", keyA))
		}
	}
	assert.Equal(t, 1, tofuEntries, "the first-recorded startup must emit exactly one TOFU warning")

	// Restart: fingerprint already recorded → no second warning.
	hook.Reset()
	readonly.ResetForTest()
	ctx, appInst = masterHarness(t, pathA, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))
	require.False(t, readonly.Blocked())
	for _, e := range hook.AllEntries() {
		assert.NotContains(t, e.Message, "TOFU", "a restart against existing metadata must not warn again")
	}
}

func TestStartupOrderingKeyMismatchBlockedBeforeRecovery(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	db := masterSetupDB(t)
	keyA := masterRandomKey(t)
	keyB := masterRandomKey(t)
	pathA := masterWriteKeyFile(t, keyA)
	pathB := masterWriteKeyFile(t, keyB)
	dbm := masterDBManager(db, true)

	// First startup with keyA → writable, fingerprint recorded.
	ctx, appInst := masterHarness(t, pathA, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))
	require.False(t, readonly.Blocked())

	// Second startup with keyB → blocked BEFORE recovery runs.
	readonly.ResetForTest()
	ctx, appInst = masterHarness(t, pathB, dbm)
	require.NoError(t, applySnapshotReadOnlyMode(ctx, appInst, dbm))
	require.True(t, readonly.Blocked())

	// The recovery pass must observe the blocked state set by verification:
	// the recorder snapshots readonly.Blocked() inside RunOnce.
	rec := &recoveryCallRecorder{}
	recCtx, cancel := context.WithCancel(context.Background())
	sched := proxy.NewRecoveryScheduler(rec, time.Minute)
	done, err := startMasterRecovery(recCtx, app.NewContext(context.Background(), appInst), sched)
	require.NoError(t, err, "blocked + successful recovery keeps startup going")
	require.NotNil(t, done, "blocked + successful recovery keeps the periodic loop running")
	calls, blockedAtCall := rec.observed()
	require.Equal(t, 1, calls)
	assert.True(t, blockedAtCall, "recovery must run AFTER verification set the blocked state")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery loop did not stop after cancellation (goroutine leak)")
	}
}

func TestStartupRecoveryBlockedFailureDegradesToNilContinue(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	readonly.Set(true, "snapshot key mismatch (test fixture)")
	rec := &recoveryCallRecorder{fail: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done, err := startMasterRecovery(ctx, app.NewContext(context.Background(), app.NewApp()),
		proxy.NewRecoveryScheduler(rec, time.Minute))
	require.NoError(t, err, "in read-only mode a failed startup recovery must degrade, not abort")
	assert.Nil(t, done, "no periodic loop may run after a degraded startup pass")
	calls, blockedAtCall := rec.observed()
	require.Equal(t, 1, calls)
	assert.True(t, blockedAtCall, "the recovery pass must still observe the blocked state")
}

func TestStartupRecoveryWritableFailureAborts(t *testing.T) {
	readonly.ResetForTest()
	t.Cleanup(readonly.ResetForTest)

	require.False(t, readonly.Blocked(), "fixture must start writable")
	rec := &recoveryCallRecorder{fail: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done, err := startMasterRecovery(ctx, app.NewContext(context.Background(), app.NewApp()),
		proxy.NewRecoveryScheduler(rec, time.Minute))
	require.Error(t, err, "a writable master must abort startup when the recovery pass fails")
	assert.Nil(t, done)
	assert.Contains(t, err.Error(), "startup transaction/port recovery failed")
	calls, _ := rec.observed()
	require.Equal(t, 1, calls)
}
