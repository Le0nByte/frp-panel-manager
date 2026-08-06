package worker

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// captureHook records every log entry the global logger emits, so the test
// can assert on the RENDERED text of real InstallWorkerd log lines.
type captureHook struct {
	mu      sync.Mutex
	entries []*logrus.Entry
}

func (h *captureHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *captureHook) Fire(e *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, e)
	return nil
}

func (h *captureHook) text() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	for _, e := range h.entries {
		s, _ := e.String()
		b.WriteString(s)
		b.WriteByte('\n')
	}
	return b.String()
}

// installWorkerdTestApp builds an app.Context backed by an in-memory SQLite
// database (no clients table): InstallWorkerd runs its real DAO path, which
// fails with a lookup error instead of panicking — the redaction assertion
// only needs the first log line, which is emitted before any DAO access.
func installWorkerdTestApp(t *testing.T, ctxValues map[any]any) *app.Context {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:install-workerd-test?mode=memory&cache=shared"), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	require.NoError(t, err)

	dbm := models.NewDBManager(defs.DBTypeSQLite3)
	dbm.SetDB(defs.DBTypeSQLite3, defs.DBRoleDefault, db)

	appInst := app.NewApp()
	appInst.SetDBManager(dbm)

	base := context.Background()
	for k, v := range ctxValues {
		base = context.WithValue(base, k, v)
	}
	return app.NewContext(base, appInst)
}

// TestInstallWorkerdLogRedactsSecrets: the "install workerd called with
// userInfo" log line must never render the user's Password or Token. A
// sentinel-filled UserEntity is placed in the context; every log entry
// emitted while the real handler runs is captured and scanned.
func TestInstallWorkerdLogRedactsSecrets(t *testing.T) {
	const (
		sentinelPassword = "sentinel-password-9f3c2a"
		sentinelToken    = "sentinel-token-7a1b4d"
	)

	user := &models.UserEntity{
		UserID:   7,
		UserName: "redact-me",
		Email:    "redact@example.com",
		Password: sentinelPassword,
		Token:    sentinelToken,
	}
	ctx := installWorkerdTestApp(t, map[any]any{defs.UserInfoKey: user})

	hook := &captureHook{}
	logger.LoggerInstance.AddHook(hook)
	defer logger.LoggerInstance.ReplaceHooks(logrus.LevelHooks{})

	clientID := "cli-redact-test"
	_, err := InstallWorkerd(ctx, &pb.InstallWorkerdRequest{ClientId: &clientID})
	// The client row does not exist: the legacy DAO lookup must fail with an
	// error (not panic) — proving the real handler ran its course.
	require.Error(t, err, "client row must not exist in the fresh test DB")

	logs := hook.text()
	require.NotEmpty(t, logs, "the install-workerd log line must have been emitted")
	require.NotContains(t, logs, sentinelPassword, "password must never appear in install-workerd logs")
	require.NotContains(t, logs, sentinelToken, "token must never appear in install-workerd logs")
	// The safe user identity must still be visible for operability.
	require.Contains(t, logs, "redact-me", "safe user name should remain in the log line")
}
