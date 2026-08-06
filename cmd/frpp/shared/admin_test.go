package shared

import (
	"embed"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/glebarez/sqlite"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---- real in-memory SQLite fixtures (no files, no secrets) ----

var adminTestDSNCounter uint64

func adminTestDSN(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("file:admin-test-%d-%s?mode=memory&cache=shared&_pragma=busy_timeout(3000)",
		atomic.AddUint64(&adminTestDSNCounter, 1), strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
}

func openAdminTestDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, db.AutoMigrate(&models.User{}, &models.AuditEvent{}))
	return db
}

// seedUser inserts a user with the given role; version != 1 forces the exact
// stored session version (0 = legacy pre-feature row).
func seedUser(t *testing.T, db *gorm.DB, userID int, name, role string, version int64) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("old-password-123"), bcrypt.MinCost)
	require.NoError(t, err)
	require.NoError(t, db.Create(&models.User{UserEntity: &models.UserEntity{
		UserID:   userID,
		UserName: name,
		Email:    name + "@example.com",
		Password: string(hash),
		Role:     role,
		TenantID: userID,
	}}).Error)
	if version != 1 {
		require.NoError(t, db.Model(&models.User{}).Where("user_id = ?", userID).Update("session_version", version).Error)
	}
}

// minCostHasher is a fast bcrypt hasher for tests (production uses cost 14).
func minCostHasher(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost)
	return string(b), err
}

// ---- resetPasswordInDB (real DB, real transaction) ----

// TestResetPasswordInDBHappyPath: the full reset — bcrypt hash verified,
// session version bumped 1 -> 2, exactly one audit row with the fixed
// marker-free summary and no password/token anywhere.
func TestResetPasswordInDBHappyPath(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "admin", defs.UserRole_Admin, 1)

	require.NoError(t, resetPasswordWithHasher(db, "admin", "new-password-123", minCostHasher))

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("new-password-123", u.Password), "stored hash must verify the new password")
	require.False(t, utils.CheckPasswordHash("old-password-123", u.Password), "old password must no longer verify")
	require.Equal(t, int64(2), u.SessionVersion, "reset must bump session version 1 -> 2 (invalidates old JWTs)")

	var audits []models.AuditEvent
	require.NoError(t, db.Find(&audits).Error)
	require.Len(t, audits, 1, "exactly one audit row")
	a := audits[0]
	require.Equal(t, models.AuditEventTypeResetPassword, a.EventType)
	require.True(t, a.Success)
	require.Equal(t, 1, a.TenantID)
	require.Equal(t, 1, a.ActorUserID)
	require.NotContains(t, strings.ToLower(a.Summary), "password", "audit summary must be marker-free")
	require.NotContains(t, strings.ToLower(a.Summary), "token", "audit summary must be marker-free")
	require.NotContains(t, strings.ToLower(a.Summary), "secret", "audit summary must be marker-free")
	require.NotContains(t, a.Summary, "new-password-123", "audit must never store the password")
}

// TestResetPasswordInDBVersionIncrements: 0 -> 2 (legacy), 1 -> 2, 2 -> 3,
// 5 -> 6. Every reset strictly bumps the effective version so ALL previous
// tokens (claimed or legacy) die.
func TestResetPasswordInDBVersionIncrements(t *testing.T) {
	for _, tc := range []struct {
		name string
		from int64
		want int64
	}{
		{"legacy zero row", 0, 2},
		{"version one", 1, 2},
		{"version two", 2, 3},
		{"version five", 5, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openAdminTestDB(t, adminTestDSN(t))
			seedUser(t, db, 1, "admin", defs.UserRole_Admin, tc.from)

			require.NoError(t, resetPasswordWithHasher(db, "admin", "new-password-123", minCostHasher))

			var u models.User
			require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
			require.Equal(t, tc.want, u.SessionVersion)
		})
	}
}

// TestResetPasswordInDBMissingUser: unknown username -> error, zero DB
// delta (password, version and audit trail untouched).
func TestResetPasswordInDBMissingUser(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "admin", defs.UserRole_Admin, 1)

	err := resetPasswordWithHasher(db, "nobody", "new-password-123", minCostHasher)
	require.Error(t, err)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("old-password-123", u.Password), "password must be untouched on failure")
	require.Equal(t, int64(1), u.SessionVersion, "version must be untouched on failure")

	var count int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&count).Error)
	require.Zero(t, count, "failed reset must not write an audit row")
}

func TestResetPasswordInDBRejectsNonAdmin(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "operator", defs.UserRole_Normal, 1)

	err := resetPasswordWithHasher(db, "operator", "new-password-123", minCostHasher)
	require.Error(t, err)
	require.Contains(t, err.Error(), "admin")

	var user models.User
	require.NoError(t, db.First(&user, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("old-password-123", user.Password))
	require.Equal(t, int64(1), user.SessionVersion)
	var count int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&count).Error)
	require.Zero(t, count)
}

// TestResetPasswordInDBRollsBackWhenAuditFails: the password + session
// version UPDATE is only durable if the audit row lands too. A real SQLite
// trigger (DB mechanism, not a mock) aborts the audit INSERT — the only
// write after the UPDATE — and the whole transaction must roll back: the OLD
// password still verifies, the session version is untouched and no audit row
// exists.
func TestResetPasswordInDBRollsBackWhenAuditFails(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "admin", defs.UserRole_Admin, 1)

	// BEFORE INSERT trigger on audit_event: every audit insert raises ABORT
	// with a distinctive message, exactly like a constraint violation.
	require.NoError(t, db.Exec(`CREATE TRIGGER fail_audit_insert
		BEFORE INSERT ON audit_event
		BEGIN
			SELECT RAISE(ABORT, 'injected audit insert failure');
		END`).Error)

	err := resetPasswordWithHasher(db, "admin", "new-password-123", minCostHasher)
	require.Error(t, err, "audit insert failure must fail the reset")
	require.Contains(t, err.Error(), "injected audit insert failure")

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("old-password-123", u.Password),
		"old password must still verify after the audit failure")
	require.False(t, utils.CheckPasswordHash("new-password-123", u.Password),
		"new password must be rolled back with the transaction")
	require.Equal(t, int64(1), u.SessionVersion, "session version must be untouched after the audit failure")

	var count int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&count).Error)
	require.Zero(t, count, "no audit row may survive a rolled-back reset")
}

// TestResetPasswordInDBRejectsWeakPassword: empty, <12 and >72 byte
// passwords are rejected (bcrypt hard cap) and nothing changes. The error
// never echoes the rejected password.
func TestResetPasswordInDBRejectsWeakPassword(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "admin", defs.UserRole_Admin, 1)

	for _, pw := range []string{"", "tiny", "elevenchars", "12345678901"} {
		err := resetPasswordWithHasher(db, "admin", pw, minCostHasher)
		require.Error(t, err, "password %q must be rejected", pw)
		if pw != "" {
			require.NotContains(t, err.Error(), pw, "rejection error must not echo the password")
		}
	}
	// 73 bytes is one over the bcrypt limit and must be rejected by the
	// policy layer (before hashing); 256 is far over and also rejected.
	for _, pw := range []string{strings.Repeat("a", 73), strings.Repeat("a", 256)} {
		err := resetPasswordWithHasher(db, "admin", pw, minCostHasher)
		require.Error(t, err, "%d-byte password must be rejected", len(pw))
		require.NotContains(t, err.Error(), strings.Repeat("a", 8), "rejection error must not echo the password")
	}

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("old-password-123", u.Password), "password must be untouched after rejections")
	require.Equal(t, int64(1), u.SessionVersion)

	var count int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&count).Error)
	require.Zero(t, count)
}

// TestResetPasswordInDBRealHasherPath: the exported entry point uses the
// production bcrypt hasher (utils.HashPassword) end to end.
func TestResetPasswordInDBRealHasherPath(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "admin", defs.UserRole_Admin, 1)

	require.NoError(t, resetPasswordInDB(db, "admin", "new-password-123"))

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("new-password-123", u.Password))
	require.Equal(t, int64(2), u.SessionVersion)
}

// ---- admin resolution ----

// TestResolveAdminUsernamePicksMinUserID: several admins + a normal user ->
// the admin with the smallest user id wins (deterministic).
func TestResolveAdminUsernamePicksMinUserID(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "boss", defs.UserRole_Admin, 1)
	seedUser(t, db, 2, "second", defs.UserRole_Admin, 1)
	seedUser(t, db, 3, "user1", defs.UserRole_Normal, 1)

	name, err := resolveAdminUsername(db)
	require.NoError(t, err)
	require.Equal(t, "boss", name)
}

// TestResolveAdminUsernameNoAdmin: no admin user -> explicit error.
func TestResolveAdminUsernameNoAdmin(t *testing.T) {
	db := openAdminTestDB(t, adminTestDSN(t))
	seedUser(t, db, 1, "user1", defs.UserRole_Normal, 1)

	_, err := resolveAdminUsername(db)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no admin")
}

// ---- password input policy ----

func TestValidatePassword(t *testing.T) {
	require.Error(t, validatePassword(""))
	require.Error(t, validatePassword("12345678901"))    // 11
	require.NoError(t, validatePassword("123456789012")) // 12
	require.NoError(t, validatePassword(strings.Repeat("a", 72)))  // 72 = bcrypt cap, allowed
	require.Error(t, validatePassword(strings.Repeat("a", 73)))    // 73 > bcrypt cap, rejected
	require.Error(t, validatePassword(strings.Repeat("a", 256)))   // 256 far over, rejected

	// rejection errors never echo the password
	require.NotContains(t, validatePassword(strings.Repeat("a", 73)).Error(), strings.Repeat("a", 8))
	require.NotContains(t, validatePassword(strings.Repeat("a", 256)).Error(), strings.Repeat("a", 8))
}

// TestReadPasswordFromStdinBoundedRead: the stdin read is capped at
// maxResetPasswordLen+1 bytes, so a pathological pipe can never buffer an
// unbounded password before the policy layer rejects it (review Minor 3).
func TestReadPasswordFromStdinBoundedRead(t *testing.T) {
	// Far larger than any plausible password; with an unbounded read the
	// whole payload would be buffered, with the cap only maxResetPasswordLen+1
	// bytes survive.
	overLong := strings.Repeat("a", maxResetPasswordLen*4+3) + "\n"
	pw, err := readPasswordFromStdin(strings.NewReader(overLong))
	require.NoError(t, err)
	require.Len(t, pw, maxResetPasswordLen+1, "read must be capped at maxResetPasswordLen+1 bytes")
	require.Error(t, validatePassword(pw), "over-cap input must be rejected by the policy layer")
}

// TestReadPasswordFromStdinRejectsTTY: an interactive terminal (char
// device) must be rejected before any read — scripted pipe-only resets.
func TestReadPasswordFromStdinRejectsTTY(t *testing.T) {
	f, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer f.Close()

	_, err = readPasswordFromStdin(f)
	require.Error(t, err)
	require.Contains(t, err.Error(), "interactive terminal")
}

// TestReadPasswordFromStdinTrimsTrailingNewline: piped input keeps its
// content minus one trailing CRLF/LF.
func TestReadPasswordFromStdinTrimsTrailingNewline(t *testing.T) {
	pw, err := readPasswordFromStdin(strings.NewReader("new-password-123\n"))
	require.NoError(t, err)
	require.Equal(t, "new-password-123", pw)

	pw, err = readPasswordFromStdin(strings.NewReader("new-password-123\r\n"))
	require.NoError(t, err)
	require.Equal(t, "new-password-123", pw)
}

// TestReadPasswordFromStdinEmpty: empty stdin yields an empty password which
// the policy layer rejects.
func TestReadPasswordFromStdinEmpty(t *testing.T) {
	pw, err := readPasswordFromStdin(strings.NewReader(""))
	require.NoError(t, err)
	require.Empty(t, pw)
	require.Error(t, validatePassword(pw))
}

// ---- CLI command surface ----

// TestNewAdminCmdStructure: `admin` is a cobra CLI tree (no gin router, no
// HTTP surface) with the reset-password subcommand; --username is optional.
func TestNewAdminCmdStructure(t *testing.T) {
	adminCmd := NewAdminCmd(conf.Config{})
	_, isCobra := interface{}(adminCmd).(*cobra.Command)
	require.True(t, isCobra, "admin surface must be a cobra CLI, not an HTTP router")
	require.Equal(t, "admin", adminCmd.Use)

	resetCmd, _, err := adminCmd.Find([]string{"reset-password"})
	require.NoError(t, err)
	require.NotNil(t, resetCmd)
	require.Equal(t, "reset-password", resetCmd.Use)

	flag := resetCmd.Flags().Lookup("password-stdin")
	require.NotNil(t, flag, "--password-stdin must exist")
	require.True(t, flag.DefValue == "false" && flag.Value.Type() == "bool")
	require.NotNil(t, resetCmd.Flags().Lookup("username"), "--username must exist")
}

// TestResetPasswordCommandRequiresPasswordStdinFlag: running reset-password
// without --password-stdin fails at the flag layer (cobra required flag).
func TestResetPasswordCommandRequiresPasswordStdinFlag(t *testing.T) {
	cmd := NewAdminCmd(conf.Config{})
	cmd.SetArgs([]string{"reset-password"})
	cmd.SetIn(strings.NewReader("new-password-123\n"))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "password-stdin")
}

// TestResetPasswordCommandEndToEnd: real CLI execution against a real
// in-memory SQLite DB via openLocalDB — password updated, version bumped,
// audit row written, old password dead.
func TestResetPasswordCommandEndToEnd(t *testing.T) {
	dsn := adminTestDSN(t)
	db := openAdminTestDB(t, dsn)
	seedUser(t, db, 1, "admin", defs.UserRole_Admin, 1)

	cfg := conf.Config{}
	cfg.DB.Type = defs.DBTypeSQLite3
	cfg.DB.DSN = dsn

	cmd := NewAdminCmd(cfg)
	cmd.SetArgs([]string{"reset-password", "--password-stdin", "--username", "admin"})
	cmd.SetIn(strings.NewReader("brand-new-password\n"))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	require.NoError(t, cmd.Execute())

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("brand-new-password", u.Password))
	require.False(t, utils.CheckPasswordHash("old-password-123", u.Password))
	require.Equal(t, int64(2), u.SessionVersion)

	var count int64
	require.NoError(t, db.Model(&models.AuditEvent{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
}

// TestResetPasswordCommandResolvesAdminWithoutUsername: omitting --username
// resolves the minimum-user-id admin (here the only admin).
func TestResetPasswordCommandResolvesAdminWithoutUsername(t *testing.T) {
	dsn := adminTestDSN(t)
	db := openAdminTestDB(t, dsn)
	seedUser(t, db, 1, "boss", defs.UserRole_Admin, 1)
	seedUser(t, db, 2, "user1", defs.UserRole_Normal, 1)

	cfg := conf.Config{}
	cfg.DB.Type = defs.DBTypeSQLite3
	cfg.DB.DSN = dsn

	cmd := NewAdminCmd(cfg)
	cmd.SetArgs([]string{"reset-password", "--password-stdin"})
	cmd.SetIn(strings.NewReader("brand-new-password\n"))
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	require.NoError(t, cmd.Execute())

	var u models.User
	require.NoError(t, db.First(&u, "user_id = ?", 1).Error)
	require.True(t, utils.CheckPasswordHash("brand-new-password", u.Password))
	require.Equal(t, int64(2), u.SessionVersion)
}

// TestBuildCommandRegistersAdmin: the root frp-panel CLI exposes
// admin -> reset-password (spec §10.7.8).
func TestBuildCommandRegistersAdmin(t *testing.T) {
	root := BuildCommand(embed.FS{})
	adminCmd, _, err := root.Find([]string{"admin"})
	require.NoError(t, err)
	require.NotNil(t, adminCmd)
	require.Equal(t, "admin", adminCmd.Use)

	resetCmd, _, err := adminCmd.Find([]string{"reset-password"})
	require.NoError(t, err)
	require.NotNil(t, resetCmd)
	require.Equal(t, "reset-password", resetCmd.Use)
}
