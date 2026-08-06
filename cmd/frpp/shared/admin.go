package shared

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/glebarez/sqlite"
	"github.com/spf13/cobra"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	// minResetPasswordLen / maxResetPasswordLen bound the local admin
	// reset-password input (spec §10.7.8). Byte length. maxResetPasswordLen
	// is capped at 72 — bcrypt's hard input limit (golang.org/x/crypto/bcrypt
	// errors on longer passwords) — so every password accepted here can
	// always be hashed.
	minResetPasswordLen = 12
	maxResetPasswordLen = 72
)

// resetPasswordAuditSummary is the FIXED, marker-free summary written for
// every successful local reset. It deliberately never contains the
// username, the password, a token or any sensitive marker (the audit read
// path replaces summaries containing secret/password/token/config/... with
// "[redacted]").
const resetPasswordAuditSummary = "local administrator reset credentials"

// NewAdminCmd builds the local-only `admin` command tree. Every subcommand
// operates directly on the local database via the shared DB manager; no
// HTTP route, listener or network call is created (spec §10.7.8 — there is
// no unauthenticated HTTP password reset interface).
func NewAdminCmd(cfg conf.Config) *cobra.Command {
	adminCmd := &cobra.Command{
		Use:   "admin",
		Short: "local-only administration commands (operate directly on the local database)",
	}
	adminCmd.AddCommand(NewAdminResetPasswordCmd(cfg))
	return adminCmd
}

// NewAdminResetPasswordCmd builds `admin reset-password`. The new password
// MUST be piped via --password-stdin; interactive terminals, empty, too
// short (<12) and too long (>72, the bcrypt cap) passwords are rejected. On
// success the user's session version is bumped so every existing JWT dies,
// and one audit row is written. The password is never echoed, logged or
// stored.
func NewAdminResetPasswordCmd(cfg conf.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset-password",
		Short: "reset a local admin password and invalidate all existing sessions (password via --password-stdin)",
		RunE: func(cmd *cobra.Command, args []string) error {
			password, err := readPasswordFromStdin(cmd.InOrStdin())
			if err != nil {
				return err
			}
			if err := validatePassword(password); err != nil {
				return err
			}

			db, err := openLocalDB(cfg)
			if err != nil {
				return fmt.Errorf("open local database: %w", err)
			}

			username, _ := cmd.Flags().GetString("username")
			if username == "" {
				username, err = resolveAdminUsername(db)
				if err != nil {
					return fmt.Errorf("resolve admin user: %w", err)
				}
			}

			if err := resetPasswordInDB(db, username, password); err != nil {
				return fmt.Errorf("reset password: %w", err)
			}

			logger.Logger(context.Background()).
				Infof("password reset successful, all existing sessions for user [%s] are invalidated", username)
			return nil
		},
	}

	cmd.Flags().Bool("password-stdin", false, "read the new password from stdin (required; interactive terminals are rejected)")
	cmd.Flags().String("username", "", "username of the admin to reset (default: the admin with the smallest user id)")
	_ = cmd.MarkFlagRequired("password-stdin")

	return cmd
}

// readPasswordFromStdin reads the new password from r. Interactive
// terminals are rejected outright: the password must be piped (scripted
// reset only), so it never appears in a PTY echo, the shell history or a
// process list. A single trailing newline (LF/CRLF) is trimmed; the
// password itself is never echoed or logged.
func readPasswordFromStdin(r io.Reader) (string, error) {
	if f, ok := r.(*os.File); ok {
		if fi, err := f.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
			return "", errors.New("refusing to read the password from an interactive terminal: pipe it via stdin " +
				"(e.g. printf '%s' | frp-panel admin reset-password --password-stdin)")
		}
	}
	raw, err := io.ReadAll(io.LimitReader(r, maxResetPasswordLen+1))
	if err != nil {
		return "", fmt.Errorf("read password from stdin: %w", err)
	}
	return strings.TrimRight(string(raw), "\r\n"), nil
}

// validatePassword enforces the reset password policy: non-empty and
// 12..72 bytes (bcrypt's hard limit — anything longer can never be hashed).
func validatePassword(password string) error {
	if password == "" {
		return errors.New("password is empty")
	}
	if len(password) < minResetPasswordLen {
		return fmt.Errorf("password too short: at least %d characters required", minResetPasswordLen)
	}
	if len(password) > maxResetPasswordLen {
		return fmt.Errorf("password too long: at most %d characters allowed", maxResetPasswordLen)
	}
	return nil
}

// resolveAdminUsername returns the username of the admin to reset when
// --username is omitted: the admin with the smallest user id (deterministic
// when several admins exist; spec §10.7.8).
func resolveAdminUsername(db *gorm.DB) (string, error) {
	var u models.User
	if err := db.Where("role = ?", defs.UserRole_Admin).Order("user_id ASC").First(&u).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", errors.New("no admin user found")
		}
		return "", err
	}
	return u.UserName, nil
}

// openLocalDB opens the local database with the shared DB manager
// (models.NewDBManager — the same manager the master's fx provider wraps)
// and runs the schema migrations (db.Init, so an existing users table is
// upgraded in place with the session_version column). No HTTP routes or
// network listeners are created, no .env file is read here (cfg arrives
// pre-built), and the DSN/password are never logged.
func openLocalDB(cfg conf.Config) (*gorm.DB, error) {
	dbm := models.NewDBManager(cfg.DB.Type)
	var (
		db  *gorm.DB
		err error
	)
	switch cfg.DB.Type {
	case defs.DBTypeSQLite3:
		db, err = gorm.Open(sqlite.Open(cfg.DB.DSN), &gorm.Config{})
	case defs.DBTypeMysql:
		db, err = gorm.Open(mysql.Open(cfg.DB.DSN), &gorm.Config{})
	case defs.DBTypePostgres:
		db, err = gorm.Open(postgres.Open(cfg.DB.DSN), &gorm.Config{})
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.DB.Type)
	}
	if err != nil {
		return nil, err
	}
	dbm.SetDB(cfg.DB.Type, defs.DBRoleDefault, db)
	dbm.Init()
	return db, nil
}

// resetPasswordInDB performs the atomic local admin password reset: bcrypt
// hash, session-version bump (invalidates every existing JWT for the user)
// and an audit row, all in ONE transaction. Any failure rolls everything
// back; the audit row stores no password, token or other secret.
func resetPasswordInDB(db *gorm.DB, username, password string) error {
	return resetPasswordWithHasher(db, username, password, utils.HashPassword)
}

// resetPasswordWithHasher is resetPasswordInDB with an injectable hasher
// (tests use a fast bcrypt cost; production uses utils.HashPassword).
func resetPasswordWithHasher(db *gorm.DB, username, password string, hashPassword func(string) (string, error)) error {
	if err := validatePassword(password); err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var user models.User
		if err := tx.Where(&models.User{UserEntity: &models.UserEntity{UserName: username}}).
			First(&user).Error; err != nil {
			return err
		}
		if user.UserEntity == nil {
			return gorm.ErrRecordNotFound
		}
		if user.Role != defs.UserRole_Admin {
			return errors.New("target user is not an admin")
		}

		hash, err := hashPassword(password)
		if err != nil {
			return fmt.Errorf("hash password: %w", err)
		}

		// Strictly bump the effective version (legacy 0 counts as 1): the
		// old value can never reappear, so every previously issued token —
		// claimed or legacy — is dead after this update (spec §10.7.8).
		newVersion := user.UserEntity.EffectiveSessionVersion() + 1
		if err := tx.Model(&models.User{}).Where("user_id = ?", user.UserID).
			Updates(map[string]interface{}{
				"password":        hash,
				"session_version": newVersion,
			}).Error; err != nil {
			return err
		}

		audit := &models.AuditEvent{
			TenantID:    user.TenantID,
			EventType:   models.AuditEventTypeResetPassword,
			Summary:     resetPasswordAuditSummary,
			ActorUserID: user.UserID,
			Success:     true,
		}
		return tx.Create(audit).Error
	})
}
