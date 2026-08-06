package models

import (
	"fmt"
	"time"

	"github.com/VaalaCat/frp-panel/defs"
	"gorm.io/gorm"
)

type User struct {
	*UserEntity
}

type UserInfo interface {
	GetUserID() int
	GetUserIDStr() string
	GetUserName() string
	GetEmail() string
	GetHashedPassword() string
	GetStatus() int
	GetRole() string
	GetToken() string
	GetTenantID() int
	GetSafeUserInfo() UserEntity
	IsAdmin() bool
	Valid() bool
}

var _ UserInfo = (*UserEntity)(nil)

type UserEntity struct {
	UserID    int    `json:"user_id" gorm:"primaryKey"`
	UserName  string `json:"user_name" gorm:"type:varchar(255);uniqueIndex;not null"`
	Password  string `json:"password"`
	Email     string `json:"email" gorm:"type:varchar(255);uniqueIndex;not null"`
	Status    int    `json:"status"`
	Role      string `json:"role"`
	TenantID  int    `json:"tenant_id"`
	Token     string `json:"token"`
	// SessionVersion is the per-user session generation (spec §10.7.8).
	// Every login token carries it as a JWT claim; the local-only admin
	// reset-password command bumps it so all previously issued tokens are
	// invalidated. Existing rows are upgraded by AutoMigrate to 1. It never
	// leaves the server (json:"-").
	SessionVersion int64 `json:"-" gorm:"not null;default:1"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      gorm.DeletedAt `gorm:"index"`

	Groups []*UserGroup `json:"groups,omitempty" gorm:"many2many:user_group_memberships;"`
}

func (u *UserEntity) GetUserID() int {
	return u.UserID
}

func (u *UserEntity) GetUserIDStr() string {
	return fmt.Sprint(u.UserID)
}

func (u *UserEntity) GetUserName() string {
	return u.UserName
}

func (u *UserEntity) GetEmail() string {
	return u.Email
}

func (u *UserEntity) GetHashedPassword() string {
	return u.Password
}

func (u *UserEntity) GetStatus() int {
	return u.Status
}

func (u *UserEntity) GetRole() string {
	return u.Role
}

func (u *UserEntity) GetTenantID() int {
	return u.TenantID
}

func (u *UserEntity) GetToken() string {
	return u.Token
}

func (u *UserEntity) GetSafeUserInfo() UserEntity {
	return UserEntity{
		UserID:   u.UserID,
		UserName: u.UserName,
		Email:    u.Email,
		Status:   u.Status,
		Role:     u.Role,
	}
}

func (u *UserEntity) Valid() bool {
	if u == nil {
		return false
	}
	if u.Status == STATUS_BANED {
		return false
	}
	return true
}

func (u *UserEntity) IsAdmin() bool {
	return u.Role == defs.UserRole_Admin
}

// EffectiveSessionVersion returns the session version used for JWT session
// checks (spec §10.7.8): rows written before the session-version feature
// carry 0 and count as version 1 (the initial generation). A nil entity
// also counts as 1 (fail-closed callers never panic).
func (u *UserEntity) EffectiveSessionVersion() int64 {
	if u == nil || u.SessionVersion < 1 {
		return 1
	}
	return u.SessionVersion
}

func (u *User) TableName() string {
	return "users"
}
