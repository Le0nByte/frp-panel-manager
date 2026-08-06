package models

import (
	"time"

	"gorm.io/gorm"
)

// Reservation statuses for port_reservation.
const (
	ReservationStatusPending   = "pending"
	ReservationStatusCommitted = "committed"
	ReservationStatusReleasing = "releasing"
)

// PortReservation blocks two save requests from grabbing the same public port
// concurrently. The unique index (server_id, protocol, remote_port) enforces
// single-owner semantics at the database level GLOBALLY (a physical port is
// one resource regardless of tenant); TenantID is recorded and every
// query/write is tenant-scoped so no tenant can see or mutate another's lease.
type PortReservation struct {
	gorm.Model
	TenantID      int       `gorm:"index"`
	ServerID      string    `gorm:"uniqueIndex:idx_port_res;size:128;not null"`
	Protocol      string    `gorm:"uniqueIndex:idx_port_res;size:16;not null"`
	RemotePort    uint32    `gorm:"uniqueIndex:idx_port_res;not null"`
	OwnerProxyID  *uint     `gorm:"index"`
	TransactionID string    `gorm:"index;size:64;not null"`
	Status        string    `gorm:"default:pending;size:32;not null"`
	ExpiresAt     time.Time `gorm:"index"`
}

func (*PortReservation) TableName() string {
	return "port_reservation"
}

// IsExpired reports whether a pending lease has expired. Committed leases
// never expire (they are only released by explicit lifecycle operations).
func (r *PortReservation) IsExpired(now time.Time) bool {
	if r.Status != ReservationStatusPending {
		return false
	}
	if r.ExpiresAt.IsZero() {
		return false
	}
	return now.After(r.ExpiresAt)
}
