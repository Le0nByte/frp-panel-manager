package dao

import (
	"strings"

	"github.com/VaalaCat/frp-panel/models"
)

type AuditQuery interface {
	// List returns audit events ordered newest first with pagination and an
	// optional transaction_id filter, scoped to the caller's tenant.
	List(userInfo models.UserInfo, page, pageSize int, txID string) ([]*models.AuditEvent, int64, error)
}

type AuditMutation interface {
	Log(userInfo models.UserInfo, event *models.AuditEvent) error
	// LogSystem appends a SYSTEM-level audit row (TenantID=0,
	// ActorUserID=0) for the reserved rpc.* event kinds only. It is the
	// single writer used by the RPC guard; ordinary authenticated flows
	// must keep using Log so they stay tenant-scoped.
	LogSystem(event *models.AuditEvent) error
}

type auditQuery struct{ *queryImpl }
type auditMutation struct{ *mutationImpl }

func newAuditQuery(base *queryImpl) AuditQuery { return &auditQuery{base} }
func newAuditMutation(base *mutationImpl) AuditMutation {
	return &auditMutation{base}
}

func (q *auditQuery) List(userInfo models.UserInfo, page, pageSize int, txID string) ([]*models.AuditEvent, int64, error) {
	if err := requireUserInfo(userInfo); err != nil {
		return nil, 0, err
	}
	db := q.ctx.GetApp().GetDBManager().GetDefaultDB()
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 20
	}

	list := []*models.AuditEvent{}
	var total int64

	base := db.Model(&models.AuditEvent{}).Where("tenant_id = ?", userInfo.GetTenantID())
	if txID != "" {
		base = base.Where("transaction_id = ?", txID)
	}
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	err := base.Order("id DESC").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (m *auditMutation) Log(userInfo models.UserInfo, event *models.AuditEvent) error {
	if err := requireUserInfo(userInfo); err != nil {
		return err
	}
	if event == nil {
		return models.ErrNilAuditEvent
	}
	// Tenant and actor are forced from the authenticated user so a caller can
	// never attribute an event to another tenant/actor.
	event.TenantID = userInfo.GetTenantID()
	event.ActorUserID = userInfo.GetUserID()
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	return db.Create(event).Error
}

// LogSystem appends a system-level audit row for the reserved rpc.* event
// kinds. The tenant/actor are FORCED to 0 regardless of caller input, and
// the rpc.* prefix gate means an authenticated flow (which can only reach
// Log, never this method with its own identity) cannot forge a system row.
func (m *auditMutation) LogSystem(event *models.AuditEvent) error {
	if event == nil {
		return models.ErrNilAuditEvent
	}
	if !strings.HasPrefix(event.EventType, "rpc.") {
		return ErrSystemAuditEventTypeForbidden
	}
	// System rows are tenant/actor-less by construction.
	event.TenantID = 0
	event.ActorUserID = 0
	db := m.ctx.GetApp().GetDBManager().GetDefaultDB()
	if db == nil {
		return ErrSystemAuditDBUnavailable
	}
	// WithContext ties the write to the mutation's context so a cancelled/
	// deadlined app context aborts the INSERT — the shutdown-time audit
	// drain can never hang forever on a stuck database (I3-C). Ordinary
	// tenant-scoped Log is deliberately unchanged.
	return db.WithContext(m.ctx).Create(event).Error
}
