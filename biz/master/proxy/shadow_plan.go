package proxy

import (
	"errors"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/samber/lo"
	"github.com/tiendc/go-deepcopy"
	"gorm.io/gorm"
)

// ShadowPlan is the read-only, in-memory description of the shadow
// conversion a safe create will persist atomically inside CommitCreate
// (spec §13.2 零配置变更: NO shadow write may touch the DB before the final
// commit transaction). PrepareShadowPlan only reads; every write implied by
// the plan happens inside CommitCreate's single DB transaction.
//
// The plan keeps IMMUTABLE before-state copies (OriginBefore / ChildBefore)
// deliberately separate from the working Child: the coordinator mutates
// plan.Child in place to build the candidate, while the before snapshot and
// the compensation restore payload must always come from the immutable
// copies — never from the mutated working entity (round-8 I2/I9).
type ShadowPlan struct {
	// FirstShadow is true when the origin client has never been shadowed:
	// CommitCreate must convert the origin row (IsShadow=true, cleared
	// config and server binding) and hard-delete its proxy_config rows
	// (they migrate to the child).
	FirstShadow bool
	// OriginBefore is an immutable deep copy of the origin row BEFORE
	// conversion (its original config). Non-nil only when FirstShadow;
	// the before snapshot and the compensation restore payload (built
	// with the ORIGIN's identity/metadata) come from it.
	OriginBefore *models.ClientEntity
	// OriginAfter is the origin row as it must look AFTER conversion.
	// Non-nil only when FirstShadow.
	OriginAfter *models.ClientEntity
	// Child is the WORKING target child client: either the existing row
	// for (origin, server) deep-copied in memory, or a brand-new
	// candidate built from the origin (initial config inherited from the
	// origin). The coordinator mutates this copy to build the candidate;
	// it is never used for before/restore bytes.
	Child *models.ClientEntity
	// ChildBefore is an immutable deep copy of the child's pre-request
	// state (existing-child flows only; first-shadow flows restore the
	// origin from OriginBefore instead). The before snapshot and the
	// existing-child restore payload come from it.
	ChildBefore *models.ClientEntity
	// ChildCreated marks Child as a brand-new row (INSERT) instead of an
	// existing row (in-place UPDATE).
	ChildCreated bool
}

// PrepareShadowPlan resolves the target child client for the given origin
// and server with ZERO DB writes:
//   - existing child for the server -> in-memory deep copy, no writes;
//   - origin never shadowed         -> in-memory OriginAfter + child
//     candidate (initial config inherited from the origin), no writes;
//   - origin shadowed, no child yet -> in-memory child candidate from the
//     shadowed origin (empty config), no writes.
//
// It must NEVER call the write paths (MakeClientShadowed /
// ChildClientForServer): the conversion is persisted only by CommitCreate,
// so every failure before the commit leaves the config tables byte-identical.
func PrepareShadowPlan(c *app.Context, clientID, serverID string) (*ShadowPlan, error) {
	userInfo := common.GetUserInfo(c)
	q := dao.NewQuery(c)

	// 1. existing child for this server (IsShadow=false)
	child, err := q.GetClientByFilter(userInfo, &models.ClientEntity{OriginClientID: clientID, ServerID: serverID}, lo.ToPtr(false))
	if err == nil {
		copied := &models.ClientEntity{}
		if err := deepcopy.Copy(copied, child); err != nil {
			return nil, err
		}
		childBefore := &models.ClientEntity{}
		if err := deepcopy.Copy(childBefore, child); err != nil {
			return nil, err
		}
		return &ShadowPlan{Child: copied, ChildBefore: childBefore}, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 2. load the origin (never shadowed, or shadowed without a child yet)
	origin, err := q.GetClientByFilter(userInfo, &models.ClientEntity{ClientID: clientID}, nil)
	if err != nil {
		return nil, err
	}

	// child id: the next shadow index (reads only)
	count, err := q.CountClientsInShadow(userInfo, origin.ClientID)
	if err != nil {
		return nil, err
	}
	childID := app.ShadowedClientID(origin.ClientID, count+1)

	// 3. first shadow: the origin row must be converted as well
	if (!origin.IsShadow || len(origin.ConfigContent) != 0) && len(origin.OriginClientID) == 0 {
		originBefore := &models.ClientEntity{}
		if err := deepcopy.Copy(originBefore, origin); err != nil {
			return nil, err
		}
		originAfter := &models.ClientEntity{}
		if err := deepcopy.Copy(originAfter, origin); err != nil {
			return nil, err
		}
		originAfter.IsShadow = true
		originAfter.ConfigContent = nil
		originAfter.ServerID = ""
		child, err = buildChildCandidate(origin, serverID, childID)
		if err != nil {
			return nil, err
		}
		return &ShadowPlan{FirstShadow: true, OriginBefore: originBefore, OriginAfter: originAfter, Child: child, ChildCreated: true}, nil
	}

	// 4. origin already shadowed but no child for this server yet
	child, err = buildChildCandidate(origin, serverID, childID)
	if err != nil {
		return nil, err
	}
	// the child's pre-request state is an immutable empty-config copy
	// (nothing to restore but the child identity; the compensation
	// re-applies an empty config so the child handler registers nothing)
	childBefore := &models.ClientEntity{}
	if err := deepcopy.Copy(childBefore, child); err != nil {
		return nil, err
	}
	return &ShadowPlan{Child: child, ChildBefore: childBefore, ChildCreated: true}, nil
}

// buildChildCandidate deep-copies the origin into a brand-new child bound
// to serverID. The initial config is inherited from the origin: for a first
// shadow that is the origin's full config; for an already-shadowed origin
// (config cleared) the child starts empty.
func buildChildCandidate(origin *models.ClientEntity, serverID, childID string) (*models.ClientEntity, error) {
	child := &models.ClientEntity{}
	if err := deepcopy.Copy(child, origin); err != nil {
		return nil, err
	}
	child.ServerID = serverID
	child.ClientID = childID
	child.OriginClientID = origin.ClientID
	child.IsShadow = false
	child.Stopped = false
	return child, nil
}
