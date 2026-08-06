package dao

import "github.com/VaalaCat/frp-panel/services/app"

type Query interface {
	AuditQuery
	CertQuery
	ClientQuery
	EndpointQuery
	LinkQuery
	NetworkQuery
	PortReservationQuery
	ProxyQuery
	ServerQuery
	SnapshotQuery
	StatsQuery
	TransactionQuery
	UserQuery
	WireGuardQuery
	WorkerQuery
}

type Mutation interface {
	AuditMutation
	CertMutation
	ClientMutation
	EndpointMutation
	LinkMutation
	NetworkMutation
	PortReservationMutation
	ProxyMutation
	ServerMutation
	SnapshotMutation
	StatsMutation
	TransactionMutation
	UserMutation
	WireGuardMutation
	WorkerMutation
	UserGroupMutation
}

// queryImpl/mutationImpl 是具体表级实现的基础结构（持有 ctx）。
type queryImpl struct {
	ctx *app.Context
}

type mutationImpl struct {
	ctx *app.Context
}

// compositeQuery / compositeMutation 组合各子领域实现，对外暴露统一入口。
type compositeQuery struct {
	AuditQuery
	CertQuery
	ClientQuery
	EndpointQuery
	LinkQuery
	NetworkQuery
	PortReservationQuery
	ProxyQuery
	ServerQuery
	SnapshotQuery
	StatsQuery
	TransactionQuery
	UserQuery
	WireGuardQuery
	WorkerQuery
}

type compositeMutation struct {
	AuditMutation
	CertMutation
	ClientMutation
	EndpointMutation
	LinkMutation
	NetworkMutation
	PortReservationMutation
	ProxyMutation
	ServerMutation
	SnapshotMutation
	StatsMutation
	TransactionMutation
	UserMutation
	WireGuardMutation
	WorkerMutation
	UserGroupMutation
}

func NewQuery(ctx *app.Context) Query {
	base := &queryImpl{ctx: ctx}
	return &compositeQuery{
		AuditQuery:           newAuditQuery(base),
		CertQuery:            newCertQuery(base),
		ClientQuery:          newClientQuery(base),
		EndpointQuery:        newEndpointQuery(base),
		LinkQuery:            newLinkQuery(base),
		NetworkQuery:         newNetworkQuery(base),
		PortReservationQuery: newPortReservationQuery(base),
		ProxyQuery:           newProxyQuery(base),
		ServerQuery:          newServerQuery(base),
		SnapshotQuery:        newSnapshotQuery(base),
		StatsQuery:           newStatsQuery(base),
		TransactionQuery:     newTransactionQuery(base),
		UserQuery:            newUserQuery(base),
		WireGuardQuery:       newWireGuardQuery(base),
		WorkerQuery:          newWorkerQuery(base),
	}
}

func NewMutation(ctx *app.Context) Mutation {
	base := &mutationImpl{ctx: ctx}
	return &compositeMutation{
		AuditMutation:           newAuditMutation(base),
		CertMutation:            newCertMutation(base),
		ClientMutation:          newClientMutation(base),
		EndpointMutation:        newEndpointMutation(base),
		LinkMutation:            newLinkMutation(base),
		NetworkMutation:         newNetworkMutation(base),
		PortReservationMutation: newPortReservationMutation(base),
		ProxyMutation:           newProxyMutation(base),
		ServerMutation:          newServerMutation(base),
		SnapshotMutation:        newSnapshotMutation(base),
		StatsMutation:           newStatsMutation(base),
		TransactionMutation:     newTransactionMutation(base),
		UserMutation:            newUserMutation(base),
		WireGuardMutation:       newWireGuardMutation(base),
		WorkerMutation:          newWorkerMutation(base),
		UserGroupMutation:       newUserGroupMutation(base),
	}
}

var (
	_ Query    = (*compositeQuery)(nil)
	_ Mutation = (*compositeMutation)(nil)
)
