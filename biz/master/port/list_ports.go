// Package port implements the Master-side port-manager read API
// (spec §7.1 POST /api/v1/port/list).
package port

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VaalaCat/frp-panel/common"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/pb"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/rpc"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// scanTimeout bounds each per-node InspectPorts RPC (spec §8.4: port
// scans get 5s; the retry stays inside the same context so the total
// never exceeds the budget). Overridable in tests.
var scanTimeout = 5 * time.Second

// newScanContext creates the per-node RPC context. A package var so tests
// can verify per-node timeouts without wall-clock sleeps.
var newScanContext = func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// backoffDelay returns the retry backoff for a transient scan failure
// (spec §8.4: random 200–500ms). A package var so tests inject zero delay.
var backoffDelay = func() time.Duration {
	return time.Duration(200+rand.Intn(301)) * time.Millisecond
}

// maxConcurrentScans bounds the all-nodes fanout: never more than 8
// in-flight agent RPCs (spec D2c ruling 4).
const maxConcurrentScans = 8

// scan attempts per node: initial + one retry (spec §8.4).
const maxScanAttempts = 2

// Evidence sources attached to UNKNOWN observations (spec D2c ruling 5).
const (
	sourceAgentOffline    = "agent-offline"
	sourceScanUnavailable = "scan-unavailable"
)

// errNodeNotVisible is the unified failure for missing AND cross-tenant
// node ids: callers must never distinguish the two (no existence leak).
var errNodeNotVisible = errors.New("node not found or not visible")

// inspectCaller sends one EVENT_INSPECT_PORTS RPC and returns the raw
// agent message. Production wraps rpc.CallClientWithContext; tests inject
// a scripted fake.
type inspectCaller interface {
	Call(ctx context.Context, appInst app.Application, nodeID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error)
}

type rpcInspectCaller struct{}

func (rpcInspectCaller) Call(ctx context.Context, appInst app.Application, nodeID string, event pb.Event, msg proto.Message) (*pb.ClientMessage, error) {
	return rpc.CallClientWithContext(ctx, appInst, nodeID, event, msg)
}

// defaultInspectCaller is the production caller; tests swap the package
// var. NOT parallel-safe by design.
var defaultInspectCaller inspectCaller = rpcInspectCaller{}

// nodeKind distinguishes the agent side a node belongs to, which decides
// the locating field of InspectPortsRequest.
type nodeKind int

const (
	nodeKindClient nodeKind = iota
	nodeKindServer
)

// visibleNode is one tenant-authorized target node for a scan.
type visibleNode struct {
	ID   string
	Kind nodeKind
}

// nodeResolver abstracts tenant-scoped node visibility so tests can inject
// in-memory fakes without a DB.
type nodeResolver interface {
	// Resolve verifies a single node id is visible to the user and returns
	// its kind. Missing and cross-tenant ids yield the SAME error.
	Resolve(userInfo models.UserInfo, nodeID string) (*visibleNode, error)
	// List returns every client/server node visible to the user,
	// deduplicated by id (clients win on id collisions).
	List(userInfo models.UserInfo) ([]*visibleNode, error)
}

// daoNodeResolver is the production resolver: read-only tenant/user scoped
// DAO queries only — never writes, never reads secrets or configs.
type daoNodeResolver struct{ ctx *app.Context }

func (r *daoNodeResolver) Resolve(userInfo models.UserInfo, nodeID string) (*visibleNode, error) {
	if nodeID == "" {
		return nil, errNodeNotVisible
	}
	// Both lookups are tenant/user scoped; a cross-tenant row does not
	// match, so its error is identical to a missing row.
	if client, err := dao.NewQuery(r.ctx).GetClientByClientID(userInfo, nodeID); err == nil {
		physicalID := client.ClientID
		if client.OriginClientID != "" {
			origin, originErr := dao.NewQuery(r.ctx).GetClientByClientID(userInfo, client.OriginClientID)
			if originErr != nil {
				return nil, errNodeNotVisible
			}
			physicalID = origin.ClientID
		}
		return &visibleNode{ID: physicalID, Kind: nodeKindClient}, nil
	}
	if _, err := dao.NewQuery(r.ctx).GetServerByServerID(userInfo, nodeID); err == nil {
		return &visibleNode{ID: nodeID, Kind: nodeKindServer}, nil
	}
	return nil, errNodeNotVisible
}

func (r *daoNodeResolver) List(userInfo models.UserInfo) ([]*visibleNode, error) {
	clients, err := dao.NewQuery(r.ctx).GetAllClients(userInfo)
	if err != nil {
		return nil, err
	}
	count, err := dao.NewQuery(r.ctx).CountServers(userInfo)
	if err != nil {
		return nil, err
	}
	var servers []*models.ServerEntity
	// ListServers also exposes the configured default server through its
	// compatibility OR clause. Reserve one additional slot so a tenant's
	// last server is not truncated; count=0 still needs one slot for default.
	serverPageSize := int(count) + 1
	servers, err = dao.NewQuery(r.ctx).ListServers(userInfo, 1, serverPageSize)
	if err != nil {
		return nil, err
	}

	// dedupe by node id; clients win over servers on collision so the
	// scan targets are deterministic
	seen := make(map[string]bool, len(clients)+len(servers))
	nodes := make([]*visibleNode, 0, len(clients)+len(servers))
	for _, c := range clients {
		physicalID := c.ClientID
		if c.OriginClientID != "" {
			physicalID = c.OriginClientID
		}
		// An orphan shadow must not turn an unverified OriginClientID into
		// an RPC target. GetAllClients is tenant-scoped; requiring the
		// canonical row in that same result prevents cross-tenant leakage.
		if c.OriginClientID != "" {
			originVisible := false
			for _, candidate := range clients {
				if candidate.ClientID == physicalID {
					originVisible = true
					break
				}
			}
			if !originVisible {
				continue
			}
		}
		if seen[physicalID] {
			continue
		}
		seen[physicalID] = true
		nodes = append(nodes, &visibleNode{ID: physicalID, Kind: nodeKindClient})
	}
	for _, s := range servers {
		if seen[s.ServerID] {
			continue
		}
		seen[s.ServerID] = true
		nodes = append(nodes, &visibleNode{ID: s.ServerID, Kind: nodeKindServer})
	}
	return nodes, nil
}

// newNodeResolver builds the production resolver; tests swap the package
// var. NOT parallel-safe by design.
var newNodeResolver = func(ctx *app.Context) nodeResolver {
	return &daoNodeResolver{ctx: ctx}
}

// listPortsFilter is the validated request filter shared by every node.
type listPortsFilter struct {
	protocol string // "" (all) | "tcp" | "udp"
	portMin  uint32
	portMax  uint32
}

// normalizeListPortsFilter validates and normalizes the request filter
// (spec D2c ruling 1): protocol is one of ""/tcp/tcp4/tcp6/udp/udp4/udp6
// normalized to tcp/udp; 0/0 means all ports, a single 0 is an open end,
// and the effective range is always 1..65535 with min<=max. On failure it
// returns a non-empty message; the caller must reply INVALID and touch no
// agent.
func normalizeListPortsFilter(req *pb.ListPortsRequest) (listPortsFilter, string) {
	f := listPortsFilter{}
	switch req.GetProtocol() {
	case "":
	case "tcp", "tcp4", "tcp6":
		f.protocol = "tcp"
	case "udp", "udp4", "udp6":
		f.protocol = "udp"
	default:
		return f, fmt.Sprintf("unsupported protocol %q, only tcp/udp are allowed", req.GetProtocol())
	}

	min, max := req.GetPortMin(), req.GetPortMax()
	if min == 0 {
		min = 1
	}
	if max == 0 {
		max = 65535
	}
	if min > 65535 || max > 65535 || min > max {
		return f, fmt.Sprintf("invalid port range %d..%d, must be within 1..65535 with min<=max", min, max)
	}
	f.portMin, f.portMax = min, max
	return f, ""
}

// inspectRequestFor builds the agent payload for one node: ONLY the node
// locating field, the normalized protocol, the effective port range and
// include_docker=true (spec D2c ruling 4).
func inspectRequestFor(node *visibleNode, filter listPortsFilter) *pb.InspectPortsRequest {
	req := &pb.InspectPortsRequest{
		Protocol:      lo.ToPtr(filter.protocol),
		PortMin:       lo.ToPtr(filter.portMin),
		PortMax:       lo.ToPtr(filter.portMax),
		IncludeDocker: lo.ToPtr(true),
	}
	if node.Kind == nodeKindClient {
		req.ClientId = lo.ToPtr(node.ID)
	} else {
		req.ServerId = lo.ToPtr(node.ID)
	}
	return req
}

// nodeScanResult is the outcome of scanning one node.
type nodeScanResult struct {
	node         *visibleNode
	observations []*pb.PortObservation // nil on failure
	source       string                // UNKNOWN evidence source on failure
	err          error
}

// scanNodes fans out one scan per target with bounded concurrency
// (maxConcurrentScans); results keep target order.
func scanNodes(c *app.Context, targets []*visibleNode, filter listPortsFilter) []nodeScanResult {
	results := make([]nodeScanResult, len(targets))
	sem := make(chan struct{}, maxConcurrentScans)
	var wg sync.WaitGroup
	for i, node := range targets {
		wg.Add(1)
		go func(i int, node *visibleNode) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = scanNode(c, node, filter)
		}(i, node)
	}
	wg.Wait()
	return results
}

// scanNode scans one node with its own 5s context; a transient read-scan
// failure is retried once inside that same context (spec §8.4). Parsing
// and agent-status failures are definite answers and never retried.
func scanNode(c *app.Context, node *visibleNode, filter listPortsFilter) nodeScanResult {
	ctx, cancel := newScanContext(c, scanTimeout)
	defer cancel()

	req := inspectRequestFor(node, filter)
	for attempt := 0; attempt < maxScanAttempts; attempt++ {
		if attempt > 0 {
			delay := backoffDelay()
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nodeScanResult{node: node, err: ctx.Err(), source: sourceAgentOffline}
			}
		}

		resp, err := defaultInspectCaller.Call(ctx, c.GetApp(), node.ID, pb.Event_EVENT_INSPECT_PORTS, req)
		if err != nil {
			// transient transport failure (offline/timeout/cancel):
			// retry once, then fail closed
			if attempt < maxScanAttempts-1 {
				continue
			}
			return nodeScanResult{node: node, err: err, source: classifyScanSource(err)}
		}

		out, err := parseInspectResponse(resp)
		if err != nil {
			// malformed protobuf or a non-SUCCESS agent status: a real
			// answer, fail closed without retry
			return nodeScanResult{node: node, err: err, source: sourceScanUnavailable}
		}
		return nodeScanResult{node: node, observations: filterObservations(out, node, filter)}
	}
	return nodeScanResult{node: node, err: errors.New("scan exhausted attempts"), source: sourceScanUnavailable}
}

// parseInspectResponse decodes and validates the agent reply; any defect
// is a hard failure (spec D2c ruling 5).
func parseInspectResponse(resp *pb.ClientMessage) (*pb.InspectPortsResponse, error) {
	if resp == nil {
		return nil, errors.New("agent returned an empty response")
	}
	if resp.GetEvent() != pb.Event_EVENT_INSPECT_PORTS {
		return nil, fmt.Errorf("unexpected agent event %s for port inspection", resp.GetEvent())
	}
	if resp.GetEvent() == pb.Event_EVENT_ERROR {
		return nil, fmt.Errorf("agent returned an error event: %s", resp.GetData())
	}
	out := &pb.InspectPortsResponse{}
	if err := proto.Unmarshal(resp.GetData(), out); err != nil {
		return nil, fmt.Errorf("cannot parse agent inspect response: %w", err)
	}
	if out.GetStatus() == nil || out.GetStatus().GetCode() != pb.RespCode_RESP_CODE_SUCCESS {
		return nil, fmt.Errorf("agent scan failed: %s", out.GetStatus().GetMessage())
	}
	return out, nil
}

// classifyScanSource maps a transport failure to the UNKNOWN evidence
// source: an unreachable agent (offline, timeout, cancelled) is
// agent-offline; anything else is scan-unavailable.
func classifyScanSource(err error) string {
	if err == nil {
		return sourceScanUnavailable
	}
	msg := err.Error()
	if strings.Contains(msg, "cannot get client") ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return sourceAgentOffline
	}
	return sourceScanUnavailable
}

// filterObservations enforces the master-side trust boundary (spec D2c
// ruling 6): node_id is pinned to the authorized target and observations
// outside the requested protocol/port range (nil, port 0, out-of-range,
// protocol mismatch, non-tcp/udp protocol) are dropped. Agent fields
// carrying shell text do not exist on the wire type.
func filterObservations(out *pb.InspectPortsResponse, node *visibleNode, filter listPortsFilter) []*pb.PortObservation {
	var kept []*pb.PortObservation
	for _, obs := range out.GetObservations() {
		if obs == nil {
			continue
		}
		obs.NodeId = node.ID // never trust the agent's node field
		protocol := normalizeObservationProtocol(obs.GetProtocol())
		if protocol == "" {
			continue
		}
		obs.Protocol = protocol
		if filter.protocol != "" && protocol != filter.protocol {
			continue // the agent may lie about the requested protocol
		}
		port := obs.GetLocalPort()
		if port < filter.portMin || port > filter.portMax {
			continue // also drops 0 and > 65535
		}
		kept = append(kept, obs)
	}
	return kept
}

func normalizeObservationProtocol(p string) string {
	switch p {
	case "tcp", "tcp4", "tcp6":
		return "tcp"
	case "udp", "udp4", "udp6":
		return "udp"
	}
	return ""
}

// unknownEvidence builds the single UNKNOWN evidence observation for a
// failed node (spec D2c ruling 5): node pinned, state UNKNOWN, source
// agent-offline / scan-unavailable, observed_at set.
func unknownEvidence(node *visibleNode, filter listPortsFilter, source string) []*pb.PortObservation {
	return []*pb.PortObservation{{
		NodeId:     node.ID,
		Protocol:   filter.protocol,
		LocalPort:  0,
		State:      pb.PortState_STATE_UNKNOWN,
		ObservedAt: time.Now().Unix(),
		Source:     source,
	}}
}

// sortObservations gives the response a deterministic order
// (node_id, protocol, port, address) so callers never see test flakiness
// from concurrent scans.
func sortObservations(obs []*pb.PortObservation) {
	sort.SliceStable(obs, func(i, j int) bool {
		a, b := obs[i], obs[j]
		if a.GetNodeId() != b.GetNodeId() {
			return a.GetNodeId() < b.GetNodeId()
		}
		if a.GetProtocol() != b.GetProtocol() {
			return a.GetProtocol() < b.GetProtocol()
		}
		if a.GetLocalPort() != b.GetLocalPort() {
			return a.GetLocalPort() < b.GetLocalPort()
		}
		return a.GetLocalAddress() < b.GetLocalAddress()
	})
}

// dedupeNodes keeps the first occurrence of each node id so a node is
// never scanned twice. The DAO resolver lists clients before servers, so
// first-wins means clients win on id collisions (deterministic).
func dedupeNodes(nodes []*visibleNode) []*visibleNode {
	seen := make(map[string]bool, len(nodes))
	out := make([]*visibleNode, 0, len(nodes))
	for _, n := range nodes {
		if seen[n.ID] {
			continue
		}
		seen[n.ID] = true
		out = append(out, n)
	}
	return out
}

// ListPorts queries port observations from one node or every node visible
// to the caller (spec §7.1 POST /api/v1/port/list). It is strictly
// read-only: no DB/config/firewall writes, no shell execution.
func ListPorts(c *app.Context, req *pb.ListPortsRequest) (*pb.ListPortsResponse, error) {
	userInfo := common.GetUserInfo(c)
	if userInfo == nil || !userInfo.Valid() {
		return &pb.ListPortsResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "invalid user"},
		}, nil
	}

	filter, msg := normalizeListPortsFilter(req)
	if msg != "" {
		return &pb.ListPortsResponse{
			Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: msg},
		}, nil
	}

	resolver := newNodeResolver(c)

	var targets []*visibleNode
	if nodeID := req.GetNodeId(); nodeID != "" {
		node, err := resolver.Resolve(userInfo, nodeID)
		if err != nil {
			// missing and cross-tenant are indistinguishable: NOT_FOUND,
			// zero RPC (no existence leak)
			return &pb.ListPortsResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_NOT_FOUND, Message: errNodeNotVisible.Error()},
			}, nil
		}
		targets = []*visibleNode{node}
	} else {
		nodes, err := resolver.List(userInfo)
		if err != nil {
			logger.Logger(c).WithError(err).Errorf("cannot enumerate visible nodes for port list")
			return &pb.ListPortsResponse{
				Status: &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "cannot enumerate visible nodes"},
			}, nil
		}
		if len(nodes) == 0 {
			// everything visible is nothing: succeed with an empty list
			return &pb.ListPortsResponse{
				Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
				Observations: []*pb.PortObservation{},
			}, nil
		}
		// no node is ever scanned twice, whatever the resolver returns
		targets = dedupeNodes(nodes)
	}

	results := scanNodes(c, targets, filter)

	// single explicit node: any failure is a hard INVALID
	if len(targets) == 1 {
		res := results[0]
		if res.err != nil {
			return &pb.ListPortsResponse{
				Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: res.err.Error()},
				Observations: unknownEvidence(res.node, filter, res.source),
			}, nil
		}
		sortObservations(res.observations)
		return &pb.ListPortsResponse{
			Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: "ok"},
			Observations: res.observations,
		}, nil
	}

	// fanout: partial failures degrade to SUCCESS message="partial" with
	// UNKNOWN evidence; total failure is INVALID (spec D2c ruling 5)
	var all []*pb.PortObservation
	failed := 0
	for _, res := range results {
		if res.err != nil {
			failed++
			all = append(all, unknownEvidence(res.node, filter, res.source)...)
			continue
		}
		all = append(all, res.observations...)
	}
	if failed == len(results) {
		return &pb.ListPortsResponse{
			Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_INVALID, Message: "all nodes failed to scan"},
			Observations: all,
		}, nil
	}
	statusMsg := "ok"
	if failed > 0 {
		statusMsg = "partial"
	}
	sortObservations(all)
	return &pb.ListPortsResponse{
		Status:       &pb.Status{Code: pb.RespCode_RESP_CODE_SUCCESS, Message: statusMsg},
		Observations: all,
	}, nil
}
