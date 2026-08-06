package dao

import (
	"fmt"
	"strings"

	"github.com/VaalaCat/frp-panel/defs"
	"github.com/VaalaCat/frp-panel/models"
	"github.com/VaalaCat/frp-panel/utils"
	v1 "github.com/fatedier/frp/pkg/config/v1"
)

// validateCommitUpdateInput enforces the static shape contract (S0) with
// ZERO DB writes: branch/payload completeness (active vs stopped),
// reservation key geometry, after-snapshot material and audit
// presence/redaction. Every violation is the stable defs.ErrInvalidCommitInput
// except an unsafe first-shadow plan on the stopped branch, which surfaces
// defs.ErrStoppedFirstShadow.
func validateCommitUpdateInput(in *CommitUpdateInput) error {
	invalid := func(format string, args ...interface{}) error {
		return fmt.Errorf("%w: %s", defs.ErrInvalidCommitInput, fmt.Sprintf(format, args...))
	}
	if in == nil {
		return invalid("nil input")
	}
	// branch payload (validated before the common identity checks so a
	// stopped-branch payload can never be interpreted as an active one)
	switch in.Branch {
	case UpdateBranchActive:
		if err := validateActiveCommitPayload(invalid, in); err != nil {
			return err
		}
	case UpdateBranchStopped:
		if err := validateStoppedCommitPayload(invalid, in); err != nil {
			return err
		}
	default:
		return invalid("branch %q is not a valid update branch", in.Branch)
	}
	if in.TxID == "" {
		return invalid("missing transaction id")
	}
	if in.OldProxyID == 0 {
		return invalid("missing old proxy id")
	}
	if in.OldName == "" {
		return invalid("missing old proxy name")
	}
	if in.OldServerID == "" {
		return invalid("missing old server id")
	}
	if in.OriginClientID == "" {
		return invalid("missing origin client id")
	}
	// reservation key geometry (spec §3 #6-8)
	if err := validateReservationKey(invalid, in.Reservation.OldKey); err != nil {
		return err
	}
	if err := validateReservationKey(invalid, in.Reservation.NewKey); err != nil {
		return err
	}
	if in.Reservation.OldKey.ServerID != in.OldServerID {
		return invalid("old reservation key server %q differs from old rule server %q", in.Reservation.OldKey.ServerID, in.OldServerID)
	}
	if in.Reservation.NewKey.ServerID != in.OldServerID {
		return invalid("new reservation key server %q differs from old rule server %q", in.Reservation.NewKey.ServerID, in.OldServerID)
	}
	switch in.Reservation.Mode {
	case ReservationModeSameKeyOwned:
		if in.Reservation.OldKey != in.Reservation.NewKey {
			return invalid("same_key_owned requires identical keys")
		}
		if !in.Reservation.OldReservationExpected {
			return invalid("same_key_owned implies an existing owned reservation")
		}
	case ReservationModeSameKeyPending:
		if in.Reservation.OldKey != in.Reservation.NewKey {
			return invalid("same_key_pending requires identical keys")
		}
		if in.Reservation.OldReservationExpected {
			return invalid("same_key_pending never expects an old committed row")
		}
	case ReservationModeDifferentKey:
		if in.Reservation.OldKey == in.Reservation.NewKey {
			return invalid("different_key requires different keys")
		}
	default:
		return invalid("reservation mode %q is not valid", in.Reservation.Mode)
	}
	// after snapshots: non-empty AES key + non-empty plaintext (§7; the
	// AES-256 SIZE is enforced by EncryptSnapshot at the snapshot step, so
	// a wrong-size key fails INSIDE the transaction and rolls everything
	// back — the atomicity contract)
	for _, snap := range []struct {
		name string
		in   SnapshotInput
	}{
		{"client", in.ClientAfterSnapshot},
		{"proxy", in.ProxyAfterSnapshot},
	} {
		if len(snap.in.Key) == 0 {
			return invalid("%s after snapshot key must not be empty", snap.name)
		}
		if len(snap.in.Plaintext) == 0 {
			return invalid("%s after snapshot plaintext must not be empty", snap.name)
		}
	}
	// audit: required, and the summary must not embed the plaintext
	// snapshots (the audit trail is redacted by construction, spec §6.4)
	if in.AuditEvent == nil {
		return invalid("audit event is required")
	}
	if summaryContainsPlaintext(in.AuditEvent.Summary, in.ClientAfterSnapshot.Plaintext, in.ProxyAfterSnapshot.Plaintext) {
		return invalid("audit summary must not contain plaintext config material")
	}
	return nil
}

// validateActiveCommitPayload checks the active-branch payload shape: the
// child client, the shadow plan and the final proxy name, plus the P0
// physical-key binding — the applied client config's real typed key must
// equal Reservation.NewKey.
func validateActiveCommitPayload(invalid func(string, ...interface{}) error, in *CommitUpdateInput) error {
	if in.Client == nil {
		return invalid("active branch requires the child client")
	}
	if in.Client.ClientID == "" {
		return invalid("active branch requires the child client id")
	}
	if in.Client.ServerID != in.OldServerID {
		return invalid("client server %q differs from old rule server %q", in.Client.ServerID, in.OldServerID)
	}
	if in.Client.OriginClientID != in.OriginClientID {
		return invalid("child origin %q differs from the planned origin %q", in.Client.OriginClientID, in.OriginClientID)
	}
	if in.Shadow == nil {
		return invalid("active branch requires the shadow commit plan")
	}
	if in.ProxyName == "" {
		return invalid("active branch requires the final proxy name")
	}
	// shadow shape: first-shadow must convert the origin AND create the
	// child; an existing child must NOT be created
	if in.Shadow.FirstShadow {
		if in.Shadow.OriginAfter == nil {
			return invalid("first-shadow requires origin-after state")
		}
		if !in.Shadow.ChildCreated {
			return invalid("first-shadow requires a created child")
		}
	} else if in.Shadow.ChildCreated {
		return invalid("existing-child update cannot create the child row")
	}
	// P0 physical-key binding (NEW side): the applied client config must
	// carry EXACTLY ONE proxy named ProxyName and its real typed key
	// (protocol + remote port, server = Client.ServerID) must equal the new
	// reservation key — a commit that locks one port while the rule runs
	// another is malformed before any DB write.
	return validateActivePhysicalKey(invalid, in.Client, in.ProxyName, in.Reservation.NewKey)
}

// validateStoppedCommitPayload checks the stopped-branch payload shape: the
// active payload must be ABSENT (zero client delta), the shadow plan must be
// nil or inert (a first-shadow plan is ErrStoppedFirstShadow — the stopped
// branch may never create a child), and the StoppedUpdatePayload must be
// complete and self-consistent: NewServerID equals OldServerID, the new
// typed JSON parses, its internal name/type match the declared fields, its
// type is tcp/udp, and its real physical key equals Reservation.NewKey.
// ProxyName is NOT required — the final name comes from
// StoppedUpdate.NewName.
func validateStoppedCommitPayload(invalid func(string, ...interface{}) error, in *CommitUpdateInput) error {
	if in.Client != nil {
		return invalid("stopped branch must not carry the active child client (zero client delta)")
	}
	if in.Shadow != nil && in.Shadow.FirstShadow {
		return fmt.Errorf("%w: stopped branch may not first-shadow an origin", defs.ErrStoppedFirstShadow)
	}
	if in.Shadow != nil && in.Shadow.ChildCreated {
		return invalid("stopped branch never creates a child")
	}
	su := in.StoppedUpdate
	if su == nil {
		return invalid("stopped branch requires the stopped-update payload")
	}
	if su.ChildClientID == "" {
		return invalid("stopped branch requires the child client id")
	}
	if su.NewName == "" {
		return invalid("stopped branch requires the new proxy name")
	}
	if su.NewServerID == "" {
		return invalid("stopped branch requires the new server id")
	}
	if su.NewServerID != in.OldServerID {
		return invalid("stopped branch server %q differs from old rule server %q", su.NewServerID, in.OldServerID)
	}
	declaredType := strings.ToLower(strings.TrimSpace(su.NewType))
	if declaredType != "tcp" && declaredType != "udp" {
		return invalid("stopped branch new type %q is not supported (only tcp/udp)", su.NewType)
	}
	if len(su.NewTypedJSON) == 0 {
		return invalid("stopped branch requires the new typed proxy json")
	}
	// parse the typed JSON: internal name/type must equal the declared
	// fields and the real physical key must equal the new reservation key
	row := &models.ProxyConfig{ProxyConfigEntity: &models.ProxyConfigEntity{Content: su.NewTypedJSON}}
	typed, err := row.GetTypedProxyConfig()
	if err != nil {
		return invalid("new typed proxy json is not parseable: %v", err)
	}
	base := typed.GetBaseConfig()
	if base == nil || base.Name != su.NewName {
		return invalid("new typed proxy json name %q differs from the declared new name %q", proxyBaseName(base), su.NewName)
	}
	jsonType := strings.ToLower(strings.TrimSpace(base.Type))
	if jsonType != declaredType {
		return invalid("new typed proxy json type %q differs from the declared new type %q", base.Type, su.NewType)
	}
	key, err := typedProxyPhysicalKey(su.NewServerID, typed)
	if err != nil {
		return invalid("new typed proxy json: %v", err)
	}
	if key != in.Reservation.NewKey {
		return invalid("new typed proxy json physical key %s/%s/%d differs from the new reservation key %s/%s/%d",
			key.ServerID, key.Protocol, key.RemotePort,
			in.Reservation.NewKey.ServerID, in.Reservation.NewKey.Protocol, in.Reservation.NewKey.RemotePort)
	}
	return nil
}

func proxyBaseName(base *v1.ProxyBaseConfig) string {
	if base == nil {
		return ""
	}
	return base.Name
}

func validateReservationKey(invalid func(string, ...interface{}) error, key ReservationKey) error {
	if key.ServerID == "" {
		return invalid("reservation key missing server")
	}
	if key.Protocol == "" {
		return invalid("reservation key missing protocol")
	}
	if key.RemotePort == 0 {
		return invalid("reservation key missing remote port")
	}
	return nil
}

// summaryContainsPlaintext reports whether the audit summary embeds any of
// the plaintext snapshot payloads — the DAO rejects such a commit outright
// so the append-only trail can never store config plaintext.
func summaryContainsPlaintext(summary string, plaintexts ...[]byte) bool {
	for _, p := range plaintexts {
		if len(p) > 0 && strings.Contains(summary, string(p)) {
			return true
		}
	}
	return false
}

// typedProxyPhysicalKey extracts the physical reservation key of a typed
// v1 proxy config: (server, normalized-lowercase protocol, remote port).
// Only tcp/udp are accepted and the remote port must be positive. This is
// the ONLY source of truth for "which port does this rule really lock" —
// the commit never trusts the caller's reservation keys alone and never
// infers keys from snapshot plaintext.
func typedProxyPhysicalKey(serverID string, cfg v1.TypedProxyConfig) (ReservationKey, error) {
	base := cfg.GetBaseConfig()
	if base == nil {
		return ReservationKey{}, fmt.Errorf("typed proxy config has no base config")
	}
	protocol := strings.ToLower(strings.TrimSpace(base.Type))
	if protocol != "tcp" && protocol != "udp" {
		return ReservationKey{}, fmt.Errorf("unsupported proxy type %q (only tcp/udp)", base.Type)
	}
	var remotePort int
	switch p := cfg.ProxyConfigurer.(type) {
	case *v1.TCPProxyConfig:
		remotePort = p.RemotePort
	case *v1.UDPProxyConfig:
		remotePort = p.RemotePort
	default:
		return ReservationKey{}, fmt.Errorf("proxy %q has no remote port", base.Name)
	}
	if remotePort <= 0 {
		return ReservationKey{}, fmt.Errorf("proxy %q has invalid remote port %d", base.Name, remotePort)
	}
	return ReservationKey{ServerID: serverID, Protocol: protocol, RemotePort: uint32(remotePort)}, nil
}

// proxyRowPhysicalKey parses a proxy_config row's typed content and returns
// its physical reservation key (server from the row, protocol+port from the
// content). Used by the S1 old-row guard and the stopped branch — the
// reservation keys a commit carries must equal the keys the rows really
// run, for active AND stopped rules.
func proxyRowPhysicalKey(row *models.ProxyConfig) (ReservationKey, error) {
	typed, err := row.GetTypedProxyConfig()
	if err != nil {
		return ReservationKey{}, fmt.Errorf("content is not a parseable typed proxy config: %v", err)
	}
	return typedProxyPhysicalKey(row.ServerID, typed)
}

// validateActivePhysicalKey enforces the P0 contract on the NEW side (S0,
// zero DB writes): the applied client config must contain EXACTLY ONE proxy
// named ProxyName, that proxy must be tcp/udp, and its real physical key
// (server = Client.ServerID) must equal Reservation.NewKey. A config that
// runs a different port than the plan locks is a malformed commit.
func validateActivePhysicalKey(invalid func(string, ...interface{}) error, client *models.ClientEntity, proxyName string, newKey ReservationKey) error {
	proxies, err := utils.LoadProxiesFromContent(client.ConfigContent)
	if err != nil {
		return invalid("cannot parse the applied client config: %v", err)
	}
	var matches []v1.TypedProxyConfig
	for _, p := range proxies {
		if base := p.GetBaseConfig(); base != nil && base.Name == proxyName {
			matches = append(matches, p)
		}
	}
	if len(matches) != 1 {
		return invalid("applied client config must contain exactly one proxy named %q (found %d)", proxyName, len(matches))
	}
	key, err := typedProxyPhysicalKey(client.ServerID, matches[0])
	if err != nil {
		return invalid("applied proxy %q: %v", proxyName, err)
	}
	if key != newKey {
		return invalid("applied proxy %q physical key %s/%s/%d differs from the new reservation key %s/%s/%d",
			proxyName, key.ServerID, key.Protocol, key.RemotePort,
			newKey.ServerID, newKey.Protocol, newKey.RemotePort)
	}
	return nil
}
