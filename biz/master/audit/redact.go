package audit

import (
	"encoding/json"
	"strings"
)

// sensitiveMarkers are the substrings that make a historical summary or an
// evidence JSON key untrusted. The write-side coordinators already redact,
// but the append-only trail may contain rows written before those guards
// existed (or tampered rows), and no such content may ever reach the UI.
var sensitiveMarkers = []string{"secret", "password", "token", "config", "plaintext", "ciphertext"}

// redactedSummary is the generic placeholder returned instead of any
// summary carrying a sensitive marker.
const redactedSummary = "[redacted]"

// maxEvidenceBytes bounds the evidence payload the GetAudit handler may
// return to the UI (spec D2c-B ruling: 64KB).
const maxEvidenceBytes = 64 * 1024

// containsSensitiveMarker reports whether the lowercased string carries any
// sensitive marker.
func containsSensitiveMarker(lower string) bool {
	for _, m := range sensitiveMarkers {
		if strings.Contains(lower, m) {
			return true
		}
	}
	return false
}

// sanitizeSummary redacts a historical audit summary: summaries carrying a
// sensitive marker are replaced by the generic [redacted] placeholder;
// legitimate known summaries (written by the coordinator redaction guards)
// pass through untouched.
func sanitizeSummary(summary string) string {
	if summary == "" {
		return summary
	}
	if containsSensitiveMarker(strings.ToLower(summary)) {
		return redactedSummary
	}
	return summary
}

// evidenceContainsSensitiveKey recursively walks a decoded JSON value and
// reports whether ANY object key at any nesting depth carries a sensitive
// marker. Values are not inspected: evidence values are already
// redacted/structured, the risk is a dirty key name smuggling content.
func evidenceContainsSensitiveKey(v any) bool {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			if containsSensitiveMarker(strings.ToLower(k)) {
				return true
			}
			if evidenceContainsSensitiveKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range val {
			if evidenceContainsSensitiveKey(child) {
				return true
			}
		}
	}
	return false
}

// sanitizeEvidence validates the transaction evidence JSON before it leaves
// the server: it must be valid JSON, at most 64KB, and free of sensitive
// keys at any nesting depth. On any violation ok=false and the caller must
// reply with EMPTY evidence — the original bytes never reach the caller.
// An empty (never-written) evidence is compliant.
func sanitizeEvidence(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}
	if len(raw) > maxEvidenceBytes {
		return false
	}
	if !json.Valid(raw) {
		return false
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return false
	}
	return !evidenceContainsSensitiveKey(v)
}
