//go:build linux || android

package portinspector

import (
	"context"
	"fmt"
)

// linuxScanner prefers gopsutil and falls back to /proc (spec §5.3).
type linuxScanner struct{}

func newScanner() Inspector { return linuxScanner{} }

func (s linuxScanner) ScanPorts(ctx context.Context, opts ScanOptions) ([]Observation, error) {
	obs, _, err := collectListenersGopsutil(ctx, opts)
	if err == nil && len(obs) > 0 {
		return obs, nil
	}
	raw, perr := readProcNetSockets(ctx)
	if perr != nil {
		if err != nil {
			return nil, fmt.Errorf("gopsutil scan failed: %w; /proc fallback failed: %v", err, perr)
		}
		return nil, perr
	}
	filtered := filterRawByOpts(raw, opts)
	return groupAndEnrich(ctx, filtered, opts), nil
}

func (s linuxScanner) ValidatePort(ctx context.Context, opts ValidateOptions) (Observation, error) {
	return validatePortWith(s, ctx, opts)
}
