//go:build !linux && !android && !windows

package portinspector

import (
	"context"
)

// otherScanner covers any remaining GOOS with gopsutil only.
type otherScanner struct{}

func newScanner() Inspector { return otherScanner{} }

func (s otherScanner) ScanPorts(ctx context.Context, opts ScanOptions) ([]Observation, error) {
	obs, _, err := collectListenersGopsutil(ctx, opts)
	return obs, err
}

func (s otherScanner) ValidatePort(ctx context.Context, opts ValidateOptions) (Observation, error) {
	return validatePortWith(s, ctx, opts)
}
