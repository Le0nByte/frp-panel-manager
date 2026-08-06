//go:build windows

package portinspector

import (
	"context"
)

// windowsScanner relies on gopsutil (GetExtendedTcpTable/UdpTable).
// /proc and the docker unix socket do not exist on Windows.
type windowsScanner struct{}

func newScanner() Inspector { return windowsScanner{} }

func (s windowsScanner) ScanPorts(ctx context.Context, opts ScanOptions) ([]Observation, error) {
	obs, _, err := collectListenersGopsutil(ctx, opts)
	return obs, err
}

func (s windowsScanner) ValidatePort(ctx context.Context, opts ValidateOptions) (Observation, error) {
	return validatePortWith(s, ctx, opts)
}
