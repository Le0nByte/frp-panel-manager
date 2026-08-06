package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/utils/logger"
)

// DefaultRecoveryInterval is the production sweep period (spec §6.1.4:
// the background reclaim task scans expired reservations every 30s).
const DefaultRecoveryInterval = 30 * time.Second

// RecoveryRunner performs ONE system recovery pass (spec §6.1.3/§6.1.4).
// The production implementation is the DAO sweep; tests inject fakes.
type RecoveryRunner interface {
	RunOnce(c *app.Context, now time.Time) (*dao.SystemRecoveryResult, error)
}

// SystemRecoveryRunner is the production runner: the fail-closed DAO sweep
// over ALL tenants in one transaction — expired pending reservations (phase
// A) plus stale no-reservation transactions (phase B, Recovery A2).
type SystemRecoveryRunner struct{}

func (SystemRecoveryRunner) RunOnce(c *app.Context, now time.Time) (*dao.SystemRecoveryResult, error) {
	return dao.SystemRecoverExpiredPending(c, now)
}

// RecoveryScheduler runs the fail-closed transaction/port recovery: one
// pass on demand (RunOnce, used at Master startup) and one pass every
// Interval until the context is cancelled. Runner, Interval and Now are
// injectable so deterministic tests can run at millisecond cadence.
type RecoveryScheduler struct {
	Runner   RecoveryRunner
	Interval time.Duration
	Now      func() time.Time
}

// NewRecoveryScheduler returns a scheduler with the production clock.
func NewRecoveryScheduler(runner RecoveryRunner, interval time.Duration) *RecoveryScheduler {
	return &RecoveryScheduler{
		Runner:   runner,
		Interval: interval,
		Now:      time.Now,
	}
}

// RunOnce executes a single recovery pass and returns its result.
func (s *RecoveryScheduler) RunOnce(c *app.Context) (*dao.SystemRecoveryResult, error) {
	return s.Runner.RunOnce(c, s.Now())
}

// Run blocks until ctx is cancelled, executing one recovery pass per
// Interval. It never runs a pass at t=0 (the startup pass is RunOnce,
// invoked separately before the ticker starts). Failures are logged and
// the sweep continues — a failed pass must never stop the loop, and the
// next tick re-evaluates from the fresh state.
func (s *RecoveryScheduler) Run(ctx context.Context, c *app.Context) {
	interval := s.Interval
	if interval <= 0 {
		interval = DefaultRecoveryInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.RunOnce(c); err != nil {
				logger.Logger(c).WithError(err).Errorf("periodic transaction/port recovery failed")
			}
		}
	}
}

// StartRecoveryLoop wires the startup pass + periodic sweep for the Master
// lifecycle (spec §6.1.3/§6.1.4):
//
//   - the startup pass runs IMMEDIATELY; when it fails the error is
//     returned and the writable Master must NOT start (an unrecovered
//     lease state means the port bookkeeping cannot be trusted);
//   - on success a goroutine runs the periodic sweep until ctx is
//     cancelled; the caller MUST cancel ctx and drain the returned done
//     channel in OnStop (no goroutine leak).
func StartRecoveryLoop(ctx context.Context, c *app.Context, sched *RecoveryScheduler) (done <-chan struct{}, err error) {
	if sched == nil {
		return nil, fmt.Errorf("startup transaction/port recovery failed: nil scheduler")
	}
	if _, err := sched.RunOnce(c); err != nil {
		return nil, fmt.Errorf("startup transaction/port recovery failed: %w", err)
	}
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		sched.Run(ctx, c)
	}()
	return doneCh, nil
}
