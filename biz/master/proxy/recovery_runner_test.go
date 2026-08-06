package proxy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRecoveryRunner counts passes and can be scripted to fail, so the
// scheduler and the startup loop are testable without touching a database.
type fakeRecoveryRunner struct {
	calls   int64
	fail    bool
	lastNow atomic.Value
	lastCtx *app.Context
}

func (f *fakeRecoveryRunner) RunOnce(c *app.Context, now time.Time) (*dao.SystemRecoveryResult, error) {
	atomic.AddInt64(&f.calls, 1)
	f.lastNow.Store(now)
	f.lastCtx = c
	if f.fail {
		return nil, errors.New("injected recovery failure")
	}
	return &dao.SystemRecoveryResult{ReleasedReservations: 1}, nil
}

func (f *fakeRecoveryRunner) callCount() int64 { return atomic.LoadInt64(&f.calls) }

func newRecoveryTestCtx() *app.Context {
	return app.NewContext(context.Background(), app.NewApp())
}

// TestRecoverySchedulerRunOnce: one pass runs immediately and returns the
// runner's result (startup path).
func TestRecoverySchedulerRunOnce(t *testing.T) {
	runner := &fakeRecoveryRunner{}
	now := time.Now()
	sched := NewRecoveryScheduler(runner, time.Minute)
	sched.Now = func() time.Time { return now }

	c := newRecoveryTestCtx()
	res, err := sched.RunOnce(c)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(1), res.ReleasedReservations)
	assert.Equal(t, int64(1), runner.callCount())
}

// TestRecoverySchedulerRunPeriodic: the ticker keeps running passes at the
// injected interval and stops promptly when the context is cancelled (no
// goroutine leak).
func TestRecoverySchedulerRunPeriodic(t *testing.T) {
	runner := &fakeRecoveryRunner{}
	sched := NewRecoveryScheduler(runner, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.Run(ctx, newRecoveryTestCtx())
	}()

	// wait for at least two periodic passes, then cancel
	deadline := time.Now().Add(2 * time.Second)
	for runner.callCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}
	assert.GreaterOrEqual(t, runner.callCount(), int64(2), "periodic passes must keep firing at the injected interval")
}

// TestRecoverySchedulerRunDefaultsInterval: a zero interval falls back to
// the production 30s default (never a hot loop).
func TestRecoverySchedulerRunDefaultsInterval(t *testing.T) {
	runner := &fakeRecoveryRunner{}
	sched := NewRecoveryScheduler(runner, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.Run(ctx, newRecoveryTestCtx())
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	assert.Equal(t, int64(0), runner.callCount(), "default 30s interval must not fire within the test window")
}

// TestStartRecoveryLoopRunsImmediatelyAndStopsOnCancel: the startup loop
// performs the immediate pass, then the periodic sweep runs until the
// context is cancelled and the done channel closes (the OnStop drain).
func TestStartRecoveryLoopRunsImmediatelyAndStopsOnCancel(t *testing.T) {
	runner := &fakeRecoveryRunner{}
	sched := NewRecoveryScheduler(runner, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done, err := StartRecoveryLoop(ctx, newRecoveryTestCtx(), sched)
	require.NoError(t, err)
	require.NotNil(t, done, "done channel must be returned for OnStop to drain")

	require.Equal(t, int64(1), runner.callCount(), "startup pass must run immediately")
	deadline := time.Now().Add(2 * time.Second)
	for runner.callCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery loop did not stop after cancellation (goroutine leak)")
	}
	assert.GreaterOrEqual(t, runner.callCount(), int64(2))
}

// TestStartRecoveryLoopFailsClosedOnStartupError: a failed startup pass
// returns the error (the writable Master must not start) and no goroutine
// is spawned.
func TestStartRecoveryLoopFailsClosedOnStartupError(t *testing.T) {
	runner := &fakeRecoveryRunner{fail: true}
	sched := NewRecoveryScheduler(runner, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, err := StartRecoveryLoop(ctx, newRecoveryTestCtx(), sched)
	require.Error(t, err, "startup recovery failure must block Master startup")
	assert.Contains(t, err.Error(), "startup transaction/port recovery failed")
	assert.Nil(t, done, "no ticker goroutine may exist after a failed startup pass")
	assert.Equal(t, int64(1), runner.callCount())
}
