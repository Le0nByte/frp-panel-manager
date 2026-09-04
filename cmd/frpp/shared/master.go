package shared

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/VaalaCat/frp-panel/biz/master/auth"
	masterclient "github.com/VaalaCat/frp-panel/biz/master/client"
	"github.com/VaalaCat/frp-panel/biz/master/proxy"
	"github.com/VaalaCat/frp-panel/biz/master/readonly"
	"github.com/VaalaCat/frp-panel/conf"
	"github.com/VaalaCat/frp-panel/services/app"
	"github.com/VaalaCat/frp-panel/services/cache"
	"github.com/VaalaCat/frp-panel/services/dao"
	"github.com/VaalaCat/frp-panel/services/master"
	"github.com/VaalaCat/frp-panel/services/mux"
	"github.com/VaalaCat/frp-panel/services/rpcguard"
	"github.com/VaalaCat/frp-panel/services/watcher"
	"github.com/VaalaCat/frp-panel/utils/logger"
	"github.com/VaalaCat/frp-panel/utils/wsgrpc"
	"github.com/gin-gonic/gin"
	"github.com/sourcegraph/conc"
	"go.uber.org/fx"
)

type runMasterParam struct {
	fx.In

	Lc fx.Lifecycle

	Ctx                 *app.Context
	AppInstance         app.Application
	DBManagerMgr        app.DBManager
	HTTPMuxServer       mux.MuxServer `name:"httpMux"`
	TLSMuxServer        mux.MuxServer `name:"tlsMux"`
	MasterRouter        *gin.Engine   `name:"masterRouter"`
	ClientLogManager    app.ClientLogManager
	WsGrpcHandler       gin.HandlerFunc      `name:"wsGrpcHandler"`
	MasterService       master.MasterService `name:"wsMasterService"`
	TaskManager         watcher.Client       `name:"masterTaskManager"`
	WsListener          *wsgrpc.WSListener
	DefaultServerConfig conf.Config `name:"defaultServerConfig"`
	PermManager         app.PermissionManager
	RPCGuard            *rpcguard.Guard
}

func runMaster(param runMasterParam) {

	param.AppInstance.SetClientLogManager(param.ClientLogManager)
	param.MasterRouter.GET("/wsgrpc", param.WsGrpcHandler)

	cache.InitCache(param.AppInstance.GetConfig())
	auth.InitAuth(param.AppInstance)

	param.TaskManager.AddCronTask("0 0 3 * * *", proxy.CollectDailyStats, param.AppInstance)
	defer param.TaskManager.Stop()

	logger.Logger(param.Ctx).Infof("start to run master")
	var wg conc.WaitGroup

	// The reservation recovery loop (spec §6.1.3/§6.1.4): the startup pass
	// runs before the Master serves anything. In writable mode its failure
	// aborts the whole startup — a writable Master must never start with an
	// unrecovered lease state. In read-only mode a failed startup pass is
	// degraded: recovery is only DB state cleanup, so its failure must
	// never take down the view-only entry points. The 30s periodic sweep
	// then keeps expired pending leases from accumulating. OnStop cancels
	// the loop and drains its done channel (no goroutine leak).
	var (
		recoveryCancel context.CancelFunc
		recoveryDone   <-chan struct{}
		auditLoop      *rpcAuditLoop
	)
	param.Lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Fail closed before recovery or any listener/goroutine starts: the
			// JWT/certificate signing secret must never fall back to a predictable
			// default in a Master process.
			if err := conf.ValidateMasterSecurity(param.AppInstance.GetConfig()); err != nil {
				return err
			}
			// Snapshot key fingerprint verification decides the read-only state
			// BEFORE any recovery pass, listener or goroutine starts (spec
			// §6.3.2). A DB failure aborts startup — it must never masquerade as
			// "read-only is available". A missing/mismatched key blocks read-only
			// mode and startup continues with view-only listeners.
			if err := applySnapshotReadOnlyMode(param.Ctx, param.AppInstance, param.DBManagerMgr); err != nil {
				return err
			}
			rctx, cancel := context.WithCancel(context.Background())
			done, err := startMasterRecovery(rctx, param.Ctx,
				proxy.NewRecoveryScheduler(proxy.SystemRecoveryRunner{}, proxy.DefaultRecoveryInterval))
			if err != nil {
				cancel()
				return err
			}
			if done != nil {
				recoveryCancel, recoveryDone = cancel, done
			} else {
				// Degraded (blocked + failed startup pass): no periodic loop
				// runs, so drop the context immediately.
				cancel()
			}

			// One-shot data-hygiene pass: rows written before the shared
			// injector landed keep the frp user and a PLAINTEXT frps token in
			// clients.config_content (a column the WebUI reads). The writers no
			// longer do that, but existing rows do not self-heal. Idempotent,
			// non-fatal, and skipped in read-only mode — a hygiene migration
			// must never block startup or write while the Master is blocked.
			if !readonly.Blocked() {
				if scanned, cleaned, err := masterclient.StripStoredCredentials(param.Ctx); err != nil {
					logger.Logger(param.Ctx).WithError(err).Warnf("credential strip pass failed, continuing startup")
				} else if cleaned > 0 {
					logger.Logger(param.Ctx).Warnf("credential strip: cleaned %d of %d stored client configs", cleaned, scanned)
				} else {
					logger.Logger(param.Ctx).Infof("credential strip: %d stored client configs already clean", scanned)
				}
			}

			if param.RPCGuard != nil {
				auditLoop = startRPCAuditLoop(param.Ctx, param.RPCGuard)
			}

			wg.Go(func() {
				if err := param.MasterService.GetServer().Serve(param.WsListener); err != nil {
					logger.Logger(param.Ctx).Fatalf("gRPC server error: %v", err)
				}
			})
			wg.Go(param.TLSMuxServer.Run)
			wg.Go(param.HTTPMuxServer.Run)
			wg.Go(param.TaskManager.Run)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			param.MasterService.GetServer().Stop()
			param.TLSMuxServer.Stop()
			param.HTTPMuxServer.Stop()
			param.TaskManager.Stop()
			wg.Wait()
			var auditErr error
			if auditLoop != nil {
				auditErr = auditLoop.Stop(ctx)
			}
			if recoveryCancel != nil {
				recoveryCancel()
			}
			if recoveryDone != nil {
				<-recoveryDone
			}
			return auditErr
		},
	})
}

// applySnapshotReadOnlyMode verifies the snapshot key fingerprint against
// system_metadata (spec §6.3.2) and derives the process-local read-only
// state. It runs BEFORE any recovery pass, listener or goroutine starts.
//
// It fail-closes first: Set(true, "verification pending") so any panic or
// error between entry and completion can never leave the Master writable.
// On return:
//   - DB manager/database nil, or a verification error → non-nil error
//     (startup must abort; DB trouble must never masquerade as "read-only
//     is available") and the blocked state stays set;
//   - key missing/invalid/mismatched/history bad → Set(true, generic
//     reason), nil error (startup continues read-only); the reason never
//     contains the key or the fingerprint;
//   - verified or first-use TOFU → Set(false, "") and, when the metadata
//     had no fingerprint before this call, a prominent generic TOFU
//     warning (the fingerprint value is never printed).
func applySnapshotReadOnlyMode(ctx *app.Context, appInstance app.Application, dbm app.DBManager) error {
	readonly.Set(true, "snapshot key verification pending")
	if dbm == nil {
		return fmt.Errorf("snapshot key verification: database manager unavailable")
	}
	db := dbm.GetDefaultDB()
	if db == nil {
		return fmt.Errorf("snapshot key verification: default database unavailable")
	}
	keyPath := appInstance.GetConfig().Master.SnapshotKeyFile

	// Was a fingerprint already recorded before THIS call? Used to emit
	// the TOFU warning only when this startup created the row.
	hadFingerprint, err := readonly.SnapshotKeyFingerprintRecorded(db)
	if err != nil {
		return fmt.Errorf("snapshot key verification: %w", err)
	}

	blocked, reason, err := readonly.VerifySnapshotKey(db, keyPath)
	if err != nil {
		return fmt.Errorf("snapshot key verification: %w", err)
	}
	if blocked {
		readonly.Set(true, reason)
		logger.Logger(ctx).Warnf("snapshot key verification: master runs read-only: %s", reason)
		return nil
	}
	readonly.Set(false, "")
	if !hadFingerprint {
		// TOFU: this startup recorded the fingerprint. The warning is
		// deliberately generic — never print the fingerprint value.
		logger.Logger(ctx).Warnf("snapshot key fingerprint recorded by TOFU; verify against deployment manifest/secret store")
	}
	return nil
}

// Run starts the process-local guard audit flusher after Master startup has
// completed its security, snapshot and recovery gates.
const rpcAuditDrainGrace = 10 * time.Second

type rpcAuditLoop struct {
	cancelLoop   context.CancelFunc
	cancelWriter context.CancelFunc
	done         <-chan struct{}
	grace        time.Duration
	stopOnce     sync.Once
	stopErr      error
}

func startRPCAuditLoop(appCtx *app.Context, guard *rpcguard.Guard) *rpcAuditLoop {
	loopCtx, cancelLoop := context.WithCancel(context.Background())
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	writer := dao.NewMutation(app.NewContext(writerCtx, appCtx.GetApp()))
	done := make(chan struct{})
	go func() {
		defer close(done)
		guard.Run(loopCtx, writer)
	}()
	return &rpcAuditLoop{
		cancelLoop: cancelLoop, cancelWriter: cancelWriter,
		done: done, grace: rpcAuditDrainGrace,
	}
}

func (l *rpcAuditLoop) Stop(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.stopOnce.Do(func() {
		l.cancelLoop()
		timer := time.NewTimer(l.grace)
		defer timer.Stop()
		select {
		case <-l.done:
			l.cancelWriter()
		case <-timer.C:
			l.cancelWriter()
			l.stopErr = fmt.Errorf("rpc audit final flush exceeded %s", l.grace)
		case <-ctx.Done():
			l.cancelWriter()
			l.stopErr = ctx.Err()
		}
	})
	return l.stopErr
}

// startMasterRecovery runs the startup reservation-recovery pass and applies
// the read-only degradation rule. In blocked mode a failed pass is logged
// and degraded to nil so view-only listeners remain available.
func startMasterRecovery(ctx context.Context, c *app.Context, sched *proxy.RecoveryScheduler) (done <-chan struct{}, err error) {
	done, err = proxy.StartRecoveryLoop(ctx, c, sched)
	if err != nil {
		if readonly.Blocked() {
			logger.Logger(c).WithError(err).Warnf("startup transaction/port recovery failed; continuing with read-only listeners")
			return nil, nil
		}
		return nil, err
	}
	return done, nil
}
