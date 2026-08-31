// Package worker consumes reliable tasks and executes them with a multi-tenant
// Runtime. Phase 3 intentionally processes one task at a time.
package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
)

type Executor interface {
	Ready(context.Context) error
	Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error)
}

type Worker struct {
	store    *messaging.Store
	executor Executor
	consumer string

	running atomic.Bool
	mu      sync.Mutex
	cancel  context.CancelFunc
}

func New(store *messaging.Store, runtime Executor, consumer string) (*Worker, error) {
	if store == nil || runtime == nil {
		return nil, errors.New("messaging store and executor are required")
	}
	if consumer == "" {
		return nil, errors.New("worker consumer name is required")
	}
	return &Worker{store: store, executor: runtime, consumer: consumer}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	if w.cancel != nil {
		w.mu.Unlock()
		cancel()
		return errors.New("worker is already running")
	}
	w.cancel = cancel
	w.mu.Unlock()
	defer func() {
		cancel()
		w.running.Store(false)
		w.mu.Lock()
		w.cancel = nil
		w.mu.Unlock()
	}()

	if err := w.store.Ready(runCtx); err != nil {
		return err
	}
	w.running.Store(true)
	for {
		if runCtx.Err() != nil {
			return nil
		}
		_, _ = w.store.PromoteRetries(runCtx, 32)
		stale, err := w.store.ClaimStale(runCtx, w.consumer, 16)
		if err == nil {
			for _, delivery := range stale {
				_ = w.store.Recover(runCtx, delivery)
			}
		}

		delivery, err := w.store.ReadTask(runCtx, w.consumer, time.Second)
		if err != nil {
			if delivery.StreamID != "" {
				_ = w.store.Reject(context.Background(), delivery, "invalid_task")
				continue
			}
			if errors.Is(err, redis.Nil) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if runCtx.Err() != nil {
				return nil
			}
			if !waitContext(runCtx, 250*time.Millisecond) {
				return nil
			}
			continue
		}
		w.process(runCtx, delivery)
	}
}

func (w *Worker) process(runCtx context.Context, delivery messaging.Delivery) {
	if err := delivery.Task.Validate(); err != nil {
		_ = w.store.Reject(context.Background(), delivery, "invalid_task")
		return
	}
	snapshot, err := w.store.Snapshot(runCtx, delivery.InboxID)
	if err != nil || delivery.InboxID != delivery.Task.InboxID() || snapshot.TaskID != delivery.Task.TaskID || !message.ConstantTimeDigestEqual(snapshot.Digest, delivery.Task.PayloadDigest) {
		_ = w.store.Reject(context.Background(), delivery, "invalid_task")
		return
	}
	lease, err := w.store.Begin(runCtx, delivery, w.consumer)
	if err != nil {
		if errors.Is(err, messaging.ErrLeaseLost) {
			_ = w.store.RequeueAfterBeginFailure(context.Background(), delivery)
		}
		return
	}
	execCtx, cancel := context.WithCancel(runCtx)
	heartbeatDone := make(chan error, 1)
	go w.heartbeat(execCtx, cancel, lease, heartbeatDone)
	reply, executeErr := w.executor.Execute(execCtx, delivery.Task)
	cancel()
	heartbeatErr := <-heartbeatDone
	if heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
		return
	}

	transitionCtx, transitionCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer transitionCancel()
	if runCtx.Err() != nil || errors.Is(executeErr, context.Canceled) {
		w.retryOrFail(transitionCtx, lease, "worker_shutdown", true)
		return
	}
	if executeErr == nil {
		_ = w.store.Complete(transitionCtx, lease, reply)
		return
	}
	code, retryable := classify(executeErr)
	if !retryable || delivery.Task.Attempt >= w.store.Config().MaxAttempts {
		_ = w.store.Fail(transitionCtx, lease, code)
		return
	}
	w.retryOrFail(transitionCtx, lease, code, false)
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, lease messaging.Lease, done chan<- error) {
	ticker := time.NewTicker(w.store.Config().HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ticker.C:
			if err := w.store.Heartbeat(ctx, lease); err != nil {
				cancel()
				done <- err
				return
			}
		}
	}
}

func (w *Worker) retryOrFail(ctx context.Context, lease messaging.Lease, code string, immediate bool) {
	if lease.Delivery.Task.Attempt >= w.store.Config().MaxAttempts {
		_ = w.store.Fail(ctx, lease, code)
		return
	}
	_ = w.store.Retry(ctx, lease, code, immediate)
}

func classify(err error) (string, bool) {
	switch {
	case errors.Is(err, executor.ErrUnknownBinding), errors.Is(err, executor.ErrConfigurationUnavailable):
		return "configuration_unavailable", false
	case errors.Is(err, executor.ErrAgentTimeout):
		return "model_timeout", true
	case errors.Is(err, executor.ErrDependencyUnavailable), errors.Is(err, executor.ErrRunnerDraining):
		return "dependency_unavailable", true
	case errors.Is(err, executor.ErrEmptyAgentResponse):
		return "empty_agent_response", true
	default:
		return "agent_failed", true
	}
}

func (w *Worker) Ready(ctx context.Context) error {
	if !w.running.Load() {
		return errors.New("worker loop is not running")
	}
	if err := w.store.Ready(ctx); err != nil {
		return err
	}
	return w.executor.Ready(ctx)
}

func (w *Worker) Close() error {
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
