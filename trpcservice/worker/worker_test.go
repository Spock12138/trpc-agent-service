package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
)

type fakeExecutor struct {
	mu       sync.Mutex
	attempts int
	failFor  int
	block    time.Duration
}

func TestClassifyToolGovernanceErrors(t *testing.T) {
	if code, retry := classify(governance.ErrToolForbidden); code != "tool_rejected" || retry {
		t.Fatalf("forbidden=(%q,%t)", code, retry)
	}
	if code, retry := classify(governance.ErrDangerousConfirmation); code != "confirmation_required" || retry {
		t.Fatalf("confirmation=(%q,%t)", code, retry)
	}
}

type denyingExecutor struct {
	fakeExecutor
	err error
}

func (e *denyingExecutor) AuthorizeTask(context.Context, message.ExecutionTask) error { return e.err }

func (f *fakeExecutor) Ready(context.Context) error { return nil }

func (f *fakeExecutor) Execute(ctx context.Context, task message.ExecutionTask) (message.OutboundMessage, error) {
	f.mu.Lock()
	f.attempts++
	attempt := f.attempts
	f.mu.Unlock()
	if f.block > 0 {
		timer := time.NewTimer(f.block)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return message.OutboundMessage{}, ctx.Err()
		case <-timer.C:
		}
	}
	if attempt <= f.failFor {
		return message.OutboundMessage{}, executor.ErrAgentFailed
	}
	return message.OutboundMessage{
		Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID,
		TraceID: task.TraceID, SessionID: task.SessionID, Text: "ok",
	}, nil
}

func (f *fakeExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func TestWorkerRetriesThenCompletes(t *testing.T) {
	store := newWorkerStore(t, 20*time.Millisecond, 200*time.Millisecond)
	exec := &fakeExecutor{failFor: 1}
	service, err := New(store, exec, "worker-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	waitReady(t, service)
	task := workerTask("retry")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	snapshot := waitTerminal(t, store, task.InboxID(), 4*time.Second)
	if snapshot.State != messaging.StateSucceeded || exec.count() != 2 {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPolicyDenialDoesNotExecuteOrAcquireSession(t *testing.T) {
	store := newWorkerStore(t, 20*time.Millisecond, 200*time.Millisecond)
	exec := &denyingExecutor{err: governance.ErrActorForbidden}
	service, err := New(store, exec, "worker-governance")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	waitReady(t, service)
	task := workerTask("actor-forbidden")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	snapshot := waitTerminal(t, store, task.InboxID(), 3*time.Second)
	if snapshot.ErrorCode != "actor_forbidden" || exec.count() != 0 {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHeartbeatPreventsStaleClaimDuringExecution(t *testing.T) {
	store := newWorkerStore(t, 10*time.Millisecond, 90*time.Millisecond)
	exec := &fakeExecutor{block: 300 * time.Millisecond}
	service, _ := New(store, exec, "worker-heartbeat")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	waitReady(t, service)
	task := workerTask("heartbeat")
	_, _, _ = store.Submit(context.Background(), task)
	deadline := time.Now().Add(time.Second)
	for exec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(180 * time.Millisecond)
	claimed, err := store.ClaimStale(context.Background(), "other-worker", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("active task was claimed: %#v", claimed)
	}
	waitTerminal(t, store, task.InboxID(), 2*time.Second)
	cancel()
	_ = <-done
}

func TestWorkerRejectsTamperedTaskWithoutExecutingAgent(t *testing.T) {
	store := newWorkerStore(t, 10*time.Millisecond, 90*time.Millisecond)
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	exec := &fakeExecutor{}
	service, _ := New(store, exec, "worker-tamper")
	task := workerTask("tamper")
	_, _, _ = store.Submit(context.Background(), task)
	delivery, err := store.ReadTask(context.Background(), "worker-tamper", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	delivery.Task.TenantID = "tenant-other"
	delivery.Task.PayloadDigest = delivery.Task.CanonicalDigest()
	service.process(context.Background(), delivery)
	snapshot := waitTerminal(t, store, task.InboxID(), time.Second)
	if snapshot.ErrorCode != "invalid_task" || exec.count() != 0 {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
}

func TestRepeatedGracefulShutdownRespectsMaxAttempts(t *testing.T) {
	store := newWorkerStore(t, 10*time.Millisecond, 90*time.Millisecond)
	exec := &fakeExecutor{block: time.Minute}
	task := workerTask("shutdown-limit")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= store.Config().MaxAttempts; attempt++ {
		service, err := New(store, exec, fmt.Sprintf("worker-shutdown-%d", attempt))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- service.Run(ctx) }()
		waitReady(t, service)
		deadline := time.Now().Add(2 * time.Second)
		for exec.count() < attempt && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if exec.count() != attempt {
			cancel()
			t.Fatalf("attempt %d did not start", attempt)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	snapshot := waitTerminal(t, store, task.InboxID(), time.Second)
	if snapshot.ErrorCode != "worker_shutdown" || exec.count() != store.Config().MaxAttempts {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
}

func newWorkerStore(t *testing.T, backoff, lease time.Duration) *messaging.Store {
	t.Helper()
	server := miniredis.RunT(t)
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "worker-" + fmt.Sprint(time.Now().UnixNano()),
		LeaseDuration: lease, HeartbeatInterval: lease / 4,
		InitialBackoff: backoff, MaxBackoff: backoff * 4, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
	}
	store, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func workerTask(id string) message.ExecutionTask {
	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: "task-" + id, Channel: "demo", ChannelBindingID: "binding", ExternalAccountID: "demo-account",
		TenantID: "tenant", AgentAppID: "app", ConfigVersion: "v1", RunnerUserID: "u", SessionID: "s",
		PlatformMessageID: "message-" + id, ActorUserID: "actor", ConversationID: "conversation", ConversationType: message.ConversationDirect,
		Text: "hello", RequestID: "request", TraceID: "trace",
		ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}

func waitReady(t *testing.T, service *Worker) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := service.Ready(context.Background()); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker did not become ready")
}

func waitTerminal(t *testing.T, store *messaging.Store, inboxID string, timeout time.Duration) messaging.Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background(), inboxID)
		if err == nil && snapshot.Terminal() {
			return snapshot
		}
		if err != nil && !errors.Is(err, messaging.ErrInboxMissing) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task did not reach a terminal state")
	return messaging.Snapshot{}
}
