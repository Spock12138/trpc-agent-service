package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type degradedRepository struct {
	control.Repository
	placementErr error
	mu           sync.Mutex
	reason       string
}

func (r *degradedRepository) GetPlacement(context.Context, string) (control.TenantPlacement, error) {
	return control.TenantPlacement{}, r.placementErr
}

func (r *degradedRepository) SetTenantDegraded(_ context.Context, _ string, reason string) error {
	r.mu.Lock()
	r.reason = reason
	r.mu.Unlock()
	return nil
}

func (r *degradedRepository) degradedReason() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reason
}

func TestChooseNodeCapacityAndDedicatedPlacement(t *testing.T) {
	now := time.Now().UTC()
	nodes := []control.NodeRecord{
		testNode("node-a", 4, 3, now),
		testNode("node-b", 2, 0, now),
	}
	node, err := chooseNode("inbox", control.DefaultTenantPlacement("tenant-a", now), nodes, now)
	if err != nil || node.NodeID != "node-b" {
		t.Fatalf("capacity selection = (%#v, %v)", node, err)
	}
	dedicated := control.DefaultTenantPlacement("tenant-a", now)
	dedicated.Mode = control.PlacementDedicated
	dedicated.NodeID = "node-a"
	node, err = chooseNode("inbox", dedicated, nodes, now)
	if err != nil || node.NodeID != "node-a" {
		t.Fatalf("dedicated selection = (%#v, %v)", node, err)
	}
	nodes[0].State = control.NodeOffline
	if _, err := chooseNode("inbox", dedicated, nodes, now); !errors.Is(err, ErrDedicatedNodeOffline) {
		t.Fatalf("offline dedicated selection = %v", err)
	}
}

func TestReconcilerOwnersAreUnique(t *testing.T) {
	first := NewReconciler(nil, nil)
	second := NewReconciler(nil, nil)
	if first.owner == second.owner {
		t.Fatalf("reconciler owners collided: %q", first.owner)
	}
}

func TestSchedulerSubmitAndControllerAdmission(t *testing.T) {
	repository, store, _ := testDependencies(t)
	ctx := context.Background()
	controller, err := NewController(repository, store, testControlConfig(), "node-a", "v6")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controller.Close() })
	waitControllerReady(t, controller)
	service, err := New(repository, store, true)
	if err != nil {
		t.Fatal(err)
	}
	task := testTask("task-a", "message-a")
	snapshot, created, err := service.Submit(ctx, task)
	if err != nil || !created || snapshot.NodeID != "node-a" {
		t.Fatalf("scheduled submit = (%#v, %t, %v)", snapshot, created, err)
	}
	delivery, err := store.ReadTask(ctx, "claiming-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := controller.Admit(ctx, delivery)
	if err != nil || !admitted {
		t.Fatalf("node admission = (%t, %v)", admitted, err)
	}
	snapshot, _ = store.Snapshot(ctx, task.InboxID())
	if snapshot.AssignmentState != string(control.AssignmentAdmitted) {
		t.Fatalf("assignment projection state = %q", snapshot.AssignmentState)
	}
}

func TestControllerMarksTenantDegradedWhenPlacementIsUnavailable(t *testing.T) {
	repository, store, _ := testDependencies(t)
	tracking := &degradedRepository{Repository: repository, placementErr: control.ErrUnavailable}
	controller, err := NewController(tracking, store, testControlConfig(), "node-a", "v6")
	if err != nil {
		t.Fatal(err)
	}
	task := testTask("task-placement-failure", "message-placement-failure")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-placement-failure", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := controller.Admit(context.Background(), delivery)
	if admitted || !errors.Is(err, ErrPlacementUnavailable) {
		t.Fatalf("Admit = (%t, %v)", admitted, err)
	}
	if reason := tracking.degradedReason(); reason != "tenant_placement_unavailable" {
		t.Fatalf("degraded reason = %q", reason)
	}
}

func TestReconcilerReassignsSharedAndBlocksDedicated(t *testing.T) {
	repository, store, redisServer := testDependencies(t)
	ctx := context.Background()
	now := time.Now().UTC()
	nodeA := testNode("node-a", 1, 0, now)
	nodeA.LeaseUntil = now.Add(time.Second)
	if err := repository.RegisterNode(ctx, nodeA); err != nil {
		t.Fatal(err)
	}
	redisServer.FastForward(2 * time.Second)
	nodeB := testNode("node-b", 1, 0, now)
	if err := repository.RegisterNode(ctx, nodeB); err != nil {
		t.Fatal(err)
	}
	sharedTask := testTask("task-shared", "message-shared")
	shared := testAssignment(sharedTask, "node-a", control.PlacementShared, now)
	if _, _, err := repository.CreateAssignment(ctx, shared); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.SubmitAssigned(ctx, sharedTask, shared); err != nil {
		t.Fatal(err)
	}
	reconciler := NewReconciler(repository, store)
	if err := reconciler.ReconcileOnce(ctx, 10); err != nil {
		t.Fatal(err)
	}
	shared, err := repository.GetAssignment(ctx, shared.InboxID)
	if err != nil || shared.NodeID != "node-b" || shared.Revision != 2 {
		t.Fatalf("shared reassignment = (%#v, %v)", shared, err)
	}

	placement, _ := repository.GetPlacement(ctx, "tenant-a")
	placement.Mode = control.PlacementDedicated
	placement.NodeID = "node-a"
	if _, err := repository.PutPlacement(ctx, placement, placement.Revision); err != nil {
		t.Fatal(err)
	}
	dedicatedTask := testTask("task-dedicated", "message-dedicated")
	dedicated := testAssignment(dedicatedTask, "node-a", control.PlacementDedicated, now)
	if _, _, err := repository.CreateAssignment(ctx, dedicated); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.SubmitAssigned(ctx, dedicatedTask, dedicated); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx, 10); err != nil {
		t.Fatal(err)
	}
	dedicated, err = repository.GetAssignment(ctx, dedicated.InboxID)
	if err != nil || dedicated.State != control.AssignmentBlocked || dedicated.NodeID != "node-a" || dedicated.BlockedReason != "dedicated_node_offline" {
		t.Fatalf("dedicated block = (%#v, %v)", dedicated, err)
	}
}

func testDependencies(t *testing.T) (control.Repository, *messaging.Store, *miniredis.Miniredis) {
	t.Helper()
	redisServer := miniredis.RunT(t)
	controlConfig := *testControlConfig()
	controlConfig.RedisEndpoint = "redis://" + redisServer.Addr()
	controlConfig.RedisURL = "redis://" + redisServer.Addr() + "/4"
	controlConfig.LogicalDB = 4
	rawRepository, err := control.NewRedisRepository(controlConfig)
	if err != nil {
		t.Fatal(err)
	}
	repository := control.NewInitializingRepository(rawRepository, []tenant.Tenant{{ID: "tenant-a", Enabled: true}}, []string{})
	t.Cleanup(func() { _ = repository.Close() })
	if err := repository.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := messaging.NewStore(config.MessagingConfig{
		RedisURL: "redis://" + redisServer.Addr() + "/0", KeyPrefix: "phase6-scheduler",
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: time.Second, MaxBackoff: 5 * time.Second, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	return repository, store, redisServer
}

func testControlConfig() *config.ControlPlaneConfig {
	return &config.ControlPlaneConfig{
		RedisEndpoint: "redis://localhost:6379", RedisURL: "redis://localhost:6379/4", LogicalDB: 4,
		KeyPrefix: "phase6-control", PolicyCacheTTL: 5 * time.Minute,
		AuditRetention: 30 * 24 * time.Hour, MetricRetention: 7 * 24 * time.Hour,
		NodeHeartbeatInterval: 10 * time.Millisecond, NodeOfflineAfter: 300 * time.Millisecond,
		NodeAssignmentWaitBackoff: 10 * time.Millisecond, TraceDigestV2Enabled: true,
		NodeAssignmentEnabled: true, DevelopmentAllowSharedRedis: true,
	}
}

func testNode(id string, capacity, inflight int, now time.Time) control.NodeRecord {
	return control.NodeRecord{
		NodeID: id, Role: "worker", State: control.NodeReady, Capacity: capacity, Inflight: inflight,
		BuildVersion: "v6", ProtocolCapabilities: []string{"governance.v1", "trace_digest_v2", "node_assignment"},
		BootID: "boot-" + id, LeaseUntil: now.Add(time.Minute), StartedAt: now, LastHeartbeat: now,
	}
}

func testTask(taskID, messageID string) message.ExecutionTask {
	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: taskID, Channel: "demo", ChannelBindingID: "binding-a",
		ExternalAccountID: "account-a", TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1",
		RunnerUserID: "user-a", SessionID: "session-a", PlatformMessageID: messageID, ActorUserID: "actor-a",
		ConversationID: "conversation-a", ConversationType: message.ConversationDirect, Text: "hello",
		RequestID: "request-a", TraceID: "trace-a", ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}

func testAssignment(task message.ExecutionTask, nodeID string, mode control.PlacementMode, now time.Time) control.NodeAssignment {
	return control.NodeAssignment{
		InboxID: task.InboxID(), TenantID: task.TenantID, AgentAppID: task.AgentAppID,
		PayloadDigest: task.PayloadDigest, NodeID: nodeID, Mode: mode, State: control.AssignmentPlanned,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func waitControllerReady(t *testing.T, controller *Controller) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if controller.Ready(context.Background()) == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("controller did not become ready")
}
