package gateway

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestGatewayWaitsForReliableReplyAndCachesResult(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, time.Second)
	defer func() { cancel(); _ = <-done }()
	inbound := gatewayInbound("message-1", "hello", "request-1")
	replyCh := make(chan message.OutboundMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		reply, err := service.Handle(context.Background(), inbound)
		replyCh <- reply
		errCh <- err
	}()
	delivery, err := store.ReadTask(context.Background(), "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), lease, message.OutboundMessage{
		Channel: "demo", BindingID: "binding", RequestID: delivery.Task.RequestID,
		TraceID: delivery.Task.TraceID, SessionID: delivery.Task.SessionID, Text: "answer",
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if reply := <-replyCh; reply.Text != "answer" || reply.RequestID != "request-1" || reply.BindingID != "binding" {
		t.Fatalf("unexpected reply: %#v", reply)
	}

	duplicate := inbound
	duplicate.RequestID = "request-2"
	reply, err := service.Handle(context.Background(), duplicate)
	if err != nil || reply.RequestID != "request-2" || reply.Text != "answer" {
		t.Fatalf("cached Handle() = (%#v, %v)", reply, err)
	}
	conflict := duplicate
	conflict.Text = "changed"
	conflictReply, err := service.Handle(context.Background(), conflict)
	if !errors.Is(err, ErrMessageConflict) || conflictReply.TraceID != inbound.TraceID {
		t.Fatalf("conflicting Handle() error = %v", err)
	}
}

func TestGatewayTimeoutLeavesTaskPending(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, 50*time.Millisecond)
	defer func() { cancel(); _ = <-done }()
	inbound := gatewayInbound("message-timeout", "hello", "request-timeout")
	reply, err := service.Handle(context.Background(), inbound)
	if !errors.Is(err, ErrTaskPending) {
		t.Fatalf("Handle() error = %v, want ErrTaskPending", err)
	}
	if reply.TraceID != inbound.TraceID {
		t.Fatalf("pending trace_id = %q, want original %q", reply.TraceID, inbound.TraceID)
	}
	snapshotTask, err := service.router.Resolve(context.Background(), inbound)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), snapshotTask.InboxID())
	if err != nil || snapshot.State != messaging.StateQueued {
		t.Fatalf("pending snapshot = (%#v, %v)", snapshot, err)
	}
}

func TestGatewayCachedFailureKeepsOriginalTaskTrace(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, time.Second)
	defer func() { cancel(); _ = <-done }()
	inbound := gatewayInbound("message-failed", "hello", "request-first")
	firstReply := make(chan message.OutboundMessage, 1)
	firstErr := make(chan error, 1)
	go func() {
		reply, err := service.Handle(context.Background(), inbound)
		firstReply <- reply
		firstErr <- err
	}()
	delivery, err := store.ReadTask(context.Background(), "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), lease, "model_timeout"); err != nil {
		t.Fatal(err)
	}
	if err := <-firstErr; !errors.Is(err, executor.ErrAgentTimeout) {
		t.Fatalf("first Handle() error = %v", err)
	}
	if reply := <-firstReply; reply.TraceID != inbound.TraceID {
		t.Fatalf("first failure trace_id = %q", reply.TraceID)
	}

	duplicate := inbound
	duplicate.RequestID = "request-second"
	duplicate.TraceID = "trace-second"
	reply, err := service.Handle(context.Background(), duplicate)
	if !errors.Is(err, executor.ErrAgentTimeout) || reply.TraceID != inbound.TraceID {
		t.Fatalf("cached failure = (%#v, %v)", reply, err)
	}
}

func newGatewayService(t *testing.T, wait time.Duration) (*Service, *messaging.Store, context.CancelFunc, <-chan error) {
	t.Helper()
	server := miniredis.RunT(t)
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "gateway-" + fmt.Sprint(time.Now().UnixNano()),
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: time.Second, MaxBackoff: 5 * time.Second, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: wait,
	}
	store, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	repository, err := tenant.NewPresetRepository(gatewayCatalog())
	if err != nil {
		t.Fatal(err)
	}
	router, err := routing.New(repository, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(router, store, "gateway-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if service.Ready(context.Background()) == nil {
			return service, store, cancel, done
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	t.Fatal("gateway did not become ready")
	return nil, nil, nil, nil
}

func gatewayInbound(messageID, text, requestID string) message.InboundMessage {
	return message.InboundMessage{
		Channel: "demo", BindingID: "binding", MessageID: messageID,
		ExternalUserID: "user", ConversationID: "conversation", Text: text,
		RequestID: requestID, TraceID: "trace", ReceivedAt: time.Now().UTC(),
	}
}

func gatewayCatalog() tenant.Catalog {
	return tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant", ID: "memory", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant", ID: "app", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{{
			TenantID: "tenant", AgentAppID: "app", Version: "v1", StorageProfileID: "memory", Instruction: "test",
			Model: tenant.ModelConfig{Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL", RequestTimeout: time.Second, MaxOutputTokens: 32},
		}},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding", Channel: "demo", ExternalAccountID: "binding", TenantID: "tenant", AgentAppID: "app", Enabled: true,
		}},
	}
}
