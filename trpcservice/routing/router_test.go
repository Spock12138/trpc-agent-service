package routing

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRouterUsesPersonForDirectAndGroupSubjectForGroup(t *testing.T) {
	router := newTestRouter(t)
	directA := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-a", ConversationID: "user-a", ConversationType: message.ConversationDirect, PlatformMessageID: "direct-a"})
	directB := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-b", ConversationID: "user-b", ConversationType: message.ConversationDirect, PlatformMessageID: "direct-b"})
	if directA.RunnerUserID == directB.RunnerUserID || directA.SessionID == directB.SessionID {
		t.Fatalf("direct users shared scope: %#v %#v", directA, directB)
	}

	groupA := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-a", ConversationID: "group-a", ConversationType: message.ConversationGroup, PlatformMessageID: "group-a-1"})
	groupB := resolveTestInbound(t, router, message.InboundMessage{ActorUserID: "user-b", ConversationID: "group-a", ConversationType: message.ConversationGroup, PlatformMessageID: "group-a-2"})
	if groupA.RunnerUserID != groupB.RunnerUserID || groupA.SessionID != groupB.SessionID {
		t.Fatalf("same group did not share scope: %#v %#v", groupA, groupB)
	}
	if groupA.ActorUserID != "user-a" || groupB.ActorUserID != "user-b" {
		t.Fatalf("group actors were not preserved: %#v %#v", groupA, groupB)
	}
}

func TestRouterIgnoresUntrustedExternalAccount(t *testing.T) {
	router := newTestRouter(t)
	task := resolveTestInbound(t, router, message.InboundMessage{
		ActorUserID: "user-a", ConversationID: "user-a", ConversationType: message.ConversationDirect,
		PlatformMessageID: "message-a", ExternalAccountID: "forged-account",
	})
	if task.ExternalAccountID != "trusted-account" || task.TenantID != "tenant-a" || task.AgentAppID != "assistant" {
		t.Fatalf("trusted routing projection = %#v", task)
	}
}

func resolveTestInbound(t *testing.T, router *Router, inbound message.InboundMessage) message.ExecutionTask {
	t.Helper()
	inbound.Channel = "telegram"
	inbound.BindingID = "binding-a"
	inbound.Text = "hello"
	inbound.RequestID = "request-" + inbound.PlatformMessageID
	inbound.TraceID = "trace-" + inbound.PlatformMessageID
	inbound.ReceivedAt = time.Now().UTC()
	task, err := router.Resolve(context.Background(), inbound)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func newTestRouter(t *testing.T) *Router {
	t.Helper()
	catalog := tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant-a", ID: "memory", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{{
			TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory", Instruction: "test",
			Model: tenant.ModelConfig{Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL", RequestTimeout: time.Second, MaxOutputTokens: 32},
		}},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding-a", Channel: "telegram", ExternalAccountID: "trusted-account", CredentialRef: "env:TELEGRAM_TOKEN",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := New(repository, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	return router
}
