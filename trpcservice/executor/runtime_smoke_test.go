package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

func TestRuntimeRealRedisSmoke(t *testing.T) {
	redisURL := os.Getenv("REDIS_SMOKE_URL")
	if redisURL == "" {
		t.Skip("set REDIS_SMOKE_URL to run the real Redis smoke test")
	}
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"smoke","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"redis smoke ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	prefix := "trpc-agent-service:phase1-smoke:" + time.Now().UTC().Format("20060102150405.000000000")
	cfg := testRuntimeConfig(redisURL, modelServer.URL, prefix)
	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer deleteKeysWithPrefix(t, redisURL, prefix)

	if err := first.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	reply, err := first.Handle(context.Background(), Request{
		BindingID: cfg.BindingID, MessageID: "smoke-message", ExternalUserID: "smoke-user",
		ConversationID: "smoke-conversation", Text: "ping", RequestID: "request", TraceID: "trace",
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "redis smoke ok" {
		t.Fatalf("reply = %q", reply.Text)
	}
	memoryKey := memory.UserKey{AppName: cfg.AppName, UserID: "smoke-user"}
	if err := first.backend.Memory().AddMemory(context.Background(), memoryKey, "real redis memory", []string{"smoke"}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	sess, err := second.backend.Session().GetSession(context.Background(), session.Key{
		AppName:   cfg.AppName,
		UserID:    identity.RunnerUserID(cfg.IdentitySecret, cfg.BindingID, "smoke-user"),
		SessionID: identity.SessionID(cfg.IdentitySecret, cfg.BindingID, "smoke-conversation"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sess == nil || len(sess.Events) == 0 {
		t.Fatalf("expected session events from real Redis, got %#v", sess)
	}
	memories, err := second.backend.Memory().SearchMemories(context.Background(), memoryKey, "redis")
	if err != nil {
		t.Fatal(err)
	}
	if len(memories) != 1 {
		t.Fatalf("expected memory from real Redis, got %#v", memories)
	}
}

func TestRuntimeDeepSeekSmoke(t *testing.T) {
	if os.Getenv("DEEPSEEK_SMOKE") != "1" {
		t.Skip("set DEEPSEEK_SMOKE=1 to run the real model smoke test")
	}
	redisURL := os.Getenv("REDIS_SMOKE_URL")
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	modelName := os.Getenv("MODEL_NAME")
	baseURL := os.Getenv("MODEL_BASE_URL")
	if redisURL == "" || apiKey == "" || modelName == "" || baseURL == "" {
		t.Fatal("REDIS_SMOKE_URL, DEEPSEEK_API_KEY, MODEL_NAME and MODEL_BASE_URL are required")
	}
	prefix := "trpc-agent-service:deepseek-smoke:" + time.Now().UTC().Format("20060102150405.000000000")
	cfg := testRuntimeConfig(redisURL, baseURL, prefix)
	cfg.ModelName = modelName
	cfg.ModelAPIKey = apiKey
	cfg.ModelRequestTimeout = 60 * time.Second
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	defer deleteKeysWithPrefix(t, redisURL, prefix)
	reply, err := runtime.Handle(context.Background(), Request{
		BindingID: cfg.BindingID, MessageID: "deepseek-smoke", ExternalUserID: "smoke-user",
		ConversationID: "smoke-conversation", Text: "Reply with exactly: phase1-ok", RequestID: "request", TraceID: "trace",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(reply.Text) == "" {
		t.Fatal("empty DeepSeek response")
	}
}

func testRuntimeConfig(redisURL, modelBaseURL, prefix string) config.Config {
	return config.Config{
		ModelName: "mock-model", ModelBaseURL: modelBaseURL, ModelAPIKeyEnv: "MOCK_KEY", ModelAPIKey: "test-key",
		ModelRequestTimeout: 5 * time.Second, ModelMaxOutput: 128,
		IdentitySecret: []byte("01234567890123456789012345678901"), RedisURL: redisURL, RedisKeyPrefix: prefix,
		BindingID: "demo-binding", TenantID: "tenant-demo", AgentAppID: "assistant", ConfigVersion: "v1",
		AppName: "tenant/tenant-demo/app/assistant",
	}
}

func deleteKeysWithPrefix(t *testing.T, redisURL, prefix string) {
	t.Helper()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Errorf("parse redis cleanup URL: %v", err)
		return
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx := context.Background()
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			t.Errorf("scan smoke keys: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete smoke keys: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
