package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
)

type fakeBackend struct {
	readyErr error
	handle   func(context.Context, executor.Request) (executor.Reply, error)
}

func (f fakeBackend) Ready(context.Context) error { return f.readyErr }
func (f fakeBackend) Handle(ctx context.Context, req executor.Request) (executor.Reply, error) {
	return f.handle(ctx, req)
}

func TestMessageHandlerSuccessAndIDs(t *testing.T) {
	handler := NewHandler(fakeBackend{handle: func(_ context.Context, req executor.Request) (executor.Reply, error) {
		return executor.Reply{RequestID: req.RequestID, TraceID: req.TraceID, SessionID: "s_test", Text: "ok"}, nil
	}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body["request_id"]) != 16 || len(body["trace_id"]) != 32 {
		t.Fatalf("unexpected IDs: %#v", body)
	}
}

func TestMessageHandlerValidationAndErrorMapping(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		err    error
		status int
	}{
		{name: "invalid json", input: "{", status: http.StatusBadRequest},
		{name: "unknown field", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello","tenant_id":"bad"}`, status: http.StatusBadRequest},
		{name: "missing field", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","text":"hello"}`, status: http.StatusBadRequest},
		{name: "empty text", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"  "}`, status: http.StatusBadRequest},
		{name: "long id", input: `{"binding_id":"demo-binding","message_id":"` + strings.Repeat("m", maxIDBytes+1) + `","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, status: http.StatusBadRequest},
		{name: "long text", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"` + strings.Repeat("x", maxTextBytes+1) + `"}`, status: http.StatusBadRequest},
		{name: "multiple objects", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"} {}`, status: http.StatusBadRequest},
		{name: "body too large", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"` + strings.Repeat("x", maxBodyBytes) + `"}`, status: http.StatusBadRequest},
		{name: "unknown binding", input: `{"binding_id":"other","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrUnknownBinding, status: http.StatusNotFound},
		{name: "dependency unavailable", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrDependencyUnavailable, status: http.StatusServiceUnavailable},
		{name: "runner draining", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrRunnerDraining, status: http.StatusServiceUnavailable},
		{name: "timeout", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrAgentTimeout, status: http.StatusGatewayTimeout},
		{name: "failure", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrEmptyAgentResponse, status: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(fakeBackend{handle: func(_ context.Context, _ executor.Request) (executor.Reply, error) {
				return executor.Reply{}, tt.err
			}})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(tt.input))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tt.status, rec.Body.String())
			}
		})
	}
}

func TestMessageHandlerDoesNotExposeBackendErrors(t *testing.T) {
	const secret = "api-key-and-upstream-response-must-not-leak"
	handler := NewHandler(fakeBackend{handle: func(context.Context, executor.Request) (executor.Reply, error) {
		return executor.Reply{}, errors.New(secret)
	}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatalf("response leaked backend error: %s", rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"code", "message", "request_id", "trace_id"} {
		if body[field] == "" {
			t.Fatalf("error response field %s is empty: %#v", field, body)
		}
	}
}

func TestReadyEndpoint(t *testing.T) {
	handler := NewHandler(fakeBackend{readyErr: errors.New("redis unavailable"), handle: func(context.Context, executor.Request) (executor.Reply, error) { return executor.Reply{}, nil }})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestHTTPRuntimeEndToEndWithMockModelAndRedis(t *testing.T) {
	redisServer := miniredis.RunT(t)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"e2e","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"end to end ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()
	cfg := config.Config{
		ModelName: "mock-model", ModelBaseURL: modelServer.URL, ModelAPIKeyEnv: "MOCK_KEY", ModelAPIKey: "test-key",
		ModelRequestTimeout: time.Second, ModelMaxOutput: 128,
		IdentitySecret: []byte("01234567890123456789012345678901"), RedisURL: "redis://" + redisServer.Addr() + "/0", RedisKeyPrefix: "http-e2e",
		BindingID: "demo-binding", TenantID: "tenant-demo", AgentAppID: "assistant", ConfigVersion: "v1",
		AppName: "tenant/tenant-demo/app/assistant",
	}
	runtime, err := executor.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handler := NewHandler(runtime)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var reply executor.Reply
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Text != "end to end ok" || reply.SessionID == "" {
		t.Fatalf("unexpected reply: %#v", reply)
	}
}
