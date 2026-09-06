package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
)

func TestAdminOperationalLookupsAreAuthorizedAndBounded(t *testing.T) {
	task := func(context.Context, string, string) (map[string]any, error) {
		return map[string]any{"task_id": "task", "state": "processing", "attempt": 2, "error_code": "", "node_id": "worker", "assignment_state": "running", "persist_attempt": 1}, nil
	}
	outbound := func(context.Context, string) (map[string]any, error) {
		return map[string]any{"task_id": "task", "state": "retry_wait", "attempt": 1, "error_code": "send_failed"}, nil
	}
	reconciler := func(context.Context) (map[string]any, error) {
		return map[string]any{"is_leader": true, "node_id": "gateway"}, nil
	}
	handler := adminHandlerWithLookups(nil, nil, control.NewAdminAuthenticator("secret"), task, outbound, reconciler)
	for _, path := range []string{"/api/v1/admin/tasks/binding/message", "/api/v1/admin/outbound/task", "/api/v1/admin/reconciler"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"payload", "raw_payload", "redis_key", "text", "credential"} {
			if strings.Contains(rec.Body.String(), forbidden) {
				t.Fatalf("%s leaked forbidden field %q", path, forbidden)
			}
		}
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/admin/reconciler", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
}
