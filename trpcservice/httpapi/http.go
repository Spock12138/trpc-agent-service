// Package httpapi exposes the phase 1 health and demo message endpoints.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

const (
	maxBodyBytes = 64 << 10
	maxIDBytes   = 256
	maxTextBytes = 16 << 10
)

type Backend interface {
	Ready(context.Context) error
	Handle(context.Context, executor.Request) (executor.Reply, error)
}

func NewHandler(backend Backend) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		if err := backend.Ready(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", "service is not ready", "", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/api/v1/demo/messages", messageHandler(backend))
	return mux
}

type messageRequest struct {
	BindingID      string `json:"binding_id"`
	MessageID      string `json:"message_id"`
	ExternalUserID string `json:"external_user_id"`
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
}

func messageHandler(backend Backend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID, err := identity.RequestID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "request_id_failed", "could not create request ID", "", "")
			return
		}
		traceID, err := identity.TraceID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "trace_id_failed", "could not create trace ID", requestID, "")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", requestID, traceID)
			return
		}

		body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
		defer body.Close()
		decoder := json.NewDecoder(body)
		decoder.DisallowUnknownFields()
		var input messageRequest
		if err := decoder.Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON request", requestID, traceID)
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid_request", "request must contain one JSON object", requestID, traceID)
			return
		}
		if err := validate(input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), requestID, traceID)
			return
		}

		reply, err := backend.Handle(r.Context(), executor.Request{
			BindingID:      input.BindingID,
			MessageID:      input.MessageID,
			ExternalUserID: input.ExternalUserID,
			ConversationID: input.ConversationID,
			Text:           input.Text,
			RequestID:      requestID,
			TraceID:        traceID,
		})
		if err != nil {
			writeMappedError(w, err, requestID, traceID)
			return
		}
		writeJSON(w, http.StatusOK, reply)
	}
}

func validate(input messageRequest) error {
	if input.BindingID == "" || input.MessageID == "" || input.ExternalUserID == "" || input.ConversationID == "" {
		return errors.New("binding_id, message_id, external_user_id and conversation_id are required")
	}
	for name, value := range map[string]string{
		"binding_id": input.BindingID, "message_id": input.MessageID,
		"external_user_id": input.ExternalUserID, "conversation_id": input.ConversationID,
	} {
		if len([]byte(value)) > maxIDBytes {
			return errors.New(name + " is too long")
		}
	}
	if strings.TrimSpace(input.Text) == "" {
		return errors.New("text is required")
	}
	if len([]byte(input.Text)) > maxTextBytes {
		return errors.New("text is too long")
	}
	return nil
}

func writeMappedError(w http.ResponseWriter, err error, requestID, traceID string) {
	status := http.StatusBadGateway
	code := "agent_failed"
	message := "agent request failed"
	switch {
	case errors.Is(err, executor.ErrUnknownBinding):
		status, code, message = http.StatusNotFound, "binding_not_found", "binding not found"
	case errors.Is(err, executor.ErrRunnerDraining), errors.Is(err, executor.ErrDependencyUnavailable):
		status, code, message = http.StatusServiceUnavailable, "not_ready", "service dependency unavailable"
	case errors.Is(err, executor.ErrAgentTimeout):
		status, code, message = http.StatusGatewayTimeout, "model_timeout", "model request timed out"
	case errors.Is(err, executor.ErrEmptyAgentResponse):
		status, code, message = http.StatusBadGateway, "empty_agent_response", "agent returned no text"
	}
	writeError(w, status, code, message, requestID, traceID)
}

func writeError(w http.ResponseWriter, status int, code, message, requestID, traceID string) {
	writeJSON(w, status, map[string]string{
		"code":       code,
		"message":    message,
		"request_id": requestID,
		"trace_id":   traceID,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
