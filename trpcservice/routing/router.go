// Package routing resolves verified channel bindings into immutable execution
// tasks without constructing Agent Runners or storage backends.
package routing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrUnknownBinding           = errors.New("unknown binding")
	ErrConfigurationUnavailable = errors.New("routing configuration unavailable")
)

type Router struct {
	repository     tenant.Repository
	identitySecret []byte
}

func New(repository tenant.Repository, identitySecret []byte) (*Router, error) {
	if repository == nil {
		return nil, errors.New("tenant repository is required")
	}
	if len(identitySecret) == 0 {
		return nil, errors.New("identity secret is required")
	}
	return &Router{repository: repository, identitySecret: append([]byte(nil), identitySecret...)}, nil
}

func (r *Router) Resolve(ctx context.Context, inbound message.InboundMessage) (message.ExecutionTask, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	channel := inbound.Channel
	if channel == "" {
		channel = "demo"
	}
	receivedAt := inbound.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	requestID := inbound.RequestID
	if requestID == "" {
		var err error
		requestID, err = identity.RequestID()
		if err != nil {
			return message.ExecutionTask{}, fmt.Errorf("create request id: %w", err)
		}
	}
	traceID := inbound.TraceID
	if traceID == "" {
		var err error
		traceID, err = identity.TraceID()
		if err != nil {
			return message.ExecutionTask{}, fmt.Errorf("create trace id: %w", err)
		}
	}
	binding, err := r.repository.ResolveBinding(ctx, channel, inbound.BindingID)
	if err != nil {
		if errors.Is(err, tenant.ErrBindingNotFound) {
			return message.ExecutionTask{}, ErrUnknownBinding
		}
		return message.ExecutionTask{}, fmt.Errorf("%w: binding lookup failed", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetTenant(ctx, binding.TenantID); err != nil {
		return message.ExecutionTask{}, fmt.Errorf("%w: tenant lookup failed", ErrConfigurationUnavailable)
	}
	app, err := r.repository.GetAgentApp(ctx, binding.TenantID, binding.AgentAppID)
	if err != nil {
		return message.ExecutionTask{}, fmt.Errorf("%w: agent app lookup failed", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetConfigVersion(ctx, binding.TenantID, binding.AgentAppID, app.ActiveConfigVersion); err != nil {
		return message.ExecutionTask{}, fmt.Errorf("%w: active config lookup failed", ErrConfigurationUnavailable)
	}
	taskID, err := identity.TaskID()
	if err != nil {
		return message.ExecutionTask{}, fmt.Errorf("create task id: %w", err)
	}
	task := message.ExecutionTask{
		SchemaVersion:     message.TaskSchemaVersion,
		TaskID:            taskID,
		Channel:           channel,
		ChannelBindingID:  binding.ID,
		TenantID:          binding.TenantID,
		AgentAppID:        binding.AgentAppID,
		ConfigVersion:     app.ActiveConfigVersion,
		RunnerUserID:      identity.RunnerUserID(r.identitySecret, binding.ID, inbound.ExternalUserID),
		SessionID:         identity.SessionID(r.identitySecret, binding.ID, inbound.ConversationID),
		PlatformMessageID: inbound.MessageID,
		Text:              inbound.Text,
		RequestID:         requestID,
		TraceID:           traceID,
		ReceivedAt:        receivedAt,
		Attempt:           1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	if err := task.Validate(); err != nil {
		return message.ExecutionTask{}, fmt.Errorf("create execution task: %w", err)
	}
	return task, nil
}
