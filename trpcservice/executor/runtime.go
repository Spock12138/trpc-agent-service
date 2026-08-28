// Package executor implements the single-process multi-tenant Agent runtime.
package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	openaioption "github.com/openai/openai-go/option"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	platformmessage "github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrUnknownBinding           = errors.New("unknown binding")
	ErrConfigurationUnavailable = errors.New("runtime configuration unavailable")
	ErrDependencyUnavailable    = errors.New("runtime dependency unavailable")
	ErrRunnerDraining           = errors.New("runner is draining")
	ErrAgentTimeout             = errors.New("agent request timed out")
	ErrAgentFailed              = errors.New("agent request failed")
	ErrEmptyAgentResponse       = errors.New("agent returned an empty response")
)

type Request = platformmessage.InboundMessage
type Reply = platformmessage.OutboundMessage

type Runtime struct {
	identitySecret []byte
	repository     tenant.Repository
	backends       *storage.BackendProvider
	registry       *agent.RunnerRegistry

	lifecycle sync.RWMutex
	closeOnce sync.Once
	closeErr  error
}

func New(cfg config.Config) (*Runtime, error) {
	catalog, credentials, err := cfg.RuntimeCatalog()
	if err != nil {
		return nil, err
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		return nil, fmt.Errorf("create tenant repository: %w", err)
	}
	backends, err := storage.NewBackendProvider(repository, credentials)
	if err != nil {
		return nil, err
	}
	registry, err := agent.NewRunnerRegistry(
		repository,
		backends,
		credentials,
		agent.DefaultCacheConfig(),
		newRunner,
	)
	if err != nil {
		_ = backends.Close()
		return nil, err
	}
	return &Runtime{
		identitySecret: append([]byte(nil), cfg.IdentitySecret...),
		repository:     repository,
		backends:       backends,
		registry:       registry,
	}, nil
}

func newRunner(
	_ context.Context,
	key agent.CacheKey,
	configVersion tenant.ConfigVersion,
	apiKey string,
	sessions session.Service,
	memories memory.Service,
) (frameworkrunner.Runner, error) {
	if key.TenantID != configVersion.TenantID || key.AgentAppID != configVersion.AgentAppID || key.ConfigVersion != configVersion.Version {
		return nil, errors.New("runner configuration does not match cache key")
	}
	return frameworkrunner.NewRunner(
		tenant.AppName(key.TenantID, key.AgentAppID),
		buildAgent(configVersion, apiKey),
		frameworkrunner.WithSessionService(sessions),
		frameworkrunner.WithMemoryService(memories),
	), nil
}

func (r *Runtime) Ready(ctx context.Context) error {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if err := r.registry.Ready(ctx); err != nil {
		return fmt.Errorf("runtime not ready: %w", err)
	}
	return nil
}

func (r *Runtime) backendForBinding(ctx context.Context, channel, bindingID string) (storage.Backend, error) {
	binding, err := r.repository.ResolveBinding(ctx, channel, bindingID)
	if err != nil {
		return nil, err
	}
	app, err := r.repository.GetAgentApp(ctx, binding.TenantID, binding.AgentAppID)
	if err != nil {
		return nil, err
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, binding.TenantID, binding.AgentAppID, app.ActiveConfigVersion)
	if err != nil {
		return nil, err
	}
	profile, err := r.repository.GetStorageProfile(ctx, binding.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return nil, err
	}
	return r.backends.BackendFor(ctx, profile)
}

func (r *Runtime) Handle(ctx context.Context, req Request) (Reply, error) {
	// Runtime.Close takes the write lock, so active Runner and backend work is
	// drained before the registry and borrowed services are closed.
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	channel := req.Channel
	if channel == "" {
		channel = "demo"
	}
	binding, err := r.repository.ResolveBinding(ctx, channel, req.BindingID)
	if err != nil {
		if errors.Is(err, tenant.ErrBindingNotFound) {
			return Reply{}, ErrUnknownBinding
		}
		return Reply{}, fmt.Errorf("%w: binding lookup failed", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetTenant(ctx, binding.TenantID); err != nil {
		return Reply{}, fmt.Errorf("%w: tenant lookup failed", ErrConfigurationUnavailable)
	}
	app, err := r.repository.GetAgentApp(ctx, binding.TenantID, binding.AgentAppID)
	if err != nil {
		return Reply{}, fmt.Errorf("%w: agent app lookup failed", ErrConfigurationUnavailable)
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, binding.TenantID, binding.AgentAppID, app.ActiveConfigVersion)
	if err != nil {
		return Reply{}, fmt.Errorf("%w: active config lookup failed", ErrConfigurationUnavailable)
	}

	requestCtx, cancel := context.WithTimeout(ctx, configVersion.Model.RequestTimeout)
	defer cancel()
	key := agent.CacheKey{
		TenantID: binding.TenantID, AgentAppID: binding.AgentAppID, ConfigVersion: app.ActiveConfigVersion,
	}
	lease, err := r.registry.Acquire(requestCtx, key)
	if err != nil {
		switch {
		case errors.Is(err, agent.ErrRunnerDrain):
			return Reply{}, ErrRunnerDraining
		case errors.Is(err, context.DeadlineExceeded):
			return Reply{}, ErrAgentTimeout
		case errors.Is(err, agent.ErrRegistryConfiguration):
			return Reply{}, fmt.Errorf("%w: runner configuration unavailable", ErrConfigurationUnavailable)
		case errors.Is(err, agent.ErrRegistryDependency), errors.Is(err, agent.ErrCacheClosed), errors.Is(err, agent.ErrCacheFull):
			return Reply{}, fmt.Errorf("%w: runner dependency unavailable", ErrDependencyUnavailable)
		default:
			return Reply{}, fmt.Errorf("acquire runner: %w", err)
		}
	}
	defer lease.Release()

	userID := identity.RunnerUserID(r.identitySecret, binding.ID, req.ExternalUserID)
	sessionID := identity.SessionID(r.identitySecret, binding.ID, req.ConversationID)
	events, err := lease.Runner.Run(
		requestCtx,
		userID,
		sessionID,
		model.Message{Role: model.RoleUser, Content: req.Text},
		frameworkagent.WithRequestID(req.RequestID),
	)
	if err != nil {
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return Reply{}, ErrAgentTimeout
		}
		if requestCtx.Err() == nil {
			if checkErr := lease.Backend.Check(requestCtx); checkErr != nil {
				return Reply{}, fmt.Errorf("%w: selected storage unavailable", ErrDependencyUnavailable)
			}
		}
		return Reply{}, fmt.Errorf("%w: %v", ErrAgentFailed, err)
	}
	text, eventErr := collectText(events)
	if eventErr != nil {
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return Reply{}, ErrAgentTimeout
		}
		if requestCtx.Err() == nil {
			if checkErr := lease.Backend.Check(requestCtx); checkErr != nil {
				return Reply{}, fmt.Errorf("%w: selected storage unavailable", ErrDependencyUnavailable)
			}
		}
		return Reply{}, eventErr
	}
	return Reply{
		Channel: channel, BindingID: binding.ID, RequestID: req.RequestID,
		TraceID: req.TraceID, SessionID: sessionID, Text: text,
	}, nil
}

func collectText(events <-chan *event.Event) (string, error) {
	var builder strings.Builder
	var eventErr error
	for current := range events {
		if current == nil {
			continue
		}
		if current.IsError() && current.Response != nil && current.Response.Error != nil {
			eventErr = fmt.Errorf("%w: %s", ErrAgentFailed, current.Response.Error.Message)
			continue
		}
		if current.Response == nil || current.IsToolCallResponse() || current.IsToolResultResponse() {
			continue
		}
		for _, choice := range current.Response.Choices {
			part := choice.Message.Content
			if current.Response.IsPartial || current.Response.Object == model.ObjectTypeChatCompletionChunk {
				part = choice.Delta.Content
			}
			builder.WriteString(part)
		}
	}
	if eventErr != nil {
		return "", eventErr
	}
	text := strings.TrimSpace(builder.String())
	if text == "" {
		return "", ErrEmptyAgentResponse
	}
	return text, nil
}

func (r *Runtime) Close() error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.closeOnce.Do(func() {
		// Runners borrow Session/Memory services from the provider. Close all
		// runners before closing the provider-owned services.
		r.closeErr = errors.Join(r.registry.Close(), r.backends.Close())
	})
	return r.closeErr
}

func newOpenAIModel(configVersion tenant.ConfigVersion, apiKey string) *openai.Model {
	return openai.New(
		configVersion.Model.Name,
		openai.WithBaseURL(configVersion.Model.BaseURL),
		openai.WithAPIKey(apiKey),
		openai.WithOpenAIOptions(openaioption.WithMaxRetries(0)),
	)
}

func buildAgent(configVersion tenant.ConfigVersion, apiKey string) frameworkagent.Agent {
	maxTokens := configVersion.Model.MaxOutputTokens
	temperature := 0.2
	return llmagent.New(
		configVersion.AgentAppID,
		llmagent.WithModel(newOpenAIModel(configVersion, apiKey)),
		llmagent.WithInstruction(configVersion.Instruction),
		llmagent.WithGenerationConfig(model.GenerationConfig{
			MaxTokens: &maxTokens, Temperature: &temperature, Stream: false,
		}),
	)
}
