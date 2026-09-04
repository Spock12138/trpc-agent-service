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
	platformmessage "github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
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
	ErrWorkerLost               = errors.New("worker lost while processing task")
	ErrTurnTooLarge             = errors.New("session turn too large")
)

type Request = platformmessage.InboundMessage
type Reply = platformmessage.OutboundMessage

type Runtime struct {
	identitySecret []byte
	repository     tenant.Repository
	router         *routing.Router
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
	fencingMode := config.DefaultSessionFencing
	if cfg.Messaging != nil && cfg.Messaging.SessionFencing != "" {
		fencingMode = cfg.Messaging.SessionFencing
	}
	messagingPrefix := ""
	messagingURL := ""
	if cfg.Messaging != nil {
		messagingPrefix = cfg.Messaging.KeyPrefix
		messagingURL = cfg.Messaging.RedisURL
	}
	backends, err := storage.NewBackendProvider(repository, credentials, fencingMode, messagingPrefix, messagingURL)
	if err != nil {
		return nil, err
	}
	if cfg.Messaging != nil {
		backends.SetTurnLimits(sessionfence.Limits{MaxTurnEvents: cfg.Messaging.MaxTurnEvents, MaxTurnBytes: cfg.Messaging.MaxTurnBytes})
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
	router, err := routing.New(repository, cfg.IdentitySecret)
	if err != nil {
		_ = registry.Close()
		_ = backends.Close()
		return nil, err
	}
	return &Runtime{
		identitySecret: append([]byte(nil), cfg.IdentitySecret...),
		repository:     repository,
		router:         router,
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

// PersistenceRoute resolves the immutable storage identity selected by the
// exact task config. Worker locks this route in Messaging Redis before running
// the Agent so a later config edit cannot move an Agent App between backends.
func (r *Runtime) PersistenceRoute(ctx context.Context, task platformmessage.ExecutionTask) (persistence.Route, error) {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if err := task.Validate(); err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	binding, err := r.repository.ResolveBinding(ctx, task.Channel, task.ChannelBindingID)
	if err != nil || binding.TenantID != task.TenantID || binding.AgentAppID != task.AgentAppID {
		return persistence.Route{}, ErrUnknownBinding
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, task.TenantID, task.AgentAppID, task.ConfigVersion)
	if err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	profile, err := r.repository.GetStorageProfile(ctx, task.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	fingerprint, err := r.backends.BackendIdentityFor(ctx, profile)
	if err != nil {
		return persistence.Route{}, fmt.Errorf("%w: selected storage identity unavailable", ErrConfigurationUnavailable)
	}
	route := persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: fingerprint}
	if err := route.Validate(); err != nil {
		return persistence.Route{}, ErrConfigurationUnavailable
	}
	return route, nil
}

// Persist commits an already staged SQL turn without invoking the Agent.
func (r *Runtime) Persist(ctx context.Context, envelope persistence.Envelope) error {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if err := envelope.Validate(); err != nil {
		return err
	}
	profile, err := r.repository.GetStorageProfile(ctx, envelope.TenantID, envelope.StorageProfileID)
	if err != nil || profile.Kind != envelope.BackendKind {
		return ErrConfigurationUnavailable
	}
	backend, err := r.backends.BackendFor(ctx, profile)
	if err != nil {
		if errors.Is(err, persistence.ErrSchemaIncompatible) {
			return persistence.ErrSchemaIncompatible
		}
		return persistence.ErrBackendUnavailable
	}
	if backend.Fingerprint() != envelope.BackendFingerprint {
		return persistence.ErrFingerprintConflict
	}
	persistent, ok := backend.(storage.PersistentBackend)
	if !ok || persistent.Committer() == nil {
		return persistence.ErrBackendUnavailable
	}
	return persistent.Committer().Commit(ctx, envelope)
}

func (r *Runtime) Handle(ctx context.Context, req Request) (Reply, error) {
	router := r.router
	if router == nil {
		var err error
		router, err = routing.New(r.repository, r.identitySecret)
		if err != nil {
			return Reply{}, fmt.Errorf("%w: router unavailable", ErrConfigurationUnavailable)
		}
	}
	task, err := router.Resolve(ctx, req)
	if err != nil {
		switch {
		case errors.Is(err, routing.ErrUnknownBinding):
			return Reply{}, ErrUnknownBinding
		case errors.Is(err, routing.ErrConfigurationUnavailable):
			return Reply{}, ErrConfigurationUnavailable
		default:
			return Reply{}, err
		}
	}
	return r.Execute(ctx, task)
}

// Execute runs an immutable task created by the trusted Router. It validates
// the binding and exact config version again before acquiring a Runner.
func (r *Runtime) Execute(ctx context.Context, task platformmessage.ExecutionTask) (Reply, error) {
	// Runtime.Close takes the write lock, so active Runner and backend work is
	// drained before the registry and borrowed services are closed.
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := task.Validate(); err != nil {
		return Reply{}, fmt.Errorf("%w: invalid execution task", ErrConfigurationUnavailable)
	}
	binding, err := r.repository.ResolveBinding(ctx, task.Channel, task.ChannelBindingID)
	if err != nil {
		if errors.Is(err, tenant.ErrBindingNotFound) {
			return Reply{}, ErrUnknownBinding
		}
		return Reply{}, fmt.Errorf("%w: binding lookup failed", ErrConfigurationUnavailable)
	}
	if binding.TenantID != task.TenantID || binding.AgentAppID != task.AgentAppID {
		return Reply{}, fmt.Errorf("%w: task binding mismatch", ErrConfigurationUnavailable)
	}
	if _, err := r.repository.GetTenant(ctx, binding.TenantID); err != nil {
		return Reply{}, fmt.Errorf("%w: tenant lookup failed", ErrConfigurationUnavailable)
	}
	app, err := r.repository.GetAgentApp(ctx, task.TenantID, task.AgentAppID)
	if err != nil {
		return Reply{}, fmt.Errorf("%w: agent app lookup failed", ErrConfigurationUnavailable)
	}
	if app.TenantID != task.TenantID || app.ID != task.AgentAppID {
		return Reply{}, fmt.Errorf("%w: task agent app mismatch", ErrConfigurationUnavailable)
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, task.TenantID, task.AgentAppID, task.ConfigVersion)
	if err != nil {
		return Reply{}, fmt.Errorf("%w: active config lookup failed", ErrConfigurationUnavailable)
	}

	requestCtx, cancel := context.WithTimeout(ctx, configVersion.Model.RequestTimeout)
	defer cancel()
	key := agent.CacheKey{
		TenantID: task.TenantID, AgentAppID: task.AgentAppID, ConfigVersion: task.ConfigVersion,
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

	events, err := lease.Runner.Run(
		requestCtx,
		task.RunnerUserID,
		task.SessionID,
		model.Message{Role: model.RoleUser, Content: task.Text},
		frameworkagent.WithRequestID(task.RequestID),
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
		return Reply{}, ErrAgentFailed
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
		Channel: task.Channel, BindingID: binding.ID, RequestID: task.RequestID,
		TraceID: task.TraceID, SessionID: task.SessionID, Text: text,
	}, nil
}

// ExecuteFenced runs a turn against the platform-owned fenced Session
// namespace. Runner writes are staged locally and committed only after the
// runner has returned successfully.
func (r *Runtime) ExecuteFenced(ctx context.Context, task platformmessage.ExecutionTask, fence sessionfence.Fence) (Reply, sessionfence.TurnCommit, error) {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := task.Validate(); err != nil {
		return Reply{}, sessionfence.TurnCommit{}, ErrConfigurationUnavailable
	}
	binding, err := r.repository.ResolveBinding(ctx, task.Channel, task.ChannelBindingID)
	if err != nil || binding.TenantID != task.TenantID || binding.AgentAppID != task.AgentAppID {
		return Reply{}, sessionfence.TurnCommit{}, ErrUnknownBinding
	}
	configVersion, err := r.repository.GetConfigVersion(ctx, task.TenantID, task.AgentAppID, task.ConfigVersion)
	if err != nil {
		return Reply{}, sessionfence.TurnCommit{}, ErrConfigurationUnavailable
	}
	requestCtx, cancel := context.WithTimeout(ctx, configVersion.Model.RequestTimeout)
	defer cancel()
	profile, err := r.repository.GetStorageProfile(ctx, task.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return Reply{}, sessionfence.TurnCommit{}, ErrConfigurationUnavailable
	}
	backend, err := r.backends.BackendFor(requestCtx, profile)
	if err != nil {
		if profile.Kind.IsSQL() {
			if errors.Is(err, persistence.ErrSchemaIncompatible) {
				return Reply{}, sessionfence.TurnCommit{}, persistence.ErrSchemaIncompatible
			}
			return Reply{}, sessionfence.TurnCommit{}, persistence.ErrBackendUnavailable
		}
		return Reply{}, sessionfence.TurnCommit{}, ErrDependencyUnavailable
	}
	svc, ok := backend.Session().(sessionfence.StagingSession)
	if !ok {
		return Reply{}, sessionfence.TurnCommit{}, errors.New("strong session fencing backend is unavailable")
	}
	key := agent.CacheKey{TenantID: task.TenantID, AgentAppID: task.AgentAppID, ConfigVersion: task.ConfigVersion}
	lease, err := r.registry.Acquire(requestCtx, key)
	if err != nil {
		return Reply{}, sessionfence.TurnCommit{}, err
	}
	defer lease.Release()
	if fence.UserCoord == "" {
		fence.UserCoord = sessionfence.UserCoordFor(task.TenantID, task.ChannelBindingID, task.RunnerUserID)
	}
	turn := svc.StartTurn(session.Key{AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID}, fence)
	requestCtx, requestCancel := context.WithCancel(requestCtx)
	defer requestCancel()
	runCtx := sessionfence.WithTurn(requestCtx, turn)
	events, err := lease.Runner.Run(runCtx, task.RunnerUserID, task.SessionID, model.Message{Role: model.RoleUser, Content: task.Text}, frameworkagent.WithRequestID(task.RequestID))
	if err != nil {
		svc.Discard(turn)
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return Reply{}, sessionfence.TurnCommit{}, ErrAgentTimeout
		}
		if errors.Is(err, persistence.ErrPostgresSummaryDisabled) {
			return Reply{}, sessionfence.TurnCommit{}, persistence.ErrPostgresSummaryDisabled
		}
		return Reply{}, sessionfence.TurnCommit{}, ErrAgentFailed
	}
	text, err := collectText(events)
	if svcTurnTooLarge(turn) {
		svc.Discard(turn)
		return Reply{}, sessionfence.TurnCommit{}, ErrTurnTooLarge
	}
	if err != nil {
		svc.Discard(turn)
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return Reply{}, sessionfence.TurnCommit{}, ErrAgentTimeout
		}
		return Reply{}, sessionfence.TurnCommit{}, err
	}
	commit, err := svc.Prepare(turn)
	if err != nil {
		svc.Discard(turn)
		return Reply{}, sessionfence.TurnCommit{}, err
	}
	return Reply{Channel: task.Channel, BindingID: binding.ID, RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: text}, commit, nil
}

func svcTurnTooLarge(turn *sessionfence.Turn) bool {
	return turn != nil && turn.IsOverLimit()
}

func collectText(events <-chan *event.Event) (string, error) {
	var builder strings.Builder
	var eventErr error
	for current := range events {
		if current == nil {
			continue
		}
		if current.IsError() && current.Response != nil && current.Response.Error != nil {
			eventErr = ErrAgentFailed
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
