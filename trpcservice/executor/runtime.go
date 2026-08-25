// Package executor implements the single-process phase 1 runtime.
package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	openaioption "github.com/openai/openai-go/option"
	"github.com/redis/go-redis/v9"
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
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var (
	ErrUnknownBinding        = errors.New("unknown binding")
	ErrDependencyUnavailable = errors.New("runtime dependency unavailable")
	ErrRunnerDraining        = errors.New("runner is draining")
	ErrAgentTimeout          = errors.New("agent request timed out")
	ErrAgentFailed           = errors.New("agent request failed")
	ErrEmptyAgentResponse    = errors.New("agent returned an empty response")
)

type Request struct {
	BindingID      string
	MessageID      string
	ExternalUserID string
	ConversationID string
	Text           string
	RequestID      string
	TraceID        string
}

type Reply struct {
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id"`
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
}

type Runtime struct {
	config       config.Config
	redisClient  *redis.Client
	sessionStore *storage.RedisSessionService
	memoryStore  *storage.RedisMemoryService
	cache        *agent.RunnerCache
	closeOnce    sync.Once
	closeErr     error
}

func New(cfg config.Config) (*Runtime, error) {
	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(redisOptions)
	sessionStore := storage.NewRedisSessionService(client, cfg.RedisKeyPrefix)
	memoryStore := storage.NewRedisMemoryService(client, cfg.RedisKeyPrefix)

	var runtime *Runtime
	cache, err := agent.NewRunnerCache(agent.DefaultCacheConfig(), func(ctx context.Context, key agent.CacheKey) (frameworkrunner.Runner, error) {
		return newRunner(ctx, cfg, key, sessionStore, memoryStore)
	})
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	runtime = &Runtime{
		config:       cfg,
		redisClient:  client,
		sessionStore: sessionStore,
		memoryStore:  memoryStore,
		cache:        cache,
	}
	return runtime, nil
}

func newRunner(_ context.Context, cfg config.Config, key agent.CacheKey, sessions session.Service, memories memory.Service) (frameworkrunner.Runner, error) {
	if key.TenantID != cfg.TenantID || key.AgentAppID != cfg.AgentAppID || key.ConfigVersion != cfg.ConfigVersion {
		return nil, fmt.Errorf("unsupported runner key")
	}
	return frameworkrunner.NewRunner(
		cfg.AppName,
		buildAgent(cfg),
		frameworkrunner.WithSessionService(sessions),
		frameworkrunner.WithMemoryService(memories),
	), nil
}

func (r *Runtime) Ready(ctx context.Context) error {
	if err := r.cache.Ready(r.cacheKey()); err != nil {
		return fmt.Errorf("runner cache: %w", err)
	}
	if err := r.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func (r *Runtime) Handle(ctx context.Context, req Request) (Reply, error) {
	if req.BindingID != r.config.BindingID {
		return Reply{}, ErrUnknownBinding
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.Ready(ctx); err != nil {
		return Reply{}, fmt.Errorf("%w: %v", ErrDependencyUnavailable, err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, r.config.ModelRequestTimeout)
	defer cancel()
	key := r.cacheKey()
	lease, err := r.cache.Acquire(requestCtx, key)
	if err != nil {
		if errors.Is(err, agent.ErrRunnerDrain) {
			return Reply{}, ErrRunnerDraining
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return Reply{}, ErrAgentTimeout
		}
		if errors.Is(err, agent.ErrCacheClosed) || errors.Is(err, agent.ErrCacheFull) {
			return Reply{}, fmt.Errorf("%w: runner cache unavailable", ErrDependencyUnavailable)
		}
		return Reply{}, fmt.Errorf("acquire runner: %w", err)
	}
	defer lease.Release()

	userID := identity.RunnerUserID(r.config.IdentitySecret, req.BindingID, req.ExternalUserID)
	sessionID := identity.SessionID(r.config.IdentitySecret, req.BindingID, req.ConversationID)
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
		return Reply{}, fmt.Errorf("%w: %v", ErrAgentFailed, err)
	}
	text, eventErr := collectText(events)
	if eventErr != nil {
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return Reply{}, ErrAgentTimeout
		}
		return Reply{}, eventErr
	}
	return Reply{RequestID: req.RequestID, TraceID: req.TraceID, SessionID: sessionID, Text: text}, nil
}

func (r *Runtime) cacheKey() agent.CacheKey {
	return agent.CacheKey{
		TenantID: r.config.TenantID, AgentAppID: r.config.AgentAppID, ConfigVersion: r.config.ConfigVersion,
	}
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
	r.closeOnce.Do(func() {
		r.closeErr = errors.Join(r.cache.Close(), r.sessionStore.Close(), r.memoryStore.Close(), r.redisClient.Close())
	})
	return r.closeErr
}

func newOpenAIModel(cfg config.Config) *openai.Model {
	return openai.New(
		cfg.ModelName,
		openai.WithBaseURL(cfg.ModelBaseURL),
		openai.WithAPIKey(cfg.ModelAPIKey),
		openai.WithOpenAIOptions(openaioption.WithMaxRetries(0)),
	)
}

func buildAgent(cfg config.Config) frameworkagent.Agent {
	maxTokens := cfg.ModelMaxOutput
	temperature := 0.2
	return llmagent.New(
		"assistant",
		llmagent.WithModel(newOpenAIModel(cfg)),
		llmagent.WithInstruction("You are a concise assistant. Answer the user's request directly."),
		llmagent.WithGenerationConfig(model.GenerationConfig{MaxTokens: &maxTokens, Temperature: &temperature, Stream: false}),
	)
}

// compile-time checks document the framework contracts used by this runtime.
var _ session.Service = (*storage.RedisSessionService)(nil)
var _ memory.Service = (*storage.RedisMemoryService)(nil)
