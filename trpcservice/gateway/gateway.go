// Package gateway resolves inbound messages, submits reliable tasks, and
// converts stored terminal results back into the synchronous Demo contract.
package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
)

var (
	ErrMessageConflict      = errors.New("message idempotency conflict")
	ErrTaskPending          = errors.New("task is still processing")
	ErrMessagingUnavailable = errors.New("messaging dependency unavailable")
)

type Service struct {
	router   *routing.Router
	store    *messaging.Store
	consumer string

	running atomic.Bool
	mu      sync.Mutex
	waiters map[string][]chan struct{}
	cancel  context.CancelFunc
}

func New(router *routing.Router, store *messaging.Store, consumer string) (*Service, error) {
	if router == nil || store == nil {
		return nil, errors.New("router and messaging store are required")
	}
	if consumer == "" {
		return nil, errors.New("gateway consumer name is required")
	}
	return &Service{router: router, store: store, consumer: consumer, waiters: make(map[string][]chan struct{})}, nil
}

func (s *Service) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		cancel()
		return errors.New("gateway reply consumer is already running")
	}
	s.cancel = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.running.Store(false)
		s.mu.Lock()
		s.cancel = nil
		s.mu.Unlock()
	}()
	if err := s.store.Ready(runCtx); err != nil {
		return err
	}
	s.running.Store(true)
	for {
		if runCtx.Err() != nil {
			return nil
		}
		claimed, err := s.store.ClaimReplies(runCtx, s.consumer, 5*time.Second, 16)
		if err == nil {
			for _, reply := range claimed {
				s.deliver(runCtx, reply)
			}
		}
		reply, err := s.store.ReadReply(runCtx, s.consumer, time.Second)
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if runCtx.Err() != nil {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.deliver(runCtx, reply)
	}
}

func (s *Service) deliver(ctx context.Context, delivery messaging.ReplyDelivery) {
	s.mu.Lock()
	waiters := s.waiters[delivery.Result.TaskID]
	delete(s.waiters, delivery.Result.TaskID)
	s.mu.Unlock()
	for _, waiter := range waiters {
		close(waiter)
	}
	_ = s.store.AckReply(ctx, delivery.StreamID)
}

func (s *Service) Handle(ctx context.Context, inbound message.InboundMessage) (message.OutboundMessage, error) {
	task, err := s.router.Resolve(ctx, inbound)
	if err != nil {
		switch {
		case errors.Is(err, routing.ErrUnknownBinding):
			return message.OutboundMessage{}, executor.ErrUnknownBinding
		case errors.Is(err, routing.ErrConfigurationUnavailable):
			return message.OutboundMessage{}, executor.ErrConfigurationUnavailable
		default:
			return message.OutboundMessage{}, err
		}
	}
	snapshot, _, err := s.store.Submit(ctx, task)
	if err != nil {
		if errors.Is(err, messaging.ErrConflict) {
			if existing, snapshotErr := s.store.Snapshot(ctx, task.InboxID()); snapshotErr == nil {
				return message.OutboundMessage{TraceID: existing.TraceID}, ErrMessageConflict
			}
			return message.OutboundMessage{}, ErrMessageConflict
		}
		return message.OutboundMessage{}, ErrMessagingUnavailable
	}
	if snapshot.Terminal() {
		return resultForRequest(snapshot, inbound.RequestID)
	}

	waiter := make(chan struct{})
	s.mu.Lock()
	s.waiters[snapshot.TaskID] = append(s.waiters[snapshot.TaskID], waiter)
	s.mu.Unlock()
	defer s.removeWaiter(snapshot.TaskID, waiter)

	timer := time.NewTimer(s.store.Config().ReplyWaitTimeout)
	poll := time.NewTicker(250 * time.Millisecond)
	defer timer.Stop()
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return message.OutboundMessage{TraceID: snapshot.TraceID}, ErrTaskPending
		case <-timer.C:
			return message.OutboundMessage{TraceID: snapshot.TraceID}, ErrTaskPending
		case <-waiter:
		case <-poll.C:
		}
		current, err := s.store.Snapshot(context.Background(), task.InboxID())
		if err == nil && current.Terminal() {
			return resultForRequest(current, inbound.RequestID)
		}
	}
}

func resultForRequest(snapshot messaging.Snapshot, requestID string) (message.OutboundMessage, error) {
	if snapshot.Result == nil {
		return message.OutboundMessage{TraceID: snapshot.TraceID}, ErrMessagingUnavailable
	}
	if snapshot.Result.Succeeded {
		reply := snapshot.Result.Reply
		reply.Channel = snapshot.Result.Channel
		reply.BindingID = snapshot.Result.BindingID
		reply.RequestID = requestID
		return reply, nil
	}
	switch snapshot.Result.ErrorCode {
	case "model_timeout":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrAgentTimeout
	case "dependency_unavailable":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrDependencyUnavailable
	case "configuration_unavailable", "invalid_task":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrConfigurationUnavailable
	case "worker_lost":
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrWorkerLost
	default:
		return message.OutboundMessage{TraceID: snapshot.Result.TraceID}, executor.ErrAgentFailed
	}
}

func (s *Service) removeWaiter(taskID string, target chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.waiters[taskID]
	for index, waiter := range current {
		if waiter == target {
			current = append(current[:index], current[index+1:]...)
			break
		}
	}
	if len(current) == 0 {
		delete(s.waiters, taskID)
	} else {
		s.waiters[taskID] = current
	}
}

func (s *Service) Ready(ctx context.Context) error {
	if !s.running.Load() {
		return errors.New("gateway reply consumer is not running")
	}
	return s.store.Ready(ctx)
}

func (s *Service) Close() error {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
