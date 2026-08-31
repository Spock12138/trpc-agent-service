// Package messaging implements the Redis-backed reliable task and reply
// transport shared by Gateway and Worker processes.
package messaging

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

const (
	StateQueued         = "queued"
	StateProcessing     = "processing"
	StateRetryWait      = "retry_wait"
	StateSucceeded      = "succeeded"
	StateFailedTerminal = "failed_terminal"

	workerGroup  = "agent-workers-v1"
	gatewayGroup = "agent-gateways-v1"
)

var (
	ErrConflict     = errors.New("message idempotency conflict")
	ErrInboxMissing = errors.New("inbox entry is missing")
	ErrLeaseLost    = errors.New("task lease is lost")
	ErrTerminal     = errors.New("task is already terminal")
	ErrKeyType      = errors.New("messaging Redis key has an incompatible type")
	ErrClosed       = errors.New("messaging store is closed")
)

type Snapshot struct {
	InboxKey   string
	TaskID     string
	Digest     string
	State      string
	Attempt    int
	TraceID    string
	ErrorCode  string
	Result     *message.TaskResult
	RawPayload string
}

func (s Snapshot) Terminal() bool {
	return s.State == StateSucceeded || s.State == StateFailedTerminal
}

type Delivery struct {
	StreamID string
	InboxID  string
	Task     message.ExecutionTask
}

type Lease struct {
	Delivery Delivery
	InboxKey string
	Owner    string
	Epoch    int64
}

type ReplyDelivery struct {
	StreamID string
	Result   message.TaskResult
}

type Store struct {
	client *redis.Client
	config config.MessagingConfig

	basePrefix  string
	taskStream  string
	replyStream string
	retryKey    string
	inboxPrefix string

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func NewStore(cfg config.MessagingConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, errors.New("parse messaging Redis URL")
	}
	base := strings.TrimRight(cfg.KeyPrefix, ":") + ":reliable-v1"
	return &Store{
		client: redis.NewClient(options), config: cfg, basePrefix: base,
		taskStream: base + ":agent.tasks", replyStream: base + ":agent.replies",
		retryKey: base + ":agent.retry", inboxPrefix: base + ":inbox:",
	}, nil
}

func (s *Store) Ready(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("messaging Redis ping: %w", err)
	}
	if err := createGroup(ctx, s.client, s.taskStream, workerGroup); err != nil {
		return err
	}
	if err := createGroup(ctx, s.client, s.replyStream, gatewayGroup); err != nil {
		return err
	}
	return nil
}

func createGroup(ctx context.Context, client *redis.Client, stream, group string) error {
	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis Stream consumer group: %w", err)
	}
	return nil
}

func (s *Store) Submit(ctx context.Context, task message.ExecutionTask) (Snapshot, bool, error) {
	if err := task.Validate(); err != nil {
		return Snapshot{}, false, err
	}
	if err := s.ensureOpen(); err != nil {
		return Snapshot{}, false, err
	}
	payload, err := json.Marshal(task)
	if err != nil {
		return Snapshot{}, false, err
	}
	inboxID := task.InboxID()
	inboxKey := s.inboxKey(inboxID)
	result, err := submitScript.Run(ctx, s.client, []string{inboxKey, s.taskStream},
		task.TaskID, task.PayloadDigest, string(payload), task.Attempt, task.TraceID, inboxID).Int()
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("submit reliable task: %w", err)
	}
	if result == -1 {
		return Snapshot{}, false, ErrConflict
	}
	if result == -9 {
		return Snapshot{}, false, ErrKeyType
	}
	snapshot, err := s.snapshotByKey(ctx, inboxKey)
	return snapshot, result == 1, err
}

func (s *Store) Snapshot(ctx context.Context, inboxID string) (Snapshot, error) {
	return s.snapshotByKey(ctx, s.inboxKey(inboxID))
}

func (s *Store) snapshotByKey(ctx context.Context, inboxKey string) (Snapshot, error) {
	values, err := s.client.HGetAll(ctx, inboxKey).Result()
	if err != nil {
		return Snapshot{}, err
	}
	if len(values) == 0 {
		return Snapshot{}, ErrInboxMissing
	}
	snapshot := Snapshot{
		InboxKey: inboxKey, TaskID: values["task_id"], Digest: values["digest"],
		State: values["state"], TraceID: values["trace_id"], ErrorCode: values["error_code"],
		RawPayload: values["payload"],
	}
	snapshot.Attempt, _ = strconv.Atoi(values["attempt"])
	if raw := values["result"]; raw != "" {
		var result message.TaskResult
		if err := decodeStrictJSON(raw, &result); err != nil {
			return Snapshot{}, errors.New("decode stored task result")
		}
		if err := result.Validate(); err != nil {
			return Snapshot{}, errors.New("stored task result is invalid")
		}
		snapshot.Result = &result
	}
	return snapshot, nil
}

func (s *Store) ReadTask(ctx context.Context, consumer string, block time.Duration) (Delivery, error) {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: workerGroup, Consumer: consumer, Streams: []string{s.taskStream, ">"}, Count: 1, Block: block,
	}).Result()
	if err != nil {
		return Delivery{}, err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return Delivery{}, redis.Nil
	}
	return decodeTaskMessage(streams[0].Messages[0])
}

func decodeTaskMessage(current redis.XMessage) (Delivery, error) {
	inboxID, ok := current.Values["inbox_id"].(string)
	if !ok || !validInboxID(inboxID) {
		return Delivery{StreamID: current.ID}, errors.New("task stream inbox_id is missing")
	}
	raw, ok := current.Values["payload"].(string)
	if !ok {
		return Delivery{StreamID: current.ID, InboxID: inboxID}, errors.New("task stream payload is missing")
	}
	var task message.ExecutionTask
	if err := decodeStrictJSON(raw, &task); err != nil {
		return Delivery{StreamID: current.ID, InboxID: inboxID}, errors.New("task stream payload is invalid")
	}
	return Delivery{StreamID: current.ID, InboxID: inboxID, Task: task}, nil
}

func (s *Store) Begin(ctx context.Context, delivery Delivery, consumer string) (Lease, error) {
	if err := delivery.Task.Validate(); err != nil {
		return Lease{}, err
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return Lease{}, err
	}
	inboxKey := s.inboxKey(delivery.InboxID)
	value, err := beginScript.Run(ctx, s.client, []string{inboxKey},
		delivery.Task.TaskID, delivery.Task.PayloadDigest, consumer, delivery.StreamID,
		delivery.Task.Attempt, now.Add(s.config.LeaseDuration).UnixMilli()).Slice()
	if err != nil {
		return Lease{}, err
	}
	if len(value) != 2 {
		return Lease{}, ErrLeaseLost
	}
	if asInt64(value[0]) == -9 {
		return Lease{}, ErrKeyType
	}
	if asInt64(value[0]) != 1 {
		return Lease{}, ErrLeaseLost
	}
	return Lease{Delivery: delivery, InboxKey: inboxKey, Owner: consumer, Epoch: asInt64(value[1])}, nil
}

// RequeueAfterBeginFailure removes this consumer's Pending entry without
// losing a queued task. If the Inbox still points at the same entry, it is
// replaced with a fresh entry before the old one is acknowledged.
func (s *Store) RequeueAfterBeginFailure(ctx context.Context, delivery Delivery) error {
	if delivery.InboxID == "" || delivery.StreamID == "" {
		return nil
	}
	result, err := beginFailureScript.Run(ctx, s.client, []string{s.inboxKey(delivery.InboxID), s.taskStream},
		delivery.Task.TaskID, delivery.Task.PayloadDigest, delivery.StreamID, workerGroup).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, lease Lease) error {
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	result, err := heartbeatScript.Run(ctx, s.client, []string{lease.InboxKey, s.taskStream},
		lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID,
		now.Add(s.config.LeaseDuration).UnixMilli(), workerGroup).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) Retry(ctx context.Context, lease Lease, errorCode string, immediate bool) error {
	next := lease.Delivery.Task
	next.Attempt++
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	delay := time.Duration(0)
	if !immediate {
		delay = s.backoff(lease.Delivery.Task.Attempt)
	}
	result, err := retryScript.Run(ctx, s.client, []string{lease.InboxKey, s.retryKey, s.taskStream},
		lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID,
		next.Attempt, string(payload), now.Add(delay).UnixMilli(), errorCode, workerGroup).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) PromoteRetries(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 32
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return 0, err
	}
	promoted, err := promoteScript.Run(ctx, s.client, []string{s.retryKey, s.taskStream}, now.UnixMilli(), limit).Int()
	if promoted == -9 {
		return 0, ErrKeyType
	}
	return promoted, err
}

func (s *Store) Complete(ctx context.Context, lease Lease, reply message.OutboundMessage) error {
	if reply.Channel == "" {
		reply.Channel = lease.Delivery.Task.Channel
	}
	if reply.BindingID == "" {
		reply.BindingID = lease.Delivery.Task.ChannelBindingID
	}
	result := message.TaskResult{
		SchemaVersion: message.TaskSchemaVersion, TaskID: lease.Delivery.Task.TaskID, Succeeded: true,
		Channel: reply.Channel, BindingID: reply.BindingID, Reply: reply,
		TraceID: lease.Delivery.Task.TraceID,
	}
	return s.finish(ctx, lease, result, "")
}

func (s *Store) Fail(ctx context.Context, lease Lease, errorCode string) error {
	result := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: lease.Delivery.Task.TaskID, ErrorCode: errorCode, TraceID: lease.Delivery.Task.TraceID}
	return s.finish(ctx, lease, result, errorCode)
}

func (s *Store) finish(ctx context.Context, lease Lease, result message.TaskResult, errorCode string) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	retention := int64(s.config.InboxRetention / time.Second)
	var applied int
	if errorCode == "" {
		applied, err = completeScript.Run(ctx, s.client, []string{lease.InboxKey, s.replyStream, s.taskStream},
			lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID,
			string(payload), retention, workerGroup).Int()
	} else {
		applied, err = failScript.Run(ctx, s.client, []string{lease.InboxKey, s.replyStream, s.taskStream},
			lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID,
			string(payload), errorCode, retention, workerGroup).Int()
	}
	if err != nil {
		return err
	}
	if applied == -9 {
		return ErrKeyType
	}
	if applied != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) Reject(ctx context.Context, delivery Delivery, errorCode string) error {
	if delivery.InboxID == "" {
		_, _ = ackReplyScript.Run(ctx, s.client, []string{s.taskStream}, workerGroup, delivery.StreamID).Result()
		return nil
	}
	inboxKey := s.inboxKey(delivery.InboxID)
	snapshot, snapshotErr := s.snapshotByKey(ctx, inboxKey)
	if snapshotErr != nil {
		_, _ = ackReplyScript.Run(ctx, s.client, []string{s.taskStream}, workerGroup, delivery.StreamID).Result()
		return nil
	}
	result := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: snapshot.TaskID, ErrorCode: errorCode, TraceID: snapshot.TraceID}
	payload, _ := json.Marshal(result)
	applied, err := rejectScript.Run(ctx, s.client, []string{inboxKey, s.replyStream, s.taskStream},
		delivery.StreamID, string(payload), errorCode, int64(s.config.InboxRetention/time.Second), workerGroup).Int()
	if err != nil {
		return err
	}
	if applied == -9 {
		return ErrKeyType
	}
	if applied != 1 {
		// A malformed orphan must not remain pending forever.
		_, _ = ackReplyScript.Run(ctx, s.client, []string{s.taskStream}, workerGroup, delivery.StreamID).Result()
	}
	return nil
}

func (s *Store) ClaimStale(ctx context.Context, consumer string, count int64) ([]Delivery, error) {
	messages, _, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: s.taskStream, Group: workerGroup, Consumer: consumer,
		MinIdle: s.config.LeaseDuration, Start: "0-0", Count: count,
	}).Result()
	if err != nil {
		return nil, err
	}
	deliveries := make([]Delivery, 0, len(messages))
	for _, current := range messages {
		delivery, decodeErr := decodeTaskMessage(current)
		if decodeErr != nil || delivery.Task.Validate() != nil || delivery.InboxID != delivery.Task.InboxID() {
			_ = s.Reject(ctx, delivery, "invalid_task")
			continue
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, nil
}

func (s *Store) Recover(ctx context.Context, delivery Delivery) error {
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	next := delivery.Task
	next.Attempt++
	nextPayload, _ := json.Marshal(next)
	failed := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: delivery.Task.TaskID, ErrorCode: "worker_lost", TraceID: delivery.Task.TraceID}
	failedPayload, _ := json.Marshal(failed)
	inboxKey := s.inboxKey(delivery.InboxID)
	result, err := recoverScript.Run(ctx, s.client, []string{inboxKey, s.replyStream, s.taskStream},
		delivery.Task.TaskID, delivery.StreamID, now.UnixMilli(), next.Attempt,
		s.config.MaxAttempts, int64(s.config.InboxRetention/time.Second), string(failedPayload),
		workerGroup, string(nextPayload), delivery.Task.PayloadDigest, delivery.Task.Attempt, delivery.InboxID).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result < 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) ReadReply(ctx context.Context, consumer string, block time.Duration) (ReplyDelivery, error) {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: gatewayGroup, Consumer: consumer, Streams: []string{s.replyStream, ">"}, Count: 1, Block: block,
	}).Result()
	if err != nil {
		return ReplyDelivery{}, err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return ReplyDelivery{}, redis.Nil
	}
	current := streams[0].Messages[0]
	raw, ok := current.Values["payload"].(string)
	if !ok {
		return ReplyDelivery{StreamID: current.ID}, errors.New("reply stream payload is missing")
	}
	var result message.TaskResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return ReplyDelivery{StreamID: current.ID}, errors.New("reply stream payload is invalid")
	}
	if err := result.Validate(); err != nil {
		return ReplyDelivery{StreamID: current.ID}, errors.New("reply stream payload is invalid")
	}
	return ReplyDelivery{StreamID: current.ID, Result: result}, nil
}

func (s *Store) ClaimReplies(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]ReplyDelivery, error) {
	messages, _, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: s.replyStream, Group: gatewayGroup, Consumer: consumer,
		MinIdle: minIdle, Start: "0-0", Count: count,
	}).Result()
	if err != nil {
		return nil, err
	}
	result := make([]ReplyDelivery, 0, len(messages))
	for _, current := range messages {
		raw, ok := current.Values["payload"].(string)
		if !ok {
			_ = s.AckReply(ctx, current.ID)
			continue
		}
		var parsed message.TaskResult
		if decodeStrictJSON(raw, &parsed) != nil || parsed.Validate() != nil {
			_ = s.AckReply(ctx, current.ID)
			continue
		}
		result = append(result, ReplyDelivery{StreamID: current.ID, Result: parsed})
	}
	return result, nil
}

func (s *Store) AckReply(ctx context.Context, streamID string) error {
	result, err := ackReplyScript.Run(ctx, s.client, []string{s.replyStream}, gatewayGroup, streamID).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	return nil
}

func (s *Store) Config() config.MessagingConfig { return s.config }

func (s *Store) StreamNames() (string, string) { return s.taskStream, s.replyStream }

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.closeErr = s.client.Close()
	})
	return s.closeErr
}

func (s *Store) ensureOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) redisTime(ctx context.Context) (time.Time, error) {
	return s.client.Time(ctx).Result()
}

func (s *Store) backoff(attempt int) time.Duration {
	delay := s.config.InitialBackoff
	for current := 1; current < attempt; current++ {
		if delay >= s.config.MaxBackoff/2 {
			return s.config.MaxBackoff
		}
		delay *= 2
	}
	if delay > s.config.MaxBackoff {
		return s.config.MaxBackoff
	}
	return delay
}

func (s *Store) inboxKey(inboxID string) string { return s.inboxPrefix + inboxID }

func validInboxID(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func decodeStrictJSON(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("JSON contains more than one value")
	}
	return nil
}

func asInt64(value any) int64 {
	switch current := value.(type) {
	case int64:
		return current
	case string:
		parsed, _ := strconv.ParseInt(current, 10, 64)
		return parsed
	default:
		return 0
	}
}
