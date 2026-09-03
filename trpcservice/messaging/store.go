// Package messaging implements the Redis-backed reliable task and reply
// transport shared by Gateway and Worker processes.
package messaging

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"github.com/liuzengh/trpc-agent-service/trpcservice/keyspace"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/redistopology"
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
	ErrConflict          = errors.New("message idempotency conflict")
	ErrInboxMissing      = errors.New("inbox entry is missing")
	ErrLeaseLost         = errors.New("task lease is lost")
	ErrTerminal          = errors.New("task is already terminal")
	ErrKeyType           = errors.New("messaging Redis key has an incompatible type")
	ErrClosed            = errors.New("messaging store is closed")
	ErrSessionBusy       = errors.New("session is busy")
	ErrSessionWait       = errors.New("session is waiting for predecessor")
	ErrSessionStale      = errors.New("session sequence is stale")
	ErrSessionLockActive = errors.New("session lock is still active")
)

type Snapshot struct {
	InboxKey     string
	TaskID       string
	Digest       string
	State        string
	Attempt      int
	TraceID      string
	ErrorCode    string
	Result       *message.TaskResult
	RawPayload   string
	SessionCoord string
	SessionSeq   int64
	Owner        string
	LeaseEpoch   int64
	LeaseUntil   int64
}

func (s Snapshot) Terminal() bool {
	return s.State == StateSucceeded || s.State == StateFailedTerminal
}

type Delivery struct {
	StreamID        string
	InboxID         string
	Task            message.ExecutionTask
	PendingConsumer string
	PendingIdle     time.Duration
}

type Lease struct {
	Delivery     Delivery
	InboxKey     string
	Owner        string
	Epoch        int64
	SessionCoord string
	SessionSeq   int64
	LockToken    string
}

type ReplyDelivery struct {
	StreamID string
	Result   message.TaskResult
}

type Store struct {
	client *redis.Client
	config config.MessagingConfig

	basePrefix     string
	taskStream     string
	replyStream    string
	retryKey       string
	inboxPrefix    string
	sessionWaitKey string

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func NewStore(cfg config.MessagingConfig) (*Store, error) {
	if cfg.SessionFencing == "" {
		cfg.SessionFencing = config.DefaultSessionFencing
	}
	if cfg.SessionLockDuration == 0 {
		cfg.SessionLockDuration = cfg.LeaseDuration
	}
	if cfg.SessionWaitBackoff == 0 {
		cfg.SessionWaitBackoff = config.DefaultSessionWaitBackoff
	}
	if cfg.SessionWaitMaxBackoff == 0 {
		cfg.SessionWaitMaxBackoff = config.DefaultSessionWaitMaxBackoff
	}
	if cfg.MaxTurnEvents == 0 {
		cfg.MaxTurnEvents = config.DefaultMaxTurnEvents
	}
	if cfg.MaxTurnBytes == 0 {
		cfg.MaxTurnBytes = config.DefaultMaxTurnBytes
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = config.DefaultShutdownTimeout
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, errors.New("parse messaging Redis URL")
	}
	base := keyspace.CoordinationPrefix(cfg.KeyPrefix)
	return &Store{
		client: redis.NewClient(options), config: cfg, basePrefix: base,
		taskStream: base + ":agent.tasks", replyStream: base + ":agent.replies",
		retryKey: base + ":agent.retry", inboxPrefix: base + ":inbox:",
		sessionWaitKey: keyspace.SessionWait(cfg.KeyPrefix),
	}, nil
}

func (s *Store) Ready(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("messaging Redis ping: %w", err)
	}
	if s.config.SessionFencing == "strong" {
		if _, err := redistopology.VerifyPrimaryStandalone(ctx, s.client); err != nil {
			return fmt.Errorf("messaging Redis topology: %w", err)
		}
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
	created := false
	if s.config.SessionFencing == "strong" {
		coord := sessionCoord(task)
		result, runErr := submitWithSessionScript.Run(ctx, s.client, []string{inboxKey, s.taskStream, s.sessionSeqKey(coord), s.sessionStateKey(coord)},
			task.TaskID, task.PayloadDigest, string(payload), task.Attempt, task.TraceID, inboxID, coord,
			strconv.FormatInt(time.Now().UnixMilli(), 10)).Slice()
		if runErr != nil {
			return Snapshot{}, false, fmt.Errorf("submit reliable task: %w", runErr)
		}
		if len(result) == 1 && asInt64(result[0]) == -1 {
			return Snapshot{}, false, ErrConflict
		}
		if len(result) == 1 && asInt64(result[0]) == -9 {
			return Snapshot{}, false, ErrKeyType
		}
		created = len(result) > 0 && asInt64(result[0]) == 1
	} else {
		result, runErr := legacySubmitScript.Run(ctx, s.client, []string{inboxKey, s.taskStream},
			task.TaskID, task.PayloadDigest, string(payload), task.Attempt, task.TraceID, inboxID).Int()
		if runErr != nil {
			return Snapshot{}, false, fmt.Errorf("submit reliable task: %w", runErr)
		}
		if result == -1 {
			return Snapshot{}, false, ErrConflict
		}
		if result == -9 {
			return Snapshot{}, false, ErrKeyType
		}
		created = result == 1
	}
	snapshot, err := s.snapshotByKey(ctx, inboxKey)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snapshot, created, nil
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
		RawPayload: values["payload"], SessionCoord: values["session_coord"], Owner: values["owner"],
	}
	snapshot.Attempt, _ = strconv.Atoi(values["attempt"])
	snapshot.SessionSeq, _ = strconv.ParseInt(values["session_seq"], 10, 64)
	snapshot.LeaseEpoch, _ = strconv.ParseInt(values["lease_epoch"], 10, 64)
	snapshot.LeaseUntil, _ = strconv.ParseInt(values["lease_until"], 10, 64)
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
	snapshot, err := s.snapshotByKey(ctx, inboxKey)
	if err != nil {
		return Lease{}, err
	}
	coord := snapshot.SessionCoord
	if coord == "" {
		coord = sessionCoord(delivery.Task)
	}
	lockToken := randomToken()
	var value []interface{}
	if s.config.SessionFencing == "strong" {
		value, err = beginScript.Run(ctx, s.client, []string{inboxKey, s.sessionLockKey(coord), s.sessionStateKey(coord)},
			delivery.Task.TaskID, delivery.Task.PayloadDigest, consumer, delivery.StreamID,
			delivery.Task.Attempt, now.Add(s.config.LeaseDuration).UnixMilli(), lockToken,
			s.config.SessionLockDuration.Milliseconds(), coord, snapshot.SessionSeq, now.Add(s.config.SessionLockDuration).UnixMilli()).Slice()
	} else {
		value, err = legacyBeginScript.Run(ctx, s.client, []string{inboxKey},
			delivery.Task.TaskID, delivery.Task.PayloadDigest, consumer, delivery.StreamID,
			delivery.Task.Attempt, now.Add(s.config.LeaseDuration).UnixMilli()).Slice()
	}
	if err != nil {
		return Lease{}, err
	}
	if len(value) != 2 {
		return Lease{}, ErrLeaseLost
	}
	if asInt64(value[0]) == -9 {
		return Lease{}, ErrKeyType
	}
	if asInt64(value[0]) == -3 {
		return Lease{}, ErrSessionBusy
	}
	if asInt64(value[0]) == -4 {
		return Lease{}, ErrSessionWait
	}
	if asInt64(value[0]) == -5 {
		return Lease{}, ErrSessionStale
	}
	if asInt64(value[0]) != 1 {
		return Lease{}, ErrLeaseLost
	}
	return Lease{Delivery: delivery, InboxKey: inboxKey, Owner: consumer, Epoch: asInt64(value[1]), SessionCoord: coord, SessionSeq: snapshot.SessionSeq, LockToken: lockToken}, nil
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
	script := taskHeartbeatScript
	args := []interface{}{lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, now.Add(s.config.LeaseDuration).UnixMilli(), workerGroup}
	if s.config.SessionFencing == "strong" {
		script = strongTaskHeartbeatScript
		args = append(args, lease.Delivery.Task.PayloadDigest, now.UnixMilli())
	}
	result, err := script.Run(ctx, s.client, []string{lease.InboxKey, s.taskStream}, args...).Int()
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

// SessionHeartbeat renews only the Session lock. Task lease and Stream
// Pending are intentionally owned by Heartbeat.
func (s *Store) SessionHeartbeat(ctx context.Context, lease Lease) error {
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	result, err := sessionHeartbeatScript.Run(ctx, s.client,
		[]string{s.sessionLockKey(lease.SessionCoord), lease.InboxKey},
		lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.LockToken,
		s.config.SessionLockDuration.Milliseconds(),
		now.Add(s.config.SessionLockDuration).UnixMilli(), lease.Delivery.Task.PayloadDigest, now.UnixMilli()).Int()
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

// DeferSession moves a queued task into the Session wait set without
// increasing its retry attempt.
func (s *Store) DeferSession(ctx context.Context, delivery Delivery, due time.Time, reason string) error {
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	if due.IsZero() {
		due = now
	}
	result, err := deferSessionScript.Run(ctx, s.client,
		[]string{s.inboxKey(delivery.InboxID), s.sessionWaitKey, s.taskStream},
		delivery.Task.TaskID, delivery.Task.PayloadDigest, delivery.StreamID,
		due.UnixMilli(), workerGroup, reason, s.config.SessionWaitBackoff.Milliseconds(), s.config.SessionWaitMaxBackoff.Milliseconds(), now.UnixMilli()).Int()
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

// PromoteSessionWait atomically requeues due Session-wait members. Multiple
// workers may call this concurrently; the ZSET membership check makes the
// operation idempotent.
func (s *Store) PromoteSessionWait(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 32
	}
	now, err := s.redisTime(ctx)
	if err != nil {
		return 0, err
	}
	members, err := s.client.ZRangeByScore(ctx, s.sessionWaitKey, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: int64(limit)}).Result()
	if err != nil {
		return 0, err
	}
	promoted := 0
	for _, member := range members {
		parts := strings.SplitN(member, "|", 2)
		if len(parts) != 2 {
			_ = s.client.ZRem(ctx, s.sessionWaitKey, member).Err()
			continue
		}
		inboxKey, oldStream := parts[0], parts[1]
		values, e := s.client.HGetAll(ctx, inboxKey).Result()
		if e != nil || len(values) == 0 {
			_ = s.client.ZRem(ctx, s.sessionWaitKey, member).Err()
			continue
		}
		coord := values["session_coord"]
		if coord == "" {
			_ = s.client.ZRem(ctx, s.sessionWaitKey, member).Err()
			continue
		}
		result, e := promoteSessionScript.Run(ctx, s.client, []string{s.sessionWaitKey, inboxKey, s.taskStream, s.sessionStateKey(coord), s.sessionLockKey(coord)}, member, now.UnixMilli(), s.config.SessionWaitBackoff.Milliseconds(), s.config.SessionWaitMaxBackoff.Milliseconds()).Int()
		if e != nil {
			return promoted, e
		}
		if result == -9 {
			return promoted, ErrKeyType
		}
		if result == -1 {
			return promoted, ErrLeaseLost
		}
		if result == 1 {
			promoted++
		}
		_ = oldStream
	}
	return promoted, nil
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
	var result int
	if s.config.SessionFencing == "strong" {
		result, err = retryScript.Run(ctx, s.client, []string{lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.retryKey, s.taskStream},
			lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.LockToken, lease.Delivery.StreamID,
			next.Attempt, string(payload), now.Add(delay).UnixMilli(), errorCode, workerGroup,
			lease.Delivery.Task.PayloadDigest, now.UnixMilli()).Int()
	} else {
		result, err = legacyRetryScript.Run(ctx, s.client, []string{lease.InboxKey, s.retryKey, s.taskStream},
			lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID,
			next.Attempt, string(payload), now.Add(delay).UnixMilli(), errorCode, workerGroup).Int()
	}
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
	if s.config.SessionFencing == "strong" {
		return ErrLeaseLost
	}
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
	} else if s.config.SessionFencing == "strong" {
		now, timeErr := s.redisTime(ctx)
		if timeErr != nil {
			return timeErr
		}
		applied, err = failWithSessionScript.Run(ctx, s.client, []string{
			lease.InboxKey, s.sessionLockKey(lease.SessionCoord), s.sessionStateKey(lease.SessionCoord),
			s.replyStream, s.taskStream, s.sessionWaitKey,
		}, lease.Delivery.Task.TaskID, lease.Owner, lease.Epoch, lease.Delivery.StreamID, lease.LockToken,
			lease.SessionSeq, string(payload), errorCode, retention, workerGroup, now.UnixMilli(), lease.Delivery.Task.PayloadDigest).Int()
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

// ReleaseSessionLock is reserved for cancellation/shutdown cleanup. Normal
// terminal transitions release the lock inside their own Lua script.
func (s *Store) ReleaseSessionLock(ctx context.Context, lease Lease) error {
	result, err := releaseSessionLockScript.Run(ctx, s.client, []string{s.sessionLockKey(lease.SessionCoord)}, lease.LockToken, lease.Delivery.Task.TaskID, lease.Epoch).Int()
	if err != nil {
		return err
	}
	if result == -9 {
		return ErrKeyType
	}
	if result == 0 {
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
	if s.config.SessionFencing == "strong" && snapshot.SessionCoord != "" && snapshot.SessionSeq > 0 {
		now, nowErr := s.redisTime(ctx)
		if nowErr != nil {
			return nowErr
		}
		applied, scriptErr := rejectWithSessionScript.Run(ctx, s.client, []string{
			inboxKey, s.sessionStateKey(snapshot.SessionCoord), s.replyStream, s.taskStream, s.sessionWaitKey, s.sessionLockKey(snapshot.SessionCoord),
		}, snapshot.TaskID, delivery.StreamID, errorCode, string(payload), workerGroup, int64(s.config.InboxRetention/time.Second), now.UnixMilli(), s.config.SessionWaitBackoff.Milliseconds(), s.config.SessionWaitMaxBackoff.Milliseconds(), snapshot.Digest, snapshot.SessionCoord, snapshot.SessionSeq, snapshot.Owner, snapshot.LeaseEpoch).Int()
		if scriptErr != nil {
			return scriptErr
		}
		if applied == -9 {
			return ErrKeyType
		}
		if applied == -3 {
			return ErrSessionLockActive
		}
		if applied == -4 {
			return ErrLeaseLost
		}
		if applied == 3 {
			return ErrSessionWait
		}
		if applied != 1 && applied != 2 {
			return ErrLeaseLost
		}
		return nil
	}
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
	if s.config.SessionFencing == "strong" {
		return s.scanStale(ctx, count)
	}
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

// scanStale reads stale Pending entries without changing their owner. Strong
// Recover performs the ownership transfer only after it has checked the
// Session lock inside recover_with_session.
func (s *Store) scanStale(ctx context.Context, count int64) ([]Delivery, error) {
	if count <= 0 {
		count = 16
	}
	entries, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.taskStream, Group: workerGroup, Idle: s.config.LeaseDuration, Start: "-", End: "+", Count: count,
	}).Result()
	if err != nil {
		return nil, err
	}
	result := make([]Delivery, 0, len(entries))
	for _, pending := range entries {
		messages, rangeErr := s.client.XRange(ctx, s.taskStream, pending.ID, pending.ID).Result()
		if rangeErr != nil {
			return nil, rangeErr
		}
		if len(messages) == 0 {
			continue
		}
		delivery, decodeErr := decodeTaskMessage(messages[0])
		delivery.PendingConsumer = pending.Consumer
		delivery.PendingIdle = pending.Idle
		if decodeErr != nil || delivery.Task.Validate() != nil || delivery.InboxID != delivery.Task.InboxID() {
			_ = s.Reject(ctx, delivery, "invalid_task")
			continue
		}
		result = append(result, delivery)
	}
	return result, nil
}

func (s *Store) Recover(ctx context.Context, delivery Delivery, currentConsumer ...string) error {
	now, err := s.redisTime(ctx)
	if err != nil {
		return err
	}
	failed := message.TaskResult{SchemaVersion: message.TaskSchemaVersion, TaskID: delivery.Task.TaskID, ErrorCode: "worker_lost", TraceID: delivery.Task.TraceID}
	failedPayload, _ := json.Marshal(failed)
	inboxKey := s.inboxKey(delivery.InboxID)
	consumer := delivery.PendingConsumer
	if len(currentConsumer) > 0 && currentConsumer[0] != "" {
		consumer = currentConsumer[0]
	}
	if consumer == "" {
		consumer = "recovery"
	}
	if s.config.SessionFencing == "strong" {
		snapshot, snapshotErr := s.snapshotByKey(ctx, inboxKey)
		if snapshotErr != nil {
			return snapshotErr
		}
		coord := snapshot.SessionCoord
		if coord == "" {
			return s.Reject(ctx, delivery, "quarantine_missing_session_coord")
		}
		next := delivery.Task
		if snapshot.State == StateProcessing {
			next.Attempt++
		}
		nextPayload, marshalErr := json.Marshal(next)
		if marshalErr != nil {
			return marshalErr
		}
		result, scriptErr := recoverWithSessionScript.Run(ctx, s.client, []string{
			inboxKey, s.sessionLockKey(coord), s.sessionStateKey(coord), s.replyStream, s.taskStream, s.sessionWaitKey,
		}, delivery.Task.TaskID, delivery.StreamID, now.UnixMilli(), delivery.Task.Attempt, s.config.MaxAttempts,
			int64(s.config.InboxRetention/time.Second), string(failedPayload), workerGroup, consumer,
			delivery.Task.PayloadDigest, delivery.Task.Attempt, string(nextPayload)).Int()
		if scriptErr != nil {
			return scriptErr
		}
		switch result {
		case -9:
			return ErrKeyType
		case -3:
			return ErrSessionLockActive
		case -4:
			return ErrSessionWait
		case -2:
			return ErrLeaseLost
		case -1:
			return ErrLeaseLost
		}
		return nil
	}
	next := delivery.Task
	next.Attempt++
	nextPayload, _ := json.Marshal(next)
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

func (s *Store) sessionSeqKey(coord string) string {
	return keyspace.SessionSequence(s.config.KeyPrefix, coord)
}
func (s *Store) sessionStateKey(coord string) string {
	return keyspace.SessionState(s.config.KeyPrefix, coord)
}
func (s *Store) sessionLockKey(coord string) string {
	return keyspace.SessionLock(s.config.KeyPrefix, coord)
}

func randomToken() string {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		// crypto/rand failures are exceptionally rare; a process-local fallback
		// still preserves uniqueness for the current worker invocation.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(token[:])
}

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
