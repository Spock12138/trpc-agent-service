// Package storage contains the phase 1 Redis-backed state adapters.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// ErrUnsupported marks framework capabilities that the Phase 1 Redis snapshot
// adapter intentionally does not implement. It prevents silent local fallback.
var ErrUnsupported = errors.New("operation is not supported by the phase 1 redis adapter")

type RedisSessionService struct {
	client redis.UniversalClient
	prefix string
	mu     sync.Mutex
}

func NewRedisSessionService(client redis.UniversalClient, prefix string) *RedisSessionService {
	return &RedisSessionService{
		client: client,
		prefix: prefix,
	}
}

func (s *RedisSessionService) key(key session.Key) string {
	return s.prefix + ":session:" + digest(key.AppName, key.UserID, key.SessionID)
}

func (s *RedisSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, _ ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, err := s.load(ctx, key); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	if state != nil {
		sess.State = cloneState(state)
	}
	if err := s.save(ctx, key, sess); err != nil {
		return nil, err
	}
	return sess, nil
}

func (s *RedisSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.load(ctx, key)
	if err != nil || sess == nil {
		return sess, err
	}
	copy := sess.Clone()
	copy.ApplyEventFiltering(opts...)
	return copy, nil
}

func (s *RedisSessionService) ListSessions(context.Context, session.UserKey, ...session.Option) ([]*session.Session, error) {
	return nil, unsupported("list sessions")
}

func (s *RedisSessionService) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, opts ...session.Option) error {
	if sess == nil {
		return session.ErrNilSession
	}
	if e == nil {
		return fmt.Errorf("event is nil")
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Reload before updating so a new process observes the latest persisted snapshot.
	latest, err := s.load(ctx, key)
	if err != nil {
		return err
	}
	if latest != nil {
		copySession(sess, latest)
	}
	sess.UpdateUserSession(e, opts...)
	return s.save(ctx, key, sess)
}

func (s *RedisSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, err := s.load(ctx, key)
	if err != nil {
		return err
	}
	if sess == nil {
		return fmt.Errorf("session not found")
	}
	for stateKey, value := range state {
		if strings.HasPrefix(stateKey, session.StateAppPrefix) || strings.HasPrefix(stateKey, session.StateUserPrefix) {
			return fmt.Errorf("session state key %s has a non-session prefix", stateKey)
		}
		sess.SetState(stateKey, value)
	}
	sess.UpdatedAt = time.Now().UTC()
	return s.save(ctx, key, sess)
}

func (s *RedisSessionService) DeleteSession(ctx context.Context, key session.Key, _ ...session.Option) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	return s.client.Del(ctx, s.key(key)).Err()
}

func (s *RedisSessionService) UpdateAppState(context.Context, string, session.StateMap) error {
	return unsupported("update app state")
}

func (s *RedisSessionService) DeleteAppState(context.Context, string, string) error {
	return unsupported("delete app state")
}

func (s *RedisSessionService) ListAppStates(context.Context, string) (session.StateMap, error) {
	return nil, unsupported("list app states")
}

func (s *RedisSessionService) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return unsupported("update user state")
}

func (s *RedisSessionService) ListUserStates(context.Context, session.UserKey) (session.StateMap, error) {
	return nil, unsupported("list user states")
}

func (s *RedisSessionService) DeleteUserState(context.Context, session.UserKey, string) error {
	return unsupported("delete user state")
}

func (s *RedisSessionService) CreateSessionSummary(context.Context, *session.Session, string, bool) error {
	return unsupported("create session summary")
}

func (s *RedisSessionService) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	return unsupported("enqueue session summary")
}

func (s *RedisSessionService) GetSessionSummaryText(context.Context, *session.Session, ...session.SummaryOption) (string, bool) {
	return "", false
}

func (s *RedisSessionService) Close() error { return nil }

func (s *RedisSessionService) load(ctx context.Context, key session.Key) (*session.Session, error) {
	raw, err := s.client.Get(ctx, s.key(key)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load session from redis: %w", err)
	}
	var sess session.Session
	if err := json.Unmarshal(raw, &sess); err != nil {
		return nil, fmt.Errorf("decode session from redis: %w", err)
	}
	sess.Hash = session.HashString(fmt.Sprintf("%s:%s:%s", sess.AppName, sess.UserID, sess.ID))
	if sess.State == nil {
		sess.State = make(session.StateMap)
	}
	if sess.Events == nil {
		sess.Events = []event.Event{}
	}
	return &sess, nil
}

func (s *RedisSessionService) save(ctx context.Context, key session.Key, sess *session.Session) error {
	raw, err := json.Marshal(sess)
	if err != nil {
		return fmt.Errorf("encode session for redis: %w", err)
	}
	if err := s.client.Set(ctx, s.key(key), raw, 0).Err(); err != nil {
		return fmt.Errorf("save session to redis: %w", err)
	}
	return nil
}

type RedisMemoryService struct {
	client redis.UniversalClient
	prefix string
	mu     sync.Mutex
}

func NewRedisMemoryService(client redis.UniversalClient, prefix string) *RedisMemoryService {
	return &RedisMemoryService{
		client: client,
		prefix: prefix,
	}
}

func (s *RedisMemoryService) key(key memory.UserKey) string {
	return s.prefix + ":memory:" + digest(key.AppName, key.UserID)
}

func (s *RedisMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	if err := key.CheckUserKey(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(ctx, key)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].UpdatedAt.After(entries[j].UpdatedAt) })
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (s *RedisMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, _ ...memory.SearchOption) ([]*memory.Entry, error) {
	entries, err := s.ReadMemories(ctx, key, 0)
	if err != nil {
		return nil, err
	}
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return entries, nil
	}
	out := make([]*memory.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Memory != nil && strings.Contains(strings.ToLower(entry.Memory.Memory), query) {
			entry.Score = 1
			out = append(out, entry)
		}
	}
	return out, nil
}

func (s *RedisMemoryService) AddMemory(ctx context.Context, key memory.UserKey, text string, topics []string, _ ...memory.AddOption) error {
	if err := key.CheckUserKey(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(ctx, key)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	id := digest(key.AppName, key.UserID, text)
	for _, entry := range entries {
		if entry.ID == id {
			entry.Memory = &memory.Memory{Memory: text, Topics: append([]string(nil), topics...), LastUpdated: &now}
			entry.UpdatedAt = now
			return s.save(ctx, key, entries)
		}
	}
	entries = append(entries, &memory.Entry{ID: id, AppName: key.AppName, UserID: key.UserID, Memory: &memory.Memory{Memory: text, Topics: append([]string(nil), topics...), LastUpdated: &now}, CreatedAt: now, UpdatedAt: now})
	return s.save(ctx, key, entries)
}

func (s *RedisMemoryService) UpdateMemory(ctx context.Context, key memory.Key, text string, topics []string, _ ...memory.UpdateOption) error {
	if err := key.CheckMemoryKey(); err != nil {
		return err
	}
	userKey := memory.UserKey{AppName: key.AppName, UserID: key.UserID}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(ctx, userKey)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.ID == key.MemoryID {
			now := time.Now().UTC()
			entry.Memory = &memory.Memory{Memory: text, Topics: append([]string(nil), topics...), LastUpdated: &now}
			entry.UpdatedAt = now
			return s.save(ctx, userKey, entries)
		}
	}
	return fmt.Errorf("memory %s not found", key.MemoryID)
}

func (s *RedisMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	if err := key.CheckMemoryKey(); err != nil {
		return err
	}
	userKey := memory.UserKey{AppName: key.AppName, UserID: key.UserID}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load(ctx, userKey)
	if err != nil {
		return err
	}
	filtered := entries[:0]
	for _, entry := range entries {
		if entry.ID != key.MemoryID {
			filtered = append(filtered, entry)
		}
	}
	return s.save(ctx, userKey, filtered)
}

func (s *RedisMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	if err := key.CheckUserKey(); err != nil {
		return err
	}
	return s.client.Del(ctx, s.key(key)).Err()
}

func (s *RedisMemoryService) Tools() []tool.Tool { return nil }

func (s *RedisMemoryService) EnqueueAutoMemoryJob(context.Context, *session.Session) error {
	return unsupported("enqueue auto memory")
}

func (s *RedisMemoryService) Close() error { return nil }

func (s *RedisMemoryService) load(ctx context.Context, key memory.UserKey) ([]*memory.Entry, error) {
	raw, err := s.client.Get(ctx, s.key(key)).Bytes()
	if err == redis.Nil {
		return []*memory.Entry{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load memories from redis: %w", err)
	}
	var entries []*memory.Entry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("decode memories from redis: %w", err)
	}
	return entries, nil
}

func (s *RedisMemoryService) save(ctx context.Context, key memory.UserKey, entries []*memory.Entry) error {
	raw, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("encode memories for redis: %w", err)
	}
	return s.client.Set(ctx, s.key(key), raw, 0).Err()
}

func digest(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		fmt.Fprintf(h, "%d:", len(part))
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func unsupported(operation string) error {
	return fmt.Errorf("%w: %s", ErrUnsupported, operation)
}

func cloneState(state session.StateMap) session.StateMap {
	if state == nil {
		return make(session.StateMap)
	}
	copyState := make(session.StateMap, len(state))
	for key, value := range state {
		copyState[key] = append([]byte(nil), value...)
	}
	return copyState
}

func copySession(dst, src *session.Session) {
	dst.ID = src.ID
	dst.AppName = src.AppName
	dst.UserID = src.UserID
	dst.State = cloneState(src.State)
	dst.Events = append([]event.Event(nil), src.Events...)
	dst.Tracks = src.Tracks
	dst.Summaries = src.Summaries
	dst.UpdatedAt = src.UpdatedAt
	dst.CreatedAt = src.CreatedAt
	dst.Hash = src.Hash
}
