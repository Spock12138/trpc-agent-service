package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestRedisMemoryServiceIsVisibleAcrossInstances(t *testing.T) {
	server := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer clientA.Close()
	defer clientB.Close()
	serviceA := NewRedisMemoryService(clientA, "test")
	serviceB := NewRedisMemoryService(clientB, "test")
	defer serviceA.Close()
	defer serviceB.Close()

	key := memory.UserKey{AppName: "app", UserID: "user"}
	if err := serviceA.AddMemory(context.Background(), key, "likes redis", []string{"preference"}); err != nil {
		t.Fatal(err)
	}
	entries, err := serviceB.SearchMemories(context.Background(), key, "redis")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != "likes redis" {
		t.Fatalf("unexpected entries: %#v", entries)
	}
}

func TestRedisSessionServiceIsVisibleAcrossInstances(t *testing.T) {
	server := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer clientA.Close()
	defer clientB.Close()
	serviceA := NewRedisSessionService(clientA, "test")
	serviceB := NewRedisSessionService(clientB, "test")

	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	if _, err := serviceA.CreateSession(context.Background(), key, session.StateMap{"turn": []byte("one")}); err != nil {
		t.Fatal(err)
	}
	got, err := serviceB.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || string(got.State["turn"]) != "one" {
		t.Fatalf("unexpected session: %#v", got)
	}
}

func TestRedisAdaptersFailClosedForUnsupportedOperations(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	sessions := NewRedisSessionService(client, "test")
	memories := NewRedisMemoryService(client, "test")

	if _, err := sessions.ListSessions(context.Background(), session.UserKey{AppName: "app", UserID: "user"}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ListSessions() error = %v, want ErrUnsupported", err)
	}
	if err := sessions.UpdateAppState(context.Background(), "app", session.StateMap{"key": []byte("value")}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("UpdateAppState() error = %v, want ErrUnsupported", err)
	}
	if err := memories.EnqueueAutoMemoryJob(context.Background(), nil); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("EnqueueAutoMemoryJob() error = %v, want ErrUnsupported", err)
	}
}
