package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestNormalizeRedisURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "host shorthand", in: "localhost:6379", want: "redis://localhost:6379/0"},
		{name: "host shorthand with db", in: "localhost:6379/2", want: "redis://localhost:6379/2"},
		{name: "full uri", in: "redis://user:pass@localhost:6379/3", want: "redis://user:pass@localhost:6379/3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeRedisURL(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeRedisURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeRedisURLRejectsUnsupportedScheme(t *testing.T) {
	for _, input := range []string{
		"http://localhost:6379",
		"redis://localhost",
		"redis://localhost:bad/0",
		"redis://localhost:6379/not-a-db",
		"redis://localhost:6379/0/extra",
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := NormalizeRedisURL(input); err == nil {
				t.Fatalf("NormalizeRedisURL(%q) unexpectedly succeeded", input)
			}
		})
	}
}

func TestLoadDefaultsAndAPIKeyIndirection(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test/v1/")
	t.Setenv("MODEL_API_KEY_ENV", "TEST_MODEL_KEY")
	t.Setenv("TEST_MODEL_KEY", "secret-value")
	t.Setenv("IDENTITY_SECRET", strings.Repeat("x", 32))
	t.Setenv("REDIS_URL", "localhost:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelRequestTimeout != 60*time.Second || cfg.ModelMaxOutput != 1024 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
	if cfg.ModelBaseURL != "https://example.test/v1" || cfg.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected normalized config: %#v", cfg)
	}
}

func TestLoadRejectsShortIdentitySecret(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test")
	t.Setenv("MODEL_API_KEY_ENV", "TEST_MODEL_KEY")
	t.Setenv("TEST_MODEL_KEY", "secret-value")
	t.Setenv("IDENTITY_SECRET", "short")
	t.Setenv("REDIS_URL", "localhost:6379")
	if _, err := Load(); err == nil {
		t.Fatal("expected short identity secret error")
	}
}

func TestLoadRejectsMissingReferencedAPIKey(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test")
	t.Setenv("MODEL_API_KEY_ENV", "potentialsecret")
	t.Setenv("potentialsecret", "")
	t.Setenv("IDENTITY_SECRET", strings.Repeat("x", 32))
	t.Setenv("REDIS_URL", "localhost:6379")
	_, err := Load()
	if err == nil {
		t.Fatal("expected missing referenced API key error")
	}
	if strings.Contains(err.Error(), "potentialsecret") {
		t.Fatalf("configuration error echoed MODEL_API_KEY_ENV value: %v", err)
	}
}

func TestLoadRejectsInvalidAPIKeyEnvironmentNameWithoutEchoingIt(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test")
	t.Setenv("MODEL_API_KEY_ENV", "sk-must-not-be-echoed")
	t.Setenv("IDENTITY_SECRET", strings.Repeat("x", 32))
	t.Setenv("REDIS_URL", "localhost:6379")
	_, err := Load()
	if err == nil {
		t.Fatal("expected invalid API key environment name error")
	}
	if strings.Contains(err.Error(), "sk-must-not-be-echoed") {
		t.Fatalf("configuration error echoed a possible API key: %v", err)
	}
}

func TestConfigFormattingRedactsSecrets(t *testing.T) {
	cfg := Config{
		ModelAPIKey:    "model-secret-value",
		IdentitySecret: []byte("identity-secret-value"),
		RedisURL:       "redis://user:redis-secret-value@localhost:6379/0",
	}
	formatted := fmt.Sprintf("%v %#v", cfg, cfg)
	for _, secret := range []string{"model-secret-value", "identity-secret-value", "redis-secret-value"} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("formatted config leaked %q: %s", secret, formatted)
		}
	}
}
