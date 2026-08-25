// Package config loads the small, explicit configuration surface used by the
// phase 1 demo runtime.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultRequestTimeout = 60 * time.Second
	DefaultMaxOutput      = 1024
	DefaultRedisPrefix    = "trpc-agent-service:phase1"
	MinIdentitySecretLen  = 32
)

// Config is intentionally limited to the phase 1 single-binding runtime.
type Config struct {
	ModelName           string
	ModelBaseURL        string
	ModelAPIKeyEnv      string
	ModelAPIKey         string
	ModelRequestTimeout time.Duration
	ModelMaxOutput      int
	IdentitySecret      []byte
	RedisURL            string
	RedisKeyPrefix      string

	BindingID     string
	TenantID      string
	AgentAppID    string
	ConfigVersion string
	AppName       string
}

// String deliberately omits secrets so configuration values are safe to use
// in diagnostics. Callers still must not log individual secret-bearing fields.
func (c Config) String() string {
	return fmt.Sprintf(
		"Config{ModelName:%q ModelBaseURL:%q ModelAPIKeyEnv:[REDACTED] ModelAPIKey:[REDACTED] ModelRequestTimeout:%s ModelMaxOutput:%d IdentitySecret:[REDACTED] RedisURL:%q RedisKeyPrefix:%q BindingID:%q TenantID:%q AgentAppID:%q ConfigVersion:%q AppName:%q}",
		c.ModelName,
		c.ModelBaseURL,
		c.ModelRequestTimeout,
		c.ModelMaxOutput,
		redactURLPassword(c.RedisURL),
		c.RedisKeyPrefix,
		c.BindingID,
		c.TenantID,
		c.AgentAppID,
		c.ConfigVersion,
		c.AppName,
	)
}

// GoString applies the same redaction to %#v formatting.
func (c Config) GoString() string { return c.String() }

// Load reads and validates the environment variables required by phase 1.
// The API key is read only through the indirection named by MODEL_API_KEY_ENV.
func Load() (Config, error) {
	modelName, err := requiredEnv("MODEL_NAME")
	if err != nil {
		return Config{}, err
	}
	baseURL, err := requiredEnv("MODEL_BASE_URL")
	if err != nil {
		return Config{}, err
	}
	keyEnv, err := requiredEnv("MODEL_API_KEY_ENV")
	if err != nil {
		return Config{}, err
	}
	if !validEnvName(keyEnv) {
		return Config{}, fmt.Errorf("MODEL_API_KEY_ENV must name a valid environment variable")
	}
	apiKey := strings.TrimSpace(os.Getenv(keyEnv))
	if apiKey == "" {
		return Config{}, fmt.Errorf("model api key referenced by MODEL_API_KEY_ENV is missing")
	}
	identitySecret, err := requiredEnv("IDENTITY_SECRET")
	if err != nil {
		return Config{}, err
	}
	if len([]byte(identitySecret)) < MinIdentitySecretLen {
		return Config{}, fmt.Errorf("IDENTITY_SECRET must be at least %d bytes", MinIdentitySecretLen)
	}
	redisRaw, err := requiredEnv("REDIS_URL")
	if err != nil {
		return Config{}, err
	}
	redisURL, err := NormalizeRedisURL(redisRaw)
	if err != nil {
		return Config{}, err
	}

	timeout, err := durationEnv("MODEL_REQUEST_TIMEOUT", DefaultRequestTimeout)
	if err != nil {
		return Config{}, err
	}
	if timeout <= 0 {
		return Config{}, fmt.Errorf("MODEL_REQUEST_TIMEOUT must be positive")
	}
	maxOutput, err := intEnv("MODEL_MAX_OUTPUT_TOKENS", DefaultMaxOutput)
	if err != nil {
		return Config{}, err
	}
	if maxOutput <= 0 {
		return Config{}, fmt.Errorf("MODEL_MAX_OUTPUT_TOKENS must be positive")
	}

	return Config{
		ModelName:           modelName,
		ModelBaseURL:        strings.TrimRight(baseURL, "/"),
		ModelAPIKeyEnv:      keyEnv,
		ModelAPIKey:         apiKey,
		ModelRequestTimeout: timeout,
		ModelMaxOutput:      maxOutput,
		IdentitySecret:      []byte(identitySecret),
		RedisURL:            redisURL,
		RedisKeyPrefix:      envOrDefault("REDIS_KEY_PREFIX", DefaultRedisPrefix),
		BindingID:           "demo-binding",
		TenantID:            "tenant-demo",
		AgentAppID:          "assistant",
		ConfigVersion:       "v1",
		AppName:             "tenant/tenant-demo/app/assistant",
	}, nil
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("required environment variable %s is missing", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return d, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return n, nil
}

// NormalizeRedisURL accepts the documented host:port shorthand and returns a
// canonical URI for the Redis client.
func NormalizeRedisURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("REDIS_URL is empty")
	}
	if !strings.Contains(raw, "://") {
		raw = "redis://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid REDIS_URL: %w", err)
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return "", fmt.Errorf("REDIS_URL must use redis or rediss scheme")
	}
	if u.Host == "" {
		return "", fmt.Errorf("REDIS_URL must include a host")
	}
	if u.Hostname() == "" || u.Port() == "" {
		return "", fmt.Errorf("REDIS_URL must include host and port")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("REDIS_URL has an invalid port")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/0"
	}
	database := strings.TrimPrefix(u.Path, "/")
	if strings.Contains(database, "/") {
		return "", fmt.Errorf("REDIS_URL database must be a non-negative integer")
	}
	db, err := strconv.Atoi(database)
	if err != nil || db < 0 {
		return "", fmt.Errorf("REDIS_URL database must be a non-negative integer")
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("REDIS_URL must not include a fragment")
	}
	return u.String(), nil
}

func redactURLPassword(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	username := u.User.Username()
	if _, hasPassword := u.User.Password(); hasPassword {
		u.User = url.UserPassword(username, "REDACTED")
	}
	return u.String()
}

func validEnvName(name string) bool {
	for i, r := range name {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return name != ""
}
