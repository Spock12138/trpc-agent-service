package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadPlatformCatalog(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	path := writeCatalogFile(t, validCatalogJSON())
	t.Setenv("PLATFORM_CONFIG_FILE", path)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CatalogPath != path || len(cfg.Catalog.Tenants) != 1 || cfg.ModelName != "" {
		t.Fatalf("unexpected catalog config: %#v", cfg)
	}
	catalog, credentials, err := cfg.RuntimeCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.ConfigVersions[0].Model.RequestTimeout != 5*time.Second {
		t.Fatalf("request timeout = %s", catalog.ConfigVersions[0].Model.RequestTimeout)
	}
	if value, err := credentials.Resolve("env:TENANT_MODEL_KEY"); err != nil || value != "model-secret" {
		t.Fatalf("credential resolve = (%q, %v)", value, err)
	}
}

func TestLoadPlatformCatalogStrictFailures(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	tests := []struct {
		name string
		data []byte
	}{
		{name: "unknown field", data: []byte(strings.Replace(validCatalogJSON(), `"schema_version": 1`, `"schema_version": 1, "plaintext_api_key": "must-reject"`, 1))},
		{name: "binding credential ref", data: []byte(strings.Replace(validCatalogJSON(), `"agent_app_id":"assistant","enabled":true}]`, `"agent_app_id":"assistant","credential_ref":"env:TENANT_MODEL_KEY","enabled":true}]`, 1))},
		{name: "multiple objects", data: []byte(validCatalogJSON() + `{}`)},
		{name: "unsupported schema", data: []byte(strings.Replace(validCatalogJSON(), `"schema_version": 1`, `"schema_version": 2`, 1))},
		{name: "oversized", data: bytes.Repeat([]byte("x"), MaxCatalogBytes+1)},
		{name: "model URL userinfo", data: []byte(strings.Replace(validCatalogJSON(), "https://example.test", "https://secret@example.test", 1))},
		{name: "model URL query", data: []byte(strings.Replace(validCatalogJSON(), "https://example.test", "https://example.test?api_key=secret", 1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeCatalogBytes(t, test.data)
			t.Setenv("PLATFORM_CONFIG_FILE", path)
			if _, err := Load(); err == nil {
				t.Fatal("expected strict catalog error")
			}
		})
	}
}

func TestPhase2ExampleCatalogParses(t *testing.T) {
	catalog, err := loadCatalogFile(filepath.Join("..", "..", "configs", "phase2.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Tenants) != 2 || len(catalog.StorageProfiles) != 2 || len(catalog.ChannelBindings) != 2 {
		t.Fatalf("unexpected example catalog: %#v", catalog)
	}
}

func TestPhase3ExampleConfigParses(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MODEL_KEY", "model-secret")
	t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", "phase3.example.json"))
	cfg, err := LoadForRole(RoleServe)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messaging == nil || len(cfg.Catalog.Tenants) != 2 {
		t.Fatalf("unexpected Phase 3 config: %#v", cfg)
	}
}

func TestLoadPlatformCatalogValidatesEveryEnabledVersionCredential(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	secondVersion := `,
    {
      "tenant_id":"tenant-a","agent_app_id":"assistant","version":"v2",
      "storage_profile_id":"redis-v1","instruction":"v2",
      "model":{"name":"model","base_url":"https://example.test","credential_ref":"env:MISSING_MODEL_KEY","request_timeout":"5s","max_output_tokens":128}
    }`
	data := strings.Replace(validCatalogJSON(), "\n  }],\n  \"channel_bindings\"", "\n  }"+secondVersion+"\n  ],\n  \"channel_bindings\"", 1)
	t.Setenv("PLATFORM_CONFIG_FILE", writeCatalogFile(t, data))
	_, err := Load()
	if err == nil {
		t.Fatal("expected missing inactive version credential to reject startup")
	}
	if strings.Contains(err.Error(), "MISSING_MODEL_KEY") {
		t.Fatalf("credential reference leaked: %v", err)
	}
}

func writeCatalogFile(t *testing.T, data string) string {
	t.Helper()
	return writeCatalogBytes(t, []byte(data))
}

func writeCatalogBytes(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "platform.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func validCatalogJSON() string {
	return `{
  "schema_version": 1,
	"messaging": {"redis_credential_ref":"env:PHASE3_MESSAGING_REDIS_URL","key_prefix":"test-messaging"},
  "tenants": [{"id":"tenant-a","enabled":true}],
  "storage_profiles": [{"tenant_id":"tenant-a","id":"redis-v1","kind":"redis","credential_ref":"env:TENANT_REDIS_URL","key_prefix":"test"}],
  "agent_apps": [{"tenant_id":"tenant-a","id":"assistant","enabled":true,"active_config_version":"v1"}],
  "config_versions": [{
    "tenant_id":"tenant-a","agent_app_id":"assistant","version":"v1",
    "storage_profile_id":"redis-v1","instruction":"test",
    "model":{"name":"model","base_url":"https://example.test","credential_ref":"env:TENANT_MODEL_KEY","request_timeout":"5s","max_output_tokens":128}
  }],
  "channel_bindings": [{"id":"binding-a","channel":"demo","external_account_id":"binding-a","tenant_id":"tenant-a","agent_app_id":"assistant","enabled":true}]
}`
}

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
	messaging := MessagingConfig{
		RedisCredentialRef: "env:SENSITIVE_MESSAGING_REF",
		RedisURL:           "redis://message-user:message-password@sensitive-messaging-host:6379/0",
	}
	cfg := Config{
		ModelAPIKey:    "model-secret-value",
		ModelBaseURL:   "https://model-user:model-url-secret@example.test/v1?api_key=query-secret",
		IdentitySecret: []byte("identity-secret-value"),
		RedisURL:       "redis://user:redis-secret-value@localhost:6379/0",
		Messaging:      &messaging,
	}
	formatted := fmt.Sprintf("%v %#v", cfg, cfg)
	for _, secret := range []string{
		"model-secret-value", "model-url-secret", "query-secret", "identity-secret-value", "redis-secret-value",
		"SENSITIVE_MESSAGING_REF", "message-password", "sensitive-messaging-host",
	} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("formatted config leaked %q: %s", secret, formatted)
		}
	}
}

func TestLegacyGatewayDoesNotReadModelKeyValue(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test/v1")
	t.Setenv("MODEL_API_KEY_ENV", "LEGACY_MISSING_MODEL_KEY")
	t.Setenv("LEGACY_MISSING_MODEL_KEY", "legacy-model-secret")
	t.Setenv("REDIS_URL", "localhost:6379")
	cfg, err := LoadForRole(RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelAPIKey != "" || cfg.Messaging == nil || cfg.Messaging.KeyPrefix != DefaultRedisPrefix+":messaging" {
		t.Fatalf("unexpected legacy Gateway config: %#v", cfg)
	}
	t.Setenv("LEGACY_MISSING_MODEL_KEY", "")
	if _, err := LoadForRole(RoleWorker); err == nil {
		t.Fatal("legacy Worker unexpectedly accepted missing model key")
	}
}

func TestGatewayRoleDoesNotResolveModelCredential(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "")
	t.Setenv("TENANT_REDIS_URL", "")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	t.Setenv("PLATFORM_CONFIG_FILE", writeCatalogFile(t, validCatalogJSON()))
	cfg, err := LoadForRole(RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messaging == nil || cfg.Messaging.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected Gateway messaging config: %#v", cfg.Messaging)
	}
	if _, err := LoadForRole(RoleWorker); err == nil {
		t.Fatal("Worker unexpectedly accepted missing model/storage credentials")
	}
}
