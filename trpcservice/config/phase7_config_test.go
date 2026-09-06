package config

import (
	"path/filepath"
	"testing"
)

func TestPhase7ExampleConfigsLoad(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", "01234567890123456789012345678901")
	t.Setenv("PHASE7_MODEL_KEY", "mock-local")
	t.Setenv("PHASE7_REAL_MODEL_KEY", "test-only")
	t.Setenv("PHASE7_TELEGRAM_TOKEN", "mock-token")
	t.Setenv("PHASE7_WECOM_BOT_ID", "test-bot")
	t.Setenv("PHASE7_WECOM_BOT_SECRET", "test-secret")
	t.Setenv("PHASE7_REDIS_URL", "redis://messaging-redis:6379/0")
	t.Setenv("PHASE7_CONTROL_REDIS_URL", "redis://control-redis:6379/0")
	t.Setenv("PHASE7_ADMIN_TOKEN", "admin-token")
	t.Setenv("PHASE7_POSTGRES_DSN", "postgres://phase7:phase7@postgres:5432/phase7?sslmode=disable")
	t.Setenv("PHASE7_MYSQL_DSN", "phase7:phase7@tcp(mysql:3306)/phase7?parseTime=true&charset=utf8mb4&loc=UTC")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	for _, name := range []string{"phase7.full.example.json", "phase7.light.example.json", "phase7.real-model.example.json", "phase7.real-wecom.example.json"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", name))
			cfg, err := LoadForRole(RoleServe)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := cfg.RuntimeCatalog(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
