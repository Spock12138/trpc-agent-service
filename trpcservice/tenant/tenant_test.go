package tenant

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPresetRepositoryResolvesActiveCatalog(t *testing.T) {
	catalog := testCatalog()
	catalog.StorageProfiles = append(catalog.StorageProfiles, StorageProfile{TenantID: "tenant-a", ID: "memory-v2", Kind: StorageKindInMemory})
	catalog.ConfigVersions = append(catalog.ConfigVersions, ConfigVersion{
		TenantID: "tenant-a", AgentAppID: "assistant", Version: "v2", StorageProfileID: "memory-v2",
		Instruction: "test v2", Model: catalog.ConfigVersions[0].Model,
	})
	repository, err := NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := repository.ResolveBinding(context.Background(), "demo", "binding-a")
	if err != nil {
		t.Fatal(err)
	}
	if binding.TenantID != "tenant-a" || binding.AgentAppID != "assistant" {
		t.Fatalf("unexpected binding: %#v", binding)
	}
	profiles, err := repository.ListActiveStorageProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 || profiles[0].ID != "memory-v1" || profiles[1].ID != "memory-v2" {
		t.Fatalf("active profiles = %#v", profiles)
	}
	profiles[0].ID = "mutated"
	again, _ := repository.ListActiveStorageProfiles(context.Background())
	if again[0].ID != "memory-v1" {
		t.Fatal("repository returned mutable profile storage")
	}
}

func TestPresetRepositoryRejectsInvalidReferencesAndDuplicates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Catalog)
	}{
		{name: "duplicate tenant", mutate: func(c *Catalog) { c.Tenants = append(c.Tenants, c.Tenants[0]) }},
		{name: "unknown profile tenant", mutate: func(c *Catalog) { c.StorageProfiles[0].TenantID = "missing" }},
		{name: "unknown active version", mutate: func(c *Catalog) { c.AgentApps[0].ActiveConfigVersion = "v2" }},
		{name: "cross tenant profile", mutate: func(c *Catalog) { c.ConfigVersions[0].StorageProfileID = "missing" }},
		{name: "duplicate binding", mutate: func(c *Catalog) { c.ChannelBindings = append(c.ChannelBindings, c.ChannelBindings[0]) }},
		{name: "duplicate binding across channels", mutate: func(c *Catalog) {
			duplicate := c.ChannelBindings[0]
			duplicate.Channel = "telegram"
			c.ChannelBindings = append(c.ChannelBindings, duplicate)
		}},
		{name: "redis missing credential", mutate: func(c *Catalog) {
			c.StorageProfiles[0].Kind = StorageKindRedis
			c.StorageProfiles[0].KeyPrefix = "prefix"
		}},
		{name: "invalid id", mutate: func(c *Catalog) { c.Tenants[0].ID = "tenant/a" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := testCatalog()
			test.mutate(&catalog)
			if _, err := NewPresetRepository(catalog); err == nil {
				t.Fatal("expected catalog validation error")
			}
		})
	}
}

func TestPresetRepositoryHidesDisabledAndUnknownBindings(t *testing.T) {
	catalog := testCatalog()
	catalog.ChannelBindings[0].Enabled = false
	repository, err := NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, bindingID := range []string{"binding-a", "missing"} {
		if _, err := repository.ResolveBinding(context.Background(), "demo", bindingID); !errors.Is(err, ErrBindingNotFound) {
			t.Fatalf("ResolveBinding(%q) error = %v", bindingID, err)
		}
	}
}

func TestValidateIDAndAppName(t *testing.T) {
	if err := ValidateID("id", "safe-ID_1.0"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateID("id", "not safe"); err == nil {
		t.Fatal("expected invalid ID error")
	}
	if got := AppName("tenant-a", "assistant"); got != "tenant/tenant-a/app/assistant" {
		t.Fatalf("AppName() = %q", got)
	}
}

func testCatalog() Catalog {
	return Catalog{
		Tenants:         []Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []StorageProfile{{TenantID: "tenant-a", ID: "memory-v1", Kind: StorageKindInMemory}},
		AgentApps:       []AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []ConfigVersion{{
			TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1",
			Instruction: "test", Model: ModelConfig{
				Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL_KEY",
				RequestTimeout: time.Second, MaxOutputTokens: 128,
			},
		}},
		ChannelBindings: []ChannelBinding{{
			ID: "binding-a", Channel: "demo", ExternalAccountID: "binding-a",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
}
