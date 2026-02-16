package profile

import (
	"testing"
)

func setupTestConfig(t *testing.T) func() {
	t.Helper()
	tmpDir := t.TempDir()
	origFunc := configDirFunc
	configDirFunc = func() (string, error) {
		return tmpDir, nil
	}
	return func() {
		configDirFunc = origFunc
	}
}

func TestAdd_NewProfile(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	err := Add("prod", "postgres://localhost/prod")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	profiles, err := List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(profiles) != 1 {
		t.Fatalf("expected 1 profile, got %d", len(profiles))
	}
	if profiles[0].Name != "prod" {
		t.Errorf("Name = %q, want prod", profiles[0].Name)
	}
	if profiles[0].ConnStr != "postgres://localhost/prod" {
		t.Errorf("ConnStr = %q", profiles[0].ConnStr)
	}
}

func TestAdd_UpdateExisting(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://localhost/prod_v1"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := Add("prod", "postgres://localhost/prod_v2"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	profiles, err := List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(profiles) != 1 {
		t.Fatalf("expected 1 profile after update, got %d", len(profiles))
	}
	if profiles[0].ConnStr != "postgres://localhost/prod_v2" {
		t.Errorf("ConnStr not updated: %q", profiles[0].ConnStr)
	}
}

func TestAdd_MultipleProfiles(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://prod-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := Add("dev", "postgres://localhost/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := Add("staging", "postgres://staging-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	profiles, err := List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(profiles) != 3 {
		t.Errorf("expected 3 profiles, got %d", len(profiles))
	}
}

func TestRemove_Existing(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://localhost/prod"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := Add("dev", "postgres://localhost/dev"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	err := Remove("prod")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	profiles, err := List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(profiles) != 1 {
		t.Fatalf("expected 1 profile after remove, got %d", len(profiles))
	}
	if profiles[0].Name != "dev" {
		t.Errorf("remaining profile = %q, want dev", profiles[0].Name)
	}
}

func TestRemove_NonExistent(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://localhost/prod"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	err := Remove("staging")
	if err == nil {
		t.Fatal("expected error when removing non-existent profile")
	}
}

func TestResolve_ExistingProfile(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://prod-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	connStr, err := Resolve("prod")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if connStr != "postgres://prod-host/db" {
		t.Errorf("ConnStr = %q", connStr)
	}
}

func TestResolve_NonExistent(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	_, err := Resolve("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent profile")
	}
}

func TestResolve_NoConfigFile(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	_, err := Resolve("anything")
	if err == nil {
		t.Fatal("expected error when no config file exists")
	}
}

func TestSetDefault(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://prod-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := Add("dev", "postgres://localhost/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	err := SetDefault("prod")
	if err != nil {
		t.Fatalf("SetDefault failed: %v", err)
	}

	defaultName, err := GetDefault()
	if err != nil {
		t.Fatalf("GetDefault failed: %v", err)
	}
	if defaultName != "prod" {
		t.Errorf("default = %q, want prod", defaultName)
	}
}

func TestSetDefault_NonExistent(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	err := SetDefault("nonexistent")
	if err == nil {
		t.Fatal("expected error when setting non-existent profile as default")
	}
}

func TestClearDefault(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://prod-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := SetDefault("prod"); err != nil {
		t.Fatalf("SetDefault failed: %v", err)
	}

	err := ClearDefault()
	if err != nil {
		t.Fatalf("ClearDefault failed: %v", err)
	}

	defaultName, err := GetDefault()
	if err != nil {
		t.Fatalf("GetDefault failed: %v", err)
	}
	if defaultName != "" {
		t.Errorf("default = %q, want empty", defaultName)
	}
}

func TestResolveConnStr_DbFlag(t *testing.T) {
	connStr, err := ResolveConnStr("postgres://direct/db", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if connStr != "postgres://direct/db" {
		t.Errorf("ConnStr = %q", connStr)
	}
}

func TestResolveConnStr_ProfileFlag(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://prod-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	connStr, err := ResolveConnStr("", "prod")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if connStr != "postgres://prod-host/db" {
		t.Errorf("ConnStr = %q", connStr)
	}
}

func TestResolveConnStr_DefaultFallback(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	if err := Add("prod", "postgres://prod-host/db"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := SetDefault("prod"); err != nil {
		t.Fatalf("SetDefault failed: %v", err)
	}

	connStr, err := ResolveConnStr("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if connStr != "postgres://prod-host/db" {
		t.Errorf("ConnStr = %q, want prod connection", connStr)
	}
}

func TestResolveConnStr_NoFlags_NoDefault(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	connStr, err := ResolveConnStr("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if connStr != "" {
		t.Errorf("ConnStr = %q, want empty", connStr)
	}
}

func TestList_EmptyConfig(t *testing.T) {
	cleanup := setupTestConfig(t)
	defer cleanup()

	profiles, err := List()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if profiles != nil {
		t.Errorf("expected nil profiles, got %v", profiles)
	}
}
