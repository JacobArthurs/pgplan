package plan

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectType_JSONExtension(t *testing.T) {

	if result := detectType([]byte("anything"), "plan.json"); result != "json" {
		t.Errorf("got %q, want json", result)
	}
}

func TestDetectType_SQLExtension(t *testing.T) {

	if result := detectType([]byte("anything"), "query.sql"); result != "sql" {
		t.Errorf("got %q, want sql", result)
	}
}

func TestDetectType_TxtExtension(t *testing.T) {

	if result := detectType([]byte("anything"), "explain.txt"); result != "text" {
		t.Errorf("got %q, want text", result)
	}
}

func TestDetectType_JSONContent(t *testing.T) {
	data := []byte(`[{"Plan": {"Node Type": "Seq Scan"}}]`)

	if result := detectType(data, ""); result != "json" {
		t.Errorf("got %q, want json", result)
	}
}

func TestDetectType_JSONContentWithWhitespace(t *testing.T) {
	data := []byte(`  [{"Plan": {"Node Type": "Seq Scan"}}]`)

	if result := detectType(data, ""); result != "json" {
		t.Errorf("got %q, want json", result)
	}
}

func TestDetectType_SQLContent(t *testing.T) {
	data := []byte("SELECT * FROM users WHERE id = 1")

	if result := detectType(data, ""); result != "sql" {
		t.Errorf("got %q, want sql (default fallback)", result)
	}
}

func TestDetectType_ExtensionOverridesContent(t *testing.T) {
	data := []byte(`[{"Plan": {}}]`)

	if result := detectType(data, "queries.sql"); result != "sql" {
		t.Errorf("got %q, want sql (extension takes priority)", result)
	}
}

func TestDetectType_StdinWithJSON(t *testing.T) {
	data := []byte(`[{"Plan": {"Node Type": "Seq Scan"}}]`)

	if result := detectType(data, "-"); result != "json" {
		t.Errorf("got %q, want json", result)
	}
}

func TestReadInput_File(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.json")
	content := []byte(`[{"Plan": {}}]`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	data, err := readInput(path, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content mismatch")
	}
}

func TestReadInput_MissingFile(t *testing.T) {

	if _, err := readInput("/nonexistent/file.json", ""); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestResolve_JSONFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "plan.json")
	content := []byte(`[{
		"Plan": {
			"Node Type": "Seq Scan",
			"Relation Name": "users",
			"Startup Cost": 0.0,
			"Total Cost": 20.0,
			"Plan Rows": 100,
			"Plan Width": 8,
			"Actual Startup Time": 0.01,
			"Actual Total Time": 0.1,
			"Actual Rows": 100,
			"Actual Loops": 1
		},
		"Planning Time": 0.1,
		"Execution Time": 0.2
	}]`)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	plan, err := Resolve(path, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Plan.NodeType != "Seq Scan" {
		t.Errorf("NodeType = %q, want Seq Scan", plan.Plan.NodeType)
	}
}

func TestResolve_SQLFileWithoutDB(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "query.sql")
	if err := os.WriteFile(path, []byte("SELECT 1"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if _, err := Resolve(path, "", ""); err == nil {
		t.Fatal("expected error for SQL input without DB connection")
	}
}

func TestResolve_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.json")
	if err := os.WriteFile(path, []byte("not json at all"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if _, err := Resolve(path, "", ""); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestResolve_EmptyJSONArray(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(path, []byte("[]"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if _, err := Resolve(path, "", ""); err == nil {
		t.Fatal("expected error for empty JSON array")
	}
}

func TestResolve_TruncatedJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "truncated.json")
	if err := os.WriteFile(path, []byte(`[{"Plan": {"Node Type": "Seq Sc`), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if _, err := Resolve(path, "", ""); err == nil {
		t.Fatal("expected error for truncated JSON")
	}
}
