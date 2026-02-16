package analyzer

import (
	"testing"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

func TestBuildContext_DetectsCTEs(t *testing.T) {
	root := plan.PlanNode{
		NodeType: "Limit",
		Plans: []plan.PlanNode{
			{
				NodeType:    "Append",
				SubplanName: "CTE test_updates",
				PlanRows:    250,
				ActualRows:  37,
				Plans: []plan.PlanNode{
					{NodeType: "Index Scan", RelationName: "scores"},
					{NodeType: "Index Scan", RelationName: "events"},
				},
			},
			{NodeType: "Sort"},
		},
	}

	ctx := BuildContext(&root)

	cte, ok := ctx.CTEs["test_updates"]
	if !ok {
		t.Fatal("CTE test_updates not found")
	}
	if cte.EstimatedRows != 250 {
		t.Errorf("EstimatedRows = %d, want 250", cte.EstimatedRows)
	}
	if cte.ActualRows != 37 {
		t.Errorf("ActualRows = %d, want 37", cte.ActualRows)
	}
}

func TestBuildContext_MultipleCTEs(t *testing.T) {
	root := plan.PlanNode{
		NodeType: "Limit",
		Plans: []plan.PlanNode{
			{NodeType: "Append", SubplanName: "CTE cte_a", PlanRows: 100, ActualRows: 50},
			{NodeType: "Append", SubplanName: "CTE cte_b", PlanRows: 200, ActualRows: 80},
			{NodeType: "Sort"},
		},
	}

	ctx := BuildContext(&root)

	if len(ctx.CTEs) != 2 {
		t.Fatalf("expected 2 CTEs, got %d", len(ctx.CTEs))
	}
	if _, ok := ctx.CTEs["cte_a"]; !ok {
		t.Error("cte_a not found")
	}
	if _, ok := ctx.CTEs["cte_b"]; !ok {
		t.Error("cte_b not found")
	}
}

func TestBuildContext_NoCTEs(t *testing.T) {
	root := plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "users",
	}

	ctx := BuildContext(&root)

	if len(ctx.CTEs) != 0 {
		t.Errorf("expected 0 CTEs, got %d", len(ctx.CTEs))
	}
}

func TestBuildContext_AllNodesFlattened(t *testing.T) {
	root := plan.PlanNode{
		NodeType: "Sort",
		Plans: []plan.PlanNode{
			{
				NodeType: "Hash Join",
				Plans: []plan.PlanNode{
					{NodeType: "Seq Scan"},
					{NodeType: "Hash", Plans: []plan.PlanNode{
						{NodeType: "Seq Scan"},
					}},
				},
			},
		},
	}

	ctx := BuildContext(&root)

	if len(ctx.AllNodes) != 5 {
		t.Errorf("expected 5 nodes, got %d", len(ctx.AllNodes))
	}
}

func TestBuildContext_ParentReferences(t *testing.T) {
	root := plan.PlanNode{
		NodeType: "Sort",
		Plans: []plan.PlanNode{
			{NodeType: "Seq Scan", RelationName: "users"},
		},
	}

	ctx := BuildContext(&root)

	if ctx.AllNodes[0].Parent != nil {
		t.Error("root should have nil parent")
	}
	if ctx.AllNodes[1].Parent == nil {
		t.Fatal("child parent is nil")
	}
	if ctx.AllNodes[1].Parent.NodeType != "Sort" {
		t.Errorf("child parent = %q, want Sort", ctx.AllNodes[1].Parent.NodeType)
	}
}

func TestBuildContext_DepthTracking(t *testing.T) {
	root := plan.PlanNode{
		NodeType: "Limit",
		Plans: []plan.PlanNode{
			{NodeType: "Sort", Plans: []plan.PlanNode{
				{NodeType: "Seq Scan"},
			}},
		},
	}

	ctx := BuildContext(&root)

	depths := map[string]int{}
	for _, ref := range ctx.AllNodes {
		depths[ref.Node.NodeType] = ref.Depth
	}

	if depths["Limit"] != 0 {
		t.Errorf("Limit depth = %d, want 0", depths["Limit"])
	}
	if depths["Sort"] != 1 {
		t.Errorf("Sort depth = %d, want 1", depths["Sort"])
	}
	if depths["Seq Scan"] != 2 {
		t.Errorf("Seq Scan depth = %d, want 2", depths["Seq Scan"])
	}
}

func TestExtractConditionColumns_Simple(t *testing.T) {
	cols := ExtractConditionColumns("(users.email = 'test@test.com')")
	if len(cols) != 1 || cols[0] != "email" {
		t.Errorf("got %v, want [email]", cols)
	}
}

func TestExtractConditionColumns_MultipleColumns(t *testing.T) {
	cols := ExtractConditionColumns("(a.id = b.a_id)")
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(cols), cols)
	}
	expected := map[string]bool{"id": true, "a_id": true}
	for _, c := range cols {
		if !expected[c] {
			t.Errorf("unexpected column: %s", c)
		}
	}
}

func TestExtractConditionColumns_WithCast(t *testing.T) {
	cols := ExtractConditionColumns("((action_code)::text = '4'::text)")
	if len(cols) != 1 || cols[0] != "action_code" {
		t.Errorf("got %v, want [action_code]", cols)
	}
}

func TestExtractConditionColumns_LowerFunction(t *testing.T) {
	cols := ExtractConditionColumns("(lower((sts.testing_service_candidate_id)::text) = (tu.testing_service_candidate_id)::text)")
	expected := map[string]bool{"testing_service_candidate_id": true}
	for _, c := range cols {
		if !expected[c] {
			t.Errorf("unexpected column: %s", c)
		}
	}
}

func TestExtractConditionColumns_Empty(t *testing.T) {
	cols := ExtractConditionColumns("")
	if cols != nil {
		t.Errorf("expected nil, got %v", cols)
	}
}

func TestExtractConditionColumns_DateComparison(t *testing.T) {
	cols := ExtractConditionColumns("(clep_ibt_score.import_date > '2023-01-27'::date)")
	if len(cols) != 1 || cols[0] != "import_date" {
		t.Errorf("got %v, want [import_date]", cols)
	}
}

func TestConditionColumnsNotIn_MissingColumn(t *testing.T) {
	filter := "((action_code)::text = '4'::text)"
	indexCond := "(import_date > '2023-01-27'::date)"

	missing := ConditionColumnsNotIn(filter, indexCond)
	if len(missing) != 1 || missing[0] != "action_code" {
		t.Errorf("got %v, want [action_code]", missing)
	}
}

func TestConditionColumnsNotIn_AllCovered(t *testing.T) {
	filter := "(import_date > '2023-01-01')"
	indexCond := "(import_date > '2023-01-01')"

	missing := ConditionColumnsNotIn(filter, indexCond)
	if len(missing) != 0 {
		t.Errorf("expected no missing columns, got %v", missing)
	}
}

func TestConditionColumnsNotIn_EmptyFilter(t *testing.T) {
	missing := ConditionColumnsNotIn("", "(id > 0)")
	if missing != nil {
		t.Errorf("expected nil, got %v", missing)
	}
}

func TestExtractLiteralValue_SimpleEquality(t *testing.T) {
	val := ExtractLiteralValue("((action_code)::text = '4'::text)")
	if val != "4" {
		t.Errorf("got %q, want %q", val, "4")
	}
}

func TestExtractLiteralValue_NoEquality(t *testing.T) {
	val := ExtractLiteralValue("(import_date > '2023-01-01'::date)")
	if val != "" {
		t.Errorf("expected empty for >, got %q", val)
	}
}

func TestExtractLiteralValue_NotEqual(t *testing.T) {
	val := ExtractLiteralValue("(status <> 'inactive'::text)")
	if val != "" {
		t.Errorf("expected empty for <>, got %q", val)
	}
}

func TestExtractLiteralValue_GreaterEqual(t *testing.T) {
	val := ExtractLiteralValue("(import_date >= '2023-01-01'::date)")
	if val != "" {
		t.Errorf("expected empty for >=, got %q", val)
	}
}

func TestExtractLiteralValue_EscapedQuote(t *testing.T) {
	val := ExtractLiteralValue("(name = 'O''Brien'::text)")
	if val != "O'Brien" {
		t.Errorf("got %q, want %q", val, "O'Brien")
	}
}

func TestExtractLiteralValue_Empty(t *testing.T) {
	val := ExtractLiteralValue("")
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}
}
