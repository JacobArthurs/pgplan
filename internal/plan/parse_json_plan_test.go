package plan

import (
	"encoding/json"
	"testing"
)

func TestParseJSONPlan_ValidPlan(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "Seq Scan",
			"Relation Name": "users",
			"Schema": "public",
			"Alias": "u",
			"Startup Cost": 0.00,
			"Total Cost": 20.00,
			"Plan Rows": 1000,
			"Plan Width": 8,
			"Actual Startup Time": 0.013,
			"Actual Total Time": 0.108,
			"Actual Rows": 1000,
			"Actual Loops": 1,
			"Filter": "(active = true)",
			"Rows Removed by Filter": 500,
			"Shared Hit Blocks": 5,
			"Shared Read Blocks": 10
		},
		"Planning Time": 0.085,
		"Execution Time": 0.523
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}

	p := plans[0]
	if p.PlanningTime != 0.085 {
		t.Errorf("PlanningTime = %f, want 0.085", p.PlanningTime)
	}
	if p.ExecutionTime != 0.523 {
		t.Errorf("ExecutionTime = %f, want 0.523", p.ExecutionTime)
	}

	node := p.Plan
	if node.NodeType != "Seq Scan" {
		t.Errorf("NodeType = %q, want %q", node.NodeType, "Seq Scan")
	}
	if node.RelationName != "users" {
		t.Errorf("RelationName = %q, want %q", node.RelationName, "users")
	}
	if node.Schema != "public" {
		t.Errorf("Schema = %q, want %q", node.Schema, "public")
	}
	if node.Alias != "u" {
		t.Errorf("Alias = %q, want %q", node.Alias, "u")
	}
	if node.StartupCost != 0.00 {
		t.Errorf("StartupCost = %f, want 0.00", node.StartupCost)
	}
	if node.TotalCost != 20.00 {
		t.Errorf("TotalCost = %f, want 20.00", node.TotalCost)
	}
	if node.PlanRows != 1000 {
		t.Errorf("PlanRows = %d, want 1000", node.PlanRows)
	}
	if node.PlanWidth != 8 {
		t.Errorf("PlanWidth = %d, want 8", node.PlanWidth)
	}
	if node.ActualStartupTime != 0.013 {
		t.Errorf("ActualStartupTime = %f, want 0.013", node.ActualStartupTime)
	}
	if node.ActualTotalTime != 0.108 {
		t.Errorf("ActualTotalTime = %f, want 0.108", node.ActualTotalTime)
	}
	if node.ActualRows != 1000 {
		t.Errorf("ActualRows = %d, want 1000", node.ActualRows)
	}
	if node.ActualLoops != 1 {
		t.Errorf("ActualLoops = %d, want 1", node.ActualLoops)
	}
	if node.Filter != "(active = true)" {
		t.Errorf("Filter = %q, want %q", node.Filter, "(active = true)")
	}
	if node.RowsRemovedByFilter != 500 {
		t.Errorf("RowsRemovedByFilter = %d, want 500", node.RowsRemovedByFilter)
	}
	if node.SharedHitBlocks != 5 {
		t.Errorf("SharedHitBlocks = %d, want 5", node.SharedHitBlocks)
	}
	if node.SharedReadBlocks != 10 {
		t.Errorf("SharedReadBlocks = %d, want 10", node.SharedReadBlocks)
	}
}

func TestParseJSONPlan_NestedPlan(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "Sort",
			"Startup Cost": 69.83,
			"Total Cost": 72.33,
			"Plan Rows": 1000,
			"Plan Width": 8,
			"Actual Startup Time": 0.456,
			"Actual Total Time": 0.478,
			"Actual Rows": 1000,
			"Actual Loops": 1,
			"Sort Key": ["id"],
			"Sort Method": "quicksort",
			"Sort Space Used": 71,
			"Sort Space Type": "Memory",
			"Plans": [{
				"Node Type": "Seq Scan",
				"Parent Relationship": "Outer",
				"Relation Name": "users",
				"Startup Cost": 0.00,
				"Total Cost": 20.00,
				"Plan Rows": 1000,
				"Plan Width": 8,
				"Actual Startup Time": 0.013,
				"Actual Total Time": 0.108,
				"Actual Rows": 1000,
				"Actual Loops": 1
			}]
		},
		"Planning Time": 0.1,
		"Execution Time": 0.5
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := plans[0].Plan
	if node.NodeType != "Sort" {
		t.Errorf("root NodeType = %q, want %q", node.NodeType, "Sort")
	}
	if len(node.SortKey) != 1 || node.SortKey[0] != "id" {
		t.Errorf("SortKey = %v, want [id]", node.SortKey)
	}
	if node.SortMethod != "quicksort" {
		t.Errorf("SortMethod = %q, want %q", node.SortMethod, "quicksort")
	}
	if node.SortSpaceUsed != 71 {
		t.Errorf("SortSpaceUsed = %d, want 71", node.SortSpaceUsed)
	}
	if node.SortSpaceType != "Memory" {
		t.Errorf("SortSpaceType = %q, want %q", node.SortSpaceType, "Memory")
	}
	if len(node.Plans) != 1 {
		t.Fatalf("expected 1 child, got %d", len(node.Plans))
	}

	child := node.Plans[0]
	if child.NodeType != "Seq Scan" {
		t.Errorf("child NodeType = %q, want %q", child.NodeType, "Seq Scan")
	}
	if child.ParentRelationship != "Outer" {
		t.Errorf("child ParentRelationship = %q, want %q", child.ParentRelationship, "Outer")
	}
	if child.RelationName != "users" {
		t.Errorf("child RelationName = %q, want %q", child.RelationName, "users")
	}
}

func TestParseJSONPlan_HashJoinWithBuffers(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "Hash Join",
			"Join Type": "Inner",
			"Startup Cost": 10.0,
			"Total Cost": 100.0,
			"Plan Rows": 500,
			"Plan Width": 16,
			"Actual Startup Time": 1.0,
			"Actual Total Time": 5.0,
			"Actual Rows": 500,
			"Actual Loops": 1,
			"Inner Unique": true,
			"Hash Cond": "(a.id = b.a_id)",
			"Hash Buckets": 1024,
			"Hash Batches": 4,
			"Original Hash Batches": 1,
			"Peak Memory Usage": 256,
			"Shared Hit Blocks": 10,
			"Shared Read Blocks": 20,
			"Shared Dirtied Blocks": 1,
			"Shared Written Blocks": 0,
			"Temp Read Blocks": 5,
			"Temp Written Blocks": 5,
			"Plans": []
		},
		"Planning Time": 0.5,
		"Execution Time": 5.5
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := plans[0].Plan
	if node.JoinType != "Inner" {
		t.Errorf("JoinType = %q, want %q", node.JoinType, "Inner")
	}
	if !node.InnerUnique {
		t.Error("InnerUnique = false, want true")
	}
	if node.HashCond != "(a.id = b.a_id)" {
		t.Errorf("HashCond = %q, want %q", node.HashCond, "(a.id = b.a_id)")
	}
	if node.HashBuckets != 1024 {
		t.Errorf("HashBuckets = %d, want 1024", node.HashBuckets)
	}
	if node.HashBatches != 4 {
		t.Errorf("HashBatches = %d, want 4", node.HashBatches)
	}
	if node.OriginalHashBatches != 1 {
		t.Errorf("OriginalHashBatches = %d, want 1", node.OriginalHashBatches)
	}
	if node.PeakMemoryUsage != 256 {
		t.Errorf("PeakMemoryUsage = %d, want 256", node.PeakMemoryUsage)
	}
	if node.SharedDirtiedBlocks != 1 {
		t.Errorf("SharedDirtiedBlocks = %d, want 1", node.SharedDirtiedBlocks)
	}
	if node.TempReadBlocks != 5 {
		t.Errorf("TempReadBlocks = %d, want 5", node.TempReadBlocks)
	}
	if node.TempWrittenBlocks != 5 {
		t.Errorf("TempWrittenBlocks = %d, want 5", node.TempWrittenBlocks)
	}
}

func TestParseJSONPlan_IndexScanFields(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "Index Scan",
			"Scan Direction": "Forward",
			"Index Name": "idx_users_email",
			"Relation Name": "users",
			"Schema": "public",
			"Alias": "u",
			"Startup Cost": 0.42,
			"Total Cost": 8.44,
			"Plan Rows": 1,
			"Plan Width": 100,
			"Actual Startup Time": 0.01,
			"Actual Total Time": 0.02,
			"Actual Rows": 1,
			"Actual Loops": 1,
			"Index Cond": "(email = 'test@example.com')",
			"Rows Removed by Index Recheck": 0
		},
		"Planning Time": 0.1,
		"Execution Time": 0.05
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := plans[0].Plan
	if node.ScanDirection != "Forward" {
		t.Errorf("ScanDirection = %q, want %q", node.ScanDirection, "Forward")
	}
	if node.IndexName != "idx_users_email" {
		t.Errorf("IndexName = %q, want %q", node.IndexName, "idx_users_email")
	}
	if node.IndexCond != "(email = 'test@example.com')" {
		t.Errorf("IndexCond = %q", node.IndexCond)
	}
}

func TestParseJSONPlan_ParallelWorkers(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "Gather",
			"Startup Cost": 1000.0,
			"Total Cost": 5000.0,
			"Plan Rows": 10000,
			"Plan Width": 8,
			"Actual Startup Time": 10.0,
			"Actual Total Time": 50.0,
			"Actual Rows": 10000,
			"Actual Loops": 1,
			"Workers Planned": 4,
			"Workers Launched": 2,
			"Plans": []
		},
		"Planning Time": 1.0,
		"Execution Time": 51.0
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := plans[0].Plan
	if node.WorkersPlanned != 4 {
		t.Errorf("WorkersPlanned = %d, want 4", node.WorkersPlanned)
	}
	if node.WorkersLaunched != 2 {
		t.Errorf("WorkersLaunched = %d, want 2", node.WorkersLaunched)
	}
}

func TestParseJSONPlan_CTESubplan(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "CTE Scan",
			"CTE Name": "recent_orders",
			"Alias": "ro",
			"Startup Cost": 0.0,
			"Total Cost": 10.0,
			"Plan Rows": 50,
			"Plan Width": 40,
			"Actual Startup Time": 0.5,
			"Actual Total Time": 1.0,
			"Actual Rows": 50,
			"Actual Loops": 1,
			"Filter": "(amount > 100)"
		},
		"Planning Time": 0.2,
		"Execution Time": 1.5
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := plans[0].Plan
	if node.CTEName != "recent_orders" {
		t.Errorf("CTEName = %q, want %q", node.CTEName, "recent_orders")
	}
	if node.Filter != "(amount > 100)" {
		t.Errorf("Filter = %q", node.Filter)
	}
}

func TestParseJSONPlan_EmptyInput(t *testing.T) {
	_, err := ParseJSONPlan([]byte("[]"))
	if err == nil {
		t.Fatal("expected error for empty plan")
	}
}

func TestParseJSONPlan_InvalidJSON(t *testing.T) {
	_, err := ParseJSONPlan([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseJSONPlan_MissingPlanField(t *testing.T) {
	input := `[{"Planning Time": 1.0, "Execution Time": 2.0}]`
	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plans[0].Plan.NodeType != "" {
		t.Errorf("expected empty NodeType, got %q", plans[0].Plan.NodeType)
	}
}

func TestParseJSONPlan_SubplanName(t *testing.T) {
	input := `[{
		"Plan": {
			"Node Type": "Append",
			"Parent Relationship": "InitPlan",
			"Subplan Name": "CTE test_updates",
			"Startup Cost": 0.42,
			"Total Cost": 100.0,
			"Plan Rows": 250,
			"Plan Width": 120,
			"Actual Startup Time": 1.0,
			"Actual Total Time": 50.0,
			"Actual Rows": 37,
			"Actual Loops": 1,
			"Plans": []
		},
		"Planning Time": 1.0,
		"Execution Time": 55.0
	}]`

	plans, err := ParseJSONPlan([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := plans[0].Plan
	if node.SubplanName != "CTE test_updates" {
		t.Errorf("SubplanName = %q, want %q", node.SubplanName, "CTE test_updates")
	}
	if node.ParentRelationship != "InitPlan" {
		t.Errorf("ParentRelationship = %q, want %q", node.ParentRelationship, "InitPlan")
	}
}

func TestParseJSONPlan_RoundTrip(t *testing.T) {
	original := ExplainOutput{
		Plan: PlanNode{
			NodeType:    "Seq Scan",
			TotalCost:   100.0,
			PlanRows:    500,
			ActualRows:  480,
			ActualLoops: 1,
		},
		PlanningTime:  1.5,
		ExecutionTime: 10.0,
	}

	data, err := json.Marshal([]ExplainOutput{original})
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	plans, err := ParseJSONPlan(data)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	got := plans[0]
	if got.Plan.NodeType != original.Plan.NodeType {
		t.Errorf("NodeType = %q, want %q", got.Plan.NodeType, original.Plan.NodeType)
	}
	if got.Plan.TotalCost != original.Plan.TotalCost {
		t.Errorf("TotalCost = %f, want %f", got.Plan.TotalCost, original.Plan.TotalCost)
	}
	if got.ExecutionTime != original.ExecutionTime {
		t.Errorf("ExecutionTime = %f, want %f", got.ExecutionTime, original.ExecutionTime)
	}
}
