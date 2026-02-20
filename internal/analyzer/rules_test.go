package analyzer

import (
	"strings"
	"testing"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

// --- Helpers ---

func emptyCtx() *PlanContext {
	return &PlanContext{CTEs: make(map[string]*CTEInfo)}
}

func findBySeverity(findings []Finding, sev Severity) []Finding {
	var result []Finding
	for _, f := range findings {
		if f.Severity == sev {
			result = append(result, f)
		}
	}
	return result
}

func requireFindings(t *testing.T, findings []Finding, minCount int) {
	t.Helper()
	if len(findings) < minCount {
		t.Fatalf("expected at least %d findings, got %d", minCount, len(findings))
	}
}

func requireNoFindings(t *testing.T, findings []Finding) {
	t.Helper()
	if len(findings) > 0 {
		t.Fatalf("expected no findings, got %d: %v", len(findings), findings)
	}
}

func TestIndexScanFilterInefficiency_HighRemoval(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Index Scan",
		RelationName:        "scores",
		IndexName:           "idx_scores_date",
		IndexCond:           "(s.updated_at > '2023-01-01'::date)",
		Filter:              "(s.type = '4')",
		ActualRows:          2,
		RowsRemovedByFilter: 41555,
		ActualLoops:         1,
	}

	findings := checkIndexScanFilterInefficiency(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)

	f := findings[0]
	if f.Severity != Critical {
		t.Errorf("severity = %v, want Critical", f.Severity)
	}
	if !strings.Contains(f.Description, "99.99%") {
		t.Errorf("expected capped percentage, got: %s", f.Description)
	}
	if !strings.Contains(f.Suggestion, "type") {
		t.Errorf("expected type in suggestion, got: %s", f.Suggestion)
	}
	if !strings.Contains(f.Suggestion, "updated_at, type") {
		t.Errorf("expected composite index suggestion, got: %s", f.Suggestion)
	}
	if !strings.Contains(f.Suggestion, "partial index") {
		t.Errorf("expected partial index suggestion, got: %s", f.Suggestion)
	}
}

func TestIndexScanFilterInefficiency_LowRemoval(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Index Scan",
		RelationName:        "users",
		IndexName:           "idx_users_email",
		Filter:              "(active = true)",
		ActualRows:          900,
		RowsRemovedByFilter: 100,
		ActualLoops:         1,
	}

	findings := checkIndexScanFilterInefficiency(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestIndexScanFilterInefficiency_NoFilter(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:     "Index Scan",
		RelationName: "users",
		IndexName:    "idx_users_email",
		ActualRows:   100,
		ActualLoops:  1,
	}

	findings := checkIndexScanFilterInefficiency(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestIndexScanFilterInefficiency_WarningSeverity(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Index Scan",
		RelationName:        "orders",
		IndexName:           "idx_orders_date",
		IndexCond:           "(created_at > '2023-01-01')",
		Filter:              "(status = 'pending')",
		ActualRows:          400,
		RowsRemovedByFilter: 600,
		ActualLoops:         1,
	}

	findings := checkIndexScanFilterInefficiency(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Warning {
		t.Errorf("severity = %v, want Warning", findings[0].Severity)
	}
}

func TestSeqScanInJoin_LargeOuter(t *testing.T) {
	seqScan := plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "student_testing_service",
		Alias:        "sts",
		ActualRows:   269578,
		ActualLoops:  1,
	}
	cteScan := plan.PlanNode{
		NodeType:    "Hash",
		ActualRows:  37,
		ActualLoops: 1,
		Plans: []plan.PlanNode{{
			NodeType:    "CTE Scan",
			CTEName:     "test_updates",
			ActualRows:  37,
			ActualLoops: 1,
		}},
	}
	parent := &plan.PlanNode{
		NodeType: "Hash Join",
		HashCond: "(lower((sts.testing_service_candidate_id)::text) = (tu.testing_service_candidate_id)::text)",
		Plans:    []plan.PlanNode{seqScan, cteScan},
	}

	findings := checkSeqScanInJoin(&parent.Plans[0], parent, 0, emptyCtx())
	requireFindings(t, findings, 1)

	f := findings[0]
	if !strings.Contains(f.Description, "269578") {
		t.Errorf("expected row count in description, got: %s", f.Description)
	}
	if !strings.Contains(f.Description, "37") {
		t.Errorf("expected sibling row count, got: %s", f.Description)
	}
	if !strings.Contains(f.Description, "CTE test_updates") {
		t.Errorf("expected CTE name, got: %s", f.Description)
	}
	if !strings.Contains(f.Suggestion, "lower(") {
		t.Errorf("expected lower() in suggestion, got: %s", f.Suggestion)
	}
}

func TestSeqScanInJoin_SmallTable(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "lookup",
		ActualRows:   50,
		ActualLoops:  1,
	}
	parent := &plan.PlanNode{
		NodeType: "Hash Join",
		Plans:    []plan.PlanNode{*node, {ActualRows: 10}},
	}

	findings := checkSeqScanInJoin(&parent.Plans[0], parent, 0, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSeqScanInJoin_NotInJoin(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "users",
		ActualRows:   100000,
		ActualLoops:  1,
	}
	parent := &plan.PlanNode{
		NodeType: "Sort",
	}

	findings := checkSeqScanInJoin(node, parent, 0, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSeqScanStandalone_LargeWithFilter(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Seq Scan",
		RelationName:        "events",
		Filter:              "(status = 'active')",
		ActualRows:          50000,
		RowsRemovedByFilter: 200000,
		ActualLoops:         1,
	}

	findings := checkSeqScanStandalone(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical (>100k total rows)", findings[0].Severity)
	}
}

func TestSeqScanStandalone_SmallTable(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Seq Scan",
		RelationName:        "config",
		Filter:              "(key = 'setting')",
		ActualRows:          1,
		RowsRemovedByFilter: 50,
		ActualLoops:         1,
	}

	findings := checkSeqScanStandalone(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSeqScanStandalone_SkipsJoinParent(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Seq Scan",
		RelationName:        "big_table",
		Filter:              "(active = true)",
		ActualRows:          50000,
		RowsRemovedByFilter: 200000,
		ActualLoops:         1,
	}
	parent := &plan.PlanNode{NodeType: "Hash Join"}

	findings := checkSeqScanStandalone(node, parent, 0, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSeqScanStandalone_NoFilter(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "users",
		ActualRows:   100000,
		ActualLoops:  1,
	}

	findings := checkSeqScanStandalone(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestBitmapHeapRecheck_HighLossy(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Bitmap Heap Scan",
		RelationName:    "orders",
		ActualRows:      1000,
		ExactHeapBlocks: 9,
		LossyHeapBlocks: 91,
	}

	findings := checkBitmapHeapRecheck(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical (91%% lossy)", findings[0].Severity)
	}
}

func TestBitmapHeapRecheck_NoLossy(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Bitmap Heap Scan",
		RelationName:    "orders",
		ActualRows:      1000,
		ExactHeapBlocks: 100,
		LossyHeapBlocks: 0,
	}

	findings := checkBitmapHeapRecheck(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestBitmapHeapRecheck_WrongNodeType(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Seq Scan",
		LossyHeapBlocks: 100,
	}

	findings := checkBitmapHeapRecheck(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestNestedLoopHighLoops_ManyIterations(t *testing.T) {
	node := &plan.PlanNode{
		NodeType: "Nested Loop",
		Plans: []plan.PlanNode{
			{NodeType: "Seq Scan", ActualRows: 50000, ActualLoops: 1},
			{NodeType: "Index Scan", RelationName: "details", ActualLoops: 50000, ActualTotalTime: 0.2},
		},
	}

	findings := checkNestedLoopHighLoops(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical (50k loops * 0.2ms = 10000ms total)", findings[0].Severity)
	}
}

func TestNestedLoopHighLoops_FewIterations(t *testing.T) {
	node := &plan.PlanNode{
		NodeType: "Nested Loop",
		Plans: []plan.PlanNode{
			{NodeType: "Seq Scan", ActualRows: 10, ActualLoops: 1},
			{NodeType: "Index Scan", ActualLoops: 10, ActualTotalTime: 0.01},
		},
	}

	findings := checkNestedLoopHighLoops(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSubPlanHighLoops_CorrelatedSubquery(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:           "Index Scan",
		ParentRelationship: "SubPlan",
		RelationName:       "orders",
		ActualLoops:        5000,
		ActualTotalTime:    0.05,
	}

	findings := checkSubPlanHighLoops(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if !strings.Contains(findings[0].Suggestion, "JOIN") {
		t.Errorf("expected JOIN suggestion, got: %s", findings[0].Suggestion)
	}
}

func TestSubPlanHighLoops_LowLoops(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:           "Index Scan",
		ParentRelationship: "SubPlan",
		ActualLoops:        5,
	}

	findings := checkSubPlanHighLoops(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSubPlanHighLoops_NotSubPlan(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:           "Index Scan",
		ParentRelationship: "Outer",
		ActualLoops:        50000,
	}

	findings := checkSubPlanHighLoops(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestSortSpill_DiskSpill(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:      "Sort",
		SortSpaceType: "Disk",
		SortSpaceUsed: 51200,
	}

	findings := checkSortSpill(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical", findings[0].Severity)
	}
}

func TestSortSpill_MemorySort(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:      "Sort",
		SortSpaceType: "Memory",
		SortSpaceUsed: 71,
	}

	findings := checkSortSpill(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestHashSpill_MultipleBatches(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Hash",
		HashBatches:     16,
		PeakMemoryUsage: 256,
	}

	findings := checkHashSpill(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical (16 batches > 8)", findings[0].Severity)
	}
}

func TestHashSpill_SingleBatch(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:    "Hash",
		HashBatches: 1,
	}

	findings := checkHashSpill(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestTempBlocks_HasTempIO(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:          "Sort",
		TempReadBlocks:    100,
		TempWrittenBlocks: 100,
	}

	findings := checkTempBlocks(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Warning {
		t.Errorf("severity = %v, want Warning", findings[0].Severity)
	}
}

func TestTempBlocks_NoTempIO(t *testing.T) {
	node := &plan.PlanNode{
		NodeType: "Sort",
	}

	findings := checkTempBlocks(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestWorkerMismatch_FewerLaunched(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Gather",
		WorkersPlanned:  4,
		WorkersLaunched: 2,
	}

	findings := checkWorkerMismatch(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
}

func TestWorkerMismatch_AllLaunched(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Gather",
		WorkersPlanned:  4,
		WorkersLaunched: 4,
	}

	findings := checkWorkerMismatch(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestParallelOverhead_GatherSlower(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Gather",
		ActualTotalTime: 100.0,
		Plans: []plan.PlanNode{{
			NodeType:        "Parallel Seq Scan",
			ActualTotalTime: 20.0,
			ActualLoops:     3,
		}},
	}

	findings := checkParallelOverhead(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if !strings.Contains(findings[0].Suggestion, "max_parallel_workers_per_gather") {
		t.Errorf("expected parallel worker suggestion, got: %s", findings[0].Suggestion)
	}
}

func TestParallelOverhead_GatherFaster(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Gather",
		ActualTotalTime: 50.0,
		Plans: []plan.PlanNode{{
			NodeType:        "Parallel Seq Scan",
			ActualTotalTime: 40.0,
			ActualLoops:     3,
		}},
	}

	findings := checkParallelOverhead(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestLargeJoinFilterRemoval_ManyRemoved(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:                "Nested Loop",
		RowsRemovedByJoinFilter: 2000000,
	}

	findings := checkLargeJoinFilterRemoval(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical", findings[0].Severity)
	}
}

func TestLargeJoinFilterRemoval_FewRemoved(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:                "Nested Loop",
		RowsRemovedByJoinFilter: 100,
	}

	findings := checkLargeJoinFilterRemoval(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestMaterializeHighLoops_ManyLoops(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:        "Materialize",
		ActualLoops:     50000,
		ActualTotalTime: 0.01,
		ActualRows:      100,
	}

	findings := checkMaterializeHighLoops(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Critical {
		t.Errorf("severity = %v, want Critical (50k loops)", findings[0].Severity)
	}
}

func TestMaterializeHighLoops_FewLoops(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:    "Materialize",
		ActualLoops: 5,
	}

	findings := checkMaterializeHighLoops(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestIndexScanLowSelectivity_HighReads(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:         "Index Scan",
		RelationName:     "big_table",
		IndexName:        "idx_big_table_status",
		ActualRows:       50000,
		SharedHitBlocks:  100,
		SharedReadBlocks: 5000,
	}

	findings := checkIndexScanLowSelectivity(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if findings[0].Severity != Info {
		t.Errorf("severity = %v, want Info", findings[0].Severity)
	}
}

func TestIndexScanLowSelectivity_SkipsWithFilter(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:            "Index Scan",
		RelationName:        "big_table",
		IndexName:           "idx_big_table_status",
		ActualRows:          50000,
		SharedHitBlocks:     100,
		SharedReadBlocks:    5000,
		Filter:              "(active = true)",
		RowsRemovedByFilter: 1000,
	}

	findings := checkIndexScanLowSelectivity(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestIndexScanLowSelectivity_FewRows(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:         "Index Scan",
		ActualRows:       100,
		SharedReadBlocks: 5000,
	}

	findings := checkIndexScanLowSelectivity(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestWideRows_WideAndMany(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "documents",
		PlanWidth:    3000,
		ActualRows:   50000,
	}

	findings := checkWideRows(node, nil, -1, emptyCtx())
	requireFindings(t, findings, 1)
	if !strings.Contains(findings[0].Description, "3000 bytes") {
		t.Errorf("expected width in description, got: %s", findings[0].Description)
	}
}

func TestWideRows_WideButFew(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:   "Seq Scan",
		PlanWidth:  3000,
		ActualRows: 50,
	}

	findings := checkWideRows(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestWideRows_NarrowAndMany(t *testing.T) {
	node := &plan.PlanNode{
		NodeType:   "Seq Scan",
		PlanWidth:  100,
		ActualRows: 500000,
	}

	findings := checkWideRows(node, nil, -1, emptyCtx())
	requireNoFindings(t, findings)
}

func TestConsolidateEstimateMismatches_InflatedCTE(t *testing.T) {
	cteScan := plan.PlanNode{
		NodeType:    "CTE Scan",
		CTEName:     "test_updates",
		PlanRows:    2500,
		ActualRows:  370,
		ActualLoops: 1,
	}
	hashJoin := plan.PlanNode{
		NodeType:    "Hash Join",
		PlanRows:    111871,
		ActualRows:  370,
		ActualLoops: 1,
		Plans:       []plan.PlanNode{cteScan},
	}
	sort := plan.PlanNode{
		NodeType:    "Sort",
		PlanRows:    111871,
		ActualRows:  100,
		ActualLoops: 1,
		Plans:       []plan.PlanNode{hashJoin},
	}
	root := plan.PlanNode{
		NodeType:    "Limit",
		PlanRows:    10,
		ActualRows:  10,
		ActualLoops: 1,
		Plans: []plan.PlanNode{
			{
				NodeType:    "Append",
				SubplanName: "CTE test_updates",
				PlanRows:    2500,
				ActualRows:  370,
				ActualLoops: 1,
				Plans:       []plan.PlanNode{},
			},
			sort,
		},
	}

	ctx := BuildContext(&root)
	findings := ConsolidateEstimateMismatches(&root, &ctx)

	requireFindings(t, findings, 1)
	f := findings[0]
	if f.Severity != Info {
		t.Errorf("severity = %v, want Info", f.Severity)
	}
	if !strings.Contains(f.Description, "inflated") {
		t.Errorf("expected 'inflated' in description, got: %s", f.Description)
	}
	if !strings.Contains(f.Description, "test_updates") {
		t.Errorf("expected CTE name in description, got: %s", f.Description)
	}
}

func TestConsolidateEstimateMismatches_SmallCTEIgnored(t *testing.T) {
	root := plan.PlanNode{
		NodeType:    "Limit",
		PlanRows:    10,
		ActualRows:  10,
		ActualLoops: 1,
		Plans: []plan.PlanNode{{
			NodeType:    "Append",
			SubplanName: "CTE small_cte",
			PlanRows:    30,
			ActualRows:  10,
			ActualLoops: 1,
		}},
	}

	ctx := BuildContext(&root)
	findings := ConsolidateEstimateMismatches(&root, &ctx)
	requireNoFindings(t, findings)
}

func TestAnalyze_FullPlan(t *testing.T) {
	output := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:      "Sort",
			TotalCost:     100.0,
			PlanRows:      1000,
			ActualRows:    1000,
			ActualLoops:   1,
			SortSpaceType: "Disk",
			SortSpaceUsed: 5000,
			Plans: []plan.PlanNode{{
				NodeType:            "Seq Scan",
				RelationName:        "events",
				Filter:              "(status = 'active')",
				ActualRows:          500,
				PlanRows:            500,
				RowsRemovedByFilter: 200000,
				ActualLoops:         1,
			}},
		},
		PlanningTime:  1.0,
		ExecutionTime: 50.0,
	}

	result := Analyze(output)

	if result.TotalCost != 100.0 {
		t.Errorf("TotalCost = %f, want 100.0", result.TotalCost)
	}
	if result.ExecutionTime != 50.0 {
		t.Errorf("ExecutionTime = %f, want 50.0", result.ExecutionTime)
	}
	if len(result.Findings) == 0 {
		t.Fatal("expected findings for disk sort + seq scan with filter")
	}

	criticals := findBySeverity(result.Findings, Critical)
	if len(criticals) == 0 {
		t.Error("expected at least one critical finding (disk sort)")
	}

	for i := 1; i < len(result.Findings); i++ {
		if result.Findings[i].Severity > result.Findings[i-1].Severity {
			t.Error("findings not sorted by severity descending")
			break
		}
	}
}
