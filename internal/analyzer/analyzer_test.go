package analyzer

import (
	"strings"
	"testing"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

// Buffers must come from the root node's own (cumulative, inclusive-of-
// children) counters, not a sum across the tree - see AggregateBuffers.
// SortSpaceUsed is the opposite: it's per-operation memory, not cumulative,
// so it must sum across every sort/hash node in the tree.
func TestAnalyze_AggregatesBuffersAndSortVolume(t *testing.T) {
	output := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:          "Sort",
			TotalCost:         50.0,
			SortSpaceUsed:     128,
			SortSpaceType:     "Memory",
			SharedHitBlocks:   10,
			LocalReadBlocks:   2,
			TempWrittenBlocks: 4,
			ActualLoops:       1,
			Plans: []plan.PlanNode{
				{
					NodeType:         "Seq Scan",
					TotalCost:        20.0,
					SharedReadBlocks: 30,
					SortSpaceUsed:    64,
					ActualLoops:      1,
				},
			},
		},
		PlanningTime:  1.0,
		ExecutionTime: 5.0,
	}

	result := Analyze(output)

	if result.Buffers.Shared.Hit != 10 {
		t.Errorf("Buffers.Shared.Hit = %d, want 10", result.Buffers.Shared.Hit)
	}
	if result.Buffers.Shared.Read != 0 {
		t.Errorf("Buffers.Shared.Read = %d, want 0 (child's own Shared.Read must not be added)", result.Buffers.Shared.Read)
	}
	if result.Buffers.Local.Read != 2 {
		t.Errorf("Buffers.Local.Read = %d, want 2", result.Buffers.Local.Read)
	}
	if result.Buffers.Temp.Written != 4 {
		t.Errorf("Buffers.Temp.Written = %d, want 4", result.Buffers.Temp.Written)
	}
	if result.SortSpaceUsed != 192 {
		t.Errorf("SortSpaceUsed = %d, want 192 (128 root + 64 child, summed across the tree)", result.SortSpaceUsed)
	}
}

func TestAnalyze_BlockSizeAffectsFindingByteSizes(t *testing.T) {
	output := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:          "Sort",
			TotalCost:         50.0,
			TempReadBlocks:    100,
			TempWrittenBlocks: 100,
			ActualLoops:       1,
		},
	}

	default8k := Analyze(output)
	custom4k := Analyze(output, 4096)

	find := func(findings []Finding) Finding {
		for _, f := range findings {
			if strings.Contains(f.Description, "Temp I/O") {
				return f
			}
		}
		t.Fatal("expected a Temp I/O finding")
		return Finding{}
	}

	d8k := find(default8k.Findings)
	d4k := find(custom4k.Findings)

	if !strings.Contains(d8k.Description, "800.0 kB") {
		t.Errorf("default block size Description = %q, want it to contain 800.0 kB (100 blocks * 8192 bytes)", d8k.Description)
	}
	if !strings.Contains(d4k.Description, "400.0 kB") {
		t.Errorf("4096 block size Description = %q, want it to contain 400.0 kB (100 blocks * 4096 bytes)", d4k.Description)
	}
}

func TestAnalyze_ActualRows_PresentWhenAnalyzed(t *testing.T) {
	output := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:      "Sort",
			TotalCost:     100.0,
			PlanRows:      1000,
			ActualRows:    1234,
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

	if !result.HasActualRows {
		t.Fatal("HasActualRows = false, want true (ActualLoops > 0)")
	}
	if result.ActualRows != 1234 {
		t.Errorf("ActualRows = %v, want 1234 (root node's ActualRows)", result.ActualRows)
	}

	if len(result.Findings) == 0 {
		t.Fatal("expected at least one finding")
	}
	for _, f := range result.Findings {
		if !f.HasActualRows {
			t.Errorf("finding %q: HasActualRows = false, want true", f.Description)
		}
	}
}

func TestAnalyze_ActualRows_AbsentWithoutAnalyze(t *testing.T) {
	// A plain EXPLAIN (no ANALYZE) never populates Actual Loops/Rows.
	output := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:  "Seq Scan",
			TotalCost: 20.0,
			Filter:    "(active = true)",
		},
	}

	result := Analyze(output)

	if result.HasActualRows {
		t.Error("HasActualRows = true, want false (no ANALYZE data)")
	}
	if result.ActualRows != 0 {
		t.Errorf("ActualRows = %v, want 0", result.ActualRows)
	}
}
