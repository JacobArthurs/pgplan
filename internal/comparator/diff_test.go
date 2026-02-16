package comparator

import (
	"testing"

	"pgplan/internal/plan"
)

func defaultComparator() *Comparator {
	return &Comparator{Threshold: 5.0}
}

func TestDiffNodes_SameNode(t *testing.T) {
	c := defaultComparator()
	node := plan.PlanNode{
		NodeType:        "Seq Scan",
		RelationName:    "users",
		TotalCost:       20.0,
		ActualTotalTime: 0.5,
		ActualRows:      100,
		ActualLoops:     1,
	}

	delta := c.diffNodes(&node, &node)

	if delta.ChangeType != NoChange {
		t.Errorf("ChangeType = %v, want NoChange", delta.ChangeType)
	}
	if delta.CostDelta != 0 {
		t.Errorf("CostDelta = %f, want 0", delta.CostDelta)
	}
}

func TestDiffNodes_CostIncrease(t *testing.T) {
	c := defaultComparator()
	old := plan.PlanNode{
		NodeType:        "Seq Scan",
		TotalCost:       20.0,
		ActualTotalTime: 0.5,
		ActualRows:      100,
		ActualLoops:     1,
	}
	new := plan.PlanNode{
		NodeType:        "Seq Scan",
		TotalCost:       40.0,
		ActualTotalTime: 1.0,
		ActualRows:      100,
		ActualLoops:     1,
	}

	delta := c.diffNodes(&old, &new)

	if delta.ChangeType != Modified {
		t.Errorf("ChangeType = %v, want Modified", delta.ChangeType)
	}
	if delta.CostDir != Regressed {
		t.Errorf("CostDir = %v, want Regressed", delta.CostDir)
	}
	if delta.CostDelta != 20.0 {
		t.Errorf("CostDelta = %f, want 20.0", delta.CostDelta)
	}
	if delta.CostPct != 100.0 {
		t.Errorf("CostPct = %f, want 100.0", delta.CostPct)
	}
}

func TestDiffNodes_TimeImproved(t *testing.T) {
	c := defaultComparator()
	old := plan.PlanNode{
		NodeType:        "Seq Scan",
		TotalCost:       20.0,
		ActualTotalTime: 10.0,
		ActualRows:      100,
		ActualLoops:     1,
	}
	new := plan.PlanNode{
		NodeType:        "Seq Scan",
		TotalCost:       20.0,
		ActualTotalTime: 3.0,
		ActualRows:      100,
		ActualLoops:     1,
	}

	delta := c.diffNodes(&old, &new)

	if delta.TimeDir != Improved {
		t.Errorf("TimeDir = %v, want Improved", delta.TimeDir)
	}
}

func TestDiffNodes_TypeChanged(t *testing.T) {
	c := defaultComparator()
	old := plan.PlanNode{
		NodeType:     "Seq Scan",
		RelationName: "users",
		TotalCost:    100.0,
		ActualRows:   1000,
		ActualLoops:  1,
	}
	new := plan.PlanNode{
		NodeType:     "Index Scan",
		RelationName: "users",
		TotalCost:    5.0,
		ActualRows:   10,
		ActualLoops:  1,
	}

	delta := c.diffNodes(&old, &new)

	if delta.ChangeType != TypeChanged {
		t.Errorf("ChangeType = %v, want TypeChanged", delta.ChangeType)
	}
	if delta.OldNodeType != "Seq Scan" {
		t.Errorf("OldNodeType = %q, want Seq Scan", delta.OldNodeType)
	}
	if delta.NewNodeType != "Index Scan" {
		t.Errorf("NewNodeType = %q, want Index Scan", delta.NewNodeType)
	}
}

func TestDiffNodes_SortSpillChange(t *testing.T) {
	c := defaultComparator()
	old := plan.PlanNode{
		NodeType:      "Sort",
		TotalCost:     100.0,
		SortSpaceType: "Disk",
		ActualLoops:   1,
	}
	new := plan.PlanNode{
		NodeType:      "Sort",
		TotalCost:     100.0,
		SortSpaceType: "Memory",
		ActualLoops:   1,
	}

	delta := c.diffNodes(&old, &new)

	if !delta.OldSortSpill {
		t.Error("OldSortSpill = false, want true")
	}
	if delta.NewSortSpill {
		t.Error("NewSortSpill = true, want false")
	}
	if delta.ChangeType == NoChange {
		t.Error("should be significant due to sort spill change")
	}
}

func TestDiffNodes_FilterChange(t *testing.T) {
	c := defaultComparator()
	old := plan.PlanNode{
		NodeType:    "Seq Scan",
		TotalCost:   20.0,
		ActualLoops: 1,
	}
	new := plan.PlanNode{
		NodeType:    "Seq Scan",
		TotalCost:   20.0,
		Filter:      "(id > 1)",
		ActualLoops: 1,
	}

	delta := c.diffNodes(&old, &new)

	if delta.OldFilter != "" {
		t.Errorf("OldFilter = %q, want empty", delta.OldFilter)
	}
	if delta.NewFilter != "(id > 1)" {
		t.Errorf("NewFilter = %q, want (id > 1)", delta.NewFilter)
	}
}

func TestDiffNodes_BufferDirection(t *testing.T) {
	c := defaultComparator()
	old := plan.PlanNode{
		NodeType:         "Seq Scan",
		TotalCost:        20.0,
		SharedReadBlocks: 1000,
		ActualLoops:      1,
	}
	new := plan.PlanNode{
		NodeType:         "Seq Scan",
		TotalCost:        20.0,
		SharedReadBlocks: 100,
		ActualLoops:      1,
	}

	delta := c.diffNodes(&old, &new)

	if delta.BufferDir != Improved {
		t.Errorf("BufferDir = %v, want Improved", delta.BufferDir)
	}
}

func TestDiffChildren_MatchedChildren(t *testing.T) {
	c := defaultComparator()
	oldKids := []plan.PlanNode{
		{NodeType: "Seq Scan", TotalCost: 10.0, ActualLoops: 1},
		{NodeType: "Hash", TotalCost: 5.0, ActualLoops: 1},
	}
	newKids := []plan.PlanNode{
		{NodeType: "Seq Scan", TotalCost: 10.0, ActualLoops: 1},
		{NodeType: "Hash", TotalCost: 5.0, ActualLoops: 1},
	}

	deltas := c.diffChildren(oldKids, newKids)

	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas, got %d", len(deltas))
	}
}

func TestDiffChildren_AddedNode(t *testing.T) {
	c := defaultComparator()
	oldKids := []plan.PlanNode{
		{NodeType: "Seq Scan", TotalCost: 10.0},
	}
	newKids := []plan.PlanNode{
		{NodeType: "Seq Scan", TotalCost: 10.0},
		{NodeType: "Hash", TotalCost: 5.0},
	}

	deltas := c.diffChildren(oldKids, newKids)

	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas, got %d", len(deltas))
	}
	if deltas[1].ChangeType != Added {
		t.Errorf("second delta ChangeType = %v, want Added", deltas[1].ChangeType)
	}
}

func TestDiffChildren_RemovedNode(t *testing.T) {
	c := defaultComparator()
	oldKids := []plan.PlanNode{
		{NodeType: "Seq Scan", TotalCost: 10.0},
		{NodeType: "Hash", TotalCost: 5.0},
	}
	newKids := []plan.PlanNode{
		{NodeType: "Seq Scan", TotalCost: 10.0},
	}

	deltas := c.diffChildren(oldKids, newKids)

	if len(deltas) != 2 {
		t.Fatalf("expected 2 deltas, got %d", len(deltas))
	}
	if deltas[1].ChangeType != Removed {
		t.Errorf("second delta ChangeType = %v, want Removed", deltas[1].ChangeType)
	}
}

func TestDiffChildren_EmptyBoth(t *testing.T) {
	c := defaultComparator()
	deltas := c.diffChildren(nil, nil)
	if len(deltas) != 0 {
		t.Errorf("expected 0 deltas, got %d", len(deltas))
	}
}

func TestCompare_BasicComparison(t *testing.T) {
	c := defaultComparator()
	old := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:        "Seq Scan",
			RelationName:    "users",
			TotalCost:       100.0,
			ActualTotalTime: 10.0,
			ActualRows:      1000,
			ActualLoops:     1,
		},
		PlanningTime:  1.0,
		ExecutionTime: 11.0,
	}
	new := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:        "Index Scan",
			RelationName:    "users",
			TotalCost:       5.0,
			ActualTotalTime: 0.5,
			ActualRows:      10,
			ActualLoops:     1,
		},
		PlanningTime:  1.5,
		ExecutionTime: 2.0,
	}

	result := c.Compare(old, new)

	s := result.Summary
	if s.CostDir != Improved {
		t.Errorf("CostDir = %v, want Improved", s.CostDir)
	}
	if s.TimeDir != Improved {
		t.Errorf("TimeDir = %v, want Improved", s.TimeDir)
	}
	if s.NodesTypeChanged != 1 {
		t.Errorf("NodesTypeChanged = %d, want 1", s.NodesTypeChanged)
	}
}

func TestCompare_IdenticalPlans(t *testing.T) {
	c := defaultComparator()
	p := plan.ExplainOutput{
		Plan: plan.PlanNode{
			NodeType:        "Seq Scan",
			TotalCost:       20.0,
			ActualTotalTime: 1.0,
			ActualRows:      100,
			ActualLoops:     1,
		},
		PlanningTime:  0.5,
		ExecutionTime: 1.5,
	}

	result := c.Compare(p, p)

	s := result.Summary
	if s.CostDelta != 0 {
		t.Errorf("CostDelta = %f, want 0", s.CostDelta)
	}
	if s.TimeDelta != 0 {
		t.Errorf("TimeDelta = %f, want 0", s.TimeDelta)
	}
	total := s.NodesAdded + s.NodesRemoved + s.NodesModified + s.NodesTypeChanged
	if total != 0 {
		t.Errorf("expected 0 changes, got %d", total)
	}
}

func TestCompare_VerdictFasterAndCheaper(t *testing.T) {
	c := defaultComparator()
	old := plan.ExplainOutput{
		Plan:          plan.PlanNode{TotalCost: 100.0, ActualLoops: 1},
		ExecutionTime: 50.0,
	}
	new := plan.ExplainOutput{
		Plan:          plan.PlanNode{TotalCost: 10.0, ActualLoops: 1},
		ExecutionTime: 5.0,
	}

	result := c.Compare(old, new)
	if result.Summary.Verdict != "faster and cheaper" {
		t.Errorf("Verdict = %q, want 'faster and cheaper'", result.Summary.Verdict)
	}
}

func TestCompare_VerdictSlowerAndMoreExpensive(t *testing.T) {
	c := defaultComparator()
	old := plan.ExplainOutput{
		Plan:          plan.PlanNode{TotalCost: 10.0, ActualLoops: 1},
		ExecutionTime: 5.0,
	}
	new := plan.ExplainOutput{
		Plan:          plan.PlanNode{TotalCost: 100.0, ActualLoops: 1},
		ExecutionTime: 50.0,
	}

	result := c.Compare(old, new)
	if result.Summary.Verdict != "slower and more expensive" {
		t.Errorf("Verdict = %q, want 'slower and more expensive'", result.Summary.Verdict)
	}
}

func TestCompare_VerdictNoChange(t *testing.T) {
	c := defaultComparator()
	p := plan.ExplainOutput{
		Plan:          plan.PlanNode{TotalCost: 20.0, ActualLoops: 1},
		ExecutionTime: 5.0,
	}

	result := c.Compare(p, p)
	if result.Summary.Verdict != "no significant change" {
		t.Errorf("Verdict = %q, want 'no significant change'", result.Summary.Verdict)
	}
}

func TestPctChange(t *testing.T) {
	tests := []struct {
		old, new, want float64
	}{
		{100, 200, 100.0},
		{100, 50, -50.0},
		{100, 100, 0},
		{0, 100, 100.0},
		{0, 0, 0},
	}

	for _, tt := range tests {
		got := pctChange(tt.old, tt.new)
		if got != tt.want {
			t.Errorf("pctChange(%f, %f) = %f, want %f", tt.old, tt.new, got, tt.want)
		}
	}
}

func TestDirection(t *testing.T) {
	c := defaultComparator()
	tests := []struct {
		old, new      float64
		lowerIsBetter bool
		want          Direction
	}{
		{100, 50, true, Improved},
		{50, 100, true, Regressed},
		{100, 100, true, Unchanged},
		{100, 99.5, true, Unchanged},
		{50, 100, false, Improved},
		{100, 50, false, Regressed},
	}

	for _, tt := range tests {
		got := c.direction(tt.old, tt.new, tt.lowerIsBetter)
		if got != tt.want {
			t.Errorf("direction(%f, %f, %v) = %v, want %v", tt.old, tt.new, tt.lowerIsBetter, got, tt.want)
		}
	}
}

func TestIsSignificant_CostChange(t *testing.T) {
	c := defaultComparator()
	d := NodeDelta{
		OldCost: 100.0,
		NewCost: 110.0,
		CostPct: 10.0,
	}
	if !c.isSignificant(d) {
		t.Error("10% cost change should be significant")
	}
}

func TestIsSignificant_TinyChange(t *testing.T) {
	c := defaultComparator()
	d := NodeDelta{
		OldCost: 100.0,
		NewCost: 100.5,
		CostPct: 0.5,
		OldTime: 10.0,
		NewTime: 10.05,
		TimePct: 0.5,
	}
	if c.isSignificant(d) {
		t.Error("0.5% change should not be significant")
	}
}

func TestIsSignificant_SortSpillChange(t *testing.T) {
	c := defaultComparator()
	d := NodeDelta{
		OldSortSpill: true,
		NewSortSpill: false,
	}
	if !c.isSignificant(d) {
		t.Error("sort spill change should be significant")
	}
}
