package plan

import "testing"

// PostgreSQL's per-node Buffers counters are cumulative - a node's reported
// counts already include everything its descendants did, the same way
// Actual Total Time does. So AggregateBuffers must read only the root node's
// own counters, not sum every node in the tree (which would recount a
// child's I/O once per ancestor level above it).
func TestAggregateBuffers_UsesRootCountersOnly(t *testing.T) {
	root := PlanNode{
		NodeType:            "Hash Join",
		SharedHitBlocks:     10,
		SharedReadBlocks:    5,
		SharedDirtiedBlocks: 1,
		SharedWrittenBlocks: 2,
		LocalHitBlocks:      3,
		LocalReadBlocks:     4,
		LocalDirtiedBlocks:  0,
		LocalWrittenBlocks:  1,
		TempReadBlocks:      6,
		TempWrittenBlocks:   7,
		Plans: []PlanNode{
			{
				NodeType:         "Seq Scan",
				SharedHitBlocks:  20,
				SharedReadBlocks: 8,
				TempReadBlocks:   1,
			},
			{
				NodeType:           "Sort",
				LocalHitBlocks:     2,
				LocalWrittenBlocks: 3,
			},
		},
	}

	got := AggregateBuffers(&root)

	want := NodeBuffers{
		Shared: BlockCounts{Hit: 10, Read: 5, Dirtied: 1, Written: 2},
		Local:  BlockCounts{Hit: 3, Read: 4, Dirtied: 0, Written: 1},
		Temp:   BlockCounts{Read: 6, Written: 7},
	}

	if got != want {
		t.Errorf("AggregateBuffers() = %+v, want %+v (root's own counters, children ignored)", got, want)
	}
}

func TestNodeBuffers_Totals(t *testing.T) {
	b := NodeBuffers{
		Shared: BlockCounts{Hit: 10, Read: 5, Dirtied: 1, Written: 2},
		Local:  BlockCounts{Hit: 3, Read: 4, Dirtied: 1, Written: 1},
		Temp:   BlockCounts{Read: 6, Written: 7},
	}

	if got := b.TotalRead(); got != 15 {
		t.Errorf("TotalRead() = %d, want 15", got)
	}
	if got := b.TotalWritten(); got != 10 {
		t.Errorf("TotalWritten() = %d, want 10", got)
	}
	if got := b.TotalHit(); got != 13 {
		t.Errorf("TotalHit() = %d, want 13", got)
	}
	if got := b.TotalDirtied(); got != 2 {
		t.Errorf("TotalDirtied() = %d, want 2", got)
	}
}

func TestAggregateSortSpaceUsed_SumsAcrossTree(t *testing.T) {
	root := PlanNode{
		NodeType: "Sort",
		Plans: []PlanNode{
			{NodeType: "Sort", SortSpaceUsed: 100},
			{NodeType: "Seq Scan", SortSpaceUsed: 0},
		},
		SortSpaceUsed: 50,
	}

	if got := AggregateSortSpaceUsed(&root); got != 150 {
		t.Errorf("AggregateSortSpaceUsed() = %d, want 150", got)
	}
}
