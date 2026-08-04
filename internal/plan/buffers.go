package plan

// BlockCounts holds the buffer hit/read/dirtied/written counters PostgreSQL
// reports for a single buffer category (shared, local, or temp).
type BlockCounts struct {
	Hit     int64
	Read    int64
	Dirtied int64
	Written int64
}

// NodeBuffers groups block counts by category. Temp buffers are never
// "hit" or "dirtied" - PostgreSQL only reports Read/Written for them.
type NodeBuffers struct {
	Shared BlockCounts
	Local  BlockCounts
	Temp   BlockCounts
}

func (n NodeBuffers) TotalRead() int64 {
	return n.Shared.Read + n.Local.Read + n.Temp.Read
}

func (n NodeBuffers) TotalWritten() int64 {
	return n.Shared.Written + n.Local.Written + n.Temp.Written
}

func (n NodeBuffers) TotalHit() int64 {
	return n.Shared.Hit + n.Local.Hit
}

func (n NodeBuffers) TotalDirtied() int64 {
	return n.Shared.Dirtied + n.Local.Dirtied
}

// NodeBufferBreakdown returns the buffer counters reported directly on node,
// without descending into its children.
func NodeBufferBreakdown(node *PlanNode) NodeBuffers {
	return NodeBuffers{
		Shared: BlockCounts{
			Hit:     node.SharedHitBlocks,
			Read:    node.SharedReadBlocks,
			Dirtied: node.SharedDirtiedBlocks,
			Written: node.SharedWrittenBlocks,
		},
		Local: BlockCounts{
			Hit:     node.LocalHitBlocks,
			Read:    node.LocalReadBlocks,
			Dirtied: node.LocalDirtiedBlocks,
			Written: node.LocalWrittenBlocks,
		},
		Temp: BlockCounts{
			Read:    node.TempReadBlocks,
			Written: node.TempWrittenBlocks,
		},
	}
}

// AggregateBuffers returns the buffer counters for the whole plan rooted at
// root. PostgreSQL's per-node Buffers counters are cumulative - like Actual
// Total Time, a node's reported counts already include everything its
// descendants did - so the root node's own counters already represent the
// query total. Summing every node in the tree would recount the same I/O
// once per ancestor level it passes through.
func AggregateBuffers(root *PlanNode) NodeBuffers {
	return NodeBufferBreakdown(root)
}

// AggregateSortSpaceUsed sums the sort/hash space (in kB) used by every sort
// node in the plan, regardless of whether it spilled to disk.
func AggregateSortSpaceUsed(root *PlanNode) int64 {
	var total int64

	var walk func(node *PlanNode)
	walk = func(node *PlanNode) {
		total += node.SortSpaceUsed
		for i := range node.Plans {
			walk(&node.Plans[i])
		}
	}
	walk(root)

	return total
}
