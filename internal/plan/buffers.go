package plan

type BlockCounts struct {
	Hit     int64
	Read    int64
	Dirtied int64
	Written int64
}

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

func AggregateBuffers(root *PlanNode) NodeBuffers {
	return NodeBufferBreakdown(root)
}

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
