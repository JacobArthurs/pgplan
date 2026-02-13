package comparator

import (
	"math"

	"pgplan/internal/plan"
)

func (c *Comparator) diffNodes(old, new *plan.PlanNode) NodeDelta {
	delta := NodeDelta{
		Relation: coalesce(old.RelationName, new.RelationName),
	}

	if old.NodeType != new.NodeType {
		delta.ChangeType = TypeChanged
		delta.OldNodeType = old.NodeType
		delta.NewNodeType = new.NodeType
		delta.NodeType = new.NodeType
	} else {
		delta.ChangeType = Modified
		delta.NodeType = old.NodeType
	}

	delta.OldCost = old.TotalCost
	delta.NewCost = new.TotalCost
	delta.CostDelta = new.TotalCost - old.TotalCost
	delta.CostPct = pctChange(old.TotalCost, new.TotalCost)
	delta.CostDir = c.direction(old.TotalCost, new.TotalCost, true)

	delta.OldTime = old.ActualTotalTime
	delta.NewTime = new.ActualTotalTime
	delta.TimeDelta = new.ActualTotalTime - old.ActualTotalTime
	delta.TimePct = pctChange(old.ActualTotalTime, new.ActualTotalTime)
	delta.TimeDir = c.direction(old.ActualTotalTime, new.ActualTotalTime, true)

	delta.OldRows = old.ActualRows
	delta.NewRows = new.ActualRows
	delta.RowsDelta = new.ActualRows - old.ActualRows
	delta.RowsPct = pctChange(float64(old.ActualRows), float64(new.ActualRows))
	delta.RowsDir = Unchanged

	delta.OldSharedHit = old.SharedHitBlocks
	delta.NewSharedHit = new.SharedHitBlocks
	delta.OldSharedRead = old.SharedReadBlocks
	delta.NewSharedRead = new.SharedReadBlocks
	delta.OldTempBlocks = old.TempReadBlocks + old.TempWrittenBlocks
	delta.NewTempBlocks = new.TempReadBlocks + new.TempWrittenBlocks
	delta.BufferDir = c.bufferDirection(old, new)

	delta.OldSortSpill = old.SortSpaceType == "Disk"
	delta.NewSortSpill = new.SortSpaceType == "Disk"

	delta.OldHashBatches = old.HashBatches
	delta.NewHashBatches = new.HashBatches

	if delta.ChangeType == Modified && !c.isSignificant(delta) {
		delta.ChangeType = NoChange
	}

	delta.Children = c.diffChildren(old.Plans, new.Plans)

	return delta
}

func (c *Comparator) diffChildren(oldKids, newKids []plan.PlanNode) []NodeDelta {
	var deltas []NodeDelta

	for i := range max(len(oldKids), len(newKids)) {
		if i >= len(oldKids) {
			deltas = append(deltas, addedNode(&newKids[i]))
			continue
		}
		if i >= len(newKids) {
			deltas = append(deltas, removedNode(&oldKids[i]))
			continue
		}
		deltas = append(deltas, c.diffNodes(&oldKids[i], &newKids[i]))
	}

	return deltas
}

func addedNode(node *plan.PlanNode) NodeDelta {
	delta := NodeDelta{
		ChangeType: Added,
		NodeType:   node.NodeType,
		Relation:   node.RelationName,
		NewCost:    node.TotalCost,
		NewTime:    node.ActualTotalTime,
		NewRows:    node.ActualRows,
	}

	for _, child := range node.Plans {
		delta.Children = append(delta.Children, addedNode(&child))
	}

	return delta
}

func removedNode(node *plan.PlanNode) NodeDelta {
	delta := NodeDelta{
		ChangeType: Removed,
		NodeType:   node.NodeType,
		Relation:   node.RelationName,
		OldCost:    node.TotalCost,
		OldTime:    node.ActualTotalTime,
		OldRows:    node.ActualRows,
	}

	for _, child := range node.Plans {
		delta.Children = append(delta.Children, removedNode(&child))
	}

	return delta
}

func (c *Comparator) isSignificant(d NodeDelta) bool {
	if math.Abs(d.CostPct) > c.Threshold {
		return true
	}
	if math.Abs(d.TimePct) > c.Threshold {
		return true
	}
	if d.OldSortSpill != d.NewSortSpill {
		return true
	}
	if d.OldHashBatches != d.NewHashBatches {
		return true
	}
	if d.OldTempBlocks != d.NewTempBlocks {
		return true
	}
	if d.OldSharedRead != d.NewSharedRead {
		return true
	}
	return false
}

func (c *Comparator) direction(old, new float64, lowerPreference bool) Direction {
	if math.Abs(pctChange(old, new)) < c.Threshold {
		return Unchanged
	}
	if lowerPreference {
		if new < old {
			return Improved
		}
		return Regressed
	}
	if new > old {
		return Improved
	}
	return Regressed
}

func (c *Comparator) bufferDirection(old, new *plan.PlanNode) Direction {
	oldTotal := float64(old.SharedReadBlocks + old.TempReadBlocks + old.TempWrittenBlocks)
	newTotal := float64(new.SharedReadBlocks + new.TempReadBlocks + new.TempWrittenBlocks)
	return c.direction(oldTotal, newTotal, true)
}

func pctChange(old, new float64) float64 {
	if old == 0 {
		if new == 0 {
			return 0
		}
		return 100
	}
	return ((new - old) / old) * 100
}

func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
