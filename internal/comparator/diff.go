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

	delta.OldLoops = old.ActualLoops
	delta.NewLoops = new.ActualLoops

	delta.OldRowsRemovedByFilter = old.RowsRemovedByFilter
	delta.NewRowsRemovedByFilter = new.RowsRemovedByFilter

	delta.OldWorkersLaunched = old.WorkersLaunched
	delta.NewWorkersLaunched = new.WorkersLaunched
	delta.OldWorkersPlanned = old.WorkersPlanned
	delta.NewWorkersPlanned = new.WorkersPlanned

	delta.OldBufferReads = old.SharedReadBlocks + old.TempReadBlocks
	delta.NewBufferReads = new.SharedReadBlocks + new.TempReadBlocks
	delta.OldBufferHits = old.SharedHitBlocks
	delta.NewBufferHits = new.SharedHitBlocks
	delta.BufferDir = c.bufferDirection(old, new)

	delta.OldSortSpill = old.SortSpaceType == "Disk"
	delta.NewSortSpill = new.SortSpaceType == "Disk"

	delta.OldHashBatches = old.HashBatches
	delta.NewHashBatches = new.HashBatches

	// Filter and IndexCond strings are compared verbatim. PG's EXPLAIN JSON
	// output is canonical, so formatting differences indicate actual changes.
	delta.OldFilter = old.Filter
	delta.NewFilter = new.Filter

	delta.OldIndexCond = old.IndexCond
	delta.NewIndexCond = new.IndexCond

	delta.OldIndexName = old.IndexName
	delta.NewIndexName = new.IndexName

	if delta.ChangeType == Modified && !c.isSignificant(delta) {
		delta.ChangeType = NoChange
	}

	delta.Children = c.diffChildren(old.Plans, new.Plans)

	return delta
}

// diffChildren uses position-based matching. When plan structure changes
// significantly (e.g., Seq Scan becomes Index Scan + Bitmap Heap Scan),
// position 0 shows as type_changed and additional nodes show as added.
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
	if d.OldLoops != d.NewLoops && d.OldLoops > 0 {
		loopRatio := float64(d.NewLoops) / float64(d.OldLoops)
		if loopRatio > 2 || loopRatio < 0.5 {
			return true
		}
	}
	if d.OldRowsRemovedByFilter != d.NewRowsRemovedByFilter {
		return true
	}
	if d.OldWorkersLaunched != d.NewWorkersLaunched {
		return true
	}
	if d.OldSortSpill != d.NewSortSpill {
		return true
	}
	if d.OldHashBatches != d.NewHashBatches {
		return true
	}
	if d.OldBufferReads != d.NewBufferReads {
		return true
	}
	if d.OldFilter != d.NewFilter {
		return true
	}
	if d.OldIndexCond != d.NewIndexCond {
		return true
	}
	if d.OldIndexName != d.NewIndexName {
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
