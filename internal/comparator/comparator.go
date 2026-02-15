package comparator

import (
	"pgplan/internal/plan"
)

type Comparator struct {
	Threshold float64
}

func (c *Comparator) Compare(old, new plan.ExplainOutput) ComparisonResult {
	rootDelta := c.diffNodes(&old.Plan, &new.Plan)

	summary := Summary{
		OldTotalCost: old.Plan.TotalCost,
		NewTotalCost: new.Plan.TotalCost,
		CostDelta:    new.Plan.TotalCost - old.Plan.TotalCost,
		CostPct:      pctChange(old.Plan.TotalCost, new.Plan.TotalCost),
		CostDir:      c.direction(old.Plan.TotalCost, new.Plan.TotalCost, true),

		OldExecutionTime: old.ExecutionTime,
		NewExecutionTime: new.ExecutionTime,
		TimeDelta:        new.ExecutionTime - old.ExecutionTime,
		TimePct:          pctChange(old.ExecutionTime, new.ExecutionTime),
		TimeDir:          c.direction(old.ExecutionTime, new.ExecutionTime, true),

		OldPlanningTime: old.PlanningTime,
		NewPlanningTime: new.PlanningTime,
		PlanningDir:     c.direction(old.PlanningTime, new.PlanningTime, true),

		OldSharedRead: old.Plan.SharedReadBlocks,
		NewSharedRead: new.Plan.SharedReadBlocks,
		OldSharedHit:  old.Plan.SharedHitBlocks,
		NewSharedHit:  new.Plan.SharedHitBlocks,
	}

	countChanges(&rootDelta, &summary)

	return ComparisonResult{
		Deltas:  []NodeDelta{rootDelta},
		Summary: summary,
	}
}

func countChanges(delta *NodeDelta, summary *Summary) {
	switch delta.ChangeType {
	case Added:
		summary.NodesAdded++
	case Removed:
		summary.NodesRemoved++
	case Modified:
		summary.NodesModified++
	case TypeChanged:
		summary.NodesTypeChanged++
	}

	for i := range delta.Children {
		countChanges(&delta.Children[i], summary)
	}
}
