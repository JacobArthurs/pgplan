package comparator

import (
	"github.com/jacobarthurs/pgplan/internal/plan"
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

		OldTotalReads: old.Plan.SharedReadBlocks + old.Plan.TempReadBlocks,
		NewTotalReads: new.Plan.SharedReadBlocks + new.Plan.TempReadBlocks,
		OldTotalHits:  old.Plan.SharedHitBlocks,
		NewTotalHits:  new.Plan.SharedHitBlocks,
	}

	countChanges(&rootDelta, &summary)
	summary.Verdict = computeVerdict(summary)

	return ComparisonResult{
		Deltas:  []NodeDelta{rootDelta},
		Summary: summary,
	}
}

var verdicts = map[[2]Direction]string{
	{Improved, Improved}:   "faster and cheaper",
	{Regressed, Regressed}: "slower and more expensive",
	{Improved, Regressed}:  "faster but higher estimated cost",
	{Regressed, Improved}:  "cheaper but slower execution",
	{Improved, Unchanged}:  "faster",
	{Regressed, Unchanged}: "slower",
	{Unchanged, Improved}:  "cheaper",
	{Unchanged, Regressed}: "more expensive",
}

func computeVerdict(s Summary) string {
	if v, ok := verdicts[[2]Direction{s.TimeDir, s.CostDir}]; ok {
		return v
	}
	return "no significant change"
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
