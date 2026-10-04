package analyzer

import (
	"sort"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

func Analyze(output plan.ExplainOutput, blockSize ...int64) AnalysisResult {
	analyzed := output.PlanningTime > 0 || output.ExecutionTime > 0

	result := AnalysisResult{
		TotalCost:     output.Plan.TotalCost,
		ExecutionTime: output.ExecutionTime,
		PlanningTime:  output.PlanningTime,
		Buffers:       plan.AggregateBuffers(&output.Plan),
		SortSpaceUsed: plan.AggregateSortSpaceUsed(&output.Plan),
		HasActualRows: analyzed,
	}
	if analyzed {
		result.ActualRows = output.Plan.ActualRows
	}

	ctx := BuildContext(&output.Plan)
	ctx.Analyzed = analyzed
	if len(blockSize) > 0 {
		ctx.BlockSize = blockSize[0]
	}
	walkTree(&output.Plan, nil, -1, defaultRules, &ctx, &result)

	consolidated := ConsolidateEstimateMismatches(&output.Plan, &ctx)
	result.Findings = append(result.Findings, consolidated...)

	sort.Slice(result.Findings, func(i, j int) bool {
		return result.Findings[i].Severity > result.Findings[j].Severity
	})

	return result
}

func walkTree(node, parent *plan.PlanNode, childIdx int, rules []Rule, ctx *PlanContext, result *AnalysisResult) {
	for _, rule := range rules {
		findings := rule(node, parent, childIdx, ctx)
		for i := range findings {
			findings[i].HasActualRows = ctx.Analyzed
			if ctx.Analyzed {
				findings[i].ActualRows = node.ActualRows
			}
		}
		result.Findings = append(result.Findings, findings...)
	}

	for i := range node.Plans {
		walkTree(&node.Plans[i], node, i, rules, ctx, result)
	}
}
