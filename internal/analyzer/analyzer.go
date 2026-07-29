package analyzer

import (
	"sort"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

// Analyze evaluates output against the default rule set. blockSize is the
// PostgreSQL page size (bytes) used to render block counts in Finding
// descriptions as human-readable sizes; omit it (or pass <= 0) to use
// plan.DefaultBlockSize.
func Analyze(output plan.ExplainOutput, blockSize ...int64) AnalysisResult {
	// Planning/Execution Time are only present in the JSON when ANALYZE was
	// used. Determined once at the query level rather than per node: a
	// node's own Actual Loops is legitimately 0 for a skipped CASE branch,
	// excluded partition, etc. even when the query was analyzed.
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
