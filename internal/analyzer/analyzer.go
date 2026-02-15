package analyzer

import (
	"sort"

	"pgplan/internal/plan"
)

func Analyze(output plan.ExplainOutput) AnalysisResult {
	result := AnalysisResult{
		TotalCost:     output.Plan.TotalCost,
		ExecutionTime: output.ExecutionTime,
		PlanningTime:  output.PlanningTime,
	}

	ctx := BuildContext(&output.Plan)
	walkTree(&output.Plan, nil, -1, defaultRules, &ctx, &result)

	consolidated := ConsolidateEstimateMismatches(&output.Plan, &ctx)
	result.Findings = append(result.Findings, consolidated...)

	sort.Slice(result.Findings, func(i, j int) bool {
		return result.Findings[i].Severity > result.Findings[j].Severity
	})

	return result
}

func walkTree(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, rules []Rule, ctx *PlanContext, result *AnalysisResult) {
	for _, rule := range rules {
		findings := rule(node, parent, childIdx, ctx)
		result.Findings = append(result.Findings, findings...)
	}

	for i := range node.Plans {
		walkTree(&node.Plans[i], node, i, rules, ctx, result)
	}
}
