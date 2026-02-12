package analyzer

import (
	"pgplan/internal/plan"
	"sort"
)

func Analyze(output plan.ExplainOutput) AnalysisResult {
	result := AnalysisResult{
		TotalCost:     output.Plan.TotalCost,
		ExecutionTime: output.ExecutionTime,
		PlanningTime:  output.PlanningTime,
	}

	walkTree(&output.Plan, nil, defaultRules, &result)

	// Sort findings
	sort.Slice(result.Findings, func(i, j int) bool {
		return result.Findings[i].Severity > result.Findings[j].Severity
	})

	return result
}

func walkTree(node *plan.PlanNode, parent *plan.PlanNode, rules []Rule, result *AnalysisResult) {
	for _, rule := range rules {
		findings := rule(node, parent)
		result.Findings = append(result.Findings, findings...)
	}

	for i := range node.Plans {
		walkTree(&node.Plans[i], node, rules, result)
	}
}
