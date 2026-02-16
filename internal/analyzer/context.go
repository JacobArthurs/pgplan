package analyzer

import (
	"regexp"
	"strings"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

type PlanContext struct {
	CTEs     map[string]*CTEInfo
	AllNodes []*NodeRef
}

type CTEInfo struct {
	Name          string
	Node          *plan.PlanNode
	EstimatedRows int64
	ActualRows    int64
}

type NodeRef struct {
	Node   *plan.PlanNode
	Parent *plan.PlanNode
	Depth  int
}

func BuildContext(root *plan.PlanNode) PlanContext {
	ctx := PlanContext{
		CTEs: make(map[string]*CTEInfo),
	}
	collectContext(root, nil, 0, &ctx)
	return ctx
}

func collectContext(node *plan.PlanNode, parent *plan.PlanNode, depth int, ctx *PlanContext) {
	ctx.AllNodes = append(ctx.AllNodes, &NodeRef{
		Node:   node,
		Parent: parent,
		Depth:  depth,
	})

	// SubplanName uses the format "CTE <name>" for CTE definitions
	if node.SubplanName != "" && strings.HasPrefix(node.SubplanName, "CTE ") {
		name := strings.TrimPrefix(node.SubplanName, "CTE ")
		ctx.CTEs[name] = &CTEInfo{
			Name:          name,
			Node:          node,
			EstimatedRows: node.PlanRows,
			ActualRows:    node.ActualRows,
		}
	}

	for i := range node.Plans {
		collectContext(&node.Plans[i], node, depth+1, ctx)
	}
}

var (
	stringLiteralRe = regexp.MustCompile(`'[^']*'`)
	columnRefRe     = regexp.MustCompile(`\b(\w+)\.(\w+)\b`)
	castColRe       = regexp.MustCompile(`\(([a-zA-Z_]\w*)\)::`)
)

func ExtractConditionColumns(cond string) []string {
	if cond == "" {
		return nil
	}
	cleaned := stringLiteralRe.ReplaceAllString(cond, "")
	seen := make(map[string]bool)
	var cols []string
	for _, m := range columnRefRe.FindAllStringSubmatch(cleaned, -1) {
		col := m[2]
		if !seen[col] {
			seen[col] = true
			cols = append(cols, col)
		}
	}
	for _, m := range castColRe.FindAllStringSubmatch(cleaned, -1) {
		col := m[1]
		if !seen[col] {
			seen[col] = true
			cols = append(cols, col)
		}
	}
	return cols
}

func ConditionColumnsNotIn(filter, indexCond string) []string {
	filterCols := ExtractConditionColumns(filter)
	indexCols := make(map[string]bool)
	for _, col := range ExtractConditionColumns(indexCond) {
		indexCols[col] = true
	}

	var missing []string
	for _, col := range filterCols {
		if !indexCols[col] {
			missing = append(missing, col)
		}
	}
	return missing
}

var literalRe = regexp.MustCompile(`(?:^|[^<>!])=\s*'((?:[^']|'')*)'`)

func ExtractLiteralValue(cond string) string {
	m := literalRe.FindStringSubmatch(cond)
	if m == nil {
		return ""
	}

	return strings.ReplaceAll(m[1], "''", "'")
}
