package analyzer

import (
	"regexp"
	"strings"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

type PlanContext struct {
	CTEs     map[string]*CTEInfo
	AllNodes []*NodeRef

	// BlockSize is the PostgreSQL page size (bytes) used to render block
	// counts as human-readable sizes in Finding descriptions. Zero means
	// "use plan.DefaultBlockSize" - see BlockSizeOrDefault.
	BlockSize int64

	// Analyzed is true when the plan was produced with EXPLAIN ANALYZE, as
	// determined once at the query level (see Analyze) - not per node, since
	// a node's own Actual Loops is legitimately 0 for a skipped CASE branch,
	// excluded partition, etc. even when the query was analyzed.
	Analyzed bool
}

// BlockSizeOrDefault returns ctx.BlockSize, falling back to
// plan.DefaultBlockSize when unset (e.g. contexts built directly by tests).
func (ctx *PlanContext) BlockSizeOrDefault() int64 {
	if ctx.BlockSize <= 0 {
		return plan.DefaultBlockSize
	}
	return ctx.BlockSize
}

type CTEInfo struct {
	Name          string
	Node          *plan.PlanNode
	EstimatedRows int64
	ActualRows    float64
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

func collectContext(node, parent *plan.PlanNode, depth int, ctx *PlanContext) {
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

		if col := m[2]; !seen[col] {
			seen[col] = true
			cols = append(cols, col)
		}
	}
	for _, m := range castColRe.FindAllStringSubmatch(cleaned, -1) {

		if col := m[1]; !seen[col] {
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
