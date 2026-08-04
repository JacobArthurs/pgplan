package analyzer

import "github.com/jacobarthurs/pgplan/internal/plan"

type Severity int

const (
	Info     Severity = 0
	Warning  Severity = 1
	Critical Severity = 2
)

func (s Severity) String() string {
	switch s {
	case Info:
		return "info"
	case Warning:
		return "warning"
	case Critical:
		return "critical"
	default:
		return "unknown"
	}
}

type Finding struct {
	Severity    Severity
	NodeType    string
	Relation    string
	Description string
	Suggestion  string

	// ActualRows is only meaningful when HasActualRows is true - EXPLAIN
	// (without ANALYZE) never populates it, and a genuinely-zero actual row
	// count must not be confused with "not reported".
	ActualRows    float64
	HasActualRows bool
}

type AnalysisResult struct {
	Findings      []Finding
	TotalCost     float64
	ExecutionTime float64
	PlanningTime  float64

	Buffers       plan.NodeBuffers
	SortSpaceUsed int64 // kB, summed across all sort nodes (memory + disk)

	// ActualRows is the root node's actual row count, only meaningful when
	// HasActualRows is true (i.e. the plan was produced with ANALYZE).
	ActualRows    float64
	HasActualRows bool
}
