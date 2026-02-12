package analyzer

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
}

type AnalysisResult struct {
	Findings      []Finding
	TotalCost     float64
	ExecutionTime float64
	PlanningTime  float64
}
