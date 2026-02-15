package comparator

type Direction int

const (
	Unchanged Direction = 0
	Improved  Direction = 1
	Regressed Direction = 2

	SignificanceThresholdPct = 1.0
)

func (d Direction) String() string {
	switch d {
	case Improved:
		return "improved"
	case Regressed:
		return "regressed"
	default:
		return "unchanged"
	}
}

type ChangeType int

const (
	NoChange    ChangeType = 0
	Modified    ChangeType = 1
	Added       ChangeType = 2
	Removed     ChangeType = 3
	TypeChanged ChangeType = 4
)

func (c ChangeType) String() string {
	switch c {
	case Modified:
		return "modified"
	case Added:
		return "added"
	case Removed:
		return "removed"
	case TypeChanged:
		return "type_changed"
	default:
		return "no_change"
	}
}

type NodeDelta struct {
	NodeType   string
	Relation   string
	ChangeType ChangeType

	OldNodeType string
	NewNodeType string

	OldCost   float64
	NewCost   float64
	CostDelta float64
	CostPct   float64
	CostDir   Direction

	OldTime   float64
	NewTime   float64
	TimeDelta float64
	TimePct   float64
	TimeDir   Direction

	OldRows   int64
	NewRows   int64
	RowsDelta int64
	RowsPct   float64
	RowsDir   Direction

	// Loops
	OldLoops int64
	NewLoops int64

	// Filter effectiveness
	OldRowsRemovedByFilter int64
	NewRowsRemovedByFilter int64

	// Parallel
	OldWorkersLaunched int
	NewWorkersLaunched int
	OldWorkersPlanned  int
	NewWorkersPlanned  int

	// Buffers (aggregated)
	OldBufferReads int64 // SharedRead + TempRead
	NewBufferReads int64
	OldBufferHits  int64
	NewBufferHits  int64
	BufferDir      Direction

	OldSortSpill   bool
	NewSortSpill   bool
	OldHashBatches int
	NewHashBatches int

	OldFilter string
	NewFilter string

	OldIndexCond string
	NewIndexCond string

	OldIndexName string
	NewIndexName string

	Children []NodeDelta
}

type ComparisonResult struct {
	Deltas  []NodeDelta
	Summary Summary
}

type Summary struct {
	OldTotalCost float64
	NewTotalCost float64
	CostDelta    float64
	CostPct      float64
	CostDir      Direction

	OldExecutionTime float64
	NewExecutionTime float64
	TimeDelta        float64
	TimePct          float64
	TimeDir          Direction

	OldPlanningTime float64
	NewPlanningTime float64
	PlanningDir     Direction

	NodesAdded       int
	NodesRemoved     int
	NodesModified    int
	NodesTypeChanged int

	OldTotalReads int64
	NewTotalReads int64
	OldTotalHits  int64
	NewTotalHits  int64

	Verdict string
}
