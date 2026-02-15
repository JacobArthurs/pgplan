package plan

type PlanNode struct {
	// Core identity
	NodeType           string `json:"Node Type"`
	ParentRelationship string `json:"Parent Relationship,omitempty"`
	Strategy           string `json:"Strategy,omitempty"`
	PartialMode        string `json:"Partial Mode,omitempty"`

	// Estimates vs actuals
	StartupCost       float64 `json:"Startup Cost"`
	TotalCost         float64 `json:"Total Cost"`
	PlanRows          int64   `json:"Plan Rows"`
	PlanWidth         int     `json:"Plan Width"`
	ActualStartupTime float64 `json:"Actual Startup Time,omitempty"`
	ActualTotalTime   float64 `json:"Actual Total Time,omitempty"`
	ActualRows        int64   `json:"Actual Rows,omitempty"`
	ActualLoops       int64   `json:"Actual Loops,omitempty"`

	// Relation/index info
	Schema        string `json:"Schema,omitempty"`
	RelationName  string `json:"Relation Name,omitempty"`
	Alias         string `json:"Alias,omitempty"`
	IndexName     string `json:"Index Name,omitempty"`
	ScanDirection string `json:"Scan Direction,omitempty"`

	// Conditions
	IndexCond                 string `json:"Index Cond,omitempty"`
	Filter                    string `json:"Filter,omitempty"`
	RowsRemovedByFilter       int64  `json:"Rows Removed by Filter,omitempty"`
	ExactHeapBlocks           int64  `json:"Exact Heap Blocks,omitempty"`
	LossyHeapBlocks           int64  `json:"Lossy Heap Blocks,omitempty"`

	// Join info
	JoinType                string `json:"Join Type,omitempty"`
	JoinFilter              string `json:"Join Filter,omitempty"`
	HashCond                string `json:"Hash Cond,omitempty"`
	MergeCond               string `json:"Merge Cond,omitempty"`
	InnerUnique             bool   `json:"Inner Unique,omitempty"`
	RowsRemovedByJoinFilter int64  `json:"Rows Removed by Join Filter,omitempty"`

	// Sort
	SortKey       []string `json:"Sort Key,omitempty"`
	SortMethod    string   `json:"Sort Method,omitempty"`
	SortSpaceUsed int64    `json:"Sort Space Used,omitempty"`
	SortSpaceType string   `json:"Sort Space Type,omitempty"`

	// Hash
	HashBuckets         int   `json:"Hash Buckets,omitempty"`
	HashBatches         int   `json:"Hash Batches,omitempty"`
	OriginalHashBatches int   `json:"Original Hash Batches,omitempty"`
	PeakMemoryUsage     int64 `json:"Peak Memory Usage,omitempty"`

	// Buffers
	SharedHitBlocks     int64 `json:"Shared Hit Blocks,omitempty"`
	SharedReadBlocks    int64 `json:"Shared Read Blocks,omitempty"`
	SharedDirtiedBlocks int64 `json:"Shared Dirtied Blocks,omitempty"`
	SharedWrittenBlocks int64 `json:"Shared Written Blocks,omitempty"`
	TempReadBlocks      int64 `json:"Temp Read Blocks,omitempty"`
	TempWrittenBlocks   int64 `json:"Temp Written Blocks,omitempty"`

	// Parallel query
	WorkersPlanned  int `json:"Workers Planned,omitempty"`
	WorkersLaunched int `json:"Workers Launched,omitempty"`

	// CTE
	CTEName string `json:"CTE Name,omitempty"`

	// Group/Aggregate
	GroupKey []string `json:"Group Key,omitempty"`

	// Children
	Plans []PlanNode `json:"Plans,omitempty"`

	SubplanName string `json:"Subplan Name,omitempty"`
}

// ExplainOutput represents the top-level EXPLAIN JSON output from PostgreSQL.
type ExplainOutput struct {
	Plan          PlanNode `json:"Plan"`
	PlanningTime  float64  `json:"Planning Time,omitempty"`
	ExecutionTime float64  `json:"Execution Time,omitempty"`
	Triggers      []any    `json:"Triggers,omitempty"`
}
