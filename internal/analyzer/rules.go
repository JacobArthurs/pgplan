package analyzer

import (
	"fmt"
	"strings"

	"github.com/jacobarthurs/pgplan/internal/plan"
)

const (
	MinRowsForSeqScanWarning  = 10000
	MinRowsForCriticalScan    = 100000
	MinRowsForCriticalSeqScan = 1000000
	MinRowsForLowSelectivity  = 10000

	FilterRemovalWarningPct  = 50.0
	FilterRemovalCriticalPct = 95.0
	FilterRemovalCapPct      = 99.99
	RecheckWarningPct        = 50.0
	RecheckCriticalPct       = 90.0
	ReadBlocksCriticalPct    = 50.0

	NestedLoopWarningLoops   = 1000
	NestedLoopCriticalLoops  = 10000
	MaterializeWarningLoops  = 100
	MaterializeCriticalLoops = 10000

	MinReadBlocksForLowSelect = 1000

	HashBatchesCritical       = 8
	JoinFilterRemovalWarning  = 10000
	JoinFilterRemovalCritical = 1000000

	EstimateMismatchRatio      = 3.0
	MinRowsForEstimateMismatch = 100
	WideRowThreshold           = 2000
	WideRowMinRows             = 10000
)

// childIdx is the node's index within parent.Plans (-1 for root).
type Rule func(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding

var defaultRules = []Rule{
	checkIndexScanFilterInefficiency,
	checkSeqScanInJoin,
	checkSeqScanStandalone,
	checkBitmapHeapRecheck,
	checkNestedLoopHighLoops,
	checkSubPlanHighLoops,
	checkSortSpill,
	checkHashSpill,
	checkTempBlocks,
	checkWorkerMismatch,
	checkParallelOverhead,
	checkLargeJoinFilterRemoval,
	checkMaterializeHighLoops,
	checkIndexScanLowSelectivity,
	checkWideRows,
}

func checkIndexScanFilterInefficiency(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Index Scan" && node.NodeType != "Index Only Scan" {
		return nil
	}
	if node.Filter == "" || node.RowsRemovedByFilter == 0 {
		return nil
	}

	total := node.ActualRows + node.RowsRemovedByFilter
	if total == 0 {
		return nil
	}
	removedPct := float64(node.RowsRemovedByFilter) / float64(total) * 100

	if removedPct < FilterRemovalWarningPct {
		return nil
	}

	if removedPct > FilterRemovalCapPct && node.ActualRows > 0 {
		removedPct = FilterRemovalCapPct
	}

	severity := Warning
	if removedPct > FilterRemovalCriticalPct {
		severity = Critical
	}

	missingCols := ConditionColumnsNotIn(node.Filter, node.IndexCond)
	indexCols := ExtractConditionColumns(node.IndexCond)

	desc := fmt.Sprintf("%s on %s using %s filters out %.2f%% of rows (%d of %d)",
		node.NodeType, node.RelationName, node.IndexName,
		removedPct, node.RowsRemovedByFilter, total)

	var suggestion string
	if len(missingCols) > 0 && len(indexCols) > 0 {
		literal := ExtractLiteralValue(node.Filter)
		compositeCols := strings.Join(append(indexCols, missingCols...), ", ")
		suggestion = fmt.Sprintf("Column `%s` in filter is not in index; consider composite index on (%s)",
			strings.Join(missingCols, ", "), compositeCols)
		if literal != "" && len(missingCols) == 1 {
			suggestion += fmt.Sprintf(" or partial index WHERE %s = '%s'", missingCols[0], literal)
		}
	} else {
		suggestion = fmt.Sprintf("Add an index on %s covering the filter condition", node.RelationName)
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: desc,
		Suggestion:  suggestion,
	}}
}

func checkSeqScanInJoin(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if parent == nil {
		return nil
	}
	if !isJoinNode(parent) {
		return nil
	}
	if node.NodeType != "Seq Scan" {
		return nil
	}

	rows := node.ActualRows
	if rows == 0 {
		rows = node.PlanRows
	}
	if rows < MinRowsForSeqScanWarning {
		return nil
	}

	siblingRows := findSiblingRows(childIdx, parent)
	if siblingRows <= 0 || siblingRows >= rows/10 {
		return nil
	}

	severity := Warning
	if rows > MinRowsForCriticalSeqScan {
		severity = Critical
	}

	joinCol := extractJoinColumnForTable(parent, node.RelationName, node.Alias)

	desc := fmt.Sprintf("Seq Scan on %s scans %d rows to join against %d rows",
		node.RelationName, rows, siblingRows)

	siblingSource := findSiblingSource(childIdx, parent)
	if siblingSource != "" {
		desc += fmt.Sprintf(" from CTE %s", siblingSource)
	}

	suggestion := "Consider index on join column to enable index lookup instead of full scan"
	if joinCol != "" {
		joinCond := parent.HashCond
		if joinCond == "" {
			joinCond = parent.MergeCond
		}
		if strings.Contains(strings.ToLower(joinCond), "lower(") {
			suggestion = fmt.Sprintf("Consider index on lower(%s) to enable index lookup instead of full scan", joinCol)
		} else {
			suggestion = fmt.Sprintf("Consider index on %s to enable index lookup instead of full scan", joinCol)
		}
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: desc,
		Suggestion:  suggestion,
	}}
}

func checkSeqScanStandalone(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Seq Scan" {
		return nil
	}
	if node.Filter == "" {
		return nil
	}
	if parent != nil && isJoinNode(parent) {
		return nil
	}
	if node.RowsRemovedByFilter == 0 {
		return nil
	}

	rows := node.ActualRows
	if rows == 0 {
		rows = node.PlanRows
	}
	if rows < MinRowsForSeqScanWarning {
		return nil
	}

	total := rows + node.RowsRemovedByFilter
	removedPct := float64(node.RowsRemovedByFilter) / float64(total) * 100

	if removedPct < FilterRemovalWarningPct {
		return nil
	}

	if removedPct > FilterRemovalCapPct && node.ActualRows > 0 {
		removedPct = FilterRemovalCapPct
	}

	severity := Warning
	if total > MinRowsForCriticalScan {
		severity = Critical
	}

	filterCols := ExtractConditionColumns(node.Filter)

	desc := fmt.Sprintf("Seq Scan on %s filters out %.2f%% of rows (%d of %d)",
		node.RelationName, removedPct, node.RowsRemovedByFilter, total)

	suggestion := fmt.Sprintf("Add an index on %s covering the filter condition", node.RelationName)
	if len(filterCols) > 0 {
		literal := ExtractLiteralValue(node.Filter)
		suggestion = fmt.Sprintf("Consider index on %s(%s)", node.RelationName, strings.Join(filterCols, ", "))
		if literal != "" && len(filterCols) == 1 {
			suggestion += fmt.Sprintf(" or partial index WHERE %s = '%s'", filterCols[0], literal)
		}
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: desc,
		Suggestion:  suggestion,
	}}
}

func checkBitmapHeapRecheck(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Bitmap Heap Scan" {
		return nil
	}
	if node.LossyHeapBlocks == 0 {
		return nil
	}

	totalBlocks := node.ExactHeapBlocks + node.LossyHeapBlocks
	lossyPct := float64(node.LossyHeapBlocks) / float64(totalBlocks) * 100

	if lossyPct < RecheckWarningPct {
		return nil
	}

	severity := Warning
	if lossyPct > RecheckCriticalPct {
		severity = Critical
	}

	return []Finding{{
		Severity: severity,
		NodeType: node.NodeType,
		Relation: node.RelationName,
		Description: fmt.Sprintf("Bitmap Heap Scan on %s has %.1f%% lossy pages (%d of %d blocks) — bitmap exceeded work_mem",
			node.RelationName, lossyPct, node.LossyHeapBlocks, totalBlocks),
		Suggestion: "Increase work_mem to keep bitmap exact, or use a more selective index to reduce bitmap size",
	}}
}

func checkNestedLoopHighLoops(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Nested Loop" {
		return nil
	}
	if len(node.Plans) < 2 {
		return nil
	}

	inner := &node.Plans[1]
	if inner.ActualLoops < NestedLoopWarningLoops {
		return nil
	}

	severity := Warning
	if inner.ActualLoops > NestedLoopCriticalLoops {
		severity = Critical
	}

	innerTime := inner.ActualTotalTime * float64(inner.ActualLoops)
	desc := fmt.Sprintf("Nested Loop executes %s %d times (%.1fms total)",
		innerNodeLabel(inner), inner.ActualLoops, innerTime)

	suggestion := "Consider Hash Join or Merge Join; verify indexes exist on inner side join columns"
	if inner.NodeType == "Index Scan" && inner.Filter != "" {
		suggestion += fmt.Sprintf("; filter on %s may warrant a more selective index", inner.RelationName)
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    inner.RelationName,
		Description: desc,
		Suggestion:  suggestion,
	}}
}

func checkSortSpill(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.SortSpaceType != "Disk" {
		return nil
	}
	return []Finding{{
		Severity:    Critical,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Sort spilled to disk (%dkB) on %s", node.SortSpaceUsed, nodeLabel(node)),
		Suggestion:  fmt.Sprintf("Increase work_mem (currently needs >%dkB) or reduce data before sorting", node.SortSpaceUsed),
	}}
}

func checkHashSpill(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.HashBatches <= 1 {
		return nil
	}
	severity := Warning
	if node.HashBatches > HashBatchesCritical {
		severity = Critical
	}
	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Hash used %d batches with %dkB memory on %s", node.HashBatches, node.PeakMemoryUsage, nodeLabel(node)),
		Suggestion:  "Increase work_mem to fit the hash table in memory",
	}}
}

func checkTempBlocks(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	total := node.TempReadBlocks + node.TempWrittenBlocks
	if total == 0 {
		return nil
	}
	sizeMB := float64(total*8) / 1024
	return []Finding{{
		Severity:    Warning,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Temp I/O: %d blocks (%.1f MB) on %s", total, sizeMB, nodeLabel(node)),
		Suggestion:  "Increase work_mem or restructure query to reduce intermediate result size",
	}}
}

func checkWorkerMismatch(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.WorkersPlanned == 0 || node.WorkersLaunched >= node.WorkersPlanned {
		return nil
	}
	return []Finding{{
		Severity:    Warning,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Only %d of %d planned parallel workers launched on %s", node.WorkersLaunched, node.WorkersPlanned, nodeLabel(node)),
		Suggestion:  "Check max_parallel_workers and max_parallel_workers_per_gather settings",
	}}
}

func checkLargeJoinFilterRemoval(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.RowsRemovedByJoinFilter < JoinFilterRemovalWarning {
		return nil
	}
	severity := Warning
	if node.RowsRemovedByJoinFilter > JoinFilterRemovalCritical {
		severity = Critical
	}
	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Join filter removed %d rows on %s", node.RowsRemovedByJoinFilter, nodeLabel(node)),
		Suggestion:  "Move filter condition into the join clause or add an index to reduce join input",
	}}
}

func checkMaterializeHighLoops(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Materialize" {
		return nil
	}
	if node.ActualLoops < MaterializeWarningLoops {
		return nil
	}

	severity := Warning
	if node.ActualLoops > MaterializeCriticalLoops {
		severity = Critical
	}

	totalTime := node.ActualTotalTime * float64(node.ActualLoops)

	return []Finding{{
		Severity: severity,
		NodeType: node.NodeType,
		Relation: node.RelationName,
		Description: fmt.Sprintf("Materialize scanned %d times (%.1fms total, %d rows per scan)",
			node.ActualLoops, totalTime, node.ActualRows),
		Suggestion: "Planner couldn't find a better strategy; consider restructuring the query to use a Hash Join or CTE",
	}}
}

func checkIndexScanLowSelectivity(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Index Scan" && node.NodeType != "Index Only Scan" {
		return nil
	}
	if node.ActualRows < MinRowsForLowSelectivity {
		return nil
	}

	totalBlocks := node.SharedHitBlocks + node.SharedReadBlocks
	if totalBlocks == 0 {
		return nil
	}

	if node.SharedReadBlocks < MinReadBlocksForLowSelect {
		return nil
	}

	readPct := float64(node.SharedReadBlocks) / float64(totalBlocks) * 100
	if readPct < ReadBlocksCriticalPct {
		return nil
	}

	// Don't flag if there's a filter — checkIndexScanFilterInefficiency handles that
	if node.Filter != "" && node.RowsRemovedByFilter > 0 {
		return nil
	}

	return []Finding{{
		Severity: Info,
		NodeType: node.NodeType,
		Relation: node.RelationName,
		Description: fmt.Sprintf("%s on %s using %s returned %d rows reading %d blocks (%d%% from disk)",
			node.NodeType, node.RelationName, node.IndexName,
			node.ActualRows, totalBlocks, int(readPct)),
		Suggestion: "Index has low selectivity for this query; a Seq Scan may be cheaper, or the query may benefit from a more selective condition",
	}}
}

func checkSubPlanHighLoops(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.ParentRelationship != "SubPlan" {
		return nil
	}
	if node.ActualLoops < NestedLoopWarningLoops {
		return nil
	}

	severity := Warning
	if node.ActualLoops > NestedLoopCriticalLoops {
		severity = Critical
	}

	totalTime := node.ActualTotalTime * float64(node.ActualLoops)

	return []Finding{{
		Severity: severity,
		NodeType: node.NodeType,
		Relation: node.RelationName,
		Description: fmt.Sprintf("Correlated SubPlan executes %d times (%.1fms total)",
			node.ActualLoops, totalTime),
		Suggestion: "Rewrite as a JOIN or lateral join to avoid per-row subquery execution",
	}}
}

func checkWideRows(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.PlanWidth < WideRowThreshold {
		return nil
	}

	rows := node.ActualRows
	if rows == 0 {
		rows = node.PlanRows
	}
	if rows < WideRowMinRows {
		return nil
	}

	return []Finding{{
		Severity:    Info,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("%s produces %d rows at %d bytes wide", nodeLabel(node), rows, node.PlanWidth),
		Suggestion:  "Select only needed columns to reduce memory usage and improve cache efficiency",
	}}
}

func checkParallelOverhead(node *plan.PlanNode, parent *plan.PlanNode, childIdx int, ctx *PlanContext) []Finding {
	if node.NodeType != "Gather" && node.NodeType != "Gather Merge" {
		return nil
	}
	if len(node.Plans) == 0 {
		return nil
	}

	child := &node.Plans[0]
	if child.ActualLoops == 0 {
		return nil
	}

	// Total worker time = per-loop time * loops (loops = workers launched + leader)
	workerTime := child.ActualTotalTime * float64(child.ActualLoops)
	gatherTime := node.ActualTotalTime

	// If gather takes longer than worker time, parallelism hurt
	if gatherTime <= workerTime {
		return nil
	}

	overhead := gatherTime - workerTime

	return []Finding{{
		Severity: Info,
		NodeType: node.NodeType,
		Relation: node.RelationName,
		Description: fmt.Sprintf("%s overhead (%.1fms) exceeds parallel benefit (workers: %.1fms, gather: %.1fms)",
			node.NodeType, overhead, workerTime, gatherTime),
		Suggestion: "Parallel execution not beneficial here; consider SET max_parallel_workers_per_gather = 0 for this query",
	}}
}

func ConsolidateEstimateMismatches(root *plan.PlanNode, ctx *PlanContext) []Finding {
	var findings []Finding

	for _, cte := range ctx.CTEs {
		if cte.ActualRows == 0 || cte.EstimatedRows == 0 {
			continue
		}

		if cte.ActualRows < MinRowsForEstimateMismatch {
			continue
		}

		ratio := float64(cte.EstimatedRows) / float64(cte.ActualRows)
		if ratio < 1 {
			ratio = 1 / ratio
		}
		if ratio < EstimateMismatchRatio {
			continue
		}

		affected := collectInflatedFromCTE(root, cte, ctx)
		if len(affected) == 0 {
			continue
		}

		affected = dedup(affected)

		direction := "inflated"
		if cte.EstimatedRows < cte.ActualRows {
			direction = "deflated"
		}

		var sourceRelations []string
		collectSourceRelations(cte.Node, &sourceRelations)

		desc := fmt.Sprintf("Row estimates %s downstream of CTE %s (estimated %d, actual %d)",
			direction, cte.Name, cte.EstimatedRows, cte.ActualRows)

		suggestion := fmt.Sprintf("Affects %s estimates", strings.Join(affected, ", "))
		if len(sourceRelations) > 0 {
			suggestion += fmt.Sprintf("; run ANALYZE on %s", strings.Join(sourceRelations, " and "))
		}

		findings = append(findings, Finding{
			Severity:    Info,
			NodeType:    "CTE",
			Relation:    cte.Name,
			Description: desc,
			Suggestion:  suggestion,
		})
	}

	return findings
}

// Only blames nodes in the CTE's data flow path, not unrelated branches.
func collectInflatedFromCTE(root *plan.PlanNode, cte *CTEInfo, ctx *PlanContext) []string {
	var consumers []*plan.PlanNode
	for _, ref := range ctx.AllNodes {
		if ref.Node.CTEName == cte.Name {
			consumers = append(consumers, ref.Node)
		}
	}

	if len(consumers) == 0 {
		return nil
	}

	ancestorNodes := make(map[*plan.PlanNode]bool)
	for _, consumer := range consumers {
		collectAncestors(root, consumer, ancestorNodes)
	}

	var affected []string
	for node := range ancestorNodes {
		if node.PlanRows > 0 && node.ActualRows > 0 && node.ActualLoops > 0 {
			ratio := float64(node.PlanRows) / float64(node.ActualRows)
			if ratio < 1 {
				ratio = 1 / ratio
			}
			if ratio > EstimateMismatchRatio {
				affected = append(affected, node.NodeType)
			}
		}
	}
	return affected
}

func collectAncestors(current *plan.PlanNode, target *plan.PlanNode, ancestors map[*plan.PlanNode]bool) bool {
	if current == target {
		return true
	}
	for i := range current.Plans {
		if collectAncestors(&current.Plans[i], target, ancestors) {
			ancestors[current] = true
			return true
		}
	}
	return false
}

func collectSourceRelations(node *plan.PlanNode, relations *[]string) {
	if node.RelationName != "" {
		*relations = append(*relations, node.RelationName)
	}
	for i := range node.Plans {
		collectSourceRelations(&node.Plans[i], relations)
	}
}

func isJoinNode(node *plan.PlanNode) bool {
	switch node.NodeType {
	case "Hash Join", "Merge Join", "Nested Loop":
		return true
	}
	return false
}

func findSiblingRows(childIdx int, parent *plan.PlanNode) int64 {
	for i := range parent.Plans {
		if i != childIdx {
			actual := parent.Plans[i].ActualRows
			if actual == 0 {
				actual = parent.Plans[i].PlanRows
			}
			return actual
		}
	}
	return -1
}

func findSiblingSource(childIdx int, parent *plan.PlanNode) string {
	for i := range parent.Plans {
		if i != childIdx {
			return findCTEName(&parent.Plans[i])
		}
	}
	return ""
}

func findCTEName(node *plan.PlanNode) string {
	if node.CTEName != "" {
		return node.CTEName
	}
	for i := range node.Plans {
		if name := findCTEName(&node.Plans[i]); name != "" {
			return name
		}
	}
	return ""
}

func extractJoinColumnForTable(joinNode *plan.PlanNode, relation, alias string) string {
	cond := joinNode.HashCond
	if cond == "" {
		cond = joinNode.MergeCond
	}
	if cond == "" {
		return ""
	}

	for _, prefix := range []string{alias, relation} {
		if prefix == "" {
			continue
		}
		cols := ExtractConditionColumns(cond)
		condLower := strings.ToLower(cond)
		for _, col := range cols {
			if strings.Contains(condLower, strings.ToLower(prefix)+"."+strings.ToLower(col)) {
				return col
			}
		}
	}
	return ""
}

func nodeLabel(node *plan.PlanNode) string {
	if node.RelationName != "" {
		if node.Alias != "" && node.Alias != node.RelationName {
			return fmt.Sprintf("%s on %s (%s)", node.NodeType, node.RelationName, node.Alias)
		}
		return fmt.Sprintf("%s on %s", node.NodeType, node.RelationName)
	}
	return node.NodeType
}

func innerNodeLabel(node *plan.PlanNode) string {
	label := node.NodeType
	if node.RelationName != "" {
		label += " on " + node.RelationName
	}
	if node.IndexName != "" {
		label += " using " + node.IndexName
	}
	return label
}

func dedup(items []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}
