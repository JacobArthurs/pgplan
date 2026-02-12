package analyzer

import (
	"fmt"
	"pgplan/internal/plan"
)

type Rule func(node *plan.PlanNode, parent *plan.PlanNode) []Finding

var defaultRules = []Rule{
	checkSeqScan,
	checkRowEstimateMismatch,
	checkSortSpill,
	checkHashSpill,
	checkTempBlocks,
	checkHighFilterRemoval,
	checkNestedLoopLargeOuter,
	checkWorkerMismatch,
	checkLargeJoinFilterRemoval,
	checkBitmapRecheck,
}

func checkSeqScan(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.NodeType != "Seq Scan" {
		return nil
	}
	if node.Filter == "" {
		return nil
	}

	rows := node.ActualRows
	if rows == 0 {
		rows = node.PlanRows
	}
	if rows < 1000 {
		return nil
	}

	severity := Warning
	if rows > 100000 {
		severity = Critical
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Sequential scan on %s with filter %s (%d rows)", node.RelationName, node.Filter, rows),
		Suggestion:  fmt.Sprintf("Consider adding an index on %s matching filter %s", node.RelationName, node.Filter),
	}}
}

func checkRowEstimateMismatch(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.ActualRows == 0 && node.PlanRows == 0 {
		return nil
	}
	if node.ActualLoops == 0 {
		return nil
	}

	actual := node.ActualRows
	estimated := node.PlanRows

	if estimated == 0 || actual == 0 {
		if actual > 1000 && estimated == 0 {
			return []Finding{{
				Severity:    Critical,
				NodeType:    node.NodeType,
				Relation:    node.RelationName,
				Description: fmt.Sprintf("Planner estimated 0 rows but got %d on %s", actual, nodeLabel(node)),
				Suggestion:  "Run ANALYZE on the table to update statistics",
			}}
		}
		return nil
	}

	ratio := float64(actual) / float64(estimated)
	if ratio < 1 {
		ratio = 1 / ratio
	}

	if ratio < 10 {
		return nil
	}

	severity := Warning
	if ratio > 100 {
		severity = Critical
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Row estimate off by %.0fx on %s (estimated %d, actual %d)", ratio, nodeLabel(node), estimated, actual),
		Suggestion:  "Run ANALYZE on the table; consider adjusting default_statistics_target or creating extended statistics",
	}}
}

func checkSortSpill(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.SortSpaceType != "Disk" {
		return nil
	}

	return []Finding{{
		Severity:    Critical,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Sort spilled to disk (%dkB) on %s", node.SortSpaceUsed, nodeLabel(node)),
		Suggestion:  fmt.Sprintf("Increase work_mem (currently needs >%dkB) or reduce the data set before sorting", node.SortSpaceUsed),
	}}
}

func checkHashSpill(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.HashBatches <= 1 {
		return nil
	}

	severity := Warning
	if node.HashBatches > 8 {
		severity = Critical
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Hash used %d batches (originally %d) with %dkB memory on %s", node.HashBatches, node.OriginalHashBatches, node.PeakMemoryUsage, nodeLabel(node)),
		Suggestion:  "Increase work_mem to fit the hash table in memory",
	}}
}

func checkTempBlocks(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
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

func checkHighFilterRemoval(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.RowsRemovedByFilter == 0 || node.ActualRows == 0 {
		return nil
	}

	total := node.ActualRows + node.RowsRemovedByFilter
	removedPct := float64(node.RowsRemovedByFilter) / float64(total) * 100

	if removedPct < 90 {
		return nil
	}

	severity := Warning
	if removedPct > 99 {
		severity = Critical
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Filter removed %.1f%% of rows (%d of %d) on %s", removedPct, node.RowsRemovedByFilter, total, nodeLabel(node)),
		Suggestion:  fmt.Sprintf("Add an index on %s to avoid scanning rows that will be filtered out", node.RelationName),
	}}
}

func checkNestedLoopLargeOuter(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.NodeType != "Nested Loop" {
		return nil
	}
	if len(node.Plans) == 0 {
		return nil
	}

	outerRows := node.Plans[0].ActualRows
	if outerRows == 0 {
		outerRows = node.Plans[0].PlanRows
	}

	if outerRows < 10000 {
		return nil
	}

	severity := Warning
	if outerRows > 100000 {
		severity = Critical
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Nested Loop with %d outer rows — inner side executes %d times", outerRows, outerRows),
		Suggestion:  "Consider if a Hash Join or Merge Join would be more efficient; check join conditions and indexes on inner table",
	}}
}

func checkWorkerMismatch(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
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

func checkLargeJoinFilterRemoval(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.RowsRemovedByJoinFilter == 0 {
		return nil
	}

	if node.RowsRemovedByJoinFilter < 10000 {
		return nil
	}

	severity := Warning
	if node.RowsRemovedByJoinFilter > 1000000 {
		severity = Critical
	}

	return []Finding{{
		Severity:    severity,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Join filter removed %d rows on %s", node.RowsRemovedByJoinFilter, nodeLabel(node)),
		Suggestion:  "Move filter condition into the join clause or add an index to reduce the join input",
	}}
}

func checkBitmapRecheck(node *plan.PlanNode, parent *plan.PlanNode) []Finding {
	if node.NodeType != "Bitmap Heap Scan" || node.RowsRemovedByIndexRecheck == 0 {
		return nil
	}
	total := node.ActualRows + node.RowsRemovedByIndexRecheck
	recheckPct := float64(node.RowsRemovedByIndexRecheck) / float64(total) * 100
	if recheckPct < 50 {
		return nil
	}
	return []Finding{{
		Severity:    Warning,
		NodeType:    node.NodeType,
		Relation:    node.RelationName,
		Description: fmt.Sprintf("Bitmap recheck removed %.1f%% of rows on %s — lossy pages", recheckPct, nodeLabel(node)),
		Suggestion:  "Increase effective_cache_size or work_mem to reduce lossy bitmap pages",
	}}
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
