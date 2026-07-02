package output

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/jacobarthurs/pgplan/internal/analyzer"
	"github.com/jacobarthurs/pgplan/internal/comparator"
	"github.com/jacobarthurs/pgplan/internal/plan"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

type textWriter struct {
	w         io.Writer
	err       error
	blockSize int64
}

func (tw *textWriter) printf(format string, args ...any) {
	if tw.err != nil {
		return
	}
	_, tw.err = fmt.Fprintf(tw.w, format, args...)
}

// bytesOf renders blocks as a human-readable size using tw.blockSize
// (falling back to plan.DefaultBlockSize when unset).
func (tw *textWriter) bytesOf(blocks int64) string {
	blockSize := tw.blockSize
	if blockSize <= 0 {
		blockSize = plan.DefaultBlockSize
	}
	return plan.FormatBytes(blocks * blockSize)
}

// RenderAnalysisText renders result as human-readable text. blockSize is the
// PostgreSQL page size (bytes) used to show block counts as human-readable
// sizes; pass <= 0 to use plan.DefaultBlockSize.
func RenderAnalysisText(w io.Writer, result analyzer.AnalysisResult, blockSize int64) error {
	tw := &textWriter{w: w, blockSize: blockSize}

	tw.printf("%s%sPlan Summary%s\n\n", colorBold, colorCyan, colorReset)
	tw.printf("  Total Cost:     %.2f\n", result.TotalCost)
	if result.HasActualRows {
		tw.printf("  Actual Rows:    %s\n", formatCount(result.ActualRows))
	}
	if result.ExecutionTime > 0 {
		tw.printf("  Execution Time: %.3f ms\n", result.ExecutionTime)
	}
	if result.PlanningTime > 0 {
		tw.printf("  Planning Time:  %.3f ms\n", result.PlanningTime)
	}
	tw.renderBufferSummary(result.Buffers, result.SortSpaceUsed)
	tw.printf("\n")

	if len(result.Findings) == 0 {
		tw.printf("%s%sNo issues found.%s\n", colorBold, colorGreen, colorReset)
		return tw.err
	}

	tw.printf("%s%sFindings (%d)%s\n\n", colorBold, colorCyan, len(result.Findings), colorReset)

	for i, f := range result.Findings {
		label, color := severityFormat(f.Severity)
		tw.printf("  %s%-8s%s %s%s", color, label, colorReset, f.Description, colorReset)
		if f.HasActualRows {
			tw.printf(" %s(actual rows: %s)%s", colorDim, formatCount(f.ActualRows), colorReset)
		}
		tw.printf("\n")
		tw.printf("  %s→ %s%s\n", colorDim, f.Suggestion, colorReset)
		if i < len(result.Findings)-1 {
			tw.printf("\n")
		}
	}

	return tw.err
}

func (tw *textWriter) renderBufferSummary(b plan.NodeBuffers, sortSpaceUsed int64) {
	if bufferTotal(b) > 0 {
		tw.printf("  I/O Read:       %d blocks (%s) (shared %d, local %d, temp %d)\n",
			b.TotalRead(), tw.bytesOf(b.TotalRead()), b.Shared.Read, b.Local.Read, b.Temp.Read)
		tw.printf("  I/O Write:      %d blocks (%s) (shared %d, local %d, temp %d)\n",
			b.TotalWritten(), tw.bytesOf(b.TotalWritten()), b.Shared.Written, b.Local.Written, b.Temp.Written)
		tw.printf("  Cache Hit:      %d blocks (%s) (shared %d, local %d)\n",
			b.TotalHit(), tw.bytesOf(b.TotalHit()), b.Shared.Hit, b.Local.Hit)
		if b.TotalDirtied() > 0 {
			tw.printf("  Dirtied:        %d blocks (%s) (shared %d, local %d)\n",
				b.TotalDirtied(), tw.bytesOf(b.TotalDirtied()), b.Shared.Dirtied, b.Local.Dirtied)
		}
	}
	if sortSpaceUsed > 0 {
		tw.printf("  Sort Volume:    %d kB\n", sortSpaceUsed)
	}
}

func bufferTotal(b plan.NodeBuffers) int64 {
	return b.TotalRead() + b.TotalWritten() + b.TotalHit() + b.TotalDirtied()
}

func (tw *textWriter) renderBufferSummaryDelta(oldB, newB plan.NodeBuffers, oldSort, newSort int64) {
	if bufferTotal(oldB) > 0 || bufferTotal(newB) > 0 {
		tw.printf("  I/O Read:       %s blocks (%s → %s) (shared %s, local %s, temp %s)\n",
			formatIntDelta(oldB.TotalRead(), newB.TotalRead(), true),
			tw.bytesOf(oldB.TotalRead()), tw.bytesOf(newB.TotalRead()),
			formatIntDelta(oldB.Shared.Read, newB.Shared.Read, true),
			formatIntDelta(oldB.Local.Read, newB.Local.Read, true),
			formatIntDelta(oldB.Temp.Read, newB.Temp.Read, true))
		tw.printf("  I/O Write:      %s blocks (%s → %s) (shared %s, local %s, temp %s)\n",
			formatIntDelta(oldB.TotalWritten(), newB.TotalWritten(), true),
			tw.bytesOf(oldB.TotalWritten()), tw.bytesOf(newB.TotalWritten()),
			formatIntDelta(oldB.Shared.Written, newB.Shared.Written, true),
			formatIntDelta(oldB.Local.Written, newB.Local.Written, true),
			formatIntDelta(oldB.Temp.Written, newB.Temp.Written, true))
		tw.printf("  Cache Hit:      %s blocks (%s → %s) (shared %s, local %s)\n",
			formatIntDelta(oldB.TotalHit(), newB.TotalHit(), false),
			tw.bytesOf(oldB.TotalHit()), tw.bytesOf(newB.TotalHit()),
			formatIntDelta(oldB.Shared.Hit, newB.Shared.Hit, false),
			formatIntDelta(oldB.Local.Hit, newB.Local.Hit, false))
		if oldB.TotalDirtied() > 0 || newB.TotalDirtied() > 0 {
			tw.printf("  Dirtied:        %s blocks (%s → %s) (shared %s, local %s)\n",
				formatIntDelta(oldB.TotalDirtied(), newB.TotalDirtied(), true),
				tw.bytesOf(oldB.TotalDirtied()), tw.bytesOf(newB.TotalDirtied()),
				formatIntDelta(oldB.Shared.Dirtied, newB.Shared.Dirtied, true),
				formatIntDelta(oldB.Local.Dirtied, newB.Local.Dirtied, true))
		}
	}
	if oldSort > 0 || newSort > 0 {
		tw.printf("  Sort Volume:    %s kB\n", formatIntDelta(oldSort, newSort, true))
	}
}

func severityFormat(s analyzer.Severity) (string, string) {
	switch s {
	case analyzer.Critical:
		return "CRITICAL", colorRed
	case analyzer.Warning:
		return "WARNING", colorYellow
	default:
		return "INFO", colorCyan
	}
}

// RenderComparisonText renders result as human-readable text. blockSize is
// the PostgreSQL page size (bytes) used to show block counts as
// human-readable sizes; pass <= 0 to use plan.DefaultBlockSize.
func RenderComparisonText(w io.Writer, result comparator.ComparisonResult, blockSize int64) error {
	tw := &textWriter{w: w, blockSize: blockSize}
	s := result.Summary

	tw.printf("%s%sSummary%s\n\n", colorBold, colorCyan, colorReset)
	tw.printf("  Cost:           %s\n", formatDelta(s.OldTotalCost, s.NewTotalCost, s.CostPct, s.CostDir, "%.2f"))
	if s.OldExecutionTime > 0 || s.NewExecutionTime > 0 {
		tw.printf("  Execution Time: %s (%s)\n",
			formatDelta(s.OldExecutionTime, s.NewExecutionTime, s.TimePct, s.TimeDir, "%.3f ms"),
			formatDurationDelta(s.TimeDelta))
	}
	if s.OldPlanningTime > 0 || s.NewPlanningTime > 0 {
		tw.printf("  Planning Time:  %s\n", formatDelta(s.OldPlanningTime, s.NewPlanningTime, pctChange(s.OldPlanningTime, s.NewPlanningTime), s.PlanningDir, "%.3f ms"))
	}
	tw.renderBufferSummaryDelta(s.OldBuffers, s.NewBuffers, s.OldSortSpaceUsed, s.NewSortSpaceUsed)
	tw.printf("\n")

	if changes := s.NodesAdded + s.NodesRemoved + s.NodesModified + s.NodesTypeChanged; changes == 0 {
		tw.printf("%s%sPlans are identical.%s\n", colorBold, colorGreen, colorReset)
		return tw.err
	}

	tw.printf("  Changes: %d modified, %d type changed, %d added, %d removed\n\n",
		s.NodesModified, s.NodesTypeChanged, s.NodesAdded, s.NodesRemoved)

	tw.printf("%s%sNode Details%s\n\n", colorBold, colorCyan, colorReset)

	for _, delta := range result.Deltas {
		tw.renderDelta(delta, 0)
	}

	tw.renderVerdict(s)

	return tw.err
}

func (tw *textWriter) renderDelta(d comparator.NodeDelta, depth int) {

	switch indent := strings.Repeat("  ", depth+1); d.ChangeType {
	case comparator.NoChange:
		for _, child := range d.Children {
			tw.renderDelta(child, depth)
		}
		return
	case comparator.Added:
		tw.renderAddedNode(indent, d)
	case comparator.Removed:
		tw.renderRemovedNode(indent, d)
	case comparator.TypeChanged:
		tw.renderTypeChangedNode(indent, d)
	case comparator.Modified:
		tw.renderModifiedNode(indent, d)
	}

	for _, child := range d.Children {
		tw.renderDelta(child, depth+1)
	}
}

func (tw *textWriter) renderAddedNode(indent string, d comparator.NodeDelta) {
	tw.printf("%s%s+ %s%s", indent, colorGreen, nodeLabel(d), colorReset)
	tw.printf(" (cost=%.2f", d.NewCost)
	if d.NewTime > 0 {
		tw.printf(" time=%.3fms", d.NewTime)
	}
	tw.printf(")\n")
}

func (tw *textWriter) renderRemovedNode(indent string, d comparator.NodeDelta) {
	tw.printf("%s%s- %s%s", indent, colorRed, nodeLabel(d), colorReset)
	tw.printf(" (cost=%.2f", d.OldCost)
	if d.OldTime > 0 {
		tw.printf(" time=%.3fms", d.OldTime)
	}
	tw.printf(")\n")
}

func (tw *textWriter) renderTypeChangedNode(indent string, d comparator.NodeDelta) {
	tw.printf("%s%s~ %s → %s%s", indent, colorYellow, d.OldNodeType, d.NewNodeType, colorReset)
	if d.Relation != "" {
		tw.printf(" on %s", d.Relation)
	}
	tw.printf("\n")
	tw.renderMetricLine(indent, "cost", d.OldCost, d.NewCost, d.CostPct, d.CostDir, "%.2f")
	if d.OldTime > 0 || d.NewTime > 0 {
		tw.renderMetricLine(indent, "time", d.OldTime, d.NewTime, d.TimePct, d.TimeDir, "%.3f ms")
	}
	if d.OldRows != d.NewRows {
		tw.renderMetricLineCount(indent, "rows", d.OldRows, d.NewRows, d.RowsPct)
	}
	tw.renderFilterChange(indent, d)
	tw.renderIndexCondChange(indent, d)
	tw.renderIndexNameChange(indent, d)
	tw.renderBufferChanges(indent, d)
	tw.renderSpillChanges(indent, d)
}

func (tw *textWriter) renderModifiedNode(indent string, d comparator.NodeDelta) {
	tw.printf("%s%s~ %s%s\n", indent, colorYellow, nodeLabel(d), colorReset)
	tw.renderMetricLine(indent, "cost", d.OldCost, d.NewCost, d.CostPct, d.CostDir, "%.2f")
	if d.OldTime > 0 || d.NewTime > 0 {
		tw.renderMetricLine(indent, "time", d.OldTime, d.NewTime, d.TimePct, d.TimeDir, "%.3f ms")
	}
	if d.OldRows != d.NewRows {
		tw.renderMetricLineCount(indent, "rows", d.OldRows, d.NewRows, d.RowsPct)
	}
	if d.OldLoops != d.NewLoops && (d.OldLoops > 1 || d.NewLoops > 1) {
		tw.renderMetricLineInt(indent, "loops", d.OldLoops, d.NewLoops,
			pctChange(float64(d.OldLoops), float64(d.NewLoops)))
	}
	if d.OldRowsRemovedByFilter != d.NewRowsRemovedByFilter {
		tw.renderMetricLineInt(indent, "rows removed by filter",
			d.OldRowsRemovedByFilter, d.NewRowsRemovedByFilter,
			pctChange(float64(d.OldRowsRemovedByFilter), float64(d.NewRowsRemovedByFilter)))
	}
	if d.OldWorkersLaunched != d.NewWorkersLaunched {
		tw.printf("%s  workers: %d/%d → %d/%d\n", indent,
			d.OldWorkersLaunched, d.OldWorkersPlanned,
			d.NewWorkersLaunched, d.NewWorkersPlanned)
	}
	tw.renderFilterChange(indent, d)
	tw.renderIndexCondChange(indent, d)
	tw.renderIndexNameChange(indent, d)
	tw.renderBufferChanges(indent, d)
	tw.renderSpillChanges(indent, d)
}

func (tw *textWriter) renderMetricLine(indent, label string, oldVal, newVal, pct float64, dir comparator.Direction, fmtStr string) {
	color := dirColor(dir)
	arrow := dirArrow(dir)
	oldStr := fmt.Sprintf(fmtStr, oldVal)
	newStr := fmt.Sprintf(fmtStr, newVal)
	tw.printf("%s  %s: %s → %s%s %s (%+.1f%%)%s\n", indent, label, oldStr, color, newStr, arrow, pct, colorReset)
}

func (tw *textWriter) renderMetricLineInt(indent, label string, oldVal, newVal int64, pct float64) {
	tw.printf("%s  %s: %d → %d (%+.1f%%)\n", indent, label, oldVal, newVal, pct)
}

// renderMetricLineCount formats a row/count metric that may be fractional
// (PostgreSQL averages some "actual" counts across loop iterations).
func (tw *textWriter) renderMetricLineCount(indent, label string, oldVal, newVal, pct float64) {
	tw.printf("%s  %s: %s → %s (%+.1f%%)\n", indent, label, formatCount(oldVal), formatCount(newVal), pct)
}

func formatCount(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%.2f", v)
}

func (tw *textWriter) renderFilterChange(indent string, d comparator.NodeDelta) {
	if d.OldFilter == d.NewFilter {
		return
	}
	if d.OldFilter == "" {
		tw.printf("%s  %sfilter added: %s%s\n", indent, colorYellow, d.NewFilter, colorReset)
	} else if d.NewFilter == "" {
		tw.printf("%s  %sfilter removed: %s%s\n", indent, colorGreen, d.OldFilter, colorReset)
	} else {
		tw.printf("%s  %sfilter: %s → %s%s\n", indent, colorYellow, d.OldFilter, d.NewFilter, colorReset)
	}
}

func (tw *textWriter) renderIndexCondChange(indent string, d comparator.NodeDelta) {
	if d.OldIndexCond == d.NewIndexCond {
		return
	}
	if d.OldIndexCond == "" {
		tw.printf("%s  %sindex added: %s%s\n", indent, colorYellow, d.NewIndexCond, colorReset)
	} else if d.NewIndexCond == "" {
		tw.printf("%s  %sindex removed: %s%s\n", indent, colorGreen, d.OldIndexCond, colorReset)
	} else {
		tw.printf("%s  %sindex: %s → %s%s\n", indent, colorYellow, d.OldIndexCond, d.NewIndexCond, colorReset)
	}
}

func (tw *textWriter) renderBufferChanges(indent string, d comparator.NodeDelta) {
	if d.OldBufferReads != d.NewBufferReads {
		color := colorGreen
		arrow := "↓"
		if d.NewBufferReads > d.OldBufferReads {
			color = colorRed
			arrow = "↑"
		}
		tw.printf("%s  disk reads: %d → %s%d %s%s (%s → %s)\n",
			indent, d.OldBufferReads, color, d.NewBufferReads, arrow, colorReset,
			tw.bytesOf(d.OldBufferReads), tw.bytesOf(d.NewBufferReads))
	}
	if d.OldBufferHits != d.NewBufferHits {
		tw.printf("%s  cache hits: %d → %d (%s → %s)\n", indent, d.OldBufferHits, d.NewBufferHits,
			tw.bytesOf(d.OldBufferHits), tw.bytesOf(d.NewBufferHits))
	}
	tw.renderBlockCountsChange(indent, "shared blocks", d.OldBuffers.Shared, d.NewBuffers.Shared, true)
	tw.renderBlockCountsChange(indent, "local blocks", d.OldBuffers.Local, d.NewBuffers.Local, true)
	tw.renderBlockCountsChange(indent, "temp blocks", d.OldBuffers.Temp, d.NewBuffers.Temp, false)
}

func (tw *textWriter) renderBlockCountsChange(indent, label string, oldC, newC plan.BlockCounts, showHitDirtied bool) {
	if oldC == newC {
		return
	}
	tw.printf("%s  %s: read %d→%d, written %d→%d", indent, label, oldC.Read, newC.Read, oldC.Written, newC.Written)
	if showHitDirtied {
		tw.printf(", hit %d→%d, dirtied %d→%d", oldC.Hit, newC.Hit, oldC.Dirtied, newC.Dirtied)
	}
	tw.printf("\n")
}

func (tw *textWriter) renderSpillChanges(indent string, d comparator.NodeDelta) {
	if d.OldSortSpill != d.NewSortSpill {
		if d.NewSortSpill {
			tw.printf("%s  %ssort: memory → disk ↑%s\n", indent, colorRed, colorReset)
		} else {
			tw.printf("%s  %ssort: disk → memory ↓%s\n", indent, colorGreen, colorReset)
		}
	}
	if d.OldHashBatches != d.NewHashBatches {
		color, arrow := deltaIndicator(int64(d.OldHashBatches), int64(d.NewHashBatches))
		tw.printf("%s  hash batches: %d → %s%d %s%s\n", indent, d.OldHashBatches, color, d.NewHashBatches, arrow, colorReset)
	}
	if d.OldSortSpaceUsed != d.NewSortSpaceUsed {
		color, arrow := deltaIndicator(d.OldSortSpaceUsed, d.NewSortSpaceUsed)
		tw.printf("%s  sort volume: %d kB → %s%d kB %s%s\n", indent, d.OldSortSpaceUsed, color, d.NewSortSpaceUsed, arrow, colorReset)
	}
}

func deltaIndicator(oldVal, newVal int64) (string, string) {
	if newVal > oldVal {
		return colorRed, "↑"
	}
	return colorGreen, "↓"
}

func formatDelta(oldVal, newVal, pct float64, dir comparator.Direction, fmtStr string) string {
	color := dirColor(dir)
	arrow := dirArrow(dir)
	oldStr := fmt.Sprintf(fmtStr, oldVal)
	newStr := fmt.Sprintf(fmtStr, newVal)
	return fmt.Sprintf("%s → %s%s %s (%+.1f%%)%s", oldStr, color, newStr, arrow, pct, colorReset)
}

// formatIntDelta renders an old→new block/kB count with the same
// "value → colored value arrow (+pct%)" shape as formatDelta. Unlike
// dirArrow, the arrow always tracks the actual numeric direction (↓ for a
// decrease, ↑ for an increase); lowerIsBetter only controls whether that
// direction is colored as an improvement (reads, writes, sort volume) or a
// regression (cache hits).
func formatIntDelta(oldVal, newVal int64, lowerIsBetter bool) string {
	pct := pctChange(float64(oldVal), float64(newVal))

	if oldVal == newVal {
		return fmt.Sprintf("%d → %d (%+.1f%%)", oldVal, newVal, pct)
	}

	arrow := "↑"
	improved := !lowerIsBetter
	if newVal < oldVal {
		arrow = "↓"
		improved = lowerIsBetter
	}
	color := colorRed
	if improved {
		color = colorGreen
	}

	return fmt.Sprintf("%d → %s%d %s (%+.1f%%)%s", oldVal, color, newVal, arrow, pct, colorReset)
}

func dirColor(d comparator.Direction) string {
	switch d {
	case comparator.Improved:
		return colorGreen
	case comparator.Regressed:
		return colorRed
	default:
		return ""
	}
}

func dirArrow(d comparator.Direction) string {
	switch d {
	case comparator.Improved:
		return "↓"
	case comparator.Regressed:
		return "↑"
	default:
		return ""
	}
}

func pctChange(old, new float64) float64 {
	if old == 0 {
		if new == 0 {
			return 0
		}
		return 100
	}
	return ((new - old) / old) * 100
}

// formatDurationDelta renders a millisecond delta as a signed hh:mm:ss.ms
// duration, e.g. "-00:00:00.049".
func formatDurationDelta(deltaMs float64) string {
	sign := "+"
	if deltaMs < 0 {
		sign = "-"
		deltaMs = -deltaMs
	}

	totalMs := int64(math.Round(deltaMs))
	hh := totalMs / 3_600_000
	mm := (totalMs % 3_600_000) / 60_000
	ss := (totalMs % 60_000) / 1_000
	ms := totalMs % 1_000

	return fmt.Sprintf("%s%02d:%02d:%02d.%03d", sign, hh, mm, ss, ms)
}

func (tw *textWriter) renderIndexNameChange(indent string, d comparator.NodeDelta) {
	if d.OldIndexName == d.NewIndexName {
		return
	}
	if d.OldIndexName == "" {
		tw.printf("%s  %sindex added: %s%s\n", indent, colorGreen, d.NewIndexName, colorReset)
	} else if d.NewIndexName == "" {
		tw.printf("%s  %sindex removed: %s%s\n", indent, colorRed, d.OldIndexName, colorReset)
	} else {
		tw.printf("%s  %sindex: %s → %s%s\n", indent, colorYellow, d.OldIndexName, d.NewIndexName, colorReset)
	}
}

func (tw *textWriter) renderVerdict(s comparator.Summary) {
	var color string
	switch {
	case s.TimeDir == comparator.Improved && s.CostDir == comparator.Improved:
		color = colorGreen
	case s.TimeDir == comparator.Regressed && s.CostDir == comparator.Regressed:
		color = colorRed
	case s.TimeDir == comparator.Improved || s.CostDir == comparator.Improved:
		color = colorYellow
	}
	if color != "" {
		tw.printf("\n%sVerdict: %s%s\n", color, s.Verdict, colorReset)
	} else {
		tw.printf("\nVerdict: %s\n", s.Verdict)
	}
}

func nodeLabel(d comparator.NodeDelta) string {
	if d.Relation != "" {
		return fmt.Sprintf("%s on %s", d.NodeType, d.Relation)
	}
	return d.NodeType
}
