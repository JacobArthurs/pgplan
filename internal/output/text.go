package output

import (
	"fmt"
	"io"
	"strings"

	"pgplan/internal/analyzer"
	"pgplan/internal/comparator"
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
	w   io.Writer
	err error
}

func (tw *textWriter) printf(format string, args ...any) {
	if tw.err != nil {
		return
	}
	_, tw.err = fmt.Fprintf(tw.w, format, args...)
}

func RenderAnalysisText(w io.Writer, result analyzer.AnalysisResult) error {
	tw := &textWriter{w: w}

	tw.printf("%s%sPlan Summary%s\n", colorBold, colorCyan, colorReset)
	tw.printf("  Total Cost:     %.2f\n", result.TotalCost)
	if result.ExecutionTime > 0 {
		tw.printf("  Execution Time: %.3f ms\n", result.ExecutionTime)
	}
	if result.PlanningTime > 0 {
		tw.printf("  Planning Time:  %.3f ms\n", result.PlanningTime)
	}
	tw.printf("\n")

	if len(result.Findings) == 0 {
		tw.printf("%s%sNo issues found.%s\n", colorBold, colorGreen, colorReset)
		return tw.err
	}

	tw.printf("%s%sFindings (%d)%s\n\n", colorBold, colorCyan, len(result.Findings), colorReset)

	for i, f := range result.Findings {
		label, color := severityFormat(f.Severity)
		tw.printf("  %s%-8s%s %s%s\n", color, label, colorReset, f.Description, colorReset)
		tw.printf("  %s→ %s%s\n", colorDim, f.Suggestion, colorReset)
		if i < len(result.Findings)-1 {
			tw.printf("\n")
		}
	}

	return tw.err
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

func RenderComparisonText(w io.Writer, result comparator.ComparisonResult) error {
	tw := &textWriter{w: w}
	s := result.Summary

	tw.printf("%s%sSummary%s\n", colorBold, colorCyan, colorReset)
	tw.printf("  Cost:           %s\n", formatDelta(s.OldTotalCost, s.NewTotalCost, s.CostPct, s.CostDir, "%.2f"))
	if s.OldExecutionTime > 0 || s.NewExecutionTime > 0 {
		tw.printf("  Execution Time: %s\n", formatDelta(s.OldExecutionTime, s.NewExecutionTime, s.TimePct, s.TimeDir, "%.3f ms"))
	}
	if s.OldPlanningTime > 0 || s.NewPlanningTime > 0 {
		tw.printf("  Planning Time:  %.3f ms → %.3f ms\n", s.OldPlanningTime, s.NewPlanningTime)
	}
	tw.printf("\n")

	changes := s.NodesAdded + s.NodesRemoved + s.NodesModified + s.NodesTypeChanged
	if changes == 0 {
		tw.printf("%s%sPlans are identical.%s\n", colorBold, colorGreen, colorReset)
		return tw.err
	}

	tw.printf("  Changes: %d modified, %d type changed, %d added, %d removed\n\n",
		s.NodesModified, s.NodesTypeChanged, s.NodesAdded, s.NodesRemoved)

	tw.printf("%s%sNode Details%s\n\n", colorBold, colorCyan, colorReset)

	for _, delta := range result.Deltas {
		tw.renderDelta(delta, 0)
	}

	return tw.err
}

func (tw *textWriter) renderDelta(d comparator.NodeDelta, depth int) {
	indent := strings.Repeat("  ", depth+1)

	switch d.ChangeType {
	case comparator.NoChange:
		for _, child := range d.Children {
			tw.renderDelta(child, depth)
		}
		return

	case comparator.Added:
		tw.printf("%s%s+ %s%s", indent, colorGreen, nodeLabel(d), colorReset)
		tw.printf(" (cost=%.2f", d.NewCost)
		if d.NewTime > 0 {
			tw.printf(" time=%.3fms", d.NewTime)
		}
		tw.printf(")\n")

	case comparator.Removed:
		tw.printf("%s%s- %s%s", indent, colorRed, nodeLabel(d), colorReset)
		tw.printf(" (cost=%.2f", d.OldCost)
		if d.OldTime > 0 {
			tw.printf(" time=%.3fms", d.OldTime)
		}
		tw.printf(")\n")

	case comparator.TypeChanged:
		tw.printf("%s%s~ %s → %s%s", indent, colorYellow, d.OldNodeType, d.NewNodeType, colorReset)
		if d.Relation != "" {
			tw.printf(" on %s", d.Relation)
		}
		tw.printf("\n")
		tw.renderMetricLine(indent, "cost", d.OldCost, d.NewCost, d.CostPct, d.CostDir, "%.2f")
		if d.OldTime > 0 || d.NewTime > 0 {
			tw.renderMetricLine(indent, "time", d.OldTime, d.NewTime, d.TimePct, d.TimeDir, "%.3f ms")
		}

	case comparator.Modified:
		tw.printf("%s%s~ %s%s\n", indent, colorYellow, nodeLabel(d), colorReset)
		tw.renderMetricLine(indent, "cost", d.OldCost, d.NewCost, d.CostPct, d.CostDir, "%.2f")
		if d.OldTime > 0 || d.NewTime > 0 {
			tw.renderMetricLine(indent, "time", d.OldTime, d.NewTime, d.TimePct, d.TimeDir, "%.3f ms")
		}
		if d.OldRows != d.NewRows {
			tw.renderMetricLineInt(indent, "rows", d.OldRows, d.NewRows, d.RowsPct)
		}
		tw.renderBufferChanges(indent, d)
		tw.renderSpillChanges(indent, d)
	}

	for _, child := range d.Children {
		tw.renderDelta(child, depth+1)
	}
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

func (tw *textWriter) renderBufferChanges(indent string, d comparator.NodeDelta) {
	if d.OldSharedRead != d.NewSharedRead {
		color, arrow := deltaIndicator(d.OldSharedRead, d.NewSharedRead)
		tw.printf("%s  shared read: %d → %s%d %s%s\n", indent, d.OldSharedRead, color, d.NewSharedRead, arrow, colorReset)
	}
	if d.OldTempBlocks != d.NewTempBlocks {
		color, arrow := deltaIndicator(d.OldTempBlocks, d.NewTempBlocks)
		tw.printf("%s  temp blocks: %d → %s%d %s%s\n", indent, d.OldTempBlocks, color, d.NewTempBlocks, arrow, colorReset)
	}
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

func nodeLabel(d comparator.NodeDelta) string {
	if d.Relation != "" {
		return fmt.Sprintf("%s on %s", d.NodeType, d.Relation)
	}
	return d.NodeType
}
