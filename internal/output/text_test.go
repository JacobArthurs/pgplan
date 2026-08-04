package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jacobarthurs/pgplan/internal/analyzer"
	"github.com/jacobarthurs/pgplan/internal/comparator"
	"github.com/jacobarthurs/pgplan/internal/plan"
)

func TestFormatIntDelta_LowerIsBetter_Decrease(t *testing.T) {
	got := formatIntDelta(1000, 100, true)
	if !strings.Contains(got, "↓") {
		t.Errorf("formatIntDelta(1000, 100, true) = %q, want a ↓ arrow", got)
	}
	if !strings.Contains(got, colorGreen) {
		t.Errorf("formatIntDelta(1000, 100, true) = %q, want colorGreen (decrease is an improvement)", got)
	}
	if !strings.Contains(got, "-90.0%") {
		t.Errorf("formatIntDelta(1000, 100, true) = %q, want -90.0%% pct", got)
	}
}

func TestFormatIntDelta_LowerIsBetter_Increase(t *testing.T) {
	got := formatIntDelta(100, 1000, true)
	if !strings.Contains(got, "↑") {
		t.Errorf("formatIntDelta(100, 1000, true) = %q, want an ↑ arrow", got)
	}
	if !strings.Contains(got, colorRed) {
		t.Errorf("formatIntDelta(100, 1000, true) = %q, want colorRed (increase is a regression)", got)
	}
}

func TestFormatIntDelta_HigherIsBetter_Increase(t *testing.T) {
	// e.g. cache hits: going up is an improvement, and the arrow must track
	// the actual numeric direction (↑), not the "improved" direction.
	got := formatIntDelta(10, 600, false)
	if !strings.Contains(got, "↑") {
		t.Errorf("formatIntDelta(10, 600, false) = %q, want an ↑ arrow tracking the increase", got)
	}
	if !strings.Contains(got, colorGreen) {
		t.Errorf("formatIntDelta(10, 600, false) = %q, want colorGreen (increase is an improvement)", got)
	}
}

func TestFormatIntDelta_HigherIsBetter_Decrease(t *testing.T) {
	got := formatIntDelta(600, 10, false)
	if !strings.Contains(got, "↓") {
		t.Errorf("formatIntDelta(600, 10, false) = %q, want a ↓ arrow tracking the decrease", got)
	}
	if !strings.Contains(got, colorRed) {
		t.Errorf("formatIntDelta(600, 10, false) = %q, want colorRed (decrease is a regression)", got)
	}
}

func TestFormatIntDelta_NoChange(t *testing.T) {
	got := formatIntDelta(42, 42, true)
	if strings.Contains(got, colorGreen) || strings.Contains(got, colorRed) {
		t.Errorf("formatIntDelta(42, 42, true) = %q, want no color when unchanged", got)
	}
	if !strings.Contains(got, "+0.0%") {
		t.Errorf("formatIntDelta(42, 42, true) = %q, want +0.0%% pct", got)
	}
}

func TestFormatDurationDelta(t *testing.T) {
	cases := []struct {
		deltaMs float64
		want    string
	}{
		{0, "+00:00:00.000"},
		{49, "+00:00:00.049"},
		{-49, "-00:00:00.049"},
		{1_500, "+00:00:01.500"},
		{61_000, "+00:01:01.000"},
		{3_661_234, "+01:01:01.234"},
	}

	for _, c := range cases {
		if got := formatDurationDelta(c.deltaMs); got != c.want {
			t.Errorf("formatDurationDelta(%v) = %q, want %q", c.deltaMs, got, c.want)
		}
	}
}

func TestRenderComparisonText_ExecutionTimeIncludesDurationDelta(t *testing.T) {
	result := comparator.ComparisonResult{
		Deltas: []comparator.NodeDelta{{ChangeType: comparator.NoChange}},
		Summary: comparator.Summary{
			OldExecutionTime: 55.0,
			NewExecutionTime: 6.0,
			TimeDelta:        6.0 - 55.0,
			TimePct:          -89.09090909090909,
			TimeDir:          comparator.Improved,
		},
	}

	var buf bytes.Buffer
	if err := RenderComparisonText(&buf, result, plan.DefaultBlockSize); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "-00:00:00.049") {
		t.Errorf("output missing duration delta -00:00:00.049\nfull output:\n%s", out)
	}
	if !strings.Contains(out, "-89.1%") {
		t.Errorf("output missing percentage -89.1%%\nfull output:\n%s", out)
	}
}

func TestRenderComparisonText_BufferSummaryIncludesPercentages(t *testing.T) {
	result := comparator.ComparisonResult{
		Deltas: []comparator.NodeDelta{{ChangeType: comparator.NoChange}},
		Summary: comparator.Summary{
			OldBuffers: plan.NodeBuffers{
				Shared: plan.BlockCounts{Read: 800, Hit: 10},
				Local:  plan.BlockCounts{Read: 20},
				Temp:   plan.BlockCounts{Read: 100},
			},
			NewBuffers: plan.NodeBuffers{
				Shared: plan.BlockCounts{Read: 10, Hit: 600},
			},
			OldSortSpaceUsed: 4096,
			NewSortSpaceUsed: 64,
		},
	}

	var buf bytes.Buffer
	if err := RenderComparisonText(&buf, result, plan.DefaultBlockSize); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"I/O Read:", "-98.9%", // total: 920 -> 10
		"-98.8%",                 // shared read: 800 -> 10
		"-100.0%",                // local read: 20 -> 0 (also matches temp read: 100 -> 0)
		"Cache Hit:", "+5900.0%", // total and shared hit: 10 -> 600
		"Sort Volume:", "-98.4%",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestRenderAnalysisText_ActualRows_ShownWhenAnalyzed(t *testing.T) {
	result := analyzer.AnalysisResult{
		TotalCost:     100.0,
		HasActualRows: true,
		ActualRows:    1234,
		Findings: []analyzer.Finding{
			{
				Severity:      analyzer.Warning,
				Description:   "some finding",
				Suggestion:    "some suggestion",
				HasActualRows: true,
				ActualRows:    56,
			},
		},
	}

	var buf bytes.Buffer
	if err := RenderAnalysisText(&buf, result, plan.DefaultBlockSize); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "Actual Rows:    1234") {
		t.Errorf("output missing summary Actual Rows line\nfull output:\n%s", out)
	}
	if !strings.Contains(out, "(actual rows: 56)") {
		t.Errorf("output missing finding-level actual rows\nfull output:\n%s", out)
	}
}

func TestRenderAnalysisText_ActualRows_HiddenWithoutAnalyze(t *testing.T) {
	result := analyzer.AnalysisResult{
		TotalCost: 100.0,
		Findings: []analyzer.Finding{
			{
				Severity:    analyzer.Warning,
				Description: "some finding",
				Suggestion:  "some suggestion",
			},
		},
	}

	var buf bytes.Buffer
	if err := RenderAnalysisText(&buf, result, plan.DefaultBlockSize); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()

	if strings.Contains(out, "Actual Rows") {
		t.Errorf("output should not mention Actual Rows without ANALYZE data\nfull output:\n%s", out)
	}
	if strings.Contains(out, "actual rows:") {
		t.Errorf("finding should not mention actual rows without ANALYZE data\nfull output:\n%s", out)
	}
}
