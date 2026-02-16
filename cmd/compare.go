/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/jacobarthurs/pgplan/internal/comparator"
	"github.com/jacobarthurs/pgplan/internal/output"
	"github.com/jacobarthurs/pgplan/internal/plan"
	"github.com/jacobarthurs/pgplan/internal/profile"

	"github.com/spf13/cobra"
)

var compareCmd = &cobra.Command{
	Use:   "compare [file1] [file2]",
	Short: "Compare two query plans",
	Long: `Compare two PostgreSQL query plans side-by-side with semantic understanding.

Inputs can be SQL files, or JSON files (EXPLAIN output).
Files don't need to be the same type. Either file (but not both) can be "-" to read from stdin.
If no files are provided, enters interactive mode.

For SQL input, a database connection is required to run EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON).`,
	Example: `  # Compare two SQL files
  pgplan compare old.sql new.sql

  # Use saved profile
  pgplan compare old.sql new.sql

  # Mix input types
  pgplan compare prod-plan.json new-query.sql

  # Read one plan from stdin
  cat old.sql |  pgplan compare - new.sql

  # Interactive mode
  pgplan compare`,
	Args: cobra.MaximumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, _ := cmd.Flags().GetString("db")
		profileName, _ := cmd.Flags().GetString("profile")
		format, _ := cmd.Flags().GetString("format")
		threshold, _ := cmd.Flags().GetFloat64("threshold")

		if format != "text" && format != "json" {
			return fmt.Errorf("invalid output format %q: must be \"text\" or \"json\"", format)
		}

		if threshold < 0 {
			return fmt.Errorf("threshold must be non-negative, got %.2f", threshold)
		}

		if threshold > 100 {
			return fmt.Errorf("threshold must be <= 100%%, got %.2f", threshold)
		}

		connStr, err := profile.ResolveConnStr(db, profileName)
		if err != nil {
			return err
		}

		var oldFile string
		if len(args) > 0 {
			oldFile = args[0]
		}

		var newFile string
		if len(args) > 1 {
			newFile = args[1]
		}

		oldPlanOutput, err := plan.Resolve(oldFile, connStr, "old plan ")
		if err != nil {
			return err
		}

		newPlanOutput, err := plan.Resolve(newFile, connStr, "new plan ")
		if err != nil {
			return err
		}

		cmp := &comparator.Comparator{Threshold: threshold}
		result := cmp.Compare(oldPlanOutput, newPlanOutput)

		switch format {
		case "json":
			return output.RenderJSON(os.Stdout, result)
		case "text":
			return output.RenderComparisonText(os.Stdout, result)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
	compareCmd.Flags().StringP("db", "d", "", "PostgreSQL connection string")
	compareCmd.Flags().StringP("profile", "p", "", "Use named profile from config")
	compareCmd.Flags().StringP("format", "f", "text", "Output format: text, json")
	compareCmd.Flags().Float64P("threshold", "t", 5.0, "Percent change threshold for significance (default 5%)")
	compareCmd.MarkFlagsMutuallyExclusive("db", "profile")
}
