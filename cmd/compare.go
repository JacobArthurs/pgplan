/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"fmt"
	"pgplan/internal/plan"

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
		profile, _ := cmd.Flags().GetString("profile")
		format, _ := cmd.Flags().GetString("format")

		if profile != "" {
			return fmt.Errorf("TODO: Implement profile selection")
		}

		if format != "text" && format != "json" {
			return fmt.Errorf("invalid output format %q: must be \"text\" or \"json\"", format)
		}

		var oldFile string
		if len(args) > 0 {
			oldFile = args[0]
		}

		var newFile string
		if len(args) > 1 {
			newFile = args[1]
		}

		oldPlanOutput, err := plan.Resolve(oldFile, db, "old plan ")
		if err != nil {
			return err
		}

		newPlanOutput, err := plan.Resolve(newFile, db, "new plan ")
		if err != nil {
			return err
		}

		fmt.Printf("Old plan: %+v\n", oldPlanOutput)
		fmt.Printf("New plan: %+v\n", newPlanOutput)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
	compareCmd.Flags().StringP("db", "d", "", "PostgreSQL connection string")
	compareCmd.Flags().StringP("profile", "p", "", "Use named profile from config")
	compareCmd.Flags().StringP("format", "f", "text", "Output format: text, json")
	compareCmd.MarkFlagsMutuallyExclusive("db", "profile")
}
