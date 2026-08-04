/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/jacobarthurs/pgplan/internal/analyzer"
	"github.com/jacobarthurs/pgplan/internal/output"
	"github.com/jacobarthurs/pgplan/internal/plan"
	"github.com/jacobarthurs/pgplan/internal/profile"

	"github.com/spf13/cobra"
)

var analyzeCmd = &cobra.Command{
	Use:   "analyze [file]",
	Short: "Analyze a single query plan",
	Long: `Analyze a single PostgreSQL query plan and provide optimization insights.

Input can be a SQL file, or JSON file (EXPLAIN output).
Use "-" to read from stdin. If no file is provided, enters interactive mode.

For SQL input, a database connection is required to run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON).`,
	Example: `  # Analyze from file
  pgplan analyze query.sql

  # Use saved profile
  pgplan analyze query.sql --profile prod

  # Read from stdin
  cat query.sql | pgplan analyze -

  # Interactive mode
  pgplan analyze`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, _ := cmd.Flags().GetString("db")
		profileName, _ := cmd.Flags().GetString("profile")
		format, _ := cmd.Flags().GetString("format")
		blockSize, _ := cmd.Flags().GetInt64("block-size")

		if format != "text" && format != "json" {
			return fmt.Errorf("invalid output format %q: must be \"text\" or \"json\"", format)
		}

		if blockSize <= 0 {
			return fmt.Errorf("block-size must be positive, got %d", blockSize)
		}

		connStr, err := profile.ResolveConnStr(db, profileName)
		if err != nil {
			return err
		}

		var file string
		if len(args) > 0 {
			file = args[0]
		}

		planOutput, err := plan.Resolve(file, connStr, "")
		if err != nil {
			return err
		}

		result := analyzer.Analyze(planOutput, blockSize)

		switch format {
		case "json":
			return output.RenderJSON(os.Stdout, result)
		case "text":
			return output.RenderAnalysisText(os.Stdout, result, blockSize)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
	analyzeCmd.Flags().StringP("db", "d", "", "PostgreSQL connection string")
	analyzeCmd.Flags().StringP("profile", "p", "", "Use named profile from config")
	analyzeCmd.Flags().StringP("format", "f", "text", "Output format: text, json")
	analyzeCmd.Flags().Int64("block-size", 8192, "PostgreSQL page size in bytes, used to show block counts as human-readable sizes")
	analyzeCmd.MarkFlagsMutuallyExclusive("db", "profile")
}
