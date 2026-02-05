/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"fmt"

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
  pgplan compare old.sql new.sql --db "postgresql://user:pass@localhost/db"

  # Use saved profile
  pgplan compare old.sql new.sql --profile prod

  # Mix input types
  pgplan compare prod-plan.json new-query.sql --profile dev

  # Read one plan from stdin
  cat old.sql |  pgplan compare - new.sql --db "postgresql://user:pass@localhost/db"

  # Interactive mode
  pgplan compare`,
	Args: cobra.MaximumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, _ := cmd.Flags().GetString("db")
		profile, _ := cmd.Flags().GetString("profile")
		format, _ := cmd.Flags().GetString("format")

		if format != "text" && format != "json" {
			return fmt.Errorf("invalid output format %q: must be \"text\" or \"json\"", format)
		}

		if len(args) > 0 {
			fmt.Printf("File1: %s\n", args[0])
		}
		if len(args) > 1 {
			fmt.Printf("File2: %s\n", args[1])
		}
		if db != "" {
			fmt.Printf("DB: %s\n", db)
		}
		if profile != "" {
			fmt.Printf("Profile: %s\n", profile)
		}

		fmt.Printf("Format: %s\n", format)

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
