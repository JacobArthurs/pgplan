/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"
)

var Version = "dev"

func init() {
	if Version == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "(devel)" {
			Version = info.Main.Version
		}
	}
	rootCmd.Version = Version
}

var rootCmd = &cobra.Command{
	Use:          "pgplan",
	SilenceUsage: true,
	Short:        "Analyze and compare PostgreSQL query plans",
	Long: `pgplan is a CLI tool for analyzing and comparing PostgreSQL EXPLAIN plans.

It provides actionable optimization insights without requiring a browser.
Supports SQL, and JSON input formats.`,
	Example: `  # Analyze a single query
  pgplan analyze query.sql
  
  # Compare two plans
  pgplan compare old.sql new.sql
  
  # Setup connection profiles
  pgplan init`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
