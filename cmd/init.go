/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Create config file with example template",
	Long: `Create ~/.config/pgplan/config.yml with an example template.

The config file stores named database connection profiles so you don't need
to pass connection strings on every invocation. If a config file already exists,
it will not be overwritten.`,
	Example: `  # Create default config
  pgplan init

  # Overwrite existing config
  pgplan init --force`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		force, _ := cmd.Flags().GetBool("force")

		fmt.Printf("Force: %v\n", force)

		fmt.Println("Creating config at ~/.config/pgplan/config.yml")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().BoolP("force", "f", false, "Overwrite existing config file")
}
