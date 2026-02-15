/*
Copyright © 2026 JACOB ARTHURS
*/
package cmd

import (
	"fmt"
	"pgplan/internal/profile"

	"github.com/spf13/cobra"
)

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Manage saved connection profiles",
	Long:  `Manage saved PostgreSQL connection profiles so you don't have to specify a connection string every time.`,
}

var profileListCmd = &cobra.Command{
	Use:   "list",
	Short: "List saved profiles",
	Example: `  pgplan profile list
  pgplan profile list --show`,
	RunE: func(cmd *cobra.Command, args []string) error {
		show, _ := cmd.Flags().GetBool("show")

		profiles, err := profile.List()
		if err != nil {
			return err
		}

		if len(profiles) == 0 {
			fmt.Println("No profiles configured. Run 'pgplan profile add <name> <conn_str>' to create one.")
			return nil
		}

		for _, p := range profiles {
			if show {
				fmt.Printf("  %s\t%s\n", p.Name, p.ConnStr)
			} else {
				fmt.Printf("  %s\n", p.Name)
			}
		}
		return nil
	},
}

var profileAddCmd = &cobra.Command{
	Use:     "add <name> <conn_str>",
	Short:   "Add or update a connection profile",
	Example: `  pgplan profile add prod "postgres://user:pass@host:5432/db"`,
	Args:    cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := profile.Add(args[0], args[1]); err != nil {
			return err
		}
		fmt.Printf("Profile %q saved.\n", args[0])
		return nil
	},
}

var profileRemoveCmd = &cobra.Command{
	Use:     "remove <name>",
	Aliases: []string{"rm"},
	Short:   "Remove a connection profile",
	Example: `  pgplan profile remove prod`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := profile.Remove(args[0]); err != nil {
			return err
		}
		fmt.Printf("Profile %q removed.\n", args[0])
		return nil
	},
}

var profileDefaultCmd = &cobra.Command{
	Use:     "default <name>",
	Short:   "Set the default profile",
	Example: `  pgplan profile default prod`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := profile.SetDefault(args[0]); err != nil {
			return err
		}
		fmt.Printf("Default profile set to %q.\n", args[0])
		return nil
	},
}

var profileClearDefaultCmd = &cobra.Command{
	Use:     "clear-default",
	Short:   "Clear the default profile",
	Example: `  pgplan profile clear-default`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := profile.ClearDefault(); err != nil {
			return err
		}
		fmt.Println("Default profile cleared.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(profileCmd)
	profileCmd.AddCommand(profileListCmd)
	profileCmd.AddCommand(profileAddCmd)
	profileCmd.AddCommand(profileRemoveCmd)
	profileCmd.AddCommand(profileDefaultCmd)
	profileCmd.AddCommand(profileClearDefaultCmd)
	profileListCmd.Flags().BoolP("show", "s", false, "Show connection strings")
}
