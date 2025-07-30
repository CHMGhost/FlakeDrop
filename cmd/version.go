package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// Version is set at build time
	Version = "dev"
	// BuildTime is set at build time
	BuildTime = "unknown"
)

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display FlakeDrop version information",
	Long:  `Display the current version of FlakeDrop along with build information.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("FlakeDrop version %s\n", Version)
		fmt.Printf("Built at: %s\n", BuildTime)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}