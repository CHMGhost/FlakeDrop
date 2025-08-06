package cmd

import (
    "fmt"
    "os"

    "flakedrop/internal/analytics"
    "github.com/spf13/cobra"
)

var analyticsCmd = &cobra.Command{
    Use:   "analytics",
    Short: "Manage analytics and usage tracking settings",
    Long:  `Configure anonymous usage tracking, view statistics, and export analytics data`,
}

var analyticsEnableCmd = &cobra.Command{
    Use:   "enable",
    Short: "Enable anonymous usage tracking",
    RunE: func(cmd *cobra.Command, args []string) error {
        manager := analytics.GetManager()
        manager.SetEnabled(true)
        fmt.Println("✓ Analytics enabled. Thank you for helping improve FlakeDrop!")
        fmt.Println("  Your privacy is protected - no personal information is collected.")
        return nil
    },
}

var analyticsDisableCmd = &cobra.Command{
    Use:   "disable",
    Short: "Disable usage tracking",
    RunE: func(cmd *cobra.Command, args []string) error {
        manager := analytics.GetManager()
        manager.SetEnabled(false)
        fmt.Println("✓ Analytics disabled.")
        return nil
    },
}

var analyticsStatusCmd = &cobra.Command{
    Use:   "status",
    Short: "Show current analytics settings and statistics",
    RunE: func(cmd *cobra.Command, args []string) error {
        manager := analytics.GetManager()
        
        fmt.Println("=== Analytics Status ===")
        if manager.IsEnabled() {
            fmt.Println("Status: Enabled")
            
            stats := manager.GetStats()
            fmt.Printf("\nUsage Statistics:\n")
            fmt.Printf("  Total Deployments: %d\n", stats.TotalDeployments)
            fmt.Printf("  Successful: %d\n", stats.SuccessfulDeploys)
            fmt.Printf("  Failed: %d\n", stats.FailedDeploys)
            fmt.Printf("  Rollbacks: %d\n", stats.TotalRollbacks)
            
            if len(stats.CommandCounts) > 0 {
                fmt.Printf("\nMost Used Commands:\n")
                for cmd, count := range stats.CommandCounts {
                    fmt.Printf("  %s: %d\n", cmd, count)
                }
            }
            
            if len(stats.FeatureCounts) > 0 {
                fmt.Printf("\nFeature Usage:\n")
                for feature, count := range stats.FeatureCounts {
                    fmt.Printf("  %s: %d\n", feature, count)
                }
            }
        } else {
            fmt.Println("Status: Disabled")
            fmt.Println("\nEnable analytics to help improve FlakeDrop:")
            fmt.Println("  flakedrop analytics enable")
        }
        
        return nil
    },
}

var analyticsExportCmd = &cobra.Command{
    Use:   "export",
    Short: "Export analytics data",
    RunE: func(cmd *cobra.Command, args []string) error {
        format, _ := cmd.Flags().GetString("format")
        output, _ := cmd.Flags().GetString("output")
        
        manager := analytics.GetManager()
        data, err := manager.ExportData(format)
        if err != nil {
            return fmt.Errorf("failed to export data: %w", err)
        }
        
        if output == "" {
            fmt.Println(string(data))
        } else {
            if err := os.WriteFile(output, data, 0600); err != nil {
                return fmt.Errorf("failed to write export file: %w", err)
            }
            fmt.Printf("✓ Analytics data exported to %s\n", output)
        }
        
        return nil
    },
}

var analyticsClearCmd = &cobra.Command{
    Use:   "clear",
    Short: "Clear all stored analytics data",
    RunE: func(cmd *cobra.Command, args []string) error {
        force, _ := cmd.Flags().GetBool("force")
        
        if !force {
            fmt.Println("This will permanently delete all analytics data.")
            fmt.Print("Are you sure? (y/N): ")
            
            var response string
            fmt.Scanln(&response)
            
            if response != "y" && response != "Y" {
                fmt.Println("Operation cancelled.")
                return nil
            }
        }
        
        manager := analytics.GetManager()
        if err := manager.ClearData(); err != nil {
            return fmt.Errorf("failed to clear analytics data: %w", err)
        }
        
        fmt.Println("✓ All analytics data has been cleared.")
        return nil
    },
}

var analyticsSetLevelCmd = &cobra.Command{
    Use:   "set-level [level]",
    Short: "Set analytics collection level (none, minimal, basic, full)",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        level := args[0]
        
        var analyticsLevel analytics.AnalyticsLevel
        switch level {
        case "none":
            analyticsLevel = analytics.AnalyticsNone
        case "minimal":
            analyticsLevel = analytics.AnalyticsMinimal
        case "basic":
            analyticsLevel = analytics.AnalyticsBasic
        case "full":
            analyticsLevel = analytics.AnalyticsFull
        default:
            return fmt.Errorf("invalid level: %s (must be none, minimal, basic, or full)", level)
        }
        
        manager := analytics.GetManager()
        manager.SetLevel(analyticsLevel)
        
        fmt.Printf("✓ Analytics level set to: %s\n", level)
        return nil
    },
}

func init() {
    rootCmd.AddCommand(analyticsCmd)
    
    analyticsCmd.AddCommand(analyticsEnableCmd)
    analyticsCmd.AddCommand(analyticsDisableCmd)
    analyticsCmd.AddCommand(analyticsStatusCmd)
    analyticsCmd.AddCommand(analyticsExportCmd)
    analyticsCmd.AddCommand(analyticsClearCmd)
    analyticsCmd.AddCommand(analyticsSetLevelCmd)
    
    analyticsExportCmd.Flags().StringP("format", "f", "json", "Export format (json, csv)")
    analyticsExportCmd.Flags().StringP("output", "o", "", "Output file path")
    
    analyticsClearCmd.Flags().BoolP("force", "f", false, "Skip confirmation prompt")
}