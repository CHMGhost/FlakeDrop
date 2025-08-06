package cmd

import (
    "context"
    "fmt"
    "os"
    "github.com/spf13/cobra"
    "github.com/spf13/viper"
    "flakedrop/internal/analytics"
)

var (
    analyticsManager *analytics.Manager
    rootCmd = &cobra.Command{
        Use:   "flakedrop",
        Short: "Deploy code to Snowflake databases",
        Long:  "FlakeDrop - A CLI tool for deploying code changes to Snowflake databases with simple commit selection",
        PersistentPreRun: func(cmd *cobra.Command, args []string) {
            // Initialize analytics for all commands
            if analyticsManager != nil && analyticsManager.IsEnabled() {
                ctx := context.Background()
                ctx = analytics.WithCollector(ctx, analyticsManager.GetCollector())
                cmd.SetContext(ctx)
                
                // Track command execution
                analyticsManager.TrackCommand(cmd.Name(), true, 0)
            }
        },
        PersistentPostRun: func(cmd *cobra.Command, args []string) {
            // Cleanup analytics after command execution
            if analyticsManager != nil {
                analytics.TrackCommandCompletion(cmd, true)
            }
        },
    }
)

func Execute() {
    // Initialize analytics manager
    analyticsManager = analytics.GetManager()
    defer analyticsManager.Shutdown()
    
    // Initialize analytics on first run if needed
    ctx := context.Background()
    analyticsManager.Initialize(ctx)
    
    if err := rootCmd.Execute(); err != nil {
        if analyticsManager != nil {
            analyticsManager.TrackError(err, "command_execution")
        }
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func init() {
    cobra.OnInitialize(initConfig)
}

func initConfig() {
    viper.SetConfigName("config")
    viper.SetConfigType("yaml")
    viper.AddConfigPath(".")
    
    home, err := os.UserHomeDir()
    if err == nil {
        viper.AddConfigPath(home + "/.flakedrop")
    }

    if err := viper.ReadInConfig(); err != nil {
        // Config file not found is okay for now
    }
}