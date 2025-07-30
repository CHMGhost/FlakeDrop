package cmd

import (
    "fmt"
    "os"
    "github.com/spf13/cobra"
    "github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
    Use:   "flakedrop",
    Short: "Deploy code to Snowflake databases",
    Long:  "FlakeDrop - A CLI tool for deploying code changes to Snowflake databases with simple commit selection",
}

func Execute() {
    if err := rootCmd.Execute(); err != nil {
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