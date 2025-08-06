package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"flakedrop/internal/config"
	"flakedrop/internal/ui"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var encryptConfigCmd = &cobra.Command{
	Use:   "encrypt-config",
	Short: "Encrypt passwords in configuration file",
	Long: `Encrypt plaintext passwords in the configuration file using AES-256-GCM encryption.
	
This command will:
- Read your current configuration file
- Encrypt any plaintext passwords found
- Save the configuration with encrypted passwords
- Create a backup of the original file

The encryption key is derived from:
1. FLAKEDROP_ENCRYPTION_KEY environment variable (if set)
2. Machine-specific identifier (hostname + home directory)

To use environment variable for password instead of encryption:
  export SNOWFLAKE_PASSWORD="your-password"`,
	RunE: runEncryptConfig,
}

var (
	configBackup bool
	configPath   string
)

func init() {
	rootCmd.AddCommand(encryptConfigCmd)
	
	encryptConfigCmd.Flags().BoolVar(&configBackup, "backup", true, "Create backup of original config")
	encryptConfigCmd.Flags().StringVar(&configPath, "config", "", "Path to config file (default: auto-detect)")
}

func runEncryptConfig(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	// Determine config file path
	var configFile string
	if configPath != "" {
		configFile = filepath.Clean(configPath)
	} else {
		configFile = config.GetConfigFile()
	}
	
	ui.Info(fmt.Sprintf("Reading configuration from: %s", configFile))
	
	// Read raw config file
	data, err := os.ReadFile(filepath.Clean(configFile))
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}
	
	// Parse config
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	
	// Check if passwords are already encrypted
	if config.IsEncrypted(cfg.Snowflake.Password) {
		ui.Info("Passwords are already encrypted")
		return nil
	}
	
	// Create backup if requested
	if configBackup {
		backupFile := configFile + ".backup"
		if err := os.WriteFile(backupFile, data, 0600); err != nil {
			return fmt.Errorf("failed to create backup: %w", err)
		}
		ui.Success(fmt.Sprintf("Created backup: %s", backupFile))
	}
	
	// Encrypt passwords
	ui.StartProgress("Encrypting passwords...")
	if err := config.EncryptConfigPasswords(cfg); err != nil {
		ui.StopProgress()
		return fmt.Errorf("failed to encrypt passwords: %w", err)
	}
	ui.StopProgress()
	
	// Save encrypted config
	configData, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	
	if err := os.WriteFile(configFile, configData, 0600); err != nil {
		return fmt.Errorf("failed to save encrypted config: %w", err)
	}
	
	ui.Success("Configuration passwords encrypted successfully")
	ui.Info("\nTo decrypt passwords at runtime, FlakeDrop will use:")
	ui.Info("1. FLAKEDROP_ENCRYPTION_KEY environment variable (if set)")
	ui.Info("2. Machine-specific key (hostname + home directory)")
	ui.Info("\nAlternatively, use SNOWFLAKE_PASSWORD environment variable to override")
	
	return nil
}