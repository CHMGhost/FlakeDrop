package config

import (
    "fmt"
    "os"
    "path/filepath"
    "flakedrop/internal/common"
    "flakedrop/pkg/models"
    "gopkg.in/yaml.v3"
)

func GetConfigPath() string {
    // Check for environment variable first
    if configPath := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG"); configPath != "" {
        return filepath.Dir(configPath)
    }
    home, _ := os.UserHomeDir()
    return filepath.Join(home, ".flakedrop")
}

func GetConfigFile() string {
    // Check for environment variable first
    if configFile := os.Getenv("SNOWFLAKE_DEPLOY_CONFIG"); configFile != "" {
        // Validate the path to prevent directory traversal
        cleaned, err := common.CleanPath(configFile)
        if err != nil {
            // Fall back to default if invalid
            return filepath.Join(GetConfigPath(), "config.yaml")
        }
        return cleaned
    }
    return filepath.Join(GetConfigPath(), "config.yaml")
}

func Load() (*models.Config, error) {
    configFile := GetConfigFile()
    
    // Validate the config file path
    cleanedPath, err := common.CleanPath(configFile)
    if err != nil {
        return nil, fmt.Errorf("invalid config file path: %w", err)
    }
    
    // Check if config file exists
    if _, err := os.Stat(cleanedPath); os.IsNotExist(err) {
        return &models.Config{}, nil
    }
    
    data, err := os.ReadFile(cleanedPath) // #nosec G304 - path is validated
    if err != nil {
        return nil, fmt.Errorf("failed to read config file: %w", err)
    }
    
    var config models.Config
    if err := yaml.Unmarshal(data, &config); err != nil {
        return nil, fmt.Errorf("failed to unmarshal config: %w", err)
    }
    return &config, nil
}

func Save(config *models.Config) error {
    configPath := GetConfigPath()
    if err := os.MkdirAll(configPath, 0700); err != nil {
        return fmt.Errorf("failed to create config directory: %w", err)
    }

    configFile := GetConfigFile()
    
    data, err := yaml.Marshal(config)
    if err != nil {
        return fmt.Errorf("failed to marshal config: %w", err)
    }

    if err := os.WriteFile(configFile, data, 0600); err != nil {
        return fmt.Errorf("failed to write config file: %w", err)
    }

    return nil
}

func Exists() bool {
    _, err := os.Stat(GetConfigFile())
    return err == nil
}