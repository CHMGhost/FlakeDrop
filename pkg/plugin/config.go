package plugin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	"flakedrop/internal/common"
	"flakedrop/pkg/models"
)

// ConfigManager manages plugin configurations
type ConfigManager struct {
	configPath     string
	globalConfig   *models.PluginConfig
	pluginConfigs  map[string]PluginConfig
	validator      *ConfigValidator
	defaults       *DefaultConfigProvider
}

// ConfigValidator validates plugin configurations
type ConfigValidator struct {
	schemas map[string]PluginManifest
}

// DefaultConfigProvider provides default configurations for plugins
type DefaultConfigProvider struct {
	defaults map[string]PluginConfig
}

// NewConfigManager creates a new plugin configuration manager
func NewConfigManager(configPath string) *ConfigManager {
	return &ConfigManager{
		configPath:    configPath,
		pluginConfigs: make(map[string]PluginConfig),
		validator:     NewConfigValidator(),
		defaults:      NewDefaultConfigProvider(),
	}
}

// NewConfigValidator creates a new configuration validator
func NewConfigValidator() *ConfigValidator {
	return &ConfigValidator{
		schemas: make(map[string]PluginManifest),
	}
}

// NewDefaultConfigProvider creates a new default configuration provider
func NewDefaultConfigProvider() *DefaultConfigProvider {
	return &DefaultConfigProvider{
		defaults: make(map[string]PluginConfig),
	}
}

// LoadGlobalConfig loads the global plugin configuration
func (cm *ConfigManager) LoadGlobalConfig() (*models.PluginConfig, error) {
	configFile := filepath.Join(cm.configPath, "plugin-config.yaml")
	
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		// Return default configuration if file doesn't exist
		return cm.getDefaultGlobalConfig(), nil
	}

	configFile, err := common.ValidatePath(configFile, cm.configPath)
	if err != nil {
		return nil, fmt.Errorf("invalid config file path: %w", err)
	}
	data, err := os.ReadFile(configFile) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin config file: %w", err)
	}

	var config models.PluginConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse plugin config: %w", err)
	}

	cm.globalConfig = &config
	return &config, nil
}

// SaveGlobalConfig saves the global plugin configuration
func (cm *ConfigManager) SaveGlobalConfig(config *models.PluginConfig) error {
	if err := os.MkdirAll(cm.configPath, common.DirPermissionNormal); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configFile := filepath.Join(cm.configPath, "plugin-config.yaml")
	
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal plugin config: %w", err)
	}

	if err := os.WriteFile(configFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write plugin config file: %w", err)
	}

	cm.globalConfig = config
	return nil
}

// LoadPluginConfig loads configuration for a specific plugin
func (cm *ConfigManager) LoadPluginConfig(pluginName string) (*PluginConfig, error) {
	// Check if config is already loaded
	if config, exists := cm.pluginConfigs[pluginName]; exists {
		return &config, nil
	}

	// Try to load from global config first
	if cm.globalConfig != nil {
		for _, pluginConfigItem := range cm.globalConfig.Plugins {
			if pluginConfigItem.Name == pluginName {
				// Convert models.PluginConfigItem to PluginConfig
				pluginConfig := PluginConfig{
					Name:     pluginConfigItem.Name,
					Enabled:  pluginConfigItem.Enabled,
					Settings: pluginConfigItem.Settings,
				}
				cm.pluginConfigs[pluginName] = pluginConfig
				return &pluginConfig, nil
			}
		}
	}

	// Try to load from individual plugin config file
	configFile := filepath.Join(cm.configPath, "plugins", pluginName+".yaml")
	if _, err := os.Stat(configFile); err == nil {
		configFile, err = common.ValidatePath(configFile, filepath.Join(cm.configPath, "plugins"))
		if err != nil {
			return nil, fmt.Errorf("invalid plugin config file path: %w", err)
		}
		data, err := os.ReadFile(configFile) // #nosec G304
		if err != nil {
			return nil, fmt.Errorf("failed to read plugin config file for %s: %w", pluginName, err)
		}

		var config PluginConfig
		if err := yaml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse plugin config for %s: %w", pluginName, err)
		}

		config.Name = pluginName
		cm.pluginConfigs[pluginName] = config
		return &config, nil
	}

	// Return default configuration
	defaultConfig := cm.defaults.GetDefaultConfig(pluginName)
	cm.pluginConfigs[pluginName] = defaultConfig
	return &defaultConfig, nil
}

// SavePluginConfig saves configuration for a specific plugin
func (cm *ConfigManager) SavePluginConfig(pluginName string, config *PluginConfig) error {
	pluginConfigDir := filepath.Join(cm.configPath, "plugins")
	if err := os.MkdirAll(pluginConfigDir, common.DirPermissionNormal); err != nil {
		return fmt.Errorf("failed to create plugin config directory: %w", err)
	}

	configFile := filepath.Join(pluginConfigDir, pluginName+".yaml")
	
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal plugin config for %s: %w", pluginName, err)
	}

	if err := os.WriteFile(configFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write plugin config file for %s: %w", pluginName, err)
	}

	cm.pluginConfigs[pluginName] = *config
	return nil
}

// ValidateConfig validates a plugin configuration against its schema
func (cm *ConfigManager) ValidateConfig(pluginName string, config *PluginConfig) error {
	return cm.validator.Validate(pluginName, config)
}

// GetAllPluginConfigs returns all loaded plugin configurations
func (cm *ConfigManager) GetAllPluginConfigs() map[string]PluginConfig {
	result := make(map[string]PluginConfig)
	for name, config := range cm.pluginConfigs {
		result[name] = config
	}
	return result
}

// UpdatePluginConfig updates a plugin's configuration
func (cm *ConfigManager) UpdatePluginConfig(pluginName string, updates map[string]interface{}) error {
	config, err := cm.LoadPluginConfig(pluginName)
	if err != nil {
		return fmt.Errorf("failed to load current config for %s: %w", pluginName, err)
	}

	// Apply updates
	if config.Settings == nil {
		config.Settings = make(map[string]interface{})
	}

	for key, value := range updates {
		config.Settings[key] = value
	}

	// Validate updated configuration
	if err := cm.ValidateConfig(pluginName, config); err != nil {
		return fmt.Errorf("validation failed for updated config: %w", err)
	}

	// Save updated configuration
	return cm.SavePluginConfig(pluginName, config)
}

// EnablePlugin enables a plugin
func (cm *ConfigManager) EnablePlugin(pluginName string) error {
	config, err := cm.LoadPluginConfig(pluginName)
	if err != nil {
		return err
	}

	config.Enabled = true
	return cm.SavePluginConfig(pluginName, config)
}

// DisablePlugin disables a plugin
func (cm *ConfigManager) DisablePlugin(pluginName string) error {
	config, err := cm.LoadPluginConfig(pluginName)
	if err != nil {
		return err
	}

	config.Enabled = false
	return cm.SavePluginConfig(pluginName, config)
}

// RegisterSchema registers a plugin schema for validation
func (cv *ConfigValidator) RegisterSchema(manifest PluginManifest) {
	cv.schemas[manifest.Name] = manifest
}

// Validate validates a plugin configuration
func (cv *ConfigValidator) Validate(pluginName string, config *PluginConfig) error {
	schema, exists := cv.schemas[pluginName]
	if !exists {
		// No schema registered, skip validation
		return nil
	}

	// Validate required settings
	for _, configSchema := range schema.Config {
		if configSchema.Required {
			if config.Settings == nil {
				return fmt.Errorf("required setting '%s' is missing", configSchema.Name)
			}

			value, exists := config.Settings[configSchema.Name]
			if !exists {
				return fmt.Errorf("required setting '%s' is missing", configSchema.Name)
			}

			// Validate type and value
			if err := cv.validateConfigValue(configSchema, value); err != nil {
				return fmt.Errorf("invalid value for setting '%s': %w", configSchema.Name, err)
			}
		}
	}

	// Validate optional settings
	if config.Settings != nil {
		for settingName, value := range config.Settings {
			var configSchema *ConfigSchema
			for _, cs := range schema.Config {
				if cs.Name == settingName {
					configSchema = &cs
					break
				}
			}

			if configSchema != nil {
				if err := cv.validateConfigValue(*configSchema, value); err != nil {
					return fmt.Errorf("invalid value for setting '%s': %w", settingName, err)
				}
			}
		}
	}

	return nil
}

// validateConfigValue validates a configuration value against its schema
func (cv *ConfigValidator) validateConfigValue(schema ConfigSchema, value interface{}) error {
	switch schema.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
		
		// Validate against options if provided
		if len(schema.Options) > 0 {
			strVal := value.(string)
			for _, option := range schema.Options {
				if strVal == option {
					return nil
				}
			}
			return fmt.Errorf("value must be one of: %v", schema.Options)
		}

	case "int", "integer":
		switch v := value.(type) {
		case int, int32, int64:
			// Valid
		case float64:
			// YAML sometimes parses integers as float64
			if v != float64(int64(v)) {
				return fmt.Errorf("expected integer, got float with decimal places")
			}
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}

	case "bool", "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}

	case "float", "number":
		switch value.(type) {
		case int, int32, int64, float32, float64:
			// Valid
		default:
			return fmt.Errorf("expected number, got %T", value)
		}

	case "array", "list":
		if _, ok := value.([]interface{}); !ok {
			return fmt.Errorf("expected array, got %T", value)
		}

	case "object", "map":
		if _, ok := value.(map[string]interface{}); !ok {
			return fmt.Errorf("expected object, got %T", value)
		}

	default:
		return fmt.Errorf("unknown config type: %s", schema.Type)
	}

	return nil
}

// SetDefaultConfig sets the default configuration for a plugin
func (dcp *DefaultConfigProvider) SetDefaultConfig(pluginName string, config PluginConfig) {
	dcp.defaults[pluginName] = config
}

// GetDefaultConfig returns the default configuration for a plugin
func (dcp *DefaultConfigProvider) GetDefaultConfig(pluginName string) PluginConfig {
	if config, exists := dcp.defaults[pluginName]; exists {
		return config
	}

	// Return basic default configuration
	return PluginConfig{
		Name:    pluginName,
		Enabled: false,
		Settings: map[string]interface{}{},
	}
}

// getDefaultGlobalConfig returns the default global plugin configuration
func (cm *ConfigManager) getDefaultGlobalConfig() *models.PluginConfig {
	return &models.PluginConfig{
		Enabled:     true,
		Directories: []string{"./plugins", "~/.flakedrop/plugins"},
		Plugins:     []models.PluginConfigItem{},
		Registry: models.RegistryConfig{
			Enabled:        false,
			URLs:           []string{},
			CacheDir:       "~/.flakedrop/plugin-cache",
			AutoUpdate:     false,
			UpdateInterval: "24h",
		},
		Security: models.PluginSecurityConfig{
			AllowUnsigned:      false,
			TrustedSources:     []string{},
			SandboxEnabled:     true,
			MaxMemoryMB:        256,
			MaxExecutionTime:   "30s",
			AllowedPermissions: []string{"read", "notify"},
		},
		Hooks: models.HookConfig{
			Timeout:         "60s",
			Parallel:        false,
			FailureBehavior: "continue",
			RetryPolicy: models.RetryPolicyConfig{
				MaxRetries: 3,
				Backoff:    "1s",
				MaxBackoff: "10s",
			},
		},
	}
}

// ConfigWizard helps users configure plugins interactively
type ConfigWizard struct {
	configManager *ConfigManager
}

// NewConfigWizard creates a new configuration wizard
func NewConfigWizard(configManager *ConfigManager) *ConfigWizard {
	return &ConfigWizard{
		configManager: configManager,
	}
}

// ConfigurePlugin guides the user through plugin configuration
func (cw *ConfigWizard) ConfigurePlugin(ctx context.Context, pluginName string, manifest PluginManifest) (*PluginConfig, error) {
	config := &PluginConfig{
		Name:     pluginName,
		Enabled:  true,
		Settings: make(map[string]interface{}),
	}

	fmt.Printf("Configuring plugin: %s\n", manifest.Name)
	fmt.Printf("Description: %s\n\n", manifest.Description)

	// Configure each setting
	for _, configSchema := range manifest.Config {
		value, err := cw.promptForSetting(configSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to configure setting %s: %w", configSchema.Name, err)
		}

		if value != nil {
			config.Settings[configSchema.Name] = value
		}
	}

	// Validate the configuration
	validator := NewConfigValidator()
	validator.RegisterSchema(manifest)
	
	if err := validator.Validate(pluginName, config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

// promptForSetting prompts the user for a configuration setting value
func (cw *ConfigWizard) promptForSetting(schema ConfigSchema) (interface{}, error) {
	fmt.Printf("Setting: %s\n", schema.Name)
	fmt.Printf("Description: %s\n", schema.Description)
	fmt.Printf("Type: %s\n", schema.Type)
	
	if schema.Default != nil {
		fmt.Printf("Default: %v\n", schema.Default)
	}
	
	if len(schema.Options) > 0 {
		fmt.Printf("Options: %v\n", schema.Options)
	}
	
	if schema.Required {
		fmt.Print("Required: Yes\n")
	} else {
		fmt.Print("Required: No\n")
	}

	fmt.Print("Enter value: ")
	
	// In a real implementation, you would use a proper input library
	// For now, return the default value
	if schema.Default != nil {
		return schema.Default, nil
	}
	
	if !schema.Required {
		return nil, nil
	}

	// Return a basic default based on type
	switch schema.Type {
	case "string":
		return "", nil
	case "int", "integer":
		return 0, nil
	case "bool", "boolean":
		return false, nil
	case "float", "number":
		return 0.0, nil
	case "array", "list":
		return []interface{}{}, nil
	case "object", "map":
		return map[string]interface{}{}, nil
	default:
		return nil, fmt.Errorf("unknown config type: %s", schema.Type)
	}
}