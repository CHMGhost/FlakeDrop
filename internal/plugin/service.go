package plugin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"flakedrop/internal/common"
	"flakedrop/pkg/models"
	"flakedrop/pkg/plugin"
	"flakedrop/pkg/plugin/builtin"
)

// Service provides plugin functionality to the main application
type Service struct {
	manager       *plugin.Manager
	configManager *plugin.ConfigManager
	security      *plugin.SecurityManager
	hookExecutor  *plugin.HookExecutor
	registry      *plugin.RegistryManager
	initialized   bool
}

// NewService creates a new plugin service
func NewService(appConfig *models.Config) (*Service, error) {
	// Setup plugin directories
	configPath := getConfigPath()
	pluginDirs := []string{
		filepath.Join(configPath, "plugins"),
		"./plugins",
	}

	// Add configured directories
	if appConfig.Plugins.Enabled {
		pluginDirs = append(pluginDirs, appConfig.Plugins.Directories...)
	}

	// Create registry manager
	registryManager := plugin.NewRegistryManager()
	
	// Add local registry
	localRegistry := plugin.NewLocalPluginRegistry(
		filepath.Join(configPath, "registry"),
		appConfig.Plugins.Registry.CacheDir,
	)
	registryManager.AddRegistry(localRegistry)

	// Add HTTP registries if configured
	if appConfig.Plugins.Registry.Enabled {
		for _, registryURL := range appConfig.Plugins.Registry.URLs {
			httpRegistry := plugin.NewHTTPPluginRegistry(
				registryURL,
				appConfig.Plugins.Registry.CacheDir,
			)
			registryManager.AddRegistry(httpRegistry)
		}
	}

	// Create plugin manager
	manager := plugin.NewManager(pluginDirs, registryManager)

	// Create configuration manager
	configManager := plugin.NewConfigManager(configPath)

	// Create security manager
	security := plugin.NewSecurityManager(&appConfig.Plugins.Security)

	// Create hook executor
	hookExecutor := plugin.NewHookExecutor(manager)

	return &Service{
		manager:       manager,
		configManager: configManager,
		security:      security,
		hookExecutor:  hookExecutor,
		registry:      registryManager,
		initialized:   false,
	}, nil
}

// Initialize initializes the plugin service
func (s *Service) Initialize(ctx context.Context) error {
	if s.initialized {
		return nil
	}

	// Initialize plugin manager
	if err := s.manager.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize plugin manager: %w", err)
	}

	// Load global plugin configuration
	globalConfig, err := s.configManager.LoadGlobalConfig()
	if err != nil {
		return fmt.Errorf("failed to load plugin configuration: %w", err)
	}

	// Load and initialize built-in plugins
	if err := s.loadBuiltinPlugins(ctx, globalConfig); err != nil {
		return fmt.Errorf("failed to load builtin plugins: %w", err)
	}

	// Load configured plugins
	for _, pluginConfigItem := range globalConfig.Plugins {
		if pluginConfigItem.Enabled {
			// Convert models.PluginConfigItem to plugin.PluginConfig
			pluginConfig := plugin.PluginConfig{
				Name:     pluginConfigItem.Name,
				Enabled:  pluginConfigItem.Enabled,
				Settings: pluginConfigItem.Settings,
			}
			if err := s.manager.LoadPlugin(ctx, pluginConfigItem.Name, pluginConfig); err != nil {
				// Log error but don't fail initialization
				fmt.Printf("Warning: failed to load plugin %s: %v\n", pluginConfigItem.Name, err)
			}
		}
	}

	s.initialized = true
	return nil
}

// loadBuiltinPlugins loads the built-in plugins
func (s *Service) loadBuiltinPlugins(ctx context.Context, globalConfig *models.PluginConfig) error {
	builtinPlugins := map[string]plugin.PluginInterface{
		"slack":   builtin.NewSlackPlugin(),
		"email":   builtin.NewEmailPlugin(),
		"webhook": builtin.NewWebhookPlugin(),
	}

	for name, pluginInstance := range builtinPlugins {
		// Check if plugin is configured
		var config plugin.PluginConfig
		found := false
		
		for _, pluginConfigItem := range globalConfig.Plugins {
			if pluginConfigItem.Name == name {
				config = plugin.PluginConfig{
					Name:     pluginConfigItem.Name,
					Enabled:  pluginConfigItem.Enabled,
					Settings: pluginConfigItem.Settings,
				}
				found = true
				break
			}
		}

		if !found {
			// Create default disabled configuration
			config = plugin.PluginConfig{
				Name:     name,
				Enabled:  false,
				Settings: make(map[string]interface{}),
			}
		}

		if config.Enabled {
			// Create security sandbox if needed
			var sandbox *plugin.Sandbox
			if globalConfig.Security.SandboxEnabled {
				var err error
				// Get the manifest from the plugin (assuming builtin plugins implement this)
				var manifest plugin.PluginManifest
				if manifestProvider, ok := pluginInstance.(interface{ GetManifest() plugin.PluginManifest }); ok {
					manifest = manifestProvider.GetManifest()
				}
				
				sandbox, err = s.security.CreateSandbox(name, manifest)
				if err != nil {
					return fmt.Errorf("failed to create sandbox for %s: %w", name, err)
				}
			}

			// Wrap plugin with security if sandbox is enabled
			var wrappedPlugin plugin.PluginInterface = pluginInstance
			if sandbox != nil {
				wrappedPlugin = plugin.NewSecurePluginWrapper(pluginInstance, s.security, sandbox)
			}

			// Store the plugin info manually since builtin plugins aren't discovered
			pluginInfo := &plugin.PluginInfo{
				Instance:  wrappedPlugin,
				Config:    config,
				LoadTime:  time.Now(),
				Enabled:   config.Enabled,
			}

			// Register plugin in manager (this is a hack for builtin plugins)
			s.manager.SetPluginInfo(name, pluginInfo)

			// Initialize the plugin
			if err := wrappedPlugin.Initialize(ctx, config); err != nil {
				return fmt.Errorf("failed to initialize builtin plugin %s: %w", name, err)
			}
		}
	}

	return nil
}

// ExecuteHook executes hooks for a deployment event
func (s *Service) ExecuteHook(ctx context.Context, hookType plugin.HookType, data plugin.HookData) error {
	if !s.initialized {
		return nil // Plugin system not initialized, skip hooks
	}

	return s.hookExecutor.ExecuteHook(ctx, hookType, data)
}

// SendNotification sends a notification through all notification plugins
func (s *Service) SendNotification(ctx context.Context, message plugin.NotificationMessage) error {
	if !s.initialized {
		return nil // Plugin system not initialized, skip notifications
	}

	return s.manager.SendNotification(ctx, message)
}

// GetPluginInfo returns information about all plugins
func (s *Service) GetPluginInfo() map[string]*plugin.PluginInfo {
	if !s.initialized {
		return make(map[string]*plugin.PluginInfo)
	}

	return s.manager.ListPlugins()
}

// GetStats returns plugin system statistics
func (s *Service) GetStats() *plugin.PluginStats {
	if !s.initialized {
		return &plugin.PluginStats{}
	}

	return s.manager.GetStats()
}

// InstallPlugin installs a plugin from the registry
func (s *Service) InstallPlugin(ctx context.Context, name, version string) error {
	// Get plugin manifest from registry
	manifest, err := s.registry.Get(ctx, name, version)
	if err != nil {
		return fmt.Errorf("failed to get plugin manifest: %w", err)
	}

	// Validate plugin security
	pluginData, err := s.registry.Download(ctx, name, version)
	if err != nil {
		return fmt.Errorf("failed to download plugin: %w", err)
	}

	// Save plugin to local directory
	pluginDir := filepath.Join(getConfigPath(), "plugins", name)
	if err := os.MkdirAll(pluginDir, common.DirPermissionNormal); err != nil {
		return fmt.Errorf("failed to create plugin directory: %w", err)
	}

	// Determine file extension based on plugin type
	pluginFile := filepath.Join(pluginDir, name+".so") // Assuming shared library
	if err := os.WriteFile(pluginFile, pluginData, common.FilePermissionExecutable); err != nil {
		return fmt.Errorf("failed to save plugin file: %w", err)
	}

	// Save manifest
	manifestFile := filepath.Join(pluginDir, "plugin.yaml")
	manifestData, err := MarshalManifest(*manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := os.WriteFile(manifestFile, manifestData, common.FilePermissionSecure); err != nil {
		return fmt.Errorf("failed to save manifest: %w", err)
	}

	// Create default configuration
	defaultConfig := plugin.PluginConfig{
		Name:     name,
		Enabled:  false, // Disabled by default for security
		Settings: make(map[string]interface{}),
	}

	if err := s.configManager.SavePluginConfig(name, &defaultConfig); err != nil {
		return fmt.Errorf("failed to save plugin configuration: %w", err)
	}

	return nil
}

// EnablePlugin enables a plugin
func (s *Service) EnablePlugin(ctx context.Context, name string) error {
	// Load and enable in configuration
	if err := s.configManager.EnablePlugin(name); err != nil {
		return fmt.Errorf("failed to enable plugin in config: %w", err)
	}

	// Load the plugin if service is initialized
	if s.initialized {
		config, err := s.configManager.LoadPluginConfig(name)
		if err != nil {
			return fmt.Errorf("failed to load plugin config: %w", err)
		}

		if err := s.manager.LoadPlugin(ctx, name, *config); err != nil {
			return fmt.Errorf("failed to load plugin: %w", err)
		}
	}

	return nil
}

// DisablePlugin disables a plugin
func (s *Service) DisablePlugin(ctx context.Context, name string) error {
	// Unload the plugin if service is initialized
	if s.initialized {
		if err := s.manager.UnloadPlugin(ctx, name); err != nil {
			return fmt.Errorf("failed to unload plugin: %w", err)
		}
	}

	// Disable in configuration
	if err := s.configManager.DisablePlugin(name); err != nil {
		return fmt.Errorf("failed to disable plugin in config: %w", err)
	}

	return nil
}

// ConfigurePlugin configures a plugin
func (s *Service) ConfigurePlugin(ctx context.Context, name string, settings map[string]interface{}) error {
	if err := s.configManager.UpdatePluginConfig(name, settings); err != nil {
		return fmt.Errorf("failed to update plugin configuration: %w", err)
	}

	// Reload plugin if it's currently loaded
	if s.initialized {
		info, err := s.manager.GetPluginInfo(name)
		if err == nil && info.Instance != nil {
			// Unload and reload with new configuration
			if err := s.manager.UnloadPlugin(ctx, name); err != nil {
				return fmt.Errorf("failed to unload plugin for reconfiguration: %w", err)
			}

			config, err := s.configManager.LoadPluginConfig(name)
			if err != nil {
				return fmt.Errorf("failed to load updated config: %w", err)
			}

			if err := s.manager.LoadPlugin(ctx, name, *config); err != nil {
				return fmt.Errorf("failed to reload plugin: %w", err)
			}
		}
	}

	return nil
}

// ListAvailablePlugins lists plugins available in registries
func (s *Service) ListAvailablePlugins(ctx context.Context) ([]plugin.PluginManifest, error) {
	return s.registry.List(ctx)
}

// SearchPlugins searches for plugins in registries
func (s *Service) SearchPlugins(ctx context.Context, query string) ([]plugin.PluginManifest, error) {
	return s.registry.Search(ctx, query)
}

// Shutdown gracefully shuts down the plugin service
func (s *Service) Shutdown(ctx context.Context) error {
	if !s.initialized {
		return nil
	}

	err := s.manager.Shutdown(ctx)
	s.initialized = false
	return err
}

// getConfigPath returns the configuration path
func getConfigPath() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".flakedrop")
	}
	return ".flakedrop"
}

// MarshalManifest marshals a plugin manifest to YAML
func MarshalManifest(manifest plugin.PluginManifest) ([]byte, error) {
	// This would typically use yaml.Marshal, but for simplicity using JSON
	// In a real implementation, you'd use gopkg.in/yaml.v3
	return []byte(fmt.Sprintf(`name: %s
version: %s
description: %s
author: %s
license: %s
`, manifest.Name, manifest.Version, manifest.Description, manifest.Author, manifest.License)), nil
}