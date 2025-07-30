package plugin

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
	"gopkg.in/yaml.v3"
	"flakedrop/internal/common"
)

// Manager manages the lifecycle of plugins
type Manager struct {
	plugins        map[string]*PluginInfo
	hooks          map[HookType][]HookPlugin
	notifications  []NotificationPlugin
	transforms     []TransformPlugin
	pluginDirs     []string
	registry       PluginRegistry
	stats          *PluginStats
	mu             sync.RWMutex
	eventListeners []func(PluginEvent)
}

// NewManager creates a new plugin manager
func NewManager(pluginDirs []string, registry PluginRegistry) *Manager {
	return &Manager{
		plugins:        make(map[string]*PluginInfo),
		hooks:          make(map[HookType][]HookPlugin),
		notifications:  make([]NotificationPlugin, 0),
		transforms:     make([]TransformPlugin, 0),
		pluginDirs:     pluginDirs,
		registry:       registry,
		stats: &PluginStats{
			HookExecutions:  make(map[HookType]int64),
			PluginUsage:     make(map[string]int64),
			AverageExecTime: make(map[string]time.Duration),
			Errors:          make([]PluginEvent, 0),
		},
		eventListeners: make([]func(PluginEvent), 0),
	}
}

// Initialize initializes the plugin manager and discovers plugins
func (m *Manager) Initialize(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Discover plugins from plugin directories
	for _, dir := range m.pluginDirs {
		if err := m.discoverPlugins(ctx, dir); err != nil {
			return fmt.Errorf("failed to discover plugins in %s: %w", dir, err)
		}
	}

	m.stats.TotalPlugins = len(m.plugins)
	return nil
}

// LoadPlugin loads a specific plugin by name
func (m *Manager) LoadPlugin(ctx context.Context, name string, config PluginConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.plugins[name]
	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	if info.Instance != nil {
		return fmt.Errorf("plugin %s is already loaded", name)
	}

	// Load the plugin based on its type
	instance, err := m.loadPluginInstance(info.Manifest)
	if err != nil {
		m.emitEvent(PluginEvent{
			Type:      "plugin_load_error",
			Plugin:    name,
			Timestamp: time.Now(),
			Error:     err.Error(),
		})
		return fmt.Errorf("failed to load plugin %s: %w", name, err)
	}

	// Initialize the plugin
	if err := instance.Initialize(ctx, config); err != nil {
		m.emitEvent(PluginEvent{
			Type:      "plugin_init_error",
			Plugin:    name,
			Timestamp: time.Now(),
			Error:     err.Error(),
		})
		return fmt.Errorf("failed to initialize plugin %s: %w", name, err)
	}

	info.Instance = instance
	info.Config = config
	info.LoadTime = time.Now()
	info.Enabled = config.Enabled

	// Register plugin with appropriate handlers
	if err := m.registerPlugin(instance); err != nil {
		return fmt.Errorf("failed to register plugin %s: %w", name, err)
	}

	if config.Enabled {
		m.stats.EnabledPlugins++
	}

	m.emitEvent(PluginEvent{
		Type:      "plugin_loaded",
		Plugin:    name,
		Timestamp: time.Now(),
		Data:      map[string]interface{}{"version": instance.Version()},
	})

	return nil
}

// UnloadPlugin unloads a specific plugin
func (m *Manager) UnloadPlugin(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.plugins[name]
	if !exists {
		return fmt.Errorf("plugin %s not found", name)
	}

	if info.Instance == nil {
		return fmt.Errorf("plugin %s is not loaded", name)
	}

	// Shutdown the plugin
	if err := info.Instance.Shutdown(ctx); err != nil {
		m.emitEvent(PluginEvent{
			Type:      "plugin_shutdown_error",
			Plugin:    name,
			Timestamp: time.Now(),
			Error:     err.Error(),
		})
		return fmt.Errorf("failed to shutdown plugin %s: %w", name, err)
	}

	// Unregister from handlers
	m.unregisterPlugin(info.Instance)

	info.Instance = nil
	if info.Enabled {
		m.stats.EnabledPlugins--
	}

	m.emitEvent(PluginEvent{
		Type:      "plugin_unloaded",
		Plugin:    name,
		Timestamp: time.Now(),
	})

	return nil
}

// ExecuteHook executes all plugins registered for a specific hook
func (m *Manager) ExecuteHook(ctx context.Context, hookType HookType, data HookData) error {
	m.mu.RLock()
	hookPlugins := m.hooks[hookType]
	m.mu.RUnlock()

	if len(hookPlugins) == 0 {
		return nil
	}

	var errors []error
	start := time.Now()

	for _, hookPlugin := range hookPlugins {
		pluginStart := time.Now()
		
		if err := hookPlugin.ExecuteHook(ctx, hookType, data); err != nil {
			errors = append(errors, fmt.Errorf("plugin %s failed: %w", hookPlugin.Name(), err))
			m.emitEvent(PluginEvent{
				Type:      "hook_execution_error",
				Plugin:    hookPlugin.Name(),
				Timestamp: time.Now(),
				Data:      map[string]interface{}{"hook": string(hookType)},
				Error:     err.Error(),
			})
		} else {
			// Update statistics
			m.mu.Lock()
			m.stats.PluginUsage[hookPlugin.Name()]++
			m.stats.HookExecutions[hookType]++
			duration := time.Since(pluginStart)
			if current, exists := m.stats.AverageExecTime[hookPlugin.Name()]; exists {
				m.stats.AverageExecTime[hookPlugin.Name()] = (current + duration) / 2
			} else {
				m.stats.AverageExecTime[hookPlugin.Name()] = duration
			}
			m.mu.Unlock()

			// Update plugin info
			if info, exists := m.plugins[hookPlugin.Name()]; exists {
				info.LastUsed = time.Now()
				info.UsageCount++
			}
		}
	}

	m.stats.LastExecution = time.Now()

	if len(errors) > 0 {
		return fmt.Errorf("hook execution failed for %d plugins: %v", len(errors), errors)
	}

	m.emitEvent(PluginEvent{
		Type:      "hook_executed",
		Plugin:    "system",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"hook":     string(hookType),
			"plugins":  len(hookPlugins),
			"duration": time.Since(start),
		},
	})

	return nil
}

// SendNotification sends a notification through all registered notification plugins
func (m *Manager) SendNotification(ctx context.Context, message NotificationMessage) error {
	m.mu.RLock()
	notificationPlugins := make([]NotificationPlugin, len(m.notifications))
	copy(notificationPlugins, m.notifications)
	m.mu.RUnlock()

	var errors []error

	for _, notificationPlugin := range notificationPlugins {
		if err := notificationPlugin.SendNotification(ctx, message); err != nil {
			errors = append(errors, fmt.Errorf("notification plugin %s failed: %w", notificationPlugin.Name(), err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("notification failed for %d plugins: %v", len(errors), errors)
	}

	return nil
}

// Transform applies transformations through all registered transform plugins
func (m *Manager) Transform(ctx context.Context, data TransformData) (TransformData, error) {
	m.mu.RLock()
	transformPlugins := make([]TransformPlugin, len(m.transforms))
	copy(transformPlugins, m.transforms)
	m.mu.RUnlock()

	result := data

	for _, transformPlugin := range transformPlugins {
		var err error
		result, err = transformPlugin.Transform(ctx, result)
		if err != nil {
			return data, fmt.Errorf("transform plugin %s failed: %w", transformPlugin.Name(), err)
		}
	}

	return result, nil
}

// GetPluginInfo returns information about a specific plugin
func (m *Manager) GetPluginInfo(name string) (*PluginInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.plugins[name]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found", name)
	}

	// Return a copy to avoid concurrent access issues
	infoCopy := *info
	return &infoCopy, nil
}

// ListPlugins returns information about all discovered plugins
func (m *Manager) ListPlugins() map[string]*PluginInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*PluginInfo, len(m.plugins))
	for name, info := range m.plugins {
		infoCopy := *info
		result[name] = &infoCopy
	}

	return result
}

// GetStats returns plugin system statistics
func (m *Manager) GetStats() *PluginStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid concurrent access issues
	statsCopy := *m.stats
	statsCopy.HookExecutions = make(map[HookType]int64)
	statsCopy.PluginUsage = make(map[string]int64)
	statsCopy.AverageExecTime = make(map[string]time.Duration)

	for k, v := range m.stats.HookExecutions {
		statsCopy.HookExecutions[k] = v
	}
	for k, v := range m.stats.PluginUsage {
		statsCopy.PluginUsage[k] = v
	}
	for k, v := range m.stats.AverageExecTime {
		statsCopy.AverageExecTime[k] = v
	}

	return &statsCopy
}

// AddEventListener adds a listener for plugin events
func (m *Manager) AddEventListener(listener func(PluginEvent)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventListeners = append(m.eventListeners, listener)
}

// Shutdown gracefully shuts down all plugins
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errors []error

	for name, info := range m.plugins {
		if info.Instance != nil {
			if err := info.Instance.Shutdown(ctx); err != nil {
				errors = append(errors, fmt.Errorf("failed to shutdown plugin %s: %w", name, err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("shutdown failed for %d plugins: %v", len(errors), errors)
	}

	return nil
}

// discoverPlugins discovers plugins in a directory
func (m *Manager) discoverPlugins(ctx context.Context, dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, skip
	}

	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			// Look for plugin manifest in subdirectories
			manifestPath := filepath.Join(path, "plugin.yaml")
			if _, err := os.Stat(manifestPath); err == nil {
				return m.loadPluginManifest(manifestPath)
			}
			return nil
		}

		// Look for standalone plugin files
		if filepath.Ext(path) == ".so" || filepath.Ext(path) == ".yaml" {
			if filepath.Base(path) == "plugin.yaml" {
				return m.loadPluginManifest(path)
			}
		}

		return nil
	})
}

// loadPluginManifest loads a plugin manifest from a file
func (m *Manager) loadPluginManifest(manifestPath string) error {
	manifestPath, err := common.ValidatePath(manifestPath, "")
	if err != nil {
		return fmt.Errorf("invalid manifest path: %w", err)
	}
	data, err := os.ReadFile(manifestPath) // #nosec G304
	if err != nil {
		return fmt.Errorf("failed to read manifest %s: %w", manifestPath, err)
	}

	var manifest PluginManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("failed to parse manifest %s: %w", manifestPath, err)
	}

	// Validate manifest
	if manifest.Name == "" {
		return fmt.Errorf("plugin manifest %s missing name", manifestPath)
	}
	if manifest.Version == "" {
		return fmt.Errorf("plugin manifest %s missing version", manifestPath)
	}

	pluginInfo := &PluginInfo{
		Manifest: manifest,
		Enabled:  false, // Plugins are disabled by default until configured
	}

	m.plugins[manifest.Name] = pluginInfo
	return nil
}

// loadPluginInstance loads the actual plugin instance
func (m *Manager) loadPluginInstance(manifest PluginManifest) (PluginInterface, error) {
	// For now, we'll implement a basic plugin loading mechanism
	// In a real implementation, this would load shared libraries or use other plugin mechanisms
	
	// This is a placeholder for the actual plugin loading logic
	// The real implementation would depend on how plugins are packaged and distributed
	return nil, fmt.Errorf("plugin loading not yet implemented for %s", manifest.Name)
}

// registerPlugin registers a plugin instance with the appropriate handlers
func (m *Manager) registerPlugin(instance PluginInterface) error {
	// Register hook plugins
	if hookPlugin, ok := instance.(HookPlugin); ok {
		for _, hookType := range hookPlugin.SupportedHooks() {
			m.hooks[hookType] = append(m.hooks[hookType], hookPlugin)
		}
	}

	// Register notification plugins
	if notificationPlugin, ok := instance.(NotificationPlugin); ok {
		m.notifications = append(m.notifications, notificationPlugin)
	}

	// Register transform plugins
	if transformPlugin, ok := instance.(TransformPlugin); ok {
		m.transforms = append(m.transforms, transformPlugin)
	}

	return nil
}

// unregisterPlugin unregisters a plugin instance from handlers
func (m *Manager) unregisterPlugin(instance PluginInterface) {
	// Unregister hook plugins
	if hookPlugin, ok := instance.(HookPlugin); ok {
		for hookType, plugins := range m.hooks {
			var filtered []HookPlugin
			for _, p := range plugins {
				if p.Name() != hookPlugin.Name() {
					filtered = append(filtered, p)
				}
			}
			m.hooks[hookType] = filtered
		}
	}

	// Unregister notification plugins
	if notificationPlugin, ok := instance.(NotificationPlugin); ok {
		var filtered []NotificationPlugin
		for _, p := range m.notifications {
			if p.Name() != notificationPlugin.Name() {
				filtered = append(filtered, p)
			}
		}
		m.notifications = filtered
	}

	// Unregister transform plugins
	if transformPlugin, ok := instance.(TransformPlugin); ok {
		var filtered []TransformPlugin
		for _, p := range m.transforms {
			if p.Name() != transformPlugin.Name() {
				filtered = append(filtered, p)
			}
		}
		m.transforms = filtered
	}
}

// emitEvent emits a plugin event to all listeners
func (m *Manager) emitEvent(event PluginEvent) {
	// Add to error log if it's an error event
	if event.Error != "" {
		m.stats.Errors = append(m.stats.Errors, event)
		// Keep only the last 100 errors
		if len(m.stats.Errors) > 100 {
			m.stats.Errors = m.stats.Errors[1:]
		}
	}

	// Notify listeners
	for _, listener := range m.eventListeners {
		go listener(event) // Run in goroutine to avoid blocking
	}
}

// SetPluginInfo sets plugin info (for builtin plugins)
func (m *Manager) SetPluginInfo(name string, info *PluginInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.plugins[name] = info
}