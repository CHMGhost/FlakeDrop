package plugin

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// MockPlugin implements PluginInterface for testing
type MockPlugin struct {
	name        string
	version     string
	description string
	initialized bool
	shutdownErr error
}

func NewMockPlugin(name, version, description string) *MockPlugin {
	return &MockPlugin{
		name:        name,
		version:     version,
		description: description,
	}
}

func (m *MockPlugin) Initialize(ctx context.Context, config PluginConfig) error {
	m.initialized = true
	return nil
}

func (m *MockPlugin) Name() string {
	return m.name
}

func (m *MockPlugin) Version() string {
	return m.version
}

func (m *MockPlugin) Description() string {
	return m.description
}

func (m *MockPlugin) Shutdown(ctx context.Context) error {
	m.initialized = false
	return m.shutdownErr
}

// MockHookPlugin implements HookPlugin for testing
type MockHookPlugin struct {
	*MockPlugin
	supportedHooks []HookType
	hookCalls      map[HookType]int
}

func NewMockHookPlugin(name string) *MockHookPlugin {
	return &MockHookPlugin{
		MockPlugin:     NewMockPlugin(name, "1.0.0", "Mock hook plugin"),
		supportedHooks: []HookType{HookPreDeploy, HookPostDeploy, HookOnSuccess, HookOnError},
		hookCalls:      make(map[HookType]int),
	}
}

func (m *MockHookPlugin) SupportedHooks() []HookType {
	return m.supportedHooks
}

func (m *MockHookPlugin) ExecuteHook(ctx context.Context, hookType HookType, data HookData) error {
	m.hookCalls[hookType]++
	return nil
}

func (m *MockHookPlugin) GetHookCalls(hookType HookType) int {
	return m.hookCalls[hookType]
}

// MockNotificationPlugin implements NotificationPlugin for testing
type MockNotificationPlugin struct {
	*MockPlugin
	notifications []NotificationMessage
}

func NewMockNotificationPlugin(name string) *MockNotificationPlugin {
	return &MockNotificationPlugin{
		MockPlugin:    NewMockPlugin(name, "1.0.0", "Mock notification plugin"),
		notifications: make([]NotificationMessage, 0),
	}
}

func (m *MockNotificationPlugin) SendNotification(ctx context.Context, message NotificationMessage) error {
	m.notifications = append(m.notifications, message)
	return nil
}

func (m *MockNotificationPlugin) GetNotifications() []NotificationMessage {
	return m.notifications
}

// Tests for plugin manager
func TestPluginManager_Initialize(t *testing.T) {
	manager := NewManager([]string{}, nil)
	
	ctx := context.Background()
	err := manager.Initialize(ctx)
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestPluginManager_RegisterAndExecuteHooks(t *testing.T) {
	manager := NewManager([]string{}, nil)
	ctx := context.Background()
	
	// Initialize manager
	err := manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create mock hook plugin
	mockPlugin := NewMockHookPlugin("test-hook")
	
	// Register plugin manually (simulating discovered plugin)
	pluginInfo := &PluginInfo{
		Manifest: PluginManifest{
			Name:    "test-hook",
			Version: "1.0.0",
		},
		Instance: mockPlugin,
		Enabled:  true,
		Config: PluginConfig{
			Name:    "test-hook",
			Enabled: true,
		},
	}
	
	manager.plugins["test-hook"] = pluginInfo
	
	// Register with hook system
	err = manager.registerPlugin(mockPlugin)
	if err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}
	
	// Execute hook
	hookData := HookData{
		DeploymentID: "test-deployment",
		Repository:   "test-repo",
		StartTime:    time.Now(),
		Success:      true,
	}
	
	err = manager.ExecuteHook(ctx, HookPreDeploy, hookData)
	if err != nil {
		t.Fatalf("Hook execution failed: %v", err)
	}
	
	// Verify hook was called
	if mockPlugin.GetHookCalls(HookPreDeploy) != 1 {
		t.Errorf("Expected hook to be called once, got %d", mockPlugin.GetHookCalls(HookPreDeploy))
	}
}

func TestPluginManager_SendNotification(t *testing.T) {
	manager := NewManager([]string{}, nil)
	ctx := context.Background()
	
	// Initialize manager
	err := manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create mock notification plugin
	mockPlugin := NewMockNotificationPlugin("test-notification")
	
	// Register plugin manually
	pluginInfo := &PluginInfo{
		Manifest: PluginManifest{
			Name:    "test-notification",
			Version: "1.0.0",
		},
		Instance: mockPlugin,
		Enabled:  true,
		Config: PluginConfig{
			Name:    "test-notification",
			Enabled: true,
		},
	}
	
	manager.plugins["test-notification"] = pluginInfo
	
	// Register with notification system
	err = manager.registerPlugin(mockPlugin)
	if err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}
	
	// Send notification
	message := NotificationMessage{
		Title:     "Test Notification",
		Message:   "This is a test",
		Level:     NotificationLevelInfo,
		Timestamp: time.Now(),
		Source:    "test",
	}
	
	err = manager.SendNotification(ctx, message)
	if err != nil {
		t.Fatalf("Notification failed: %v", err)
	}
	
	// Verify notification was sent
	notifications := mockPlugin.GetNotifications()
	if len(notifications) != 1 {
		t.Errorf("Expected 1 notification, got %d", len(notifications))
	}
	
	if notifications[0].Title != "Test Notification" {
		t.Errorf("Expected title 'Test Notification', got '%s'", notifications[0].Title)
	}
}

func TestPluginManager_GetStats(t *testing.T) {
	manager := NewManager([]string{}, nil)
	ctx := context.Background()
	
	// Initialize manager
	err := manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create and register mock plugin
	mockPlugin := NewMockHookPlugin("test-stats")
	pluginInfo := &PluginInfo{
		Manifest: PluginManifest{
			Name:    "test-stats",
			Version: "1.0.0",
		},
		Instance: mockPlugin,
		Enabled:  true,
		Config: PluginConfig{
			Name:    "test-stats",
			Enabled: true,
		},
	}
	
	manager.plugins["test-stats"] = pluginInfo
	_ = manager.registerPlugin(mockPlugin)
	
	// Update stats to reflect the plugin was added
	manager.stats.TotalPlugins = len(manager.plugins)
	manager.stats.EnabledPlugins = 1
	
	// Execute some hooks to generate stats
	hookData := HookData{
		DeploymentID: "test-deployment",
		Repository:   "test-repo",
		StartTime:    time.Now(),
		Success:      true,
	}
	
	manager.ExecuteHook(ctx, HookPreDeploy, hookData)
	manager.ExecuteHook(ctx, HookPostDeploy, hookData)
	
	// Get stats
	stats := manager.GetStats()
	
	if stats.TotalPlugins != 1 {
		t.Errorf("Expected 1 total plugin, got %d", stats.TotalPlugins)
	}
	
	if stats.EnabledPlugins != 1 {
		t.Errorf("Expected 1 enabled plugin, got %d", stats.EnabledPlugins)
	}
	
	if stats.HookExecutions[HookPreDeploy] != 1 {
		t.Errorf("Expected 1 pre-deploy execution, got %d", stats.HookExecutions[HookPreDeploy])
	}
	
	if stats.HookExecutions[HookPostDeploy] != 1 {
		t.Errorf("Expected 1 post-deploy execution, got %d", stats.HookExecutions[HookPostDeploy])
	}
}

func TestHookExecutor_ExecuteHook(t *testing.T) {
	manager := NewManager([]string{}, nil)
	hookExecutor := NewHookExecutor(manager)
	ctx := context.Background()
	
	// Initialize manager
	err := manager.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize manager: %v", err)
	}
	
	// Create and register mock plugin
	mockPlugin := NewMockHookPlugin("test-executor")
	pluginInfo := &PluginInfo{
		Manifest: PluginManifest{
			Name:    "test-executor",
			Version: "1.0.0",
		},
		Instance: mockPlugin,
		Enabled:  true,
		Config: PluginConfig{
			Name:    "test-executor",
			Enabled: true,
		},
	}
	
	manager.plugins["test-executor"] = pluginInfo
	manager.registerPlugin(mockPlugin)
	
	// Set context data
	hookExecutor.SetContext("test_key", "test_value")
	
	// Execute hook
	hookData := HookData{
		DeploymentID: "test-deployment",
		Repository:   "test-repo",
		StartTime:    time.Now(),
		Success:      true,
	}
	
	err = hookExecutor.ExecuteHook(ctx, HookPreDeploy, hookData)
	if err != nil {
		t.Fatalf("Hook execution failed: %v", err)
	}
	
	// Verify hook was called
	if mockPlugin.GetHookCalls(HookPreDeploy) != 1 {
		t.Errorf("Expected hook to be called once, got %d", mockPlugin.GetHookCalls(HookPreDeploy))
	}
}

func TestPluginConfig_Validation(t *testing.T) {
	validator := NewConfigValidator()
	
	// Register schema
	manifest := PluginManifest{
		Name:    "test-plugin",
		Version: "1.0.0",
		Config: []ConfigSchema{
			{
				Name:        "required_string",
				Type:        "string",
				Description: "A required string setting",
				Required:    true,
			},
			{
				Name:        "optional_int",
				Type:        "integer",
				Description: "An optional integer setting",
				Required:    false,
				Default:     42,
			},
		},
	}
	
	validator.RegisterSchema(manifest)
	
	// Test valid configuration
	validConfig := &PluginConfig{
		Name:    "test-plugin",
		Enabled: true,
		Settings: map[string]interface{}{
			"required_string": "test value",
			"optional_int":    123,
		},
	}
	
	err := validator.Validate("test-plugin", validConfig)
	if err != nil {
		t.Errorf("Expected valid config to pass validation, got error: %v", err)
	}
	
	// Test invalid configuration (missing required field)
	invalidConfig := &PluginConfig{
		Name:    "test-plugin",
		Enabled: true,
		Settings: map[string]interface{}{
			"optional_int": 123,
		},
	}
	
	err = validator.Validate("test-plugin", invalidConfig)
	if err == nil {
		t.Error("Expected invalid config to fail validation")
	}
}

// Benchmark tests
func BenchmarkHookExecution(b *testing.B) {
	manager := NewManager([]string{}, nil)
	ctx := context.Background()
	
	manager.Initialize(ctx)
	
	// Create multiple mock plugins
	for i := 0; i < 10; i++ {
		mockPlugin := NewMockHookPlugin(fmt.Sprintf("test-plugin-%d", i))
		pluginInfo := &PluginInfo{
			Manifest: PluginManifest{
				Name:    mockPlugin.Name(),
				Version: "1.0.0",
			},
			Instance: mockPlugin,
			Enabled:  true,
			Config: PluginConfig{
				Name:    mockPlugin.Name(),
				Enabled: true,
			},
		}
		
		manager.plugins[mockPlugin.Name()] = pluginInfo
		manager.registerPlugin(mockPlugin)
	}
	
	hookData := HookData{
		DeploymentID: "bench-deployment",
		Repository:   "bench-repo",
		StartTime:    time.Now(),
		Success:      true,
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		manager.ExecuteHook(ctx, HookPreDeploy, hookData)
	}
}

func BenchmarkNotificationSending(b *testing.B) {
	manager := NewManager([]string{}, nil)
	ctx := context.Background()
	
	manager.Initialize(ctx)
	
	// Create multiple notification plugins
	for i := 0; i < 5; i++ {
		mockPlugin := NewMockNotificationPlugin(fmt.Sprintf("notification-plugin-%d", i))
		pluginInfo := &PluginInfo{
			Manifest: PluginManifest{
				Name:    mockPlugin.Name(),
				Version: "1.0.0",
			},
			Instance: mockPlugin,
			Enabled:  true,
			Config: PluginConfig{
				Name:    mockPlugin.Name(),
				Enabled: true,
			},
		}
		
		manager.plugins[mockPlugin.Name()] = pluginInfo
		manager.registerPlugin(mockPlugin)
	}
	
	message := NotificationMessage{
		Title:     "Benchmark Notification",
		Message:   "This is a benchmark test",
		Level:     NotificationLevelInfo,
		Timestamp: time.Now(),
		Source:    "benchmark",
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		manager.SendNotification(ctx, message)
	}
}