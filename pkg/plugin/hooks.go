package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// HookExecutor manages the execution of hooks in the deployment lifecycle
type HookExecutor struct {
	manager    *Manager
	context    map[string]interface{}
	mu         sync.RWMutex
	logger     HookLogger
	middleware []HookMiddleware
}

// HookLogger defines the interface for hook logging
type HookLogger interface {
	LogHookStart(hookType HookType, pluginName string, data HookData)
	LogHookComplete(hookType HookType, pluginName string, duration time.Duration, err error)
	LogHookSkipped(hookType HookType, pluginName string, reason string)
}

// HookMiddleware defines middleware that can be applied to hook execution
type HookMiddleware interface {
	Before(ctx context.Context, hookType HookType, pluginName string, data HookData) (HookData, error)
	After(ctx context.Context, hookType HookType, pluginName string, data HookData, err error) error
}

// DefaultHookLogger provides a default implementation of HookLogger
type DefaultHookLogger struct{}

func (l *DefaultHookLogger) LogHookStart(hookType HookType, pluginName string, data HookData) {
	fmt.Printf("[HOOK] Starting %s hook for plugin %s (deployment: %s)\n", 
		hookType, pluginName, data.DeploymentID)
}

func (l *DefaultHookLogger) LogHookComplete(hookType HookType, pluginName string, duration time.Duration, err error) {
	if err != nil {
		fmt.Printf("[HOOK] Failed %s hook for plugin %s in %s: %v\n", 
			hookType, pluginName, duration, err)
	} else {
		fmt.Printf("[HOOK] Completed %s hook for plugin %s in %s\n", 
			hookType, pluginName, duration)
	}
}

func (l *DefaultHookLogger) LogHookSkipped(hookType HookType, pluginName string, reason string) {
	fmt.Printf("[HOOK] Skipped %s hook for plugin %s: %s\n", 
		hookType, pluginName, reason)
}

// NewHookExecutor creates a new hook executor
func NewHookExecutor(manager *Manager) *HookExecutor {
	return &HookExecutor{
		manager:    manager,
		context:    make(map[string]interface{}),
		logger:     &DefaultHookLogger{},
		middleware: make([]HookMiddleware, 0),
	}
}

// SetLogger sets the hook logger
func (h *HookExecutor) SetLogger(logger HookLogger) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logger = logger
}

// AddMiddleware adds middleware to the hook execution chain
func (h *HookExecutor) AddMiddleware(middleware HookMiddleware) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.middleware = append(h.middleware, middleware)
}

// SetContext sets a context value that will be available to all hooks
func (h *HookExecutor) SetContext(key string, value interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.context[key] = value
}

// GetContext gets a context value
func (h *HookExecutor) GetContext(key string) (interface{}, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	value, exists := h.context[key]
	return value, exists
}

// ExecuteHook executes a specific hook with enhanced features
func (h *HookExecutor) ExecuteHook(ctx context.Context, hookType HookType, data HookData) error {
	// Add context data to hook data
	h.mu.RLock()
	for key, value := range h.context {
		if data.Metadata == nil {
			data.Metadata = make(map[string]interface{})
		}
		data.Metadata[key] = value
	}
	middleware := make([]HookMiddleware, len(h.middleware))
	copy(middleware, h.middleware)
	logger := h.logger
	h.mu.RUnlock()

	// Get plugins for this hook type
	plugins := h.manager.getHookPlugins(hookType)
	if len(plugins) == 0 {
		return nil
	}

	var errors []error
	
	for _, plugin := range plugins {
		pluginName := plugin.Name()
		
		// Check if plugin should be skipped
		if !h.shouldExecutePlugin(plugin, hookType, data) {
			logger.LogHookSkipped(hookType, pluginName, "plugin disabled or conditions not met")
			continue
		}

		start := time.Now()
		logger.LogHookStart(hookType, pluginName, data)

		// Apply before middleware
		processedData := data
		for _, mw := range middleware {
			var err error
			processedData, err = mw.Before(ctx, hookType, pluginName, processedData)
			if err != nil {
				errors = append(errors, fmt.Errorf("middleware before failed for %s: %w", pluginName, err))
				continue
			}
		}

		// Execute the hook
		var hookErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					hookErr = fmt.Errorf("plugin %s panicked: %v", pluginName, r)
				}
			}()

			hookErr = plugin.ExecuteHook(ctx, hookType, processedData)
		}()

		duration := time.Since(start)
		logger.LogHookComplete(hookType, pluginName, duration, hookErr)

		// Apply after middleware
		for _, mw := range middleware {
			if err := mw.After(ctx, hookType, pluginName, processedData, hookErr); err != nil {
				errors = append(errors, fmt.Errorf("middleware after failed for %s: %w", pluginName, err))
			}
		}

		if hookErr != nil {
			errors = append(errors, fmt.Errorf("plugin %s failed: %w", pluginName, hookErr))
		}
	}

	if len(errors) > 0 {
		return &HookExecutionError{
			HookType: hookType,
			Errors:   errors,
		}
	}

	return nil
}

// ExecuteHookChain executes a chain of hooks in sequence
func (h *HookExecutor) ExecuteHookChain(ctx context.Context, hooks []HookType, data HookData) error {
	for _, hookType := range hooks {
		if err := h.ExecuteHook(ctx, hookType, data); err != nil {
			return fmt.Errorf("hook chain failed at %s: %w", hookType, err)
		}
		
		// Update timing for next hook
		data.StartTime = time.Now()
	}
	return nil
}

// ExecuteHookParallel executes multiple hooks in parallel
func (h *HookExecutor) ExecuteHookParallel(ctx context.Context, hooks []HookType, data HookData) error {
	if len(hooks) == 0 {
		return nil
	}

	errChan := make(chan error, len(hooks))
	var wg sync.WaitGroup

	for _, hookType := range hooks {
		wg.Add(1)
		go func(ht HookType) {
			defer wg.Done()
			if err := h.ExecuteHook(ctx, ht, data); err != nil {
				errChan <- fmt.Errorf("parallel hook %s failed: %w", ht, err)
			}
		}(hookType)
	}

	wg.Wait()
	close(errChan)

	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return &HookExecutionError{
			HookType: "parallel_execution",
			Errors:   errors,
		}
	}

	return nil
}

// shouldExecutePlugin determines if a plugin should be executed for a hook
func (h *HookExecutor) shouldExecutePlugin(plugin HookPlugin, hookType HookType, data HookData) bool {
	// Check if plugin is enabled
	info, err := h.manager.GetPluginInfo(plugin.Name())
	if err != nil || !info.Enabled {
		return false
	}

	// Check if plugin supports this hook type
	supportedHooks := plugin.SupportedHooks()
	for _, supported := range supportedHooks {
		if supported == hookType {
			return true
		}
	}

	return false
}

// HookExecutionError represents an error during hook execution
type HookExecutionError struct {
	HookType HookType
	Errors   []error
}

func (e *HookExecutionError) Error() string {
	return fmt.Sprintf("hook execution failed for %s: %d errors occurred", e.HookType, len(e.Errors))
}

func (e *HookExecutionError) Unwrap() []error {
	return e.Errors
}

// ConditionalHookMiddleware provides conditional execution based on deployment data
type ConditionalHookMiddleware struct {
	conditions map[string]func(HookData) bool
}

// NewConditionalHookMiddleware creates a new conditional hook middleware
func NewConditionalHookMiddleware() *ConditionalHookMiddleware {
	return &ConditionalHookMiddleware{
		conditions: make(map[string]func(HookData) bool),
	}
}

// AddCondition adds a condition for plugin execution
func (m *ConditionalHookMiddleware) AddCondition(pluginName string, condition func(HookData) bool) {
	m.conditions[pluginName] = condition
}

func (m *ConditionalHookMiddleware) Before(ctx context.Context, hookType HookType, pluginName string, data HookData) (HookData, error) {
	if condition, exists := m.conditions[pluginName]; exists {
		if !condition(data) {
			return data, fmt.Errorf("condition not met, skipping plugin")
		}
	}
	return data, nil
}

func (m *ConditionalHookMiddleware) After(ctx context.Context, hookType HookType, pluginName string, data HookData, err error) error {
	return nil // No post-processing needed
}

// TimingHookMiddleware provides timing and performance monitoring
type TimingHookMiddleware struct {
	timings map[string][]time.Duration
	mu      sync.RWMutex
}

// NewTimingHookMiddleware creates a new timing middleware
func NewTimingHookMiddleware() *TimingHookMiddleware {
	return &TimingHookMiddleware{
		timings: make(map[string][]time.Duration),
	}
}

func (m *TimingHookMiddleware) Before(ctx context.Context, hookType HookType, pluginName string, data HookData) (HookData, error) {
	// Store start time in context
	data.StartTime = time.Now()
	return data, nil
}

func (m *TimingHookMiddleware) After(ctx context.Context, hookType HookType, pluginName string, data HookData, err error) error {
	duration := time.Since(data.StartTime)
	
	m.mu.Lock()
	key := fmt.Sprintf("%s:%s", hookType, pluginName)
	m.timings[key] = append(m.timings[key], duration)
	// Keep only last 100 timings
	if len(m.timings[key]) > 100 {
		m.timings[key] = m.timings[key][1:]
	}
	m.mu.Unlock()
	
	return nil
}

// GetAverageTime returns the average execution time for a plugin/hook combination
func (m *TimingHookMiddleware) GetAverageTime(hookType HookType, pluginName string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	key := fmt.Sprintf("%s:%s", hookType, pluginName)
	timings, exists := m.timings[key]
	if !exists || len(timings) == 0 {
		return 0
	}
	
	var total time.Duration
	for _, t := range timings {
		total += t
	}
	
	return total / time.Duration(len(timings))
}

// RetryHookMiddleware provides retry logic for failed hooks
type RetryHookMiddleware struct {
	maxRetries map[string]int
	backoff    time.Duration
}

// NewRetryHookMiddleware creates a new retry middleware
func NewRetryHookMiddleware(defaultMaxRetries int, backoff time.Duration) *RetryHookMiddleware {
	return &RetryHookMiddleware{
		maxRetries: make(map[string]int),
		backoff:    backoff,
	}
}

// SetRetries sets the maximum number of retries for a specific plugin
func (m *RetryHookMiddleware) SetRetries(pluginName string, maxRetries int) {
	m.maxRetries[pluginName] = maxRetries
}

func (m *RetryHookMiddleware) Before(ctx context.Context, hookType HookType, pluginName string, data HookData) (HookData, error) {
	return data, nil
}

func (m *RetryHookMiddleware) After(ctx context.Context, hookType HookType, pluginName string, data HookData, err error) error {
	if err == nil {
		return nil
	}

	_, exists := m.maxRetries[pluginName]
	if !exists {
		// maxRetries = 3 // Default - will be used in future implementation
	}

	// This is a simplified retry logic
	// In a real implementation, you'd want to track retry counts and implement actual retry logic
	// TODO: Implement actual retry logic using maxRetries
	return err
}

// Helper method to get hook plugins from manager
func (m *Manager) getHookPlugins(hookType HookType) []HookPlugin {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	plugins, exists := m.hooks[hookType]
	if !exists {
		return nil
	}
	
	// Return a copy to avoid concurrent access issues
	result := make([]HookPlugin, len(plugins))
	copy(result, plugins)
	return result
}