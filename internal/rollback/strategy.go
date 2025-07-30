package rollback

import (
	"context"
	"fmt"
	"sync"
	"time"

	"flakedrop/pkg/errors"
)

// StrategyManager manages recovery strategies for different failure scenarios
type StrategyManager struct {
	strategies map[FailureScenario]*RecoveryStrategy
	mu         sync.RWMutex
}

// NewStrategyManager creates a new strategy manager with default strategies
func NewStrategyManager() *StrategyManager {
	sm := &StrategyManager{
		strategies: make(map[FailureScenario]*RecoveryStrategy),
	}

	// Register default strategies
	sm.registerDefaultStrategies()

	return sm
}

// GetStrategy retrieves a recovery strategy for a failure scenario
func (sm *StrategyManager) GetStrategy(scenario FailureScenario) (*RecoveryStrategy, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	strategy, exists := sm.strategies[scenario]
	if !exists {
		return nil, errors.New(errors.ErrCodeNotFound, "no recovery strategy found").
			WithContext("scenario", string(scenario))
	}

	return strategy, nil
}

// RegisterStrategy registers a custom recovery strategy
func (sm *StrategyManager) RegisterStrategy(scenario FailureScenario, strategy *RecoveryStrategy) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.strategies[scenario] = strategy
}

// ExecuteStrategy executes a recovery strategy
func (sm *StrategyManager) ExecuteStrategy(ctx context.Context, scenario FailureScenario, context map[string]interface{}) (*StrategyExecutionResult, error) {
	strategy, err := sm.GetStrategy(scenario)
	if err != nil {
		return nil, err
	}

	result := &StrategyExecutionResult{
		Scenario:  scenario,
		StartTime: time.Now(),
		Steps:     []StepExecutionResult{},
		Success:   true,
	}

	// Execute each recovery step
	for _, step := range strategy.Steps {
		stepResult := sm.executeStep(ctx, step, context, strategy)
		result.Steps = append(result.Steps, stepResult)

		if !stepResult.Success {
			result.Success = false
			
			switch step.OnFailure {
			case "abort":
				result.AbortReason = fmt.Sprintf("Step %d failed: %s", step.Order, stepResult.Error)
			case "retry":
				// Retry logic would go here
				continue
			case "continue":
				// Continue to next step
				continue
			}
			break
		}
	}

	result.EndTime = timePtr(time.Now())
	return result, nil
}

// Private methods

func (sm *StrategyManager) registerDefaultStrategies() {
	// Partial deployment failure strategy
	sm.strategies[FailurePartialDeployment] = &RecoveryStrategy{
		Scenario:    FailurePartialDeployment,
		AutoRecover: true,
		MaxRetries:  3,
		RetryDelay:  5 * time.Second,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "identify_failed_files",
				Description: "Identify which files failed to deploy",
				Timeout:     30 * time.Second,
				OnFailure:   "abort",
			},
			{
				Order:       2,
				Action:      "create_savepoint",
				Description: "Create a savepoint for partial rollback",
				Timeout:     10 * time.Second,
				OnFailure:   "abort",
			},
			{
				Order:       3,
				Action:      "rollback_failed_files",
				Description: "Rollback only the failed file changes",
				Timeout:     5 * time.Minute,
				OnFailure:   "continue",
			},
			{
				Order:       4,
				Action:      "verify_consistency",
				Description: "Verify schema consistency after partial rollback",
				Timeout:     2 * time.Minute,
				OnFailure:   "continue",
			},
		},
		Notification: true,
	}

	// Connection lost strategy
	sm.strategies[FailureConnectionLost] = &RecoveryStrategy{
		Scenario:    FailureConnectionLost,
		AutoRecover: true,
		MaxRetries:  5,
		RetryDelay:  10 * time.Second,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "wait_and_reconnect",
				Description: "Wait for network stability and attempt reconnection",
				Timeout:     2 * time.Minute,
				OnFailure:   "retry",
			},
			{
				Order:       2,
				Action:      "check_deployment_state",
				Description: "Check the state of the interrupted deployment",
				Timeout:     30 * time.Second,
				OnFailure:   "abort",
			},
			{
				Order:       3,
				Action:      "resume_or_rollback",
				Description: "Resume deployment if safe, otherwise rollback",
				Timeout:     5 * time.Minute,
				OnFailure:   "abort",
			},
		},
		Notification: true,
	}

	// Syntax error strategy
	sm.strategies[FailureSyntaxError] = &RecoveryStrategy{
		Scenario:    FailureSyntaxError,
		AutoRecover: false,
		MaxRetries:  0,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "identify_syntax_error",
				Description: "Identify the file and line with syntax error",
				Timeout:     10 * time.Second,
				OnFailure:   "continue",
			},
			{
				Order:       2,
				Action:      "rollback_file",
				Description: "Rollback the file with syntax error",
				Timeout:     1 * time.Minute,
				OnFailure:   "abort",
			},
			{
				Order:       3,
				Action:      "notify_developer",
				Description: "Notify developer about the syntax error",
				Timeout:     30 * time.Second,
				OnFailure:   "continue",
			},
		},
		Notification: true,
	}

	// Permission denied strategy
	sm.strategies[FailurePermissionDenied] = &RecoveryStrategy{
		Scenario:    FailurePermissionDenied,
		AutoRecover: false,
		MaxRetries:  2,
		RetryDelay:  5 * time.Second,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "check_permissions",
				Description: "Check current user permissions",
				Timeout:     30 * time.Second,
				OnFailure:   "continue",
			},
			{
				Order:       2,
				Action:      "request_permission_grant",
				Description: "Request necessary permissions if possible",
				Timeout:     2 * time.Minute,
				OnFailure:   "abort",
			},
			{
				Order:       3,
				Action:      "retry_with_permissions",
				Description: "Retry the operation with updated permissions",
				Timeout:     3 * time.Minute,
				OnFailure:   "abort",
			},
		},
		Notification: true,
	}

	// Constraint violation strategy
	sm.strategies[FailureConstraintViolation] = &RecoveryStrategy{
		Scenario:    FailureConstraintViolation,
		AutoRecover: false,
		MaxRetries:  0,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "identify_constraint",
				Description: "Identify the violated constraint",
				Timeout:     30 * time.Second,
				OnFailure:   "continue",
			},
			{
				Order:       2,
				Action:      "check_data_integrity",
				Description: "Check data integrity and dependencies",
				Timeout:     2 * time.Minute,
				OnFailure:   "continue",
			},
			{
				Order:       3,
				Action:      "rollback_transaction",
				Description: "Rollback the entire transaction",
				Timeout:     1 * time.Minute,
				OnFailure:   "abort",
			},
		},
		Notification: true,
	}

	// Timeout strategy
	sm.strategies[FailureTimeout] = &RecoveryStrategy{
		Scenario:    FailureTimeout,
		AutoRecover: true,
		MaxRetries:  2,
		RetryDelay:  30 * time.Second,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "check_long_running_queries",
				Description: "Check for long-running queries",
				Timeout:     1 * time.Minute,
				OnFailure:   "continue",
			},
			{
				Order:       2,
				Action:      "kill_blocking_sessions",
				Description: "Kill blocking sessions if safe",
				Timeout:     30 * time.Second,
				OnFailure:   "continue",
			},
			{
				Order:       3,
				Action:      "retry_with_extended_timeout",
				Description: "Retry operation with extended timeout",
				Timeout:     10 * time.Minute,
				OnFailure:   "abort",
			},
		},
		Notification: true,
	}

	// Resource limit strategy
	sm.strategies[FailureResourceLimit] = &RecoveryStrategy{
		Scenario:    FailureResourceLimit,
		AutoRecover: true,
		MaxRetries:  1,
		RetryDelay:  1 * time.Minute,
		Steps: []RecoveryStep{
			{
				Order:       1,
				Action:      "check_resource_usage",
				Description: "Check current resource usage",
				Timeout:     30 * time.Second,
				OnFailure:   "continue",
			},
			{
				Order:       2,
				Action:      "scale_warehouse",
				Description: "Scale up warehouse if possible",
				Timeout:     2 * time.Minute,
				OnFailure:   "continue",
			},
			{
				Order:       3,
				Action:      "retry_with_scaled_resources",
				Description: "Retry operation with scaled resources",
				Timeout:     5 * time.Minute,
				OnFailure:   "abort",
			},
		},
		Notification: true,
	}
}

func (sm *StrategyManager) executeStep(ctx context.Context, step RecoveryStep, contextData map[string]interface{}, strategy *RecoveryStrategy) StepExecutionResult {
	result := StepExecutionResult{
		Step:      step,
		StartTime: time.Now(),
		Success:   true,
	}

	// Create timeout context
	stepCtx, cancel := context.WithTimeout(ctx, step.Timeout)
	defer cancel()

	// Execute step action
	err := sm.executeAction(stepCtx, step.Action, contextData)
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		
		// Handle retries if configured
		if strategy.MaxRetries > 0 && step.OnFailure == "retry" {
			for i := 0; i < strategy.MaxRetries; i++ {
				time.Sleep(strategy.RetryDelay)
				
				retryErr := sm.executeAction(stepCtx, step.Action, contextData)
				if retryErr == nil {
					result.Success = true
					result.Retries = i + 1
					break
				}
			}
		}
	}

	result.EndTime = timePtr(time.Now())
	return result
}

func (sm *StrategyManager) executeAction(ctx context.Context, action string, context map[string]interface{}) error {
	// This would contain the actual implementation of each action
	// For now, we'll simulate the actions
	
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		// Simulate action execution
		return nil
	}
}

// Helper types

// StrategyExecutionResult represents the result of executing a recovery strategy
type StrategyExecutionResult struct {
	Scenario    FailureScenario
	StartTime   time.Time
	EndTime     *time.Time
	Steps       []StepExecutionResult
	Success     bool
	AbortReason string
}

// StepExecutionResult represents the result of executing a recovery step
type StepExecutionResult struct {
	Step      RecoveryStep
	StartTime time.Time
	EndTime   *time.Time
	Success   bool
	Error     string
	Retries   int
}