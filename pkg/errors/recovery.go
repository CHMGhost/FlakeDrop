package errors

import (
	"context"
	"fmt"
	"os"
	"time"
)

// RecoveryStrategy defines a strategy for recovering from errors
type RecoveryStrategy interface {
	Recover(ctx context.Context, err error) error
	CanRecover(err error) bool
}

// RecoveryManager manages multiple recovery strategies
type RecoveryManager struct {
	strategies map[ErrorCode]RecoveryStrategy
	handler    *ErrorHandler
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(handler *ErrorHandler) *RecoveryManager {
	rm := &RecoveryManager{
		strategies: make(map[ErrorCode]RecoveryStrategy),
		handler:    handler,
	}
	
	// Register default recovery strategies
	rm.RegisterStrategy(ErrCodeConnectionTimeout, &ConnectionRecoveryStrategy{})
	rm.RegisterStrategy(ErrCodeAuthenticationFailed, &AuthenticationRecoveryStrategy{})
	rm.RegisterStrategy(ErrCodeConfigMissing, &ConfigRecoveryStrategy{})
	rm.RegisterStrategy(ErrCodeDiskSpace, &DiskSpaceRecoveryStrategy{})
	
	return rm
}

// RegisterStrategy registers a recovery strategy for an error code
func (rm *RecoveryManager) RegisterStrategy(code ErrorCode, strategy RecoveryStrategy) {
	rm.strategies[code] = strategy
}

// Recover attempts to recover from an error
func (rm *RecoveryManager) Recover(ctx context.Context, err error) error {
	code := GetErrorCode(err)
	
	strategy, exists := rm.strategies[code]
	if !exists || !strategy.CanRecover(err) {
		return err
	}
	
	// Log recovery attempt
	rm.handler.Handle(New(ErrCodeInternal, fmt.Sprintf("Attempting recovery for error: %s", code)).
		WithSeverity(SeverityInfo))
	
	// Attempt recovery with timeout
	recoveryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	
	recoveryErr := strategy.Recover(recoveryCtx, err)
	if recoveryErr != nil {
		rm.handler.Handle(Wrap(recoveryErr, ErrCodeInternal, "Recovery failed"))
		return err // Return original error
	}
	
	rm.handler.Handle(New(ErrCodeInternal, "Recovery successful").
		WithSeverity(SeverityInfo))
	
	return nil
}

// ConnectionRecoveryStrategy recovers from connection errors
type ConnectionRecoveryStrategy struct{}

func (s *ConnectionRecoveryStrategy) CanRecover(err error) bool {
	return IsRecoverable(err)
}

func (s *ConnectionRecoveryStrategy) Recover(ctx context.Context, err error) error {
	// Wait for network to stabilize
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}
	
	// Could implement actual connection retry logic here
	return nil
}

// AuthenticationRecoveryStrategy recovers from authentication errors
type AuthenticationRecoveryStrategy struct{}

func (s *AuthenticationRecoveryStrategy) CanRecover(err error) bool {
	appErr, ok := err.(*AppError)
	if !ok {
		return false
	}
	
	// Can recover if credentials are expired or invalid
	return appErr.Code == ErrCodeAuthenticationFailed || 
	       appErr.Code == ErrCodeCredentialsExpired
}

func (s *AuthenticationRecoveryStrategy) Recover(ctx context.Context, err error) error {
	// In a real implementation, this could:
	// 1. Prompt for new credentials
	// 2. Refresh tokens
	// 3. Re-authenticate with cached credentials
	
	fmt.Fprintln(os.Stderr, "Authentication recovery would be attempted here")
	return fmt.Errorf("authentication recovery not implemented")
}

// ConfigRecoveryStrategy recovers from configuration errors
type ConfigRecoveryStrategy struct{}

func (s *ConfigRecoveryStrategy) CanRecover(err error) bool {
	appErr, ok := err.(*AppError)
	if !ok {
		return false
	}
	
	return appErr.Code == ErrCodeConfigMissing || 
	       appErr.Code == ErrCodeConfigNotFound
}

func (s *ConfigRecoveryStrategy) Recover(ctx context.Context, err error) error {
	// Could create default configuration or prompt user
	fmt.Fprintln(os.Stderr, "Configuration recovery would be attempted here")
	return fmt.Errorf("configuration recovery not implemented")
}

// DiskSpaceRecoveryStrategy recovers from disk space errors
type DiskSpaceRecoveryStrategy struct{}

func (s *DiskSpaceRecoveryStrategy) CanRecover(err error) bool {
	return GetErrorCode(err) == ErrCodeDiskSpace
}

func (s *DiskSpaceRecoveryStrategy) Recover(ctx context.Context, err error) error {
	// Could attempt to:
	// 1. Clean up temporary files
	// 2. Rotate logs
	// 3. Alert user to free space
	
	fmt.Fprintln(os.Stderr, "Attempting to free disk space...")
	
	// Clean temporary directory
	tempDir := os.TempDir()
	// In real implementation, would clean old temp files
	_ = tempDir
	
	return nil
}

// GracefulDegradation provides fallback options when primary operations fail
type GracefulDegradation struct {
	handler *ErrorHandler
}

// NewGracefulDegradation creates a new graceful degradation handler
func NewGracefulDegradation(handler *ErrorHandler) *GracefulDegradation {
	return &GracefulDegradation{handler: handler}
}

// WithFallback executes a primary function with a fallback option
func (gd *GracefulDegradation) WithFallback(
	primary func() error,
	fallback func() error,
	degradationMessage string,
) error {
	// Try primary function
	if err := primary(); err != nil {
		// Log the degradation
		gd.handler.Handle(New(ErrCodeInternal, degradationMessage).
			WithSeverity(SeverityWarning).
			WithContext("primary_error", err.Error()))
		
		// Try fallback
		if fallbackErr := fallback(); fallbackErr != nil {
			// Both failed
			return Wrap(err, ErrCodeInternal, "Primary and fallback operations failed").
				WithContext("fallback_error", fallbackErr.Error())
		}
		
		// Fallback succeeded
		fmt.Fprintf(os.Stderr, "Operating in degraded mode: %s\n", degradationMessage)
		return nil
	}
	
	// Primary succeeded
	return nil
}

// DegradationOptions provides multiple fallback levels
type DegradationOptions struct {
	Levels []DegradationLevel
}

// DegradationLevel represents a single degradation level
type DegradationLevel struct {
	Name        string
	Description string
	Execute     func() error
}

// ExecuteWithDegradation tries multiple degradation levels
func (gd *GracefulDegradation) ExecuteWithDegradation(options DegradationOptions) error {
	var lastError error
	
	for i, level := range options.Levels {
		if i > 0 {
			// Not the primary level, log degradation
			gd.handler.Handle(New(ErrCodeInternal, 
				fmt.Sprintf("Degrading to: %s", level.Description)).
				WithSeverity(SeverityWarning))
		}
		
		if err := level.Execute(); err != nil {
			lastError = err
			continue
		}
		
		// Success at this level
		if i > 0 {
			fmt.Fprintf(os.Stderr, "Operating at degradation level %d: %s\n", i, level.Name)
		}
		return nil
	}
	
	// All levels failed
	return Wrap(lastError, ErrCodeInternal, "All operation levels failed")
}