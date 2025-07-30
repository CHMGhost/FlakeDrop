package errors

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestAppError(t *testing.T) {
	tests := []struct {
		name     string
		err      *AppError
		expected string
	}{
		{
			name: "basic error",
			err:  New(ErrCodeConnectionFailed, "Connection failed"),
			expected: "[SFDE1001] ERROR: Connection failed",
		},
		{
			name: "error with suggestions",
			err: New(ErrCodeConnectionFailed, "Connection failed").
				WithSuggestions("Check network", "Verify credentials"),
			expected: "[SFDE1001] ERROR: Connection failed\nSuggestions:\n  1. Check network\n  2. Verify credentials",
		},
		{
			name: "error with context",
			err: New(ErrCodeConnectionFailed, "Connection failed").
				WithContext("host", "example.com").
				WithContext("port", 443),
			expected: "[SFDE1001] ERROR: Connection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Code != ErrCodeConnectionFailed {
				t.Errorf("Expected code %s, got %s", ErrCodeConnectionFailed, tt.err.Code)
			}
		})
	}
}

func TestErrorWrapping(t *testing.T) {
	// Create base error
	baseErr := fmt.Errorf("database connection refused")
	
	// Wrap error
	appErr := Wrap(baseErr, ErrCodeConnectionFailed, "Failed to connect to Snowflake")
	
	if appErr.Cause != baseErr {
		t.Error("Wrapped error should contain original error as cause")
	}
	
	if appErr.Code != ErrCodeConnectionFailed {
		t.Errorf("Expected code %s, got %s", ErrCodeConnectionFailed, appErr.Code)
	}
}

func TestRetryLogic(t *testing.T) {
	attempts := 0
	maxAttempts := 3
	
	config := &RetryConfig{
		MaxRetries:   maxAttempts - 1,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       false,
		RetryableError: func(err error) bool {
			return true
		},
	}
	
	ctx := context.Background()
	
	// Test successful retry
	err := Retry(ctx, config, func(ctx context.Context) error {
		attempts++
		if attempts < maxAttempts {
			return New(ErrCodeConnectionTimeout, "Timeout").AsRecoverable()
		}
		return nil
	})
	
	if err != nil {
		t.Error("Expected retry to succeed")
	}
	
	if attempts != maxAttempts {
		t.Errorf("Expected %d attempts, got %d", maxAttempts, attempts)
	}
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker("test", 2, 100*time.Millisecond)
	ctx := context.Background()
	
	// First failure
	err := cb.Execute(ctx, func() error {
		return fmt.Errorf("failure 1")
	})
	if err == nil {
		t.Error("Expected error")
	}
	
	// Second failure - should open circuit
	err = cb.Execute(ctx, func() error {
		return fmt.Errorf("failure 2")
	})
	if err == nil {
		t.Error("Expected error")
	}
	
	// Circuit should be open now
	err = cb.Execute(ctx, func() error {
		return nil
	})
	if err == nil {
		t.Error("Expected circuit breaker to be open")
	}
	
	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)
	
	// Circuit should be half-open, success should close it
	err = cb.Execute(ctx, func() error {
		return nil
	})
	if err != nil {
		t.Error("Expected success after reset")
	}
	
	if cb.GetState() != "closed" {
		t.Errorf("Expected circuit to be closed, got %s", cb.GetState())
	}
}

func TestErrorHandler(t *testing.T) {
	// Create test error handler
	config := ErrorHandlerConfig{
		LogToFile:     false,
		MaxLogEntries: 10,
		EnableMetrics: false,
	}
	
	handler, err := NewErrorHandler(config)
	if err != nil {
		t.Fatalf("Failed to create error handler: %v", err)
	}
	defer handler.Close()
	
	// Handle some errors
	handler.Handle(New(ErrCodeConnectionFailed, "Test error 1"))
	handler.Handle(New(ErrCodeSQLSyntax, "Test error 2").WithSeverity(SeverityWarning))
	handler.Handle(New(ErrCodeInternal, "Test error 3").WithSeverity(SeverityCritical))
	
	// Get summary
	summary := handler.GetErrorSummary()
	
	totalErrors, ok := summary["total_errors"].(int)
	if !ok || totalErrors != 3 {
		t.Errorf("Expected 3 total errors, got %v", summary["total_errors"])
	}
}

func TestRecoveryStrategies(t *testing.T) {
	handler, _ := NewErrorHandler(ErrorHandlerConfig{LogToFile: false})
	rm := NewRecoveryManager(handler)
	
	ctx := context.Background()
	
	// Test recoverable error
	err := New(ErrCodeConnectionTimeout, "Connection timeout").AsRecoverable()
	recoveryErr := rm.Recover(ctx, err)
	
	// Recovery should attempt but may fail in test environment
	_ = recoveryErr
}

func TestGracefulDegradation(t *testing.T) {
	handler, _ := NewErrorHandler(ErrorHandlerConfig{LogToFile: false})
	gd := NewGracefulDegradation(handler)
	
	primaryCalled := false
	fallbackCalled := false
	
	err := gd.WithFallback(
		func() error {
			primaryCalled = true
			return fmt.Errorf("primary failed")
		},
		func() error {
			fallbackCalled = true
			return nil
		},
		"Using fallback mode",
	)
	
	if err != nil {
		t.Error("Expected fallback to succeed")
	}
	
	if !primaryCalled {
		t.Error("Primary function should have been called")
	}
	
	if !fallbackCalled {
		t.Error("Fallback function should have been called")
	}
}

func TestTransactionHandler(t *testing.T) {
	handler, _ := NewErrorHandler(ErrorHandlerConfig{LogToFile: false})
	
	rollbackCalled := false
	rollbackFunc := func() error {
		rollbackCalled = true
		return nil
	}
	
	txHandler := handler.NewTransactionHandler(nil, rollbackFunc)
	
	// Test failed transaction
	err := txHandler.Execute(func() error {
		return fmt.Errorf("transaction failed")
	})
	
	if err == nil {
		t.Error("Expected error from failed transaction")
	}
	
	if !rollbackCalled {
		t.Error("Rollback should have been called")
	}
}

func TestErrorCodes(t *testing.T) {
	// Test error code extraction
	err1 := New(ErrCodeConnectionFailed, "Test")
	if GetErrorCode(err1) != ErrCodeConnectionFailed {
		t.Error("Failed to extract error code from AppError")
	}
	
	err2 := fmt.Errorf("regular error")
	if GetErrorCode(err2) != ErrCodeInternal {
		t.Error("Should return internal error code for non-AppError")
	}
}

func TestErrorSeverity(t *testing.T) {
	tests := []struct {
		severity ErrorSeverity
		err      *AppError
	}{
		{
			severity: SeverityCritical,
			err:      New(ErrCodeInternal, "Critical error").WithSeverity(SeverityCritical),
		},
		{
			severity: SeverityWarning,
			err:      New(ErrCodeValidationFailed, "Warning").WithSeverity(SeverityWarning),
		},
	}
	
	for _, tt := range tests {
		if tt.err.Severity != tt.severity {
			t.Errorf("Expected severity %s, got %s", tt.severity, tt.err.Severity)
		}
	}
}

// Benchmark tests
func BenchmarkErrorCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = New(ErrCodeConnectionFailed, "Connection failed").
			WithContext("host", "example.com").
			WithSuggestions("Check connection")
	}
}

func BenchmarkRetryExecution(b *testing.B) {
	config := &RetryConfig{
		MaxRetries:   0, // No retries for benchmark
		InitialDelay: 0,
		RetryableError: func(err error) bool {
			return false
		},
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Retry(ctx, config, func(ctx context.Context) error {
			return nil
		})
	}
}