package errors

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// RetryConfig holds configuration for retry logic
type RetryConfig struct {
	MaxRetries     int
	InitialDelay   time.Duration
	MaxDelay       time.Duration
	Multiplier     float64
	Jitter         bool
	RetryableError func(error) bool
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:   3,
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		Jitter:       true,
		RetryableError: func(err error) bool {
			// Default: retry on recoverable errors and specific error codes
			if IsRecoverable(err) {
				return true
			}
			
			code := GetErrorCode(err)
			switch code {
			case ErrCodeConnectionTimeout,
				ErrCodeNetworkUnavailable,
				ErrCodeTimeout,
				ErrCodeServiceUnavailable:
				return true
			default:
				return false
			}
		},
	}
}

// RetryableFunc represents a function that can be retried
type RetryableFunc func(ctx context.Context) error

// Retry executes a function with retry logic
func Retry(ctx context.Context, config *RetryConfig, fn RetryableFunc) error {
	var lastErr error
	
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Execute the function
		err := fn(ctx)
		if err == nil {
			return nil
		}
		
		lastErr = err
		
		// Check if error is retryable
		if !config.RetryableError(err) {
			return err
		}
		
		// Check if we've exhausted retries
		if attempt == config.MaxRetries {
			break
		}
		
		// Calculate delay with exponential backoff
		delay := calculateDelay(attempt, config)
		
		// Create retry error with context
		retryErr := Wrap(err, ErrCodeInternal, fmt.Sprintf("Attempt %d/%d failed", attempt+1, config.MaxRetries+1)).
			WithContext("attempt", attempt+1).
			WithContext("next_retry_in", delay.String()).
			AsRecoverable()
		
		// Log retry attempt (could be replaced with proper logging)
		fmt.Printf("Retry: %v\n", retryErr)
		
		// Wait before retrying
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	
	// All retries exhausted
	return Wrap(lastErr, ErrCodeResourceExhausted, 
		fmt.Sprintf("Operation failed after %d retries", config.MaxRetries+1)).
		WithSeverity(SeverityError)
}

// calculateDelay calculates the delay for the next retry attempt
func calculateDelay(attempt int, config *RetryConfig) time.Duration {
	// Calculate base delay with exponential backoff
	delay := float64(config.InitialDelay) * math.Pow(config.Multiplier, float64(attempt))
	
	// Cap at max delay
	if delay > float64(config.MaxDelay) {
		delay = float64(config.MaxDelay)
	}
	
	// Add jitter if enabled
	if config.Jitter {
		// Use crypto/rand for secure random jitter
		var b [8]byte
		_, _ = cryptorand.Read(b[:])
		randomFloat := float64(binary.LittleEndian.Uint64(b[:])) / float64(^uint64(0))
		jitter := randomFloat * 0.3 * delay // Up to 30% jitter
		delay = delay + jitter
	}
	
	return time.Duration(delay)
}

// RetryWithBackoff is a convenience function for common retry scenarios
func RetryWithBackoff(ctx context.Context, fn RetryableFunc) error {
	return Retry(ctx, DefaultRetryConfig(), fn)
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	name            string
	maxFailures     int
	resetTimeout    time.Duration
	halfOpenTimeout time.Duration
	
	failures        int
	lastFailureTime time.Time
	state           CircuitState
	stateChanged    time.Time
}

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(name string, maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:            name,
		maxFailures:     maxFailures,
		resetTimeout:    resetTimeout,
		halfOpenTimeout: resetTimeout / 2,
		state:           StateClosed,
		stateChanged:    time.Now(),
	}
}

// Execute runs a function through the circuit breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	// Check circuit state
	if err := cb.checkState(); err != nil {
		return err
	}
	
	// Execute the function
	err := fn()
	
	// Update circuit breaker state based on result
	cb.recordResult(err)
	
	return err
}

// checkState checks if the circuit breaker allows execution
func (cb *CircuitBreaker) checkState() error {
	switch cb.state {
	case StateClosed:
		return nil
		
	case StateOpen:
		// Check if we should transition to half-open
		if time.Since(cb.stateChanged) > cb.resetTimeout {
			cb.setState(StateHalfOpen)
			return nil
		}
		return New(ErrCodeServiceUnavailable, 
			fmt.Sprintf("Circuit breaker '%s' is open", cb.name)).
			WithContext("failures", cb.failures).
			WithContext("will_retry_at", cb.stateChanged.Add(cb.resetTimeout)).
			WithSuggestions(
				"Wait for the circuit to reset",
				"Check service health",
				"Review recent error logs",
			).AsRecoverable()
		
	case StateHalfOpen:
		return nil
		
	default:
		return New(ErrCodeInternal, "Invalid circuit breaker state")
	}
}

// recordResult records the result of an execution
func (cb *CircuitBreaker) recordResult(err error) {
	if err == nil {
		// Success
		if cb.state == StateHalfOpen {
			// Circuit recovered
			cb.reset()
		}
		return
	}
	
	// Failure
	cb.failures++
	cb.lastFailureTime = time.Now()
	
	if cb.state == StateHalfOpen {
		// Failed in half-open state, go back to open
		cb.setState(StateOpen)
	} else if cb.failures >= cb.maxFailures {
		// Too many failures, open the circuit
		cb.setState(StateOpen)
	}
}

// setState changes the circuit breaker state
func (cb *CircuitBreaker) setState(state CircuitState) {
	cb.state = state
	cb.stateChanged = time.Now()
}

// reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) reset() {
	cb.failures = 0
	cb.setState(StateClosed)
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() string {
	switch cb.state {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerManager manages multiple circuit breakers
type CircuitBreakerManager struct {
	breakers map[string]*CircuitBreaker
}

// NewCircuitBreakerManager creates a new circuit breaker manager
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetBreaker gets or creates a circuit breaker for a service
func (m *CircuitBreakerManager) GetBreaker(name string) *CircuitBreaker {
	if breaker, exists := m.breakers[name]; exists {
		return breaker
	}
	
	// Create default circuit breaker
	breaker := NewCircuitBreaker(name, 5, 30*time.Second)
	m.breakers[name] = breaker
	return breaker
}

// Execute runs a function through a named circuit breaker
func (m *CircuitBreakerManager) Execute(ctx context.Context, name string, fn func() error) error {
	breaker := m.GetBreaker(name)
	return breaker.Execute(ctx, fn)
}