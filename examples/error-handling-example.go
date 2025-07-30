//go:build example
// +build example

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"flakedrop/pkg/errors"
)

// Example demonstrating comprehensive error handling in FlakeDrop CLI

func errorHandlingDemo() {
	// Initialize error handler
	errorHandler, err := errors.NewErrorHandler(errors.DefaultErrorHandlerConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer errorHandler.Close()

	fmt.Println("=== FlakeDrop Error Handling Examples ===\n")

	// Example 1: Basic error creation and handling
	example1BasicErrors()

	// Example 2: Retry mechanism
	example2RetryMechanism()

	// Example 3: Circuit breaker pattern
	example3CircuitBreaker()

	// Example 4: Transaction handling
	example4TransactionHandling(errorHandler)

	// Example 5: Graceful degradation
	example5GracefulDegradation(errorHandler)

	// Example 6: Recovery strategies
	example6RecoveryStrategies(errorHandler)
}

func example1BasicErrors() {
	fmt.Println("1. Basic Error Creation and Handling")
	fmt.Println("------------------------------------")

	// Create a connection error
	err := errors.ConnectionError(
		"Failed to connect to Snowflake", 
		fmt.Errorf("network timeout"),
	).WithContext("account", "myaccount.snowflakecomputing.com").
		WithContext("warehouse", "COMPUTE_WH").
		WithContext("attempt", 1)

	fmt.Printf("Error: %v\n\n", err)

	// Create a validation error
	validationErr := errors.ValidationError(
		"warehouse_size",
		"XXXL",
		"Invalid warehouse size",
	).WithSuggestions(
		"Valid sizes: X-SMALL, SMALL, MEDIUM, LARGE, X-LARGE",
		"Check your configuration file",
	)

	fmt.Printf("Validation Error: %v\n\n", validationErr)

	// Create SQL error
	sqlErr := errors.SQLError(
		"Failed to create table",
		"CREATE TABLE users (id INT, name VARCHAR)",
		fmt.Errorf("table already exists"),
	)

	fmt.Printf("SQL Error: %v\n\n", sqlErr)
}

func example2RetryMechanism() {
	fmt.Println("2. Retry Mechanism Example")
	fmt.Println("--------------------------")

	attempts := 0
	ctx := context.Background()

	// Configure retry with custom settings
	config := &errors.RetryConfig{
		MaxRetries:   3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       true,
		RetryableError: func(err error) bool {
			// Retry on specific error codes
			code := errors.GetErrorCode(err)
			return code == errors.ErrCodeConnectionTimeout ||
				code == errors.ErrCodeNetworkUnavailable
		},
	}

	// Simulate operation that fails twice then succeeds
	err := errors.Retry(ctx, config, func(ctx context.Context) error {
		attempts++
		fmt.Printf("  Attempt %d: ", attempts)

		if attempts < 3 {
			fmt.Println("Failed (will retry)")
			return errors.New(errors.ErrCodeConnectionTimeout, "Connection timeout").
				AsRecoverable()
		}

		fmt.Println("Success!")
		return nil
	})

	if err != nil {
		fmt.Printf("  Final error after retries: %v\n", err)
	}
	fmt.Println()
}

func example3CircuitBreaker() {
	fmt.Println("3. Circuit Breaker Example")
	fmt.Println("--------------------------")

	// Create circuit breaker for external service
	cb := errors.NewCircuitBreaker("snowflake-api", 2, 2*time.Second)
	ctx := context.Background()

	// Simulate failures to trip the circuit
	for i := 1; i <= 4; i++ {
		fmt.Printf("  Request %d: ", i)
		
		err := cb.Execute(ctx, func() error {
			if i <= 2 {
				return fmt.Errorf("service unavailable")
			}
			return nil
		})

		if err != nil {
			fmt.Printf("Failed - %v (Circuit: %s)\n", err, cb.GetState())
		} else {
			fmt.Printf("Success (Circuit: %s)\n", cb.GetState())
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Wait for circuit to reset
	fmt.Println("  Waiting for circuit reset...")
	time.Sleep(2 * time.Second)

	// Try again after reset
	fmt.Print("  Request after reset: ")
	err := cb.Execute(ctx, func() error {
		return nil
	})
	if err == nil {
		fmt.Printf("Success (Circuit: %s)\n", cb.GetState())
	}
	fmt.Println()
}

func example4TransactionHandling(handler *errors.ErrorHandler) {
	fmt.Println("4. Transaction Handling Example")
	fmt.Println("-------------------------------")

	// Simulate a database transaction
	type mockTx struct {
		committed bool
		rolled    bool
	}

	tx := &mockTx{}

	rollbackFunc := func() error {
		tx.rolled = true
		fmt.Println("  Transaction rolled back")
		return nil
	}

	txHandler := handler.NewTransactionHandler(tx, rollbackFunc)

	// Simulate failed transaction
	fmt.Println("  Executing transaction that will fail...")
	err := txHandler.Execute(func() error {
		fmt.Println("  Executing SQL statement 1...")
		fmt.Println("  Executing SQL statement 2...")
		fmt.Println("  Error in SQL statement 3!")
		return errors.New(errors.ErrCodeSQLSyntax, "Syntax error in SQL")
	})

	if err != nil {
		fmt.Printf("  Transaction failed: %v\n", err)
	}
	fmt.Println()
}

func example5GracefulDegradation(handler *errors.ErrorHandler) {
	fmt.Println("5. Graceful Degradation Example")
	fmt.Println("-------------------------------")

	gd := errors.NewGracefulDegradation(handler)

	// Example with fallback
	fmt.Println("  Attempting primary operation...")
	err := gd.WithFallback(
		func() error {
			fmt.Println("  Primary: Connecting with SSL...")
			return fmt.Errorf("SSL connection failed")
		},
		func() error {
			fmt.Println("  Fallback: Connecting without SSL...")
			return nil
		},
		"SSL connection failed, using non-SSL connection",
	)

	if err == nil {
		fmt.Println("  Operation succeeded with degradation")
	}

	// Example with multiple degradation levels
	fmt.Println("\n  Attempting multi-level degradation...")
	err = gd.ExecuteWithDegradation(errors.DegradationOptions{
		Levels: []errors.DegradationLevel{
			{
				Name:        "High Performance",
				Description: "Full parallel processing",
				Execute: func() error {
					fmt.Println("  Level 1: High performance mode...")
					return fmt.Errorf("insufficient resources")
				},
			},
			{
				Name:        "Standard",
				Description: "Sequential processing",
				Execute: func() error {
					fmt.Println("  Level 2: Standard mode...")
					return fmt.Errorf("still failing")
				},
			},
			{
				Name:        "Safe Mode",
				Description: "Minimal resource usage",
				Execute: func() error {
					fmt.Println("  Level 3: Safe mode...")
					return nil
				},
			},
		},
	})

	if err == nil {
		fmt.Println("  Operation succeeded in degraded mode")
	}
	fmt.Println()
}

func example6RecoveryStrategies(handler *errors.ErrorHandler) {
	fmt.Println("6. Recovery Strategies Example")
	fmt.Println("------------------------------")

	rm := errors.NewRecoveryManager(handler)
	ctx := context.Background()

	// Create recoverable errors
	errorList := []error{
		errors.New(errors.ErrCodeConnectionTimeout, "Connection timeout").
			AsRecoverable(),
		errors.New(errors.ErrCodeAuthenticationFailed, "Invalid credentials"),
		errors.New(errors.ErrCodeDiskSpace, "Insufficient disk space").
			AsRecoverable(),
	}

	for _, err := range errorList {
		fmt.Printf("  Error: %s\n", err)
		fmt.Printf("  Recoverable: %v\n", errors.IsRecoverable(err))
		
		if errors.IsRecoverable(err) {
			fmt.Println("  Attempting recovery...")
			recoveryErr := rm.Recover(ctx, err)
			if recoveryErr != nil {
				fmt.Printf("  Recovery failed: %v\n", recoveryErr)
			} else {
				fmt.Println("  Recovery successful!")
			}
		} else {
			fmt.Println("  No recovery available")
		}
		fmt.Println()
	}
}

// Example: How to use error handling in a real deployment scenario
func ExampleRealWorldDeployment() {
	errorHandler, _ := errors.NewErrorHandler(errors.DefaultErrorHandlerConfig())
	defer errorHandler.Close()

	ctx := context.Background()

	// Wrap the entire deployment in error handling
	err := deployWithErrorHandling(ctx, errorHandler)
	if err != nil {
		// Error is already handled and logged
		errorHandler.Handle(err)
		
		// Get error summary for reporting
		summary := errorHandler.GetErrorSummary()
		fmt.Printf("Deployment failed. Error summary: %+v\n", summary)
	}
}

func deployWithErrorHandling(ctx context.Context, handler *errors.ErrorHandler) error {
	// Step 1: Connect to Snowflake with retry
	var connection interface{}
	err := errors.RetryWithBackoff(ctx, func(ctx context.Context) error {
		// Simulate connection
		// connection = snowflake.Connect()
		return nil
	})
	
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeConnectionFailed, 
			"Failed to establish Snowflake connection after retries")
	}

	// Step 2: Execute deployment with transaction support
	txHandler := handler.NewTransactionHandler(connection, func() error {
		// Rollback logic
		return nil
	})

	return txHandler.Execute(func() error {
		// Deployment logic here
		return nil
	})
}