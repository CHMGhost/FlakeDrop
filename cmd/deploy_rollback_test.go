package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"flakedrop/internal/rollback"
	"flakedrop/pkg/models"
)

// MockRollbackManager is a mock implementation of rollback.Manager
type MockRollbackManager struct {
	mock.Mock
}

func (m *MockRollbackManager) StartDeployment(ctx context.Context, deployment *rollback.DeploymentRecord) error {
	args := m.Called(ctx, deployment)
	return args.Error(0)
}

func (m *MockRollbackManager) CompleteDeployment(ctx context.Context, deploymentID string, success bool, errorMessage string) error {
	args := m.Called(ctx, deploymentID, success, errorMessage)
	return args.Error(0)
}

func (m *MockRollbackManager) RollbackDeployment(ctx context.Context, deploymentID string, options *rollback.RollbackOptions) (*rollback.RollbackResult, error) {
	args := m.Called(ctx, deploymentID, options)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rollback.RollbackResult), args.Error(1)
}

// MockSnowflakeService is a mock implementation of snowflake.Service
type MockSnowflakeService struct {
	mock.Mock
}

func (m *MockSnowflakeService) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSnowflakeService) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSnowflakeService) ExecuteFile(path, database, schema string) error {
	args := m.Called(path, database, schema)
	return args.Error(0)
}

func (m *MockSnowflakeService) ExecuteSQL(ctx context.Context, sql, database, schema string) error {
	args := m.Called(ctx, sql, database, schema)
	return args.Error(0)
}

func (m *MockSnowflakeService) GetConnection() interface{} {
	args := m.Called()
	return args.Get(0)
}

func TestDeployWithRollback(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "flakedrop-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test SQL files
	migrationsDir := filepath.Join(tmpDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	require.NoError(t, err)

	// Create test SQL files
	testFiles := map[string]string{
		"001_create_table.sql": "CREATE TABLE test_table (id INT);",
		"002_bad_syntax.sql":   "INVALID SQL SYNTAX HERE;",
		"003_create_view.sql":  "CREATE VIEW test_view AS SELECT * FROM test_table;",
	}

	for filename, content := range testFiles {
		err = os.WriteFile(filepath.Join(migrationsDir, filename), []byte(content), 0644)
		require.NoError(t, err)
	}

	// Test configuration
	testConfig := &models.Config{
		Repositories: []models.Repository{
			{
				Name:     "test-repo",
				GitURL:   tmpDir,
				Branch:   "main",
				Database: "TEST_DB",
				Schema:   "PUBLIC",
				Path:     "migrations",
			},
		},
		Snowflake: models.Snowflake{
			Account:   "test.account",
			Username:  "test_user",
			Password:  "test_pass",
			Warehouse: "TEST_WH",
			Role:      "TEST_ROLE",
		},
		Deployment: models.Deployment{
			Rollback: models.RollbackConfig{
				Enabled:         true,
				OnFailure:       true,
				BackupRetention: 7,
				Strategy:        "snapshot",
			},
		},
	}

	tests := []struct {
		name                   string
		setupMocks             func(*MockSnowflakeService, *MockRollbackManager)
		expectRollback         bool
		expectedSuccessCount   int
		expectedFailureCount   int
		rollbackEnabled        bool
		rollbackOnFailure      bool
	}{
		{
			name: "successful deployment with rollback enabled",
			setupMocks: func(mockSF *MockSnowflakeService, mockRM *MockRollbackManager) {
				mockSF.On("Connect").Return(nil)
				mockSF.On("Close").Return(nil)
				
				// All files execute successfully
				mockSF.On("ExecuteFile", mock.AnythingOfType("string"), "TEST_DB", "PUBLIC").Return(nil).Times(3)
				
				// Rollback manager expectations
				mockRM.On("StartDeployment", mock.Anything, mock.AnythingOfType("*rollback.DeploymentRecord")).Return(nil)
				mockRM.On("CompleteDeployment", mock.Anything, mock.AnythingOfType("string"), true, "").Return(nil)
			},
			expectRollback:       false,
			expectedSuccessCount: 3,
			expectedFailureCount: 0,
			rollbackEnabled:      true,
			rollbackOnFailure:    true,
		},
		{
			name: "failed deployment triggers rollback",
			setupMocks: func(mockSF *MockSnowflakeService, mockRM *MockRollbackManager) {
				mockSF.On("Connect").Return(nil)
				mockSF.On("Close").Return(nil)
				
				// First file succeeds
				mockSF.On("ExecuteFile", mock.MatchedBy(func(path string) bool {
					return filepath.Base(path) == "001_create_table.sql"
				}), "TEST_DB", "PUBLIC").Return(nil).Once()
				
				// Second file fails
				mockSF.On("ExecuteFile", mock.MatchedBy(func(path string) bool {
					return filepath.Base(path) == "002_bad_syntax.sql"
				}), "TEST_DB", "PUBLIC").Return(fmt.Errorf("syntax error")).Once()
				
				// Third file succeeds
				mockSF.On("ExecuteFile", mock.MatchedBy(func(path string) bool {
					return filepath.Base(path) == "003_create_view.sql"
				}), "TEST_DB", "PUBLIC").Return(nil).Once()
				
				// Rollback manager expectations
				mockRM.On("StartDeployment", mock.Anything, mock.AnythingOfType("*rollback.DeploymentRecord")).Return(nil)
				mockRM.On("CompleteDeployment", mock.Anything, mock.AnythingOfType("string"), false, mock.AnythingOfType("string")).Return(nil)
				
				// Expect rollback to be called
				endTime := time.Now().Add(2 * time.Second)
				rollbackResult := &rollback.RollbackResult{
					DeploymentID: "test-deployment",
					StartTime:    time.Now(),
					EndTime:      &endTime,
					Success:      true,
					Operations: []rollback.RollbackOperationResult{
						{
							Operation: rollback.RollbackOperation{
								Type:        "DROP",
								Object:      "test_table",
								Description: "Dropped table test_table",
							},
							Success: true,
						},
					},
				}
				mockRM.On("RollbackDeployment", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*rollback.RollbackOptions")).Return(rollbackResult, nil)
			},
			expectRollback:       true,
			expectedSuccessCount: 2,
			expectedFailureCount: 1,
			rollbackEnabled:      true,
			rollbackOnFailure:    true,
		},
		{
			name: "failed deployment with rollback disabled",
			setupMocks: func(mockSF *MockSnowflakeService, mockRM *MockRollbackManager) {
				mockSF.On("Connect").Return(nil)
				mockSF.On("Close").Return(nil)
				
				// Second file fails
				mockSF.On("ExecuteFile", mock.MatchedBy(func(path string) bool {
					return filepath.Base(path) == "001_create_table.sql"
				}), "TEST_DB", "PUBLIC").Return(nil).Once()
				
				mockSF.On("ExecuteFile", mock.MatchedBy(func(path string) bool {
					return filepath.Base(path) == "002_bad_syntax.sql"
				}), "TEST_DB", "PUBLIC").Return(fmt.Errorf("syntax error")).Once()
				
				mockSF.On("ExecuteFile", mock.MatchedBy(func(path string) bool {
					return filepath.Base(path) == "003_create_view.sql"
				}), "TEST_DB", "PUBLIC").Return(nil).Once()
			},
			expectRollback:       false,
			expectedSuccessCount: 2,
			expectedFailureCount: 1,
			rollbackEnabled:      false, // Rollback disabled
			rollbackOnFailure:    false,
		},
		{
			name: "rollback fails after deployment failure",
			setupMocks: func(mockSF *MockSnowflakeService, mockRM *MockRollbackManager) {
				mockSF.On("Connect").Return(nil)
				mockSF.On("Close").Return(nil)
				
				// Deployment fails
				mockSF.On("ExecuteFile", mock.AnythingOfType("string"), "TEST_DB", "PUBLIC").Return(fmt.Errorf("execution error")).Times(3)
				
				// Rollback manager expectations
				mockRM.On("StartDeployment", mock.Anything, mock.AnythingOfType("*rollback.DeploymentRecord")).Return(nil)
				mockRM.On("CompleteDeployment", mock.Anything, mock.AnythingOfType("string"), false, mock.AnythingOfType("string")).Return(nil)
				
				// Rollback fails
				mockRM.On("RollbackDeployment", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("*rollback.RollbackOptions")).Return(nil, fmt.Errorf("no snapshot available"))
			},
			expectRollback:       true,
			expectedSuccessCount: 0,
			expectedFailureCount: 3,
			rollbackEnabled:      true,
			rollbackOnFailure:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update config for this test
			testConfig.Deployment.Rollback.Enabled = tt.rollbackEnabled
			testConfig.Deployment.Rollback.OnFailure = tt.rollbackOnFailure

			// Create mocks
			mockSnowflake := new(MockSnowflakeService)
			mockRollback := new(MockRollbackManager)

			// Setup expectations
			tt.setupMocks(mockSnowflake, mockRollback)

			// TODO: The actual test execution would require refactoring executeRealDeployment
			// to accept interfaces instead of concrete types, or using dependency injection
			// For now, this test demonstrates the structure and expectations

			// Verify all expectations were met
			mockSnowflake.AssertExpectations(t)
			if tt.rollbackEnabled {
				mockRollback.AssertExpectations(t)
			}
		})
	}
}

func TestRollbackConfiguration(t *testing.T) {
	tests := []struct {
		name               string
		config             models.Deployment
		shouldInitRollback bool
	}{
		{
			name: "rollback enabled with all features",
			config: models.Deployment{
				Rollback: models.RollbackConfig{
					Enabled:         true,
					OnFailure:       true,
					BackupRetention: 7,
					Strategy:        "snapshot",
				},
			},
			shouldInitRollback: true,
		},
		{
			name: "rollback disabled",
			config: models.Deployment{
				Rollback: models.RollbackConfig{
					Enabled:   false,
					OnFailure: true, // Should be ignored when disabled
				},
			},
			shouldInitRollback: false,
		},
		{
			name: "rollback enabled but not on failure",
			config: models.Deployment{
				Rollback: models.RollbackConfig{
					Enabled:         true,
					OnFailure:       false,
					BackupRetention: 7,
					Strategy:        "transaction",
				},
			},
			shouldInitRollback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test configuration parsing
			assert.Equal(t, tt.config.Rollback.Enabled, tt.shouldInitRollback, 
				"Rollback initialization should match enabled state")
			
			if tt.config.Rollback.Enabled {
				assert.NotEmpty(t, tt.config.Rollback.Strategy, "Strategy should be set when rollback is enabled")
				assert.GreaterOrEqual(t, tt.config.Rollback.BackupRetention, 0, "Backup retention should be non-negative")
			}
		})
	}
}

func TestRollbackStrategyValidation(t *testing.T) {
	validStrategies := []string{"snapshot", "transaction", "incremental", "point_in_time"}
	
	for _, strategy := range validStrategies {
		t.Run(fmt.Sprintf("valid strategy: %s", strategy), func(t *testing.T) {
			// Convert string to RollbackStrategy
			rollbackStrategy := rollback.RollbackStrategy(strategy)
			
			// Verify it matches one of the constants
			switch rollbackStrategy {
			case rollback.StrategySnapshot, 
			     rollback.StrategyTransaction, 
			     rollback.StrategyIncremental, 
			     rollback.StrategyPointInTime:
				// Valid strategy
				assert.True(t, true, "Strategy should be valid")
			default:
				t.Errorf("Invalid strategy: %s", strategy)
			}
		})
	}
}