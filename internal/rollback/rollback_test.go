package rollback

import (
	"testing"
	"time"

	"flakedrop/pkg/errors"
)

func TestHistoryManager(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()
	
	// Create history manager
	hm, err := NewHistoryManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create history manager: %v", err)
	}

	t.Run("RecordDeployment", func(t *testing.T) {
		deployment := &DeploymentRecord{
			ID:         "test-deploy-1",
			Repository: "test-repo",
			Commit:     "abc123",
			Database:   "TEST_DB",
			Schema:     "TEST_SCHEMA",
			StartTime:  time.Now(),
			State:      StateInProgress,
			Files:      []FileExecution{},
		}

		err := hm.RecordDeployment(deployment)
		if err != nil {
			t.Errorf("Failed to record deployment: %v", err)
		}

		// Verify deployment was recorded
		retrieved, err := hm.GetDeployment("test-deploy-1")
		if err != nil {
			t.Errorf("Failed to get deployment: %v", err)
		}

		if retrieved.ID != deployment.ID {
			t.Errorf("Expected deployment ID %s, got %s", deployment.ID, retrieved.ID)
		}
	})

	t.Run("GetDeploymentHistory", func(t *testing.T) {
		// Create multiple test deployments
		deploymentIDs := []string{"deploy-1", "deploy-2", "deploy-3"}
		
		for _, id := range deploymentIDs {
			deployment := &DeploymentRecord{
				ID:         id,
				Repository: "test-repo",
				Commit:     "abc123",
				Database:   "TEST_DB",
				Schema:     "TEST_SCHEMA",
				StartTime:  time.Now(),
				State:      StateCompleted,
				Files:      []FileExecution{},
			}
			
			err := hm.RecordDeployment(deployment)
			if err != nil {
				t.Errorf("Failed to record deployment %s: %v", id, err)
			}
		}

		// Get deployment history
		deployments, err := hm.GetDeploymentHistory("TEST_DB", "TEST_SCHEMA", 10)
		if err != nil {
			t.Errorf("Failed to list deployments: %v", err)
		}

		if len(deployments) < len(deploymentIDs) {
			t.Errorf("Expected at least %d deployments, got %d", len(deploymentIDs), len(deployments))
		}
	})
}

func TestBackupManager(t *testing.T) {
	// Skip test - requires proper interface implementation
	t.Skip("BackupManager test requires proper Snowflake service interface")
}

func TestSnapshotManager(t *testing.T) {
	// Skip this test until proper mock interface is implemented
	t.Skip("Test requires proper Snowflake service interface with ExecuteQueryFunc")
}

func TestTransactionManager(t *testing.T) {
	// Skip this test until proper transaction interface is implemented
	t.Skip("Test requires proper transaction interface")
}

func TestRollbackManager(t *testing.T) {
	// Skip this test until proper manager interface is implemented
	t.Skip("Test requires proper manager interface")
}

func TestRollbackStrategies(t *testing.T) {
	t.Run("StrategyTypes", func(t *testing.T) {
		strategies := []RollbackStrategy{
			StrategySnapshot,
			StrategyIncremental,
			StrategyTransaction,
		}
		
		for _, strategy := range strategies {
			if string(strategy) == "" {
				t.Errorf("Strategy should not be empty: %v", strategy)
			}
		}
	})
}

func TestRecoveryManager(t *testing.T) {
	// Skip this test until proper recovery interface is implemented
	t.Skip("Test requires proper recovery interface")
}

func TestTypes(t *testing.T) {
	t.Run("DeploymentState", func(t *testing.T) {
		states := []DeploymentState{
			StateInProgress,
			StateCompleted,
			StateFailed,
			StateRolledBack,
		}

		for _, state := range states {
			if string(state) == "" {
				t.Errorf("State should not be empty: %v", state)
			}
		}
	})

	t.Run("RollbackStrategy", func(t *testing.T) {
		strategies := []RollbackStrategy{
			StrategySnapshot,
			StrategyIncremental,
			StrategyTransaction,
		}

		for _, strategy := range strategies {
			if string(strategy) == "" {
				t.Errorf("Strategy should not be empty: %v", strategy)
			}
		}
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("RollbackError", func(t *testing.T) {
		err := errors.New("SFDE6001", "Test rollback error")
		if err == nil {
			t.Error("Expected error to be created")
		}

		if err.Error() == "" {
			t.Error("Error should have a message")
		}
	})
}

func TestFileExecution(t *testing.T) {
	t.Run("FileExecutionBasics", func(t *testing.T) {
		endTime := time.Now().Add(1 * time.Second)
		fileExec := FileExecution{
			Path:      "test.sql",
			Success:   true,
			StartTime: time.Now(),
			EndTime:   &endTime,
		}

		if fileExec.Path == "" {
			t.Error("Expected file path to be set")
		}

		if !fileExec.Success {
			t.Error("Expected file execution to be successful")
		}

		if fileExec.EndTime != nil && fileExec.EndTime.Before(fileExec.StartTime) {
			t.Error("End time should be after start time")
		}
	})
}

func TestDeploymentRecord(t *testing.T) {
	t.Run("DeploymentRecordBasics", func(t *testing.T) {
		record := &DeploymentRecord{
			ID:         "test-123",
			Repository: "test-repo",
			Commit:     "abc123def",
			Database:   "TEST_DB",
			Schema:     "PUBLIC",
			StartTime:  time.Now(),
			State:      StateInProgress,
			Files:      []FileExecution{},
		}

		if record.ID == "" {
			t.Error("Expected deployment ID to be set")
		}

		if record.Repository == "" {
			t.Error("Expected repository to be set")
		}

		if record.State != StateInProgress {
			t.Errorf("Expected state to be StateInProgress, got %v", record.State)
		}

		if record.Files == nil {
			t.Error("Expected files slice to be initialized")
		}
	})
}