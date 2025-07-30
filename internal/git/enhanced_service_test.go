package git

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"flakedrop/pkg/errors"
	"flakedrop/pkg/models"
)

func TestEnhancedService_SyncRepositoryEnhanced(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	service := NewEnhancedService()
	service.cacheDir = tmpDir

	// Create a local test repository
	testRepoPath := filepath.Join(tmpDir, "test-origin-repo")
	err = initTestRepo(testRepoPath)
	require.NoError(t, err)

	tests := []struct {
		name      string
		repo      models.Repository
		setupFunc func()
		wantErr   bool
		errCode   errors.ErrorCode
	}{
		{
			name: "successful sync",
			repo: models.Repository{
				Name:   "test-repo",
				GitURL: testRepoPath,
				Branch: "master",
			},
			wantErr: false,
		},
		{
			name: "invalid URL",
			repo: models.Repository{
				Name:   "invalid-repo",
				GitURL: "not-a-valid-url",
				Branch: "main",
			},
			wantErr: true,
		},
		{
			name: "network error simulation",
			repo: models.Repository{
				Name:   "network-fail",
				GitURL: "https://non-existent-host-12345.com/repo.git",
				Branch: "main",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupFunc != nil {
				tt.setupFunc()
			}

			ctx := context.Background()
			err := service.SyncRepositoryEnhanced(ctx, tt.repo)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errCode != "" {
					appErr, ok := err.(*errors.AppError)
					assert.True(t, ok)
					assert.Equal(t, tt.errCode, appErr.Code)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAuthManager_StoreAndRetrieveCredentials(t *testing.T) {
	authManager := NewAuthManager()
	
	// Store token
	err := authManager.StoreToken("test.example.com", "test-token-123")
	assert.NoError(t, err)
	
	// Retrieve token
	token := authManager.tokenManager.GetToken("test.example.com")
	assert.Equal(t, "test-token-123", token)
	
	// Store credentials
	err = authManager.StoreCredentials("test.example.com", "testuser", "testpass")
	assert.NoError(t, err)
	
	// Clean up
	err = authManager.RemoveCredentials("test.example.com")
	assert.NoError(t, err)
}

func TestBranchManager_Operations(t *testing.T) {
	// Skip if not in CI or if git is not available
	if os.Getenv("CI") == "" {
		t.Skip("Skipping branch manager test outside of CI")
	}

	tmpDir, err := os.MkdirTemp("", "branch-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Initialize a test repository
	repoPath := filepath.Join(tmpDir, "test-repo")
	err = initTestRepo(repoPath)
	require.NoError(t, err)

	branchManager := NewBranchManager()

	t.Run("list branches", func(t *testing.T) {
		branches, err := branchManager.ListBranches(repoPath, false)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(branches), 1)
		
		// Should have at least main/master branch
		found := false
		for _, b := range branches {
			if b.Name == "main" || b.Name == "master" {
				found = true
				break
			}
		}
		assert.True(t, found, "Should have main or master branch")
	})

	t.Run("create branch", func(t *testing.T) {
		err := branchManager.CreateBranch(repoPath, "test-branch", "", false)
		assert.NoError(t, err)

		// Verify branch exists
		branches, err := branchManager.ListBranches(repoPath, false)
		assert.NoError(t, err)
		
		found := false
		for _, b := range branches {
			if b.Name == "test-branch" {
				found = true
				break
			}
		}
		assert.True(t, found, "Should have created test-branch")
	})

	t.Run("delete branch", func(t *testing.T) {
		// First switch away from the branch
		err := branchManager.CheckoutBranch(repoPath, "main", false)
		if err != nil {
			// Try master if main doesn't exist
			err = branchManager.CheckoutBranch(repoPath, "master", false)
		}
		assert.NoError(t, err)

		// Now delete the branch
		err = branchManager.DeleteBranch(repoPath, "test-branch", false, false)
		assert.NoError(t, err)

		// Verify branch is deleted
		branches, err := branchManager.ListBranches(repoPath, false)
		assert.NoError(t, err)
		
		found := false
		for _, b := range branches {
			if b.Name == "test-branch" {
				found = true
				break
			}
		}
		assert.False(t, found, "test-branch should be deleted")
	})
}

func TestConflictHandler_ParseConflicts(t *testing.T) {
	handler := NewConflictHandler()

	conflictContent := `line 1
line 2
<<<<<<< HEAD
our change
=======
their change
>>>>>>> branch
line 3`

	conflicts := handler.parseConflicts(conflictContent)
	assert.Len(t, conflicts, 1)
	
	conflict := conflicts[0]
	assert.Equal(t, 3, conflict.StartLine)
	assert.Equal(t, 7, conflict.EndLine)
	assert.Contains(t, conflict.Ours, "our change")
	assert.Contains(t, conflict.Theirs, "their change")
}

func TestProgressWriter(t *testing.T) {
	pw := NewProgressWriter("Test Operation")
	
	// Write some progress
	testData := []byte("Counting objects: 100% (50/50), done.\n")
	n, err := pw.Write(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
	
	pw.Complete()
}

func TestCircuitBreaker(t *testing.T) {
	cb := errors.NewCircuitBreaker("test", 2, 100*time.Millisecond)
	ctx := context.Background()

	// Success calls
	err := cb.Execute(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "closed", cb.GetState())

	// Fail twice to open circuit
	for i := 0; i < 2; i++ {
		err = cb.Execute(ctx, func() error {
			return fmt.Errorf("test error")
		})
		assert.Error(t, err)
	}
	
	// Circuit should be open
	assert.Equal(t, "open", cb.GetState())
	
	// Next call should fail immediately
	err = cb.Execute(ctx, func() error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Circuit breaker")
	
	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)
	
	// Should be half-open now
	err = cb.Execute(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "closed", cb.GetState())
}

func TestRetryWithBackoff(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	
	// Test successful retry after 2 failures
	err := errors.RetryWithBackoff(ctx, func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New(errors.ErrCodeNetworkUnavailable, "network error").AsRecoverable()
		}
		return nil
	})
	
	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)
	
	// Test non-recoverable error
	attempts = 0
	err = errors.RetryWithBackoff(ctx, func(ctx context.Context) error {
		attempts++
		return errors.New(errors.ErrCodeAuthenticationFailed, "auth failed")
	})
	
	assert.Error(t, err)
	assert.Equal(t, 1, attempts) // Should not retry
}

func TestValidateRepository(t *testing.T) {
	service := NewEnhancedService()
	ctx := context.Background()
	
	// Test validation of non-existent repository
	validation, err := service.ValidateRepository(ctx, "non-existent-repo")
	assert.NoError(t, err)
	assert.False(t, validation.Valid)
	assert.NotEmpty(t, validation.Issues)
	
	// Should have at least one error about repository not found
	found := false
	for _, issue := range validation.Issues {
		if issue.Severity == "ERROR" && strings.Contains(issue.Message, "not found") {
			found = true
			break
		}
	}
	assert.True(t, found, "Should have repository not found error")
}

// Helper function to initialize a test repository
func initTestRepo(path string) error {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return err
	}
	
	// Initialize git repo
	cmds := [][]string{
		{"git", "init"},
		{"git", "config", "user.email", "test@example.com"},
		{"git", "config", "user.name", "Test User"},
	}
	
	for _, args := range cmds {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = path
		if err := cmd.Run(); err != nil {
			return err
		}
	}
	
	// Create README.md
	readmePath := filepath.Join(path, "README.md")
	if err := os.WriteFile(readmePath, []byte("test"), 0644); err != nil {
		return err
	}
	
	// Add and commit
	addCmd := exec.Command("git", "add", ".")
	addCmd.Dir = path
	if err := addCmd.Run(); err != nil {
		return err
	}
	
	commitCmd := exec.Command("git", "commit", "-m", "Initial commit")
	commitCmd.Dir = path
	return commitCmd.Run()
}

func TestCommitOptions_Filtering(t *testing.T) {
	// This test verifies the CommitOptions structure
	options := CommitOptions{
		Limit:      5,
		Since:      time.Now().AddDate(0, -1, 0),
		Until:      time.Now(),
		Author:     "John Doe",
		Branch:     "feature/test",
		PathFilter: "src/",
	}
	
	assert.Equal(t, 5, options.Limit)
	assert.Equal(t, "John Doe", options.Author)
	assert.Equal(t, "feature/test", options.Branch)
	assert.Equal(t, "src/", options.PathFilter)
}

func TestMergeStrategy(t *testing.T) {
	// Test merge strategy constants
	assert.Equal(t, MergeStrategy("ours"), MergeStrategyOurs)
	assert.Equal(t, MergeStrategy("theirs"), MergeStrategyTheirs)
	assert.Equal(t, MergeStrategy("manual"), MergeStrategyManual)
}