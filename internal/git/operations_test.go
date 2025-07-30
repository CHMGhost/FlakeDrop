package git

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewGitManager tests the creation of a new GitManager
func TestNewGitManager(t *testing.T) {
	// Create a temporary directory for the test repository
	tempDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Initialize a git repository
	_, err = git.PlainInit(tempDir, false)
	require.NoError(t, err)

	// Create GitManager
	gm, err := NewGitManager(tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, gm)
	assert.Equal(t, tempDir, gm.repoPath)

	// Test with non-existent repository
	_, err = NewGitManager("/non/existent/path")
	assert.Error(t, err)
}

// TestGetRecentCommits tests fetching recent commits
func TestGetRecentCommits(t *testing.T) {
	// Create a temporary repository with commits
	tempDir, repo := createTestRepository(t)
	defer os.RemoveAll(tempDir)

	// Create test commits
	createTestCommits(t, repo, tempDir, 5)

	// Create GitManager
	gm, err := NewGitManager(tempDir)
	require.NoError(t, err)

	// Test getting recent commits
	commits, err := gm.GetRecentCommits(3)
	assert.NoError(t, err)
	assert.Len(t, commits, 3)

	// Verify commit order (most recent first)
	for i := 0; i < len(commits)-1; i++ {
		assert.True(t, commits[i].Date.After(commits[i+1].Date) || commits[i].Date.Equal(commits[i+1].Date))
	}

	// Test with limit larger than available commits
	commits, err = gm.GetRecentCommits(10)
	assert.NoError(t, err)
	assert.Len(t, commits, 5)
}

// TestGetCommitFiles tests getting changed files from a commit
func TestGetCommitFiles(t *testing.T) {
	// Create a temporary repository
	tempDir, repo := createTestRepository(t)
	defer os.RemoveAll(tempDir)

	// Create initial commit with SQL files
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	// Add SQL files
	sqlFiles := []string{"schema.sql", "data.sql", "procedures/proc1.sql"}
	for _, file := range sqlFiles {
		dir := filepath.Dir(file)
		if dir != "." {
			err = os.MkdirAll(filepath.Join(tempDir, dir), 0755)
			require.NoError(t, err)
		}
		err = os.WriteFile(filepath.Join(tempDir, file), []byte("-- SQL content"), 0644)
		require.NoError(t, err)
		_, err = worktree.Add(file)
		require.NoError(t, err)
	}

	// Add non-SQL file (should be ignored)
	err = os.WriteFile(filepath.Join(tempDir, "readme.txt"), []byte("readme"), 0644)
	require.NoError(t, err)
	_, err = worktree.Add("readme.txt")
	require.NoError(t, err)

	// Commit
	commit, err := worktree.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Create GitManager
	gm, err := NewGitManager(tempDir)
	require.NoError(t, err)

	// Get files from commit
	files, err := gm.GetCommitFiles(commit.String())
	assert.NoError(t, err)
	assert.Len(t, files, 3) // Only SQL files

	// Verify all returned files are SQL files
	for _, file := range files {
		assert.Contains(t, file, ".sql")
	}
}

// TestGetFileContent tests retrieving file content from a specific commit
func TestGetFileContent(t *testing.T) {
	// Create a temporary repository
	tempDir, repo := createTestRepository(t)
	defer os.RemoveAll(tempDir)

	// Create a commit with a file
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	testContent := `CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);`
	testFile := "create_users.sql"

	err = os.WriteFile(filepath.Join(tempDir, testFile), []byte(testContent), 0644)
	require.NoError(t, err)
	_, err = worktree.Add(testFile)
	require.NoError(t, err)

	commit, err := worktree.Commit("Add users table", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Create GitManager
	gm, err := NewGitManager(tempDir)
	require.NoError(t, err)

	// Get file content
	content, err := gm.GetFileContent(commit.String(), testFile)
	assert.NoError(t, err)
	assert.Equal(t, testContent, content)

	// Test with non-existent file
	_, err = gm.GetFileContent(commit.String(), "nonexistent.sql")
	assert.Error(t, err)
}

// TestCloneOrFetch tests cloning and fetching repositories
func TestCloneOrFetch(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "clone-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a source repository (non-bare)
	sourceDir := filepath.Join(tempDir, "source")
	sourceRepo, err := git.PlainInit(sourceDir, false)
	require.NoError(t, err)

	// Add a commit to make the repository non-empty
	worktree, err := sourceRepo.Worktree()
	require.NoError(t, err)

	testFile := filepath.Join(sourceDir, "test.sql")
	err = os.WriteFile(testFile, []byte("-- test"), 0644)
	require.NoError(t, err)

	_, err = worktree.Add("test.sql")
	require.NoError(t, err)

	_, err = worktree.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Create a destination path
	destDir := filepath.Join(tempDir, "dest")

	// Test cloning
	err = CloneOrFetch(sourceDir, destDir)
	assert.NoError(t, err)

	// Verify repository was cloned
	_, err = git.PlainOpen(destDir)
	assert.NoError(t, err)

	// Test fetching (should not error on already existing repo)
	err = CloneOrFetch(sourceDir, destDir)
	assert.NoError(t, err)
}

// TestGetCacheDirectory tests the cache directory path
func TestGetCacheDirectory(t *testing.T) {
	cacheDir := GetCacheDirectory()
	assert.NotEmpty(t, cacheDir)
	assert.Contains(t, cacheDir, ".flakedrop")
	assert.Contains(t, cacheDir, "repos")
}

// TestGetAuthMethod tests authentication method selection
func TestGetAuthMethod(t *testing.T) {
	tests := []struct {
		name     string
		gitURL   string
		envVars  map[string]string
		checkNil bool
	}{
		{
			name:     "SSH URL",
			gitURL:   "git@github.com:user/repo.git",
			checkNil: true, // Will be nil if no SSH key exists
		},
		{
			name:   "HTTPS with credentials",
			gitURL: "https://github.com/user/repo.git",
			envVars: map[string]string{
				"GIT_USERNAME": "testuser",
				"GIT_PASSWORD": "testpass",
			},
			checkNil: false,
		},
		{
			name:   "HTTPS with GitHub token",
			gitURL: "https://github.com/user/repo.git",
			envVars: map[string]string{
				"GITHUB_TOKEN": "ghp_testtoken",
			},
			checkNil: false,
		},
		{
			name:     "HTTPS without auth",
			gitURL:   "https://github.com/user/repo.git",
			checkNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
				defer os.Unsetenv(k)
			}

			auth := getAuthMethod(tt.gitURL)
			if tt.checkNil {
				// We can't assert nil/not nil without knowing if SSH keys exist
				// Just verify the function doesn't panic
				_ = auth
			} else {
				assert.NotNil(t, auth)
			}
		})
	}
}

// Helper function to create a test repository
func createTestRepository(t *testing.T) (string, *git.Repository) {
	tempDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)

	repo, err := git.PlainInit(tempDir, false)
	require.NoError(t, err)

	return tempDir, repo
}

// Helper function to create test commits
func createTestCommits(t *testing.T, repo *git.Repository, tempDir string, count int) {
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	for i := 0; i < count; i++ {
		// Create a file
		filename := fmt.Sprintf("file%d.sql", i)
		content := fmt.Sprintf("-- SQL file %d", i)
		err = os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0644)
		require.NoError(t, err)

		// Add and commit
		_, err = worktree.Add(filename)
		require.NoError(t, err)

		_, err = worktree.Commit(fmt.Sprintf("Commit %d", i), &git.CommitOptions{
			Author: &object.Signature{
				Name:  fmt.Sprintf("User %d", i),
				Email: fmt.Sprintf("user%d@example.com", i),
				When:  time.Now().Add(time.Duration(i) * time.Minute),
			},
		})
		require.NoError(t, err)
	}
}

// TestCheckoutBranch tests checking out branches
func TestCheckoutBranch(t *testing.T) {
	// Create a temporary directory for the test repository
	tempDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Initialize a git repository
	repo, err := git.PlainInit(tempDir, false)
	require.NoError(t, err)

	// Create initial commit on main branch
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(tempDir, "main.sql"), []byte("-- main branch"), 0644)
	require.NoError(t, err)
	_, err = worktree.Add("main.sql")
	require.NoError(t, err)
	_, err = worktree.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Create a new branch
	err = worktree.Checkout(&git.CheckoutOptions{
		Branch: "refs/heads/feature",
		Create: true,
	})
	require.NoError(t, err)

	// Add a commit to the feature branch
	err = os.WriteFile(filepath.Join(tempDir, "feature.sql"), []byte("-- feature branch"), 0644)
	require.NoError(t, err)
	_, err = worktree.Add("feature.sql")
	require.NoError(t, err)
	_, err = worktree.Commit("Feature commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Switch back to main/master (git might use either as default)
	head, err := repo.Head()
	require.NoError(t, err)
	mainBranch := head.Name().Short()
	
	err = worktree.Checkout(&git.CheckoutOptions{
		Branch: plumbing.ReferenceName("refs/heads/" + mainBranch),
	})
	require.NoError(t, err)

	// Test CheckoutBranch function
	err = CheckoutBranch(tempDir, "feature")
	assert.NoError(t, err)

	// Verify we're on the feature branch
	branch, err := GetCurrentBranch(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, "feature", branch)

	// Test checking out main branch
	err = CheckoutBranch(tempDir, mainBranch)
	assert.NoError(t, err)

	branch, err = GetCurrentBranch(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, mainBranch, branch)
}

// TestGetCurrentBranch tests getting the current branch
func TestGetCurrentBranch(t *testing.T) {
	// Create a temporary directory for the test repository
	tempDir, err := os.MkdirTemp("", "git-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Initialize a git repository
	repo, err := git.PlainInit(tempDir, false)
	require.NoError(t, err)

	// Create initial commit
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(tempDir, "test.sql"), []byte("-- test"), 0644)
	require.NoError(t, err)
	_, err = worktree.Add("test.sql")
	require.NoError(t, err)
	_, err = worktree.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Test getting current branch (should be main or master)
	branch, err := GetCurrentBranch(tempDir)
	assert.NoError(t, err)
	assert.Contains(t, []string{"main", "master"}, branch)

	// Create and checkout a new branch
	err = worktree.Checkout(&git.CheckoutOptions{
		Branch: "refs/heads/develop",
		Create: true,
	})
	require.NoError(t, err)

	// Test getting current branch
	branch, err = GetCurrentBranch(tempDir)
	assert.NoError(t, err)
	assert.Equal(t, "develop", branch)

	// Test with non-existent repository
	_, err = GetCurrentBranch("/non/existent/path")
	assert.Error(t, err)
}