package git

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"flakedrop/pkg/models"
)

// TestNewService tests creating a new git service
func TestNewService(t *testing.T) {
	service := NewService()
	assert.NotNil(t, service)
	assert.NotEmpty(t, service.cacheDir)
}

// TestSyncRepository tests syncing a repository
func TestSyncRepository(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "git-service-test-*")
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

	// Create service with custom cache directory
	service := &Service{
		cacheDir: filepath.Join(tempDir, "cache"),
	}

	// Test syncing repository
	repo := models.Repository{
		Name:   "test-repo",
		GitURL: sourceDir,
		Branch: "main",
	}

	err = service.SyncRepository(repo)
	assert.NoError(t, err)

	// Verify repository was cloned
	localPath := service.getLocalPath(repo.Name)
	_, err = os.Stat(localPath)
	assert.NoError(t, err)
}

// TestGetRecentCommitsForRepo tests retrieving recent commits
func TestGetRecentCommitsForRepo(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "git-service-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create service with custom cache directory
	service := &Service{
		cacheDir: tempDir,
	}

	// Create a test repository with commits
	repoName := "test-repo"
	localPath := service.getLocalPath(repoName)
	repo, err := git.PlainInit(localPath, false)
	require.NoError(t, err)

	// Create test commits with SQL files
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		// Create SQL file
		filename := fmt.Sprintf("query%d.sql", i)
		content := fmt.Sprintf("SELECT * FROM table%d;", i)
		err = os.WriteFile(filepath.Join(localPath, filename), []byte(content), 0644)
		require.NoError(t, err)

		// Add and commit
		_, err = worktree.Add(filename)
		require.NoError(t, err)

		_, err = worktree.Commit(fmt.Sprintf("Add query %d", i), &git.CommitOptions{
			Author: &object.Signature{
				Name:  "Test User",
				Email: "test@example.com",
				When:  time.Now().Add(time.Duration(i) * time.Hour),
			},
		})
		require.NoError(t, err)
	}

	// Test getting recent commits
	commits, err := service.GetRecentCommitsForRepo(repoName, 2)
	assert.NoError(t, err)
	assert.Len(t, commits, 2)

	// Verify commits have SQL files
	for _, commit := range commits {
		assert.NotEmpty(t, commit.Hash)
		assert.NotEmpty(t, commit.Message)
		assert.NotEmpty(t, commit.Author)
		assert.Len(t, commit.SQLFiles, 1)
	}

	// Test with non-existent repository
	_, err = service.GetRecentCommitsForRepo("non-existent", 10)
	assert.Error(t, err)
}

// TestGetFileFromCommit tests retrieving file content from a commit
func TestGetFileFromCommit(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "git-service-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create service with custom cache directory
	service := &Service{
		cacheDir: tempDir,
	}

	// Create a test repository
	repoName := "test-repo"
	localPath := service.getLocalPath(repoName)
	repo, err := git.PlainInit(localPath, false)
	require.NoError(t, err)

	// Create a commit with a file
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	testFile := "schema.sql"
	testContent := "CREATE TABLE test (id INT);"
	err = os.WriteFile(filepath.Join(localPath, testFile), []byte(testContent), 0644)
	require.NoError(t, err)

	_, err = worktree.Add(testFile)
	require.NoError(t, err)

	hash, err := worktree.Commit("Add schema", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Test getting file from commit
	file, err := service.GetFileFromCommit(repoName, hash.String(), testFile)
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, testFile, file.Path)
	assert.Equal(t, testContent, file.Content)
	assert.Equal(t, hash.String(), file.Hash)
}

// TestGetSQLFilesFromCommit tests retrieving all SQL files from a commit
func TestGetSQLFilesFromCommit(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "git-service-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create service with custom cache directory
	service := &Service{
		cacheDir: tempDir,
	}

	// Create a test repository
	repoName := "test-repo"
	localPath := service.getLocalPath(repoName)
	repo, err := git.PlainInit(localPath, false)
	require.NoError(t, err)

	// Create a commit with multiple SQL files
	worktree, err := repo.Worktree()
	require.NoError(t, err)

	sqlFiles := map[string]string{
		"create.sql": "CREATE TABLE users (id INT);",
		"insert.sql": "INSERT INTO users VALUES (1);",
		"query.sql":  "SELECT * FROM users;",
	}

	for name, content := range sqlFiles {
		err = os.WriteFile(filepath.Join(localPath, name), []byte(content), 0644)
		require.NoError(t, err)
		_, err = worktree.Add(name)
		require.NoError(t, err)
	}

	// Add a non-SQL file (should be ignored)
	err = os.WriteFile(filepath.Join(localPath, "readme.md"), []byte("README"), 0644)
	require.NoError(t, err)
	_, err = worktree.Add("readme.md")
	require.NoError(t, err)

	hash, err := worktree.Commit("Add SQL files", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test User",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	require.NoError(t, err)

	// Test getting SQL files from commit
	files, err := service.GetSQLFilesFromCommit(repoName, hash.String())
	assert.NoError(t, err)
	assert.Len(t, files, 3) // Only SQL files

	// Verify file contents
	for _, file := range files {
		expectedContent, exists := sqlFiles[file.Path]
		assert.True(t, exists, "Unexpected file: %s", file.Path)
		assert.Equal(t, expectedContent, file.Content)
		assert.Equal(t, hash.String(), file.Hash)
	}
}

// TestGetCachedRepositories tests listing cached repositories
func TestGetCachedRepositories(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "git-service-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create service with custom cache directory
	service := &Service{
		cacheDir: tempDir,
	}

	// Create some test repositories
	repoNames := []string{"repo1", "repo2", "repo3"}
	for _, name := range repoNames {
		repoPath := filepath.Join(tempDir, name)
		_, err := git.PlainInit(repoPath, false)
		require.NoError(t, err)
	}

	// Create a non-git directory (should be ignored)
	err = os.MkdirAll(filepath.Join(tempDir, "not-a-repo"), 0755)
	require.NoError(t, err)

	// Test getting cached repositories
	repos, err := service.GetCachedRepositories()
	assert.NoError(t, err)
	assert.Len(t, repos, 3)

	// Verify repository names
	foundRepos := make(map[string]bool)
	for _, repo := range repos {
		foundRepos[repo.RepoName] = true
		assert.NotEmpty(t, repo.LocalPath)
		assert.Contains(t, repo.LocalPath, repo.RepoName)
	}

	for _, name := range repoNames {
		assert.True(t, foundRepos[name], "Repository %s not found", name)
	}
}

// TestCleanCache tests cleaning old cached repositories
func TestCleanCache(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "git-service-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create service with custom cache directory
	service := &Service{
		cacheDir: tempDir,
	}

	// Create test repositories
	oldRepo := filepath.Join(tempDir, "old-repo")
	newRepo := filepath.Join(tempDir, "new-repo")

	_, err = git.PlainInit(oldRepo, false)
	require.NoError(t, err)
	_, err = git.PlainInit(newRepo, false)
	require.NoError(t, err)

	// Make old-repo appear old by modifying its timestamp
	oldTime := time.Now().Add(-48 * time.Hour)
	err = os.Chtimes(oldRepo, oldTime, oldTime)
	require.NoError(t, err)

	// Clean cache with 24 hour max age
	err = service.CleanCache(24 * time.Hour)
	assert.NoError(t, err)

	// Verify old repo was removed
	_, err = os.Stat(oldRepo)
	assert.True(t, os.IsNotExist(err))

	// Verify new repo still exists
	_, err = os.Stat(newRepo)
	assert.NoError(t, err)
}

// TestGetLocalPath tests path generation for repositories
func TestGetLocalPath(t *testing.T) {
	service := NewService()

	tests := []struct {
		repoName string
		expected string
	}{
		{"simple-repo", "simple-repo"},
		{"org/repo", "org_repo"},
		{"path\\with\\backslash", "path_with_backslash"},
	}

	for _, tt := range tests {
		t.Run(tt.repoName, func(t *testing.T) {
			path := service.getLocalPath(tt.repoName)
			assert.Contains(t, path, tt.expected)
			assert.Contains(t, path, service.cacheDir)
		})
	}
}