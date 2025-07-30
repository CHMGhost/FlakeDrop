package git

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"flakedrop/internal/common"
	"flakedrop/pkg/errors"
	"flakedrop/pkg/models"
)

// Service provides high-level git operations for repositories
type Service struct {
	cacheDir string
}

// NewService creates a new git service
func NewService() *Service {
	return &Service{
		cacheDir: GetCacheDirectory(),
	}
}

// SyncRepository clones or updates a repository based on the configuration
func (s *Service) SyncRepository(repo models.Repository) error {
	localPath := s.getLocalPath(repo.Name)
	
	// Use retry mechanism for network operations
	ctx := context.Background()
	err := errors.RetryWithBackoff(ctx, func(ctx context.Context) error {
		// Clone or fetch the repository
		if err := CloneOrFetch(repo.GitURL, localPath); err != nil {
			// Check if it's a network-related error
			if strings.Contains(err.Error(), "connection") || 
			   strings.Contains(err.Error(), "timeout") ||
			   strings.Contains(err.Error(), "unreachable") {
				return errors.New(errors.ErrCodeNetworkUnavailable, 
					"Network error while syncing repository").
					WithContext("repository", repo.Name).
					WithContext("url", repo.GitURL).
					AsRecoverable()
			}
			
			// Check for authentication errors
			if strings.Contains(err.Error(), "authentication") ||
			   strings.Contains(err.Error(), "unauthorized") {
				return errors.New(errors.ErrCodeRepoAccessDenied,
					"Authentication failed for repository").
					WithContext("repository", repo.Name).
					WithSuggestions(
						"Check your Git credentials",
						"Ensure you have access to the repository",
						"Try cloning the repository manually to verify access",
					)
			}
			
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed, 
				fmt.Sprintf("Failed to sync repository %s", repo.Name))
		}
		
		return nil
	})
	
	if err != nil {
		return err
	}

	// If a specific branch is configured, checkout that branch
	if repo.Branch != "" && repo.Branch != "main" && repo.Branch != "master" {
		err = s.checkoutBranch(localPath, repo.Branch)
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				fmt.Sprintf("Failed to checkout branch %s", repo.Branch)).
				WithContext("branch", repo.Branch).
				WithSuggestions(
					fmt.Sprintf("Verify branch '%s' exists", repo.Branch),
					"Check for typos in branch name",
					"Use 'git branch -r' to list remote branches",
				)
		}
	}

	return nil
}

// GetRecentCommitsForRepo retrieves recent commits for a specific repository
func (s *Service) GetRecentCommitsForRepo(repoName string, limit int) ([]models.GitCommit, error) {
	localPath := s.getLocalPath(repoName)
	
	// Check if repository exists locally
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("repository %s not found in cache, please sync first", repoName)
	}

	// Create git manager
	gm, err := NewGitManager(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}

	// Get recent commits
	commits, err := gm.GetRecentCommits(limit)
	if err != nil {
		return nil, err
	}

	// Convert to model format and get SQL files for each commit
	var result []models.GitCommit
	for _, commit := range commits {
		sqlFiles, err := gm.GetCommitFiles(commit.Hash)
		if err != nil {
			// Log error but continue with other commits
			fmt.Fprintf(os.Stderr, "Warning: failed to get files for commit %s: %v\n", commit.Hash, err)
			sqlFiles = []string{}
		}

		result = append(result, models.GitCommit{
			Hash:     commit.Hash,
			Message:  commit.Message,
			Author:   commit.Author,
			Date:     commit.Date,
			SQLFiles: sqlFiles,
		})
	}

	return result, nil
}

// GetFileFromCommit retrieves a specific file content from a commit
func (s *Service) GetFileFromCommit(repoName, commitHash, filePath string) (*models.GitFile, error) {
	localPath := s.getLocalPath(repoName)
	
	// Create git manager
	gm, err := NewGitManager(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}

	// Get file content
	content, err := gm.GetFileContent(commitHash, filePath)
	if err != nil {
		return nil, err
	}

	return &models.GitFile{
		Path:    filePath,
		Content: content,
		Hash:    commitHash,
	}, nil
}

// GetSQLFilesFromCommit retrieves all SQL files from a specific commit
func (s *Service) GetSQLFilesFromCommit(repoName, commitHash string) ([]models.GitFile, error) {
	localPath := s.getLocalPath(repoName)
	
	// Create git manager
	gm, err := NewGitManager(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}

	// Get list of SQL files
	fileList, err := gm.GetCommitFiles(commitHash)
	if err != nil {
		return nil, err
	}

	// Get content for each file
	var files []models.GitFile
	for _, filePath := range fileList {
		content, err := gm.GetFileContent(commitHash, filePath)
		if err != nil {
			// Log error but continue with other files
			fmt.Fprintf(os.Stderr, "Warning: failed to get content for file %s: %v\n", filePath, err)
			continue
		}

		files = append(files, models.GitFile{
			Path:    filePath,
			Content: content,
			Hash:    commitHash,
		})
	}

	return files, nil
}

// GetCachedRepositories returns information about all cached repositories
func (s *Service) GetCachedRepositories() ([]models.RepoCache, error) {
	// Ensure cache directory exists
	if err := os.MkdirAll(s.cacheDir, common.DirPermissionNormal); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	entries, err := os.ReadDir(s.cacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache directory: %w", err)
	}

	var repos []models.RepoCache
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		repoPath := filepath.Join(s.cacheDir, entry.Name())
		
		// Check if it's a git repository
		if _, err := os.Stat(filepath.Join(repoPath, ".git")); os.IsNotExist(err) {
			continue
		}

		// Get repository info
		info, err := entry.Info()
		if err != nil {
			continue
		}

		repos = append(repos, models.RepoCache{
			RepoName:    entry.Name(),
			LocalPath:   repoPath,
			LastFetched: info.ModTime(),
			Branch:      s.getCurrentBranch(repoPath),
		})
	}

	return repos, nil
}

// CleanCache removes cached repositories that haven't been accessed recently
func (s *Service) CleanCache(maxAge time.Duration) error {
	repos, err := s.GetCachedRepositories()
	if err != nil {
		return err
	}

	now := time.Now()
	for _, repo := range repos {
		if now.Sub(repo.LastFetched) > maxAge {
			if err := os.RemoveAll(repo.LocalPath); err != nil {
				return fmt.Errorf("failed to remove old repository %s: %w", repo.RepoName, err)
			}
		}
	}

	return nil
}

// getLocalPath returns the local cache path for a repository
func (s *Service) getLocalPath(repoName string) string {
	// Sanitize repository name for filesystem
	safeName := strings.ReplaceAll(repoName, "/", "_")
	safeName = strings.ReplaceAll(safeName, "\\", "_")
	return filepath.Join(s.cacheDir, safeName)
}

// checkoutBranch switches to a specific branch in the repository
func (s *Service) checkoutBranch(repoPath, branch string) error {
	return CheckoutBranch(repoPath, branch)
}

// getCurrentBranch returns the current branch of a repository
func (s *Service) getCurrentBranch(repoPath string) string {
	branch, err := GetCurrentBranch(repoPath)
	if err != nil {
		// Default to main if we can't determine the branch
		return "main"
	}
	return branch
}

// CloneOrUpdate clones a repository if it doesn't exist, or updates it if it does
func (s *Service) CloneOrUpdate(gitURL, localPath, branch string) error {
	// Use the existing CloneOrFetch function
	if err := CloneOrFetch(gitURL, localPath); err != nil {
		return fmt.Errorf("failed to clone or fetch repository: %w", err)
	}
	
	// Checkout the specified branch if provided
	if branch != "" && branch != "main" && branch != "master" {
		if err := s.checkoutBranch(localPath, branch); err != nil {
			return fmt.Errorf("failed to checkout branch %s: %w", branch, err)
		}
	}
	
	return nil
}

// GetChangedFiles returns the list of changed files for a specific commit
func (s *Service) GetChangedFiles(repoPath, commitHash string) ([]string, error) {
	// Create git manager
	gm, err := NewGitManager(repoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}
	
	// Get the list of files from the commit
	files, err := gm.GetCommitFiles(commitHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get files for commit %s: %w", commitHash, err)
	}
	
	return files, nil
}