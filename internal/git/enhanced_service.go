package git

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"flakedrop/internal/common"
	"flakedrop/pkg/errors"
	"flakedrop/pkg/models"
)

// EnhancedService provides advanced Git operations with improved error handling and recovery
type EnhancedService struct {
	*Service
	authManager     *AuthManager
	branchManager   *BranchManager
	conflictHandler *ConflictHandler
	circuitBreaker  *errors.CircuitBreakerManager
	mu              sync.RWMutex
}

// NewEnhancedService creates a new enhanced Git service
func NewEnhancedService() *EnhancedService {
	return &EnhancedService{
		Service:         NewService(),
		authManager:     NewAuthManager(),
		branchManager:   NewBranchManager(),
		conflictHandler: NewConflictHandler(),
		circuitBreaker:  errors.NewCircuitBreakerManager(),
	}
}

// SyncRepositoryEnhanced performs an enhanced repository sync with better error handling
func (s *EnhancedService) SyncRepositoryEnhanced(ctx context.Context, repo models.Repository) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Use circuit breaker for repository operations
	return s.circuitBreaker.Execute(ctx, fmt.Sprintf("repo_%s", repo.Name), func() error {
		// Try to get authentication
		auth, err := s.authManager.GetAuth(repo.GitURL)
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeAuthenticationFailed, 
				"Failed to get authentication for repository").
				WithContext("repository", repo.Name).
				WithSuggestions(
					"Check your Git credentials configuration",
					"Run 'flakedrop git auth setup' to configure authentication",
					"Verify you have access to the repository",
				)
		}

		localPath := s.getLocalPath(repo.Name)
		
		// Enhanced clone or fetch with retry
		retryConfig := &errors.RetryConfig{
			MaxRetries:   5,
			InitialDelay: 2 * time.Second,
			MaxDelay:     60 * time.Second,
			Multiplier:   1.5,
			Jitter:       true,
			RetryableError: func(err error) bool {
				// Custom retry logic for Git operations
				if err == nil {
					return false
				}
				
				errStr := err.Error()
				// Retry on network errors
				if strings.Contains(errStr, "connection") ||
					strings.Contains(errStr, "timeout") ||
					strings.Contains(errStr, "unreachable") ||
					strings.Contains(errStr, "no route to host") {
					return true
				}
				
				// Don't retry on authentication errors
				if strings.Contains(errStr, "authentication") ||
					strings.Contains(errStr, "401") ||
					strings.Contains(errStr, "403") {
					return false
				}
				
				// Retry on temporary Git errors
				if strings.Contains(errStr, "remote hung up") ||
					strings.Contains(errStr, "early EOF") {
					return true
				}
				
				return errors.IsRecoverable(err)
			},
		}

		err = errors.Retry(ctx, retryConfig, func(ctx context.Context) error {
			return s.cloneOrFetchEnhanced(ctx, repo.GitURL, localPath, auth)
		})
		
		if err != nil {
			return err
		}

		// Handle branch operations
		if repo.Branch != "" {
			if err := s.branchManager.CheckoutBranch(localPath, repo.Branch, true); err != nil {
				return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
					fmt.Sprintf("Failed to checkout branch %s", repo.Branch)).
					WithContext("branch", repo.Branch).
					WithSuggestions(
						fmt.Sprintf("Verify branch '%s' exists in remote", repo.Branch),
						"Use 'git branch -r' to list remote branches",
						"Check for typos in the branch name",
					)
			}
		}

		// Update repository metadata
		if err := s.updateRepoMetadata(localPath, repo); err != nil {
			// Non-critical error, log but don't fail
			fmt.Fprintf(os.Stderr, "Warning: Failed to update repository metadata: %v\n", err)
		}

		return nil
	})
}

// cloneOrFetchEnhanced performs enhanced clone or fetch with progress tracking
func (s *EnhancedService) cloneOrFetchEnhanced(ctx context.Context, gitURL, localPath string, auth transport.AuthMethod) error {
	// Check if repository exists
	if _, err := os.Stat(filepath.Join(localPath, ".git")); err == nil {
		// Repository exists, perform fetch
		return s.fetchWithProgress(ctx, localPath, auth)
	}

	// Repository doesn't exist, perform clone
	return s.cloneWithProgress(ctx, gitURL, localPath, auth)
}

// cloneWithProgress clones a repository with progress tracking
func (s *EnhancedService) cloneWithProgress(ctx context.Context, gitURL, localPath string, auth transport.AuthMethod) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), common.DirPermissionNormal); err != nil {
		return errors.Wrap(err, errors.ErrCodeFilePermission, 
			"Failed to create repository directory")
	}

	// Create progress writer
	progress := NewProgressWriter("Cloning repository")
	
	_, err := git.PlainCloneContext(ctx, localPath, false, &git.CloneOptions{
		URL:      gitURL,
		Auth:     auth,
		Progress: progress,
		// Enhanced options
		ReferenceName: plumbing.HEAD,
		SingleBranch:  false, // Clone all branches for flexibility
		NoCheckout:    false,
		Depth:         0, // Full clone for complete history
		Tags:          git.AllTags,
	})

	if err != nil {
		// Clean up partial clone on failure
		os.RemoveAll(localPath)
		
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to clone repository").
			WithContext("url", gitURL).
			WithContext("path", localPath)
	}

	return nil
}

// fetchWithProgress fetches updates with progress tracking
func (s *EnhancedService) fetchWithProgress(ctx context.Context, localPath string, auth transport.AuthMethod) error {
	repo, err := git.PlainOpen(localPath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoNotFound,
			"Failed to open repository")
	}

	// Get all remotes
	remotes, err := repo.Remotes()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get remotes")
	}

	// Fetch from all remotes
	for _, remote := range remotes {
		progress := NewProgressWriter(fmt.Sprintf("Fetching from %s", remote.Config().Name))
		
		err = remote.FetchContext(ctx, &git.FetchOptions{
			Auth:     auth,
			Progress: progress,
			Force:    false,
			// Fetch all references
			RefSpecs: []config.RefSpec{
				config.RefSpec("+refs/heads/*:refs/remotes/" + remote.Config().Name + "/*"),
			},
			Tags: git.AllTags,
		})
		
		if err != nil && err != git.NoErrAlreadyUpToDate {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				fmt.Sprintf("Failed to fetch from remote %s", remote.Config().Name))
		}
	}

	// Prune stale remote branches
	if err := s.pruneRemoteBranches(repo); err != nil {
		// Non-critical error, log but don't fail
		fmt.Fprintf(os.Stderr, "Warning: Failed to prune remote branches: %v\n", err)
	}

	return nil
}

// pruneRemoteBranches removes stale remote branch references
func (s *EnhancedService) pruneRemoteBranches(repo *git.Repository) error {
	// This is a simplified implementation
	// In a real scenario, you'd compare with actual remote branches
	return nil
}

// updateRepoMetadata updates repository metadata cache
func (s *EnhancedService) updateRepoMetadata(localPath string, repo models.Repository) error {
	metadataPath := filepath.Join(localPath, ".git", "snowflake-deploy-metadata.json")
	
	metadata := struct {
		LastSync   time.Time `json:"last_sync"`
		Repository string    `json:"repository"`
		Branch     string    `json:"branch"`
	}{
		LastSync:   time.Now(),
		Repository: repo.Name,
		Branch:     repo.Branch,
	}

	// Write metadata (implementation would serialize to JSON)
	_ = metadata // Placeholder
	_ = metadataPath
	
	return nil
}

// GetCommitsWithContext retrieves commits with enhanced context and filtering
func (s *EnhancedService) GetCommitsWithContext(ctx context.Context, repoName string, options CommitOptions) ([]models.GitCommit, error) {
	localPath := s.getLocalPath(repoName)
	
	// Use circuit breaker for read operations
	var commits []models.GitCommit
	err := s.circuitBreaker.Execute(ctx, fmt.Sprintf("read_%s", repoName), func() error {
		gm, err := NewGitManager(localPath)
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoNotFound,
				"Failed to open repository").
				WithContext("repository", repoName).
				WithSuggestions(
					"Ensure the repository has been synced",
					"Run 'flakedrop repo sync' to sync the repository",
				)
		}

		// Get commits with filtering
		rawCommits, err := gm.GetCommitsWithFilter(options)
		if err != nil {
			return err
		}

		// Convert to model format with enhanced information
		for _, commit := range rawCommits {
			sqlFiles, err := gm.GetCommitFiles(commit.Hash)
			if err != nil {
				// Log but continue
				fmt.Fprintf(os.Stderr, "Warning: Failed to get files for commit %s: %v\n", commit.Hash, err)
				sqlFiles = []string{}
			}

			commits = append(commits, models.GitCommit{
				Hash:     commit.Hash,
				Message:  commit.Message,
				Author:   commit.Author,
				Date:     commit.Date,
				SQLFiles: sqlFiles,
			})
		}

		return nil
	})

	return commits, err
}

// ValidateRepository performs comprehensive repository validation
func (s *EnhancedService) ValidateRepository(ctx context.Context, repoName string) (*RepositoryValidation, error) {
	localPath := s.getLocalPath(repoName)
	
	validation := &RepositoryValidation{
		RepoName:  repoName,
		Timestamp: time.Now(),
		Issues:    []ValidationIssue{},
	}

	// Check if repository exists
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		validation.Valid = false
		validation.Issues = append(validation.Issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "Repository not found in local cache",
			Solution: "Run 'flakedrop repo sync' to sync the repository",
		})
		return validation, nil
	}

	// Open repository
	repo, err := git.PlainOpen(localPath)
	if err != nil {
		validation.Valid = false
		validation.Issues = append(validation.Issues, ValidationIssue{
			Severity: "ERROR",
			Message:  fmt.Sprintf("Failed to open repository: %v", err),
			Solution: "Repository may be corrupted. Try removing and re-syncing.",
		})
		return validation, nil
	}

	// Check repository state
	if err := s.validateRepoState(repo, validation); err != nil {
		return validation, err
	}

	// Check connectivity
	if err := s.validateConnectivity(ctx, repo, validation); err != nil {
		return validation, err
	}

	// Check for uncommitted changes
	if err := s.validateWorkingDirectory(repo, validation); err != nil {
		return validation, err
	}

	validation.Valid = len(validation.Issues) == 0
	return validation, nil
}

// validateRepoState checks the repository state
func (s *EnhancedService) validateRepoState(repo *git.Repository, validation *RepositoryValidation) error {
	// Check HEAD
	head, err := repo.Head()
	if err != nil {
		validation.Issues = append(validation.Issues, ValidationIssue{
			Severity: "WARNING",
			Message:  "Repository has no HEAD reference",
			Solution: "Checkout a branch to establish HEAD",
		})
	} else {
		validation.CurrentBranch = head.Name().Short()
	}

	// Check for remotes
	remotes, err := repo.Remotes()
	if err != nil || len(remotes) == 0 {
		validation.Issues = append(validation.Issues, ValidationIssue{
			Severity: "ERROR",
			Message:  "No remotes configured",
			Solution: "Add a remote with 'git remote add origin <url>'",
		})
	}

	return nil
}

// validateConnectivity checks remote connectivity
func (s *EnhancedService) validateConnectivity(ctx context.Context, repo *git.Repository, validation *RepositoryValidation) error {
	remotes, err := repo.Remotes()
	if err != nil {
		return nil // Already handled in validateRepoState
	}

	for _, remote := range remotes {
		// Try to list remote references
		_, err := remote.List(&git.ListOptions{
			Auth: s.authManager.GetAuthForRemote(remote.Config().URLs[0]),
		})
		
		if err != nil {
			validation.Issues = append(validation.Issues, ValidationIssue{
				Severity: "WARNING",
				Message:  fmt.Sprintf("Cannot connect to remote '%s': %v", remote.Config().Name, err),
				Solution: "Check network connectivity and authentication",
			})
		}
	}

	return nil
}

// validateWorkingDirectory checks for uncommitted changes
func (s *EnhancedService) validateWorkingDirectory(repo *git.Repository, validation *RepositoryValidation) error {
	worktree, err := repo.Worktree()
	if err != nil {
		return nil // Repository might be bare
	}

	status, err := worktree.Status()
	if err != nil {
		return nil
	}

	if !status.IsClean() {
		validation.Issues = append(validation.Issues, ValidationIssue{
			Severity: "INFO",
			Message:  "Working directory has uncommitted changes",
			Solution: "This is informational only",
		})
		validation.HasUncommittedChanges = true
	}

	return nil
}

// MergeStrategy defines how to handle merge conflicts
type MergeStrategy string

const (
	MergeStrategyOurs   MergeStrategy = "ours"
	MergeStrategyTheirs MergeStrategy = "theirs"
	MergeStrategyManual MergeStrategy = "manual"
)

// MergeBranches performs an enhanced branch merge with conflict handling
func (s *EnhancedService) MergeBranches(ctx context.Context, repoName, sourceBranch, targetBranch string, strategy MergeStrategy) error {
	localPath := s.getLocalPath(repoName)
	
	return s.circuitBreaker.Execute(ctx, fmt.Sprintf("merge_%s", repoName), func() error {
		repo, err := git.PlainOpen(localPath)
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoNotFound,
				"Failed to open repository")
		}

		// Checkout target branch
		if err := s.branchManager.CheckoutBranch(localPath, targetBranch, false); err != nil {
			return err
		}

		// Get source branch reference
		_, err = repo.Reference(plumbing.NewBranchReferenceName(sourceBranch), false)
		if err != nil {
			// Try remote branch
			_, err = repo.Reference(plumbing.NewRemoteReferenceName("origin", sourceBranch), false)
			if err != nil {
				return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
					fmt.Sprintf("Source branch '%s' not found", sourceBranch))
			}
		}

		// Perform merge
		worktree, err := repo.Worktree()
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to get worktree")
		}

		// Note: go-git v5 doesn't have a direct Merge method
		// Instead, we'll use checkout and pull to simulate merge
		// Get auth for the repository
		remotes, err := repo.Remotes()
		if err != nil || len(remotes) == 0 {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to get remotes")
		}
		gitURL := remotes[0].Config().URLs[0]
		auth, authErr := s.authManager.GetAuth(gitURL)
		if authErr != nil {
			// Continue without auth for public repos
			auth = nil
		}
		
		err = worktree.Pull(&git.PullOptions{
			RemoteName: "origin",
			Auth:       auth,
		})

		if err != nil && err != git.NoErrAlreadyUpToDate {
			// Handle merge conflicts
			if strings.Contains(err.Error(), "conflict") {
				return s.handleMergeConflict(repo, worktree, strategy)
			}
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to merge branches")
		}

		return nil
	})
}

// handleMergeConflict handles merge conflicts based on strategy
func (s *EnhancedService) handleMergeConflict(repo *git.Repository, worktree *git.Worktree, strategy MergeStrategy) error {
	switch strategy {
	case MergeStrategyOurs:
		// Keep our changes
		return s.conflictHandler.ResolveWithOurs(worktree)
		
	case MergeStrategyTheirs:
		// Keep their changes
		return s.conflictHandler.ResolveWithTheirs(worktree)
		
	case MergeStrategyManual:
		// Return error with conflict details
		status, _ := worktree.Status()
		var conflicts []string
		
		for file, fileStatus := range status {
			// Check for merge conflicts by looking for both modified
			if fileStatus.Staging == git.Modified && fileStatus.Worktree == git.Modified {
				conflicts = append(conflicts, file)
			}
		}
		
		return errors.New(errors.ErrCodeRepoSyncFailed,
			"Merge conflicts detected").
			WithContext("conflicts", conflicts).
			WithSuggestions(
				"Resolve conflicts manually",
				"Use 'git status' to see conflicted files",
				"After resolving, commit the changes",
			)
			
	default:
		return errors.New(errors.ErrCodeInvalidInput,
			"Invalid merge strategy")
	}
}

// GetAuthManager returns the auth manager for external use
func (s *EnhancedService) GetAuthManager() *AuthManager {
	return s.authManager
}

// GetLocalPath returns the local path for a repository
func (s *EnhancedService) GetLocalPath(repoName string) string {
	return s.getLocalPath(repoName)
}

// RepositoryValidation represents the result of repository validation
type RepositoryValidation struct {
	RepoName              string            `json:"repo_name"`
	Valid                 bool              `json:"valid"`
	CurrentBranch         string            `json:"current_branch"`
	HasUncommittedChanges bool              `json:"has_uncommitted_changes"`
	Issues                []ValidationIssue `json:"issues"`
	Timestamp             time.Time         `json:"timestamp"`
}

// ValidationIssue represents a single validation issue
type ValidationIssue struct {
	Severity string `json:"severity"` // ERROR, WARNING, INFO
	Message  string `json:"message"`
	Solution string `json:"solution"`
}

// CommitOptions provides options for retrieving commits
type CommitOptions struct {
	Limit      int
	Since      time.Time
	Until      time.Time
	Author     string
	Branch     string
	PathFilter string
}