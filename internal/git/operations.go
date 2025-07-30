package git

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"flakedrop/internal/common"
)

// CommitInfo represents information about a git commit
type CommitInfo struct {
	Hash    string
	Message string
	Author  string
	Date    time.Time
}

// FileInfo represents information about a file in a commit
type FileInfo struct {
	Path    string
	Content string
}

// GitManager handles git operations for repositories
type GitManager struct {
	repoPath string
	repo     *git.Repository
}

// NewGitManager creates a new GitManager instance
func NewGitManager(repoPath string) (*GitManager, error) {
	// Open existing repository
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open repository: %w", err)
	}

	return &GitManager{
		repoPath: repoPath,
		repo:     repo,
	}, nil
}

// GetRecentCommits returns the most recent commits from the repository
func (gm *GitManager) GetRecentCommits(limit int) ([]CommitInfo, error) {
	// Get the HEAD reference
	ref, err := gm.repo.Head()
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD reference: %w", err)
	}

	// Get the commit history
	commitIter, err := gm.repo.Log(&git.LogOptions{
		From: ref.Hash(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get commit log: %w", err)
	}

	var commits []CommitInfo
	count := 0

	err = commitIter.ForEach(func(c *object.Commit) error {
		if count >= limit {
			return nil
		}

		commits = append(commits, CommitInfo{
			Hash:    c.Hash.String(),
			Message: strings.TrimSpace(c.Message),
			Author:  c.Author.Name,
			Date:    c.Author.When,
		})

		count++
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate commits: %w", err)
	}

	return commits, nil
}

// GetCommitFiles returns the list of changed SQL files in a specific commit
func (gm *GitManager) GetCommitFiles(hash string) ([]string, error) {
	// Get the commit object
	commitHash := plumbing.NewHash(hash)
	commit, err := gm.repo.CommitObject(commitHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit object: %w", err)
	}

	// Get the tree for this commit
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get commit tree: %w", err)
	}

	// Get parent commit to compare changes
	var parentTree *object.Tree
	if commit.NumParents() > 0 {
		parent, err := commit.Parent(0)
		if err != nil {
			return nil, fmt.Errorf("failed to get parent commit: %w", err)
		}
		parentTree, err = parent.Tree()
		if err != nil {
			return nil, fmt.Errorf("failed to get parent tree: %w", err)
		}
	}

	// Get changes between commits
	var files []string
	if parentTree != nil {
		changes, err := parentTree.Diff(tree)
		if err != nil {
			return nil, fmt.Errorf("failed to get diff: %w", err)
		}

		for _, change := range changes {
			// Only include SQL files
			if strings.HasSuffix(strings.ToLower(change.To.Name), ".sql") {
				files = append(files, change.To.Name)
			}
		}
	} else {
		// If no parent, this is the initial commit, list all SQL files
		err = tree.Files().ForEach(func(f *object.File) error {
			if strings.HasSuffix(strings.ToLower(f.Name), ".sql") {
				files = append(files, f.Name)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to iterate files: %w", err)
		}
	}

	return files, nil
}

// GetFileContent returns the content of a file at a specific commit
func (gm *GitManager) GetFileContent(hash string, path string) (string, error) {
	// Get the commit object
	commitHash := plumbing.NewHash(hash)
	commit, err := gm.repo.CommitObject(commitHash)
	if err != nil {
		return "", fmt.Errorf("failed to get commit object: %w", err)
	}

	// Get the file from the commit tree
	file, err := commit.File(path)
	if err != nil {
		return "", fmt.Errorf("failed to get file from commit: %w", err)
	}

	// Get file content
	content, err := file.Contents()
	if err != nil {
		return "", fmt.Errorf("failed to get file content: %w", err)
	}

	return content, nil
}

// CloneOrFetch clones a repository or fetches updates if it already exists
func CloneOrFetch(gitURL, localPath string) error {
	// Ensure the cache directory exists
	cacheDir := filepath.Dir(localPath)
	if err := os.MkdirAll(cacheDir, common.DirPermissionNormal); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Check if repository already exists
	if _, err := os.Stat(filepath.Join(localPath, ".git")); err == nil {
		// Repository exists, fetch updates
		repo, err := git.PlainOpen(localPath)
		if err != nil {
			return fmt.Errorf("failed to open existing repository: %w", err)
		}

		// Get the remote
		remote, err := repo.Remote("origin")
		if err != nil {
			return fmt.Errorf("failed to get remote: %w", err)
		}

		// Fetch updates
		auth := getAuthMethod(gitURL)
		err = remote.Fetch(&git.FetchOptions{
			Auth:     auth,
			Progress: os.Stdout,
		})
		if err != nil && err != git.NoErrAlreadyUpToDate {
			return fmt.Errorf("failed to fetch updates: %w", err)
		}

		return nil
	}

	// Repository doesn't exist, clone it
	auth := getAuthMethod(gitURL)
	_, err := git.PlainClone(localPath, false, &git.CloneOptions{
		URL:      gitURL,
		Auth:     auth,
		Progress: os.Stdout,
	})
	if err != nil {
		return fmt.Errorf("failed to clone repository: %w", err)
	}

	return nil
}

// getAuthMethod returns the appropriate auth method based on the URL
func getAuthMethod(gitURL string) transport.AuthMethod {
	// Check if it's an SSH URL
	if strings.HasPrefix(gitURL, "git@") || strings.HasPrefix(gitURL, "ssh://") {
		// Try to use SSH key from default location
		sshKeyPath := filepath.Join(os.Getenv("HOME"), ".ssh", "id_rsa")
		if _, err := os.Stat(sshKeyPath); err == nil {
			auth, err := ssh.NewPublicKeysFromFile("git", sshKeyPath, "")
			if err == nil {
				return auth
			}
		}
	}

	// Check for HTTPS with credentials
	if strings.HasPrefix(gitURL, "https://") {
		// Check for environment variables
		username := os.Getenv("GIT_USERNAME")
		password := os.Getenv("GIT_PASSWORD")
		if username != "" && password != "" {
			return &http.BasicAuth{
				Username: username,
				Password: password,
			}
		}

		// Check for GitHub token
		token := os.Getenv("GITHUB_TOKEN")
		if token != "" {
			return &http.BasicAuth{
				Username: "token",
				Password: token,
			}
		}
	}

	// No auth method found
	return nil
}

// GetCacheDirectory returns the default cache directory for repositories
func GetCacheDirectory() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(homeDir, ".flakedrop", "repos")
}

// CheckoutBranch checks out a specific branch in the repository
func CheckoutBranch(repoPath, branchName string) error {
	// Open the repository
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return fmt.Errorf("failed to open repository: %w", err)
	}

	// Get the worktree
	worktree, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	// First check if the branch already exists locally
	branchRef := plumbing.ReferenceName("refs/heads/" + branchName)
	_, err = repo.Reference(branchRef, false)
	if err == nil {
		// Branch exists, just checkout
		return worktree.Checkout(&git.CheckoutOptions{
			Branch: branchRef,
			Force:  false,
		})
	}

	// Try to find the branch in remotes
	remoteRef := plumbing.ReferenceName("refs/remotes/origin/" + branchName)
	ref, err := repo.Reference(remoteRef, false)
	if err == nil {
		// Found in remote, create local branch from remote
		return worktree.Checkout(&git.CheckoutOptions{
			Branch: branchRef,
			Hash:   ref.Hash(),
			Create: true,
		})
	}

	// If not found in remotes either, create a new branch from current HEAD
	head, err := repo.Head()
	if err != nil {
		return fmt.Errorf("failed to get HEAD: %w", err)
	}

	return worktree.Checkout(&git.CheckoutOptions{
		Branch: branchRef,
		Hash:   head.Hash(),
		Create: true,
	})
}

// GetCurrentBranch returns the current branch name of the repository
func GetCurrentBranch(repoPath string) (string, error) {
	// Open the repository
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return "", fmt.Errorf("failed to open repository: %w", err)
	}

	// Get HEAD reference
	head, err := repo.Head()
	if err != nil {
		return "", fmt.Errorf("failed to get HEAD: %w", err)
	}

	// Extract branch name from reference
	refName := head.Name()
	if refName.IsBranch() {
		return refName.Short(), nil
	}

	return "", fmt.Errorf("HEAD is not pointing to a branch")
}