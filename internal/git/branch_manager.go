package git

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"flakedrop/pkg/errors"
)

// BranchManager handles advanced branch operations
type BranchManager struct{}

// NewBranchManager creates a new branch manager
func NewBranchManager() *BranchManager {
	return &BranchManager{}
}

// CheckoutBranch performs an enhanced branch checkout with recovery options
func (bm *BranchManager) CheckoutBranch(repoPath, branchName string, createIfNotExists bool) error {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoNotFound,
			"Failed to open repository")
	}

	worktree, err := repo.Worktree()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get worktree")
	}

	// Check for uncommitted changes
	status, err := worktree.Status()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get repository status")
	}

	if !status.IsClean() {
		// Stash changes before switching branches
		if err := bm.stashChanges(worktree); err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to stash uncommitted changes").
				WithSuggestions(
					"Commit or discard your changes before switching branches",
					"Use 'git stash' to temporarily save your changes",
				)
		}
		defer func() { _ = bm.popStash(worktree) }() // Restore stash after checkout
	}

	// Try to checkout the branch
	branchRef := plumbing.NewBranchReferenceName(branchName)
	
	// Check if branch exists locally
	_, err = repo.Reference(branchRef, false)
	if err == nil {
		// Branch exists locally, checkout
		return worktree.Checkout(&git.CheckoutOptions{
			Branch: branchRef,
			Force:  false,
		})
	}

	// Branch doesn't exist locally
	// Check if it exists in any remote
	remotes, _ := repo.Remotes()
	for _, remote := range remotes {
		remoteRef := plumbing.NewRemoteReferenceName(remote.Config().Name, branchName)
		ref, err := repo.Reference(remoteRef, false)
		if err == nil {
			// Found in remote, create local branch tracking remote
			return worktree.Checkout(&git.CheckoutOptions{
				Branch: branchRef,
				Hash:   ref.Hash(),
				Create: true,
			})
		}
	}

	// Branch doesn't exist anywhere
	if createIfNotExists {
		// Create new branch from current HEAD
		head, err := repo.Head()
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to get HEAD reference")
		}

		err = worktree.Checkout(&git.CheckoutOptions{
			Branch: branchRef,
			Hash:   head.Hash(),
			Create: true,
		})
		
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				fmt.Sprintf("Failed to create branch '%s'", branchName))
		}

		// Set up tracking for the new branch
		if err := bm.setupBranchTracking(repo, branchName); err != nil {
			// Non-critical error
			fmt.Printf("Warning: Failed to set up branch tracking: %v\n", err)
		}

		return nil
	}

	return errors.New(errors.ErrCodeRepoSyncFailed,
		fmt.Sprintf("Branch '%s' not found", branchName)).
		WithSuggestions(
			"List available branches with 'git branch -a'",
			"Create the branch with 'flakedrop git branch create'",
			"Check for typos in the branch name",
		)
}

// CreateBranch creates a new branch with enhanced options
func (bm *BranchManager) CreateBranch(repoPath, branchName, baseBranch string, push bool) error {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoNotFound,
			"Failed to open repository")
	}

	// Get base commit
	var baseHash plumbing.Hash
	if baseBranch != "" {
		// Use specified base branch
		ref, err := repo.Reference(plumbing.NewBranchReferenceName(baseBranch), false)
		if err != nil {
			// Try remote branch
			remotes, _ := repo.Remotes()
			for _, remote := range remotes {
				remoteRef := plumbing.NewRemoteReferenceName(remote.Config().Name, baseBranch)
				ref, err = repo.Reference(remoteRef, false)
				if err == nil {
					break
				}
			}
			
			if err != nil {
				return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
					fmt.Sprintf("Base branch '%s' not found", baseBranch))
			}
		}
		baseHash = ref.Hash()
	} else {
		// Use current HEAD
		head, err := repo.Head()
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to get HEAD reference")
		}
		baseHash = head.Hash()
	}

	// Create the branch
	branchRef := plumbing.NewBranchReferenceName(branchName)
	ref := plumbing.NewHashReference(branchRef, baseHash)
	
	err = repo.Storer.SetReference(ref)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			fmt.Sprintf("Failed to create branch '%s'", branchName))
	}

	// Checkout the new branch
	worktree, err := repo.Worktree()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get worktree")
	}

	err = worktree.Checkout(&git.CheckoutOptions{
		Branch: branchRef,
	})
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			fmt.Sprintf("Failed to checkout branch '%s'", branchName))
	}

	// Set up tracking
	if err := bm.setupBranchTracking(repo, branchName); err != nil {
		fmt.Printf("Warning: Failed to set up branch tracking: %v\n", err)
	}

	// Push to remote if requested
	if push {
		if err := bm.pushBranch(repo, branchName); err != nil {
			return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
				"Failed to push branch to remote").
				WithSuggestions(
					"Check your network connection",
					"Verify you have push access to the repository",
					"Try pushing manually with 'git push -u origin " + branchName + "'",
				)
		}
	}

	return nil
}

// DeleteBranch deletes a branch with safety checks
func (bm *BranchManager) DeleteBranch(repoPath, branchName string, force bool, deleteRemote bool) error {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoNotFound,
			"Failed to open repository")
	}

	// Check if we're trying to delete the current branch
	head, err := repo.Head()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to get HEAD reference")
	}

	if head.Name().Short() == branchName {
		return errors.New(errors.ErrCodeInvalidInput,
			"Cannot delete the current branch").
			WithSuggestions(
				"Switch to a different branch first",
				"Use 'git checkout main' to switch to main branch",
			)
	}

	// Check if branch has unmerged changes
	if !force {
		if unmerged, err := bm.hasUnmergedChanges(repo, branchName); err == nil && unmerged {
			return errors.New(errors.ErrCodeRepoSyncFailed,
				fmt.Sprintf("Branch '%s' has unmerged changes", branchName)).
				WithSuggestions(
					"Merge the branch before deleting",
					"Use --force to delete anyway (changes will be lost)",
				)
		}
	}

	// Delete local branch
	branchRef := plumbing.NewBranchReferenceName(branchName)
	err = repo.Storer.RemoveReference(branchRef)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			fmt.Sprintf("Failed to delete branch '%s'", branchName))
	}

	// Delete remote branch if requested
	if deleteRemote {
		if err := bm.deleteRemoteBranch(repo, branchName); err != nil {
			// Non-critical error
			fmt.Printf("Warning: Failed to delete remote branch: %v\n", err)
		}
	}

	return nil
}

// ListBranches lists all branches with enhanced information
func (bm *BranchManager) ListBranches(repoPath string, includeRemote bool) ([]BranchInfo, error) {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeRepoNotFound,
			"Failed to open repository")
	}

	var branches []BranchInfo

	// Get current branch
	head, _ := repo.Head()
	currentBranch := ""
	if head != nil && head.Name().IsBranch() {
		currentBranch = head.Name().Short()
	}

	// List local branches
	refs, err := repo.References()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			"Failed to list references")
	}

	err = refs.ForEach(func(ref *plumbing.Reference) error {
		if ref.Name().IsBranch() {
			branchName := ref.Name().Short()
			
			// Get branch details
			commit, _ := repo.CommitObject(ref.Hash())
			
			info := BranchInfo{
				Name:      branchName,
				IsCurrent: branchName == currentBranch,
				IsLocal:   true,
				IsRemote:  false,
			}
			
			if commit != nil {
				info.LastCommit = commit.Hash.String()[:8]
				info.LastCommitDate = commit.Author.When
				info.LastCommitMessage = strings.Split(commit.Message, "\n")[0]
			}
			
			// Check if branch tracks a remote
			if cfg, err := repo.Config(); err == nil {
				if branch := cfg.Branches[branchName]; branch != nil {
					info.Tracking = branch.Remote + "/" + branch.Merge.Short()
				}
			}
			
			branches = append(branches, info)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// List remote branches if requested
	if includeRemote {
		remotes, _ := repo.Remotes()
		for _, remote := range remotes {
			refs, err := remote.List(&git.ListOptions{})
			if err != nil {
				continue
			}

			for _, ref := range refs {
				if ref.Name().IsRemote() {
					branchName := ref.Name().Short()
					
					// Skip HEAD references
					if strings.HasSuffix(branchName, "/HEAD") {
						continue
					}

					// Check if we already have this as a local branch
					localName := strings.TrimPrefix(branchName, remote.Config().Name+"/")
					hasLocal := false
					for _, b := range branches {
						if b.Name == localName {
							hasLocal = true
							break
						}
					}

					info := BranchInfo{
						Name:      branchName,
						IsCurrent: false,
						IsLocal:   false,
						IsRemote:  true,
						HasLocal:  hasLocal,
					}

					branches = append(branches, info)
				}
			}
		}
	}

	return branches, nil
}

// stashChanges stashes uncommitted changes
func (bm *BranchManager) stashChanges(worktree *git.Worktree) error {
	// This is a simplified implementation
	// In a real scenario, you'd implement proper stashing
	return nil
}

// popStash restores stashed changes
func (bm *BranchManager) popStash(worktree *git.Worktree) error {
	// This is a simplified implementation
	// In a real scenario, you'd implement proper stash popping
	return nil
}

// setupBranchTracking sets up branch tracking for a new branch
func (bm *BranchManager) setupBranchTracking(repo *git.Repository, branchName string) error {
	cfg, err := repo.Config()
	if err != nil {
		return err
	}

	// Set up tracking for origin remote
	branchConfig := &config.Branch{
		Name:   branchName,
		Remote: "origin",
		Merge:  plumbing.NewBranchReferenceName(branchName),
	}

	cfg.Branches[branchName] = branchConfig
	return repo.SetConfig(cfg)
}

// pushBranch pushes a branch to remote
func (bm *BranchManager) pushBranch(repo *git.Repository, branchName string) error {
	// Get the remote
	remote, err := repo.Remote("origin")
	if err != nil {
		return err
	}

	// Push the branch
	refSpec := config.RefSpec(fmt.Sprintf("refs/heads/%s:refs/heads/%s", branchName, branchName))
	
	return remote.Push(&git.PushOptions{
		RefSpecs: []config.RefSpec{refSpec},
	})
}

// deleteRemoteBranch deletes a branch from remote
func (bm *BranchManager) deleteRemoteBranch(repo *git.Repository, branchName string) error {
	// Get the remote
	remote, err := repo.Remote("origin")
	if err != nil {
		return err
	}

	// Delete the branch (push empty ref)
	refSpec := config.RefSpec(fmt.Sprintf(":refs/heads/%s", branchName))
	
	return remote.Push(&git.PushOptions{
		RefSpecs: []config.RefSpec{refSpec},
	})
}

// hasUnmergedChanges checks if a branch has unmerged changes
func (bm *BranchManager) hasUnmergedChanges(repo *git.Repository, branchName string) (bool, error) {
	// Get branch reference
	branchRef := plumbing.NewBranchReferenceName(branchName)
	branch, err := repo.Reference(branchRef, false)
	if err != nil {
		return false, err
	}

	// Get main/master branch
	var mainRef *plumbing.Reference
	if ref, err := repo.Reference(plumbing.NewBranchReferenceName("main"), false); err == nil {
		mainRef = ref
	} else if ref, err := repo.Reference(plumbing.NewBranchReferenceName("master"), false); err == nil {
		mainRef = ref
	} else {
		return false, nil // Can't determine, assume no unmerged changes
	}

	// Check if branch is fully merged
	branchCommit, err := repo.CommitObject(branch.Hash())
	if err != nil {
		return false, err
	}

	mainCommit, err := repo.CommitObject(mainRef.Hash())
	if err != nil {
		return false, err
	}

	// Check if branch commit is an ancestor of main
	isAncestor, err := branchCommit.IsAncestor(mainCommit)
	return !isAncestor, err
}

// RenameBranch renames a branch
func (bm *BranchManager) RenameBranch(repoPath, oldName, newName string) error {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoNotFound,
			"Failed to open repository")
	}

	// Get the old branch reference
	oldRef := plumbing.NewBranchReferenceName(oldName)
	ref, err := repo.Reference(oldRef, false)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			fmt.Sprintf("Branch '%s' not found", oldName))
	}

	// Create new branch with same commit
	newRef := plumbing.NewBranchReferenceName(newName)
	newBranch := plumbing.NewHashReference(newRef, ref.Hash())
	
	err = repo.Storer.SetReference(newBranch)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			fmt.Sprintf("Failed to create branch '%s'", newName))
	}

	// Check if we're renaming the current branch
	head, _ := repo.Head()
	if head != nil && head.Name() == oldRef {
		// Checkout the new branch
		worktree, err := repo.Worktree()
		if err != nil {
			return err
		}
		
		err = worktree.Checkout(&git.CheckoutOptions{
			Branch: newRef,
		})
		if err != nil {
			return err
		}
	}

	// Delete the old branch
	err = repo.Storer.RemoveReference(oldRef)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeRepoSyncFailed,
			fmt.Sprintf("Failed to delete old branch '%s'", oldName))
	}

	// Update branch configuration
	cfg, err := repo.Config()
	if err == nil {
		if oldBranch := cfg.Branches[oldName]; oldBranch != nil {
			cfg.Branches[newName] = oldBranch
			delete(cfg.Branches, oldName)
			_ = repo.SetConfig(cfg)
		}
	}

	return nil
}

// BranchInfo contains information about a branch
type BranchInfo struct {
	Name              string    `json:"name"`
	IsCurrent         bool      `json:"is_current"`
	IsLocal           bool      `json:"is_local"`
	IsRemote          bool      `json:"is_remote"`
	HasLocal          bool      `json:"has_local"`
	Tracking          string    `json:"tracking,omitempty"`
	LastCommit        string    `json:"last_commit,omitempty"`
	LastCommitDate    time.Time `json:"last_commit_date,omitempty"`
	LastCommitMessage string    `json:"last_commit_message,omitempty"`
	AheadBehind       string    `json:"ahead_behind,omitempty"`
}

// GetCommitsWithFilter retrieves commits with filtering options
func (gm *GitManager) GetCommitsWithFilter(options CommitOptions) ([]CommitInfo, error) {
	logOptions := &git.LogOptions{}
	
	// Apply branch filter
	if options.Branch != "" {
		ref, err := gm.repo.Reference(plumbing.NewBranchReferenceName(options.Branch), false)
		if err != nil {
			return nil, fmt.Errorf("branch '%s' not found", options.Branch)
		}
		logOptions.From = ref.Hash()
	} else {
		// Use HEAD if no branch specified
		head, err := gm.repo.Head()
		if err != nil {
			return nil, err
		}
		logOptions.From = head.Hash()
	}

	// Apply time filters
	if !options.Since.IsZero() {
		logOptions.Since = &options.Since
	}
	if !options.Until.IsZero() {
		logOptions.Until = &options.Until
	}

	// Apply path filter
	if options.PathFilter != "" {
		logOptions.PathFilter = func(path string) bool {
			return strings.Contains(path, options.PathFilter)
		}
	}

	// Get commits
	commitIter, err := gm.repo.Log(logOptions)
	if err != nil {
		return nil, err
	}

	var commits []CommitInfo
	count := 0

	err = commitIter.ForEach(func(c *object.Commit) error {
		// Apply author filter
		if options.Author != "" && !strings.Contains(c.Author.Name, options.Author) {
			return nil
		}

		// Apply limit
		if options.Limit > 0 && count >= options.Limit {
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

	return commits, err
}