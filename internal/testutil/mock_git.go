package testutil

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// MockGitService provides a comprehensive mock for Git operations
type MockGitService struct {
	mu sync.Mutex
	
	// Repository state
	CurrentBranch string
	Branches      []string
	Tags          []string
	Remotes       map[string]string
	
	// Commit history
	Commits []MockCommit
	HEAD    string
	
	// Working directory
	WorkingDir   string
	TrackedFiles map[string]FileState
	
	// Error simulation
	CloneError  error
	FetchError  error
	PullError   error
	StatusError error
	
	// Operation tracking
	Operations []GitOperation
}

// MockCommit represents a git commit
type MockCommit struct {
	Hash      string
	ShortHash string
	Message   string
	Author    string
	Email     string
	Date      time.Time
	Files     []string
	Parent    string
}

// FileState represents the state of a file in git
type FileState struct {
	Path     string
	Content  string
	Modified bool
	Staged   bool
	Status   string // "added", "modified", "deleted", "untracked"
}

// GitOperation tracks git operations performed
type GitOperation struct {
	Type      string // "clone", "fetch", "pull", "checkout", etc.
	Arguments []string
	Timestamp time.Time
	Error     error
}

// NewMockGitService creates a new mock git service
func NewMockGitService() *MockGitService {
	return &MockGitService{
		CurrentBranch: "main",
		Branches:      []string{"main", "develop", "feature/test"},
		Tags:          []string{"v1.0.0", "v1.1.0"},
		Remotes:       make(map[string]string),
		Commits:       make([]MockCommit, 0),
		TrackedFiles:  make(map[string]FileState),
		Operations:    make([]GitOperation, 0),
	}
}

// Clone simulates cloning a repository
func (g *MockGitService) Clone(url, directory string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("clone", []string{url, directory})
	
	if g.CloneError != nil {
		return g.CloneError
	}
	
	g.WorkingDir = directory
	g.Remotes["origin"] = url
	
	// Add some default commits
	g.addDefaultCommits()
	
	return nil
}

// Fetch simulates fetching from remote
func (g *MockGitService) Fetch() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("fetch", []string{})
	
	if g.FetchError != nil {
		return g.FetchError
	}
	
	// Simulate adding new remote commits
	g.Commits = append(g.Commits, MockCommit{
		Hash:      "remote123",
		ShortHash: "remote1",
		Message:   "Remote commit",
		Author:    "Remote User",
		Email:     "remote@example.com",
		Date:      time.Now(),
		Files:     []string{"remote.sql"},
	})
	
	return nil
}

// Pull simulates pulling from remote
func (g *MockGitService) Pull() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("pull", []string{})
	
	if g.PullError != nil {
		return g.PullError
	}
	
	// First fetch
	if err := g.Fetch(); err != nil {
		return err
	}
	
	// Then merge
	g.HEAD = g.Commits[len(g.Commits)-1].Hash
	
	return nil
}

// Checkout simulates checking out a branch or commit
func (g *MockGitService) Checkout(ref string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("checkout", []string{ref})
	
	// Check if it's a branch
	for _, branch := range g.Branches {
		if branch == ref {
			g.CurrentBranch = ref
			return nil
		}
	}
	
	// Check if it's a commit
	for _, commit := range g.Commits {
		if strings.HasPrefix(commit.Hash, ref) || commit.ShortHash == ref {
			g.HEAD = commit.Hash
			g.CurrentBranch = ""
			return nil
		}
	}
	
	return fmt.Errorf("reference not found: %s", ref)
}

// GetCommits returns mock commits
func (g *MockGitService) GetCommits(limit int) ([]MockCommit, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	if len(g.Commits) == 0 {
		g.addDefaultCommits()
	}
	
	end := len(g.Commits)
	if limit > 0 && limit < end {
		end = limit
	}
	
	// Return in reverse order (newest first)
	result := make([]MockCommit, end)
	for i := 0; i < end; i++ {
		result[i] = g.Commits[len(g.Commits)-1-i]
	}
	
	return result, nil
}

// GetCommitFiles returns files changed in a commit
func (g *MockGitService) GetCommitFiles(hash string) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	for _, commit := range g.Commits {
		if commit.Hash == hash || commit.ShortHash == hash {
			return commit.Files, nil
		}
	}
	
	return nil, fmt.Errorf("commit not found: %s", hash)
}

// GetFileContent returns file content at a specific commit
func (g *MockGitService) GetFileContent(commitHash, filepath string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	// Check if commit exists
	var commit *MockCommit
	for _, c := range g.Commits {
		if c.Hash == commitHash || c.ShortHash == commitHash {
			commit = &c
			break
		}
	}
	
	if commit == nil {
		return "", fmt.Errorf("commit not found: %s", commitHash)
	}
	
	// Check if file is in commit
	fileInCommit := false
	for _, f := range commit.Files {
		if f == filepath {
			fileInCommit = true
			break
		}
	}
	
	if !fileInCommit {
		return "", fmt.Errorf("file not found in commit: %s", filepath)
	}
	
	// Return mock content
	return fmt.Sprintf("-- Mock content for %s at commit %s\nCREATE TABLE test (id INT);", 
		filepath, commit.ShortHash), nil
}

// Status returns the current repository status
func (g *MockGitService) Status() (map[string]FileState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	if g.StatusError != nil {
		return nil, g.StatusError
	}
	
	return g.TrackedFiles, nil
}

// Add stages files
func (g *MockGitService) Add(files ...string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("add", files)
	
	for _, file := range files {
		if state, exists := g.TrackedFiles[file]; exists {
			state.Staged = true
			g.TrackedFiles[file] = state
		} else {
			g.TrackedFiles[file] = FileState{
				Path:   file,
				Staged: true,
				Status: "added",
			}
		}
	}
	
	return nil
}

// Commit creates a new commit
func (g *MockGitService) Commit(message string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("commit", []string{"-m", message})
	
	// Collect staged files
	var files []string
	for path, state := range g.TrackedFiles {
		if state.Staged {
			files = append(files, path)
			state.Staged = false
			state.Modified = false
			g.TrackedFiles[path] = state
		}
	}
	
	if len(files) == 0 {
		return fmt.Errorf("nothing to commit")
	}
	
	// Create new commit
	commit := MockCommit{
		Hash:      fmt.Sprintf("commit%d", len(g.Commits)+1),
		ShortHash: fmt.Sprintf("c%d", len(g.Commits)+1),
		Message:   message,
		Author:    "Test User",
		Email:     "test@example.com",
		Date:      time.Now(),
		Files:     files,
		Parent:    g.HEAD,
	}
	
	g.Commits = append(g.Commits, commit)
	g.HEAD = commit.Hash
	
	return nil
}

// Diff shows differences
func (g *MockGitService) Diff(commitA, commitB string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.recordOperation("diff", []string{commitA, commitB})
	
	// Simple mock diff
	return "diff --git a/file.sql b/file.sql\n--- a/file.sql\n+++ b/file.sql\n@@ -1,1 +1,1 @@\n-OLD LINE\n+NEW LINE\n", nil
}

// GetBranches returns all branches
func (g *MockGitService) GetBranches() ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	return g.Branches, nil
}

// GetTags returns all tags
func (g *MockGitService) GetTags() ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	return g.Tags, nil
}

// Helper methods

func (g *MockGitService) recordOperation(opType string, args []string) {
	g.Operations = append(g.Operations, GitOperation{
		Type:      opType,
		Arguments: args,
		Timestamp: time.Now(),
	})
}

func (g *MockGitService) addDefaultCommits() {
	baseTime := time.Now().Add(-24 * time.Hour)
	
	g.Commits = []MockCommit{
		{
			Hash:      "initial123",
			ShortHash: "initial",
			Message:   "Initial commit",
			Author:    "John Doe",
			Email:     "john@example.com",
			Date:      baseTime.Add(-48 * time.Hour),
			Files:     []string{"README.md"},
		},
		{
			Hash:      "feature456",
			ShortHash: "feature",
			Message:   "Add user table",
			Author:    "Jane Smith",
			Email:     "jane@example.com",
			Date:      baseTime.Add(-24 * time.Hour),
			Files:     []string{"sql/users.sql", "sql/permissions.sql"},
			Parent:    "initial123",
		},
		{
			Hash:      "fix789",
			ShortHash: "fix789",
			Message:   "Fix data pipeline",
			Author:    "Bob Johnson",
			Email:     "bob@example.com",
			Date:      baseTime,
			Files:     []string{"sql/pipeline.sql"},
			Parent:    "feature456",
		},
	}
	
	g.HEAD = g.Commits[len(g.Commits)-1].Hash
}

// AddCommit adds a custom commit to the history
func (g *MockGitService) AddCommit(commit MockCommit) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	if commit.Hash == "" {
		commit.Hash = fmt.Sprintf("custom%d", len(g.Commits)+1)
	}
	if commit.ShortHash == "" {
		if len(commit.Hash) >= 7 {
			commit.ShortHash = commit.Hash[:7]
		} else {
			commit.ShortHash = commit.Hash
		}
	}
	if commit.Date.IsZero() {
		commit.Date = time.Now()
	}
	
	g.Commits = append(g.Commits, commit)
}

// SetFileContent sets the content for a file in the working directory
func (g *MockGitService) SetFileContent(path, content string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.TrackedFiles[path] = FileState{
		Path:     path,
		Content:  content,
		Modified: true,
		Status:   "modified",
	}
}

// Reset clears all mock data
func (g *MockGitService) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.CurrentBranch = "main"
	g.Commits = make([]MockCommit, 0)
	g.TrackedFiles = make(map[string]FileState)
	g.Operations = make([]GitOperation, 0)
	g.HEAD = ""
}