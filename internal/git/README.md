# Git Integration for FlakeDrop CLI

This package provides Git integration capabilities for the Snowflake deployment CLI, allowing you to sync repositories, browse commits, and retrieve SQL files from specific commits.

## Features

- **Repository Management**: Clone and fetch Git repositories with support for both SSH and HTTPS authentication
- **Commit History**: Browse recent commits and see which SQL files were modified
- **File Retrieval**: Get SQL file contents from specific commits
- **Branch Support**: Checkout and work with different branches
- **Local Caching**: Repositories are cached locally in `~/.flakedrop/repos/`
- **Authentication**: Support for SSH keys, HTTPS credentials, and GitHub tokens

## Usage

### Basic Operations

```go
import "flakedrop/internal/git"

// Create a new Git service
service := git.NewService()

// Sync a repository (clone or fetch updates)
repo := models.Repository{
    Name:   "my-sql-repo",
    GitURL: "https://github.com/example/sql-scripts.git",
    Branch: "main",
}
err := service.SyncRepository(repo)

// Get recent commits
commits, err := service.GetRecentCommitsForRepo("my-sql-repo", 10)

// Get SQL files from a specific commit
files, err := service.GetSQLFilesFromCommit("my-sql-repo", "abc123def456")
```

### Low-Level Git Operations

```go
// Create a GitManager for direct repository operations
gm, err := git.NewGitManager("/path/to/repo")

// Get recent commits
commits, err := gm.GetRecentCommits(10)

// Get changed files in a commit
files, err := gm.GetCommitFiles("abc123def456")

// Get file content from a commit
content, err := gm.GetFileContent("abc123def456", "schema/tables.sql")
```

### Authentication

The Git integration supports multiple authentication methods:

#### SSH Authentication
- Place your SSH key in the default location: `~/.ssh/id_rsa`
- Or use SSH agent for key management

#### HTTPS Authentication
Set environment variables:
```bash
# Basic authentication
export GIT_USERNAME=your-username
export GIT_PASSWORD=your-password

# GitHub token authentication
export GITHUB_TOKEN=ghp_your_token_here
```

### Utility Functions

```go
// Validate a Git URL
err := git.ValidateGitURL("https://github.com/user/repo.git")

// Normalize a Git URL (add .git suffix, trim whitespace)
normalizedURL := git.NormalizeGitURL("https://github.com/user/repo")

// Extract repository name from URL
repoName := git.ExtractRepoName("git@github.com:user/repo.git") // Returns "repo"

// Check if a file is an SQL file
isSql := git.IsSQLFile("schema.sql") // Returns true

// Filter SQL files from a list
sqlFiles := git.FilterSQLFiles([]string{"schema.sql", "readme.md", "data.sql"})
// Returns ["schema.sql", "data.sql"]
```

### Cache Management

```go
// List cached repositories
repos, err := service.GetCachedRepositories()

// Clean old repositories (older than 7 days)
err := service.CleanCache(7 * 24 * time.Hour)

// Get cache directory location
cacheDir := git.GetCacheDirectory() // Returns ~/.flakedrop/repos
```

## Integration with CLI

To integrate with the existing Snowflake deployment CLI:

1. Add Git repository configuration to your `config.yaml`:
```yaml
repositories:
  - name: "analytics-sql"
    git_url: "https://github.com/company/analytics-sql.git"
    branch: "main"
    database: "ANALYTICS"
    schema: "PUBLIC"
```

2. In your CLI command, use the Git service to sync and retrieve SQL files:
```go
service := git.NewService()

// Sync repository
err := service.SyncRepository(config.Repositories[0])

// Get SQL files from latest commit
commits, _ := service.GetRecentCommitsForRepo(repo.Name, 1)
if len(commits) > 0 {
    files, _ := service.GetSQLFilesFromCommit(repo.Name, commits[0].Hash)
    // Deploy files to Snowflake...
}
```

## Testing

Run tests with:
```bash
go test ./internal/git/... -v
```

The package includes comprehensive unit tests for all operations.

## Directory Structure

```
internal/git/
├── operations.go          # Core Git operations using go-git
├── operations_test.go     # Tests for Git operations
├── service.go            # High-level service for repository management
├── service_test.go       # Tests for service
├── utils.go              # Utility functions
├── utils_test.go         # Tests for utilities
└── example_integration.go # Example usage patterns
```