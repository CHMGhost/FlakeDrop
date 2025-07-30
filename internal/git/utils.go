package git

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
)

// GenerateRepoID creates a unique identifier for a repository based on its URL
func GenerateRepoID(gitURL string) string {
	// Remove protocol prefix
	url := strings.TrimPrefix(gitURL, "https://")
	url = strings.TrimPrefix(url, "http://")
	url = strings.TrimPrefix(url, "git@")
	url = strings.TrimPrefix(url, "ssh://")

	// Remove .git suffix
	url = strings.TrimSuffix(url, ".git")

	// Replace special characters
	url = strings.ReplaceAll(url, ":", "_")
	url = strings.ReplaceAll(url, "/", "_")
	url = strings.ReplaceAll(url, "\\", "_")

	// If the result is too long, hash it
	if len(url) > 50 {
		hash := sha256.Sum256([]byte(url))
		return fmt.Sprintf("%x", hash[:4]) // Use first 4 bytes = 8 hex chars
	}

	return url
}

// IsSSHURL checks if a git URL is using SSH protocol
func IsSSHURL(gitURL string) bool {
	return strings.HasPrefix(gitURL, "git@") || strings.HasPrefix(gitURL, "ssh://")
}

// IsHTTPSURL checks if a git URL is using HTTPS protocol
func IsHTTPSURL(gitURL string) bool {
	return strings.HasPrefix(gitURL, "https://") || strings.HasPrefix(gitURL, "http://")
}

// ExtractRepoName extracts the repository name from a git URL
func ExtractRepoName(gitURL string) string {
	// Remove protocol prefix
	url := gitURL
	if IsSSHURL(url) {
		// Handle SSH URLs like git@github.com:user/repo.git
		parts := strings.Split(url, ":")
		if len(parts) > 1 {
			url = parts[1]
		}
	} else if IsHTTPSURL(url) {
		// Handle HTTPS URLs
		url = strings.TrimPrefix(url, "https://")
		url = strings.TrimPrefix(url, "http://")
		// Remove domain part
		parts := strings.Split(url, "/")
		if len(parts) > 1 {
			url = strings.Join(parts[1:], "/")
		}
	}

	// Remove .git suffix
	url = strings.TrimSuffix(url, ".git")

	// Get the last part as repo name
	parts := strings.Split(url, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return "unknown"
}

// ValidateGitURL performs basic validation on a git URL
func ValidateGitURL(gitURL string) error {
	if gitURL == "" {
		return fmt.Errorf("git URL cannot be empty")
	}

	if !IsSSHURL(gitURL) && !IsHTTPSURL(gitURL) {
		// Check if it's a local path
		if !filepath.IsAbs(gitURL) {
			return fmt.Errorf("invalid git URL: must be SSH, HTTPS, or absolute local path")
		}
	}

	return nil
}

// NormalizeGitURL normalizes a git URL to a standard format
func NormalizeGitURL(gitURL string) string {
	// Trim whitespace
	url := strings.TrimSpace(gitURL)

	// Ensure .git suffix for remote URLs
	if (IsSSHURL(url) || IsHTTPSURL(url)) && !strings.HasSuffix(url, ".git") {
		url += ".git"
	}

	return url
}

// IsSQLFile checks if a file path represents an SQL file
func IsSQLFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".sql"
}

// FilterSQLFiles filters a list of file paths to only include SQL files
func FilterSQLFiles(files []string) []string {
	var sqlFiles []string
	for _, file := range files {
		if IsSQLFile(file) {
			sqlFiles = append(sqlFiles, file)
		}
	}
	return sqlFiles
}