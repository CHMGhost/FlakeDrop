package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/go-git/go-git/v5/plumbing/object"
)


// FormatCommit formats a git commit for display
func FormatCommit(commit *object.Commit, fileCount int) CommitInfo {
	hash := commit.Hash.String()
	shortHash := hash[:7]
	
	// Truncate long commit messages
	message := commit.Message
	if idx := strings.Index(message, "\n"); idx > 0 {
		message = message[:idx]
	}
	if len(message) > 50 {
		message = message[:47] + "..."
	}
	
	return CommitInfo{
		Hash:      hash,
		ShortHash: shortHash,
		Message:   message,
		Author:    commit.Author.Name,
		Time:      commit.Author.When,
		Files:     fileCount,
	}
}

// SelectCommit displays an interactive commit selector
func SelectCommit(commits []CommitInfo) (string, error) {
	if len(commits) == 0 {
		return "", fmt.Errorf("no commits available")
	}
	
	options := make([]string, len(commits))
	hashMap := make(map[string]string)
	
	for i, commit := range commits {
		relTime := formatRelativeTime(commit.Time)
		option := fmt.Sprintf("%s - %s (%s) [%d files]",
			commit.ShortHash,
			commit.Message,
			relTime,
			commit.Files,
		)
		options[i] = option
		hashMap[option] = commit.Hash
	}
	
	var selected string
	prompt := &survey.Select{
		Message: "Select commit to deploy:",
		Options: options,
		PageSize: 10,
	}
	
	if err := survey.AskOne(prompt, &selected); err != nil {
		return "", err
	}
	
	return hashMap[selected], nil
}

// formatRelativeTime formats time as relative (e.g., "2 hours ago")
func formatRelativeTime(t time.Time) string {
	duration := time.Since(t)
	
	switch {
	case duration < time.Minute:
		return "just now"
	case duration < time.Hour:
		minutes := int(duration.Minutes())
		if minutes == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", minutes)
	case duration < 24*time.Hour:
		hours := int(duration.Hours())
		if hours == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", hours)
	case duration < 7*24*time.Hour:
		days := int(duration.Hours() / 24)
		if days == 1 {
			return "1 day ago"
		}
		return fmt.Sprintf("%d days ago", days)
	case duration < 30*24*time.Hour:
		weeks := int(duration.Hours() / (24 * 7))
		if weeks == 1 {
			return "1 week ago"
		}
		return fmt.Sprintf("%d weeks ago", weeks)
	default:
		months := int(duration.Hours() / (24 * 30))
		if months == 1 {
			return "1 month ago"
		}
		return fmt.Sprintf("%d months ago", months)
	}
}