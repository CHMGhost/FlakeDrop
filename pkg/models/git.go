package models

import "time"

// GitCommit represents a git commit with its associated metadata
type GitCommit struct {
	Hash      string    `json:"hash"`
	Message   string    `json:"message"`
	Author    string    `json:"author"`
	Date      time.Time `json:"date"`
	SQLFiles  []string  `json:"sql_files"`
}

// GitFile represents a file in a git repository
type GitFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	Hash    string `json:"commit_hash"`
}

// RepoCache represents cached repository information
type RepoCache struct {
	RepoName    string    `json:"repo_name"`
	LocalPath   string    `json:"local_path"`
	LastFetched time.Time `json:"last_fetched"`
	Branch      string    `json:"branch"`
}