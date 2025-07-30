package testutil

import (
    "flakedrop/pkg/models"
    "time"
)

// MockGitManager provides a mock implementation of Git operations for testing
type MockGitManager struct {
    Commits      []CommitInfo
    CommitFiles  map[string][]string
    FileContents map[string]string
    Error        error
}

type CommitInfo struct {
    Hash    string
    Message string
    Author  string
    Date    time.Time
}

func (m *MockGitManager) GetRecentCommits(limit int) ([]CommitInfo, error) {
    if m.Error != nil {
        return nil, m.Error
    }
    if len(m.Commits) <= limit {
        return m.Commits, nil
    }
    return m.Commits[:limit], nil
}

func (m *MockGitManager) GetCommitFiles(hash string) ([]string, error) {
    if m.Error != nil {
        return nil, m.Error
    }
    return m.CommitFiles[hash], nil
}

func (m *MockGitManager) GetFileContent(hash, path string) (string, error) {
    if m.Error != nil {
        return "", m.Error
    }
    key := hash + ":" + path
    return m.FileContents[key], nil
}

// MockSnowflakeClient provides a mock implementation of Snowflake operations for testing
type MockSnowflakeClient struct {
    Connected      bool
    ExecutedSQL    []string
    ExecutedDBs    []string
    ExecutedSchemas []string
    Error          error
}

func (m *MockSnowflakeClient) TestConnection() error {
    if m.Error != nil {
        return m.Error
    }
    m.Connected = true
    return nil
}

func (m *MockSnowflakeClient) ExecuteSQL(database, schema, sqlContent string) error {
    if m.Error != nil {
        return m.Error
    }
    m.ExecutedSQL = append(m.ExecutedSQL, sqlContent)
    m.ExecutedDBs = append(m.ExecutedDBs, database)
    m.ExecutedSchemas = append(m.ExecutedSchemas, schema)
    return nil
}

func (m *MockSnowflakeClient) Close() error {
    m.Connected = false
    return nil
}

// TestConfig returns a sample configuration for testing
func TestConfig() *models.Config {
    return &models.Config{
        Repositories: []models.Repository{
            {
                Name:     "test-repo",
                GitURL:   "https://github.com/test/repo.git",
                Branch:   "main",
                Database: "TEST_DB",
                Schema:   "PUBLIC",
            },
        },
        Snowflake: models.Snowflake{
            Account:   "test123.us-east-1",
            Username:  "testuser",
            Password:  "testpass",
            Role:      "TESTROLE",
            Warehouse: "TEST_WH",
        },
        License: models.License{
            Key:       "TEST-LICENSE-KEY",
            ExpiresAt: "2025-12-31",
            LastCheck: "2025-01-01",
        },
    }
}

// TestCommits returns sample commits for testing
func TestCommits() []CommitInfo {
    return []CommitInfo{
        {
            Hash:    "abc12345",
            Message: "Add user table",
            Author:  "John Doe",
            Date:    time.Now().Add(-2 * time.Hour),
        },
        {
            Hash:    "def67890",
            Message: "Update permissions",
            Author:  "Jane Smith",
            Date:    time.Now().Add(-24 * time.Hour),
        },
        {
            Hash:    "ghi11111",
            Message: "Fix data pipeline",
            Author:  "Bob Johnson",
            Date:    time.Now().Add(-48 * time.Hour),
        },
    }
}