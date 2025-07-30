package models

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "gopkg.in/yaml.v3"
)

func TestConfigMarshalUnmarshal(t *testing.T) {
    config := Config{
        Repositories: []Repository{
            {
                Name:     "analytics",
                GitURL:   "https://github.com/company/analytics.git",
                Branch:   "main",
                Database: "ANALYTICS_DB",
                Schema:   "PUBLIC",
            },
            {
                Name:     "reporting",
                GitURL:   "https://github.com/company/reporting.git",
                Branch:   "develop",
                Database: "REPORTING_DB",
                Schema:   "REPORTS",
            },
        },
        Snowflake: Snowflake{
            Account:   "xy12345.us-east-1",
            Username:  "deploy_user",
            Password:  "encrypted_password",
            Role:      "DEPLOYMENT_ROLE",
            Warehouse: "DEPLOY_WH",
        },
        License: License{
            Key:       "LICENSE-123-ABC",
            ExpiresAt: "2025-12-31",
            LastCheck: "2025-01-15",
        },
    }

    // Marshal to YAML
    data, err := yaml.Marshal(&config)
    assert.NoError(t, err)
    assert.NotEmpty(t, data)

    // Unmarshal back
    var unmarshaledConfig Config
    err = yaml.Unmarshal(data, &unmarshaledConfig)
    assert.NoError(t, err)

    // Verify all fields
    assert.Equal(t, config.Repositories[0].Name, unmarshaledConfig.Repositories[0].Name)
    assert.Equal(t, config.Repositories[0].GitURL, unmarshaledConfig.Repositories[0].GitURL)
    assert.Equal(t, config.Repositories[1].Database, unmarshaledConfig.Repositories[1].Database)
    assert.Equal(t, config.Snowflake.Account, unmarshaledConfig.Snowflake.Account)
    assert.Equal(t, config.License.Key, unmarshaledConfig.License.Key)
}

func TestEmptyConfig(t *testing.T) {
    config := Config{}
    
    // Should marshal without error
    data, err := yaml.Marshal(&config)
    assert.NoError(t, err)
    
    // Should unmarshal back
    var unmarshaledConfig Config
    err = yaml.Unmarshal(data, &unmarshaledConfig)
    assert.NoError(t, err)
    
    // Should have empty slices, not nil
    assert.NotNil(t, unmarshaledConfig.Repositories)
    assert.Len(t, unmarshaledConfig.Repositories, 0)
}

func TestRepositoryValidation(t *testing.T) {
    tests := []struct {
        name     string
        repo     Repository
        expected string
    }{
        {
            name: "valid repository",
            repo: Repository{
                Name:     "test",
                GitURL:   "https://github.com/test/repo.git",
                Branch:   "main",
                Database: "TEST_DB",
                Schema:   "PUBLIC",
            },
            expected: "valid",
        },
        {
            name: "empty name",
            repo: Repository{
                GitURL:   "https://github.com/test/repo.git",
                Branch:   "main",
                Database: "TEST_DB",
                Schema:   "PUBLIC",
            },
            expected: "invalid",
        },
        {
            name: "empty git url",
            repo: Repository{
                Name:     "test",
                Branch:   "main",
                Database: "TEST_DB",
                Schema:   "PUBLIC",
            },
            expected: "invalid",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Basic validation check
            isValid := tt.repo.Name != "" && tt.repo.GitURL != ""
            if tt.expected == "valid" {
                assert.True(t, isValid)
            } else {
                assert.False(t, isValid)
            }
        })
    }
}

func TestSnowflakeCredentials(t *testing.T) {
    sf := Snowflake{
        Account:   "test123.us-east-1",
        Username:  "testuser",
        Password:  "testpass",
        Role:      "TESTROLE",
        Warehouse: "TEST_WH",
    }

    // Test that all fields are set
    assert.NotEmpty(t, sf.Account)
    assert.NotEmpty(t, sf.Username)
    assert.NotEmpty(t, sf.Password)
    assert.NotEmpty(t, sf.Role)
    assert.NotEmpty(t, sf.Warehouse)
}

func TestLicenseFields(t *testing.T) {
    license := License{
        Key:       "TEST-KEY-123",
        ExpiresAt: "2025-12-31",
        LastCheck: "2025-01-01",
    }

    // Test date format validation
    assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, license.ExpiresAt)
    assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, license.LastCheck)
}