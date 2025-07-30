package models

type Config struct {
    Repositories []Repository    `yaml:"repositories"`
    Snowflake    Snowflake       `yaml:"snowflake"`
    License      License         `yaml:"license"`
    Plugins      PluginConfig    `yaml:"plugins"`
    Deployment   Deployment      `yaml:"deployment"`
    Environments []Environment   `yaml:"environments"`
}

type Repository struct {
    Name     string `yaml:"name"`
    GitURL   string `yaml:"git_url"`
    Branch   string `yaml:"branch"`
    Database string `yaml:"database"`
    Schema   string `yaml:"schema"`
    Path     string `yaml:"path"`     // Local path for cloned repository
}

type Snowflake struct {
    Account   string `yaml:"account"`
    Username  string `yaml:"username"`
    Password  string `yaml:"password"`
    Role      string `yaml:"role"`
    Warehouse string `yaml:"warehouse"`
    Database  string `yaml:"database"`
    Schema    string `yaml:"schema"`
}

type License struct {
    Key       string `yaml:"key"`
    ExpiresAt string `yaml:"expires_at"`
    LastCheck string `yaml:"last_check"`
}

// PluginConfigItem represents configuration for a single plugin
type PluginConfigItem struct {
    Name     string                 `yaml:"name" json:"name"`
    Enabled  bool                   `yaml:"enabled" json:"enabled"`
    Settings map[string]interface{} `yaml:"settings" json:"settings"`
}

// PluginConfig contains plugin system configuration
type PluginConfig struct {
    Enabled      bool                    `yaml:"enabled"`
    Directories  []string                `yaml:"directories"`
    Plugins      []PluginConfigItem      `yaml:"plugins"`
    Registry     RegistryConfig          `yaml:"registry"`
    Security     PluginSecurityConfig    `yaml:"security"`
    Hooks        HookConfig              `yaml:"hooks"`
}

// RegistryConfig contains plugin registry configuration
type RegistryConfig struct {
    Enabled     bool     `yaml:"enabled"`
    URLs        []string `yaml:"urls"`
    CacheDir    string   `yaml:"cache_dir"`
    AutoUpdate  bool     `yaml:"auto_update"`
    UpdateInterval string `yaml:"update_interval"`
}

// PluginSecurityConfig contains security settings for plugins
type PluginSecurityConfig struct {
    AllowUnsigned   bool     `yaml:"allow_unsigned"`
    TrustedSources  []string `yaml:"trusted_sources"`
    SandboxEnabled  bool     `yaml:"sandbox_enabled"`
    MaxMemoryMB     int      `yaml:"max_memory_mb"`
    MaxExecutionTime string  `yaml:"max_execution_time"`
    AllowedPermissions []string `yaml:"allowed_permissions"`
}

// HookConfig contains hook system configuration
type HookConfig struct {
    Timeout      string              `yaml:"timeout"`
    RetryPolicy  RetryPolicyConfig   `yaml:"retry_policy"`
    Parallel     bool                `yaml:"parallel"`
    FailureBehavior string           `yaml:"failure_behavior"` // "continue", "stop", "rollback"
}

// RetryPolicyConfig contains retry policy configuration
type RetryPolicyConfig struct {
    MaxRetries int    `yaml:"max_retries"`
    Backoff    string `yaml:"backoff"` // e.g., "1s", "exponential"
    MaxBackoff string `yaml:"max_backoff"`
}

// Deployment contains deployment-specific configuration
type Deployment struct {
    Timeout         string            `yaml:"timeout"`          // e.g., "30m"
    MaxRetries      int               `yaml:"max_retries"`      // Maximum deployment retries
    Parallel        bool              `yaml:"parallel"`         // Enable parallel execution
    BatchSize       int               `yaml:"batch_size"`       // Files per batch
    PreHooks        []string          `yaml:"pre_hooks"`        // Commands to run before deployment
    PostHooks       []string          `yaml:"post_hooks"`       // Commands to run after deployment
    Rollback        RollbackConfig    `yaml:"rollback"`         // Rollback configuration
    Validation      ValidationConfig  `yaml:"validation"`       // Validation settings
    Environment     string            `yaml:"environment"`      // Target environment
    DryRun          bool              `yaml:"dry_run"`          // Enable dry-run mode by default
}

// RollbackConfig contains rollback-specific settings
type RollbackConfig struct {
    Enabled         bool   `yaml:"enabled"`          // Enable automatic rollback
    OnFailure       bool   `yaml:"on_failure"`       // Rollback on deployment failure
    BackupRetention int    `yaml:"backup_retention"` // Days to retain backups
    Strategy        string `yaml:"strategy"`         // "immediate", "manual", "scheduled"
}

// ValidationConfig contains validation settings
type ValidationConfig struct {
    Enabled       bool     `yaml:"enabled"`        // Enable validation
    SyntaxCheck   bool     `yaml:"syntax_check"`   // Check SQL syntax
    DryRun        bool     `yaml:"dry_run"`        // Perform dry-run validation
    SkipPatterns  []string `yaml:"skip_patterns"`  // Patterns to skip validation
    CustomRules   []string `yaml:"custom_rules"`   // Custom validation rules
}

// Environment represents a deployment environment configuration
type Environment struct {
    Name      string `yaml:"name"`       // Environment name (e.g., "dev", "staging", "prod")
    Account   string `yaml:"account"`    // Snowflake account
    Username  string `yaml:"username"`   // Snowflake username
    Password  string `yaml:"password"`   // Snowflake password
    Database  string `yaml:"database"`   // Default database
    Schema    string `yaml:"schema"`     // Default schema
    Warehouse string `yaml:"warehouse"`  // Default warehouse
    Role      string `yaml:"role"`       // Default role
    Timeout   string `yaml:"timeout"`    // Connection timeout
}