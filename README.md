# FlakeDrop

A powerful platform for deploying SQL changes from Git repositories to Snowflake databases. Available as both a command-line interface (CLI) and a modern web-based GUI/SaaS solution with enterprise-grade features.

## Features

### Core Features
- **Interactive Commit Selection**: Browse and select specific commits with detailed information
- **Repository Management**: Configure and manage multiple Git repositories with different target databases
- **Secure Credential Storage**: Encrypted storage of Snowflake credentials
- **Dry-Run Mode**: Preview changes before applying them to production
- **Progress Tracking**: Real-time progress indicators and detailed execution logs
- **Multi-Platform Support**: Works seamlessly on macOS, Linux, and Windows
- **License Management**: Built-in license key validation and management
- **CI/CD Ready**: Non-interactive mode for automated deployments
- **Automated Rollback**: Automatic rollback of failed deployments with configurable strategies
- **Disaster Recovery**: Schema snapshots, backup/restore, and point-in-time recovery
- **Deployment History**: Full audit trail with versioning and history tracking
- **Schema Comparison**: Compare schemas between environments with drift detection
- **Synchronization Scripts**: Generate SQL scripts to synchronize schema differences
- **Visual Diff Display**: Side-by-side comparison with color-coded differences
- **Version Tracking**: Save and compare historical schema versions

## Editions

### Community Edition (CLI)
- Free and open-source command-line tool
- Full feature set for individual developers
- Self-hosted deployment
- Community support

## Quick Start

```bash
# Install the CLI
go build -o flakedrop main.go

# Run initial setup
flakedrop setup

# Add a repository
flakedrop repo add

# Deploy changes (with automatic rollback on failure)
flakedrop deploy analytics

# Compare schemas between environments
flakedrop compare --source dev --target prod

# View deployment history
flakedrop rollback list

# Manual rollback if needed
flakedrop rollback deployment <deployment-id>

# Create a backup
flakedrop rollback backup --database ANALYTICS_DB --schema PUBLIC
```

## Installation

### Pre-built Binaries

Download the appropriate binary for your platform from the [releases page](https://github.com/CHMGhost/FlakeDrop/releases):

```bash
# macOS (Intel)
curl -L -o flakedrop-darwin-amd64.tar.gz https://github.com/CHMGhost/FlakeDrop/releases/latest/download/flakedrop-darwin-amd64.tar.gz
tar -xzf flakedrop-darwin-amd64.tar.gz
chmod +x flakedrop-darwin-amd64
sudo mv flakedrop-darwin-amd64 /usr/local/bin/flakedrop

# macOS (Apple Silicon)
curl -L -o flakedrop-darwin-arm64.tar.gz https://github.com/CHMGhost/FlakeDrop/releases/latest/download/flakedrop-darwin-arm64.tar.gz
tar -xzf flakedrop-darwin-arm64.tar.gz
chmod +x flakedrop-darwin-arm64
sudo mv flakedrop-darwin-arm64 /usr/local/bin/flakedrop

# Linux (x64)
curl -L -o flakedrop-linux-amd64.tar.gz https://github.com/CHMGhost/FlakeDrop/releases/latest/download/flakedrop-linux-amd64.tar.gz
tar -xzf flakedrop-linux-amd64.tar.gz
chmod +x flakedrop-linux-amd64
sudo mv flakedrop-linux-amd64 /usr/local/bin/flakedrop

# Linux (ARM64)
curl -L -o flakedrop-linux-arm64.tar.gz https://github.com/CHMGhost/FlakeDrop/releases/latest/download/flakedrop-linux-arm64.tar.gz
tar -xzf flakedrop-linux-arm64.tar.gz
chmod +x flakedrop-linux-arm64
sudo mv flakedrop-linux-arm64 /usr/local/bin/flakedrop

# Windows (x64)
# Download flakedrop-windows-amd64.zip from the releases page
# Extract and add flakedrop-windows-amd64.exe to your PATH
```

### Building from Source

Requirements:
- Go 1.21 or higher
- Git

```bash
# Clone the repository
git clone https://github.com/CHMGhost/FlakeDrop.git
cd FlakeDrop

# Build the binary
go build -o flakedrop main.go

# Install to system path (macOS/Linux)
sudo mv flakedrop /usr/local/bin/

# Verify installation
flakedrop --version
```

## Usage

### Initial Setup

Configure your Snowflake connection and credentials:

```bash
flakedrop setup
```

This interactive setup will guide you through:
- Snowflake account configuration
- Authentication credentials
- Default warehouse and role settings
- Optional first repository setup

### Deploying Changes

Deploy changes from a configured repository:

```bash
# Interactive mode (default)
flakedrop deploy analytics

# Deploy specific commit
flakedrop deploy analytics --commit abc1234

# Dry-run mode
flakedrop deploy analytics --dry-run

# Non-interactive mode (uses latest commit)
flakedrop deploy analytics --interactive=false
```

### Managing Repositories

```bash
# List configured repositories
flakedrop repo list

# Add a new repository
flakedrop repo add

# Remove a repository
flakedrop repo remove analytics
```

### Rollback and Recovery

FlakeDrop v1.2.0 introduces comprehensive rollback functionality:

```bash
# View deployment history
flakedrop rollback list

# Rollback a specific deployment
flakedrop rollback deployment <deployment-id>

# Create a manual backup
flakedrop rollback backup --database ANALYTICS_DB --schema PUBLIC

# Restore from a backup
flakedrop rollback restore <backup-id>

# Validate rollback capability
flakedrop rollback validate <deployment-id>
```

**Automatic Rollback**: When enabled in configuration, FlakeDrop automatically rolls back failed deployments:
- Creates pre-deployment snapshots
- Tracks all schema changes
- Automatically reverts changes on failure
- Maintains full audit trail

### License Management

```bash
# Show license status
flakedrop license show

# Activate a license
flakedrop license activate --key YOUR-LICENSE-KEY
```

### Security Features

FlakeDrop includes comprehensive security features for managing credentials, access control, and audit logging:

#### Password Encryption

Protect sensitive passwords in configuration files:

```bash
# Encrypt passwords in your config file
flakedrop encrypt-config

# Or use environment variable (recommended for CI/CD)
export SNOWFLAKE_PASSWORD="your-password"
```

#### Access Control

Manage users and roles with fine-grained permissions:

```bash
# View security status
flakedrop security status

# Create a new user
flakedrop security create-user --username john.doe --email john.doe@company.com

# Create a custom role with specific permissions
flakedrop security create-role --name developer \
  --description "Developer role with deployment permissions" \
  --permissions "deployment:*:read,create,update" \
  --permissions "repository:*:read" \
  --permissions "config:*:read"

# Assign role to user
flakedrop security assign-role --user john.doe --role developer

# Check user permissions
flakedrop security check-access --user john.doe --action create --resource deployment:prod
```

#### Security Scanning

Scan configurations for security vulnerabilities:

```bash
# Scan current configuration
flakedrop security scan-config

# Generate detailed report
flakedrop security scan-config --report security-report.json
```

#### Audit Logging

Track all security-related activities:

```bash
# View audit logs
flakedrop security audit-log --days 7

# Filter by user or action
flakedrop security audit-log --user john.doe --action create

# Verify audit log integrity
flakedrop security verify-audit

# Comprehensive security audit
flakedrop security audit --output audit-report.json
```

#### Credential Management

Securely store and manage credentials:

```bash
# Store a credential
flakedrop security store-credential -n github-token -t api_key -v "ghp_xxxx"

# List stored credentials
flakedrop security list-credentials
```

## Configuration

Configuration is stored in `~/.flakedrop/config.yaml`:

```yaml
snowflake:
  account: "xy12345.us-east-1"
  username: "DEPLOY_USER"
  password: "encrypted_password"
  role: "DEPLOYMENT_ROLE"
  warehouse: "DEPLOY_WH"

repositories:
  - name: "analytics"
    git_url: "https://github.com/company/analytics-repo.git"
    branch: "main"
    database: "ANALYTICS_DB"
    schema: "PUBLIC"

deployment:
  rollback:
    enabled: true                    # Enable rollback functionality
    on_failure: true                # Automatically rollback on deployment failure
    backup_retention: 7             # Days to retain backups
    strategy: "snapshot"            # Rollback strategy: snapshot, incremental, or transaction

license:
  key: "XXXX-XXXX-XXXX-XXXX"
  expires_at: "2024-12-31"
```

## Documentation

- [Quick Reference](docs/QUICK_REFERENCE.md) - Essential commands and workflows
- [User Guide](docs/USER_GUIDE.md) - Comprehensive guide for getting started
- [Command Reference](docs/COMMANDS.md) - Detailed documentation of all commands
- [Configuration Guide](docs/CONFIGURATION.md) - Configuration options and best practices
- [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) - Deployment strategies and workflows
- [Deployment Best Practices](docs/DEPLOYMENT_BEST_PRACTICES.md) - Production deployment best practices
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions
- [Examples](docs/EXAMPLES.md) - Real-world usage examples
- [Contributing](docs/CONTRIBUTING.md) - Guidelines for contributors

## Development

### Technology Stack

- **Go 1.21+** - Core language
- **Cobra** - CLI framework
- **Viper** - Configuration management
- **Survey** - Interactive UI components
- **go-git** - Git operations
- **snowflake-connector-go** - Snowflake connectivity

### Project Structure

```
flakedrop/
├── cmd/              # Command implementations
├── internal/         # Internal packages
│   ├── config/       # Configuration management
│   ├── git/          # Git operations
│   ├── snowflake/    # Snowflake client
│   └── ui/           # UI components
├── pkg/              # Public packages
├── docs/             # Documentation
└── test/             # Test files
```

### Building for Distribution

```bash
# Build for all platforms
make build-all

# Or build individually:
# macOS
GOOS=darwin GOARCH=amd64 go build -o flakedrop-darwin main.go

# Linux
GOOS=linux GOARCH=amd64 go build -o flakedrop-linux main.go

# Windows
GOOS=windows GOARCH=amd64 go build -o flakedrop-windows.exe main.go
```

### Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run integration tests
go test -tags=integration ./test/integration
```

## Support

- **Documentation**: See the [docs](docs/) directory
- **Issues**: [GitHub Issues](https://github.com/CHMGhost/FlakeDrop/issues)
- **Discussions**: [GitHub Discussions](https://github.com/CHMGhost/FlakeDrop/discussions)

## License

FlakeDrop uses a dual licensing model:

- **Community Edition (CLI)**: Licensed under the MIT License - free for any use

Copyright (c) 2025 FlakeDrop. All rights reserved.

See [LICENSE](LICENSE) file for complete licensing details.

## Acknowledgments

Built by the FlakeDrop team.