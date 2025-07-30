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
- **Comprehensive Rollback**: Automated rollback with transaction support and point-in-time recovery
- **Disaster Recovery**: Schema snapshots, backup/restore, and recovery strategies
- **Deployment History**: Full audit trail with versioning and history tracking
- **Schema Comparison**: Compare schemas between environments with drift detection
- **Synchronization Scripts**: Generate SQL scripts to synchronize schema differences
- **Visual Diff Display**: Side-by-side comparison with color-coded differences
- **Version Tracking**: Save and compare historical schema versions

### GUI/SaaS Features (Coming Soon)
- **Web-Based Interface**: Modern, intuitive web UI for managing deployments
- **Team Collaboration**: Multi-user support with role-based access control
- **Deployment Pipelines**: Visual pipeline builder for complex deployment workflows
- **Approval Workflows**: Built-in approval processes for production deployments
- **Real-Time Monitoring**: Live dashboards for deployment status and health
- **API Access**: RESTful API for integration with existing tools
- **Audit Compliance**: Enterprise-grade audit logging and compliance reporting
- **Cloud-Native**: Hosted SaaS option with automatic updates and maintenance

## Editions

### Community Edition (CLI)
- Free and open-source command-line tool
- Full feature set for individual developers
- Self-hosted deployment
- Community support

### Professional Edition (Coming Soon)
- Web-based GUI interface
- Team collaboration features
- Priority support
- Self-hosted or SaaS options

### Enterprise Edition (Coming Soon)
- Advanced security and compliance features
- Custom integrations
- Dedicated support
- On-premises or private cloud deployment

## Quick Start

```bash
# Install the CLI
go build -o flakedrop main.go

# Run initial setup
flakedrop setup

# Add a repository
flakedrop repo add

# Deploy changes
flakedrop deploy analytics

# Compare schemas between environments
flakedrop compare --source dev --target prod

# View deployment history
flakedrop rollback list

# Rollback if needed
flakedrop rollback deployment <deployment-id>
```

## Installation

### Pre-built Binaries

Download the appropriate binary for your platform from the [releases page](https://github.com/CHMGhost/FlakeDrop/releases):

```bash
# macOS
curl -L -o flakedrop https://github.com/CHMGhost/FlakeDrop/releases/latest/download/flakedrop-darwin
chmod +x flakedrop
sudo mv flakedrop /usr/local/bin/

# Linux
curl -L -o flakedrop https://github.com/CHMGhost/FlakeDrop/releases/latest/download/flakedrop-linux
chmod +x flakedrop
sudo mv flakedrop /usr/local/bin/

# Windows
# Download flakedrop-windows.exe and add to your PATH
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

### License Management

```bash
# Show license status
flakedrop license show

# Activate a license
flakedrop license activate --key YOUR-LICENSE-KEY
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
- **Professional & Enterprise Editions (GUI/SaaS)**: Commercial license required

Copyright (c) 2024 FlakeDrop. All rights reserved.

See [LICENSE](LICENSE) file for complete licensing details.

## Acknowledgments

Built by the FlakeDrop team.