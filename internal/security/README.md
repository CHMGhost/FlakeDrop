# Security Package

The security package provides comprehensive security features for FlakeDrop, including credential management, audit logging, access control, security scanning, and secure configuration handling.

## Components

### 1. Credential Manager (`credentials.go`)

Manages secure storage and retrieval of sensitive credentials using system keychains or encrypted file storage.

**Features:**
- System keychain integration (macOS Keychain, Windows Credential Manager, Linux Secret Service)
- AES-256-GCM encryption for file-based storage
- PBKDF2 key derivation with 100,000 iterations
- Machine-specific encryption keys
- Credential export/import with password protection

**Usage:**
```go
// Create credential manager
cm, err := security.NewCredentialManager()

// Store credential
err = cm.StoreCredential("snowflake-prod", "password", "secret123", 
    map[string]string{"environment": "production"})

// Retrieve credential
cred, err := cm.GetCredential("snowflake-prod")
fmt.Println(cred.Value) // "secret123"

// List all credentials
names, err := cm.ListCredentials()
```

### 2. Audit Logger (`audit.go`)

Provides tamper-proof audit logging with hash chain integrity verification.

**Features:**
- SHA-256 hash chain for tamper detection
- Structured JSON logging
- Automatic log rotation (size and time-based)
- Event categorization and filtering
- Integrity verification

**Event Types:**
- Authentication events
- Access control events
- Deployment events
- Configuration changes
- Security events

**Usage:**
```go
// Create audit logger
logger, err := security.NewAuditLogger("")

// Log an event
err = logger.LogDeployment("deploy", "repo-name", "success", 
    map[string]interface{}{"commit": "abc123"})

// Query logs
filter := security.AuditFilter{
    EventType: security.EventTypeDeployment,
    StartTime: &startTime,
    Limit:     100,
}
events, err := logger.Query(filter)

// Verify integrity
report, err := logger.VerifyIntegrity(startTime, endTime)
```

### 3. Security Scanner (`scanner.go`)

Scans code and configurations for security vulnerabilities.

**Built-in Rules:**
- Hard-coded passwords and API keys
- Private key detection
- SQL injection vulnerabilities
- Insecure TLS configurations
- Weak cryptographic algorithms
- Overly permissive file permissions
- Missing error handling

**Usage:**
```go
// Create scanner
scanner := security.NewScanner()

// Scan file
report, err := scanner.ScanFile("deploy.sql")

// Scan directory
report, err := scanner.ScanDirectory("./sql-scripts")

// Add custom rule
scanner.AddRule(security.ScanRule{
    ID:       "CUSTOM001",
    Name:     "Production Table Access",
    Severity: security.SeverityHigh,
    Pattern:  regexp.MustCompile("PROD_.*"),
})
```

### 4. Access Control (`access.go`)

Implements role-based access control (RBAC) for deployment operations.

**Features:**
- User and role management
- Policy-based access control
- Conditional access rules
- Permission inheritance
- Audit trail integration

**Usage:**
```go
// Create access control
ac, err := security.NewAccessControl("", auditLogger)

// Create user
user := &security.User{
    Username: "john.doe",
    Email:    "john@example.com",
}
err = ac.CreateUser(user)

// Assign role
err = ac.AssignRole(user.ID, "deployer")

// Check access
request := security.AccessRequest{
    UserID:   user.ID,
    Resource: "deployment:production",
    Action:   "create",
}
decision := ac.CheckAccess(request)
```

### 5. Secure Configuration (`config.go`)

Handles encrypted configuration management with automatic sensitive data protection.

**Features:**
- Automatic encryption of sensitive fields
- Credential reference resolution
- Configuration validation
- Backup and restore
- Checksum verification

**Usage:**
```go
// Create secure config manager
sc, err := security.NewSecureConfig("", credManager, auditLogger)

// Load encrypted config
config, err := sc.LoadConfig("app-config.yaml")

// Get secure value
password, err := sc.GetSecureValue(config, "database_password")

// Save with encryption
err = sc.SaveConfig("app-config.yaml", config, true)
```

### 6. Security Manager (`manager.go`)

Provides a unified interface for all security features.

**Features:**
- Centralized security management
- Integration with FlakeDrop models
- Security audit reports
- SSL/TLS verification
- Deployment access control

**Usage:**
```go
// Create security manager
manager, err := security.NewManager()

// Secure Snowflake config
err = manager.SecureSnowflakeConfig(&config.Snowflake)

// Check deployment access
err = manager.CheckDeploymentAccess("analytics", "deploy")

// Perform security audit
report, err := manager.PerformSecurityAudit()
```

## Security Best Practices

1. **Credential Storage**
   - Never store credentials in plain text
   - Use system keychains when available
   - Rotate credentials regularly

2. **Audit Logging**
   - Enable for all production deployments
   - Regularly verify log integrity
   - Archive logs securely

3. **Access Control**
   - Follow principle of least privilege
   - Regular access reviews
   - Use time-based restrictions for sensitive operations

4. **Security Scanning**
   - Scan all code before deployment
   - Create custom rules for your environment
   - Address critical issues immediately

5. **Configuration Security**
   - Encrypt sensitive configuration data
   - Use separate configs per environment
   - Version control configs (without secrets)

## Testing

Run security tests:
```bash
go test ./internal/security/... -v
```

Run with race detection:
```bash
go test ./internal/security/... -race
```

## Dependencies

- `github.com/zalando/go-keyring` - System keychain integration
- `golang.org/x/crypto` - Cryptographic functions
- Standard library crypto packages

## Future Enhancements

1. **Hardware Security Module (HSM) Support**
   - Integration with cloud HSMs
   - PKCS#11 support

2. **Multi-Factor Authentication**
   - TOTP/HOTP support
   - Hardware token integration

3. **Advanced Threat Detection**
   - Anomaly detection in audit logs
   - Machine learning-based security scanning

4. **Compliance Automation**
   - Automated compliance reports
   - Policy as code

5. **Zero-Trust Architecture**
   - Service mesh integration
   - mTLS for all communications