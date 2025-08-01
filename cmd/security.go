package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"flakedrop/internal/config"
	"flakedrop/internal/security"
	"flakedrop/internal/ui"
	"flakedrop/pkg/models"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var securityCmd = &cobra.Command{
	Use:   "security",
	Short: "Security management commands",
	Long:  `Manage security features including credentials, audit logs, access control, and security scanning.`,
}

var securityStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show security status",
	Long:  `Display the current security status including credentials, audit logs, and access control.`,
	RunE:  runSecurityStatus,
}

var storeCredentialCmd = &cobra.Command{
	Use:   "store-credential",
	Short: "Store a credential securely",
	Long:  `Store a credential in the secure credential manager using system keychain or encrypted storage.`,
	RunE:  runStoreCredential,
}

var listCredentialsCmd = &cobra.Command{
	Use:   "list-credentials",
	Short: "List stored credentials",
	Long:  `List all stored credentials (names only, not values).`,
	RunE:  runListCredentials,
}

var scanConfigCmd = &cobra.Command{
	Use:   "scan-config",
	Short: "Scan configuration for security issues",
	Long:  `Perform a security scan on the current configuration to detect vulnerabilities and misconfigurations.`,
	RunE:  runScanConfig,
}

var auditLogCmd = &cobra.Command{
	Use:   "audit-log",
	Short: "View audit logs",
	Long:  `View and search audit logs for security events and access attempts.`,
	RunE:  runAuditLog,
}

var verifyAuditCmd = &cobra.Command{
	Use:   "verify-audit",
	Short: "Verify audit log integrity",
	Long:  `Verify the integrity of audit logs using hash chain verification.`,
	RunE:  runVerifyAudit,
}

var createUserCmd = &cobra.Command{
	Use:   "create-user",
	Short: "Create a new user",
	Long:  `Create a new user in the access control system.`,
	RunE:  runCreateUser,
}

var createRoleCmd = &cobra.Command{
	Use:   "create-role",
	Short: "Create a new role",
	Long:  `Create a new role in the access control system with specific permissions.

Permission Format:
  resource:action1,action2,action3

Resources can use wildcards (*) to match multiple items:
  - deployment:* matches all deployments
  - repository:* matches all repositories  
  - config:* matches all configurations
  - * matches all resources

Common Actions:
  - read: View/list resources
  - create: Create new resources
  - update: Modify existing resources
  - delete: Remove resources
  - * matches all actions

Examples:
  # Read-only access to all deployments
  --permissions "deployment:*:read"
  
  # Full access to deployments and read access to repos
  --permissions "deployment:*:read,create,update,delete" --permissions "repository:*:read"
  
  # Admin access to everything
  --permissions "*:*"

Default Roles Available:
  - admin: Full system access
  - deployer: Can perform deployments
  - viewer: Read-only access`,
	RunE:  runCreateRole,
}

var assignRoleCmd = &cobra.Command{
	Use:   "assign-role",
	Short: "Assign role to user",
	Long:  `Assign a role to a user for access control.`,
	RunE:  runAssignRole,
}

var checkAccessCmd = &cobra.Command{
	Use:   "check-access",
	Short: "Check access permissions",
	Long:  `Check if a user has permission to perform an action on a resource.`,
	RunE:  runCheckAccess,
}

var securityAuditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Perform comprehensive security audit",
	Long:  `Perform a comprehensive security audit of the system including credentials, configurations, and access controls.`,
	RunE:  runSecurityAudit,
}

// Command flags
var (
	credentialName     string
	credentialType     string
	credentialValue    string
	credentialMetadata map[string]string
	
	auditEventType    string
	auditDays         int
	auditUser         string
	auditAction       string
	
	scanPath          string
	scanReport        string
	
	verifyStartDate   string
	verifyEndDate     string
	
	userName          string
	userEmail         string
	userRole          string
	
	roleName          string
	roleDescription   string
	rolePermissions   []string
	
	checkResource     string
	checkAction       string
	
	auditOutput       string
)

func init() {
	rootCmd.AddCommand(securityCmd)
	
	// Add subcommands
	securityCmd.AddCommand(securityStatusCmd)
	securityCmd.AddCommand(storeCredentialCmd)
	securityCmd.AddCommand(listCredentialsCmd)
	securityCmd.AddCommand(scanConfigCmd)
	securityCmd.AddCommand(auditLogCmd)
	securityCmd.AddCommand(verifyAuditCmd)
	securityCmd.AddCommand(createUserCmd)
	securityCmd.AddCommand(createRoleCmd)
	securityCmd.AddCommand(assignRoleCmd)
	securityCmd.AddCommand(checkAccessCmd)
	securityCmd.AddCommand(securityAuditCmd)
	
	// Store credential flags
	storeCredentialCmd.Flags().StringVarP(&credentialName, "name", "n", "", "Credential name (required)")
	storeCredentialCmd.Flags().StringVarP(&credentialType, "type", "t", "", "Credential type (required)")
	storeCredentialCmd.Flags().StringVarP(&credentialValue, "value", "v", "", "Credential value (required)")
	storeCredentialCmd.MarkFlagRequired("name")
	storeCredentialCmd.MarkFlagRequired("type")
	storeCredentialCmd.MarkFlagRequired("value")
	
	// Audit log flags
	auditLogCmd.Flags().StringVar(&auditEventType, "type", "", "Filter by event type")
	auditLogCmd.Flags().IntVar(&auditDays, "days", 7, "Number of days to look back")
	auditLogCmd.Flags().StringVar(&auditUser, "user", "", "Filter by user")
	auditLogCmd.Flags().StringVar(&auditAction, "action", "", "Filter by action")
	
	// Scan flags
	scanConfigCmd.Flags().StringVar(&scanReport, "report", "", "Output report file path")
	
	// Verify audit flags
	verifyAuditCmd.Flags().StringVar(&verifyStartDate, "start", "", "Start date (YYYY-MM-DD)")
	verifyAuditCmd.Flags().StringVar(&verifyEndDate, "end", "", "End date (YYYY-MM-DD)")
	
	// User management flags
	createUserCmd.Flags().StringVar(&userName, "username", "", "Username (required)")
	createUserCmd.Flags().StringVar(&userEmail, "email", "", "Email address (required)")
	createUserCmd.MarkFlagRequired("username")
	createUserCmd.MarkFlagRequired("email")
	
	// Role management flags
	createRoleCmd.Flags().StringVar(&roleName, "name", "", "Role name (required)")
	createRoleCmd.Flags().StringVar(&roleDescription, "description", "", "Role description")
	createRoleCmd.Flags().StringArrayVar(&rolePermissions, "permissions", []string{}, "Permissions in format resource:action1,action2 (use multiple flags for multiple resources)")
	createRoleCmd.MarkFlagRequired("name")
	
	// Role assignment flags
	assignRoleCmd.Flags().StringVar(&userName, "user", "", "Username (required)")
	assignRoleCmd.Flags().StringVar(&userRole, "role", "", "Role name (required)")
	assignRoleCmd.MarkFlagRequired("user")
	assignRoleCmd.MarkFlagRequired("role")
	
	// Access check flags
	checkAccessCmd.Flags().StringVar(&userName, "user", "", "Username (required)")
	checkAccessCmd.Flags().StringVar(&checkResource, "resource", "", "Resource (required)")
	checkAccessCmd.Flags().StringVar(&checkAction, "action", "", "Action (required)")
	checkAccessCmd.MarkFlagRequired("user")
	checkAccessCmd.MarkFlagRequired("resource")
	checkAccessCmd.MarkFlagRequired("action")
	
	// Security audit flags
	securityAuditCmd.Flags().StringVar(&auditOutput, "output", "", "Output file for audit report")
}

func runSecurityStatus(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	ui.Info("Security Status")
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Get credential count
	creds, err := secManager.GetCredentialManager().ListCredentials()
	if err != nil {
		ui.Warning("Failed to list credentials: " + err.Error())
	} else {
		ui.Success(fmt.Sprintf("Stored Credentials: %d (all encrypted)", len(creds)))
	}
	
	// Check audit log status
	auditLogger := secManager.GetAuditLogger()
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)
	
	events, err := auditLogger.Query(security.AuditFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
	})
	if err != nil {
		ui.Warning("Failed to query audit logs: " + err.Error())
	} else {
		ui.Success(fmt.Sprintf("Audit Events (24h): %d", len(events)))
	}
	
	// Check access control
	ac := secManager.GetAccessControl()
	users := ac.ListUsers()
	roles := ac.ListRoles()
	policies := ac.ListPolicies()
	
	ui.Success(fmt.Sprintf("Users: %d | Roles: %d | Policies: %d", 
		len(users), len(roles), len(policies)))
	
	// Show recent security events
	ui.Info("\nRecent Security Events:")
	secEvents, _ := auditLogger.Query(security.AuditFilter{
		EventType: security.EventTypeSecurity,
		Limit:     5,
	})
	
	for _, event := range secEvents {
		ui.Print(fmt.Sprintf("  [%s] %s - %s (%s)", 
			event.Timestamp.Format("2006-01-02 15:04"),
			event.Action,
			event.Resource,
			event.Result))
	}
	
	return nil
}

func runStoreCredential(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Store credential
	err = secManager.GetCredentialManager().StoreCredential(
		credentialName,
		credentialType,
		credentialValue,
		credentialMetadata,
	)
	
	if err != nil {
		return fmt.Errorf("failed to store credential: %w", err)
	}
	
	ui.Success(fmt.Sprintf("Credential '%s' stored securely", credentialName))
	return nil
}

func runListCredentials(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	creds, err := secManager.GetCredentialManager().ListCredentials()
	if err != nil {
		return fmt.Errorf("failed to list credentials: %w", err)
	}
	
	if len(creds) == 0 {
		ui.Info("No credentials stored")
		return nil
	}
	
	ui.Info("Stored Credentials:")
	for _, name := range creds {
		ui.Print(fmt.Sprintf("  - %s", name))
	}
	
	return nil
}

func runScanConfig(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	ui.StartProgress("Scanning configuration for security issues...")
	
	secManager, err := security.NewManager()
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Load raw configuration without decryption for scanning
	configFile := config.GetConfigFile()
	data, err := os.ReadFile(configFile)
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("failed to read configuration: %w", err)
	}
	
	var rawConfig models.Config
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		ui.StopProgress()
		return fmt.Errorf("failed to parse configuration: %w", err)
	}
	
	// Perform scan on raw config (with encrypted passwords)
	report, err := secManager.ScanConfiguration(&rawConfig)
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("security scan failed: %w", err)
	}
	
	ui.StopProgress()
	
	// Display results
	ui.Info(fmt.Sprintf("Security Scan Results (completed in %s)", report.Duration))
	ui.Print(fmt.Sprintf("Total Issues: %d", report.Summary.Total))
	
	if report.Summary.Critical > 0 {
		ui.Error(fmt.Sprintf("  Critical: %d", report.Summary.Critical))
	}
	if report.Summary.High > 0 {
		ui.Warning(fmt.Sprintf("  High: %d", report.Summary.High))
	}
	if report.Summary.Medium > 0 {
		ui.Warning(fmt.Sprintf("  Medium: %d", report.Summary.Medium))
	}
	if report.Summary.Low > 0 {
		ui.Info(fmt.Sprintf("  Low: %d", report.Summary.Low))
	}
	
	// Show critical findings
	if report.Summary.Critical > 0 {
		ui.Error("\nCritical Issues:")
		for _, finding := range report.Findings {
			if finding.Severity == security.SeverityCritical {
				ui.Print(fmt.Sprintf("  [%s] %s", finding.RuleID, finding.Message))
				if finding.Remediation != "" {
					ui.Print(fmt.Sprintf("    TIP: %s", finding.Remediation))
				}
			}
		}
	}
	
	// Save report if requested
	if scanReport != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal report: %w", err)
		}
		
		if err := os.WriteFile(scanReport, data, 0600); err != nil {
			return fmt.Errorf("failed to save report: %w", err)
		}
		
		ui.Success(fmt.Sprintf("Report saved to: %s", scanReport))
	}
	
	return nil
}

func runAuditLog(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Build filter
	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -auditDays)
	
	filter := security.AuditFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
		EventType: auditEventType,
		User:      auditUser,
		Action:    auditAction,
		Limit:     100,
	}
	
	// Query logs
	events, err := secManager.GetAuditLogger().Query(filter)
	if err != nil {
		return fmt.Errorf("failed to query audit logs: %w", err)
	}
	
	if len(events) == 0 {
		ui.Info("No matching audit events found")
		return nil
	}
	
	ui.Info(fmt.Sprintf("Audit Events (%d found):", len(events)))
	
	for _, event := range events {
		timestamp := event.Timestamp.Format("2006-01-02 15:04:05")
		ui.Print(fmt.Sprintf("\n[%s] %s", timestamp, event.ID))
		ui.Print(fmt.Sprintf("  Type: %s | Action: %s | User: %s", 
			event.EventType, event.Action, event.User))
		ui.Print(fmt.Sprintf("  Resource: %s | Result: %s", 
			event.Resource, event.Result))
		
		if len(event.Details) > 0 {
			ui.Print("  Details:")
			for k, v := range event.Details {
				ui.Print(fmt.Sprintf("    %s: %v", k, v))
			}
		}
	}
	
	return nil
}

func runVerifyAudit(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	ui.StartProgress("Verifying audit log integrity...")
	
	secManager, err := security.NewManager()
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Parse dates
	var startTime, endTime time.Time
	if verifyStartDate != "" {
		startTime, err = time.Parse("2006-01-02", verifyStartDate)
		if err != nil {
			ui.StopProgress()
			return fmt.Errorf("invalid start date: %w", err)
		}
	} else {
		startTime = time.Now().AddDate(0, -1, 0) // Default: last month
	}
	
	if verifyEndDate != "" {
		endTime, err = time.Parse("2006-01-02", verifyEndDate)
		if err != nil {
			ui.StopProgress()
			return fmt.Errorf("invalid end date: %w", err)
		}
	} else {
		endTime = time.Now()
	}
	
	// Verify integrity
	report, err := secManager.GetAuditLogger().VerifyIntegrity(startTime, endTime)
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("integrity verification failed: %w", err)
	}
	
	ui.StopProgress()
	
	// Display results
	ui.Info("Audit Log Integrity Report")
	ui.Print(fmt.Sprintf("Period: %s to %s", 
		startTime.Format("2006-01-02"), 
		endTime.Format("2006-01-02")))
	ui.Print(fmt.Sprintf("Files Checked: %d", report.CheckedFiles))
	ui.Print(fmt.Sprintf("Valid Events: %d", report.ValidEvents))
	
	if report.IsValid {
		ui.Success("Integrity verification PASSED")
	} else {
		ui.Error("Integrity verification FAILED")
		ui.Error("Issues found:")
		for _, issue := range report.Issues {
			ui.Print(fmt.Sprintf("  - %s", issue))
		}
	}
	
	return nil
}

func runCreateUser(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Create user
	user := &security.User{
		Username: userName,
		Email:    userEmail,
	}
	
	err = secManager.GetAccessControl().CreateUser(user)
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}
	
	ui.Success(fmt.Sprintf("User '%s' created successfully", userName))
	ui.Info("User ID: " + user.ID)
	
	return nil
}

func runCreateRole(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Parse permissions
	permissions := []security.Permission{}
	for _, perm := range rolePermissions {
		// Trim spaces
		perm = strings.TrimSpace(perm)
		parts := strings.SplitN(perm, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid permission format: %s (expected resource:action,action)", perm)
		}
		
		resource := parts[0]
		actions := strings.Split(parts[1], ",")
		
		permissions = append(permissions, security.Permission{
			Resource: resource,
			Actions:  actions,
		})
	}
	
	// Create role
	role := &security.Role{
		Name:        roleName,
		Description: roleDescription,
		Permissions: permissions,
	}
	
	err = secManager.GetAccessControl().CreateRole(role)
	if err != nil {
		return fmt.Errorf("failed to create role: %w", err)
	}
	
	ui.Success(fmt.Sprintf("Role '%s' created successfully", roleName))
	ui.Info("Role ID: " + role.ID)
	
	return nil
}

func runAssignRole(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Find user by username
	users := secManager.GetAccessControl().ListUsers()
	var userID string
	for _, u := range users {
		if u.Username == userName {
			userID = u.ID
			break
		}
	}
	
	if userID == "" {
		return fmt.Errorf("user '%s' not found", userName)
	}
	
	// Find role by name
	roles := secManager.GetAccessControl().ListRoles()
	var roleID string
	for _, r := range roles {
		if r.Name == userRole || r.ID == userRole {
			roleID = r.ID
			break
		}
	}
	
	if roleID == "" {
		return fmt.Errorf("role '%s' not found", userRole)
	}
	
	// Assign role
	err = secManager.GetAccessControl().AssignRole(userID, roleID)
	if err != nil {
		return fmt.Errorf("failed to assign role: %w", err)
	}
	
	ui.Success(fmt.Sprintf("Role '%s' assigned to user '%s'", userRole, userName))
	
	return nil
}

func runCheckAccess(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	
	secManager, err := security.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Find user by username
	users := secManager.GetAccessControl().ListUsers()
	var userID string
	for _, u := range users {
		if u.Username == userName {
			userID = u.ID
			break
		}
	}
	
	if userID == "" {
		return fmt.Errorf("user '%s' not found", userName)
	}
	
	// Check access
	request := security.AccessRequest{
		UserID:   userID,
		Resource: checkResource,
		Action:   checkAction,
	}
	
	decision := secManager.GetAccessControl().CheckAccess(request)
	
	ui.Info("Access Check Result:")
	ui.Print(fmt.Sprintf("  User: %s", userName))
	ui.Print(fmt.Sprintf("  Resource: %s", checkResource))
	ui.Print(fmt.Sprintf("  Action: %s", checkAction))
	
	if decision.Allowed {
		ui.Success(fmt.Sprintf("  Result: ALLOWED (%s)", decision.Reason))
	} else {
		ui.Error(fmt.Sprintf("  Result: DENIED (%s)", decision.Reason))
	}
	
	return nil
}

func runSecurityAudit(cmd *cobra.Command, args []string) error {
	ui := ui.NewUI(false, false)
	ui.StartProgress("Performing comprehensive security audit...")
	
	secManager, err := security.NewManager()
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("failed to initialize security manager: %w", err)
	}
	defer secManager.Close()
	
	// Perform audit
	report, err := secManager.PerformSecurityAudit()
	if err != nil {
		ui.StopProgress()
		return fmt.Errorf("security audit failed: %w", err)
	}
	
	ui.StopProgress()
	
	// Display results
	ui.Info("Security Audit Report")
	ui.Print(fmt.Sprintf("Timestamp: %s", report.Timestamp.Format("2006-01-02 15:04:05")))
	
	// Display each section
	for section, data := range report.Sections {
		ui.Info(fmt.Sprintf("\n%s:", strings.Title(strings.ReplaceAll(section, "_", " "))))
		
		switch v := data.(type) {
		case map[string]interface{}:
			for key, value := range v {
				ui.Print(fmt.Sprintf("  %s: %v", key, value))
			}
		case *security.IntegrityReport:
			ui.Print(fmt.Sprintf("  Files Checked: %d", v.CheckedFiles))
			ui.Print(fmt.Sprintf("  Valid Events: %d", v.ValidEvents))
			ui.Print(fmt.Sprintf("  Integrity: %v", v.IsValid))
		case security.Summary:
			ui.Print(fmt.Sprintf("  Total Issues: %d", v.Total))
			if v.Critical > 0 {
				ui.Error(fmt.Sprintf("  Critical: %d", v.Critical))
			}
			if v.High > 0 {
				ui.Warning(fmt.Sprintf("  High: %d", v.High))
			}
		default:
			ui.Print(fmt.Sprintf("  %v", data))
		}
	}
	
	// Save report if requested
	if auditOutput != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal report: %w", err)
		}
		
		if err := os.WriteFile(auditOutput, data, 0600); err != nil {
			return fmt.Errorf("failed to save report: %w", err)
		}
		
		ui.Success(fmt.Sprintf("\nAudit report saved to: %s", auditOutput))
	}
	
	return nil
}