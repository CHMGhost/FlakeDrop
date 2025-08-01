package security

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"flakedrop/internal/common"
)

// AccessControl manages permissions and access control
type AccessControl struct {
	mu          sync.RWMutex
	policies    map[string]*Policy
	roles       map[string]*Role
	users       map[string]*User
	configPath  string
	auditLogger *AuditLogger
}

// Policy defines access control policies
type Policy struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Rules       []PolicyRule        `json:"rules"`
	Created     time.Time           `json:"created"`
	Modified    time.Time           `json:"modified"`
	Metadata    map[string]string   `json:"metadata,omitempty"`
}

// PolicyRule defines a single access rule
type PolicyRule struct {
	Resource   string   `json:"resource"`
	Actions    []string `json:"actions"`
	Effect     string   `json:"effect"` // "allow" or "deny"
	Conditions []Condition `json:"conditions,omitempty"`
}

// Condition defines conditions for policy rules
type Condition struct {
	Type     string `json:"type"`
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// Role represents a user role
type Role struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Policies    []string          `json:"policies"`
	Permissions []Permission      `json:"permissions"`
	Created     time.Time         `json:"created"`
	Modified    time.Time         `json:"modified"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Permission represents a specific permission
type Permission struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}

// User represents a system user
type User struct {
	ID       string            `json:"id"`
	Username string            `json:"username"`
	Email    string            `json:"email"`
	Roles    []string          `json:"roles"`
	Active   bool              `json:"active"`
	Created  time.Time         `json:"created"`
	Modified time.Time         `json:"modified"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// AccessRequest represents a request to access a resource
type AccessRequest struct {
	UserID   string            `json:"user_id"`
	Resource string            `json:"resource"`
	Action   string            `json:"action"`
	Context  map[string]string `json:"context,omitempty"`
}

// AccessDecision represents the result of an access control check
type AccessDecision struct {
	Allowed     bool     `json:"allowed"`
	Reason      string   `json:"reason"`
	AppliedRules []string `json:"applied_rules,omitempty"`
}

// NewAccessControl creates a new access control manager
func NewAccessControl(configPath string, auditLogger *AuditLogger) (*AccessControl, error) {
	if configPath == "" {
		home, _ := os.UserHomeDir()
		configPath = filepath.Join(home, ".flakedrop", "access")
	}

	// Ensure directory exists
	if err := os.MkdirAll(configPath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create access control directory: %w", err)
	}

	ac := &AccessControl{
		policies:    make(map[string]*Policy),
		roles:       make(map[string]*Role),
		users:       make(map[string]*User),
		configPath:  configPath,
		auditLogger: auditLogger,
	}

	// Load existing configuration
	if err := ac.loadConfig(); err != nil {
		// If no config exists, initialize with defaults
		ac.initializeDefaults()
	}

	return ac, nil
}

// CheckAccess checks if a user has access to perform an action on a resource
func (ac *AccessControl) CheckAccess(request AccessRequest) AccessDecision {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	decision := AccessDecision{
		Allowed: false,
		Reason:  "No matching policy found",
	}

	// Get user
	user, exists := ac.users[request.UserID]
	if !exists {
		decision.Reason = "User not found"
		ac.logAccessAttempt(request, decision)
		return decision
	}

	if !user.Active {
		decision.Reason = "User is not active"
		ac.logAccessAttempt(request, decision)
		return decision
	}

	// Check all user roles
	for _, roleID := range user.Roles {
		role, exists := ac.roles[roleID]
		if !exists {
			continue
		}

		// Check role permissions
		if ac.checkRolePermissions(role, request) {
			decision.Allowed = true
			decision.Reason = fmt.Sprintf("Allowed by role: %s", role.Name)
			decision.AppliedRules = append(decision.AppliedRules, roleID)
			ac.logAccessAttempt(request, decision)
			return decision
		}

		// Check role policies
		for _, policyID := range role.Policies {
			policy, exists := ac.policies[policyID]
			if !exists {
				continue
			}

			if result := ac.evaluatePolicy(policy, request); result != nil {
				if result.Allowed {
					decision = *result
					ac.logAccessAttempt(request, decision)
					return decision
				}
				// Keep track of explicit denies
				if result.Reason == "Explicitly denied by policy" {
					decision = *result
				}
			}
		}
	}

	ac.logAccessAttempt(request, decision)
	return decision
}

// CreatePolicy creates a new access control policy
func (ac *AccessControl) CreatePolicy(policy *Policy) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if policy.ID == "" {
		policy.ID = generateID("policy")
	}

	policy.Created = time.Now()
	policy.Modified = policy.Created

	ac.policies[policy.ID] = policy

	if err := ac.saveConfig(); err != nil {
		return fmt.Errorf("failed to save policy: %w", err)
	}

	_ = ac.auditLogger.LogEvent(EventTypeSecurity, ActionCreate, "policy:"+policy.ID, "success",
		map[string]interface{}{"policy_name": policy.Name})

	return nil
}

// CreateRole creates a new role
func (ac *AccessControl) CreateRole(role *Role) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if role.ID == "" {
		role.ID = generateID("role")
	}

	role.Created = time.Now()
	role.Modified = role.Created

	ac.roles[role.ID] = role

	if err := ac.saveConfig(); err != nil {
		return fmt.Errorf("failed to save role: %w", err)
	}

	_ = ac.auditLogger.LogEvent(EventTypeSecurity, ActionCreate, "role:"+role.ID, "success",
		map[string]interface{}{"role_name": role.Name})

	return nil
}

// CreateUser creates a new user
func (ac *AccessControl) CreateUser(user *User) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if user.ID == "" {
		user.ID = generateID("user")
	}

	user.Created = time.Now()
	user.Modified = user.Created
	user.Active = true

	ac.users[user.ID] = user

	if err := ac.saveConfig(); err != nil {
		return fmt.Errorf("failed to save user: %w", err)
	}

	_ = ac.auditLogger.LogEvent(EventTypeSecurity, ActionCreate, "user:"+user.ID, "success",
		map[string]interface{}{"username": user.Username})

	return nil
}

// AssignRole assigns a role to a user
func (ac *AccessControl) AssignRole(userID, roleID string) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	user, exists := ac.users[userID]
	if !exists {
		return fmt.Errorf("user not found")
	}

	role, exists := ac.roles[roleID]
	if !exists {
		return fmt.Errorf("role not found")
	}

	// Check if role already assigned
	for _, r := range user.Roles {
		if r == roleID {
			return nil // Already assigned
		}
	}

	user.Roles = append(user.Roles, roleID)
	user.Modified = time.Now()

	if err := ac.saveConfig(); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	ac.auditLogger.LogEvent(EventTypeSecurity, ActionGrant, "role:"+roleID, "success",
		map[string]interface{}{
			"user_id":   userID,
			"username":  user.Username,
			"role_name": role.Name,
		})

	return nil
}

// RevokeRole revokes a role from a user
func (ac *AccessControl) RevokeRole(userID, roleID string) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	user, exists := ac.users[userID]
	if !exists {
		return fmt.Errorf("user not found")
	}

	// Remove role
	newRoles := []string{}
	found := false
	for _, r := range user.Roles {
		if r != roleID {
			newRoles = append(newRoles, r)
		} else {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("role not assigned to user")
	}

	user.Roles = newRoles
	user.Modified = time.Now()

	if err := ac.saveConfig(); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	ac.auditLogger.LogEvent(EventTypeSecurity, ActionRevoke, "role:"+roleID, "success",
		map[string]interface{}{
			"user_id":  userID,
			"username": user.Username,
		})

	return nil
}

// ListUsers returns all users
func (ac *AccessControl) ListUsers() []*User {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	users := make([]*User, 0, len(ac.users))
	for _, user := range ac.users {
		users = append(users, user)
	}
	return users
}

// ListRoles returns all roles
func (ac *AccessControl) ListRoles() []*Role {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	roles := make([]*Role, 0, len(ac.roles))
	for _, role := range ac.roles {
		roles = append(roles, role)
	}
	return roles
}

// ListPolicies returns all policies
func (ac *AccessControl) ListPolicies() []*Policy {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	policies := make([]*Policy, 0, len(ac.policies))
	for _, policy := range ac.policies {
		policies = append(policies, policy)
	}
	return policies
}

// Helper methods

func (ac *AccessControl) checkRolePermissions(role *Role, request AccessRequest) bool {
	for _, perm := range role.Permissions {
		if ac.matchResource(perm.Resource, request.Resource) {
			for _, action := range perm.Actions {
				if action == "*" || action == request.Action {
					return true
				}
			}
		}
	}
	return false
}

func (ac *AccessControl) evaluatePolicy(policy *Policy, request AccessRequest) *AccessDecision {
	for _, rule := range policy.Rules {
		if !ac.matchResource(rule.Resource, request.Resource) {
			continue
		}

		// Check if action matches
		actionMatches := false
		for _, action := range rule.Actions {
			if action == "*" || action == request.Action {
				actionMatches = true
				break
			}
		}

		if !actionMatches {
			continue
		}

		// Evaluate conditions
		if !ac.evaluateConditions(rule.Conditions, request.Context) {
			continue
		}

		// Rule matches
		decision := &AccessDecision{
			Allowed:      rule.Effect == "allow",
			AppliedRules: []string{policy.ID},
		}

		if rule.Effect == "allow" {
			decision.Reason = fmt.Sprintf("Allowed by policy: %s", policy.Name)
		} else {
			decision.Reason = "Explicitly denied by policy"
		}

		return decision
	}

	return nil
}

func (ac *AccessControl) matchResource(pattern, resource string) bool {
	// Simple pattern matching with wildcards
	if pattern == "*" {
		return true
	}

	// Convert pattern to regex
	pattern = strings.ReplaceAll(pattern, "*", ".*")
	pattern = "^" + pattern + "$"

	matched, _ := regexp.MatchString(pattern, resource)
	return matched
}

func (ac *AccessControl) evaluateConditions(conditions []Condition, context map[string]string) bool {
	for _, cond := range conditions {
		value, exists := context[cond.Key]
		if !exists {
			return false
		}

		if !ac.evaluateCondition(cond, value) {
			return false
		}
	}
	return true
}

func (ac *AccessControl) evaluateCondition(cond Condition, value string) bool {
	switch cond.Operator {
	case "equals":
		return value == cond.Value
	case "not_equals":
		return value != cond.Value
	case "contains":
		return strings.Contains(value, cond.Value)
	case "starts_with":
		return strings.HasPrefix(value, cond.Value)
	case "ends_with":
		return strings.HasSuffix(value, cond.Value)
	default:
		return false
	}
}

func (ac *AccessControl) logAccessAttempt(request AccessRequest, decision AccessDecision) {
	result := "denied"
	if decision.Allowed {
		result = "allowed"
	}

	_ = ac.auditLogger.LogAccess(request.Action, request.Resource, result,
		map[string]interface{}{
			"user_id": request.UserID,
			"reason":  decision.Reason,
			"context": request.Context,
		})
}

// Configuration persistence

func (ac *AccessControl) loadConfig() error {
	var hasAnyFile bool
	
	// Load policies
	policiesFile := filepath.Join(ac.configPath, "policies.json")
	validatedPoliciesFile, err := common.ValidatePath(policiesFile, ac.configPath)
	if err != nil {
		return fmt.Errorf("invalid policies file path: %w", err)
	}
	data, err := os.ReadFile(validatedPoliciesFile) // #nosec G304 - path is validated
	if err == nil {
		hasAnyFile = true
		if err := json.Unmarshal(data, &ac.policies); err != nil {
			return fmt.Errorf("failed to unmarshal policies: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to read policies file: %w", err)
	}

	// Load roles
	rolesFile := filepath.Join(ac.configPath, "roles.json")
	validatedRolesFile, err := common.ValidatePath(rolesFile, ac.configPath)
	if err != nil {
		return fmt.Errorf("invalid roles file path: %w", err)
	}
	data, err = os.ReadFile(validatedRolesFile) // #nosec G304 - path is validated
	if err == nil {
		hasAnyFile = true
		if err := json.Unmarshal(data, &ac.roles); err != nil {
			return fmt.Errorf("failed to unmarshal roles: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to read roles file: %w", err)
	}

	// Load users
	usersFile := filepath.Join(ac.configPath, "users.json")
	validatedUsersFile, err := common.ValidatePath(usersFile, ac.configPath)
	if err != nil {
		return fmt.Errorf("invalid users file path: %w", err)
	}
	data, err = os.ReadFile(validatedUsersFile) // #nosec G304 - path is validated
	if err == nil {
		hasAnyFile = true
		if err := json.Unmarshal(data, &ac.users); err != nil {
			return fmt.Errorf("failed to unmarshal users: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to read users file: %w", err)
	}

	// Return error if no files exist to trigger initialization
	if !hasAnyFile {
		return fmt.Errorf("no access control files found")
	}

	return nil
}

func (ac *AccessControl) saveConfig() error {
	// Save policies
	policiesFile := filepath.Join(ac.configPath, "policies.json")
	if data, err := json.MarshalIndent(ac.policies, "", "  "); err == nil {
		if err := os.WriteFile(policiesFile, data, 0600); err != nil {
			return fmt.Errorf("failed to save policies: %w", err)
		}
	}

	// Save roles
	rolesFile := filepath.Join(ac.configPath, "roles.json")
	if data, err := json.MarshalIndent(ac.roles, "", "  "); err == nil {
		if err := os.WriteFile(rolesFile, data, 0600); err != nil {
			return fmt.Errorf("failed to save roles: %w", err)
		}
	}

	// Save users
	usersFile := filepath.Join(ac.configPath, "users.json")
	if data, err := json.MarshalIndent(ac.users, "", "  "); err == nil {
		if err := os.WriteFile(usersFile, data, 0600); err != nil {
			return fmt.Errorf("failed to save users: %w", err)
		}
	}

	return nil
}

// Initialize default roles and policies
func (ac *AccessControl) initializeDefaults() {
	// Create default admin role
	adminRole := &Role{
		ID:          "admin",
		Name:        "Administrator",
		Description: "Full system access",
		Permissions: []Permission{
			{Resource: "*", Actions: []string{"*"}},
		},
		Created:  time.Now(),
		Modified: time.Now(),
	}
	ac.roles[adminRole.ID] = adminRole

	// Create default deployer role
	deployerRole := &Role{
		ID:          "deployer",
		Name:        "Deployer",
		Description: "Can perform deployments",
		Permissions: []Permission{
			{Resource: "deployment:*", Actions: []string{"create", "read", "update"}},
			{Resource: "repository:*", Actions: []string{"read"}},
			{Resource: "config:*", Actions: []string{"read"}},
		},
		Created:  time.Now(),
		Modified: time.Now(),
	}
	ac.roles[deployerRole.ID] = deployerRole

	// Create default viewer role
	viewerRole := &Role{
		ID:          "viewer",
		Name:        "Viewer",
		Description: "Read-only access",
		Permissions: []Permission{
			{Resource: "*", Actions: []string{"read"}},
		},
		Created:  time.Now(),
		Modified: time.Now(),
	}
	ac.roles[viewerRole.ID] = viewerRole

	// Create default security policy
	securityPolicy := &Policy{
		ID:          "default-security",
		Name:        "Default Security Policy",
		Description: "Basic security rules",
		Rules: []PolicyRule{
			{
				Resource: "credential:*",
				Actions:  []string{"*"},
				Effect:   "deny",
				Conditions: []Condition{
					{
						Type:     "user",
						Key:      "role",
						Operator: "not_equals",
						Value:    "admin",
					},
				},
			},
			{
				Resource: "audit:*",
				Actions:  []string{"delete", "update"},
				Effect:   "deny",
			},
		},
		Created:  time.Now(),
		Modified: time.Now(),
	}
	ac.policies[securityPolicy.ID] = securityPolicy

	// Create default admin user
	adminUser := &User{
		ID:       "admin",
		Username: "admin",
		Email:    "admin@snowflake-deploy.local",
		Roles:    []string{"admin"},
		Active:   true,
		Created:  time.Now(),
		Modified: time.Now(),
	}
	ac.users[adminUser.ID] = adminUser

	// Save defaults
	_ = ac.saveConfig()
}

// Utility function
func generateID(prefix string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s-%d", prefix, timestamp)
}