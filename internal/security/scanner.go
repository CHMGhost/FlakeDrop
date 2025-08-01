package security

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	
	"flakedrop/internal/common"
)

// Scanner performs security scanning on configurations and deployments
type Scanner struct {
	rules      []ScanRule
	exclusions []string
	reports    []ScanReport
}

// ScanRule defines a security scanning rule
type ScanRule struct {
	ID          string
	Name        string
	Description string
	Severity    string
	Category    string
	Pattern     *regexp.Regexp
	FileTypes   []string
	Check       func(content string, metadata map[string]string) []Finding
}

// Finding represents a security issue found during scanning
type Finding struct {
	RuleID      string            `json:"rule_id"`
	Severity    string            `json:"severity"`
	Message     string            `json:"message"`
	File        string            `json:"file,omitempty"`
	Line        int               `json:"line,omitempty"`
	Column      int               `json:"column,omitempty"`
	Evidence    string            `json:"evidence,omitempty"`
	Remediation string            `json:"remediation,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ScanReport contains the results of a security scan
type ScanReport struct {
	ID         string    `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	Target     string    `json:"target"`
	Type       string    `json:"type"`
	Findings   []Finding `json:"findings"`
	Summary    Summary   `json:"summary"`
	Duration   string    `json:"duration"`
}

// Summary provides a summary of scan results
type Summary struct {
	Total    int `json:"total"`
	Critical int `json:"critical"`
	High     int `json:"high"`
	Medium   int `json:"medium"`
	Low      int `json:"low"`
	Info     int `json:"info"`
}

// Severity levels
const (
	SeverityCritical = "critical"
	SeverityHigh     = "high"
	SeverityMedium   = "medium"
	SeverityLow      = "low"
	SeverityInfo     = "info"
)

// NewScanner creates a new security scanner with default rules
func NewScanner() *Scanner {
	scanner := &Scanner{
		exclusions: []string{
			".git",
			"node_modules",
			"vendor",
			"*.test",
			"*_test.go",
		},
	}

	// Initialize default security rules
	scanner.initializeRules()

	return scanner
}

// ScanFile scans a single file for security issues
func (s *Scanner) ScanFile(filepath string) (*ScanReport, error) {
	startTime := time.Now()

	// Validate the file path
	validatedPath, err := common.CleanPath(filepath)
	if err != nil {
		return nil, fmt.Errorf("invalid file path: %w", err)
	}

	content, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	report := &ScanReport{
		ID:        generateScanID(),
		Timestamp: startTime,
		Target:    filepath,
		Type:      "file",
		Findings:  []Finding{},
	}

	// Check if file should be excluded
	if s.shouldExclude(filepath) {
		return report, nil
	}

	// Apply each rule
	for _, rule := range s.rules {
		if s.shouldApplyRule(rule, filepath) {
			findings := s.applyRule(rule, string(content), filepath)
			report.Findings = append(report.Findings, findings...)
		}
	}

	// Calculate summary
	report.Summary = s.calculateSummary(report.Findings)
	report.Duration = time.Since(startTime).String()

	return report, nil
}

// ScanDirectory recursively scans a directory for security issues
func (s *Scanner) ScanDirectory(dir string) (*ScanReport, error) {
	startTime := time.Now()

	report := &ScanReport{
		ID:        generateScanID(),
		Timestamp: startTime,
		Target:    dir,
		Type:      "directory",
		Findings:  []Finding{},
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip files with errors
		}

		if info.IsDir() {
			if s.shouldExclude(path) {
				return filepath.SkipDir
			}
			return nil
		}

		fileReport, err := s.ScanFile(path)
		if err != nil {
			return nil // Skip files with errors
		}

		report.Findings = append(report.Findings, fileReport.Findings...)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan directory: %w", err)
	}

	report.Summary = s.calculateSummary(report.Findings)
	report.Duration = time.Since(startTime).String()

	return report, nil
}

// ScanConfig scans configuration for security issues
func (s *Scanner) ScanConfig(config map[string]interface{}) (*ScanReport, error) {
	startTime := time.Now()

	report := &ScanReport{
		ID:        generateScanID(),
		Timestamp: startTime,
		Target:    "configuration",
		Type:      "config",
		Findings:  []Finding{},
	}

	// Convert config to string for pattern matching
	configStr := fmt.Sprintf("%v", config)

	// Apply configuration-specific rules
	for _, rule := range s.rules {
		if rule.Category == "configuration" {
			findings := s.applyRule(rule, configStr, "config")
			report.Findings = append(report.Findings, findings...)
		}
	}

	// Check for specific configuration issues
	report.Findings = append(report.Findings, s.checkConfigSecurity(config)...)

	report.Summary = s.calculateSummary(report.Findings)
	report.Duration = time.Since(startTime).String()

	return report, nil
}

// VerifySSLCertificate verifies SSL/TLS certificate for a given host
func (s *Scanner) VerifySSLCertificate(host string) (*CertificateReport, error) {
	if !strings.Contains(host, ":") {
		host = host + ":443"
	}

	// Connect with InsecureSkipVerify to get the certificate for verification
	// #nosec G402 - We're intentionally connecting to verify the certificate
	conn, err := tls.Dial("tcp", host, &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	report := &CertificateReport{
		Host:      host,
		CheckedAt: time.Now(),
		Issues:    []string{},
	}

	// Get certificate chain
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		report.Issues = append(report.Issues, "No certificates found")
		report.IsValid = false
		return report, nil
	}

	cert := certs[0]
	report.Subject = cert.Subject.String()
	report.Issuer = cert.Issuer.String()
	report.NotBefore = cert.NotBefore
	report.NotAfter = cert.NotAfter
	report.DNSNames = cert.DNSNames

	// Check certificate validity
	now := time.Now()
	if now.Before(cert.NotBefore) {
		report.Issues = append(report.Issues, "Certificate not yet valid")
	}
	if now.After(cert.NotAfter) {
		report.Issues = append(report.Issues, "Certificate has expired")
	}

	// Check hostname
	if err := cert.VerifyHostname(strings.Split(host, ":")[0]); err != nil {
		report.Issues = append(report.Issues, fmt.Sprintf("Hostname verification failed: %v", err))
	}

	// Check certificate chain
	opts := x509.VerifyOptions{
		Intermediates: x509.NewCertPool(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}

	if _, err := cert.Verify(opts); err != nil {
		report.Issues = append(report.Issues, fmt.Sprintf("Certificate chain verification failed: %v", err))
	}

	report.IsValid = len(report.Issues) == 0
	return report, nil
}

// CertificateReport contains SSL/TLS certificate verification results
type CertificateReport struct {
	Host      string    `json:"host"`
	CheckedAt time.Time `json:"checked_at"`
	IsValid   bool      `json:"is_valid"`
	Subject   string    `json:"subject"`
	Issuer    string    `json:"issuer"`
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
	DNSNames  []string  `json:"dns_names"`
	Issues    []string  `json:"issues,omitempty"`
}

// Initialize default security rules
func (s *Scanner) initializeRules() {
	s.rules = []ScanRule{
		// Credential detection rules
		{
			ID:          "CRED001",
			Name:        "Hard-coded Password",
			Description: "Detects hard-coded passwords in source code",
			Severity:    SeverityCritical,
			Category:    "credentials",
			Pattern:     regexp.MustCompile(`(?i)(password|passwd|pwd)\s*[:=]\s*["'][\w\S]{4,}["']`),
			FileTypes:   []string{".go", ".py", ".js", ".yaml", ".yml", ".json"},
			Check:       s.checkHardcodedCredentials,
		},
		{
			ID:          "CRED002",
			Name:        "API Key Detection",
			Description: "Detects exposed API keys",
			Severity:    SeverityCritical,
			Category:    "credentials",
			Pattern:     regexp.MustCompile(`(?i)(api[_-]?key|apikey|api[_-]?secret)\s*[:=]\s*["'][\w\-]{16,}["']`),
			FileTypes:   []string{".go", ".py", ".js", ".yaml", ".yml", ".json", ".env"},
			Check:       s.checkAPIKeys,
		},
		{
			ID:          "CRED003",
			Name:        "Private Key Detection",
			Description: "Detects private keys in code",
			Severity:    SeverityCritical,
			Category:    "credentials",
			Pattern:     regexp.MustCompile(`-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----`),
			FileTypes:   []string{},
			Check:       s.checkPrivateKeys,
		},

		// SQL Injection vulnerabilities
		{
			ID:          "SQL001",
			Name:        "SQL Injection Risk",
			Description: "Detects potential SQL injection vulnerabilities",
			Severity:    SeverityHigh,
			Category:    "injection",
			Pattern:     regexp.MustCompile(`(?i)(exec|execute|query)\s*\(\s*["']?\s*\+|fmt\.sprintf.*select|string\s*\+.*where`),
			FileTypes:   []string{".go", ".py", ".js"},
			Check:       s.checkSQLInjection,
		},

		// Insecure configurations
		{
			ID:          "CONFIG001",
			Name:        "Insecure TLS Configuration",
			Description: "Detects insecure TLS/SSL configurations",
			Severity:    SeverityHigh,
			Category:    "configuration",
			Pattern:     regexp.MustCompile(`(?i)insecureskipverify\s*[:=]\s*true|tls\.config.*insecure|sslverify\s*[:=]\s*false`),
			FileTypes:   []string{".go", ".yaml", ".yml"},
			Check:       s.checkInsecureTLS,
		},
		{
			ID:          "CONFIG002",
			Name:        "Weak Cryptography",
			Description: "Detects use of weak cryptographic algorithms",
			Severity:    SeverityMedium,
			Category:    "cryptography",
			Pattern:     regexp.MustCompile(`(?i)(md5|sha1|des|rc4)\.new|crypto\/(md5|sha1|des|rc4)`),
			FileTypes:   []string{".go", ".py", ".js"},
			Check:       s.checkWeakCrypto,
		},

		// File permissions
		{
			ID:          "PERM001",
			Name:        "Overly Permissive File Mode",
			Description: "Detects overly permissive file permissions",
			Severity:    SeverityMedium,
			Category:    "permissions",
			Pattern:     regexp.MustCompile(`(?i)os\.(chmod|mkdir|mkdirall|writefile|create).*0[67]77|perm:\s*0[67]77`),
			FileTypes:   []string{".go"},
			Check:       s.checkFilePermissions,
		},

		// Error handling
		{
			ID:          "ERROR001",
			Name:        "Missing Error Handling",
			Description: "Detects missing error handling",
			Severity:    SeverityLow,
			Category:    "error-handling",
			Pattern:     regexp.MustCompile(`_\s*:=\s*\w+\(`),
			FileTypes:   []string{".go"},
			Check:       s.checkErrorHandling,
		},
	}
}

// Rule check implementations

func (s *Scanner) checkHardcodedCredentials(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[0].Pattern.MatchString(line) {
			// Additional checks to reduce false positives
			if strings.Contains(line, "example") || strings.Contains(line, "test") {
				continue
			}
			
			findings = append(findings, Finding{
				RuleID:      "CRED001",
				Severity:    SeverityCritical,
				Message:     "Hard-coded password detected",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Use environment variables or secure credential storage instead of hard-coding passwords",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkAPIKeys(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[1].Pattern.MatchString(line) {
			findings = append(findings, Finding{
				RuleID:      "CRED002",
				Severity:    SeverityCritical,
				Message:     "Exposed API key detected",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Store API keys in environment variables or secure vaults",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkPrivateKeys(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	if s.rules[2].Pattern.MatchString(content) {
		findings = append(findings, Finding{
			RuleID:      "CRED003",
			Severity:    SeverityCritical,
			Message:     "Private key found in source code",
			Remediation: "Never commit private keys to source control. Use secure key management systems",
		})
	}
	
	return findings
}

func (s *Scanner) checkSQLInjection(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[3].Pattern.MatchString(line) {
			findings = append(findings, Finding{
				RuleID:      "SQL001",
				Severity:    SeverityHigh,
				Message:     "Potential SQL injection vulnerability",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Use parameterized queries or prepared statements",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkInsecureTLS(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[4].Pattern.MatchString(line) {
			findings = append(findings, Finding{
				RuleID:      "CONFIG001",
				Severity:    SeverityHigh,
				Message:     "Insecure TLS configuration detected",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Enable proper TLS certificate verification",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkWeakCrypto(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[5].Pattern.MatchString(line) {
			findings = append(findings, Finding{
				RuleID:      "CONFIG002",
				Severity:    SeverityMedium,
				Message:     "Use of weak cryptographic algorithm",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Use strong cryptographic algorithms like SHA-256, AES-256",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkFilePermissions(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[6].Pattern.MatchString(line) {
			findings = append(findings, Finding{
				RuleID:      "PERM001",
				Severity:    SeverityMedium,
				Message:     "Overly permissive file permissions",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Use restrictive file permissions (0600 or 0700)",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkErrorHandling(content string, metadata map[string]string) []Finding {
	var findings []Finding
	
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if s.rules[7].Pattern.MatchString(line) {
			findings = append(findings, Finding{
				RuleID:      "ERROR001",
				Severity:    SeverityLow,
				Message:     "Error ignored with blank identifier",
				Line:        i + 1,
				Evidence:    strings.TrimSpace(line),
				Remediation: "Handle errors appropriately instead of ignoring them",
			})
		}
	}
	
	return findings
}

func (s *Scanner) checkConfigSecurity(config map[string]interface{}) []Finding {
	var findings []Finding

	// Check for plaintext passwords in config
	if checkConfigValue(config, "password", func(v interface{}) bool {
		if str, ok := v.(string); ok && str != "" {
			// Check if password is not encrypted (doesn't start with ENC[ or $)
			if !strings.HasPrefix(str, "ENC[") && !strings.HasPrefix(str, "$") {
				return true
			}
		}
		return false
	}) {
		findings = append(findings, Finding{
			RuleID:      "CONFIG003",
			Severity:    SeverityCritical,
			Message:     "Plaintext password in configuration",
			Remediation: "Encrypt passwords using 'flakedrop encrypt-config' or use SNOWFLAKE_PASSWORD environment variable",
		})
	}

	// Check for missing encryption settings
	if !checkConfigValue(config, "encryption_enabled", func(v interface{}) bool {
		if b, ok := v.(bool); ok && b {
			return true
		}
		return false
	}) {
		findings = append(findings, Finding{
			RuleID:      "CONFIG004",
			Severity:    SeverityHigh,
			Message:     "Encryption not enabled in configuration",
			Remediation: "Enable encryption for sensitive data",
		})
	}

	return findings
}

// Helper methods

func (s *Scanner) shouldExclude(path string) bool {
	for _, pattern := range s.exclusions {
		if matched, _ := filepath.Match(pattern, filepath.Base(path)); matched {
			return true
		}
		if strings.Contains(path, pattern) {
			return true
		}
	}
	return false
}

func (s *Scanner) shouldApplyRule(rule ScanRule, filepath string) bool {
	if len(rule.FileTypes) == 0 {
		return true
	}
	
	ext := strings.ToLower(filepath[strings.LastIndex(filepath, "."):])
	for _, ft := range rule.FileTypes {
		if ext == ft {
			return true
		}
	}
	return false
}

func (s *Scanner) applyRule(rule ScanRule, content, filepath string) []Finding {
	if rule.Check != nil {
		metadata := map[string]string{"file": filepath}
		findings := rule.Check(content, metadata)
		for i := range findings {
			findings[i].File = filepath
		}
		return findings
	}
	return []Finding{}
}

func (s *Scanner) calculateSummary(findings []Finding) Summary {
	summary := Summary{}
	
	for _, finding := range findings {
		summary.Total++
		switch finding.Severity {
		case SeverityCritical:
			summary.Critical++
		case SeverityHigh:
			summary.High++
		case SeverityMedium:
			summary.Medium++
		case SeverityLow:
			summary.Low++
		case SeverityInfo:
			summary.Info++
		}
	}
	
	return summary
}

// Utility functions

func generateScanID() string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("scan-%d", timestamp)
}

func checkConfigValue(config map[string]interface{}, key string, check func(interface{}) bool) bool {
	if val, ok := config[key]; ok {
		return check(val)
	}
	
	// Check nested values
	for _, v := range config {
		if nested, ok := v.(map[string]interface{}); ok {
			if checkConfigValue(nested, key, check) {
				return true
			}
		}
	}
	
	return false
}

// AddRule adds a custom security rule
func (s *Scanner) AddRule(rule ScanRule) {
	s.rules = append(s.rules, rule)
}

// SetExclusions sets the exclusion patterns
func (s *Scanner) SetExclusions(exclusions []string) {
	s.exclusions = exclusions
}

// GetReport retrieves a specific scan report
func (s *Scanner) GetReport(id string) (*ScanReport, error) {
	for _, report := range s.reports {
		if report.ID == id {
			return &report, nil
		}
	}
	return nil, fmt.Errorf("report not found")
}

// SaveReport saves a scan report
func (s *Scanner) SaveReport(report *ScanReport, filepath string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	
	return os.WriteFile(filepath, data, 0600)
}