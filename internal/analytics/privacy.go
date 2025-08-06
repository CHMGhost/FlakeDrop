package analytics

import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    "crypto/sha256"
    "encoding/base64"
    "encoding/hex"
    "fmt"
    "io"
    "regexp"
    "strings"
)

var (
    emailRegex = regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
    ipRegex    = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
    pathRegex  = regexp.MustCompile(`(/Users/[^/\s]+|/home/[^/\s]+|C:\\Users\\[^\\s]+)`)
    tokenRegex = regexp.MustCompile(`(?i)(token|key|secret|password|pwd|api[_-]?key)[\s:=]+[\S]+`)
)

type PrivacyFilter struct {
    enableHashing    bool
    enableEncryption bool
    encryptionKey    []byte
    redactPatterns   []*regexp.Regexp
}

func NewPrivacyFilter(enableHashing, enableEncryption bool) *PrivacyFilter {
    filter := &PrivacyFilter{
        enableHashing:    enableHashing,
        enableEncryption: enableEncryption,
        redactPatterns: []*regexp.Regexp{
            emailRegex,
            ipRegex,
            pathRegex,
            tokenRegex,
        },
    }

    if enableEncryption {
        filter.generateEncryptionKey()
    }

    return filter
}

func (pf *PrivacyFilter) SanitizeString(input string) string {
    sanitized := input

    for _, pattern := range pf.redactPatterns {
        sanitized = pattern.ReplaceAllString(sanitized, "[REDACTED]")
    }

    sanitized = pf.redactSensitiveKeywords(sanitized)

    return sanitized
}

func (pf *PrivacyFilter) SanitizeMap(data map[string]interface{}) map[string]interface{} {
    sanitized := make(map[string]interface{})
    
    for key, value := range data {
        sanitizedKey := pf.sanitizeKey(key)
        
        switch v := value.(type) {
        case string:
            sanitized[sanitizedKey] = pf.SanitizeString(v)
        case map[string]interface{}:
            sanitized[sanitizedKey] = pf.SanitizeMap(v)
        case []interface{}:
            sanitized[sanitizedKey] = pf.sanitizeSlice(v)
        default:
            sanitized[sanitizedKey] = v
        }
    }
    
    return sanitized
}

func (pf *PrivacyFilter) HashIdentifier(identifier string) string {
    if !pf.enableHashing {
        return identifier
    }

    hash := sha256.Sum256([]byte(identifier))
    return hex.EncodeToString(hash[:16])
}

func (pf *PrivacyFilter) EncryptSensitiveData(data []byte) (string, error) {
    if !pf.enableEncryption || pf.encryptionKey == nil {
        return base64.StdEncoding.EncodeToString(data), nil
    }

    block, err := aes.NewCipher(pf.encryptionKey)
    if err != nil {
        return "", err
    }

    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return "", err
    }

    nonce := make([]byte, gcm.NonceSize())
    if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
        return "", err
    }

    ciphertext := gcm.Seal(nonce, nonce, data, nil)
    return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (pf *PrivacyFilter) DecryptSensitiveData(encrypted string) ([]byte, error) {
    if !pf.enableEncryption || pf.encryptionKey == nil {
        return base64.StdEncoding.DecodeString(encrypted)
    }

    ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
    if err != nil {
        return nil, err
    }

    block, err := aes.NewCipher(pf.encryptionKey)
    if err != nil {
        return nil, err
    }

    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return nil, err
    }

    nonceSize := gcm.NonceSize()
    if len(ciphertext) < nonceSize {
        return nil, fmt.Errorf("ciphertext too short")
    }

    nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
    return gcm.Open(nil, nonce, ciphertext, nil)
}

func (pf *PrivacyFilter) sanitizeKey(key string) string {
    lowerKey := strings.ToLower(key)
    
    sensitiveKeys := []string{
        "password", "pwd", "secret", "token", "key", "api",
        "credential", "auth", "private", "ssn", "social",
    }
    
    for _, sensitive := range sensitiveKeys {
        if strings.Contains(lowerKey, sensitive) {
            return "[SENSITIVE_KEY]"
        }
    }
    
    return key
}

func (pf *PrivacyFilter) sanitizeSlice(data []interface{}) []interface{} {
    sanitized := make([]interface{}, len(data))
    
    for i, item := range data {
        switch v := item.(type) {
        case string:
            sanitized[i] = pf.SanitizeString(v)
        case map[string]interface{}:
            sanitized[i] = pf.SanitizeMap(v)
        case []interface{}:
            sanitized[i] = pf.sanitizeSlice(v)
        default:
            sanitized[i] = v
        }
    }
    
    return sanitized
}

func (pf *PrivacyFilter) redactSensitiveKeywords(input string) string {
    sensitiveWords := []string{
        "credit card", "creditcard", "cc number",
        "social security", "ssn", "sin",
        "bank account", "routing number",
        "pin", "cvv", "cvc",
    }
    
    result := input
    for _, word := range sensitiveWords {
        re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(word) + `[\s:]*[\S]+`)
        result = re.ReplaceAllString(result, "[REDACTED]")
    }
    
    return result
}

func (pf *PrivacyFilter) generateEncryptionKey() {
    key := make([]byte, 32)
    if _, err := rand.Read(key); err != nil {
        // Fallback to a deterministic key if random fails
        hash := sha256.Sum256([]byte("flakedrop-analytics-default-key"))
        pf.encryptionKey = hash[:]
    } else {
        pf.encryptionKey = key
    }
}

func (pf *PrivacyFilter) AddCustomPattern(pattern string) error {
    re, err := regexp.Compile(pattern)
    if err != nil {
        return fmt.Errorf("invalid regex pattern: %w", err)
    }
    
    pf.redactPatterns = append(pf.redactPatterns, re)
    return nil
}

func (pf *PrivacyFilter) RemovePattern(pattern string) {
    for i, p := range pf.redactPatterns {
        if p.String() == pattern {
            pf.redactPatterns = append(pf.redactPatterns[:i], pf.redactPatterns[i+1:]...)
            return
        }
    }
}