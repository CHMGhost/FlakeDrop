package security

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCredentialManager(t *testing.T) {
	// Create temp directory for testing
	tempDir := t.TempDir()
	os.Setenv("HOME", tempDir)
	defer os.Unsetenv("HOME")

	// Force use of encrypted file storage for testing
	originalKeyring := os.Getenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN")
	os.Setenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN", "false")
	defer func() {
		if originalKeyring != "" {
			os.Setenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN", originalKeyring)
		} else {
			os.Unsetenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN")
		}
	}()

	t.Run("Create credential manager", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)
		assert.NotNil(t, cm)
		assert.False(t, cm.useKeyring)
		assert.NotNil(t, cm.masterKey)
	})

	t.Run("Store and retrieve credential", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store credential
		err = cm.StoreCredential("test-cred", "password", "secret123", map[string]string{
			"environment": "test",
		})
		require.NoError(t, err)

		// Retrieve credential
		cred, err := cm.GetCredential("test-cred")
		require.NoError(t, err)
		assert.Equal(t, "test-cred", cred.Name)
		assert.Equal(t, "password", cred.Type)
		assert.Equal(t, "secret123", cred.Value)
		assert.Equal(t, "test", cred.Metadata["environment"])
	})

	t.Run("List credentials", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store multiple credentials
		err = cm.StoreCredential("cred1", "password", "pass1", nil)
		require.NoError(t, err)
		err = cm.StoreCredential("cred2", "api_key", "key123", nil)
		require.NoError(t, err)

		// List credentials
		names, err := cm.ListCredentials()
		require.NoError(t, err)
		assert.Contains(t, names, "cred1")
		assert.Contains(t, names, "cred2")
	})

	t.Run("Delete credential", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store and delete
		err = cm.StoreCredential("temp-cred", "password", "temp123", nil)
		require.NoError(t, err)

		err = cm.DeleteCredential("temp-cred")
		require.NoError(t, err)

		// Verify deleted
		_, err = cm.GetCredential("temp-cred")
		assert.Error(t, err)
	})

	t.Run("Encryption and decryption", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		plaintext := "sensitive data"
		
		// Encrypt
		encrypted, err := cm.encrypt(plaintext)
		require.NoError(t, err)
		assert.NotEqual(t, plaintext, encrypted)
		assert.NotEmpty(t, encrypted)

		// Decrypt
		decrypted, err := cm.decrypt(encrypted)
		require.NoError(t, err)
		assert.Equal(t, plaintext, decrypted)
	})

	t.Run("Export and import credentials", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store credentials
		err = cm.StoreCredential("export-test1", "password", "pass1", nil)
		require.NoError(t, err)
		err = cm.StoreCredential("export-test2", "api_key", "key2", nil)
		require.NoError(t, err)

		// Export
		backupPassword := "backup123"
		exportData, err := cm.ExportCredentials(backupPassword)
		require.NoError(t, err)
		assert.NotEmpty(t, exportData)

		// Clear credentials
		err = cm.DeleteCredential("export-test1")
		require.NoError(t, err)
		err = cm.DeleteCredential("export-test2")
		require.NoError(t, err)

		// Import
		err = cm.ImportCredentials(exportData, backupPassword)
		require.NoError(t, err)

		// Verify imported
		cred1, err := cm.GetCredential("export-test1")
		require.NoError(t, err)
		assert.Equal(t, "pass1", cred1.Value)

		cred2, err := cm.GetCredential("export-test2")
		require.NoError(t, err)
		assert.Equal(t, "key2", cred2.Value)
	})

	t.Run("Invalid backup password", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store credential
		err = cm.StoreCredential("backup-test", "password", "secret", nil)
		require.NoError(t, err)

		// Export
		exportData, err := cm.ExportCredentials("correct-password")
		require.NoError(t, err)

		// Try import with wrong password
		err = cm.ImportCredentials(exportData, "wrong-password")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid password")
	})
}

func TestCredentialManagerSecurity(t *testing.T) {
	// Create temp directory for testing
	tempDir := t.TempDir()
	os.Setenv("HOME", tempDir)
	defer os.Unsetenv("HOME")

	// Force use of encrypted file storage for testing
	originalKeyring := os.Getenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN")
	os.Setenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN", "false")
	defer func() {
		if originalKeyring != "" {
			os.Setenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN", originalKeyring)
		} else {
			os.Unsetenv("SNOWFLAKE_DEPLOY_USE_KEYCHAIN")
		}
	}()

	t.Run("Master key uniqueness", func(t *testing.T) {
		cm1, err := NewCredentialManager()
		require.NoError(t, err)

		// Create second instance
		cm2, err := NewCredentialManager()
		require.NoError(t, err)

		// Keys should be the same (loaded from file)
		assert.Equal(t, cm1.masterKey, cm2.masterKey)
	})

	t.Run("File permissions", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store credential
		err = cm.StoreCredential("perm-test", "password", "secret", nil)
		require.NoError(t, err)

		// Check file permissions
		credPath := cm.getCredentialPath("perm-test")
		info, err := os.Stat(credPath)
		require.NoError(t, err)

		// Should be readable/writable by owner only (0600)
		mode := info.Mode()
		assert.Equal(t, os.FileMode(0600), mode.Perm())
	})

	t.Run("Credential tampering detection", func(t *testing.T) {
		cm, err := NewCredentialManager()
		require.NoError(t, err)

		// Store credential
		err = cm.StoreCredential("tamper-test", "password", "secret", nil)
		require.NoError(t, err)

		// Tamper with the file
		credPath := cm.getCredentialPath("tamper-test")
		data, err := os.ReadFile(credPath)
		require.NoError(t, err)

		// Modify the encrypted value
		tamperedData := append(data, []byte("tampered")...)
		err = os.WriteFile(credPath, tamperedData, 0600)
		require.NoError(t, err)

		// Try to retrieve - should fail
		_, err = cm.GetCredential("tamper-test")
		assert.Error(t, err)
	})
}