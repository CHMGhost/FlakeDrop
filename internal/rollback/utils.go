package rollback

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// generateRandomID generates a random ID of specified length
func generateRandomID(length int) string {
	bytes := make([]byte, length/2)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}