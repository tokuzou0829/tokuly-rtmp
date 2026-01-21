package rtmp

import (
	"crypto/sha256"
	"encoding/hex"
)

func maskStreamKey(streamKey string) string {
	if streamKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(streamKey))
	return hex.EncodeToString(sum[:4])
}
