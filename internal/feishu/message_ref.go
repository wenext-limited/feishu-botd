package feishu

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// MessageRefForApp derives the provider-safe identity of one Feishu message.
// The app alias is part of the digest so equal raw ids cannot correlate two
// configured applications. The raw id never appears in the returned value.
func MessageRefForApp(appAlias, messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return ""
	}
	seed := "feishu-botd/message-ref/v1\x00" + effectiveAppAlias(appAlias) + "\x00" + messageID
	sum := sha256.Sum256([]byte(seed))
	return "msgref_" + hex.EncodeToString(sum[:])
}
