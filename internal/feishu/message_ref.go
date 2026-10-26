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

// ThreadRefForApp derives the provider-safe identity of one Feishu thread
// (topic). It is domain-separated from message refs, so a thread ref can never
// be mistaken for, or used as, the ref of the message that opened the thread.
func ThreadRefForApp(appAlias, threadID string) string {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return ""
	}
	seed := "feishu-botd/thread-ref/v1\x00" + effectiveAppAlias(appAlias) + "\x00" + threadID
	sum := sha256.Sum256([]byte(seed))
	return "threadref_" + hex.EncodeToString(sum[:])
}

// ThreadRootMessageRefForApp returns the ref of the message that opened the
// Feishu thread holding messageID, or "" when the message is not in a thread
// or is itself that root. Feishu's root_id is the thread root for every reply
// inside a thread, however deep the reply-to-a-reply chain goes.
func ThreadRootMessageRefForApp(appAlias, messageID, threadID, rootID string) string {
	messageID = strings.TrimSpace(messageID)
	rootID = strings.TrimSpace(rootID)
	if strings.TrimSpace(threadID) == "" || rootID == "" || rootID == messageID {
		return ""
	}
	return MessageRefForApp(appAlias, rootID)
}
