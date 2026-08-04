package feishu

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const (
	chatTitleTTL        = 5 * time.Minute
	chatTitleMaxEntries = 1024
	chatTitleMaxBytes   = 200
)

// ChatTitleLookup keeps raw chat ids inside the Feishu adapter.
type ChatTitleLookup interface {
	Title(ctx context.Context, chatID string) (string, error)
}

type sdkChatTitleLookup struct {
	client *lark.Client
}

func newSDKChatTitleLookup(appID, appSecret string, logger *slog.Logger) ChatTitleLookup {
	client := lark.NewClient(
		appID,
		appSecret,
		lark.WithReqTimeout(15*time.Second),
		lark.WithLogger(safeSDKLogger{logger: logger}),
	)
	return &sdkChatTitleLookup{client: client}
}

func (s *sdkChatTitleLookup) Title(ctx context.Context, chatID string) (string, error) {
	req := larkim.NewGetChatReqBuilder().ChatId(chatID).Build()
	resp, err := s.client.Im.V1.Chat.Get(ctx, req)
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Success() || resp.Data == nil || resp.Data.Name == nil {
		return "", errors.New("chat title lookup was not successful")
	}
	return *resp.Data.Name, nil
}

type chatTitleEntry struct {
	title     string
	cachedAt  time.Time
	expiresAt time.Time
}

type chatTitleCache struct {
	mu      sync.Mutex
	lookup  ChatTitleLookup
	entries map[string]chatTitleEntry
	now     func() time.Time
}

func newChatTitleCache(lookup ChatTitleLookup) *chatTitleCache {
	return &chatTitleCache{
		lookup:  lookup,
		entries: make(map[string]chatTitleEntry),
		now:     time.Now,
	}
}

func (c *chatTitleCache) title(ctx context.Context, chatID string) (string, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || c == nil || c.lookup == nil {
		return "", nil
	}
	now := c.now()
	c.mu.Lock()
	if entry, ok := c.entries[chatID]; ok && now.Before(entry.expiresAt) {
		c.mu.Unlock()
		return entry.title, nil
	}
	c.mu.Unlock()

	title, err := c.lookup.Title(ctx, chatID)
	if err != nil {
		return "", err
	}
	title = clampUTF8Bytes(strings.TrimSpace(title), chatTitleMaxBytes)
	if title == "" {
		return "", nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
	if _, exists := c.entries[chatID]; !exists && len(c.entries) >= chatTitleMaxEntries {
		oldestKey := ""
		var oldest time.Time
		for key, entry := range c.entries {
			if oldestKey == "" || entry.cachedAt.Before(oldest) {
				oldestKey = key
				oldest = entry.cachedAt
			}
		}
		delete(c.entries, oldestKey)
	}
	c.entries[chatID] = chatTitleEntry{
		title:     title,
		cachedAt:  now,
		expiresAt: now.Add(chatTitleTTL),
	}
	return title, nil
}

func clampUTF8Bytes(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	end := limit
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end]
}
