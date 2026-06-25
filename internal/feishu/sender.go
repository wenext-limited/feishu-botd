package feishu

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkchannel "github.com/larksuite/oapi-sdk-go/v3/channel"
	channeltypes "github.com/larksuite/oapi-sdk-go/v3/channel/types"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"

	"feishu-botd/internal/notify"
)

type Sender interface {
	Ready(ctx context.Context) error
	Send(ctx context.Context, chatID string, req notify.Request) (string, error)
}

type ChannelSender struct {
	appID     string
	appSecret string
	client    *lark.Client
	channel   channeltypes.Channel
	logger    *slog.Logger

	mu             sync.Mutex
	readyUntil     time.Time
	lastReadyError error
}

func NewChannelSender(appID, appSecret string, logger *slog.Logger) *ChannelSender {
	client := lark.NewClient(appID, appSecret, lark.WithReqTimeout(15*time.Second))
	return &ChannelSender{appID: appID, appSecret: appSecret, client: client, channel: larkchannel.NewChannel(client, nil), logger: logger}
}

func (s *ChannelSender) Ready(ctx context.Context) error {
	now := time.Now()
	s.mu.Lock()
	if now.Before(s.readyUntil) {
		err := s.lastReadyError
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	resp, err := s.client.GetTenantAccessTokenBySelfBuiltApp(ctx, &larkcore.SelfBuiltTenantAccessTokenReq{AppID: s.appID, AppSecret: s.appSecret})
	if err == nil && (resp == nil || !resp.Success() || resp.TenantAccessToken == "") {
		if resp == nil {
			err = fmt.Errorf("tenant token response was empty")
		} else {
			err = fmt.Errorf("tenant token rejected: code=%d", resp.Code)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReadyError = err
	if err == nil {
		s.readyUntil = now.Add(5 * time.Minute)
	} else {
		s.readyUntil = now.Add(30 * time.Second)
	}
	return err
}

func (s *ChannelSender) Send(ctx context.Context, chatID string, req notify.Request) (string, error) {
	input := &channeltypes.SendInput{
		ChatID:   chatID,
		Title:    req.Title,
		Markdown: req.Markdown,
	}
	result, err := s.channel.Send(ctx, input)
	if err != nil {
		return "", fmt.Errorf("feishu send failed: %w", err)
	}
	if result == nil || result.MessageID == "" {
		return "", fmt.Errorf("feishu send returned no message id")
	}
	return result.MessageID, nil
}
