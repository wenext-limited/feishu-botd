package feishu

import (
	"testing"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestMessageReactionEventsNormalizeWithoutRawIDs(t *testing.T) {
	t.Parallel()
	r := NewCommandReceiver(CommandReceiverConfig{AppAlias: "support", AppID: "app", AppSecret: "secret"}, nil, nil)

	created, ok := r.MessageReactionFromCreatedEvent(reactionCreatedEvent("evt_reaction_private", "om_private", "THUMBSUP"))
	if !ok {
		t.Fatal("created reaction was not normalized")
	}
	if created.DeliveryID == "evt_reaction_private" || created.MessageRef == "om_private" {
		t.Fatalf("reaction leaked raw ids: %#v", created)
	}
	if created.MessageRef != MessageRefForApp("support", "om_private") || created.SenderID != "ou_actor" || created.ReactionType != "THUMBSUP" || created.Operation != MessageReactionAdded {
		t.Fatalf("created reaction = %#v", created)
	}

	deleted, ok := r.MessageReactionFromDeletedEvent(reactionDeletedEvent("evt_deleted_private", "om_private", "ThumbsDown"))
	if !ok {
		t.Fatal("deleted reaction was not normalized")
	}
	if deleted.Operation != MessageReactionRemoved || deleted.ReactionType != "ThumbsDown" {
		t.Fatalf("deleted reaction = %#v", deleted)
	}
}

func TestMessageReactionIgnoresUnsupportedEmojiAndMissingIdentity(t *testing.T) {
	t.Parallel()
	r := NewCommandReceiver(CommandReceiverConfig{AppAlias: "support", AppID: "app", AppSecret: "secret"}, nil, nil)
	if _, ok := r.MessageReactionFromCreatedEvent(reactionCreatedEvent("evt", "om", "SMILE")); ok {
		t.Fatal("unsupported emoji was forwarded")
	}
	event := reactionCreatedEvent("evt", "om", "THUMBSUP")
	event.Event.UserId = nil
	if _, ok := r.MessageReactionFromCreatedEvent(event); ok {
		t.Fatal("reaction without an actor was forwarded")
	}
}

func reactionCreatedEvent(eventID, messageID, emoji string) *larkim.P2MessageReactionCreatedV1 {
	return &larkim.P2MessageReactionCreatedV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{EventID: eventID}},
		Event: &larkim.P2MessageReactionCreatedV1Data{
			MessageId:    ptr(messageID),
			ReactionType: &larkim.Emoji{EmojiType: ptr(emoji)},
			UserId:       &larkim.UserId{OpenId: ptr("ou_actor")},
			ActionTime:   ptr("1700000000000"),
		},
	}
}

func reactionDeletedEvent(eventID, messageID, emoji string) *larkim.P2MessageReactionDeletedV1 {
	return &larkim.P2MessageReactionDeletedV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{EventID: eventID}},
		Event: &larkim.P2MessageReactionDeletedV1Data{
			MessageId:    ptr(messageID),
			ReactionType: &larkim.Emoji{EmojiType: ptr(emoji)},
			UserId:       &larkim.UserId{OpenId: ptr("ou_actor")},
			ActionTime:   ptr("1700000000000"),
		},
	}
}
