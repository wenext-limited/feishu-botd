package service

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"feishu-botd/internal/config"
	"feishu-botd/internal/dedupe"
	"feishu-botd/internal/feishu"
	"feishu-botd/internal/ownership"
)

// fakePostBackend adds the proactive-post path to the CardKit and follow-up
// doubles. postErrs is consumed one entry per call.
type fakePostBackend struct {
	fakeFollowUpBackend

	postMu   sync.Mutex
	posts    []feishu.PostMessageRequest
	postErrs []error
	nextPost int
}

func newFakePostBackend() *fakePostBackend {
	return &fakePostBackend{fakeFollowUpBackend: fakeFollowUpBackend{fakeAgentBackend: *newFakeAgentBackend()}}
}

func (f *fakePostBackend) PostMessage(_ context.Context, req feishu.PostMessageRequest) (feishu.PostedMessage, error) {
	f.postMu.Lock()
	defer f.postMu.Unlock()
	f.posts = append(f.posts, req)
	if len(f.postErrs) > 0 {
		err := f.postErrs[0]
		f.postErrs = f.postErrs[1:]
		if err != nil {
			return feishu.PostedMessage{}, err
		}
	}
	f.nextPost++
	posted := feishu.PostedMessage{MessageIDs: []string{fmt.Sprintf("om_post_%d", f.nextPost)}}
	if req.ReplyInThread {
		posted.ThreadID = "omt_mirrored"
	}
	return posted, nil
}

func (f *fakePostBackend) postSnapshot() []feishu.PostMessageRequest {
	f.postMu.Lock()
	defer f.postMu.Unlock()
	return append([]feishu.PostMessageRequest(nil), f.posts...)
}

// commonsConfig is one app ("support") with two configured groups. commons is
// the Commons steward: ambient and proactive in refund-backend only, plus the
// ordinary agent grants an ambient delivery must still not unlock. ibot is an
// existing conversational provider with none of the new grants. observer is
// ambient-only.
func commonsConfig() config.Config {
	return config.Config{
		DedupeTTL:   time.Hour,
		SendTimeout: time.Second,
		ChannelRoutes: map[string]config.ChannelRoute{
			"refund-backend": {AppAlias: "support", ChatID: "oc_refund"},
			"ops":            {AppAlias: "support", ChatID: "oc_ops"},
		},
		Apps: map[string]config.AppConfig{
			"support": {
				AppID:     "app",
				AppSecret: "secret",
				Commands:  config.CommandConfig{Enabled: true, AllowUnconfiguredGroupChats: true},
				Channels:  map[string]string{"refund-backend": "oc_refund", "ops": "oc_ops"},
			},
		},
		AgentProviders: map[string]config.AgentProviderConfig{
			"commons": {
				AllowUnmatchedMessages: true, AllowAttachedContext: true, AllowFollowUpMessages: true, AllowImageUpload: true,
				AllowAmbientMessages: true, AmbientChatAliases: []string{"refund-backend"},
				AllowProactiveMessages: true, ProactiveChatAliases: []string{"refund-backend"},
			},
			"ibot":     {AllowUnmatchedMessages: true, AllowFollowUpMessages: true},
			"observer": {AllowAmbientMessages: true, AmbientChatAliases: []string{"refund-backend"}},
		},
	}
}

func commonsService(t *testing.T) (*Service, *fakePostBackend, *fakeOwnerStore) {
	t.Helper()
	backend := newFakePostBackend()
	svc := NewMultiAppService(commonsConfig(), map[string]feishu.Sender{"support": backend}, dedupe.NewMemoryStore(time.Hour), nil)
	owners := &fakeOwnerStore{owners: make(map[string]ownership.Owner)}
	svc.SetAgentOwnershipStore(owners)
	return svc, backend, owners
}

// humanGroupMessage is a group message as the receiver hands it over: the
// legacy Prompt plus the named rendering and people list.
func humanGroupMessage(chatAlias, chatID, deliveryID, messageID string) CommandInput {
	return CommandInput{
		AppAlias:       "support",
		DeliveryID:     deliveryID,
		Command:        "refund",
		Text:           "job is stuck, @_user_2 can you look",
		Prompt:         "refund job is stuck, @_user_2 can you look",
		NamedPrompt:    "refund job is stuck, @Alice can you look",
		Mentions:       []AgentMention{{UserID: "ou_alice", Name: "Alice"}},
		ConversationID: "conv_" + chatAlias,
		ChatAlias:      chatAlias,
		SenderID:       "ou_human",
		ChatID:         chatID,
		HumanSender:    true,
		Metadata:       map[string]string{"chat_type": "group", "message_type": "text", "message_id": messageID},
	}
}

func ambientMessage(deliveryID, messageID string) CommandInput {
	in := humanGroupMessage("refund-backend", "oc_refund", deliveryID, messageID)
	in.AmbientOnly = true
	return in
}

func TestAmbientDeliversUnaddressedHumanMessagesOnlyInGrantedChats(t *testing.T) {
	svc, _, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeUnmatchedMessages: true})

	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_ambient", "om_ambient"))
	event := receiveAgentEvent(t, commons)
	message := event.Message
	if message == nil || message.Addressing != AgentAddressingAmbient {
		t.Fatalf("ambient event = %#v", event)
	}
	if message.Text != "refund job is stuck, @Alice can you look" ||
		!reflect.DeepEqual(message.Mentions, []AgentMention{{UserID: "ou_alice", Name: "Alice"}}) {
		t.Fatalf("ambient content = %#v", message)
	}
	if message.MessageRef != feishu.MessageRefForApp("support", "om_ambient") || message.ReplyToMessageRef != "" {
		t.Fatalf("ambient refs = %#v", message)
	}
	if event.ChatAlias != "refund-backend" || event.SenderID != "ou_human" || event.DeliveryID != "delivery_ambient" {
		t.Fatalf("ambient envelope = %#v", event)
	}
	assertNoAgentEvent(t, ibot)

	ungranted := humanGroupMessage("ops", "oc_ops", "delivery_ops", "om_ops")
	ungranted.AmbientOnly = true
	mustDispatchAgentPrompt(t, svc, ungranted)
	assertNoAgentEvent(t, commons)
	assertNoAgentEvent(t, ibot)
}

func TestAmbientRequiresTheSubscriptionSelectorAndTheConfigGrant(t *testing.T) {
	svc, _, _ := commonsService(t)
	// Subscribed for ambient, but configuration grants ibot no ambient chat.
	ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeAmbientMessages: true})
	// Granted, but this stream did not ask for ambient events.
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})

	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_selector", "om_selector"))
	assertNoAgentEvent(t, ibot)
	assertNoAgentEvent(t, commons)
}

func TestAmbientNeverDeliversBotAuthoredMessages(t *testing.T) {
	svc, _, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

	in := ambientMessage("delivery_bot", "om_bot")
	in.HumanSender = false
	mustDispatchAgentPrompt(t, svc, in)

	// Even an addressed message is not observed ambiently when a bot wrote it.
	addressed := humanGroupMessage("refund-backend", "oc_refund", "delivery_bot_mention", "om_bot_mention")
	addressed.HumanSender = false
	observer := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "observer", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, addressed)
	assertNoAgentEvent(t, commons)
	assertNoAgentEvent(t, observer)
}

func TestAmbientDeliveryCarriesNoResponseCapability(t *testing.T) {
	svc, backend, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	mustDispatchAgentPrompt(t, svc, ambientMessage("delivery_observed", "om_observed"))
	event := receiveAgentEvent(t, commons)

	_, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "commons", DeliveryID: event.DeliveryID, OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "noted"},
	})
	if apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("start on an ambient delivery = %#v, want unknown_delivery", apiErr)
	}
	if _, apiErr := svc.GetAgentAttachedContext(context.Background(), AgentAttachedContextInput{
		Provider: "commons", DeliveryID: event.DeliveryID,
	}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("attached context on an ambient delivery = %#v, want unknown_delivery", apiErr)
	}
	if _, apiErr := svc.UploadAgentImage(context.Background(), AgentImageUploadInput{
		Provider: "commons", DeliveryID: event.DeliveryID, OperationID: "img-1", Data: pngBytes,
	}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("image upload on an ambient delivery = %#v, want unknown_delivery", apiErr)
	}
	if _, apiErr := sendFollowUp(t, svc, "commons", event.ConversationID, "fup-1", "unprompted"); apiErr == nil || apiErr.Code != "unknown_conversation" {
		t.Fatalf("follow-up into an ambient-only conversation = %#v, want unknown_conversation", apiErr)
	}
	if len(backend.createdCards) != 0 || len(backend.snapshot()) != 0 {
		t.Fatalf("an ambient delivery reached Feishu: cards=%d sends=%d", len(backend.createdCards), len(backend.snapshot()))
	}
}

func TestAddressedMessageIsDeliveredOnceWithTheAddressedVariant(t *testing.T) {
	svc, _, _ := commonsService(t)
	commonsPrompt := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	commonsAmbient := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	observer := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "observer", IncludeAmbientMessages: true})

	mustDispatchAgentPrompt(t, svc, humanGroupMessage("refund-backend", "oc_refund", "delivery_mention", "om_mention"))
	addressed := receiveAgentEvent(t, commonsPrompt)
	if addressed.Message.Addressing != AgentAddressingMention {
		t.Fatalf("commons variant = %v, want MENTION", addressed.Message.Addressing)
	}
	assertNoAgentEvent(t, commonsAmbient)
	assertNoAgentEvent(t, commonsPrompt)

	observed := receiveAgentEvent(t, observer)
	if observed.Message.Addressing != AgentAddressingAmbient || observed.DeliveryID != addressed.DeliveryID {
		t.Fatalf("observer variant = %#v", observed)
	}
	// The observer shares the delivery id but not the response capability.
	if _, apiErr := svc.StartAgentResponse(context.Background(), StartAgentResponseInput{
		Provider: "observer", DeliveryID: observed.DeliveryID, OperationID: "start-1",
		Content: AgentResponseContent{Markdown: "mine now"},
	}); apiErr == nil || apiErr.Code != "unknown_delivery" {
		t.Fatalf("observer start = %#v, want unknown_delivery", apiErr)
	}
}

func TestLegacyProviderKeepsLegacyTextAndNoMentionIdentities(t *testing.T) {
	svc, _, _ := commonsService(t)
	ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeUnmatchedMessages: true})
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

	in := humanGroupMessage("refund-backend", "oc_refund", "delivery_ibot", "om_ibot")
	mustDispatchAgentPrompt(t, svc, in)

	legacy := receiveAgentEvent(t, ibot)
	if legacy.Message.Text != in.Prompt || legacy.Message.Mentions != nil || legacy.Message.Command != "refund" {
		t.Fatalf("legacy provider content changed: %#v", legacy.Message)
	}
	if legacy.Message.Addressing != AgentAddressingMention || legacy.Message.MessageRef != feishu.MessageRefForApp("support", "om_ibot") {
		t.Fatalf("legacy provider additive fields = %#v", legacy.Message)
	}
	observed := receiveAgentEvent(t, commons)
	if observed.Message.Addressing != AgentAddressingAmbient || observed.Message.Text != in.NamedPrompt {
		t.Fatalf("commons observation = %#v", observed.Message)
	}
}

func TestAmbientStillObservesMessagesALegacyCommandConsumerHandled(t *testing.T) {
	svc, _, _ := commonsService(t)
	legacy, apiErr := svc.SubscribeCommands(context.Background(), "legacy", []string{"refund"})
	if apiErr != nil {
		t.Fatalf("subscribe legacy: %v", apiErr)
	}
	t.Cleanup(legacy.Close)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})

	mustDispatchAgentPrompt(t, svc, humanGroupMessage("refund-backend", "oc_refund", "delivery_legacy", "om_legacy"))
	select {
	case command := <-legacy.C:
		if command.Command != "refund" || command.NamedPrompt != "" || command.Mentions != nil {
			t.Fatalf("legacy command shape changed: %#v", command)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy consumer did not receive its command")
	}
	observed := receiveAgentEvent(t, commons)
	if observed.Message.Addressing != AgentAddressingAmbient {
		t.Fatalf("commons variant = %v, want AMBIENT once the legacy consumer owns the reply", observed.Message.Addressing)
	}
	assertNoAgentEvent(t, commons)
}

func TestAmbientRedeliveryIsNotRepeated(t *testing.T) {
	svc, _, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

	in := ambientMessage("delivery_retry", "om_retry")
	mustDispatchAgentPrompt(t, svc, in)
	_ = receiveAgentEvent(t, commons)
	mustDispatchAgentPrompt(t, svc, in)
	assertNoAgentEvent(t, commons)
}

func TestUnmentionedRepliesRouteByParentOwnership(t *testing.T) {
	t.Run("unowned parent becomes an observation", func(t *testing.T) {
		svc, _, _ := commonsService(t)
		commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})
		ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeUnmatchedMessages: true})

		in := humanGroupMessage("refund-backend", "oc_refund", "delivery_unowned", "om_unowned_reply")
		in.UnmentionedReply = true
		in.Metadata["parent_id"] = "om_human_parent"
		mustDispatchAgentPrompt(t, svc, in)

		event := receiveAgentEvent(t, commons)
		if event.Message.Addressing != AgentAddressingAmbient || event.Message.ReplyToMessageRef != "" {
			t.Fatalf("unowned reply = %#v", event.Message)
		}
		assertNoAgentEvent(t, ibot)
	})

	t.Run("owned parent stays a reply to its author", func(t *testing.T) {
		svc, _, owners := commonsService(t)
		parentRef := feishu.MessageRefForApp("support", "om_commons_post")
		owners.owners[parentRef] = ownership.Owner{Provider: "commons", ExpiresAt: time.Now().Add(time.Hour).Unix()}
		commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true, IncludeAmbientMessages: true})
		observer := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "observer", IncludeAmbientMessages: true})

		in := humanGroupMessage("refund-backend", "oc_refund", "delivery_owned", "om_owned_reply")
		in.UnmentionedReply = true
		in.Metadata["parent_id"] = "om_commons_post"
		mustDispatchAgentPrompt(t, svc, in)

		reply := receiveAgentEvent(t, commons)
		if reply.Message.Addressing != AgentAddressingReply || reply.Message.ReplyToMessageRef != parentRef {
			t.Fatalf("owned reply = %#v", reply.Message)
		}
		assertNoAgentEvent(t, commons)
		observed := receiveAgentEvent(t, observer)
		if observed.Message.Addressing != AgentAddressingAmbient || observed.Message.ReplyToMessageRef != "" {
			t.Fatalf("observer copy of an owned reply = %#v", observed.Message)
		}
	})

	t.Run("an ambient-only author still learns it was answered", func(t *testing.T) {
		svc, _, owners := commonsService(t)
		parentRef := feishu.MessageRefForApp("support", "om_commons_post")
		owners.owners[parentRef] = ownership.Owner{Provider: "commons", ExpiresAt: time.Now().Add(time.Hour).Unix()}
		commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

		in := humanGroupMessage("refund-backend", "oc_refund", "delivery_owned_ambient", "om_owned_ambient")
		in.UnmentionedReply = true
		in.Metadata["parent_id"] = "om_commons_post"
		mustDispatchAgentPrompt(t, svc, in)

		event := receiveAgentEvent(t, commons)
		if event.Message.Addressing != AgentAddressingAmbient || event.Message.ReplyToMessageRef != parentRef {
			t.Fatalf("ambient copy for the parent's author = %#v", event.Message)
		}
	})
}

func TestInboundMessageCarriesThreadRootForAReplyToAReply(t *testing.T) {
	svc, _, _ := commonsService(t)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

	// A human answers another human's reply, deep inside a thread opened on
	// the steward's mirrored root.
	in := humanGroupMessage("refund-backend", "oc_refund", "delivery_deep", "om_deep_reply")
	in.UnmentionedReply = true
	in.Metadata["parent_id"] = "om_human_reply"
	in.Metadata["root_id"] = "om_mirrored_root"
	in.Metadata["thread_id"] = "omt_mirror_thread"
	mustDispatchAgentPrompt(t, svc, in)

	message := receiveAgentEvent(t, commons).Message
	if message.ThreadRootMessageRef != feishu.MessageRefForApp("support", "om_mirrored_root") {
		t.Fatalf("thread root ref = %q", message.ThreadRootMessageRef)
	}
	if message.ThreadRef != feishu.ThreadRefForApp("support", "omt_mirror_thread") || message.ThreadRef == "" {
		t.Fatalf("thread ref = %q", message.ThreadRef)
	}
	if message.MessageRef != feishu.MessageRefForApp("support", "om_deep_reply") {
		t.Fatalf("message ref = %q", message.MessageRef)
	}
}

func TestAddressingReflectsWhyAMessageWasDelivered(t *testing.T) {
	svc, _, _ := commonsService(t)
	ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeUnmatchedMessages: true})

	direct := humanGroupMessage("direct", "oc_dm", "delivery_dm", "om_dm")
	direct.Metadata["chat_type"] = "p2p"
	direct.AmbientOnly = true // meaningless outside a group, and ignored
	mustDispatchAgentPrompt(t, svc, direct)
	event := receiveAgentEvent(t, ibot)
	if event.Message.Addressing != AgentAddressingDirect || event.Message.ThreadRef != "" || event.Message.ThreadRootMessageRef != "" {
		t.Fatalf("direct message = %#v", event.Message)
	}
}

func TestOverlongNamedPromptFallsBackToThePromptInsteadOfDroppingTheMessage(t *testing.T) {
	svc, _, _ := commonsService(t)
	ibot := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "ibot", IncludeUnmatchedMessages: true})
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

	in := humanGroupMessage("refund-backend", "oc_refund", "delivery_long", "om_long")
	in.Prompt = strings.Repeat("p", maxInboundPromptBytes-16)
	// Display names longer than their mention keys push only the named
	// rendering past the prompt limit.
	in.NamedPrompt = strings.Repeat("n", maxInboundPromptBytes+512)
	mustDispatchAgentPrompt(t, svc, in)

	if got := receiveAgentEvent(t, ibot).Message.Text; got != in.Prompt {
		t.Fatalf("legacy provider text length = %d, want the unchanged prompt", len(got))
	}
	if got := receiveAgentEvent(t, commons).Message.Text; got != in.Prompt {
		t.Fatalf("ambient provider text length = %d, want the prompt fallback", len(got))
	}
}

func TestProviderThatTookALegacyCommandGetsNoAmbientCopy(t *testing.T) {
	svc, _, _ := commonsService(t)
	legacy, apiErr := svc.SubscribeCommands(context.Background(), "commons", []string{"refund"})
	if apiErr != nil {
		t.Fatalf("subscribe legacy: %v", apiErr)
	}
	t.Cleanup(legacy.Close)
	commons := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})
	observer := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "observer", IncludeAmbientMessages: true})

	mustDispatchAgentPrompt(t, svc, humanGroupMessage("refund-backend", "oc_refund", "delivery_legacy_commons", "om_legacy_commons"))
	select {
	case <-legacy.C:
	case <-time.After(time.Second):
		t.Fatal("legacy stream did not receive the command")
	}
	assertNoAgentEvent(t, commons)
	if observed := receiveAgentEvent(t, observer); observed.Message.Addressing != AgentAddressingAmbient {
		t.Fatalf("observer = %#v", observed.Message)
	}
}

func TestAddressedThenRedeliveredMessageIsNotRepeatedAsAmbient(t *testing.T) {
	svc, _, _ := commonsService(t)
	commonsPrompt := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeUnmatchedMessages: true})
	commonsAmbient := mustSubscribeAgent(t, svc, AgentSubscribeOptions{Provider: "commons", IncludeAmbientMessages: true})

	in := humanGroupMessage("refund-backend", "oc_refund", "delivery_first_event", "om_redelivered")
	mustDispatchAgentPrompt(t, svc, in)
	_ = receiveAgentEvent(t, commonsPrompt)
	// The same message arrives again under another event id.
	in.DeliveryID = "delivery_second_event"
	mustDispatchAgentPrompt(t, svc, in)
	assertNoAgentEvent(t, commonsPrompt)
	assertNoAgentEvent(t, commonsAmbient)
}
