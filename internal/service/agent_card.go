package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"feishu-botd/internal/notify"
)

// Card JSON 2.0 element shapes. Every field order here is the wire order, so a
// response that carries no timeline serializes byte-for-byte as it did before
// the panel existed.
type cardText struct {
	Tag     string `json:"tag"`
	Content string `json:"content"`
}

type cardCallbackBehavior struct {
	Type  string         `json:"type"`
	Value map[string]any `json:"value"`
}

type cardElement struct {
	Tag       string                 `json:"tag"`
	ElementID string                 `json:"element_id,omitempty"`
	Content   string                 `json:"content,omitempty"`
	Text      *cardText              `json:"text,omitempty"`
	Type      string                 `json:"type,omitempty"`
	Behaviors []cardCallbackBehavior `json:"behaviors,omitempty"`
}

// cardPanelIcon is the disclosure chevron. icon_expanded_angle rotates it when
// the panel opens, which is what makes the header read as a control.
type cardPanelIcon struct {
	Tag   string `json:"tag"`
	Token string `json:"token"`
	Size  string `json:"size"`
}

type cardPanelHeader struct {
	Title             cardText      `json:"title"`
	VerticalAlign     string        `json:"vertical_align"`
	Icon              cardPanelIcon `json:"icon"`
	IconPosition      string        `json:"icon_position"`
	IconExpandedAngle int           `json:"icon_expanded_angle"`
}

type cardPanelBorder struct {
	Color        string `json:"color"`
	CornerRadius string `json:"corner_radius"`
}

// cardCollapsiblePanel is the Feishu collapsible_panel container. It holds the
// run timeline so the step log never displaces the answer: collapsed, the
// header alone reports where the run is.
type cardCollapsiblePanel struct {
	Tag             string          `json:"tag"`
	ElementID       string          `json:"element_id"`
	Expanded        bool            `json:"expanded"`
	Header          cardPanelHeader `json:"header"`
	Border          cardPanelBorder `json:"border"`
	VerticalSpacing string          `json:"vertical_spacing"`
	Padding         string          `json:"padding"`
	Elements        []cardElement   `json:"elements"`
}

// agentTimelineState is the timeline half of a response's card state. A panel
// is created once, by Start, and present never changes afterwards.
type agentTimelineState struct {
	present  bool
	markdown string
	title    string
}

// agentCard is one built CardKit entity plus the state the response handle has
// to remember to keep applying partial updates to it.
type agentCard struct {
	json          string
	actionDigests map[string]string
	timeline      agentTimelineState
}

func agentTimelineHeader(title string) cardPanelHeader {
	return cardPanelHeader{
		// plain_text, not markdown: a step line is arbitrary agent-authored text
		// and must not be able to render as headings or emphasis in the header.
		Title:         cardText{Tag: "plain_text", Content: title},
		VerticalAlign: "center",
		Icon: cardPanelIcon{
			Tag:   "standard_icon",
			Token: "down-small-ccm_outlined",
			Size:  "16px 16px",
		},
		IconPosition:      "right",
		IconExpandedAngle: -180,
	}
}

func agentTimelinePanel(timeline agentTimelineState) cardCollapsiblePanel {
	return cardCollapsiblePanel{
		Tag:       "collapsible_panel",
		ElementID: agentTimelinePanelElementID,
		// Collapsed by default. The header carries the current step, so the
		// answer stays the tallest thing on the card while the run streams.
		Expanded:        false,
		Header:          agentTimelineHeader(timeline.title),
		Border:          cardPanelBorder{Color: "grey", CornerRadius: "5px"},
		VerticalSpacing: "8px",
		Padding:         "8px 8px 8px 8px",
		Elements: []cardElement{{
			Tag:       "markdown",
			ElementID: agentTimelineElementID,
			Content:   timeline.markdown,
		}},
	}
}

// agentTimelineHeaderPatch is a batch_update action list that replaces the
// panel header. The streaming content endpoint only writes text elements, so
// the collapsed header line is moved by a partial element update instead.
// partial_element replaces the whole header object rather than reaching into
// header.title, so the patch never depends on Feishu's nested merge rules.
func agentTimelineHeaderPatch(title string) string {
	actions := []map[string]any{{
		"action": "partial_update_element",
		"params": map[string]any{
			"element_id":      agentTimelinePanelElementID,
			"partial_element": map[string]any{"header": agentTimelineHeader(title)},
		},
	}}
	data, _ := json.Marshal(actions)
	return string(data)
}

// agentTerminalReplacementPatch updates every visible terminal field in one
// CardKit batch operation. Unlike the streaming content endpoint, batch
// element replacement remains valid after streaming mode has been disabled.
func agentTerminalReplacementPatch(markdown string, hasTimeline bool, timeline agentTimelineParts) string {
	actions := []map[string]any{{
		"action": "partial_update_element",
		"params": map[string]any{
			"element_id":      agentContentElementID,
			"partial_element": map[string]any{"content": markdown},
		},
	}}
	if hasTimeline && timeline.Markdown != "" {
		actions = append(actions, map[string]any{
			"action": "partial_update_element",
			"params": map[string]any{
				"element_id":      agentTimelineElementID,
				"partial_element": map[string]any{"content": timeline.Markdown},
			},
		})
	}
	if hasTimeline && timeline.Title != "" {
		actions = append(actions, map[string]any{
			"action": "partial_update_element",
			"params": map[string]any{
				"element_id":      agentTimelinePanelElementID,
				"partial_element": map[string]any{"header": agentTimelineHeader(timeline.Title)},
			},
		})
	}
	data, _ := json.Marshal(actions)
	return string(data)
}

func agentFinishSettings(summary string) string {
	config := map[string]any{"streaming_mode": false}
	if summary != "" {
		config["summary"] = map[string]any{"content": summary}
	}
	data, _ := json.Marshal(map[string]any{"config": config})
	return string(data)
}

func cardButtonStyle(style AgentResponseActionStyle) string {
	switch style {
	case AgentResponseActionStylePrimary:
		return "primary"
	case AgentResponseActionStyleDanger:
		return "danger"
	default:
		return "default"
	}
}

func buildAgentCard(content AgentResponseContent) (agentCard, *notify.APIError) {
	content.Title = strings.TrimSpace(content.Title)
	content.Markdown = strings.TrimSpace(content.Markdown)
	content.TimelineMarkdown = strings.TrimSpace(content.TimelineMarkdown)
	content.TimelineTitle = strings.TrimSpace(content.TimelineTitle)
	if len(content.Title) > maxAgentTitleBytes || len(content.TimelineTitle) > maxAgentTitleBytes ||
		len(content.Actions) > 8 {
		return agentCard{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	// Feishu's size ceiling is per card, so the answer and the timeline share
	// the daemon's budget rather than each getting their own.
	if len(content.Markdown)+len(content.TimelineMarkdown) > maxAgentCardBytes {
		return agentCard{}, notify.BadRequest("field_too_large", "one or more fields are too large")
	}
	if content.Markdown == "" {
		content.Markdown = agentPlaceholderMarkdown
	}

	timeline := agentTimelineState{
		present: content.TimelineMarkdown != "" || content.TimelineTitle != "",
	}
	elements := make([]any, 0, 2+len(content.Actions))
	if timeline.present {
		timeline.markdown = normalizedInitialMarkdown(content.TimelineMarkdown)
		timeline.title = normalizedInitialMarkdown(content.TimelineTitle)
		// The panel leads the card: collapsed, its header is the progress line
		// that belongs above the answer it is producing.
		elements = append(elements, agentTimelinePanel(timeline))
	}
	elements = append(elements, cardElement{
		Tag: "markdown", ElementID: agentContentElementID, Content: content.Markdown,
	})

	actionDigests, apiErr := appendAgentActions(&elements, content.Actions)
	if apiErr != nil {
		return agentCard{}, apiErr
	}

	cardConfig := map[string]any{
		"streaming_mode": true,
		"update_multi":   true,
	}
	if content.Title != "" {
		cardConfig["summary"] = map[string]any{"content": content.Title}
	}
	card := map[string]any{
		"schema": "2.0",
		"config": cardConfig,
		"body":   map[string]any{"elements": elements},
	}
	if content.Title != "" {
		card["header"] = map[string]any{"title": map[string]any{"tag": "plain_text", "content": content.Title}}
	}
	data, err := json.Marshal(card)
	if err != nil {
		return agentCard{}, notify.NewAPIError(500, "internal", "could not encode agent card", false)
	}
	if len(data) > maxAgentCardBytes {
		return agentCard{}, notify.BadRequest("field_too_large", "agent card exceeds the daemon size limit")
	}
	return agentCard{json: string(data), actionDigests: actionDigests, timeline: timeline}, nil
}

func appendAgentActions(elements *[]any, actions []AgentResponseAction) (map[string]string, *notify.APIError) {
	actionDigests := make(map[string]string, len(actions))
	seen := make(map[string]struct{}, len(actions))
	for index, action := range actions {
		action.ActionID = strings.TrimSpace(action.ActionID)
		action.Label = strings.TrimSpace(action.Label)
		if action.ActionID == "" || action.Label == "" {
			return nil, notify.BadRequest("invalid_action", "action_id and label are required")
		}
		if len(action.ActionID) > 64 || len(action.Label) > 100 || len(action.PayloadJSON) > 8*1024 {
			return nil, notify.BadRequest("field_too_large", "one or more fields are too large")
		}
		if _, duplicate := seen[action.ActionID]; duplicate {
			return nil, notify.BadRequest("duplicate_action", "action ids must be unique")
		}
		switch action.Style {
		case AgentResponseActionStyleUnspecified, AgentResponseActionStyleDefault,
			AgentResponseActionStylePrimary, AgentResponseActionStyleDanger:
		default:
			return nil, notify.BadRequest("invalid_action_style", "action style is not supported")
		}
		seen[action.ActionID] = struct{}{}
		payloadJSON := strings.TrimSpace(action.PayloadJSON)
		if payloadJSON == "" {
			payloadJSON = "{}"
		}
		var apiErr *notify.APIError
		payloadJSON, apiErr = normalizeJSONObjectRaw(payloadJSON, "invalid_action_payload")
		if apiErr != nil {
			return nil, apiErr
		}
		actionDigests[action.ActionID] = actionPayloadDigest(payloadJSON)
		// The pinned Feishu SDK decodes callback values through interface{},
		// which would round large JSON integers to float64. Carry the provider
		// payload as a JSON string across Feishu and reconstruct it losslessly
		// when normalizing the callback for the provider.
		value := map[string]any{"action_id": action.ActionID, "payload_json": payloadJSON}
		label := cardText{Tag: "plain_text", Content: action.Label}
		*elements = append(*elements, cardElement{
			Tag:       "button",
			ElementID: fmt.Sprintf("agent_action_%d", index+1),
			Text:      &label,
			Type:      cardButtonStyle(action.Style),
			Behaviors: []cardCallbackBehavior{{Type: "callback", Value: value}},
		})
	}
	return actionDigests, nil
}
