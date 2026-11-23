package feishu

import (
	"encoding/json"
	"sort"
	"strings"
)

// attachedCardContentType asks the history API for a card's raw content.
// Without it, a schema-2.0 / CardKit card (every agent answer botd streams)
// comes back as a fixed "please upgrade your client" stub with no text.
const attachedCardContentType = "raw_card_content"

// attachedCardUpgradeStub is the text Feishu substitutes for a card its
// simplified history rendering cannot show. It is never real content.
const attachedCardUpgradeStub = "请升级至最新版本客户端，以查看内容"

// parseAttachedCard flattens a received interactive card to readable text.
//
// With card_msg_content_type=raw_card_content, the history API returns
// `{"json_card":"<rendered card tree>","card_schema":2,…}`: a tree of
// {"tag","property"} nodes (markdown → plain_text / code_span / link / br,
// code_block, table, …). Older cards, or a response without the raw form,
// carry the simplified `{"title":…,"elements":[[{"tag","text"}…]]}` shape.
// It returns "" when nothing readable is present (images only, or the stub),
// and the caller falls back to the unsupported-message placeholder.
//
// Card content never yields image keys: card images are decoration whose
// bytes would spend the attached-image budget meant for user-shared images.
func parseAttachedCard(raw string) string {
	var envelope struct {
		JSONCard string          `json:"json_card"`
		Title    *string         `json:"title"`
		Elements [][]postElement `json:"elements"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return ""
	}
	if strings.TrimSpace(envelope.JSONCard) != "" {
		var card map[string]any
		if err := json.Unmarshal([]byte(envelope.JSONCard), &card); err != nil {
			return ""
		}
		parts := make([]string, 0, 2)
		for _, key := range []string{"header", "body"} {
			if text := renderCardNode(card[key]); text != "" {
				parts = append(parts, text)
			}
		}
		return normalizeCardText(strings.Join(parts, "\n"))
	}
	lines := make([]string, 0, len(envelope.Elements)+1)
	if envelope.Title != nil {
		if title := strings.TrimSpace(*envelope.Title); title != "" {
			lines = append(lines, title)
		}
	}
	for _, row := range envelope.Elements {
		parts := make([]string, 0, len(row))
		for _, element := range row {
			switch strings.TrimSpace(element.Tag) {
			case "at":
				parts = append(parts, "@participant")
			case "img", "image":
			default:
				if element.Text != attachedCardUpgradeStub {
					parts = append(parts, element.Text)
				}
			}
		}
		if line := strings.TrimSpace(strings.Join(parts, "")); line != "" {
			lines = append(lines, line)
		}
	}
	return normalizeCardText(strings.Join(lines, "\n"))
}

// cardInlineContainers lay their children out as one run of prose; every
// other container stacks its children as separate lines.
var cardInlineContainers = map[string]bool{"markdown": true, "div": true, "text": true, "lark_md": true}

func renderCardNode(value any) string {
	switch node := value.(type) {
	case []any:
		return joinCardBlocks(node)
	case map[string]any:
		tag, _ := node["tag"].(string)
		property, _ := node["property"].(map[string]any)
		switch tag {
		case "br":
			return "\n"
		case "at":
			// A mention's display name is a provider identity, not typed text.
			return "@participant"
		case "img", "image", "img_combination", "emoji", "hr":
			return ""
		case "code_block":
			return renderCardCodeBlock(property)
		case "table":
			return renderCardTable(property)
		}
		if property == nil {
			// Some nodes (a header title, a cell) carry text without the
			// tag/property wrapper.
			if content, ok := node["content"].(string); ok {
				return content
			}
			return ""
		}
		if content, ok := property["content"].(string); ok && content != "" {
			if tag == "code_span" {
				return "`" + content + "`"
			}
			return content
		}
		if elements, ok := property["elements"].([]any); ok {
			if cardInlineContainers[tag] {
				parts := make([]string, 0, len(elements))
				for _, element := range elements {
					text := renderCardNode(element)
					if child, ok := element.(map[string]any); ok && text != "" {
						if childTag, _ := child["tag"].(string); childTag == "code_block" || childTag == "table" {
							// Block content inside prose starts and ends its own lines.
							text = "\n" + text + "\n"
						}
					}
					parts = append(parts, text)
				}
				return strings.Join(parts, "")
			}
			return joinCardBlocks(elements)
		}
		// Unknown container: render every nested node it holds (columns,
		// a header's title/subtitle, a button's text, …) in a stable order.
		keys := make([]string, 0, len(property))
		for key := range property {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		blocks := make([]any, 0, len(keys))
		for _, key := range keys {
			switch property[key].(type) {
			case map[string]any, []any:
				blocks = append(blocks, property[key])
			}
		}
		return joinCardBlocks(blocks)
	}
	return ""
}

func joinCardBlocks(nodes []any) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if text := strings.TrimSpace(renderCardNode(node)); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func renderCardCodeBlock(property map[string]any) string {
	var builder strings.Builder
	lines, _ := property["contents"].([]any)
	for _, line := range lines {
		lineNode, _ := line.(map[string]any)
		runs, _ := lineNode["contents"].([]any)
		for _, run := range runs {
			runNode, _ := run.(map[string]any)
			if content, ok := runNode["content"].(string); ok {
				builder.WriteString(content)
			}
		}
	}
	code := strings.TrimRight(builder.String(), "\n")
	if strings.TrimSpace(code) == "" {
		return ""
	}
	return "```\n" + code + "\n```"
}

func renderCardTable(property map[string]any) string {
	columns, _ := property["columns"].([]any)
	names := make([]string, 0, len(columns))
	headers := make([]string, 0, len(columns))
	for _, column := range columns {
		columnNode, _ := column.(map[string]any)
		name, _ := columnNode["name"].(string)
		display, _ := columnNode["displayName"].(string)
		names = append(names, name)
		headers = append(headers, strings.TrimSpace(display))
	}
	lines := make([]string, 0)
	if strings.TrimSpace(strings.Join(headers, "")) != "" {
		lines = append(lines, strings.Join(headers, " | "))
	}
	rows, _ := property["rows"].([]any)
	for _, row := range rows {
		rowNode, _ := row.(map[string]any)
		cells := make([]string, 0, len(names))
		for _, name := range names {
			cell := rowNode[name]
			if cellNode, ok := cell.(map[string]any); ok {
				if data, ok := cellNode["data"]; ok {
					cell = data
				}
			}
			var text string
			switch value := cell.(type) {
			case string:
				text = value
			default:
				text = renderCardNode(value)
			}
			cells = append(cells, strings.Join(strings.Fields(text), " "))
		}
		if strings.TrimSpace(strings.Join(cells, "")) != "" {
			lines = append(lines, strings.Join(cells, " | "))
		}
	}
	return strings.Join(lines, "\n")
}

// normalizeCardText trims each line and collapses runs of blank lines, so
// `br` pairs and block separators do not pile up.
func normalizeCardText(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	blank := false
	inCode := false
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inCode = !inCode
		}
		if !inCode {
			line = strings.TrimRight(line, " \t")
		}
		if strings.TrimSpace(line) == "" && !inCode {
			if blank || len(out) == 0 {
				continue
			}
			blank = true
			out = append(out, "")
			continue
		}
		blank = false
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}
