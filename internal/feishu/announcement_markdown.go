package feishu

import (
	"net/url"
	"strings"
)

// AnnouncementBlocksFromMarkdown renders the small Markdown subset an
// announcement section accepts into docx blocks: blank-line-separated
// paragraphs become text blocks (lines inside one are joined by a space),
// lines starting with "- " or "* " become bullet blocks, and inside either
// **bold**, `code` and [label](http(s) url) are styled. Anything else,
// including an unmatched marker, is kept as literal text.
func AnnouncementBlocksFromMarkdown(markdown string) []AnnouncementBlock {
	markdown = strings.ReplaceAll(markdown, "\r\n", "\n")
	var blocks []AnnouncementBlock
	var paragraph []string
	flush := func() {
		if len(paragraph) == 0 {
			return
		}
		if runs := parseAnnouncementInline(strings.Join(paragraph, " ")); len(runs) > 0 {
			blocks = append(blocks, AnnouncementBlock{Type: AnnouncementBlockText, Runs: runs})
		}
		paragraph = nil
	}
	for _, line := range strings.Split(markdown, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
			flush()
		case strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "* "):
			flush()
			if runs := parseAnnouncementInline(strings.TrimSpace(trimmed[2:])); len(runs) > 0 {
				blocks = append(blocks, AnnouncementBlock{Type: AnnouncementBlockBullet, Runs: runs})
			}
		default:
			paragraph = append(paragraph, trimmed)
		}
	}
	flush()
	return blocks
}

// parseAnnouncementInline splits one line of text into styled runs.
func parseAnnouncementInline(text string) []AnnouncementTextRun {
	var runs []AnnouncementTextRun
	appendInlineRuns(&runs, text, false)
	return runs
}

func appendInlineRuns(runs *[]AnnouncementTextRun, text string, bold bool) {
	var literal strings.Builder
	emit := func(run AnnouncementTextRun) {
		if literal.Len() > 0 {
			addRun(runs, AnnouncementTextRun{Content: literal.String(), Bold: bold})
			literal.Reset()
		}
		addRun(runs, run)
	}
	for i := 0; i < len(text); {
		rest := text[i:]
		switch {
		case !bold && strings.HasPrefix(rest, "**"):
			if end := strings.Index(rest[2:], "**"); end > 0 {
				emit(AnnouncementTextRun{})
				appendInlineRuns(runs, rest[2:2+end], true)
				i += 2 + end + 2
				continue
			}
		case rest[0] == '`':
			if end := strings.IndexByte(rest[1:], '`'); end > 0 {
				emit(AnnouncementTextRun{Content: rest[1 : 1+end], Bold: bold, Code: true})
				i += 1 + end + 1
				continue
			}
		case rest[0] == '[':
			if label, target, n, ok := markdownLink(rest); ok {
				emit(AnnouncementTextRun{Content: label, Bold: bold, LinkURL: target})
				i += n
				continue
			}
		}
		literal.WriteByte(text[i])
		i++
	}
	if literal.Len() > 0 {
		addRun(runs, AnnouncementTextRun{Content: literal.String(), Bold: bold})
	}
}

// markdownLink parses "[label](url)" at the start of s. Only absolute http
// and https URLs are links; anything else stays literal.
func markdownLink(s string) (label, target string, n int, ok bool) {
	closeLabel := strings.Index(s, "](")
	if closeLabel <= 1 || strings.ContainsAny(s[1:closeLabel], "[]") {
		return "", "", 0, false
	}
	closeURL := strings.IndexByte(s[closeLabel+2:], ')')
	if closeURL <= 0 {
		return "", "", 0, false
	}
	target = strings.TrimSpace(s[closeLabel+2 : closeLabel+2+closeURL])
	parsed, err := url.Parse(target)
	if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" || strings.ContainsAny(target, " \t") {
		return "", "", 0, false
	}
	return s[1:closeLabel], target, closeLabel + 2 + closeURL + 1, true
}

// addRun appends run, merging it into the previous run when both are styled
// the same. Empty runs are dropped.
func addRun(runs *[]AnnouncementTextRun, run AnnouncementTextRun) {
	if run.Content == "" {
		return
	}
	if n := len(*runs); n > 0 {
		last := &(*runs)[n-1]
		if last.Bold == run.Bold && last.Code == run.Code && last.LinkURL == run.LinkURL {
			last.Content += run.Content
			return
		}
	}
	*runs = append(*runs, run)
}
