package feishu

import (
	"reflect"
	"testing"
)

func TestAnnouncementMarkdownRendersParagraphsBulletsAndInlineStyles(t *testing.T) {
	got := AnnouncementBlocksFromMarkdown("Status: **green**\nall `make check` passes\r\n\n" +
		"- see [the plan](https://example.com/plan?a=1&b=2)\n" +
		"* owner **[bob](https://example.com/u/bob)**\n\n" +
		"last line")
	want := []AnnouncementBlock{
		{Type: AnnouncementBlockText, Runs: []AnnouncementTextRun{
			{Content: "Status: "},
			{Content: "green", Bold: true},
			{Content: " all "},
			{Content: "make check", Code: true},
			{Content: " passes"},
		}},
		{Type: AnnouncementBlockBullet, Runs: []AnnouncementTextRun{
			{Content: "see "},
			{Content: "the plan", LinkURL: "https://example.com/plan?a=1&b=2"},
		}},
		{Type: AnnouncementBlockBullet, Runs: []AnnouncementTextRun{
			{Content: "owner "},
			{Content: "bob", Bold: true, LinkURL: "https://example.com/u/bob"},
		}},
		{Type: AnnouncementBlockText, Runs: []AnnouncementTextRun{{Content: "last line"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("blocks =\n%#v\nwant\n%#v", got, want)
	}
}

func TestAnnouncementMarkdownKeepsEverythingElseLiteral(t *testing.T) {
	for input, want := range map[string]string{
		"# not a heading":                  "# not a heading",
		"**unclosed bold":                  "**unclosed bold",
		"a ** b":                           "a ** b",
		"tick ` alone":                     "tick ` alone",
		"[no url]":                         "[no url]",
		"[js](javascript:alert(1))":        "[js](javascript:alert(1))",
		"[rel](/relative)":                 "[rel](/relative)",
		"1. ordered _stays_ ~~plain~~ > q": "1. ordered _stays_ ~~plain~~ > q",
		"-not a bullet":                    "-not a bullet",
	} {
		blocks := AnnouncementBlocksFromMarkdown(input)
		if len(blocks) != 1 || blocks[0].Type != AnnouncementBlockText || len(blocks[0].Runs) != 1 ||
			blocks[0].Runs[0] != (AnnouncementTextRun{Content: want}) {
			t.Errorf("%q rendered as %#v", input, blocks)
		}
	}
}

func TestAnnouncementMarkdownDropsEmptyBlocks(t *testing.T) {
	if blocks := AnnouncementBlocksFromMarkdown("\n\n  \n\t\n"); len(blocks) != 0 {
		t.Fatalf("blocks = %#v", blocks)
	}
	if blocks := AnnouncementBlocksFromMarkdown(""); len(blocks) != 0 {
		t.Fatalf("blocks = %#v", blocks)
	}
}
