package feishu

import "testing"

func TestMessageRefIsStableOpaqueAndAppScoped(t *testing.T) {
	t.Parallel()

	first := MessageRefForApp("support", "om_raw_message")
	if first == "" {
		t.Fatal("message ref is empty")
	}
	if first != MessageRefForApp("support", "om_raw_message") {
		t.Fatal("message ref is not deterministic")
	}
	if first == MessageRefForApp("other", "om_raw_message") {
		t.Fatal("message ref is not app-scoped")
	}
	if first == MessageRefForApp("support", "om_other_message") {
		t.Fatal("distinct messages share a ref")
	}
	if first[:7] != "msgref_" {
		t.Fatalf("message ref prefix = %q, want msgref_", first[:7])
	}
	if first == "om_raw_message" {
		t.Fatal("raw message id crossed the public boundary")
	}
}

func TestMessageRefRejectsBlankMessageID(t *testing.T) {
	t.Parallel()

	if got := MessageRefForApp("support", "  "); got != "" {
		t.Fatalf("blank message ref = %q, want empty", got)
	}
}
