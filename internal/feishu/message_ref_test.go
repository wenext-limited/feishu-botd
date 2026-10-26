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

func TestThreadRefIsOpaqueAppScopedAndDistinctFromMessageRefs(t *testing.T) {
	t.Parallel()

	ref := ThreadRefForApp("support", "omt_thread")
	if ref == "" || ref != ThreadRefForApp("support", "omt_thread") {
		t.Fatalf("thread ref is empty or unstable: %q", ref)
	}
	if ref == ThreadRefForApp("other", "omt_thread") {
		t.Fatal("thread ref is not app-scoped")
	}
	if ref == MessageRefForApp("support", "omt_thread") {
		t.Fatal("thread ref shares the message-ref domain")
	}
	if ref[:10] != "threadref_" {
		t.Fatalf("thread ref prefix = %q", ref[:10])
	}
	if ThreadRefForApp("support", " ") != "" {
		t.Fatal("blank thread id produced a ref")
	}
}

func TestThreadRootMessageRefOnlyForRepliesInsideAThread(t *testing.T) {
	t.Parallel()

	want := MessageRefForApp("support", "om_root")
	if got := ThreadRootMessageRefForApp("support", "om_reply_to_reply", "omt_thread", "om_root"); got != want {
		t.Fatalf("thread root ref = %q, want %q", got, want)
	}
	for name, got := range map[string]string{
		"not in a thread":    ThreadRootMessageRefForApp("support", "om_reply", "", "om_root"),
		"is the root itself": ThreadRootMessageRefForApp("support", "om_root", "omt_thread", "om_root"),
		"topic root":         ThreadRootMessageRefForApp("support", "om_root", "omt_thread", ""),
	} {
		if got != "" {
			t.Fatalf("%s: thread root ref = %q, want empty", name, got)
		}
	}
}
