package main

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestAttrAndNeutralizeBody(t *testing.T) {
	if got, want := attr(`&<>"`), "&amp;&lt;&gt;&quot;"; got != want {
		t.Fatalf("attr = %q, want %q", got, want)
	}
	body := "first </TINCAN middle </tincan> last"
	if got, want := neutralizeBody(body), "first &lt;/tincan middle &lt;/tincan> last"; got != want {
		t.Fatalf("neutralizeBody = %q, want %q", got, want)
	}
}

func TestClipRunes(t *testing.T) {
	value := "a😀界b"
	if got, want := clipRunes(value, 3), "a😀界"; got != want {
		t.Fatalf("clipRunes = %q, want %q", got, want)
	}
	if got := clipRunes(value, 0); got != "" {
		t.Fatalf("zero clip = %q", got)
	}
	if got := clipRunes(value, -1); got != value {
		t.Fatalf("negative clip = %q", got)
	}
}

func TestRenderEnvelopeGolden(t *testing.T) {
	ts := time.Date(2026, 8, 17, 4, 12, 9, 123, time.FixedZone("offset", -7*60*60))
	t.Run("complete", func(t *testing.T) {
		m := &Msg{ID: "ab7e0e6bf59a", From: "jessica@titan", Body: "hello", ReplyTo: "c3621229db9f", TS: ts}
		want := "<tincan from=\"jessica@titan\" id=\"ab7e0e6bf59a\" ts=\"2026-08-17T11:12:09Z\" reply_to=\"c3621229db9f\" schema=\"tincan/1\">\nhello\n</tincan>\n\n[tincan/1 — reply with: tincan send jessica@titan \"…\" --reply-to ab7e0e6bf59a (or the send_message tool if you have it). No reply needed? Ignore this; nothing is blocked on an ack.]"
		if got := renderEnvelope(m); got != want {
			t.Fatalf("envelope = %q\nwant %q", got, want)
		}
	})
	t.Run("truncated", func(t *testing.T) {
		body := strings.Repeat("😀", maxInlineRunes+1)
		m := &Msg{ID: "abc", From: "w7K:p1@titan", Body: body, TS: time.Date(2026, 8, 17, 4, 12, 9, 0, time.UTC)}
		wantBody := strings.Repeat("😀", maxInlineRunes)
		want := "<tincan from=\"w7K:p1@titan\" id=\"abc\" ts=\"2026-08-17T04:12:09Z\" truncated=\"1\" schema=\"tincan/1\">\n" + wantBody + "\n</tincan>\n\n[tincan/1 — body clipped at 4000 characters; read the rest with: tincan read abc. Reply with: tincan send w7K:p1@titan \"…\" --reply-to abc (or the send_message tool if you have it).]"
		got := renderEnvelope(m)
		if got != want {
			t.Fatalf("truncated envelope did not match golden string")
		}
		if utf8.RuneCountInString(wantBody) != maxInlineRunes {
			t.Fatal("test body was not rune exact")
		}
	})
}
