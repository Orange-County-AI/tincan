package main

// Pump tests: flag -> sink wiring, and drainOnce's ordered ack-the-delivered-
// prefix contract (the piece pump shares with serve). Sink internals are
// covered in sink_test.go.

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// fakeSink records deliveries and fails on the Nth call.
type fakeSink struct {
	delivered []string // event_ids in delivery order
	failAt    int      // 1-based call number to fail on; 0 = never
}

func (f *fakeSink) deliver(content string, meta map[string]string) error {
	if f.failAt > 0 && len(f.delivered)+1 == f.failAt {
		return errors.New("sink down")
	}
	f.delivered = append(f.delivered, meta["event_id"])
	return nil
}

func TestDrainOnceAcksDeliveredPrefixOnly(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	q := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	for i, id := range []string{"aaa111", "bbb222", "ccc333"} {
		msg := &Msg{ID: id, From: "fifty", To: "jessica", Body: "m" + id, QueuedAt: q.Add(time.Duration(i) * time.Second)}
		if err := enqueueMsg(msg); err != nil {
			t.Fatal(err)
		}
	}
	f := &fakeSink{failAt: 2} // second message fails -> break, keep order
	drainOnce("jessica", f)
	if len(f.delivered) != 1 || f.delivered[0] != "aaa111" {
		t.Fatalf("delivered = %v, want [aaa111]", f.delivered)
	}
	// First acked; the failed message and everything behind it retained.
	files, _ := filepath.Glob(filepath.Join(queueDir("jessica"), "*.json"))
	if len(files) != 2 {
		t.Fatalf("want 2 retained messages, got %v", files)
	}
	// Next cycle re-claims the tail in queue order.
	f2 := &fakeSink{}
	drainOnce("jessica", f2)
	if len(f2.delivered) != 2 || f2.delivered[0] != "bbb222" || f2.delivered[1] != "ccc333" {
		t.Fatalf("retry delivered = %v, want [bbb222 ccc333]", f2.delivered)
	}
	if files, _ := filepath.Glob(filepath.Join(queueDir("jessica"), "*.json")); len(files) != 0 {
		t.Fatalf("queue not empty after retry: %v", files)
	}
}

func TestPumpSetupOpencodeDefaultsTitleToMailbox(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	t.Setenv("CHANNEL_SINK", "")
	t.Setenv("OPENCODE_SESSION_TITLE", "")
	box, dlv, err := pumpSetup([]string{"opencode", "--mailbox", "clem"})
	if err != nil {
		t.Fatal(err)
	}
	if box != "clem" {
		t.Fatalf("box = %q", box)
	}
	oc, ok := dlv.(*opencodeSink)
	if !ok {
		t.Fatalf("sink is %T, want *opencodeSink", dlv)
	}
	if oc.title != "tincan: clem" {
		t.Fatalf("title = %q, want \"tincan: clem\"", oc.title)
	}
	if oc.base != "http://127.0.0.1:4096" {
		t.Fatalf("base = %q", oc.base)
	}
}

func TestPumpSetupHermesRequiresURL(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	t.Setenv("HERMES_WEBHOOK_URL", "")
	if _, _, err := pumpSetup([]string{"hermes", "--mailbox", "clem"}); err == nil {
		t.Fatal("expected error without a webhook URL")
	}
	box, dlv, err := pumpSetup([]string{"hermes", "--mailbox", "clem", "--url", "http://127.0.0.1:8644/webhooks/tincan", "--secret", "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := dlv.(*hermesSink)
	if !ok || box != "clem" {
		t.Fatalf("got (%q, %T)", box, dlv)
	}
	if h.url != "http://127.0.0.1:8644/webhooks/tincan" || h.secret != "s3cret" {
		t.Fatalf("sink = %+v", h)
	}
}

func TestPumpSetupRejectsUnknownKindAndMissingMailbox(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	t.Setenv("TINCAN_MAILBOX", "")
	if _, _, err := pumpSetup([]string{"codex", "--mailbox", "clem"}); err == nil {
		t.Fatal("expected error for unknown sink kind")
	}
	if _, _, err := pumpSetup([]string{"opencode"}); err == nil {
		t.Fatal("expected error without a mailbox")
	}
}
