package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSplitAddr(t *testing.T) {
	cases := []struct {
		in, mailbox, host string
	}{
		{"clem", "clem", ""},
		{"clem@gigachad", "clem", "gigachad"},
		{"clem@host.example.com", "clem", "host.example.com"},
	}
	for _, c := range cases {
		mb, host := splitAddr(c.in)
		if mb != c.mailbox || host != c.host {
			t.Errorf("splitAddr(%q) = (%q, %q), want (%q, %q)", c.in, mb, host, c.mailbox, c.host)
		}
	}
}

func TestAddrRe(t *testing.T) {
	valid := []string{"clem", "a", "a-1", "clem@gigachad", "cli@host-2", "x@a.b.c"}
	invalid := []string{"", "Clem", "clem@", "@gigachad", "-x", "clem@Gigachad", "a b", "clem@@host"}
	for _, a := range valid {
		if !addrRe.MatchString(a) {
			t.Errorf("addrRe should match %q", a)
		}
	}
	for _, a := range invalid {
		if addrRe.MatchString(a) {
			t.Errorf("addrRe should not match %q", a)
		}
	}
}

func TestEnqueueMsgDedup(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	msg := &Msg{ID: "abc123", From: "clem@citadel", To: "jessica", Body: "hi", QueuedAt: time.Now().UTC()}
	if err := enqueueMsg(msg); err != nil {
		t.Fatalf("first enqueueMsg: %v", err)
	}
	// Same ID again (redelivery): silent no-op.
	dup := &Msg{ID: "abc123", From: "clem@citadel", To: "jessica", Body: "hi", QueuedAt: msg.QueuedAt}
	if err := enqueueMsg(dup); err != nil {
		t.Fatalf("duplicate enqueueMsg: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(queueDir("jessica"), "msg-*.json"))
	if len(files) != 1 {
		t.Fatalf("want 1 queued file after duplicate delivery, got %d", len(files))
	}
	// A different ID queues a second message.
	if err := enqueueMsg(&Msg{ID: "def456", From: "clem@citadel", To: "jessica", Body: "hi again"}); err != nil {
		t.Fatalf("second enqueueMsg: %v", err)
	}
	files, _ = filepath.Glob(filepath.Join(queueDir("jessica"), "msg-*.json"))
	if len(files) != 2 {
		t.Fatalf("want 2 queued files, got %d", len(files))
	}
}

func TestSweepOutboxOrdering(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	orig := remoteDeliver
	defer func() { remoteDeliver = orig }()

	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	for i, id := range []string{"id1", "id2", "id3"} {
		msg := &Msg{ID: id, From: "clem@citadel", To: "jessica", Body: "m" + id, QueuedAt: base.Add(time.Duration(i) * time.Second)}
		if err := spoolOutbox("gigachad", msg); err != nil {
			t.Fatalf("spoolOutbox: %v", err)
		}
	}

	// First sweep: delivery fails at the second message; the sweep must stop
	// there (FIFO preserved) with one sent, two remaining.
	var delivered []string
	remoteDeliver = func(host string, msg *Msg) error {
		if msg.ID == "id2" {
			return fmt.Errorf("boom")
		}
		delivered = append(delivered, msg.ID)
		return nil
	}
	sent, remaining, err := sweepOutbox("gigachad")
	if sent != 1 || remaining != 2 || err == nil {
		t.Fatalf("sweep 1: got sent=%d remaining=%d err=%v, want 1, 2, non-nil", sent, remaining, err)
	}
	if len(delivered) != 1 || delivered[0] != "id1" {
		t.Fatalf("sweep 1 delivered %v, want [id1]", delivered)
	}

	// Second sweep: everything goes through, oldest first.
	remoteDeliver = func(host string, msg *Msg) error {
		delivered = append(delivered, msg.ID)
		return nil
	}
	sent, remaining, err = sweepOutbox("gigachad")
	if sent != 2 || remaining != 0 || err != nil {
		t.Fatalf("sweep 2: got sent=%d remaining=%d err=%v, want 2, 0, nil", sent, remaining, err)
	}
	want := []string{"id1", "id2", "id3"}
	for i, id := range want {
		if delivered[i] != id {
			t.Fatalf("delivery order %v, want %v", delivered, want)
		}
	}
	if outboxPending("gigachad") != 0 {
		t.Fatalf("outbox not drained: %d pending", outboxPending("gigachad"))
	}
	// The stamp file must not count as pending and must exist after a sweep.
	if _, err := os.Stat(attemptStampPath("gigachad")); err != nil {
		t.Fatalf("missing .last-attempt stamp: %v", err)
	}
}

func TestSendToSpoolsWhenUnreachable(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	t.Setenv("TINCAN_HOST", "citadel")
	orig := remoteDeliver
	defer func() { remoteDeliver = orig }()
	remoteDeliver = func(host string, msg *Msg) error { return fmt.Errorf("ssh %s: connection refused", host) }

	msg, status, err := sendTo("jessica@gigachad", "clem", "hello", "")
	if err != nil {
		t.Fatalf("sendTo: %v", err)
	}
	if msg.From != "clem@citadel" {
		t.Errorf("From not host-qualified: %q", msg.From)
	}
	if msg.To != "jessica" {
		t.Errorf("To should be the bare mailbox: %q", msg.To)
	}
	if outboxPending("gigachad") != 1 {
		t.Errorf("message not spooled to outbox")
	}
	if status == "" || err != nil {
		t.Errorf("want honest queued status, got %q, %v", status, err)
	}
}
