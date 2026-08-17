package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testMsg(id, to string) *Msg {
	return &Msg{ID: id, From: "jessica@titan", To: to, Body: "hello", TS: time.Date(2026, 8, 17, 4, 0, 0, 0, time.UTC), TTLSeconds: 60}
}

func TestStore(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T)
	}{
		{"enqueue, claim, ack, and dedup", testStoreEnqueueClaimAckAndDedup},
		{"release backoff progression", testStoreReleaseBackoffProgression},
		{"expiry and dead letter", testStoreExpiryAndDeadLetter},
		{"per-recipient queue cap", testStoreQueueCap},
		{"orphan reclaim", testStoreReclaimOrphans},
		{"sender round trip", testStoreSenderRoundTrip},
	}
	for _, test := range cases {
		t.Run(test.name, test.run)
	}
}

func testStoreEnqueueClaimAckAndDedup(t *testing.T) {
	s := openTestStore(t)
	m := testMsg("abc123def456", "clem")
	if err := s.EnqueueLocal(m); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimLocal(queueKey("clem"), m.TS)
	if err != nil || claim == nil {
		t.Fatalf("ClaimLocal = %#v, %v", claim, err)
	}
	if err := s.EnqueueLocal(testMsg(m.ID, "clem")); err != nil {
		t.Fatal(err)
	}
	if got, _, err := s.Counts(); err != nil || got != 1 {
		t.Fatalf("queued after claimed dedup = %d, %v", got, err)
	}
	if err := s.Ack(claim); err != nil {
		t.Fatal(err)
	}
	if _, err := s.History(m.ID); err != nil {
		t.Fatal(err)
	}
	if got, _, err := s.Counts(); err != nil || got != 0 {
		t.Fatalf("queued after ack = %d, %v", got, err)
	}
}

func testStoreReleaseBackoffProgression(t *testing.T) {
	s := openTestStore(t)
	m := testMsg("backoff00001", "clem")
	if err := s.EnqueueLocal(m); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimLocal(queueKey(m.To), m.TS)
	if err != nil || claim == nil {
		t.Fatalf("initial claim = %#v, %v", claim, err)
	}
	for _, delay := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second} {
		if err := s.Release(claim, "temporary", 7); err != nil {
			t.Fatal(err)
		}
		if claim.Msg.Attempts <= 0 {
			t.Fatal("release did not increment attempts")
		}
		before := claim.Msg.LastAttempt.Add(delay - time.Nanosecond)
		if got, err := s.ClaimLocal(queueKey(m.To), before); err != nil || got != nil {
			t.Fatalf("claimed before %s backoff: %#v, %v", delay, got, err)
		}
		claim, err = s.ClaimLocal(queueKey(m.To), claim.Msg.LastAttempt.Add(delay))
		if err != nil || claim == nil {
			t.Fatalf("claim after %s backoff = %#v, %v", delay, claim, err)
		}
	}
}

func testStoreExpiryAndDeadLetter(t *testing.T) {
	s := openTestStore(t)
	m := testMsg("expired00001", "clem")
	m.TTLSeconds = 2
	if err := s.EnqueueLocal(m); err != nil {
		t.Fatal(err)
	}
	if s.Expired(m, m.TS.Add(time.Second)) {
		t.Fatal("message expired early")
	}
	if !s.Expired(m, m.TS.Add(2*time.Second)) {
		t.Fatal("message did not expire at TTL")
	}
	claim, err := s.ClaimLocal(queueKey(m.To), m.TS)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	if err := s.Kill(claim, "expired"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.Root(), "dead", m.ID+".json")); err != nil {
		t.Fatal(err)
	}
}

func testStoreQueueCap(t *testing.T) {
	s := openTestStore(t)
	for i := range 100 {
		m := testMsg("cap"+newID(), "clem")
		m.TS = m.TS.Add(time.Duration(i) * time.Second)
		if err := s.EnqueueLocal(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnqueueLocal(testMsg("cap-overflow", "clem")); codeOf(err) != "queue_full" {
		t.Fatalf("overflow code = %q (%v)", codeOf(err), err)
	}
}

func testStoreReclaimOrphans(t *testing.T) {
	s := openTestStore(t)
	m := testMsg("orphan000001", "clem")
	if err := s.EnqueueLocal(m); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimLocal(queueKey(m.To), m.TS)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	if err := s.ReclaimOrphans(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(claim.Path); !os.IsNotExist(err) {
		t.Fatalf("claim still exists: %v", err)
	}
	reclaimed, err := s.ClaimLocal(queueKey(m.To), m.TS)
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaimed message = %#v, %v", reclaimed, err)
	}
}

func testStoreSenderRoundTrip(t *testing.T) {
	s := openTestStore(t)
	if err := s.RecordSender("ticket500", "jessica@titan"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordSender("ticket500", "jessica@titan"); err != nil {
		t.Fatal(err)
	}
	known, err := s.KnownSender("ticket500", "jessica@titan")
	if err != nil || !known {
		t.Fatalf("KnownSender = %v, %v", known, err)
	}
	if got := s.KnownSenderCount("ticket500"); got != 1 {
		t.Fatalf("KnownSenderCount = %d", got)
	}
}

func TestListLocalPendingIncludesPendingAndClaimedNames(t *testing.T) {
	s := openTestStore(t)
	first := testMsg("pending00001", "clem")
	second := testMsg("pending00002", "clem")
	second.TS = second.TS.Add(time.Second)
	if err := s.EnqueueLocal(first); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueLocal(second); err != nil {
		t.Fatal(err)
	}
	if claim, err := s.ClaimLocal(queueKey("clem"), first.TS); err != nil || claim == nil {
		t.Fatalf("ClaimLocal = %#v, %v", claim, err)
	}
	messages, names, err := s.ListLocalPending(queueKey("clem"))
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || len(names) != 2 {
		t.Fatalf("ListLocalPending = %#v, %#v", messages, names)
	}
	if !strings.HasPrefix(names[0], "claimed-") || !strings.HasPrefix(names[1], "msg-") {
		t.Fatalf("names = %v, want claimed then pending filename", names)
	}
	if messages[0].ID != first.ID || messages[1].ID != second.ID {
		t.Fatalf("messages = %#v, want %q then %q", messages, first.ID, second.ID)
	}
}
