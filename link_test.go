package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type linkTestHost struct {
	host   string
	roster []RosterAgent
	accept func(context.Context, string, MsgFrame) error

	mu       sync.Mutex
	accepted []MsgFrame
}

func (h *linkTestHost) LocalHost() string { return h.host }
func (h *linkTestHost) LocalRoster(context.Context) ([]RosterAgent, error) {
	return append([]RosterAgent(nil), h.roster...), nil
}
func (h *linkTestHost) AcceptInbound(ctx context.Context, from string, f MsgFrame) error {
	h.mu.Lock()
	h.accepted = append(h.accepted, f)
	h.mu.Unlock()
	if h.accept != nil {
		return h.accept(ctx, from, f)
	}
	return nil
}
func (h *linkTestHost) Logf(string, ...any) {}
func (h *linkTestHost) Accepted() []MsgFrame {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]MsgFrame(nil), h.accepted...)
}

func TestLinkFrameCodec(t *testing.T) {
	frames := []MsgFrame{
		{T: "hello", Host: "a", Proto: linkProto, Ver: "v"},
		{T: "hello_ok", Host: "b", Proto: linkProto, Ver: "v"},
		{T: "msg", ID: "abc", From: "amy@a", To: "bob", ReplyTo: "old", Body: "hi", TS: "2026-08-17T00:00:00Z", TTLSeconds: 9},
		{T: "ack", ID: "abc"}, {T: "nak", ID: "abc", Code: "queue_full", Detail: "full"},
		{T: "roster_req", RID: "r1"},
		{T: "roster", RID: "r1", Host: "b", Agents: []RosterAgent{{Addr: "bob@b", Name: "bob", Host: "b"}}},
		{T: "ping", N: 7}, {T: "pong", N: 7},
	}
	for _, want := range frames {
		t.Run(want.T, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeFrame(&buf, want); err != nil {
				t.Fatal(err)
			}
			got, err := newFrameReader(&buf).next()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("frame = %#v, want %#v", got, want)
			}
		})
	}

	t.Run("oversize is rejected", func(t *testing.T) {
		data := []byte(`{"t":"msg","body":"` + strings.Repeat("x", maxLinkFrame) + `"}` + "\n")
		if _, err := newFrameReader(bytes.NewReader(data)).next(); err == nil {
			t.Fatal("oversize frame was accepted")
		}
	})
}

func TestInboundHelloRejectsMismatch(t *testing.T) {
	cases := []struct {
		name  string
		hello MsgFrame
		code  string
	}{
		{"protocol", MsgFrame{T: "hello", Host: "peer", Proto: linkProto + 1}, "unsupported_proto"},
		{"host", MsgFrame{T: "hello", Host: "local", Proto: linkProto}, "host_mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t, "local", &linkTestHost{host: "local"})
			client, server := net.Pipe()
			done := make(chan struct{})
			go func() { m.ServeInbound(context.Background(), server); close(done) }()
			if err := writeFrame(client, tc.hello); err != nil {
				t.Fatal(err)
			}
			got, err := newFrameReader(client).next()
			if err != nil {
				t.Fatal(err)
			}
			if got.T != "nak" || got.Code != tc.code {
				t.Fatalf("reply = %#v", got)
			}
			_ = client.Close()
			waitDone(t, done)
		})
	}
}

func TestHelloOKValidation(t *testing.T) {
	cases := []struct {
		name  string
		frame MsgFrame
		want  string
	}{
		{"protocol", MsgFrame{T: "hello_ok", Host: "peer", Proto: linkProto + 1}, "unsupported_proto"},
		{"host", MsgFrame{T: "hello_ok", Host: "other", Proto: linkProto}, "host_mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyHelloOK("peer", tc.frame)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("verifyHelloOK error = %v, want %q", err, tc.want)
			}
		})
	}
}
func TestInboundMessageAcknowledgement(t *testing.T) {
	t.Run("ack", func(t *testing.T) {
		h := &linkTestHost{host: "b"}
		m := newTestManager(t, "b", h)
		client, done := connectInbound(t, m, "a")
		defer closeLinkForTest(client, done)
		if err := writeFrame(client, MsgFrame{T: "msg", ID: "m1", From: "amy@a", To: "bob", Body: "hello"}); err != nil {
			t.Fatal(err)
		}
		got, err := newFrameReader(client).next()
		if err != nil {
			t.Fatal(err)
		}
		if got.T != "ack" || got.ID != "m1" {
			t.Fatalf("reply = %#v", got)
		}
		if got := h.Accepted(); len(got) != 1 {
			t.Fatalf("AcceptInbound calls = %d, want 1", len(got))
		}
	})
	t.Run("coded error becomes nak", func(t *testing.T) {
		h := &linkTestHost{host: "b", accept: func(context.Context, string, MsgFrame) error { return codedErrorf("queue_full", "full") }}
		m := newTestManager(t, "b", h)
		client, done := connectInbound(t, m, "a")
		defer closeLinkForTest(client, done)
		if err := writeFrame(client, MsgFrame{T: "msg", ID: "m2", From: "amy@a", To: "bob"}); err != nil {
			t.Fatal(err)
		}
		got, err := newFrameReader(client).next()
		if err != nil {
			t.Fatal(err)
		}
		if got.T != "nak" || got.Code != "queue_full" {
			t.Fatalf("reply = %#v", got)
		}
	})
}

func TestRosterRequest(t *testing.T) {
	want := []RosterAgent{{Addr: "bob@b", Name: "bob", PaneID: "w1:p2", Host: "b"}}
	m := newTestManager(t, "b", &linkTestHost{host: "b", roster: want})
	client, done := connectInbound(t, m, "a")
	defer closeLinkForTest(client, done)
	if err := writeFrame(client, MsgFrame{T: "roster_req", RID: "req"}); err != nil {
		t.Fatal(err)
	}
	got, err := newFrameReader(client).next()
	if err != nil {
		t.Fatal(err)
	}
	if got.T != "roster" || got.RID != "req" || got.Host != "b" || !reflect.DeepEqual(got.Agents, want) {
		t.Fatalf("roster = %#v", got)
	}
}

func TestOutboxNakHandling(t *testing.T) {
	cases := []struct {
		name, code string
		dead       bool
	}{
		{"permanent", "not_permitted", true},
		{"transient", "queue_full", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stA := newTestStore(t)
			hB := &linkTestHost{host: "b", accept: func(context.Context, string, MsgFrame) error { return codedErrorf(tc.code, "%s", tc.code) }}
			mA := newLinkManager(&Config{Host: "a"}, stA, &linkTestHost{host: "a"})
			mB := newLinkManager(&Config{Host: "b"}, newTestStore(t), hB)
			client, doneB := connectManagers(t, mA, mB, "a", "b")
			defer func() { mA.Close(); mB.Close(); closeLinkForTest(client, doneB) }()
			msg := &Msg{ID: "nakcase", From: "sender@a", To: "target", Host: "b", Body: "body", TS: time.Now(), TTLSeconds: 60}
			if err := stA.EnqueueOutbox(msg); err != nil {
				t.Fatal(err)
			}
			mA.Notify()
			if tc.dead {
				waitFor(t, func() bool { _, err := os.Stat(filepath.Join(stA.Root(), "dead", msg.ID+".json")); return err == nil })
				claim, err := stA.ClaimLocal(queueKey("sender"), time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if claim == nil {
					t.Fatal("bounce was not enqueued")
				}
			} else {
				var queued Msg
				waitFor(t, func() bool {
					paths, err := filepath.Glob(filepath.Join(stA.Root(), "outbox", "b", "msg-*.json"))
					if err != nil || len(paths) != 1 {
						return false
					}
					data, err := os.ReadFile(paths[0])
					if err != nil {
						return false
					}
					queued = Msg{}
					if err := json.Unmarshal(data, &queued); err != nil {
						return false
					}
					return queued.Attempts >= 1
				})
				if queued.Attempts != 1 {
					t.Fatalf("attempts = %d, want 1", queued.Attempts)
				}
				if queued.LastError != tc.code {
					t.Fatalf("last_error = %q, want %q", queued.LastError, tc.code)
				}
			}
		})
	}
}

func TestReversePathOverInboundLink(t *testing.T) {
	stA, stB := newTestStore(t), newTestStore(t)
	hA := &linkTestHost{host: "a"}
	hB := &linkTestHost{host: "b"}
	mA := newLinkManager(&Config{Host: "a", Peers: []Peer{{Host: "b", SSH: "b"}}}, stA, hA)
	// b intentionally has no peer configuration: it can only use the inbound
	// stream from a and therefore cannot enumerate a through dialable peers.
	cfgB := &Config{Host: "b"}
	mB := newLinkManager(cfgB, stB, hB)
	dB := newDaemon(cfgB, stB, nil)
	dB.links = mB
	client, doneB := connectManagers(t, mA, mB, "a", "b")
	defer func() { mA.Close(); mB.Close(); closeLinkForTest(client, doneB) }()

	first := &Msg{ID: "a-to-b", From: "amy@a", To: "bob", Host: "b", Body: "first", TS: time.Now(), TTLSeconds: 60}
	if err := stA.EnqueueOutbox(first); err != nil {
		t.Fatal(err)
	}
	mA.Notify()
	waitFor(t, func() bool { return len(hB.Accepted()) == 1 })
	if err := stB.RecordSender("a", "amy@a"); err != nil {
		t.Fatal(err)
	}
	if known, err := stB.KnownSender("a", "amy@a"); err != nil || !known {
		t.Fatalf("known sender = %t, %v", known, err)
	}
	if len(mB.cfg.Peers) != 0 {
		t.Fatal("inbound-only host unexpectedly has a dialable peer")
	}
	if _, up := mB.Route("a"); !up {
		t.Fatal("inbound route was not retained")
	}

	reply := dB.handleSend(context.Background(), map[string]any{
		"to": "amy@a", "body": "reply", "from": "bob",
	})
	if ok, _ := reply["ok"].(bool); !ok {
		t.Fatalf("inbound-link reply was rejected: %#v", reply)
	}
	waitFor(t, func() bool { return len(hA.Accepted()) == 1 })
	if hA.Accepted()[0].Body != "reply" {
		t.Fatalf("reverse body = %q", hA.Accepted()[0].Body)
	}

	unsolicited := dB.handleSend(context.Background(), map[string]any{
		"to": "other@a", "body": "unsolicited", "from": "bob",
	})
	if got, _ := unsolicited["code"].(string); got != "not_permitted" {
		t.Fatalf("unsolicited send result = %#v, want not_permitted", unsolicited)
	}
}

// A peer reached over an ssh alias derives its own host from os.Hostname (on the
// real workspace box, "workspace-0"), which is not the name its parent addresses
// it by. The dialer therefore names it in hello.you, and every address that peer
// puts on the link must carry the adopted name instead of its local one.
func TestInboundLinkAdoptsDialerSuppliedName(t *testing.T) {
	st := newTestStore(t)
	host := &linkTestHost{host: "workspace-0", roster: []RosterAgent{
		{Addr: "w9:p1@workspace-0", PaneID: "w9:p1", Kind: "omp", Status: "idle", Host: "workspace-0"},
	}}
	m := newLinkManager(&Config{Host: "workspace-0"}, st, host)
	defer m.Close()

	client, server := net.Pipe()
	done := make(chan struct{})
	go func() { m.ServeInbound(context.Background(), server); close(done) }()
	defer func() { _ = client.Close(); <-done }()

	if err := writeFrame(client, MsgFrame{T: "hello", Host: "titan", You: "ticket500", Proto: linkProto}); err != nil {
		t.Fatal(err)
	}
	reader := newFrameReader(client)
	hello, err := reader.next()
	if err != nil {
		t.Fatal(err)
	}
	if hello.T != "hello_ok" || hello.Host != "ticket500" {
		t.Fatalf("hello_ok = %#v, want host ticket500", hello)
	}

	if err := writeFrame(client, MsgFrame{T: "roster_req", RID: "r1"}); err != nil {
		t.Fatal(err)
	}
	roster, err := reader.next()
	if err != nil {
		t.Fatal(err)
	}
	if roster.Host != "ticket500" || len(roster.Agents) != 1 || roster.Agents[0].Addr != "w9:p1@ticket500" {
		t.Fatalf("roster = %#v, want ticket500-qualified addresses", roster)
	}

	// A message this side sends back must be attributed to the adopted name, or
	// the parent rejects it as a sender/host mismatch.
	out := &Msg{ID: "childreply", From: "bob@workspace-0", To: "amy", Host: "titan", Body: "reply", TS: time.Now(), TTLSeconds: 60}
	if err := st.EnqueueOutbox(out); err != nil {
		t.Fatal(err)
	}
	m.Notify()
	frame, err := reader.next()
	if err != nil {
		t.Fatal(err)
	}
	if frame.T != "msg" || frame.From != "bob@ticket500" {
		t.Fatalf("outbound frame = %#v, want from bob@ticket500", frame)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return st
}
func newTestManager(t *testing.T, host string, h linkHost) *linkManager {
	t.Helper()
	return newLinkManager(&Config{Host: host}, newTestStore(t), h)
}
func connectInbound(t *testing.T, m *linkManager, remote string) (net.Conn, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() { m.ServeInbound(context.Background(), server); close(done) }()
	if err := writeFrame(client, MsgFrame{T: "hello", Host: remote, Proto: linkProto}); err != nil {
		t.Fatal(err)
	}
	got, err := newFrameReader(client).next()
	if err != nil {
		t.Fatal(err)
	}
	if got.T != "hello_ok" {
		t.Fatalf("hello reply = %#v", got)
	}
	return client, done
}
func connectManagers(t *testing.T, a, b *linkManager, ahost, bhost string) (net.Conn, <-chan struct{}) {
	t.Helper()
	client, done := connectInbound(t, b, ahost)
	reader := newFrameReader(client)
	// connectInbound already consumes hello_ok, so use a fresh socket reader only
	// after this point. net.Pipe has no buffered bytes until a starts sending.
	s := a.newSession(client, bhost, "outbound", ahost)
	go a.runEstablished(context.Background(), s, reader)
	waitFor(t, func() bool { _, up := a.Route(bhost); return up })
	return client, done
}
func closeLinkForTest(conn net.Conn, done <-chan struct{}) { _ = conn.Close(); <-done }
func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("link did not stop")
	}
}
func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met")
}

// connectInboundNamed establishes an inbound link that names the accepting side,
// mirroring what a real dialer sends in hello.you.
func connectInboundNamed(t *testing.T, m *linkManager, remote, adopted string) (net.Conn, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() { m.ServeInbound(context.Background(), server); close(done) }()
	if err := writeFrame(client, MsgFrame{T: "hello", Host: remote, You: adopted, Proto: linkProto}); err != nil {
		t.Fatal(err)
	}
	got, err := newFrameReader(client).next()
	if err != nil {
		t.Fatal(err)
	}
	if got.T != "hello_ok" || got.Host != adopted {
		t.Fatalf("hello reply = %#v, want hello_ok as %s", got, adopted)
	}
	waitFor(t, func() bool { _, up := m.Route(remote); return up })
	return client, done
}

// newNopStream is a link stream that never yields a frame, for tests that only
// need an installed session's identity.
func newNopStream() io.ReadWriteCloser { return nopStream{} }

type nopStream struct{}

func (nopStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (nopStream) Write(p []byte) (int, error) { return len(p), nil }
func (nopStream) Close() error                { return nil }
