package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// pumpOnce mirrors one runPump cycle: claim, deliver, ack the delivered prefix.
func pumpOnce(t *testing.T, box string, sink pumpSink) (delivered int, deliverErr error) {
	t.Helper()
	claims, err := claimPending(box)
	if err != nil {
		t.Fatalf("claimPending: %v", err)
	}
	msgs := make([]Msg, len(claims))
	for i, c := range claims {
		msgs[i] = c.msg
	}
	n, err := sink.deliver(msgs)
	for i := 0; i < n && i < len(claims); i++ {
		claims[i].ack()
	}
	return n, err
}

func queueFiles(t *testing.T, box string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(queueDir(box), "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestOpencodeSinkBatchesIntoOneTurn(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	var gotPath, gotText string
	var posts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		gotPath = r.URL.Path
		var body struct {
			Parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"parts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Parts) != 1 {
			t.Errorf("bad prompt body: %v (parts=%d)", err, len(body.Parts))
		} else {
			gotText = body.Parts[0].Text
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	for _, m := range []*Msg{
		{ID: "aaa111", From: "fifty", To: "clem", Body: "first", QueuedAt: q},
		{ID: "bbb222", From: "stub@citadel", To: "clem", Body: "second", ReplyTo: "aaa111", QueuedAt: q.Add(time.Second)},
	} {
		if err := enqueueMsg(m); err != nil {
			t.Fatal(err)
		}
	}

	sink := &opencodeSink{base: srv.URL, session: "ses_test", box: "clem"}
	n, err := pumpOnce(t, "clem", sink)
	if err != nil || n != 2 {
		t.Fatalf("deliver = (%d, %v), want (2, nil)", n, err)
	}
	if posts != 1 {
		t.Fatalf("want the whole backlog batched into 1 POST, got %d", posts)
	}
	if gotPath != "/session/ses_test/prompt_async" {
		t.Errorf("posted to %q", gotPath)
	}
	// Same event format serve pushes, ordered by queue time, one blank line apart.
	wantFirst := "<channel source=\"tincan\" kind=\"message\" from=\"fifty\" event_id=\"aaa111\" queued_at=\"2026-07-19T12:00:00Z\">\nfirst\n</channel>"
	if !strings.HasPrefix(gotText, wantFirst) {
		t.Errorf("first block:\n%s\nwant prefix:\n%s", gotText, wantFirst)
	}
	if !strings.Contains(gotText, `reply_to="aaa111"`) || strings.Index(gotText, "aaa111") > strings.Index(gotText, "bbb222") {
		t.Errorf("second block missing reply_to or out of order:\n%s", gotText)
	}
	if files := queueFiles(t, "clem"); len(files) != 0 {
		t.Errorf("queue not empty after ack: %v", files)
	}
}

func TestOpencodeSinkAutoCreatesAndPersistsSession(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	var created, prompted int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/session":
			created++
			user, pass, ok := r.BasicAuth()
			if !ok || user != "opencode" || pass != "hunter2" {
				t.Errorf("basic auth = (%q, %q, %v)", user, pass, ok)
			}
			json.NewEncoder(w).Encode(map[string]string{"id": "ses_new"})
		case strings.HasSuffix(r.URL.Path, "/prompt_async"):
			prompted++
			if r.URL.Path != "/session/ses_new/prompt_async" {
				t.Errorf("prompted %q", r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	if err := enqueueMsg(&Msg{ID: "ccc333", From: "cli", To: "clem", Body: "hi"}); err != nil {
		t.Fatal(err)
	}
	sink := &opencodeSink{base: srv.URL, box: "clem", username: "opencode", password: "hunter2"}
	if n, err := pumpOnce(t, "clem", sink); err != nil || n != 1 {
		t.Fatalf("deliver = (%d, %v)", n, err)
	}
	if created != 1 || prompted != 1 {
		t.Fatalf("created=%d prompted=%d", created, prompted)
	}
	// The session ID is persisted next to the mailbox and reused on restart.
	fresh := &opencodeSink{base: srv.URL, box: "clem", username: "opencode", password: "hunter2"}
	if err := enqueueMsg(&Msg{ID: "ddd444", From: "cli", To: "clem", Body: "again"}); err != nil {
		t.Fatal(err)
	}
	if n, err := pumpOnce(t, "clem", fresh); err != nil || n != 1 {
		t.Fatalf("second deliver = (%d, %v)", n, err)
	}
	if created != 1 {
		t.Errorf("session re-created on restart: created=%d", created)
	}
}

func TestOpencodeSinkForgetsGoneManagedSession(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	if err := ensureBox("clem"); err != nil {
		t.Fatal(err)
	}
	sink := &opencodeSink{base: srv.URL, box: "clem"}
	if err := writeJSON(sink.sessionFile(), map[string]string{"id": "ses_stale"}); err != nil {
		t.Fatal(err)
	}
	if err := enqueueMsg(&Msg{ID: "eee555", From: "cli", To: "clem", Body: "hi"}); err != nil {
		t.Fatal(err)
	}
	n, err := pumpOnce(t, "clem", sink)
	if n != 0 || err == nil {
		t.Fatalf("deliver = (%d, %v), want (0, error)", n, err)
	}
	if _, statErr := os.Stat(sink.sessionFile()); !os.IsNotExist(statErr) {
		t.Errorf("stale session file not removed")
	}
	// Message stayed claimed: still present, re-claimed by the next cycle.
	if files := queueFiles(t, "clem"); len(files) != 1 {
		t.Errorf("want 1 retained message, got %v", files)
	}
}

func TestHermesSinkSignsAndStopsAtFirstFailure(t *testing.T) {
	t.Setenv("TINCAN_DATA_DIR", t.TempDir())
	const secret = "route-secret"
	var posts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		body, _ := io.ReadAll(r.Body)
		ts := r.Header.Get("X-Webhook-Timestamp")
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(ts + "."))
		mac.Write(body)
		if want := hex.EncodeToString(mac.Sum(nil)); r.Header.Get("X-Webhook-Signature-V2") != want {
			t.Errorf("bad signature for body %s", body)
		}
		var m Msg
		if err := json.Unmarshal(body, &m); err != nil || m.From == "" || m.Body == "" {
			t.Errorf("payload is not a Msg: %s", body)
		}
		if posts == 2 {
			w.WriteHeader(http.StatusBadGateway) // second message fails
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	for _, m := range []*Msg{
		{ID: "fff666", From: "fifty", To: "jessica", Body: "one", QueuedAt: q},
		{ID: "ggg777", From: "fifty", To: "jessica", Body: "two", QueuedAt: q.Add(time.Second)},
	} {
		if err := enqueueMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	sink := &hermesSink{url: srv.URL + "/webhooks/tincan", secret: secret}
	n, err := pumpOnce(t, "jessica", sink)
	if n != 1 || err == nil {
		t.Fatalf("deliver = (%d, %v), want (1, error)", n, err)
	}
	// First acked, second retained for the next cycle.
	if files := queueFiles(t, "jessica"); len(files) != 1 {
		t.Errorf("want 1 retained message, got %v", files)
	}
	claims, err := claimPending("jessica")
	if err != nil || len(claims) != 1 || claims[0].msg.ID != "ggg777" {
		t.Fatalf("re-claim = (%v, %v), want the failed message back", claims, err)
	}
}
