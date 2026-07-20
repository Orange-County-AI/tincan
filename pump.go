package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// pump is the harness-agnostic delivery head: everything below the last hop
// (mailbox spool, claim -> ack, presence, outbox sweep) is shared with serve;
// only the injection mechanism differs. Where serve pushes MCP channel events
// into Claude Code, pump POSTs messages into another harness's live session:
//
//   tincan pump opencode   -> opencode serve's HTTP API (POST /session/:id/prompt_async)
//   tincan pump hermes     -> a hermes gateway webhook route (POST :8644/webhooks/<route>)
//
// Same contract as serve: claim -> inject -> ack, at-least-once, presence
// heartbeat so the mailbox shows as listening in list_peers. A message is
// acked only after the harness accepts it (2xx); failures leave it claimed
// and it is re-claimed next cycle.

// pumpSink injects a batch of claimed messages into a harness. It delivers a
// prefix of msgs and returns how many were durably handed off; the pump acks
// exactly those. (opencode batches all-or-nothing into one turn; hermes posts
// per-message and stops at the first failure.)
type pumpSink interface {
	describe() string
	deliver(msgs []Msg) (int, error)
}

func cmdPump(args []string) error {
	if len(args) < 1 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: tincan pump {opencode|hermes} [flags]  (see tincan help)")
	}
	kind := args[0]
	fs := flag.NewFlagSet("pump", flag.ContinueOnError)
	mailbox := fs.String("mailbox", "", "mailbox to drain (default TINCAN_MAILBOX)")
	url := fs.String("url", "", "opencode: server base URL (default http://127.0.0.1:4096); hermes: full webhook route URL (required)")
	session := fs.String("session", "", "opencode: session ID to inject into (default: auto-create one per mailbox and reuse it)")
	secret := fs.String("secret", "", "hermes: route secret for Generic V2 HMAC signing (default TINCAN_HERMES_SECRET; empty = unsigned)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	box := *mailbox
	if box == "" {
		box = mailboxName()
	}
	if box == "" {
		return fmt.Errorf("mailbox required: set TINCAN_MAILBOX or pass --mailbox")
	}
	if err := validName("mailbox", box); err != nil {
		return err
	}

	var sink pumpSink
	switch kind {
	case "opencode":
		base := *url
		if base == "" {
			base = "http://127.0.0.1:4096"
		}
		sink = &opencodeSink{
			base:     strings.TrimRight(base, "/"),
			session:  *session,
			box:      box,
			username: envDefault("OPENCODE_SERVER_USERNAME", "opencode"),
			password: os.Getenv("OPENCODE_SERVER_PASSWORD"),
		}
	case "hermes":
		if *url == "" {
			return fmt.Errorf("hermes: --url is required (the full webhook route URL, e.g. http://127.0.0.1:8644/webhooks/tincan)")
		}
		sec := *secret
		if sec == "" {
			sec = os.Getenv("TINCAN_HERMES_SECRET")
		}
		sink = &hermesSink{url: *url, secret: sec}
	default:
		return fmt.Errorf("unknown pump sink %q (want opencode or hermes)", kind)
	}
	return runPump(box, sink)
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// runPump is drainLoop with an HTTP sink in place of the channel push.
func runPump(box string, sink pumpSink) error {
	if err := ensureBox(box); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "tincan: pumping mailbox %q -> %s\n", box, sink.describe())
	since := time.Now().UTC()
	ticker := time.NewTicker(pollInterval())
	defer ticker.Stop()
	for {
		markPresence(box, since)
		go sweepAllOutboxes() // opportunistic cross-host retry; self rate-limited
		claims, err := claimPending(box)
		if err != nil {
			fmt.Fprintf(os.Stderr, "tincan: pump: %v\n", err)
		}
		if len(claims) > 0 {
			msgs := make([]Msg, len(claims))
			for i, c := range claims {
				msgs[i] = c.msg
			}
			n, err := sink.deliver(msgs)
			for i := 0; i < n && i < len(claims); i++ {
				claims[i].ack()
			}
			if err != nil {
				// Undelivered messages stay claimed and are re-claimed next
				// cycle (at-least-once, same as a serve crash mid-delivery).
				fmt.Fprintf(os.Stderr, "tincan: pump: delivered %d/%d: %v\n", n, len(claims), err)
			}
		}
		<-ticker.C
	}
}

// channelBlock renders a message in the same event format serve pushes into
// Claude Code, so agents see one shape regardless of harness. event_id stays
// the idempotency key for the rare at-least-once duplicate.
func channelBlock(m Msg) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<channel source="tincan" kind="message" from=%q event_id=%q queued_at=%q`,
		m.From, m.ID, m.QueuedAt.Format(time.RFC3339))
	if m.ReplyTo != "" {
		fmt.Fprintf(&b, ` reply_to=%q`, m.ReplyTo)
	}
	b.WriteString(">\n")
	b.WriteString(m.Body)
	b.WriteString("\n</channel>")
	return b.String()
}

// --- opencode ----------------------------------------------------------------

// opencodeSink injects into a live `opencode serve` session over its HTTP API.
// The whole claim batch becomes one user turn (one POST): a backlog of five
// messages should not cost five turns. prompt_async is used so the pump never
// blocks on the agent's turn; 2xx means opencode accepted the prompt.
type opencodeSink struct {
	base     string // e.g. http://127.0.0.1:4096
	session  string // explicit --session; empty = managed per-mailbox session
	box      string
	username string
	password string // OPENCODE_SERVER_PASSWORD; empty = no auth
	managed  bool   // session came from the state file (safe to reset on 404)
}

func (s *opencodeSink) describe() string {
	sess := s.session
	if sess == "" {
		sess = "auto"
	}
	return fmt.Sprintf("opencode %s (session %s)", s.base, sess)
}

// sessionFile persists the auto-created session ID next to the mailbox, so a
// pump restart re-enters the same conversation (identity = launch config).
func (s *opencodeSink) sessionFile() string {
	return filepath.Join(boxDir(s.box), "opencode-session.json")
}

func (s *opencodeSink) ensureSession() (string, error) {
	if s.session != "" {
		return s.session, nil
	}
	if data, err := os.ReadFile(s.sessionFile()); err == nil {
		var st struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(data, &st) == nil && st.ID != "" {
			s.session, s.managed = st.ID, true
			return s.session, nil
		}
	}
	body, _ := json.Marshal(map[string]any{"title": "tincan: " + s.box})
	resp, err := s.do("POST", "/session", body)
	if err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("create session: opencode returned %s", resp.Status)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil || created.ID == "" {
		return "", fmt.Errorf("create session: bad response: %v", err)
	}
	if err := writeJSON(s.sessionFile(), map[string]string{"id": created.ID}); err != nil {
		return "", err
	}
	s.session, s.managed = created.ID, true
	return s.session, nil
}

func (s *opencodeSink) do(method, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, s.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.password != "" {
		req.SetBasicAuth(s.username, s.password)
	}
	return http.DefaultClient.Do(req)
}

func (s *opencodeSink) deliver(msgs []Msg) (int, error) {
	sess, err := s.ensureSession()
	if err != nil {
		return 0, err
	}
	blocks := make([]string, len(msgs))
	for i, m := range msgs {
		blocks[i] = channelBlock(m)
	}
	body, _ := json.Marshal(map[string]any{
		"parts": []map[string]any{{"type": "text", "text": strings.Join(blocks, "\n\n")}},
	})
	resp, err := s.do("POST", "/session/"+sess+"/prompt_async", body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound && s.managed {
		// The managed session evaporated (opencode storage reset); forget it
		// and re-create on the next cycle.
		os.Remove(s.sessionFile())
		s.session, s.managed = "", false
		return 0, fmt.Errorf("session %s gone (404); will re-create", sess)
	}
	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("opencode returned %s", resp.Status)
	}
	return len(msgs), nil
}

// --- hermes ------------------------------------------------------------------

// hermesSink POSTs each message to a hermes gateway webhook route
// (platforms.webhook.extra.routes in the gateway's config.yaml). The route's
// prompt template turns the payload into an agent turn: fields of the Msg
// JSON are addressable as {from}, {body}, {id}, {reply_to}, {queued_at}.
// One POST per message — hermes processes one payload per webhook event.
type hermesSink struct {
	url    string // full route URL, e.g. http://127.0.0.1:8644/webhooks/tincan
	secret string // Generic V2 signing secret; empty = unsigned (INSECURE_NO_AUTH routes)
}

func (s *hermesSink) describe() string { return "hermes " + s.url }

func (s *hermesSink) deliver(msgs []Msg) (int, error) {
	for i, m := range msgs {
		body, err := json.Marshal(m)
		if err != nil {
			return i, err
		}
		req, err := http.NewRequest("POST", s.url, bytes.NewReader(body))
		if err != nil {
			return i, err
		}
		req.Header.Set("Content-Type", "application/json")
		if s.secret != "" {
			// Generic V2: HMAC-SHA256 hex of "<unix-ts>.<body>".
			ts := fmt.Sprintf("%d", time.Now().Unix())
			mac := hmac.New(sha256.New, []byte(s.secret))
			mac.Write([]byte(ts + "."))
			mac.Write(body)
			req.Header.Set("X-Webhook-Timestamp", ts)
			req.Header.Set("X-Webhook-Signature-V2", hex.EncodeToString(mac.Sum(nil)))
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return i, err
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return i, fmt.Errorf("hermes returned %s for message %s", resp.Status, m.ID)
		}
	}
	return len(msgs), nil
}
