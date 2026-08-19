package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordedPrompt struct {
	target string
	text   string
}

type recordedKeys struct {
	paneID string
	keys   []string
}

type fakeDaemonHerdr struct {
	mu            sync.Mutex
	agents        []herdrAgent
	prompts       []recordedPrompt
	promptErr     error
	paneScreens   map[string]string
	paneScreenErr error
	sentKeys      []recordedKeys
	sendKeysErr   error
	notifications []recordedNotification
	notifyErr     error
}

func (f *fakeDaemonHerdr) Ping(context.Context) (string, int, error) { return "test", 19, nil }

func (f *fakeDaemonHerdr) ListAgents(context.Context) ([]herdrAgent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]herdrAgent(nil), f.agents...), nil
}

func (f *fakeDaemonHerdr) GetAgent(_ context.Context, target string) (*herdrAgent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, agent := range f.agents {
		if agent.Name == target || agent.PaneID == target {
			copy := agent
			return &copy, nil
		}
	}
	return nil, nil
}

func (f *fakeDaemonHerdr) PaneScreen(_ context.Context, paneID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.paneScreenErr != nil {
		return "", f.paneScreenErr
	}
	screen, found := f.paneScreens[paneID]
	if !found {
		return "", errors.New("pane screen not configured")
	}
	return screen, nil
}

func (f *fakeDaemonHerdr) SendKeys(_ context.Context, paneID string, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentKeys = append(f.sentKeys, recordedKeys{paneID: paneID, keys: append([]string(nil), keys...)})
	return f.sendKeysErr
}

func (f *fakeDaemonHerdr) Prompt(_ context.Context, target, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prompts = append(f.prompts, recordedPrompt{target: target, text: text})
	return f.promptErr
}

func (f *fakeDaemonHerdr) Rename(_ context.Context, target, name string) (*herdrAgent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.agents {
		if f.agents[i].PaneID == target || f.agents[i].Name == target {
			f.agents[i].Name = name
			copy := f.agents[i]
			return &copy, nil
		}
	}
	return nil, codedErrorf("agent_not_found", "agent %q was not found", target)
}

type recordedNotification struct {
	title string
	body  string
}

func (f *fakeDaemonHerdr) Notify(_ context.Context, title, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notifications = append(f.notifications, recordedNotification{title: title, body: body})
	return f.notifyErr
}

func (f *fakeDaemonHerdr) setAgents(agents ...herdrAgent) {
	f.mu.Lock()
	f.agents = append([]herdrAgent(nil), agents...)
	f.mu.Unlock()
}

func (f *fakeDaemonHerdr) promptsSnapshot() []recordedPrompt {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedPrompt(nil), f.prompts...)
}

func (f *fakeDaemonHerdr) notificationsSnapshot() []recordedNotification {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedNotification(nil), f.notifications...)
}

func newTestDaemon(t *testing.T, cfg *Config, agents ...herdrAgent) (*daemon, *fakeDaemonHerdr) {
	t.Helper()
	if cfg == nil {
		cfg = &Config{Host: "titan", DeliverWhen: "now", TTLSeconds: 86400}
	}
	root := t.TempDir()
	t.Setenv("TINCAN_DATA_DIR", root)
	st, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	herdr := &fakeDaemonHerdr{agents: agents, paneScreens: make(map[string]string)}
	d := newDaemon(cfg, st, herdr)
	if err := d.refreshRoster(context.Background()); err != nil {
		t.Fatal(err)
	}
	return d, herdr
}

func sendForTest(t *testing.T, d *daemon, req map[string]any) string {
	t.Helper()
	res := d.handleSend(context.Background(), req)
	if ok, _ := res["ok"].(bool); !ok {
		t.Fatalf("send failed: %#v", res)
	}
	return res["id"].(string)
}

func TestDaemonSendsAndArchivesRenderedEnvelope(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"})
	id := sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})
	d.dispatch(context.Background(), time.Now())

	prompts := herdr.promptsSnapshot()
	if len(prompts) != 1 || prompts[0].target != "clem" {
		t.Fatalf("prompts = %#v", prompts)
	}
	history, err := d.store.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := prompts[0].text, renderEnvelope(history); got != want {
		t.Fatalf("prompt envelope = %q, want %q", got, want)
	}
	queued, _, err := d.store.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("queued = %d, want 0", queued)
	}
}
func TestDaemonDraftHoldPreservesQueueAndResumes(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "claude", Status: "idle"})
	herdr.paneScreens["w7K:p2"] = readScreenFixture(t, "claude-draft.txt")
	id := sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})

	dir := d.store.queueDir(queueKey("clem"))
	names, err := pendingNames(dir)
	if err != nil || len(names) != 1 {
		t.Fatalf("queued names before hold = %v, %v", names, err)
	}
	before, err := readMsg(filepath.Join(dir, names[0]))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	d.dispatch(context.Background(), now)

	if prompts := herdr.promptsSnapshot(); len(prompts) != 0 {
		t.Fatalf("prompts while draft is present = %#v", prompts)
	}
	names, err = pendingNames(dir)
	if err != nil || len(names) != 1 {
		t.Fatalf("queued names after hold = %v, %v", names, err)
	}
	after, err := readMsg(filepath.Join(dir, names[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("held queue message changed: got %#v, want %#v", after, before)
	}
	if hold := d.draftHoldStatus(); len(hold) != 1 || hold[0].PaneID != "w7K:p2" {
		t.Fatalf("draft holds = %#v", hold)
	}

	herdr.paneScreens["w7K:p2"] = readScreenFixture(t, "claude-empty.txt")
	d.dispatch(context.Background(), now)
	if prompts := herdr.promptsSnapshot(); len(prompts) != 1 || prompts[0].target != "clem" {
		t.Fatalf("prompts after composer clears = %#v", prompts)
	}
	if _, err := d.store.History(id); err != nil {
		t.Fatalf("resumed delivery was not archived: %v", err)
	}
	if hold := d.draftHoldStatus(); len(hold) != 0 {
		t.Fatalf("draft holds after release = %#v", hold)
	}
}

func TestDaemonDraftGuardCanBeDisabled(t *testing.T) {
	t.Setenv("TINCAN_DRAFT_GUARD", "false")
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "claude", Status: "idle"})
	herdr.paneScreens["w7K:p2"] = readScreenFixture(t, "claude-draft.txt")
	id := sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})
	d.dispatch(context.Background(), time.Now())

	if prompts := herdr.promptsSnapshot(); len(prompts) != 1 {
		t.Fatalf("prompts with draft guard disabled = %#v", prompts)
	}
	if _, err := d.store.History(id); err != nil {
		t.Fatalf("disabled guard delivery was not archived: %v", err)
	}
}

func TestDaemonUnreadablePaneScreenFailsOpen(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "claude", Status: "idle"})
	herdr.paneScreenErr = errors.New("pane read failed")
	id := sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})
	d.dispatch(context.Background(), time.Now())

	if prompts := herdr.promptsSnapshot(); len(prompts) != 1 {
		t.Fatalf("prompts after unreadable pane = %#v", prompts)
	}
	if _, err := d.store.History(id); err != nil {
		t.Fatalf("fail-open delivery was not archived: %v", err)
	}
}

func TestDaemonResolvesPaneIdentityAndIgnoresFrom(t *testing.T) {
	d, herdr := newTestDaemon(t, nil,
		herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"},
		herdrAgent{PaneID: "w7K:p1", Kind: "omp", Status: "idle"},
	)
	sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "forged", "pane_id": "w7K:p1"})
	d.dispatch(context.Background(), time.Now())
	prompts := herdr.promptsSnapshot()
	if len(prompts) != 1 {
		t.Fatalf("prompts = %#v", prompts)
	}
	if want := `from="w7K:p1@titan"`; !contains(prompts[0].text, want) {
		t.Fatalf("prompt %q does not contain %q", prompts[0].text, want)
	}
}

func TestDaemonDefersUnavailableTargets(t *testing.T) {
	cases := []struct {
		name        string
		cfg         *Config
		agent       herdrAgent
		update      herdrAgent
		lastAttempt time.Duration
	}{
		{
			name: "launch pending", agent: herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle", LaunchPending: true},
			update: herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"}, lastAttempt: 3 * time.Second,
		},
		{
			name: "settled working", cfg: &Config{Host: "titan", DeliverWhen: "settled", TTLSeconds: 86400},
			agent:  herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "working"},
			update: herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"}, lastAttempt: 3 * time.Second,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, herdr := newTestDaemon(t, tc.cfg, tc.agent)
			sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})
			now := time.Now()
			d.dispatch(context.Background(), now)
			if got := len(herdr.promptsSnapshot()); got != 0 {
				t.Fatalf("prompted %d times while target deferred", got)
			}
			herdr.setAgents(tc.update)
			if err := d.refreshRoster(context.Background()); err != nil {
				t.Fatal(err)
			}
			d.dispatch(context.Background(), now.Add(tc.lastAttempt))
			if got := len(herdr.promptsSnapshot()); got != 1 {
				t.Fatalf("prompted %d times after target became available", got)
			}
		})
	}
}

func TestDaemonTransportDedupProbeAcksWithoutSecondPrompt(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle", StateChangeSeq: 4})
	herdr.promptErr = errors.New("socket reset")
	id := sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})
	now := time.Now()
	d.dispatch(context.Background(), now)
	herdr.setAgents(herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle", StateChangeSeq: 5})
	if err := d.refreshRoster(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.dispatch(context.Background(), now.Add(3*time.Second))
	if got := len(herdr.promptsSnapshot()); got != 1 {
		t.Fatalf("prompts = %d, want 1", got)
	}
	if _, err := d.store.History(id); err != nil {
		t.Fatalf("deduplicated message not archived: %v", err)
	}
}

func TestDaemonTTLExpiryCreatesBounce(t *testing.T) {
	d, _ := newTestDaemon(t, &Config{Host: "titan", DeliverWhen: "now", TTLSeconds: 86400})
	expired := &Msg{ID: "abcdef123456", From: "ci@titan", To: "missing", Body: "hello", TS: time.Now().Add(-2 * time.Second), TTLSeconds: 1}
	if err := d.store.EnqueueLocal(expired); err != nil {
		t.Fatal(err)
	}
	d.dispatch(context.Background(), time.Now())
	if _, err := os.Stat(filepath.Join(d.store.Root(), "dead", expired.ID+".json")); err != nil {
		t.Fatalf("expired message was not moved to dead: %v", err)
	}
	claim, err := d.store.ClaimLocal(queueKey("ci"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("expected bounce")
	}
	want := "undeliverable after 24h0m0s: missing@titan — expired"
	if claim.Msg.Body != want {
		t.Fatalf("bounce = %q, want %q", claim.Msg.Body, want)
	}
}

func TestDaemonSendUnknownHostAndAgentsSkipInboundOnlyPeer(t *testing.T) {
	cfg := &Config{Host: "titan", DeliverWhen: "now", TTLSeconds: 86400, Peers: []Peer{{Host: "inbound"}}}
	d, _ := newTestDaemon(t, cfg, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"})
	res := d.handleSend(context.Background(), map[string]any{"to": "clem@missing", "body": "hello", "from": "ci"})
	if code := valueString(res, "code"); code != "no_route" {
		t.Fatalf("code = %q, want no_route (%#v)", code, res)
	}
	if err := d.store.RecordSender("parent", "reply@parent"); err != nil {
		t.Fatal(err)
	}
	reply := d.handleSend(context.Background(), map[string]any{"to": "reply@parent", "body": "hello", "from": "ci"})
	if ok, _ := reply["ok"].(bool); !ok || valueString(reply, "route") != "peer:parent" {
		t.Fatalf("recorded sender reply = %#v", reply)
	}
	unsolicited := d.handleSend(context.Background(), map[string]any{"to": "other@parent", "body": "hello", "from": "ci"})
	if code := valueString(unsolicited, "code"); code != "not_permitted" {
		t.Fatalf("unsolicited code = %q, want not_permitted (%#v)", code, unsolicited)
	}
	agents := d.handleAgents(context.Background(), map[string]any{"op": "agents"})
	rows, _ := agents["agents"].([]RosterAgent)
	if len(rows) != 1 || rows[0].Host != "titan" {
		t.Fatalf("agents = %#v; non-dialable peer was queried", agents)
	}
}

func contains(s, fragment string) bool { return strings.Contains(s, fragment) }

// Local output must never present a name a peer cannot route as if it were an
// address, and it must not present any single name as global: a name is routable
// only by the peer on the link that supplied it. Measured failure this replaces:
// an agent read `tincan agents` on an inbound-only box, saw stub@workspace-0, and
// handed that unroutable label to a third host.
func TestWhoamiAndAgentsAnswerPerLink(t *testing.T) {
	cfg := &Config{Host: "workspace-0", DeliverWhen: "now", TTLSeconds: 86400}
	d, _ := newTestDaemon(t, cfg, herdrAgent{Name: "stub", Kind: "omp", Status: "idle", PaneID: "w9:p1"})

	t.Run("no link says so instead of implying a hostname is routable", func(t *testing.T) {
		who := d.handleWhoami(context.Background(), map[string]any{"pane_id": "w9:p1"})
		addresses, _ := who["addresses"].([]map[string]any)
		if len(addresses) != 0 {
			t.Fatalf("addresses without a link = %#v, want none", addresses)
		}
		local, _ := who["local"].(map[string]any)
		if valueString(local, "addr") != "stub@workspace-0" || valueBool(local, "routable") {
			t.Fatalf("local = %#v, want the local form marked unroutable", local)
		}
		text := renderWhoami(normalizeForRender(t, who))
		if !contains(text, "no peer link is up") || !contains(text, "per link") {
			t.Fatalf("whoami render = %q, must say nothing is routable yet", text)
		}
		agents := normalizeForRender(t, d.handleAgents(context.Background(), map[string]any{}))
		if got := renderAgents(agents); !contains(got, "[local-only]") || !contains(got, "no link is up") {
			t.Fatalf("agents render = %q, must mark local-only rows and say no link is up", got)
		}
	})

	host := &linkTestHost{host: "workspace-0"}
	manager := newLinkManager(cfg, d.store, host)
	defer manager.Close()
	d.links = manager
	first, doneFirst := connectInboundNamed(t, manager, "titan", "ticket500")
	defer func() { _ = first.Close(); <-doneFirst }()

	t.Run("adopted name is reported with the peer that adopted it", func(t *testing.T) {
		who := d.handleWhoami(context.Background(), map[string]any{"pane_id": "w9:p1"})
		addresses, _ := who["addresses"].([]map[string]any)
		if len(addresses) != 1 || valueString(addresses[0], "addr") != "stub@ticket500" ||
			valueString(addresses[0], "peer") != "titan" || valueString(addresses[0], "how") != "named by titan" {
			t.Fatalf("addresses = %#v, want stub@ticket500 named by titan", addresses)
		}
		local, _ := who["local"].(map[string]any)
		if valueBool(local, "routable") {
			t.Fatalf("local %#v must stay unroutable: no link answers to workspace-0", local)
		}
		text := renderWhoami(normalizeForRender(t, who))
		if !contains(text, "stub@ticket500") || !contains(text, "named by titan") || !contains(text, "not routable off-box") {
			t.Fatalf("whoami render = %q, must attribute the name and mark the local one", text)
		}

		agents := d.handleAgents(context.Background(), map[string]any{})
		rows, _ := agents["agents"].([]RosterAgent)
		if len(rows) != 1 || !rows[0].LocalOnly || len(rows[0].ReachableAs) != 1 || rows[0].ReachableAs[0] != "stub@ticket500" {
			t.Fatalf("agents rows = %#v, want a local-only row with the ticket500 form", rows)
		}
		rendered := renderAgents(normalizeForRender(t, agents))
		if !contains(rendered, "[local-only]") || !contains(rendered, "@ticket500 (named by titan)") {
			t.Fatalf("agents render = %q, must mark the row and name the link form", rendered)
		}
	})

	t.Run("two dialers adopting different names show both", func(t *testing.T) {
		second, doneSecond := connectInboundNamed(t, manager, "gigachad", "boxalias")
		defer func() { _ = second.Close(); <-doneSecond }()
		who := d.handleWhoami(context.Background(), map[string]any{"pane_id": "w9:p1"})
		if got := valueStrings(who, "reachable_as"); len(got) != 2 || got[0] != "stub@boxalias" || got[1] != "stub@ticket500" {
			t.Fatalf("reachable_as = %v, want both adopted names", got)
		}
		text := renderWhoami(normalizeForRender(t, who))
		if !contains(text, "stub@boxalias") || !contains(text, "named by gigachad") || !contains(text, "stub@ticket500") {
			t.Fatalf("whoami render = %q, must list every link's address", text)
		}
	})
}

// A dialer announces its own configured host, so there the local name IS the wire
// name and must not be labelled unroutable.
func TestWhoamiOnDialerReportsItsOwnNameAsRoutable(t *testing.T) {
	cfg := &Config{Host: "titan", DeliverWhen: "now", TTLSeconds: 86400}
	d, _ := newTestDaemon(t, cfg, herdrAgent{Name: "main", Kind: "omp", Status: "idle", PaneID: "w7K:p1"})
	manager := newLinkManager(cfg, d.store, &linkTestHost{host: "titan"})
	defer manager.Close()
	d.links = manager
	session := manager.newSession(newNopStream(), "ticket500", "outbound", "titan")
	if !manager.install(session) {
		t.Fatal("session was not installed")
	}

	who := d.handleWhoami(context.Background(), map[string]any{"pane_id": "w7K:p1"})
	local, _ := who["local"].(map[string]any)
	if !valueBool(local, "routable") {
		t.Fatalf("local = %#v, want main@titan reported routable", local)
	}
	addresses, _ := who["addresses"].([]map[string]any)
	if len(addresses) != 1 || valueString(addresses[0], "addr") != "main@titan" || valueString(addresses[0], "how") != "announced to ticket500" {
		t.Fatalf("addresses = %#v, want main@titan announced to ticket500", addresses)
	}
	agents := d.handleAgents(context.Background(), map[string]any{})
	rows, _ := agents["agents"].([]RosterAgent)
	if len(rows) != 1 || rows[0].LocalOnly {
		t.Fatalf("agents rows = %#v, want no local-only mark on a dialer", rows)
	}
	if got := renderAgents(normalizeForRender(t, agents)); contains(got, "[local-only]") {
		t.Fatalf("agents render = %q, must not mark a dialer's own name", got)
	}
}

// normalizeForRender round-trips a daemon response through JSON so the renderers
// see exactly the decoded shapes a CLI client receives, not in-process structs.
func normalizeForRender(t *testing.T, res map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

// An inbound-only peer cannot enumerate the host that dialed it, so the senders
// that reached it are its only routing information and must be reported.
func TestAgentsListsReplyOnlySenders(t *testing.T) {
	d, _ := newTestDaemon(t, &Config{Host: "workspace-0", DeliverWhen: "now", TTLSeconds: 86400,
		Peers: []Peer{{Host: "sibling", SSH: "sibling"}}}, herdrAgent{Name: "stub", Kind: "omp", Status: "idle", PaneID: "w9:p1"})
	if err := d.store.RecordSender("titan", "main@titan"); err != nil {
		t.Fatal(err)
	}
	// A dialable peer is enumerable through its own roster and must not be
	// duplicated into the reply-only list.
	if err := d.store.RecordSender("sibling", "peer@sibling"); err != nil {
		t.Fatal(err)
	}
	res := d.handleAgents(context.Background(), map[string]any{})
	rows, _ := res["reply_only"].([]map[string]any)
	if len(rows) != 1 || valueString(rows[0], "addr") != "main@titan" || valueString(rows[0], "via") != "inbound" {
		t.Fatalf("reply_only = %#v, want only main@titan via inbound", res["reply_only"])
	}
	wire := map[string]any{"agents": []any{}, "reply_only": []any{map[string]any{"addr": "main@titan", "host": "titan", "via": "inbound"}}}
	if text := renderAgents(wire); !contains(text, "main@titan") || !contains(text, "reply-only") {

		t.Fatalf("agents render = %q, must name reply-only senders", text)
	}
}
func TestDaemonInboxRowsResolvePaneQueues(t *testing.T) {
	d, _ := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"})
	byName := testMsg("inbox-name01", "clem")
	byName.From = "dana@titan"
	byPane := testMsg("inbox-pane01", "w7K:p2")
	byPane.From = "tester@titan"
	byPane.TS = byPane.TS.Add(time.Second)
	if err := d.store.EnqueueLocal(byName); err != nil {
		t.Fatal(err)
	}
	if err := d.store.EnqueueLocal(byPane); err != nil {
		t.Fatal(err)
	}
	if claim, err := d.store.ClaimLocal(queueKey("clem"), byName.TS); err != nil || claim == nil {
		t.Fatalf("ClaimLocal = %#v, %v", claim, err)
	}

	res := d.handle(context.Background(), map[string]any{"op": "inbox", "pane_id": "w7K:p2"})
	if !valueBool(res, "ok") {
		t.Fatalf("inbox = %#v", res)
	}
	rows, _ := res["rows"].([]map[string]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %#v, want both named and pane queues", rows)
	}
	states := map[string]string{}
	for _, row := range rows {
		states[valueString(row, "id")] = valueString(row, "state")
	}
	if states[byName.ID] != "claimed" || states[byPane.ID] != "pending" {
		t.Fatalf("inbox states = %#v", states)
	}

	all := d.handle(context.Background(), map[string]any{"op": "inbox"})
	allRows, _ := all["rows"].([]map[string]any)
	if len(allRows) != 2 {
		t.Fatalf("all inbox rows = %#v, want every local queue", allRows)
	}
}

func TestDaemonPauseIPCStopsDispatch(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"})
	message := testMsg("pause0000001", "clem")
	if err := d.store.EnqueueLocal(message); err != nil {
		t.Fatal(err)
	}
	if res := d.handle(context.Background(), map[string]any{"op": "pause", "paused": true}); !valueBool(res, "ok") || !valueBool(res, "paused") {
		t.Fatalf("pause response = %#v", res)
	}
	if !valueBool(d.handleStatus(context.Background()), "paused") {
		t.Fatal("status did not report paused")
	}
	d.dispatch(context.Background(), time.Now())
	if prompts := herdr.promptsSnapshot(); len(prompts) != 0 {
		t.Fatalf("prompts while paused = %#v", prompts)
	}
	messages, names, err := d.store.ListLocalPending(queueKey("clem"))
	if err != nil || len(messages) != 1 || !strings.HasPrefix(names[0], "msg-") || messages[0].Attempts != 0 {
		t.Fatalf("pending after paused dispatch = %#v, %#v, %v", messages, names, err)
	}
	if res := d.handle(context.Background(), map[string]any{"op": "pause", "paused": "yes"}); valueString(res, "code") != "bad_request" {
		t.Fatalf("non-boolean pause response = %#v", res)
	}
}

func TestDaemonDraftHoldNotifiesOnce(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"})
	first := testMsg("notice000001", "clem")
	first.From = "dana@titan"
	second := testMsg("notice000002", "clem")
	second.From = "alex@titan"
	second.TS = second.TS.Add(time.Second)
	if err := d.store.EnqueueLocal(first); err != nil {
		t.Fatal(err)
	}
	if err := d.store.EnqueueLocal(second); err != nil {
		t.Fatal(err)
	}
	hold := draftHold{PaneID: "w7K:p2", Agent: "omp", At: time.Now()}
	d.applyDraftHold(context.Background(), queueKey("clem"), hold)
	d.applyDraftHold(context.Background(), queueKey("clem"), hold)
	notifications := herdr.notificationsSnapshot()
	if len(notifications) != 1 || notifications[0].title != "tincan: message waiting" ||
		notifications[0].body != "dana@titan — 2 waiting until your composer is clear" {
		t.Fatalf("notifications = %#v", notifications)
	}
}

func TestDaemonDraftNotifyCanBeDisabled(t *testing.T) {
	t.Setenv("TINCAN_DRAFT_NOTIFY", "false")
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "omp", Status: "idle"})
	message := testMsg("notice-off01", "clem")
	if err := d.store.EnqueueLocal(message); err != nil {
		t.Fatal(err)
	}
	d.applyDraftHold(context.Background(), queueKey("clem"), draftHold{PaneID: "w7K:p2", Agent: "omp", At: time.Now()})
	if notifications := herdr.notificationsSnapshot(); len(notifications) != 0 {
		t.Fatalf("notifications = %#v, want none", notifications)
	}
}

func TestDaemonDraftHoldNotifyFailureDoesNotBlockDelivery(t *testing.T) {
	d, herdr := newTestDaemon(t, nil, herdrAgent{Name: "clem", PaneID: "w7K:p2", Kind: "claude", Status: "idle"})
	herdr.notifyErr = errors.New("notification unavailable")
	herdr.paneScreens["w7K:p2"] = readScreenFixture(t, "claude-draft.txt")
	id := sendForTest(t, d, map[string]any{"to": "clem", "body": "hello", "from": "ci"})
	d.dispatch(context.Background(), time.Now())
	if got := len(herdr.notificationsSnapshot()); got != 1 {
		t.Fatalf("notifications = %d, want 1", got)
	}
	herdr.paneScreens["w7K:p2"] = readScreenFixture(t, "claude-empty.txt")
	d.dispatch(context.Background(), time.Now())
	if got := len(herdr.promptsSnapshot()); got != 1 {
		t.Fatalf("prompts after notification failure = %d", got)
	}
	if _, err := d.store.History(id); err != nil {
		t.Fatalf("history after notification failure: %v", err)
	}
}
