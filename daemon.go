package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// daemon owns the one writer to the local spool and the one connection point
// for herdr and peer links.
type draftHold struct {
	PaneID string    `json:"pane_id"`
	Agent  string    `json:"agent"`
	At     time.Time `json:"at"`
}

// daemon owns the one writer to the local spool and the one connection point
// for herdr and peer links.
type daemon struct {
	cfg   *Config
	store *Store
	herdr herdrDriver
	links *linkManager

	started time.Time
	version string
	proto   int

	rosterMu sync.RWMutex
	roster   map[string]herdrAgent
	agents   []herdrAgent

	holdMu     sync.RWMutex
	draftHolds map[string]draftHold
	paused     bool
}

func newDaemon(cfg *Config, store *Store, herdr herdrDriver) *daemon {
	return &daemon{
		cfg:        cfg,
		store:      store,
		herdr:      herdr,
		started:    time.Now(),
		roster:     make(map[string]herdrAgent),
		draftHolds: make(map[string]draftHold),
	}
}

func acquireDaemonLock(root string) (func(), error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(root, "daemon.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		data, _ := os.ReadFile(lockPath)
		pid := strings.TrimSpace(string(data))
		if pid == "" {
			pid = "unknown"
		}
		_ = f.Close()
		return nil, fmt.Errorf("tincan daemon is already running (pid %s)", pid)
	}
	if err := f.Truncate(0); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return nil, err
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func cmdDaemon(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: tincan daemon")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	st, err := OpenStore(dataDir())
	if err != nil {
		return err
	}
	releaseLock, err := acquireDaemonLock(st.Root())
	if err != nil {
		return err
	}
	defer releaseLock()

	socket, err := herdrSocketPath(cfg)
	if err != nil {
		return err
	}
	driver := newHerdrSocket(socket, func(message string) { log.Print("tincan: ", message) })
	version, proto, err := driver.Ping(context.Background())
	if err != nil {
		return err
	}
	d := newDaemon(cfg, st, driver)
	d.version, d.proto = version, proto
	if err := st.ReclaimOrphans(); err != nil {
		return err
	}
	if err := d.refreshRoster(context.Background()); err != nil {
		return err
	}
	log.Printf("tincan: herdr %s protocol %d, %d agents", version, proto, d.agentCount())

	ln, err := listenIPC(socketPath())
	if err != nil {
		return err
	}
	defer func() {
		_ = ln.Close()
		_ = os.Remove(socketPath())
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	d.links = newLinkManager(cfg, st, d)
	d.links.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); _ = serveIPC(ctx, ln, d) }()
	go func() { defer wg.Done(); d.rosterLoop(ctx) }()
	go func() { defer wg.Done(); d.dispatchLoop(ctx) }()
	go func() { defer wg.Done(); d.pruneLoop(ctx) }()
	<-ctx.Done()
	_ = ln.Close()
	wg.Wait()
	d.links.Close()
	return nil
}

func (d *daemon) rosterLoop(ctx context.Context) {
	ticker := time.NewTicker(pollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.refreshRoster(ctx); err != nil {
				d.Logf("roster refresh: %v", err)
			}
		}
	}
}

func (d *daemon) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatch(context.Background(), time.Now())
		}
	}
}

func (d *daemon) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.store.PruneHistory(7 * 24 * time.Hour); err != nil {
				d.Logf("history prune: %v", err)
			}
		}
	}
}

func (d *daemon) refreshRoster(ctx context.Context) error {
	agents, err := d.herdr.ListAgents(ctx)
	if err != nil {
		return err
	}
	indexed := make(map[string]herdrAgent, len(agents)*2)
	for _, agent := range agents {
		if agent.Name != "" {
			indexed[queueKey(agent.Name)] = agent
		}
		if agent.PaneID != "" {
			indexed[queueKey(agent.PaneID)] = agent
		}
	}
	d.rosterMu.Lock()
	d.roster, d.agents = indexed, append([]herdrAgent(nil), agents...)
	d.rosterMu.Unlock()
	return nil
}

func (d *daemon) resolve(key string) (herdrAgent, bool) {
	d.rosterMu.RLock()
	agent, ok := d.roster[key]
	d.rosterMu.RUnlock()
	return agent, ok
}

func (d *daemon) agentCount() int {
	d.rosterMu.RLock()
	defer d.rosterMu.RUnlock()
	return len(d.agents)
}

func (d *daemon) localRoster() []RosterAgent {
	d.rosterMu.RLock()
	agents := append([]herdrAgent(nil), d.agents...)
	d.rosterMu.RUnlock()
	out := make([]RosterAgent, 0, len(agents))
	for _, agent := range agents {
		target := agent.Name
		if target == "" {
			target = agent.PaneID
		}
		if target == "" {
			continue
		}
		out = append(out, RosterAgent{
			Addr: targetAddr(target, d.cfg.Host), Name: agent.Name, PaneID: agent.PaneID,
			Kind: agent.Kind, Status: agent.Status, CWD: agent.CWD, Title: agent.Title, Host: d.cfg.Host,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

func targetAddr(agent, host string) string { return joinAddr(agent, host) }

func (d *daemon) dispatch(ctx context.Context, now time.Time) {
	d.holdMu.RLock()
	paused := d.paused
	d.holdMu.RUnlock()
	if paused {
		return
	}
	keys, err := d.store.QueueKeys()
	if err != nil {
		d.Logf("list queues: %v", err)
		return
	}
	for _, key := range keys {
		// Keep draft-held work pending, so neither its retry state nor its
		// delivery order changes while a person is typing in the target pane.
		if draftGuardEnabled() {
			claimable, err := d.store.HasClaimableLocal(key, now)
			if err != nil {
				d.Logf("peek %s: %v", key, err)
				continue
			}
			if !claimable {
				d.releaseDraftHold(key)
				continue
			}
			if hold := d.composerHold(ctx, key); hold != nil {
				d.applyDraftHold(ctx, key, *hold)
				continue
			}
			d.releaseDraftHold(key)
		} else {
			d.releaseDraftHold(key)
		}
		claim, err := d.store.ClaimLocal(key, now)
		if err != nil {
			d.Logf("claim %s: %v", key, err)
			continue
		}
		if claim == nil {
			continue
		}
		d.dispatchClaim(ctx, key, claim, now)
	}
}

// composerHold uses agent.get rather than the cached roster so the pane and
// harness kind describe the exact target about to receive a prompt.
func (d *daemon) composerHold(ctx context.Context, key string) *draftHold {
	agent, cached := d.resolve(key)
	if !cached {
		return nil
	}
	target := agent.Name
	if strings.HasPrefix(key, "p-") || target == "" {
		target = agent.PaneID
	}
	if target == "" {
		return nil
	}
	probe, err := d.herdr.GetAgent(ctx, target)
	if err != nil || probe == nil || probe.PaneID == "" {
		return nil
	}
	screen, err := d.herdr.PaneScreen(ctx, probe.PaneID)
	if err != nil || DetectComposer(probe.Kind, screen) != ComposerDraft {
		return nil
	}
	return &draftHold{PaneID: probe.PaneID, Agent: probe.Kind, At: time.Now().UTC()}
}

func (d *daemon) applyDraftHold(ctx context.Context, key string, hold draftHold) {
	d.holdMu.Lock()
	_, alreadyHeld := d.draftHolds[key]
	d.draftHolds[key] = hold
	d.holdMu.Unlock()
	if alreadyHeld {
		return
	}
	d.Logf("delivery held — %s has unsent input in pane %s; retrying until the composer is clear", hold.Agent, hold.PaneID)
	if !draftNotifyEnabled() {
		return
	}
	// One toast on the same edge the log line is written: a held message is
	// otherwise invisible, and re-notifying every second would train the human to
	// ignore it. A notification is an extra — its failure never holds delivery.
	body := "delivery held until your composer is clear"
	if messages, _, err := d.store.ListLocalPending(key); err == nil && len(messages) > 0 {
		body = fmt.Sprintf("%s — %d waiting until your composer is clear", messages[0].From, len(messages))
	}
	if err := d.herdr.Notify(ctx, "tincan: message waiting", body); err != nil {
		d.Logf("notify failed — %v", err)
	}
}

func (d *daemon) releaseDraftHold(key string) {
	d.holdMu.Lock()
	hold, held := d.draftHolds[key]
	delete(d.draftHolds, key)
	d.holdMu.Unlock()
	if held {
		d.Logf("delivery resumed — pane %s composer is clear", hold.PaneID)
	}
}

func (d *daemon) draftHoldStatus() []draftHold {
	d.holdMu.RLock()
	holds := make([]draftHold, 0, len(d.draftHolds))
	for _, hold := range d.draftHolds {
		holds = append(holds, hold)
	}
	d.holdMu.RUnlock()
	sort.Slice(holds, func(i, j int) bool { return holds[i].PaneID < holds[j].PaneID })
	return holds
}

func (d *daemon) setPaused(paused bool) {
	d.holdMu.Lock()
	d.paused = paused
	d.holdMu.Unlock()
}

func (d *daemon) isPaused() bool {
	d.holdMu.RLock()
	defer d.holdMu.RUnlock()
	return d.paused
}

func draftGuardEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("TINCAN_DRAFT_GUARD")))
	return value != "0" && value != "false"
}

func draftNotifyEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("TINCAN_DRAFT_NOTIFY")))
	return value != "0" && value != "false"
}

func (d *daemon) dispatchClaim(ctx context.Context, key string, claim *Claimed, now time.Time) {
	msg := claim.Msg
	if d.store.Expired(msg, now) {
		reason := msg.LastError
		if reason == "" {
			reason = "expired"
		}
		if err := d.store.Kill(claim, reason); err != nil {
			d.Logf("kill expired %s: %v", msg.ID, err)
			return
		}
		d.enqueueBounce(msg, reason)
		return
	}
	agent, found := d.resolve(key)
	if !found {
		_ = d.store.Release(claim, "agent_not_found", 0)
		return
	}
	target := agent.Name
	if strings.HasPrefix(key, "p-") || target == "" {
		target = agent.PaneID
	}
	if target == "" {
		_ = d.store.Release(claim, "agent_not_found", 0)
		return
	}
	if strings.HasPrefix(msg.LastError, "transport:") {
		probe, err := d.herdr.GetAgent(ctx, target)
		if err == nil && probe != nil && probe.StateChangeSeq != msg.LastSeq {
			if err := d.store.Ack(claim); err != nil {
				d.Logf("ack deduplicated %s: %v", msg.ID, err)
			} else {
				d.Logf("%s probably delivered (seq moved), not resending", msg.ID)
			}
			return
		}
	}
	if agent.LaunchPending {
		_ = d.store.Release(claim, "agent_launch_pending", 0)
		return
	}
	if d.cfg.DeliverWhen == "settled" && agent.Status == "working" {
		_ = d.store.Release(claim, "agent_working", 0)
		return
	}
	err := d.herdr.Prompt(ctx, target, renderEnvelope(msg))
	if err == nil {
		if err := d.store.Ack(claim); err != nil {
			d.Logf("ack %s: %v", msg.ID, err)
		}
		return
	}
	lastErr := err.Error()
	if codeOf(err) == "" {
		lastErr = "transport:" + lastErr
	}
	if releaseErr := d.store.Release(claim, lastErr, agent.StateChangeSeq); releaseErr != nil {
		d.Logf("release %s: %v", msg.ID, releaseErr)
	}
}

func (d *daemon) enqueueBounce(original *Msg, reason string) {
	fromAgent, fromHost, err := parseAddr(original.From)
	if err != nil || fromAgent == reservedName {
		return
	}
	if fromHost == "" {
		fromHost = d.cfg.Host
	}
	if fromHost != d.cfg.Host {
		peer, known := d.cfg.FindPeer(fromHost)
		if !known || (!peer.Dialable() && !d.knownSender(fromHost, original.From)) {
			return
		}
	}
	destHost := ""
	if fromHost != d.cfg.Host {
		destHost = fromHost
	}
	destination := targetAddr(original.To, original.Host)
	if original.Host == "" {
		destination = targetAddr(original.To, d.cfg.Host)
	}
	bounce := &Msg{
		ID: newID(), From: joinAddr(reservedName, d.cfg.Host), To: fromAgent, Host: destHost,
		Body: fmt.Sprintf("undeliverable after %s: %s — %s", d.cfg.TTL(), destination, reason),
		TS:   time.Now().UTC(), TTLSeconds: d.cfg.TTLSeconds,
	}
	if destHost == "" {
		_ = d.store.EnqueueLocal(bounce)
		return
	}
	if err := d.store.EnqueueOutbox(bounce); err == nil && d.links != nil {
		d.links.Notify()
	}
}

func (d *daemon) knownSender(host, addr string) bool {
	known, err := d.store.KnownSender(host, addr)
	return err == nil && known
}

// LocalHost, LocalRoster, AcceptInbound and Logf implement linkHost.
func (d *daemon) LocalHost() string { return d.cfg.Host }

func (d *daemon) LocalRoster(ctx context.Context) ([]RosterAgent, error) {
	return d.localRoster(), nil
}

func (d *daemon) AcceptInbound(ctx context.Context, fromHost string, f MsgFrame) error {
	to, host, err := parseAddr(f.To)
	if err != nil || host != "" {
		return codedErrorf("bad_address", "destination must be a local agent address")
	}
	if len([]byte(f.Body)) > maxBodyBytes {
		return codedErrorf("body_too_large", "message body exceeds %d bytes", maxBodyBytes)
	}
	if f.ID == "" {
		return codedErrorf("bad_address", "message id is required")
	}
	from, frameHost, ok := strings.Cut(f.From, "@")
	if !ok || frameHost != fromHost || !(agentNameRe.MatchString(from) || paneIDRe.MatchString(from)) {
		return codedErrorf("bad_address", "invalid sender address")
	}
	ts, err := time.Parse(time.RFC3339, f.TS)
	if err != nil {
		return codedErrorf("bad_address", "invalid message timestamp")
	}
	ttl := f.TTLSeconds
	if ttl <= 0 {
		return codedErrorf("bad_address", "invalid message TTL")
	}
	msg := &Msg{ID: f.ID, From: f.From, To: to, Body: f.Body, ReplyTo: f.ReplyTo, TS: ts, TTLSeconds: ttl}
	if err := d.store.EnqueueLocal(msg); err != nil {
		if codeOf(err) != "" {
			return err
		}
		return codedErrorf("internal", "enqueue inbound message: %v", err)
	}
	if err := d.store.RecordSender(fromHost, f.From); err != nil {
		return codedErrorf("internal", "record sender: %v", err)
	}
	return nil
}

func (d *daemon) Logf(format string, a ...any) { log.Printf("tincan: "+format, a...) }

func success(fields map[string]any) map[string]any {
	fields["ok"] = true
	return fields
}

func failure(err error) map[string]any {
	code := codeOf(err)
	if code == "" {
		code = "internal"
	}
	return map[string]any{"ok": false, "code": code, "error": err.Error()}
}

func stringField(req map[string]any, name string) string {
	value, _ := req[name].(string)
	return value
}

func (d *daemon) handle(ctx context.Context, req map[string]any) map[string]any {
	switch stringField(req, "op") {
	case "send":
		return d.handleSend(ctx, req)
	case "agents":
		return d.handleAgents(ctx, req)
	case "peers":
		if d.links == nil {
			return success(map[string]any{"peers": []PeerStatus{}})
		}
		return success(map[string]any{"peers": d.links.Peers(ctx)})
	case "read":
		return d.handleRead(req)
	case "name":
		return d.handleName(ctx, req)
	case "whoami":
		return d.handleWhoami(ctx, req)
	case "status":
		return d.handleStatus(ctx)
	case "pause":
		return d.handlePause(req)
	case "inbox":
		return d.handleInbox(ctx, req)
	default:
		return failure(codedErrorf("bad_request", "unknown operation %q", stringField(req, "op")))
	}
}

func (d *daemon) handlePause(req map[string]any) map[string]any {
	paused, ok := req["paused"].(bool)
	if !ok {
		return failure(codedErrorf("bad_request", "paused must be a boolean"))
	}
	d.setPaused(paused)
	return success(map[string]any{"paused": paused})
}

func inboxPreview(body string) string {
	runes := []rune(body)
	if len(runes) <= 100 {
		return body
	}
	return string(runes[:100]) + "…"
}

func (d *daemon) handleInbox(ctx context.Context, req map[string]any) map[string]any {
	paneID := stringField(req, "pane_id")
	keys := []string{}
	if paneID == "" {
		var err error
		keys, err = d.store.QueueKeys()
		if err != nil {
			return failure(err)
		}
	} else {
		agent, err := d.herdr.GetAgent(ctx, paneID)
		if err != nil {
			return failure(err)
		}
		if agent == nil {
			return failure(codedErrorf("agent_not_found", "agent %q was not found", paneID))
		}
		targetPane := agent.PaneID
		if targetPane == "" {
			targetPane = paneID
		}
		keys = append(keys, queueKey(targetPane))
		if agent.Name != "" {
			keys = append(keys, queueKey(agent.Name))
		}
	}

	rows := make([]map[string]any, 0)
	for _, key := range keys {
		messages, names, err := d.store.ListLocalPending(key)
		if err != nil {
			return failure(err)
		}
		for i, message := range messages {
			state := "pending"
			if strings.HasPrefix(names[i], "claimed-") {
				state = "claimed"
			}
			var lastError any
			if message.LastError != "" {
				lastError = message.LastError
			}
			rows = append(rows, map[string]any{
				"queue": key, "id": message.ID, "from": message.From, "to": message.To,
				"state": state, "ts": message.TS, "attempts": message.Attempts,
				"last_error": lastError, "preview": inboxPreview(message.Body),
			})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		left, _ := rows[i]["ts"].(time.Time)
		right, _ := rows[j]["ts"].(time.Time)
		return left.Before(right)
	})
	return success(map[string]any{
		"paused": d.isPaused(), "draft_holds": d.draftHoldStatus(), "rows": rows,
	})
}

func (d *daemon) handleSend(ctx context.Context, req map[string]any) map[string]any {
	toRaw, body := stringField(req, "to"), stringField(req, "body")
	to, host, err := parseAddr(toRaw)
	if err != nil {
		return failure(err)
	}
	if len([]byte(body)) > maxBodyBytes {
		return failure(codedErrorf("body_too_large", "message body exceeds %d bytes", maxBodyBytes))
	}
	paneID := stringField(req, "pane_id")
	from := stringField(req, "from")
	if paneID != "" {
		agent, err := d.herdr.GetAgent(ctx, paneID)
		if err != nil {
			return failure(err)
		}
		if agent == nil {
			return failure(codedErrorf("agent_not_found", "agent %q was not found", paneID))
		}
		from = agent.Name
		if from == "" {
			from = paneID
		}
	} else if !agentNameRe.MatchString(from) {
		return failure(codedErrorf("bad_address", "sender must be an agent name"))
	}
	message := &Msg{ID: newID(), From: joinAddr(from, d.cfg.Host), To: to, Body: body, ReplyTo: stringField(req, "reply_to"), TS: time.Now().UTC(), TTLSeconds: d.cfg.TTLSeconds}
	if host == "" || host == d.cfg.Host {
		if err := d.store.EnqueueLocal(message); err != nil {
			return failure(err)
		}
		warn := ""
		if _, found := d.resolve(queueKey(to)); !found {
			warn = fmt.Sprintf("agent %s is not currently available; message remains queued", to)
		}
		return success(map[string]any{"id": message.ID, "route": "local", "warn": warn})
	}
	peer, configured := d.cfg.FindPeer(host)
	inboundOnly := configured && !peer.Dialable()
	if d.links != nil {
		direction, up := d.links.Route(host)
		inboundOnly = inboundOnly || (up && direction == "inbound")
	}
	// A sender record survives a temporary link outage, so a child host can
	// durably reply after its parent restarts without needing a peer config.
	inboundOnly = inboundOnly || d.store.KnownSenderCount(host) > 0
	if !configured && !inboundOnly {
		return failure(codedErrorf("no_route", "no peer named %s; add it to ~/.config/tincan/config.json", host))
	}
	if configured && peer.Dialable() {
		// A configured dialable peer may receive any target address.
	} else if !d.knownSender(host, joinAddr(to, host)) {
		return failure(codedErrorf("not_permitted", "no ssh route to %s; you may only reply to agents there that messaged you first", host))
	}
	message.Host = host
	if err := d.store.EnqueueOutbox(message); err != nil {
		return failure(err)
	}
	if d.links != nil {
		d.links.Notify()
	}
	return success(map[string]any{"id": message.ID, "route": "peer:" + host, "warn": ""})
}

func (d *daemon) handleAgents(ctx context.Context, req map[string]any) map[string]any {
	host := stringField(req, "host")
	if host != "" && host != d.cfg.Host {
		peer, found := d.cfg.FindPeer(host)
		if !found || !peer.Dialable() {
			return failure(codedErrorf("unknown_host", "no dialable peer named %s", host))
		}
	}
	agents := []RosterAgent{}
	hosts := []map[string]any{}
	names := d.wireNames()
	selfRoutable := localRoutable(names, d.cfg.Host)
	if host == "" || host == d.cfg.Host {
		local := d.localRoster()
		// A local address is routable only if a live link answers to this host's
		// own name. Otherwise it is a local label, and a reader that hands it to a
		// peer gets a dead address with no error at the point of the mistake — so
		// say so on every row and give the per-link forms.
		for i := range local {
			local[i].ReachableAs = reachableAs(local[i].Addr, names)
			local[i].LocalOnly = !selfRoutable
		}
		agents = append(agents, local...)
		hosts = append(hosts, map[string]any{"host": d.cfg.Host, "source": "local", "ok": true,
			"local_only": !selfRoutable, "wire_names": names})
	}
	for _, peer := range d.cfg.Peers {
		if !peer.Dialable() || (host != "" && peer.Host != host) {
			continue
		}
		if d.links == nil {
			hosts = append(hosts, map[string]any{"host": peer.Host, "source": "peer", "ok": false, "error": "link down"})
			continue
		}
		remote, err := d.links.Roster(ctx, peer.Host)
		if err != nil {
			hosts = append(hosts, map[string]any{"host": peer.Host, "source": "peer", "ok": false, "error": err.Error()})
			continue
		}
		hosts = append(hosts, map[string]any{"host": peer.Host, "source": "peer", "ok": true})
		agents = append(agents, remote...)
	}
	// An inbound-only peer cannot enumerate the host that dialed it. The senders
	// that reached it are its entire routable world, so name them rather than
	// leaving an agent with an empty roster and no next step.
	replyOnly := []map[string]any{}
	if host == "" {
		senders, err := d.store.ReplyOnlySenders()
		if err != nil {
			d.Logf("tincan: list reply-only senders: %v", err)
		}
		for _, senderHost := range sortedKeys(senders) {
			if peer, found := d.cfg.FindPeer(senderHost); found && peer.Dialable() {
				continue // already enumerable through its own roster
			}
			for _, addr := range senders[senderHost] {
				replyOnly = append(replyOnly, map[string]any{"addr": addr, "host": senderHost, "via": "inbound"})
			}
		}
	}
	return success(map[string]any{"agents": agents, "hosts": hosts, "reply_only": replyOnly,
		"wire_names": names, "local_host": d.cfg.Host, "local_routable": selfRoutable})
}

// wireNames lists the per-link names this host answers to. It is deliberately a
// list and not a single value: an address is routable by the peer on that link
// and by nobody else.
func (d *daemon) wireNames() []WireName {
	if d.links == nil {
		return []WireName{}
	}
	return d.links.WireNames()
}

// localRoutable reports whether this host's own name is one a live link can route.
// A dialer announces its configured host, so there it is true; a box that only
// ever accepts links is known by the alias its dialer chose, so there it is false.
func localRoutable(names []WireName, host string) bool {
	for _, name := range names {
		if name.Addr == host {
			return true
		}
	}
	return false
}

// agentAddresses restates one agent's address for every live link.
func agentAddresses(addr string, names []WireName) []map[string]any {
	agent, _, err := parseAddr(addr)
	if err != nil || len(names) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(names))
	for _, name := range names {
		out = append(out, map[string]any{
			"addr":      joinAddr(agent, name.Addr),
			"peer":      name.Peer,
			"direction": name.Direction,
			"how":       wireNameOrigin(name),
		})
	}
	return out
}

func wireNameOrigin(name WireName) string {
	if name.Direction == "inbound" {
		return "named by " + name.Peer
	}
	return "announced to " + name.Peer
}

func reachableAs(addr string, names []WireName) []string {
	agent, _, err := parseAddr(addr)
	if err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, name := range names {
		form := joinAddr(agent, name.Addr)
		if _, dup := seen[form]; dup {
			continue
		}
		seen[form] = struct{}{}
		out = append(out, form)
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (d *daemon) handleRead(req map[string]any) map[string]any {
	msg, err := d.store.History(stringField(req, "id"))
	if err != nil {
		return failure(err)
	}
	return success(map[string]any{"from": msg.From, "ts": msg.TS.UTC().Format(time.RFC3339), "reply_to": msg.ReplyTo, "body": msg.Body})
}

func (d *daemon) handleName(ctx context.Context, req map[string]any) map[string]any {
	paneID, name := stringField(req, "pane_id"), stringField(req, "name")
	if paneID == "" {
		return failure(codedErrorf("agent_not_found", "pane_id is required"))
	}
	agent, err := d.herdr.Rename(ctx, paneID, name)
	if err != nil {
		return failure(err)
	}
	if agent == nil || agent.Name == "" {
		return failure(codedErrorf("agent_not_found", "agent %q was not found", paneID))
	}
	return success(map[string]any{"addr": joinAddr(agent.Name, d.cfg.Host)})
}

func (d *daemon) handleWhoami(ctx context.Context, req map[string]any) map[string]any {
	paneID := stringField(req, "pane_id")
	if paneID == "" {
		return failure(codedErrorf("agent_not_found", "pane_id is required"))
	}
	agent, err := d.herdr.GetAgent(ctx, paneID)
	if err != nil {
		return failure(err)
	}
	if agent == nil {
		return failure(codedErrorf("agent_not_found", "agent %q was not found", paneID))
	}
	identity := agent.Name
	if identity == "" {
		identity = agent.PaneID
	}
	local := joinAddr(identity, d.cfg.Host)
	names := d.wireNames()
	// There is no single answer to "what is my address": a name is routable by the
	// peer on the link that supplied it. Answer per link, and say plainly when the
	// local form is only a local label.
	return success(map[string]any{
		"local": map[string]any{
			"addr":     local,
			"host":     d.cfg.Host,
			"routable": localRoutable(names, d.cfg.Host),
		},
		"addresses":    agentAddresses(local, names),
		"reachable_as": reachableAs(local, names),
		"name":         agent.Name,
		"pane_id":      agent.PaneID,
		"kind":         agent.Kind,
		"status":       agent.Status,
		"named":        agent.Name != "",
		"note":         "addressing is per link; prefer replying to the exact from address on a message",
	})
}

func (d *daemon) handleStatus(ctx context.Context) map[string]any {
	queued, outbox, err := d.store.Counts()
	if err != nil {
		return failure(err)
	}
	links := []PeerStatus{}
	if d.links != nil {
		links = d.links.Peers(ctx)
	}
	return success(map[string]any{
		"host": d.cfg.Host, "uptime_s": int64(time.Since(d.started).Seconds()),
		"herdr":  map[string]any{"version": d.version, "protocol": d.proto, "agents": d.agentCount()},
		"queued": queued, "outbox": outbox, "links": links, "paused": d.isPaused(),
		"draft_holds": d.draftHoldStatus(),
	})
}
