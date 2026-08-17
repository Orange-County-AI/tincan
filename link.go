package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	linkProto     = 1
	maxLinkFrame  = 1 << 20
	linkPingEvery = 30 * time.Second
)

// MsgFrame is the complete, deliberately small, wire vocabulary for a tincan
// link. A link is symmetric: either endpoint may send every request frame.
type MsgFrame struct {
	T          string `json:"t"`
	ID         string `json:"id,omitempty"`
	From       string `json:"from,omitempty"`
	To         string `json:"to,omitempty"`
	ReplyTo    string `json:"reply_to,omitempty"`
	Body       string `json:"body,omitempty"`
	TS         string `json:"ts,omitempty"`
	TTLSeconds int    `json:"ttl_s,omitempty"`
	Host       string `json:"host,omitempty"`
	// You is the name the dialer knows the accepting peer by. A peer reached
	// over an ssh alias cannot discover that alias itself — its hostname is
	// whatever its image happened to set — so the side that dialed names the
	// side that answered, and the answer adopts it for this link's addresses.
	You    string        `json:"you,omitempty"`
	Proto  int           `json:"proto,omitempty"`
	Ver    string        `json:"ver,omitempty"`
	Code   string        `json:"code,omitempty"`
	Detail string        `json:"detail,omitempty"`
	RID    string        `json:"rid,omitempty"`
	Agents []RosterAgent `json:"agents,omitempty"`
	N      int           `json:"n,omitempty"`
}

type PeerStatus struct {
	Host         string `json:"host"`
	Dialable     bool   `json:"dialable"`
	SSH          string `json:"ssh,omitempty"`
	Link         string `json:"link"`
	Direction    string `json:"direction"`
	Since        string `json:"since,omitempty"`
	Queued       int    `json:"queued"`
	LastError    string `json:"last_error,omitempty"`
	KnownSenders int    `json:"known_senders,omitempty"`
}

type linkHost interface {
	LocalHost() string
	LocalRoster(ctx context.Context) ([]RosterAgent, error)
	AcceptInbound(ctx context.Context, fromHost string, f MsgFrame) error
	Logf(format string, a ...any)
}

type frameReader struct{ scanner *bufio.Scanner }

func newFrameReader(r io.Reader) *frameReader {
	s := bufio.NewScanner(r)
	// Scanner reports ErrTooLong rather than silently splitting a frame. The
	// initial buffer avoids allocating a megabyte for ordinary small messages.
	s.Buffer(make([]byte, 4096), maxLinkFrame+1)
	return &frameReader{scanner: s}
}

func (r *frameReader) next() (MsgFrame, error) {
	var f MsgFrame
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return f, fmt.Errorf("link frame exceeds %d bytes or is unreadable: %w", maxLinkFrame, err)
		}
		return f, io.EOF
	}
	if len(r.scanner.Bytes()) > maxLinkFrame {
		return f, fmt.Errorf("link frame exceeds %d bytes", maxLinkFrame)
	}
	if err := json.Unmarshal(r.scanner.Bytes(), &f); err != nil {
		return f, fmt.Errorf("invalid link frame: %w", err)
	}
	if f.T == "" {
		return f, fmt.Errorf("invalid link frame: missing t")
	}
	return f, nil
}

func writeFrame(w io.Writer, f MsgFrame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("encode link frame: %w", err)
	}
	if len(data) > maxLinkFrame {
		return fmt.Errorf("link frame exceeds %d bytes", maxLinkFrame)
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write link frame: %w", err)
	}
	return nil
}

func verifyHelloOK(expectedHost string, f MsgFrame) error {
	if f.T != "hello_ok" {
		return fmt.Errorf("link hello rejected: %s %s", f.Code, f.Detail)
	}
	if f.Proto != linkProto {
		return fmt.Errorf("unsupported_proto: peer uses %d", f.Proto)
	}
	if f.Host != expectedHost {
		return fmt.Errorf("host_mismatch: expected %s, got %s", expectedHost, f.Host)
	}
	return nil
}

type linkManager struct {
	cfg  *Config
	st   *Store
	host linkHost

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	links   map[string]*linkSession
	lastErr map[string]string
	learned map[string]string
	closed  bool
}

type linkSession struct {
	manager   *linkManager
	conn      io.ReadWriteCloser
	remote    string
	direction string
	// localName is the name this endpoint answers to on THIS link: the dialer's
	// own configured host outbound, or the name the dialer gave us inbound. Every
	// address this side puts on the wire is qualified with it, so an alias-named
	// peer stays addressable by the alias its parent uses.
	localName string
	since     time.Time

	writeMu sync.Mutex
	mu      sync.Mutex
	acks    map[string]chan MsgFrame
	rosters map[string]chan MsgFrame
	done    chan struct{}
	wake    chan struct{}
	close   sync.Once
}

func newLinkManager(cfg *Config, st *Store, host linkHost) *linkManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &linkManager{
		cfg:     cfg,
		st:      st,
		host:    host,
		ctx:     ctx,
		cancel:  cancel,
		links:   make(map[string]*linkSession),
		lastErr: make(map[string]string),
		learned: make(map[string]string),
	}
}

func (m *linkManager) Start(ctx context.Context) {
	// The caller's cancellation is part of daemon lifetime; keep manager Close
	// idempotent so shutdown can also explicitly close every active socket.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-ctx.Done():
			m.Close()
		case <-m.ctx.Done():
		}
	}()
	for _, peer := range m.cfg.Peers {
		if !peer.Dialable() {
			continue
		}
		peer := peer
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.dialPeer(peer)
		}()
	}
}

func (m *linkManager) Notify() {
	m.mu.Lock()
	links := make([]*linkSession, 0, len(m.links))
	for _, s := range m.links {
		links = append(links, s)
	}
	m.mu.Unlock()
	for _, s := range links {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (m *linkManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.cancel()
	links := make([]*linkSession, 0, len(m.links))
	for _, s := range m.links {
		links = append(links, s)
	}
	m.mu.Unlock()
	for _, s := range links {
		s.shutdown()
	}
}

func (m *linkManager) Route(host string) (direction string, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.links[host]
	if s == nil {
		return "", false
	}
	select {
	case <-s.done:
		return "", false
	default:
		return s.direction, true
	}
}

// WireName is one link's answer to "what is this host called?". There is no
// global answer: on an outbound link we announced our configured host, and on an
// inbound link the dialer named us something our hostname need not match.
type WireName struct {
	Addr      string `json:"addr"`
	Peer      string `json:"peer"`
	Direction string `json:"direction"`
}

// WireNames reports the name this host answers to on every live link. Callers
// must present these per link rather than collapsing them into one address: a
// name adopted from one dialer is routable only by that dialer.
func (m *linkManager) WireNames() []WireName {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]WireName, 0, len(m.links))
	for host, s := range m.links {
		if s == nil || s.localName == "" {
			continue
		}
		select {
		case <-s.done:
		default:
			names = append(names, WireName{Addr: s.localName, Peer: host, Direction: s.direction})
		}
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i].Addr != names[j].Addr {
			return names[i].Addr < names[j].Addr
		}
		return names[i].Peer < names[j].Peer
	})
	return names
}

func (m *linkManager) Peers(ctx context.Context) []PeerStatus {
	_, outbox, err := m.st.Counts()
	if err != nil {
		m.host.Logf("tincan: count outbox: %v", err)
		outbox = map[string]int{}
	}
	m.mu.Lock()
	configured := make(map[string]Peer, len(m.cfg.Peers))
	for _, p := range m.cfg.Peers {
		configured[p.Host] = p
	}
	remotes := make(map[string]*linkSession, len(m.links))
	for h, s := range m.links {
		remotes[h] = s
	}
	learned := make(map[string]string, len(m.learned))
	for h, direction := range m.learned {
		learned[h] = direction
	}
	errs := make(map[string]string, len(m.lastErr))
	for h, e := range m.lastErr {
		errs[h] = e
	}
	m.mu.Unlock()

	statuses := make([]PeerStatus, 0, len(configured)+len(remotes)+len(learned))
	seen := make(map[string]bool, len(configured)+len(remotes)+len(learned))
	appendStatus := func(name string, p Peer, hasPeer bool) {
		seen[name] = true
		status := PeerStatus{Host: name, Link: "down", Queued: outbox[name], KnownSenders: m.st.KnownSenderCount(name)}
		if hasPeer {
			status.Dialable = p.Dialable()
			status.SSH = p.SSH
		}
		if status.Direction == "" {
			status.Direction = learned[name]
		}
		if s := remotes[name]; s != nil {
			select {
			case <-s.done:
			default:
				status.Link = "up"
				status.Direction = s.direction
				status.Since = s.since.UTC().Format(time.RFC3339)
			}
		}
		if status.Link == "down" {
			status.LastError = errs[name]
		}
		statuses = append(statuses, status)
	}
	for _, p := range m.cfg.Peers {
		appendStatus(p.Host, p, true)
	}
	for h := range remotes {
		if !seen[h] {
			appendStatus(h, Peer{}, false)
		}
	}
	for h := range learned {
		if !seen[h] {
			appendStatus(h, Peer{}, false)
		}
	}
	// Stable output makes CLI and JSON callers predictable without requiring a
	// map-shaped response.
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Host < statuses[j].Host })
	return statuses
}

func (m *linkManager) Roster(ctx context.Context, remote string) ([]RosterAgent, error) {
	m.mu.Lock()
	s := m.links[remote]
	m.mu.Unlock()
	if s == nil {
		return nil, fmt.Errorf("link down")
	}
	rid := newID()
	ch := make(chan MsgFrame, 1)
	if !s.registerRoster(rid, ch) {
		return nil, fmt.Errorf("link down")
	}
	defer s.forgetRoster(rid)
	if err := s.send(MsgFrame{T: "roster_req", RID: rid}); err != nil {
		return nil, err
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case f := <-ch:
		return f.Agents, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("roster timeout")
	case <-s.done:
		return nil, fmt.Errorf("link down")
	}
}

// ServeInbound owns conn until its peer exits. The link command already
// consumed its local IPC request line, so this function sees only link frames.
func (m *linkManager) ServeInbound(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := newFrameReader(conn)
	hello, err := reader.next()
	if err != nil {
		m.host.Logf("tincan: inbound link hello: %v", err)
		return
	}
	if hello.T != "hello" {
		_ = writeFrame(conn, MsgFrame{T: "nak", Code: "bad_hello", Detail: "expected hello"})
		return
	}
	if hello.Proto != linkProto {
		_ = writeFrame(conn, MsgFrame{T: "nak", Code: "unsupported_proto", Detail: fmt.Sprintf("expected %d", linkProto)})
		return
	}
	if !hostRe.MatchString(hello.Host) || hello.Host == m.host.LocalHost() {
		_ = writeFrame(conn, MsgFrame{T: "nak", Code: "host_mismatch", Detail: "invalid remote host"})
		return
	}
	// The dialer knows us by an ssh alias we cannot introspect, so adopt the name
	// it addressed us as. Falling back to our own host keeps a peerless link (a
	// dialer that sends no `you`) working exactly as before.
	local := m.host.LocalHost()
	if hello.You != "" && hostRe.MatchString(hello.You) && hello.You != hello.Host {
		local = hello.You
	}
	if err := writeFrame(conn, MsgFrame{T: "hello_ok", Host: local, Proto: linkProto, Ver: version}); err != nil {
		return
	}
	s := m.newSession(conn, hello.Host, "inbound", local)
	m.runEstablished(ctx, s, reader)
}

func (m *linkManager) newSession(conn io.ReadWriteCloser, remote, direction, localName string) *linkSession {
	return &linkSession{
		manager: m, conn: conn, remote: remote, direction: direction, localName: localName, since: time.Now(),
		acks: make(map[string]chan MsgFrame), rosters: make(map[string]chan MsgFrame),
		done: make(chan struct{}), wake: make(chan struct{}, 1),
	}
}

func (m *linkManager) install(s *linkSession) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	if old := m.links[s.remote]; old != nil && old != s {
		old.shutdown()
	}
	m.links[s.remote] = s
	m.lastErr[s.remote] = ""
	if s.direction == "inbound" {
		m.learned[s.remote] = s.direction
	}
	return true
}

func (m *linkManager) remove(s *linkSession, err error) {
	m.mu.Lock()
	if m.links[s.remote] == s {
		delete(m.links, s.remote)
	}
	if err != nil && m.ctx.Err() == nil {
		m.lastErr[s.remote] = err.Error()
	}
	m.mu.Unlock()
}

func (m *linkManager) runEstablished(parent context.Context, s *linkSession, reader *frameReader) {
	if !m.install(s) {
		s.shutdown()
		return
	}
	m.Notify()
	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()
	go func() {
		select {
		case <-parent.Done():
			s.shutdown()
		case <-ctx.Done():
		case <-s.done:
		}
	}()
	// The drain goroutine writes to the store (ack, release, dead-letter), so the
	// session must outlive it. Returning while it is mid-write leaves a store
	// mutation racing daemon shutdown.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		m.drainSession(ctx, s)
	}()

	var loopErr error
	for {
		f, err := reader.next()
		if err != nil {
			if err != io.EOF {
				loopErr = err
			}
			break
		}
		if err := m.handleFrame(ctx, s, f); err != nil {
			loopErr = err
			break
		}
	}
	s.shutdown()
	cancel()
	<-drained
	m.remove(s, loopErr)
}

func (m *linkManager) handleFrame(ctx context.Context, s *linkSession, f MsgFrame) error {
	switch f.T {
	case "msg":
		// A link is one hop only. Inbound frames conventionally carry a bare To,
		// but reject a qualified target for a third host even if a caller forged it.
		_, targetHost, err := parseAddr(f.To)
		if err != nil {
			return s.send(MsgFrame{T: "nak", ID: f.ID, Code: codeOf(err), Detail: err.Error()})
		}
		if targetHost != "" && targetHost != m.host.LocalHost() && targetHost != s.localName {
			return s.send(MsgFrame{T: "nak", ID: f.ID, Code: "no_route", Detail: "single-hop links do not relay"})
		}
		if targetHost != "" {
			agent, _, _ := parseAddr(f.To)
			f.To = agent
		}
		if err := m.host.AcceptInbound(ctx, s.remote, f); err != nil {
			code := codeOf(err)
			if code == "" {
				code = "delivery_failed"
			}
			return s.send(MsgFrame{T: "nak", ID: f.ID, Code: code, Detail: err.Error()})
		}
		return s.send(MsgFrame{T: "ack", ID: f.ID})
	case "ack", "nak":
		s.deliverAck(f)
		return nil
	case "roster_req":
		agents, err := m.host.LocalRoster(ctx)
		if err != nil {
			return s.send(MsgFrame{T: "nak", RID: f.RID, Code: codeOf(err), Detail: err.Error()})
		}
		// Re-label every address with the name this link knows us by, so the
		// asking side can address what it sees.
		for i := range agents {
			agent, _, err := parseAddr(agents[i].Addr)
			if err != nil {
				continue
			}
			agents[i].Host = s.localName
			agents[i].Addr = joinAddr(agent, s.localName)
		}
		return s.send(MsgFrame{T: "roster", RID: f.RID, Host: s.localName, Agents: agents})
	case "roster":
		s.deliverRoster(f)
		return nil
	case "ping":
		return s.send(MsgFrame{T: "pong", N: f.N})
	case "pong":
		s.deliverPong(f.N)
		return nil
	default:
		return fmt.Errorf("unknown link frame %q", f.T)
	}
}

func (m *linkManager) drainSession(ctx context.Context, s *linkSession) {
	tick := time.NewTicker(15 * time.Second)
	ping := time.NewTicker(linkPingEvery)
	defer tick.Stop()
	defer ping.Stop()
	for {
		m.drainOnce(ctx, s)
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-s.wake:
		case <-tick.C:
		case <-ping.C:
			if !s.ping(ctx) {
				s.shutdown()
				return
			}
		}
	}
}

func (m *linkManager) drainOnce(ctx context.Context, s *linkSession) {
	for {
		claim, err := m.st.ClaimOutbox(s.remote, time.Now())
		if err != nil {
			m.host.Logf("tincan: claim outbox %s: %v", s.remote, err)
			return
		}
		if claim == nil {
			return
		}
		if m.st.Expired(claim.Msg, time.Now()) {
			reason := claim.Msg.LastError
			if reason == "" {
				reason = "expired"
			}
			if err := m.st.Kill(claim, reason); err != nil {
				m.host.Logf("tincan: expire outbox %s: %v", claim.Msg.ID, err)
				return
			}
			m.bounce(claim.Msg, reason)
			continue
		}
		f := MsgFrame{T: "msg", ID: claim.Msg.ID, From: s.qualifyFrom(claim.Msg.From), To: claim.Msg.To, ReplyTo: claim.Msg.ReplyTo, Body: claim.Msg.Body, TS: claim.Msg.TS.UTC().Format(time.RFC3339), TTLSeconds: claim.Msg.TTLSeconds}
		response, err := s.requestAck(ctx, f)
		if err != nil {
			if releaseErr := m.st.Release(claim, err.Error(), 0); releaseErr != nil {
				m.host.Logf("tincan: release outbox %s: %v", claim.Msg.ID, releaseErr)
			}
			return
		}
		if response.T == "ack" {
			if err := m.st.Ack(claim); err != nil {
				m.host.Logf("tincan: ack outbox %s: %v", claim.Msg.ID, err)
			}
			continue
		}
		if permanentNak(response.Code) {
			if err := m.st.Kill(claim, response.Code); err != nil {
				m.host.Logf("tincan: dead-letter outbox %s: %v", claim.Msg.ID, err)
				continue
			}
			m.bounce(claim.Msg, response.Code)
			continue
		}
		if err := m.st.Release(claim, response.Code, 0); err != nil {
			m.host.Logf("tincan: release outbox %s: %v", claim.Msg.ID, err)
		}
		return
	}
}

func permanentNak(code string) bool {
	switch code {
	case "bad_address", "reserved_name", "body_too_large", "not_permitted", "no_route":
		return true
	default:
		return false
	}
}

func (m *linkManager) bounce(original *Msg, code string) {
	to, _, err := parseAddr(original.From)
	if err != nil {
		m.host.Logf("tincan: cannot bounce %s: %v", original.ID, err)
		return
	}
	ttlSeconds := m.cfg.TTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 86400
	}
	addr := joinAddr(original.To, original.Host)
	bounce := &Msg{
		ID:         newID(),
		From:       joinAddr(reservedName, m.cfg.Host),
		To:         to,
		Body:       fmt.Sprintf("undeliverable: %s — %s", addr, code),
		TS:         time.Now(),
		TTLSeconds: ttlSeconds,
	}
	if err := m.st.EnqueueLocal(bounce); err != nil {
		m.host.Logf("tincan: enqueue bounce for %s: %v", original.ID, err)
	}
}

func (m *linkManager) dialPeer(peer Peer) {
	backoff := time.Second
	for m.ctx.Err() == nil {
		err := m.dialOnce(peer)
		if err != nil && m.ctx.Err() == nil {
			m.setLastError(peer.Host, err.Error())
			m.host.Logf("tincan: link %s: %v", peer.Host, err)
		}
		if m.ctx.Err() != nil {
			return
		}
		t := time.NewTimer(backoff)
		select {
		case <-m.ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (m *linkManager) setLastError(host, message string) {
	m.mu.Lock()
	m.lastErr[host] = message
	m.mu.Unlock()
}

func (m *linkManager) dialOnce(peer Peer) error {
	argv := peer.Dial
	if len(argv) == 0 {
		ssh := os.Getenv("TINCAN_SSH")
		if ssh == "" {
			ssh = "ssh"
		}
		argv = []string{ssh, "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3", peer.SSH, peer.RemoteBin() + " link"}
	}
	if len(argv) == 0 {
		return fmt.Errorf("no dial command")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	stream := &commandStream{r: stdout, w: stdin, cmd: cmd}
	reader := newFrameReader(stream)
	if err := writeFrame(stream, MsgFrame{T: "hello", Host: m.cfg.Host, You: peer.Host, Proto: linkProto, Ver: version}); err != nil {
		stream.Close()
		return m.commandError(peer.Host, err, &stderr, cmd)
	}
	answer, err := reader.next()
	if err != nil {
		stream.Close()
		return m.commandError(peer.Host, err, &stderr, cmd)
	}
	if err := verifyHelloOK(peer.Host, answer); err != nil {
		stream.Close()
		return m.commandError(peer.Host, err, &stderr, cmd)
	}
	s := m.newSession(stream, peer.Host, "outbound", m.cfg.Host)
	m.runEstablished(m.ctx, s, reader)
	return m.commandError(peer.Host, io.EOF, &stderr, cmd)
}

func (m *linkManager) commandError(host string, prior error, stderr *bytes.Buffer, cmd *exec.Cmd) error {
	// commandStream.Close kills the child to break a blocked stdio read; Wait is
	// still required to release its resources and obtain ssh's exit status.
	waitErr := cmd.Wait()
	message := strings.TrimSpace(stderr.String())
	notInstalled := strings.Contains(strings.ToLower(message), "command not found")
	if exitErr, ok := waitErr.(*exec.ExitError); ok && exitErr.ExitCode() == 127 {
		notInstalled = true
	}
	if notInstalled {
		m.host.Logf("tincan not installed on %s", host)
	}
	if message != "" {
		return fmt.Errorf("%w: %s", prior, message)
	}
	if waitErr != nil && prior == io.EOF {
		return waitErr
	}
	return prior
}

// commandStream adapts a child process's separate stdio pipes to the single
// stream interface used by a link session. Closing it makes a reconnect prompt.
type commandStream struct {
	r    io.ReadCloser
	w    io.WriteCloser
	cmd  *exec.Cmd
	once sync.Once
}

func (s *commandStream) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *commandStream) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *commandStream) Close() error {
	s.once.Do(func() {
		_ = s.w.Close()
		_ = s.r.Close()
		if s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
		}
	})
	return nil
}

// qualifyFrom restates a local sender address with the name this link knows us
// by. The store records what the daemon called itself; the wire must carry what
// the peer can route back to.
func (s *linkSession) qualifyFrom(from string) string {
	agent, host, err := parseAddr(from)
	if err != nil || s.localName == "" || host == s.localName {
		return from
	}
	if host != "" && host != s.manager.host.LocalHost() {
		return from
	}
	return joinAddr(agent, s.localName)
}

func (s *linkSession) send(f MsgFrame) error {
	select {
	case <-s.done:
		return fmt.Errorf("link down")
	default:
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return writeFrame(s.conn, f)
}

func (s *linkSession) shutdown() {
	s.close.Do(func() {
		close(s.done)
		_ = s.conn.Close()
		s.mu.Lock()
		// Waiting callers also select on done, so channels need not be closed.
		// Leaving them alone prevents a receive-side race from panicking while
		// attempting to notify a request that is concurrently being torn down.
		s.acks = map[string]chan MsgFrame{}
		s.rosters = map[string]chan MsgFrame{}
		s.mu.Unlock()
	})
}

func (s *linkSession) requestAck(ctx context.Context, f MsgFrame) (MsgFrame, error) {
	ch := make(chan MsgFrame, 1)
	if !s.registerAck(f.ID, ch) {
		return MsgFrame{}, fmt.Errorf("link down")
	}
	defer s.forgetAck(f.ID)
	if err := s.send(f); err != nil {
		return MsgFrame{}, err
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case reply, ok := <-ch:
		if !ok {
			return MsgFrame{}, fmt.Errorf("link down")
		}
		return reply, nil
	case <-ctx.Done():
		return MsgFrame{}, ctx.Err()
	case <-s.done:
		return MsgFrame{}, fmt.Errorf("link down")
	case <-timer.C:
		return MsgFrame{}, fmt.Errorf("link acknowledgement timeout")
	}

}
func (s *linkSession) registerAck(id string, ch chan MsgFrame) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return false
	default:
		s.acks[id] = ch
		return true
	}
}
func (s *linkSession) forgetAck(id string) { s.mu.Lock(); delete(s.acks, id); s.mu.Unlock() }
func (s *linkSession) deliverAck(f MsgFrame) {
	s.mu.Lock()
	ch := s.acks[f.ID]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- f:
		default:
		}
	}
}
func (s *linkSession) registerRoster(id string, ch chan MsgFrame) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return false
	default:
		s.rosters[id] = ch
		return true
	}
}
func (s *linkSession) forgetRoster(id string) { s.mu.Lock(); delete(s.rosters, id); s.mu.Unlock() }
func (s *linkSession) deliverRoster(f MsgFrame) {
	s.mu.Lock()
	ch := s.rosters[f.RID]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- f:
		default:
		}
	}
}
func (s *linkSession) deliverPong(n int) {
	s.mu.Lock()
	ch := s.acks[fmt.Sprintf("ping-%d", n)]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- MsgFrame{T: "pong", N: n}:
		default:
		}
	}
}
func (s *linkSession) ping(ctx context.Context) bool {
	n := int(time.Now().UnixNano())
	id := fmt.Sprintf("ping-%d", n)
	ch := make(chan MsgFrame, 1)
	if !s.registerAck(id, ch) {
		return false
	}
	defer s.forgetAck(id)
	if s.send(MsgFrame{T: "ping", N: n}) != nil {
		return false
	}
	for range 2 {
		select {
		case <-ch:
			return true
		case <-time.After(linkPingEvery):
		case <-ctx.Done():
			return false
		case <-s.done:
			return false
		}
	}
	return false
}
