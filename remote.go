package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Cross-host messaging: `mailbox@host` addresses a mailbox on another machine
// reachable as an ssh config alias. The fast path execs
// `ssh <host> tincan deliver` with the message JSON on stdin (never argv, so
// there is no remote-shell quoting). On failure the message is spooled into a
// local outbox and retried opportunistically. No daemon, no network listener.

// addrRe validates a full address: a mailbox name, optionally @host.
var addrRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,40}(@[a-z0-9][a-z0-9.-]{0,63})?$`)

// splitAddr splits "mailbox@host" on the last '@'; host is "" for bare names.
func splitAddr(to string) (mailbox, host string) {
	if i := strings.LastIndex(to, "@"); i >= 0 {
		return to[:i], to[i+1:]
	}
	return to, ""
}

// localHost is this machine's short name, used to qualify From on cross-host
// sends so the reply-to-`from` convention routes back automatically.
// TINCAN_HOST overrides (useful when the hostname is not the ssh alias peers
// know this machine by).
func localHost() string {
	if h := os.Getenv("TINCAN_HOST"); h != "" {
		return h
	}
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "localhost"
	}
	if i := strings.IndexByte(h, '.'); i >= 0 {
		h = h[:i]
	}
	return strings.ToLower(h)
}

// remoteBin is the tincan binary path on remote hosts (TINCAN_REMOTE_BIN
// overrides). Tilde is expanded by the remote shell.
func remoteBin() string {
	if b := os.Getenv("TINCAN_REMOTE_BIN"); b != "" {
		return b
	}
	return "~/.local/bin/tincan"
}

// errRemoteMissing distinguishes "host reachable but tincan not installed"
// (ssh exit 127) from generic unreachability.
var errRemoteMissing = errors.New("tincan not installed on remote host")

// remoteDeliver hands one message to a remote host's tincan over ssh, JSON on
// stdin. A var so tests can stub the ssh exec.
var remoteDeliver = func(host string, msg *Msg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, remoteBin()+" deliver")
	cmd.Stdin = bytes.NewReader(data)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		errText := strings.TrimSpace(stderr.String())
		var exit *exec.ExitError
		if errors.As(err, &exit) && (exit.ExitCode() == 127 || strings.Contains(errText, "command not found")) {
			return fmt.Errorf("%w %s: %s", errRemoteMissing, host, errText)
		}
		if errText != "" {
			return fmt.Errorf("ssh %s: %v: %s", host, err, errText)
		}
		return fmt.Errorf("ssh %s: %v", host, err)
	}
	return nil
}

// sendTo is the single send entry point for the CLI and MCP. Bare names take
// the existing local enqueue path. mailbox@host tries direct ssh delivery;
// on failure the message is spooled to the outbox (spooled == success:
// durable at-least-once, same contract as local sends) and the returned
// status says so honestly. err is reserved for invalid input or local I/O
// failure.
func sendTo(addr, from, body, replyTo string) (msg *Msg, status string, err error) {
	if !addrRe.MatchString(addr) {
		return nil, "", fmt.Errorf("invalid address %q: mailbox or mailbox@host, matching %s", addr, addrRe)
	}
	box, host := splitAddr(addr)
	if host == "" || host == localHost() {
		msg, err := enqueue(box, from, body, replyTo)
		if err != nil {
			return nil, "", err
		}
		status := "spooled; not currently listening, waits until a session mounts the mailbox"
		if boxListening(box) {
			status = "spooled; the mailbox is listening now"
		}
		return msg, status, nil
	}
	if body == "" {
		return nil, "", fmt.Errorf("message body is required")
	}
	if !strings.Contains(from, "@") {
		from += "@" + localHost()
	}
	if !addrRe.MatchString(from) {
		return nil, "", fmt.Errorf("invalid sender %q: must match %s", from, addrRe)
	}
	msg = &Msg{ID: randomID(), From: from, To: box, Body: body, ReplyTo: replyTo, QueuedAt: time.Now().UTC()}
	if derr := remoteDeliver(host, msg); derr != nil {
		if spoolErr := spoolOutbox(host, msg); spoolErr != nil {
			return nil, "", fmt.Errorf("remote delivery failed (%v) and outbox spool failed: %w", derr, spoolErr)
		}
		if errors.Is(derr, errRemoteMissing) {
			return msg, fmt.Sprintf("tincan not installed on %s (expected %s); queued locally, will retry", host, remoteBin()), nil
		}
		return msg, fmt.Sprintf("queued locally; %s unreachable, will retry", host), nil
	}
	return msg, "delivered to " + host, nil
}

// --- outbox --------------------------------------------------------------------
//
// <root>/.outbox/<host>/msg-<ts>-<id>.json — the dot prefix keeps it out of
// listBoxes, whose nameRe requires a leading [a-z0-9].

func outboxRoot() string           { return filepath.Join(rootDir(), ".outbox") }
func outboxDir(host string) string { return filepath.Join(outboxRoot(), host) }

func spoolOutbox(host string, msg *Msg) error {
	if err := os.MkdirAll(outboxDir(host), 0o755); err != nil {
		return err
	}
	name := "msg-" + msg.QueuedAt.Format("20060102T150405") + "-" + msg.ID + ".json"
	return writeJSON(filepath.Join(outboxDir(host), name), msg)
}

// sweepOutbox retries every spooled message for one host, oldest first,
// stopping at the first failure so rough FIFO order is preserved.
func sweepOutbox(host string) (sent, remaining int, err error) {
	entries, err := os.ReadDir(outboxDir(host))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	touchAttemptStamp(host)
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "msg-") && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // timestamp-prefixed filenames: oldest first
	for i, name := range names {
		path := filepath.Join(outboxDir(host), name)
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return sent, len(names) - i, rerr
		}
		var msg Msg
		if json.Unmarshal(data, &msg) != nil {
			os.Remove(path) // corrupt; drop it
			continue
		}
		if derr := remoteDeliver(host, &msg); derr != nil {
			return sent, len(names) - i, derr
		}
		os.Remove(path)
		sent++
	}
	return sent, 0, nil
}

// outboxPending counts messages queued for one host.
func outboxPending(host string) int {
	entries, err := os.ReadDir(outboxDir(host))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "msg-") {
			n++
		}
	}
	return n
}

// outboxHosts lists every host with an outbox directory, sorted.
func outboxHosts() []string {
	entries, err := os.ReadDir(outboxRoot())
	if err != nil {
		return nil
	}
	var hosts []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			hosts = append(hosts, e.Name())
		}
	}
	sort.Strings(hosts)
	return hosts
}

const sweepMinInterval = 30 * time.Second

var sweepInFlight sync.Mutex

// sweepAllOutboxes opportunistically retries every host's outbox. Cheap to
// fire every poll cycle: single-flighted in-process and rate-limited per host
// via the .last-attempt stamp file (skipped while the last attempt is fresh).
func sweepAllOutboxes() {
	if !sweepInFlight.TryLock() {
		return
	}
	defer sweepInFlight.Unlock()
	for _, host := range outboxHosts() {
		if outboxPending(host) == 0 {
			continue
		}
		if fi, err := os.Stat(attemptStampPath(host)); err == nil && time.Since(fi.ModTime()) < sweepMinInterval {
			continue
		}
		sweepOutbox(host) // best effort; failures stay spooled
	}
}

func attemptStampPath(host string) string {
	return filepath.Join(outboxDir(host), ".last-attempt")
}

func touchAttemptStamp(host string) {
	path := attemptStampPath(host)
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		_ = os.WriteFile(path, nil, 0o644)
	}
}

// --- remote directory ------------------------------------------------------------

// remotePeers lists the mailboxes on a remote host via `tincan list --json`.
func remotePeers(host string) ([]BoxInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, remoteBin()+" list --json")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ssh %s: %v", host, err)
	}
	var boxes []BoxInfo
	if err := json.Unmarshal(stdout.Bytes(), &boxes); err != nil {
		return nil, fmt.Errorf("bad list --json from %s: %v", host, err)
	}
	return boxes, nil
}

// remoteHosts is the set of hosts worth showing in list_peers: any with an
// outbox plus any in TINCAN_PEERS (comma-separated ssh aliases).
func remoteHosts() []string {
	set := map[string]bool{}
	for _, h := range outboxHosts() {
		set[h] = true
	}
	for _, h := range strings.Split(os.Getenv("TINCAN_PEERS"), ",") {
		if h = strings.TrimSpace(h); h != "" {
			set[h] = true
		}
	}
	delete(set, localHost())
	hosts := make([]string, 0, len(set))
	for h := range set {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	return hosts
}
