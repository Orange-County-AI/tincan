package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type Msg struct {
	ID          string    `json:"id"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Host        string    `json:"host,omitempty"`
	Body        string    `json:"body"`
	ReplyTo     string    `json:"reply_to,omitempty"`
	TS          time.Time `json:"ts"`
	TTLSeconds  int       `json:"ttl_s"`
	Attempts    int       `json:"attempts,omitempty"`
	LastAttempt time.Time `json:"last_attempt,omitzero"`
	LastError   string    `json:"last_error,omitempty"`
	LastSeq     uint64    `json:"last_seq,omitempty"`
}

type Claimed struct {
	Msg  *Msg
	Path string
}

type Store struct{ root string }

type deadMessage struct {
	Msg    *Msg      `json:"msg"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

type senderList struct {
	Addrs []string `json:"addrs"`
}

func OpenStore(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("store root is required")
	}
	for _, dir := range []string{"queue", "outbox", "history", "dead", "senders"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return nil, fmt.Errorf("create store %s: %w", dir, err)
		}
	}
	return &Store{root: root}, nil
}

func newID() string {
	var bytes [6]byte
	// crypto/rand failures are exceptional; matching the prior spool's
	// contract still guarantees the fixed 12-character hexadecimal shape.
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}

func (s *Store) Root() string { return s.root }

func (s *Store) queueDir(key string) string   { return filepath.Join(s.root, "queue", key) }
func (s *Store) outboxDir(host string) string { return filepath.Join(s.root, "outbox", host) }

func (s *Store) withLock(fn func() error) error {
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(s.root, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return fn()
}

func writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func messageFilename(m *Msg) string {
	return "msg-" + m.TS.UTC().Format("20060102T150405") + "-" + m.ID + ".json"
}

func claimedFilename(name string) string {
	return "claimed-" + strings.TrimPrefix(name, "msg-")
}

func pendingNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if (strings.HasPrefix(name, "msg-") || strings.HasPrefix(name, "claimed-")) && strings.HasSuffix(name, ".json") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func countPending(dir string) (int, error) {
	names, err := pendingNames(dir)
	return len(names), err
}

func hasID(dir, id string) (bool, error) {
	for _, name := range []string{"msg-*-" + id + ".json", "claimed-*-" + id + ".json"} {
		matches, err := filepath.Glob(filepath.Join(dir, name))
		if err != nil {
			return false, err
		}
		if len(matches) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func validateSpoolMessage(m *Msg) error {
	if m == nil {
		return codedErrorf("bad_address", "message is required")
	}
	to, host, err := parseAddr(m.To)
	if err != nil {
		return err
	}
	if host != "" {
		return codedErrorf("bad_address", "message target must not include a host")
	}
	m.To = to
	if len(m.Body) > maxBodyBytes {
		return codedErrorf("body_too_large", "message body exceeds %d bytes", maxBodyBytes)
	}
	return nil
}

func (s *Store) enqueue(m *Msg, dir string) error {
	if err := validateSpoolMessage(m); err != nil {
		return err
	}
	if m.ID == "" {
		m.ID = newID()
	}
	if m.TS.IsZero() {
		m.TS = time.Now().UTC()
	}
	return s.withLock(func() error {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		duplicate, err := hasID(dir, m.ID)
		if err != nil {
			return err
		}
		if duplicate {
			return nil
		}
		count, err := countPending(dir)
		if err != nil {
			return err
		}
		if count >= 100 {
			return codedErrorf("queue_full", "queue is full")
		}
		return writeJSON(filepath.Join(dir, messageFilename(m)), m)
	})
}

func (s *Store) EnqueueLocal(m *Msg) error {
	if m == nil {
		return codedErrorf("bad_address", "message is required")
	}
	if _, host, err := parseAddr(m.To); err != nil {
		return err
	} else if host != "" || m.Host != "" {
		return codedErrorf("bad_address", "local message target must not include a host")
	}
	return s.enqueue(m, s.queueDir(queueKey(m.To)))
}

func (s *Store) EnqueueOutbox(m *Msg) error {
	if m == nil {
		return codedErrorf("bad_address", "message is required")
	}
	if !hostRe.MatchString(m.Host) {
		return codedErrorf("bad_address", "invalid outbox host %q", m.Host)
	}
	return s.enqueue(m, s.outboxDir(m.Host))
}

func subdirs(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, entry := range entries {
		if entry.IsDir() {
			out = append(out, entry.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) QueueKeys() ([]string, error)   { return subdirs(filepath.Join(s.root, "queue")) }
func (s *Store) OutboxHosts() ([]string, error) { return subdirs(filepath.Join(s.root, "outbox")) }

func readMsg(path string) (*Msg, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Msg
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Store) claim(dir string, now time.Time) (*Claimed, error) {
	var claimed *Claimed
	err := s.withLock(func() error {
		names, err := pendingNames(dir)
		if err != nil {
			return err
		}
		for _, name := range names {
			if !strings.HasPrefix(name, "msg-") {
				continue
			}
			path := filepath.Join(dir, name)
			m, err := readMsg(path)
			if err != nil {
				return fmt.Errorf("read queued message %s: %w", path, err)
			}
			if !s.Eligible(m, now) {
				continue
			}
			claimPath := filepath.Join(dir, claimedFilename(name))
			if err := os.Rename(path, claimPath); err != nil {
				return err
			}
			claimed = &Claimed{Msg: m, Path: claimPath}
			return nil
		}
		return nil
	})
	return claimed, err
}

func (s *Store) ClaimLocal(key string, now time.Time) (*Claimed, error) {
	return s.claim(s.queueDir(key), now)
}

func (s *Store) ClaimOutbox(host string, now time.Time) (*Claimed, error) {
	return s.claim(s.outboxDir(host), now)
}

func (s *Store) Ack(c *Claimed) error {
	if c == nil || c.Msg == nil || c.Path == "" {
		return fmt.Errorf("invalid claim")
	}
	return s.withLock(func() error {
		if err := os.Remove(c.Path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return writeJSON(filepath.Join(s.root, "history", c.Msg.ID+".json"), c.Msg)
	})
}

func (s *Store) Release(c *Claimed, lastErr string, lastSeq uint64) error {
	if c == nil || c.Msg == nil || c.Path == "" {
		return fmt.Errorf("invalid claim")
	}
	return s.withLock(func() error {
		c.Msg.Attempts++
		c.Msg.LastAttempt = time.Now().UTC()
		c.Msg.LastError = lastErr
		c.Msg.LastSeq = lastSeq
		name := messageFilename(c.Msg)
		newPath := filepath.Join(filepath.Dir(c.Path), name)
		if err := writeJSON(c.Path, c.Msg); err != nil {
			return err
		}
		if err := os.Rename(c.Path, newPath); err != nil {
			return err
		}
		c.Path = newPath
		return nil
	})
}

func (s *Store) Kill(c *Claimed, reason string) error {
	if c == nil || c.Msg == nil || c.Path == "" {
		return fmt.Errorf("invalid claim")
	}
	return s.withLock(func() error {
		if err := os.Remove(c.Path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return writeJSON(filepath.Join(s.root, "dead", c.Msg.ID+".json"), deadMessage{Msg: c.Msg, Reason: reason, At: time.Now().UTC()})
	})
}

func (s *Store) Eligible(m *Msg, now time.Time) bool {
	if m == nil || m.LastAttempt.IsZero() {
		return m != nil
	}
	attempt := m.Attempts
	if attempt < 1 {
		attempt = 1
	}
	wait := 2 * time.Second
	for range attempt - 1 {
		if wait >= 60*time.Second {
			wait = 60 * time.Second
			break
		}
		wait *= 2
	}
	if wait > 60*time.Second {
		wait = 60 * time.Second
	}
	return !now.Before(m.LastAttempt.Add(wait))
}

func (s *Store) Expired(m *Msg, now time.Time) bool {
	if m == nil || m.TTLSeconds <= 0 || m.TS.IsZero() {
		return false
	}
	return !now.Before(m.TS.Add(time.Duration(m.TTLSeconds) * time.Second))
}

func (s *Store) Counts() (queued int, outbox map[string]int, err error) {
	outbox = make(map[string]int)
	keys, err := s.QueueKeys()
	if err != nil {
		return 0, nil, err
	}
	for _, key := range keys {
		count, countErr := countPending(s.queueDir(key))
		if countErr != nil {
			return 0, nil, countErr
		}
		queued += count
	}
	hosts, err := s.OutboxHosts()
	if err != nil {
		return 0, nil, err
	}
	for _, host := range hosts {
		count, countErr := countPending(s.outboxDir(host))
		if countErr != nil {
			return 0, nil, countErr
		}
		outbox[host] = count
	}
	return queued, outbox, nil
}

func (s *Store) History(id string) (*Msg, error) {
	m, err := readMsg(filepath.Join(s.root, "history", id+".json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, codedErrorf("not_found", "message %q not found", id)
		}
		return nil, err
	}
	return m, nil
}

func (s *Store) PruneHistory(maxAge time.Duration) error {
	return s.withLock(func() error {
		entries, err := os.ReadDir(filepath.Join(s.root, "history"))
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		cutoff := time.Now().Add(-maxAge)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			path := filepath.Join(s.root, "history", entry.Name())
			m, err := readMsg(path)
			if err != nil {
				return err
			}
			if !m.TS.After(cutoff) {
				if err := os.Remove(path); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) ReclaimOrphans() error {
	return s.withLock(func() error {
		for _, parent := range []string{"queue", "outbox"} {
			keys, err := subdirs(filepath.Join(s.root, parent))
			if err != nil {
				return err
			}
			for _, key := range keys {
				dir := filepath.Join(s.root, parent, key)
				names, err := pendingNames(dir)
				if err != nil {
					return err
				}
				for _, name := range names {
					if !strings.HasPrefix(name, "claimed-") {
						continue
					}
					msgName := "msg-" + strings.TrimPrefix(name, "claimed-")
					if err := os.Rename(filepath.Join(dir, name), filepath.Join(dir, msgName)); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func (s *Store) senderPath(host string) string { return filepath.Join(s.root, "senders", host+".json") }

func (s *Store) RecordSender(host, addr string) error {
	if !hostRe.MatchString(host) {
		return codedErrorf("bad_address", "invalid sender host %q", host)
	}
	if strings.Count(addr, "@") != 1 {
		return codedErrorf("bad_address", "invalid sender address %q", addr)
	}
	agent, senderHost, _ := strings.Cut(addr, "@")
	if (!agentNameRe.MatchString(agent) && !paneIDRe.MatchString(agent)) || !hostRe.MatchString(senderHost) {
		return codedErrorf("bad_address", "invalid sender address %q", addr)
	}
	addr = joinAddr(agent, senderHost)
	return s.withLock(func() error {
		path := s.senderPath(host)
		var list senderList
		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && json.Unmarshal(data, &list) != nil {
			return fmt.Errorf("parse sender list %s", path)
		}
		for _, existing := range list.Addrs {
			if existing == addr {
				return nil
			}
		}
		list.Addrs = append(list.Addrs, addr)
		sort.Strings(list.Addrs)
		return writeJSON(path, list)
	})
}

func (s *Store) KnownSender(host, addr string) (bool, error) {
	data, err := os.ReadFile(s.senderPath(host))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	var list senderList
	if err := json.Unmarshal(data, &list); err != nil {
		return false, err
	}
	for _, existing := range list.Addrs {
		if existing == addr {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) KnownSenderCount(host string) int {
	data, err := os.ReadFile(s.senderPath(host))
	if err != nil {
		return 0
	}
	var list senderList
	if json.Unmarshal(data, &list) != nil {
		return 0
	}
	return len(list.Addrs)
}

// ReplyOnlySenders lists every address that has written to this host over a
// link, grouped by the host the link belongs to. These are exactly the addresses
// an inbound-only peer is allowed to answer, and the only routing information it
// has: it cannot enumerate a host it has no ssh route to.
func (s *Store) ReplyOnlySenders() (map[string][]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "senders"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string][]string{}, nil
		}
		return nil, err
	}
	senders := make(map[string][]string, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		host := strings.TrimSuffix(name, ".json")
		if !hostRe.MatchString(host) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.root, "senders", name))
		if err != nil {
			return nil, err
		}
		var list senderList
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("parse sender list %s: %w", name, err)
		}
		if len(list.Addrs) > 0 {
			senders[host] = list.Addrs
		}
	}
	return senders, nil
}
