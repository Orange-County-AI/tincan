package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Msg is one spooled message awaiting delivery to a mailbox's session.
type Msg struct {
	ID       string    `json:"id"`
	From     string    `json:"from"`
	To       string    `json:"to"`
	Body     string    `json:"body"`
	ReplyTo  string    `json:"reply_to,omitempty"`
	QueuedAt time.Time `json:"queued_at"`
}

// Presence is the heartbeat a serving session writes into its own mailbox so
// peers can see who is currently listening.
type Presence struct {
	PID       int       `json:"pid"`
	Since     time.Time `json:"since"`
	UpdatedAt time.Time `json:"updated_at"`
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,40}$`)

// presenceFresh is how recently a mailbox's heartbeat must have been written
// for it to count as listening. serve refreshes it every poll (default 2s).
const presenceFresh = 15 * time.Second

// mailboxName is this process's identity: the mailbox `serve` drains and the
// default sender name. Set TINCAN_MAILBOX per session in .mcp.json.
func mailboxName() string { return os.Getenv("TINCAN_MAILBOX") }

func checkMailboxEnv() error {
	mb := mailboxName()
	if mb == "" || nameRe.MatchString(mb) {
		return nil
	}
	return fmt.Errorf("invalid TINCAN_MAILBOX %q: must match %s", mb, nameRe)
}

func rootDir() string {
	if d := os.Getenv("TINCAN_DATA_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "tincan")
}

func boxDir(name string) string   { return filepath.Join(rootDir(), name) }
func queueDir(name string) string { return filepath.Join(boxDir(name), "queue") }

func validName(what, name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid %s %q: must match %s", what, name, nameRe)
	}
	return nil
}

func ensureBox(name string) error {
	return os.MkdirAll(queueDir(name), 0o755)
}

// withBoxLock serializes queue mutations for one mailbox across processes
// (concurrent senders vs the draining serve).
func withBoxLock(name string, fn func() error) error {
	if err := ensureBox(name); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(boxDir(name), "queue.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func randomID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// enqueue spools one message into the target mailbox. The mailbox is created
// on first send; messages wait until a session mounts it. Never coalesced.
func enqueue(to, from, body, replyTo string) (*Msg, error) {
	if err := validName("mailbox", to); err != nil {
		return nil, err
	}
	if err := validName("sender", from); err != nil {
		return nil, err
	}
	if body == "" {
		return nil, fmt.Errorf("message body is required")
	}
	msg := &Msg{ID: randomID(), From: from, To: to, Body: body, ReplyTo: replyTo, QueuedAt: time.Now().UTC()}
	err := withBoxLock(to, func() error {
		name := "msg-" + msg.QueuedAt.Format("20060102T150405") + "-" + msg.ID + ".json"
		return writeJSON(filepath.Join(queueDir(to), name), msg)
	})
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// claimPending atomically claims every spooled message in a mailbox by
// renaming it to claimed-*. Claimed files left behind by a crash are
// re-claimed on the next call (at-least-once delivery).
func claimPending(box string) ([]claimed, error) {
	var out []claimed
	err := withBoxLock(box, func() error {
		entries, err := os.ReadDir(queueDir(box))
		if err != nil {
			return err
		}
		for _, e := range entries {
			name := e.Name()
			isNew := strings.HasPrefix(name, "msg-")
			isOrphan := strings.HasPrefix(name, "claimed-")
			if !isNew && !isOrphan {
				continue
			}
			path := filepath.Join(queueDir(box), name)
			if isNew {
				dst := filepath.Join(queueDir(box), "claimed-"+randomID()+".json")
				if err := os.Rename(path, dst); err != nil {
					continue
				}
				path = dst
			}
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var msg Msg
			if err := json.Unmarshal(data, &msg); err != nil {
				os.Remove(path) // corrupt; drop it
				continue
			}
			out = append(out, claimed{msg: msg, path: path})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].msg.QueuedAt.Before(out[j].msg.QueuedAt) })
	return out, err
}

type claimed struct {
	msg  Msg
	path string
}

// ack deletes a claimed message after it has been written to the session.
func (c claimed) ack() { os.Remove(c.path) }

// --- presence -----------------------------------------------------------------

func presencePath(name string) string { return filepath.Join(boxDir(name), "presence.json") }

// markPresence refreshes this mailbox's heartbeat; called every drain cycle.
func markPresence(name string, since time.Time) {
	_ = ensureBox(name)
	_ = writeJSON(presencePath(name), &Presence{PID: os.Getpid(), Since: since, UpdatedAt: time.Now().UTC()})
}

func readPresence(name string) *Presence {
	data, err := os.ReadFile(presencePath(name))
	if err != nil {
		return nil
	}
	var p Presence
	if json.Unmarshal(data, &p) != nil {
		return nil
	}
	return &p
}

// BoxInfo is one row of the peer directory.
type BoxInfo struct {
	Name      string
	Listening bool
	LastSeen  time.Time // zero if never served
	Pending   int
}

func listBoxes() ([]BoxInfo, error) {
	entries, err := os.ReadDir(rootDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var boxes []BoxInfo
	for _, e := range entries {
		if !e.IsDir() || !nameRe.MatchString(e.Name()) {
			continue
		}
		info := BoxInfo{Name: e.Name()}
		if p := readPresence(e.Name()); p != nil {
			info.LastSeen = p.UpdatedAt
			info.Listening = time.Since(p.UpdatedAt) < presenceFresh && processAlive(p.PID)
		}
		if qe, err := os.ReadDir(queueDir(e.Name())); err == nil {
			for _, q := range qe {
				if strings.HasPrefix(q.Name(), "msg-") || strings.HasPrefix(q.Name(), "claimed-") {
					info.Pending++
				}
			}
		}
		boxes = append(boxes, info)
	}
	sort.Slice(boxes, func(i, j int) bool { return boxes[i].Name < boxes[j].Name })
	return boxes, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}
