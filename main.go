// tincan: session-to-session messaging for Claude Code sessions on one
// machine, via named mailboxes on the filesystem. Two tin cans and a string.
//
// One binary, two roles:
//   - `tincan serve`  the MCP channel server Claude Code spawns (stdio);
//     drains the mailbox named by TINCAN_MAILBOX
//   - CLI             send/list, so any shell or script can message a session
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

const version = "0.1.0"

const usage = `tincan %s - session-to-session messaging for Claude Code, via named mailboxes

Usage:
  tincan serve                                   run as MCP channel server (stdio);
                                                 drains the TINCAN_MAILBOX mailbox
  tincan send TO MESSAGE [--from NAME] [--reply-to ID]
                                                 TO is a mailbox, or mailbox@host
                                                 (ssh alias) for cross-host delivery
  tincan list [--json]                           all mailboxes, listening status, backlog
  tincan flush [HOST]                            retry cross-host messages queued in
                                                 the local outbox
  tincan pump opencode [--url BASE] [--session ID | --title T] [--mailbox NAME]
                                                 standalone drain of a mailbox into a
                                                 live "opencode serve" session over HTTP
  tincan pump hermes --url WEBHOOK_URL [--secret S] [--mailbox NAME]
                                                 standalone drain of a mailbox into a
                                                 hermes gateway webhook route

Delivery: serve picks its last hop via CHANNEL_SINK (claude|opencode|hermes,
or none for tools-only serve alongside a standalone pump); pump runs the same
drain loop without the MCP stdio side, for deployments where nothing mounts
serve.

Identity: TINCAN_MAILBOX names this session's mailbox (required for serve,
default --from for send). Names: lowercase letters, digits, hyphens (max 41 chars).
State: ~/.local/share/tincan/<mailbox>/  (TINCAN_DATA_DIR overrides the root)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}
	if err := checkMailboxEnv(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve()
	case "send":
		err = cmdSend(os.Args[2:])
	case "list":
		err = cmdList(os.Args[2:])
	case "flush":
		err = cmdFlush(os.Args[2:])
	case "pump": // alternative delivery head: drain a mailbox into a non-Claude harness
		err = cmdPump(os.Args[2:])
	case "deliver": // plumbing: remote hosts pipe a Msg JSON into this over ssh
		err = cmdDeliver()
	case "version", "--version", "-v":
		fmt.Println(version)
	case "help", "--help", "-h":
		fmt.Printf(usage, version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n"+usage, os.Args[1], version)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdSend(args []string) error {
	if len(args) < 2 || args[0] == "" || args[0][0] == '-' {
		return fmt.Errorf("usage: tincan send TO MESSAGE [--from NAME] [--reply-to ID]")
	}
	to, body := args[0], args[1]
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	from := fs.String("from", "", "sender mailbox name (default TINCAN_MAILBOX, else \"cli\")")
	replyTo := fs.String("reply-to", "", "event_id this message replies to")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	sender := *from
	if sender == "" {
		sender = mailboxName()
	}
	if sender == "" {
		sender = "cli"
	}
	msg, status, err := sendTo(to, sender, body, *replyTo)
	if err != nil {
		return err
	}
	fmt.Printf("Message %s to %q from %q: %s.\n", msg.ID, to, msg.From, status)
	return nil
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "machine-readable output (the cross-host wire format)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	boxes, err := listBoxes()
	if err != nil {
		return err
	}
	if *jsonOut {
		if boxes == nil {
			boxes = []BoxInfo{}
		}
		data, err := json.Marshal(boxes)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	if len(boxes) == 0 {
		fmt.Println("No mailboxes exist yet.")
	}
	for _, box := range boxes {
		status := "never seen listening"
		if box.Listening {
			status = "listening"
		} else if !box.LastSeen.IsZero() {
			status = "last seen " + box.LastSeen.Local().Format(time.RFC3339)
		}
		fmt.Printf("- %s: %s | pending=%d\n", box.Name, status, box.Pending)
	}
	for _, host := range outboxHosts() {
		if n := outboxPending(host); n > 0 {
			fmt.Printf("outbox: %d queued for %s (tincan flush %s to retry now)\n", n, host, host)
		}
	}
	return nil
}

// cmdDeliver is the receiving end of cross-host delivery: a Msg as JSON on
// stdin, enqueued into the local mailbox it names. Silent on success so the
// ssh exec fast path stays cheap; idempotent on message ID.
func cmdDeliver() error {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	var msg Msg
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("bad message JSON on stdin: %v", err)
	}
	return enqueueMsg(&msg)
}

func cmdFlush(args []string) error {
	hosts := outboxHosts()
	if len(args) > 0 {
		hosts = []string{args[0]}
	}
	if len(hosts) == 0 {
		fmt.Println("Outbox is empty.")
		return nil
	}
	for _, host := range hosts {
		sent, remaining, err := sweepOutbox(host)
		switch {
		case err != nil:
			fmt.Printf("%s: sent %d, %d still queued (%v)\n", host, sent, remaining, err)
		case sent == 0 && remaining == 0:
			fmt.Printf("%s: outbox empty\n", host)
		default:
			fmt.Printf("%s: sent %d, outbox clear\n", host, sent)
		}
	}
	return nil
}
