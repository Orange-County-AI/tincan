// tincan: session-to-session messaging for Claude Code sessions on one
// machine, via named mailboxes on the filesystem. Two tin cans and a string.
//
// One binary, two roles:
//   - `tincan serve`  the MCP channel server Claude Code spawns (stdio);
//     drains the mailbox named by TINCAN_MAILBOX
//   - CLI             send/list, so any shell or script can message a session
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

const version = "0.1.0"

const usage = `tincan %s - session-to-session messaging for Claude Code, via named mailboxes

Usage:
  tincan serve                                   run as MCP channel server (stdio);
                                                 drains the TINCAN_MAILBOX mailbox
  tincan send TO MESSAGE [--from NAME] [--reply-to ID]
  tincan list                                    all mailboxes, listening status, backlog

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
		err = cmdList()
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
	msg, err := enqueue(to, sender, body, *replyTo)
	if err != nil {
		return err
	}
	fmt.Printf("Message %s spooled to %q from %q.\n", msg.ID, to, sender)
	return nil
}

func cmdList() error {
	boxes, err := listBoxes()
	if err != nil {
		return err
	}
	if len(boxes) == 0 {
		fmt.Println("No mailboxes exist yet.")
		return nil
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
	return nil
}
