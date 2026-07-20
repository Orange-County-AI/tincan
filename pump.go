package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// tincan pump — the standalone delivery head. serve already delivers through
// a pluggable sink (CHANNEL_SINK, sink.go), but serve is an MCP stdio server:
// something has to mount it and hold stdin open. Under opencode that's free —
// register `tincan serve` in opencode.json with CHANNEL_SINK=opencode and one
// process provides the send_message/list_peers tools AND injects inbound
// messages. Under hermes (or any harness that can't host an MCP server)
// nothing does, so `tincan pump` runs the same drain loop — claim -> deliver
// -> ack, presence heartbeat, outbox sweep — as a plain foreground process
// (e.g. a systemd user unit), with flags as sugar over the sink envs.
func cmdPump(args []string) error {
	box, dlv, err := pumpSetup(args)
	if err != nil {
		return err
	}
	if err := ensureBox(box); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "tincan: pumping mailbox %q via %s sink\n", box, os.Getenv("CHANNEL_SINK"))
	drainLoop(box, dlv) // never returns
	return nil
}

// pumpSetup parses `pump {opencode|hermes} [flags]` into a mailbox and a
// configured sink. Flags override the corresponding envs; anything not
// flagged falls through to the same envs serve's newSink reads.
func pumpSetup(args []string) (string, sink, error) {
	if len(args) < 1 || strings.HasPrefix(args[0], "-") {
		return "", nil, fmt.Errorf("usage: tincan pump {opencode|hermes} [flags]  (see tincan help)")
	}
	kind := args[0]
	fs := flag.NewFlagSet("pump", flag.ContinueOnError)
	mailbox := fs.String("mailbox", "", "mailbox to drain (default TINCAN_MAILBOX)")
	url := fs.String("url", "", "opencode: server base URL (OPENCODE_URL, default http://127.0.0.1:4096); hermes: full webhook route URL (HERMES_WEBHOOK_URL, required)")
	session := fs.String("session", "", "opencode: explicit session ID (OPENCODE_SESSION_ID; default: resolve/create by title)")
	title := fs.String("title", "", `opencode: session title to resolve or create (OPENCODE_SESSION_TITLE, default "tincan: <mailbox>")`)
	secret := fs.String("secret", "", "hermes: route secret for Generic V2 HMAC signing (HERMES_WEBHOOK_SECRET; empty = unsigned)")
	if err := fs.Parse(args[1:]); err != nil {
		return "", nil, err
	}
	box := *mailbox
	if box == "" {
		box = mailboxName()
	}
	if box == "" {
		return "", nil, fmt.Errorf("mailbox required: set TINCAN_MAILBOX or pass --mailbox")
	}
	if err := validName("mailbox", box); err != nil {
		return "", nil, err
	}
	setIf := func(key, val string) {
		if val != "" {
			os.Setenv(key, val)
		}
	}
	switch kind {
	case "opencode":
		setIf("OPENCODE_URL", *url)
		setIf("OPENCODE_SESSION_ID", *session)
		if *title == "" && os.Getenv("OPENCODE_SESSION_TITLE") == "" {
			// Identity = launch config: a stable per-mailbox title makes a pump
			// restart resolve back into the same opencode conversation.
			*title = "tincan: " + box
		}
		setIf("OPENCODE_SESSION_TITLE", *title)
	case "hermes":
		setIf("HERMES_WEBHOOK_URL", *url)
		setIf("HERMES_WEBHOOK_SECRET", *secret)
	default:
		return "", nil, fmt.Errorf("unknown pump sink %q (want opencode or hermes)", kind)
	}
	os.Setenv("CHANNEL_SINK", kind)
	dlv, err := newSink("tincan", nil)
	if err != nil {
		return "", nil, err
	}
	return box, dlv, nil
}
