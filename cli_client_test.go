package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestRunInboxEmptyQueueExits(t *testing.T) {
	var out bytes.Buffer
	err := runInbox(strings.NewReader("q\n"), &out, "", "clem@titan", func(req map[string]any) (map[string]any, error) {
		if req["op"] != "inbox" {
			t.Fatalf("request = %#v", req)
		}
		return map[string]any{"ok": true, "paused": false, "draft_holds": []any{}, "rows": []any{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "tincan inbox — clem@titan — delivering") || !strings.Contains(got, "no messages waiting") {
		t.Fatalf("frame = %q", got)
	}
}

func TestRunInboxPauseRerendersPaused(t *testing.T) {
	var out bytes.Buffer
	var pauseRequests []map[string]any
	paused := false
	err := runInbox(strings.NewReader("p\nq\n"), &out, "", "clem@titan", func(req map[string]any) (map[string]any, error) {
		switch req["op"] {
		case "inbox":
			return map[string]any{"ok": true, "paused": paused, "draft_holds": []any{}, "rows": []any{}}, nil
		case "pause":
			pauseRequests = append(pauseRequests, req)
			paused = valueBool(req, "paused")
			return map[string]any{"ok": true, "paused": paused}, nil
		default:
			t.Fatalf("request = %#v", req)
			return nil, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pauseRequests) != 1 || !valueBool(pauseRequests[0], "paused") {
		t.Fatalf("pause requests = %#v", pauseRequests)
	}
	if got := out.String(); !strings.Contains(got, "tincan inbox — clem@titan — delivering  [paused]") || !strings.Contains(got, "\npaused\n") {
		t.Fatalf("frames = %q", got)
	}
}

func TestRunInboxErrorRendersAndStillExits(t *testing.T) {
	var out bytes.Buffer
	err := runInbox(strings.NewReader("q\n"), &out, "", "clem@titan", func(map[string]any) (map[string]any, error) {
		return nil, errors.New("daemon restarting")
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "error: daemon restarting") || !strings.Contains(got, "[enter] refresh   [p] pause/resume   [q] quit") {
		t.Fatalf("frame = %q", got)
	}
}

// A body the shell split into words used to be sent as its first word, with the
// remainder discarded and the caller told it succeeded. Refusing is the fix:
// silent truncation of a message is worse than a failed send.
func TestSendRefusesExtraPositionalsInsteadOfTruncating(t *testing.T) {
	// A fake socket so a future refactor that moves the guard after the daemon
	// call cannot make this test send anything real.
	startMCPDaemon(t, func(map[string]any) map[string]any {
		t.Error("a refused command reached the daemon")
		return map[string]any{"ok": true}
	})
	err := cmdSend([]string{"clem", "first sentence.", "second", "sentence."})
	if err == nil {
		t.Fatal("cmdSend accepted a shell-split body")
	}
	if !strings.Contains(err.Error(), "exactly one message") || !strings.Contains(err.Error(), "quote the body") {
		t.Fatalf("error = %q, want it to name the cause and the fix", err)
	}
	// Every sibling subcommand already guards this way; send was the exception.
	for name, call := range map[string]func([]string) error{
		"agents": cmdAgents, "status": cmdStatus, "inbox": cmdInbox, "pause": cmdPause,
	} {
		if err := call([]string{"stray"}); err == nil {
			t.Errorf("%s accepted a stray positional", name)
		}
	}
}

func TestSendFlagsStillParseAfterTheGuard(t *testing.T) {
	// A fake daemon socket, because cmdSend really sends: without this the test
	// enqueues junk to a live agent and depends on production state.
	sent := make(chan map[string]any, 4)
	startMCPDaemon(t, func(request map[string]any) map[string]any {
		sent <- request
		return map[string]any{"ok": true, "id": "test-id", "route": "local"}
	})
	// The guard must not reject the legitimate flag forms, which sit after the
	// body and are what fs.Parse(args[2:]) exists for.
	for _, args := range [][]string{
		{"clem", "body", "--reply-to", "abc123"},
		{"clem", "body"},
	} {
		if err := cmdSend(args); err != nil {
			t.Fatalf("cmdSend(%v) rejected a valid form: %v", args, err)
		}
	}
	close(sent)
	var bodies []string
	for request := range sent {
		body, _ := request["body"].(string)
		bodies = append(bodies, body)
	}
	if len(bodies) != 2 || bodies[0] != "body" || bodies[1] != "body" {
		t.Fatalf("bodies sent = %#v, want the whole body once per valid form", bodies)
	}
}

func TestSendRefusesFromInsideAPane(t *testing.T) {
	// --from names a sender, so inside a pane it would let a caller impersonate
	// another agent; identity comes from the pane instead.
	t.Setenv("HERDR_PANE_ID", "w7V:p1")
	startMCPDaemon(t, func(map[string]any) map[string]any {
		t.Error("cmdSend reached the daemon with --from inside a pane")
		return map[string]any{"ok": true}
	})
	err := cmdSend([]string{"clem", "body", "--from", "ci"})
	if err == nil || !strings.Contains(err.Error(), "--from is not allowed inside a herdr pane") {
		t.Fatalf("error = %v", err)
	}
}

func TestSendEchoFlattensTheBodyToOneLine(t *testing.T) {
	if got := sendEcho("first line\n\nsecond   line\t"); got != "first line second line" {
		t.Fatalf("sendEcho = %q", got)
	}
}
