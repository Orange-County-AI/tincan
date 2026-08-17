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
