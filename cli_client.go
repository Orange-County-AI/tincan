package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func cmdSend(args []string) error {
	if len(args) < 2 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: tincan send TO MESSAGE [--reply-to ID] [--from NAME]")
	}
	to, body := args[0], args[1]
	fs := newFlagSet("send")
	replyTo := fs.String("reply-to", "", "message id being answered")
	from := fs.String("from", "", "sender name outside a herdr pane")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	paneID := os.Getenv("HERDR_PANE_ID")
	if paneID != "" && *from != "" {
		return fmt.Errorf("--from is not allowed inside a herdr pane: identity comes from the pane")
	}
	res, err := daemonCall(map[string]any{"op": "send", "to": to, "body": body, "reply_to": *replyTo, "from": *from, "pane_id": paneID})
	if err != nil {
		return err
	}
	return printResult(res, false, func(res map[string]any) string {
		text := fmt.Sprintf("Message %s to %q via %s.", valueString(res, "id"), to, valueString(res, "route"))
		if warn := valueString(res, "warn"); warn != "" {
			text += "\n" + warn
		}
		return text
	})
}

func cmdAgents(args []string) error {
	fs := newFlagSet("agents")
	host := fs.String("host", "", "host to list")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: tincan agents [--host H] [--json]")
	}
	res, err := daemonCall(map[string]any{"op": "agents", "host": *host})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, renderAgents)
}

func renderAgents(res map[string]any) string {
	lines := []string{}
	localOnlySeen := false
	if rows, ok := res["agents"].([]any); ok {
		for _, row := range rows {
			agent, _ := row.(map[string]any)
			if agent == nil {
				continue
			}
			detail := valueString(agent, "title")
			if detail == "" {
				detail = valueString(agent, "cwd")
			}
			addr := valueString(agent, "addr")
			// Never present a local-only label as if it were an address. The row
			// says so where the mistake would be made, and the footer says what to
			// use instead — addressing is per link, so a row cannot answer it.
			if valueBool(agent, "local_only") {
				localOnlySeen = true
				addr += "  [local-only]"
			}
			lines = append(lines, fmt.Sprintf("%s  %s  %s  %s", addr, valueString(agent, "kind"), valueString(agent, "status"), detail))
		}
	}
	if rows, ok := res["reply_only"].([]any); ok && len(rows) > 0 {
		for _, row := range rows {
			sender, _ := row.(map[string]any)
			if sender == nil {
				continue
			}
			lines = append(lines, fmt.Sprintf("%s  reply-only  wrote to you over an inbound link from %s; not enumerable", valueString(sender, "addr"), valueString(sender, "host")))
		}
	}
	if rows, ok := res["hosts"].([]any); ok {
		for _, row := range rows {
			host, _ := row.(map[string]any)
			if host == nil || valueBool(host, "ok") {
				continue
			}
			lines = append(lines, fmt.Sprintf("%s: %s", valueString(host, "host"), valueString(host, "error")))
		}
	}
	if localOnlySeen {
		lines = append(lines, renderWireNameNote(res))
	}
	return strings.Join(lines, "\n")
}

// renderWireNameNote states the per-link rule once, naming each link's form. A
// caller that cannot be told a routable address is told exactly that.
func renderWireNameNote(res map[string]any) string {
	names, _ := res["wire_names"].([]any)
	if len(names) == 0 {
		return "note: [local-only] addresses are this host's own name and no peer can route them; no link is up, so nothing here is reachable from another host."
	}
	parts := []string{}
	for _, value := range names {
		name, _ := value.(map[string]any)
		if name == nil {
			continue
		}
		origin := "announced to " + valueString(name, "peer")
		if valueString(name, "direction") == "inbound" {
			origin = "named by " + valueString(name, "peer")
		}
		parts = append(parts, fmt.Sprintf("@%s (%s)", valueString(name, "addr"), origin))
	}
	return "note: [local-only] addresses are this host's own name and no peer can route them. Per link this host is " + strings.Join(parts, ", ") + "; addressing is per link, so use the form for the link that peer reaches you on — or just reply to the from address on a message. `tincan whoami` shows your own."
}

func cmdRead(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: tincan read ID [--json]")
	}
	id := args[0]
	fs := newFlagSet("read")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: tincan read ID [--json]")
	}
	res, err := daemonCall(map[string]any{"op": "read", "id": id})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, func(res map[string]any) string {
		return fmt.Sprintf("%s  %s  %s", valueString(res, "from"), valueString(res, "ts"), valueString(res, "body"))
	})
}

func cmdName(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: tincan name NAME [--json]")
	}
	name := args[0]
	fs := newFlagSet("name")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: tincan name NAME [--json]")
	}
	res, err := daemonCall(map[string]any{"op": "name", "pane_id": os.Getenv("HERDR_PANE_ID"), "name": name})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, func(res map[string]any) string { return valueString(res, "addr") })
}

func cmdWhoami(args []string) error {
	fs := newFlagSet("whoami")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: tincan whoami [--json]")
	}
	res, err := daemonCall(map[string]any{"op": "whoami", "pane_id": os.Getenv("HERDR_PANE_ID")})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, renderWhoami)
}

// renderWhoami answers per link on purpose. A single line would invite a reader
// to treat one name as globally meaningful, which is how a dead address gets
// handed to a third party.
func renderWhoami(res map[string]any) string {
	local, _ := res["local"].(map[string]any)
	localAddr, localRoutable := "", false
	if local != nil {
		localAddr = valueString(local, "addr")
		localRoutable = valueBool(local, "routable")
	}
	addresses, _ := res["addresses"].([]any)
	lines := []string{}
	for _, value := range addresses {
		entry, _ := value.(map[string]any)
		if entry == nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s  (%s, %s link)", valueString(entry, "addr"), valueString(entry, "how"), valueString(entry, "direction")))
	}
	switch {
	case len(lines) == 0:
		lines = append(lines, fmt.Sprintf("local: %s — no peer link is up, so no address of yours is routable from another host yet", localAddr))
	case localRoutable:
		lines = append(lines, fmt.Sprintf("local: %s — same name this host announces", localAddr))
	default:
		lines = append(lines, fmt.Sprintf("local: %s — this host's own name, not routable off-box", localAddr))
	}
	lines = append(lines, "addressing is per link: give a peer the address for the link it reaches you on, or simply reply to the from address on a message.")
	return strings.Join(lines, "\n")
}

func cmdStatus(args []string) error {
	fs := newFlagSet("status")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: tincan status [--json]")
	}
	res, err := daemonCall(map[string]any{"op": "status"})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, func(res map[string]any) string {
		herdr, _ := res["herdr"].(map[string]any)
		summary := fmt.Sprintf("%s: herdr %s protocol %s, %s agents; %s queued", valueString(res, "host"), valueString(herdr, "version"), valueNumber(herdr, "protocol"), valueNumber(herdr, "agents"), valueNumber(res, "queued"))
		for _, raw := range valueMaps(res, "draft_holds") {
			summary += fmt.Sprintf("\nholding %s since %s", valueString(raw, "pane_id"), valueString(raw, "at"))
		}
		return summary
	})
}

func cmdInbox(args []string) error {
	fs := newFlagSet("inbox")
	paneID := fs.String("pane", "", "limit to a herdr pane")
	jsonOut := fs.Bool("json", false, "print JSON")
	watch := fs.Bool("watch", false, "watch inbox in a pane")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || (*watch && *jsonOut) {
		return fmt.Errorf("usage: tincan inbox [--pane ID] [--json] [--watch]")
	}
	if *watch {
		localAddr := "local"
		if currentPane := os.Getenv("HERDR_PANE_ID"); currentPane != "" {
			if whoami, err := daemonCall(map[string]any{"op": "whoami", "pane_id": currentPane}); err == nil && daemonResponseError(whoami) == nil {
				if local, ok := whoami["local"].(map[string]any); ok && valueString(local, "addr") != "" {
					localAddr = valueString(local, "addr")
				}
			}
		}
		return runInbox(os.Stdin, os.Stdout, *paneID, localAddr, daemonCall)
	}
	res, err := daemonCall(map[string]any{"op": "inbox", "pane_id": *paneID})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, renderInboxRows)
}

func cmdPause(args []string) error {
	fs := newFlagSet("pause")
	on := fs.Bool("on", false, "pause delivery")
	off := fs.Bool("off", false, "resume delivery")
	toggle := fs.Bool("toggle", false, "toggle delivery pause")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || boolCount(*on, *off, *toggle) > 1 {
		return fmt.Errorf("usage: tincan pause [--on|--off|--toggle]")
	}
	paused := *on
	if !*on {
		if *off {
			paused = false
		} else {
			status, err := daemonCall(map[string]any{"op": "status"})
			if err != nil {
				return err
			}
			if err := daemonResponseError(status); err != nil {
				return err
			}
			paused = !valueBool(status, "paused")
		}
	}
	res, err := daemonCall(map[string]any{"op": "pause", "paused": paused})
	if err != nil {
		return err
	}
	return printResult(res, false, func(res map[string]any) string {
		if valueBool(res, "paused") {
			return "paused"
		}
		return "resumed"
	})
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func daemonResponseError(res map[string]any) error {
	if valueBool(res, "ok") {
		return nil
	}
	if message := valueString(res, "error"); message != "" {
		return fmt.Errorf("%s", message)
	}
	return fmt.Errorf("request failed")
}

type inboxCall func(map[string]any) (map[string]any, error)

func runInbox(in io.Reader, out io.Writer, paneID, localAddr string, call inboxCall) error {
	scanner := bufio.NewScanner(in)
	status := ""
	for {
		res, err := call(map[string]any{"op": "inbox", "pane_id": paneID})
		if err == nil {
			err = daemonResponseError(res)
		}
		if err != nil {
			renderInboxFrame(out, localAddr, nil, "error: "+err.Error())
		} else {
			renderInboxFrame(out, localAddr, res, status)
		}
		if !scanner.Scan() {
			return scanner.Err()
		}
		command := strings.TrimSpace(scanner.Text())
		switch command {
		case "q":
			return nil
		case "":
			status = ""
		case "p":
			if err != nil {
				status = "error: " + err.Error()
				continue
			}
			nextPaused := !valueBool(res, "paused")
			pausedRes, pauseErr := call(map[string]any{"op": "pause", "paused": nextPaused})
			if pauseErr == nil {
				pauseErr = daemonResponseError(pausedRes)
			}
			if pauseErr != nil {
				status = "error: " + pauseErr.Error()
			} else if valueBool(pausedRes, "paused") {
				status = "paused"
			} else {
				status = "resumed"
			}
		default:
			status = "unknown command: " + command
		}
	}
}

func renderInboxRows(res map[string]any) string {
	rows := valueMaps(res, "rows")
	if len(rows) == 0 {
		return "no messages waiting"
	}
	previewWidth := inboxPreviewWidth()
	lines := make([]string, 0, len(rows))
	for index, row := range rows {
		lines = append(lines, fmt.Sprintf("%3d  %-6s  %-20s  %-9s  %s",
			index+1, inboxAge(valueString(row, "ts")), truncateInbox(valueString(row, "from"), 20),
			valueString(row, "state"), truncateInbox(valueString(row, "preview"), previewWidth)))
	}
	return strings.Join(lines, "\n")
}

func renderInboxFrame(out io.Writer, localAddr string, res map[string]any, status string) {
	header := "tincan inbox — " + localAddr
	if res != nil {
		holds := valueMaps(res, "draft_holds")
		if len(holds) > 0 {
			header += fmt.Sprintf(" — held: pane %s (%s)", valueString(holds[0], "pane_id"), valueString(holds[0], "agent"))
		} else {
			header += " — delivering"
		}
		if valueBool(res, "paused") {
			header += "  [paused]"
		}
	} else {
		header += " — delivering"
	}
	fmt.Fprint(out, "\x1b[2J\x1b[H", header, "\n\n")
	if res != nil {
		rows := valueMaps(res, "rows")
		if len(rows) == 0 {
			fmt.Fprintln(out, "no messages waiting")
		} else {
			fmt.Fprintln(out, "  #  age     from                  state      preview")
			fmt.Fprintln(out, renderInboxRows(res))
		}
	}
	fmt.Fprint(out, "\n", status, "\n[enter] refresh   [p] pause/resume   [q] quit\n")
}

func inboxAge(raw string) string {
	then, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return "—"
	}
	age := time.Since(then)
	switch {
	case age < time.Minute:
		return strconv.FormatInt(int64(age.Seconds()), 10) + "s"
	case age < time.Hour:
		return strconv.FormatInt(int64(age.Minutes()), 10) + "m"
	case age < 24*time.Hour:
		return strconv.FormatInt(int64(age.Hours()), 10) + "h"
	default:
		return strconv.FormatInt(int64(age.Hours()/24), 10) + "d"
	}
}

func truncateInbox(text string, width int) string {
	runes := []rune(text)
	if len(runes) <= width {
		return text
	}
	if width <= 1 {
		return string(runes[:width])
	}
	return string(runes[:width-1]) + "…"
}

func inboxPreviewWidth() int {
	const defaultColumns = 100
	columns := defaultColumns
	if value, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && value > 0 {
		columns = value
	}
	// The fixed columns and their separating spaces consume 46 characters.
	if columns-46 < 1 {
		return 1
	}
	return columns - 46
}

func valueString(fields map[string]any, name string) string {
	value, _ := fields[name].(string)
	return value
}

func valueMaps(fields map[string]any, name string) []map[string]any {
	switch values := fields[name].(type) {
	case []map[string]any:
		return append([]map[string]any(nil), values...)
	case []any:
		out := make([]map[string]any, 0, len(values))
		for _, value := range values {
			if object, ok := value.(map[string]any); ok {
				out = append(out, object)
			}
		}
		return out
	default:
		return nil
	}
}

// valueStrings reads a JSON string array, tolerating both a decoded []any (the
// daemon response path) and a plain []string.
func valueStrings(fields map[string]any, name string) []string {
	switch value := fields[name].(type) {
	case []string:
		return value
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok && text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func valueBool(fields map[string]any, name string) bool {
	value, _ := fields[name].(bool)
	return value
}

func valueNumber(fields map[string]any, name string) string {
	switch value := fields[name].(type) {
	case float64:
		return fmt.Sprintf("%.0f", value)
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	case uint64:
		return fmt.Sprintf("%d", value)
	default:
		return "0"
	}
}
