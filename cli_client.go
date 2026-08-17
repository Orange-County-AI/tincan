package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
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

func valueString(fields map[string]any, name string) string {
	value, _ := fields[name].(string)
	return value
}

func valueMaps(fields map[string]any, name string) []map[string]any {
	values, _ := fields[name].([]any)
	out := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if object, ok := value.(map[string]any); ok {
			out = append(out, object)
		}
	}
	return out
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
