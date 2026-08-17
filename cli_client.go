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
			lines = append(lines, fmt.Sprintf("%s  %s  %s  %s", valueString(agent, "addr"), valueString(agent, "kind"), valueString(agent, "status"), detail))
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
	return strings.Join(lines, "\n")
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
	return printResult(res, *jsonOut, func(res map[string]any) string { return valueString(res, "addr") })
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
		return fmt.Sprintf("%s: herdr %s protocol %s, %s agents; %s queued", valueString(res, "host"), valueString(herdr, "version"), valueNumber(herdr, "protocol"), valueNumber(herdr, "agents"), valueNumber(res, "queued"))
	})
}

func valueString(fields map[string]any, name string) string {
	value, _ := fields[name].(string)
	return value
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
