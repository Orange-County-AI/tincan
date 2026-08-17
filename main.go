package main

import (
	"fmt"
	"io"
	"os"
)

const version = "0.5.2"

const usage = `Usage: tincan <command> [arguments]

Agent-to-agent messaging through the local tincan daemon.

Commands:
  daemon                         Run the per-host daemon in the foreground
  send TO MESSAGE [--reply-to ID] [--from NAME]
                                 Send a message to a local or peer agent
  agents [--host HOST] [--json]  List discoverable agents
  peers [--json]                 Show configured and connected peers
  read ID [--json]               Read a delivered message by id
  name NAME                      Claim a stable name for this herdr pane
  whoami [--json]                Show this pane's tincan address
  status [--json]                Show daemon status and queue counts
  inbox [--pane ID] [--json] [--watch]
                                 Show pending local messages or run the inbox pane
  pause [--on|--off|--toggle]    Pause, resume, or toggle delivery
  link                           Bridge standard input/output to the daemon link
  mcp                            Run the stdio MCP server
  version                        Print the tincan version
  help                           Print this help
`

func printUsage(w io.Writer) {
	fmt.Fprint(w, usage)
}

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stderr)
		os.Exit(2)
	}

	command := os.Args[1]
	if command == "version" || command == "--version" || command == "-v" {
		fmt.Println(buildVersion())
		return
	}
	if command == "help" || command == "--help" || command == "-h" {
		printUsage(os.Stdout)
		return
	}

	var err error
	switch command {
	case "daemon":
		err = cmdDaemon(os.Args[2:])
	case "send":
		err = cmdSend(os.Args[2:])
	case "agents":
		err = cmdAgents(os.Args[2:])
	case "peers":
		err = cmdPeers(os.Args[2:])
	case "read":
		err = cmdRead(os.Args[2:])
	case "name":
		err = cmdName(os.Args[2:])
	case "whoami":
		err = cmdWhoami(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	case "inbox":
		err = cmdInbox(os.Args[2:])
	case "pause":
		err = cmdPause(os.Args[2:])
	case "link":
		err = cmdLink(os.Args[2:])
	case "mcp":
		err = cmdMCP(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", command)
		printUsage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
