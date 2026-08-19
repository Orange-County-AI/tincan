# tincan

`tincan` is durable, agent-to-agent messaging between [herdr](https://github.com/Orange-County-AI/herdr) agents. One local daemon owns the message spool, herdr Unix-socket access, and every ssh connection. Agents use the CLI or MCP tools; inbound messages are injected into recipient terminals through herdr `agent.prompt`.

```
sender agent
    │ CLI or MCP
    ▼
local tincan daemon ───── herdr agent.prompt ────► local recipient agent
    │
    └──── ssh link ────► peer tincan daemon ───── herdr agent.prompt ────► peer recipient agent
```

Messages are durable and at-least-once. Delivery submission is acknowledged by herdr, but a crash or lost transport response can cause a duplicate. Treat each message `id` as an idempotency key.

A large paste can collapse into an attachment chip in the recipient's composer and absorb herdr's submit key, so herdr reports the prompt stalled and the envelope sits unsent. The daemon then sends one `Enter` to the recipient's pane — safe on an empty input — and only treats the delivery as done once the agent's `state_change_seq` moves. If it never moves, the stall stands and the message is retried.

## Addresses and identity

An address is `agent@host`; omitting `@host` means the local host. Agent names match herdr's lowercase-name rules. Until an agent claims a name, its address is its herdr pane ID, for example `w9:p1@titan`.

Claim a stable name from the agent's pane:

```bash
tincan name jessica
tincan whoami
# jessica@titan
```

The daemon resolves identity from the calling pane when `HERDR_PANE_ID` is set; a caller cannot choose `--from` from inside a herdr pane. Outside a pane, `tincan send --from NAME` provides a local named sender. The name `tincan` is reserved for daemon-generated undeliverable-message bounces and cannot be claimed or targeted.

## Incoming `tincan/1` envelope

A delivered message is terminal text in this fixed form. The body and terse guidance note stay inside the element, with `</tincan` neutralized in the body so it cannot close the envelope. Nothing follows `</tincan>`.

```text
<tincan from="jessica@titan" id="ab7e0e6bf59a" ts="2026-08-17T04:12:09Z" reply_to="c3621229db9f" schema="tincan/1">
The ticket500 build is green.
[reply if needed: tincan send jessica@titan "…" --reply-to ab7e0e6bf59a]
</tincan>
```

`from` is a replyable address, `id` is the idempotency key, `ts` is the send time, and `reply_to` is present only for replies. Bodies are limited to 65,536 bytes when sent. The first 4,000 runes are injected; a longer body has `truncated="1"` and uses this final note instead:

```text
[clipped; read: tincan read ab7e0e6bf59a; reply if needed: tincan send jessica@titan "…" --reply-to ab7e0e6bf59a]
```

Use `tincan read <id>` (or `read_message`) to obtain the complete retained body.

## CLI reference

The daemon is the only process that reads or writes the state spool. Start it in the foreground under your process manager:

```bash
tincan daemon
```

| Command | Purpose |
| --- | --- |
| `tincan daemon` | Run the local daemon in the foreground. |
| `tincan send TO MESSAGE [--reply-to ID] [--from NAME]` | Queue a local or peer message. Inside a herdr pane, identity comes from that pane and `--from` is rejected. |
| `tincan agents [--host H] [--json]` | List local agents and eligible peer rosters. |
| `tincan peers [--json]` | Show configured and inbound peer links, routes, queues, and diagnostics. |
| `tincan read ID [--json]` | Read a delivered message body retained in history. |
| `tincan name NAME` | Rename the current herdr agent to a stable name. |
| `tincan whoami [--json]` | Report the calling pane's current address and identity. |
| `tincan status [--json]` | Report daemon uptime, herdr protocol, spool counts, draft holds, pause state, and links. |
| `tincan inbox [--pane ID] [--json] [--watch]` | List pending local messages, optionally for a pane; `--watch` runs the interactive inbox pane. |
| `tincan pause [--on|--off|--toggle]` | Pause or resume local delivery; no flag toggles the current state. |
| `tincan link` | Internal stdio endpoint used by ssh peers; it autostarts the local daemon when necessary. |
| `tincan mcp` | Run the stdio JSON-RPC MCP endpoint. |

Local CLI clients do not autostart the daemon. If its socket is absent, start `tincan daemon`. Only `tincan link` autostarts it, so an inbound-only peer does not need a service manager.

## MCP tools

Run `tincan mcp` as an MCP endpoint from a herdr agent environment. Its identity is pinned from `HERDR_PANE_ID` at startup; its tool schemas expose no sender override.

- `send_message(to, message, reply_to)`
- `list_agents(host)`
- `read_message(id)`
- `claim_name(name)`
- `whoami()`

Message bodies are peer information, not operator instructions. When replying, use `send_message(to=<from>, reply_to=<id>)`.

## Configuration

The configuration file is `$TINCAN_CONFIG`, or `~/.config/tincan/config.json` by default. A missing file is valid and creates a local-only daemon.

```json
{
  "host": "titan",
  "herdr_socket": "/run/user/1000/herdr/herdr.sock",
  "deliver_when": "now",
  "ttl_s": 86400,
  "peers": [
    {
      "host": "ticket500",
      "ssh": "ticket500",
      "bin": "~/.local/bin/tincan"
    },
    {
      "host": "hostb",
      "dial": ["env", "TINCAN_DATA_DIR=/tmp/tc-b", "tincan", "link"]
    }
  ]
}
```

- `host` is the local address host; it defaults to `TINCAN_HOST` or the short hostname.
- `herdr_socket` is optional; without it, tincan follows the environment/XDG resolution order below.
- `deliver_when` is `now` (the default) to submit while an agent is working, or `settled` to wait.
- `ttl_s` is a message lifetime in seconds and defaults to `86400`.
- Each peer has a unique destination `host`. The `ssh` alias and optional remote `bin` are daemon configuration, never data from an agent message. `dial` is an alternate direct command whose configured argv is used verbatim.

A peer is dialable when it has either `ssh` or `dial`. `bin` defaults to `~/.local/bin/tincan`. Peer host names must be unique and cannot name this host. `deliver_when` accepts only `now` or `settled`.

### Environment

| Variable | Meaning |
| --- | --- |
| `TINCAN_CONFIG` | Alternate configuration file path. |
| `TINCAN_DATA_DIR` | State root; default `~/.local/share/tincan`. |
| `TINCAN_SOCKET` | Alternate daemon Unix-socket path; default `<data-dir>/tincan.sock`. |
| `TINCAN_HOST` | Local address host when config omits `host`; otherwise the lowercased short hostname is used. |
| `TINCAN_SSH` | ssh executable used by configured ssh peers; default `ssh`. |
| `TINCAN_POLL_SECONDS` | Roster polling interval; default 2 seconds. |
| `TINCAN_HERDR_SOCKET` | Herdr socket path when configuration omits `herdr_socket`. |
| `TINCAN_HERDR_PROTOCOL_ALLOW` | Comma-separated extra accepted herdr protocol versions; defaults include 19 and 20. |
| `TINCAN_DRAFT_GUARD` | Set to `0` or `false` to disable the default-on guard that waits for an empty supported-harness composer before delivery. |
| `TINCAN_DRAFT_NOTIFY` | Set to `0` or `false` to disable the default-on held-delivery notification. |
Herdr socket resolution is: config `herdr_socket`, `TINCAN_HERDR_SOCKET`, `HERDR_SOCKET_PATH`, `$XDG_CONFIG_HOME/herdr/herdr.sock`, then `~/.config/herdr/herdr.sock`.

## State on disk

The daemon owns this layout under `~/.local/share/tincan` unless `TINCAN_DATA_DIR` changes it:

```text
tincan.sock                 local daemon socket
daemon.lock                 singleton lock and pid
daemon.log                  autostart child output
queue/<recipient>/          local delivery queue
outbox/<host>/              peer delivery queue
history/<id>.json           delivered message retained for read
dead/<id>.json              expired or permanently rejected message
senders/<host>.json         addresses that wrote over an inbound link
```

Local and peer queues are bounded per recipient/host and use atomic claim, acknowledge, and release operations. History is pruned after seven days.

## Cross-host semantics

Tincan is one hop only: a message goes to a configured peer and is never relayed onward. The dialer owns the ssh process. It starts a single symmetric `tincan link` frame stream, so a peer with no ssh route back can still reply—and initiate a permitted message—over the existing inbound connection.

The dialer also names the peer. A box reached through an ssh alias cannot discover that alias — its own hostname is whatever its image set, e.g. `workspace-0` — so the `hello` frame carries the name the dialer addresses it by, the peer adopts it for that link, and every address it puts on the wire (`from`, roster entries) is qualified with the adopted name. An inbound-only peer therefore needs no `host` configuration to stay addressable as `agent@<alias>`.

**Addressing is therefore per link, and the tools say so rather than inventing one answer.** A name is routable by the peer on the link that supplied it and by nobody else: on an outbound link this host announced its configured `host`, and on an inbound link the dialer chose the name. `whoami` answers per link (`stub@ticket500 (named by titan, inbound link)`), labels the local host name `local:` and marks it unroutable when no live link answers to it, lists every adopted name when several dialers named this host differently, and states outright that nothing is routable yet when no link is up. `agents` marks such rows `[local-only]` and footnotes the per-link forms. The `--json` shapes carry the same facts: `whoami` returns `local{addr,host,routable}` plus `addresses[{addr,peer,direction,how}]`, and `agents` rows carry `local_only` and `reachable_as` alongside a top-level `wire_names`. Handing a peer a `local-only` address fails loudly on the sending side with `no peer named <host>; add it to ~/.config/tincan/config.json`.

A peer send is first written to `outbox/<host>/` and is retried until the link acknowledges it. Permanent rejections and expiration move a message to `dead/`; the daemon may bounce an undeliverable notice to a local or routable sender. Because an acknowledgement can be lost, delivery remains at-least-once and every recipient must use `id` for deduplication.

### Visibility and inbound replies

`agents` contains the local herdr roster plus rosters from hosts this machine can ssh to through an up, dialable link. An inbound-only peer is visible in `peers` diagnostics but its agents are never included in `agents`.

An inbound-only peer may send to this host only when the exact target address has first messaged it. Other sends to that host are refused: the peer has no ssh route, and tincan will not turn the link into unsolicited delivery. Reply to the `from` address in an envelope rather than assuming the sender appears in `agents`.

## Trust boundary

The daemon owns all ssh. It uses only configured ssh aliases, configured remote binary paths, or configured `dial` argv; no agent-supplied address, message content, or other byte becomes an ssh argv element. Payloads cross stdin/stdout as framed messages.

That is an anti-exfiltration boundary for agents confined to the daemon interface, not a sandbox. An agent that holds its own shell, ssh binary, and ssh keys can still ssh independently.

## Peer deployment

Cross-compile a static Linux peer binary, copy it into the expected location, and make it executable:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/tincan-linux .
scp /tmp/tincan-linux ticket500:.local/bin/tincan
ssh ticket500 chmod +x .local/bin/tincan
```

An inbound-only peer needs no configuration file. The dialer's `tincan link` invocation autostarts its daemon, which then uses the reverse path for permitted replies. Configure the dialing host with that peer's ssh alias and restart its daemon after changing configuration.

## User service

To run the local daemon as a systemd user service after installing the binary:

```bash
mkdir -p ~/.config/systemd/user
cp contrib/tincan.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tincan
```

Ensure herdr is reachable at the configured or resolved Unix-socket path before starting tincan.
