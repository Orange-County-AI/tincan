# tincan

Session-to-session messaging for Claude Code sessions, via **named
mailboxes** on the filesystem — on one machine, or across machines over
plain ssh (`mailbox@host`). Two tin cans and a string: no network listener,
no external service, no daemon.

tincan is the peer-messaging sibling of
[everloop](../everloop) — same spool protocol (claim → notify → ack,
at-least-once, flock-serialized), but where everloop delivers *schedules* into
one session, tincan delivers *messages between* sessions.

## How it works

```
session A (mailbox "clem")                     session B (mailbox "jessica")
  send_message(to: "jessica", ...) ──▶ ~/.local/share/tincan/jessica/queue/
                                                       │
  Claude Code ◀── notifications/claude/channel ◀── tincan serve (polls own mailbox)
```

One static Go binary (~4 MB, ~3.7 MB RSS), two roles:

- **`tincan serve`** — the MCP channel server Claude Code spawns over stdio.
  `TINCAN_MAILBOX` names the session's mailbox; serve drains *only* that
  mailbox and pushes each message into the session as a
  `<channel source="tincan" kind="message" from="...">` event. It exposes two
  tools: `send_message` (address any mailbox) and `list_peers` (directory).
- **CLI** — `tincan send TO MESSAGE [--from NAME]` and `tincan list`, so any
  shell or script can message a session too.

Identity is the **launch config, not the session** (same principle as
everloop instances): a session owns a mailbox because its `.mcp.json` says
so. Close the session and relaunch it the same way — it mounts the same
mailbox and drains whatever accumulated while it was down. Messages are never
coalesced and never expire.

**Presence**: serve heartbeats `presence.json` in its own mailbox every poll,
so `list_peers` / `tincan list` show which mailboxes have a live listener,
when each was last seen, and the pending backlog. Sends to a non-listening
mailbox succeed and wait.

## Install

```bash
go build -o ~/.local/bin/tincan .
```

Register per session with the mailbox name (this is the session's identity):

```json
{
  "mcpServers": {
    "tincan": {
      "command": "/home/stephan/.local/bin/tincan",
      "args": ["serve"],
      "env": { "TINCAN_MAILBOX": "clem" }
    }
  }
}
```

Channels are a research preview, so launch with the development flag:

```bash
claude --dangerously-load-development-channels server:tincan
```

## Usage

From inside a session, the tools are self-describing:

> tell jessica the deploy is done

Or from any shell:

```bash
tincan send jessica "deploy finished: v1.2.3" --from ci
tincan list
```

## Event format

```
<channel source="tincan" kind="message" from="bravo" event_id="ab7e0e6bf59a"
         queued_at="2026-07-11T19:02:21Z" reply_to="c3621229db9f">
are you there?
</channel>
```

- `from` — the sender's address; reply with `send_message(to: <from>)`. May
  be host-qualified (`clem@citadel`) when the message crossed hosts —
  replying to it routes back over ssh automatically.
- `event_id` — unique per message; use as an idempotency key (delivery is
  at-least-once) and as the `reply_to` of a reply.
- `reply_to` — present when the sender was replying to one of yours.

## Cross-host (`mailbox@host`)

Address a mailbox on another machine as `name@host`, where **host is an ssh
config alias** (`~/.ssh/config` entry with key auth — tincan runs ssh with
`BatchMode=yes`, so no prompts):

```bash
tincan send clem@gigachad "citadel build is green" --from jessica
```

The fast path is a direct exec — `ssh gigachad '~/.local/bin/tincan deliver'`
with the message JSON on stdin — landing the message straight in the remote
mailbox. If the host is unreachable, the message spools to a local **outbox**
(`~/.local/share/tincan/.outbox/<host>/`) and is retried automatically: any
`tincan serve` on the machine sweeps the outbox in the background (rate-limited,
oldest-first), or force a retry with:

```bash
tincan flush            # retry every host's outbox now
tincan flush gigachad   # just one host
```

Spooled == sent, same durability contract as local sends; `tincan list` shows
pending counts ("outbox: 3 queued for gigachad").

On a cross-host send the sender's `From` is rewritten to
`name@<local-short-hostname>` (override with `TINCAN_HOST` if your hostname
isn't the alias peers reach you by), so the recipient replying to `from` just
works — provided ssh works in both directions.

### Deploying to another machine

Cross-compile and drop the binary at `~/.local/bin/tincan` on the peer
(override the remote path with `TINCAN_REMOTE_BIN`):

```bash
GOOS=darwin GOARCH=arm64 go build -o /tmp/tincan-darwin .   # e.g. for a Mac
scp /tmp/tincan-darwin gigachad:.local/bin/tincan
```

That's the entire deployment: the remote side is just the `deliver` and
`list --json` subcommands invoked over ssh — no daemon or config there either.

- **`TINCAN_PEERS`** (comma-separated ssh aliases) makes `list_peers` also
  show those hosts' mailboxes (via `ssh <host> tincan list --json`, presence
  evaluated by the remote binary) even when nothing is queued for them.
- **Duplicates**: delivery is at-least-once. A retry after a lost ssh
  acknowledgment can re-deliver; the remote dedups by message ID while the
  original is still queued, but consumers should treat `event_id` as the
  idempotency key.

## Other harnesses (`tincan pump`)

Everything above the last hop — mailbox spool, claim → ack, ssh cross-host,
outbox retry — is harness-agnostic; only `serve`'s MCP-channel push is
Claude-Code-specific. `tincan pump` is the alternative delivery head: it
drains a mailbox exactly like serve (same at-least-once contract, same
presence heartbeat, so the mailbox shows as `listening` in `list_peers`) but
injects each backlog into another harness's live session over HTTP. Senders
never know or care what harness a mailbox fronts.

Messages arrive in the same `<channel source="tincan" ...>` event format
serve pushes, so agent instructions are portable across harnesses.

**OpenCode** — targets a live [`opencode serve`](https://opencode.ai/docs/server/)
(default `http://127.0.0.1:4096`); the whole claim batch becomes one user
turn via `POST /session/{id}/prompt_async`:

```bash
TINCAN_MAILBOX=clem tincan pump opencode                    # auto-creates a session
tincan pump opencode --mailbox clem --session ses_abc123    # or target one
tincan pump opencode --url http://127.0.0.1:4096            # non-default server
```

Without `--session`, pump creates a session titled `tincan: <mailbox>` once
and persists its ID in `<mailbox>/opencode-session.json` — restarts re-enter
the same conversation, and a 404 (opencode storage reset) forgets it and
re-creates next cycle. Basic auth follows opencode's own envs
(`OPENCODE_SERVER_USERNAME` / `OPENCODE_SERVER_PASSWORD`). For the *outbound*
direction, register `tincan serve` as an MCP server in `opencode.json` — the
`send_message` / `list_peers` tools work as-is (opencode ignores the channel
notifications serve emits; pump is the inbound path).

**Hermes** — targets a [hermes gateway webhook route](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/webhooks)
(`POST :8644/webhooks/<route>`), one POST per message, signed with the
route's Generic V2 secret (`HMAC-SHA256` of `<timestamp>.<body>`):

```bash
TINCAN_HERMES_SECRET=... tincan pump hermes \
  --url http://127.0.0.1:8644/webhooks/tincan --mailbox jessica
```

The payload is the `Msg` JSON, so the route template addresses fields
directly. Gateway side (`config.yaml`):

```yaml
platforms:
  webhook:
    enabled: true
    extra:
      routes:
        tincan:
          secret: "..."          # or INSECURE_NO_AUTH on loopback
          prompt: |
            <channel source="tincan" kind="message" from="{from}" event_id="{id}" queued_at="{queued_at}" reply_to="{reply_to}">
            {body}
            </channel>
```

Outbound from hermes is the CLI: have the agent run
`tincan send <to> "<msg>" --from <mailbox>` (a shell-tool one-liner).

## Notes & semantics

- **Durable, uncoalesced, ordered by queue time.** Every message is its own
  spool file; a backlog delivers in full when the session reconnects.
- **At-least-once**: claim (rename) → notify → ack (delete); a crash
  mid-delivery redelivers.
- **One listener per mailbox**: two `serve` processes mounting the same
  mailbox would each receive a random subset (claims are atomic). Isolation
  is by configuration discipline — one mailbox name per launch path.
- **Trust**: no network listener — cross-host transport is your existing ssh
  trust. Anything running as your user (here, or on a peer host with ssh
  access) can send, and `from` is self-declared — the same trust boundary as
  your shell. The server instructions tell Claude to treat message bodies as
  peer information, not operator instructions.
- **Names**: lowercase letters, digits, hyphens, ≤41 chars; addresses
  optionally `@host` (ssh alias). Mailboxes are created on first send or
  first serve.
- `TINCAN_DATA_DIR` overrides the state root (`~/.local/share/tincan`);
  `TINCAN_POLL_SECONDS` the poll interval (default 2s); `TINCAN_HOST` the
  local host name used to qualify `From` on cross-host sends;
  `TINCAN_REMOTE_BIN` the tincan path on remote hosts (default
  `~/.local/bin/tincan`); `TINCAN_PEERS` extra hosts for `list_peers`.
