# tincan

Session-to-session messaging for Claude Code sessions on one machine, via
**named mailboxes** on the filesystem. Two tin cans and a string: no network
listener, no external service, no daemon.

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

- `from` — the sender's mailbox name; reply with `send_message(to: <from>)`.
- `event_id` — unique per message; use as an idempotency key (delivery is
  at-least-once) and as the `reply_to` of a reply.
- `reply_to` — present when the sender was replying to one of yours.

## Notes & semantics

- **Durable, uncoalesced, ordered by queue time.** Every message is its own
  spool file; a backlog delivers in full when the session reconnects.
- **At-least-once**: claim (rename) → notify → ack (delete); a crash
  mid-delivery redelivers.
- **One listener per mailbox**: two `serve` processes mounting the same
  mailbox would each receive a random subset (claims are atomic). Isolation
  is by configuration discipline — one mailbox name per launch path.
- **Trust**: local-only, no network listener. Anything running as your user
  can send, and `from` is self-declared — the same trust boundary as your
  shell. The server instructions tell Claude to treat message bodies as
  peer information, not operator instructions.
- **Names**: lowercase letters, digits, hyphens, ≤41 chars. Mailboxes are
  created on first send or first serve.
- `TINCAN_DATA_DIR` overrides the state root (`~/.local/share/tincan`);
  `TINCAN_POLL_SECONDS` the poll interval (default 2s).
