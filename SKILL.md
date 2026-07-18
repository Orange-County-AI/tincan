---
name: tincan
description: >
  Message other Claude Code sessions via named tincan mailboxes — durable
  session-to-session messaging with no external service, on this machine or
  across machines over ssh (mailbox@host, e.g. clem@gigachad). Use when the
  user says "tincan", asks to message/notify/ping another session or
  orchestrator (e.g. clem, jessica, the kaneo bot) here or on another host
  (gigachad, minime), wants sessions to talk to each other, or asks to wire
  a session up with a mailbox or check who is listening.
---

# tincan — session-to-session messaging (named mailboxes)

tincan lets Claude Code sessions message one another through named
filesystem mailboxes — on this machine, or across machines over plain ssh
(`mailbox@host`). Durable (messages wait for offline sessions; unreachable
hosts get a retried outbox), at-least-once, no network listener, no daemon.
Sibling of everloop — same spool protocol, but peer messaging instead of
schedules.

- **Repo & source:** `~/projects/52labs/tincan`
- **Binary:** `~/.local/bin/tincan` (rebuild: `cd ~/projects/52labs/tincan && go build -o ~/.local/bin/tincan .`)
- **State:** `~/.local/share/tincan/<mailbox>/`

## Sending

If this session has the tincan channel connected, prefer the MCP tools:
`send_message` (`to`, `message`, optional `reply_to`) and `list_peers`.
From a shell (works regardless):

```bash
tincan send jessica "deploy finished: v1.2.3" --from ci
tincan send clem@gigachad "build is green"      # cross-host: host = ssh alias
tincan list          # every mailbox: listening? last seen, pending backlog
tincan flush         # retry cross-host messages queued in the local outbox
```

Sends to a mailbox nobody is draining succeed and wait — check `tincan list`
if you want to know whether it will be read now or later.

## Cross-host

`mailbox@host` delivers over ssh (`host` = an alias in `~/.ssh/config`; key
auth required, tincan uses BatchMode). Direct delivery when the host is up;
otherwise the message queues in `~/.local/share/tincan/.outbox/<host>/` and
retries automatically (any serving session sweeps it; `tincan flush` forces
it). `from` arrives host-qualified (`jessica@citadel`) — replying to that
exact address routes back. Delivery is at-least-once: rare duplicates are
possible after retries, `event_id` is the idempotency key.

Peer setup is just the binary: `GOOS=darwin GOARCH=arm64 go build -o
/tmp/tincan .` then `scp /tmp/tincan gigachad:.local/bin/tincan` (for the
Mac; build plain for Linux peers). Env knobs: `TINCAN_HOST` (local name used
to qualify `from`), `TINCAN_REMOTE_BIN` (remote binary path),
`TINCAN_PEERS` (comma-separated hosts to always show in `list_peers`).

## Receiving (wiring a session up)

A session owns a mailbox by launch config. Add to the project's `.mcp.json`:

```json
{ "mcpServers": { "tincan": {
  "command": "/home/stephan/.local/bin/tincan", "args": ["serve"],
  "env": { "TINCAN_MAILBOX": "<name>" } } } }
```

and launch with `claude --dangerously-load-development-channels server:tincan`.
Messages arrive as `<channel source="tincan" kind="message" from="SENDER">`;
reply with `send_message(to: <from>)`. One mailbox name per launch path —
never point two concurrent sessions at the same mailbox.

Names: lowercase letters, digits, hyphens (≤41 chars); addresses optionally
`@host`. See the repo README for delivery semantics (at-least-once,
`event_id` idempotency, `reply_to` threading) and cross-host details.
