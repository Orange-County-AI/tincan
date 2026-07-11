---
name: tincan
description: >
  Message other Claude Code sessions on this machine via named tincan
  mailboxes — durable session-to-session messaging with no external service.
  Use when the user says "tincan", asks to message/notify/ping another
  session or orchestrator (e.g. clem, jessica, the kaneo bot), wants
  sessions to talk to each other, or asks to wire a session up with a
  mailbox or check who is listening.
---

# tincan — session-to-session messaging (named mailboxes)

tincan lets Claude Code sessions on this machine message one another through
named filesystem mailboxes. Durable (messages wait for offline sessions),
at-least-once, no network, no daemon. Sibling of everloop — same spool
protocol, but peer messaging instead of schedules.

- **Repo & source:** `~/projects/52labs/tincan`
- **Binary:** `~/.local/bin/tincan` (rebuild: `cd ~/projects/52labs/tincan && go build -o ~/.local/bin/tincan .`)
- **State:** `~/.local/share/tincan/<mailbox>/`

## Sending

If this session has the tincan channel connected, prefer the MCP tools:
`send_message` (`to`, `message`, optional `reply_to`) and `list_peers`.
From a shell (works regardless):

```bash
tincan send jessica "deploy finished: v1.2.3" --from ci
tincan list          # every mailbox: listening? last seen, pending backlog
```

Sends to a mailbox nobody is draining succeed and wait — check `tincan list`
if you want to know whether it will be read now or later.

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

Names: lowercase letters, digits, hyphens (≤41 chars). See the repo README
for delivery semantics (at-least-once, `event_id` idempotency, `reply_to`
threading).
