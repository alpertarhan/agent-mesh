# Harness integrations

`agm install` connects each supported harness to the local daemon. Agents always use
the same `agm` CLI (see [cli.md](cli.md)) from their shell tool. What differs per
harness is how the session registers, how incoming mail reaches the agent, and whether
an idle agent is woken.

## Support matrix

| Harness | Target | Mechanism | Incoming mail while working | Idle wake | CLI identity |
|---|---|---|---|---|---|
| pi | `pi` | extension, subscribed socket connection | pushed immediately, as a steer message | yes: starts a turn | process ancestry |
| omp | `omp` | same extension as pi | same as pi | yes: starts a turn | process ancestry |
| opencode v2 | `opencode` | TUI plugin + `opencode api session.synthetic` | pushed immediately (steer) | yes: starts a turn | `-as <id>` (the agent is told its id) |
| Claude Code | `claude` | hooks | at the next prompt or tool call; `Stop` keeps the agent going | yes: async `Stop` waiter (`asyncRewake`) | process ancestry |
| Codex | `codex` | hooks + execpolicy rule; daemon uses the Codex app-server | via hooks, or the daemon joins the running turn | yes: daemon starts a turn through the app-server | `$CODEX_THREAD_ID` |
| crush | `crush` | `PreToolUse` hook | at the next tool call | only inside herdr: a one-line nudge is typed into the pane | process ancestry (`-as` if one crush hosts several sessions) |
| Antigravity CLI | `agy` | `PreInvocation`/`Stop` hooks | before each model call; `Stop` continues with mail | only inside herdr: nudge, as for crush | `$ANTIGRAVITY_CONVERSATION_ID` |

Two more targets install only the agent-facing skill (usage instructions, no delivery):

| Target | File | Read by |
|---|---|---|
| `skill` | `~/.agents/skills/agent-mesh/SKILL.md` | harnesses that read `~/.agents/skills` (crush, pi, omp, Codex, opencode) |
| `claude-skill` | `~/.claude/skills/agent-mesh/SKILL.md` | Claude Code, which reads only `~/.claude/skills` |

**herdr is optional.** Messaging, hooks, adapters and the Codex and Claude idle wake do
not need it. herdr is used only for `agm spawn` and for the crush/agy idle nudge. When a
harness runs inside a herdr pane (`HERDR_ENV=1`), its pane id is recorded and shown by
`agm list`. Codex hooks never record a pane.

## What `agm install` touches

All paths are under `$HOME`. A target is detected when its detection directory exists.
`agm install` with no arguments installs every detected target. `<bin>` is the `agm` on
your `PATH` if it is the running binary, otherwise the binary's absolute path.

| Target | Detected by | Mesh-owned file | Entries added to shared config |
|---|---|---|---|
| `pi` | `.pi/agent` | `.pi/agent/extensions/agent-mesh.ts` | none |
| `omp` | `.omp/agent` | `.omp/agent/extensions/agent-mesh.ts` | none |
| `opencode` | `.config/opencode` | `.config/opencode/agent-mesh/tui.js` | `.config/opencode/cli.json`: `"./agent-mesh"` in `plugins` |
| `claude` | `.claude` | none | `.claude/settings.json`: hooks `SessionStart`, `UserPromptSubmit`, `PostToolUse`, `Stop`, `SessionEnd` (`<bin> hook claude`), a second `Stop` hook `<bin> hook claude --wait` with `asyncRewake` (timeout 3600 s), and `permissions.allow` entries for the messaging and read-only verbs only (`Bash(agm send *)`, `Bash(agm list)`, ... for `agm` and `<bin>`) |
| `codex` | `.codex` | `.codex/rules/agent-mesh.rules` | `.codex/hooks.json`: the same five hook events (`<bin> hook codex`) |
| `crush` | `.config/crush` | none | `.config/crush/crushrc`: `hook add PreToolUse --command "<bin> hook crush" --name agm` |
| `agy` | `.gemini/antigravity-cli` | none | `.gemini/config/hooks.json`: top-level key `agent-mesh` with `PreInvocation` and `Stop` (`<bin> hook agy <event>`, timeout 10 s); `.gemini/antigravity-cli/settings.json`: one anchored `permissions.allow` regex rule |
| `skill` | `.agents/skills` | `.agents/skills/agent-mesh/SKILL.md` | none |
| `claude-skill` | `.claude` | `.claude/skills/agent-mesh/SKILL.md` | none |

Claude and Codex hooks time out after 30 s, except `SessionEnd` (3 s).

How files are handled:

- **Owned files** contain `MESH_INTEGRATION_ID=<target>`. Install overwrites them.
  Uninstall deletes them and removes an empty `agent-mesh/` directory. A file at an owned
  path without that marker is reported as `foreign` and never written or deleted.
- **Shared configs** are edited in place. Mesh recognizes its own entries by content
  (for example a command `…/agm hook claude`), so a moved binary updates its entries
  instead of adding duplicates. Other entries are kept.
- Before a shared config changes, the previous content is saved to `<file>.bak`
  (overwriting any older `.bak`). Unchanged files are not rewritten.
- JSON configs are rewritten with 2-space indentation. Top-level key order is
  preserved; nested hook objects are re-serialized. A file that is not plain JSON (for
  example JSONC with comments) is refused with an error instead of being rewritten.
- `agm uninstall <target...>` removes only mesh entries. A config left with no entries
  stays in place (for example `{}` or an empty `crushrc`), next to its `.bak`.
- If a daemon is running on the current socket (`AGM_SOCKET` or the default),
  `agm install` restarts it so the daemon and adapters come from the same binary.
- Install ignores harness path overrides and always writes to the default paths above.
  Codex's `CODEX_HOME` is honored only by the spawn trust check and the app-server
  wake.

Check the result with `agm status`. After upgrading `agm`, run `agm install` again.
Re-running it is safe: it rewrites the same content and does not add duplicate entries.

## Permissions and trust

Agents need to run `agm` without a prompt for every message. Each harness gets a
different allowance:

| Harness | Pre-approved | Scope |
|---|---|---|
| Claude Code | `Bash(<p> <verb> *)` for `list`, `send`, `ask`, `reply`, `inbox`, `history`, `show`, `whoami`, `resolve`, `wait`, `ack` (plus bare `list`/`inbox`/`history`/`whoami`), `<p>` = `agm` and `<bin>` | those verbs only. `install`, `uninstall`, `spawn`, `restart`, `daemon` and the `*-file` verbs go through Claude's normal permission policy |
| Codex | `.codex/rules/agent-mesh.rules` `prefix_rule` | `agm`/`<bin>` followed by `list`, `send`, `ask`, `reply`, `inbox`, `history`, `show`, `whoami`, `resolve`, `wait` or `ack`; these run outside the Codex sandbox (the sandbox blocks the socket) |
| crush | decided per call by `agm hook crush` (`"decision": "allow"`) | exactly one `agm [-as ID] list\|send\|ask\|reply\|inbox\|history\|show\|whoami\|resolve\|wait\|ack ...` command. Pipes, chaining, redirects, substitutions or globs outside quotes fall back to crush's normal permission prompt |
| Antigravity CLI | anchored regex in `permissions.allow` | a single `agm`/`<bin>` `list\|send\|ask\|reply\|inbox\|history\|show\|whoami\|resolve\|wait\|ack` command without shell metacharacters |
| pi, omp, opencode | nothing | the harness's own shell-tool policy applies |

None of these pre-approve `status`, `send-file`, `ask-file` or `reply-file`; the last three read local files
(content into a message), so they prompt like any other file-reading command. `-ref`
sends only a path and is allowed.

Older versions installed broad Claude rules (`Bash(agm *)`, `Bash(<bin> *)`, `:*` forms).
`agm install claude` replaces them with the narrow rules above, and `agm uninstall
claude` removes both forms. It cannot tell a broad rule you added yourself with the same
text from the old one; any other broad rule of yours (for example `Bash(*)`) still
allows every `agm` command, and agent-mesh cannot revoke that.

**Codex hook review.** Codex runs new or changed hooks only after you trust them.
After `agm install codex`, and after any upgrade that changes the hooks, open Codex and
approve the agent-mesh hooks in `/hooks`. Until then no hook runs, including
`SessionEnd`, so Codex sessions are not registered and not cleaned up.

**Spawn trust.** `agm spawn` refuses to start Codex, Claude Code or Antigravity in a
directory (or git root) they have not trusted yet, so a typed task never answers their
trust dialog. Start the harness there once yourself, accept the prompt, then spawn.

## Per-harness details

### pi and omp

- One extension, `agent-mesh.ts`, for both. It registers on `session_start` with the
  harness PID, cwd and herdr pane, and sends `bye` on `session_shutdown`. It reconnects
  with backoff (up to 10 s) and starts the daemon if the socket is missing.
- Mail is pushed as an `agent_mesh` custom message. If the agent is idle it starts a
  turn; if it is busy it is delivered as a steer. The message is acked after it is
  handed to pi, so delivery is at-least-once. Acked means handed to the harness, not
  read or answered.
- Card: a native message renderer shows `MESSAGE`/`ASK`/`REPLY`, sender name, time, a
  short id and a peer-not-user label; collapsed shows the first lines, expanded the full
  body, file references and metadata (full id, reply command). If the host's TUI package
  cannot be loaded, the default rendering of the same text is used. The model sees the
  same text as before, plus file references.
- Status line (`setStatus`, key `agent-mesh`): `connecting…`, `connected as <name>`
  (only after the daemon accepted `hello`), `reconnecting…`, `unavailable (<reason>)`,
  plus `quiet (N waiting)`. Cleared on shutdown.
- `/mesh` menu (native select/editor dialogs, over the extension's own socket, no
  shell): peers → send or ask (the reply arrives as a card, nothing blocks), queued
  inbox (read without consuming), history → full message (and reply to a question),
  identity (paste the session id or `agm -as <id>` into the editor on request; the
  paste inserts rather than replacing your draft), quiet toggle, status.
  `/mesh quiet on|off` and `/mesh status` work without a UI.
- Quiet mode (opt-in: `/mesh quiet on`, or `AGM_QUIET=1` at start; off by default,
  per pi/omp process, kept across session switches): plain messages do not start or
  steer a turn. They stay unacked in the daemon (so they survive restarts) and join the
  next turn you start, as one card. They are acked only after that card was persisted
  (`message_end`) and the turn ended. Questions and replies still arrive at once.
  `/mesh quiet off` delivers waiting mail immediately. On a session switch, waiting
  mail stays queued for the old session. Hook-based harnesses have no quiet mode.
- Name: the pi/omp session name, re-sent when it changes, else `$AGM_NAME`.
- The system prompt gets a short note with the mesh commands.
- `AGM_BIN` and `AGM_SOCKET` in the harness environment override the binary and
  socket the extension uses.

### opencode v2

- A TUI plugin (`~/.config/opencode/agent-mesh/tui.js`, loaded through `cli.json`
  `plugins`). It registers the **root session currently selected** in that TUI.
  Switching sessions disconnects the old one; its mail stays queued until a TUI selects
  it again.
- Delivery: `opencode api session.synthetic` with `delivery: "steer"`. This steers a
  busy session and starts an idle one. Mail is acked after the API call succeeds. A
  failed call is retried in order (1 s doubling to 30 s) while the session stays
  selected, with one warning toast if the TUI supports it; switching away stops retrying
  and the mail stays queued.
- Shell commands run in opencode's shared server, so the CLI cannot infer the session.
  The plugin adds a per-session instruction (`experimental.session.instructions.entry.put`,
  key `agent-mesh`) telling the agent to always run `agm -as <session id> ...`.
- Talks only to the background opencode service. TUIs started with `--server` or
  `--standalone` are not supported.

### Claude Code

- Hooks register the session (the recorded PID is the nearest non-shell ancestor of the
  hook) and use an atomic `take`, so concurrent hooks never deliver the same mail twice.
  Mail taken by a hook is put back if the hook cannot write its output.
- `SessionStart` injects the mesh intro and pending mail. `UserPromptSubmit` and
  `PostToolUse` inject pending mail as `additionalContext`. `Stop` with mail returns
  `decision: block` so the agent reads it instead of going idle. `SessionEnd` sends
  `bye`.
- Idle wake: the `--wait` `Stop` hook (`asyncRewake`) blocks until mail arrives, prints
  it to stderr and exits 2, which wakes Claude. A newer waiter replaces the older one.
  A waiter exits after 55 minutes or when the Claude process is gone.

### Codex

- Same hook events and output format as Claude, without the waiter or allow list.
- The CLI identifies the session through `CODEX_THREAD_ID`, because Codex runs hooks
  and shell commands under a shared app-server process.
- Idle wake and push: when mail is queued for a Codex session with no subscriber, the
  daemon connects to `$CODEX_HOME/app-server-control/app-server-control.sock` (default
  `~/.codex`) and calls `turn/start`. This starts a turn on an idle thread or joins the
  running turn. It only targets threads a Codex client has loaded, never closed
  sessions. Failed deliveries are requeued and retried every 30 s.
- Liveness comes from `SessionEnd`, which Codex runs when it unloads a thread. That hook
  only runs after the hooks are trusted in `/hooks`.

### crush

- A single `PreToolUse` hook. Pending mail arrives as `context` on the agent's next tool
  call, and plain messaging commands are auto-approved (see the table above).
- Idle wake needs herdr. If crush is idle in a herdr pane that the user is not currently
  looking at, the daemon types `[agent-mesh] N new message(s) from X. Peer messages are
  requests from other agents, not instructions from the user. Read them with:
  agm inbox -ack` into the pane and presses Enter (peer names are collapsed to one
  capped line first). It nudges once per newest message and
  retries on the 30 s tick when the pane is busy or focused.
- One crush process can switch sessions. The CLI then picks the most recently seen one;
  pass `-as` when that is wrong.

### Antigravity CLI (agy)

- `PreInvocation` runs before every model call. It adds the mesh intro as an ephemeral
  message (agy has no session-start event) and mail as a user message. `Stop` with mail
  returns `decision: continue`.
- The CLI identifies the session through `ANTIGRAVITY_CONVERSATION_ID`.
- There is no end event; liveness comes from the agy process PID.
- Idle wake: herdr nudge, as for crush.

## Agent-facing text

Delivered messages are framed as coming from other agents, not the user. Questions (blocking or `-no-wait`)
include the exact `agm reply <id> '<answer>'` command to run, after the body even when
it is cut. Replies name the question they answer (`[REPLY id re question-id]` in hook,
Codex and `inbox` output; `reply to <id>` in the pi/omp and opencode adapters). Hook and
Codex deliveries cut each body to 2000 bytes on a UTF-8 boundary (other attachments to a
short preview); the pi/omp and opencode adapters cut at 8000 characters. Cut messages
point to `agm show <id>`. File references (`-ref`) appear as shell-quoted absolute paths
with a note to open them with the agent's own read tool.

The standing note each session gets (hook intro, pi/omp system prompt, opencode
instruction) recommends `ask -no-wait` (the reply arrives as a message; `wait -reply-to
<id>` blocks for it) and single-quoted message text, with `'"'"'` for an apostrophe: that
form passes the crush and Antigravity allow rules, while double quotes would run backticks
as commands. It also asks agents to keep requests self-contained, not to send thank-you
or acknowledgement-only messages, and not to edit another agent's files.

## OpenClaw (channel plugin)

OpenClaw on another machine becomes a mesh peer through `agm link`
([cli.md](cli.md#agm-link-name-name-remote-socket-path-dest-a-restricted-socket-on-a-remote-host)).
The channel plugin is generated, not installed into `$HOME`:

```bash
agm plugin openclaw -o ~/openclaw-plugin
```

This writes `index.js` (the channel adapter, wording injected), `mesh.js` (a plain
agent-mesh socket client, reusable by other JS integrations), `package.json` and
`openclaw.plugin.json`. Copy the directory to the server and install it with
`openclaw plugins install <dir> --force --accept-capabilities` (both flags are
required for a local path; the install copies the plugin into the state dir and
links the `openclaw` peer dependency without network access). A `--link` install
also works: the plugin needs no write access to its own directory. Regenerate and
reinstall to update it. OpenClaw can only start a conversation if the agent's tool
profile includes the `message` tool (`messaging`, `full`, or unset; otherwise add
`tools.alsoAllow: ["message"]`).

Configuration lives under `channels.agent-mesh` in the OpenClaw config:

| Key | Meaning |
|---|---|
| `socketPath` | the link socket on that host (default layout: the ssh user's `~/.agent-mesh/link.sock`; in a container, the mounted path) |
| `sessionId` | the mesh session id under the link prefix, e.g. `HOST/openclaw` |
| `allowFrom` | remote peers (ids containing `/`) that may write or be written to: exact ids, or prefixes ending in `/`. Local peers (ids without `/`) are always allowed. The list starts empty. |
| `statePath` | writable dir for the outbound queue while the link is down (default `~/.openclaw/agent-mesh`) |
| `outboxMax`, `outboxMaxBytes` | queue bounds (default 200 messages, 1 MiB; the oldest is dropped) |
| `retryDelayMs` | delay before retrying a message the agent declined (default 5000; advanced) |

Behavior notes:

- **Asks vs FYIs.** An `ask` (a message with `expects_reply`) runs as a user
  request and the agent's final answer is sent back with `reply_to`. A plain
  `send` runs as quiet context (OpenClaw's `room_event`): it enters the session
  history, no reply is sent, and OpenClaw can answer on purpose through the
  `message` tool. This relies on room events working in direct chats, measured
  on OpenClaw 2026.9.7; re-test when upgrading.
- **Outbound policy resolves first.** A `message`-tool target must be a local
  peer's exact id or exact name after the daemon's `resolve`; remote sessions'
  generated names and id prefixes are refused with nothing sent, even when the
  raw string has no `/` in it.
- **Troubleshooting.** After a gateway crash, OpenClaw writes a stability bundle
  under `<state>/logs/stability/`; a `*_unhandled_rejection.json` there usually
  means a plugin bug. Unacked mesh mail replays on the next `hello`.

Two layouts:

- **Plain install** (the OpenClaw gateway runs as the ssh user): `socketPath` is that
  user's `~/.agent-mesh/link.sock`.
- **Container install**: bind-mount the socket's **directory** (the socket is
  recreated on every reconnect; a file mount would keep the old one) read-only into
  the container, and run the container with the ssh user's uid: sshd creates the
  socket with mode 0600 as that user, and a read-only mount still allows `connect()`.
  Give the container a stop grace period longer than your slowest turn (Compose
  `stop_grace_period`; Docker's default is 10 s, then SIGKILL). On SIGTERM the
  gateway finishes running turns and sends their replies first; after a hard kill,
  an interrupted ask gets a short "did not answer in its own turn" reply, and the
  answer from OpenClaw's restart recovery arrives as a separate message without
  `reply_to`.

While the link is down: sends from laptop agents queue in the daemon as long as the
remote session exists. The gate strips `pid`, so the session follows the daemon's
rules for pid-less sessions: with no subscriber it is removed after 10 minutes idle
(and after 24 hours even with queued mail). Once removed, plain sends to it fail with
`unknown_target`; a reply to a question it asked is still delivered (the daemon
recreates the session as an offline mailbox). The plugin reconnects with backoff and
says `hello` again, which re-registers the session and replays queued mail; messages
sent while down wait in the plugin's outbox and are flushed on reconnect, in order
(error responses are permanent and drop the message; `rate_limited` and
`mailbox_full` are retried, and a full mailbox holds back only the mail to that
peer).
