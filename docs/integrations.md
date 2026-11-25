# Harness integrations

`agm install` connects each supported harness to the local daemon. Pi/OMP expose a
native messaging tool; every harness can also use the shared `agm` CLI
(see [cli.md](cli.md)). Registration, incoming delivery and idle wake vary by harness.

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
| Grok Build | `grok` | `SessionStart`, `UserPromptSubmit`, `SessionEnd` hooks in `~/.grok/hooks/agent-mesh.json` | no push; the agent reads `agm inbox` | no: a Grok hook cannot start or continue a turn | `$GROK_SESSION_ID` when the shell tool has it, else process ancestry |
| Muse Code | `muse` | `SessionStart`, `UserPromptSubmit`, `SessionEnd` hooks in `~/.config/muse/settings.json` | at session start and each prompt, as plain hook output | no | `$MUSE_SESSION_ID` in the commands the model runs, else process ancestry |
| Factory Droid | `droid` | `SessionStart`/`UserPromptSubmit` hooks | as context at session start and at the next user prompt | none: check `agm inbox` | session `session_id` (stdin; the same value as env) |

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
| `grok` | `.grok` | none | `.grok/hooks/agent-mesh.json`: hooks `SessionStart`, `UserPromptSubmit`, `SessionEnd` (`<bin> hook grok`); uninstall removes the file when it holds nothing but mesh hooks |
| `muse` | `.config/muse` | none | `.config/muse/settings.json`: hooks `SessionStart`, `UserPromptSubmit`, `SessionEnd` (`<bin> hook muse`); the file is never created and must already hold `schema_version` |
| `agy` | `.gemini/antigravity-cli` | none | `.gemini/config/hooks.json`: top-level key `agent-mesh` with `PreInvocation` and `Stop` (`<bin> hook agy <event>`, timeout 10 s); `.gemini/antigravity-cli/settings.json`: one anchored `permissions.allow` regex rule |
| `droid` | `.factory` | none | `.factory/hooks.json`: top-level `SessionStart`, `UserPromptSubmit`, `SessionEnd` (`<bin> hook droid`) |
| `skill` | `.agents/skills` | `.agents/skills/agent-mesh/SKILL.md` | none |
| `claude-skill` | `.claude` | `.claude/skills/agent-mesh/SKILL.md` | none |

Claude, Codex and Grok hooks time out after 30 s, except `SessionEnd` (3 s; Grok's own default is 5 s).

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
  stays in place (for example `{}` or an empty `crushrc`), next to its `.bak`. The
  exception is Grok's `agent-mesh.json`, which is removed when it holds nothing but
  mesh hooks; a `.bak` next to it is never removed or rewritten on the way out.
- If a daemon is running on the current socket (`AGM_SOCKET` or the default),
  `agm install` restarts it so the daemon and adapters come from the same binary.
- Install ignores harness path overrides and always writes to the default paths above.
  Codex's `CODEX_HOME` is honored only by the spawn trust check and the app-server
  wake.

On startup the default daemon refreshes targets reported as `outdated`, using
the same ownership checks as install. Current, foreign and uninstalled targets
are skipped; custom-socket daemons do not edit the shared configuration.
Run `agm refresh` for an immediate refresh and error report, and `agm status`
to check the result. Reload running harnesses to pick up changed adapters.

## Permissions and trust

Agents need to run `agm` without a prompt for every message. Each harness gets a
different allowance:

| Harness | Pre-approved | Scope |
|---|---|---|
| Claude Code | `Bash(<p> <verb> *)` for `list`, `send`, `ask`, `reply`, `inbox`, `history`, `show`, `whoami`, `resolve`, `wait`, `ack` (plus bare `list`/`inbox`/`history`/`whoami`), `<p>` = `agm` and `<bin>` | those verbs only. `install`, `uninstall`, `spawn`, `restart`, `daemon` and the `*-file` verbs go through Claude's normal permission policy |
| Codex | `.codex/rules/agent-mesh.rules` `prefix_rule` | `agm`/`<bin>` followed by `list`, `send`, `ask`, `reply`, `inbox`, `history`, `show`, `whoami`, `resolve`, `wait` or `ack`; these run outside the Codex sandbox (the sandbox blocks the socket) |
| crush | decided per call by `agm hook crush` (`"decision": "allow"`) | exactly one `agm [-as ID] list\|send\|ask\|reply\|inbox\|history\|show\|whoami\|resolve\|wait\|ack ...` command. Pipes, chaining, redirects, substitutions or globs outside quotes fall back to crush's normal permission prompt |
| Antigravity CLI | anchored regex in `permissions.allow` | a single `agm`/`<bin>` `list\|send\|ask\|reply\|inbox\|history\|show\|whoami\|resolve\|wait\|ack` command without shell metacharacters |
| Grok Build | nothing: `agm` commands follow Grok's permission mode (`ask` by default) | allow rules live in `~/.grok/config.toml` (`[permission] rules`), which agent-mesh does not edit |
| Muse Code | nothing: Muse's own approvals and sandbox apply to the `agm` commands the model runs | no allow list is edited; permission profiles are the user's choice |
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
- Native tool `agent_mesh`: `action` is `send`, `ask` or `reply`; `to` selects a
  recipient for send/ask, while `reply_to` selects the original message for replies.
  `summary` is required; optional `constraints`, `files` and `details` become readable
  message sections. `summary` and `details` have no fixed character cap; the broker's
  serialized frame limit applies. Paths/code are preserved verbatim. `files` is a literal path
  list, not attachments or a file read; use CLI `-ref` for actual references. For an
  oversized serialized payload, write the body to a file and use the CLI send/reply
  command with `-ref`; the native tool never truncates or splits the original.
  Asks are asynchronous. The result is a compact `broker_accepted` receipt with id,
  actual recipient, timestamp and `request_key`, never a read/completion claim.
  New calls get fresh random keys, independent of provider tool-call ids. On an
  uncertain outcome, repeat the same fields with the returned key; the broker's
  bounded deduplication window applies (see [safe retries](cli.md#safe-retries-and-compact-receipts)).
  Cancellation after writing is uncertain, not proof of non-delivery. Old daemons
  without `idempotency >= 1` are refused before sending. Tool-disabled/older hosts
  keep the CLI path, and the adapter never overrides a user's active-tool selection.
- Mail is pushed as an `agent_mesh` custom message. If the agent is idle it starts a
  turn; if it is busy it is delivered as a steer. All messages wait for the host's
  `message_end` and a completed turn before ACK; merely queuing a steer is not
  persistence. Delivery is at-least-once. ACK does not mean read or answered.
- Compaction coordination is optional and extension-only. A cooperating Smart Compact
  extension can hold inbound mail during foreground preparation or native compaction;
  held mail neither starts a turn nor receives an ACK. After release, normal delivery
  resumes, including quiet-mode handling. Session switches/shutdown invalidate old
  callbacks and leave unacknowledged mail in the broker for replay. Without a listener,
  delivery is unchanged; this is not a standalone compaction detector.
  The synchronous `pi.events` channel is `smart-compact:defer-delivery`, with
  `{version: 1, sessionId, deliver, deferred: false}`. The coordinator claims a request
  by setting `deferred = true` before `emit` returns, then calls `deliver()` in FIFO
  order at a safe post-compaction boundary. The callback is one-shot and bypasses
  coordination on release; a rejected host handoff is retried through broker replay.
- Card: a native message renderer shows `MESSAGE`/`REQUEST`/`REPLY`, a "from <sender>" name,
  a reply hint for requests, time, a short id and a peer-not-user label; collapsed shows the first lines, expanded the full
  body, file references and metadata (full id, reply command). If the host's TUI package
  cannot be loaded, the default rendering of the same text is used. The model sees the
  same text as before, plus file references.
- Status line (`setStatus`, key `agent-mesh`): `● mesh · <name>` once the daemon
  accepted `hello` (name in the theme's accent color); also `◐ mesh · connecting…`,
  `◌ mesh · reconnecting…`, `○ mesh · unavailable (<reason>)`, plus a dim
  `· ☾ quiet (N waiting)` suffix. Cleared on shutdown.
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
- Name: the pi/omp session name, else `$AGM_NAME`. Name-change events update the
  broker and footer without starting a model turn. Hosts without a name subscription
  use a 500 ms in-memory check that sends only on change; failed updates retry on
  reconnect. Name watchers are removed on session switch/shutdown.
- Presence: after negotiating `coordination >= 1`, publish model, activity, active
  tool names and context estimates at most once/second, with a 10-second heartbeat.
  Native UI-prompt events report `waiting`; missing host events stay unknown rather
  than guessing a stall. Prompts, reasoning, tool arguments and outputs are never
  copied into observations. `/mesh status` includes reporting capability/state.
- Metrics describe the last completed model request: OMP's native duration/TTFT when
  available, or Pi's duration plus locally observed first content delta. Missing
  timings stay null. Response-model provenance distinguishes a resolved response
  model from a request alias. Heartbeats preserve the metric's original timestamp
  and do not count as work progress. Timers/metrics reset on session switch/shutdown.
- Older daemons keep messaging without observations. A metadata rejection disables
  reporting for that connection without breaking mail; reconnect negotiates again.
- Socket handshakes and menu requests time out after 10 s instead of hanging. A
  timed-out send has an unknown outcome: check history before resending.
- The system prompt gets a short native-tool or CLI note, matching tool availability.
  Modern Pi prompt sections remain composable with other extensions; older string
  and OMP array prompts retain their existing shape.
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
  sessions. The mail stays queued until the app-server accepted the turn: a failed
  delivery is retried every 30 s, and a daemon crash after acceptance delivers the
  batch once more.
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

### Factory Droid (droid)

Verified with Factory CLI 1.19.0 and a scripted loopback model: of two sessions in
one workspace, only the addressed session received the ask on resume. Its `Execute`
call ran `agm reply` without `-as`; the sender received the reply from that session.
No hosted provider or API key was used.

- `SessionStart` and `UserPromptSubmit` hooks return the mesh intro / mail as
  `hookSpecificOutput.additionalContext`; `SessionEnd` sends `bye`. Only those
  events take mail: on other events the hook registers presence and leaves
  mail queued (read it with `agm inbox`).
- Sessions are identified by the `session_id` in the hook input (Droid sets
  the same value as the `session_id` env var for hook processes only; ordinary
  tool shells get no verified session id and must not invent one). Two sessions
  in one `cwd` are distinct mesh sessions.
- No idle wake: an idle Droid session is not interrupted until its next user
  prompt; `SessionStart` context batching applies from the next accepted prompt.

### Grok Build (`grok`)

Grok loads every `~/.grok/hooks/*.json`. agent-mesh writes its own, `agent-mesh.json`,
with `SessionStart`, `UserPromptSubmit` and `SessionEnd` hooks that run `<bin> hook grok`.
The contract is the one in Grok's hook documentation (docs.x.ai/build/features/hooks):
the event arrives as JSON on stdin (`hookEventName`, `sessionId`, `cwd`, `workspaceRoot`),
with the same values in `GROK_HOOK_EVENT`, `GROK_SESSION_ID` and `GROK_WORKSPACE_ROOT`.

- **Trust.** Run `grok inspect` in the workspace. In 1.0.92, even personal hooks
  report `waitsForFolderTrust` until you trust the folder in Grok.
- **Delivery.** The Grok hooks only register, refresh and end sessions. They do not
  wake the agent, inject context, deny tool calls or consume mail. Check `agm inbox`
  for incoming mail and `agm ack <msg-id>` after handling it. To await an answer
  to your own question, run `agm wait -reply-to <question-id>`; this does not wait
  for unsolicited messages.
- **Identity.** Grok 1.0.92 passes `GROK_SESSION_ID` to tool commands. Two sessions
  can therefore share a directory and run `agm reply` without specifying `-as`.
  Without that variable, process ancestry is a fallback; check `agm whoami` and
  use `-as <session-id>` if it names the wrong session.
- **Files.** The installer uses `~/.grok`, not a custom `GROK_HOME`, and does not
  pre-approve commands. Uninstall removes the hooks file when only mesh entries
  remain; if it contains other hooks, uninstall preserves them.
- **Verification.** Two real headless Grok Build 1.0.92 sessions in one trusted
  workspace exchanged an addressed question and reply through their shell tools.
  The model was scripted on loopback; no subscription or provider key was used.
  With both adapters installed, the Claude adapter ignores hooks dispatched by Grok:
  queued mail stayed in the inbox, no Stop waiter ran, and empty sessions exited
  without leaving a mesh entry.

### Muse Code (`muse`)

- Muse reads the `hooks` member of `~/.config/muse/settings.json` as user hooks, with no
  trust step. Project hooks (`<project>/.muse/hooks.json`) need folder trust and are not used.
  The path is taken from `$HOME`; `XDG_CONFIG_HOME` is not honored.
- The target only edits a settings file Muse already wrote. It refuses to create one, or to
  touch a file without `schema_version`, because Muse does not start without its own keys.
- Hooks: `SessionStart`, `UserPromptSubmit` and `SessionEnd` running `<bin> hook muse`. stdin
  is Claude Code's shape. The answers are not: on 1.4.25 a
  `hookSpecificOutput.additionalContext` answer was accepted but did not reach the model, so `agm`
  speaks plainly where Muse documents plain text as context (`SessionStart`: the standing
  instructions and mail; `UserPromptSubmit`: mail) and ends the session at `SessionEnd`.
- Mail is delivered at prompts only. There is no `Stop` hook and no continuation, no
  `PostToolUse` hook (Muse passes nothing on from it) and no `--wait` waiter (Muse skips a
  handler with a field outside its closed set, and `asyncRewake` is not in it). A `Stop`
  handler that an earlier version left in the settings file takes no mail and blocks nothing:
  `agm status muse` reports that install as outdated, and the next `agm install muse` or
  `agm uninstall muse` removes the handler.
- Identity: the commands the model runs carry `MUSE_SESSION_ID`, which `agm` uses, so `-as` is
  not needed. Hook processes get a cleared environment, so `AGM_SOCKET` does not reach them:
  a non-default socket is not supported for this target.
- Sandbox and approvals: not touched. Hooks reach the mesh under the default sandbox, so
  delivery needs nothing. Under the default sandbox (`--sandbox-network proxy-only`) a command the
  model runs cannot open the mesh socket (EPERM): the model can read mail but not send. The
  tested prerequisite for sending is the run flag `muse exec --sandbox-network enabled`. Which
  runs get it is the user's choice; agent-mesh passes no such flag and changes no sandbox or
  approval setting. Muse's approvals apply to the `agm` commands the model runs.
- **Verification.** Muse Code 1.4.25. With the echo provider (no model cost): `agm install`
  `muse` into a settings file Muse accepts, every hook dispatched, and a leftover `Stop`
  handler called with empty output and no continuation. With mail queued during a run: still
  queued at `Stop`, spoken once at the next prompt (its payload replayed through the built
  binary), and `SessionEnd` left no session. Two concurrent sessions in one workspace
  listed as two sessions with their own mail. With `muse-spark-1.3` in isolated HOME, XDG dirs
  and mesh: settings-level hooks fired, a question spoken as plain text reached the model,
  and the model's `agm reply` was attributed to its session. Muse did not run the
  hooks of a `.claude/settings.json`.

## Agent-facing text



Delivered messages are framed as coming from other agents, not the user. Questions (blocking or `-no-wait`)
include the exact `agm reply <id> '<answer>'` command to run, after the body even when
it is cut. Replies name the question they answer (`[REPLY id re question-id]` in hook,
Codex and `inbox` output; `reply to <id>` in the pi/omp and opencode adapters). Hook and
Codex deliveries keep a 2000-byte body preview and short attachment previews, with
an aggregate ceiling of 8000 UTF-8 bytes per message and 24000 per delivery batch.
Headers and retrieval/reply hints count too; reference paths are whole or omitted.
Hooks use negotiated `take_ids` selection so excess mail stays queued atomically;
older daemons without this capability retain queued mail until upgraded. Codex wakes
hand off one bounded batch per wake/sweep, not an immediate full-backlog loop.
All JavaScript adapters use the same embedded UTF-8 formatter, with an 8000-byte
total message ceiling and no added runtime dependency. Cut messages point to
`agm show <id>`. File-reference attachments
appear whole or are omitted with a retrieval hint.

Pi/OMP charge immediate deliveries and deferred batches against the same 24000-byte
run budget. Pending steers are included, and excess remains unacked. The next batch
can wake on a non-aborted `agent_settled` event with an idle host; older hosts without
that event wait for the next natural run. Quiet FYI mail does not wake a turn.
OpenCode limits input attempts (including ambiguous
retries) to 24000 bytes between observed completed runs. Its TUI forwards the native
`Session.Info.time.idle`/`outcome` snapshot; unchanged, foreign or unknown data cannot
reset the quota. Interrupted runs pause queued input. Hosts without that completion
signal retain excess until session reselection. API admission is still not proof of
model processing; these limits do not bound cumulative conversation history.

The standing note each session gets (hook intro, pi/omp system prompt, opencode
instruction) recommends `ask -no-wait` (the reply arrives as a message; `wait -reply-to
<id>` blocks for it) and single-quoted message text, with `'"'"'` for an apostrophe: that
form passes the crush and Antigravity allow rules, while double quotes would run backticks
as commands. It also asks agents to keep requests self-contained, not to send thank-you
or acknowledgement-only messages, and not to edit another agent's files.

On the server's sshd, `ClientAliveInterval 15` and `ClientAliveCountMax 3` are
recommended: a dead session (sleeping local machine, changed network) and its listener
then go away in 45 s instead of lingering for the kernel keepalive's ~2 h. With
`-bridge`, every connection rides the one ssh TCP stream; sshd's `MaxSessions`
does not limit forwarded channels — 16 streamlocal connections over one TCP
connection pass an sshd at `MaxSessions=10` (measured, OpenSSH on Linux) —
so only the pre-step's one exec channel counts and the default needs no
raising. The `-L` forward exposes DEST's
daemon socket to the link process on your machine only — it binds
`link-NAME-remote.sock` (mode 0600) next to your daemon socket, never a network
port. Relays over a tunnel slower than roughly 16 KiB/s are not retried while
bytes keep arriving (progress-based deadlines), so a stalled-but-alive tunnel
delays mail instead of duplicating it — that floor sits next to the at-least-once
window above.

## The bridge on a server (`agm link -bridge`)

With `-bridge`, the link process
on your machine also mirrors sessions between your daemon and DEST's daemon, so
server agents message your agents (and each other) through real sessions on the
server's own daemon, with the server's own hooks and waker.

On the server:

```bash
# once: install agm there, then its daemon and the usual adapters
agm install && agm restart
```

On your machine:

```bash
# the gate plus the bridge, kept running (herdr pane, tmux, or a launchd unit)
agm link -bridge -name srv ops@203.0.113.7
```

- **The upgrade rule.** After upgrading agm on either host, run `agm restart`
  there: the bridge talks to the running daemons, not the binaries, and it pauses
  mirroring until both daemons report bridge support in their `protocol` answer
  (the gate keeps running meanwhile, and the bridge picks the daemon up again on
  its next ping — no link restart needed).
- Each side's sessions appear on the other as `PREFIX/` proxies; a session that
  ends has its queued mail bounced to the senders before its row is removed, and
  rows of sessions that ended during a bridge restart are finished after it
  (`link-NAME.rows`).
- A launchd unit that keeps the link up (`~/Library/LaunchAgents/`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>dev.tarhan.agent-mesh.link.srv</string>
  <key>ProgramArguments</key><array>
    <string>/opt/homebrew/bin/agm</string>
    <string>link</string>
    <string>-bridge</string>
    <string>-name</string><string>srv</string>
    <string>ops@203.0.113.7</string>
  </array>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>/tmp/agm-link-srv.log</string>
  <key>StandardErrorPath</key><string>/tmp/agm-link-srv.log</string>
</dict></plist>
```

  (the link reconnects on its own; `KeepAlive` covers a crash of the process
  itself. Point `ProgramArguments` at your `agm` — `command -v agm` — and see
  [cli.md](cli.md#remote-link)
  for the flags).
