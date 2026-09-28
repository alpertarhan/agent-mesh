# agent-mesh — design decisions

> **Historical design research, not a current specification.** These notes include
> proposals and superseded implementation details and are not kept in sync with the
> code. For current behavior, read the [CLI reference](cli.md) and
> [integration guide](integrations.md). In particular, the MCP server, Claude Channels,
> broadcast, ACP and A2A proposals below are not implemented.

Goal: one local mesh that replaces the per-harness intercom plugins (pi-intercom,
omp intercom fork, crush `intercom.py`) so agents in different harnesses can message
each other, and spawn each other through herdr.

## Problem

Today there are three incompatible intercoms: pi (`~/.pi/agent/intercom/broker.sock`),
omp (`~/.omp/agent/intercom/broker.sock`), and crush (file-based, `PreToolUse` hook).
opencode, Claude Code, and Codex have none. pi cannot talk to omp.

## Shape

One Go binary, `agm`:

| Command | Role |
|---|---|
| `agm daemon` | broker: registry, router, mailboxes. Unix socket, NDJSON. Auto-started by clients. |
| `agm send/ask/reply/list/inbox` | CLI, usable by any harness that can run shell |
| `agm mcp` | MCP stdio server (+ Claude `claude/channel` capability for push) |
| `agm hook <harness>` | hook entry point (Claude, Codex, crush, Antigravity, dsh) |
| `agm spawn` | spawn agents in herdr panes |
| `agm install/uninstall/status` | per-harness integration, herdr-style |

Thin TS adapters only where native push exists: `pi-omp.ts` (pi and omp share one API),
`opencode.js` (`session.promptAsync`). Embedded with `go:embed`.

## Standards and how we use them

- **MCP**: universal adapter surface (tools), not the mesh itself.
- **Claude Channels**: push into Claude sessions. Research preview; custom channels
  need `--dangerously-load-development-channels`.
- **Hooks** (`PreToolUse`/`PostToolUse` `additionalContext`, `Stop` `decision:block`):
  delivery on the next tool call, or wake at end of turn.
- **herdr** `agent prompt/wait/start`: spawn, plus a universal fallback for idle TUIs.
- **A2A**: not implemented. Borrowed concepts: agent card = `capabilities`, task states = ask lifecycle.
  Possible later as a cross-machine gateway.
- **ACP / Codex app-server**: later, for headless spawn.
- Prior art: `mcp_agent_mail` (pull-only, Python). We differ with push, herdr, one binary.

## Delivery tiers (router picks best per session capability)

Sessions record their herdr pane (`HERDR_PANE_ID`) when the harness process runs in it.

1. native push: pi/omp steer, opencode `promptAsync`, Claude channel
2. hook piggyback: next tool call / `Stop` hook
3. herdr `agent prompt` into an idle pane
4. pull: `inbox`

## Hook delivery (crush, Claude Code, Codex)

One handler, `agm hook <harness>`; Claude and Codex share the hook format.

| Event | Action / output |
|---|---|
| crush `PreToolUse` | `{"decision":"none","context":...}` |
| `SessionStart` | register + one-line mesh intro + pending mail as `additionalContext` |
| `UserPromptSubmit`, `PostToolUse` | pending mail as `hookSpecificOutput.additionalContext` |
| `Stop` | pending mail → `{"decision":"block","reason":...}` (agent continues instead of idling) |
| `SessionEnd` | `bye` |
| Claude `Stop` + `asyncRewake` | `agm hook claude --wait`: blocks until mail, stderr + exit 2 wakes idle Claude |

- Idle wake for Claude without the Channels preview flag: `asyncRewake`. The broker keeps
  one exclusive waiter per session (newest replaces older; Claude does not dedup).
- Hooks use atomic `take` so concurrent hooks/waiter never deliver twice. If the hook cannot
  write its output (harness gone: EPIPE, SIGPIPE ignored) the mail is requeued. Ceiling:
  mail written to a harness that dies before reading it is lost (hooks have no ack).
- Harness PID = nearest non-shell ancestor of the hook (`sh -c` under Claude/Codex).
- Codex push: the daemon's waker delivers mail for Codex sessions through the shared
  app-server (`~/.codex/app-server-control/app-server-control.sock`, JSON-RPC over a
  WebSocket; stdlib client in `internal/codex`). `turn/start` starts a turn on an idle
  thread and joins the running turn of a busy one; the TUI shows it as a prompt. Only
  threads in `thread/loaded/list` are touched (never run a closed session headless).
  Failures requeue the mail; the 30s sweep retries.
- Codex (0.158) runs hooks and shell commands under a shared `codex app-server
  --managed-daemon`, so the recorded PID is that daemon: identity uses `CODEX_THREAD_ID`
  (= hook `session_id`) before ancestor-PID inference. Liveness: `SessionEnd` bye. The app-server
  runs it when it unloads a thread, ~60s after its TUI is gone (also after `kill -9` or a
  closed pane). Changed hooks must be re-trusted in Codex, or SessionEnd silently does not run. Its hook env is
  the daemon's too (`HERDR_PANE_ID` of whichever pane started it), so Codex hooks send no pane.
- Codex sandboxes shell commands (no access to the mesh socket): `agm install codex`
  also writes `~/.codex/rules/agent-mesh.rules`, a `prefix_rule` allowing only
  `agm list|send|ask|reply|inbox` outside the sandbox without prompting.
  New/changed hooks must be trusted once in Codex (`/hooks`).
- crush runs each session in its own process (no shared server by default): PID works.
- Idle crush wake: the waker types a one-line notice into the crush pane with
  `herdr pane send-text` + Enter ("N new message(s) from X. Read them with: agm inbox -ack");
  the agent's next tool call delivers the mail through the PreToolUse hook. Guards: crush PID
  alive, herdr reports agent `crush` in the pane with status idle/done, and the user is not
  looking at it (pane, tab and workspace all focused → skip; they could be mid-prompt).
  One nudge per newest message. `herdr agent prompt` would be cleaner but only targets
  agents started by `herdr agent start`, which does not support crush.

## pi / omp adapter

`internal/integrations/pi.ts`, one file for both (only node builtins, `@ts-nocheck`; the
API differs only in `before_agent_start.systemPrompt`: string in pi, string[] in omp).
Subscribes on `session_start`, `bye` on `session_shutdown`, reconnects with backoff,
auto-starts the daemon. Inbound: `pi.sendMessage` with `triggerTurn` when idle, `steer`
when busy, then `ack` (at-least-once). No custom tools: agents use the `agm` CLI from
their shell tool (self identity via ancestor PID); the system prompt gets one note.
Agent-facing hints must say "run with your shell tool": GPT models otherwise print the command.

## opencode v2 adapter

v2 runs one background server (`opencode serve --service`) shared by TUIs, so the
adapter is a TUI plugin (`~/.config/opencode/agent-mesh/tui.js`, listed in `cli.json`
`plugins`, v2 `setup(api)` entry). It registers the selected root session
(`api.ui.router`, `api.data.session`) and delivers via `opencode api session.synthetic`
with `delivery: "steer"`: durable, steers a busy session, starts an idle one; ack after
success. Identity: bash tools run in the shared server, so ancestor-PID inference cannot
tell sessions apart. A per-session instruction entry
(`experimental.session.instructions.entry.put`, key `agent-mesh`) tells the agent its id
and to use `agm -as <id>`; delivered messages repeat it.
Rejected: `session.environment` replaces the whole shell env (PATH lost) and only takes
effect before the session's first shell command.

## Antigravity CLI (agy) adapter

Hooks only (`~/.gemini/config/hooks.json`, our own top-level key `agent-mesh`); the event
name is an argument (`agm hook agy PreInvocation|Stop`), the payload is camelCase
(`conversationId`, `workspacePaths`). Hooks run as direct children of the agy process (PID
works); shell commands get `ANTIGRAVITY_CONVERSATION_ID` (identity, like Codex).

- `PreInvocation` (before every model call): mesh intro as `ephemeralMessage` (agy has no
  session-start event), mail as `userMessage` (persists, later steps still see it).
- `Stop`: mail → `{"decision":"continue","reason":...}`.
- No end event: liveness by PID. Idle wake: herdr nudge, as for crush.
- `PreToolUse` is not used: any output without a decision (even `{}`) denies the tool call.
- Messaging commands run without prompting via `permissions.allow` in
  `~/.gemini/antigravity-cli/settings.json`: `command(regex:^(agm|<bin>) (list|send|ask|reply|inbox)( [^;&|<>$`...]*)?$)`.
  Must be anchored: an unanchored rule let `agm list; touch x` run without a prompt.
- agy has a trust dialog (default "Yes"): spawn checks `trustedWorkspaces` first.

## Installation

Binary: `GOBIN=~/.local/bin go install ./cmd/mesh` (on PATH; atomic replace).
`agm install [harness...]` (default: every detected harness), `uninstall`, `status`,
`restart`. Adapters are embedded (`go:embed`) and rendered with the absolute binary path.

| Target | Owned file | Shared config edit |
|---|---|---|
| pi / omp | `~/.{pi,omp}/agent/extensions/agent-mesh.ts` | none |
| opencode | `~/.config/opencode/agent-mesh/tui.js` | `cli.json` `plugins` += `./agent-mesh` |
| claude | none | `~/.claude/settings.json` hooks (5 events + Stop waiter) and `permissions.allow` for the agm CLI |
| codex | `~/.codex/rules/agent-mesh.rules` (execpolicy) | `~/.codex/hooks.json` hooks |
| crush | none | `crushrc` line `hook add PreToolUse ... --name agm` |
| agy | none | `~/.gemini/config/hooks.json` key `agent-mesh`; `settings.json` `permissions.allow` rule |
| skill | `~/.agents/skills/agent-mesh/SKILL.md` | none |
| claude-skill | `~/.claude/skills/agent-mesh/SKILL.md` (Claude Code does not read `~/.agents`) | none |

- Owned files carry `MESH_INTEGRATION_ID=<name>`; a file without it is "foreign" and never touched.
- Shared configs: mesh entries are recognized by content (`agm hook <harness>`, `--name agm`,
  `Bash(.../mesh ...)`), so a moved binary still updates in place instead of duplicating.
  Top-level JSON key order is kept, previous content goes to `.bak`, JSONC is refused.
- `status` compares with what the current binary would write: current / outdated / not installed.
- `install` restarts a running daemon (`shutdown` op) so daemon and adapters match.
- **Clean migration** (done once, code removed): legacy pi-intercom, omp-intercom and the crush
  `intercom.py` hook were uninstalled by hand; no wire compatibility.

## Distribution

A `v*` tag runs `.github/workflows/release.yml`:

- GoReleaser: GitHub release (`agm_<os>_<arch>.tar.gz`, versionless names, `checksums.txt`)
  with GitHub build provenance (`gh attestation verify <file> -R alpertarhan/agent-mesh`),
  AUR `agent-mesh-bin` (`AUR_KEY`; skipped if unset).
- Homebrew formula `alpertarhan/tap/agent-mesh`, built from the tag's source. The release job
  pushes `bump/agent-mesh-<version>` to the tap (deploy key `HOMEBREW_TAP_KEY`); the tap's
  `bottles` workflow runs `brew test-bot` on macOS arm64/intel and Linux, uploads the bottles
  to a tap release with provenance (`brew pr-upload`, `actions/attest`) and only then moves
  `main`, so users never see a version without bottles. Other platforms build from source.
- npm `@alpertarhan/agent-mesh` (bun uses the same registry): the four release binaries
  plus a node shim, no install scripts; published with npm trusted publishing (OIDC).
- `install.sh` (curl): latest release, checksum verified, `~/.local/bin`.

Upgrades: `agm install` writes the stable path (the `agm` on PATH if it is this binary,
e.g. `/opt/homebrew/bin/agm`, never brew's versioned Cellar path). The daemon exits when
its binary file is replaced or removed and the next client starts the new one; a
`flock` on `mesh.sock.lock` keeps racing starters to one daemon.

## Concurrency and limits

The bottleneck is agents (turn latency, context, API cost, RAM), not the broker.
Limits exist to protect agents: loops, floods, fork bombs.

- Goroutines per connection: reader + writer with a bounded out channel. A slow
  client is disconnected; its messages stay in the mailbox.
- Router state: one `sync.Mutex`. Pushes never block under the lock.
- Mailbox per session: bounded slice. Full means `mailbox_full` to the sender
  (visible backpressure, never silent drop).
- At-least-once: a message stays in the mailbox until `ack`. Dedup by message id.
  FIFO per sender→receiver pair.
- Ask replies are routed straight to the waiting ask connection when it is alive,
  otherwise to the mailbox.
- **Deadlock**: an ask that would close a cycle in the wait-for graph is rejected
  with `would_deadlock`.
- Session GC (sweep every 30s): a session is kept while it has a subscriber or its
  harness PID is alive. Otherwise it is removed immediately if the PID is known dead
  and the mailbox is empty, after `IdleTTL` (10m) if there is no PID, and after
  `MailTTL` (24h) even with queued mail (covers resumed sessions).
- Self identity: CLI calls without `-as` match their ancestor PIDs against registered
  harness PIDs, so agents just run `agm send ...` from their shell tool.
- Naming: harness session name (pi/omp `getSessionName`, re-sent when it changes) >
  `$AGM_NAME` (adapters, hooks, `agm hello`) > generated `adjective-noun` (64×64 words)
  derived from an FNV hash of the session id, so it is stable across daemon restarts and
  resumes; a taken name moves to the next combination. PIDs are not names (unstable,
  one process can host several sessions); they decide liveness.
- Resolution: exact id → name (case-insensitive, optional `name@harness`) → id prefix.
  Several matches: live sessions win; still several → `ambiguous_target` listing
  `id (harness, cwd)`. `unknown_target` lists live `name@harness` so agents self-correct.
- Persistence: undelivered messages + pending asks snapshot to a JSON spool. No SQLite.
- Security: socket dir `0700`, socket `0600`. Inbound auto-trigger policy stays
  per adapter, because agent-to-agent messages can carry prompt injection.

| Limit | Default |
|---|---|
| frame size | 1 MB (large attachments go by path) |
| `mailbox_cap` | 256 |
| sender rate | 20/min, burst 10 |
| `max_hops` (reply chain depth) | 8 |
| `ask_timeout` | 120s |
| pending asks per sender | 4 |
| broadcast fanout | 16 (not in v0) |
| `spawn_max` / `spawn_depth` | 8 / 2 (v2) |
| inject coalesce window | 2s (adapter side) |

## Spawn

`agm spawn [-harness pi] [-name N] [-cwd D] [-focus] "<task>"` (from a herdr pane):

1. Daemon `spawn` op reserves the name (generated if empty) and enforces limits: at most
   `SpawnMax` (8) spawned sessions live or pending, depth ≤ `SpawnDepth` (2; user-started
   agents are depth 0). Parent = the calling session, if any.
2. `herdr tab create --no-focus --env AGM_NAME=<name>` in the caller's workspace.
3. `herdr agent start --kind <harness>` (crush: typed, it is not a herdr kind).
4. The task is the first prompt (`herdr agent prompt --wait --until working|blocked`), steps
   first ("do the task; deliver the result with `agm send <parent>`"): models skip a trailing
   report instruction. First prompt rather than a mesh message because Codex, opencode and
   crush only register after their first prompt/tool call.
5. The session saying hello with the reserved name is linked (`parent`, `depth`). Codex hooks
   cannot see the tab env: `UserPromptSubmit` reads the name from the spawn prompt
   (`[agent-mesh] You are "<name>", an agent spawned by ...`), so concurrent spawns cannot swap names.

Safety:
- Codex and Claude have a trust dialog for new directories; a typed task + Enter would answer
  it (trust on the user's behalf). Spawn refuses unless the dir or its git root is already
  trusted (`~/.codex/config.toml` `trust_level`, `~/.claude.json` `hasTrustDialogAccepted`,
  agy `settings.json` `trustedWorkspaces`).
- crush has no command-level allow list: `agm hook crush` returns `decision: allow` for a bash
  call that is exactly one `agm [-as ID] list|send|ask|reply|inbox ...` (quote-aware; any
  chaining, pipe, redirect, substitution or glob outside quotes → normal permission flow).

## Out of scope for now

Separate MQ, multi-machine, A2A, SQLite, file leases, priorities.

## Phases

- **v0**: daemon, CLI, limits, deadlock check, spool, `agm hook` for crush / Claude / Codex / dsh.
- **v1**: `agm mcp` + Claude channel, pi/omp adapter, opencode plugin, `agm install`; retire legacy intercoms.
- **v2** (done): idle wake for every harness (Codex app-server, crush herdr nudge), `agm spawn`.
- **later**: ACP / Codex app-server headless spawn, A2A gateway.

Open spikes: Antigravity hook semantics; attaching to a running Codex TUI via app-server.
