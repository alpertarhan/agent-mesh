# Coordination expansion

Implemented for v0.6.0, with Pi/OMP runtime observations first. Reliability,
discovery, linked task metadata and bounded delivery are included; future ideas
below are explicitly separate from this release. Other harnesses remain usable
with `null`/`unknown` observations. See the [release notes](releases/v0.6.0.md).

## 1. Reliability and rename

Included in v0.6.0:

- Keep the daemon lock alive through GC and shutdown draining; cancellation closes
  blocked socket writers. Bridge write waits obey deadlines as well as reads.
- Commit sends before delivery. A failed commit returns `persistence_failed`
  without publishing the message or mutating history/asks; bridge and OpenClaw
  retry this transient failure. Non-send checkpoints retain their existing
  best-effort behavior. This is not an exactly-once or power-loss guarantee.
- Pi/OMP ACK only after the host's message/turn persistence boundary, including
  normal busy-session steering. Unpersisted mail survives session switches.
- Follow native name changes without a model turn, refresh the footer after the
  broker accepts the name, and use a 500 ms in-memory fallback on older hosts.
- Preserve shared config/backup permissions and reject trailing invalid JSON.

Tests cover failed send/retry, storage before delivery, GC, shutdown draining,
blocked bridge writers, config permissions, idle renames, and ACK/replay lifecycle.
Real harness UI and SSH checks remain separate from the isolated test suite.

## 2. Discovery and live coordination

### CLI

| Command | Status / purpose |
|---|---|
| `agm list [-state STATE] [-metrics] [-json]` | Implemented: filterable peer discovery |
| `agm inspect [-json] TARGET` | Implemented: identity, task, freshness and last-request metrics |
| `agm watch [-json]` | Implemented: initial snapshot, then changed snapshots via one read-only poll/second |
| `agm task set/clear` | Implemented: publish/clear the caller's summary, scope and blocker |
| `agm doctor` | Future: consolidated capabilities/storage-health diagnostics |

Keep flag ordering consistent with existing commands. Names are mutable labels;
session IDs remain routing identities. No automatic assignment or scheduler is
required for this stage.

### Information and remaining observations

Implemented: session ID/name, harness/cwd, connection versus activity, selected
model and response/request-model provenance, context usage, task/scope/blocker,
last-request output TPS/TTFT and metric age. Native and locally observed timings
are distinguished. Presence expires after 30 seconds or connection loss; heartbeats
do not count as work progress. Raw prompts, reasoning and tool payloads are excluded.

Still future: explicit host/repository/worktree discovery, per-tool durations and
rolling metric aggregates. Broader field goals:

- **Identity:** session ID/name, harness, host, cwd, repository/worktree where
  available, selected provider/model and the actual model used for the last response.
- **Presence:** connection status separately from activity (`idle`, `busy`,
  `waiting`, `unknown`), activity start, last progress, and a reason when waiting.
  A silent long-running tool is not proof of a stuck agent. A suspected stall must
  state its evidence and remain distinguishable from a reported failure.
- **Work/context:** self-declared summary, bounded file/directory scope, peer or
  reply being awaited, and context token usage/capacity. Do not publish raw prompts,
  tool arguments, secrets, transcripts or reasoning as task metadata.
- **Metrics:** request-scoped TTFT, output TPS, token usage and tool duration;
  rolling statistics only with an explicit sample count. Each value needs a unit,
  source and observation time. Exclude tool/user wait time from generation TPS.
- **Capabilities/freshness:** unsupported versus stale data must be explicit.
  Reconnection/session switching must not reuse another session's observations.

### Implementation constraints

Keep transient presence/metrics separate from the durable message spool. Do not
serialize the whole history for heartbeats or individual token events. Updates are
coalesced to at most 1 Hz, with a 10-second heartbeat. Watch deliberately reuses
`list`: no daemon watcher queue, no accumulated event history; memory scales with
the current registry. It exits on transport failure; rerun for a fresh snapshot.

Use host events for Pi/OMP. The installed OMP exposes response `duration`/`ttft`;
Pi exposes response duration and stream events. Missing timings stay unknown.
Treat provider timestamps and locally observed timing as different sources.

Negotiate new capabilities before sending metadata. Existing OpenClaw checks an
exact protocol version, the link gate has an operation/field allowlist, and the
bridge explicitly copies identity fields. Direct link clients can publish `observe`
and `task` only for their bound, namespaced session; mirrored proxies currently
report unknown observations. Metadata capability negotiation keeps old daemons
from silently accepting and dropping newer fields.

Acceptance checks: no model turn for metadata changes; fresh identity after rapid
rename/reconnect; correct busy/idle transitions on abort and error; no cross-session
leaks; no stale data presented as live; bounded watcher/metric memory; old clients
can still send/receive; unsupported harnesses remain visible as unknown.

## 3. Message/task coordination

Reuse `reply_to`, `history -thread` and the canonical wording in
`internal/integrations/text.go`; do not introduce a second conversation system.

The installed skill now documents a short TASK, SCOPE, CONTEXT, DONE WHEN and RETURN contract.
Results should include artifacts/test evidence and remaining risks. A blocker
should identify the missing input or decision. Transport ACK means durable receipt,
not task acceptance or completion. Task updates must preserve their request link.

Hook/Codex total delivery budgets are implemented: 8000 UTF-8 bytes per message,
24000 per batch, including metadata and hints. Negotiated `take_ids` claims only the
selected queue entries; excess stays queued without a consume/requeue window.
Codex hands off one batch per wake/sweep. Full bodies remain retrievable with `show`.

All JavaScript adapters now bound complete message previews at 8000 UTF-8 bytes,
including headers, attachments, refs and hints. OpenClaw also bounds the final host
envelope and every inbound body field. Pi/OMP share a 24000-byte immediate/deferred
run budget, retaining excess unacked; settled idle hosts can release another batch,
while older hosts resume at the next natural run. Aborts do not restart the backlog.

OpenCode caps input attempts (including ambiguous retries) at 24000 bytes between
native completed-run snapshots. In-flight requests count across the boundary;
stale/foreign/unknown state cannot reset quota. Interrupted runs pause queued input.
The schema was checked against an isolated OpenCode 2.1.4 `/openapi.json`; admission
alone is not a completed run. No model or live installation was used for that check.

Task lifecycle uses the existing one-task metadata slot, not new messages or a task
queue. Negotiated `task_lifecycle` adds explicit `accepted`/`working`/`blocked`/`done`
and a validated received-request link. Linked updates retain omitted request/state/
scope; metadata does not ACK mail, wake a model, or fulfill an ask. Results still use
normal replies. Persistence failure rolls back; current links survive body eviction.

Pi/OMP presence now samples native `isIdle()` when publishing, rather than treating
`agent_end` as final idle. Settlement (including abort) requests an observation;
older hosts also get native sampling on the existing heartbeat. Missing/throwing
signals remain unknown. Adapter regressions reproduce the old early-idle error and
verify the fix, terminal error/abort handling, and no telemetry-triggered model turn.

Installed Pi 1.4.0 and OMP 17.3.5 SDK checks pass with real session/extension runtimes
and a deterministic fake model stream. They exercise idle renaming (1200ms bound),
native tool calls, invalid required parameters, same-key retries, busy steering,
session-entry persistence before ACK, terminal errors, aborts and idle-triggered
message delivery. They use an isolated broker, fresh HOME/config/data directories,
no inherited credentials, and a network-denying fetch guard. No live installation
or real provider is exercised.

The SDK test exposed Pi's rejection of camelCase system-prompt section names; the
adapter now uses `agent_mesh`. OMP may repair model arguments (including dropping
extra properties) before calling the tool. The adapter independently validates the
parameters it actually receives; it cannot reconstruct pre-normalization input.

### Re-run the optional installed-SDK checks

Set these variables to the installed **package directories**, not executable paths:

```sh
AGM_PI_SDK=/path/to/@earendil-works/pi-coding-agent \
AGM_OMP_SDK=/path/to/@oh-my-pi/pi-coding-agent \
go test -race -count=1 ./internal/integrations -run '^TestInstalledSDKAdapters$' -v
```

Pi uses Node; OMP uses Bun. Ordinary CI skips each SDK not explicitly configured.
Failed fixtures retain their generated adapter, data and `output.log` in the printed
`/tmp/agm-sdk-*` directory. Successful fixtures clean up their temporary files.

Final verification passed: `make fmt-check vet`, then
`go test -race -count=1 -timeout 30m ./...` with both installed-SDK variables set,
and `git diff --check`. The complete run took 796 seconds. These isolated checks
do not exercise live installations or real-provider requests.
