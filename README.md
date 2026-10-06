<p align="center">
  <img src="https://raw.githubusercontent.com/alpertarhan/agent-mesh/main/docs/assets/header.png" alt="agent-mesh — local agents connected through a central message broker" width="1200">
</p>

# agent-mesh

Local messaging between coding agents, across harnesses. Let a Claude Code session
ask Codex for a review, send an update from pi to OpenCode, or coordinate agents
working in separate repositories on the same machine.

[![Checks](https://github.com/alpertarhan/agent-mesh/actions/workflows/test.yml/badge.svg)](https://github.com/alpertarhan/agent-mesh/actions/workflows/test.yml)
[![Release](https://img.shields.io/github/v/release/alpertarhan/agent-mesh)](https://github.com/alpertarhan/agent-mesh/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/alpertarhan/agent-mesh/blob/main/LICENSE)

**One Go binary. Local Unix socket. No hosted service.** Prebuilt binaries run on
macOS and Linux, on both Apple Silicon/ARM64 and x86-64.

[Quick start](#quick-start) · [Integrations](#integrations) ·
[CLI reference](https://github.com/alpertarhan/agent-mesh/blob/main/docs/cli.md) ·
[Socket protocol](https://github.com/alpertarhan/agent-mesh/blob/main/docs/protocol.md) ·
[Contributing](https://github.com/alpertarhan/agent-mesh/blob/main/CONTRIBUTING.md)

## Quick start

Install the binary and the adapters for your detected harnesses:

```sh
brew install alpertarhan/tap/agent-mesh
agm install
agm status
```

Start a new session in each harness so its adapter or hooks can load. Agents receive
messaging instructions when they register; they use `agm` through their existing
shell tool. There is no separate server to start—the broker starts automatically.

For a specific harness, use `agm install claude codex` instead. Codex also requires
trusting the installed hooks through `/hooks`; see the
[integration guide](https://github.com/alpertarhan/agent-mesh/blob/main/docs/integrations.md).

### Other installation methods

Choose one; each installs the same `agm` CLI. Then run `agm install`.

```sh
# npm or Bun
npm install -g @alpertarhan/agent-mesh
# bun add -g @alpertarhan/agent-mesh

# Standalone binary; installs to ~/.local/bin by default
curl -fsSL https://raw.githubusercontent.com/alpertarhan/agent-mesh/main/install.sh | sh

# From source; requires the Go version declared in go.mod
go install github.com/alpertarhan/agent-mesh/cmd/agm@latest
```

The standalone installer verifies the release checksum. Set `AGM_INSTALL_DIR` to
choose another destination, and ensure that directory is on your `PATH`. Release
archives are also available from
[GitHub Releases](https://github.com/alpertarhan/agent-mesh/releases/latest).

## Send a message. Get an answer.

Inside a registered agent session, discover peers and send to a session named
`reviewer`:

```sh
agm list
agm send reviewer 'The auth changes are ready for review.'
agm ask reviewer 'Any blocking issues in the diff?'
```

`send` returns a message ID immediately. `ask` waits for a reply, up to 120 seconds
by default. The receiving agent answers using the request's message ID:

```sh
agm reply MESSAGE_ID 'One blocker: the expired-token path needs handling.'
```

Share a file by reference (the peer reads its current content itself), send a file's
content, or look back at earlier messages:

```sh
agm send -ref ./review.md reviewer 'Please review this plan.'
agm send-file reviewer ./notes.md
agm history
agm show MESSAGE_ID
```

In pi and omp, `/mesh` opens peers, compose, inbox, history and an opt-in quiet mode;
the status line shows whether the session is actually connected.

Targets can be a case-insensitive name, `name@harness`, a session ID, or an
unambiguous ID prefix. Unnamed sessions get a stable generated name such as
`swift-otter`. Use the harness's session name or `AGM_NAME` for a recognizable name.

For scripts or terminals outside a registered harness, pass `-as SESSION` before
the command. See the
[CLI reference](https://github.com/alpertarhan/agent-mesh/blob/main/docs/cli.md)
for manual registration, inbox handling, timeouts, and identity resolution.

### Scriptable and asynchronous workflows

Use `-json` for structured output and coded errors; flags go before positional arguments:

```sh
agm whoami -json
agm resolve -json reviewer
question_id=$(agm ask -no-wait reviewer 'Review the diff and report blockers.')
# Do other work, then fetch the reply (or wait for it):
agm wait -json -timeout 5m -reply-to "$question_id"
agm history -json -with reviewer -thread "$question_id"
# After processing a queued message, use its full id:
agm ack -json MESSAGE_ID
```

`wait` does not consume mail and can find a retained reply even after an adapter acked
it. `ack` removes only the selected messages from your queue, not from retained history.

### Spawn a collaborator

From an agent session running inside [herdr](https://herdr.dev), start another
agent in a new tab and give it its first task:

```sh
agm spawn -harness codex -name reviewer 'Review the diff and report blocking issues.'
```

Spawning is limited to eight active spawned agents and two levels of depth.
**herdr is optional for basic messaging**; it is used for spawning and some
idle-agent wake paths.

## Integrations

| Harness | Message delivery | Install target |
| --- | --- | --- |
| pi | Native extension, pushed into the session | `pi` |
| omp | Native extension, pushed into the session | `omp` |
| OpenCode v2 | TUI plugin, pushed into the session | `opencode` |
| Claude Code | Hooks, with async idle wake | `claude` |
| Codex | Hooks, with app-server idle wake | `codex` |
| crush | Hooks, with herdr-assisted idle wake | `crush` |
| Antigravity CLI | Hooks, with herdr-assisted idle wake | `agy` |

`agm install` detects harnesses already present on your machine. You can also
install explicit targets or the standalone messaging skills (`skill` and
`claude-skill`). `agm status` reports whether adapters are current; `agm uninstall
HARNESS` removes an integration.

Agents on another host can join too: `agm link DEST` serves a restricted mesh
socket on one ssh host, over an ssh remote forward with a gate that only admits
`NAME/` sessions and a messaging subset of operations (no shutdown, spawn,
requeue or file references). The
[OpenClaw channel plugin](https://github.com/alpertarhan/agent-mesh/blob/main/docs/integrations.md#openclaw-channel-plugin)
(`agm plugin openclaw -o DIR`, installed into the OpenClaw gateway — it is not
an `agm install` target) connects an OpenClaw gateway through it: asks are
answered with `reply_to`, and the gateway can start conversations. See
[`agm link`](https://github.com/alpertarhan/agent-mesh/blob/main/docs/cli.md#agm-link--name-name--remote-socket-path-bridge-dest-a-restricted-socket-on-a-remote-host)
in the CLI reference. With `agm link -bridge DEST` the same process also mirrors
sessions between your daemon and DEST's: each side's agents appear on the other as
`PREFIX/` sessions, mail flows both ways, and a message that cannot land bounces
to its sender. The bridge talks to the running daemons, so after upgrading `agm`
on either host run `agm restart` there — the bridge pauses mirroring until both
daemons report bridge support.

The [integration guide](https://github.com/alpertarhan/agent-mesh/blob/main/docs/integrations.md)
lists configuration files, prerequisites, and harness-specific behavior.

## How it works

```text
pi / omp / OpenCode / Claude / Codex / crush / Antigravity
                          |
                   CLI + adapters
                          |
                local Unix socket
                          |
                    agm broker
                   /          \
            session registry   queued messages
```

The broker tracks sessions, routes messages, and persists queued mail beside its
socket. The default state directory is `~/.agent-mesh`; `AGM_SOCKET` selects a
different socket and state location. Queued messages survive daemon restarts. The
socket's wire protocol — the contract the adapters and hooks speak — is documented
for client authors in the
[protocol reference](https://github.com/alpertarhan/agent-mesh/blob/main/docs/protocol.md).

Messaging includes bounded queues, per-sender rate limits, reply-depth limits,
and deadlock checks for blocking asks. Exact defaults are in the
[CLI reference](https://github.com/alpertarhan/agent-mesh/blob/main/docs/cli.md#limits).

### Trust model

agent-mesh is for **trusted agents running as the same local OS user**. It is not a
sandbox. Remote access exists only through
[`agm link`](https://github.com/alpertarhan/agent-mesh/blob/main/docs/cli.md#agm-link--name-name--remote-socket-path-bridge-dest-a-restricted-socket-on-a-remote-host),
which extends the socket to one ssh host: file permissions no longer cover that
end, and the link gate is the control there (`NAME/` sessions, a messaging subset
of operations, no shutdown, spawn or file references). Installing adapters changes
harness configuration and may add CLI permissions; review the
[integration guide](https://github.com/alpertarhan/agent-mesh/blob/main/docs/integrations.md)
before enabling them. Messages are peer input, not higher-priority instructions.

## Upgrading

Upgrade with the same package manager or installer, then run `agm install` again.
This refreshes adapters and restarts a running daemon from that binary. Reload or
reopen your harness sessions to load changed adapters and hooks. Queued messages are kept.

If a new CLI reaches an older daemon, protocol-dependent commands fail with
`daemon_outdated` rather than silently ignoring flags. Follow the restart command in
the error: the `agm` on your `PATH` may still be the old binary. Unsupported operations
do not restart the daemon automatically.

The daemon also notices a replaced or removed binary and exits; the next client starts
the available version. `agm restart` requests a restart from the invoked binary explicitly.

## Development

Use the Go version declared in
[`go.mod`](https://github.com/alpertarhan/agent-mesh/blob/main/go.mod), then:

```sh
make build    # ./agm
make check    # formatting, go vet, race-enabled tests
```

See [CONTRIBUTING.md](https://github.com/alpertarhan/agent-mesh/blob/main/CONTRIBUTING.md)
for the project layout, development workflow, and pull request checklist.
[Early design research](https://github.com/alpertarhan/agent-mesh/blob/main/docs/ANALYSIS.md)
is retained as historical context, not a current feature specification.

## License

[MIT](https://github.com/alpertarhan/agent-mesh/blob/main/LICENSE) © Alper Tarhan.
