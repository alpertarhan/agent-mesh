# Remote agents: design analysis

> **Design analysis.** Written on 2026-09-29 against agent-mesh v0.3.0. On 2026-09-30:
>
> - the Level 0 test results and the status of `docs/protocol.md` were added;
> - the open question was answered, and section 8 records the chosen design
>   (`agm link`), which is being implemented.
>
> Otherwise the note is not kept in sync with the code. For current
> behavior, read the [CLI reference](cli.md), the [socket protocol](protocol.md) and
> the [integration guide](integrations.md). Earlier research ([ANALYSIS.md](ANALYSIS.md))
> left multi-machine setups out of scope and ACP/A2A for later; this note revisits both.

**Goal:** let agents and people on one machine reach agents on another machine: a dev
box or server whose harnesses are connected to agm there, or servers that run agent
services such as pi, Hermes, OpenClaw or QwenPaw, reachable over a VPN or SSH.

**Short answer:** three levels, plus the link chosen on 2026-09-30 for two-way
conversations (section 8). None of them needs a broker change to start. The daemon
stays machine-local, and SSH does the authentication.

| Level | Reaches | How | Status |
|---|---|---|---|
| 0 | Sessions on a host that runs agm | The agm CLI on that host, run over SSH (option E) | Uses only v0.3.0 commands. Tested on a local daemon, not yet over SSH; replies after 10 idle minutes need the next release (section 4) |
| 1 | Harnesses that speak ACP, with or without agm on the host | `agm remote`: a proxy session that talks the Agent Client Protocol (ACP) over SSH (option D) | Proposed |
| 2 | Live sessions on another host, in both directions | A bridge between two daemons (option C) | Only if needed; wants `name@host` addressing |
| Link | Agents on any host reachable over SSH, in both directions; that host can also start conversations | `agm link` plus an adapter for each harness on that host (option A behind a gate, section 8) | Chosen on 2026-09-30. `agm link` and the OpenClaw plugin are done; CLI-based harnesses are next |

The daemon's socket protocol is already the plugin interface: the pi and opencode
adapters use it from outside the daemon, and `agm remote` would be one more adapter.

## 1. What is machine-local in agm today

Verified in the v0.3.0 code:

- **Transport and identity.** The daemon listens only on a Unix socket and has no
  authentication: `hello` accepts whatever session id the client states
  (`internal/broker/server.go`). The socket file's permissions are the security
  boundary. Serving the protocol over TCP would let anyone who can connect
  impersonate a session and send prompts to agents.
- **Liveness.** A session is live while it has a subscriber or while its PID passes
  `kill(pid, 0)` (`Broker.live`, `pidAlive`). A remote PID means nothing locally.
  Hook-based sessions (Claude Code, Codex, crush, Antigravity) would look dead and,
  with an empty mailbox, be removed at the next 30-second sweep. They would look
  alive when the number happens to match a local process.
- **Auto-start.** `dial()` starts a daemon on the socket path when it cannot connect
  (`cmd/agm/client.go`). If a forwarded socket's tunnel drops, the remote side starts
  its own daemon and the mesh splits in two.
- **Local-only features.** `-ref` sends an absolute local path. Idle wake-up (herdr
  nudge, Codex app-server) and `spawn` work on the local machine only.
- **Protocol limits:**
  - One session per connection (`connection already bound`).
  - The only event is `message`; there are no join or leave events.
  - The hop limit (`MaxHops`, 8) applies only to reply chains. A relayed message sent
    as a new message starts again at hop 0, so a bridge has to prevent loops itself.
  - A pending ask expires after `AskTimeout` (120 s), but a late reply is still
    routed while the question is among the last 4096 messages (`seenCap`, kept in the
    spool across restarts). `wait -reply-to` finds it in history (500 messages or
    4 MiB).

## 2. How the remote harnesses can be driven

Checked in their documentation in September 2026:

| Harness | ACP over stdio | Other interfaces |
|---|---|---|
| pi | Community adapters such as `pi-acp` (MVP; wraps `pi --mode rpc`) | `pi --mode rpc` (JSONL over stdio), SDK, agm's pi adapter |
| Hermes | `hermes acp` | TUI gateway JSON-RPC over stdio or WebSocket (can attach to live sessions of that gateway process); OpenAI-compatible HTTP API with SSE |
| OpenClaw | `openclaw acp`: bridges to the running Gateway and maps ACP sessions to Gateway session keys | Gateway WebSocket, default port 18789. The docs recommend a loopback bind reached through Tailscale Serve, a trusted LAN or tailnet bind, or an SSH tunnel |
| QwenPaw | `qwenpaw acp [--agent NAME] [--workspace DIR]` (logs on stderr) | REST `POST /api/console/chat` on port 8088; requests from localhost skip authentication |

- ACP over stdio is the common denominator: `ssh host hermes acp` needs no open port.
  ACP's own remote transport (HTTP or WebSocket) is still a work in progress in the
  specification, so stdio carried by SSH is the robust choice today.
- `hermes acp`, `qwenpaw acp` and `pi-acp` start a new agent process per connection
  (possibly sharing the on-disk workspace). They do not attach to an interactive
  session already open on the server; Hermes' TUI gateway can, within its own
  process. `openclaw acp` talks to the running Gateway and its sessions.
- ACP support is broad. In September 2026 the ACP site lists 42 agents, including
  Claude (through the `claude-agent-acp` adapter), Codex CLI (`codex-acp`), Gemini CLI,
  OpenCode, Goose, Hermes, OpenClaw and pi (`pi-acp`). crush, Antigravity and omp are
  not listed; on another host they are reachable through Level 0.

## 3. Options

| Option | Core change | Pros | Cons |
|---|---|---|---|
| **A.** Forward the socket over SSH (`ssh -R`) | None | No code; the pi adapter works on the server | Wrong liveness; split mesh when the tunnel drops; `-ref` broken; the laptop becomes the hub; only for harnesses with an agm adapter |
| **A+.** Forward a gated socket: `agm link` (chosen, section 8) | None; edge code only | Two-way, and the server can start conversations; no agm needed on the server; SSH authentication; the gate confines the server to its own `NAME/` sessions | The laptop is the hub; liveness follows the tunnel; each harness on the server needs an adapter |
| **B.** TCP/TLS listener on the daemon | Large: transport, authentication, host field | One mesh | New attack surface; the PID, `-ref` and wake-up problems remain |
| **C.** Bridge between two daemons | None strictly: one proxy connection per exposed session. `name@host` addressing and join/leave events make it practical | Two-way talk with live sessions on other hosts | Message-id mapping, proxy sessions on both sides, loop prevention, split history; the most complex option |
| **D.** Remote-agent adapter over ACP | None | Every ACP harness, with or without agm on the host; SSH authentication; the daemon stays local | One direction (us to them); most harnesses start a fresh agent session per connection |
| **E.** The agm CLI on the remote host, over SSH | None | Works with today's commands; reaches the live sessions agm knows on that host; SSH authentication | Replies are fetched with `wait`, not pushed; the caller needs an explicit identity (`-as`), and CLI-only identities are removed after 10 idle minutes (in 0.3.0 a reply to a removed identity never reaches it; fixed in the next release, see section 4); text should come from stdin (`*-file -`) to avoid quoting through two shells |

## 4. Recommendation: three levels, as ports and adapters

agm stays the message bus between agents. Human dashboards across machines and chat
channels (Telegram or Slack bots in agent platforms) are separate layers; agm does not
grow into a hub.

### Level 0: the agm CLI over SSH (option E)

For hosts where agm and the harness adapters are installed. No code:

```bash
# Once: an identity on the remote mesh (a plain shell is not a registered harness).
ssh devbox agm -as alice-laptop hello -name alice -harness shell
# Ask. The text comes from stdin, so it reaches agm unchanged through both shells.
printf '%s' "$question" | ssh devbox agm -as alice-laptop ask-file -no-wait bob -
# Later: the reply, even if bob's adapter already acked it.
ssh devbox agm -as alice-laptop wait -timeout 10m -reply-to QUESTION_ID
```

Tested on isolated daemons with the real 10-minute GC. Both askers were removed
about 10.5 minutes after asking, one of them while it was blocked in `wait`:

- agm 0.3.0 loses the reply. The replier gets `unknown_target`, and the asker's
  `wait` runs into its timeout.
- From the next release, a reply always reaches the asker's exact id. The daemon
  recreates a removed asker as an offline mailbox, kept for up to 24 hours:
  - an open `wait` gets the reply pushed;
  - a later `wait` with the same `-as` id finds it.

  The same change fixes a misdelivery in 0.3.0: such a reply went to another
  session with the same name, or with a matching id prefix.

Still to test: the same flow over a real SSH connection, and how idle sessions on that
host are woken (herdr, Codex app-server).

### Level 1: `agm remote` over ACP (option D)

```text
 laptop ────────────────────────────────────────────────────────
   agents → CLI · pi.ts · opencode.js · hooks        (existing adapters)
                       │ NDJSON port
                   [ broker ]                         (domain, unchanged)
                       │ NDJSON port
           agm remote  = proxy session "hermes-srv1"  (new adapter)
 ──────────────────────┼─────────────────────────────────────────
                       │ ssh srv1 hermes acp   (stdio JSON-RPC, SSH authentication)
                   Hermes (its own sandbox and approval settings)
```

Patterns and their jobs here:

- **Ports and adapters (hexagonal architecture):** the broker is the core, the socket
  protocol is the port, and `agm remote` is one more adapter. Dependencies point
  inward; the broker knows nothing about ACP.
- **Proxy and Channel Adapter:** the remote agent appears as an ordinary session, so
  `list`, `ask -no-wait`, `wait` and `history` work unchanged.
- **Anti-corruption layer and Message Translator:** the mapping between agm's
  MSG/ASK/REPLY and ACP's `session/prompt`, `session/update` and
  `session/request_permission` stays inside the adapter.
- **Correlation Identifier:** each mesh sender maps to an ACP session, and each mesh
  message to an ACP prompt.
- **Ambassador:** the `ssh` process represents the remote end; network, keys and
  reconnection are its job.
- **One adapter per protocol, not per harness:** one ACP driver serves every ACP
  harness. Write a driver interface only when a second protocol is needed.

First version:

- **Run:** `agm remote -name hermes-srv1 -- ssh srv1 hermes acp`, one long-running
  process per remote agent (launchd, systemd or a herdr pane).
- **Registration:** after the ACP handshake, hello and subscribe without a PID; the
  connection itself is the liveness signal.
- **Flow:** an incoming message becomes a prompt in the sender's ACP session. For an
  ask, the final answer goes back with `reply`. Prompts in one session run one at a
  time. Mail is ACKed only after the reply is sent (at least once); after a restart,
  a message that already has a reply in history is skipped.
- **Long work:** senders use `ask -no-wait` and `wait -reply-to` (see the ask-lifetime
  note in section 1).
- **Permission requests** from the remote agent are denied by default and allowed
  with a flag. The real boundary is the remote agent's own sandbox and approval
  settings.
- **Files:** the adapter does not advertise ACP's `fs/*` and `terminal/*` client
  capabilities, so the remote agent cannot read laptop files. This matches the rule
  that file-content reading is a separately permitted command. `-ref` paths enter
  the prompt with a note that they are laptop paths.
- **Estimated size:** 300–400 lines of stdlib Go, tested against a fake ACP agent.

### Level 2: a bridge between daemons (option C), only if needed

For two-way conversations with live sessions on another host, including another
person's machine. A sketch, not a design:

- One `agm link` process per linked host reaches the other daemon through SSH's
  stdio, so no port opens.
- Each side shows the other side's sessions as proxies named `name@host` and relays
  messages both ways, mapping message ids so replies and threads stay intact.
- The link carries a hop count across hosts to stop loops, and marks `-ref` paths as
  belonging to the other host.
- Core work: host-qualified names and join/leave events (Protocol 3), so proxies
  appear and disappear with the remote sessions.

## 5. Security

- **SSH forced command:** in the server's `authorized_keys`,
  `restrict,command="hermes acp" ssh-ed25519 …` lets that key run only this command,
  with forwarding and PTY allocation disabled. The VPN only provides reachability.
- **Larger blast radius:** remote agents run shell commands on servers, so malicious
  content read by a local agent can turn into commands on a server (Prompt
  Infection; OWASP LLM01 prompt injection and LLM06 excessive agency). Mitigations:
  - least privilege for remote agents;
  - an allowlist of which local sessions may use which remote agent;
  - mesh history as an audit trail, bounded to 500 messages or 4 MiB.
- **Do not expose** the daemon over TCP. A QwenPaw REST endpoint reached through an
  SSH tunnel counts as localhost and skips authentication, so SSH access equals full
  API access.

## 6. When the core would change

- **Option E:** no change; documentation only.
- **Option D:** no change. The code lives in `cmd/agm/remote.go` (edge code, like
  `hook.go` and `wake.go`) or in a separate binary; `internal/broker` stays as is.
- **Core work, done for the next release:** [`docs/protocol.md`](protocol.md)
  documents the socket protocol (`Protocol=2`), so third-party plugins get a stable
  API. A test (`proto_doc_test.go`) fails when an op or error code has no entry there.
- **Later, if needed:** a `Host` field on sessions (`name@host`, Protocol 3), and
  join and leave events for option C.
- **Not recommended:** options A and B, for the reasons in section 1.

## 7. Phases

0. **No code:** reply delivery for Level 0 is tested on a local daemon (section 4).
   Still to do:
   - run the same flow over a real SSH connection;
   - test how idle sessions are woken;
   - document Level 0 in the CLI reference;
   - on each server, try `ssh srvX <harness> acp` with a forced command and a small
     script, and see how permission requests and session continuity behave.
1. **`agm remote`** (Level 1) with the ACP driver and one harness, for example Hermes.
   It builds on the socket contract in `docs/protocol.md`.
2. **Other harnesses:** only the command changes. If a harness without ACP is needed,
   add a second driver (pi RPC or A2A), and only then a shared interface.
3. **Bridge (Level 2):** if two-way talk with live sessions on other hosts is needed.

## 8. Decision (2026-09-30): two-way links

The open question was whether server agents would be used as services or for two-way
conversations. The answer is both:

- Server agents must be able to answer, and to start a conversation at any time.
- The design must be general: anyone can link the agents on their own servers with
  the harnesses on their own machine.

The chosen design is option A made safe. `agm link` forwards a socket over SSH through
a policy gate, and each harness on the server gets an adapter.

| Part | What it is | Status |
|---|---|---|
| `agm link DEST` | Edge code on the laptop: `ssh -R` of a restricted socket. No agm is needed on the server. See the [CLI reference](cli.md) and [link sockets](protocol.md#link-sockets) | Step 3a, done |
| OpenClaw | A channel plugin inside the Gateway: one session per mesh peer, answers sent with `reply_to`, conversations started from its `message` tool, and a queue while the link is down | Step 3b, done |
| pi, Claude Code, Codex, opencode, crush | The agm CLI and its adapters in a remote mode | Step 3c, next |
| ACP-only harnesses (Hermes, QwenPaw) | Level 1, `agm remote` | Proposed |
| Anything else | A client of the [link socket protocol](protocol.md#link-sockets) | Possible today |

**The gate** sits in front of the socket port. The broker is unchanged, and `Protocol`
stays 2. The gate covers the reasons that section 1 gives against option A:

- **Namespace:** remote sessions live in their own namespace. Their ids and names must
  start with `NAME/`.
- **Stripped `hello`:** the gate rebuilds `hello` from id, name, harness and cwd. It
  drops PID, pane and parent, which mean nothing on the laptop.
- **Refused:** `shutdown`, `spawn`, `requeue`, unknown ops, `ref` attachments, and
  harnesses that the daemon wakes locally (`codex`, `crush`, `agy`).
- **Checked requests:** every request is checked and re-encoded, never forwarded as
  raw bytes. A link carries at most 32 remote connections.
- **Untrusted answers from the server:** the server's answers to the link itself are
  untrusted.
  - The remote directory must be a plain path, because ssh expands `${VAR}` and `%`
    tokens in `-R` with laptop values.
  - The server's stderr is bounded and cleaned before it reaches the terminal.

**Limits that remain:**

- **The laptop is the hub** for the hosts it links. It is still the user's own daemon
  on a Unix socket: agm opens no network port and has no accounts. While the laptop is
  off or asleep, linked hosts reach no one; the OpenClaw plugin queues its sends.
- **Liveness follows the tunnel.** A remote session has no PID, so it is live only while
  it has a subscriber.
  - After a disconnect it stays registered for 10 minutes if its mailbox is empty, and
    up to 24 hours if mail is waiting (`IdleTTL`, `MailTTL`).
  - Its next `hello` brings it back.
- **Open points for 3c** (see section 1):
  - Never auto-start a daemon on a link socket. Otherwise the mesh splits when the
    tunnel drops.
  - Put the `NAME/` prefix on session ids and names.
  - Keep hook-based sessions (Claude Code, Codex) alive. They neither subscribe nor
    have a PID that the laptop can check, so their liveness has to be checked on the
    server. One way is a small server-side process that subscribes for them.
  - Run idle wake-ups (herdr nudge, Codex app-server) on the server. One option is for
    the gate to label remote harnesses instead of refusing them, so the laptop's waker
    ignores them.
- **Teams, or servers that talk to each other without a laptop,** need the hub
  elsewhere: a daemon on a server that laptops link to, or Level 2.

## References

- Alistair Cockburn, *Hexagonal Architecture* (2005): ports and adapters.
- Gregor Hohpe and Bobby Woolf, *Enterprise Integration Patterns* (2003): Channel
  Adapter, Messaging Bridge, Message Translator, Correlation Identifier.
- Eric Evans, *Domain-Driven Design* (2003): anti-corruption layer.
- Brendan Burns and David Oppenheimer, *Design Patterns for Container-based
  Distributed Systems* (HotCloud 2016): sidecar and ambassador.
- Agent Client Protocol: <https://agentclientprotocol.com> (JSON-RPC over stdio;
  remote transport in progress); agent list:
  <https://agentclientprotocol.com/get-started/agents>.
- A2A Protocol 1.0: <https://a2a-protocol.org/latest/specification/> (Agent Card,
  task states, SSE, push notifications).
- A. Ehtesham et al., *A survey of agent interoperability protocols: MCP, ACP, A2A,
  and ANP*, arXiv:2505.02279 (2025). Its "ACP" is IBM's Agent Communication Protocol,
  not the Agent Client Protocol.
- D. Lee and M. Tiwari, *Prompt Infection: LLM-to-LLM Prompt Injection within
  Multi-Agent Systems*, arXiv:2410.07283 (2024).
- OWASP Top 10 for LLM Applications 2025: LLM01 Prompt Injection, LLM06 Excessive
  Agency.
- Harness documentation:
  - Hermes: <https://hermes-agent.nousresearch.com/docs/developer-guide/programmatic-integration>
  - OpenClaw: <https://docs.openclaw.ai/cli/acp>, <https://docs.openclaw.ai/gateway/remote>
  - QwenPaw: [ACP integration](https://github.com/agentscope-ai/QwenPaw/blob/main/website/public/docs/acp-integration.en.md),
    [REST API](https://github.com/agentscope-ai/QwenPaw/blob/main/website/public/docs/api-tutorial.en.md)
  - pi: `docs/rpc.md` in the pi-coding-agent package; `pi-acp` on npm:
    <https://www.npmjs.com/package/pi-acp>
