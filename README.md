# agent-mesh

Local messaging between coding agents in different harnesses: pi, omp, opencode v2,
Claude Code, Codex, crush, Antigravity CLI. One Go binary, no dependencies. Agents can also spawn each
other in [herdr](https://herdr.dev) tabs.

```bash
brew install alpertarhan/tap/agent-mesh      # macOS, Linux
npm i -g @alpertarhan/agent-mesh             # or: bun add -g @alpertarhan/agent-mesh
yay -S agent-mesh-bin                        # Arch (AUR)
curl -fsSL https://raw.githubusercontent.com/alpertarhan/agent-mesh/main/install.sh | sh
go install github.com/alpertarhan/agent-mesh/cmd/agm@latest

agm install   # hooks/adapters for every detected harness; `agm status` to check
```

Upgrade with the same tool, then run `agm install` again (no-op when nothing changed).
The daemon restarts itself on the new binary; queued messages are kept.

Agents use the CLI from their shell tool (they are told how on session start):

```bash
agm list                        # sessions
agm send reviewer "PR is up"    # message
agm ask reviewer "LGTM?"        # blocks until `agm reply <id> ...`
agm spawn -harness codex "review the diff and report"   # new herdr tab, task as first prompt
```

Targets: name (case-insensitive), `name@harness`, id or id prefix. Unnamed sessions get
stable generated names (`swift-otter`); set one with the harness's session name or `AGM_NAME`.

Delivery: pi/omp/opencode adapters push into the session, Claude/Codex/crush/agy get mail via
hooks; idle agents are woken (Claude async hook, Codex app-server, crush/agy herdr nudge).

Limits: 256 queued per session, 20 msg/min per sender, reply chains ≤ 8 hops, asks time out
after 120s and are refused if they would deadlock, ≤ 8 spawned agents, ≤ 2 levels deep.

Design notes: [docs/ANALYSIS.md](docs/ANALYSIS.md).

macOS and Linux. MIT licensed.
