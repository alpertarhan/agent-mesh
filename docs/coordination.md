# Session coordination

Use `agm list` to find a peer and `agm inspect <session>` to check its current work.
Send to a session ID when the recipient must remain the same after a rename.

## Role, task and presence

- A **role** describes a recurring responsibility. Two sessions may declare the same
  role; it is not an address, a permission or an availability signal.
- A **task** describes current work. It can refer to a received request and declare
  a scope or blocker. Updating it does not reserve files, acknowledge mail or send
  a result. Reply to the original request when finished or blocked.
- **Presence** is a transient host observation. A connected session can be busy,
  idle or waiting. Missing metrics do not mean a session is stuck. Pi and OMP
  coalesce updates to at most once per second and send a heartbeat every 10 seconds.
  Observations become stale after 30 seconds or a disconnect; heartbeats do not
  count as progress. A heartbeat does not write the message spool.

See the [CLI guide](cli.md#coordination-and-metric-meaning) for commands, units and
examples, and the [protocol reference](protocol.md) for capability negotiation.
A link can publish tasks for its bound identity. The link gate rejects role writes,
and mirrored sessions do not copy roles.

## Delivery

A broker receipt confirms acceptance, not that the recipient read or completed
the work. Give delegated requests an outcome, scope and acceptance check. Return
the result, supporting evidence and anything not verified. Do not publish secrets
or raw transcripts in role or task descriptions.

Adapters bound the context inserted into a model; additional messages can stay
queued. Use `agm show <message-id>` for the retained full message. Delivery,
acknowledgement and idle wake behavior differ by host; see the
[integration guide](integrations.md#support-matrix).

### Re-run the optional installed-SDK checks

Set these variables to the installed **package directories**, not executable paths:

```sh
AGM_PI_SDK=/path/to/@earendil-works/pi-coding-agent \
AGM_OMP_SDK=/path/to/@oh-my-pi/pi-coding-agent \
go test -race -count=1 ./internal/integrations -run '^TestInstalledSDKAdapters$' -v
```

Pi uses Node; OMP uses Bun. The tests run with a temporary home, an isolated
broker and a deterministic model stream. They do not exercise a real provider.
CI skips each SDK not explicitly configured. Failed fixtures print their
temporary directory for inspection; successful fixtures clean up after themselves.
