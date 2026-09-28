# Contributing

Bug reports, fixes and new harness integrations are welcome. For anything larger than a
small fix, open an issue first so the change can be discussed before you write it.

## Requirements

- Go 1.27.1, the version in `go.mod` (CI installs it from there). No third-party Go modules.
- Make for the commands below, or run their underlying Go commands directly.
- macOS or Linux. CI runs on `ubuntu-latest` and `macos-latest`.
- Node 22.18+ (or 23.6+) to run the pi/omp and opencode adapter checks
  (`TestAdapterScripts`, scripts in `internal/integrations/testdata`); without it they are skipped.

## Commands

```bash
make build    # ./agm
make test     # go test -race ./...
make fmt      # gofmt -w .
make vet      # go vet ./...
make check    # gofmt check, go vet, go test -race: what CI runs
make clean    # remove ./agm
```

Release builds set `agm version` with `-ldflags "-X main.version=..."`. Other builds
report the module version Go records at build time (for example `v0.1.1+dirty` in a
modified checkout), or `dev` when there is none.

## Layout

```
cmd/agm/                  CLI and daemon entry point
  main.go                 command dispatch and usage text
  client.go               daemon connection, auto-start, restart
  hook.go                 `agm hook` entry point for crush, Claude Code, Codex, Antigravity
  install.go              `agm install`/`uninstall`/`status`
  spawn.go, trust.go      `agm spawn` in herdr tabs, directory trust checks
  wake.go                 waking idle sessions (Codex app-server, herdr nudge)
internal/broker/          router, NDJSON socket protocol, session names, spool
internal/codex/           delivery into Codex through its app-server
internal/integrations/    install targets per harness and the embedded adapters
  integrations.go         target table: detect dir, owned file, edits to shared config
  pi.ts, opencode.js      adapters for pi/omp and opencode (embedded with go:embed)
  skill.md                the agent-mesh skill (embedded)
docs/cli.md               commands, identity, state and limits
docs/integrations.md      harness setup, delivery and permissions
docs/assets/              README artwork
docs/ANALYSIS.md          historical design research, not a current specification
npm/                      npm/bun package wrapper around the release binaries
install.sh                curl installer (downloads the latest GitHub release)
```

## Changing an integration

Harness integrations live in `internal/integrations`, with hook handling in
`cmd/agm/hook.go`.

- A target may fully own one file, and may add or remove only its own entries in shared
  config files (recognized by content). `agm install` must be a no-op when nothing
  changed, and `agm uninstall` must leave the user's own configuration intact.
- Add or update a test in `internal/integrations/integrations_test.go`; tests use a
  temporary directory as `$HOME`.
- To try a change without touching your real setup, run the built binary with a
  throwaway home, which also moves the daemon socket and spool:

  ```bash
  make build
  HOME=$(mktemp -d) ./agm status
  ```

- Say in the pull request which harnesses and versions you exercised by hand.

## Pull requests

- Keep each pull request to one change, and run `make check` before pushing.
- Add or update tests for behavior changes.
- Update the relevant guide in `docs/` and `README.md` when commands, harness support
  or limits change.

## Releases

Maintainers release by pushing a `v*` tag (for example `v0.1.1`). `release.yml` then:

- builds `agm` for darwin and linux (amd64, arm64) with GoReleaser, publishes the GitHub
  release with checksums and build provenance attestations, and updates the AUR package
  when its key is configured;
- pushes a formula bump branch to `alpertarhan/homebrew-tap`;
- publishes the same binaries to npm as `@alpertarhan/agent-mesh`.

Release notes are generated from GitHub (`changelog: use: github-native`).
