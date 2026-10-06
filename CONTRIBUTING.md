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
report the module version Go records at build time (for example `v0.2.0+dirty` in a
modified checkout), or `dev` when there is none.

## Layout

```
cmd/agm/                  CLI and daemon entry point
  main.go                 command dispatch and usage text
  client.go               daemon connection, auto-start, restart, protocol check
  errors.go               CLI error codes, -json output and errors
  input.go, format.go     message text/-ref/stdin input; human output formats
  hook.go                 `agm hook` entry point for crush, Claude Code, Codex, Antigravity
  install.go              `agm install`/`uninstall`/`status`
  spawn.go, trust.go      `agm spawn` in herdr tabs, directory trust checks
  wake.go                 waking idle sessions (Codex app-server, herdr nudge)
internal/broker/          router, NDJSON socket protocol, session names, spool
  history.go              message history, show, filters, reply wait
internal/codex/           delivery into Codex through its app-server
internal/integrations/    install targets per harness and the embedded adapters
  integrations.go         target table: detect dir, owned file, edits to shared config
  text.go                 canonical agent-facing wording (hooks, CLI, injected adapters)
  pi.ts, opencode.js      adapters for pi/omp and opencode (embedded with go:embed)
  skill.md                the agent-mesh skill (embedded)
  testdata/               Node checks for the pi/omp and opencode adapters
docs/cli.md               commands, identity, state and limits
docs/protocol.md          the daemon socket protocol, for adapter and tool authors
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
  config files (recognized by content). Unchanged files must not be rewritten;
  `agm install` still restarts a running daemon. `agm uninstall` must leave the user's
  own configuration intact.
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
- The `cmd/agm` package needs roughly 13 minutes under `-race` (its bridge probes
  time real round-trip windows on purpose), so `make check` and CI pass
  `-timeout 30m`. A plain `go test ./cmd/agm/` panics at Go's default 10-minute
  cap: pass `-timeout 25m` (or more, as the package grows) when running it
  yourself.
- Add or update tests for behavior changes.
- Update the relevant guide in `docs/` and `README.md` when commands, harness support
  or limits change.

## Releases

Maintainers release by pushing an unused semantic-version tag such as `v0.2.0`.
Never move or reuse a published tag. Before pushing one:

1. Run `make check` with Node available so adapter checks are not skipped, then
   `goreleaser check` and `goreleaser build --snapshot --clean` to check all four builds
   without publishing.
2. Commit and push to `main`. Wait for the test workflow to pass on Linux and macOS
   for that exact commit. The release workflow does **not** wait for the test workflow.
3. Create an annotated tag at the tested commit and push that tag.

`.github/workflows/release.yml` then:

- builds `agm` for darwin and linux (amd64, arm64) with GoReleaser, publishes the GitHub
  release with checksums and build provenance attestations, and updates the AUR package
  when its key is configured;
- pushes a formula bump branch to `alpertarhan/homebrew-tap`;
- publishes the same binaries to npm as `@alpertarhan/agent-mesh`.

The Homebrew job only pushes the bump branch: also wait for the tap's separate
`bottles` workflow to finish. It builds and tests bottles before publishing the formula
on the tap's `main`. An absent `AUR_KEY` means AUR publication is intentionally skipped.

After publication:

- Confirm the tag's test and release workflows, and the tap's `bottles` workflow, passed.
- Verify the release archives against `checksums.txt` and their provenance with
  `gh attestation verify <archive> -R alpertarhan/agent-mesh`.
- Check the published binary's `agm version` and smoke-test with a scratch `HOME` and
  `AGM_SOCKET`, not the live mesh. Check the npm version and `latest` tag, and that its
  four bundled binaries match the GitHub archives.
- Review the generated GitHub release notes (`changelog: use: github-native`) and add
  user-facing highlights and upgrade instructions where needed.

`npm/package.json` deliberately stays at `0.0.0`: the release job sets its version
from the tag. Do not bump it manually. Repository docs can be updated on `main`
without changing an existing release; the embedded skill and adapters only reach
installed users through a new build and `agm install`.
