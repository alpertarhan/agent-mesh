# The socket protocol

`agm` is a daemon (`agm daemon`) plus clients that talk to it over a local Unix socket:
the `agm` CLI itself, the pi/omp and opencode adapters, and the harness hooks. This
document is the wire contract for writing such a client. The CLI view of the same
behavior (commands, identity, limits) is in [cli.md](cli.md); what each installed
adapter does with the protocol is in [integrations.md](integrations.md).

The implementation lives in `internal/broker/{proto,server,broker,history}.go`; a test
fails when an operation or error code is added there without being documented here.
The protocol version is 2 (see [Versioning](#versioning)).

## Transport

- A Unix stream socket at `$AGM_SOCKET`, default `~/.agent-mesh/mesh.sock`. The daemon
  forces the socket to mode `0600` and its directory to `0700`.
- One daemon per socket. The daemon holds an exclusive `flock` on `<socket>.lock` for
  its life; a second daemon fails to start (`daemon already running`). A leftover
  socket file without a lock holder is removed at startup.
- **There is no authentication.** A session id is a claim, not a credential: any process
  that can connect can act as any session, read and ack its queue, and send as it.
  Socket file permissions are the only boundary, which is why the daemon listens on a
  Unix socket only and must never be bridged to TCP or another machine. See
  [Trust model](cli.md#trust-model).
- Auto-start is client behavior, not part of the protocol: a client dials, and if that
  fails starts `agm daemon` detached (logging to `<socket dir>/daemon.log`) and retries
  for a few seconds. The daemon has no opinion about it.
- `SIGINT`/`SIGTERM` stop the daemon and remove the socket file; any connection can
  request the same with `shutdown`. Queued mail and other state survive in
  `<socket dir>/spool.json`. State files, restarts and the daemon's 30 s tick are
  described in [cli.md](cli.md#socket-state-and-daemon-lifecycle).

For experiments, run an isolated daemon with `AGM_SOCKET` in a temp directory
([cli.md](cli.md#isolated-daemons-with-agm_socket)); never test against `~/.agent-mesh`.

## Framing

One JSON object per line (NDJSON), UTF-8, in both directions. A line is at most
1 MiB (`MaxFrame`), newline included; one byte more gets the connection closed
without a response, so clients must refuse to send such a line at all (the CLI
suggests `-ref` instead).

Three frame shapes:

```text
{"id":1,"op":"send","to":"bob","text":"hi"}                    client → daemon: Request
{"id":1,"result":{"id":"36a3...","from":"alice",...}}           daemon → client: Response
{"id":1,"result":null,"error":{"code":"unknown_target","message":"no session \"bob\"; ..."}}
{"event":"message","message":{"id":"36a3...","from":"alice",...}}  daemon → client: Event
```

- `Request.id` is chosen by the client (an integer, `int64`) and echoed in the
  matching `Response`. Use distinct ids per connection and **match responses by id**:
  events have no id and can arrive between responses, including before the response
  of the request that caused them (the `hello` mailbox replay is pushed before the
  `hello` response). Ignore frames that are neither a response to a pending id nor a
  known event kind.
- One Response per Request, in request order, while the connection stays open:
  requests on one connection are processed serially (use several connections for
  concurrency). The daemon also closes connections *without* answering: a line over
  the frame limit, a full output queue, a newer exclusive subscriber, and `bye` from
  a subscribed connection (below).
- A Response carries `result`, `error`, or both. If `error` is present, the request
  failed: ignore `result`, which a failed request may also carry (`null` or a zero
  value, e.g. from `resolve`; some failures carry none, e.g. `inbox` before `hello`).
  On success, `result` is absent only for ops that return nothing at all (`requeue`,
  `bye`, `shutdown`); an op with nothing to show returns `null` (empty `inbox`,
  `take` or `history`, a still-pending `wait`). `error` is `{"code","message"}`:
  match on `code`; the message is human-readable and not stable.
- Events arrive only on connections registered to receive them (`hello` with
  `subscribe` or `wait`, a blocking ask, or the `wait` op). The only event kind today
  is `message`; future kinds must be ignored by clients.
- Each connection has a 256-line output queue. The daemon never blocks on a slow
  reader: a client that stops reading is disconnected. Reconnect and catch up with
  `inbox`/`history`/`show`.
- A line that is not valid JSON gets a `bad_request` Response with `id` 0. Valid
  JSON with a wrongly typed field is also `bad_request`, echoing the `id` if the
  `id` field itself parsed as an integer. An unknown op gets `bad_request` after
  `hello` and `not_registered` before it (the session check comes first).

### Request fields

Everything a Request can carry (`id` and `op` aside; the `send` fields are top-level,
not nested):

| Field | Used by | Meaning |
|---|---|---|
| `session` | `hello`, `spawn` | a [SessionInfo](#types); on `spawn` only its `name` is read |
| `subscribe` | `hello` | make this connection a subscriber |
| `wait` | `hello` | exclusive subscriber (see [Session binding](#session-binding)) |
| `to` | `send`, `resolve` | target: id, id prefix, name, or `name@harness` |
| `text` | `send` | the body; required (non-blank) |
| `attachments` | `send` | see [Attachment](#types) |
| `reply_to` | `send` | id of the message being answered |
| `expects_reply` | `send` | make this a question |
| `no_wait` | `send` | question whose reply is queued, not pushed |
| `ids` | `ack`, `wait`, `show` | message ids (one each for `wait`/`show`) |
| `messages` | `requeue` | messages to put back |
| `limit` | `history` | max entries (default 20, max 200) |
| `with` | `history` | only messages with this peer |
| `thread` | `history` | only this message's reply thread |

## Session binding

A connection starts anonymous. `hello` binds it to exactly one session id and creates
or refreshes that session's record; non-empty `session` fields overwrite the stored
ones, so a reconnecting adapter just says `hello` again.

- Before `hello`, only `protocol`, `list`, `resolve`, `spawn` and `shutdown` work;
  everything else fails with `not_registered`.
- A bound connection stays bound: a later `hello` with a different session id fails
  with `bad_request` (`connection already bound`). Open a new connection to act as
  another session; one session per connection.
- `subscribe: true` makes the connection a subscriber: new mail for the session is
  pushed as `message` events, and the current mailbox is replayed first. Subscribing
  does not consume anything: mail stays queued until `ack`ed, so a crashing client
  loses nothing. Any number of subscribers may coexist.
- `wait: true` (implies `subscribe`) asks to be the **exclusive** subscriber: the
  daemon closes the previous exclusive subscriber's connection. Background waiters
  that a harness might spawn repeatedly (the Claude Code `--wait` hook) use this.
- `bye` ends the session: its subscribers are closed (the calling connection too, if
  it subscribed; it may close before the `bye` response), and the session is removed
  if its mailbox is empty; with queued mail it is kept (PID cleared) so mail survives
  for a resumed session until garbage collection. A connection that did not
  subscribe survives its own `bye` and stays bound: if the session was removed, its
  later ops fail with `not_registered` until it says `hello` again with the same id;
  while queued mail still keeps the session, they keep working.
- Disconnecting (however it happens) drops this connection's subscriptions, `wait`
  registrations, and exclusive-subscriber status. Asks that were blocking on this
  connection fall back to delivering the reply to your mailbox. The session record
  itself stays until [GC](#limits-and-lifetimes-clients-observe).
- A session is live only while it has a subscriber or while its recorded `pid`
  answers; a bound connection that did not subscribe is not liveness, and neither is
  a connection parked in the `wait` op (see
  [GC](#limits-and-lifetimes-clients-observe)).
  Adapters record the harness process; hooks record the nearest non-shell ancestor.
  A name sent in `hello` is used as given (collisions are possible; resolution prefers
  live sessions). Without a name the daemon derives a stable `adjective-noun` name
  from the session id.

## Operations

State-changing: `hello`, `send`, `ack`, `take`, `requeue`, `bye`, `shutdown`, `spawn`.
The rest only read (or, for `wait`, register a watcher).

### `protocol`

The daemon's protocol version and capabilities. No request fields. Result:
`{"protocol":2,"bridge":1}`. Read-only and works before `hello`. Check it before
relying on any field or op newer than protocol 1 (see [Versioning](#versioning)).
`bridge` reports the daemon's bridge behaviors (rows with `/` in their id are kept
until `MailTTL`, never `IdleTTL`, and are never woken locally); a daemon that does
not report it — v0.4.x, or an upgraded binary nobody restarted — is too old for
`agm link -bridge`, which pauses mirroring until both daemons report it. Unknown
result fields are ignorable: the number is the version, not the field count.

### `hello`

Binds the connection and registers or refreshes the session. Request: `session`
(`id` required), `subscribe`, `wait`. Result: `{"id":"<session id>"}`. Errors:
`bad_request` (no session, blank id, an id that is not one clean line of at most
256 bytes, or the connection is already bound to another
id). Effects: may replay the mailbox as events, and may close a previous exclusive
subscriber.

### `list`

All sessions, sorted by id. Result: an array of [SessionInfo](#types) (with the
output-only `live`, `queued`, `last_seen`). Read-only, works before `hello`.

### `resolve`

The session a `send` to `to` would reach, resolved by the same rules. Result: one
SessionInfo. Errors: as for `send` targets (`unknown_target`, `ambiguous_target`,
`bad_request` for an empty target). Read-only, works before `hello`.

### `send`

Routes one message. Result: the created [Message](#types); its `id` is what the
receiver quotes in `reply_to`. State-changing. Variants:

- **Plain message**: `to` + `text`. Queued for the target and pushed to its
  subscribers.
- **Question (blocking ask)**: add `expects_reply: true`. The `send` Response returns
  at once with the question; only your client blocks. The daemon keeps the question
  open for 120 s, and the reply is later pushed **to the asking connection** as a
  `message` event whose `reply_to` is the question id. It is not queued in your
  mailbox while that connection lives; if the connection is gone by then, it falls
  back to your mailbox. Any client-side timeout is your own: after it, or after the
  daemon drops the ask, a reply is delivered to your mailbox.
- **Question with `no_wait: true`**: requires `expects_reply`. The reply is never
  handed straight to the asking connection the way a blocking ask's is: it always
  goes through your mailbox and history, where the `wait` op finds it. A subscribed
  connection of yours still gets it pushed like any other mail.
- **Reply**: `reply_to` set to a recent message id; `to` may be empty and then means
  the original sender. A reply always reaches its asker when `to` is empty or is the
  asker's exact id: it is routed by that exact id, never by name or prefix, and if the
  asker's session was garbage-collected meanwhile, the daemon recreates it as an
  offline mailbox with a generated name and queues the reply there; an open `wait`
  still gets the push. The usual checks still apply (`hop_limit`, `too_large`, the
  rate limit), and plain sends to a missing session still fail with `unknown_target`.
  The reply's `hop` is the original's plus one; past 8 the chain fails with
  `hop_limit`. `reply_to` must be one of the last 4096 routed message ids.

`attachments` ride along: `file`, `snippet` and `context` carry inline `content`; `ref`
carries only an absolute `path` (at most 4096 bytes, no content, at most 16 per
message) and the **receiver reads that file itself**, seeing its content at read time.
The CLI only produces `ref` attachments (`agm -ref`); the other types are rendered by
receivers as inline attachments. Malformed refs fail with `bad_request`.

Checks and errors, as a client must handle them: `bad_request` (blank text, sending to
yourself, bad refs, `no_wait` without `expects_reply`), `unknown_message` (unknown
`reply_to`), `unknown_target`/`ambiguous_target` (unresolvable `to`; several matches
list `id (harness, cwd)`, live sessions preferred), `mailbox_full` (256 queued for
the target; a reply to a question the target is still waiting on is still accepted),
`rate_limited` (20 messages/min per sender, burst 10), `too_many_asks` (4 questions
pending, `no_wait` ones included), `would_deadlock` (a blocking ask that closes a wait
cycle; `no_wait` asks never do), `too_large` (encoded message event over 1 MiB − 4 KiB
with JSON escaping included; share big content as a `ref`).

An accepted `send` is queued, pushed, archived in history, remembered for reply
routing, and counted against the rate limit, in that one request. One exception: a
reply handed straight to a blocking asker's live connection is pushed but not queued.

### `inbox`

Your oldest queued messages that fit one response frame (at least one if any are
queued). Result: array of Messages. Does not remove anything; an empty mailbox yields
`result: null`. Pair with `ack` after handling each message. Read-only.

### `ack`

Removes messages from **your** mailbox. Request: `ids`. Result: the ids actually
removed (ids that were unknown, foreign or already gone are silently ignored, not an
error). Removing is not a read receipt; nobody is told. State-changing.

### `take`

Like `inbox`, but atomically removes the returned batch (oldest first, one
frame-sized batch; the rest stays queued for the next `take`). For deliverers that
race each other (hooks): one taker wins, exactly once, at the cost of requeueing on
failure. State-changing.

### `history`

One-line [summaries](#types) of messages you sent or received, oldest first, newest
`limit` (default 20, max 200) after filtering (and possibly fewer than `limit`, so
the response fits one frame). Filters: `with` (a peer, resolved like
a target; a peer that no longer exists can be given by its exact id as it appears in
your history) and `thread` (the reply thread of one retained message you sent or
received). Read-only. Errors: `unknown_target`/`ambiguous_target` (bad `with`),
`unknown_message` (bad thread anchor).

### `show`

One full message, by exact id, that you sent or received, from retained history or
your mailbox. Request: exactly one id in `ids`. Result: a Message. Read-only. Errors:
`unknown_message` (not yours, evicted, or wrong form).

### `wait`

The reply to a question **you** sent, usually with `no_wait`. Request: exactly one id
in `ids`. If a reply already arrived and is still retained (in history, or queued
anywhere for you), it is the result; nothing is acked or removed, even if the mail
was delivered and acked long ago. Otherwise the result is `null`: the connection is
registered, and the reply is pushed as a `message` event when it arrives. That is
also what happens when the reply was evicted from history and is no longer queued:
the `wait` then blocks until the client gives up. Lookup and registration are
atomic, so no reply is missed in between. Several `wait`s for the same question all
get the reply. Read-only. Errors: `unknown_message` (not your question, or beyond
the 4096 remembered ids).

### `requeue`

Puts messages back at the **front** of your mailbox after you `take` them and then
fail to deliver them. Request: `messages` (as returned by `take`). No result; silently
ignored if the session is gone. State-changing.

### `bye`

Ends your session (see [Session binding](#session-binding)). No request fields. A
subscribed connection is closed by its own `bye`, possibly before the response is
written — a client must not read the `bye` result on a subscribed connection: it
may never arrive, and that is not an error. An unsubscribed connection gets
`{"id":N}` and stays bound. State-changing.

### `shutdown`

Asks the daemon to exit (what `agm restart` sends). No fields, no result; works
before `hello`. The response usually arrives, then the daemon closes every
connection; treat the close as confirmation. State-changing.

### `spawn`

Reserves a name for an agent the caller is about to start, and records the caller as
its parent if the connection is bound to a session (otherwise the parent is the
user). The reserved name comes from `session.name`; empty gets a generated one.
Result: `{"name":"...","depth":N}`; `depth` is always at least 1: 1 for a user
spawn or a caller at depth 0 (user-started sessions are depth 0), the caller's
depth plus 1 below that. Errors: `spawn_limit` (8 spawned agents live or pending, or
more than 2 levels deep), `name_taken`, `not_registered` (the bound session was
garbage-collected). The reservation expires after 2 minutes; the session that then
says `hello` with that name is linked to the parent. State-changing; works before
`hello`.

## Types

### SessionInfo

Identity fields are peer-supplied and printed everywhere, so `hello` reduces
`name`, `harness`, `cwd` and `pane` to one clean line (every whitespace, control and
format rune becomes a space, runs collapse, ends trim) and caps them; a name that is
empty afterwards means "no name" (the generated name applies). `id` must itself be
one clean line of at most 256 bytes, or `hello` fails with `bad_request`: ids are
identities and are never rewritten.

```json
{"id":"alpha-7","name":"alpha","harness":"pi","cwd":"~/app","pid":4213,"pane":"p3",
 "parent":"","depth":0,"last_seen":"2026-09-30T14:40:37+03:00","live":true,"queued":0}
```

| Field | Direction | Meaning |
|---|---|---|
| `id` | input (required) | session id; free-form, treated as an identity claim |
| `name` | input/output | display name, one clean line, at most 64 runes; empty input keeps the stored or generated one |
| `harness` | input/output | harness tag, one clean line, at most 32 runes (e.g. `pi`, `omp`, `opencode`, `claude`, `codex`, `crush`, `agy`) |
| `cwd` | input/output | working directory, one clean line, uncapped; shown by `agm list` |
| `pid` | input/output | owning harness process; non-zero input overwrites, `0` keeps |
| `pane` | input/output | herdr pane, one clean line, at most 64 runes |
| `parent`, `depth` | output only | set by the daemon from `spawn` reservations |
| `last_seen` | output only | set by `hello` and by the close of a bound connection; other ops leave it (it drives GC) |
| `live`, `queued` | output only | subscriber or live PID; queued message count |

### Message

```json
{"id":"36a30994e4632aa5","from":"beta-9","from_name":"polar-lemur","to":"alpha-7",
 "text":"auth diff is ready - review?","expects_reply":true,"hop":0,
 "at":"2026-09-30T14:40:37.998235+03:00"}
```

`id` is 16 hex characters, daemon-generated. `from_name` is the sender's display name
at send time. `attachments` and `reply_to` appear when used; `hop` counts reply-chain
depth (0 for a new conversation). `at` is the daemon's clock (RFC 3339).

### Attachment

```json
{"type":"ref","name":"review.md","path":"/home/alper/app/review.md"}
{"type":"snippet","name":"diff","content":"...","language":"diff"}
```

`type`: `file`, `snippet`, `context` (inline `content`) or `ref` (`path` only, receiver
reads it). `name` is a display name.

### Summary

One `history` entry: `id`, `dir` (`in`/`out` from your perspective), `from`,
`from_name`, `to`, `to_name`, `reply_to`, `expects_reply`, `at`, `bytes` (encoded
size), `preview` (the whole body on one line, runs of whitespace collapsed to single
spaces, cut at 160 code points with `…` appended when cut) and `refs` (names of `ref`
attachments).

### Error

`{"code":"...","message":"..."}`; see the table below.

## Error codes

| Code | Meaning |
|---|---|
| `bad_request` | malformed frame or JSON, unknown op, missing/blank fields, self-send, bad refs, `no_wait` without `expects_reply`, `hello` without a session, a dirty (not one clean line, or over 256 bytes) session id, re-binding a connection, `wait`/`show` without exactly one id |
| `not_registered` | the op needs a session, but the connection has not said `hello` (or the session is gone) |
| `unknown_target` | no session matches `to`; the message lists live sessions |
| `ambiguous_target` | several sessions match; the message lists `id (harness, cwd)`, so use an id |
| `unknown_message` | `reply_to`/`wait`/`show`/`thread` id that is unknown, too old, or not yours |
| `mailbox_full` | the recipient has 256 messages queued (replies to its pending asks excepted) |
| `rate_limited` | the sender exceeded 20 messages/min (burst 10) |
| `hop_limit` | the reply chain exceeded 8 hops |
| `too_many_asks` | the sender already has 4 pending questions (with or without `no_wait`) |
| `would_deadlock` | a blocking ask would close a wait cycle; use `no_wait` |
| `spawn_limit` | 8 spawned agents already live or pending, or nesting deeper than 2 |
| `name_taken` | a live session or pending spawn already uses that name |
| `too_large` | the encoded message exceeds 1 MiB − 4 KiB |

## Limits and lifetimes clients observe

The daemon's compiled-in defaults, unchanged from the [CLI tables](cli.md#limits):

| Bound | Value | On violation |
|---|---|---|
| Frame | 1 MiB per NDJSON line, newline included | connection closed (no response) |
| Message | 1 MiB − 4 KiB encoded (push event, escaping included) | `too_large` |
| Mailbox | 256 queued per session | `mailbox_full` |
| Send rate | 20/min per sender, burst 10 | `rate_limited` |
| Reply chain | 8 hops | `hop_limit` |
| Pending asks | 4 per sender, 120 s each | `too_many_asks`; late replies go to the mailbox |
| Reply routing | last 4096 message ids remembered | `unknown_message` |
| History | 500 messages / 4 MiB, full bodies | `show` fails with `unknown_message`; an evicted reply is beyond `wait` too |
| Connection queue | 256 outgoing lines | slow readers are disconnected |
| Spawn | 8 live or pending, 2 levels, reservation 2 min | `spawn_limit` / `name_taken` |

A sweep runs every 30 s and removes sessions with no subscriber and no live recorded
PID: at the next sweep if the recorded PID is dead and the mailbox is empty, after
10 minutes idle with an empty mailbox, and after 24 hours even with queued mail.

Two consequences for clients:

- A bound connection that did not subscribe is not liveness, and neither is a
  connection blocked in the `wait` op. The CLI records no PID, so a CLI-only session
  is removed 10 minutes after its last `hello` or disconnect (empty mailbox; with
  queued mail it survives up to the 24-hour rule), even while one of its connections
  sits in `wait`.
- Once removed, the id resolves to nothing for plain sends (`unknown_target` at the
  sender). A reply to a question that session asked is the exception: it recreates
  the session as an offline mailbox and is delivered normally, so an open `wait`
  still gets its reply pushed.

Clients should expect a session to disappear after disconnecting, and simply say
`hello` again.

## Link sockets

`agm link` serves the mesh on a remote host over ssh (`-R`), for clients like the
OpenClaw channel plugin. The gate speaks this same protocol with three
restrictions; `protocol` still returns 2.

- **Op subset.** `protocol`, `hello`, `list`, `resolve`, `send`, `inbox`, `ack`,
  `take`, `history`, `show`, `wait` and `bye` are forwarded. `shutdown`, `spawn`,
  `requeue` and unknown ops are refused. Daemon frames pass through unchanged.
- **Prefix rule.** Every session on a linked host needs an id starting with `NAME/`
  (the link's `-name`, with something after the slash). The display name must
  start with `NAME/` too after the usual one-clean-line normalization; an empty
  name becomes the id (the gate sets it), so a remote session never gets a
  local-looking generated name.
- **Stripped `hello`.** The gate rebuilds the session from `id`, `name`, `harness`
  and `cwd` only: `pid`, `pane`, `parent`, `depth` and the output fields are dropped;
  a remote pid and pane mean nothing locally. A `harness` the daemon would wake
  locally (`codex`, `crush`, `agy`) is refused, as are `ref` attachments (paths do
  not cross hosts); content attachments pass.
- **Refusals** come back on the same stream as `bad_request` with an `agm link:`
  message, and nothing is forwarded. Invalid JSON gets the same error with id 0.
  A refusal is written at once, so it can arrive before the answers to earlier
  requests: match responses by id. A request that grows past one frame when the
  gate re-encodes it (unescaped `<`, `>` and `&` stay one byte, but U+2028/U+2029
  and invalid UTF-8 still grow) is refused as `too_large` instead
  (`agm link: request too large after re-encoding`); genuinely oversized messages
  get the daemon's own `too_large`, exactly as for a direct client.

## The bridge as a client

`agm link -bridge` runs a bridge in the link process: a plain client of two daemons
(the local one directly, the remote one through the same ssh tunnel), no wire
changes. It mirrors sessions whose id has no `/` as `PREFIX/` proxy rows, relays
mail both ways under the sender's proxy identity, and maps `reply_to` pairs across
restarts in `link-NAME.map`. Two behaviors a client of the bridge will observe:

- **Delivery is at-least-once.** A crash between the far send and the map write can
  resend a message once; a `reply_to` whose pair the restarted bridge no longer
  knows is sent unlinked with a one-line note instead of dropped. And a relay over
  a link slower than roughly 16 KiB/s is not retried while bytes keep arriving
  (its deadline is progress-based), so a stalled-but-alive tunnel delays rather
  than duplicates.
- **The bridge finishes what it registered.** Rows it created are tracked in
  `link-NAME.rows`; if a session ends while no running bridge holds its proxy (a
  restart gap, an upgrade, a crash), the restarted bridge answers for that row's
  queued mail — each message bounces to its sender, asks get their bounce as a
  reply — acks it, and byes the row away. A row leaves the set only when a fresh
  far list shows it gone, never because a `bye` returned: a bye does not delete a
  row that still holds mail.

## Versioning

- `{"op":"protocol"}` returns `{"protocol":2,"bridge":1}`. Call it first, on every
  connection, before using anything newer than protocol 1.
- **Older daemons silently ignore unknown request fields.** Sending a newer field to
  an old daemon looks successful while doing nothing (a `ref` would vanish, filters
  would not filter). This is why the CLI fails with `daemon_outdated` instead, and why
  `Protocol` is bumped whenever a request gains fields or ops that an older daemon
  would silently ignore, so new clients can refuse to use them there. A refusal
  restarts nothing: restart the daemon yourself (`agm restart`). Separately, the
  daemon exits on its own within 30 s when its binary was replaced or removed, and
  the next client then starts the new one
  ([cli.md](cli.md#socket-state-and-daemon-lifecycle)).
- Protocol 2 added: `ref` attachments, `history`/`show`, `resolve`, `no_wait` and
  `wait`, history filters, and the `ack` result. Protocol-1 daemons predate the
  `protocol` op itself: they answer an unknown op with `bad_request` (after `hello`)
  or `not_registered` (before it).
- New event kinds and response fields may be added without a bump: clients must
  ignore frames they do not recognize. Unknown request fields are accepted and
  silently dropped (not stored, forwarded or echoed).

## Worked example

Run for real against an isolated daemon (`AGM_SOCKET` in a temp directory), with two
raw connections: A subscribes as `alpha`; B registers and asks A a question; A receives
the event and replies; the reply is pushed to B's waiting connection. Timestamps are
shortened; ids are as printed. The last `inbox` shows that a `send` does not consume
the recipient's mailbox.

```console
$ export AGM_SOCKET="$(mktemp -d /tmp/agm-proto.XXXXXX)/m.sock"
$ agm daemon &                          # a real client would auto-start it
A -> {"id":1,"op":"protocol"}
A <- {"id":1,"result":{"protocol":2}}
A -> {"id":2,"op":"hello","session":{"id":"alpha-7","name":"alpha","harness":"demo","cwd":"~/app"},"subscribe":true}
A <- {"id":2,"result":{"id":"alpha-7"}}
B -> {"id":1,"op":"hello","session":{"id":"beta-9","harness":"demo"}}
B <- {"id":1,"result":{"id":"beta-9"}}
B -> {"id":2,"op":"send","to":"alpha","text":"auth diff is ready - review?","expects_reply":true}
B <- {"id":2,"result":{"id":"36a30994e4632aa5","from":"beta-9","from_name":"polar-lemur","to":"alpha-7",
      "text":"auth diff is ready - review?","expects_reply":true,"hop":0,"at":"14:40:37"}}
A <- {"event":"message","message":{"id":"36a30994e4632aa5","from":"beta-9","from_name":"polar-lemur",
      "to":"alpha-7","text":"auth diff is ready - review?","expects_reply":true,"hop":0,"at":"14:40:37"}}
A -> {"id":3,"op":"send","reply_to":"36a30994e4632aa5","text":"yes, one comment on line 12"}
A <- {"id":3,"result":{"id":"0f81ebac8655f7c0","from":"alpha-7","from_name":"alpha","to":"beta-9",
      "text":"yes, one comment on line 12","reply_to":"36a30994e4632aa5","hop":1,"at":"14:40:37"}}
B <- {"event":"message","message":{"id":"0f81ebac8655f7c0","from":"alpha-7","from_name":"alpha","to":"beta-9",
      "text":"yes, one comment on line 12","reply_to":"36a30994e4632aa5","hop":1,"at":"14:40:37"}}
A -> {"id":4,"op":"inbox"}
A <- {"id":4,"result":[{"id":"36a30994e4632aa5","from":"beta-9","from_name":"polar-lemur","to":"alpha-7",
      "text":"auth diff is ready - review?","expects_reply":true,"hop":0,"at":"14:40:37"}]}
```

B never sent a name, so the daemon generated one (`polar-lemur`, stable for that id).
B's reply arrived as an event on the same connection that asked; it was not queued in
B's mailbox. A's question stays queued until A `ack`s it (`agm -as alpha-7 inbox -ack`
from a plain shell would show and clear it).
