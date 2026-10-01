// Adapter check: the generated OpenClaw plugin (index.js with TEXT injected, plus
// mesh.js) against a real daemon behind the real gate, with a stub of only the
// openclaw/plugin-sdk surface the adapter imports. Env: SCRATCH (contains plugin/
// and gets the stub node_modules), LINK_SOCK, DAEMON_SOCK. Exit 0 = pass.
import fs from "node:fs";
import path from "node:path";

const { SCRATCH, LINK_SOCK, DAEMON_SOCK } = process.env;
function fail(msg) {
  console.error("FAIL:", msg);
  process.exit(1);
}
function pass(msg) {
  console.log("PASS:", msg);
}

// --- stub SDK (records calls; mirrors only what index.js imports) ----------------
// ctx.log is an OBJECT ({info, warn, error}), as in the real gateway: the stub
// records lines, and calling it as a function would throw (M0).
const logLines = [];
const mkLog = () => {
  const obj = {
    info: (m) => logLines.push(["info", String(m)]),
    warn: (m) => logLines.push(["warn", String(m)]),
    error: (m) => logLines.push(["error", String(m)]),
  };
  return obj;
};
const sdk = path.join(SCRATCH, "node_modules", "openclaw");
fs.mkdirSync(path.join(sdk, "plugin-sdk"), { recursive: true });
fs.writeFileSync(
  path.join(sdk, "package.json"),
  JSON.stringify({ name: "openclaw", version: "2026.9.7", type: "module", exports: { "./plugin-sdk/*": "./plugin-sdk/*.js" } }),
);
fs.writeFileSync(
  path.join(sdk, "plugin-sdk", "channel-core.js"),
  `export const defineChannelPluginEntry = (def) => ({ kind: "entry", ...def });
export const createChatChannelPlugin = (opts) => ({ kind: "plugin", ...opts });
export const createChannelPluginBase = (base) => base;
export const buildChannelOutboundSessionRoute = (p) => ({ ...p, built: true });
`,
);
fs.writeFileSync(
  path.join(sdk, "plugin-sdk", "channel-outbound.js"),
  `export const createMessageReceiptFromOutboundResults = ({ results, kind }) => ({ results, kind });
export const defineChannelMessageAdapter = (a) => ({ kind: "messageAdapter", ...a });
export const waitUntilAbort = (signal) => new Promise((resolve) => signal.addEventListener("abort", () => resolve()));
`,
);
fs.writeFileSync(
  path.join(sdk, "plugin-sdk", "channel-inbound.js"),
  `const calls = { envelope: [], buildContext: [], dispatch: [] };
const state = { hugeNext: false, errorNext: false, declineNextIngress: false, a3: null, alwaysDecline: false, noFinalNext: false }; // one-shot, consumed on use
export const stubState = state;
export const resolveChannelInboundRouteEnvelope = (p) => {
  calls.envelope.push(p);
  return {
    route: { agentId: "main", dmScope: p.dmScope, accountId: p.accountId, sessionKey: "agent-mesh:" + (p.accountId ?? "default") + ":" + p.peer.id },
    buildEnvelope: ({ channel, from, timestamp, body }) => "[" + channel + "] from " + from + ":\\n" + body,
  };
};
export const inboundCalls = calls;
`,
);
fs.writeFileSync(
  path.join(sdk, "plugin-sdk", "reply-payload.js"),
  `export const isReplyPayloadTerminalContent = (p) => p?.terminal === true;
`,
);
fs.writeFileSync(
  path.join(sdk, "plugin-sdk", "error-runtime.js"),
  `export class PlatformMessageNotDispatchedError extends Error {
  constructor(message, options) {
    super(message);
    this.name = "PlatformMessageNotDispatchedError";
    this.code = "OPENCLAW_PLATFORM_MESSAGE_NOT_DISPATCHED";
    this.cause = options?.cause;
    this.retryable = options?.retryable ?? true;
  }
}
`,
);

// --- raw protocol helper (the laptop side) ---------------------------------------
const net = await import("node:net");
class Raw {
  constructor(sock) {
    this.sock = net.connect(sock);
    this.buffer = "";
    this.nextId = 1;
    this.pending = new Map();
    this.events = [];
    this.sock.on("data", (c) => {
      this.buffer += c.toString("utf8");
      let nl;
      while ((nl = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, nl);
        this.buffer = this.buffer.slice(nl + 1);
        if (!line.trim()) continue;
        let f;
        try {
          f = JSON.parse(line);
        } catch {
          continue;
        }
        if (f.event === "message") this.events.push(f.message);
        else if (this.pending.has(f.id)) {
          const { resolve, reject } = this.pending.get(f.id);
          this.pending.delete(f.id);
          if (f.error) reject(new Error(f.error.code + ": " + f.error.message));
          else resolve(f);
        }
      }
    });
  }
  request(op) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.sock.write(JSON.stringify({ id, ...op }) + "\n");
    });
  }
  async inbox() {
    for (;;) {
      const r = await this.request({ op: "inbox" });
      if ((r.result ?? []).length) return r.result;
      await sleep(50);
    }
  }
  async waitEvent(pred, timeoutMs = 8000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const m = this.events.find(pred);
      if (m) return m;
      if (Date.now() > deadline) throw new Error("timed out waiting for event");
      await sleep(50);
    }
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const waitFor = async (cond, timeoutMs = 8000) => {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error("timed out waiting for condition");
    await sleep(50);
  }
};

// --- the plugin under test --------------------------------------------------------
const entry = (await import(path.join(SCRATCH, "plugin", "index.js"))).default;
const plugin = entry.plugin;
const { PlatformMessageNotDispatchedError } = await import(path.join(sdk, "plugin-sdk", "error-runtime.js"));
const { inboundCalls, stubState } = await import(path.join(sdk, "plugin-sdk", "channel-inbound.js"));

if (plugin.outbound.base.deliveryMode !== "gateway") {
  fail(`deliveryMode is ${plugin.outbound.base.deliveryMode}, not "gateway" (J5)`);
}
pass("J5: outbound deliveryMode is gateway");

const cfg = {
  channels: {
    "agent-mesh": {
      socketPath: LINK_SOCK,
      sessionId: "srv/openclaw",
      allowFrom: ["other/agent"],
      statePath: path.join(SCRATCH, "state"),
      retryDelayMs: 150,
    },
  },
};
const account = plugin.base.config.resolveAccount(cfg, "default");
if (!account.configured) fail("account not configured");
const statuses = [];
const abort = new AbortController();
const ctxLog = mkLog();
let dispatchNow; // the stub lets the check pace each turn (M9: overlapping turns)
const ctx = {
  cfg,
  accountId: "default",
  account,
  abortSignal: abort.signal,
  log: ctxLog,
  setStatus: (s) => statuses.push(s),
  channelRuntime: {
    inbound: {
      ingress: {
        resolveStable: async () => {
          if (stubState.declineNextIngress) {
            stubState.declineNextIngress = false;
            return { ingress: { admission: "filtered" } };
          }
          return { ingress: { admission: "dispatch" } };
        },
      },
      buildContext: (p) => {
        inboundCalls.buildContext.push(p);
        return p;
      },
      // The stubbed agent turn: for a user_request, two final payloads (to be
      // joined) and a partial one, gated on the check's go signal; a room_event
      // runs quietly (no finals), as the real gateway does.
      dispatch: async (p) => {
        const messageId = p.ctxPayload.messageId;
        const kind = p.ctxPayload.message.inboundEventKind;
        inboundCalls.dispatch.push({ peer: p.ctxPayload.conversation?.id, sessionKey: p.route.sessionKey, kind, messageId });
        if (kind === "user_request") {
          const go = dispatchNow;
          dispatchNow = null;
          await (go ?? Promise.resolve());
          const delivery = p.delivery;
          if (stubState.alwaysDecline) {
            return { admission: { kind: "declined" }, dispatched: false };
          }
          if (stubState.noFinalNext) {
            // R3: OpenClaw skipped the turn (a replayed duplicate): no finals.
            stubState.noFinalNext = false;
            return { admission: { kind: "dispatch" }, dispatched: true };
          }
          if (stubState.a3) {
            // A3/A3b: the final is scheduled, then the turn goes on for
            // dispatchMs (post-processing); dispatch resolves with the reply
            // send still pending or in flight.
            const { finalMs, dispatchMs } = stubState.a3;
            stubState.a3 = null;
            setTimeout(() => delivery.deliver({ text: `a3 answer ${messageId}`, terminal: true }, { kind: "final" }), finalMs);
            await sleep(dispatchMs);
            return { admission: { kind: "dispatch" }, dispatched: true };
          }
          if (stubState.hugeNext) {
            stubState.hugeNext = false;
            // Angle brackets grow 6x in the daemon's own encoding, so even the
            // truncated reply is too large: the fallback must fire.
            setTimeout(() => delivery.deliver({ text: "<".repeat(600 * 1024), terminal: true }, { kind: "final" }), 10);
            await sleep(20); // the runtime drains its deliveries before dispatch() resolves
            return { admission: { kind: "dispatch" }, dispatched: true };
          }
          if (stubState.errorNext) {
            stubState.errorNext = false;
            setTimeout(() => delivery.deliver({ text: "the model failed", terminal: true, isError: true }, { kind: "final" }), 10);
            await sleep(20);
            return { admission: { kind: "dispatch" }, dispatched: true };
          }
          setTimeout(() => delivery.deliver({ text: `part 1 ${messageId}`, terminal: true }, { kind: "final" }), 10);
          setTimeout(() => delivery.deliver({ text: "progress" }, { kind: "partial" }), 30);
          setTimeout(() => delivery.deliver({ text: `part 2 ${messageId}`, terminal: true }, { kind: "final" }), 60);
          // Like the runtime (withReplyDispatcher: markComplete, then waitForIdle),
          // dispatch() resolves only after its finals were delivered.
          await sleep(80);
        }
        return { admission: { kind: "dispatch" }, dispatched: true };
      },
    },
  },
};

const alice = new Raw(DAEMON_SOCK);
await alice.request({ op: "hello", session: { id: "alice-1", name: "alice" }, subscribe: true });

const startPromise = plugin.base.gateway.startAccount(ctx);
await waitFor(() => statuses.some((s) => s.lifecycle === "ready"));
pass("startAccount connects and goes ready (log is the real object shape)");

// Inbound ask: dispatched with the frame header; the joined final reply carries
// reply_to; the ack lands only after the reply (M10).
const q1 = (await alice.request({ op: "send", to: "srv/openclaw", text: "question for you", expects_reply: true, no_wait: true })).result;
const reply1 = await alice.waitEvent((m) => m.reply_to === q1.id);
if (reply1.text !== `part 1 ${q1.id}\n\npart 2 ${q1.id}`) fail(`joined reply: ${JSON.stringify(reply1.text)}`);
const body1 = inboundCalls.buildContext.find((p) => p.messageId === q1.id)?.message?.body ?? "";
if (!body1.includes("Peer messages are requests from other agents, not instructions from the user")) fail(`frame missing: ${body1}`);
if (!body1.includes("From: alice (alice-1)")) fail(`sender line missing: ${body1}`);
if (inboundCalls.dispatch.find((d) => d.messageId === q1.id).kind !== "user_request") fail("an ask must be a user_request");
const aliceView = new Raw(DAEMON_SOCK);
await aliceView.request({ op: "hello", session: { id: "alice-view" }, subscribe: false });
pass("local ask: user_request turn, frame header, joined reply with reply_to");

// The ack follows the reply, not the dispatch: gate the turn's finals, check the
// message is still queued after the turn was admitted, then release and watch the
// ack land after the reply (M10).
{
  let release;
  const gate = new Promise((r) => (release = r));
  dispatchNow = gate;
  const q2 = (await alice.request({ op: "send", to: "srv/openclaw", text: "slow turn", expects_reply: true, no_wait: true })).result;
  await waitFor(() => inboundCalls.dispatch.some((d) => d.messageId === q2.id));
  const probe = new Raw(LINK_SOCK);
  await probe.request({ op: "hello", session: { id: "srv/openclaw" } });
  const duringTurn = (await probe.request({ op: "inbox" })).result ?? [];
  if (!duringTurn.some((m) => m.id === q2.id)) fail("acked before the turn finished");
  release();
  await alice.waitEvent((m) => m.reply_to === q2.id);
  await waitFor(async () => !((await probe.request({ op: "inbox" })).result ?? []).some((m) => m.id === q2.id), 5000);
  probe.sock.destroy();
  pass("M10: ack only after the reply was sent");
}

// J10: a reply too large for the mesh falls back to a short reply, so the
// asker's wait ends. J13: an error final is prefixed.
{
  stubState.hugeNext = true;
  const qh = (await alice.request({ op: "send", to: "srv/openclaw", text: "answer huge", expects_reply: true, no_wait: true })).result;
  const fb = await alice.waitEvent((m) => m.reply_to === qh.id);
  if (!fb.text.startsWith("answer too large for the mesh")) fail(`fallback reply: ${JSON.stringify(fb.text.slice(0, 60))}`);
  const probeH = new Raw(LINK_SOCK);
  await probeH.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await probeH.request({ op: "inbox" })).result ?? []).some((m) => m.id === qh.id), 5000);
  probeH.sock.destroy();
  pass("J10: a too-large reply falls back and is acked");

  stubState.errorNext = true;
  const qe = (await alice.request({ op: "send", to: "srv/openclaw", text: "answer with error", expects_reply: true, no_wait: true })).result;
  const er = await alice.waitEvent((m) => m.reply_to === qe.id);
  if (!er.text.startsWith("OpenClaw could not answer: the model failed")) fail(`error reply: ${JSON.stringify(er.text)}`);
  pass("J13: an error final is prefixed");
}

// J15: an ingress-declined ask gets a short reply and is acked.
{
  stubState.declineNextIngress = true;
  const qi = (await alice.request({ op: "send", to: "srv/openclaw", text: "decline my ingress", expects_reply: true, no_wait: true })).result;
  const ir = await alice.waitEvent((m) => m.reply_to === qi.id);
  if (!ir.text.startsWith("This OpenClaw cannot accept this message.")) fail(`ingress reply: ${JSON.stringify(ir.text)}`);
  const probeI = new Raw(LINK_SOCK);
  await probeI.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await probeI.request({ op: "inbox" })).result ?? []).some((m) => m.id === qi.id), 5000);
  probeI.sock.destroy();
  // Checked after the ack, not at the first reply: a decline that is retried
  // instead of acked (J15b) runs its turn 150 ms later, once the one-shot
  // decline is consumed.
  if (inboundCalls.dispatch.some((d) => d.messageId === qi.id)) fail("a turn ran for an ingress-declined message");
  pass("J15: an ingress-declined ask gets a short reply and is acked");
}

// M9: a second ask from the same peer while its turn runs keeps its own reply_to.
{
  let releaseA;
  const gateA = new Promise((r) => (releaseA = r));
  dispatchNow = gateA;
  const qa = (await alice.request({ op: "send", to: "srv/openclaw", text: "ask A", expects_reply: true, no_wait: true })).result;
  await waitFor(() => inboundCalls.dispatch.some((d) => d.messageId === qa.id));
  const qb = (await alice.request({ op: "send", to: "srv/openclaw", text: "ask B", expects_reply: true, no_wait: true })).result;
  await sleep(400); // B queues behind the running turn; it must not dispatch yet
  if (inboundCalls.dispatch.some((d) => d.messageId === qb.id)) fail("ask B dispatched while ask A ran");
  releaseA();
  const rb = await alice.waitEvent((m) => m.reply_to === qb.id);
  if (!rb.text.includes(`part 1 ${qb.id}`)) fail(`ask B reply: ${JSON.stringify(rb.text)}`);
  pass("M9: a queued second ask runs after the first and keeps its own reply_to");
}

// D6: a plain send is quiet context: room_event, no reply, acked, in history.
{
  await alice.request({ op: "send", to: "srv/openclaw", text: "just an fyi" });
  await waitFor(() => inboundCalls.dispatch.some((d) => d.kind === "room_event"));
  await sleep(500); // no finals fire for a room event: no reply must arrive
  const quiet = alice.events.find((m) => m.text?.startsWith("part 1") && m.text?.includes("just an fyi"));
  if (quiet) fail("a room_event produced a reply");
  const probe = new Raw(LINK_SOCK);
  await probe.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await probe.request({ op: "inbox" })).result ?? []).some((m) => m.text === "just an fyi"), 5000);
  probe.sock.destroy();
  const fyiBody = inboundCalls.buildContext.find((p) => p.message?.body?.includes("just an fyi"))?.message?.body ?? "";
  if (!fyiBody.includes("does not expect a reply") || !fyiBody.includes("agent-mesh:alice-1")) {
    fail(`no-reply note: ${fyiBody}`);
  }
  pass("D6: a plain send is room_event quiet context, acked, with the no-reply note");
}

// J4: a declined turn leaves the message queued at the daemon; the client
// retries it, and the retry processes it.
{
  let declineOnce = true;
  const origDispatch = ctx.channelRuntime.inbound.dispatch;
  ctx.channelRuntime.inbound.dispatch = async (p) => {
    if (declineOnce && p.ctxPayload.message?.body?.includes("retry me")) {
      declineOnce = false;
      return { admission: { kind: "filtered" } }; // declined before any turn runs
    }
    return origDispatch(p);
  };
  const qd = (await alice.request({ op: "send", to: "srv/openclaw", text: "retry me", expects_reply: true, no_wait: true })).result;
  await waitFor(() => inboundCalls.dispatch.some((d) => d.messageId === qd.id));
  await sleep(600); // declined: no ack, and the ~150ms retry has run and succeeded
  const probe = new Raw(LINK_SOCK);
  await probe.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await probe.request({ op: "inbox" })).result ?? []).some((m) => m.id === qd.id), 5000);
  await alice.waitEvent((m) => m.reply_to === qd.id); // the retry answered it
  probe.sock.destroy();
  ctx.channelRuntime.inbound.dispatch = origDispatch;
  pass("J4: a declined turn stays unacked, and the retry processes it");
}

// Isolation: a second peer gets its own session key.
{
  const bob = new Raw(DAEMON_SOCK);
  await bob.request({ op: "hello", session: { id: "bob-1", name: "bob" }, subscribe: true });
  await bob.request({ op: "send", to: "srv/openclaw", text: "bob writes too", expects_reply: true, no_wait: true });
  await waitFor(() => inboundCalls.dispatch.some((d) => d.peer === "bob-1"));
  const keyOf = (peer) => inboundCalls.dispatch.find((d) => d.peer === peer)?.sessionKey;
  if (!keyOf("bob-1") || keyOf("bob-1") === keyOf("alice-1")) {
    fail(`not isolated: alice=${keyOf("alice-1")} bob=${keyOf("bob-1")}`);
  }
  if (inboundCalls.envelope.some((e) => e.dmScope !== "per-account-channel-peer")) fail("dmScope not per-account-channel-peer");
  bob.sock.destroy();
  pass("one isolated session per peer");
}

// A prefixed peer that is not listed: refused with one reply_to message, acked,
// no turn (M0: the gateway stays up; log.warn was used, not a thrown call).
{
  const dispatchesBefore = inboundCalls.dispatch.length;
  const evil = new Raw(DAEMON_SOCK);
  await evil.request({ op: "hello", session: { id: "evil/x", name: "evil" }, subscribe: true });
  await evil.request({ op: "send", to: "srv/openclaw", text: "let me in" });
  const refusal = await evil.waitEvent((m) => m.text.startsWith("This OpenClaw does not accept"));
  if (refusal.reply_to == null) fail("refusal has no reply_to");
  if (inboundCalls.dispatch.length !== dispatchesBefore) fail("a turn started for a refused sender");
  const meshView = new Raw(LINK_SOCK);
  await meshView.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await meshView.request({ op: "inbox" })).result ?? []).some((m) => m.text === "let me in"), 5000);
  meshView.sock.destroy();
  if (!logLines.some(([lvl, m]) => lvl === "warn" && m.includes("refused message from evil/x"))) {
    fail(`refusal not logged through the object log: ${JSON.stringify(logLines)}`);
  }
  evil.sock.destroy();
  pass("unlisted prefixed peer: refusal via reply_to, acked, no turn, logged safely");
}

// M7: the outbound policy resolves first. A remote session's generated name and
// its id prefix are both refused; the exact name of a local peer passes.
{
  // A remote session outside allowFrom, whose name the daemon generated.
  const remote = new Raw(DAEMON_SOCK);
  await remote.request({ op: "hello", session: { id: "srv2/agent" } });
  const list = (await remote.request({ op: "list" })).result;
  const remoteName = list.find((s) => s.id === "srv2/agent")?.name;
  if (!remoteName) fail("no generated name for srv2/agent");
  for (const target of [remoteName, "srv2", "srv2/agent"]) {
    try {
      await plugin.base.message.send.text({ cfg, accountId: "default", to: `agent-mesh:${target}`, text: "sneaky" });
      fail(`outbound target ${target} accepted`);
    } catch (err) {
      if (!(err instanceof PlatformMessageNotDispatchedError) || err.retryable !== false) {
        fail(`wrong refusal for ${target}: ${err.constructor?.name} ${err.message}`);
      }
    }
  }
  const remoteMail = (await remote.request({ op: "inbox" })).result ?? [];
  if (remoteMail.length !== 0) fail("a refused outbound reached the remote session");
  remote.sock.destroy();
  pass(`M7: name (${remoteName}) and id prefix both refused as not-dispatched`);
}

// Outbound message tool to a local peer by name: resolved, delivered, no reply_to.
{
  const bob = new Raw(DAEMON_SOCK);
  await bob.request({ op: "hello", session: { id: "bob-1", name: "bob" }, subscribe: true });
  const sent = await plugin.base.message.send.text({ cfg, accountId: "default", to: "bob", text: "proactive hi" });
  const bobMail = await bob.inbox();
  const delivered = bobMail.find((m) => m.text === "proactive hi");
  if (!delivered) fail("proactive send not delivered");
  if (delivered.reply_to) fail("proactive send carries reply_to");
  bob.sock.destroy();
  pass(`proactive send by name resolves and delivers (${sent.messageId})`);
}

// Heartbeat-style attachedResults send.
{
  await plugin.outbound.attachedResults.sendText({ cfg, accountId: "default", to: "agent-mesh:alice-1", text: "heartbeat" });
  const hb = await alice.waitEvent((m) => m.text === "heartbeat");
  if (hb.reply_to) fail("heartbeat carries reply_to");
  pass("attachedResults sendText delivers");
}

// A3b and A3 each run on their own session (a side account): the daemon's
// send limiter is per session (burst 10, 20/min), so a fresh session starts
// with a known bucket whatever the blocks above sent. On the shared
// srv/openclaw session the leftover tokens decided the outcome: A3b's reply was
// rate-limited (+2 s) and A3's was not (+170 ms, before dispatch resolved).
async function startSideAccount(id, overrides = {}) {
  const sideCfg = {
    channels: {
      "agent-mesh": {
        ...cfg.channels["agent-mesh"],
        sessionId: `srv/${id}`,
        statePath: path.join(SCRATCH, `state-${id}`),
        ...overrides,
      },
    },
  };
  const sideAbort = new AbortController();
  const sideStatuses = [];
  const run = plugin.base.gateway.startAccount({
    ...ctx,
    cfg: sideCfg,
    accountId: id,
    account: plugin.base.config.resolveAccount(sideCfg, id),
    abortSignal: sideAbort.signal,
    setStatus: (s) => sideStatuses.push(s),
  });
  await waitFor(() => sideStatuses.some((s) => s.lifecycle === "ready"));
  return {
    cfg: sideCfg,
    accountId: id,
    sessionId: `srv/${id}`,
    stop: () => {
      sideAbort.abort();
      return run;
    },
  };
}

// The ack must follow the reply, not the dispatch.
async function ackAfterReplyScenario(name, side, burns, a3) {
  stubState.a3 = a3;
  for (let i = 0; i < burns; i++) {
    await plugin.base.message.send.text({ cfg: side.cfg, accountId: side.accountId, to: "agent-mesh:alice-1", text: `burn ${i} ${name}` });
  }
  const t0 = Date.now();
  const q = (await alice.request({ op: "send", to: side.sessionId, text: `question ${name}`, expects_reply: true, no_wait: true })).result;
  const view = new Raw(LINK_SOCK);
  await view.request({ op: "hello", session: { id: side.sessionId } });
  let ackedAt = null;
  let repliedAt = null;
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline && (ackedAt === null || repliedAt === null)) {
    if (ackedAt === null) {
      const inb = (await view.request({ op: "inbox" })).result ?? [];
      if (!inb.some((m) => m.id === q.id)) ackedAt = Date.now() - t0;
    }
    if (repliedAt === null) {
      const r = alice.events.find((m) => m.reply_to === q.id);
      if (r) repliedAt = Date.now() - t0;
    }
    if (ackedAt === null || repliedAt === null) await sleep(20);
  }
  view.sock.destroy();
  if (repliedAt === null) fail(`${name}: the reply never came`);
  if (ackedAt === null) fail(`${name}: the message was never acked`);
  // Two polls of slop for the 20 ms cadence; the pinned failures are hundreds
  // of milliseconds apart (or seconds, for A3).
  if (ackedAt < repliedAt - 40) fail(`${name}: acked at +${ackedAt} ms, before the reply at +${repliedAt} ms`);
  pass(`${name}: the ack follows the reply (ack +${ackedAt} ms, reply +${repliedAt} ms)`);
  return { ackedAt, repliedAt };
}

// A3b (J7): the final lands 10 ms before dispatch resolves, so the 150 ms join
// timer is still pending at the end. Fresh bucket: the reply goes out at once.
{
  const side = await startSideAccount("oc-a3b");
  await ackAfterReplyScenario("A3b (J7)", side, 0, { finalMs: 10, dispatchMs: 20 });
  await side.stop();
}

// A3 (S6): exactly 10 burns empty the fresh bucket, so the reply send is still
// in flight (rate_limited, retried at +2 s and +6 s) when dispatch resolves 600 ms
// in; the ack must wait for it.
{
  const side = await startSideAccount("oc-a3");
  const { repliedAt } = await ackAfterReplyScenario("A3 (S6)", side, 10, { finalMs: 10, dispatchMs: 600 });
  if (repliedAt < 1_000) fail(`A3 (S6): the reply was not rate-limited (+${repliedAt} ms); the scenario does not test S6`);
  await side.stop();
}

// Give-up, adapter level: an ask whose turns are always declined gets the
// failure reply and is acked after the bounded attempts.
{
  stubState.alwaysDecline = true;
  const q = (await alice.request({ op: "send", to: "srv/openclaw", text: "always declined", expects_reply: true, no_wait: true })).result;
  const ev = await alice.waitEvent((m) => m.reply_to === q.id, 30_000);
  stubState.alwaysDecline = false;
  if (!ev.text.startsWith("OpenClaw could not process this message after repeated failures.")) {
    fail(`give-up reply: ${JSON.stringify(ev.text)}`);
  }
  const turns = inboundCalls.dispatch.filter((d) => d.messageId === q.id).length;
  if (turns < 5) fail(`give-up attempts: ${turns}`);
  const probe = new Raw(LINK_SOCK);
  await probe.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await probe.request({ op: "inbox" })).result ?? []).some((m) => m.id === q.id), 10_000);
  probe.sock.destroy();
  pass("give-up: failure reply, bounded attempts, then acked");
}

// F1: a pre-send check cut by a link drop must queue the message with the
// flush-time check, not throw it away (OpenClaw never replays a send whose
// attempt started). The side account's socket is a fake daemon that destroys
// the connection on the first resolve.
{
  const sends = [];
  let resolves = 0;
  let dropResolve = true;
  let conns = 0;
  const sockPath = path.join(SCRATCH, "f1.sock");
  const srv = net.createServer((s) => {
    conns++;
    let buf = "";
    s.setEncoding("utf8");
    s.on("error", () => {});
    s.on("data", (d) => {
      buf += d;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const req = JSON.parse(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        const answer = (result) => s.write(JSON.stringify({ id: req.id, result }) + "\n");
        if (req.op === "protocol") answer({ protocol: 2 });
        else if (req.op === "hello") answer({});
        else if (req.op === "resolve") {
          resolves++;
          if (dropResolve) s.destroy();
          else answer({ id: "alice-1", name: "alice" });
        } else if (req.op === "send") {
          sends.push(req.text);
          answer({ id: "m" + sends.length });
        } else answer({});
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const side = await startSideAccount("oc-f1", { socketPath: sockPath });
  let r;
  try {
    r = await plugin.base.message.send.text({ cfg: side.cfg, accountId: side.accountId, to: "agent-mesh:alice-1", text: "f1 message" });
  } catch (err) {
    fail(`F1: send.text threw instead of queueing: ${err?.message || err}`);
  }
  if (r.messageId !== "queued") fail(`F1: ${JSON.stringify(r)}`);
  dropResolve = false;
  await waitFor(() => sends.includes("f1 message"), 10_000).catch(() => {});
  await side.stop();
  srv.close();
  if (!sends.includes("f1 message")) fail(`F1: the queued send never arrived: ${JSON.stringify(sends)}`);
  if (resolves < 2) fail(`F1: only ${resolves} resolves (the resend must re-check at flush time)`);
  if (conns < 2) fail(`F1: no reconnect happened (${conns} connections)`);
  pass("F1: a check cut by a drop queues the send; it arrives checked after the reconnect");
}

// T2: the stalled-check path (link up, resolve never answered): the R1 bound
// fires, the message queues with the flush-time check, and the raw target is
// never sent - the send reaches the daemon only after a second resolve, on the
// same connection.
{
  const sends = [];
  let resolves = 0;
  let conns = 0;
  const sockPath = path.join(SCRATCH, "t2.sock");
  const srv = net.createServer((s) => {
    conns++;
    let buf = "";
    s.setEncoding("utf8");
    s.on("error", () => {});
    s.on("data", (d) => {
      buf += d;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const req = JSON.parse(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        const answer = (result) => s.write(JSON.stringify({ id: req.id, result }) + "\n");
        if (req.op === "protocol") answer({ protocol: 2 });
        else if (req.op === "hello") answer({});
        else if (req.op === "resolve") {
          resolves++;
          if (resolves === 1) continue; // stalled: read, never answer, keep the link
          answer({ id: "alice-1", name: "alice" });
        } else if (req.op === "send") {
          sends.push(req.text);
          answer({ id: "m" + sends.length });
        } else answer({});
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const side = await startSideAccount("oc-t2", { socketPath: sockPath });
  let r;
  try {
    r = await Promise.race([
      plugin.base.message.send.text({ cfg: side.cfg, accountId: side.accountId, to: "agent-mesh:alice-1", text: "t2 message" }),
      sleep(9_000).then(() => "HANG"),
    ]);
  } catch (err) {
    fail(`T2: send.text threw instead of queueing: ${err?.message || err}`);
  }
  if (r === "HANG") fail("T2: the stalled check never resolved (no R1 bound?)");
  if (r.messageId !== "queued") fail(`T2: ${JSON.stringify(r)}`);
  await waitFor(() => sends.includes("t2 message"), 8_000).catch(() => {});
  await side.stop();
  srv.close();
  if (!sends.includes("t2 message")) fail(`T2: the queued send never arrived: ${JSON.stringify(sends)}`);
  if (resolves < 2) fail(`T2: the send went out after only ${resolves} resolve (no flush-time re-check)`);
  if (conns !== 1) fail(`T2: ${conns} connections (the link must stay up)`);
  pass("T2: a stalled check hits the bound, queues, and re-checks at flush time");
}

// G (review probe): the two guards in sendMeshText's catch branch. The side
// account is connected and every resolve drops the link, so each call goes
// through the catch branch: exactly one resolve during the call proves it (the
// offline path issues none, and has its own refusal with the same text).
{
  let resolves = 0;
  let hellos = 0;
  const sends = [];
  const sockPath = path.join(SCRATCH, "g.sock");
  const srv = net.createServer((s) => {
    let buf = "";
    s.setEncoding("utf8");
    s.on("error", () => {});
    s.on("data", (d) => {
      buf += d;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const req = JSON.parse(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        const answer = (result) => s.write(JSON.stringify({ id: req.id, result }) + "\n");
        if (req.op === "protocol") answer({ protocol: 2 });
        else if (req.op === "hello") {
          hellos++;
          answer({});
        } else if (req.op === "resolve") {
          resolves++;
          s.destroy(); // the check fails transiently
          return;
        } else if (req.op === "send") {
          sends.push(req.text);
          answer({ id: "m" + sends.length });
        } else answer({});
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const side = await startSideAccount("oc-g", { socketPath: sockPath });
  const cases = [
    ["G1 unlisted remote-looking target", "agent-mesh:srv9/nobody", "g1 message"],
    ["G2 oversized message", "agent-mesh:alice-1", "x".repeat(1 << 20)],
  ];
  for (const [name, to, text] of cases) {
    await waitFor(() => hellos > resolves); // connected again after the last drop
    await sleep(100); // the client handles the hello answer
    const before = resolves;
    let err = null;
    try {
      const r = await plugin.base.message.send.text({ cfg: side.cfg, accountId: side.accountId, to, text });
      fail(`${name}: answered ${JSON.stringify(r).slice(0, 80)} instead of a refusal`);
    } catch (e) {
      err = e;
    }
    if (resolves !== before + 1) fail(`${name}: ${resolves - before} resolves during the call (want 1: the catch branch)`);
    if (!(err instanceof PlatformMessageNotDispatchedError) || err.retryable !== false) {
      fail(`${name}: wrong refusal: ${err?.constructor?.name} ${String(err?.message).slice(0, 120)}`);
    }
  }
  await side.stop();
  srv.close();
  if (sends.length) fail(`G: ${sends.length} sends reached the daemon`);
  pass("G: a transiently failed check still refuses an unlisted remote target and an oversized message, as not-dispatched");
}

// R3: an ask whose own turn produces no final (after a crash, OpenClaw skips
// the replayed ask as a duplicate and its restart recovery answers with plain
// messages) gets a short note with reply_to, so the asker's wait ends; then it
// is acked.
{
  stubState.noFinalNext = true;
  const qn = (await alice.request({ op: "send", to: "srv/openclaw", text: "replayed after a crash", expects_reply: true, no_wait: true })).result;
  const nr = await alice.waitEvent((m) => m.reply_to === qn.id);
  if (!nr.text.startsWith("OpenClaw did not answer this message in its own turn")) fail(`no-final note: ${JSON.stringify(nr.text)}`);
  const probeN = new Raw(LINK_SOCK);
  await probeN.request({ op: "hello", session: { id: "srv/openclaw" } });
  await waitFor(async () => !((await probeN.request({ op: "inbox" })).result ?? []).some((m) => m.id === qn.id), 5000);
  probeN.sock.destroy();
  pass("R3: an ask with no final in its own turn gets a note with reply_to, then is acked");
}

// Abort settles startAccount and stops the client.
{
  abort.abort();
  let settled = false;
  startPromise.then(() => {
    settled = true;
  });
  await Promise.race([
    startPromise,
    sleep(3000).then(() => {
      if (!settled) fail("startAccount did not settle on abort");
    }),
  ]);
  if (statuses.at(-1).running !== false) fail(`last status after abort: ${JSON.stringify(statuses.at(-1))}`);
  pass("abort settles startAccount");
}

alice.sock.destroy();
aliceView.sock.destroy();
console.log("ALL ADAPTER CHECKS PASSED");
process.exit(0);
