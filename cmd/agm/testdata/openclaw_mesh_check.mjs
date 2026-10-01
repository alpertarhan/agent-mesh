// Mesh-client check against a real daemon behind the real 3a gate (serveLink).
// Env: LINK_SOCK (the gate socket), DAEMON_SOCK (the daemon), MESH_JS (generated
// mesh.js path), STATE_DIR (outbox state). Exit 0 = all scenarios pass.
import net from "node:net";
import fs from "node:fs";
import path from "node:path";

const { LINK_SOCK, DAEMON_SOCK, MESH_JS, STATE_DIR } = process.env;
const { MeshClient, MeshError } = await import(MESH_JS);

// B1: count every dial the client makes (a stable link must show exactly one).
const origConnect = net.connect;
net.connect = (...args) => origConnect(...args);

function fail(msg) {
  console.error("FAIL:", msg);
  process.exit(1);
}
function pass(msg) {
  console.log("PASS:", msg);
}

/** A raw protocol connection (the laptop side, straight to the daemon). */
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
          if (f.error) reject(new MeshError(f.error.code, f.error.message));
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
  async waitEvent(text, timeoutMs = 5000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const m = this.events.find((e) => e.text === text);
      if (m) return m;
      if (Date.now() > deadline) fail(`timed out waiting for event ${JSON.stringify(text)}`);
      await sleep(50);
    }
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(cond, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error("timed out waiting for condition");
    await sleep(50);
  }
}

// The laptop peer, straight on the daemon.
const alice = new Raw(DAEMON_SOCK);
await alice.request({ op: "hello", session: { id: "alice-1", name: "alice" }, subscribe: true });

// 1. connect through the gate, under the prefix.
const got = [];
const client = new MeshClient({
  socketPath: LINK_SOCK,
  sessionId: "srv/openclaw",
  name: "srv/openclaw",
  harness: "openclaw",
  statePath: STATE_DIR,
  onMessage: (m) => {
    got.push(m);
    return true;
  },
  log: () => {},
});
client.start().catch(() => {});
await waitFor(() => client.connected);
pass("hello under the prefix through the gate");

// 2. receive; the client acks once the adapter accepted (onMessage returned true).
await alice.request({ op: "send", to: "srv/openclaw", text: "hello channel" });
await waitFor(() => got.some((g) => g.text === "hello channel"));
const view = new Raw(LINK_SOCK);
await view.request({ op: "hello", session: { id: "srv/openclaw" } });
await waitFor(async () => ((await view.request({ op: "inbox" })).result ?? []).length === 0);
pass("receive, ack after acceptance");

// 3. a reply with reply_to reaches a local Wait.
const q = (await alice.request({ op: "send", to: "srv/openclaw", text: "a question", expects_reply: true, no_wait: true })).result;
await client.send({ to: "alice-1", replyTo: q.id, text: "the answer" });
const waitRes = await alice.request({ op: "wait", ids: [q.id] });
if (waitRes.result?.reply_to !== q.id || waitRes.result?.text !== "the answer") {
  fail(`wait: ${JSON.stringify(waitRes.result)}`);
}
pass("reply with reply_to reaches a local wait");

// 4. permanent errors reject (nothing queued). M3: the size check counts bytes:
// 600k two-byte chars are 1.2 MB of UTF-8 and must refuse before queueing.
try {
  await client.send({ to: "nobody-xyz", text: "x" });
  fail("unknown target send resolved");
} catch (err) {
  if (!(err instanceof MeshError) || err.code !== "unknown_target") fail(`send error: ${err.message}`);
}
try {
  await client.send({ to: "alice-1", text: "ş".repeat(600_000) });
  fail("oversized send resolved");
} catch (err) {
  if (!(err instanceof MeshError) || err.code !== "too_large") fail(`size error: ${err.message}`);
}
pass("permanent errors reject; the size limit counts bytes");

// 5. P1/P2: accepted mail is re-acked on replay (even when an ack was lost); mail
// the adapter declines is retried, and the retry processes it.
let decline = true;
const seenIds = [];
const client2 = new MeshClient({
  socketPath: LINK_SOCK,
  sessionId: "srv/openclaw2",
  statePath: path.join(STATE_DIR, "two"),
  retryDelayMs: 100,
  onMessage: (m) => {
    seenIds.push(m.id);
    if (m.text === "decline me once" && decline) {
      decline = false;
      return false;
    }
    return true;
  },
  log: () => {},
});
// Suppress the first ack of the accepted message, to simulate a lost ack.
let suppressAck = true;
const ack2 = client2.ack.bind(client2);
client2.ack = async (...ids) => {
  if (suppressAck) {
    suppressAck = false;
    return;
  }
  return ack2(...ids);
};
client2.start().catch(() => {});
await waitFor(() => client2.connected);
await alice.request({ op: "send", to: "srv/openclaw2", text: "ack gets lost" });
await alice.request({ op: "send", to: "srv/openclaw2", text: "decline me once" });
const view2 = new Raw(LINK_SOCK);
await view2.request({ op: "hello", session: { id: "srv/openclaw2" } });
await waitFor(async () => {
  const box = (await view2.request({ op: "inbox" })).result ?? [];
  return box.length === 1 && box[0].text === "ack gets lost";
}, 3000); // declined once, retried, accepted; only the lost-ack one is queued
client2.sock.destroy();
await waitFor(() => client2.connected); // reconnect replays it
await waitFor(async () => ((await view2.request({ op: "inbox" })).result ?? []).length === 0);
if (seenIds.filter((id) => id === seenIds[0]).length !== 1) fail("replayed accepted mail re-delivered to the adapter");
pass("P1: lost ack re-acked on replay; P2: declined mail retried, then processed");

// 6. M6: a half frame from a dead connection must not corrupt the next one.
const junk = net.createServer((s) => {
  let answered = false;
  s.on("data", () => {
    if (answered) {
      s.destroy();
      return; // a later attempt: drop it, the client retries elsewhere
    }
    answered = true;
    s.end('{"id":1,"result":{"protocol":2}}\n{"id":2,"res');
  });
});
await new Promise((r) => junk.listen(0, r));
const junkPath = `/tmp/agm-junk-${process.pid}.sock`;
junk.close(() => {});
await new Promise((r) => {
  junk.listen(junkPath, r);
});
const client3 = new MeshClient({
  socketPath: junkPath,
  sessionId: "srv/openclaw",
  statePath: path.join(STATE_DIR, "three"),
  onMessage: () => true,
  log: () => {},
});
client3.start().catch(() => {});
await sleep(700);
client3.socketPath = LINK_SOCK; // the next attempt goes to the real gate
client3.sock.destroy();
await waitFor(() => client3.connected, 15_000);
client3.stop();
try { fs.unlinkSync(junkPath); } catch { /* best effort */ }
pass("M6: a half frame from a dead connection does not corrupt the next");

// 7. the outbox: sends while down queue, persist, and flush on reconnect in
// order. J6: a permanently failing head (unknown target) is dropped, and the
// next message is still delivered.
const state4 = path.join(STATE_DIR, "four");
const client4 = new MeshClient({
  socketPath: LINK_SOCK,
  sessionId: "srv/openclaw4",
  statePath: state4,
  onMessage: () => true,
  log: () => {},
});
const r1 = await client4.send({ to: "alice-1", text: "queued one" });
if (!r1.queued) fail("down send did not queue");
client4.enqueue({ msg: { to: "nobody-xyz", text: "queued bad" }, check: 0 });
client4.enqueue({ msg: { to: "alice-1", text: "queued two" }, check: 0 });
client4.start().catch(() => {});
await waitFor(() => client4.connected);
let inboxA;
await waitFor(async () => {
  inboxA = (await alice.request({ op: "inbox" })).result ?? [];
  return inboxA.filter((x) => x.text.startsWith("queued")).length === 2;
});
const order = inboxA.filter((x) => x.text.startsWith("queued")).map((x) => x.text);
if (order[0] !== "queued one" || order[1] !== "queued two") fail(`outbox order: ${order}`);
if ((await client4.request({ op: "send", to: "alice-1", text: "after flush" })).error !== undefined) {
  fail("connection unusable after outbox flush");
}
pass("outbox queues while down, drops a bad head, flushes the rest in order");

client.stop();
client2.stop();
client4.stop();
view.sock.destroy();
view2.sock.destroy();
alice.sock.destroy();

// B1: a stable link holds ONE connection. Before the fix, start() resolved at
// hello and dialed again every 250 ms, filling the gate's connection cap in
// about 10 s and parking every later send in the outbox.
{
  const held = [];
  let stableDials = 0;
  net.connect = (...args) => {
    stableDials++;
    const s = origConnect(...args);
    held.push(s);
    return s;
  };
  const c = new MeshClient({
    socketPath: LINK_SOCK,
    sessionId: "srv/stable",
    statePath: path.join(STATE_DIR, "stable"),
    onMessage: () => true,
    log: () => {},
  });
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  await sleep(10_000);
  const open = held.filter((s) => !s.destroyed).length;
  c.stop();
  if (stableDials !== 1 || open !== 1) {
    fail(`B1: ${stableDials} dials, ${open} open sockets over 10 s (want 1/1)`);
  }
  pass("B1: one dial and one open socket over 10 s");
}

// K1: a frame split inside a multi-byte character decodes intact. A fake daemon
// writes the frame in two chunks cut inside a "ş" (real chunking is not
// deterministic: where it cuts depends on the generated sender name's length).
{
  const text = "ş".repeat(1000);
  const frame = Buffer.from(JSON.stringify({ event: "message", message: { id: "k1", from: "x", to: "srv/k1", text } }) + "\n");
  const cut = frame.indexOf(Buffer.from("ş")) + 1;
  const srv = net.createServer((s) => {
    let buf = "";
    s.on("error", () => {});
    s.on("data", (d) => {
      buf += d;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const req = JSON.parse(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        s.write(JSON.stringify({ id: req.id, result: req.op === "protocol" ? { protocol: 2 } : {} }) + "\n");
        if (req.op === "hello") {
          s.write(frame.subarray(0, cut));
          setTimeout(() => s.write(frame.subarray(cut)), 50);
        }
      }
    });
  });
  const k1Sock = path.join(STATE_DIR, "k1.sock");
  await new Promise((r) => srv.listen(k1Sock, r));
  const got = [];
  const c = new MeshClient({
    socketPath: k1Sock,
    sessionId: "srv/k1",
    statePath: path.join(STATE_DIR, "k1"),
    onMessage: (m) => {
      got.push(m);
      return true;
    },
    log: () => {},
  });
  c.start().catch(() => {});
  await waitFor(() => got.length > 0, 5_000);
  c.stop();
  srv.close();
  if (got[0].text !== text) fail(`K1: split character corrupted (FFFD ${got[0].text.includes("\uFFFD")})`);
  pass("K1: a frame split inside a character decodes intact");
}

// K2/K3: the size check runs before queueing, offline too.
{
  const c = new MeshClient({
    socketPath: LINK_SOCK,
    sessionId: "srv/k2",
    statePath: path.join(STATE_DIR, "k2"),
    onMessage: () => true,
    log: () => {},
  });
  try {
    await c.send({ to: "alice-1", text: "ş".repeat(600_000) });
    fail("offline oversized send queued");
  } catch (err) {
    if (!(err instanceof MeshError) || err.code !== "too_large") fail(`K2: ${err.message}`);
  }
  let saved;
  try {
    saved = JSON.parse(fs.readFileSync(path.join(STATE_DIR, "k2", "outbox.json"), "utf8"));
  } catch {
    saved = { items: [] };
  }
  if (saved.items.length !== 0) fail("K3: the oversized message was queued");
  pass("K2/K3: oversized sends refuse before queueing, online and offline");
}

// P2b: an always-declined message is retried a bounded number of times, then
// given up: a failure reply for asks (checked adapter-side) and an ack, so the
// daemon-side mailbox does not fill with mail that can never succeed.
{
  const seen = [];
  const logs = [];
  const c = new MeshClient({
    socketPath: LINK_SOCK,
    sessionId: "srv/p2b",
    statePath: path.join(STATE_DIR, "p2b"),
    retryDelayMs: 100,
    onMessage: (m) => (seen.push(m.id), false),
    onGiveUp: (m) => (logs.push(`gave-up reply for ${m.id}`), Promise.resolve()),
    log: (l) => logs.push(l),
  });
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  const bob = new Raw(DAEMON_SOCK);
  await bob.request({ op: "hello", session: { id: "p2b-asker" } });
  const id = (await bob.request({ op: "send", to: "srv/p2b", text: "p2b", expects_reply: true, no_wait: true })).result.id;
  await sleep(1_500);
  c.stop();
  const view = new Raw(LINK_SOCK);
  await view.request({ op: "hello", session: { id: "srv/p2b" } });
  const still = ((await view.request({ op: "inbox" })).result ?? []).some((m) => m.id === id);
  view.sock.destroy();
  bob.sock.destroy();
  if (!seen.includes(id) || seen.length < 5) fail(`P2b attempts: ${seen.length}`);
  if (!logs.some((l) => l.includes("gave up"))) fail(`P2b no give-up log: ${JSON.stringify(logs)}`);
  if (still) fail("P2b the given-up message stayed queued at the daemon");
  pass("P2b: bounded retries, give-up log, give-up reply hook, then acked");
}

// P8: a connection that dies mid-frame must not corrupt the next one (M6).
{
  const sockPath = path.join(STATE_DIR, "p8.sock");
  let conns = 0;
  const srv = net.createServer((s) => {
    const n = ++conns;
    let buf = "";
    s.setEncoding("utf8");
    s.on("error", () => {});
    s.on("data", (d) => {
      buf += d;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const req = JSON.parse(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        if (req.op === "protocol") s.write(JSON.stringify({ id: req.id, result: { protocol: 2 } }) + "\n");
        if (req.op === "hello") {
          s.write(JSON.stringify({ id: req.id, result: {} }) + "\n");
          if (n === 1) {
            s.write('{"event":"message","message":{"id":"m1","text":"cut off'); // no newline
            setTimeout(() => s.destroy(), 50);
          }
        }
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const ups = [];
  const c = new MeshClient({
    socketPath: sockPath,
    sessionId: "srv/p8",
    statePath: path.join(STATE_DIR, "p8"),
    onState: (up) => ups.push(up),
    log: () => {},
  });
  c.start().catch(() => {});
  await sleep(3_000);
  c.stop();
  srv.close();
  if (conns < 2) fail(`P8 reconnect count: ${conns}`);
  if (!c.connected && ups.filter(Boolean).length < 2) fail(`P8 stuck: ups=${ups.filter(Boolean).length}`);
  pass("P8: a mid-frame drop does not corrupt the next connection");
}

// P10: two overlapping flush loops must not duplicate or lose mail. A fake
// daemon answers each send after 100 ms (a slow ssh link); a send made 0.5 s
// after the reconnect enqueues and schedules a second flush 2 s later, while
// the first loop is still draining 30 items.
{
  const received = [];
  let conns = 0;
  const sockPath = path.join(STATE_DIR, "p10.sock");
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
        const answer = (result, ms = 0) => setTimeout(() => s.destroyed || s.write(JSON.stringify({ id: req.id, result }) + "\n"), ms);
        if (req.op === "protocol") answer({ protocol: 2 });
        else if (req.op === "hello") answer({});
        else if (req.op === "send") {
          received.push(req.text);
          answer({ id: "m" + received.length }, 100);
        } else answer({}, 100);
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const c = new MeshClient({
    socketPath: path.join(STATE_DIR, "nowhere.sock"),
    sessionId: "srv/p10",
    statePath: path.join(STATE_DIR, "p10"),
    log: () => {},
  });
  const want = [];
  for (let i = 0; i < 30; i++) {
    want.push(`q${i}`);
    await c.send({ to: "alice", text: `q${i}` }); // not started: queued
  }
  c.socketPath = sockPath;
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  await sleep(500);
  await c.send({ to: "alice", text: "late" }); // outbox not empty: queued, flush scheduled in 2 s
  want.push("late");
  const end = Date.now() + 15_000;
  while ((c.outbox.length || received.length < want.length) && Date.now() < end) await sleep(100);
  await sleep(500);
  c.stop();
  srv.close();
  const counts = {};
  for (const t of received) counts[t] = (counts[t] || 0) + 1;
  const dup = Object.entries(counts).filter(([, n]) => n > 1).map(([t]) => t);
  const lost = want.filter((t) => !counts[t]);
  if (dup.length || lost.length || received.length !== want.length) {
    fail(`P10: ${received.length} sends for ${want.length} items; dup ${JSON.stringify(dup)}; lost ${JSON.stringify(lost)}`);
  }
  if (conns !== 1) fail(`P10: ${conns} connections (the late send must not reconnect)`);
  pass("P10: overlapping flush loops send each item exactly once, no reconnect");
}

// P10b: a link drop while the loops overlap must keep the in-flight item and
// redeliver it after the reconnect. The fake daemon drops the connection 150 ms
// after choosing a new item to "die on the link".
{
  const received = [];
  let overlap = false;
  let dropped = null;
  const sockPath = path.join(STATE_DIR, "p10b.sock");
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
        const answer = (result, ms = 0) => setTimeout(() => s.destroyed || s.write(JSON.stringify({ id: req.id, result }) + "\n"), ms);
        if (req.op === "protocol") answer({ protocol: 2 });
        else if (req.op === "hello") answer({});
        else if (req.op === "send") {
          const isNew = !received.includes(req.text);
          if (!isNew) overlap = true;
          if (!dropped && isNew && (overlap || req.text === "q27")) {
            dropped = req.text; // lost on the link: never reaches the daemon
            setTimeout(() => s.destroy(), 150);
            continue;
          }
          received.push(req.text);
          answer({ id: "m" + received.length }, 100);
        } else answer({}, 100);
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const c = new MeshClient({
    socketPath: path.join(STATE_DIR, "nowhere2.sock"),
    sessionId: "srv/p10b",
    statePath: path.join(STATE_DIR, "p10b"),
    log: () => {},
  });
  const want = [];
  for (let i = 0; i < 30; i++) {
    want.push(`q${i}`);
    await c.send({ to: "alice", text: `q${i}` });
  }
  c.socketPath = sockPath;
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  await sleep(500);
  await c.send({ to: "alice", text: "late" });
  want.push("late");
  const end = Date.now() + 20_000;
  while ((c.outbox.length || want.some((t) => !received.includes(t))) && Date.now() < end) await sleep(100);
  await sleep(500);
  c.stop();
  srv.close();
  const counts = {};
  for (const t of received) counts[t] = (counts[t] || 0) + 1;
  const dup = Object.entries(counts).filter(([, n]) => n > 1).map(([t]) => t);
  const lost = want.filter((t) => !counts[t]);
  if (!dropped) fail("P10b: the drop never happened (timing changed)");
  if (dup.length || lost.length) fail(`P10b: dup ${JSON.stringify(dup)}; NEVER delivered ${JSON.stringify(lost)}`);
  pass(`P10b: a drop during the overlap keeps and redelivers ${dropped}`);
}

// P9: mailbox_full holds back only that recipient's mail, in its order; the
// rest of the queue keeps flowing. The fake daemon answers mailbox_full for
// "full-peer" until released.
{
  const received = [];
  let released = false;
  let conns = 0;
  const sockPath = path.join(STATE_DIR, "p9.sock");
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
        const answer = (frame) => s.destroyed || s.write(JSON.stringify({ id: req.id, ...frame }) + "\n");
        if (req.op === "protocol") answer({ result: { protocol: 2 } });
        else if (req.op === "send" && req.to === "full-peer" && !released) {
          answer({ error: { code: "mailbox_full", message: 'mailbox of "full-peer" is full (256)' } });
        } else if (req.op === "send") {
          received.push(req.text);
          answer({ result: { id: "m" + received.length } });
        } else answer({ result: {} });
      }
    });
  });
  await new Promise((r) => srv.listen(sockPath, r));
  const c = new MeshClient({
    socketPath: path.join(STATE_DIR, "nowhere-p9.sock"),
    sessionId: "srv/p9",
    statePath: path.join(STATE_DIR, "p9"),
    log: () => {},
  });
  for (const [to, text] of [["full-peer", "f1"], ["alice", "a1"], ["full-peer", "f2"], ["alice", "a2"]]) {
    await c.send({ to, text }); // not started: queued
  }
  c.socketPath = sockPath;
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  await waitFor(() => received.length >= 2, 1_000).catch(() => {});
  if (JSON.stringify(received) !== '["a1","a2"]') {
    fail(`P9: while full-peer is full, the rest of the queue sent ${JSON.stringify(received)}, want ["a1","a2"]`);
  }
  released = true;
  await waitFor(() => received.length >= 4, 5_000).catch(() => {});
  c.stop();
  srv.close();
  if (JSON.stringify(received) !== '["a1","a2","f1","f2"]') fail(`P9: after the release: ${JSON.stringify(received)}`);
  if (c.outbox.length !== 0) fail(`P9: outbox left ${c.outbox.length}`);
  if (conns !== 1) fail(`P9: ${conns} connections`);
  pass("P9: a full mailbox holds back only its own recipient's mail, in order");
}

// A fake daemon for the outbox blocks below: answers protocol and hello, records
// accepted sends, and lets each block choose a send's error.
async function fakeDaemon(name, sendError) {
  const d = { received: [], conns: 0, sockPath: path.join(STATE_DIR, `${name}.sock`) };
  d.srv = net.createServer((s) => {
    d.conns++;
    let buf = "";
    s.setEncoding("utf8");
    s.on("error", () => {});
    s.on("data", (chunk) => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const req = JSON.parse(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        let frame = { result: {} };
        if (req.op === "protocol") frame = { result: { protocol: 2 } };
        else if (req.op === "send") {
          const error = sendError(req);
          if (error) frame = { error };
          else {
            d.received.push(req.text);
            frame = { result: { id: "m" + d.received.length } };
          }
        }
        s.write(JSON.stringify({ id: req.id, ...frame }) + "\n");
      }
    });
  });
  await new Promise((r) => d.srv.listen(d.sockPath, r));
  return d;
}

// B4a: an item queued while connected into an empty outbox (send() does this
// once its in-place retries run out) schedules its own flush. P10 cannot pin
// this: its late item joins a flush loop that is already running.
{
  const d = await fakeDaemon("b4a", () => null);
  const c = new MeshClient({ socketPath: d.sockPath, sessionId: "srv/b4a", statePath: path.join(STATE_DIR, "b4a"), log: () => {} });
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  c.enqueue({ msg: { to: "alice", text: "b4a" }, check: 0 });
  await waitFor(() => d.received.includes("b4a"), 5_000).catch(() => {});
  c.stop();
  d.srv.close();
  if (!d.received.includes("b4a")) fail("B4a: an item queued while connected waited for a reconnect");
  if (d.conns !== 1) fail(`B4a: ${d.conns} connections`);
  pass("B4a: an item queued while connected flushes on the same connection");
}

// K4/K5: rate_limited on the outbox head reschedules the flush on the same
// connection (K4), and a send made while the outbox is backed up queues behind
// it instead of jumping ahead (K5). The fake daemon rate-limits q0's first try.
for (const late of [false, true]) {
  let limited = false;
  const d = await fakeDaemon(`k4-${late}`, (req) => {
    if (req.text !== "q0" || limited) return null;
    limited = true;
    return { code: "rate_limited", message: "more than 20 messages/min" };
  });
  const c = new MeshClient({
    socketPath: path.join(STATE_DIR, "nowhere-k4.sock"),
    sessionId: `srv/k4-${late}`,
    statePath: path.join(STATE_DIR, `k4-${late}`),
    log: () => {},
  });
  for (const t of ["q0", "q1", "q2"]) await c.send({ to: "alice", text: t }); // not started: queued
  c.socketPath = d.sockPath;
  c.start().catch(() => {});
  await waitFor(() => c.connected);
  const want = ["q0", "q1", "q2"];
  if (late) {
    await sleep(500);
    await c.send({ to: "alice", text: "late" }); // the head is rate-limited: queue behind it
    want.push("late");
  }
  await waitFor(() => d.received.length >= want.length, 6_000).catch(() => {});
  c.stop();
  d.srv.close();
  if (JSON.stringify(d.received) !== JSON.stringify(want)) fail(`K4/K5: sent ${JSON.stringify(d.received)}, want ${JSON.stringify(want)}`);
  if (d.conns !== 1) fail(`K4/K5: ${d.conns} connections`);
}
pass("K4/K5: a rate-limited head is retried on the same connection, and later sends queue behind it");

console.log("ALL MESH CHECKS PASSED");
process.exit(0); // the junk server and timers may still hold the loop
