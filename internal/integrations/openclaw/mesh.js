// Plain agent-mesh socket client for JS harness adapters. No OpenClaw imports:
// step 3c reuses this for remote CLI-based harnesses.
//
// It owns: connect, protocol check, hello with subscribe, reconnect with backoff,
// replay dedupe (accepted mail is re-acked, failed mail is retried), ack only after
// the adapter accepted the message, response matching by id (a gate writes
// refusals ahead of earlier answers), and a bounded persisted outbox for sends
// made while the link is down. Only a dropped connection is transient: error
// responses are permanent and drop the message, except rate_limited and
// mailbox_full, which are retried after a delay.
import net from "node:net";
import fs from "node:fs";
import path from "node:path";

const PROTOCOL = 2;
// The daemon measures a send in its own encoding and refuses above MaxMessage
// (MaxFrame - 4 KiB). Stay under it in bytes, counting the newline.
const MAX_SEND_BYTES = (1 << 20) - (4 << 10);
const RETRY_ERRORS = new Set(["rate_limited", "mailbox_full"]);
const DEDUPE_MAX = 1024;
const HANDSHAKE_TIMEOUT_MS = 10_000;
const RETRY_DELAY_MS = 5_000;
const MAX_ATTEMPTS = 5;

/** A daemon/gate error response, with its code. */
export class MeshError extends Error {
  constructor(code, message) {
    super(`${code}: ${message}`);
    this.code = code;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** One mesh connection plus its retry loop. */
export class MeshClient {
  /**
   * @param {object} opts
   * @param {string} opts.socketPath
   * @param {string} opts.sessionId   must sit under the link prefix (NAME/...)
   * @param {string} [opts.name]      display name (default: sessionId)
   * @param {string} [opts.harness]
   * @param {string} [opts.statePath] writable dir for the outbox (default ~/.openclaw/agent-mesh)
   * @param {number} [opts.outboxMax] max queued messages (default 200)
   * @param {number} [opts.outboxMaxBytes] max queued JSON bytes (default 1 MiB)
   * @param {(m: any) => boolean | Promise<boolean>} [opts.onMessage]
   *        resolves true once the message is accepted (the reply was sent, for a
   *        harness adapter); the client acks only then, and re-acks on replay.
   * @param {(up: boolean) => void} [opts.onState] connection state changes
   * @param {(...args: unknown[]) => void} [opts.log]
   */
  constructor(opts) {
    this.socketPath = opts.socketPath;
    this.sessionId = opts.sessionId;
    this.name = opts.name || opts.sessionId;
    this.harness = opts.harness || "";
    this.statePath = opts.statePath || path.join(process.env.HOME || ".", ".openclaw", "agent-mesh");
    this.outboxMax = opts.outboxMax ?? 200;
    this.outboxMaxBytes = opts.outboxMaxBytes ?? 1 << 20;
    this.onMessage = opts.onMessage || (() => true);
    this.onState = opts.onState || (() => {});
    this.checkSend = opts.checkSend || null; // outbound re-check for queued sends (M7)
    this.onGiveUp = opts.onGiveUp || null; // last-attempt hook: reply to asks, then ack
    // The client is a library: a throwing logger must not take the loop down.
    this.log = (msg) => {
      try {
        opts.log?.(msg);
      } catch {
        /* the host's logger is broken, not us */
      }
    };
    this.sock = null;
    this.stopped = false;
    this.connected = false; // true between hello and the next drop
    this.nextId = 1;
    this.pending = new Map(); // request id -> {resolve, reject}
    this.buffer = "";
    this.seen = new Map(); // message id -> "inflight" | "accepted" | {retry: m, attempts}
    this.timers = new Set();
    this.retryDelayMs = opts.retryDelayMs ?? RETRY_DELAY_MS;
    this.outbox = this.loadOutbox();
    this.flushScheduled = false;
    this.flushChain = Promise.resolve(); // P10: one flush loop at a time
  }

  /** Runs the connect/hello/reconnect loop until stop(). Never resolves. */
  async start() {
    let backoff = 250;
    while (!this.stopped) {
      let helloed = false;
      try {
        helloed = await this.oneConnection();
        if (this.stopped) return;
      } catch (err) {
        if (this.stopped) return;
        this.log(`mesh: ${err?.message || err}`);
      }
      // The backoff resets only after a completed hello: a working connection
      // counts, a refused one does not.
      if (helloed) backoff = 250;
      await sleep(Math.min(backoff, 10_000));
      backoff = Math.min(backoff * 2, 10_000);
    }
  }

  stop() {
    this.stopped = true;
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    this.failAllPending(new Error("mesh client stopped"));
    if (this.sock) this.sock.destroy();
    this.sock = null;
  }

  later(fn, ms) {
    const t = setTimeout(() => {
      this.timers.delete(t);
      Promise.resolve()
        .then(fn)
        .catch((err) => this.log(`mesh: ${err?.message || err}`));
    }, ms);
    this.timers.add(t);
    return t;
  }

  /** One connection attempt: dial, protocol, hello+subscribe, then read to EOF.
   * Resolves true once the connection ENDS, if hello had completed (B1: start()
   * must hold this connection open, not dial again 250 ms later). */
  oneConnection() {
    return new Promise((resolve, reject) => {
      const sock = net.connect(this.socketPath);
      this.sock = sock;
      this.buffer = ""; // a half frame from a dead connection must not leak here
      let helloed = false;
      let settled = false;
      const fail = (err) => {
        if (settled || sock !== this.sock) return;
        settled = true;
        clearTimeout(handshake);
        sock.destroy(); // M5: failed connections are closed, never leaked
        this.failAllPending(err);
        if (helloed) resolve(true); // a connection that said hello ends normally
        else reject(err);
      };
      sock.on("error", (err) => {
        if (sock !== this.sock) return; // a superseded connection (B1 defense)
        if (this.connected) this.onState(false);
        this.connected = false;
        fail(err);
      });
      sock.on("close", () => {
        if (sock !== this.sock) return; // a superseded connection (B1 defense)
        if (this.connected) this.onState(false);
        this.connected = false;
        this.failAllPending(new Error("connection closed"));
        if (!settled) {
          settled = true;
          resolve(helloed); // fall back to the retry loop, not an error
        }
      });
      // M6: a server that never answers the handshake must not hang the loop.
      const handshake = setTimeout(
        () => fail(new Error("handshake timeout")),
        HANDSHAKE_TIMEOUT_MS,
      );
      // M2: the socket decoder splits multi-byte runes across chunks correctly.
      sock.setEncoding("utf8");
      sock.on("data", (chunk) => {
        if (sock !== this.sock) return; // a superseded connection (B1 defense)
        this.buffer += chunk;
        let nl;
        while ((nl = this.buffer.indexOf("\n")) >= 0) {
          const line = this.buffer.slice(0, nl);
          this.buffer = this.buffer.slice(nl + 1);
          if (line.trim()) this.handleFrame(line);
        }
        if (Buffer.byteLength(this.buffer, "utf8") > 1 << 20) {
          this.log("mesh: frame over limit; dropping connection");
          sock.destroy();
        }
      });
      this.request({ op: "protocol" })
        .then((res) => {
          if (res?.result?.protocol !== PROTOCOL) {
            throw new Error(`daemon protocol ${res?.result?.protocol}, want ${PROTOCOL}`);
          }
          return this.request({
            op: "hello",
            session: { id: this.sessionId, name: this.name, harness: this.harness },
            subscribe: true,
          });
        })
        .then(async () => {
          helloed = true;
          this.connected = true;
          this.onState(true);
          clearTimeout(handshake);
          // B1: do not resolve here; start() holds this connection until it ends.
          await this.flushOutbox();
        })
        .catch(fail);
    });
  }

  /** One frame: a response for a pending request, or a pushed message event. */
  handleFrame(line) {
    let frame;
    try {
      frame = JSON.parse(line);
    } catch {
      this.log("mesh: ignoring unparseable frame"); // M0: never throw from data
      return;
    }
    if (frame.event === "message" && frame.message) {
      this.deliver(frame.message).catch(() => {});
      return;
    }
    const waiter = this.pending.get(frame.id);
    if (!waiter) return;
    this.pending.delete(frame.id);
    if (frame.error) waiter.reject(new MeshError(frame.error.code, frame.error.message));
    else waiter.resolve(frame);
  }

  /**
   * Pushed-message lifecycle (M1). inflight: a replay is ignored. accepted: a
   * replay re-acks (idempotent). A message the adapter declines is retried after
   * a delay, with bounded attempts, without needing a reconnect.
   */
  async deliver(m) {
    const state = this.seen.get(m.id);
    if (state === "inflight") return;
    if (state === "accepted") {
      await this.ack(m.id); // the ack was lost, or the daemon replayed: say it again
      return;
    }
    const attempts = (state?.attempts ?? 0) + 1;
    this.remember(m.id, "inflight");
    let ok = false;
    try {
      ok = await this.onMessage(m);
    } catch (err) {
      this.log(`mesh: handling ${m.id}: ${err?.message || err}`);
      ok = false;
    }
    if (this.stopped) return;
    if (ok) {
      this.remember(m.id, "accepted");
      await this.ack(m.id);
      return;
    }
    if (attempts >= MAX_ATTEMPTS) {
      this.seen.delete(m.id); // stays queued at the daemon; replayed on reconnect
      this.log(`mesh: gave up on message ${m.id} after ${attempts} attempts`);
      if (this.onGiveUp) {
        try {
          await this.onGiveUp(m);
        } catch (err) {
          this.log(`mesh: give-up reply: ${err?.message || err}`);
        }
      }
      await this.ack(m.id); // the mailbox must not fill with mail that can never succeed
      return;
    }
    this.seen.set(m.id, { attempts });
    this.later(() => this.deliver(m), this.retryDelayMs);
  }

  remember(id, state) {
    this.seen.set(id, state);
    while (this.seen.size > DEDUPE_MAX) {
      // Evict the oldest entry that is not in flight.
      for (const key of this.seen.keys()) {
        if (this.seen.get(key) === "inflight") continue;
        this.seen.delete(key);
        break;
      }
    }
  }

  failAllPending(err) {
    for (const [, w] of this.pending) w.reject(err);
    this.pending.clear();
  }

  /** Sends one request and resolves with its response frame (matched by id). */
  request(req) {
    return new Promise((resolve, reject) => {
      if (!this.sock || this.sock.destroyed) {
        reject(new Error("not connected"));
        return;
      }
      req.id = this.nextId++;
      const data = JSON.stringify(req);
      // M3: the daemon measures bytes in its own encoding; count bytes, not chars.
      if (Buffer.byteLength(data, "utf8") + 1 > MAX_SEND_BYTES) {
        reject(new MeshError("too_large", `request is ${Buffer.byteLength(data)} bytes, limit ${MAX_SEND_BYTES}`));
        return;
      }
      this.pending.set(req.id, { resolve, reject });
      this.sock.write(data + "\n", (err) => {
        if (err) {
          this.pending.delete(req.id);
          reject(err);
        }
      });
    });
  }

  /**
   * Sends a message. While disconnected (or while the outbox is backed up, so
   * order holds) it queues in the outbox and resolves {queued:true}. Connected:
   * rate_limited/mailbox_full are retried in place, and when the retries run out
   * the message is queued and the flush rescheduled (B4: a rate-limited reply is
   * never lost); every other error is permanent and rejects. Messages with
   * replyTo skip the flush-time checks (their recipient is the exact sender id,
   * already checked inbound); everything else is re-checked at flush time by the
   * adapter's checkSend, which returns the resolved recipient id.
   */
  async send(msg, checkSend) {
    const err = this.checkSize(msg);
    if (err) throw err;
    if (!this.connected || this.outbox.length > 0) {
      this.enqueue({ msg, check: checkSend ? 1 : 0 });
      return { queued: true };
    }
    for (let attempt = 0; ; attempt++) {
      let res;
      try {
        res = await this.request({
          op: "send",
          to: msg.to || "",
          reply_to: msg.replyTo || "",
          text: msg.text,
        });
      } catch (err2) {
        if (err2 instanceof MeshError && RETRY_ERRORS.has(err2.code)) {
          if (attempt >= 4) {
            this.enqueue({ msg, check: checkSend ? 1 : 0 }); // B4: persist, retry on the next flush
            return { queued: true };
          }
          await sleep(2_000 * (attempt + 1));
          continue;
        }
        if (err2 instanceof MeshError) throw err2; // permanent: caller logs and moves on
        if (this.stopped) throw err2;
        this.enqueue({ msg, check: checkSend ? 1 : 0 }); // dropped mid-send: retry on reconnect
        return { queued: true };
      }
      return { message: res.result };
    }
  }

  checkSize(msg) {
    const data = JSON.stringify({ op: "send", to: msg.to || "", reply_to: msg.replyTo || "", text: msg.text });
    if (Buffer.byteLength(data, "utf8") + 1 > MAX_SEND_BYTES) {
      return new MeshError("too_large", `message is ${Buffer.byteLength(data)} bytes, limit ${MAX_SEND_BYTES}`);
    }
    return null;
  }

  /** Resolves a target (id prefix or name) to the full session record. */
  async resolve(target) {
    const res = await this.request({ op: "resolve", to: target });
    return res.result;
  }

  /** Acks queued messages; failure logs and leaves redelivery to the daemon. */
  async ack(...ids) {
    if (!ids.length) return;
    try {
      await this.request({ op: "ack", ids });
    } catch (err) {
      this.log(`mesh: ack ${ids}: ${err?.message || err}`);
    }
  }

  // --- outbox ---------------------------------------------------------------

  outboxFile() {
    return path.join(this.statePath, "outbox.json");
  }

  loadOutbox() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.outboxFile(), "utf8"));
      if (Array.isArray(raw?.items)) return raw.items.filter((it) => it?.msg?.text != null);
    } catch {
      /* first run, or unreadable: start empty */
    }
    return [];
  }

  enqueue(item) {
    this.outbox.push(item);
    this.trimOutbox(`mesh: outbox full, dropped the oldest: to=${item.msg.to || "(reply)"}`);
    this.saveOutbox();
    // While connected, only hello and a rate-limited flush start a flush: an item
    // added to an otherwise empty outbox must schedule its own (B4).
    if (this.connected) this.scheduleFlush(2_000);
  }

  trimOutbox(warn) {
    let bytes = this.outboxBytes();
    while (this.outbox.length > this.outboxMax || bytes > this.outboxMaxBytes) {
      const dropped = this.outbox.shift();
      if (!dropped) break;
      this.log(warn);
      bytes -= Buffer.byteLength(JSON.stringify(dropped));
    }
  }

  outboxBytes() {
    return Buffer.byteLength(JSON.stringify(this.outbox));
  }

  saveOutbox() {
    try {
      fs.mkdirSync(this.statePath, { recursive: true });
      const tmp = this.outboxFile() + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify({ items: this.outbox }));
      fs.renameSync(tmp, this.outboxFile());
    } catch (err) {
      this.log(`mesh: outbox persist: ${err?.message || err}`);
    }
  }

  /** Flushes the outbox in order. A flush asked for while one runs is chained
   * after it (P10: two loops send the same head, and one loop's shift drops the
   * other's unsent item). A permanent error drops the message (so one bad
   * message never blocks the queue); rate_limited schedules the next attempt
   * (M4) instead of waiting for another reconnect; mailbox_full holds back only
   * that recipient's mail, in order (P9); a dropped connection stops the flush
   * and keeps the rest for the next one. checkSend failures that are not daemon
   * answers are transient: they stop the flush too, keeping the message (B3). */
  flushOutbox() {
    const run = this.flushChain.then(() => this.flushLoop());
    this.flushChain = run.catch(() => {});
    return run;
  }

  async flushLoop() {
    const full = new Set(); // recipients answering mailbox_full in this pass
    let i = 0;
    while (i < this.outbox.length && this.connected && !this.stopped) {
      const item = this.outbox[i];
      const { msg } = item;
      if (full.has(msg.to)) {
        i++;
        continue;
      }
      let to = msg.to;
      try {
        if (item.check && this.checkSend) {
          to = await this.checkSend(msg); // resolves the target and checks policy (M7)
        }
        await this.request({
          op: "send",
          to: to || "",
          reply_to: msg.replyTo || "",
          text: msg.text,
        });
      } catch (err) {
        if (err instanceof MeshError && err.code === "mailbox_full") {
          this.log(`mesh: outbox: ${err.message}; mail to ${msg.to} waits`);
          full.add(msg.to);
          this.scheduleFlush(2_000);
          i++;
          continue;
        }
        if (err instanceof MeshError && err.code === "rate_limited") {
          this.log("mesh: outbox rate_limited, retrying shortly");
          this.scheduleFlush(2_000);
          return;
        }
        if (err instanceof MeshError) {
          this.log(`mesh: outbox dropped a message: ${err.message}`);
          this.unqueue(item);
          continue;
        }
        return; // connection dropped or a transient check: keep the message
      }
      this.unqueue(item);
    }
  }

  /** By identity, not position: trimOutbox may drop items while a send is in flight. */
  unqueue(item) {
    const k = this.outbox.indexOf(item);
    if (k >= 0) this.outbox.splice(k, 1);
    this.saveOutbox();
  }

  /** M4: keep draining without waiting for a reconnect. */
  scheduleFlush(ms) {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    this.later(() => {
      this.flushScheduled = false;
      return this.flushOutbox();
    }, ms);
  }
}
