// agent-mesh channel plugin for OpenClaw. Wiring mirrors the bundled A2A channel
// (extensions/a2a: startAccount that stays pending until abortSignal, per-peer
// isolated DM routes via resolveChannelInboundRouteEnvelope with dmScope
// "per-account-channel-peer", inbound ingress + buildContext + dispatch, final
// replies through the turn plan's delivery). Text templates are injected at
// generation time (agm plugin openclaw).
import {
  createChatChannelPlugin,
  createChannelPluginBase,
  buildChannelOutboundSessionRoute,
  defineChannelPluginEntry,
} from "openclaw/plugin-sdk/channel-core";
import {
  createMessageReceiptFromOutboundResults,
  defineChannelMessageAdapter,
  waitUntilAbort,
} from "openclaw/plugin-sdk/channel-outbound";
import { resolveChannelInboundRouteEnvelope } from "openclaw/plugin-sdk/channel-inbound";
import { isReplyPayloadTerminalContent } from "openclaw/plugin-sdk/reply-payload";
import { PlatformMessageNotDispatchedError } from "openclaw/plugin-sdk/error-runtime";
import { MeshClient, MeshError } from "./mesh.js";

const TEXT = /*__MESH_TEXT__*/ null;

const CHANNEL = "agent-mesh";
const TARGET_PREFIX = `${CHANNEL}:`;
// One frame (1 MiB) minus envelope room; final replies are joined into one message.
const REPLY_MAX_BYTES = 512 * 1024;
// Final payloads that arrive before dispatch() resolves are joined briefly, so
// multi-part answers go out whole; when dispatch resolves, all finals are in.
const JOIN_DELAY_MS = 150;
// R1: how long the pre-send recipient check may take before it is treated as
// transient (see sendMeshText).
const CHECK_TIMEOUT_MS = 5_000;
const withTimeout = (p, ms) => {
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(`no answer within ${ms} ms`)), ms);
    t.unref?.();
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(t)); // don't leave the timer behind
};
// The reply to an ask whose own turn produced no final (R3).
const NO_DIRECT_ANSWER =
  "OpenClaw did not answer this message in its own turn (after a restart it may already have been taken); any answer arrives as a separate message.";

/** M0: ctx.log is a {info, warn, error} object, and a broken host logger must not
 * take the gateway down. Every log line goes through here. */
function safeLog(ctxLog) {
  const one = (level, msg) => {
    try {
      ctxLog?.[level]?.(String(msg));
    } catch {
      /* the host's logger is broken, not us */
    }
  };
  return {
    info: (m) => one("info", m),
    warn: (m) => one("warn", m),
    error: (m) => one("error", m),
  };
}

function resolveAccount(cfg, accountId) {
  const section = cfg?.channels?.[CHANNEL] ?? {};
  const configured = Boolean(section.socketPath && section.sessionId);
  return {
    accountId: accountId ?? "default",
    enabled: section.enabled !== false,
    configured,
    config: section,
  };
}

/** Laptop-local peers (ids without "/", the 3a gate forces remote ids under NAME/)
 * may write; any other id only when listed in allowFrom (exact, or a prefix ending
 * in "/"). Outbound sends follow the same rule. */
export function allowedPeer(id, allowFrom) {
  if (!id || typeof id !== "string") return false;
  if (!id.includes("/")) return true;
  return (allowFrom ?? []).some(
    (entry) => entry === id || (entry.endsWith("/") && id.startsWith(entry)),
  );
}

function normalizeTarget(raw) {
  const target = String(raw ?? "").replace(new RegExp(`^${TARGET_PREFIX}`, "i"), "").trim();
  return target || undefined;
}

function peerLabel(m) {
  return m.from_name || m.from;
}

/** The agent-visible body: the frame header, the sender, the text, and how to read
 * attachments. Refs point at the sender's machine; the channel says so. */
export function renderBody(m) {
  const lines = [`[agent-mesh] ${TEXT.channel}`, `From: ${peerLabel(m)} (${m.from})`, "", m.text || ""];
  for (const a of m.attachments ?? []) {
    if (a.type === "ref") {
      lines.push("", `file on the sender's machine: ${a.path} (not reachable from here)`);
    } else {
      lines.push("", `--- ${a.type}: ${a.name || "(unnamed)"} ---`, bound(a.content));
    }
  }
  if (m.reply_to) lines.push("", `(a reply to message ${m.reply_to})`);
  if (!m.expects_reply) {
    lines.push("", TEXT.noReply.replace("{peer}", m.from));
  }
  return lines.join("\n");
}

function bound(text, max = 8 * 1024) {
  const s = String(text ?? "");
  return s.length > max ? s.slice(0, max) + "\n[truncated]" : s;
}

/** Truncate to a UTF-8 byte budget without splitting a rune. */
function truncateUtf8(s, maxBytes) {
  const bytes = Buffer.byteLength(s, "utf8");
  if (bytes <= maxBytes) return s;
  const buf = Buffer.from(s, "utf8").subarray(0, maxBytes - 32);
  return buf.toString("utf8").replace(/\uFFFD$/, "") + `\n[truncated at ${maxBytes} bytes]`;
}

// The running MeshClient per account: outbound sends (heartbeats, announces, the
// message tool) need the live connection that startAccount owns. The account id
// keys it; "gateway" delivery (M8) routes CLI sends into the gateway process.
const runningAccounts = new Map();

// One dispatch per peer at a time (M9): a second message from the same peer runs
// as its own turn afterwards, keeping its own reply_to instead of being folded
// into the running turn as a follow-up.
const peerChains = new Map();

/** Dispatches one inbound message. Resolves true once the message is fully
 * handled (the reply sent or deliberately none, per D6), so the client can ack;
 * false means: retry later. */
async function dispatchInbound(params) {
  const { ctx, mesh, m, account, channelRuntime } = params;
  const log = safeLog(ctx.log);
  const allowFrom = account.config.allowFrom ?? [];
  if (!allowedPeer(m.from, allowFrom)) {
    log.warn(`agent-mesh: refused message from ${m.from}`);
    try {
      await sendReply(mesh, m.from, m.id, `This OpenClaw does not accept messages from ${m.from}.`);
    } catch (err) {
      log.error(`agent-mesh: refusal reply: ${err?.message || err}`);
      return false; // not accepted: the client retries, then redelivery
    }
    return true;
  }

  // One isolated session per peer, exactly like A2A: the envelope resolver scopes
  // the DM route to this account+channel+peer, whatever session.dmScope is.
  const { route, buildEnvelope } = resolveChannelInboundRouteEnvelope({
    cfg: ctx.cfg,
    channel: CHANNEL,
    accountId: account.accountId,
    peer: { kind: "direct", id: m.from },
    parentPeer: { kind: "direct", id: m.from },
    dmScope: "per-account-channel-peer",
  });

  // D6: an ask is a user_request (OpenClaw must answer, and the final answer is
  // the reply); anything else is quiet context: the turn runs, the answer is not
  // sent, and the mail lands in the session history.
  const kind = m.expects_reply ? "user_request" : "room_event";

  // Ingress admission (replay/dedupe/pairing) with the already-checked sender as
  // the sole allowed entry, exactly like A2A passes its peer list.
  const ingress = await channelRuntime.inbound.ingress.resolveStable({
    channelId: CHANNEL,
    accountId: account.accountId,
    cfg: ctx.cfg,
    identity: { key: "sender", entryIdPrefix: "agent-mesh-entry" },
    subject: { stableId: m.from },
    conversation: { kind: "direct", id: m.from },
    contextBinding: {
      agentId: route.agentId,
      sessionKey: route.sessionKey,
      messageId: m.id,
      inboundEventKind: kind,
    },
    dmPolicy: "allowlist",
    allowFrom: [m.from],
  });
  if (ingress?.ingress?.admission !== "dispatch") {
    log.warn(`agent-mesh: ingress declined a message from ${m.from} (${ingress?.ingress?.admission})`);
    if (m.expects_reply) {
      // The asker's wait must end even when core says no.
      try {
        await sendReply(mesh, m.from, m.id, "This OpenClaw cannot accept this message.");
      } catch (err) {
        log.error(`agent-mesh: ingress-decline reply: ${err?.message || err}`);
        return false;
      }
    }
    return true; // core said no: accept and ack, no turn
  }

  const timestamp = Date.parse(m.at || "") || Date.now();
  const body = buildEnvelope({ channel: CHANNEL, from: peerLabel(m), timestamp, body: renderBody(m) });
  const target = `${TARGET_PREFIX}${m.from}`;
  const ctxPayload = channelRuntime.inbound.buildContext({
    channel: CHANNEL,
    accountId: route.accountId ?? account.accountId,
    messageId: m.id,
    messageIdFull: m.id,
    timestamp,
    from: target,
    sender: { id: m.from, name: peerLabel(m) },
    conversation: { kind: "direct", id: m.from, label: peerLabel(m) },
    route: {
      agentId: route.agentId,
      dmScope: route.dmScope,
      accountId: route.accountId,
      routeSessionKey: route.sessionKey,
      dispatchSessionKey: route.sessionKey,
    },
    reply: { to: target, originatingTo: target },
    message: { body, bodyForAgent: body, rawBody: body, commandBody: body, inboundEventKind: kind },
    channelIngress: ingress,
  });

  // Final payloads of the turn, joined into one message and sent with reply_to.
  // Only asks answer: quiet context may still answer on purpose, through the
  // message tool. S6: every flush goes through one chain, so nothing rejects
  // unhandled and the ack waits for the flush in flight.
  const finals = [];
  let answered = false; // a terminal final reached deliver: an answer, or OpenClaw's error text
  let flushTimer = null;
  let flushing = Promise.resolve();
  const scheduleFlush = () => {
    if (!flushTimer) {
      flushTimer = setTimeout(() => {
        flushTimer = null;
        flushing = flushing.then(flush).catch((err) => log.error(`agent-mesh: reply flush: ${err?.message || err}`));
      }, JOIN_DELAY_MS);
    }
  };
  const flush = async () => {
    if (!finals.length) return;
    let text = finals.map((f) => f.text).join("\n\n");
    if (finals.some((f) => f.isError)) text = `OpenClaw could not answer: ${text}`; // N2
    finals.length = 0;
    try {
      await sendReply(mesh, m.from, m.id, truncateUtf8(text, REPLY_MAX_BYTES));
    } catch (err) {
      if (err instanceof MeshError && err.code === "too_large") {
        // S1: end the asker's wait instead of dropping the answer.
        await sendReply(mesh, m.from, m.id, `answer too large for the mesh (${err.message})`);
        return;
      }
      // B4: send() already persisted rate-limited replies and retried the rest;
      // what lands here is permanent. Log it and ack: re-running the turn would
      // be skipped as a duplicate, losing the answer either way.
      log.error(`agent-mesh: reply send failed (${err?.message || err}); acknowledging`);
    }
  };

  const dispatch = await channelRuntime.inbound.dispatch({
    cfg: ctx.cfg,
    channel: CHANNEL,
    accountId: account.accountId,
    route: { agentId: route.agentId, dmScope: route.dmScope, sessionKey: route.sessionKey },
    ctxPayload,
    delivery: {
      deliver: async (payload, info) => {
        if (info.kind !== "final" || !isReplyPayloadTerminalContent(payload)) return;
        answered = true;
        finals.push({ text: payload.text ?? "", isError: payload.isError === true });
        scheduleFlush();
      },
      onError: (error) => {
        log.error(`agent-mesh: turn error: ${error?.message || error}`);
      },
    },
    replyOptions: { sourceReplyDeliveryMode: "automatic" },
    replyPipeline: {},
  });
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
    flushing = flushing.then(flush).catch((err) => log.error(`agent-mesh: reply flush: ${err?.message || err}`));
  }
  await flushing; // M10/S6: the ack waits for the reply, including one in flight
  if (dispatch.admission.kind !== "dispatch") {
    log.warn(`agent-mesh: turn declined: ${dispatch.admission.kind}`);
    return false; // not accepted: retry, then redelivery (M1)
  }
  if (!dispatch.dispatched) {
    log.warn("agent-mesh: turn accepted without dispatching");
    return false;
  }
  if (m.expects_reply && !answered) {
    // R3: an ask's own turn always ends in a visible final (the answer, or
    // OpenClaw's error text), so none means the turn was skipped. After a crash,
    // the replayed ask is a duplicate, and OpenClaw's restart recovery answers
    // with plain messages that carry no reply_to. End the asker's wait instead of
    // letting it time out.
    log.warn(`agent-mesh: ask ${m.id} from ${m.from} got no final in its own turn`);
    try {
      await sendReply(mesh, m.from, m.id, NO_DIRECT_ANSWER);
    } catch (err) {
      log.error(`agent-mesh: no-answer note: ${err?.message || err}`);
      return false;
    }
  }
  return true; // reply sent (or deliberately none): ack now, not before (M10)
}

/** Sends one reply: exact recipient (already checked inbound), reply_to set. A
 * queued result counts as sent for ack purposes (M10: sent, or persisted). */
async function sendReply(mesh, to, replyTo, text) {
  const res = await mesh.send({ to, replyTo, text });
  if (!res.queued && !res.message?.id) throw new Error("send without a message id");
  return res;
}

function startAccount(ctx) {
  const account = ctx.account;
  const log = safeLog(ctx.log);
  if (!account.configured) {
    throw new Error(`agent-mesh channel is not configured for account "${account.accountId}"`);
  }
  const channelRuntime = ctx.channelRuntime;
  if (!channelRuntime?.inbound) {
    throw new Error("agent-mesh: this OpenClaw host provides no channel runtime");
  }
  ctx.setStatus({
    accountId: account.accountId,
    running: true,
    connected: false,
    lifecycle: "starting",
    configured: true,
    enabled: account.enabled,
  });
  const mesh = new MeshClient({
    socketPath: account.config.socketPath,
    sessionId: account.config.sessionId,
    name: account.config.sessionId,
    harness: "openclaw",
    statePath: account.config.statePath,
    outboxMax: account.config.outboxMax,
    outboxMaxBytes: account.config.outboxMaxBytes,
    retryDelayMs: account.config.retryDelayMs,
    onState: (up) =>
      ctx.setStatus(
        up
          ? {
              accountId: account.accountId,
              running: true,
              connected: true,
              lifecycle: "ready",
              lastConnectedAt: Date.now(),
              lastError: null,
            }
          : { accountId: account.accountId, connected: false, lifecycle: "starting" },
      ),
    onMessage: (m) => {
      // M9: one turn per peer at a time; a queued message runs after the current
      // one and keeps its own reply_to.
      const run = () => dispatchInbound({ ctx, mesh, m, account, channelRuntime });
      const prev = peerChains.get(m.from) ?? Promise.resolve();
      const next = prev.then(run, run);
      const stored = next.catch(() => {}); // the chain itself never rejects
      stored.finally(() => {
        if (peerChains.get(m.from) === stored) peerChains.delete(m.from);
      });
      peerChains.set(m.from, stored);
      return next;
    },
    checkSend: (msg) => checkOutbound(ctx, msg.to).then((info) => info.id),
    onGiveUp: async (m) => {
      if (m.expects_reply) {
        await sendReply(mesh, m.from, m.id, "OpenClaw could not process this message after repeated failures.");
      }
    },
    log: (msg) => log.info(msg),
  });
  runningAccounts.set(account.accountId, mesh);
  mesh.start().catch(() => {}); // M0: the loop itself never rejects
  return waitUntilAbort(ctx.abortSignal).finally(() => {
    runningAccounts.delete(account.accountId);
    mesh.stop();
    ctx.setStatus({
      accountId: account.accountId,
      running: false,
      connected: false,
      lifecycle: "stopped",
    });
  });
}

/** Outbound policy on the resolved session (M7): the daemon resolves names and
 * id prefixes, so a raw local-looking string can mean a remote session. Only an
 * exact id or exact name match counts, and the resolved id must be allowed.
 * Returns the session record (the flush sends to info.id). Failures that are not
 * daemon answers (a dropped connection) rethrow unchanged: they are transient
 * (B3). */
async function checkOutbound(ctx, target) {
  const account = resolveAccount(ctx.cfg, ctx.accountId);
  const mesh = runningAccounts.get(account.accountId);
  if (!mesh) throw new MeshError("bad_request", `agent-mesh: account ${account.accountId} is not running`);
  let info;
  try {
    info = await mesh.resolve(target);
  } catch (err) {
    if (err instanceof MeshError) {
      throw new MeshError("bad_request", `agent-mesh: cannot resolve ${target}: ${err.message}`);
    }
    throw err; // transient (B3): not connected, connection closed mid-resolve
  }
  if (info?.id !== target && info?.name !== target) {
    throw new MeshError("bad_request", `agent-mesh: ${target} is neither a session id nor its exact name`);
  }
  if (!allowedPeer(info.id, account.config.allowFrom ?? [])) {
    throw new MeshError("bad_request", `agent-mesh: ${info.id} is not a local peer and not in allowFrom; nothing was sent`);
  }
  return info;
}

/** Wrap: a permanent failure means no recipient-visible send began (S4), so the
 * durable queue must not replay it. While the link is down, the message is
 * queued with a flush-time check instead (B2): only the obviously-refused case
 * (a remote-looking target that is not listed) fails at once. */
async function sendMeshText(params) {
  const account = resolveAccount(params.cfg, params.accountId);
  const mesh = runningAccounts.get(account.accountId);
  const target = normalizeTarget(params.to);
  const notDispatched = (message, cause = null) =>
    new PlatformMessageNotDispatchedError(message, { cause, retryable: false });
  if (!target) throw notDispatched(`agent-mesh: no target in "${params.to}"`);
  if (!mesh) {
    // Transient in a restart window: the durable queue may retry.
    throw new PlatformMessageNotDispatchedError(
      `agent-mesh: account ${account.accountId} is not running`,
      { cause: null, retryable: true },
    );
  }
  if (!mesh.connected && target.includes("/") && !allowedPeer(target, account.config.allowFrom ?? [])) {
    throw notDispatched(`agent-mesh: ${target} is not a local peer and not in allowFrom; nothing was sent`);
  }
  let to = target; // B2: offline, queue as is; the flush resolves and re-checks
  if (mesh.connected) {
    try {
      // R1: a resolve is a tiny read-only request; if it sees no answer within
      // 5 s the link is probably silent (the heartbeat drop can take ~50 s).
      // Treat that as transient and queue: nothing was sent yet, the flush
      // re-checks, and the CLI gets "queued" inside its own budget instead of
      // an "outcome unknown" that invites a duplicate retry. (No timeout on
      // send itself: its frame may already be through.)
      to = (await withTimeout(checkOutbound(params, target), CHECK_TIMEOUT_MS)).id;
    } catch (err) {
      if (err instanceof MeshError) throw notDispatched(err.message, err);
      // The same immediate refusal as the offline path: an unlisted
      // remote-looking target is refused now, not answered "queued" and then
      // dropped by the flush-time check where only the gateway log shows it.
      if (target.includes("/") && !allowedPeer(target, account.config.allowFrom ?? [])) {
        throw notDispatched(`agent-mesh: ${target} is not a local peer and not in allowFrom; nothing was sent`);
      }
      // F1: a check cut by a link drop (or stalled past the bound) must queue
      // the message with the flush-time check, not throw it: OpenClaw never
      // replays a send whose attempt started, so throwing loses it. Sending
      // the raw target now would skip the check if the link came back between.
      try {
        mesh.queue({ to: target, text: params.text }, true);
      } catch (qerr) {
        // e.g. too_large: permanent, and nothing was sent (Q1's class of gap).
        if (qerr instanceof MeshError) throw notDispatched(qerr.message, qerr);
        throw qerr;
      }
      return {
        messageId: "queued",
        receipt: createMessageReceiptFromOutboundResults({
          results: [{ channel: CHANNEL, messageId: "queued" }],
          kind: "text",
        }),
      };
    }
  }
  try {
    const res = await mesh.send({ to, text: params.text }, true); // re-check at flush (M7)
    const messageId = res.message?.id ?? "queued";
    return {
      messageId,
      receipt: createMessageReceiptFromOutboundResults({
        results: [{ channel: CHANNEL, messageId }],
        kind: "text",
      }),
    };
  } catch (err) {
    // A dropped connection mid-send is ambiguous (it may have gone out): a plain
    // error, so the queue keeps it. Everything the daemon answered is permanent.
    if (err instanceof MeshError) throw notDispatched(err.message, err);
    throw err;
  }
}

const messageAdapter = defineChannelMessageAdapter({
  id: CHANNEL,
  durableFinal: { capabilities: { text: true } },
  send: {
    text: async (sendCtx) =>
      sendMeshText({
        cfg: sendCtx.cfg,
        accountId: sendCtx.accountId,
        to: sendCtx.to,
        text: sendCtx.text,
      }),
  },
});

export const agentMeshPlugin = createChatChannelPlugin({
  base: {
    ...createChannelPluginBase({
      id: CHANNEL,
      meta: {
        id: CHANNEL,
        label: "agent-mesh",
        selectionLabel: "agent-mesh (local agents)",
        docsPath: "https://github.com/alpertarhan/agent-mesh/blob/main/docs/integrations.md#openclaw-channel-plugin",
        blurb: "Talk to the agents on the machine that runs `agm link`.",
      },
      capabilities: { chatTypes: ["direct"] },
      reload: { configPrefixes: [`channels.${CHANNEL}`] },
      config: {
        listAccountIds: (cfg) => (cfg.channels?.[CHANNEL] ? ["default"] : []),
        resolveAccount,
        defaultAccountId: () => "default",
        isConfigured: (account) => account.configured,
        isEnabled: (account) => account.enabled,
      },
      setup: {
        applyAccountConfig: ({ cfg, input }) => ({
          ...cfg,
          channels: {
            ...cfg.channels,
            [CHANNEL]: { ...cfg.channels?.[CHANNEL], ...input, enabled: true },
          },
        }),
      },
    }),
    messaging: {
      normalizeTarget: (raw) => normalizeTarget(raw),
      inferTargetChatType: () => "direct",
      targetResolver: {
        looksLikeId: (raw) => normalizeTarget(raw) !== undefined,
        hint: "<mesh session id or name>",
      },
      resolveOutboundSessionRoute: ({ cfg, agentId, accountId, target }) => {
        const peer = normalizeTarget(target);
        if (!peer) return null;
        return buildChannelOutboundSessionRoute({
          cfg,
          agentId,
          channel: CHANNEL,
          accountId,
          recipientSessionExact: true,
          peer: { kind: "direct", id: peer },
          chatType: "direct",
          from: `${TARGET_PREFIX}${accountId ?? "default"}`,
          to: peer,
        });
      },
    },
    gateway: { startAccount },
    message: messageAdapter,
  },
  outbound: {
    // M8: the send needs the gateway's socket connection ("direct" would run it
    // in a CLI process that never started the account).
    base: { deliveryMode: "gateway" },
    attachedResults: { channel: CHANNEL, sendText: sendMeshText },
  },
});

export default defineChannelPluginEntry({
  id: CHANNEL,
  name: "agent-mesh",
  description: "Talk to agents on the machine that runs `agm link` over the mesh socket.",
  plugin: agentMeshPlugin,
});
