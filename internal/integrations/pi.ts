// @ts-nocheck — one file for pi and omp; node builtins plus the host's own TUI package
// (loaded lazily, optional), no harness type imports.

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { stripVTControlCharacters } from "node:util";

const HARNESS = "__MESH_HARNESS__";
const MESH = process.env.AGM_BIN || "__MESH_BIN__";
// Canonical agent-facing wording (internal/integrations/text.go), injected by `agm install`.
const TEXT = /*__MESH_TEXT__*/ null;
/*__MESH_CONTEXT__*/
// fill replaces the {cli}/{session}/{msg} placeholders in one pass, known keys only;
// never run it over peer text (a peer could send "{cli}").
const fill = (t: string, kv: Record<string, string>) => t.replace(/\{(cli|session|msg)\}/g, (_, k) => kv[k] ?? "");
const PROTOCOL = 2; // broker.Protocol this adapter needs for /mesh Ask (no_wait)
const SOCKET = process.env.AGM_SOCKET || path.join(os.homedir(), ".agent-mesh", "mesh.sock");
const BATCH_MAX = 24_000; // UTF-8 bytes, including separators and deferred-mail notice
const MAX_FRAME = 1 << 20; // daemon protocol line limit (bytes)
const STATUS = "agent-mesh";
const TYPE = "agent_mesh";
const MESSAGE_PARAMETERS = {
	type: "object", additionalProperties: false, required: ["action", "summary"],
	properties: {
		action: { type: "string", enum: ["send", "ask", "reply"] },
		to: { type: "string", minLength: 1, maxLength: 256, description: "Recipient name or session ID for send/ask; omit for reply." },
		reply_to: { type: "string", minLength: 1, maxLength: 256, description: "Original message ID for reply; omit for send/ask." },
		summary: { type: "string", minLength: 1, maxLength: 500, description: "Short, readable message with ordinary spacing. Preserve technical names exactly." },
		details: { type: "string", maxLength: 8000, description: "Optional details or code, preserved verbatim." },
		files: { type: "array", maxItems: 16, items: { type: "string", minLength: 1, maxLength: 4096 }, description: "Paths mentioned verbatim in the message, not attachments. No files are read; use CLI -ref for file references." },
		constraints: { type: "array", maxItems: 8, items: { type: "string", minLength: 1, maxLength: 1000 }, description: "Boundaries the recipient must preserve, in readable sentences." },
		request_key: { type: "string", minLength: 1, maxLength: 128, description: "For an uncertain retry, reuse the returned key and identical message fields. New messages get an automatic key." },
	},
};

// Keep direct tool calls as bounded as host-validated calls; hosts may normalize arguments first.
function validateMessageParameters(params: any) {
	if (!params || typeof params !== "object" || Array.isArray(params)) throw new Error("message parameters must be an object");
	for (const key of MESSAGE_PARAMETERS.required) {
		if (!Object.hasOwn(params, key)) throw new Error(`${key} is required`);
	}
	const stringField = (value: any, spec: any, name: string) => {
		if (typeof value !== "string") throw new Error(`${name} must be a string`);
		if (spec.maxLength !== undefined && value.length > spec.maxLength * 2) throw new Error(`${name} exceeds ${spec.maxLength} characters`);
		const length = Array.from(value).length;
		if (length < (spec.minLength ?? 0)) throw new Error(`${name} must not be empty`);
		if (spec.maxLength !== undefined && length > spec.maxLength) throw new Error(`${name} exceeds ${spec.maxLength} characters`);
		if (spec.enum && !spec.enum.includes(value)) throw new Error(`${name} must be ${spec.enum.join(", ")}`);
	};
	for (const [key, value] of Object.entries(params)) {
		if (!Object.hasOwn(MESSAGE_PARAMETERS.properties, key)) throw new Error(`unknown message field: ${key}`);
		const spec = MESSAGE_PARAMETERS.properties[key];
		if (spec.type === "array") {
			if (!Array.isArray(value) || value.length > spec.maxItems) throw new Error(`${key} must be an array of at most ${spec.maxItems} strings`);
			for (const item of value) stringField(item, spec.items, key);
		} else stringField(value, spec, key);
	}
}

// Card components: the host's TUI package (Pi and OMP both resolve this name). If it
// cannot be loaded, the renderer returns undefined and the host's default rendering
// of the same message is used.
let tui: any;
import("@earendil-works/pi-tui").then((m) => (tui = m)).catch(() => {});

// clip cuts by code points (never splits a surrogate pair).
const cps = (s: string) => Array.from(String(s ?? ""));
const clip = (s: string, n: number) => {
	const a = cps(s);
	return a.length > n ? a.slice(0, n).join("") + "… (truncated)" : String(s ?? "");
};
// safe: peer text for the terminal. Escape sequences (e.g. OSC 52 clipboard writes) and
// other C0/C1 controls are removed; newlines and tabs stay. Model text is not changed.
const safe = (s: any) =>
	stripVTControlCharacters(String(s ?? "")).replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/g, "");
const shq = (s: string) => "'" + String(s).replaceAll("'", "'\\''") + "'";
const kindOf = (m: any) => (m.expects_reply ? "ASK" : m.reply_to ? "REPLY" : "MESSAGE");
const fromOf = (m: any) => (m.from_name ? `${m.from_name} (${m.from})` : m.from);
const isFyi = (m: any) => !m.expects_reply && !m.reply_to;

// Model-facing text of one message (unchanged wording plus file references).
function modelText(m: any, native = false) {
	const kind = m.expects_reply ? "question" : m.reply_to ? `reply to ${m.reply_to}` : "message";
	const blocks = [{ text: m.text }];
	for (const a of m.attachments ?? []) {
		blocks.push(a.type === "ref"
			? { text: `\n\nfile: ${shq(a.path)}`, whole: true }
			: { header: `\n\n--- ${a.type}: ${String(a.name ?? "")} ---\n`, text: a.content });
	}
	const refs = (m.attachments ?? []).some((a) => a.type === "ref") ? "\n" + TEXT.refNote : "";
	const hint = m.expects_reply ? `\n\n${fill(native ? TEXT.nativeReply : TEXT.reply, { cli: MESH, msg: m.id })}` : "";
	return meshContext(
		`**agent-mesh ${kind} from ${fromOf(m)}** [${m.id}]\n\n${TEXT.frame}\n\n`,
		blocks, refs + hint, `\n\n${fill(TEXT.fullText, { cli: MESH, msg: m.id })}`,
	);
}

// --- card rendering ---

const hhmm = (at: string) => {
	const d = new Date(at);
	return Number.isNaN(d.getTime()) ? "" : d.toTimeString().slice(0, 5);
};

function cardLines(m: any, expanded: boolean, th: any) {
	const fg = (c: string, t: string) => (th?.fg ? th.fg(c, t) : t);
	const bold = (t: string) => (th?.bold ? th.bold(t) : t);
	const kind = kindOf(m);
	const color = kind === "ASK" ? "warning" : kind === "REPLY" ? "success" : "accent";
	const who = safe(m.from_name || String(m.from ?? "").slice(0, 8));
	const out = [
		`${fg(color, bold(`◆ ${kind}`))} ${bold(who)} ${fg("dim", `· ${hhmm(m.at)} · #${safe(String(m.id).slice(0, 8))} · peer agent, not the user`)}`,
	];
	const text = safe(m.text).replace(/\s+$/, "");
	if (expanded) {
		out.push(text);
	} else {
		const lines = text.split("\n");
		let shown = lines.slice(0, 6).map((l) => clip(l, 200)).join("\n");
		shown = clip(shown, 600);
		out.push(shown);
		if (shown !== text) out.push(fg("dim", `… ${cps(text).length} chars, expand to read all`));
	}
	for (const a of m.attachments ?? []) {
		if (a.type === "ref") out.push(fg("muted", `file: ${safe(a.path)}`));
		else out.push(fg("muted", `${safe(a.type)}: ${safe(a.name)} (${cps(a.content).length} chars)`) + (expanded ? `\n${safe(a.content)}` : ""));
	}
	if (expanded) {
		const meta = [`id ${m.id}`, `from ${m.from}`, m.reply_to && `reply to ${m.reply_to}`, `hop ${m.hop ?? 0}`, m.at && `at ${m.at}`].filter(Boolean);
		out.push(fg("dim", safe(meta.join(" · "))));
		if (m.expects_reply) out.push(fg("dim", `answer: ${MESH} reply ${safe(m.id)} '<answer>'`));
	}
	return out;
}

function renderCard(message: any, opts: any, th: any) {
	if (!tui?.Box || !tui?.Text) return undefined;
	const d = message.details ?? {};
	const msgs = Array.isArray(d.batch) ? d.batch : d.id ? [d] : [];
	if (!msgs.length) return undefined;
	const expanded = !!opts?.expanded;
	const lines = [];
	if (Array.isArray(d.batch)) lines.push(th?.fg ? th.fg("dim", `agent-mesh · ${msgs.length} deferred message(s) (quiet mode)`) : `agent-mesh · ${msgs.length} deferred message(s)`);
	for (const m of msgs) lines.push(...cardLines(m, expanded, th));
	const bg = th?.bg ? (t: string) => th.bg("customMessageBg", t) : undefined;
	const box = new tui.Box(opts?.outputPad ?? 1, 1, bg);
	box.addChild(new tui.Text(lines.join("\n"), 0, 0));
	return box;
}

// One completed request, never an entire agent turn (which includes tool/user waits).
function requestMetrics(m: any, firstDeltaAt: number | undefined) {
	const duration = m.durationMs ?? m.duration;
	if (!Number.isFinite(duration) || duration <= 0 || !Number.isSafeInteger(m.usage?.output) || m.usage.output < 0) return null;
	let ttft = m.ttft;
	let source = "native";
	if (!Number.isFinite(ttft)) {
		ttft = firstDeltaAt !== undefined && Number.isFinite(m.timestamp) ? firstDeltaAt - m.timestamp : undefined;
		source = "observed";
	}
	if (!Number.isFinite(ttft) || ttft < 0 || ttft > duration) {
		ttft = null;
		source = "unknown";
	}
	return {
		provider: m.provider, model: m.responseModel || m.model, model_source: m.responseModel ? "response" : "request", output_tokens: m.usage.output,
		duration_ms: duration, ttft_ms: ttft, ttft_source: source, observed_at: new Date().toISOString(),
	};
}

export default function agentMesh(pi) {
	let sessionId: string | undefined;
	let ctx: any;
	let nativeRegistered = false;
	let sock: net.Socket | undefined;
	let retry: ReturnType<typeof setTimeout> | undefined;
	let backoff = 250;
	let reqId = 0;
	let started = false; // daemon spawn attempted
	let sentName: string | undefined;
	let myName: string | undefined;
	let nameSyncing = false;
	let namePoll: ReturnType<typeof setInterval> | undefined;
	let stopNameWatch: (() => void) | undefined;
	let state = "off"; // connecting | connected | reconnecting | unavailable | off
	let reason = "";
	let everConnected = false;
	let failures = 0;
	const replies = new Map<number, { resolve: Function; reject: Function; timer: ReturnType<typeof setTimeout> }>();
	// Quiet mode (opt-in): FYI messages wait, unacked, for the next natural turn.
	let quiet = process.env.AGM_QUIET === "1";
	const deferred = new Map<string, any>(); // id → message, not acked
	const inflight = new Map<string, number>(); // id → UTF-8 bytes, waiting for host persistence
	let deliveryBytes = 0; // charged across immediate and deferred deliveries in this run
	const compacting = new Set<string>(); // held by an optional compaction coordinator, not acked
	let deliveryGeneration = 0;
	const handed = new Set<string>(); // ids whose card reached message_end
	const recent: string[] = []; // acked ids: skip replays racing the ack
	const lateAcks = new Set<string>(); // persisted while disconnected: ack on reconnect
	let promptDepth = 0;
	const activeTools = new Map<string, string>(); // names only; no arguments or outputs
	let metrics: any = null;
	let responseStarted: number | undefined;
	let firstDeltaAt: number | undefined;
	let observationState = "off";
	let observationTimer: ReturnType<typeof setTimeout> | undefined;
	let heartbeat: ReturnType<typeof setInterval> | undefined;
	let observing = false;
	let observationDirty = false;
	let progressPending = false;
	let lastObservation = 0;

	// Mesh name: harness session name (/name), else $AGM_NAME, else the broker
	// generates a stable one from the session id (swift-otter).
	const sessionName = () => {
		try {
			return pi.getSessionName?.() || process.env.AGM_NAME || undefined;
		} catch {
			return undefined;
		}
	};

	const status = () => {
		if (!ctx?.hasUI) return;
		// Cozy footer: a state dot (shape + color, never color alone), the session name in
		// accent, a moon for quiet mode. Dynamic pieces are sanitized before styling —
		// safe() would strip the theme's own escapes.
		const th = ctx?.ui?.theme;
		const fg = (c: string, s: string) => (th?.fg ? th.fg(c, s) : s);
		let t = "";
		if (state === "connecting") t = fg("warning", "◐") + fg("dim", " mesh · connecting…");
		else if (state === "connected")
			t =
				fg("success", "●") +
				(myName
					? fg("dim", " mesh · ") + fg("accent", th?.bold ? th.bold(safe(myName)) : safe(myName))
					: fg("dim", " mesh · connected"));
		else if (state === "reconnecting") t = fg("warning", "◌") + fg("dim", " mesh · reconnecting…");
		else if (state === "unavailable") t = fg("error", "○") + fg("dim", ` mesh · unavailable${reason ? ` (${safe(reason)})` : ""}`);
		if (t && quiet) t += fg("dim", ` · ☾ quiet${deferred.size ? ` (${deferred.size} waiting)` : ""}`);
		else if (t && deferred.size) t += fg("dim", ` · ${deferred.size} waiting`);
		try {
			ctx.ui.setStatus?.(STATUS, t || undefined);
		} catch {}
	};
	const setState = (s: string, why = "") => {
		state = s;
		reason = why;
		status();
	};

	const write = (req: Record<string, unknown>) => {
		const id = ++reqId;
		sock?.write(JSON.stringify({ id, ...req }) + "\n");
		return id;
	};
	// request sends req on the session connection and resolves with its result.
	const sessionRequest = (req: Record<string, unknown>, signal?: AbortSignal): Promise<any> =>
		new Promise((resolve, reject) => {
			if (signal?.aborted) return reject(new Error("mesh request cancelled"));
			if (!sock || state !== "connected") return reject(new Error(`mesh is ${state === "off" ? "not connected" : state}`));
			// Same bound as the CLI: an oversized line would make the daemon drop the connection.
			const size = Buffer.byteLength(JSON.stringify({ id: reqId + 1, ...req })) + 1;
			if (size >= MAX_FRAME) return reject(new Error(`message too large (${size} bytes encoded, limit ${MAX_FRAME}): save it to a file and share it with \`${MESH} send -ref PATH <to>\``));
			const s = sock;
			const id = write(req);
			const onAbort = () => {
				const pending = replies.get(id);
				if (!pending) return;
				clearTimeout(pending.timer);
				replies.delete(id);
				pending.reject(new Error("mesh request cancelled; outcome unknown"));
			};
			const cleanup = () => signal?.removeEventListener("abort", onAbort);
			const timer = setTimeout(() => {
				replies.delete(id);
				cleanup();
				reject(new Error("mesh request timed out; outcome unknown — check history or retry with the same request_key"));
				s.destroy();
			}, 10_000);
			timer.unref?.();
			replies.set(id, {
				resolve: (value) => { cleanup(); resolve(value); },
				reject: (error) => { cleanup(); reject(error); }, timer,
			});
			signal?.addEventListener("abort", onAbort, { once: true });
			if (signal?.aborted) onAbort();
		});
	const ack = (ids: string[]) => {
		if (!ids.length) return;
		write({ op: "ack", ids });
		recent.push(...ids);
		recent.splice(0, Math.max(0, recent.length - 500));
	};
	const failReplies = (why: string) => {
		for (const r of replies.values()) {
			clearTimeout(r.timer);
			r.reject(new Error(why));
		}
		replies.clear();
	};

	// Event bursts coalesce to at most one observation/second; idle heartbeat is 10s.
	function queueObservation(progress = false) {
		if (!sessionId) return;
		observationDirty = true;
		progressPending ||= progress;
		if (observationState !== "active" || observing || observationTimer) return;
		observationTimer = setTimeout(() => {
			observationTimer = undefined;
			void observe();
		}, Math.max(0, 1000 - (Date.now() - lastObservation)));
		observationTimer.unref?.();
	}

	async function observe() {
		if (observationState !== "active" || state !== "connected" || !sessionId) return;
		const s = sock, id = sessionId;
		let activity = "unknown";
		try {
			// agent_end can precede retry/recovery; read the host at publication time.
			const idle = ctx?.isIdle?.();
			if (typeof idle === "boolean") activity = idle ? "idle" : "busy";
		} catch {} // an unavailable native signal is not evidence of idle
		let context = null;
		try {
			const usage = ctx?.getContextUsage?.();
			if (Number.isSafeInteger(usage?.tokens) && usage.tokens >= 0 && Number.isSafeInteger(usage?.contextWindow) && usage.contextWindow > 0)
				context = { tokens: usage.tokens, window: usage.contextWindow };
		} catch {} // unknown during compaction or on older hosts
		const progress = progressPending;
		progressPending = observationDirty = false;
		observing = true;
		lastObservation = Date.now();
		try {
			await sessionRequest({ op: "observe", progress, presence: {
				state: promptDepth ? "waiting" : activity,
				provider: ctx?.model?.provider, model: ctx?.model?.id,
				active_tools: [...new Set(activeTools.values())].slice(0, 8), context, metrics,
			} });
		} catch (err: any) {
			if (sock === s && sessionId === id && state === "connected") {
				stopObserving("unavailable"); // metadata failure must not break message delivery
				notify(ctx, `agent-mesh presence unavailable: ${err?.code || "connection error"}`, "warning");
			}
		} finally {
			if (sock === s && sessionId === id) {
				observing = false;
				if (observationDirty) queueObservation();
			}
		}
	}

	function stopObserving(next = "off") {
		clearTimeout(observationTimer);
		clearInterval(heartbeat);
		observationTimer = heartbeat = undefined;
		observationState = next;
		observing = false;
	}

	async function startObserving(s: net.Socket, id: string) {
		try {
			const capabilities = await sessionRequest({ op: "protocol" });
			if (sock !== s || sessionId !== id) return;
			if (!(capabilities?.coordination >= 1)) return stopObserving("unsupported");
			observationState = "active";
			heartbeat = setInterval(() => queueObservation(), 10_000);
			heartbeat.unref?.();
			queueObservation();
		} catch (err: any) {
			if (sock === s && sessionId === id)
				stopObserving(err?.code === "bad_request" || err?.code === "not_registered" ? "unsupported" : "unavailable");
		}
	}

	// Ignore host events for a session that has already been switched away from.
	function currentContext(c?: any) {
		if (!sessionId || (c && c.sessionManager?.getSessionId() !== sessionId)) return false;
		if (c) ctx = c;
		return true;
	}

	async function syncName() {
		if (state !== "connected" || nameSyncing || !sessionId) return;
		const name = sessionName();
		if (!name || name === sentName) return;
		const s = sock;
		const id = sessionId;
		nameSyncing = true;
		try {
			await sessionRequest({ op: "hello", session: { id, name } });
			if (sock !== s || sessionId !== id) return;
			sentName = name; // only cache accepted updates, so failed renames can retry
			const list = await sessionRequest({ op: "list" });
			if (sock !== s || sessionId !== id) return;
			myName = list?.find?.((x) => x.id === id)?.name;
			status();
		} catch {
			if (sock === s) s?.destroy(); // reconnect publishes the latest name
		} finally {
			if (sock === s && !s?.destroyed && sessionId === id) {
				nameSyncing = false;
				if (sessionName() !== sentName) void syncName();
			}
		}
	}

	function connect() {
		clearTimeout(retry);
		if (!sessionId) return;
		const id = sessionId;
		const s = net.createConnection(SOCKET);
		s.setEncoding("utf8"); // one decoder per stream: a rune split across chunks stays whole
		sock = s;
		let buf = "";
		let helloId = 0;
		const handshake = setTimeout(() => s.destroy(), 10_000);
		handshake.unref?.();
		s.on("connect", () => {
			if (sock !== s) return s.destroy(); // replaced (session switch) before it connected
			backoff = 250;
			started = false; // a later outage (e.g. daemon upgrade) may start it again
			sentName = sessionName();
			helloId = write({ op: "hello", subscribe: true, session: { id, name: sentName, harness: HARNESS, cwd: ctx?.cwd ?? process.cwd(), pid: process.pid, pane: process.env.HERDR_ENV === "1" ? process.env.HERDR_PANE_ID : undefined } });
		});
		s.on("data", (chunk) => {
			if (sock !== s) return;
			buf += chunk;
			let nl: number;
			while ((nl = buf.indexOf("\n")) >= 0) {
				const line = buf.slice(0, nl);
				buf = buf.slice(nl + 1);
				let frame;
				try {
					frame = JSON.parse(line);
				} catch {
					continue;
				}
				if (frame.event === "message" && frame.message) {
					deliver(frame.message);
				} else if (frame.id === helloId) {
					clearTimeout(handshake);
					if (frame.error) {
						setState("unavailable", frame.error.code || "hello failed");
						s.destroy();
					} else {
						everConnected = true;
						failures = 0;
						setState("connected");
						void startObserving(s, id);
						ack([...lateAcks]);
						lateAcks.clear();
						sessionRequest({ op: "list" })
							.then((list) => {
								if (sock !== s || sessionId !== id) return;
								myName = list?.find?.((x) => x.id === id)?.name;
								status();
								void syncName();
							})
							.catch(() => {});
					}
				} else if (replies.has(frame.id)) {
					const r = replies.get(frame.id);
					clearTimeout(r.timer);
					replies.delete(frame.id);
					frame.error ? r.reject(Object.assign(new Error(frame.error.message || frame.error.code), { code: frame.error.code })) : r.resolve(frame.result);
				}
			}
		});
		s.on("error", (err: any) => {
			if (!started && (err.code === "ENOENT" || err.code === "ECONNREFUSED")) {
				started = true;
				try {
					spawn(MESH, ["daemon"], { detached: true, stdio: "ignore" }).unref();
				} catch {}
			}
		});
		s.on("close", () => {
			clearTimeout(handshake);
			if (sock !== s || sessionId !== id) return; // replaced or shut down
			sock = undefined;
			nameSyncing = false;
			stopObserving();
			failReplies("mesh connection closed");
			failures++;
			if (everConnected) setState("reconnecting");
			else if (failures >= 3) setState("unavailable", "daemon not reachable, retrying");
			else setState("connecting");
			retry = setTimeout(connect, backoff);
			retry.unref?.();
			backoff = Math.min(backoff * 2, 10_000);
		});
	}

	function deliver(m: any, released = false) {
		if (!ctx || !sessionId) return; // not acked: stays queued, replayed on the next connect
		if (lateAcks.has(m.id) || deferred.has(m.id) || inflight.has(m.id) || compacting.has(m.id)) return;
		if (recent.includes(m.id)) return void write({ op: "ack", ids: [m.id] }); // ack was lost: re-ack, do not show again
		if (!released && typeof pi.events?.emit === "function") {
			const generation = deliveryGeneration;
			let resumed = false;
			const request = { version: 1, sessionId, deferred: false, deliver: () => {
				if (resumed || generation !== deliveryGeneration) return;
				resumed = true;
				compacting.delete(m.id);
				deliver(m, true); // bypass the coordinator, but retain quiet mode and persistence ACKs
			} };
			compacting.add(m.id);
			try {
				pi.events.emit("smart-compact:defer-delivery", request);
			} catch {
				resumed = true;
				compacting.delete(m.id);
				sock?.destroy(); // failed coordination: leave mail unacked for replay
				return;
			}
			if (!request.deferred) request.deliver(); // no listener: normal delivery, once
			return;
		}
		if (quiet && isFyi(m)) {
			deferred.set(m.id, m); // acked once its card is persisted (see drain)
			status();
			return;
		}
		const content = modelText(m, nativeMessagingActive());
		const bytes = meshBytes(content);
		if (deliveryBytes + bytes > BATCH_MAX) {
			deferred.set(m.id, m);
			status();
			return;
		}
		inflight.set(m.id, bytes);
		deliveryBytes += bytes;
		try {
			pi.sendMessage({ customType: TYPE, content, display: true, details: { ...m, session: sessionId } }, ctx.isIdle() ? { triggerTurn: true } : { deliverAs: "steer" });
		} catch {
			deliveryBytes -= bytes;
			inflight.delete(m.id);
			sock?.destroy(); // unacked mail is replayed after reconnect
		}
	}

	function flushDeferred() {
		const msgs = [...deferred.values()].filter((m) => !handed.has(m.id) && (!quiet || !isFyi(m)));
		for (const m of msgs) {
			deferred.delete(m.id);
			deliver(m);
		}
		status();
	}

	function stop() {
		deliveryGeneration++;
		compacting.clear();
		stopObserving();
		promptDepth = 0;
		activeTools.clear();
		metrics = null;
		responseStarted = firstDeltaAt = undefined;
		observationDirty = progressPending = false;
		lastObservation = 0;
		clearTimeout(retry);
		clearInterval(namePoll);
		stopNameWatch?.();
		stopNameWatch = undefined;
		nameSyncing = false;
		const s = sock;
		sessionId = undefined;
		sock = undefined;
		failReplies("mesh session ended");
		// Deferred mail was never acked: it stays queued for that session.
		deferred.clear();
		inflight.clear();
		deliveryBytes = 0;
		handed.clear();
		lateAcks.clear(); // unacked: replayed to that session later (at-least-once)
		everConnected = false;
		failures = 0;
		myName = undefined;
		setState("off");
		if (s && !s.destroyed) {
			s.write(JSON.stringify({ id: ++reqId, op: "bye" }) + "\n");
			s.end();
		}
	}

	function nativeMessagingActive() {
		try {
			return nativeRegistered && (!pi.getActiveTools || pi.getActiveTools().includes(TYPE));
		} catch {
			return false;
		}
	}

	if (typeof pi.registerTool === "function") {
		pi.registerTool({
			name: TYPE, label: "Agent mesh", loadMode: "essential", approval: "write",
			description: "Send, ask or reply to another agent in this mesh. Use readable spacing and preserve paths/code verbatim. Asks are asynchronous. The compact result means broker acceptance, not read or task completion. On an uncertain outcome retry with the SAME returned request_key and unchanged fields; deduplication covers this broker's last 4096 accepted keyed sends. Requires an idempotency-capable daemon; older daemons are refused before sending.",
			parameters: MESSAGE_PARAMETERS,
			async execute(_toolCallId, params, signal, _onUpdate, c) {
				if (!currentContext(c)) throw new Error("the mesh session changed; retry in the current session");
				validateMessageParameters(params);
				if (!params.summary.trim()) throw new Error("summary must not be blank");
				if (params.action === "reply") {
					if (!params.reply_to?.trim() || params.to !== undefined) throw new Error("reply requires reply_to and no to field");
				} else if (!params.to?.trim() || params.reply_to !== undefined) {
					throw new Error("send/ask requires to and no reply_to field");
				}
				const key = params.request_key ?? randomUUID();
				const generation = deliveryGeneration;
				const connection = sock;
				const parts = [params.summary];
				if (params.constraints?.length) parts.push("Constraints:\n" + params.constraints.map((s) => `- ${s}`).join("\n"));
				if (params.files?.length) parts.push("Files:\n" + params.files.map((s) => `- ${s}`).join("\n"));
				if (params.details) parts.push(params.details);
				let attempted = false;
				try {
					const capabilities = await sessionRequest({ op: "protocol" }, signal);
					if (!(capabilities?.idempotency >= 1)) throw new Error("daemon lacks idempotent sends; upgrade/restart it before using this tool");
					if (generation !== deliveryGeneration || connection !== sock) throw new Error("the mesh session or connection changed; retry in the current session");
					if (signal?.aborted) throw new Error("mesh request cancelled before sending");
					attempted = true;
					const m = await sessionRequest({
						op: "send", request_key: key, text: parts.join("\n\n"),
						to: params.action === "reply" ? undefined : params.to,
						reply_to: params.action === "reply" ? params.reply_to : undefined,
						expects_reply: params.action === "ask", no_wait: params.action === "ask",
					}, signal);
					const receipt = { id: m.id, to: m.to, status: "broker_accepted", request_key: key, at: m.at };
					return { content: [{ type: "text", text: JSON.stringify(receipt) }], details: receipt };
				} catch (error: any) {
					throw new Error(JSON.stringify({
						status: !attempted ? "not_sent" : error?.code ? "rejected" : "unknown",
						request_key: key, error: clip(safe(error?.message ?? error), 1000),
						retry: "Reuse this request_key and identical fields; broker acceptance is not a completion receipt.",
					}));
				}
			},
		});
		nativeRegistered = true;
	}

	pi.registerMessageRenderer?.(TYPE, (message, opts, theme) => {
		try {
			return renderCard(message, opts, theme);
		} catch {
			return undefined;
		}
	});

	pi.on("session_start", (_event, c) => {
		ctx = c;
		const id = c.sessionManager.getSessionId();
		if (id === sessionId && sock) return status();
		stop();
		sessionId = id;
		setState("connecting");
		connect();
		stopNameWatch = c.sessionManager.onSessionNameChanged?.(() => void syncName());
		if (!stopNameWatch) {
			// Older hosts expose no name event; this reads memory, sending only on change.
			namePoll = setInterval(() => void syncName(), 500);
			namePoll.unref?.();
		}
	});

	pi.on("session_info_changed", () => void syncName());
	pi.on("model_select", (_event, c) => { if (currentContext(c)) queueObservation(); });
	pi.on("agent_start", (_event, c) => {
		if (!currentContext(c)) return;
		queueObservation(true);
	});
	pi.on("ui_prompt_start", (_event, c) => {
		if (!currentContext(c)) return;
		promptDepth++;
		queueObservation(true);
	});
	pi.on("ui_prompt_end", (_event, c) => {
		if (!currentContext(c)) return;
		promptDepth = Math.max(0, promptDepth - 1);
		queueObservation(true);
	});
	pi.on("tool_execution_start", (event, c) => {
		if (!currentContext(c)) return;
		activeTools.set(event.toolCallId, String(event.toolName));
		queueObservation(true);
	});
	pi.on("tool_execution_update", (_event, c) => { if (currentContext(c)) queueObservation(true); });
	pi.on("tool_execution_end", (event, c) => {
		if (!currentContext(c)) return;
		activeTools.delete(event.toolCallId);
		queueObservation(true);
	});
	pi.on("message_start", (event, c) => {
		if (!currentContext(c) || event.message?.role !== "assistant") return;
		responseStarted = event.message.timestamp;
		firstDeltaAt = undefined;
		queueObservation(true);
	});
	pi.on("message_update", (event, c) => {
		if (!currentContext(c) || event.message?.role !== "assistant") return;
		if (["text_delta", "thinking_delta", "toolcall_delta"].includes(event.assistantMessageEvent?.type)) {
			firstDeltaAt ??= Date.now();
			queueObservation(true);
		}
	});

	pi.on("session_shutdown", () => {
		stop();
		try {
			ctx?.ui?.setStatus?.(STATUS, undefined);
		} catch {}
	});

	// All cards wait for message_end and a completed turn before ACK: sendMessage
	// may only queue a steer in memory. A session switch before persistence must replay it.
	pi.on("message_end", (event, c) => {
		if (!currentContext(c)) return;
		if (event.message?.role === "assistant") {
			metrics = requestMetrics(event.message, event.message.timestamp === responseStarted ? firstDeltaAt : undefined);
			responseStarted = firstDeltaAt = undefined;
			queueObservation(true);
		}
		const d = event?.message?.customType === TYPE ? event.message.details : undefined;
		if (!d || d.session !== sessionId) return;
		for (const m of Array.isArray(d.batch) ? d.batch : [d]) {
			if (deferred.has(m.id) || inflight.has(m.id)) handed.add(m.id);
		}
	});
	const ackHanded = () => {
		const ids = [...handed];
		handed.clear();
		for (const id of ids) {
			deferred.delete(id);
			inflight.delete(id);
		}
		if (sock && state === "connected") ack(ids);
		else for (const id of ids) lateAcks.add(id); // acked after the next hello; replays skipped
		status();
	};
	pi.on("turn_end", ackHanded);
	pi.on("agent_end", (_event, c) => {
		if (!currentContext(c)) return;
		ackHanded();
		activeTools.clear();
		queueObservation(true);
	});

	pi.on("agent_settled", (event, c) => {
		if (!currentContext(c)) return;
		queueObservation(true); // aborts also end activity, but must not restart queued work
		if (event.aborted || !ctx?.isIdle()) return;
		deliveryBytes = [...inflight.values()].reduce((sum, bytes) => sum + bytes, 0);
		flushDeferred();
	});

	pi.on("before_agent_start", (event) => {
		if (!sessionId) return;
		// Pending steers already belong to this run; do not grant them a second budget.
		deliveryBytes = [...inflight.values()].reduce((sum, bytes) => sum + bytes, 0);
		void syncName();
		const note = fill(nativeMessagingActive() ? TEXT.nativeInstruction : TEXT.instruction, { cli: MESH, session: sessionId });
		const sp = event.systemPrompt;
		const result: any = {};
		if (event.systemPromptOptions?.sections) event.systemPromptOptions.sections[TYPE] = note;
		else result.systemPrompt = Array.isArray(sp) ? [...sp, note] : `${sp}\n\n${note}`;
		// Quiet mode drain: deferred FYI mail joins this (natural) turn.
		// Reserve the largest header/notice first; unselected mail remains unacked.
		const batch = [];
		const texts = [];
		const available = [...deferred.keys()].filter((id) => !handed.has(id)).length;
		const heading = (n: number) => `**agent-mesh: ${n} deferred message(s)**\n\n`;
		const waiting = (n: number) => `\n\n(${n} more waiting; they follow on a later turn.)`;
		const separator = "\n\n---\n\n";
		let size = deliveryBytes + meshBytes(heading(available) + waiting(available));
		for (const m of deferred.values()) {
			if (handed.has(m.id)) continue;
			const t = modelText(m, nativeMessagingActive());
			const added = meshBytes(t) + (batch.length ? meshBytes(separator) : 0);
			if (size + added > BATCH_MAX) break;
			batch.push(m);
			texts.push(t);
			size += added;
		}
		const more = available - batch.length;
		if (batch.length) {
			result.message = {
				customType: TYPE,
				content:
					heading(batch.length) + texts.join(separator) + (more > 0 ? waiting(more) : ""),
				display: true,
				details: { batch, session: sessionId },
			};
			deliveryBytes += meshBytes(result.message.content);
		}
		return result;
	});

	// --- /mesh ---

	const notify = (c: any, text: string, type = "info") => {
		try {
			c?.ui?.notify ? c.ui.notify(safe(text), type) : console.log(safe(text));
		} catch {}
	};
	const statusLine = () => {
		const s = state === "connected" ? `connected${myName ? ` as ${myName}` : ""}` : state === "unavailable" ? `unavailable${reason ? ` (${reason})` : ""}` : state;
		return `agent-mesh: ${s} · session ${sessionId ?? "-"} · quiet ${quiet ? "on" : "off"}${deferred.size ? ` (${deferred.size} waiting)` : ""} · presence ${observationState}`;
	};
	const setQuiet = (c: any, on: boolean) => {
		quiet = on;
		if (!on) flushDeferred();
		status();
		notify(c, on ? "agent-mesh quiet on: peer FYI messages wait for your next turn; questions and replies still arrive at once." : "agent-mesh quiet off: messages arrive immediately.");
	};
	const view = async (c: any, title: string, text: string) => {
		// ponytail: an editor dialog is the only multi-line viewer both hosts offer; edits are discarded.
		if (c.ui.editor) await c.ui.editor(safe(title), safe(text));
		else notify(c, text);
	};
	const showFull = async (c: any, request: Function, id: string) => {
		const m = await request({ op: "show", ids: [id] });
		const refs = (m.attachments ?? []).filter((a) => a.type === "ref").map((a) => `file: ${a.path}`);
		const other = (m.attachments ?? []).filter((a) => a.type !== "ref").map((a) => `--- ${a.type}: ${a.name} ---\n${a.content}`);
		await view(c, `${kindOf(m)} #${m.id} from ${m.from_name || m.from} (view only)`, [m.text, ...other, ...refs].join("\n\n"));
		return m;
	};
	const compose = async (c: any, request: Function, to: any) => {
		const how = await c.ui.select(safe(`To ${to.name}@${to.harness}`), ["Send message", "Ask (reply arrives here as a card)"]);
		if (!how) return;
		const text = await c.ui.editor(safe(`${how.startsWith("Ask") ? "Ask" : "Message to"} ${to.name}`), "");
		if (!text?.trim()) return;
		// A /mesh ask never blocks (the reply arrives as a card): no_wait keeps it out of
		// the daemon's deadlock graph, so the peer can still ask back.
		const ask = how.startsWith("Ask");
		if (ask) {
			// An older daemon would ignore no_wait (a false deadlock edge): check first. It
			// answers the unknown op with bad_request (or not_registered before hello).
			const old = (e: any) => (e?.code === "bad_request" || e?.code === "not_registered" ? { protocol: 1 } : Promise.reject(e));
			const p = await request({ op: "protocol" }).catch(old);
			if (!(p?.protocol >= PROTOCOL)) return notify(c, `Not sent: the running agent-mesh daemon is older than this adapter. Restart it with the new binary: ${shq(MESH)} restart`);
		}
		const m = await request({ op: "send", to: to.id, text, expects_reply: ask, no_wait: ask || undefined });
		notify(c, `Queued for ${to.name} (#${m.id.slice(0, 8)}). Queued is not read${how.startsWith("Ask") ? "; the reply shows up here" : ""}.`);
	};

	pi.registerCommand("mesh", {
		description: "agent-mesh: peers, compose, inbox, history, identity, quiet on|off, status",
		handler: async (args: string, c: any) => {
			const [sub, arg] = String(args ?? "").trim().split(/\s+/);
			// Dialogs await the user: never act as a session that replaced this one meanwhile.
			const sid = sessionId;
			const request = (req: Record<string, unknown>) => (sessionId === sid && sid ? sessionRequest(req) : Promise.reject(new Error("the mesh session changed; reopen /mesh")));
			try {
				if (sub === "quiet") return setQuiet(c, arg === "on" ? true : arg === "off" ? false : !quiet);
				if (sub === "status" || !c?.hasUI) return notify(c, statusLine());
				const items = ["Peers / message…", "Inbox (queued, not consumed)", "History…", "Identity", `Quiet mode: ${quiet ? "on → off" : "off → on"}`, "Status"];
				const pick = await c.ui.select("agent-mesh", items);
				if (!pick) return;
				if (pick === items[5]) return notify(c, statusLine());
				if (pick === items[4]) return setQuiet(c, !quiet);
				if (pick === items[3]) {
					const cli = `${MESH} -as ${sessionId}`;
					const act = await c.ui.select(safe(`session ${sessionId} · ${myName ?? "?"}`), ["Paste CLI prefix into the editor", "Paste session id into the editor", "Close"]);
					if (act?.startsWith("Paste")) c.ui.pasteToEditor ? c.ui.pasteToEditor(act.includes("CLI") ? cli : sessionId) : notify(c, cli);
					return;
				}
				if (pick === items[0]) {
					const list = (await request({ op: "list" })).filter((s) => s.id !== sid);
					list.sort((a, b) => Number(b.live) - Number(a.live) || String(a.name).localeCompare(String(b.name)));
					if (!list.length) return notify(c, "No other sessions on the mesh.");
					const labels = list.map((s, i) => safe(`${i + 1}. ${s.name}@${s.harness || "?"} · ${s.live ? "live" : "offline"} · ${(s.cwd || "").replace(os.homedir(), "~")} · ${String(s.id).slice(0, 8)}`));
					const who = await c.ui.select("Peers", labels);
					if (who) await compose(c, request, list[labels.indexOf(who)]);
					return;
				}
				if (pick === items[1]) {
					const msgs = await request({ op: "inbox" });
					if (!msgs?.length) return notify(c, "Inbox empty: nothing queued for this session.");
					const labels = msgs.map((m, i) => safe(`${i + 1}. ${kindOf(m)} ${m.from_name || m.from}: ${clip(m.text.split("\n")[0], 60)}${deferred.has(m.id) ? " (quiet, waiting)" : ""}`));
					const sel = await c.ui.select(`Queued (${msgs.length})`, labels);
					if (sel) await showFull(c, request, msgs[labels.indexOf(sel)].id);
					return;
				}
				if (pick === items[2]) {
					const h = await request({ op: "history", limit: 30 });
					if (!h?.length) return notify(c, "No messages in history.");
					h.reverse(); // newest first
					const labels = h.map((s, i) => safe(`${i + 1}. ${hhmm(s.at)} ${s.dir === "in" ? "←" : "→"} ${s.dir === "in" ? s.from_name || s.from : s.to_name || s.to} ${s.expects_reply ? "ASK" : s.reply_to ? "REPLY" : "MSG"}: ${clip(s.preview, 60)}`));
					const sel = await c.ui.select("History (newest first)", labels);
					if (!sel) return;
					const s = h[labels.indexOf(sel)];
					const m = await showFull(c, request, s.id);
					if (s.dir === "in" && m.expects_reply) {
						const text = await c.ui.editor(safe(`Reply to ${m.from_name || m.from}`), "");
						if (text?.trim()) {
							await request({ op: "send", reply_to: m.id, text });
							notify(c, `Reply sent to ${m.from_name || m.from}.`);
						}
					}
				}
			} catch (err: any) {
				notify(c, `agent-mesh: ${err?.message ?? err}`, "error");
			}
		},
	});
}
