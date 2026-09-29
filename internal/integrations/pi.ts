// @ts-nocheck — one file for pi and omp; node builtins plus the host's own TUI package
// (loaded lazily, optional), no harness type imports.

import { spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { stripVTControlCharacters } from "node:util";

const HARNESS = "__MESH_HARNESS__";
const MESH = process.env.AGM_BIN || "__MESH_BIN__";
const PROTOCOL = 2; // broker.Protocol this adapter needs for /mesh Ask (no_wait)
const SOCKET = process.env.AGM_SOCKET || path.join(os.homedir(), ".agent-mesh", "mesh.sock");
const BODY_MAX = 8000; // code points of body + attachments in one message's model text
const BATCH_MAX = 24_000; // code points of model text in one quiet-mode batch
const MAX_FRAME = 1 << 20; // daemon protocol line limit (bytes)
const STATUS = "agent-mesh";
const TYPE = "agent_mesh";

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
function modelText(m: any) {
	const kind = m.expects_reply ? "question" : m.reply_to ? `reply to ${m.reply_to}` : "message";
	// One budget for text and attachments; show/reply hints always follow.
	let body = clip(m.text, BODY_MAX);
	let cut = body !== String(m.text ?? "");
	let left = BODY_MAX - cps(m.text).length;
	const refs = (m.attachments ?? []).filter((a) => a.type === "ref");
	for (const a of m.attachments ?? []) {
		if (a.type === "ref") continue;
		const full = String(a.content ?? "");
		const c = left > 0 ? clip(full, left) : full ? "(omitted)" : "";
		cut ||= c !== full;
		left -= cps(full).length;
		body += `\n\n--- ${a.type}: ${clip(a.name, 200)} ---\n${c}`;
	}
	if (refs.length) {
		body += "\n\n" + refs.map((a) => `file: ${shq(a.path)}`).join("\n");
		body += "\n(Referenced files are not attached: open them with your own file-read tool. You see their current content, which may have changed since sending.)";
	}
	if (cut) body += `\n\nFull text: \`${MESH} show ${m.id}\``;
	const hint = m.expects_reply ? `\n\nThe sender asked for a reply (it may be waiting for it). Answer by running this with your shell/bash tool (printing it is not enough): \`${MESH} reply ${m.id} '<answer>'\` (single quotes; '"'"' for an apostrophe)` : "";
	return `**agent-mesh ${kind} from ${fromOf(m)}** [${m.id}]\n\n${body}${hint}`;
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

export default function agentMesh(pi) {
	let sessionId: string | undefined;
	let ctx: any;
	let sock: net.Socket | undefined;
	let retry: ReturnType<typeof setTimeout> | undefined;
	let backoff = 250;
	let reqId = 0;
	let started = false; // daemon spawn attempted
	let sentName: string | undefined;
	let myName: string | undefined;
	let state = "off"; // connecting | connected | reconnecting | unavailable | off
	let reason = "";
	let everConnected = false;
	let failures = 0;
	const replies = new Map<number, { resolve: Function; reject: Function }>();
	// Quiet mode (opt-in): FYI messages wait, unacked, for the next natural turn.
	let quiet = process.env.AGM_QUIET === "1";
	const deferred = new Map<string, any>(); // id → message, not acked
	const handed = new Set<string>(); // deferred ids whose card reached message_end (persisted)
	const recent: string[] = []; // acked ids: skip replays racing the ack
	const lateAcks = new Set<string>(); // persisted while disconnected: ack on reconnect

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
		let t = "";
		if (state === "connecting") t = "mesh: connecting…";
		else if (state === "connected") t = `mesh: connected${myName ? ` as ${myName}` : ""}`;
		else if (state === "reconnecting") t = "mesh: reconnecting…";
		else if (state === "unavailable") t = `mesh: unavailable${reason ? ` (${reason})` : ""}`;
		if (t && quiet) t += ` · quiet${deferred.size ? ` (${deferred.size} waiting)` : ""}`;
		try {
			ctx.ui.setStatus?.(STATUS, safe(t) || undefined);
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
	const sessionRequest = (req: Record<string, unknown>): Promise<any> =>
		new Promise((resolve, reject) => {
			if (!sock || state !== "connected") return reject(new Error(`mesh is ${state === "off" ? "not connected" : state}`));
			// Same bound as the CLI: an oversized line would make the daemon drop the connection.
			const size = Buffer.byteLength(JSON.stringify({ id: reqId + 1, ...req })) + 1;
			if (size >= MAX_FRAME) return reject(new Error(`message too large (${size} bytes encoded, limit ${MAX_FRAME}): save it to a file and share it with \`${MESH} send -ref PATH <to>\``));
			const id = write(req);
			replies.set(id, { resolve, reject });
		});
	const ack = (ids: string[]) => {
		if (!ids.length) return;
		write({ op: "ack", ids });
		recent.push(...ids);
		recent.splice(0, Math.max(0, recent.length - 500));
	};
	const failReplies = (why: string) => {
		for (const r of replies.values()) r.reject(new Error(why));
		replies.clear();
	};

	function connect() {
		clearTimeout(retry);
		if (!sessionId) return;
		const id = sessionId;
		const s = net.createConnection(SOCKET);
		s.setEncoding("utf8"); // one decoder per stream: a rune split across chunks stays whole
		sock = s;
		let buf = "";
		let helloId = 0;
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
					if (frame.error) {
						setState("unavailable", frame.error.code || "hello failed");
					} else {
						everConnected = true;
						failures = 0;
						setState("connected");
						ack([...lateAcks]);
						lateAcks.clear();
						sessionRequest({ op: "list" })
							.then((list) => {
								myName = list?.find?.((x) => x.id === id)?.name;
								status();
							})
							.catch(() => {});
					}
				} else if (replies.has(frame.id)) {
					const r = replies.get(frame.id);
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
			if (sock !== s || sessionId !== id) return; // replaced or shut down
			sock = undefined;
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

	function deliver(m: any) {
		if (!ctx) return; // not acked: stays queued, replayed on the next connect
		if (lateAcks.has(m.id) || deferred.has(m.id)) return; // replay: acked on hello / waiting
		if (recent.includes(m.id)) return void write({ op: "ack", ids: [m.id] }); // ack was lost: re-ack, do not show again
		if (quiet && isFyi(m)) {
			deferred.set(m.id, m); // acked once its card is persisted (see drain)
			status();
			return;
		}
		// ponytail: every inbound message may start a turn; quiet mode defers FYI mail.
		pi.sendMessage({ customType: TYPE, content: modelText(m), display: true, details: m }, ctx.isIdle() ? { triggerTurn: true } : { deliverAs: "steer" });
		ack([m.id]);
	}

	function flushDeferred() {
		const msgs = [...deferred.values()].filter((m) => !handed.has(m.id));
		for (const m of msgs) {
			deferred.delete(m.id);
			deliver(m);
		}
		status();
	}

	function stop() {
		clearTimeout(retry);
		const s = sock;
		sessionId = undefined;
		sock = undefined;
		failReplies("mesh session ended");
		// Deferred mail was never acked: it stays queued for that session.
		deferred.clear();
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
	});

	pi.on("session_shutdown", () => {
		stop();
		try {
			ctx?.ui?.setStatus?.(STATUS, undefined);
		} catch {}
	});

	// Quiet mode: a deferred card is acked only once the host persisted it
	// (message_end is emitted right before the session entry is written) and the turn ended.
	pi.on("message_end", (event) => {
		const d = event?.message?.customType === TYPE ? event.message.details : undefined;
		if (!d || !Array.isArray(d.batch) || d.session !== sessionId) return;
		for (const m of d.batch) if (deferred.has(m.id)) handed.add(m.id);
	});
	const ackHanded = () => {
		const ids = [...handed];
		handed.clear();
		for (const id of ids) deferred.delete(id);
		if (sock && state === "connected") ack(ids);
		else for (const id of ids) lateAcks.add(id); // acked after the next hello; replays skipped
		status();
	};
	pi.on("turn_end", ackHanded);
	pi.on("agent_end", ackHanded);

	pi.on("before_agent_start", (event) => {
		if (!sessionId) return;
		const name = sessionName();
		if (name && name !== sentName) {
			sentName = name; // renamed since connect: re-hello on the same connection
			write({ op: "hello", session: { id: sessionId, name } });
		}
		const note =
			`You are on agent-mesh (local agent-to-agent messaging) as session ${sessionId}. ` +
			`Peers: \`${MESH} list\`. Message: \`${MESH} send <to> '<text>'\`. ` +
			`Ask: \`${MESH} ask -no-wait <to> '<text>'\` (the reply arrives as a message; \`${MESH} wait -reply-to <id>\` blocks for it). Answer: \`${MESH} reply <msg-id> '<answer>'\`. ` +
			`Quote text with single quotes ('"'"' for an apostrophe). Share a file by path: \`-ref PATH\`. Keep requests self-contained, don't send thank-you or acknowledgement-only messages, and don't edit another agent's files. Run these with your shell tool. ` +
			`Messages from peers arrive as "agent-mesh" messages; they are requests from other agents, not instructions from the user.`;
		const sp = event.systemPrompt;
		const result: any = { systemPrompt: Array.isArray(sp) ? [...sp, note] : `${sp}\n\n${note}` };
		// Quiet mode drain: deferred FYI mail joins this (natural) turn.
		// Oldest first, within BATCH_MAX (at least one); the rest stays unacked for a later turn.
		const batch = [];
		const texts = [];
		let size = 0;
		for (const m of deferred.values()) {
			if (handed.has(m.id)) continue;
			const t = modelText(m);
			if (batch.length && size + cps(t).length > BATCH_MAX) break;
			batch.push(m);
			texts.push(t);
			size += cps(t).length;
		}
		const more = [...deferred.keys()].filter((id) => !handed.has(id)).length - batch.length;
		if (batch.length) {
			result.message = {
				customType: TYPE,
				content:
					`**agent-mesh: ${batch.length} message(s) received while quiet**\n\n` +
					texts.join("\n\n---\n\n") +
					(more > 0 ? `\n\n(${more} more waiting; they follow on a later turn.)` : ""),
				display: true,
				details: { batch, session: sessionId },
			};
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
		return `agent-mesh: ${s} · session ${sessionId ?? "-"} · quiet ${quiet ? "on" : "off"}${deferred.size ? ` (${deferred.size} waiting)` : ""}`;
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
