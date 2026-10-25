// agent-mesh for opencode v2: TUI plugin (loaded from cli.json "plugins").
// The TUI registers the selected root session on the mesh and delivers inbound
// messages through the server: `opencode api session.synthetic` (durable; steers a
// busy session, starts an idle one). Only node builtins.

import { execFile, spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const MESH = process.env.AGM_BIN || "__MESH_BIN__";
// Canonical agent-facing wording (internal/integrations/text.go), injected by `agm install`.
const TEXT = /*__MESH_TEXT__*/ null;
/*__MESH_CONTEXT__*/
// fill replaces the {cli}/{session}/{msg} placeholders in one pass, known keys only;
// never run it over peer text (a peer could send "{cli}").
const fill = (t, kv) => t.replace(/\{(cli|session|msg)\}/g, (_, k) => kv[k] ?? "");
const SOCKET = process.env.AGM_SOCKET || path.join(os.homedir(), ".agent-mesh", "mesh.sock");
const OPENCODE = path.basename(process.execPath).startsWith("opencode") ? process.execPath : "opencode";
const POLL_MS = 500;
const BATCH_MAX = 24_000; // UTF-8 bytes attempted between observed run completions

// ponytail: always talks to the background service; TUIs started with --server or
// --standalone are not supported (pass --server through if that is ever needed).
function api(operation, params, body) {
	const args = ["api", operation];
	for (const [k, v] of Object.entries(params)) args.push("--param", `${k}=${v}`);
	args.push("-d", JSON.stringify(body));
	return new Promise((resolve) => execFile(OPENCODE, args, { timeout: 15_000 }, (err) => resolve(!err)));
}

// Bash tools run in the shared server process, so the agm CLI cannot infer which
// session it runs under: agents pass -as explicitly.
const cli = (sessionID) => `${MESH} -as ${sessionID}`;

const shq = (s) => "'" + String(s).replaceAll("'", "'\\''") + "'";

function format(m, sessionID) {
	const from = m.from_name ? `${m.from_name} (${m.from})` : m.from;
	const kind = m.expects_reply ? "question" : m.reply_to ? `reply to ${m.reply_to}` : "message";
	const blocks = [{ text: m.text }];
	for (const a of m.attachments ?? []) {
		blocks.push(a.type === "ref"
			? { text: `\n\nfile: ${shq(a.path)}`, whole: true }
			: { header: `\n\n--- ${a.type}: ${a.name} ---\n`, text: a.content });
	}
	const refs = (m.attachments ?? []).some((a) => a.type === "ref") ? `\n${TEXT.refNote}` : "";
	const hint = m.expects_reply ? `\n\n${fill(TEXT.reply, { cli: cli(sessionID), msg: m.id })}` : "";
	return meshContext(
		`[agent-mesh ${kind} from ${from}] [${m.id}]\n` +
		`(${TEXT.frame} Mesh CLI: \`${cli(sessionID)} list|send|ask|reply\`, run with your shell tool.)\n\n`,
		blocks, refs + hint, `\n${fill(TEXT.fullText, { cli: cli(sessionID), msg: m.id })}`,
	);
}

// One mesh connection for one opencode session.
function connectSession(sessionID, directory, toast, initialIdle) {
	let sock;
	let retry;
	let backoff = 250;
	let closed = false;
	let started = false;
	let reqId = 0;
	let chain = Promise.resolve(); // deliver in order
	let lastIdle = initialIdle;
	let deliveryBytes = 0;
	let reservedBytes = 0; // an API request crossing an idle observation counts in both windows
	let budgetWake;
	let paused = false;
	const write = (req) => sock?.write(JSON.stringify({ id: ++reqId, ...req }) + "\n");

	// Durable per-session instruction (the opencode analog of a system prompt note).
	api("experimental.session.instructions.entry.put", { sessionID, key: "agent-mesh" }, {
		value: fill(TEXT.instruction, { cli: cli(sessionID), session: sessionID }),
	});

	// Ordered delivery: a failed synthetic call is retried (bounded backoff) while this
	// session stays selected; ACK only after success. Replays of in-flight ids are skipped.
	const pending = new Set();
	let warned = false;
	let nap; // pending retry sleep: { timer, wake }
	const sleep = (ms) =>
		new Promise((wake) => {
			const timer = setTimeout(() => ((nap = undefined), wake()), ms);
			timer.unref?.();
			nap = { timer, wake };
		});
	const deliver = (m) => {
		if (!m?.id || pending.has(m.id)) return;
		pending.add(m.id);
		chain = chain.then(async () => {
			if (closed) { pending.delete(m.id); return; }
			const text = format(m, sessionID);
			const bytes = meshBytes(text);
			for (let wait = 1000; !closed; wait = Math.min(wait * 2, 30_000)) {
				while (!closed && (paused || deliveryBytes + bytes > BATCH_MAX)) {
					try { toast?.("agent-mesh: peer burst remains queued until this session finishes a run"); } catch {}
					await new Promise((wake) => { budgetWake = wake; });
				}
				if (closed) break;
				deliveryBytes += bytes; // failures can be ambiguous: retries consume budget too
				reservedBytes = bytes;
				const accepted = await api("session.synthetic", { sessionID }, { text, delivery: "steer" });
				reservedBytes = 0;
				if (accepted) {
					if (!closed) write({ op: "ack", ids: [m.id] });
					break;
				}
				if (!warned && typeof toast === "function") {
					warned = true;
					try {
						toast("agent-mesh: could not deliver a peer message yet; retrying");
					} catch {}
				}
				await sleep(wait);
			}
			pending.delete(m.id); // closed: stays queued in the broker for the next selection
		});
	};

	const connect = () => {
		if (closed) return;
		const s = net.createConnection(SOCKET);
		s.setEncoding("utf8"); // one decoder per stream: a rune split across chunks stays whole
		sock = s;
		let buf = "";
		s.on("connect", () => {
			backoff = 250;
			started = false; // a later outage (e.g. daemon upgrade) may start it again
			write({
				op: "hello",
				subscribe: true,
				session: { id: sessionID, name: process.env.AGM_NAME || undefined, harness: "opencode", cwd: directory, pid: process.pid, pane: process.env.HERDR_ENV === "1" ? process.env.HERDR_PANE_ID : undefined },
			});
		});
		s.on("data", (chunk) => {
			buf += chunk;
			let nl;
			while ((nl = buf.indexOf("\n")) >= 0) {
				const line = buf.slice(0, nl);
				buf = buf.slice(nl + 1);
				try {
					const frame = JSON.parse(line);
					if (frame.event === "message" && frame.message) deliver(frame.message);
				} catch {}
			}
		});
		s.on("error", (err) => {
			if (!started && (err.code === "ENOENT" || err.code === "ECONNREFUSED")) {
				started = true;
				try {
					spawn(MESH, ["daemon"], { detached: true, stdio: "ignore" }).unref();
				} catch {}
			}
		});
		s.on("close", () => {
			if (closed || sock !== s) return;
			retry = setTimeout(connect, backoff);
			retry.unref?.();
			backoff = Math.min(backoff * 2, 10_000);
		});
	};
	connect();

	// Switching away keeps the mesh session (and its queued mail); the next TUI that
	// selects it resumes delivery.
	return {
		observe(info) {
			const idle = info?.time?.idle;
			if (closed || info?.id !== sessionID || !Number.isFinite(idle) || (lastIdle !== undefined && idle <= lastIdle)) return;
			if (!["succeeded", "failed", "interrupted"].includes(info.outcome)) return;
			lastIdle = idle;
			paused = info.outcome === "interrupted";
			if (paused) return; // do not restart a user-aborted run
			deliveryBytes = reservedBytes;
			budgetWake?.();
			budgetWake = undefined;
		},
		close() {
			closed = true;
			clearTimeout(retry);
			budgetWake?.();
			budgetWake = undefined;
			if (nap) {
				clearTimeout(nap.timer);
				nap.wake(); // ends the retry loop (closed); mail stays queued
				nap = undefined;
			}
			sock?.end();
		},
	};
}

function setup(api) {
	let selected;
	let connection;

	const root = (id) => {
		const seen = new Set();
		while (typeof id === "string" && !seen.has(id)) {
			seen.add(id);
			const s = api.data.session.get(id);
			if (!s) return;
			if (!s.parentID) return id;
			id = s.parentID;
		}
	};

	const sync = () => {
		const route = api.ui.router.current();
		const id = route?.type === "session" ? root(route.sessionID) : undefined;
		const session = id ? api.data.session.get(id) : undefined;
		if (id === selected) return connection?.observe(session);
		connection?.close();
		connection = undefined;
		selected = id;
		if (id) connection = connectSession(id, session?.location?.directory || process.cwd(), (message) => api.ui?.toast?.({ variant: "warning", message }), session?.time?.idle);
	};

	const unsubscribe = api.data.listen(() => sync());
	sync();
	const poll = setInterval(sync, POLL_MS);
	return () => {
		clearInterval(poll);
		unsubscribe();
		connection?.close();
	};
}

export default { id: "agent-mesh", setup };
