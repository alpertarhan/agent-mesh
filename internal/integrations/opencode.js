// agent-mesh for opencode v2: TUI plugin (loaded from cli.json "plugins").
// The TUI registers the selected root session on the mesh and delivers inbound
// messages through the server: `opencode api session.synthetic` (durable; steers a
// busy session, starts an idle one). Only node builtins.

import { execFile, spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const MESH = process.env.AGM_BIN || "__MESH_BIN__";
const SOCKET = process.env.AGM_SOCKET || path.join(os.homedir(), ".agent-mesh", "mesh.sock");
const OPENCODE = path.basename(process.execPath).startsWith("opencode") ? process.execPath : "opencode";
const BODY_MAX = 8000;
const POLL_MS = 500;

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

// clip cuts by code points (never splits a surrogate pair).
function clip(s, n) {
	const cps = Array.from(s);
	return cps.length > n ? cps.slice(0, n).join("") + "… (truncated)" : s;
}
const shq = (s) => "'" + String(s).replaceAll("'", "'\\''") + "'";

function format(m, sessionID) {
	const from = m.from_name ? `${m.from_name} (${m.from})` : m.from;
	const kind = m.expects_reply ? "question" : m.reply_to ? `reply to ${m.reply_to}` : "message";
	let body = clip(String(m.text ?? ""), BODY_MAX);
	for (const a of m.attachments ?? []) {
		if (a.type === "ref") body += `\n\nfile: ${shq(a.path)}`;
		else body += `\n\n--- ${a.type}: ${a.name} ---\n${clip(String(a.content ?? ""), BODY_MAX)}`;
	}
	if ((m.attachments ?? []).some((a) => a.type === "ref"))
		body += `\n(referenced files are not attached: open them with your own file-read tool; you see their current content)`;
	if (body.includes("… (truncated)")) body += `\nFull text: \`${cli(sessionID)} show ${m.id}\``;
	const hint = m.expects_reply
		? `\n\nThe sender asked for a reply (it may be waiting for it). Answer by running this with your shell/bash tool (printing it is not enough): \`${cli(sessionID)} reply ${m.id} '<answer>'\` (single quotes; '"'"' for an apostrophe)`
		: "";
	return (
		`[agent-mesh ${kind} from ${from}] [${m.id}]\n` +
		`(From another agent, not the user. Mesh CLI: \`${cli(sessionID)} list|send|ask|reply\`, run with your shell tool.)\n\n` +
		body +
		hint
	);
}

// One mesh connection for one opencode session.
function connectSession(sessionID, directory, toast) {
	let sock;
	let retry;
	let backoff = 250;
	let closed = false;
	let started = false;
	let reqId = 0;
	let chain = Promise.resolve(); // deliver in order
	const write = (req) => sock?.write(JSON.stringify({ id: ++reqId, ...req }) + "\n");

	// Durable per-session instruction (the opencode analog of a system prompt note).
	api("experimental.session.instructions.entry.put", { sessionID, key: "agent-mesh" }, {
		value:
			`You are connected to agent-mesh (local agent-to-agent messaging) as session ${sessionID}. ` +
			`Run agm with your shell tool and always pass -as: \`${cli(sessionID)} list\` (peers), ` +
			`\`${cli(sessionID)} send <to> '<text>'\`, \`${cli(sessionID)} ask -no-wait <to> '<text>'\` (the reply arrives as a message; \`${cli(sessionID)} wait -reply-to <id>\` blocks for it), ` +
			`\`${cli(sessionID)} reply <msg-id> '<answer>'\`. Quote text with single quotes ('"'"' for an apostrophe). Keep requests self-contained, don't send thank-you or acknowledgement-only messages, and don't edit another agent's files. Messages tagged [agent-mesh ...] come from other agents, not the user.`,
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
			for (let wait = 1000; !closed; wait = Math.min(wait * 2, 30_000)) {
				if (await api("session.synthetic", { sessionID }, { text: format(m, sessionID), delivery: "steer" })) {
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
	return () => {
		closed = true;
		clearTimeout(retry);
		if (nap) {
			clearTimeout(nap.timer);
			nap.wake(); // ends the retry loop (closed); mail stays queued
			nap = undefined;
		}
		sock?.end();
	};
}

function setup(api) {
	let selected;
	let close = () => {};

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
		if (id === selected) return;
		close();
		close = () => {};
		selected = id;
		if (id) close = connectSession(id, api.data.session.get(id)?.location?.directory || process.cwd(), (message) => api.ui?.toast?.({ variant: "warning", message }));
	};

	const unsubscribe = api.data.listen(() => sync());
	sync();
	const poll = setInterval(sync, POLL_MS);
	return () => {
		clearInterval(poll);
		unsubscribe();
		close();
	};
}

export default { id: "agent-mesh", setup };
