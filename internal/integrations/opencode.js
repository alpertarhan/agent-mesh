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

function format(m, sessionID) {
	const from = m.from_name ? `${m.from_name} (${m.from})` : m.from;
	const kind = m.expects_reply ? "question" : m.reply_to ? `reply to ${m.reply_to}` : "message";
	let body = String(m.text ?? "");
	if (body.length > BODY_MAX) body = body.slice(0, BODY_MAX) + "… (truncated)";
	for (const a of m.attachments ?? []) body += `\n\n--- ${a.type}: ${a.name} ---\n${a.content}`;
	const hint = m.expects_reply
		? `\n\nThe sender is blocked waiting. Answer by running this with your shell/bash tool (printing it is not enough): \`${cli(sessionID)} reply ${m.id} "<answer>"\``
		: "";
	return (
		`[agent-mesh ${kind} from ${from}] [${m.id}]\n` +
		`(From another agent, not the user. Mesh CLI: \`${cli(sessionID)} list|send|ask|reply\`, run with your shell tool.)\n\n` +
		body +
		hint
	);
}

// One mesh connection for one opencode session.
function connectSession(sessionID, directory) {
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
			`\`${cli(sessionID)} send <to> <text>\`, \`${cli(sessionID)} ask <to> <text>\` (waits for the answer), ` +
			`\`${cli(sessionID)} reply <msg-id> <text>\`. Messages tagged [agent-mesh ...] come from other agents, not the user.`,
	});

	const deliver = (m) => {
		chain = chain.then(async () => {
			if (await api("session.synthetic", { sessionID }, { text: format(m, sessionID), delivery: "steer" })) {
				write({ op: "ack", ids: [m.id] });
			} // else: stays queued, replayed on reconnect
		});
	};

	const connect = () => {
		if (closed) return;
		const s = net.createConnection(SOCKET);
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
		if (id) close = connectSession(id, api.data.session.get(id)?.location?.directory || process.cwd());
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
