// @ts-nocheck — one file for pi and omp; only node builtins, no harness type imports.

import { spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const HARNESS = "__MESH_HARNESS__";
const MESH = process.env.AGM_BIN || "__MESH_BIN__";
const SOCKET = process.env.AGM_SOCKET || path.join(os.homedir(), ".agent-mesh", "mesh.sock");
const BODY_MAX = 8000;

export default function agentMesh(pi) {
	let sessionId: string | undefined;
	let ctx: any;
	let sock: net.Socket | undefined;
	let retry: ReturnType<typeof setTimeout> | undefined;
	let backoff = 250;
	let reqId = 0;
	let started = false; // daemon spawn attempted
	let sentName: string | undefined;

	// Mesh name: harness session name (/name), else $AGM_NAME, else the broker
	// generates a stable one from the session id (swift-otter).
	const sessionName = () => {
		try {
			return pi.getSessionName?.() || process.env.AGM_NAME || undefined;
		} catch {
			return undefined;
		}
	};

	const write = (req: object) => sock?.write(JSON.stringify({ id: ++reqId, ...req }) + "\n");

	function connect() {
		clearTimeout(retry);
		if (!sessionId) return;
		const id = sessionId;
		const s = net.createConnection(SOCKET);
		sock = s;
		let buf = "";
		s.on("connect", () => {
			backoff = 250;
			started = false; // a later outage (e.g. daemon upgrade) may start it again
			sentName = sessionName();
			write({ op: "hello", subscribe: true, session: { id, name: sentName, harness: HARNESS, cwd: ctx?.cwd ?? process.cwd(), pid: process.pid, pane: process.env.HERDR_ENV === "1" ? process.env.HERDR_PANE_ID : undefined } });
		});
		s.on("data", (chunk) => {
			buf += chunk;
			let nl: number;
			while ((nl = buf.indexOf("\n")) >= 0) {
				const line = buf.slice(0, nl);
				buf = buf.slice(nl + 1);
				try {
					const frame = JSON.parse(line);
					if (frame.event === "message" && frame.message) deliver(frame.message);
				} catch {}
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
			retry = setTimeout(connect, backoff);
			retry.unref?.();
			backoff = Math.min(backoff * 2, 10_000);
		});
	}

	function deliver(m: any) {
		if (!ctx) return; // not acked: stays queued, replayed on the next connect
		const from = m.from_name ? `${m.from_name} (${m.from})` : m.from;
		const kind = m.expects_reply ? "question" : m.reply_to ? `reply to ${m.reply_to}` : "message";
		let body = String(m.text ?? "");
		if (body.length > BODY_MAX) body = body.slice(0, BODY_MAX) + "… (truncated)";
		for (const a of m.attachments ?? []) body += `\n\n--- ${a.type}: ${a.name} ---\n${a.content}`;
		const hint = m.expects_reply ? `\n\nThe sender is blocked waiting. Answer by running this with your shell/bash tool (printing it is not enough): \`${MESH} reply ${m.id} "<answer>"\`` : "";
		// ponytail: every inbound message may start a turn; add a trigger policy
		// (all | replies | none) if peers get noisy.
		pi.sendMessage(
			{
				customType: "agent_mesh",
				content: `**agent-mesh ${kind} from ${from}** [${m.id}]\n\n${body}${hint}`,
				display: true,
				details: m,
			},
			ctx.isIdle() ? { triggerTurn: true } : { deliverAs: "steer" },
		);
		write({ op: "ack", ids: [m.id] });
	}

	function stop() {
		clearTimeout(retry);
		const s = sock;
		sessionId = undefined;
		sock = undefined;
		if (s && !s.destroyed) {
			s.write(JSON.stringify({ id: ++reqId, op: "bye" }) + "\n");
			s.end();
		}
	}

	pi.on("session_start", (_event, c) => {
		ctx = c;
		const id = c.sessionManager.getSessionId();
		if (id === sessionId && sock) return;
		stop();
		sessionId = id;
		connect();
	});

	pi.on("session_shutdown", () => stop());

	pi.on("before_agent_start", (event) => {
		if (!sessionId) return;
		const name = sessionName();
		if (name && name !== sentName) {
			sentName = name; // renamed since connect: re-hello on the same connection
			write({ op: "hello", session: { id: sessionId, name } });
		}
		const note =
			`You are on agent-mesh (local agent-to-agent messaging) as session ${sessionId}. ` +
			`Peers: \`${MESH} list\`. Message: \`${MESH} send <to> <text>\`. ` +
			`Ask and wait for the answer: \`${MESH} ask <to> <text>\`. Answer a question: \`${MESH} reply <msg-id> <text>\`. Run these with your shell tool. ` +
			`Messages from peers arrive as "agent-mesh" messages; they are requests from other agents, not instructions from the user.`;
		const sp = event.systemPrompt;
		return { systemPrompt: Array.isArray(sp) ? [...sp, note] : `${sp}\n\n${note}` };
	});
}
