// Embedded into each standalone adapter by the installer; no runtime dependency.
const MESH_CONTEXT_MAX = 8000; // UTF-8 bytes, including metadata and all hints
const meshBytes = (text) => Buffer.byteLength(String(text), "utf8");

function meshCutUtf8(text, maxBytes) {
	const bytes = Buffer.from(String(text), "utf8");
	if (bytes.length <= maxBytes) return String(text);
	let end = Math.max(0, maxBytes);
	while (end > 0 && (bytes[end] & 0xc0) === 0x80) end--;
	return bytes.subarray(0, end).toString("utf8");
}

// Reserve trust/retrieval/reply text first. Whole blocks (reference paths) are
// either included intact or omitted; attachment headings never appear partially.
function meshContext(prefix, blocks, suffix, fullHint, maxBytes = MESH_CONTEXT_MAX) {
	let left = maxBytes - meshBytes(prefix + suffix + fullHint);
	if (left < 0) {
		const notice = `[agent-mesh]\n${TEXT.frame}\nMessage preview omitted: metadata exceeds its budget.`;
		const fallback = notice + fullHint + suffix;
		return meshBytes(fallback) <= maxBytes ? fallback : notice + " Inspect mesh history/inbox before responding or acting.";
	}
	const marker = "… (truncated)";
	let output = prefix;
	let cut = false;
	for (const block of blocks) {
		const header = String(block.header ?? "");
		const text = String(block.text ?? "");
		const size = meshBytes(header + text);
		if (size <= left) {
			output += header + text;
			left -= size;
			continue;
		}
		cut = true;
		const room = left - meshBytes(header + marker);
		if (!block.whole && room > 0) {
			const clipped = header + meshCutUtf8(text, room) + marker;
			output += clipped;
			left -= meshBytes(clipped);
		}
	}
	return output + (cut ? fullHint : "") + suffix;
}
