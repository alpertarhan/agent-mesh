#!/usr/bin/env node
// Runs the prebuilt agm binary for this platform (all four ship in dist/, filled by
// the release workflow). No install scripts, so it works with npm, pnpm and bun.
// ponytail: one package with every platform (~10 MB); split into per-platform
// optionalDependencies if the download size matters.
const { spawnSync } = require("node:child_process");
const { chmodSync } = require("node:fs");
const path = require("node:path");

const bin = path.join(__dirname, "..", "dist", `agm-${process.platform}-${process.arch}`);
try {
	chmodSync(bin, 0o755);
} catch {}
const r = spawnSync(bin, process.argv.slice(2), { stdio: "inherit" });
if (r.error) {
	console.error(`agm: no prebuilt binary for ${process.platform}-${process.arch} (${r.error.message})`);
	process.exit(1);
}
process.exit(r.status ?? 1);
