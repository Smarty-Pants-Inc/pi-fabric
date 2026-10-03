import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/** Optional owner-requested native regression receipts, never enabled in CI by
 * default. No credentials, shell commands or background work. */
export const scratchEvidence = (name: string, receipt: Record<string, unknown>): void => {
  const output = process.env.PI_FABRIC_SCRATCH_EVIDENCE_DIR;
  if (!output) return;
  if (!path.isAbsolute(output) || !/^[a-z0-9-]+$/.test(name)) throw new Error("Unsafe scratch evidence address");
  fs.mkdirSync(output, { recursive: true, mode: 0o700 });
  const sources = ["src/agents/transports/process-utils.ts", "src/agents/transports/process-transport.ts",
    "src/storage/process-scratch-scope.ts", "src/storage/run-scratch.ts", "src/storage/temp-root.ts",
    "src/storage/retention.ts", "src/storage/scratch-process-census.ts", "dist/worker.js", "dist/index.js"];
  const hashes = Object.fromEntries(sources.map(file => [file, fs.existsSync(file) ? createHash("sha256").update(fs.readFileSync(file)).digest("hex") : null]));
  fs.writeFileSync(path.join(output, name + ".json"), JSON.stringify({ recordedAt: new Date().toISOString(),
    platform: process.platform, runtime: process.version, sourceHashes: hashes, ...receipt }, null, 2) + "\n", { mode: 0o600, flag: "wx" });
};
