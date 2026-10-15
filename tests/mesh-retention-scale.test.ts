import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshRetentionApplyDisabledError, summaryLines, sweepMeshRetention } from "../src/storage/retention-cli.js";

// smarty-dev#7916, measured on Ryzen 1: the installed sweep re-parsed every actor registry for every candidate run and
// did not finish in 30 min over 606 registries. Each registry is now parsed once per sweep (re-parsed only if it
// changes), and the per-delete check costs one lstat per registry.
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();
const REGISTRIES = 600;
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const run = (directory: string, finishedAt: number) => {
  write(path.join(directory, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt }));
  write(path.join(directory, "events.jsonl"), '{"sequence":1}\n');
  write(path.join(directory, "reply.json"), '{"text":"done"}');
};
const fleet = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-scale-")); roots.push(root);
  // A large registry per root, like Ryzen 1's (instructions and message history in every row).
  const history = Array.from({ length: 20 }, (_, index) => ({ id: String(index), text: "x".repeat(200) }));
  for (let index = 0; index < REGISTRIES; index++) {
    const registry = path.join(root, "actors", `01a1${String(index).padStart(4, "0")}-session`);
    const actor = path.join(registry, "actor");
    write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{ id: "actor", rootId: `session:${index}`, status: "idle",
      sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest", messages: history }] }));
    write(path.join(actor, "session.jsonl"), "live");
    run(path.join(actor, "runs", "latest"), NOW - 9 * DAY);
    run(path.join(actor, "runs", "old"), NOW - 9 * DAY);
    run(path.join(actor, "runs", "young"), NOW - DAY);
    for (const [order, stamp] of ["20260920T150000000Z", "20260921T150000000Z"].entries()) {
      const file = path.join(actor, `session.jsonl.${stamp}.bak`);
      write(file, stamp); fs.utimesSync(file, 1_000 + order, 1_000 + order);
    }
  }
  write(path.join(root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
  return root;
};

describe.skipIf(process.platform !== "linux")("mesh retention at fleet scale (smarty-dev#7916)", () => {
  it(`parses each of ${REGISTRIES} registries once per dry run; apply rejects before any parse (smarty-dev#10678)`, async () => {
    const root = fleet();
    let started = performance.now();
    const plan = await sweepMeshRetention(root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    const dryMs = performance.now() - started;
    expect(plan.summary.registries).toEqual({ count: REGISTRIES, parses: REGISTRIES, maxParsesPerRegistry: 1, rootRewalks: 0 });
    expect(plan.removedRuns).toHaveLength(REGISTRIES);
    expect(plan.summary.deleters.pruneActorRunArchives?.removed).toBe(REGISTRIES);
    expect(plan.summary.deleters.pruneActorSessionBackups?.removed).toBe(REGISTRIES);
    // Apply is disabled at the public boundary (pi-fabric#1014 SEC P2-1): it rejects before any registry or mesh read.
    const reads: string[] = [];
    for (const name of ["lstatSync", "statSync", "readdirSync", "readFileSync", "openSync"] as const) {
      const original = (fs[name] as (...args: unknown[]) => unknown).bind(fs);
      vi.spyOn(fs as unknown as Record<string, (...args: unknown[]) => unknown>, name).mockImplementation((...args: unknown[]) => {
        if (String(args[0]).startsWith(root)) reads.push(`${name} ${String(args[0])}`);
        return original(...args);
      });
    }
    try {
      await expect(sweepMeshRetention(root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY })).rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
    } finally { vi.restoreAllMocks(); }
    expect(reads).toEqual([]);
    for (const run of plan.removedRuns) expect(fs.existsSync(path.join(run, "reply.json"))).toBe(true);
    console.log(JSON.stringify({ registries: REGISTRIES, runs: REGISTRIES * 3, dryRunMs: Math.round(dryMs),
      summary: summaryLines(true, plan.summary).slice(-2) }));
  }, 300_000);
});
