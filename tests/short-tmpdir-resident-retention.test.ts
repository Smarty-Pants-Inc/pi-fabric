import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// pi-fabric#1554 P2-2 (smarty-dev#15878): resident run retention (ResidentRequestRetention's V2 sweep and its
// off-Linux main sweep, src/residency/retention.ts) deletes a resident run tree directly. A resident actor run gets a
// short TMPDIR outside that tree too (the host's AgentManager spawn path), so it must leave with the run. On fd533e62
// both sweeps removed the run and leaked its `r.<hex>` directory.
// The platform gate is toggled, never process.platform, so both sweeps run on this host.
const platform = vi.hoisted(() => ({ v2: true }));
vi.mock("../src/storage/retention-platform.js", () => ({ retentionV2Enabled: () => platform.v2 }));
const { ResidentRequestRetention } = await import("../src/residency/retention.js");

const DAY = 86_400_000;
const EXITED_PID = "2147483647";
const shortName = (id: string): string => `r.${createHash("sha256").update(id).digest("hex").slice(0, 10)}`;
const roots: string[] = [];
afterEach(() => {
  platform.v2 = true;
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (scratchEntries: number) => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-short-tmpdir-")));
  roots.push(root);
  const residencyRoot = path.join(root, "residency");
  const id = "0f3c8e2a9b7d4c1e8f6a5b4c3d2e1f00";
  const run = path.join(residencyRoot, "runs", id);
  // As the host's AgentManager allocates it: under the caller's TMPDIR, outside the run tree.
  const temporary = path.join(root, shortName(id));
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  fs.mkdirSync(temporary, { mode: 0o700 });
  for (let index = 0; index < scratchEntries; index++) fs.writeFileSync(path.join(temporary, `scratch-${index}`), "x");
  const now = Date.now();
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({
    id, name: id, task: "resident actor run", cwd: root, runner: "pi", transport: "process", status: "failed",
    sessionId: EXITED_PID, startedAt: now - 3 * DAY, finishedAt: now - 3 * DAY, updatedAt: now - 3 * DAY,
    turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    temporaryDirectory: temporary,
  }), { mode: 0o600 });
  const rawTrace = new Map<string, Buffer>([
    ["events.jsonl", Buffer.from('{"raw":"events π"}\n')],
    ["session.jsonl", Buffer.from('{"raw":"session λ"}\n')],
    ["task.txt", Buffer.from("prompt survives whole-tree custody\n")],
    ["reply.json", Buffer.from('{"text":"saved result survives"}')],
  ]);
  for (const [name, bytes] of rawTrace) fs.writeFileSync(path.join(run, name), bytes, { mode: 0o600 });
  fs.utimesSync(run, (now - 2 * DAY) / 1000, (now - 2 * DAY) / 1000);
  return { residencyRoot, run, temporary, now, rawTrace };
};
const sweep = (residencyRoot: string, now: number): void => {
  const collector = new ResidentRequestRetention(residencyRoot, [], { retainRuns: false });
  try { collector.sweep(now, new Set(), 10_000); } finally { collector.close(); }
};

describe.skipIf(process.platform === "win32")("resident run retention removes a run's short TMPDIR (pi-fabric#1554 P2-2)", () => {
  it.each([["V2", true], ["main", false]] as const)("the %s sweep moves an expired resident run and short TMPDIR to byte-preserving custody", (_name, v2) => {
    platform.v2 = v2;
    const { residencyRoot, run, temporary, now, rawTrace } = fixture(10);
    const unlink = vi.spyOn(fs, "unlinkSync");
    const remove = vi.spyOn(fs, "rmSync");
    sweep(residencyRoot, now);
    expect(fs.existsSync(run)).toBe(false);
    expect(fs.existsSync(temporary)).toBe(false);
    const archive = path.join(residencyRoot, "runs.trace-archive");
    const receipts = fs.readdirSync(archive).map(name => JSON.parse(fs.readFileSync(path.join(archive, name, "custody.json"), "utf8")));
    const runReceipt = receipts.find(row => row.source === run && row.state === "moved");
    const tmpReceipt = receipts.find(row => row.source === temporary && row.state === "moved");
    expect(runReceipt).toBeDefined(); expect(tmpReceipt).toBeDefined();
    for (const [name, bytes] of rawTrace) expect(fs.readFileSync(path.join(runReceipt.destination, name))).toEqual(bytes);
    for (let index = 0; index < 10; index++) expect(fs.readFileSync(path.join(tmpReceipt.destination, `scratch-${index}`), "utf8")).toBe("x");
    expect(unlink.mock.calls.filter(([file]) => [...rawTrace.keys()].some(name => String(file) === path.join(run, name)))).toEqual([]);
    expect(remove.mock.calls.filter(([file]) => String(file) === run || String(file) === temporary)).toEqual([]);
  });

  it.each([["V2", true], ["main", false]] as const)("the %s sweep keeps the run and its short TMPDIR when the scratch is unsafe", (_name, v2) => {
    platform.v2 = v2;
    const { residencyRoot, run, temporary, now } = fixture(65);
    sweep(residencyRoot, now);
    // Custody unchanged: more than 64 entries vetoes the run, exactly as an in-tree tmp/ did.
    expect(fs.existsSync(path.join(run, "status.json"))).toBe(true);
    expect(fs.readdirSync(temporary)).toHaveLength(65);
  });
});
