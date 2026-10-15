import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { canRemoveTerminalRun, compactTerminalRunEvents, pruneActorRunArchives } from "../src/storage/retention.js";
import { legacyRunTreeProof } from "../src/residency/legacy-run-archive.js";

// Decision C: real retention entry points, dry-run only. Every product removal
// callback is recorded; no apply, real product prune, unlink or fleet sweep runs.
let root: string;
const now = 100_000, age = 1000;
const restore = `.events.jsonl.restore-123-${"a".repeat(32)}.tmp`;
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "compressed-retention-v2-")); });
afterEach(() => { vi.restoreAllMocks(); });
const fixture = (legacy = false, names = ["events.jsonl.zst"]) => {
  const runs = path.join(root, "runs");
  const run = path.join(runs, "a".repeat(32));
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  const status = { id: "a".repeat(32), status: "completed", finishedAt: 1, transport: "process", ...(!legacy ? { sessionId: "2147483647" } : {}) };
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(status), { mode: 0o600 });
  for (const name of names) fs.writeFileSync(path.join(run, name), `${name}\n`, { mode: 0o600 });
  return { run, runs, status };
};
const dryRun = (runs: string, extra: Partial<Parameters<typeof pruneActorRunArchives>[0]> = {}) => {
  const changes: string[] = [];
  const removed = pruneActorRunArchives({ runsDirectory: runs, retentionMs: age, now, ...extra,
    dryRun: true, onRemove: (file) => { changes.push(file); } });
  expect(removed).toEqual(changes);
  return removed;
};

describe("compressed artifacts keep existing whole-run custody", () => {
  it.each([["events.jsonl.zst"], ["events.jsonl", "events.jsonl.zst"], ["events.jsonl.zst", restore]].map(names => ({ names })))(
    "recognizes $names for dry-run collection without removing any bytes", ({ names }) => {
      const f = fixture(false, names);
      const before = fs.readdirSync(f.run).sort();
      expect(canRemoveTerminalRun(f.run)).toBe(true);
      expect(dryRun(f.runs)).toEqual([f.run]);
      expect(fs.readdirSync(f.run).sort()).toEqual(before);
      for (const name of names) expect(fs.readFileSync(path.join(f.run, name), "utf8")).toBe(`${name}\n`);
    });

  it.each([["events.jsonl.zst"], ["events.jsonl", "events.jsonl.zst", restore]].map(names => ({ names })))(
    "legacy tree proof recognizes $names with stable ownership fingerprint", async ({ names }) => {
      const f = fixture(true, names);
      const proof = await legacyRunTreeProof(f.run, now, age);
      expect(proof).toMatchObject({ status: "completed", finishedAt: 1 });
      for (const name of names) expect(proof?.fingerprint).toContain(name);
      expect(await legacyRunTreeProof(f.run, now, age)).toEqual(proof);
    });

  it.each(["unknown.bin", ".events.jsonl.restore-0-" + "a".repeat(32) + ".tmp",
    ".events.jsonl.restore-01-" + "a".repeat(32) + ".tmp", ".events.jsonl.restore-1-" + "A".repeat(32) + ".tmp",
    ".events.jsonl.restore-1-" + "a".repeat(31) + ".tmp", "events.jsonl.zst.1.tmp"])(
    "unknown artifact %s still vetoes both proofs", async (name) => {
      const f = fixture(true, ["events.jsonl.zst", name]);
      expect(canRemoveTerminalRun(f.run)).toBe(false);
      expect(dryRun(f.runs)).toEqual([]);
      expect(await legacyRunTreeProof(f.run, now, age)).toBeUndefined();
    });

  it("keeps latest/referenced/young/exit-pending compressed trees", () => {
    const f = fixture();
    expect(dryRun(f.runs, { latestRunId: f.status.id })).toEqual([]);
    expect(dryRun(f.runs, { isRetained: () => true })).toEqual([]);
    expect(dryRun(f.runs, { retentionMs: now })).toEqual([]);
    fs.writeFileSync(path.join(f.run, "status.json"), JSON.stringify({ ...f.status, sessionId: String(process.pid) }));
    expect(dryRun(f.runs)).toEqual([]);
    expect(fs.existsSync(path.join(f.run, "events.jsonl.zst"))).toBe(true);
  });

  it("keeps young and pending legacy compressed trees", async () => {
    const f = fixture(true);
    expect(await legacyRunTreeProof(f.run, 500, age)).toBeUndefined();
    fs.writeFileSync(path.join(f.run, "status.json"), JSON.stringify({ ...f.status, cleanupPending: true }));
    expect(await legacyRunTreeProof(f.run, now, age)).toBeUndefined();
  });

  it("crash restore nlink=2 fails closed without treating the linked temp as disposable", async () => {
    const f = fixture(true, ["events.jsonl.zst", restore]);
    fs.linkSync(path.join(f.run, restore), path.join(f.run, "events.jsonl"));
    expect(fs.lstatSync(path.join(f.run, restore)).nlink).toBe(2);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(dryRun(f.runs)).toEqual([]);
    expect(await legacyRunTreeProof(f.run, now, age)).toBeUndefined();
    expect(fs.lstatSync(path.join(f.run, restore)).nlink).toBe(2);
  });

  it("symlinked compressed entries retain the no-alias fence", async () => {
    const f = fixture(true, []);
    const target = path.join(root, "foreign"); fs.writeFileSync(target, "not archive authority");
    fs.symlinkSync(target, path.join(f.run, "events.jsonl.zst"));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(await legacyRunTreeProof(f.run, now, age)).toBeUndefined();
    expect(dryRun(f.runs)).toEqual([]);
  });

  it("foreign compressed entry is independently vetoed on an otherwise safe tree", async () => {
    const f = fixture(true);
    expect(await legacyRunTreeProof(f.run, now, age)).toBeDefined();
    const lstat = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, opts?: unknown) => {
      const stat = lstat(file, opts as never);
      return stat && String(file) === path.join(f.run, "events.jsonl.zst")
        ? Object.assign(stat, { uid: (process.getuid?.() ?? 0) + 1 }) : stat;
    }) as typeof fs.lstatSync);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(dryRun(f.runs)).toEqual([]);
  });

  it("never tails plain while any zst target exists, including an interrupted writer", () => {
    const f = fixture(false, ["events.jsonl", "events.jsonl.zst"]);
    const plain = path.join(f.run, "events.jsonl");
    const contents = '{"text":"' + "x".repeat(100) + '"}\n';
    fs.writeFileSync(plain, contents.repeat(300));
    expect(canRemoveTerminalRun(f.run)).toBe(true); // not an unknown-artifact/exit veto
    const changes: unknown[] = [];
    expect(compactTerminalRunEvents(f.run, { now, terminalRunEventsAgeMs: 0, terminalRunEventsMaxBytes: 1024,
      dryRun: true, onCompact: change => { changes.push(change); } })).toBe(false);
    expect(changes).toEqual([]);
    expect(fs.readFileSync(plain, "utf8")).toBe(contents.repeat(300));
  });
});
