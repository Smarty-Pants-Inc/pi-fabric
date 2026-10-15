import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { sweepResidentGenerationRecords } from "../src/residency/generation-retention.js";
import { MeshRetentionApplyDisabledError, sweepMeshRetention } from "../src/storage/retention-cli.js";
import { startRemovalTally, stopRemovalTally } from "../src/storage/verified-remove.js";

const DAY = 24 * 60 * 60 * 1_000;
const OLD = "11111111-1111-4111-8111-111111111111";
const CURRENT = "22222222-2222-4222-8222-222222222222";
const RECENT = "33333333-3333-4333-8333-333333333333";
const roots: string[] = [];
afterEach(() => {
  stopRemovalTally();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-generation-retention-"));
  roots.push(root);
  fs.mkdirSync(path.join(root, "generations"), { mode: 0o700 });
  fs.writeFileSync(path.join(root, "owner.json"), JSON.stringify({ format: 1, token: CURRENT }), { mode: 0o600 });
  return { root, dir: path.join(root, "generations") };
};
const record = (dir: string, token: string, suffix: string, text: string, mtime: number) => {
  const file = path.join(dir, `${token}.${suffix}.json`);
  fs.writeFileSync(file, text, { mode: 0o600 });
  fs.utimesSync(file, mtime / 1_000, mtime / 1_000);
  return file;
};

describe("resident generation record retention", () => {
  it.skipIf(process.platform !== "linux")("previews aged canonical records, never the current generation, and counts bytes without changing files", () => {
    const { root, dir } = fixture();
    const now = Date.now();
    const aged = [
      record(dir, OLD, "exit", "exit-bytes", now - 10 * DAY),
      record(dir, OLD, "launcher-exit", "launcher-bytes", now - 10 * DAY),
      record(dir, OLD, "error", "error-bytes", now - 10 * DAY),
    ];
    const current = record(dir, CURRENT, "exit", "current", now - 30 * DAY);
    const recent = record(dir, RECENT, "error", "recent", now - DAY);
    const before = new Map([ ...aged, current, recent ].map(file => [file, fs.readFileSync(file, "utf8")]));
    const tally = startRemovalTally();
    const changes: Array<{ path: string; beforeBytes: number; afterBytes: 0 }> = [];

    sweepResidentGenerationRecords(root, now, () => false, true, change => changes.push(change));

    expect(tally.residentGenerationRecords).toMatchObject({ removed: 3, bytes: 35, refused: {} });
    expect(changes).toEqual([...aged].sort().map(file => ({ path: file, beforeBytes: fs.statSync(file).size, afterBytes: 0 })));
    expect([...before].every(([file, text]) => fs.readFileSync(file, "utf8") === text)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("keeps symlink and writable records and refuses malformed current-owner state closed", () => {
    const { root, dir } = fixture();
    const now = Date.now();
    const old = record(dir, OLD, "exit", "old", now - 10 * DAY);
    const writable = record(dir, RECENT, "error", "writable", now - 10 * DAY);
    fs.chmodSync(writable, 0o666);
    const link = path.join(dir, `${RECENT}.launcher-exit.json`);
    fs.symlinkSync(old, link);
    const tally = startRemovalTally();
    sweepResidentGenerationRecords(root, now, () => false, true, () => {});
    expect(fs.readFileSync(old, "utf8")).toBe("old");
    expect(fs.lstatSync(link).isSymbolicLink()).toBe(true);
    expect(fs.readFileSync(writable, "utf8")).toBe("writable");
    expect(tally.residentGenerationRecords?.refused).toMatchObject({
      "generation record is not a regular file": 1,
      "generation record is not private and owned": 1,
    });

    fs.writeFileSync(path.join(root, "owner.json"), "not-json", { mode: 0o600 });
    const before = tally.residentGenerationRecords!.removed;
    sweepResidentGenerationRecords(root, now, () => false, true, () => {});
    expect(tally.residentGenerationRecords?.removed).toBe(before);
    expect(fs.readFileSync(old, "utf8")).toBe("old");
  });

  it("rejects API apply before it reads the mesh", async () => {
    const reads: string[] = [];
    const original = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
      reads.push(String(args[0]));
      return original(...args);
    }) as typeof fs.lstatSync);
    await expect(sweepMeshRetention("/not-read-by-apply", { dryRun: false })).rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
    expect(reads).toEqual([]);
  });
});
