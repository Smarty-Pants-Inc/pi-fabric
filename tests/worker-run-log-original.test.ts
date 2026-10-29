import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { compactTerminalRunLog } from "../src/worker/run-log.js";
import { canRemoveTerminalRun, compactTerminalRunEvents, isSettledTerminalRun, pruneActorRunArchives,
  removeUnreferencedRun, runTreeExitVeto, runTreeRemovalVeto, runTreeResourceVeto, sweepTempRunRoots } from "../src/storage/retention.js";
import { ResidentRequestRetention, sweepResidentRuns } from "../src/residency/retention.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Data-safety regression: equal parsed results do not prove equal raw log bytes.
// All payloads here are synthetic; escapes, spacing and CRLF must survive too.
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "run-log-original-"));
  roots.push(root);
  const content = [{ type: "text", text: `λ${"x".repeat(9 * 1024)}` }];
  const result = { content, details: { count: 7 }, opaque: { keep: true } };
  const end = { type: "tool_execution_end", toolCallId: "synthetic-call", toolName: "synthetic", result, isError: false };
  const canonical = { type: "message_end", message: { role: "toolResult", toolCallId: end.toolCallId,
    toolName: end.toolName, content, details: result.details, isError: false } };
  const rawEnd = JSON.stringify(end).replaceAll("λ", "\\u03bb").replaceAll(":", ": ");
  const original = Buffer.from(`${rawEnd}\r\n${JSON.stringify(canonical)}\n`);
  const file = path.join(root, "events.jsonl");
  fs.writeFileSync(file, original, { mode: 0o600 });
  return { root, file, original };
};

describe("terminal run-log raw-byte preservation", () => {
  it.each(["completed", "failed", "stopped", "timed_out"])("keeps every original byte after a %s rewrite", status => {
    const { root, file, original } = fixture();
    const outcome = compactTerminalRunLog(file, status);
    expect(outcome.error).toBeUndefined();
    expect(outcome.compactionSkipped).toBeUndefined();
    expect(outcome.compacted).toBe(1);
    const rewritten = fs.readFileSync(file);
    expect(rewritten.equals(original)).toBe(false);
    expect(JSON.parse(rewritten.toString().split("\n")[0]!).result.elided).toBe(true);
    // Old code loses this exact encoding even though canonical values are equal.
    const copies = fs.readdirSync(root).map(name => fs.readFileSync(path.join(root, name)));
    expect(copies.some(bytes => bytes.equals(original)), "original bytes must remain recoverable after rewrite").toBe(true);
    expect(fs.readFileSync(`${file}.original`)).toEqual(original);
    expect(fs.statSync(`${file}.original`).mode & 0o777).toBe(fs.statSync(file).mode & 0o777);
    expect(compactTerminalRunLog(file, status).compacted).toBe(0);
    expect(fs.readFileSync(`${file}.original`)).toEqual(original);
  });

  it("retains the reader-facing log when preserving the source fails", () => {
    const { file, original, root } = fixture();
    const link = vi.spyOn(fs, "linkSync").mockImplementation(() => {
      throw Object.assign(new Error("injected unsupported hard link"), { code: "ENOTSUP" });
    });
    const rename = vi.spyOn(fs, "renameSync");
    expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0,
      error: expect.stringContaining("unsupported hard link") });
    expect(link).toHaveBeenCalledTimes(1);
    expect(rename).not.toHaveBeenCalled();
    expect(fs.readFileSync(file)).toEqual(original);
    expect(fs.readdirSync(root)).toEqual(["events.jsonl"]);
  });

  it("never overwrites an existing original or a symlink at its name", () => {
    const { file, original } = fixture();
    const prior = Buffer.from("older retained original\n");
    fs.writeFileSync(`${file}.original`, prior);
    expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0,
      error: expect.stringContaining("EEXIST") });
    expect(fs.readFileSync(`${file}.original`)).toEqual(prior);
    expect(fs.readFileSync(file)).toEqual(original);
    fs.unlinkSync(`${file}.original`);
    fs.symlinkSync(file, `${file}.original`);
    expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0,
      error: expect.stringContaining("EEXIST") });
    expect(fs.lstatSync(`${file}.original`).isSymbolicLink()).toBe(true);
    expect(fs.readFileSync(file)).toEqual(original);
  });

  it("does not reuse an unrelated original whose wide file ID rounds to the source ID", () => {
    const { file, original } = fixture();
    const other = Buffer.alloc(original.length, 0x79);
    fs.writeFileSync(`${file}.original`, other);
    const seconds = Math.floor(Date.now() / 1000);
    for (const name of [file, `${file}.original`]) fs.utimesSync(name, seconds, seconds);
    const wide = 2n ** 54n;
    expect(Number(wide + 1n)).toBe(Number(wide));
    const fstat = fs.fstatSync;
    const lstat = fs.lstatSync;
    // Model only the wide IDs; use real files, bytes, timestamps and replacement I/O.
    vi.spyOn(fs, "fstatSync").mockImplementation((fd, options) => {
      const stat = fstat(fd, options);
      Object.defineProperty(stat, "ino", { value: options?.bigint ? wide : Number(wide) });
      return stat;
    });
    vi.spyOn(fs, "lstatSync").mockImplementation((name, options) => {
      const stat = lstat(name, options);
      const id = String(name) === `${file}.original` ? wide + 1n : wide;
      Object.defineProperty(stat, "ino", { value: options?.bigint ? id : Number(id) });
      return stat;
    });
    expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0,
      error: expect.stringContaining("EEXIST") });
    expect(fs.readFileSync(file)).toEqual(original);
    expect(fs.readFileSync(`${file}.original`)).toEqual(other);
  });

  it("keeps events.jsonl readable and the original complete before publishing", () => {
    const { file, original } = fixture();
    const rename = fs.renameSync;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      expect(to).toBe(file);
      expect(fs.readFileSync(file)).toEqual(original);
      expect(fs.readFileSync(`${file}.original`)).toEqual(original);
      rename(from, to);
    });
    expect(compactTerminalRunLog(file, "completed").compacted).toBe(1);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe("retained-original namespace durability", () => {
  it.skipIf(process.platform === "win32")("syncs the retained link's directory before publishing the compacted view", () => {
    const { file, original } = fixture();
    const sync = fs.fsyncSync;
    const rename = fs.renameSync;
    let directorySynced = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) {
        expect(fs.readFileSync(`${file}.original`)).toEqual(original);
        directorySynced = true;
      }
      sync(fd);
    });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      expect(directorySynced).toBe(true);
      rename(from, to);
    });
    // This test asserts durable ordering, not the independent elapsed-work ceiling.
    vi.spyOn(performance, "now").mockReturnValue(0);
    expect(compactTerminalRunLog(file, "completed").compacted).toBe(1);
  });

  it.skipIf(process.platform === "win32")("retains both full-log names when the directory barrier fails", () => {
    const { file, original } = fixture();
    const sync = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) throw new Error("synthetic directory sync failure");
      sync(fd);
    });
    const rename = vi.spyOn(fs, "renameSync");
    expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0,
      error: "synthetic directory sync failure" });
    expect(rename).not.toHaveBeenCalled();
    expect(fs.readFileSync(file)).toEqual(original);
    expect(fs.readFileSync(`${file}.original`)).toEqual(original);
    expect(fs.statSync(file).nlink).toBe(2);
    expect(fs.statSync(`${file}.original`).ino).toBe(fs.statSync(file).ino);
  });
});

// Exercise the actual filesystem retention entry points, not mock call counts.
// The positive control proves terminal/exit custody would otherwise permit collection.
describe("retention of original run logs", () => {
  const day = 24 * 60 * 60 * 1000;
  const now = Date.now();
  const retained = () => {
    const { root, file, original } = fixture();
    const runs = path.join(root, "runs");
    const run = path.join(runs, "history");
    fs.mkdirSync(run, { recursive: true, mode: 0o700 });
    const events = path.join(run, "events.jsonl");
    fs.renameSync(file, events);
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed",
      transport: "process", sessionId: "2147483647", finishedAt: now - 8 * day }));
    expect(canRemoveTerminalRun(run)).toBe(true);
    expect(compactTerminalRunLog(events, "completed").compacted).toBe(1);
    fs.utimesSync(run, (now - 8 * day) / 1000, (now - 8 * day) / 1000);
    return { root, runs, run, events, original };
  };

  it("vetoes event-tail compaction and actor run pruning without changing resource cleanup", () => {
    const { runs, run, events, original } = retained();
    const compacted = fs.readFileSync(events);
    expect(runTreeExitVeto(run)).toBeUndefined();
    expect(runTreeRemovalVeto(run)).toBe("original run log is retained");
    expect(runTreeResourceVeto(run)).toBeUndefined();
    expect(isSettledTerminalRun(run)).toBe(true);
    expect(canRemoveTerminalRun(run)).toBe(false);
    expect(compactTerminalRunEvents(run, { now, terminalRunEventsAgeMs: 0,
      terminalRunEventsMaxBytes: 100 })).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: runs, now, retentionMs: 0 })).toEqual([]);
    expect(fs.readFileSync(events)).toEqual(compacted);
    expect(fs.readFileSync(`${events}.original`)).toEqual(original);
  });

  it("keeps originals at the shared run-tree removal entry point", () => {
    const { run, events, original } = retained();
    expect(removeUnreferencedRun(run, fs.statSync(run))).toBe(false);
    expect(fs.readFileSync(`${events}.original`)).toEqual(original);
  });

  it.each([false, true])("keeps raw bytes through resident retention (retainRuns=%s)", retainRuns => {
    const { root, runs, events, original } = retained();
    const compacted = fs.readFileSync(events);
    expect(sweepResidentRuns(runs, now, 10_000, { retainRuns, retentionMs: 0,
      terminalRunEventsAgeMs: 0, terminalRunEventsMaxBytes: 100 })).toEqual([]);
    const collector = new ResidentRequestRetention(root, [], {
      terminalRunEventsAgeMs: 0, terminalRunEventsMaxBytes: 100 });
    try { collector.sweep(now, new Set(), 10_000); } finally { collector.close(); }
    expect(fs.readFileSync(events)).toEqual(compacted);
    expect(fs.readFileSync(`${events}.original`)).toEqual(original);
  });

  it("keeps originals through closed temporary-root retention", () => {
    const { root, run, original } = retained();
    const tempRoot = path.join(root, "temp");
    const managedRoot = path.join(tempRoot, "pi-fabric-runs-retained");
    fs.mkdirSync(managedRoot, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(managedRoot, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647,
      startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }), { mode: 0o600 });
    const managedRun = path.join(managedRoot, "history");
    fs.renameSync(run, managedRun);
    expect(sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 }))
      .toEqual({ removedRuns: [], removedRoots: [] });
    expect(fs.readFileSync(path.join(managedRun, "events.jsonl.original"))).toEqual(original);
  });

  it("propagates retained-original custody from a nested run to its parent", () => {
    const { root, run, events, original } = retained();
    const parent = path.join(root, "parent");
    fs.mkdirSync(path.join(parent, "nested"), { recursive: true });
    fs.writeFileSync(path.join(parent, "status.json"), fs.readFileSync(path.join(run, "status.json")));
    fs.renameSync(run, path.join(parent, "nested", "child"));
    expect(runTreeExitVeto(parent)).toBeUndefined();
    expect(runTreeRemovalVeto(parent)).toBe("original run log is retained");
    expect(isSettledTerminalRun(parent)).toBe(true);
    expect(canRemoveTerminalRun(parent)).toBe(false);
    expect(fs.readFileSync(path.join(parent, "nested", "child", path.basename(`${events}.original`)))).toEqual(original);
  });
});
