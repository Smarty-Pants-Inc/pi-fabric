import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import { FABRIC_RUN_ROOT_PREFIX, archiveRunSessionTraces, isTraceArchivePath, markRunRootActive, markRunRootClosed, RUN_SESSION_TRACE_ARCHIVE_DIRECTORY,
  runSessionTraceArchivePath, sweepTempRunRoots, removeUnreferencedRun, compactTerminalRunEvents, pruneActorRunArchives,
  pruneActorSessionBackups, removeEmptyRunRoot } from "../src/storage/retention.js";
import { sweepResidentRuns } from "../src/residency/retention.js";
import { testTempRoot } from "./helpers/test-temp-root.js";

const roots: string[] = [];
const HOSTED_ARCHIVES = hostedOnly("outside-TMPDIR trace archive tests");
const DAY = 24 * 60 * 60 * 1_000;
const tracePaths = [
  "events.jsonl",
  "events.jsonl.raw",
  "session.jsonl",
  "route-session.jsonl",
  "handoff-session/fork-1.jsonl",
  "nested/child/session.jsonl",
] as const;

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(kind: "closed" | "orphaned", durableArchiveRoot = false) {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-trace-retention-"));
  const meshBase = durableArchiveRoot ? os.userInfo().homedir : os.tmpdir();
  const meshPrefix = durableArchiveRoot ? ".pi-fabric-trace-mesh-" : "fabric-trace-mesh-";
  const meshRoot = fs.mkdtempSync(path.join(meshBase, meshPrefix));
  roots.push(tempRoot, meshRoot);
  const runRoot = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}${kind}`);
  const run = path.join(runRoot, "run-1");
  fs.mkdirSync(path.join(run, "handoff-session"), { recursive: true });
  fs.mkdirSync(path.join(run, "nested", "child"), { recursive: true });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1 }));
  fs.writeFileSync(path.join(run, "nested", "child", "status.json"), JSON.stringify({
    status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1,
  }));
  const mtimes = new Map<string, number>();
  for (const relative of tracePaths) {
    const file = path.join(run, relative);
    const content = `raw transcript: ${relative}\n`;
    fs.writeFileSync(file, content);
    fs.utimesSync(file, new Date(1_000), new Date(1_000));
    mtimes.set(relative, fs.statSync(file).mtimeMs);
  }
  if (kind === "closed") {
    markRunRootActive(runRoot, 1);
    markRunRootClosed(runRoot, 1, true);
  } else {
    fs.writeFileSync(path.join(runRoot, ".fabric-owner.json"), JSON.stringify({
      pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1,
    }));
  }
  const archive = runSessionTraceArchivePath(meshRoot, run);
  const sweep = () => sweepTempRunRoots({
    tempRoot, meshRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1,
  });
  return { tempRoot, meshRoot, runRoot, run, archive, sweep, mtimes };
}

describe("raw trace retention", () => {
  it.skipIf(process.platform === "win32")("vetoes a moved-tree identity mismatch, restores the run, and keeps the archived raw trace", () => {
    const root = testTempRoot("resident-moved-identity-");
    roots.push(root);
    const runs = path.join(root, "runs"), meshRoot = path.join(root, "mesh"), run = path.join(runs, "run-identity");
    fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647" }));
    fs.writeFileSync(path.join(run, "events.jsonl"), "raw resident trace\n");
    expect(() => process.kill(2147483647, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    const judged = fs.statSync(run);
    let checks = 0;
    let mismatchObserved = false;
    const referenced = (): boolean => {
      checks++;
      if (checks === 1) return false;
      let moved: string;
      if (process.platform === "linux") {
        const grave = path.join(runs, `.retention-graveyard-${process.pid}`);
        const name = fs.readdirSync(grave).find(entry => entry.startsWith(`${path.basename(run)}.`));
        if (!name) return true;
        moved = path.join(grave, name);
      } else moved = path.join(runs, `.retention-tombstone-${path.basename(run)}`);
      const current = fs.statSync(moved);
      // Simulate a stale/mismatched identity in the final path judgment without changing the held tree.
      mismatchObserved = current.dev !== judged.dev || current.ino !== judged.ino + 1;
      return mismatchObserved;
    };

    const removed = removeUnreferencedRun(run, judged, referenced, false, "residentMovedIdentityTest", 0, false, meshRoot);
    const archive = runSessionTraceArchivePath(meshRoot, run);
    expect(removed).toBe(false);
    expect(checks).toBe(2);
    expect(mismatchObserved).toBe(true);
    expect(fs.existsSync(run)).toBe(true);
    expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toContain("completed");
    expect(fs.existsSync(path.join(run, "events.jsonl"))).toBe(false);
    expect(fs.readFileSync(path.join(archive, "events.jsonl"), "utf8")).toBe("raw resident trace\n");
  });

  it("rejects temporary and reaped archive roots without creating them", () => {
    const source = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-archive-placement-source-"));
    roots.push(source);
    fs.writeFileSync(path.join(source, "events.jsonl"), "raw events\n");
    const meshRoots = [
      path.join(os.tmpdir(), `fabric-unsafe-mesh-${process.pid}-${Date.now()}`),
      path.join("/tmp", `pi-fabric-runs-placement-${process.pid}-${Date.now()}`),
      path.join("/srv/scratch/paul/smarty-pi-tmp", `placement-${process.pid}-${Date.now()}`),
    ];
    for (const meshRoot of meshRoots) {
      expect(archiveRunSessionTraces(source, undefined, meshRoot)).toBe(false);
      expect(fs.existsSync(path.join(meshRoot, RUN_SESSION_TRACE_ARCHIVE_DIRECTORY))).toBe(false);
      expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).toBe("raw events\n");
    }
  });

  it.skipIf(!HOSTED_ARCHIVES).each(["closed", "orphaned"] as const)("archives every raw transcript before removing a %s temp run", kind => {
    const f = fixture(kind, true);
    const result = f.sweep();
    if (kind === "closed") expect(result.removedRuns).toEqual([f.run]);
    else expect(result.removedRuns).toEqual([]); // Orphan collection is reported at the root boundary.
    expect(fs.existsSync(f.run)).toBe(false);
    expect(fs.existsSync(f.archive)).toBe(true);
    for (const relative of tracePaths) {
      const archived = path.join(f.archive, relative);
      expect(fs.readFileSync(archived, "utf8")).toBe(`raw transcript: ${relative}\n`);
      expect(fs.statSync(archived).mtimeMs).toBe(f.mtimes.get(relative));
    }
    expect(fs.existsSync(f.runRoot), "the archive does not depend on the temporary run root").toBe(false);
  });

  it.skipIf(!HOSTED_ARCHIVES)("moves a pre-existing sibling archive into the durable mesh trace tree", () => {
    const f = fixture("closed", true);
    const legacyRoot = path.join(f.runRoot, ".pi-fabric-session-traces", `run-${path.basename(f.run)}`);
    fs.mkdirSync(legacyRoot, { recursive: true });
    const legacyFile = path.join(legacyRoot, "session.jsonl");
    fs.writeFileSync(legacyFile, "previously archived transcript\n");
    f.sweep();
    const durableLegacy = path.join(f.archive, ".legacy", ".pi-fabric-session-traces", path.basename(legacyRoot), "session.jsonl");
    expect(fs.existsSync(legacyFile)).toBe(false);
    expect(fs.readFileSync(durableLegacy, "utf8")).toBe("previously archived transcript\n");
  });

  it.skipIf(!HOSTED_ARCHIVES)("keeps an archive after its entire TMPDIR-style parent is removed", () => {
    const f = fixture("closed", true);
    f.sweep();
    fs.rmSync(f.tempRoot, { recursive: true, force: true });
    expect(fs.existsSync(f.runRoot)).toBe(false);
    expect(fs.existsSync(f.archive)).toBe(true);
    for (const relative of tracePaths) expect(fs.readFileSync(path.join(f.archive, relative), "utf8")).toBe(`raw transcript: ${relative}\n`);
  });

  it.skipIf(!HOSTED_ARCHIVES)("copies, syncs, and verifies every trace before source removal", () => {
    const f = fixture("closed", true);
    f.sweep();
    expect(fs.existsSync(f.run)).toBe(false);
    const manifest = JSON.parse(fs.readFileSync(path.join(f.archive, ".manifest.json"), "utf8")) as {
      files: Array<{ path: string; size: number; sha256?: string }>;
    };
    expect(manifest.files).toHaveLength(tracePaths.length);
    for (const entry of manifest.files) {
      const bytes = fs.readFileSync(path.join(f.archive, entry.path));
      expect(entry.size).toBe(bytes.length);
      expect(entry.sha256).toBe(createHash("sha256").update(bytes).digest("hex"));
    }
    for (const relative of tracePaths) expect(fs.readFileSync(path.join(f.archive, relative), "utf8")).toBe(`raw transcript: ${relative}\n`);
  });

  it.skipIf(!HOSTED_ARCHIVES)("fsyncs a matching pre-existing staging file before unlinking its source", () => {
    const f = fixture("closed", true);
    const source = path.join(f.run, "events.jsonl");
    const target = path.join(f.archive, "events.jsonl");
    const sourceStat = fs.statSync(source);
    const onlyPaths = new Map([["events.jsonl", sourceStat]]);
    fs.mkdirSync(f.archive, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(f.archive, ".manifest.json"), JSON.stringify({ format: 1, files: [{
      path: "events.jsonl", dev: sourceStat.dev, ino: sourceStat.ino, size: sourceStat.size, mtimeMs: sourceStat.mtimeMs,
    }] }), { mode: 0o600 });
    const stagingDirectory = path.join(f.archive, ".staging");
    const stagingFile = path.join(stagingDirectory, `${createHash("sha256").update("events.jsonl").digest("hex")}.tmp`);
    fs.mkdirSync(stagingDirectory, { mode: 0o700 });
    fs.writeFileSync(stagingFile, fs.readFileSync(source), { mode: 0o600 });
    fs.utimesSync(stagingFile, new Date(sourceStat.mtimeMs), new Date(sourceStat.mtimeMs));

    const realOpen = fs.openSync;
    const realSync = fs.fsyncSync;
    const realUnlink = fs.unlinkSync;
    const stagingDescriptors = new Set<number>();
    const order: string[] = [];
    const open = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = realOpen(file, flags, mode);
      if (path.resolve(String(file)) === path.resolve(stagingFile)) stagingDescriptors.add(fd);
      return fd;
    });
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (stagingDescriptors.has(fd)) order.push("staging-fsync");
      realSync(fd);
    });
    const unlink = vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (path.resolve(String(file)) === path.resolve(source)) order.push("source-unlink");
      return realUnlink(file);
    });
    try {
      expect(archiveRunSessionTraces(f.run, onlyPaths, f.meshRoot)).toBe(true);
      expect(order).toEqual(["staging-fsync", "source-unlink"]);
      expect(fs.existsSync(source)).toBe(false);
    } finally { unlink.mockRestore(); sync.mockRestore(); open.mockRestore(); }
  });

  it.skipIf(!HOSTED_ARCHIVES)("keeps the raw source when an archive hash mismatches", () => {
    const f = fixture("closed", true);
    const source = path.join(f.run, "events.jsonl");
    const target = path.join(f.archive, "events.jsonl");
    const sourceStat = fs.statSync(source);
    const sourceBytes = fs.readFileSync(source);
    const onlyPaths = new Map([["events.jsonl", sourceStat]]);
    const realLink = fs.linkSync;
    const link = vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      realLink(from, to);
      if (String(to) === target) {
        const tampered = fs.readFileSync(target);
        tampered.writeUInt8(tampered.readUInt8(0) ^ 1, 0);
        fs.writeFileSync(target, tampered);
        fs.utimesSync(target, new Date(sourceStat.mtimeMs), new Date(sourceStat.mtimeMs));
      }
    });
    try {
      expect(archiveRunSessionTraces(f.run, onlyPaths, f.meshRoot)).toBe(false);
      expect(fs.readFileSync(source)).toEqual(sourceBytes);
      expect(fs.statSync(source).ino).toBe(sourceStat.ino);
      expect(fs.readFileSync(target)).not.toEqual(sourceBytes);
    } finally { link.mockRestore(); }
  });

  it.skipIf(!HOSTED_ARCHIVES)("does not unlink a source until the destination directory sync succeeds", () => {
    const f = fixture("closed", true);
    const source = path.join(f.run, "events.jsonl");
    const target = path.join(f.archive, "events.jsonl");
    const destination = path.resolve(f.archive);
    const realOpen = fs.openSync;
    const realSync = fs.fsyncSync;
    const directoryDescriptors = new Set<number>();
    let failedSyncs = 2;
    const open = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = realOpen(file, flags, mode);
      if (path.resolve(String(file)) === destination) directoryDescriptors.add(fd);
      else directoryDescriptors.delete(fd);
      return fd;
    });
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (directoryDescriptors.has(fd) && fs.existsSync(target) && fs.existsSync(source) && failedSyncs > 0) {
        failedSyncs--;
        throw Object.assign(new Error("injected destination directory fsync failure"), { code: "EIO" });
      }
      realSync(fd);
    });
    try {
      expect(archiveRunSessionTraces(f.run, undefined, f.meshRoot)).toBe(false);
      expect(fs.existsSync(source)).toBe(true);
      expect(fs.readFileSync(target, "utf8")).toBe("raw transcript: events.jsonl\n");
      expect(archiveRunSessionTraces(f.run, undefined, f.meshRoot)).toBe(false);
      expect(fs.existsSync(source)).toBe(true);
      expect(archiveRunSessionTraces(f.run, undefined, f.meshRoot)).toBe(true);
      expect(fs.existsSync(source)).toBe(false);
      expect(fs.readFileSync(target, "utf8")).toBe("raw transcript: events.jsonl\n");
      expect(failedSyncs).toBe(0);
    } finally { sync.mockRestore(); open.mockRestore(); }
  });

  it("excludes the durable trace tree from deletion and compaction", () => {
    const f = fixture("closed");
    const traceRun = path.join(f.meshRoot, RUN_SESSION_TRACE_ARCHIVE_DIRECTORY, "host", "run-root", "run-trace");
    fs.mkdirSync(traceRun, { recursive: true });
    const events = path.join(traceRun, "events.jsonl");
    fs.writeFileSync(events, `${JSON.stringify({ at: 1, event: "raw" })}\n`);
    fs.writeFileSync(path.join(traceRun, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1 }));
    const judged = fs.lstatSync(traceRun);
    expect(isTraceArchivePath(traceRun, f.meshRoot)).toBe(true);
    expect(removeUnreferencedRun(traceRun, judged, undefined, false, "trace-exclusion", 0, false, f.meshRoot)).toBe(false);
    expect(compactTerminalRunEvents(traceRun, { now: 100 * DAY, terminalRunEventsAgeMs: 1, meshRoot: f.meshRoot })).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: traceRun, meshRoot: f.meshRoot, retentionMs: 1, now: 100 * DAY })).toEqual([]);
    expect(sweepResidentRuns(traceRun, 100 * DAY, 100, { meshRoot: f.meshRoot })).toEqual([]);
    const sessionFile = path.join(traceRun, "session.jsonl");
    fs.writeFileSync(sessionFile, "raw session\n");
    for (const stamp of ["20250101T000000000Z", "20250102T000000000Z"]) {
      fs.writeFileSync(`${sessionFile}.${stamp}.bak`, `backup ${stamp}`);
    }
    expect(pruneActorSessionBackups(sessionFile, { meshRoot: f.meshRoot })).toEqual([]);
    const emptyTraceRoot = path.join(traceRun, "empty-root");
    fs.mkdirSync(emptyTraceRoot);
    fs.writeFileSync(path.join(emptyTraceRoot, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1 }));
    expect(removeEmptyRunRoot(emptyTraceRoot, f.meshRoot)).toBe(false);
    expect(fs.existsSync(emptyTraceRoot)).toBe(true);
    expect(fs.existsSync(events)).toBe(true);
  });

  it.skipIf(!HOSTED_ARCHIVES).each(["closed", "orphaned"] as const)("does not remove a %s run if any trace archive link fails, then retries the partial archive", kind => {
    const f = fixture(kind, true);
    const failedSource = path.join(f.run, "route-session.jsonl");
    const failedTarget = path.join(f.archive, "route-session.jsonl");
    const realLink = fs.linkSync;
    const link = vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(to) === failedTarget) throw Object.assign(new Error("injected archive link failure"), { code: "EIO" });
      realLink(from, to);
    });
    f.sweep();
    expect(fs.existsSync(f.run)).toBe(true);
    expect(fs.readFileSync(failedSource, "utf8")).toBe("raw transcript: route-session.jsonl\n");
    expect(fs.readFileSync(path.join(f.archive, "events.jsonl"), "utf8")).toBe("raw transcript: events.jsonl\n");
    expect(fs.readFileSync(path.join(f.run, "status.json"), "utf8")).toContain('"status":"completed"');
    link.mockRestore();
    f.sweep();
    expect(fs.existsSync(f.run)).toBe(false);
    for (const relative of tracePaths) expect(fs.existsSync(path.join(f.archive, relative))).toBe(true);
  });
});
