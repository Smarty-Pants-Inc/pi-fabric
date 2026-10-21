import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FABRIC_RUN_ROOT_PREFIX, isTraceArchivePath, markRunRootActive, markRunRootClosed, RUN_SESSION_TRACE_ARCHIVE_DIRECTORY,
  runSessionTraceArchivePath, sweepTempRunRoots, removeUnreferencedRun, compactTerminalRunEvents, pruneActorRunArchives,
  pruneActorSessionBackups, removeEmptyRunRoot } from "../src/storage/retention.js";
import { sweepResidentRuns } from "../src/residency/retention.js";

const roots: string[] = [];
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

function fixture(kind: "closed" | "orphaned") {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-trace-retention-"));
  const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-trace-mesh-"));
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
  it.each(["closed", "orphaned"] as const)("archives every raw transcript before removing a %s temp run", kind => {
    const f = fixture(kind);
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

  it("moves a pre-existing sibling archive into the durable mesh trace tree", () => {
    const f = fixture("closed");
    const legacyRoot = path.join(f.runRoot, ".pi-fabric-session-traces", `run-${path.basename(f.run)}`);
    fs.mkdirSync(legacyRoot, { recursive: true });
    const legacyFile = path.join(legacyRoot, "session.jsonl");
    fs.writeFileSync(legacyFile, "previously archived transcript\n");
    f.sweep();
    const durableLegacy = path.join(f.archive, ".legacy", ".pi-fabric-session-traces", path.basename(legacyRoot), "session.jsonl");
    expect(fs.existsSync(legacyFile)).toBe(false);
    expect(fs.readFileSync(durableLegacy, "utf8")).toBe("previously archived transcript\n");
  });

  it("keeps an archive after its entire TMPDIR-style parent is removed", () => {
    const f = fixture("closed");
    f.sweep();
    fs.rmSync(f.tempRoot, { recursive: true, force: true });
    expect(fs.existsSync(f.runRoot)).toBe(false);
    expect(fs.existsSync(f.archive)).toBe(true);
    for (const relative of tracePaths) expect(fs.readFileSync(path.join(f.archive, relative), "utf8")).toBe(`raw transcript: ${relative}\n`);
  });

  it("copies, syncs, and verifies traces when archive rename crosses filesystems", () => {
    const f = fixture("closed");
    const realRename = fs.renameSync;
    const rename = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from).startsWith(f.run + path.sep) && String(to).startsWith(f.archive + path.sep)) {
        throw Object.assign(new Error("injected cross-device rename"), { code: "EXDEV" });
      }
      return realRename(from, to);
    });
    f.sweep();
    rename.mockRestore();
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

  it.each(["closed", "orphaned"] as const)("does not remove a %s run if any trace rename fails, then retries the partial archive", kind => {
    const f = fixture(kind);
    const failedSource = path.join(f.run, "route-session.jsonl");
    const realRename = fs.renameSync;
    const rename = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === failedSource) throw Object.assign(new Error("injected archive failure"), { code: "EIO" });
      return realRename(from, to);
    });
    f.sweep();
    expect(fs.existsSync(f.run)).toBe(true);
    expect(fs.readFileSync(failedSource, "utf8")).toBe("raw transcript: route-session.jsonl\n");
    expect(fs.readFileSync(path.join(f.archive, "events.jsonl"), "utf8")).toBe("raw transcript: events.jsonl\n");
    expect(fs.readFileSync(path.join(f.run, "status.json"), "utf8")).toContain('"status":"completed"');
    rename.mockRestore();
    f.sweep();
    expect(fs.existsSync(f.run)).toBe(false);
    for (const relative of tracePaths) expect(fs.existsSync(path.join(f.archive, relative))).toBe(true);
  });
});
