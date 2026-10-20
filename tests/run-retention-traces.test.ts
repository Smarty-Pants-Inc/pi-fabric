import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed, RUN_SESSION_TRACE_ARCHIVE_DIRECTORY, sweepTempRunRoots } from "../src/storage/retention.js";

const roots: string[] = [];
const DAY = 24 * 60 * 60 * 1_000;
const tracePaths = [
  "events.jsonl",
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
  roots.push(tempRoot);
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
  const archive = path.join(runRoot, RUN_SESSION_TRACE_ARCHIVE_DIRECTORY, `run-${path.basename(run)}`);
  const sweep = () => sweepTempRunRoots({
    tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1,
  });
  return { tempRoot, runRoot, run, archive, sweep, mtimes };
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
    expect(fs.existsSync(f.runRoot), "the trace archive keeps its run root durable").toBe(true);
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
