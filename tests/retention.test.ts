import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  FABRIC_RUN_ROOT_PREFIX,
  markRunRootActive,
  markRunRootClosed,
  hasUnresolvedWorker,
  markUnresolvedWorker,
  pruneActorRunArchives,
  sweepTempRunRoots,
} from "../src/storage/retention.js";

const roots: string[] = [];
const HOUR = 60 * 60 * 1_000;
const DAY = 24 * HOUR;

const temporaryDirectory = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-retention-test-"));
  roots.push(root);
  return root;
};

const writeStatus = (
  directory: string,
  record: Record<string, unknown>,
): void => {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify(record));
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("safe run roots", () => {
  const sweep = (tempRoot: string, now = 100 * DAY) => sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });

  it("preserves malformed/unmarked ownership and unknown root contents", () => {
    const tempRoot = temporaryDirectory();
    for (const [suffix, owner] of [["bad", {}], ["pid", { pid: "gone", startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }], ["time", { pid: 2147483647, startedAt: 1, heartbeatAt: "old", orphanedAt: 1 }]] as const) {
      const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + suffix);
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify(owner));
    }
    const unknown = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "unknown");
    markRunRootActive(unknown, 1);
    fs.writeFileSync(path.join(unknown, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    fs.writeFileSync(path.join(unknown, "mine"), "do not delete");
    const unmarked = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "unmarked");
    fs.mkdirSync(unmarked);
    expect(sweep(tempRoot).removedRoots).toEqual([]);
    expect(fs.readdirSync(tempRoot)).toHaveLength(5);
  });

  it("rejects symlink roots and status markers without touching targets", () => {
    const tempRoot = temporaryDirectory();
    const target = temporaryDirectory();
    markRunRootActive(target, 1);
    const run = path.join(target, "run");
    writeStatus(run, { status: "completed", finishedAt: 1 });
    markRunRootClosed(target, 1);
    fs.symlinkSync(target, path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "link"), "junction");
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "status-link");
    markRunRootActive(root, 1);
    fs.mkdirSync(path.join(root, "run"));
    fs.symlinkSync(path.join(run, "status.json"), path.join(root, "run", "status.json"));
    markRunRootClosed(root, 1, true);
    expect(sweep(tempRoot).removedRuns).toEqual([]);
    expect(fs.existsSync(run)).toBe(true);
  });

  it("expires shutdown-confirmed incomplete runs, but never a live descendant", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed-incomplete");
    markRunRootActive(root, 1);
    const incomplete = path.join(root, "incomplete");
    fs.mkdirSync(incomplete);
    fs.writeFileSync(path.join(incomplete, "task.txt"), "incomplete launch");
    const active = path.join(root, "active");
    writeStatus(active, { status: "running", transport: "process", sessionId: String(process.pid) });
    fs.writeFileSync(path.join(active, "task.txt"), "still live");
    markRunRootClosed(root, 1, true);
    expect(sweep(tempRoot, 5 * HOUR).removedRuns).toEqual([]);
    expect(sweep(tempRoot, 6 * HOUR + 1).removedRuns).toEqual([incomplete]);
    expect(fs.existsSync(active)).toBe(true);
  });

  it("keeps unknown incomplete runs and live nested work under dead owners", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "dead-nested");
    markRunRootActive(root, 1);
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    const run = path.join(root, "outer");
    writeStatus(run, { status: "completed", finishedAt: 1 });
    const nested = path.join(run, "nested", "live");
    writeStatus(nested, { status: "running", transport: "process", sessionId: String(process.pid) });
    expect(sweep(tempRoot).removedRoots).toEqual([]);
    expect(fs.existsSync(nested)).toBe(true);
  });
});

describe("temporal retention", () => {
  // review/astra on 3257dba, D1: a lost worker's run survives its manager, in both sweeps.
  it("never removes a run marked with an unresolved worker, after its owner is gone", () => {
    const tempRoot = temporaryDirectory();
    const orphaned = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphaned");
    const closed = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
    for (const runRoot of [orphaned, closed]) {
      const run = path.join(runRoot, "lost");
      writeStatus(run, { status: "failed", transport: "herdr", sessionId: "pane-1", finishedAt: 1, updatedAt: 1 });
      fs.writeFileSync(path.join(run, "task.txt"), "work");
      markUnresolvedWorker(run, "the Herdr server has been unreachable for 300 s");
    }
    fs.writeFileSync(path.join(orphaned, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    fs.writeFileSync(path.join(closed, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
    const sweep = (at: string) => sweepTempRunRoots({ tempRoot: at, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });
    const result = sweep(tempRoot);
    expect(result).toEqual({ removedRoots: [], removedRuns: [] });
    expect(fs.existsSync(path.join(orphaned, "lost"))).toBe(true);
    expect(fs.existsSync(path.join(closed, "lost"))).toBe(true);
    // Without the marker the same runs are swept.
    for (const runRoot of [orphaned, closed]) fs.rmSync(path.join(runRoot, "lost", "unresolved-worker.json"));
    const unmarked = sweep(tempRoot);
    expect(unmarked.removedRoots).toContain(orphaned);         // the closed root goes too once empty
    expect(unmarked.removedRuns).toEqual([path.join(closed, "lost")]);
  });

  it("never removes a completed parent run whose nested child is marked unresolved", () => {
    const tempRoot = temporaryDirectory();
    const orphaned = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphaned-parent");
    const closed = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed-parent");
    for (const runRoot of [orphaned, closed]) {
      const parent = path.join(runRoot, "parent");
      writeStatus(parent, { status: "completed", transport: "process", finishedAt: 1, updatedAt: 1 });
      fs.writeFileSync(path.join(parent, "task.txt"), "work");
      const child = path.join(parent, "nested", "child");
      writeStatus(child, { status: "failed", transport: "herdr", sessionId: "pane-1", finishedAt: 1, updatedAt: 1 });
      fs.writeFileSync(path.join(child, "task.txt"), "work");
      markUnresolvedWorker(child, "the Herdr server has been unreachable for 300 s");
    }
    fs.writeFileSync(path.join(orphaned, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    fs.writeFileSync(path.join(closed, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
    const result = sweepTempRunRoots({ tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });
    expect(result).toEqual({ removedRoots: [], removedRuns: [] });
    expect(hasUnresolvedWorker(path.join(orphaned, "parent"))).toBe(true);
  });

  it("removes dead temporary run roots after six hours", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "dead");
    fs.mkdirSync(runRoot);
    fs.writeFileSync(
      path.join(runRoot, ".fabric-owner.json"),
      JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1 }),
    );

    const detected = sweepTempRunRoots({
      tempRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 2,
    });
    expect(detected.removedRoots).toEqual([]);

    const result = sweepTempRunRoots({
      tempRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 6 * HOUR + 2,
    });

    expect(result.removedRoots).toEqual([runRoot]);
    expect(fs.existsSync(runRoot)).toBe(false);
  });

  it("keeps live roots and the current root out of orphan cleanup", () => {
    const tempRoot = temporaryDirectory();
    const liveRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "live");
    markRunRootActive(liveRoot, 1);

    const result = sweepTempRunRoots({
      tempRoot,
      currentRoot: liveRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 30 * DAY,
    });

    expect(result.removedRoots).toEqual([]);
    expect(fs.existsSync(liveRoot)).toBe(true);
  });

  it("expires terminal one-shot runs from gracefully retained roots after 24 hours", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
    markRunRootActive(runRoot, 1);
    const expired = path.join(runRoot, "expired");
    const fresh = path.join(runRoot, "fresh");
    const actorTemp = path.join(runRoot, "actor-temp");
    writeStatus(expired, { status: "completed", finishedAt: DAY });
    writeStatus(fresh, { status: "completed", finishedAt: 2 * DAY });
    writeStatus(actorTemp, { status: "failed", actorId: "actor-1", finishedAt: DAY });
    markRunRootClosed(runRoot, 2 * DAY);

    const result = sweepTempRunRoots({
      tempRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 2 * DAY + 1,
    });

    expect(result.removedRuns.sort()).toEqual([actorTemp, expired].sort());
    expect(fs.existsSync(expired)).toBe(false);
    expect(fs.existsSync(actorTemp)).toBe(false);
    expect(fs.existsSync(fresh)).toBe(true);
  });

  it("expires actor archives after seven days while preserving the latest run", () => {
    const root = temporaryDirectory();
    const runsDirectory = path.join(root, "runs");
    const expired = path.join(runsDirectory, "expired");
    const latest = path.join(runsDirectory, "latest");
    const fresh = path.join(runsDirectory, "fresh");
    writeStatus(expired, { status: "completed", finishedAt: DAY });
    writeStatus(latest, { status: "completed", finishedAt: DAY });
    writeStatus(fresh, { status: "completed", finishedAt: 8 * DAY });

    const removed = pruneActorRunArchives({
      runsDirectory,
      latestRunId: "latest",
      retentionMs: 7 * DAY,
      now: 8 * DAY + 1,
    });

    expect(removed).toEqual([expired]);
    expect(fs.existsSync(expired)).toBe(false);
    expect(fs.existsSync(latest)).toBe(true);
    expect(fs.existsSync(fresh)).toBe(true);
  });
});
