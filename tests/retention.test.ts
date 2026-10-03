import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  FABRIC_RUN_ROOT_PREFIX,
  markRunRootActive,
  markRunRootClosed,
  hasUnresolvedWorker,
  runTreeExitVeto,
  markUnresolvedWorker,
  pruneActorRunArchives,
  RUN_ROOT_SWEEP_MARKER,
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

describe("shared run-tree exit veto", () => {
  it("refuses malformed records and failed nested inspection rather than inferring exit", () => {
    const run = temporaryDirectory();
    const status = path.join(run, "status.json"); fs.writeFileSync(status, "{broken");
    expect(runTreeExitVeto(run)).toMatch(/exit is unconfirmed/);
    fs.writeFileSync(status, JSON.stringify({ status: "completed", transport: "process" }));
    const nested = path.join(run, "nested"); fs.mkdirSync(nested);
    const readdir = fs.readdirSync;
    const fault = vi.spyOn(fs, "readdirSync").mockImplementation((...args: Parameters<typeof readdir>) => {
      if (String(args[0]) === nested) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return readdir(...args);
    });
    try { expect(runTreeExitVeto(run)).toMatch(/inspection failed/); }
    finally { fault.mockRestore(); }
    expect(runTreeExitVeto(run)).toBeUndefined();
  });

  it("ownership retention requires checked exit identities for every descendant, including terminal live writers", () => {
    const run = temporaryDirectory();
    const child = path.join(run, "nested", "child");
    const grandchild = path.join(child, "nested", "grandchild");
    const veto = () => runTreeExitVeto(run, 0, undefined, true);
    writeStatus(child, { status: "completed", transport: "process", sessionId: String(process.pid) });
    expect(veto()).toMatch(/descendant worker may still be running/);
    for (const sessionId of [undefined, "", "0", "-1", "not-a-pid", "9007199254740993"]) {
      writeStatus(child, { status: "completed", transport: "process", sessionId });
      expect(veto()).toMatch(/unknown descendant identity/);
    }
    writeStatus(child, { status: "running", transport: "process", sessionId: "2147483647" });
    // Real process exit proves a crashed worker gone even with a stale running record.
    expect(veto()).toBeUndefined();
    writeStatus(grandchild, { status: "completed", transport: "process", sessionId: String(process.pid) });
    expect(veto()).toMatch(/descendant worker may still be running/);
    writeStatus(grandchild, { status: "completed", transport: "process", sessionId: "2147483647" });
    expect(veto()).toBeUndefined();
    fs.rmSync(path.join(grandchild, "status.json"));
    expect(veto()).toMatch(/unknown descendant identity/);
    expect(runTreeExitVeto(path.join(run, "gone"), 1, undefined, true)).toMatch(/inspection failed/);
    expect(runTreeExitVeto(path.join(run, "gone"), 0, undefined, true)).toMatch(/inspection failed/);
    expect(runTreeExitVeto(run, 0, () => true, true)).toMatch(/incomplete/);
  });

  it("refuses depth/deadline truncation and nested symlinks", () => {
    const run = temporaryDirectory();
    expect(runTreeExitVeto(run, 33)).toMatch(/incomplete/);
    expect(runTreeExitVeto(run, 0, () => true)).toMatch(/incomplete/);
    const target = temporaryDirectory(); fs.symlinkSync(target, path.join(run, "nested"), "junction");
    expect(runTreeExitVeto(run)).toMatch(/unsafe nested/);
    expect(fs.existsSync(target)).toBe(true);
  });
});
describe("safe run roots", () => {
  const sweep = (tempRoot: string, now = 100 * DAY) => sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });

  it.each(["closed", "orphan"])("R3 collects owned route sessions in expired %s roots without weakening fences", kind => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + kind);
    for (const name of ["done", "pending", "live", "unresolved"]) {
      const directory = path.join(root, name);
      writeStatus(directory, { status: "completed", finishedAt: 1, transport: "process", ...(name === "live" ? { sessionId: String(process.pid) } : {}) });
      fs.writeFileSync(path.join(directory, "route-session.jsonl"), '{"type":"session"}\n');
      if (name === "pending") fs.writeFileSync(path.join(directory, "pending-route-outcome.json"), "{}");
      if (name === "unresolved") markUnresolvedWorker(directory, "not joined");
    }
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, ...(kind === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }));
    const result = sweep(tempRoot);
    if (kind === "closed") expect(result.removedRuns).toContain(path.join(root, "done"));
    expect(fs.existsSync(path.join(root, "done"))).toBe(false);
    for (const name of ["pending", "live", "unresolved"]) expect(fs.existsSync(path.join(root, name))).toBe(true);
  });
  it.each(["closed", "orphan"])("retains uncontained scratch in expired %s runs, even when worker/nested records say exited", kind => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + kind);
    for (const name of ["done", "live", "unsettled", "nested-live", "unknown", "nested-unknown", "unsafe"]) {
      const directory = path.join(root, name);
      writeStatus(directory, { status: "completed", finishedAt: 1, transport: "process", sessionId: name === "live" ? String(process.pid) : "2147483647" });
      const tmp = path.join(directory, "tmp");
      fs.mkdirSync(path.join(tmp, "compound-suffix"), { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(tmp, "compound-suffix", "anything.tmp"), "leftover");
      if (name === "unsettled") markUnresolvedWorker(directory, "exit unsettled");
      if (name === "nested-live") writeStatus(path.join(directory, "nested", "child"), {
        status: "completed", transport: "process", sessionId: String(process.pid), finishedAt: 1,
      });
      if (name === "unknown") writeStatus(directory, { status: "completed", transport: "process", finishedAt: 1 });
      if (name === "nested-unknown") writeStatus(path.join(directory, "nested", "child"), { status: "completed", transport: "process", finishedAt: 1 });
      if (name === "unsafe") fs.symlinkSync(temporaryDirectory(), path.join(tmp, "linked"), "junction");
    }
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1,
      ...(kind === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }));
    sweep(tempRoot);
    for (const name of ["done", "live", "unsettled", "nested-live", "unknown", "nested-unknown", "unsafe"]) {
      expect(fs.existsSync(path.join(root, name, "tmp", "compound-suffix", "anything.tmp"))).toBe(true);
    }
  });

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

  it("sweeps a host at most once per interval, and again once the marker is stale (smarty-dev#2010)", () => {
    const tempRoot = temporaryDirectory();
    const deadRoot = (name: string): string => {
      const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + name);
      fs.mkdirSync(runRoot);
      fs.writeFileSync(
        path.join(runRoot, ".fabric-owner.json"),
        JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }),
      );
      return runRoot;
    };
    const sweepAt = (now: number) => sweepTempRunRoots({
      tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY, minIntervalMs: HOUR,
    });
    const first = deadRoot("first");
    expect(sweepAt(DAY).removedRoots).toEqual([first]);
    const second = deadRoot("second");
    // Another process swept within the hour: this one skips the walk.
    expect(sweepAt(DAY + HOUR - 1).removedRoots).toEqual([]);
    expect(fs.existsSync(second)).toBe(true);
    expect(sweepAt(DAY + HOUR).removedRoots).toEqual([second]);
    // A marker from the future (clock skew, tampering) does not suppress collection.
    fs.writeFileSync(path.join(tempRoot, RUN_ROOT_SWEEP_MARKER), JSON.stringify({ sweptAt: 10 * DAY }));
    const third = deadRoot("third");
    expect(sweepAt(2 * DAY).removedRoots).toEqual([third]);
  });

  it("collects expired actor runs with worker reply and overflow files, still vetoed by unknown files (smarty-dev#2010)", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "actor");
    markRunRootActive(runRoot, 1);
    const actorRun = path.join(runRoot, "actor-run");
    writeStatus(actorRun, { status: "completed", actorId: "actor-1", finishedAt: DAY });
    for (const name of ["task.txt", "events.jsonl", "reply.json", "relaunches.jsonl", "oversized-event-prefix.txt", "oversized-event-prefix-2.txt"]) {
      fs.writeFileSync(path.join(actorRun, name), "x");
    }
    const unknown = path.join(runRoot, "unknown");
    writeStatus(unknown, { status: "completed", actorId: "actor-1", finishedAt: DAY });
    fs.writeFileSync(path.join(unknown, "reply.json.bak"), "not ours");
    markRunRootClosed(runRoot, DAY + 1);
    const result = sweepTempRunRoots({
      tempRoot, now: 2 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
    });
    expect(result.removedRuns).toEqual([actorRun]);
    expect(fs.existsSync(unknown)).toBe(true);
  });

  it("skips a closed root younger than the shortest retention, and stops at the budget", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "young");
    markRunRootActive(runRoot, DAY);
    const run = path.join(runRoot, "run");
    writeStatus(run, { status: "completed", finishedAt: DAY });
    markRunRootClosed(runRoot, DAY + 1);
    const read = vi.spyOn(fs, "readdirSync");
    const sweepAt = (now: number, budgetMs?: number) => sweepTempRunRoots({
      tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      ...(budgetMs !== undefined ? { budgetMs } : {}),
    });
    expect(sweepAt(DAY + 6 * HOUR - 1).removedRuns).toEqual([]);
    // Only the temp root and the empty-root check were listed: the young root's runs were not walked.
    expect(read.mock.calls.filter(([dir]) => String(dir) === runRoot).length).toBeLessThanOrEqual(1);
    read.mockRestore();
    // A spent budget visits nothing; an unbounded sweep collects the run once it is due.
    expect(sweepAt(3 * DAY, 0).removedRuns).toEqual([]);
    expect(fs.existsSync(run)).toBe(true);
    expect(sweepAt(3 * DAY).removedRuns).toEqual([run]);
  });

  describe("a deadline crossed mid-walk (smarty-dev#2010 review)", () => {
    const budgetSweep = (tempRoot: string, budgetMs?: number) => sweepTempRunRoots({
      tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      ...(budgetMs !== undefined ? { budgetMs } : {}),
    });
    const completedRun = (directory: string): void => {
      writeStatus(directory, { status: "completed", finishedAt: 1 });
      fs.writeFileSync(path.join(directory, "task.txt"), "work");
    };

    it("stops removing an expired orphan root's runs at the budget and keeps the root", () => {
      const tempRoot = temporaryDirectory();
      const orphan = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphan");
      for (let index = 0; index < 40; index++) completedRun(path.join(orphan, `run-${index}`));
      fs.writeFileSync(path.join(orphan, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
      let removed = 0;
      const rm = fs.rmSync;
      vi.spyOn(fs, "rmSync").mockImplementation((...args: Parameters<typeof fs.rmSync>) => { removed++; return rm(...args); });
      // The clock passes the budget right after the fifth run is removed.
      vi.spyOn(performance, "now").mockImplementation(() => (removed >= 5 ? 10_000 : 0));
      expect(budgetSweep(tempRoot, 1_000)).toEqual({ removedRoots: [], removedRuns: [] });
      expect(removed).toBe(5);
      expect(fs.readdirSync(orphan).filter((name) => name.startsWith("run-"))).toHaveLength(35);
      vi.restoreAllMocks();
      expect(budgetSweep(tempRoot).removedRoots).toEqual([orphan]);
    });

    it("abandons a nested-run walk at the budget without removing the run", () => {
      const tempRoot = temporaryDirectory();
      const closed = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
      const parent = path.join(closed, "parent");
      let deepest = parent;
      completedRun(parent);
      for (let depth = 0; depth < 20; depth++) {
        deepest = path.join(deepest, "nested", `child-${depth}`);
        completedRun(deepest);
        // A stopped host alone cannot prove descendant exit; supply the
        // fixture identity so this test reaches its intended deadline.
        writeStatus(deepest, { status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1 });
      }
      fs.writeFileSync(path.join(closed, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
      let deepVisits = 0;
      const exists = fs.existsSync;
      vi.spyOn(fs, "existsSync").mockImplementation((file) => {
        // Native separators: count "nested" path components below the parent run.
        const relative = path.relative(parent, String(file));
        if (!relative.startsWith("..") && relative.split(path.sep).filter((part) => part === "nested").length >= 10) deepVisits++;
        return exists(file);
      });
      // The clock passes the budget once the walk reaches nesting depth 10.
      vi.spyOn(performance, "now").mockImplementation(() => (deepVisits > 0 ? 10_000 : 0));
      expect(budgetSweep(tempRoot, 1_000)).toEqual({ removedRoots: [], removedRuns: [] });
      // The walk stopped at the crossing: it never went deeper, and the run was kept.
      expect(deepVisits).toBe(1);
      expect(fs.existsSync(deepest)).toBe(true);
      vi.restoreAllMocks();
      expect(budgetSweep(tempRoot).removedRuns).toEqual([parent]);
    });
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
