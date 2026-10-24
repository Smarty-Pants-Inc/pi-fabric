import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  captureLockStatsWriter, createLockStats, LOCK_STATS_MAX_FILE_BYTES, LOCK_STATS_WRITER_MAX_CHARS, LOCK_STATS_MAX_FILES, LOCK_STATS_RETAIN_MINUTES, lockEventProcessName, lockStatsHost, readLockStats, summarizeLockStats,
  type LockStatsBucket, type LockStatsFile,
} from "../src/mesh/commit-stats.js";
import { main } from "../src/mesh-lock-stats-cli.js";
import { taskAgentEnvironment } from "../src/agents/task-environment.js";
import { hostedAdmissionMarker } from "../scripts/hosted-only.mjs";

const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const registry = globalThis as typeof globalThis & {
  [lockKey]?: { version: number; flush(): void; dispose(): void; stats: unknown };
};
const temps: string[] = [];
const temp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-stats-"));
  temps.push(root);
  return root;
};
const T0 = Date.UTC(2026, 9, 7, 12, 0, 10);
const MINUTE0 = Math.floor(T0 / 60_000);
const ownFile = (root: string): string => path.join(root, "lock-stats", `${lockStatsHost()}-${process.pid}.json`);
const read = (root: string): LockStatsFile => JSON.parse(fs.readFileSync(ownFile(root), "utf8")) as LockStatsFile;
const bucket = (patch: Partial<LockStatsBucket>): LockStatsBucket => ({
  n: 0, waitMs: 0, waitMaxMs: 0, holdMs: 0, holdMaxMs: 0, timeouts: 0, tries: 0, failedWaitMs: 0,
  waitHist: new Array(14).fill(0), holdHist: new Array(14).fill(0), ...patch,
});
const fixture = (root: string, host: string, pid: number, minutes: LockStatsFile["minutes"]): void => {
  fs.mkdirSync(path.join(root, "lock-stats"), { recursive: true });
  const file: LockStatsFile = { version: 1, host, pid, root, startedAt: T0, updatedAt: T0, minutes };
  fs.writeFileSync(path.join(root, "lock-stats", `${host}-${pid}.json`), JSON.stringify(file));
};

// The test environment disables the recorder (PI_FABRIC_LOCK_STATS=0); each test opts in.
beforeEach(() => {
  registry[lockKey]?.dispose();
  delete registry[lockKey];
});
afterEach(() => {
  registry[lockKey]?.dispose();
  delete registry[lockKey];
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  for (const root of temps.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mesh lock stats recorder", () => {
  it("records real SQLite writer holds and busy give-ups in the shared minute stats", async () => {
    // Decision C: #6477's SQLite holds were missing from L8; force contention cheaply here,
    // while the installed Ryzen 1 check compares minute holdMax with the same-minute event log.
    vi.stubEnv("PI_FABRIC_LOCK_STATS", "1");
    vi.resetModules();
    const { SqliteStateStore, openNodeSqlite } = await import("../src/mesh/state-sqlite.js");
    const { MeshLockTimeoutError } = await import("../src/core/atomic-write.js");
    const root = temp();
    const state = await SqliteStateStore.open(root, 64 * 1024, 100, { lockTimeoutMs: 20 });
    const holder = openNodeSqlite(path.join(root, "state.db"));
    const identity = { id: "sqlite-writer", name: "sqlite-writer", kind: "agent" } as const;
    const recorded = () => {
      registry[lockKey]!.flush();
      const summary = summarizeLockStats(root, readLockStats(root), { minutes: 3, now: Date.now() + 120_000 });
      return Object.fromEntries(summary.classes.map(row => [row.lockClass, row]));
    };
    try {
      // Nested try scopes are budgets, not extra acquisitions. The afterCommit custody,
      // however, really reacquires SQLite's writer lock and must count separately.
      await state.withTryLock(() => state.withTryLock(() => state.writeBatch({
        identity, lockClass: "bridge", ops: [{ kind: "put", key: "bridge/value", value: 1 }],
        afterCommit: view => { expect(view.get("bridge/value")?.value).toBe(1); },
      }), 0), 0);
      expect(state.get("bridge/value")?.value).toBe(1);
      await state.writeBatch({ identity, lockClass: "bridge", ops: [],
        prepare: () => [{ kind: "put", key: "bridge/timed", value: 1 }], afterCommit: () => undefined,
        lockTiming: { operation: "bridge-presence-mirror", preparePhase: "mirror-revalidate", commitPhase: "mirror-commit" } });
      const bridge = recorded().bridge!;
      expect(bridge).toMatchObject({ n: 4, timeouts: 0, tries: 0 });
      const timings = read(root).minutes.flatMap(minute => [
        minute.operations?.["bridge-presence-mirror"],
        minute.phases?.["bridge-presence-mirror"]?.["mirror-revalidate"],
        minute.phases?.["bridge-presence-mirror"]?.["mirror-commit"],
        minute.phases?.["bridge-presence-mirror"]?.["after-commit"],
      ]);
      expect(timings[0]).toMatchObject({ n: 2, holdMs: expect.any(Number) });
      expect(timings[1]).toMatchObject({ n: 1, holdMs: expect.any(Number) });
      expect(timings[2]).toMatchObject({ n: 1, holdMs: expect.any(Number) });
      expect(timings[3]).toMatchObject({ n: 1, holdMs: expect.any(Number) });
      expect(bridge.holdMs).toBeGreaterThan(0);
      expect(bridge.holdMaxMs).toBeGreaterThan(0);

      holder.exec("BEGIN IMMEDIATE");
      const write = () => state.writeBatch({
        identity, lockClass: "bridge", ops: [{ kind: "put" as const, key: "bridge/value", value: 2 }],
      });
      await expect(write()).rejects.toBeInstanceOf(MeshLockTimeoutError);
      await expect(state.withTryLock(() => state.withTryLock(write, 0), 0)).rejects.toBeInstanceOf(MeshLockTimeoutError);
      await expect(state.exclusive(() => undefined, 0)).rejects.toBeInstanceOf(MeshLockTimeoutError);
      const failed = recorded();
      expect(failed.bridge).toMatchObject({ n: 4, timeouts: 1, tries: 1 });
      expect(read(root).minutes.reduce((total, minute) => total + (minute.classes.bridge?.failedWaitMs ?? 0), 0)).toBeGreaterThan(0);
      expect(failed.custody).toMatchObject({ n: 0, timeouts: 0, tries: 1 });
      expect(state.get("bridge/value")?.value).toBe(1);
    } finally {
      try { if (holder.isTransaction) holder.exec("ROLLBACK"); }
      finally { holder.close(); state.close(); }
    }
  });

  it("records zero-wait SQLite fence refusals by operation and phase with the validated holder identity", async () => {
    vi.stubEnv("PI_FABRIC_LOCK_STATS", "1");
    vi.resetModules();
    const { SqliteStateStore, openNodeSqlite } = await import("../src/mesh/state-sqlite.js");
    const { MeshLockTimeoutError } = await import("../src/core/atomic-write.js");
    const { walWriterPid } = await import("../src/mesh/wal-readers.js");
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    const prototype = sqlite.DatabaseSync.prototype as unknown as { exec(this: object, sql: string): void };
    const originalExec = prototype.exec;
    const root = temp();
    const state = await SqliteStateStore.open(root, 64 * 1024, 100, { lockTimeoutMs: 20 });
    const holder = openNodeSqlite(path.join(root, "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    let beginAttempts = 0;
    prototype.exec = function (this: object, sql: string): void {
      if (sql.trim().toUpperCase() === "BEGIN IMMEDIATE") beginAttempts++;
      return originalExec.call(this, sql);
    };
    const timing = { operation: "bridge-notification-fence", phase: "notification-fence" };
    const called = vi.fn();
    try {
      const holderPid = walWriterPid(path.join(root, "state.db"));
      expect(() => state.fenceSync(() => called(), timing)).toThrow(MeshLockTimeoutError);
      expect(beginAttempts).toBe(1);
      expect(called).not.toHaveBeenCalled();
      registry[lockKey]!.flush();
      const minute = read(root).minutes.at(-1)!;
      const comm = holderPid === undefined ? null : lockEventProcessName(holderPid) ?? null;
      const refusal = { fenceRefusals: 1, fenceRefusalHolders: [{ pid: holderPid ?? null, comm, n: 1 }] };
      expect(minute.operations?.[timing.operation]).toMatchObject(refusal);
      expect(minute.phases?.[timing.operation]?.[timing.phase]).toMatchObject(refusal);
      expect(minute.classes.bridge).toMatchObject({ tries: 1 });
      expect(summarizeLockStats(root, readLockStats(root), { minutes: 3, now: Date.now() + 120_000 }).phases)
        .toMatchObject([{ operation: timing.operation, phase: timing.phase, fenceRefusals: 1,
          fenceRefusalHolders: [{ pid: holderPid ?? null, comm, n: 1 }] }]);
    } finally {
      prototype.exec = originalExec;
      try { if (holder.isTransaction) holder.exec("ROLLBACK"); }
      finally { holder.close(); state.close(); }
    }
  });

  it("aggregates wait and hold per class and flushes after the minute only on real work", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const root = temp();
    const exitListeners = process.listenerCount("exit");
    const stats = createLockStats("1")!;
    // Nothing is scheduled before the first acquisition.
    expect(vi.getTimerCount()).toBe(0);
    expect(process.listenerCount("exit")).toBe(exitListeners);
    stats.acquired(root, "publish", 2, 30);
    stats.acquired(root, "publish", 0.5, 12);
    stats.acquired(root, "heartbeat/confirm", 50, 0.2);
    stats.failed(root, "custody", 10_000, false);
    stats.failed(root, "custody", 50, true);
    // Even dirty startup samples have no scheduled flush: a minute boundary is not work.
    expect(vi.getTimerCount()).toBe(0);
    expect(process.listenerCount("exit")).toBe(exitListeners + 1);
    vi.advanceTimersByTime(49_000);
    expect(fs.existsSync(ownFile(root))).toBe(false);
    vi.advanceTimersByTime(14_000); // past the minute, still no idle diagnostic wake
    expect(fs.existsSync(ownFile(root))).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
    registry[lockKey]!.flush(); // An explicit diagnostic request publishes the samples.
    const file = read(root);
    expect(file).toMatchObject({ version: 1, host: lockStatsHost(), pid: process.pid, root: fs.realpathSync.native(root) });
    expect(file.minutes).toHaveLength(1);
    expect(file.minutes[0]!.minute).toBe(MINUTE0);
    const { publish, custody } = file.minutes[0]!.classes;
    expect(publish).toMatchObject({ n: 2, waitMs: 2.5, waitMaxMs: 2, holdMs: 42, holdMaxMs: 30, timeouts: 0, tries: 0 });
    // Bounds 1,2,5,10,20,50,...: wait 0.5 -> [0], wait 2 -> [1]; hold 12 -> [4], hold 30 -> [5].
    expect(publish!.waitHist.slice(0, 3)).toEqual([1, 1, 0]);
    expect(publish!.holdHist.slice(3, 7)).toEqual([0, 1, 1, 0]);
    expect(custody).toMatchObject({ n: 0, timeouts: 1, tries: 1, failedWaitMs: 10_050 });
    expect(file.minutes[0]!.classes["heartbeat/confirm"]).toMatchObject({ n: 1, holdMs: 0.2, waitMs: 50 });
    // An unchanged recorder does not rewrite; the next minute's data does.
    const written = fs.statSync(ownFile(root)).mtimeMs;
    vi.advanceTimersByTime(60_000);
    expect(fs.statSync(ownFile(root)).mtimeMs).toBe(written);
    stats.acquired(root, "put/delete", 1, 3);
    vi.advanceTimersByTime(60_000);
    expect(read(root).minutes.map(minute => minute.minute)).toEqual([MINUTE0, MINUTE0 + 2]);
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 3, now: (MINUTE0 + 3) * 60_000 });
    expect(summary).toMatchObject({ n: 4, timeouts: 1, tries: 1, processes: 1 });
    expect(summary.busyPct).toBeCloseTo((42 + 0.2 + 3) / 180_000 * 100, 6);
    registry[lockKey]!.dispose();
    expect(vi.getTimerCount()).toBe(0);
    expect(process.listenerCount("exit")).toBe(exitListeners);
  });

  it("aggregates held time by operation and phase in the minute file and summary", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const root = temp();
    const stats = createLockStats("1")!;
    stats.acquired(root, "bridge", 2, 20, { operation: "bridge-presence-mirror" });
    stats.phase(root, "bridge", "bridge-presence-mirror", "mirror-revalidate", 5);
    stats.phase(root, "bridge", "bridge-presence-mirror", "mirror-commit", 15);
    registry[lockKey]!.flush();
    const minute = read(root).minutes[0]!;
    expect(minute.operations?.["bridge-presence-mirror"]).toMatchObject({ n: 1, holdMs: 20, holdMaxMs: 20 });
    expect(minute.phases?.["bridge-presence-mirror"]?.["mirror-revalidate"]).toMatchObject({ n: 1, holdMs: 5 });
    expect(minute.phases?.["bridge-presence-mirror"]?.["mirror-commit"]).toMatchObject({ n: 1, holdMs: 15 });
    const summary = summarizeLockStats(root, readLockStats(root, [], { minutes: 5, now: (MINUTE0 + 4) * 60_000 }),
      { minutes: 5, now: (MINUTE0 + 4) * 60_000 });
    expect(summary.operations).toEqual([{ operation: "bridge-presence-mirror", n: 1, totalHeldMs: 20, p50HoldMs: 20, p95HoldMs: 20, maxHoldMs: 20 }]);
    expect(summary.phases).toMatchObject([
      { operation: "bridge-presence-mirror", phase: "mirror-commit", n: 1, totalHeldMs: 15, p50HoldMs: 20, p95HoldMs: 20, maxHoldMs: 15 },
      { operation: "bridge-presence-mirror", phase: "mirror-revalidate", n: 1, totalHeldMs: 5, p50HoldMs: 5, p95HoldMs: 5, maxHoldMs: 5 },
    ]);
    const problems: string[] = [];
    expect(readLockStats(root, problems, { minutes: 3, now: (MINUTE0 + 4) * 60_000 })).toHaveLength(1);
    expect(problems).toEqual([]);
  });

  it("stays bounded: one hour of minutes and at most 32 roots per process", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const parent = temp();
    const stats = createLockStats("1")!;
    const roots = Array.from({ length: 40 }, (_, index) => path.join(parent, `root-${index}`));
    for (const root of roots) fs.mkdirSync(root);
    for (let minute = 0; minute < 90; minute++) {
      stats.acquired(roots[0]!, "writeBatch", 1, 1);
      vi.advanceTimersByTime(60_000);
    }
    for (const root of roots) stats.acquired(root, "other", 1, 1);
    registry[lockKey]!.flush();
    const minutes = read(roots[0]!).minutes;
    expect(minutes.length).toBeLessThanOrEqual(LOCK_STATS_RETAIN_MINUTES + 1);
    expect(minutes.at(-1)!.minute).toBe(MINUTE0 + 90);
    expect(roots.filter(root => fs.existsSync(ownFile(root)))).toHaveLength(32);
    expect(fs.statSync(ownFile(roots[0]!)).size).toBeLessThan(32 * 1024);
  });

  it("retains the oldest minute of a 60-minute query: M-60 counts during minute M, M-61 never does", () => {
    vi.useFakeTimers();
    const root = temp();
    const stats = createLockStats("1")!;
    const M = MINUTE0 + 61;
    vi.setSystemTime((M - 61) * 60_000 + 1_000);
    stats.acquired(root, "custody", 1, 7); // M-61: outside every window queried during M
    stats.failed(root, "custody", 9, false);
    vi.setSystemTime((M - 60) * 60_000 + 1_000);
    stats.acquired(root, "publish", 2, 300); // M-60: the oldest minute of --minutes 60
    stats.failed(root, "publish", 10_000, false);
    vi.setSystemTime((M - 1) * 60_000 + 59_000);
    stats.acquired(root, "writeBatch", 1, 20); // M-1: the newest complete minute
    vi.setSystemTime(M * 60_000 + 30_000);
    stats.acquired(root, "put/delete", 1, 50); // M: current, incomplete, never counted
    registry[lockKey]!.flush();
    const minutes = read(root).minutes.map(minute => minute.minute);
    expect(minutes).toEqual([M - 60, M - 1, M]);
    const now = M * 60_000 + 45_000;
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 60, now });
    expect(summary).toMatchObject({ fromMinute: M - 60, toMinute: M - 1, minutes: 60, n: 2, holdMs: 320, timeouts: 1 });
    // The CLI's longest window is exactly the retention: --minutes 61 clamps to the same 60.
    for (const span of ["60", "61", "1000"]) {
      let out = "";
      expect(main(["--mesh", root, "--minutes", span, "--json", "--max-timeouts", "0"], { stdout: text => { out += text; }, now })).toBe(3);
      expect(JSON.parse(out)).toMatchObject({ fromMinute: M - 60, minutes: LOCK_STATS_RETAIN_MINUTES, n: 2, timeouts: 1 });
    }
    // At the next minute M-60 is out of the window and trimmed by the next write; M-59.. remain.
    vi.setSystemTime((M + 1) * 60_000 + 1_000);
    stats.acquired(root, "other", 1, 1);
    registry[lockKey]!.flush();
    expect(read(root).minutes.map(minute => minute.minute)).toEqual([M - 1, M, M + 1]);
    expect(summarizeLockStats(root, readLockStats(root), { minutes: 60, now: (M + 1) * 60_000 + 5_000 }))
      .toMatchObject({ fromMinute: M - 59, n: 2, holdMs: 70, timeouts: 0 });
  });

  it("never recreates a removed root or throws, and prunes stale files of dead processes", () => {
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(path.join(root, "lock-stats"), { recursive: true, mode: 0o700 });
    const stale = path.join(root, "lock-stats", "gone-1.json");
    const fresh = path.join(root, "lock-stats", "alive-2.json");
    fs.writeFileSync(stale, "{}");
    fs.writeFileSync(fresh, "{}");
    const old = (Date.now() - 25 * 60 * 60_000) / 1000;
    fs.utimesSync(stale, old, old);
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush();
    expect(fs.existsSync(stale)).toBe(false);
    expect(fs.existsSync(fresh)).toBe(true);
    expect(fs.existsSync(ownFile(root))).toBe(true);
    fs.rmSync(root, { recursive: true, force: true });
    stats.acquired(root, "custody", 1, 1);
    expect(() => registry[lockKey]!.flush()).not.toThrow();
    expect(fs.existsSync(root)).toBe(false);
  });

  it.skipIf(process.platform === "win32")("writes through a unique exclusive temporary and never follows a planted symlink", () => {
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(path.join(root, "lock-stats"), { recursive: true, mode: 0o700 });
    const victim = path.join(parent, "victim.txt");
    const other = path.join(parent, "other.txt");
    fs.writeFileSync(victim, "precious");
    fs.writeFileSync(other, "also precious");
    // The old predictable temporary name, and the final name, both planted as symlinks.
    fs.symlinkSync(victim, `${ownFile(root)}.tmp`);
    fs.symlinkSync(other, ownFile(root));
    const warn = vi.fn();
    const stats = createLockStats("1", { warn })!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush();
    expect(fs.readFileSync(victim, "utf8")).toBe("precious");
    expect(fs.readFileSync(other, "utf8")).toBe("also precious");
    expect(fs.lstatSync(`${ownFile(root)}.tmp`).isSymbolicLink()).toBe(true);
    // The rename replaced the planted link itself with the private regular file.
    const written = fs.lstatSync(ownFile(root));
    expect(written.isFile() && (written.mode & 0o777)).toBe(0o600);
    expect(read(root).minutes.at(-1)!.classes.custody).toMatchObject({ n: 1, holdMs: 1 });
    // No temporary is left behind besides the planted one.
    expect(fs.readdirSync(path.join(root, "lock-stats")).sort())
      .toEqual([path.basename(ownFile(root)), `${path.basename(ownFile(root))}.tmp`]);
    expect(warn).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")("disables a root whose lock-stats directory is a symlink or writable by others, once", () => {
    const parent = temp();
    const linked = path.join(parent, "linked");
    const elsewhere = path.join(parent, "elsewhere");
    fs.mkdirSync(linked);
    fs.mkdirSync(elsewhere, { mode: 0o700 });
    fs.symlinkSync(elsewhere, path.join(linked, "lock-stats"));
    const open = path.join(parent, "open");
    fs.mkdirSync(path.join(open, "lock-stats"), { recursive: true });
    fs.chmodSync(path.join(open, "lock-stats"), 0o777);
    const normal = path.join(parent, "normal");
    fs.mkdirSync(normal);
    const warn = vi.fn();
    const stats = createLockStats("1", { warn })!;
    for (const root of [linked, open, normal]) stats.acquired(root, "custody", 1, 1);
    expect(() => registry[lockKey]!.flush()).not.toThrow();
    expect(fs.readdirSync(elsewhere)).toEqual([]);
    expect(fs.readdirSync(path.join(open, "lock-stats"))).toEqual([]);
    expect(read(normal).minutes.at(-1)!.classes.custody).toMatchObject({ n: 1 });
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[0]![0]).toMatch(/lock-stats is a symlink/);
    expect(warn.mock.calls[1]![0]).toMatch(/lock-stats is group- or other-writable \(mode 777\)/);
    // Disabled for good: later acquisitions neither write nor warn again.
    for (const root of [linked, open]) stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush();
    expect(warn).toHaveBeenCalledTimes(2);
    expect(fs.readdirSync(elsewhere)).toEqual([]);
    expect(fs.readdirSync(path.join(open, "lock-stats"))).toEqual([]);
  });

  it("re-checks a stale file just before unlinking it and keeps one its owner refreshed meanwhile", () => {
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(path.join(root, "lock-stats"), { recursive: true, mode: 0o700 });
    const racing = path.join(root, "lock-stats", "alive-3.json");
    const stale = path.join(root, "lock-stats", "gone-1.json");
    const old = (Date.now() - 25 * 60 * 60_000) / 1000;
    for (const file of [racing, stale]) {
      fs.writeFileSync(file, "{}");
      fs.utimesSync(file, old, old);
    }
    const lstat = fs.lstatSync;
    let refreshed = false;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      const result = (lstat as (...args: unknown[]) => fs.Stats)(file, ...rest);
      if (file === racing && !refreshed) {
        // The owner's rename lands between the age check and the unlink.
        refreshed = true;
        fs.writeFileSync(`${racing}.next`, "{\"fresh\":true}");
        fs.renameSync(`${racing}.next`, racing);
      }
      return result;
    }) as typeof fs.lstatSync);
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush();
    expect(refreshed).toBe(true);
    expect(fs.readFileSync(racing, "utf8")).toBe("{\"fresh\":true}");
    expect(fs.existsSync(stale)).toBe(false);
  });

  it("keeps a stats file its writer refreshes between the age check and the delete (smarty-dev#7879)", () => {
    const root = path.join(temp(), "mesh");
    const directory = path.join(root, "lock-stats");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    // `once`: the writer's refresh lands right before the pruner moves or deletes the file.
    // `twice`: and a second refresh lands just after the pruner moved the first one aside.
    const once = path.join(directory, "once-3.json");
    const twice = path.join(directory, "twice-4.json");
    const old = (Date.now() - 25 * 60 * 60_000) / 1000;
    for (const file of [once, twice]) {
      fs.writeFileSync(file, "{}");
      fs.utimesSync(file, old, old);
    }
    const refresh = (file: string, body: string): void => {
      fs.writeFileSync(`${file}.next`, body);
      renameSync(`${file}.next`, file);
    };
    const renameSync = fs.renameSync, unlinkSync = fs.unlinkSync;
    const refreshed = new Set<string>();
    const before = (file: string): void => {
      if (refreshed.has(file) || (file !== once && file !== twice)) return;
      refreshed.add(file);
      refresh(file, `{"fresh":"${path.basename(file)}"}`);
    };
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      before(String(from));
      renameSync(from, to);
      if (String(from) === twice && String(to) !== twice) refresh(twice, "{\"fresh\":2}");
    });
    vi.spyOn(fs, "unlinkSync").mockImplementation((file) => { before(String(file)); unlinkSync(file); });
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush();
    expect([...refreshed].sort()).toEqual([once, twice]);
    expect(fs.readFileSync(once, "utf8")).toBe("{\"fresh\":\"once-3.json\"}");
    expect(fs.readFileSync(twice, "utf8")).toBe("{\"fresh\":2}");
    expect(fs.readdirSync(directory).filter(name => !name.startsWith(lockStatsHost())).sort()).toEqual(["once-3.json", "twice-4.json"]);
  });

  it("never replaces a newer stats file that lands just before the pruner restores a moved one (smarty-dev#7879)", () => {
    const root = path.join(temp(), "mesh");
    const directory = path.join(root, "lock-stats");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const file = path.join(directory, "racer-5.json");
    const old = (Date.now() - 25 * 60 * 60_000) / 1000;
    fs.writeFileSync(file, "{}");
    fs.utimesSync(file, old, old);
    const renameSync = fs.renameSync, linkSync = fs.linkSync;
    const refresh = (body: string): void => {
      fs.writeFileSync(`${file}.next`, body);
      renameSync(`${file}.next`, file);
    };
    let landed = 0;
    // The pruner moves the writer's first refresh aside; the second lands after that move and
    // right before the pruner puts the moved file back at the name.
    const restoring = (from: fs.PathLike, to: fs.PathLike): void => {
      if (String(from) === file && landed === 0) { landed++; refresh("{\"fresh\":1}"); }
      else if (String(to) === file && String(from).startsWith(`${file}.prune-`) && landed === 1) { landed++; refresh("{\"fresh\":2}"); }
    };
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { restoring(from, to); renameSync(from, to); });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => { restoring(from, to); linkSync(from, to); });
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush();
    expect(landed).toBe(2);
    expect(fs.readFileSync(file, "utf8")).toBe("{\"fresh\":2}");
    expect(fs.readdirSync(directory).filter(name => !name.startsWith(lockStatsHost()))).toEqual(["racer-5.json"]);
  });

  it("keeps a refreshed graveyard when relinking fails, but drops it on EEXIST (smarty-dev#8954)", () => {
    const stats = createLockStats("1")!;
    for (const code of ["EPERM", "EEXIST"]) {
      const root = path.join(temp(), "mesh");
      const directory = path.join(root, "lock-stats");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      const file = path.join(directory, "racer-5.json");
      const grave = `${file}.prune-${process.pid}`;
      const old = (Date.now() - 25 * 60 * 60_000) / 1000;
      fs.writeFileSync(file, "{}");
      fs.utimesSync(file, old, old);
      const renameSync = fs.renameSync, linkSync = fs.linkSync;
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(from) === file) {
          // A writer replaces the aged inode just before the pruner moves it aside.
          fs.writeFileSync(`${file}.next`, "{\"fresh\":1}");
          renameSync(`${file}.next`, file);
        }
        renameSync(from, to);
      });
      const link = vi.spyOn(fs, "linkSync").mockImplementationOnce((from, to) => {
        if (code === "EPERM") throw Object.assign(new Error("relink denied"), { code });
        fs.writeFileSync(file, "{\"fresh\":2}");
        linkSync(from, to); // The newer file makes the real no-replace link fail with EEXIST.
      });
      stats.acquired(root, "custody", 1, 1);
      registry[lockKey]!.flush();
      expect(link).toHaveBeenCalledExactlyOnceWith(grave, file);
      if (code === "EPERM") {
        expect(fs.existsSync(grave)).toBe(true);
        expect(fs.readFileSync(grave, "utf8")).toBe("{\"fresh\":1}");
        expect(fs.existsSync(file)).toBe(false);
      } else {
        expect(fs.existsSync(grave)).toBe(false);
        expect(fs.readFileSync(file, "utf8")).toBe("{\"fresh\":2}");
      }
      vi.restoreAllMocks();
    }
  });

  it("prunes a quiet root opportunistically on explicit flush and drops a removed root", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root);
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush(); // written and pruned once; the root is quiet from now on
    vi.advanceTimersByTime(63_000);
    const written = fs.statSync(ownFile(root)).mtimeMs;
    const stale = path.join(root, "lock-stats", "gone-1.json");
    const fresh = path.join(root, "lock-stats", "alive-2.json");
    fs.writeFileSync(stale, "{}");
    fs.writeFileSync(fresh, "{}");
    fs.utimesSync(stale, (T0 - 25 * 60 * 60_000) / 1000, (T0 - 25 * 60 * 60_000) / 1000);
    fs.utimesSync(fresh, T0 / 1000, T0 / 1000);
    vi.advanceTimersByTime(30 * 60_000);
    registry[lockKey]!.flush();
    expect(fs.existsSync(stale)).toBe(true); // throttled to at most hourly during actual work
    vi.advanceTimersByTime(31 * 60_000);
    expect(fs.existsSync(stale)).toBe(true); // no idle pruning timer
    expect(vi.getTimerCount()).toBe(0);
    registry[lockKey]!.flush();
    expect(fs.existsSync(stale)).toBe(false);
    expect(fs.existsSync(fresh)).toBe(true);
    expect(fs.statSync(ownFile(root)).mtimeMs).toBe(written); // pruning did not rewrite
    fs.rmSync(root, { recursive: true, force: true });
    vi.advanceTimersByTime(61 * 60_000);
    expect(() => registry[lockKey]!.flush()).not.toThrow();
    expect(fs.existsSync(root)).toBe(false);
    // Dropped from tracking: later explicit flushes no longer touch it.
    const readdir = vi.spyOn(fs, "readdirSync");
    vi.advanceTimersByTime(61 * 60_000);
    registry[lockKey]!.flush();
    expect(readdir.mock.calls.filter(([target]) => String(target).startsWith(root))).toHaveLength(0);
    expect(fs.existsSync(root)).toBe(false);
  });

  it("prunes its own file by age on real work: kept while fresh, removed by flush after 24 h", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root);
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    registry[lockKey]!.flush(); // written (and pruned once); quiet from now on
    vi.advanceTimersByTime(63_000);
    // File times follow the real clock; pin the own file to the fake write time.
    fs.utimesSync(ownFile(root), T0 / 1000, T0 / 1000);
    vi.advanceTimersByTime(23 * 60 * 60_000);
    registry[lockKey]!.flush(); // within the retention: kept
    expect(fs.existsSync(ownFile(root))).toBe(true);
    vi.advanceTimersByTime(2 * 60 * 60_000); // past 24 h with no acquisition: still no wake
    expect(fs.existsSync(ownFile(root))).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    registry[lockKey]!.flush();
    expect(fs.existsSync(ownFile(root))).toBe(false);
    // Still tracked: the next acquisition recreates the file.
    stats.acquired(root, "publish", 1, 2);
    registry[lockKey]!.flush();
    expect(read(root).minutes.at(-1)!.classes.publish).toMatchObject({ n: 1, holdMs: 2 });
  });

  it("merges spellings of one root into one bucket and one file", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root);
    const link = path.join(temp(), "link");
    fs.symlinkSync(root, link, "junction");
    const spellings = [root, `${root}${path.sep}`, path.relative(process.cwd(), root) || ".", link, path.join(link, "..", "link")];
    if (process.platform === "win32") spellings.push(root.toUpperCase());
    const stats = createLockStats("1")!;
    for (const spelling of spellings) {
      stats.acquired(spelling, "publish", 1, 10);
      stats.failed(spelling, "custody", 5, false);
    }
    registry[lockKey]!.flush();
    expect(fs.readdirSync(path.join(root, "lock-stats"))).toEqual([path.basename(ownFile(root))]);
    const file = read(root);
    expect(file.root).toBe(fs.realpathSync.native(root));
    const classes = file.minutes.at(-1)!.classes;
    expect(classes.publish).toMatchObject({ n: spellings.length, holdMs: 10 * spellings.length });
    expect(classes.custody).toMatchObject({ timeouts: spellings.length, failedWaitMs: 5 * spellings.length });
  });

  it("captures a disabled setting once across a release reload", async () => {
    vi.useFakeTimers();
    expect(createLockStats("0")).toBeUndefined();
    vi.resetModules();
    const reloaded = await import("../src/mesh/commit-stats.js");
    expect(reloaded.createLockStats("1")).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("shares one recorder across module generations", async () => {
    const root = temp();
    // Each generation is a full installed tree: verified-remove's diagnostics import must stay
    // generation-local, including its core agent-dir dependency and lazy log module.
    const copies = ["release-a", "release-b"].map(release => {
      for (const module of ["mesh/commit-stats.ts", "storage/verified-remove.ts",
        "core/diagnostics.ts", "core/diagnostics-log.ts", "core/agent-dir.ts"]) {
        fs.mkdirSync(path.dirname(path.join(root, release, module)), { recursive: true });
        fs.copyFileSync(fileURLToPath(new URL(`../src/${module}`, import.meta.url)), path.join(root, release, module));
      }
      return path.join(root, release, "mesh/commit-stats.ts");
    });
    const first = await import(/* @vite-ignore */ pathToFileURL(copies[0]!).href);
    const second = await import(/* @vite-ignore */ pathToFileURL(copies[1]!).href);
    expect(second.createLockStats).not.toBe(first.createLockStats);
    const stats = first.createLockStats("1");
    expect(second.createLockStats("1")).toBe(stats);
    expect(second.createLockStats("0")).toBe(stats);
  });

  it("records its writer once at first acquisition, immutable across env changes, flushes and reloads (smarty-dev#8305)", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const root = temp();
    for (const name of ["PI_FABRIC_AGENT_NAME", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME", "PI_FABRIC_ROLE", "SMARTY_ROLE"]) vi.stubEnv(name, "");
    vi.stubEnv("PI_FABRIC_AGENT_NAME", "before-create");
    const stats = createLockStats("1")!;
    // Captured at first use, not at creation, and not per acquisition.
    vi.stubEnv("PI_FABRIC_AGENT_NAME", "worker-7");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor-0123456789");
    vi.stubEnv("SMARTY_ROLE", "task-agent@0123456789ab");
    stats.acquired(root, "publish", 1, 2);
    vi.stubEnv("PI_FABRIC_AGENT_NAME", "changed");
    vi.stubEnv("PI_FABRIC_ROLE", "org");
    registry[lockKey]!.flush();
    const expected = { argv1: path.basename(process.argv[1]!), agentName: "worker-7", actorId: "actor-0123456789",
      smartyRole: "task-agent@0123456789ab", ppid: process.ppid };
    expect(read(root).writer).toEqual(expected);
    vi.resetModules();
    const reloaded = await import("../src/mesh/commit-stats.js");
    const again = reloaded.createLockStats("1")!;
    expect(again).toBe(stats);
    vi.setSystemTime(T0 + 60_000);
    again.acquired(root, "custody", 1, 2); // the next minute's flush rewrites the file
    expect(read(root).minutes).toHaveLength(2);
    expect(read(root).writer).toEqual(expected);
  });

  it("captures only set, bounded, separate source facts", () => {
    const writer = captureLockStatsWriter({ PI_FABRIC_AGENT_NAME: "  ", PI_FABRIC_ROLE: "r".repeat(500), PI_FABRIC_ACTOR_NAME: "Ada" },
      "/opt/pi/dist/bridge-worker.js", 42);
    expect(writer).toEqual({ argv1: "bridge-worker.js", actorName: "Ada", role: "r".repeat(LOCK_STATS_WRITER_MAX_CHARS), ppid: 42 });
    expect(Object.isFrozen(writer)).toBe(true);
    expect(captureLockStatsWriter({}, "", -1)).toEqual({});
  });

  it("keeps an exited child writer attributable from its file", () => {
    const root = temp();
    const env = taskAgentEnvironment({ ...process.env, PI_FABRIC_AGENT_NAME: "child-writer", PI_FABRIC_ACTOR_ID: "", PI_FABRIC_ACTOR_NAME: "",
      PI_FABRIC_ROLE: "" });
    const child = spawnSync(process.execPath, [path.resolve("tests/fixtures/lock-stats-writer.mjs"),
      path.resolve("src/mesh/commit-stats.ts"), root], { env, encoding: "utf8", timeout: 30_000 });
    if (env[hostedAdmissionMarker] === "1") expect(child.stderr).toBe("");
    else expect(child.stderr).toContain("hosted-only (outside-temp deletion reporting): push and let CI run it");
    expect(child.status).toBe(0);
    const file = JSON.parse(fs.readFileSync(path.join(root, "lock-stats", `${lockStatsHost()}-${child.pid}.json`), "utf8")) as LockStatsFile;
    expect(file.writer).toEqual({ argv1: "lock-stats-writer.mjs", agentName: "child-writer", smartyRole: "task-agent", ppid: process.pid });
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1, now: file.updatedAt + 60_000 });
    expect(summary.pids[0]!.writer).toEqual(file.writer);
  });
});

describe("fleet summary and fabric-mesh-lock-stats", () => {
  const seed = (root: string): void => {
    const hist = (index: number, count: number): number[] => Object.assign(new Array(14).fill(0), { [index]: count });
    fixture(root, "ryzen1", 100, [
      { minute: MINUTE0 - 1, classes: { publish: bucket({ n: 5, holdMs: 99_999 }) } }, // outside the window
      { minute: MINUTE0, classes: {
        writeBatch: bucket({ n: 10, holdMs: 12_000, holdMaxMs: 3_000, waitMs: 1_000, waitMaxMs: 400, holdHist: hist(7, 10), waitHist: hist(6, 10) }),
        "heartbeat/confirm": bucket({ n: 100, holdMs: 600, holdMaxMs: 40, waitMs: 20_000, waitMaxMs: 900, holdHist: hist(1, 100), waitHist: hist(8, 100) }),
      } },
    ]);
    fixture(root, "ryzen1", 200, [
      { minute: MINUTE0 + 1, classes: {
        publish: bucket({ n: 4, holdMs: 6_000, holdMaxMs: 2_000, holdHist: hist(9, 4), waitHist: hist(0, 4) }),
        custody: bucket({ timeouts: 2, tries: 3, failedWaitMs: 20_000 }),
      } },
    ]);
    fs.writeFileSync(path.join(root, "lock-stats", "torn-1.json"), "{\"version\":1,");
    fs.writeFileSync(path.join(root, "lock-stats", "ryzen1-300.json.tmp"), "{}");
  };

  it("sums processes into busy %, timeouts, classes by hold time and top pids", () => {
    const root = temp();
    seed(root);
    expect(readLockStats(root)).toHaveLength(2);
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 2, now: (MINUTE0 + 2) * 60_000 + 5_000, top: 1 });
    expect(summary).toMatchObject({ fromMinute: MINUTE0, toMinute: MINUTE0 + 1, minutes: 2, processes: 2,
      n: 114, holdMs: 18_600, timeouts: 2, tries: 3, holdMaxMs: 3_000, waitMaxMs: 900 });
    expect(summary.busyPct).toBeCloseTo(18_600 / 120_000 * 100, 6);
    expect(summary.peakMinuteBusyPct).toBeCloseTo(12_600 / 60_000 * 100, 6);
    expect(summary.classes.map(row => row.lockClass)).toEqual(["writeBatch", "publish", "heartbeat/confirm", "custody"]);
    expect(summary.classes[0]).toMatchObject({ n: 10, holdMeanMs: 1_200, holdP99Ms: 250, waitP99Ms: 100 });
    expect(summary.classes[0]!.holdSharePct).toBeCloseTo(12_000 / 18_600 * 100, 6);
    expect(summary.classes.find(row => row.lockClass === "heartbeat/confirm")).toMatchObject({ n: 100, waitMeanMs: 200, waitP99Ms: 500 });
    expect(summary.pids).toEqual([expect.objectContaining({ host: "ryzen1", pid: 100, n: 110, holdMs: 12_600, topClass: "writeBatch" })]);
    // p99 across all acquisitions: 110 of 114 holds are <= 250 ms, the rest <= 1 s.
    expect(summary.holdP99Ms).toBe(1_000);
  });

  it("prints the window and gates on busy % and timeouts", () => {
    const root = temp();
    seed(root);
    const now = (MINUTE0 + 2) * 60_000 + 5_000;
    let out = "";
    let err = "";
    const io = { stdout: (text: string) => { out += text; }, stderr: (text: string) => { err += text; }, now };
    expect(main(["--mesh", root, "--minutes", "2"], io)).toBe(0);
    expect(err).toContain("ignored torn-1.json:");
    expect(out).toContain("busy 15.5% (peak minute 21.0%), 114 acquisitions, timeouts 2, failed tries 3");
    expect(out).toMatch(/classes by hold time:\nclass\s+acq/);
    expect(out.indexOf("writeBatch")).toBeLessThan(out.indexOf("heartbeat/confirm"));
    expect(out).toMatch(/ryzen1-100\s+110/);
    // The torn file fails any gate, even one the readable files pass.
    err = "";
    expect(main(["--mesh", root, "--minutes", "2", "--max-busy", "30"], io)).toBe(3);
    expect(err).toContain("gate not passed: 1 stats file could not be read or validated");
    fs.rmSync(path.join(root, "lock-stats", "torn-1.json"));
    out = "";
    expect(main(["--mesh", root, "--minutes", "2", "--json", "--max-busy", "30"], io)).toBe(0);
    const summary = JSON.parse(out) as { n: number; timeouts: number; busyPct: number };
    expect(summary).toMatchObject({ n: 114, timeouts: 2 });
    // --max-busy is the bound to stay under (busy < PCT): equality fails. --max-timeouts N allows N.
    expect(main(["--mesh", root, "--minutes", "2", "--max-busy", String(summary.busyPct)], io)).toBe(3);
    expect(main(["--mesh", root, "--minutes", "2", "--max-busy", String(summary.busyPct + 0.01)], io)).toBe(0);
    expect(main(["--mesh", root, "--minutes", "2", "--max-timeouts", "2"], io)).toBe(0);
    expect(main(["--mesh", root, "--minutes", "2", "--max-timeouts", "1"], io)).toBe(3);
    expect(main(["--mesh", root, "--minutes", "2", "--max-busy", "10"], io)).toBe(3);
    expect(main(["--mesh", root, "--minutes", "2", "--max-timeouts", "0"], io)).toBe(3);
    out = "";
    expect(main(["--mesh", path.join(root, "empty"), "--minutes", "5"], io)).toBe(0);
    expect(out).toContain("no lock acquisitions recorded in this window");
    expect(() => main(["--minutes"], io)).toThrow(/Missing value/);
    expect(() => main(["--bogus", "1"], io)).toThrow(/Bad argument/);
  });

  it("ignores invalid counters, unknown classes, symlinks and oversized files, and no gate passes on them", () => {
    const root = temp();
    const now = (MINUTE0 + 2) * 60_000 + 5_000;
    fixture(root, "ryzen1", 100, [{ minute: MINUTE0, classes: { publish: bucket({ n: 1, holdMs: 10 }) } }]);
    fixture(root, "evil", 1, [{ minute: MINUTE0, classes: { custody: bucket({ timeouts: -1 }) } }]);
    fixture(root, "evil", 2, [{ minute: MINUTE0, classes: { custody: bucket({ n: 1.5 }) } }]);
    fixture(root, "evil", 3, [{ minute: MINUTE0, classes: { custody: bucket({ waitHist: [1] }) } }]);
    fixture(root, "evil", 4, [{ minute: MINUTE0, classes: { bogus: bucket({}) } as LockStatsFile["minutes"][number]["classes"] }]);
    fixture(root, "evil", 5, [{ minute: MINUTE0, classes: { custody: bucket({ holdMs: -5, failedWaitMs: Number.NaN }) } }]);
    fs.writeFileSync(path.join(root, "lock-stats", "big-7.json"), " ".repeat(LOCK_STATS_MAX_FILE_BYTES + 1));
    const symlinks = process.platform !== "win32";
    if (symlinks) fs.symlinkSync(path.join(root, "lock-stats", "ryzen1-100.json"), path.join(root, "lock-stats", "link-6.json"));
    const problems: string[] = [];
    expect(readLockStats(root, problems).map(file => file.pid)).toEqual([100]);
    expect(problems).toHaveLength(symlinks ? 7 : 6);
    expect(problems).toEqual(expect.arrayContaining([
      "evil-1.json: invalid custody counters", "evil-2.json: invalid custody counters",
      "evil-3.json: invalid custody counters", "evil-4.json: unknown lock class", "evil-5.json: invalid custody counters",
      `big-7.json: larger than ${LOCK_STATS_MAX_FILE_BYTES} bytes`,
      ...symlinks ? ["link-6.json: not a regular file"] : [],
    ]));
    let out = "";
    let err = "";
    const io = { stdout: (text: string) => { out += text; }, stderr: (text: string) => { err += text; }, now };
    // timeouts:-1 must not make --max-timeouts 0 pass: the file is ignored, and the gate fails on it.
    expect(main(["--mesh", root, "--minutes", "2", "--max-timeouts", "0"], io)).toBe(3);
    expect(err).toContain("ignored evil-1.json: invalid custody counters");
    expect(err).toContain(`gate not passed: ${problems.length} stats files could not be read or validated`);
    expect(out).toContain("timeouts 0");
    // Without a gate the view prints and exits 0; the warnings stay on stderr.
    expect(main(["--mesh", root, "--minutes", "2"], io)).toBe(0);
  });

  it("carries optional writer facts through read, summary, JSON and table; old files stay valid; a bad writer fails the gate", () => {
    const root = temp();
    const now = (MINUTE0 + 2) * 60_000 + 5_000;
    const minutes = (holdMs: number): LockStatsFile["minutes"] => [{ minute: MINUTE0, classes: { publish: bucket({ n: 1, holdMs }) } }];
    const write = (name: string, body: unknown): void => fs.writeFileSync(path.join(root, "lock-stats", name), JSON.stringify(body));
    fixture(root, "ryzen1", 100, minutes(30)); // pre-#8305 file: no writer
    write("ryzen1-200.json", { version: 1, host: "ryzen1", pid: 200, root, startedAt: T0, updatedAt: T0, minutes: minutes(20),
      writer: { argv1: "cli.js", agentName: "lane\u001b[2J", smartyRole: "task-agent@abc", ppid: 7, future: "dropped" } });
    const bad = [{ argv1: 5 }, ["cli.js"], "cli.js", { ppid: -1 }, { agentName: "x".repeat(LOCK_STATS_WRITER_MAX_CHARS + 1) }, null];
    bad.forEach((writer, index) => write(`evil-${index}.json`,
      { version: 1, host: "evil", pid: index, root, startedAt: T0, updatedAt: T0, minutes: minutes(1), writer }));
    const problems: string[] = [];
    const files = readLockStats(root, problems);
    expect(files.map(file => file.pid).sort()).toEqual([100, 200]);
    expect(problems.sort()).toEqual(bad.map((_, index) => `evil-${index}.json: bad writer`));
    const summary = summarizeLockStats(root, files, { minutes: 2, now });
    expect(summary.pids.map(row => [row.pid, row.writer])).toEqual([
      [100, undefined],
      [200, { argv1: "cli.js", agentName: "lane\u001b[2J", smartyRole: "task-agent@abc", ppid: 7 }],
    ]);
    expect("writer" in summary.pids[0]!).toBe(false);
    let out = "";
    let err = "";
    const io = { stdout: (text: string) => { out += text; }, stderr: (text: string) => { err += text; }, now };
    expect(main(["--mesh", root, "--minutes", "2", "--max-busy", "90"], io)).toBe(3);
    expect(err).toContain("ignored evil-0.json: bad writer");
    expect(out).toMatch(/top class\s+writer\n/);
    expect(out).toMatch(/ryzen1-100\s.*publish\s+-\n/);
    expect(out).toContain("cli.js lane\\u001b[2J task-agent ppid=7");
    expect(out).not.toContain("\u001b");
    for (let index = 0; index < bad.length; index++) fs.rmSync(path.join(root, "lock-stats", `evil-${index}.json`));
    out = "";
    expect(main(["--mesh", root, "--minutes", "2", "--json", "--max-busy", "90"], io)).toBe(0);
    const json = JSON.parse(out) as { pids: Array<{ pid: number; writer?: unknown }> };
    expect(json.pids.find(row => row.pid === 200)!.writer).toEqual(summary.pids[1]!.writer);
    expect(json.pids.find(row => row.pid === 100)!.writer).toBeUndefined();
  });

  it("escapes control characters in printed host labels", () => {
    const root = temp();
    fs.mkdirSync(path.join(root, "lock-stats"));
    const file: LockStatsFile = { version: 1, host: "a\u001b[2Jb\nc", pid: 9, root, startedAt: T0, updatedAt: T0,
      minutes: [{ minute: MINUTE0, classes: { publish: bucket({ n: 1, holdMs: 10 }) } }] };
    fs.writeFileSync(path.join(root, "lock-stats", "x-9.json"), JSON.stringify(file));
    let out = "";
    expect(main(["--mesh", root, "--minutes", "2"], { stdout: (text: string) => { out += text; }, now: (MINUTE0 + 2) * 60_000 })).toBe(0);
    expect(out).toContain("a\\u001b[2Jb\\u000ac-9");
    expect(out).not.toContain("\u001b");
  });

  it("skips files modified before the window before the cap, and caps the stalest (smarty-dev#7826)", () => {
    const root = temp();
    const directory = path.join(root, "lock-stats");
    const now = (MINUTE0 + 2) * 60_000 + 5_000;
    const old = (now - (LOCK_STATS_RETAIN_MINUTES + 5) * 60_000) / 1000;
    for (let pid = 1; pid <= 5_000; pid++) {
      fixture(root, "aaa-dead", pid, [{ minute: MINUTE0 - 90, classes: { publish: bucket({ n: 1 }) } }]);
      fs.utimesSync(path.join(directory, `aaa-dead-${pid}.json`), old, old);
    }
    fixture(root, "zzz-bridge", 7, [{ minute: MINUTE0 + 1, classes: { publish: bucket({ n: 3, holdMs: 30 }) } }]);
    const problems: string[] = [];
    expect(readLockStats(root, problems, { minutes: 2, now }).map(file => file.host)).toEqual(["zzz-bridge"]);
    expect(problems).toEqual([]);
    let out = "";
    const io = { stdout: (text: string) => { out += text; }, stderr: () => {}, now };
    expect(main(["--mesh", root, "--minutes", "2"], io)).toBe(0);
    expect(out).toContain("3 acquisitions");
    // The boundary is the summary window's first minute (MINUTE0 with --minutes 2 at MINUTE0 + 2):
    // a file last written just before it is skipped; one written at its start is read.
    fixture(root, "edge-before", 8, [{ minute: MINUTE0 - 1, classes: { publish: bucket({ n: 1 }) } }]);
    fixture(root, "edge-at", 9, [{ minute: MINUTE0, classes: { publish: bucket({ n: 1 }) } }]);
    fs.utimesSync(path.join(directory, "edge-before-8.json"), (MINUTE0 * 60_000 - 1) / 1000, (MINUTE0 * 60_000 - 1) / 1000);
    fs.utimesSync(path.join(directory, "edge-at-9.json"), MINUTE0 * 60, MINUTE0 * 60);
    expect(readLockStats(root, [], { minutes: 2, now }).map(file => file.host).sort()).toEqual(["edge-at", "zzz-bridge"]);
    fs.unlinkSync(path.join(directory, "edge-before-8.json"));
    fs.unlinkSync(path.join(directory, "edge-at-9.json"));
    // Still over the cap inside the window: the newest are read and the cut is reported.
    for (let pid = 1; pid <= LOCK_STATS_MAX_FILES; pid++) fixture(root, "aaa-fresh", pid, [{ minute: MINUTE0 + 1, classes: {} }]);
    const newer = Date.now() / 1000 + 3_600;
    fs.utimesSync(path.join(directory, "zzz-bridge-7.json"), newer, newer);
    const capped: string[] = [];
    const files = readLockStats(root, capped, { minutes: 2, now });
    expect(files).toHaveLength(LOCK_STATS_MAX_FILES);
    expect(files[0]!.host).toBe("zzz-bridge");
    expect(capped).toEqual([`${directory}: ${LOCK_STATS_MAX_FILES + 1} stats files in the window, read only the newest ${LOCK_STATS_MAX_FILES}`]);
  });

  it("gives no cap slot to files that hold only the current, incomplete minute (smarty-dev#7894)", () => {
    const root = temp();
    const directory = path.join(root, "lock-stats");
    const now = (MINUTE0 + 2) * 60_000 + 5_000;
    // Newer than the in-window file, so they sort first; each holds only the current minute.
    for (let pid = 1; pid <= LOCK_STATS_MAX_FILES + 1; pid++) {
      fixture(root, "aaa-current", pid, [{ minute: MINUTE0 + 2, classes: { publish: bucket({ n: 1 }) } }]);
    }
    fixture(root, "zzz-bridge", 7, [{ minute: MINUTE0 + 1, classes: { publish: bucket({ n: 3, holdMs: 30 }) } },
      { minute: MINUTE0 + 2, classes: { publish: bucket({ n: 1 }) } }]);
    fs.utimesSync(path.join(directory, "zzz-bridge-7.json"), now / 1000, now / 1000);
    const problems: string[] = [];
    expect(readLockStats(root, problems, { minutes: 2, now }).map(file => file.host)).toEqual(["zzz-bridge"]);
    expect(problems).toEqual([]);
  });
});
