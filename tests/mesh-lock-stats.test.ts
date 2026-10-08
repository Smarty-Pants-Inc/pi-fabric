import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createLockStats, LOCK_STATS_RETAIN_MINUTES, lockStatsHost, readLockStats, summarizeLockStats,
  type LockStatsBucket, type LockStatsFile,
} from "../src/mesh/commit-stats.js";
import { main } from "../src/mesh-lock-stats-cli.js";

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
  it("aggregates wait and hold per class and wall-clock minute and writes after the minute", () => {
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
    expect(vi.getTimerCount()).toBe(1);
    expect(process.listenerCount("exit")).toBe(exitListeners + 1);
    vi.advanceTimersByTime(49_000);
    expect(fs.existsSync(ownFile(root))).toBe(false);
    vi.advanceTimersByTime(14_000); // past the minute plus the pid spread (<= 2.5 s)
    const file = read(root);
    expect(file).toMatchObject({ version: 1, host: lockStatsHost(), pid: process.pid, root: path.resolve(root) });
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
    fs.mkdirSync(path.join(root, "lock-stats"), { recursive: true });
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

  it("prunes a quiet root hourly without new acquisitions and drops a removed root", () => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const parent = temp();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root);
    const stats = createLockStats("1")!;
    stats.acquired(root, "custody", 1, 1);
    vi.advanceTimersByTime(63_000); // written and pruned once; the root is quiet from now on
    const written = fs.statSync(ownFile(root)).mtimeMs;
    const stale = path.join(root, "lock-stats", "gone-1.json");
    const fresh = path.join(root, "lock-stats", "alive-2.json");
    fs.writeFileSync(stale, "{}");
    fs.writeFileSync(fresh, "{}");
    fs.utimesSync(stale, (T0 - 25 * 60 * 60_000) / 1000, (T0 - 25 * 60 * 60_000) / 1000);
    fs.utimesSync(fresh, T0 / 1000, T0 / 1000);
    vi.advanceTimersByTime(30 * 60_000);
    expect(fs.existsSync(stale)).toBe(true); // hourly, not every minute
    vi.advanceTimersByTime(31 * 60_000);
    expect(fs.existsSync(stale)).toBe(false);
    expect(fs.existsSync(fresh)).toBe(true);
    expect(fs.statSync(ownFile(root)).mtimeMs).toBe(written); // pruning did not rewrite
    fs.rmSync(root, { recursive: true, force: true });
    expect(() => vi.advanceTimersByTime(61 * 60_000)).not.toThrow();
    expect(fs.existsSync(root)).toBe(false);
    // Dropped from tracking: later hourly flushes no longer touch it.
    const readdir = vi.spyOn(fs, "readdirSync");
    vi.advanceTimersByTime(61 * 60_000);
    expect(readdir.mock.calls.filter(([target]) => String(target).startsWith(root))).toHaveLength(0);
    expect(fs.existsSync(root)).toBe(false);
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
    const source = fileURLToPath(new URL("../src/mesh/commit-stats.ts", import.meta.url));
    const copies = ["release-a.ts", "release-b.ts"].map(name => path.join(root, name));
    for (const copy of copies) fs.copyFileSync(source, copy);
    const first = await import(/* @vite-ignore */ pathToFileURL(copies[0]!).href);
    const second = await import(/* @vite-ignore */ pathToFileURL(copies[1]!).href);
    expect(second.createLockStats).not.toBe(first.createLockStats);
    const stats = first.createLockStats("1");
    expect(second.createLockStats("1")).toBe(stats);
    expect(second.createLockStats("0")).toBe(stats);
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
    const io = { stdout: (text: string) => { out += text; }, now };
    expect(main(["--mesh", root, "--minutes", "2"], io)).toBe(0);
    expect(out).toContain("busy 15.5% (peak minute 21.0%), 114 acquisitions, timeouts 2, failed tries 3");
    expect(out).toMatch(/classes by hold time:\nclass\s+acq/);
    expect(out.indexOf("writeBatch")).toBeLessThan(out.indexOf("heartbeat/confirm"));
    expect(out).toMatch(/ryzen1-100\s+110/);
    out = "";
    expect(main(["--mesh", root, "--minutes", "2", "--json", "--max-busy", "30"], io)).toBe(0);
    expect(JSON.parse(out)).toMatchObject({ n: 114, timeouts: 2 });
    expect(main(["--mesh", root, "--minutes", "2", "--max-busy", "10"], io)).toBe(3);
    expect(main(["--mesh", root, "--minutes", "2", "--max-timeouts", "0"], io)).toBe(3);
    out = "";
    expect(main(["--mesh", path.join(root, "empty"), "--minutes", "5"], io)).toBe(0);
    expect(out).toContain("no lock acquisitions recorded in this window");
    expect(() => main(["--minutes"], io)).toThrow(/Missing value/);
    expect(() => main(["--bogus", "1"], io)).toThrow(/Bad argument/);
  });
});
