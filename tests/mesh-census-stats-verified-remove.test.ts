// smarty-dev#8907: mesh census, lock-stats and delivery-outcome deleters remove only the inode they judged.
// Nothing is deleted here: rm/unlink are recorded no-ops and verifiedRemove runs its real checks as a dry run.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createLockEvents, createLockStats, LOCK_EVENTS_FILE, LOCK_EVENTS_ROTATE_LOCK } from "../src/mesh/commit-stats.js";
import { appendDeliveryOutcome } from "../src/mesh/delivery-outcomes.js";
import { censusHostSlug, pruneDeadCensusRecords } from "../src/mesh/store.js";
import { censusSync } from "../src/mesh/writer-census.js";

type Call = { target: string; deleter: string; ino: number | undefined; removed: boolean; reason?: string };
const probe = vi.hoisted(() => ({ dryRun: true, calls: [] as Call[] }));
vi.mock("../src/storage/verified-remove.js", async (importOriginal) => {
  const real = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return {
    ...real,
    verifiedRemove: (...[target, judged, options]: Parameters<typeof real.verifiedRemove>) => {
      const result = real.verifiedRemove(target, judged, probe.dryRun ? { ...options, dryRun: true } : options);
      probe.calls.push({ target, deleter: options.deleter, ino: judged?.ino, ...result });
      return result;
    },
  };
});

const registry = globalThis as Record<symbol, { dispose(): void } | undefined>;
const globalsKeys = [Symbol.for("pi-fabric.mesh.lock-stats"), Symbol.for("pi-fabric.mesh.lock-events")];
const temps: string[] = [];
const primitive: Array<{ file: string; options: unknown }> = [];
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const rmSync = fs.rmSync;
const dir = (...parts: string[]): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-8907-"));
  temps.push(root);
  const directory = path.join(root, ...parts);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  return directory;
};
const deadPid = ((): number => {
  for (let pid = 4_000_000; pid > 1; pid--) {
    try { process.kill(pid, 0); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return pid; }
  }
  throw new Error("no dead pid");
})();
/** Replaces `file` with a new inode (a writer's rename), keeping the old one out of reach. */
const swap = (file: string, body: string): void => {
  fs.writeFileSync(`${file}.swap`, body, { mode: 0o600 });
  fs.renameSync(`${file}.swap`, file);
};
/** Runs `inject` once, right after the deleter's judgment primitive returns for `file`. */
const afterLstat = (file: string, inject: () => void): void => {
  const lstat = fs.lstatSync;
  let done = false;
  vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    const result = (lstat as (...args: unknown[]) => unknown)(target, ...rest);
    if (!done && String(target) === file) { done = true; inject(); }
    return result;
  }) as typeof fs.lstatSync);
};
const afterLiveness = (inject: () => void): void => {
  const kill = process.kill.bind(process);
  let done = false;
  vi.spyOn(process, "kill").mockImplementation((pid: number, signal?: string | number) => {
    if (!done && pid === deadPid) { done = true; inject(); }
    return kill(pid, signal);
  });
};
const host = os.hostname();
const STARTED = 1_700_000_000_000;
const record = (pid: number): string => JSON.stringify({ format: 1, pid, host, startedAt: STARTED, releaseSha: "abc",
  lockProtocol: 3, stateBackend: "file" });
const censusRecord = (): string => {
  const file = path.join(dir(".writer-census"), `${censusHostSlug(host)}-${deadPid}-${STARTED}.json`);
  fs.writeFileSync(file, record(deadPid), { mode: 0o600 });
  return file;
};
const staleStatsTemp = (): { root: string; file: string } => {
  const directory = dir("mesh", "lock-stats");
  const file = path.join(directory, "gone-1.json.abc123.tmp");
  fs.writeFileSync(file, "{}", { mode: 0o600 });
  const old = (Date.now() - 25 * 60 * 60_000) / 1000;
  fs.utimesSync(file, old, old);
  return { root: path.dirname(directory), file };
};
const flushStats = (root: string): void => {
  createLockStats("1")!.acquired(root, "custody", 1, 1);
  (registry[Symbol.for("pi-fabric.mesh.lock-stats")] as unknown as { flush(): void }).flush();
};
const expiredPartition = (): { root: string; file: string } => {
  const directory = dir("mesh", "delivery-outcomes");
  const file = path.join(directory, "20200101.jsonl");
  fs.writeFileSync(file, "{}\n", { mode: 0o600 });
  return { root: path.dirname(directory), file };
};
const send = { eventId: "e1", to: "a", from: "b", mode: "publish" as const };
const refusedSwap = (file: string, body: string): void => {
  expect(primitive.map(item => item.file)).not.toContain(file);
  expect(fs.readFileSync(file, "utf8")).toBe(body);
  expect(probe.calls.filter(call => call.target === file)).toEqual([expect.objectContaining({ removed: false,
    reason: expect.stringMatching(/replaced since judgment/) })]);
};

beforeEach(() => {
  probe.dryRun = true;
  probe.calls.length = 0;
  primitive.length = 0;
  for (const key of globalsKeys) { registry[key]?.dispose(); delete registry[key]; }
  const record = (file: fs.PathLike, options?: unknown): void => { primitive.push({ file: String(file), options }); };
  vi.spyOn(fs, "rmSync").mockImplementation(record as typeof fs.rmSync);
  vi.spyOn(fs, "unlinkSync").mockImplementation(record as typeof fs.unlinkSync);
});
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(process, "platform", platform);
  for (const key of globalsKeys) { registry[key]?.dispose(); delete registry[key]; }
});
// Test fixtures only (our own mkdtemp roots), with the real primitive restored.
afterAll(() => { for (const root of temps.splice(0)) rmSync(root, { recursive: true, force: true }); });

// Linux only: off Linux the accepted #7858 fallback is path-based (no inode check) and its dry run verifies nothing.
describe.runIf(process.platform === "linux")("mesh judge-then-delete through verifiedRemove (smarty-dev#8907)", () => {
  it("census prune keeps a record swapped in after the dead-writer judgment", () => {
    const file = censusRecord();
    const live = record(process.pid);
    afterLiveness(() => swap(file, live));
    pruneDeadCensusRecords(path.dirname(path.dirname(file)));
    refusedSwap(file, live);
  });

  it("lock-stats prune keeps a temporary swapped in after the age judgment", () => {
    const { root, file } = staleStatsTemp();
    afterLstat(file, () => swap(file, "fresh"));
    flushStats(root);
    refusedSwap(file, "fresh");
  });

  it("lock-events rotation keeps a lock swapped in after the stale-lock judgment", () => {
    const directory = dir("mesh", "lock-stats");
    fs.writeFileSync(path.join(directory, LOCK_EVENTS_FILE), "x\n", { mode: 0o600 });
    const lock = path.join(directory, LOCK_EVENTS_ROTATE_LOCK);
    fs.writeFileSync(lock, "", { mode: 0o600 });
    const old = (Date.now() - 60_000) / 1000;
    fs.utimesSync(lock, old, old);
    afterLstat(lock, () => swap(lock, "holder"));
    createLockEvents("1", { rotateBytes: 1 })!.held(path.dirname(directory), "mesh-lock", "custody", 1);
    refusedSwap(lock, "holder");
  });

  it("delivery-outcome expiry keeps a partition swapped in after the expiry judgment, and its index", () => {
    const { root, file } = expiredPartition();
    afterLstat(file, () => swap(file, "late\n"));
    appendDeliveryOutcome(root, send, "delivered", "ok", Date.UTC(2026, 9, 13));
    refusedSwap(file, "late\n");
  });

  it("removes (dry run) the unchanged judged inode with its judgment stat, never by a primitive", () => {
    const census = censusRecord();
    const stats = staleStatsTemp();
    const outcomes = expiredPartition();
    const inodes = [census, stats.file, outcomes.file].map(file => fs.lstatSync(file).ino);
    pruneDeadCensusRecords(path.dirname(path.dirname(census)));
    flushStats(stats.root);
    appendDeliveryOutcome(outcomes.root, send, "delivered", "ok", Date.UTC(2026, 9, 13));
    expect(primitive).toEqual([]);
    for (const [index, file] of [census, stats.file, outcomes.file].entries()) {
      expect(probe.calls.filter(call => call.target === file)).toEqual([expect.objectContaining({ removed: true, ino: inodes[index] })]);
      expect(fs.existsSync(file)).toBe(true);
    }
  });

  it("never removes graveyards or directories under removable names (file-only, as main)", () => {
    const stats = staleStatsTemp();
    const census = censusRecord();
    const outcomes = expiredPartition();
    const old = (Date.now() - 25 * 60 * 60_000) / 1000;
    const dirs = [path.join(path.dirname(stats.file), ".retention-graveyard-1"), path.join(path.dirname(stats.file), "gone-2.json.ab12.tmp"),
      path.join(path.dirname(stats.file), "gone-3.json"), path.join(path.dirname(census), ".retention-graveyard-1"),
      path.join(path.dirname(outcomes.file), ".retention-graveyard-1"), path.join(path.dirname(outcomes.file), "20200102.jsonl")];
    for (const directory of dirs) {
      fs.mkdirSync(directory, { mode: 0o700 });
      fs.writeFileSync(path.join(directory, "held"), "x", { mode: 0o600 });
      fs.utimesSync(directory, old, old);
    }
    flushStats(stats.root);
    const report = censusSync(path.dirname(path.dirname(census)));
    appendDeliveryOutcome(outcomes.root, send, "delivered", "ok", Date.UTC(2026, 9, 13));
    expect(probe.calls.map(call => call.target).filter(target => dirs.includes(target))).toEqual([]);
    expect(primitive.filter(item => dirs.includes(item.file))).toEqual([]);
    expect(report.unknown.map(item => item.name)).toContain(".retention-graveyard-1");
    for (const directory of dirs) expect(fs.existsSync(path.join(directory, "held"))).toBe(true);
  });
});

// Platform-neutral: no inode guarantee is asserted off Linux, only that main's primitive is the fallback and that its
// failure is reported as not removed. On Linux the platform is stubbed to darwin; elsewhere the real platform applies.
describe("mesh deleters off Linux (smarty-dev#7858 fallback)", () => {
  it("runs main's same primitive as the fallback and reports its failure as not removed", () => {
    probe.dryRun = false;
    if (process.platform === "linux") Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
    const first = expiredPartition();
    appendDeliveryOutcome(first.root, send, "delivered", "ok", Date.UTC(2026, 9, 13));
    expect(primitive).toEqual([{ file: first.file, options: { force: true } }]);
    expect(probe.calls).toEqual([expect.objectContaining({ target: first.file, deleter: "deliveryOutcomeExpiry", removed: true })]);

    vi.mocked(fs.rmSync).mockImplementation(() => { throw Object.assign(new Error("busy"), { code: "EBUSY" }); });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const second = expiredPartition();
    appendDeliveryOutcome(second.root, send, "delivered", "ok", Date.UTC(2026, 9, 13));
    expect(probe.calls.at(-1)).toEqual(expect.objectContaining({ target: second.file, removed: false,
      reason: expect.stringMatching(/removal failed/) }));
  });
});
