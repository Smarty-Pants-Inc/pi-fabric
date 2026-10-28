import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// pi-fabric#1014 SEC P2-1 (smarty-dev#10678): the exported sweepMeshRetention refuses dryRun:false at its own boundary,
// not only in the CLI. Safe by construction, even against code without the boundary: every deleter and verifiedRemove
// is wrapped to record its calls and to call through only for an explicit dry run; destructive fs calls and process
// signals are recorded and never executed. Fixtures are written before the spies; cleanup runs after they are restored.

type Call = { name: string; args: unknown[] };
const calls: Call[] = [];
const dryRunOf = (value: unknown): unknown => (value as { dryRun?: unknown } | undefined)?.dryRun;
// Calls through only when the call itself is an explicit dry run; anything else is recorded and returns a no-op.
const guarded = <F extends (...args: any[]) => any>(name: string, actual: F, optionsAt: number, noop: () => ReturnType<F>) =>
  ((...args: Parameters<F>): ReturnType<F> => {
    calls.push({ name, args });
    return dryRunOf(args[optionsAt]) === true ? actual(...args) : noop();
  }) as F;

vi.mock("../src/storage/retention.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/retention.js")>();
  return {
    ...actual,
    pruneActorRunArchives: guarded("pruneActorRunArchives", actual.pruneActorRunArchives, 0, () => []),
    pruneActorSessionBackups: guarded("pruneActorSessionBackups", actual.pruneActorSessionBackups, 1, () => []),
    compactTerminalRunEvents: guarded("compactTerminalRunEvents", actual.compactTerminalRunEvents, 1, () => false),
  };
});
vi.mock("../src/residency/retention.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/residency/retention.js")>();
  return { ...actual, sweepResidentRuns: guarded("sweepResidentRuns", actual.sweepResidentRuns, 3, () => []) };
});
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return {
    ...actual,
    verifiedRemove: guarded("verifiedRemove", actual.verifiedRemove, 2, () => ({ removed: false, reason: "test: recorded, not executed" }) as never),
    startRemovalTally: (...args: Parameters<typeof actual.startRemovalTally>) => { calls.push({ name: "startRemovalTally", args }); return actual.startRemovalTally(...args); },
    setWindowsVerifiedRemoval: (on: boolean) => { calls.push({ name: "setWindowsVerifiedRemoval", args: [on] }); actual.setWindowsVerifiedRemoval(on); },
  };
});

const { sweepMeshRetention, MeshRetentionApplyDisabledError, MESH_RETENTION_APPLY_DISABLED_CODE } = await import("../src/storage/retention-cli.js");

const DELETERS = ["pruneActorRunArchives", "pruneActorSessionBackups", "sweepResidentRuns", "compactTerminalRunEvents", "verifiedRemove"];
const DESTRUCTIVE = ["rmSync", "rmdirSync", "unlinkSync", "renameSync", "truncateSync", "ftruncateSync", "writeFileSync", "writeSync",
  "appendFileSync", "mkdirSync", "mkdtempSync", "copyFileSync", "linkSync", "symlinkSync", "chmodSync", "chownSync", "utimesSync", "lutimesSync"] as const;
const READS = ["lstatSync", "statSync", "readdirSync", "readFileSync", "openSync", "existsSync", "accessSync", "realpathSync"] as const;
const WRITE_FLAGS = fs.constants.O_WRONLY | fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_APPEND;
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();

const roots: string[] = [];
const destructive: Call[] = [];
const meshReads: Call[] = [];
const signals: Call[] = [];
let mesh = "";
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const tree = (root: string): unknown[] => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name); const stat = fs.lstatSync(file);
  return [name, stat.ino, stat.size, stat.mtimeMs, stat.ctimeMs, stat.isDirectory() ? tree(file) : fs.readFileSync(file, "utf8")];
});

/** A dead Main's registry (no resident root, so no host flock child process): one expired unreferenced run, its latest
 * run, and two rotation backups. A dry run plans the old run and the older backup. */
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-apply-boundary-")); roots.push(root);
  const registry = path.join(root, "actors", "01a0cd9c-dead-session");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{
    id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest" }] }));
  const run = (id: string, finishedAt: number) => {
    const directory = path.join(actor, "runs", id);
    write(path.join(directory, "status.json"), JSON.stringify({ transport: "process", sessionId: "2147483647", status: "completed", finishedAt }));
    write(path.join(directory, "task.txt"), "generic retention fixture\n");
    write(path.join(directory, "reply.json"), '{"text":"done"}');
    return directory;
  };
  const old = run("old", NOW - 8 * DAY);
  const latest = run("latest", NOW - 9 * DAY);
  const backups = ["20260920T150000000Z", "20260921T150000000Z"].map((stamp, order) => {
    const file = path.join(actor, `session.jsonl.${stamp}.bak`);
    write(file, stamp);
    fs.utimesSync(file, 1_000 + order, 1_000 + order);
    return file;
  });
  write(path.join(actor, "session.jsonl"), "live");
  // A valid mesh state, so code without the boundary passes its state gate and reaches the deleters (the RED probe).
  write(path.join(root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
  return { root, actor, old, latest, backups };
};

beforeEach(() => {
  calls.length = 0; destructive.length = 0; meshReads.length = 0; signals.length = 0; mesh = "";
  const fsAny = fs as unknown as Record<string, (...args: unknown[]) => unknown>;
  for (const name of DESTRUCTIVE) vi.spyOn(fsAny, name).mockImplementation((...args: unknown[]) => {
    destructive.push({ name, args });
    throw Object.assign(new Error(`test: ${name} recorded, not executed`), { code: "EPERM" });
  });
  for (const name of READS) {
    const original = fsAny[name]!.bind(fs);
    vi.spyOn(fsAny, name).mockImplementation((...args: unknown[]) => {
      const target = typeof args[0] === "string" ? args[0] : String(args[0]);
      if (mesh && (target === mesh || target.startsWith(mesh + path.sep))) meshReads.push({ name, args });
      if (name === "openSync") {
        const flags = args[1];
        if ((typeof flags === "number" && (flags & WRITE_FLAGS) !== 0) || (typeof flags === "string" && flags !== "r")) {
          destructive.push({ name, args });
          throw Object.assign(new Error("test: writable open recorded, not executed"), { code: "EPERM" });
        }
      }
      return original(...args);
    });
  }
  const kill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation(((pid: number, signal?: string | number) => {
    signals.push({ name: "kill", args: [pid, signal] });
    if (signal !== 0) throw Object.assign(new Error("test: signal recorded, not delivered"), { code: "EPERM" });
    return kill(pid, 0);
  }) as typeof process.kill);
});
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform !== "linux")("sweepMeshRetention apply boundary (smarty-dev#10678)", () => {
  it("rejects dryRun:false with the typed error before any tally, Windows mode, mesh read, signal or deleter", async () => {
    vi.restoreAllMocks(); // the fixture is written unobserved; observation starts below
    const { root, old, backups } = fixture();
    const before = tree(root);
    calls.length = 0;
    const observed = { destructive: [] as string[], reads: [] as string[] };
    const fsAny = fs as unknown as Record<string, (...args: unknown[]) => unknown>;
    for (const name of DESTRUCTIVE) vi.spyOn(fsAny, name).mockImplementation((...args: unknown[]) => {
      observed.destructive.push(`${name} ${String(args[0])}`);
      throw Object.assign(new Error(`test: ${name} recorded, not executed`), { code: "EPERM" });
    });
    for (const name of READS) {
      const original = fsAny[name]!.bind(fs);
      vi.spyOn(fsAny, name).mockImplementation((...args: unknown[]) => {
        if (String(args[0]).startsWith(root)) observed.reads.push(`${name} ${String(args[0])}`);
        if (name === "openSync" && ((typeof args[1] === "number" && (args[1] & WRITE_FLAGS) !== 0) || (typeof args[1] === "string" && args[1] !== "r"))) {
          observed.destructive.push(`openSync ${String(args[0])}`);
          throw Object.assign(new Error("test: writable open recorded, not executed"), { code: "EPERM" });
        }
        return original(...args);
      });
    }
    const kills: unknown[] = [];
    vi.spyOn(process, "kill").mockImplementation(((pid: number, signal?: string | number) => { kills.push([pid, signal]); return true; }) as typeof process.kill);

    let error: unknown;
    try { await sweepMeshRetention(root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY }); }
    catch (caught) { error = caught; }
    const record = { calls: calls.map(call => call.name), destructive: observed.destructive, reads: observed.reads, kills };
    vi.restoreAllMocks();

    expect(error, JSON.stringify(record)).toBeInstanceOf(MeshRetentionApplyDisabledError);
    expect(error).toMatchObject({ name: "MeshRetentionApplyDisabledError", code: MESH_RETENTION_APPLY_DISABLED_CODE, issue: "smarty-dev#10678" });
    expect(MESH_RETENTION_APPLY_DISABLED_CODE).toBe("ERR_MESH_RETENTION_APPLY_DISABLED");
    expect((error as Error).message).toMatch(/smarty-dev#10678/);
    // Nothing armed: no deleter, no tally, no Windows removal mode, no mesh read, no signal, no destructive fs call.
    expect(record.calls.filter(name => DELETERS.includes(name))).toEqual([]);
    expect(record.calls).toEqual([]);
    expect(record.reads).toEqual([]);
    expect(record.kills).toEqual([]);
    expect(record.destructive).toEqual([]);
    expect(tree(root)).toEqual(before);
    expect(fs.existsSync(path.join(old, "reply.json"))).toBe(true);
    for (const backup of backups) expect(fs.existsSync(backup)).toBe(true);
  });

  it("rejects dryRun:false with no options beyond it, for a mesh root that does not exist", async () => {
    const missing = path.join(os.tmpdir(), `fabric-apply-boundary-missing-${process.pid}-${NOW}`);
    mesh = missing;
    await expect(sweepMeshRetention(missing, { dryRun: false })).rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
    expect(calls).toEqual([]);
    expect(meshReads).toEqual([]);
    expect(destructive).toEqual([]);
  });

  it.each([["default", undefined], ["true", true]] as const)("a %s dryRun still plans the eligible run and backup without mutating", async (_name, dryRun) => {
    vi.restoreAllMocks();
    const { root, old, latest, backups } = fixture();
    const before = tree(root);
    // Re-arm the recorders for the sweep itself (the fixture was written unobserved).
    const fsAny = fs as unknown as Record<string, (...args: unknown[]) => unknown>;
    for (const name of DESTRUCTIVE) vi.spyOn(fsAny, name).mockImplementation((...args: unknown[]) => {
      destructive.push({ name, args });
      throw Object.assign(new Error(`test: ${name} recorded, not executed`), { code: "EPERM" });
    });
    const open = fs.openSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags?: fs.OpenMode, ...rest: unknown[]) => {
      if ((typeof flags === "number" && (flags & WRITE_FLAGS) !== 0) || (typeof flags === "string" && flags !== "r")) {
        destructive.push({ name: "openSync", args: [file, flags] });
        throw Object.assign(new Error("test: writable open recorded, not executed"), { code: "EPERM" });
      }
      return (open as (...args: unknown[]) => number)(file, flags, ...rest);
    }) as typeof fs.openSync);
    const kill = process.kill.bind(process);
    vi.spyOn(process, "kill").mockImplementation(((pid: number, signal?: string | number) => {
      signals.push({ name: "kill", args: [pid, signal] });
      if (signal !== 0) throw Object.assign(new Error("test: signal recorded, not delivered"), { code: "EPERM" });
      return kill(pid, 0);
    }) as typeof process.kill);
    calls.length = 0;

    const plan = await sweepMeshRetention(root, { now: NOW, runRetentionMs: 7 * DAY, ...(dryRun === undefined ? {} : { dryRun }) });
    const record = { destructive: destructive.map(call => `${call.name} ${String(call.args[0])}`), signals: signals.map(call => call.args) };
    vi.restoreAllMocks();

    expect(plan.dryRun).toBe(true);
    expect(plan.removedRuns).toEqual([old]);
    expect(plan.changes.map(change => change.path)).toContain(backups[0]);
    expect(plan.changes.map(change => change.path)).not.toContain(backups[1]);
    expect(plan.removedRuns).not.toContain(latest);
    expect(plan.summary.applyRefusal).toBeUndefined();
    expect(Object.values(plan.summary.deleters).reduce((sum, item) => sum + item.removed, 0)).toBeGreaterThan(0);
    // The deleters ran, and every call was an explicit dry run.
    const deleterCalls = calls.filter(call => DELETERS.includes(call.name));
    expect(deleterCalls.map(call => call.name)).toEqual(expect.arrayContaining(["pruneActorRunArchives", "pruneActorSessionBackups"]));
    for (const call of deleterCalls) {
      const at = { pruneActorRunArchives: 0, pruneActorSessionBackups: 1, compactTerminalRunEvents: 1, sweepResidentRuns: 3, verifiedRemove: 2 }[call.name]!;
      expect(dryRunOf(call.args[at]), call.name).toBe(true);
    }
    expect(record.destructive).toEqual([]);
    expect(record.signals.every(([, signal]) => signal === 0)).toBe(true);
    expect(tree(root)).toEqual(before);
  });

  it("a getter that later answers false never reaches a deleter as an apply", async () => {
    vi.restoreAllMocks();
    const { root } = fixture();
    const before = tree(root);
    calls.length = 0;
    let reads = 0;
    const options = { now: NOW, runRetentionMs: 7 * DAY, get dryRun() { return reads++ === 0; } };
    const plan = await sweepMeshRetention(root, options);
    expect(plan.dryRun).toBe(true);
    for (const call of calls.filter(item => DELETERS.includes(item.name))) {
      const at = { pruneActorRunArchives: 0, pruneActorSessionBackups: 1, compactTerminalRunEvents: 1, sweepResidentRuns: 3, verifiedRemove: 2 }[call.name]!;
      expect(dryRunOf(call.args[at]), call.name).toBe(true);
    }
    expect(tree(root)).toEqual(before);
  });
});
