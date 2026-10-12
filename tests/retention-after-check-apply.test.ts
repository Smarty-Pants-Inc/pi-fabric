import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";

// pi-fabric#1005 CODE_REVIEW P1 (c6132012432) and code-security-v2 finding 2: after-check --apply coverage. A swap of the
// registry/root namespace (or a hold) that lands AFTER the caller's final check and BEFORE the helper's filesystem
// action must retain everything: run removal and session-backup pruning alike.
//
// Safety: every destructive product call (fs.unlinkSync, rmdirSync, rmSync, promises.rm) is a RECORDER that deletes
// nothing; a rename onto an existing destination is suppressed (EEXIST) and recorded. Fixture manipulation uses the
// original fs functions captured below; fixture cleanup (own mkdtemp roots only) runs after every spy is restored.

const real = {
  unlinkSync: fs.unlinkSync, rmdirSync: fs.rmdirSync, rmSync: fs.rmSync, renameSync: fs.renameSync,
  openSync: fs.openSync, fstatSync: fs.fstatSync, readdirSync: fs.readdirSync, lstatSync: fs.lstatSync,
  readlinkSync: fs.readlinkSync, promisesRm: fs.promises.rm,
};
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();
const SESSION = "01a0cd9c-dead-session";
const linux = process.platform === "linux" && typeof process.getuid === "function";

const roots: string[] = [];
const spies: MockInstance[] = [];
let stackLimit = Error.stackTraceLimit;
beforeEach(() => { stackLimit = Error.stackTraceLimit; Error.stackTraceLimit = 200; });
afterEach(() => {
  for (const spy of spies.splice(0)) spy.mockRestore();
  vi.restoreAllMocks();
  Error.stackTraceLimit = stackLimit;
  // Own temp roots only, after the recorders are gone.
  for (const root of roots.splice(0)) real.rmSync(root, { recursive: true, force: true });
});

const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 }); fs.writeFileSync(file, data, { mode: 0o600 }); };
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const id = (stat: fs.Stats) => `${stat.dev}:${stat.ino}`;
const idOf = (file: string) => id(real.lstatSync(file));
const run = (directory: string, status: Record<string, unknown>) => {
  write(path.join(directory, "status.json"), JSON.stringify({ transport: "process", sessionId: "2147483647", ...status }));
  write(path.join(directory, "events.jsonl"), '{"sequence":1}\n');
  write(path.join(directory, "reply.json"), '{"text":"done"}');
  return directory;
};
/** Every inode id below (and including) `root`. */
const ids = (root: string): string[] => {
  const stat = real.lstatSync(root);
  return [id(stat), ...(stat.isDirectory() ? (real.readdirSync(root) as string[]).flatMap(name => ids(path.join(root, name))) : [])];
};

/** A dead Main's session registry (no resident root, mesh-wide branch), a valid state.json, three dated backups with
 * old trusted mtimes (the two oldest prunable), the latest run retained; with `oldRun`, an 8-day-old terminal run. */
const mesh = (oldRun: boolean) => {
  const root = tmp("fabric-after-check-");
  write(path.join(root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
  const registry = path.join(root, "actors", SESSION);
  const actor = path.join(registry, "actor");
  const row = { id: "actor", rootId: `session:${SESSION}`, status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest" };
  const registryText = JSON.stringify({ actors: [row] });
  write(path.join(registry, "actors.json"), registryText);
  const runs = path.join(actor, "runs");
  const latest = run(path.join(runs, "latest"), { status: "completed", finishedAt: NOW - 9 * DAY });
  const old = oldRun ? run(path.join(runs, "old"), { status: "completed", finishedAt: NOW - 8 * DAY }) : undefined;
  const backups = ["20260920T150000000Z", "20260921T150000000Z", "20260922T150000000Z"].map((stamp, order) => {
    const file = path.join(actor, `session.jsonl.${stamp}.bak`);
    write(file, stamp);
    fs.utimesSync(file, 1_000 + order, 1_000 + order);
    return file;
  });
  write(path.join(actor, "session.jsonl"), "live");
  return { root, registry, actor, runs, latest, old, backups, row, registryText };
};

type Record_ = { op: string; raw: string; resolved: string; id?: string; inHelper: boolean };
/** The recorders and the after-final-check seam. */
const instrument = (options: {
  actorsRoot: string; holdFile: string;
  /** The candidate's actual parent (runs/ for a run, the actor home for a backup): its dev:ino. */
  parentId: string;
  /** run: arm after the caller's final reference check runs inside the helper (proceed); backup: arm when the helper
   * opens the parent after the caller's stop gate was asked. */
  mode: "run" | "backup";
  inject?: () => void;
}) => {
  const records: Record_[] = [];
  const seam = { parentFd: undefined as number | undefined, parentOpenStack: "", gateChecks: 0, gateChecksBeforeOpen: 0,
    finalChecksAfterOpen: 0, fired: 0, firedStack: "", parentIdAtFire: "", firedFd: -1 };
  const stack = () => new Error().stack ?? "";
  const inHelper = (text: string) => /\bverifiedRemove\b/.test(text);
  const inCallerCheck = (text: string) => /\b(referencedNow|registryRootsChanged|gate|applyRefusal)\b/.test(text);
  const resolve = (raw: string): string => {
    const match = /^\/proc\/self\/fd\/(\d+)(?:\/(.*))?$/.exec(raw);
    if (!match) return raw;
    try { const dir = real.readlinkSync(`/proc/self/fd/${match[1]}`); return match[2] ? path.join(dir, match[2]) : dir; }
    catch { return raw; }
  };
  const record = (op: string, file: fs.PathLike) => {
    const raw = String(file);
    let identity: string | undefined;
    try { identity = idOf(raw); } catch { /* gone */ }
    records.push({ op, raw, resolved: resolve(raw), ...(identity ? { id: identity } : {}), inHelper: inHelper(stack()) });
  };
  const spy = <T extends object, K extends keyof T>(target: T, key: K, impl: unknown) => {
    spies.push(vi.spyOn(target as any, key as any).mockImplementation(impl as any));
  };
  spy(fs, "unlinkSync", (file: fs.PathLike) => { record("unlink", file); });
  spy(fs, "rmdirSync", (file: fs.PathLike) => { record("rmdir", file); });
  spy(fs, "rmSync", (file: fs.PathLike) => { record("rm", file); });
  spy(fs.promises, "rm", async (file: fs.PathLike) => { record("promises.rm", file); });
  spy(fs, "renameSync", (from: fs.PathLike, to: fs.PathLike) => {
    let exists = false;
    try { real.lstatSync(to); exists = true; } catch { /* free */ }
    if (exists) {
      record("rename-overwrite-suppressed", to);
      throw Object.assign(new Error(`EEXIST: suppressed overwriting rename onto ${String(to)}`), { code: "EEXIST" });
    }
    return real.renameSync(from, to);
  });
  spy(fs, "lstatSync", (file: fs.PathLike, ...rest: unknown[]) => {
    if (String(file) === options.holdFile) seam.gateChecks++;
    return (real.lstatSync as (...args: unknown[]) => unknown)(file, ...rest);
  });
  spy(fs, "openSync", (file: fs.PathLike, ...rest: unknown[]) => {
    const fd = (real.openSync as (...args: unknown[]) => number)(file, ...rest);
    if (seam.parentFd === undefined && seam.fired === 0) {
      const text = stack();
      if (inHelper(text)) {
        try {
          const stat = real.fstatSync(fd);
          if (stat.isDirectory() && id(stat) === options.parentId) {
            seam.parentFd = fd; seam.parentOpenStack = text; seam.gateChecksBeforeOpen = seam.gateChecks;
          }
        } catch { /* not ours */ }
      }
    }
    return fd;
  });
  spy(fs, "readdirSync", (file: fs.PathLike, ...rest: unknown[]) => {
    if (seam.parentFd !== undefined && seam.fired === 0 && String(file) === options.actorsRoot) {
      const text = stack();
      // The CLI's final reference check (referencedNow -> registryRootsChanged) running inside the helper's proceed().
      if (inHelper(text) && /\breferencedNow\b/.test(text)) seam.finalChecksAfterOpen++;
    }
    return (real.readdirSync as (...args: unknown[]) => unknown)(file, ...rest);
  });
  spy(fs, "fstatSync", (fd: number, ...rest: unknown[]) => {
    if (seam.fired === 0 && seam.parentFd !== undefined) {
      const armed = options.mode === "run" ? seam.finalChecksAfterOpen > 0 : seam.gateChecksBeforeOpen > 0;
      const text = armed ? stack() : "";
      if (armed && inHelper(text) && !inCallerCheck(text)) {
        seam.fired++; seam.firedStack = text; seam.firedFd = fd;
        seam.parentIdAtFire = id(real.fstatSync(seam.parentFd));
        options.inject?.();
      }
    }
    return (real.fstatSync as (...args: unknown[]) => unknown)(fd, ...rest);
  });
  /** Tree-entry deletions: every recorded unlink/rmdir/rm except the rmdir of an (empty) graveyard container. */
  const treeDeletes = () => records.filter(item => item.op !== "rename-overwrite-suppressed" &&
    !(item.op === "rmdir" && /^\.retention-graveyard-\d+$/.test(path.basename(item.resolved))));
  return { records, seam, treeDeletes };
};

/** Swap the DIRECT actors/<session> registry directory: the original moves to an own root outside actors/ (with the
 * actor homes and every candidate below it), a same-named replacement holds a fake actors.json with the old rows, and
 * the moved original registry is rewritten (`rows`). Fixture-only rename/mkdir/write. */
const swapRegistry = (fixture: ReturnType<typeof mesh>, rows: Record<string, unknown>[]) => {
  const outside = tmp("fabric-after-check-moved-");
  const moved = path.join(outside, SESSION);
  real.renameSync(fixture.registry, moved);
  fs.mkdirSync(fixture.registry, { mode: 0o700 });
  fs.writeFileSync(path.join(fixture.registry, "actors.json"), fixture.registryText, { mode: 0o600 });
  fs.writeFileSync(path.join(moved, "actors.json"), JSON.stringify({ actors: rows }), { mode: 0o600 });
  return moved;
};
const sweep = (root: string) => sweepMeshRetention(root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
const expectSeamAfterFinalCheck = (seam: ReturnType<typeof instrument>["seam"], parentId: string) => {
  expect(seam.fired).toBe(1);
  // Landed inside the helper, outside every caller check, with the helper's parent fd still on the judged parent.
  expect(seam.firedStack).toMatch(/\bverifiedRemove\b/);
  expect(seam.firedStack).not.toMatch(/\b(referencedNow|registryRootsChanged|discover)\b/);
  expect(seam.parentOpenStack).toMatch(/\bverifiedRemove\b/);
  expect(seam.parentIdAtFire).toBe(parentId);
};

describe.skipIf(!linux)("retention --apply after the caller's final check (pi-fabric#1005 P1)", () => {
  it("control: an unchanged run plan is removed (recorded); latest run and newest backup are retained", async () => {
    const fixture = mesh(true);
    const parentId = idOf(fixture.runs);
    const oldIds = ids(fixture.old!), latestIds = ids(fixture.latest), newest = idOf(fixture.backups[2]!);
    const backupIds = fixture.backups.slice(0, 2).map(idOf);
    const tools = instrument({ actorsRoot: path.join(fixture.root, "actors"), holdFile: path.join(fixture.root, ".mesh-retention-hold.json"), parentId, mode: "run" });
    const applied = await sweep(fixture.root);
    expectSeamAfterFinalCheck(tools.seam, parentId);
    expect(applied.skipped).toEqual([]);
    expect(applied.removedRuns).toEqual([fixture.old]);
    const deleted = new Set(tools.treeDeletes().map(item => item.id));
    for (const each of [...oldIds, ...backupIds]) expect(deleted.has(each)).toBe(true);
    for (const each of [...latestIds, newest]) expect(deleted.has(each)).toBe(false);
    expect(tools.treeDeletes().every(item => item.inHelper)).toBe(true);
    expect(tools.records.filter(item => item.op === "rename-overwrite-suppressed")).toEqual([]);
  });

  it("control: an unchanged backup plan prunes the two older backups (recorded) and keeps the newest", async () => {
    const fixture = mesh(false);
    const parentId = idOf(fixture.actor);
    const backupIds = fixture.backups.map(idOf);
    const tools = instrument({ actorsRoot: path.join(fixture.root, "actors"), holdFile: path.join(fixture.root, ".mesh-retention-hold.json"), parentId, mode: "backup" });
    const applied = await sweep(fixture.root);
    expectSeamAfterFinalCheck(tools.seam, parentId);
    expect(tools.seam.gateChecksBeforeOpen).toBeGreaterThan(0);
    expect(applied.removedRuns).toEqual([]);
    expect(applied.changes.map(change => change.path).sort()).toEqual(fixture.backups.slice(0, 2));
    expect(tools.treeDeletes().map(item => item.id).sort()).toEqual(backupIds.slice(0, 2).sort());
  });

  it("retains the run when actors/<session> is swapped after the final reference check (original now references it)", async () => {
    const fixture = mesh(true);
    const parentId = idOf(fixture.runs);
    const candidateIds = new Set([...ids(fixture.old!), ...fixture.backups.map(idOf)]);
    let moved = "";
    const tools = instrument({ actorsRoot: path.join(fixture.root, "actors"), holdFile: path.join(fixture.root, ".mesh-retention-hold.json"), parentId, mode: "run",
      // The original registry (with the candidate's real parent below it) leaves actors/; its rows now reference the
      // candidate as latest and in flight. The path-based checks would read the replacement's old rows.
      inject: () => { moved = swapRegistry(fixture, [{ ...fixture.row, status: "running", lastRunId: "old",
        inFlightRun: { id: "old", startedAt: NOW, ageS: 0 } }]); } });
    const applied = await sweep(fixture.root);
    expectSeamAfterFinalCheck(tools.seam, parentId);
    expect(moved).not.toBe("");
    // Nothing of the tree may be unlinked or rmdir'd: not the judged run, not a later candidate (sticky).
    expect(tools.treeDeletes()).toEqual([]);
    expect(tools.treeDeletes().filter(item => item.id && candidateIds.has(item.id))).toEqual([]);
    expect(applied.removedRuns).not.toContain(fixture.old);
    expect(applied.changes).toEqual([]);
  });

  it("retains session backups when actors/<session> is swapped after the stop gate (original shows an active writer)", async () => {
    const fixture = mesh(false);
    const parentId = idOf(fixture.actor);
    let moved = "";
    const tools = instrument({ actorsRoot: path.join(fixture.root, "actors"), holdFile: path.join(fixture.root, ".mesh-retention-hold.json"), parentId, mode: "backup",
      inject: () => { moved = swapRegistry(fixture, [{ ...fixture.row, status: "running", inFlightRun: { id: "next", startedAt: NOW, ageS: 0 } }]); } });
    const applied = await sweep(fixture.root);
    expectSeamAfterFinalCheck(tools.seam, parentId);
    expect(tools.seam.gateChecksBeforeOpen).toBeGreaterThan(0);
    expect(moved).not.toBe("");
    expect(tools.treeDeletes()).toEqual([]);
    expect(applied.changes).toEqual([]);
    for (const backup of fixture.backups) expect(fs.existsSync(path.join(moved, path.relative(fixture.registry, backup)))).toBe(true);
  });

  it("retains session backups when a hold appears after the stop gate passed, before the helper acts", async () => {
    const fixture = mesh(false);
    const parentId = idOf(fixture.actor);
    const hold = path.join(fixture.root, ".mesh-retention-hold.json");
    let held = false;
    const tools = instrument({ actorsRoot: path.join(fixture.root, "actors"), holdFile: hold, parentId, mode: "backup",
      inject: () => { fs.writeFileSync(hold, "{}", { mode: 0o600 }); held = true; } });
    const applied = await sweep(fixture.root);
    expectSeamAfterFinalCheck(tools.seam, parentId);
    expect(tools.seam.gateChecksBeforeOpen).toBeGreaterThan(0);
    expect(held).toBe(true);
    // A hold means --apply changes nothing: the helper must re-check the gate inside its fd lifespan.
    expect(tools.treeDeletes()).toEqual([]);
    expect(applied.changes).toEqual([]);
    for (const backup of fixture.backups) expect(fs.existsSync(backup)).toBe(true);
  });
});
