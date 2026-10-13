import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FABRIC_RUN_ROOT_PREFIX, sweepTempRunRoots } from "../src/storage/retention.js";
import { startRemovalTally, stopRemovalTally, verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// smarty-dev#7916 audit gap: the run-root deleters pruneClosedRunRoot (closed roots) and sweepTempRunRoots (orphaned
// roots) hand their judged stat to verifiedRemove but had no per-deleter swap tests. Both are reached through the
// public sweepTempRunRoots and told apart by their tally deleter name. No product delete ever runs: every
// fs.unlinkSync/rmdirSync/rmSync and fs.promises.rm/unlink/rmdir call is recorded and never called through; renameSync
// calls through only for the graveyard move (a rollback or any other rename is recorded and suppressed).
const HOUR = 60 * 60 * 1_000;
const DAY = 24 * HOUR;
const DEAD_PID = 2_147_483_647; // above any pid_max: kill(pid, 0) is ESRCH, a checked dead exit
const NOW = 100 * DAY;
const roots: string[] = [];
const real = { renameSync: fs.renameSync, fstatSync: fs.fstatSync };
type Call = { op: string; args: string[] };
let calls: Call[] = [];

afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  vi.restoreAllMocks();
  stopRemovalTally();
  // Only this file's own mkdtemp roots, and only after every stub is restored.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const record = (op: string) => ((...args: unknown[]) => { calls.push({ op, args: args.filter(a => typeof a === "string" || a instanceof URL || Buffer.isBuffer(a)).map(String) }); }) as never;
const recordAsync = (op: string) => ((...args: unknown[]) => { calls.push({ op, args: args.map(String) }); return Promise.resolve(); }) as never;
const protect = (): void => {
  calls = [];
  vi.spyOn(fs, "unlinkSync").mockImplementation(record("unlinkSync"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(record("rmdirSync"));
  vi.spyOn(fs, "rmSync").mockImplementation(record("rmSync"));
  vi.spyOn(fs.promises, "rm").mockImplementation(recordAsync("promises.rm"));
  vi.spyOn(fs.promises, "unlink").mockImplementation(recordAsync("promises.unlink"));
  vi.spyOn(fs.promises, "rmdir").mockImplementation(recordAsync("promises.rmdir"));
  vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
    const graveyardMove = /^\/proc\/self\/fd\/\d+\/run$/.test(String(from)) && /^\/proc\/self\/fd\/\d+\/run\.[0-9a-f-]{36}$/.test(String(to));
    calls.push({ op: graveyardMove ? "renameSync(graveyard move, called through)" : "renameSync(suppressed)", args: [String(from), String(to)] });
    if (graveyardMove) real.renameSync(from, to);
  }) as typeof fs.renameSync);
};
const deletions = (): Call[] => calls.filter(call => !call.op.startsWith("renameSync"));

const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isSymbolicLink()) return ["link", fs.readlinkSync(file)];
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};

type Kind = "closed" | "orphan";
const DELETER: Record<Kind, string> = { closed: "pruneClosedRunRoot", orphan: "sweepTempRunRoots" };
/** An owned, closed (0700) temp root with one expired, terminal process run whose saved worker PID is dead. */
const fixture = (kind: Kind) => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-runroot-swaps-"));
  roots.push(tempRoot);
  const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}${kind}`);
  const run = path.join(root, "run");
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  fs.chmodSync(root, 0o700); fs.chmodSync(run, 0o700);
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id: "run", status: "completed", finishedAt: 1, transport: "process", sessionId: String(DEAD_PID) }), { mode: 0o600 });
  fs.writeFileSync(path.join(run, "events.jsonl"), '{"sequence":0}\n', { mode: 0o600 });
  fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: DEAD_PID, startedAt: 1, heartbeatAt: 1,
    ...(kind === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }), { mode: 0o600 });
  // Old enough that only the judged mtime (not a fresh one) is eligible either way.
  const old = new Date(Date.now() - 30 * DAY);
  fs.utimesSync(run, old, old);
  const sweep = () => sweepTempRunRoots({ tempRoot, now: NOW, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });
  return { tempRoot, root, run, sweep, judged: shape(run), ino: fs.lstatSync(run).ino };
};

type Swap = "replaced inode" | "symlink" | "foreign owner" | "refreshed mtime";
const SWAPS: Swap[] = ["replaced inode", "symlink", "foreign owner", "refreshed mtime"];
const REASON: Record<Swap, RegExp> = {
  "replaced inode": /^target inode was replaced since judgment$/,
  // O_PATH|O_NOFOLLOW|O_DIRECTORY on a symbolic link fails ENOTDIR: refused before any open succeeds.
  symlink: /^type changed since judgment$/,
  "foreign owner": /^target is not owned by this user$/,
  "refreshed mtime": /^target was modified since judgment$/,
};

describe.skipIf(process.platform !== "linux")("run-root deleters remove only the judged run (smarty-dev#7916)", () => {
  describe.each(["closed", "orphan"] as Kind[])("%s root", (kind) => {
    const deleter = DELETER[kind];

    it(`positive control: ${deleter} removes the unchanged eligible run (recorded, never called through)`, () => {
      const f = fixture(kind);
      const hooked: string[] = [];
      verifiedRemoveTestHooks.beforeOpen = (target) => { hooked.push(target); };
      const tally = startRemovalTally();
      protect();
      const result = f.sweep();
      vi.restoreAllMocks();
      expect(hooked).toEqual([f.run]);
      expect(tally[deleter]).toMatchObject({ removed: 1, refused: {} });
      expect(Object.keys(tally)).toEqual([deleter]);
      expect(result.removedRuns).toEqual(kind === "closed" ? [f.run] : []);
      // The recorders never deleted: the root still holds the graveyard, so it is not reported removed.
      expect(result.removedRoots).toEqual([]);
      const renames = calls.filter(call => call.op.startsWith("renameSync"));
      expect(renames.map(call => call.op)).toEqual(["renameSync(graveyard move, called through)"]);
      const unlinks = deletions().filter(call => call.op === "unlinkSync").map(call => path.basename(call.args[0]!)).sort();
      const rmdirs = deletions().filter(call => call.op === "rmdirSync").map(call => path.basename(call.args[0]!));
      expect(unlinks).toEqual(["events.jsonl", "status.json"]);
      expect(rmdirs).toHaveLength(2);
      expect(rmdirs[0]).toMatch(/^run\.[0-9a-f-]{36}$/);
      expect(rmdirs[1]).toBe(`.retention-graveyard-${process.pid}`);
      expect(deletions().filter(call => !["unlinkSync", "rmdirSync"].includes(call.op))).toEqual([]);
      // Physically intact (moved into the graveyard, nothing deleted).
      const grave = path.join(f.root, `.retention-graveyard-${process.pid}`);
      const moved = fs.readdirSync(grave);
      expect(moved).toEqual([rmdirs[0]]);
      expect(shape(path.join(grave, moved[0]!))).toEqual(f.judged);
      expect(fs.existsSync(f.run)).toBe(false);
    });

    it.each(SWAPS)(`${deleter}: a %s swap after judgment deletes no tree entry`, (swap) => {
      const f = fixture(kind);
      const aside = path.join(f.root, "..", "judged-aside");
      const outside = path.join(f.tempRoot, "outside");
      fs.mkdirSync(outside, { mode: 0o700 });
      fs.writeFileSync(path.join(outside, "keep"), "outside");
      let swapped = 0;
      verifiedRemoveTestHooks.beforeOpen = (target) => {
        if (target !== f.run) return;
        swapped++;
        if (swap === "refreshed mtime") { fs.utimesSync(f.run, new Date(), new Date()); return; }
        if (swap === "foreign owner") {
          vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
            const stat = (real.fstatSync as (...args: unknown[]) => fs.Stats)(fd, ...rest);
            if (stat.ino !== f.ino) return stat;
            return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: stat.uid + 1 });
          }) as typeof fs.fstatSync);
          return;
        }
        // The judged tree moves aside intact (no deletion); the replacement exists before, so no inode reuse.
        real.renameSync(f.run, aside);
        if (swap === "symlink") { fs.symlinkSync(outside, f.run); return; }
        fs.mkdirSync(f.run, { mode: 0o700 });
        fs.writeFileSync(path.join(f.run, "status.json"), "fresh", { mode: 0o600 });
      };
      const tally = startRemovalTally();
      const before = { outside: shape(outside), run: swap === "foreign owner" || swap === "refreshed mtime" ? f.judged : undefined };
      protect();
      const result = f.sweep();
      vi.restoreAllMocks();
      expect(swapped).toBe(1);
      expect(calls).toEqual([]); // no unlink/rmdir/rm/promises.rm and no rename at all
      expect(result).toEqual({ removedRoots: [], removedRuns: [] });
      expect(tally[deleter]?.removed ?? 0).toBe(0);
      const refused = Object.keys(tally[deleter]?.refused ?? {});
      expect(refused).toHaveLength(1);
      expect(refused[0]).toMatch(REASON[swap]);
      expect(tally[deleter]?.graveyards).toBeUndefined();
      // The target is at its path, untouched; no graveyard was created.
      expect(fs.readdirSync(f.root).filter(name => name.startsWith(".retention-graveyard-"))).toEqual([]);
      expect(shape(outside)).toEqual(before.outside);
      if (swap === "symlink") expect(shape(f.run)).toEqual(["link", outside]);
      else if (swap === "replaced inode") expect(shape(f.run)).toEqual({ "status.json": "fresh" });
      else expect(shape(f.run)).toEqual(before.run);
      if (swap === "symlink" || swap === "replaced inode") expect(shape(aside)).toEqual(f.judged);
    });
  });
});
