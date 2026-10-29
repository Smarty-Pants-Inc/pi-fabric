import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  claimMeshRetentionSweep, compactTerminalRunEvents, FABRIC_RUN_ROOT_PREFIX, pruneActorRunArchives, pruneActorSessionBackups,
  removeEmptyRunRoot, sweepTempRunRoots,
} from "../src/storage/retention.js";
import { sweepResidentRuns } from "../src/residency/retention.js";
import { closeScratch, createScratch, sweepScratch } from "../src/storage/scratch.js";
import { MeshRetentionApplyDisabledError, sweepMeshRetention } from "../src/storage/retention-cli.js";

// smarty-dev#7916 acceptance: for each routed deleter, a swap between the judgment and the delete (file to symlink,
// a replaced inode, a foreign owner, a refreshed mtime) deletes nothing; no getuid deletes nothing; a future-dated
// backup name never costs the newest backup. The file runs unchanged on main for the RED evidence: there the swap is
// triggered by main's own removal call (rmSync/unlinkSync/renameSync/promises.rm on the target), on the head by the
// helper's seam right before its first open. Either way the swap lands after the judgment and before the delete.
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();
const DEAD = "2147483647";
const roots: string[] = [];
const restores: (() => void)[] = [];
afterEach(() => {
  for (const restore of restores.splice(0).reverse()) restore();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const LOG = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence }) + "\n").join("");
const run = (directory: string, status: Record<string, unknown> = { status: "completed", finishedAt: 1 }) => {
  write(path.join(directory, "status.json"), JSON.stringify({ transport: "process", sessionId: DEAD, ...status }));
  write(path.join(directory, "events.jsonl"), LOG);
  write(path.join(directory, "reply.json"), '{"text":"done"}');
  return directory;
};
const old = (file: string) => { const at = (NOW - 30 * DAY) / 1000; fs.utimesSync(file, at, at); return file; };
/** The whole observable tree below a path (lstat, no following), for "nothing deleted" checks. */
const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isSymbolicLink()) return ["link", fs.readlinkSync(file)];
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};

type Swap = "symlink" | "replaced inode" | "foreign owner" | "refreshed mtime";
const SWAPS: Swap[] = ["symlink", "replaced inode", "foreign owner", "refreshed mtime"];
/** Apply `swap` to `target` once, right before the removal; returns what must still exist afterwards. */
const armSwap = async (target: string, swap: Swap): Promise<() => Record<string, unknown>> => {
  const outside = tmp("fabric-swap-outside-");
  const moved = path.join(outside, "moved");
  let fired = false;
  const doSwap = () => {
    if (fired) return;
    fired = true;
    if (swap === "symlink") { fs.renameSync(target, moved); fs.symlinkSync(moved, target); }
    else if (swap === "replaced inode") { fs.cpSync(target, moved, { recursive: true }); fs.renameSync(target, path.join(outside, "original")); fs.renameSync(moved, target); }
    else if (swap === "refreshed mtime") { const at = Date.now() / 1000 + 5; fs.utimesSync(target, at, at); }
    else {
      // Another user's file: the process now runs as another uid than the target's owner.
      const real = process.geteuid!.bind(process); const uid = real() + 1;
      Object.defineProperty(process, "geteuid", { value: () => uid, configurable: true, writable: true });
      restores.push(() => Object.defineProperty(process, "geteuid", { value: real, configurable: true, writable: true }));
    }
  };
  const hooks = await import("../src/storage/verified-remove.js").then(module => module.verifiedRemoveTestHooks).catch(() => undefined);
  if (hooks) {
    hooks.beforeOpen = (file) => { if (file === target) doSwap(); };
    restores.push(() => { delete hooks.beforeOpen; });
  } else {
    // main: swap inside main's own removal call on the target, before it runs.
    const intercept = <K extends "rmSync" | "unlinkSync" | "renameSync">(name: K) => {
      const original = fs[name] as (...args: unknown[]) => unknown;
      vi.spyOn(fs, name).mockImplementation(((...args: unknown[]) => {
        if (args[0] === target || (name === "renameSync" && args[1] === target)) doSwap();
        return original(...args);
      }) as never);
    };
    intercept("rmSync"); intercept("unlinkSync"); intercept("renameSync");
    const rm = fs.promises.rm;
    vi.spyOn(fs.promises, "rm").mockImplementation(((file: fs.PathLike, options?: fs.RmOptions) => { if (file === target) doSwap(); return rm(file, options); }) as never);
  }
  return () => ({ target: shape(target), outside: shape(outside) });
};

interface Fixture { target: string; act: () => unknown; runRoot?: string; custodyArchive?: string; preserveTree?: boolean }
const FIXTURES: Record<string, () => Fixture> = {
  pruneActorRunArchives: () => {
    const runs = tmp("fabric-vr-archives-");
    const runRoot = path.join(runs, "old");
    const target = run(runRoot);
    return { target, runRoot, custodyArchive: `${runs}.trace-archive`, act: () => pruneActorRunArchives({ runsDirectory: runs, retentionMs: DAY, now: NOW }) };
  },
  sweepResidentRuns: () => {
    const residency = tmp("fabric-vr-resident-");
    const runs = path.join(residency, "runs");
    const runRoot = path.join(runs, "old");
    const target = old(run(runRoot));
    return { target, runRoot, custodyArchive: `${runs}.trace-archive`, act: () => sweepResidentRuns(runs, NOW, 10_000, { retentionMs: DAY }) };
  },
  compactTerminalRunEvents: () => {
    const directory = run(path.join(tmp("fabric-vr-compact-"), "old"));
    const target = path.join(directory, "events.jsonl");
    return { target, runRoot: directory, preserveTree: true, act: () => compactTerminalRunEvents(directory, { now: NOW, terminalRunEventsMaxBytes: 1024 }) };
  },
  "pruneClosedRunRoot (sweepTempRunRoots)": () => {
    const tempRoot = tmp("fabric-vr-closed-");
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
    const runRoot = path.join(root, "old");
    const target = run(runRoot);
    write(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: Number(DEAD), startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
    return { target, runRoot, custodyArchive: `${root}.trace-archive`, act: () => sweepTempRunRoots({ tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * 3600_000, oneShotRunRetentionMs: DAY }) };
  },
  "sweepTempRunRoots (orphaned root)": () => {
    const tempRoot = tmp("fabric-vr-orphan-");
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphan");
    const runRoot = path.join(root, "old");
    const target = run(runRoot);
    write(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: Number(DEAD), startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    return { target, runRoot, custodyArchive: `${root}.trace-archive`, act: () => sweepTempRunRoots({ tempRoot, now: NOW + 100 * DAY, orphanedTempRunRetentionMs: 6 * 3600_000, oneShotRunRetentionMs: DAY }) };
  },
  removeEmptyRunRoot: () => {
    const tempRoot = tmp("fabric-vr-empty-");
    const target = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "empty");
    write(path.join(target, ".fabric-owner.json"), JSON.stringify({ pid: Number(DEAD), startedAt: 1, heartbeatAt: 1, closedAt: 1 }));
    return { target, act: () => removeEmptyRunRoot(target) };
  },
  // smarty-dev#16444: pruneActorSessionBackups is no longer a deleter (it moves older backups into session-archive/,
  // tests/actor-session-trace-archive.test.ts), so it left this deletion matrix.
  sweepScratch: () => {
    const tempRoot = tmp("fabric-vr-scratch-");
    const target = createScratch("output", tempRoot);
    write(path.join(target, "output.txt"), "x");
    closeScratch(target);
    return { target, act: () => sweepScratch({ tempRoot, now: Date.now() + 2 * DAY }) };
  },
  claimMeshRetentionSweep: () => {
    const meshRoot = tmp("fabric-vr-claim-");
    const hour = 3600_000;
    claimMeshRetentionSweep(meshRoot, hour, 10 * hour);
    const target = path.join(meshRoot, ".mesh-retention-sweep-10.json");
    return { target, act: () => claimMeshRetentionSweep(meshRoot, hour, 12 * hour) };
  },
};
const custodyTree = (archive: string, source: string): string | undefined => {
  if (!fs.existsSync(archive)) return undefined;
  for (const name of fs.readdirSync(archive)) {
    try {
      const receipt = JSON.parse(fs.readFileSync(path.join(archive, name, "custody.json"), "utf8"));
      if (receipt.source === source && receipt.state === "moved") return receipt.destination;
    } catch { /* not a published moved receipt */ }
  }
  return undefined;
};
// The public mesh pass is not in FIXTURES: its apply rejects at the boundary until smarty-dev#10678 (pi-fabric#1014 SEC
// P2-1), so no helper seam can fire through it. The deleter it calls (pruneActorRunArchives) keeps the swap matrix above.
const meshPass = () => {
  const root = tmp("fabric-vr-mesh-");
  const registry = path.join(root, "actors", "project");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{ id: "actor", rootId: "session:dead", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest" }] }));
  write(path.join(actor, "session.jsonl"), "live");
  run(path.join(actor, "runs", "latest"));
  const target = run(path.join(actor, "runs", "old"), { status: "completed", finishedAt: NOW - 9 * DAY });
  write(path.join(root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
  return { root, target };
};

describe.skipIf(process.platform !== "linux")("verified removal: every routed retention deleter (smarty-dev#7916)", () => {
  it.each(Object.keys(FIXTURES))("%s removes its expired target when nothing changes (control)", async name => {
    const fixture = FIXTURES[name]!();
    const before = fixture.runRoot ? shape(fixture.runRoot) : undefined;
    await fixture.act();
    if (fixture.preserveTree) expect(shape(fixture.runRoot!)).toEqual(before);
    else if (fixture.runRoot && fixture.custodyArchive) {
      expect(fs.existsSync(fixture.target)).toBe(false);
      const held = custodyTree(fixture.custodyArchive, fixture.runRoot);
      expect(held).toBeDefined();
      expect(shape(held!)).toEqual(before);
    } else expect(fs.existsSync(fixture.target)).toBe(false);
  });

  const cases = Object.keys(FIXTURES).flatMap(name => SWAPS.map(swap => [name, swap] as const));
  it.each(cases)("%s deletes nothing after a swap to a %s between judgment and delete", async (name, swap) => {
    const fixture = FIXTURES[name]!();
    const before = fixture.runRoot ? shape(fixture.runRoot) : undefined;
    const observe = await armSwap(fixture.target, swap);
    await fixture.act();
    const after = observe();
    for (const restore of restores.splice(0).reverse()) restore();
    if (name === "compactTerminalRunEvents") {
      expect(after.target).toBe(LOG);
      expect(after.outside).toEqual({});
      return;
    }
    if (fixture.runRoot && fixture.custodyArchive) {
      // Raw-run deleters now atomically move the full root; replacement races at
      // that boundary are covered by retention-runroot-swaps.test.ts.
      expect(after.target, `${name} / ${swap}: source trace`).toBeNull();
      expect(after.outside, `${name} / ${swap}: no unrequested swap`).toEqual({});
      const held = custodyTree(fixture.custodyArchive, fixture.runRoot);
      expect(held).toBeDefined();
      expect(shape(held!)).toEqual(before);
      return;
    }
    // Non-run deleters still use verified removal and must leave swapped-in objects intact.
    expect(after.target, `${name} / ${swap}: target path`).not.toBeNull();
    if (swap === "symlink") expect(after.target).toEqual(["link", path.join(path.dirname(String((after.target as string[])[1])), "moved")]);
    if (swap === "symlink" || swap === "replaced inode") {
      const outside = after.outside as Record<string, unknown>;
      expect(Object.values(outside).every(value => value !== null), `${name} / ${swap}: moved original`).toBe(true);
    }
  });

  it.each(Object.keys(FIXTURES))("%s deletes nothing without getuid", async name => {
    const { target, act } = FIXTURES[name]!();
    const before = shape(target);
    const getuid = process.getuid;
    Object.defineProperty(process, "getuid", { value: undefined, configurable: true, writable: true });
    restores.push(() => Object.defineProperty(process, "getuid", { value: getuid, configurable: true, writable: true }));
    try { await act(); } catch { /* a refusal may throw; it must not delete */ }
    expect(shape(target)).toEqual(before);
  });

  it("mesh --runs-older-than pass: a dry run plans the expired run; apply rejects before the helper's seam (smarty-dev#10678)", async () => {
    const { root, target } = meshPass();
    const before = shape(target);
    const plan = await sweepMeshRetention(root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(plan.removedRuns).toEqual([target]);
    expect(shape(target)).toEqual(before);
    const { verifiedRemoveTestHooks: hooks } = await import("../src/storage/verified-remove.js");
    const seen: string[] = [];
    hooks.beforeOpen = file => { seen.push(file); };
    restores.push(() => { delete hooks.beforeOpen; });
    await expect(sweepMeshRetention(root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY })).rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
    expect(seen).toEqual([]);
    expect(shape(target)).toEqual(before);
  });

  it("a swap between the verify and the graveyard move is caught after the move: left in the graveyard, reported, nothing deleted", async () => {
    const { verifiedRemove } = await import("../src/storage/verified-remove.js");
    const parent = tmp("fabric-vr-grave-");
    const outside = tmp("fabric-vr-grave-outside-");
    const target = run(path.join(parent, "old"));
    const judged = fs.lstatSync(target);
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
      // The helper's move into the graveyard (directory-relative through /proc/self/fd).
      if (String(from).startsWith("/proc/self/fd/") && path.basename(String(from)) === "old") {
        fs.cpSync(target, path.join(outside, "copy"), { recursive: true });
        rename(target, path.join(outside, "original"));
        rename(path.join(outside, "copy"), target);
      }
      return rename(from, to);
    }) as never);
    const result = verifiedRemove(target, judged, { deleter: "test" });
    vi.restoreAllMocks();
    expect(result).toMatchObject({ removed: false, reason: expect.stringMatching(/replaced.*graveyard/) });
    const graveyard = (result as { graveyard: string }).graveyard;
    expect(shape(graveyard)).toEqual(shape(path.join(outside, "original")));
    expect(shape(path.join(outside, "original"))).toMatchObject({ "reply.json": '{"text":"done"}' });
  });

  // pi-fabric#850 round 1 P1: the name-based unlink is bound because no other uid can change any entry in the tree.
  it.each([["the target", ""], ["a nested directory", "nested"]])("refuses a tree where %s is writable by group or others, and deletes nothing", async (_name, sub) => {
    const { verifiedRemove } = await import("../src/storage/verified-remove.js");
    const target = run(path.join(tmp("fabric-vr-open-"), "old"));
    const open = path.join(target, sub);
    if (sub) fs.mkdirSync(open);
    fs.chmodSync(open, 0o777);
    const before = shape(target);
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "test" });
    expect(result).toMatchObject({ removed: false, reason: expect.stringMatching(/writable by group or others/) });
    expect(shape(target)).toEqual(before);
  });

  it("removes only from inside a private graveyard that no other uid can traverse", async () => {
    const { verifiedRemove } = await import("../src/storage/verified-remove.js");
    const parent = tmp("fabric-vr-private-");
    const target = run(path.join(parent, "old"));
    let seen: { mode: number; uid: number; entries: string[] } | undefined;
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "test", proceed: () => {
      const grave = fs.readdirSync(parent).find(name => name.startsWith(".retention-graveyard-"))!;
      const stat = fs.lstatSync(path.join(parent, grave));
      seen = { mode: stat.mode & 0o777, uid: stat.uid, entries: fs.readdirSync(path.join(parent, grave)) };
      return true;
    } });
    expect(result).toMatchObject({ removed: true });
    expect(seen).toMatchObject({ mode: 0o700, uid: process.geteuid!(), entries: [expect.stringMatching(/^old\.[0-9a-f-]{36}$/)] });
    expect(fs.readdirSync(parent)).toEqual([]);
  });

  // pi-fabric#850 round 1 P2: off Linux, main's removal is counted like the verified path.
  it("off Linux, a dry run counts main's removal with its bytes and changes nothing", async () => {
    const { verifiedRemove, startRemovalTally, stopRemovalTally } = await import("../src/storage/verified-remove.js");
    const target = run(path.join(tmp("fabric-vr-darwin-"), "old"));
    const before = shape(target);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { value: "darwin" });
    const tally = startRemovalTally();
    let fallbackRan = false;
    try {
      const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "test", dryRun: true, fallback: () => (fallbackRan = true) });
      expect(result).toMatchObject({ removed: true, bytes: expect.any(Number) });
      expect((result as { bytes: number }).bytes).toBeGreaterThan(LOG.length);
      expect(tally.test).toMatchObject({ removed: 1, bytes: (result as { bytes: number }).bytes });
    } finally { stopRemovalTally(); Object.defineProperty(process, "platform", platform); }
    expect(fallbackRan).toBe(false);
    expect(shape(target)).toEqual(before);
  });

  it("a dry run verifies the whole tree and reports its bytes, and changes nothing", async () => {
    const { verifiedRemove } = await import("../src/storage/verified-remove.js");
    const target = run(path.join(tmp("fabric-vr-dry-"), "old"));
    const before = shape(path.dirname(target));
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "test", dryRun: true });
    expect(result).toEqual({ removed: true, bytes: LOG.length + 15 + fs.statSync(path.join(target, "status.json")).size });
    expect(shape(path.dirname(target))).toEqual(before);
  });

  it("a future-dated backup name never costs the newest backup (design 3)", () => {
    const directory = tmp("fabric-vr-future-");
    const session = path.join(directory, "session.jsonl");
    write(session, "live");
    // Rotation order by mtime: oldest, then a forged far-future name, then the real newest.
    const order = ["20260920T150000000Z", "20991231T235959999Z", "20260925T150000000Z"].map(stamp => `${session}.${stamp}.bak`);
    order.forEach((name, index) => { write(name, name); const at = 1_000 + index; fs.utimesSync(name, at, at); });
    pruneActorSessionBackups(session);
    expect(fs.existsSync(order[2]!)).toBe(true);
    expect(fs.existsSync(order[0]!)).toBe(false);
  });
});
