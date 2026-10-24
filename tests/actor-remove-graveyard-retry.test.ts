import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";

// A real ActorManager removal whose tree delete fails AFTER the tree has left its original name (a busy entry at the
// first unlink). The fault is injected on fs, never on the removal helper, and only for this test's own fixture files
// below its own temporary actor root. Behavioral invariants only, no marker schema:
// - the failed cleanup stays pending: obligation reported, marker kept, the actor data retained (never lost),
// - a retry while the cause persists stays pending, although the original path is now absent,
// - a cold manager over the same root still reports it pending and keeps it,
// - once the cause is gone, the exact-id retry really removes the retained tree and the marker,
// - a directory that appears at the original name after the retry's judgment is never deleted.
// Linux only: the verified tree delete and the Main publication fence are Linux paths.

const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  const tmp = fs.realpathSync(os.tmpdir());
  for (const root of roots.splice(0)) {
    // Only this test's own mkdtemp fixture is removed.
    let own = false;
    try { own = path.dirname(fs.realpathSync(root)) === tmp && path.basename(root).startsWith("fabric-graveyard-retry-"); }
    catch { /* already gone */ }
    if (own) fs.rmSync(root, { recursive: true, force: true });
  }
});

/** This test's own actor data files (distinct names), written closed (0600) so the delete never refuses them for mode. */
const DATA = ["fixture-history.jsonl", "fixture-a.bin", "fixture-b.bin"];
const NAMES = new Set(DATA);

/** Every fixture data file at or below `file` (no link followed), relative to `base`, sorted. */
const dataBelow = (file: string, base = file): string[] => {
  let stat: fs.Stats;
  try { stat = fs.lstatSync(file); } catch { return []; }
  if (stat.isFile()) return NAMES.has(path.basename(file)) ? [path.relative(base, file) || path.basename(file)] : [];
  if (!stat.isDirectory()) return [];
  return fs.readdirSync(file).flatMap(name => dataBelow(path.join(file, name), base)).sort();
};
const dataNames = (actorRoot: string): string[] => dataBelow(actorRoot).map(file => path.basename(file)).sort();
/** Any entry still named for the actor anywhere below the actor root (its tree, or a retained copy of it). */
const namedFor = (dir: string, id: string): string[] => {
  let names: string[] = [];
  try { names = fs.readdirSync(dir); } catch { return []; }
  return names.flatMap(name => {
    const file = path.join(dir, name);
    const own = name.startsWith(id) ? [file] : [];
    const stat = fs.lstatSync(file, { throwIfNoEntry: false });
    return stat?.isDirectory() ? [...own, ...namedFor(file, id)] : own;
  });
};
const isPending = (result: unknown): boolean => !!(result as { pending?: string } | undefined)?.pending;

const busy = (): Error => Object.assign(new Error("EBUSY: resource busy (injected by the test on its own fixture)"), { code: "EBUSY" });
/**
 * A persistent busy fault on this fixture's data files only, wherever they now are below its actor root (their own
 * name, or anywhere a removal moved them; directory-relative paths are resolved through their parent). `allowFirst`
 * lets exactly one fixture file go first, so the tree is partially removed when the fault hits.
 */
const busyFault = (actorRoot: string, options: { allowFirst?: boolean } = {}) => {
  const real = fs.realpathSync(actorRoot);
  const below = (file: string): boolean => file === real || file.startsWith(real + path.sep);
  const state = { blocked: true, faults: 0, unlinked: [] as string[] };
  const ours = (file: unknown): boolean => {
    if (!NAMES.has(path.basename(String(file)))) return false;
    try { return below(fs.realpathSync(path.dirname(String(file)))); } catch { return false; }
  };
  const holdsData = (file: unknown): boolean => {
    try { const resolved = fs.realpathSync(String(file)); return below(resolved) && dataBelow(resolved).length > 0; }
    catch { return false; }
  };
  const unlinkSync = fs.unlinkSync.bind(fs);
  vi.spyOn(fs, "unlinkSync").mockImplementation((file) => {
    if (ours(file)) {
      if (state.blocked && !(options.allowFirst && state.unlinked.length === 0)) { state.faults++; throw busy(); }
      state.unlinked.push(path.basename(String(file)));
    }
    unlinkSync(file);
  });
  const unlink = fs.promises.unlink.bind(fs.promises);
  vi.spyOn(fs.promises, "unlink").mockImplementation(async (file) => {
    if (state.blocked && ours(file)) { state.faults++; throw busy(); }
    await unlink(file);
  });
  // A path-based recursive delete of anything that still holds fixture data is busy too.
  const rmSync = fs.rmSync.bind(fs);
  vi.spyOn(fs, "rmSync").mockImplementation((file, rmOptions) => {
    if (state.blocked && holdsData(file)) { state.faults++; throw busy(); }
    rmSync(file, rmOptions);
  });
  const rm = fs.promises.rm.bind(fs.promises);
  vi.spyOn(fs.promises, "rm").mockImplementation(async (file, rmOptions) => {
    if (state.blocked && holdsData(file)) { state.faults++; throw busy(); }
    await rm(file, rmOptions);
  });
  return { state, release: () => { state.blocked = false; } };
};

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-graveyard-retry-"));
  roots.push(root);
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  closers.push(() => agents.close()); // closed last (closers run in reverse)
  /** A manager over the same root: the first one, or a cold restart after the previous one closed. */
  const manager = () => {
    const actors = new ActorManager("graveyard", { id: "session:graveyard", name: "main", kind: "main", sessionId: "graveyard" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json") });
    closers.push(() => actors.close());
    return actors;
  };
  const actors = manager();
  const actor = await actors.create({ name: "retained", instructions: "Retain.", responseMode: "text" });
  const sessionDir = path.join(actorRoot, actor.id);
  fs.mkdirSync(path.join(sessionDir, "sub"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(sessionDir, DATA[0]!), "history\n", { mode: 0o600 });
  fs.writeFileSync(path.join(sessionDir, "sub", DATA[1]!), "actor data a\n", { mode: 0o600 });
  fs.writeFileSync(path.join(sessionDir, "sub", DATA[2]!), "actor data b\n", { mode: 0o600 });
  const marker = path.join(actorRoot, `removal-${actor.id}.json`);
  return { root, actorRoot, sessionDir, marker, id: actor.id, actors, manager };
};

/** The cleanup is pending and nothing it still owes is lost: obligation, marker and the retained data. */
const expectPending = (actors: ActorManager, s: { id: string; marker: string; actorRoot: string }, result: unknown, retained: string[]) => {
  expect(result).toMatchObject({ removed: true, cleaned: false, pending: expect.any(String) });
  expect(actors.cleanupObligation(s.id)).toBeDefined();
  expect(actors.pendingRemovals().map(entry => entry.id)).toContain(s.id);
  expect(fs.existsSync(s.marker)).toBe(true);
  expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({ id: s.id });
  expect(dataNames(s.actorRoot)).toEqual(retained);
};
/** The exact-id cleanup really finished: no fixture data, nothing named for the actor, no marker, no obligation. */
const expectRemoved = (actors: ActorManager, s: { id: string; marker: string; actorRoot: string; sessionDir: string }, result: unknown) => {
  expect(result).not.toHaveProperty("pending");
  expect(dataNames(s.actorRoot)).toEqual([]);
  expect(namedFor(s.actorRoot, s.id)).toEqual([]);
  expect(fs.existsSync(s.sessionDir)).toBe(false);
  expect(fs.existsSync(s.marker)).toBe(false);
  expect(actors.cleanupObligation(s.id)).toBeUndefined();
  expect(actors.pendingRemovals().map(entry => entry.id)).not.toContain(s.id);
  expect(actors.list().map(entry => entry.id)).not.toContain(s.id);
};

describe.runIf(process.platform === "linux")("actor tree removal that fails after the move keeps its cleanup until the retained tree is removed", () => {
  it("a busy entry after the move keeps the cleanup pending, blocked retries stay pending with the original path absent, and the released exact-id retry removes the retained tree and marker", async () => {
    const s = await setup();
    const all = [...DATA].sort();
    const fault = busyFault(s.actorRoot);
    const first = await s.actors.remove(s.id);
    expect(fault.state.faults).toBeGreaterThan(0); // the fault was really hit
    expectPending(s.actors, s, first, all);
    // Premise: the failure came after the tree left its original name.
    expect(fs.existsSync(s.sessionDir)).toBe(false);

    for (let attempt = 0; attempt < 2; attempt++) {
      // Exact id, cause still present: the original path is absent, yet the retained data is not "already gone".
      const blocked = await s.actors.remove(s.id);
      expectPending(s.actors, s, blocked, all);
      expect(fs.existsSync(s.sessionDir)).toBe(false);
    }

    fault.release();
    const retry = await s.actors.remove(s.id);
    expectRemoved(s.actors, s, retry);
    // Settled: a further exact-id request finds no cleanup left to finish.
    const again = await s.actors.remove(s.id).catch((error: Error) => ({ error: error.message }));
    expect(again).not.toHaveProperty("pending");
    expect(fs.existsSync(s.marker)).toBe(false);
  }, 60_000);

  it("a partially removed retained tree survives a cold restart as pending, and the released exact-id retry on the cold manager removes it", async () => {
    const s = await setup();
    const fault = busyFault(s.actorRoot, { allowFirst: true });
    const first = await s.actors.remove(s.id);
    expect(fault.state.faults).toBeGreaterThan(0);
    expect(fault.state.unlinked).toHaveLength(1); // partial: one fixture file went before the busy one
    const retained = DATA.filter(name => !fault.state.unlinked.includes(name)).sort();
    expect(retained).toHaveLength(DATA.length - 1);
    expectPending(s.actors, s, first, retained);
    expect(fs.existsSync(s.sessionDir)).toBe(false);

    // The owner stops; a cold manager over the same root takes over while the cause persists.
    await s.actors.close();
    const restarted = s.manager();
    await restarted.finishPendingRemovals(); // a host start finishes what it can
    expect(restarted.cleanupObligation(s.id)).toBeDefined();
    expect(restarted.pendingRemovals().map(entry => entry.id)).toContain(s.id);
    expect(fs.existsSync(s.marker)).toBe(true);
    expect(dataNames(s.actorRoot)).toEqual(retained);
    const blocked = await restarted.remove(s.id);
    expectPending(restarted, s, blocked, retained);

    fault.release();
    const retry = await restarted.remove(s.id);
    expectRemoved(restarted, s, retry);

    // A later cold start has nothing left to finish.
    await restarted.close();
    const third = s.manager();
    await third.finishPendingRemovals();
    expect(third.cleanupObligation(s.id)).toBeUndefined();
    expect(third.pendingRemovals().map(entry => entry.id)).not.toContain(s.id);
    expect(fs.existsSync(s.marker)).toBe(false);
    expect(dataNames(s.actorRoot)).toEqual([]);
  }, 60_000);

  it("counterexample: a directory that appears at the original name after the retry's judgment is never deleted, and a finished cleanup strands none of the retained data", async () => {
    const s = await setup();
    const fault = busyFault(s.actorRoot);
    const first = await s.actors.remove(s.id);
    expect(fault.state.faults).toBeGreaterThan(0);
    expectPending(s.actors, s, first, [...DATA].sort());
    expect(fs.existsSync(s.sessionDir)).toBe(false);

    fault.release();
    const successor = path.join(s.sessionDir, "successor.jsonl");
    let replaced = false;
    // Under the held fence, after the final check judged what to delete: a same-name directory appears.
    actorTreeDeletionHooks.beforeDelete = () => {
      if (replaced) return;
      replaced = true;
      fs.mkdirSync(s.sessionDir, { mode: 0o700 });
      fs.writeFileSync(successor, "successor history\n", { mode: 0o600 });
    };
    const retry = await s.actors.remove(s.id);
    expect(replaced).toBe(true); // the retained-tree delete ran under the fence
    expect(fs.readFileSync(successor, "utf8")).toBe("successor history\n");
    // Whatever it reports, a cleanup reported finished leaves none of the retained data behind.
    const cleaned = !isPending(retry);
    expect({ cleaned, left: cleaned ? dataNames(s.actorRoot) : [] }).toEqual({ cleaned, left: [] });
    if (!cleaned) expect(fs.existsSync(s.marker)).toBe(true);
  }, 60_000);
});
