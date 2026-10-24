import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";
import { failActorTreeRemoval } from "./helpers/actor-tree-removal.js";

// Fail-closed coverage for the actor tree removal obligation marker, on a real ActorManager over its own temporary
// fixture. Faults are injected on fs only: the durable publish (temporary -> name rename) of this fixture's own
// removal marker, and (smarty-dev#16444: a removal archives the tree by one rename, never deletes it) the archive
// rename of this fixture's own judged tree. The removal helper itself is never mocked.
// Linux only, like the tracked tree removal.

const PREFIX = "fabric-removal-marker-";
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
    try { own = path.dirname(fs.realpathSync(root)) === tmp && path.basename(root).startsWith(PREFIX); }
    catch { /* already gone */ }
    if (own) fs.rmSync(root, { recursive: true, force: true });
  }
});

const DATA = ["fixture-history.jsonl", "fixture-a.bin", "fixture-b.bin"];
const NAMES = new Set(DATA);
const ALL = [...DATA].sort();

/** Basenames of this fixture's data files at or below `file` (no link followed), sorted. */
const dataIn = (file: string): string[] => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (stat?.isFile()) return NAMES.has(path.basename(file)) ? [path.basename(file)] : [];
  if (!stat?.isDirectory()) return [];
  return fs.readdirSync(file).flatMap(name => dataIn(path.join(file, name))).sort();
};
/** Every entry named for the actor anywhere below `dir`. */
const namedFor = (dir: string, id: string): string[] => {
  let names: string[] = [];
  try { names = fs.readdirSync(dir); } catch { return []; }
  return names.flatMap(name => {
    const file = path.join(dir, name);
    const own = name.startsWith(id) ? [file] : [];
    return fs.lstatSync(file, { throwIfNoEntry: false })?.isDirectory() ? [...own, ...namedFor(file, id)] : own;
  });
};

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), PREFIX));
  roots.push(root);
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  closers.push(() => agents.close()); // closed last (closers run in reverse)
  /** A manager over the same root: the first one, or a cold restart after the previous one closed. */
  const manager = () => {
    const actors = new ActorManager("marker", { id: "session:marker", name: "main", kind: "main", sessionId: "marker" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json") });
    closers.push(() => actors.close());
    return actors;
  };
  const actors = manager();
  const actor = await actors.create({ name: "marked", instructions: "Retain.", responseMode: "text" });
  const sessionDir = path.join(actorRoot, actor.id);
  fs.mkdirSync(path.join(sessionDir, "sub"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(sessionDir, DATA[0]!), "history\n", { mode: 0o600 });
  fs.writeFileSync(path.join(sessionDir, "sub", DATA[1]!), "actor data a\n", { mode: 0o600 });
  fs.writeFileSync(path.join(sessionDir, "sub", DATA[2]!), "actor data b\n", { mode: 0o600 });
  const marker = path.join(actorRoot, `removal-${actor.id}.json`);
  return { root, actorRoot, sessionDir, marker, id: actor.id, actors, manager };
};
type Fixture = Awaited<ReturnType<typeof setup>>;

/** Copies of the actor's tree outside its original name (a helper move), anywhere below the actor root. */
const movedCopies = (s: Fixture): string[] => namedFor(s.actorRoot, s.id)
  .filter(file => file !== s.sessionDir && !file.startsWith(s.sessionDir + path.sep));

/** Fail the durable publish of this fixture's own removal marker while `when(published JSON)` holds. */
const markerPublishFault = (s: Fixture, when: (published: Record<string, unknown>) => boolean) => {
  const state = { blocked: true, faults: 0 };
  const realRoot = fs.realpathSync(s.actorRoot);
  const rename = fs.renameSync.bind(fs);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    const target = String(to);
    if (state.blocked && path.basename(target) === path.basename(s.marker) && String(from).startsWith(`${target}.`)) {
      let published: unknown;
      try { published = fs.realpathSync(path.dirname(target)) === realRoot ? JSON.parse(fs.readFileSync(String(from), "utf8")) : undefined; }
      catch { published = undefined; }
      if (published && typeof published === "object" && when(published as Record<string, unknown>)) {
        state.faults++;
        throw Object.assign(new Error("EIO: marker publish failed (injected by the test on its own fixture)"), { code: "EIO" });
      }
    }
    rename(from, to);
  });
  return { state, release: () => { state.blocked = false; } };
};
/** A busy archive rename of this fixture's own judged tree (into the fleet session-archive) while the fault holds.
 * smarty-dev#16444: the tree leaves its location only by that one rename; a failed rename leaves it in place. */
const busyArchiveRename = (s: Fixture) => {
  const state = { blocked: true, faults: 0 };
  const spy = failActorTreeRemoval(s.sessionDir, () => {
    if (!state.blocked) return;
    state.faults++;
    throw Object.assign(new Error("EBUSY: resource busy (injected by the test on its own fixture)"), { code: "EBUSY" });
  });
  return { state, release: () => { state.blocked = false; spy.mockRestore(); } };
};
/** The fleet archive of this fixture's mesh (every root): where an archived actor tree goes. */
const fleetArchive = (s: Fixture): string => path.join(s.root, "mesh", "session-archive");
/** The actor's archived trees in the fleet archive (never anywhere else). */
const archivedTrees = (s: Fixture): string[] => namedFor(fleetArchive(s), s.id);
/** The obligation stays: pending result, reported obligation, marker kept. */
const expectPending = (actors: ActorManager, s: Fixture, result: unknown) => {
  expect(result).toMatchObject({ removed: true, cleaned: false, pending: expect.any(String) });
  expect(actors.cleanupObligation(s.id)).toBeDefined();
  expect(actors.pendingRemovals().map(entry => entry.id)).toContain(s.id);
  expect(fs.existsSync(s.marker)).toBe(true);
  expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({ id: s.id });
};
const coldStart = async (s: Fixture, previous: ActorManager): Promise<ActorManager> => {
  await previous.close();
  const next = s.manager();
  await next.finishPendingRemovals();
  return next;
};
/** The first publish carrying the tree obligation while the tree is still at its original name. */
const initialObligation = (s: Fixture) => (published: Record<string, unknown>): boolean =>
  "treeRemoval" in published && fs.existsSync(s.sessionDir);

describe.runIf(process.platform === "linux")("actor tree removal marker failures fail closed", () => {
  it("an initial obligation publish failure keeps the tree in place and pending, and the released exact-id retry removes it", async () => {
    const s = await setup();
    const ino = fs.lstatSync(s.sessionDir).ino;
    const intact = () => {
      expect(fs.lstatSync(s.sessionDir).ino).toBe(ino);
      expect(dataIn(s.sessionDir)).toEqual(ALL);
      expect(movedCopies(s)).toEqual([]);
    };
    const fault = markerPublishFault(s, initialObligation(s));
    let deletes = 0;
    actorTreeDeletionHooks.beforeDelete = () => { deletes++; };
    const first = await s.actors.remove(s.id);
    expect(fault.state.faults).toBeGreaterThan(0);
    expectPending(s.actors, s, first);
    expect(deletes).toBe(0); // failed in the final check: the delete stage never started
    intact();

    // Still unpublishable: a retry neither moves nor deletes anything.
    expectPending(s.actors, s, await s.actors.remove(s.id));
    intact();

    fault.release();
    const retry = await s.actors.remove(s.id);
    expect(retry).not.toHaveProperty("pending");
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    expect(dataIn(s.actorRoot)).toEqual([]);
    expect(namedFor(s.actorRoot, s.id)).toEqual([]);
    expect(fs.existsSync(s.marker)).toBe(false);
    expect(s.actors.cleanupObligation(s.id)).toBeUndefined();
    // smarty-dev#16444: removed from the actor root by its archive rename; nothing lost.
    const archived = archivedTrees(s);
    expect(archived).toHaveLength(1);
    expect(fs.lstatSync(archived[0]!).ino).toBe(ino);
    expect(dataIn(archived[0]!)).toEqual(ALL);
  }, 60_000);

  it("a failed archive rename keeps the tree in place and pending, in process and on a cold manager, and the released retry archives it with the data retained", async () => {
    const s = await setup();
    const ino = fs.lstatSync(s.sessionDir).ino;
    const intact = () => {
      expect(fs.lstatSync(s.sessionDir).ino).toBe(ino);
      expect(dataIn(s.sessionDir)).toEqual(ALL);
      expect(fs.readFileSync(path.join(s.sessionDir, DATA[0]!), "utf8")).toBe("history\n");
      expect(movedCopies(s)).toEqual([]);
      expect(archivedTrees(s)).toEqual([]);
    };
    const busy = busyArchiveRename(s);
    const first = await s.actors.remove(s.id);
    expect(busy.state.faults).toBeGreaterThan(0); // the real archive rename was reached, and refused
    expectPending(s.actors, s, first);
    intact();
    // The tree obligation was published before the rename: the judged tree, still at its original name.
    expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({
      treeRemoval: { ino, location: path.resolve(s.sessionDir), status: "outstanding" } });

    // Still refused: a retry, in process and on a cold manager, moves nothing and keeps the cleanup pending.
    expectPending(s.actors, s, await s.actors.remove(s.id));
    intact();
    const restarted = await coldStart(s, s.actors);
    expect(restarted.cleanupObligation(s.id)).toBeDefined();
    expectPending(restarted, s, await restarted.remove(s.id));
    intact();

    // Every fault released: the exact-id retry archives the whole judged tree, every data file in it.
    busy.release();
    const retry = await restarted.remove(s.id);
    expect(retry).not.toHaveProperty("pending");
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    expect(fs.existsSync(s.marker)).toBe(false);
    expect(restarted.cleanupObligation(s.id)).toBeUndefined();
    const archived = archivedTrees(s);
    expect(archived).toHaveLength(1);
    expect(fs.lstatSync(archived[0]!).ino).toBe(ino);
    expect(dataIn(archived[0]!)).toEqual(ALL);
    expect(fs.readFileSync(path.join(archived[0]!, DATA[0]!), "utf8")).toBe("history\n");
  }, 60_000);

  type Extension = { dev: number; ino: number; uid: number; type: number; location: string; status: string };
  const malformed: Array<{ name: string; outside?: true; extension: (valid: Extension, s: Fixture) => unknown }> = [
    { name: "a null extension", extension: () => null },
    { name: "a partial extension", extension: valid => ({ location: valid.location, status: "outstanding" }) },
    { name: "an unknown status", extension: valid => ({ ...valid, status: "removed" }) },
    { name: "an unrelated location inside the actor root", extension: valid => valid },
    { name: "a traversal location", outside: true,
      extension: (valid, s) => ({ ...valid, location: [s.actorRoot, "..", "elsewhere", s.id].join(path.sep) }) },
  ];
  it.each(malformed)("$name stays pending on a cold manager with the marker and the moved data untouched", async ({ outside, extension }) => {
    const s = await setup();
    // A real revoked, pending cleanup whose tree obligation was never published.
    const fault = markerPublishFault(s, initialObligation(s));
    expectPending(s.actors, s, await s.actors.remove(s.id));
    fault.release();
    await s.actors.close();

    // The data is elsewhere in the fixture root and the original path is absent: a legacy cleanup would call it done.
    const place = outside ? path.join(s.root, "elsewhere", s.id) : path.join(s.actorRoot, "unrelated", s.id);
    fs.mkdirSync(path.dirname(place), { mode: 0o700 });
    fs.renameSync(s.sessionDir, place);
    const stat = fs.lstatSync(place);
    const valid = { dev: stat.dev, ino: stat.ino, uid: stat.uid, type: stat.mode & 0o170000, location: place, status: "outstanding" };
    const bytes = JSON.stringify({ ...JSON.parse(fs.readFileSync(s.marker, "utf8")), treeRemoval: extension(valid, s) });
    fs.writeFileSync(s.marker, bytes, { mode: 0o600 });

    const restarted = s.manager();
    await restarted.finishPendingRemovals();
    expect(restarted.cleanupObligation(s.id)).toBeDefined();
    expectPending(restarted, s, await restarted.remove(s.id));
    expect(fs.readFileSync(s.marker, "utf8")).toBe(bytes);
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    expect(fs.lstatSync(place).ino).toBe(stat.ino);
    expect(dataIn(place)).toEqual(ALL);
  }, 60_000);

  it("a different inode at the recorded location stays pending and archives (or deletes) neither the replacement nor the retained data", async () => {
    const s = await setup();
    const busy = busyArchiveRename(s);
    expectPending(s.actors, s, await s.actors.remove(s.id));
    expect(busy.state.faults).toBeGreaterThan(0); // the archive rename failed: the tree is kept where it was judged
    expect(archivedTrees(s)).toEqual([]);
    // Premise: the retained tree's location (its original name, smarty-dev#16444) was published with its identity.
    const location = (JSON.parse(fs.readFileSync(s.marker, "utf8")) as { treeRemoval?: { location?: unknown } }).treeRemoval?.location;
    expect(location).toBe(path.resolve(s.sessionDir));
    expect(dataIn(location as string)).toEqual(ALL);

    // The retained tree leaves for elsewhere in the fixture root; a different directory takes the recorded location.
    const kept = path.join(s.root, "kept");
    fs.renameSync(location as string, kept);
    fs.mkdirSync(location as string, { mode: 0o700 });
    const replacement = path.join(location as string, "replacement.jsonl");
    fs.writeFileSync(replacement, "replacement\n", { mode: 0o600 });
    const replacementIno = fs.lstatSync(location as string).ino;
    busy.release(); // nothing blocks the archive rename any more: only the identity check keeps the replacement
    const untouched = () => {
      expect(fs.lstatSync(location as string).ino).toBe(replacementIno);
      expect(fs.readFileSync(replacement, "utf8")).toBe("replacement\n");
      expect(dataIn(kept)).toEqual(ALL);
      expect(archivedTrees(s)).toEqual([]);
    };

    expectPending(s.actors, s, await s.actors.remove(s.id));
    untouched();
    const restarted = await coldStart(s, s.actors);
    expectPending(restarted, s, await restarted.remove(s.id));
    untouched();
  }, 60_000);
});
