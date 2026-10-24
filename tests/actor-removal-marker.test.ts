import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";

// Fail-closed coverage for the actor tree removal obligation marker, on a real ActorManager over its own temporary
// fixture. Faults are injected on fs only: the durable publish (temporary -> name rename) of this fixture's own
// removal marker, and unlinks of this fixture's own data files. The removal helper itself is never mocked.
// Linux only, like the tracked tree delete.

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
/** A busy unlink of this fixture's own data files wherever they are below its actor root (after a move too). */
const busyUnlinks = (s: Fixture) => {
  const state = { blocked: true, faults: 0 };
  const realRoot = fs.realpathSync(s.actorRoot);
  const unlink = fs.unlinkSync.bind(fs);
  vi.spyOn(fs, "unlinkSync").mockImplementation((file) => {
    if (state.blocked && NAMES.has(path.basename(String(file)))) {
      let below = false;
      try { const dir = fs.realpathSync(path.dirname(String(file))); below = dir === realRoot || dir.startsWith(realRoot + path.sep); }
      catch { below = false; }
      if (below) {
        state.faults++;
        throw Object.assign(new Error("EBUSY: resource busy (injected by the test on its own fixture)"), { code: "EBUSY" });
      }
    }
    unlink(file);
  });
  return { state, release: () => { state.blocked = false; } };
};
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
  }, 60_000);

  it("an unpublished returned graveyard location stays pending with the data retained, in process and on a cold manager, with every fault released", async () => {
    const s = await setup();
    const busy = busyUnlinks(s);
    // Only a publish after the tree left its original name: the returned location save.
    const fault = markerPublishFault(s, published => "treeRemoval" in published && !fs.existsSync(s.sessionDir));
    const first = await s.actors.remove(s.id);
    expect(busy.state.faults).toBeGreaterThan(0); // the real move happened, then a busy entry
    expect(fault.state.faults).toBeGreaterThan(0);
    expectPending(s.actors, s, first);
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    expect(dataIn(s.actorRoot)).toEqual(ALL);
    const moved = movedCopies(s);
    expect(moved.length).toBeGreaterThan(0);

    // Nothing blocks deletion any more, but the tree's location was never published: absence is not completion.
    busy.release();
    fault.release();
    expectPending(s.actors, s, await s.actors.remove(s.id));
    expect(dataIn(s.actorRoot)).toEqual(ALL);

    const restarted = await coldStart(s, s.actors);
    expect(restarted.cleanupObligation(s.id)).toBeDefined();
    expectPending(restarted, s, await restarted.remove(s.id));
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    expect(dataIn(s.actorRoot)).toEqual(ALL);
    expect(movedCopies(s)).toEqual(moved);
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

  it("a different inode at the recorded location stays pending and deletes neither the replacement nor the retained data", async () => {
    const s = await setup();
    const busy = busyUnlinks(s);
    expectPending(s.actors, s, await s.actors.remove(s.id));
    expect(busy.state.faults).toBeGreaterThan(0);
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    // Premise: the retained tree's moved location was published.
    const location = (JSON.parse(fs.readFileSync(s.marker, "utf8")) as { treeRemoval?: { location?: unknown } }).treeRemoval?.location;
    expect(typeof location).toBe("string");
    expect(dataIn(location as string)).toEqual(ALL);

    // The retained tree leaves for elsewhere in the fixture root; a different directory takes the recorded location.
    const kept = path.join(s.root, "kept");
    fs.renameSync(location as string, kept);
    fs.mkdirSync(location as string, { mode: 0o700 });
    const replacement = path.join(location as string, "replacement.jsonl");
    fs.writeFileSync(replacement, "replacement\n", { mode: 0o600 });
    busy.release();
    const untouched = () => {
      expect(fs.readFileSync(replacement, "utf8")).toBe("replacement\n");
      expect(dataIn(kept)).toEqual(ALL);
    };

    expectPending(s.actors, s, await s.actors.remove(s.id));
    untouched();
    const restarted = await coldStart(s, s.actors);
    expectPending(restarted, s, await restarted.remove(s.id));
    untouched();
  }, 60_000);
});
