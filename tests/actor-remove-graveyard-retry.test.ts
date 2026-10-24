import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { fleetSessionArchive } from "../src/storage/session-archive.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";
import { failActorTreeRemoval } from "./helpers/actor-tree-removal.js";

// smarty-dev#16444 (R-keep-all-traces), pi-fabric#1568 round 3: an actor tree is archived, never deleted (one rename
// into `<meshRoot>/session-archive/<root>/<actor>`). This file used to prove the verifiedRemove graveyard retry (a
// delete that failed after the tree left its name stays pending, and a retry finds the retained tree). The archive
// rename keeps the same guarantees, proven on a real ActorManager removal. Faults are injected on fs, only for this
// test's own fixture, never on the removal code:
// - a FAILED rename keeps the tree in place (every inode and byte), the cleanup pending (obligation, marker), blocked
//   retries pending too, and the released exact-id retry archives the whole tree with nothing lost;
// - an INTERRUPTED removal (the rename completed, its discharge was never saved, as a crash there leaves it) survives a
//   cold restart as pending, and the cold manager's retry finds the pinned tree in the archive and completes;
// - counterexample: a directory that appears at the original name after the retry's judgment is never moved or
//   deleted, and the finished cleanup strands none of the archived data.
// No fixture data is ever unlinked: every delete of it is recorded and refused.
// Linux only: the held-parent rename and the Main publication fence are Linux paths.

const ROOT_ID = "session:graveyard";
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

/** This test's own actor data files (relative to the actor directory), written closed (0600). */
const DATA = ["fixture-history.jsonl", "sub/fixture-a.bin", "sub/fixture-b.bin"];
/** The tree's own inode plus each fixture file's inode and bytes: what "nothing lost" compares. */
const dataSnapshot = (tree: string): Record<string, string> => Object.fromEntries([
  [".", `dir:${fs.lstatSync(tree).ino}`],
  ...DATA.map(name => { const file = path.join(tree, name);
    return [name, `${fs.lstatSync(file).ino}:${fs.readFileSync(file).toString("hex")}`]; }),
]);

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-graveyard-retry-"));
  roots.push(root);
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  closers.push(() => agents.close()); // closed last (closers run in reverse)
  /** A manager over the same root: the first one, or a cold restart after the previous one closed. removalRetryMs is
   * large: the only cleanup attempts are the ones this test makes. */
  const manager = () => {
    const actors = new ActorManager("graveyard", { id: ROOT_ID, name: "main", kind: "main", sessionId: "graveyard" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json"), removalRetryMs: 60_000 });
    closers.push(() => actors.close());
    return actors;
  };
  const actors = manager();
  const actor = await actors.create({ name: "retained", instructions: "Retain.", responseMode: "text" });
  const sessionDir = path.join(actorRoot, actor.id);
  fs.mkdirSync(path.join(sessionDir, "sub"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(sessionDir, DATA[0]!), "history\n", { mode: 0o600 });
  fs.writeFileSync(path.join(sessionDir, DATA[1]!), "actor data a\n", { mode: 0o600 });
  fs.writeFileSync(path.join(sessionDir, DATA[2]!), "actor data b\n", { mode: 0o600 });
  const marker = path.join(actorRoot, `removal-${actor.id}.json`);
  const fleet = fleetSessionArchive(mesh.root, ROOT_ID);
  const archived = fleetSessionArchive(mesh.root, ROOT_ID, actor.id);
  /** The fleet manifest's phases, in order (none before the first intent). */
  const phases = (): string[] => {
    const file = path.join(fleet, "MANIFEST.jsonl");
    return fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line).phase) : [];
  };
  return { root, actorRoot, sessionDir, marker, fleet, archived, phases, id: actor.id, actors, manager };
};
type Fixture = Awaited<ReturnType<typeof setup>>;

/** Every delete of fixture data, wherever it is now (the tree or the archive), is recorded and refused. */
const guardDeletes = (s: Fixture): string[] => {
  const deletes: string[] = [];
  const holdsData = (target: unknown): boolean => {
    const name = String(target);
    let resolved = path.resolve(name);
    try { resolved = path.join(fs.realpathSync(path.dirname(name)), path.basename(name)); } catch { /* as given */ }
    const below = (root: string) => resolved === root || resolved.startsWith(root + path.sep);
    if (!below(s.sessionDir) && !below(s.archived) && !below(`${s.sessionDir}.judged`)) {
      // A directory that contains the tree (the actor root, the fleet archive) holds the data too.
      return [s.sessionDir, s.archived].some(tree => tree.startsWith(resolved + path.sep) && fs.existsSync(tree));
    }
    return fs.existsSync(resolved);
  };
  const refuse = (api: string, target: unknown): void => {
    if (!holdsData(target)) return;
    deletes.push(`${api} ${String(target)}`);
    throw Object.assign(new Error(`guard: ${api} of fixture data`), { code: "EPERM" });
  };
  for (const name of ["unlinkSync", "rmSync", "rmdirSync"] as const) {
    const original = (fs[name] as (...args: unknown[]) => unknown).bind(fs);
    vi.spyOn(fs as unknown as Record<string, (...args: unknown[]) => unknown>, name)
      .mockImplementation((...args: unknown[]) => { refuse(`fs.${name}`, args[0]); return original(...args); });
  }
  for (const name of ["unlink", "rm", "rmdir"] as const) {
    const original = (fs.promises[name] as (...args: unknown[]) => Promise<unknown>).bind(fs.promises);
    vi.spyOn(fs.promises as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>, name)
      .mockImplementation(async (...args: unknown[]) => { refuse(`fs.promises.${name}`, args[0]); return original(...args); });
  }
  return deletes;
};

const busy = (): Error => Object.assign(new Error("EBUSY: resource busy (injected by the test on its own fixture)"), { code: "EBUSY" });
/** A persistent fault on this fixture's archive rename only (failActorTreeRemoval: the judged tree into session-archive). */
const renameFault = (s: Fixture) => {
  const state = { blocked: true, faults: 0 };
  const spy = failActorTreeRemoval(s.sessionDir, () => { if (state.blocked) { state.faults++; throw busy(); } });
  return { state, release: () => { state.blocked = false; spy.mockRestore(); } };
};
/** The archive rename succeeds; the marker save right after it (the discharge) fails once: on disk the obligation is
 * still outstanding with its tree in the archive, exactly what a crash between the two leaves. */
const dischargeFault = (s: Fixture) => {
  const state = { renamed: false, faults: 0 };
  const rename = fs.renameSync.bind(fs);
  vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
    if (state.renamed && state.faults === 0 && path.resolve(String(destination)) === s.marker) {
      state.faults++;
      throw Object.assign(new Error("EIO: injected discharge save fault"), { code: "EIO" });
    }
    rename(source, destination);
    if (path.resolve(String(destination)) === s.archived) state.renamed = true;
  });
  return state;
};

/** The cleanup is pending and nothing it owes is lost: obligation, marker and the tree in place, every byte. */
const expectPending = (actors: ActorManager, s: Fixture, result: unknown, before: Record<string, string>) => {
  expect(result).toMatchObject({ removed: true, cleaned: false, pending: expect.any(String) });
  expect(actors.cleanupObligation(s.id)).toBeDefined();
  expect(actors.pendingRemovals().map(entry => entry.id)).toContain(s.id);
  expect(fs.existsSync(s.marker)).toBe(true);
  expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({ id: s.id });
  expect(dataSnapshot(s.sessionDir)).toEqual(before);
  expect(fs.existsSync(s.archived)).toBe(false);
};
/** The exact-id cleanup really finished, and the whole judged tree is in the archive: same inodes, same bytes. */
const expectArchived = (actors: ActorManager, s: Fixture, result: unknown, before: Record<string, string>) => {
  expect(result).not.toHaveProperty("pending");
  expect(fs.existsSync(s.sessionDir)).toBe(false);
  expect(dataSnapshot(s.archived)).toEqual(before);
  expect(fs.existsSync(s.marker)).toBe(false);
  expect(actors.cleanupObligation(s.id)).toBeUndefined();
  expect(actors.pendingRemovals().map(entry => entry.id)).not.toContain(s.id);
  expect(actors.list().map(entry => entry.id)).not.toContain(s.id);
};

describe.runIf(process.platform === "linux")("actor tree archive rename that fails or is interrupted keeps its cleanup until the tree is archived", () => {
  it("a failed rename keeps the tree in place and the cleanup pending, blocked retries stay pending, and the released exact-id retry archives the whole tree with nothing lost", async () => {
    const s = await setup();
    const before = dataSnapshot(s.sessionDir);
    const deletes = guardDeletes(s);
    const fault = renameFault(s);
    const first = await s.actors.remove(s.id);
    expect(fault.state.faults).toBe(1); // the fault was really hit, at the rename itself
    expectPending(s.actors, s, first, before);
    // The intent was recorded before the rename, and its failed outcome after it; nothing reached the archive.
    expect(s.phases()).toEqual(["MOVING", "MOVE-FAILED"]);

    for (let attempt = 0; attempt < 2; attempt++) {
      // Exact id, cause still present: the tree stays in place, the cleanup pending.
      const blocked = await s.actors.remove(s.id);
      expectPending(s.actors, s, blocked, before);
    }
    expect(fault.state.faults).toBe(3);

    fault.release();
    const retry = await s.actors.remove(s.id);
    expectArchived(s.actors, s, retry, before);
    expect(s.phases()).toEqual([..."xxx"].flatMap(() => ["MOVING", "MOVE-FAILED"]).concat(["MOVING", "MOVED-TO"]));
    // Settled: a further exact-id request finds no cleanup left to finish.
    const again = await s.actors.remove(s.id).catch((error: Error) => ({ error: error.message }));
    expect(again).not.toHaveProperty("pending");
    expect(fs.existsSync(s.marker)).toBe(false);
    expect(dataSnapshot(s.archived)).toEqual(before);
    expect(deletes).toEqual([]);
  }, 60_000);

  it("an interrupted removal (rename done, discharge not saved) survives a cold restart as pending, and the cold manager's retry finds the pinned tree in the archive and completes", async () => {
    const s = await setup();
    const before = dataSnapshot(s.sessionDir);
    const deletes = guardDeletes(s);
    const fault = dischargeFault(s);
    const first = await s.actors.remove(s.id);
    expect(fault).toEqual({ renamed: true, faults: 1 });
    expect(first).toMatchObject({ removed: true, cleaned: false, pending: expect.stringContaining("EIO") });
    // The tree left its name INTO the archive (nothing lost), but the saved obligation still says outstanding.
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    expect(dataSnapshot(s.archived)).toEqual(before);
    expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({ id: s.id, treeRemoval: { status: "outstanding" } });
    expect(s.phases()).toEqual(["MOVING", "MOVED-TO"]);

    // The owner stops; a cold manager over the same root takes over from the saved obligation alone.
    await s.actors.close();
    const restarted = s.manager();
    expect(restarted.cleanupObligation(s.id)).toBeDefined();
    await restarted.finishPendingRemovals(); // a host start finishes what it can
    expectArchived(restarted, s, {}, before);
    // The rename's own MOVED-TO line stands; the recovery adds no second one.
    expect(s.phases()).toEqual(["MOVING", "MOVED-TO"]);

    // A later cold start has nothing left to finish.
    await restarted.close();
    const third = s.manager();
    await third.finishPendingRemovals();
    expect(third.cleanupObligation(s.id)).toBeUndefined();
    expect(fs.existsSync(s.marker)).toBe(false);
    expect(dataSnapshot(s.archived)).toEqual(before);
    expect(deletes).toEqual([]);
  }, 60_000);

  it("counterexample: a directory that appears at the original name after the retry's judgment is never moved or deleted, and the finished cleanup strands none of the archived data", async () => {
    const s = await setup();
    const before = dataSnapshot(s.sessionDir);
    const deletes = guardDeletes(s);
    dischargeFault(s);
    const first = await s.actors.remove(s.id);
    expect(first).toMatchObject({ cleaned: false, pending: expect.any(String) });
    expect(fs.existsSync(s.sessionDir)).toBe(false);
    await s.actors.close();

    const successor = path.join(s.sessionDir, "successor.jsonl");
    let replaced: number | undefined;
    // Under the held fence, after the cold retry's final check judged the tree: a same-name directory appears.
    actorTreeDeletionHooks.beforeDelete = () => {
      if (replaced !== undefined) return;
      fs.mkdirSync(s.sessionDir, { mode: 0o700 });
      fs.writeFileSync(successor, "successor history\n", { mode: 0o600 });
      replaced = fs.lstatSync(s.sessionDir).ino;
    };
    const restarted = s.manager();
    await restarted.finishPendingRemovals();
    expect(replaced).toBeDefined(); // the retry ran its fenced step
    // The successor never moved (same inode, same bytes) and was not deleted; the archive holds only the judged tree.
    expect(fs.lstatSync(s.sessionDir).ino).toBe(replaced);
    expect(fs.readFileSync(successor, "utf8")).toBe("successor history\n");
    expect(fs.existsSync(path.join(s.archived, "successor.jsonl"))).toBe(false);
    expect(dataSnapshot(s.archived)).toEqual(before);
    // Whatever it reports, a cleanup reported finished leaves none of the judged data outside the archive.
    const cleaned = restarted.cleanupObligation(s.id) === undefined;
    if (cleaned) expect(fs.existsSync(s.marker)).toBe(false); else expect(fs.existsSync(s.marker)).toBe(true);
    expect(fs.readdirSync(s.fleet).filter(name => name !== "MANIFEST.jsonl")).toEqual([path.basename(s.archived)]);
    expect(deletes).toEqual([]);
  }, 60_000);
});
