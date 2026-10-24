import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { fleetSessionArchive } from "../src/storage/session-archive.js";
import { mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { failActorTreeRemoval } from "./helpers/actor-tree-removal.js";

// Supplement to actor-directory.test.ts "... cleanup-only id after exhausted retries": the same exact-id cleanup retry
// contract, around the archive rename. smarty-dev#16444 (R-keep-all-traces): an actor tree is archived, never deleted
// (one rename of the judged directory into `<meshRoot>/session-archive/<root>/<actor>`), so nothing is deleted
// locally and the rename really runs. The first cleanup runs the REAL moveActorTreeToArchive with the manager's own
// arguments; failActorTreeRemoval faults its rename, so the cleanup stays pending with the tree in place. The explicit
// exact-id retry runs the same REAL rename: recorded with its judged identity and the fence state, it moves the tree
// (same inode, same bytes) into the archive, with its MOVING and MOVED-TO lines.
type Move = typeof import("../src/storage/tree-archive.js").moveActorTreeToArchive;
const archiver = vi.hoisted(() => ({ wrap: undefined as undefined | ((real: Move, ...args: Parameters<Move>) => ReturnType<Move>) }));
vi.mock("../src/storage/tree-archive.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/storage/tree-archive.js")>();
  const moveActorTreeToArchive: Move = (...args) => archiver.wrap ? archiver.wrap(actual.moveActorTreeToArchive, ...args) : actual.moveActorTreeToArchive(...args);
  return { ...actual, moveActorTreeToArchive };
});

const UNSUPPORTED_PLATFORM = process.platform !== "linux" && process.platform !== "darwin" && process.platform !== "win32";
const ROOT_ID = "session:alpha";

/** Raw deletes of a kept tree (or anything under it) throw and are recorded; restored by descriptor after close. */
const guardRawDeletes = (trees: string[], blocked: string[]): (() => void) => {
  const inside = (operand: unknown): boolean => {
    const at = path.resolve(String(operand instanceof URL ? operand.pathname : operand));
    return trees.some((tree) => at === tree || at.startsWith(tree + path.sep));
  };
  const saved: Array<[object, string, PropertyDescriptor]> = [];
  const patch = (owner: object, names: string[]): void => {
    for (const name of names) {
      const descriptor = Object.getOwnPropertyDescriptor(owner, name)!;
      const original = descriptor.value as (...args: unknown[]) => unknown;
      saved.push([owner, name, descriptor]);
      Object.defineProperty(owner, name, { ...descriptor, value: function (this: unknown, operand: unknown, ...rest: unknown[]) {
        if (inside(operand)) {
          // close() drops its per-session queue snapshot. Suppress that metadata operation as a no-op too;
          // do not perform it, and never exempt a directory, recursive removal, history file or sentinel.
          const file = path.resolve(String(operand));
          const stat = fs.lstatSync(file, { throwIfNoEntry: false });
          if (owner === fs && name === "rmSync" && /^queue-[0-9a-f]+\.json$/.test(path.basename(file)) &&
              trees.includes(path.dirname(file)) && (!stat || stat.isFile()) &&
              !(rest[0] as fs.RmOptions | undefined)?.recursive) return;
          blocked.push(`${name} ${String(operand)}`);
          throw Object.assign(new Error(`guard: raw ${name} of a kept actor tree`), { code: "EPERM" });
        }
        return original.call(this, operand, ...rest);
      } });
    }
  };
  patch(fs, ["rmSync", "rmdirSync", "unlinkSync", "rm", "rmdir", "unlink"]);
  patch(fs.promises, ["rm", "rmdir", "unlink"]);
  return () => { for (const [owner, name, descriptor] of saved.reverse()) Object.defineProperty(owner, name, descriptor); };
};

describe.skipIf(UNSUPPORTED_PLATFORM)("ActorDirectory recorded cleanup retry", () => {
  it.each(["project", "session"] as const)("retries an exact %s cleanup-only id through the real archive rename, recorded", async (scope) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-cleanup-retry-recorded-"));
    const meshRoot = path.join(root, "mesh");
    const trees: string[] = [];
    const blocked: string[] = [];
    const restoreGuards = guardRawDeletes(trees, blocked);
    const identity: MeshIdentity = { id: ROOT_ID, name: "main", kind: "main", sessionId: "alpha" };
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs-alpha"),
    });
    const actorRoots = { project: path.join(meshRoot, "actors"), session: path.join(meshRoot, "actors", "alpha") };
    // removalRetryMs is large and no run is launched: the only cleanup attempts are the two this test makes.
    const directory = new ActorDirectory(["alpha", identity, new MeshStore(meshRoot, 64 * 1024, 100),
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, () => {},
      { persistent: true, rootId: ROOT_ID, removalRetryMs: 60_000 }], actorRoots, "project");
    try {
      const actor = await directory.create({ scope, name: "reviewer", instructions: "Work." });
      const dir = path.join(actorRoots[scope], actor.id);
      expect(path.dirname(actor.sessionFile!)).toBe(dir);
      trees.push(dir);
      fs.mkdirSync(dir, { recursive: true });
      const sentinel = path.join(dir, "kept-recorded.txt");
      fs.writeFileSync(sentinel, "history\n");
      const target = fs.lstatSync(dir);
      const marker = path.join(actorRoots[scope], `removal-${actor.id}.json`);
      const fleet = fleetSessionArchive(meshRoot, ROOT_ID);
      const archived = fleetSessionArchive(meshRoot, ROOT_ID, actor.id);
      trees.push(archived); // a raw delete of the archived tree is refused too

      let phase: "fault" | "retry" = "fault";
      const calls: Array<{ phase: string; to: string; sameInode: boolean; fenced: boolean; moved: boolean }> = [];
      archiver.wrap = (real, source, pin, archive, chain, name, route) => {
        if (archive !== fleet || !name?.startsWith(actor.id)) return real(source, pin, archive, chain, name, route);
        // The judged pin and the entry the rename takes are this exact tree.
        const now = fs.lstatSync(source, { throwIfNoEntry: false });
        const sameInode = !!now && now.dev === pin.dev && now.ino === pin.ino && now.dev === target.dev && now.ino === target.ino;
        const fenced = mainPublicationFenced(meshRoot, ROOT_ID);
        let moved = false;
        try { const to = real(source, pin, archive, chain, name, route); moved = true; return to; }
        finally { calls.push({ phase, to: path.join(archive, name), sameInode, fenced, moved }); }
      };

      let faults = 0;
      const fault = failActorTreeRemoval(dir, () => { faults++; throw new Error("injected actor tree removal fault"); });
      try {
        await expect(directory.remove(actor.id, { wait: false })).resolves.toMatchObject({
          removed: true, cleaned: false, pending: expect.stringContaining("cleanup failed") });
        await directory.removalSettled(actor.id);
      } finally { fault.mockRestore(); }
      expect(faults).toBe(1);
      expect(calls).toEqual([{ phase: "fault", to: archived, sameInode: true, fenced: true, moved: false }]);
      // The failed rename kept the tree in place (same inode, same bytes); the intent and its failure are recorded.
      expect(fs.lstatSync(dir)).toMatchObject({ dev: target.dev, ino: target.ino });
      expect(fs.readFileSync(sentinel, "utf8")).toBe("history\n");
      expect(fs.existsSync(archived)).toBe(false);
      expect(fs.existsSync(marker)).toBe(true);
      expect(directory.owns(actor.id)).toBe(true);
      expect(directory.pendingRemovals()).toEqual([expect.objectContaining({ id: actor.id, state: expect.stringContaining("cleanup failed") })]);
      expect(directory.list()).toContainEqual(expect.objectContaining({ id: actor.id, scope, status: "stopped", removal: expect.any(Object) }));
      expect(() => directory.status(actor.id)).toThrow(/Unknown Fabric actor/);
      expect(directory.listOwned()).toEqual([]);

      const successor = await directory.create({ scope, name: "reviewer", instructions: "Successor." });
      trees.push(path.dirname(successor.sessionFile!));
      fs.mkdirSync(path.dirname(successor.sessionFile!), { recursive: true });
      fs.writeFileSync(successor.sessionFile!, "successor\n");
      expect(directory.status("reviewer").id).toBe(successor.id);
      await expect(directory.remove(actor.id.slice(0, 12))).rejects.toThrow();
      expect(calls).toHaveLength(1);

      phase = "retry";
      await expect(directory.remove(actor.id)).resolves.toEqual({ removed: true });
      expect(faults).toBe(1);
      expect(calls.slice(1)).toEqual([{ phase: "retry", to: archived, sameInode: true, fenced: true, moved: true }]);
      expect(directory.pendingRemovals()).toEqual([]);
      expect(fs.existsSync(marker)).toBe(false);
      // Archived, never deleted: the old tree (same inode) and its bytes are in the fleet archive, recorded MOVING
      // then MOVED-TO after the faulted attempt's MOVING and MOVE-FAILED; the successor is untouched.
      expect(fs.existsSync(dir)).toBe(false);
      expect(fs.lstatSync(archived)).toMatchObject({ dev: target.dev, ino: target.ino });
      expect(fs.readFileSync(path.join(archived, "kept-recorded.txt"), "utf8")).toBe("history\n");
      const lines = fs.readFileSync(path.join(fleet, "MANIFEST.jsonl"), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
      expect(lines.map(line => [line.phase, line.from, line.to])).toEqual([
        ["MOVING", dir, archived], ["MOVE-FAILED", dir, archived], ["MOVING", dir, archived], ["MOVED-TO", dir, archived]]);
      expect(directory.status("reviewer").id).toBe(successor.id);
      expect(fs.readFileSync(successor.sessionFile!, "utf8")).toBe("successor\n");
    } finally {
      archiver.wrap = undefined;
      try { await directory.close(); await manager.close(); } finally { restoreGuards(); }
    }
    expect(blocked).toEqual([]);
  });
});
