import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { failActorTreeRemoval } from "./helpers/actor-tree-removal.js";

// Supplement to actor-directory.test.ts "#169 round 1 ... exhausted retries": the same exact-id cleanup retry contract,
// recorded instead of deleting (smarty-dev#4664: no product actor tree is actually deleted locally). The first cleanup
// runs the REAL verifiedRemove with the manager's own options; failActorTreeRemoval faults its first tree mutation
// (Linux: the graveyard move; elsewhere: main's path fallback), so the cleanup stays pending. The explicit exact-id
// retry runs the same REAL verifier with dryRun: true only: it verifies the judged tree under the held fence and
// reports success, and the tree stays on disk (a recorded removal, not an absence).
type Verify = typeof import("../src/storage/verified-remove.js").verifiedRemove;
const verifier = vi.hoisted(() => ({ wrap: undefined as undefined | ((real: Verify, ...args: Parameters<Verify>) => ReturnType<Verify>) }));
vi.mock("../src/storage/verified-remove.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  const verifiedRemove: Verify = (...args) => verifier.wrap ? verifier.wrap(actual.verifiedRemove, ...args) : actual.verifiedRemove(...args);
  return { ...actual, verifiedRemove };
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
  it.each(["project", "session"] as const)("retries an exact %s cleanup-only id through the real verifier, recorded", async (scope) => {
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

      let phase: "fault" | "retry" = "fault";
      const calls: Array<{ phase: string; dryRun: boolean; sameInode: boolean; fenced: boolean; removed: boolean }> = [];
      verifier.wrap = (real, at, judged, options) => {
        if (options.deleter !== "actor-tree" || at !== dir) return real(at, judged, options);
        const now = fs.lstatSync(at, { throwIfNoEntry: false });
        const sameInode = !!judged && !!now && now.dev === judged.dev && now.ino === judged.ino &&
          now.dev === target.dev && now.ino === target.ino;
        const fenced = mainPublicationFenced(meshRoot, ROOT_ID);
        // Initial phase: the manager's options object, unchanged, so the real move (and its injected fault) runs.
        const result = phase === "fault" ? real(at, judged, options) : real(at, judged, { ...options, dryRun: true });
        calls.push({ phase, dryRun: phase === "retry", sameInode, fenced, removed: result.removed });
        return result;
      };

      let faults = 0;
      const fault = failActorTreeRemoval(dir, () => { faults++; throw new Error("injected actor tree removal fault"); });
      try {
        await expect(directory.remove(actor.id, { wait: false })).resolves.toMatchObject({
          removed: true, cleaned: false, pending: expect.stringContaining("cleanup failed") });
        await directory.removalSettled(actor.id);
      } finally { fault.mockRestore(); }
      expect(faults).toBe(1);
      expect(calls).toEqual([{ phase: "fault", dryRun: false, sameInode: true, fenced: true, removed: false }]);
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
      expect(calls.slice(1)).toEqual([{ phase: "retry", dryRun: true, sameInode: true, fenced: true, removed: true }]);
      expect(directory.pendingRemovals()).toEqual([]);
      expect(fs.existsSync(marker)).toBe(false);
      // Recorded, not absent: the old tree and its bytes are kept, and the successor is untouched.
      expect(fs.lstatSync(dir)).toMatchObject({ dev: target.dev, ino: target.ino });
      expect(fs.readFileSync(sentinel, "utf8")).toBe("history\n");
      expect(directory.status("reviewer").id).toBe(successor.id);
      expect(fs.readFileSync(successor.sessionFile!, "utf8")).toBe("successor\n");
    } finally {
      verifier.wrap = undefined;
      try { await directory.close(); await manager.close(); } finally { restoreGuards(); }
    }
    expect(blocked).toEqual([]);
  });
});
