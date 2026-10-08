import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";
import { publicationGeneration } from "../src/topology/publication-generation.js";

// pi-fabric#640 review round 1, P1: on SQLite, state commits never touch state.json, so a
// publication generation stamped from state.json let ActorManager's registry-save validation pass
// on an ownership observation that a later state commit had made stale.
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "publication-generation-")); roots.push(dir); return dir; };
const stat = (file: string) => { try { const s = fs.statSync(file, { bigint: true }); return `${s.ino}:${s.size}:${s.mtimeNs}:${s.ctimeNs}`; } catch { return "absent"; } };
const owner = { id: "session:other", name: "other", kind: "main" as const, sessionId: "other" };
const ownershipChange = (mesh: MeshStore) => mesh.put({ key: "topology/hosts/other", identity: owner,
  value: { format: 1, id: owner.id, rootId: owner.id, identity: owner, startedAt: Date.now(), updatedAt: Date.now(), expiresAt: Date.now() + 15_000 } });

describe.each(["sqlite", "file"] as const)("publication generation follows the active state backend (%s)", (stateBackend) => {
  const options: MeshStoreOptions = { stateBackend };

  it("MeshStore names its backend's revision, and an ownership commit moves the generation", async () => {
    const mesh = new MeshStore(path.join(tempRoot(), "mesh"), 64 * 1024, 100, options);
    expect(mesh.stateBackend).toBe(stateBackend);
    const stateJson = path.join(mesh.root, "state.json");
    const before = { generation: publicationGeneration(mesh), revision: mesh.stateRevision(), file: stat(stateJson),
      stamp: mesh.stateBackendHandle.stateStamp() };
    await ownershipChange(mesh);
    expect(publicationGeneration(mesh)).not.toBe(before.generation);
    if (stateBackend === "sqlite") {
      // The regression: state.json does not move on a SQLite commit; the backend revision does.
      expect(stat(stateJson)).toBe(before.file);
      expect(before.revision).toBe(before.stamp);
      expect(mesh.stateRevision()).not.toBe(before.revision);
    } else {
      expect(before.revision).toBeUndefined();
    }
  });

  it.each([false, true])("ActorManager's registry save validation fails iff ownership changed after its snapshot (changed=%s)", async (changed) => {
    const dir = tempRoot();
    const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100, options);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
    const actors = new ActorManager("gen", { id: "session:gen", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, { actorRoot: path.join(dir, "actors"), persistent: true });
    closers.push(() => agents.close(), () => actors.close());
    const actor = await actors.create({ name: "gen", instructions: "Reply" });
    // The next registry save: ActorManager selects from its ownership snapshot; before the update
    // acquires the registry fence (where it validates), another session commits an ownership change.
    const realUpdate = ActorRegistryStore.prototype.update;
    const realLock = ActorRegistryStore.prototype.withLock;
    const outcomes: boolean[] = [];
    let pending: Promise<unknown> | undefined;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(async function (this: ActorRegistryStore, ...args: any[]) {
      const change = pending;
      pending = undefined;
      await change;
      return (realLock as any).apply(this, args);
    } as any);
    vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(function (this: ActorRegistryStore, select: any) {
      return (realUpdate as any).call(this, (current: unknown) => {
        const mutation = select(current);
        if (!mutation || typeof mutation.validate !== "function") return mutation;
        if (outcomes.length === 0 && changed) pending = ownershipChange(mesh);
        return { ...mutation, validate: () => { const valid = mutation.validate(); outcomes.push(valid); return valid; } };
      });
    } as any);
    await actors.setNice(actor.id, 7);
    // Unchanged: the first validation passes. Changed: it fails, and a fresh selection commits.
    expect(outcomes[0]).toBe(!changed);
    expect(outcomes.at(-1)).toBe(true);
    expect(new ActorRegistryStore(path.join(dir, "actors")).records().find(row => row.id === actor.id)?.nice).toBe(7);
  });
});
