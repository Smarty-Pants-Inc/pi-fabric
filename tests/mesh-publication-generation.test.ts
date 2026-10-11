import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { SqliteStateBackend } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";
import { observeActorOwnership, ownershipPointReads, publicationGeneration } from "../src/topology/publication-generation.js";

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
// smarty-dev#6829: the registry save observes only its OWN actors' ownership inputs. This commits a
// shared participant record for the actor (an observed input) whose ownership facts differ.
const actorOwnershipChange = (mesh: MeshStore, actorId: string) => mesh.put({
  key: `topology/participants/${createHash("sha256").update(actorId).digest("hex")}`, identity: owner,
  value: { format: 1, id: actorId, kind: "actor", rootId: "session:gen", ownerHostId: "session:gen", residency: "session" } });

/** smarty-dev#6477: counts full-state reads (a fresh snapshot, a prefix scan, a SQLite export) on `mesh`. */
const fullReads = (mesh: MeshStore) => {
  let count = 0;
  const spies: Array<{ mockRestore(): void }> = [];
  const wrap = <T extends object>(target: T, name: string) => {
    const real = (target as any)[name];
    if (typeof real !== "function") return;
    spies.push(vi.spyOn(target as any, name).mockImplementation(function (this: unknown, ...args: unknown[]) { count++; return real.apply(this, args); }));
  };
  for (const name of ["stateToken", "listAll", "listAllShared", "list"]) wrap(mesh, name);
  const backend = mesh.stateBackendHandle;
  if (backend instanceof SqliteStateBackend) { wrap(backend, "stateToken"); wrap(backend.store, "exportState"); wrap(backend.store, "listAll"); }
  return { count: () => count, restore: () => { for (const spy of spies.splice(0)) spy.mockRestore(); } };
};
const ownerHostKey = (hostId: string) => `topology/hosts/${createHash("sha256").update(hostId).digest("hex")}`;
const actorParticipant = (mesh: MeshStore, actorId: string, ownerHostId: string) => mesh.put({
  key: `topology/participants/${createHash("sha256").update(actorId).digest("hex")}`, identity: owner,
  value: { format: 1, id: actorId, kind: "actor", rootId: "session:gen", ownerHostId, residency: "session" } });
const ownerHost = (mesh: MeshStore, hostId: string, identityId: string) => mesh.put({ key: ownerHostKey(hostId), identity: owner,
  value: { format: 1, id: hostId, rootId: "session:gen", identity: { ...owner, id: identityId }, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 } });

describe.each(["sqlite", "file"] as const)("smarty-dev#6477 ownership validation uses point reads (%s)", (stateBackend) => {
  const options: MeshStoreOptions = { stateBackend };
  const setup = async () => {
    const mesh = new MeshStore(path.join(tempRoot(), "mesh"), 64 * 1024, 100, options);
    closers.push(async () => mesh.closeState());
    await actorParticipant(mesh, "actor:mine", "host:gen");
    await ownerHost(mesh, "host:gen", "session:gen");
    return mesh;
  };

  it("an unrelated commit moves the stamp; validation passes with no full-state read", async () => {
    const mesh = await setup();
    const observed = observeActorOwnership(mesh, ["actor:mine"], ownershipPointReads(mesh));
    await ownershipChange(mesh);
    await mesh.put({ key: "work/unrelated", identity: owner, value: { n: 1 } });
    const full = fullReads(mesh);
    const get = vi.spyOn(mesh, "get");
    expect(observed.unchanged()).toBe(true);
    expect(full.count()).toBe(0);
    // Exactly the observed keys: participant, owner host record, lineage closure.
    expect(get.mock.calls.map(([key]) => key.split("/").slice(0, 2).join("/")).sort())
      .toEqual(["topology/hosts", "topology/lineage-closures", "topology/participants"]);
    expect(get.mock.calls.every(([, read]) => read?.fresh === true)).toBe(true);
  });

  it("a concurrent change of the owner's shared host record or the participant vetoes, with no full-state read", async () => {
    const mesh = await setup();
    const hostMoved = observeActorOwnership(mesh, ["actor:mine"], ownershipPointReads(mesh));
    await ownerHost(mesh, "host:gen", "session:gen-2");
    let full = fullReads(mesh);
    expect(hostMoved.unchanged()).toBe(false);
    expect(full.count()).toBe(0);
    full.restore();

    const participantMoved = observeActorOwnership(mesh, ["actor:mine"], ownershipPointReads(mesh));
    await actorParticipant(mesh, "actor:mine", "host:elsewhere");
    full = fullReads(mesh);
    expect(participantMoved.unchanged()).toBe(false);
    expect(full.count()).toBe(0);
    full.restore();

    const closed = observeActorOwnership(mesh, ["actor:mine"], ownershipPointReads(mesh));
    await mesh.put({ key: `topology/lineage-closures/${createHash("sha256").update("session:gen").digest("hex")}`, identity: owner,
      value: { closedAt: Date.now() } });
    expect(closed.unchanged()).toBe(false);
  });

  it("the observation itself does no full-state read", async () => {
    const mesh = await setup();
    const full = fullReads(mesh);
    observeActorOwnership(mesh, ["actor:mine"], ownershipPointReads(mesh)).unchanged();
    expect(full.count()).toBe(0);
  });
});

describe.each(["sqlite", "file"] as const)("publication generation follows the active state backend (%s)", (stateBackend) => {
  const options: MeshStoreOptions = { stateBackend };

  it("MeshStore names its backend's revision, and an ownership commit moves the generation", async () => {
    const mesh = new MeshStore(path.join(tempRoot(), "mesh"), 64 * 1024, 100, options);
    // pi-fabric#640 (Windows CI): an open state.db handle makes the root's removal fail with EBUSY.
    closers.push(async () => mesh.closeState());
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

  it("closing the store releases the state files, so the root can be renamed or removed (Windows EBUSY, pi-fabric#640)", async () => {
    const mesh = new MeshStore(path.join(tempRoot(), "mesh"), 64 * 1024, 100, options);
    closers.push(async () => mesh.closeState());
    await ownershipChange(mesh);
    const backend = mesh.stateBackendHandle;
    if (stateBackend === "sqlite") expect(fs.existsSync(path.join(mesh.root, "state.db"))).toBe(true);
    mesh.closeState();
    if (backend instanceof SqliteStateBackend) {
      // The backend reports closed: no handle remains and none is reopened behind the caller.
      expect(() => backend.store).toThrow(/closed/);
      expect(backend.stateStamp()).toBeUndefined();
    } else {
      expect(stateBackend).toBe("file");
    }
    // Windows refuses to rename a file with an open handle (EBUSY/EPERM); Linux checks the report above.
    for (const name of fs.readdirSync(mesh.root).filter((entry) => entry.startsWith("state"))) {
      const file = path.join(mesh.root, name);
      fs.renameSync(file, `${file}.moved`);
      fs.renameSync(`${file}.moved`, file);
    }
    fs.rmSync(mesh.root, { recursive: true, force: true });
    expect(fs.existsSync(mesh.root)).toBe(false);
  });

  it.each(["none", "unrelated", "owned"] as const)("ActorManager's registry save validation fails iff its actors' ownership changed after its snapshot (change=%s)", async (change) => {
    const changed = change === "owned";
    const dir = tempRoot();
    const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100, options);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
    const actors = new ActorManager("gen", { id: "session:gen", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, { actorRoot: path.join(dir, "actors"), persistent: true });
    // Closed in reverse: actors, agents, then the store's state handle (MeshStore is not owned by
    // ActorManager; leaving it open fails the root's removal on Windows with EBUSY, pi-fabric#640).
    closers.push(async () => mesh.closeState(), () => agents.close(), () => actors.close());
    const actor = await actors.create({ name: "gen", instructions: "Reply" });
    // The next registry save: ActorManager selects from its ownership snapshot; before the update
    // acquires the registry fence (where it validates), another session commits an ownership change.
    const realUpdate = ActorRegistryStore.prototype.update;
    const realLock = ActorRegistryStore.prototype.withLock;
    const outcomes: boolean[] = [];
    let firstValidationFullReads = -1;
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
        // "unrelated": another host's record moves; the narrow observation (smarty-dev#6829) must
        // not veto. "owned": the actor's own participant record moves; on SQLite only the backend
        // revision sees that commit (state.json is untouched), so it must still veto (pi-fabric#640).
        if (outcomes.length === 0 && change === "unrelated") pending = ownershipChange(mesh);
        if (outcomes.length === 0 && change === "owned") pending = actorOwnershipChange(mesh, actor.id);
        return { ...mutation, validate: () => {
          // smarty-dev#6477: validation runs under the registry locks; a moved stamp re-reads only
          // the observed keys (point reads), never a full fresh snapshot of the shared state.
          const full = fullReads(mesh);
          const valid = mutation.validate();
          if (outcomes.length === 0) firstValidationFullReads = full.count();
          full.restore();
          outcomes.push(valid);
          return valid;
        } };
      });
    } as any);
    await actors.setNice(actor.id, 7);
    // Unchanged: the first validation passes. Changed: it fails, and a fresh selection commits.
    expect(outcomes[0]).toBe(!changed);
    expect(outcomes.at(-1)).toBe(true);
    expect(firstValidationFullReads).toBe(0);
    expect(new ActorRegistryStore(path.join(dir, "actors")).records().find(row => row.id === actor.id)?.nice).toBe(7);
  });
});
