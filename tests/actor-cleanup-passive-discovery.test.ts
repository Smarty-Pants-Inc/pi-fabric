import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { ResidencyClient } from "../src/residency/client.js";
import { failActorTreeRemoval } from "./helpers/actor-tree-removal.js";

// A passive Main learns a resident owner's revoked cleanup obligation from its marker on disk, without a
// restart, and routes the exact id to the owner. This standalone suite reproduces that public path on a
// temporary fixture: real ActorDirectory owner/passive pair over one actor root, real AgentsProvider over
// the passive directory, and the real tree deletion. The fault is injected on the fs move of this fixture's
// own actor tree (failActorTreeRemoval), never on the removal helper or manager state.
// Proof boundary: the resident hop is an injected ResidencyClient whose removeActor calls owner.remove for
// the routed id. It exercises provider routing and both directories, not the resident host wire protocol.
// Invariants:
// - after the owner finishes the routed cleanup, the passive actors view no longer lists the settled id;
// - an owned known-inode obligation whose marker disappears before completion stays pending and reported;
// - while the marker is present, the passive view keeps the obligation visible and routes its exact id,
//   and a routed attempt that is still pending does not clear the passive row.
// Linux only: the judged-inode (tracked tree) removal obligation is the Linux deletion path. Other
// platforms use a path-based fallback or a native helper and are covered by hosted suites.

const PREFIX = "fabric-passive-cleanup-";
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];

afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  const tmp = fs.realpathSync(os.tmpdir());
  for (const root of roots.splice(0)) {
    // Only this suite's own mkdtemp fixture is removed.
    let own = false;
    try { own = path.dirname(fs.realpathSync(root)) === tmp && path.basename(root).startsWith(PREFIX); }
    catch { /* already gone */ }
    if (own) fs.rmSync(root, { recursive: true, force: true });
  }
});

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), PREFIX));
  roots.push(root);
  const identity: MeshIdentity = { id: "session:neutral", name: "main", kind: "main", sessionId: "neutral" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents,
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  closers.push(() => agents.close()); // closed last (closers run in reverse)
  const actorRoots = { project: path.join(root, "actors", "project"), session: path.join(root, "actors", "session") };
  const directory = (options: { claimResidency: "durable" } | { canManageActor: (id: string) => boolean | undefined }) => {
    const opened = new ActorDirectory(["neutral", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {},
      { persistent: true, rootId: identity.id, ...options }], actorRoots, "project");
    closers.push(() => opened.close());
    return opened;
  };
  // The resident owner claims durable actors; Main's passive view never manages them.
  const owner = directory({ claimResidency: "durable" });
  const passive = directory({ canManageActor: () => false });
  const unused = (): never => { throw new Error("unexpected Main routing in this fixture"); };
  const participants = { list: () => [], get: () => undefined, self: unused, peers: () => [], refresh: async () => {}, scheduleRefresh() {} };
  const mainAgent = { id: identity.id, local: true, matches: (id: string) => id === identity.id || id === "main", info: unused, deliverAgent: unused };
  const lifecycle = new LifecycleBroker(mesh, passive.identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, () => {});
  closers.push(() => lifecycle.close());
  // Injected resident hop: the exact routed id goes to the owner directory's public remove.
  const removeActor = vi.fn((id: string, _signal?: AbortSignal) => owner.remove(id));
  const provider = new AgentsProvider(agents, passive, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants,
    undefined, lifecycle, undefined, { removeActor } as unknown as ResidencyClient, false);
  closers.push(() => provider.close());
  const context: FabricInvocationContext = { cwd: root, signal: new AbortController().signal, parentToolCallId: "passive-cleanup",
    nestedToolCallId: "passive-cleanup", extensionContext: {} as FabricInvocationContext["extensionContext"], update() {} };
  return { identity, actorRoots, owner, passive, provider, removeActor, context };
};

type Fixture = ReturnType<typeof fixture>;

/** A durable owner actor whose first removal revokes it but leaves a pending cleanup: the tree move failed. */
const pendingObligation = async (state: Fixture) => {
  const actor = await state.owner.create({ name: "resident obligation", instructions: "Work.", residency: "durable" });
  // The passive Main had the pre-removal view of the owner's actor, without managing it.
  expect(state.passive.list().map((entry) => entry.id)).toContain(actor.id);
  expect(state.passive.owns(actor.id)).toBe(false);
  const dir = path.join(state.actorRoots.project, actor.id);
  const marker = path.join(state.actorRoots.project, `removal-${actor.id}.json`);
  const failing = failActorTreeRemoval(dir, () => { throw new Error("cleanup unavailable"); });
  try {
    await expect(state.owner.remove(actor.id)).resolves.toMatchObject({
      removed: true, cleaned: false, pending: expect.stringContaining("removal cleanup failed") });
  } finally { failing.mockRestore(); }
  // A revoked, durable obligation: marker kept, actor tree still at its own name (no move happened).
  expect(fs.existsSync(marker)).toBe(true);
  expect(fs.lstatSync(dir).isDirectory()).toBe(true);
  expect(state.owner.pendingRemovals()).toContainEqual(expect.objectContaining({ id: actor.id }));
  return { actor, dir, marker };
};

const listed = async (state: Fixture): Promise<Array<{ id: string; removal?: unknown }>> =>
  await state.provider.invoke("actors", {}, state.context) as Array<{ id: string; removal?: unknown }>;

describe.skipIf(process.platform !== "linux")("passive discovery of a resident cleanup obligation", () => {
  it("drops the settled id from the passive actors view after the routed owner cleanup completes", async () => {
    const state = fixture();
    const { actor, dir, marker } = await pendingObligation(state);
    await expect(state.provider.invoke("remove", { id: actor.id }, state.context)).resolves.toEqual({ removed: true });
    expect(state.removeActor).toHaveBeenCalledExactlyOnceWith(actor.id, state.context.signal);
    // The owner really finished it: tree, marker and owner obligation are gone.
    expect(fs.existsSync(dir)).toBe(false);
    expect(fs.existsSync(marker)).toBe(false);
    expect(state.owner.pendingRemovals()).not.toContainEqual(expect.objectContaining({ id: actor.id }));
    // The passive view must not keep reporting the settled id.
    expect(await listed(state)).not.toContainEqual(expect.objectContaining({ id: actor.id }));
    expect(state.passive.pendingRemovals()).not.toContainEqual(expect.objectContaining({ id: actor.id }));
    expect(state.passive.cleanupObligation(actor.id)).toBeUndefined();
  });

  it("keeps an owned known-inode obligation pending when its marker disappears before completion", async () => {
    const state = fixture();
    const { actor, dir, marker } = await pendingObligation(state);
    // Losing this fixture's marker file is not proof that the owed tree was removed.
    fs.rmSync(marker);
    expect(state.owner.pendingRemovals()).toContainEqual(expect.objectContaining({
      id: actor.id, state: expect.stringContaining("removal cleanup failed") }));
    expect(state.owner.list()).toContainEqual(expect.objectContaining({ id: actor.id, status: "stopped", removal: expect.any(Object) }));
    expect(state.owner.cleanupObligation(actor.id)).toMatchObject({ id: actor.id, status: "stopped", residency: "durable" });
    expect(fs.lstatSync(dir).isDirectory()).toBe(true);
    // A later exact-id remove by the owner still finishes the owed tree.
    await expect(state.owner.remove(actor.id)).resolves.toEqual({ removed: true });
    expect(fs.existsSync(dir)).toBe(false);
    expect(fs.existsSync(marker)).toBe(false);
    expect(state.owner.pendingRemovals()).not.toContainEqual(expect.objectContaining({ id: actor.id }));
    expect(state.owner.list()).not.toContainEqual(expect.objectContaining({ id: actor.id }));
  });

  it("keeps a marker-present obligation visible and routable, and a routed pending failure does not clear it", async () => {
    const state = fixture();
    const { actor, dir, marker } = await pendingObligation(state);
    // Discovered from the marker without a restart.
    expect(await listed(state)).toContainEqual(expect.objectContaining({ id: actor.id, removal: expect.any(Object) }));
    expect(state.passive.cleanupObligation(actor.id)).toMatchObject({ id: actor.id, status: "stopped", rootId: state.identity.id, residency: "durable" });
    const failing = failActorTreeRemoval(dir, () => { throw new Error("cleanup still unavailable"); });
    try {
      for (let attempt = 1; attempt <= 2; attempt++) {
        await expect(state.provider.invoke("remove", { id: actor.id }, state.context)).resolves.toMatchObject({
          removed: true, cleaned: false, pending: expect.stringContaining("removal cleanup failed") });
        expect(state.removeActor).toHaveBeenCalledTimes(attempt);
        expect(state.removeActor).toHaveBeenLastCalledWith(actor.id, state.context.signal);
        // Still owed by the owner, so the passive row stays visible and routable.
        expect(fs.existsSync(marker)).toBe(true);
        expect(fs.lstatSync(dir).isDirectory()).toBe(true);
        expect(state.owner.pendingRemovals()).toContainEqual(expect.objectContaining({ id: actor.id }));
        expect(await listed(state)).toContainEqual(expect.objectContaining({ id: actor.id, removal: expect.any(Object) }));
        expect(state.passive.pendingRemovals()).toContainEqual(expect.objectContaining({ id: actor.id }));
        expect(state.passive.cleanupObligation(actor.id)).toMatchObject({ id: actor.id, status: "stopped" });
      }
    } finally { failing.mockRestore(); }
  });
});
