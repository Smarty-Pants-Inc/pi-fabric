// Keep: real native trust boundary + durable data safety, #7991. In-process
// SQLite/registry/queue flow, no worker, process child, actual product removal,
// hosted-only suite, install or live host mutation. Trusted installer policy is
// supplied by this fixture; SQL transactions, inode attestation and sync are real.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { AgentManager, AgentModelAdmission } from "../src/agents/manager.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { readHostPolicy } from "../src/host-policy.js";
import { initializeFreshSlotAuthority, slotPhysicalHostIdentity, slotRoutesDigest, type SlotAuthorityBinding } from "../src/actors/slot-authority.js";
import { SLOT_ADMISSION_PREFIX, slotAdmissionFor } from "../src/actors/slot-admission.js";

vi.mock("../src/host-policy.js", () => ({ readHostPolicy: vi.fn() }));
const identity = { id: "session:native-main", name: "main", kind: "main" as const, sessionId: "native-main" };
const routes = [{ repository: "acme/demo", topic: "github.demo.pulls", maxSlot: 4 }];
const names = ["demo-review-astra", "demo-security-astra"];
const claimKey = "factory/review-slots/v1/acme/demo/1/base";
const roots: string[] = [], meshes: MeshStore[] = [], directories: ActorDirectory[] = [];
const openMesh = (root: string) => { const mesh = new MeshStore(root, 64 * 1024, 1000, { stateBackend: "sqlite" }); meshes.push(mesh); return mesh; };
// Nonprotected fixture contract only; protected slot topics do not grant model exceptions.
const assertModelAllowed: AgentManager["assertModelAllowed"] = (model, runner = DEFAULT_FABRIC_CONFIG.agents.runner) => {
  if ((model !== undefined && model !== "test-model") || (runner !== "pi" && runner !== "pi-durable")) {
    throw new Error(`Unexpected slot fixture model/runner: ${model}/${runner}`);
  }
};
const resolveModelReasonPolicy = (
  ...[model, _reason, runner, options = {}]: Parameters<AgentManager["resolveModelReasonPolicy"]>
): AgentModelAdmission => {
  assertModelAllowed(model, runner);
  return { model, thinking: options.thinking, fallback: false };
};
function openDirectory(mesh: MeshStore, actorRoots: { project: string; session: string }) {
  const run = vi.fn(() => { throw new Error("A worker may not run in this safe native projection suite"); });
  const agents = { config: DEFAULT_FABRIC_CONFIG.agents, resolveKernel: () => undefined, defaultModel: () => undefined,
    assertModelAllowed, resolveModelReasonPolicy, actorArchiveSources: () => new Map(), run, close: async () => {} } as unknown as AgentManager;
  const directory = new ActorDirectory(["native-main", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    persistent: true, rootId: identity.id, claimResidency: "session", releasePaused: true,
    canConsumeMesh: () => false, meshRetentionSweepPath: false, reapDeadSessionPresence: false,
  }], actorRoots, "project");
  directories.push(directory);
  return { directory, run };
}
async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-slot-native-")); roots.push(root);
  const actorRoots = { project: path.join(root, "project-actors"), session: path.join(root, "session-actors") };
  const plan: SlotAuthorityBinding = { protocol: "slot-admission-v1", meshRoot: path.join(root, "mesh"),
    registryRoots: Object.values(actorRoots), rootIds: [identity.id], authorityGeneration: "manager-native-7991-fresh", hubRootId: "hub:manager-test",
    physicalHostIdentity: slotPhysicalHostIdentity(), routesDigest: slotRoutesDigest(routes) };
  let authorities = [plan];
  vi.mocked(readHostPolicy).mockImplementation(() => ({ status: "valid", document: { slotAdmission: { protocol: "slot-admission-v1", routes, authorities } } }));
  const binding = await initializeFreshSlotAuthority(plan.meshRoot);
  authorities = [binding];
  const mesh = openMesh(plan.meshRoot);
  expect(slotAdmissionFor(mesh).status().enforcement).toBe("enforced");
  const claim = await mesh.put({ key: claimKey, identity, value: { version: 1, repository: "acme/demo", slot: 1,
    generation: "base", owner: "native-manager-test", hubRootId: binding.hubRootId, names, claimedBy: identity.id } });
  const request = (scope: "project" | "session"): FabricActorRequest => ({ scope, name: names[0]!, instructions: "Review this exact change.",
    topics: [routes[0]!.topic], idempotencyKey: "native-review-v1", slotClaim: { key: claimKey, ifVersion: claim.version } });
  return { root, actorRoots, mesh, claim, request, ...openDirectory(mesh, actorRoots) };
}
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const mesh of meshes.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) {
    expect(root.startsWith(path.resolve(process.env.TMPDIR!) + path.sep)).toBe(true);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe.runIf(process.platform === "linux")("manager with native fresh SQLite admission", () => {
  it.each(["project", "session"] as const)("fences a %s birth, returns its original retry ID, and reopens its accepted queue", async scope => {
    const f = await fixture();
    const receipts: string[] = [];
    const actor = await f.directory.create(f.request(scope), { beforeCommit: id => { receipts.push(id); } });
    const queued = f.directory.tell(actor.id, "already accepted");
    const retry = await f.directory.create(f.request(scope), { beforeCommit: id => { receipts.push(id); } });
    expect(retry.id).toBe(actor.id);
    expect(receipts).toEqual([actor.id, actor.id]);
    expect(f.directory.status(actor.id).queued).toBe(1);
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toHaveLength(1);
    await f.directory.close();
    const otherMesh = openMesh(f.mesh.root);
    const { directory: restarted, run } = openDirectory(otherMesh, f.actorRoots);
    expect(restarted.listOwned()).toEqual([]); // constructor cannot publish a registry projection
    await restarted.recoverProtectedActors();
    expect(restarted.status(actor.id).queued).toBe(1);
    const history = restarted.messages(actor.id);
    expect(history).toContainEqual(expect.objectContaining({ id: queued.messageId, data: { message: "already accepted" } }));
    const exact = await restarted.create(f.request(scope));
    expect(exact.id).toBe(actor.id);
    expect(restarted.messages(actor.id)).toEqual(history);
    expect(run).not.toHaveBeenCalled();
    expect(f.run).not.toHaveBeenCalled();
  });

  it("parks a registered registry at construction even when the wrong DB reports unenforced", async () => {
    const f = await fixture();
    const actor = await f.directory.create(f.request("session"));
    await f.directory.close();
    const wrongMesh = openMesh(path.join(f.root, "wrong-mesh"));
    const slots = slotAdmissionFor(wrongMesh);
    expect(slots.status()).toMatchObject({ enforcement: "unenforced", reason: "existing-authority" });
    expect(slots.requiresEnforcement(f.request("session").topics!, f.actorRoots.session, identity.id)).toBe(true);
    const { directory: wrong, run } = openDirectory(wrongMesh, f.actorRoots);
    // Before ANY await/recovery, restore must not expose a runnable/publishable actor.
    expect(wrong.listOwned()).toEqual([]);
    expect(wrong.presenceBatch(true).ops).toEqual([]);
    expect(wrongMesh.list("actors/")).toEqual([]);
    const failure = await wrong.recoverProtectedActors().catch(error => error);
    expect(failure).toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    expect(wrong.slotRecoveryStatus()).toMatchObject({ ready: false, error: { code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" } });
    wrong.resumeQueued();
    wrong.resumeAfterRelease();
    expect(wrong.listOwned()).toEqual([]);
    expect(new ActorRegistryStore(f.actorRoots.session).snapshot().actors.map(row => row.id)).toEqual([actor.id]);
    expect(run).not.toHaveBeenCalled();
    expect(f.run).not.toHaveBeenCalled();
  });

  it("projects a committed registryless birth only after a new native recovery sync", async () => {
    const f = await fixture();
    await f.directory.recoverProtectedActors();
    const sync = vi.spyOn(f.mesh.stateBackendHandle, "sync").mockRejectedValueOnce(new Error("injected keyed-state sync failure"));
    const failure = await f.directory.create(f.request("session")).catch(error => error);
    expect(failure).toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN", actorId: expect.any(String) });
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toHaveLength(1);
    expect(fs.existsSync(path.join(f.actorRoots.session, failure.actorId))).toBe(false);
    expect(new ActorRegistryStore(f.actorRoots.session).snapshot().actors).toEqual([]);
    sync.mockRestore();
    await f.directory.recoverProtectedActors();
    expect(f.directory.listOwned().map(actor => actor.id)).toEqual([failure.actorId]);
    const retry = await f.directory.create(f.request("session"));
    expect(retry.id).toBe(failure.actorId);
    expect(f.run).not.toHaveBeenCalled();
  });

  it("refuses a missing claim before stopped cross-scope predecessor or registry effects", async () => {
    const f = await fixture();
    const predecessor = await f.directory.create({ scope: "project", name: names[0]!, instructions: "legacy ordinary observer" });
    await f.directory.stop(predecessor.id);
    const before = new ActorRegistryStore(f.actorRoots.project).snapshot().bytes;
    const { slotClaim: _token, ...unclaimed } = f.request("session");
    const removal = vi.spyOn(f.directory, "remove").mockResolvedValue({ removed: true });
    await expect(f.directory.create(unclaimed)).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "required" });
    expect(removal).not.toHaveBeenCalled();
    expect(new ActorRegistryStore(f.actorRoots.project).snapshot().bytes).toBe(before);
    expect(new ActorRegistryStore(f.actorRoots.session).snapshot().actors).toEqual([]);
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
    expect(f.run).not.toHaveBeenCalled();
  });

  it("rejects claim replacement before birth with no projection and no acceptance callback", async () => {
    const f = await fixture();
    const stale = f.request("session"), accepted = vi.fn();
    await f.mesh.put({ key: claimKey, value: f.claim.value, identity });
    await expect(f.directory.create(stale, { beforeCommit: accepted })).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "stale" });
    expect(accepted).not.toHaveBeenCalled();
    expect(new ActorRegistryStore(f.actorRoots.session).snapshot().actors).toEqual([]);
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
  });
});
