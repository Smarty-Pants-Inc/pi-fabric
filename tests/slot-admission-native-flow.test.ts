// Decision C — KEEP: native trust boundary and data safety (#7991). This is an
// integrated local create/recovery flow: real ActorDirectory/ActorManager,
// SlotAdmissionCoordinator, fresh initializer/binding, MeshStore and SQLite.
// Only the trusted readHostPolicy source boundary is mocked. Its file custody
// checks, installer deployment and real worker execution are NOT proved here.
// No child process, product removal, live host root or hosted-only suite is used.
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AgentManager } from "../src/agents/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import { SLOT_ADMISSION_PREFIX, slotAdmissionFor } from "../src/actors/slot-admission.js";
import { initializeFreshSlotAuthority, slotPhysicalHostIdentity, slotRoutesDigest,
  type SlotAuthorityBinding } from "../src/actors/slot-authority.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readHostPolicy } from "../src/host-policy.js";
import { MeshStore } from "../src/mesh/store.js";

vi.mock("../src/host-policy.js", () => ({ readHostPolicy: vi.fn() }));

const sessionId = "slot-native-flow-main";
const identity = { id: `session:${sessionId}`, name: "main", kind: "main" as const, sessionId };
const routes = [{ repository: "acme/native-flow", topic: "github.native-flow.pulls", maxSlot: 4 }];
const names = ["slot-native-review-astra", "slot-native-security-astra"] as const;
const claimKey = "factory/review-slots/v1/acme/native-flow/1/base";
type Scope = "project" | "session";
type ActorRoots = Record<Scope, string>;
const roots: string[] = [];
const meshes = new Set<MeshStore>();
const directories = new Set<ActorDirectory>();
const launchCounts: Array<() => number> = [];

function privateRoot(): string {
  // Never fall back to a shared /tmp or a production fabric data root.
  const configured = process.env.TMPDIR;
  if (!configured || !path.isAbsolute(configured)) throw new Error("Run with a caller-owned private TMPDIR");
  const parent = fs.realpathSync(configured);
  const stat = fs.statSync(parent);
  if (stat.uid !== process.getuid?.() || (stat.mode & 0o777) !== 0o700) {
    throw new Error("TMPDIR must belong to this uid and have mode 0700");
  }
  const root = fs.mkdtempSync(path.join(parent, "slot-native-flow-"));
  roots.push(root);
  return root;
}

function openMesh(root: string): MeshStore {
  const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
  meshes.add(mesh);
  return mesh;
}

function openDirectory(mesh: MeshStore, actorRoots: ActorRoots, durable = false, meshCursorPath?: string): ActorDirectory {
  let launches = 0;
  // A deliberately non-executing AgentManager fixture, not a mocked create or
  // registry. releasePaused keeps accepted callerless work queued for inspection.
  const agents = {
    config: DEFAULT_FABRIC_CONFIG.agents,
    resolveKernel: () => undefined,
    defaultModel: () => undefined,
    actorArchiveSources: () => new Map(),
    run: async () => { launches++; throw new Error("Worker launch forbidden in native projection flow"); },
    close: async () => {},
  } as unknown as AgentManager;
  launchCounts.push(() => launches);
  const nativeIdentity = durable ? { id: "resident:slot-native-flow", name: "resident", kind: "agent" as const } : identity;
  const directory = new ActorDirectory([sessionId, nativeIdentity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    persistent: true, rootId: identity.id, claimResidency: durable ? "durable" : "session", releasePaused: true,
    canConsumeMesh: () => meshCursorPath !== undefined,
    ...(meshCursorPath === undefined ? {} : { meshCursorPath }),
    meshRetentionSweepPath: false, reapDeadSessionPresence: false,
  }], actorRoots, "project");
  directories.add(directory);
  return directory;
}

async function closeDirectory(directory: ActorDirectory): Promise<void> {
  const closing = directory.close();
  if (vi.isFakeTimers()) await vi.advanceTimersByTimeAsync(1);
  await closing;
  directories.delete(directory);
}

function closeMesh(mesh: MeshStore): void {
  mesh.closeState();
  meshes.delete(mesh);
}

async function fixture() {
  const root = privateRoot();
  const actorRoots: ActorRoots = { project: path.join(root, "project-actors"), session: path.join(root, "session-actors") };
  const plan: SlotAuthorityBinding = {
    protocol: "slot-admission-v1", meshRoot: path.join(root, "mesh"), registryRoots: Object.values(actorRoots),
    rootIds: [identity.id], authorityGeneration: "native-flow-fresh-7991", hubRootId: "hub:native-flow",
    physicalHostIdentity: slotPhysicalHostIdentity(), routesDigest: slotRoutesDigest(routes),
  };
  const publishPolicy = (authorities: SlotAuthorityBinding[]) => {
    vi.mocked(readHostPolicy).mockImplementation(() => ({ status: "valid", document: {
      slotAdmission: { protocol: "slot-admission-v1", routes, authorities },
    } }));
  };
  publishPolicy([plan]);
  const binding = await initializeFreshSlotAuthority(plan.meshRoot);
  publishPolicy([binding]);
  const mesh = openMesh(binding.meshRoot);
  expect(slotAdmissionFor(mesh).status()).toMatchObject({ enforcement: "enforced", databaseFileIdentity: binding.databaseFileIdentity });
  const claim = await mesh.put({ key: claimKey, identity, value: {
    version: 1, repository: routes[0]!.repository, slot: 1, generation: "base", owner: "native-flow-fixture",
    hubRootId: binding.hubRootId, names: [...names], claimedBy: identity.id,
  } });
  const request = (scope: Scope, role: 0 | 1 = 0): FabricActorRequest => ({
    scope, name: names[role], instructions: "Review this exact local change.", topics: [routes[0]!.topic],
    coalesce: false, idempotencyKey: `native-flow-birth-${role}`,
    slotClaim: { key: claimKey, ifVersion: claim.version },
  });
  return { root, actorRoots, binding, publishPolicy, mesh, claim, request, directory: openDirectory(mesh, actorRoots) };
}

const registryRows = (root: string) => new ActorRegistryStore(root).snapshot().actors;
const actorDirs = (root: string) => fs.existsSync(root)
  ? fs.readdirSync(root).filter(name => /^[a-f0-9]{32}$/.test(name)).sort() : [];
function effects(mesh: MeshStore, actorRoots: ActorRoots) {
  return {
    registries: Object.fromEntries(Object.entries(actorRoots).map(([scope, root]) => {
      const file = path.join(root, "actors.json");
      return [scope, { actorDirs: actorDirs(root), bytes: fs.existsSync(file) ? fs.readFileSync(file, "utf8") : null }];
    })),
    presence: mesh.listAll(`actors/${sessionId}/`, { fresh: true }),
    admissions: mesh.listAll(SLOT_ADMISSION_PREFIX, { fresh: true }),
  };
}
function queues(root: string, actorId: string): Record<string, string> {
  const directory = path.join(root, actorId);
  return Object.fromEntries(fs.readdirSync(directory).filter(name => /^queue-.*\.json$/.test(name)).sort()
    .map(name => [name, fs.readFileSync(path.join(directory, name), "utf8")]));
}

// This native acceptance hook is a fail-safe, not a manager/registry mock. Even
// a regression that invokes materialization too early must stop before any
// cross-scope predecessor cleanup. A refusal must retain its structured reason,
// not turn into this error. None of these tests invokes product removal.
const forbidMaterialization = () => { throw new Error("Refused birth reached materialization"); };

afterEach(async () => {
  try {
    await Promise.all([...directories].map(directory => closeDirectory(directory)));
  } finally {
    for (const mesh of meshes) mesh.closeState();
    meshes.clear();
    for (const root of roots.splice(0)) {
      const parent = fs.realpathSync(process.env.TMPDIR!);
      if (path.dirname(root) !== parent || !path.basename(root).startsWith("slot-native-flow-")) {
        throw new Error(`Refusing fixture cleanup outside private TMPDIR: ${root}`);
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }
  for (const count of launchCounts.splice(0)) expect(count()).toBe(0);
});

describe.runIf(process.platform === "linux")("native local slot admission user flow", () => {
  it("refuses missing, stale and foreign claims without touching a cross-scope predecessor, directories or presence", async () => {
    const f = await fixture();
    await f.directory.recoverProtectedActors();
    const predecessor = await f.directory.create({ scope: "project", name: names[0], instructions: "Ordinary predecessor; keep intact." });
    const marker = path.join(f.actorRoots.project, predecessor.id, "keep.txt");
    fs.writeFileSync(marker, "predecessor evidence\n", { mode: 0o600 });
    const { slotClaim: _claim, ...missing } = f.request("session");
    const stale = { ...f.request("session"), slotClaim: { key: claimKey, ifVersion: f.claim.version + 1 } };
    for (const [request, reason] of [[missing, "required"], [stale, "stale"]] as const) {
      const before = effects(f.mesh, f.actorRoots);
      await expect(f.directory.create(request, { beforeMaterialize: forbidMaterialization })).rejects.toMatchObject({
        code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason,
      });
      expect(effects(f.mesh, f.actorRoots)).toEqual(before);
      expect(f.directory.status(predecessor.id)).toMatchObject({ id: predecessor.id, name: names[0], scope: "project", status: "idle" });
      expect(fs.readFileSync(marker, "utf8")).toBe("predecessor evidence\n");
    }
    const foreign = { id: "session:foreign-flow-main", name: "foreign", kind: "main" as const };
    const foreignClaim = await f.mesh.put({ key: claimKey, identity: foreign,
      value: { ...(f.claim.value as Record<string, unknown>), claimedBy: foreign.id } });
    const beforeForeign = effects(f.mesh, f.actorRoots);
    await expect(f.directory.create({ ...f.request("session"), slotClaim: { key: claimKey, ifVersion: foreignClaim.version } }, {
      beforeMaterialize: forbidMaterialization,
    })).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "foreign-root" });
    expect(effects(f.mesh, f.actorRoots)).toEqual(beforeForeign);
    expect(f.directory.status(predecessor.id).id).toBe(predecessor.id);
    expect(fs.readFileSync(marker, "utf8")).toBe("predecessor evidence\n");
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
  });

  it.each(["project", "session"] as const)("admits one %s identity, preserves exact retry/queue, and refuses a duplicate incarnation", async scope => {
    const f = await fixture();
    const actor = await f.directory.create(f.request(scope));
    const first = f.directory.tell(actor.id, "accepted first", { ordinal: 1 });
    const second = f.directory.tell(actor.id, "accepted second", { ordinal: 2 });
    const beforeQueues = queues(f.actorRoots[scope], actor.id);
    expect(Object.values(beforeQueues)).toHaveLength(1);
    expect(JSON.parse(Object.values(beforeQueues)[0]!).items.map((item: { id: string }) => item.id))
      .toEqual([first.messageId, second.messageId]);
    const history = f.directory.messages(actor.id);
    const retry = await f.directory.create(f.request(scope));
    expect(retry).toMatchObject({ id: actor.id, scope, queued: 2 });
    expect(queues(f.actorRoots[scope], actor.id)).toEqual(beforeQueues);
    expect(f.directory.messages(actor.id)).toEqual(history);
    const beforeDuplicate = effects(f.mesh, f.actorRoots);
    await expect(f.directory.create({ ...f.request(scope), idempotencyKey: "different-incarnation" }, {
      beforeMaterialize: forbidMaterialization,
    })).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "occupied" });
    expect(effects(f.mesh, f.actorRoots)).toEqual(beforeDuplicate);
    expect(queues(f.actorRoots[scope], actor.id)).toEqual(beforeQueues);
    expect(registryRows(f.actorRoots[scope]).map(row => row.id)).toEqual([actor.id]);

    // A different MeshStore is a different observer: same physical DB is allowed,
    // but constructor/restore still owes its own awaited native recovery barrier.
    await closeDirectory(f.directory);
    const otherMesh = openMesh(f.mesh.root);
    expect(otherMesh.stateBackendHandle.attestDatabaseFile?.()).toEqual(f.binding.databaseFileIdentity);
    expect(slotAdmissionFor(otherMesh).isCurrent(actor.id, [routes[0]!.topic], f.actorRoots[scope])).toBe(false);
    const restarted = openDirectory(otherMesh, f.actorRoots);
    expect(restarted.listOwned()).toEqual([]);
    expect(restarted.presenceBatch(true).actorIds).not.toContain(actor.id);
    expect(() => restarted.tell(actor.id, "must not enter parked queue")).toThrow();
    await restarted.recoverProtectedActors();
    expect(restarted.status(actor.id)).toMatchObject({ id: actor.id, scope, queued: 2 });
    expect(restarted.messages(actor.id)).toEqual(history);
    expect((await restarted.create(f.request(scope))).id).toBe(actor.id);
    expect(queues(f.actorRoots[scope], actor.id)).toEqual(beforeQueues);
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toHaveLength(1);
  });

  it("creates durable actors on the same bound receiver DB through the common seam, with the verified Main as caller", async () => {
    const f = await fixture();
    await closeDirectory(f.directory);
    const receiverMesh = openMesh(f.mesh.root);
    const receiver = openDirectory(receiverMesh, f.actorRoots, true);
    await receiver.recoverProtectedActors();
    const request = { ...f.request("session"), residency: "durable" as const };
    const options = { asRegistryOwner: true, supervisorOwner: identity };
    const actor = await receiver.create(request, options);
    expect(actor).toMatchObject({ residency: "durable", rootId: identity.id });
    expect(receiverMesh.stateBackendHandle.attestDatabaseFile?.()).toEqual(f.mesh.stateBackendHandle.attestDatabaseFile?.());
    expect(slotAdmissionFor(f.mesh).status()).toMatchObject({ enforcement: "enforced" });
    expect((await receiver.create(request, options)).id).toBe(actor.id);
    receiver.tell(actor.id, "durable accepted input");
    expect(receiver.status(actor.id).queued).toBe(1);
    await closeDirectory(receiver);
    const recovered = openDirectory(openMesh(f.mesh.root), f.actorRoots, true);
    expect(recovered.listOwned()).toEqual([]);
    await recovered.recoverProtectedActors();
    expect(recovered.status(actor.id)).toMatchObject({ residency: "durable", queued: 1 });
    expect((await recovered.create(request, options)).id).toBe(actor.id);
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX, { fresh: true })).toHaveLength(1);
  });

  it("reconstructs registryless intents at exact IDs across both scopes, keeping ordinary actors usable while recovery is parked", async () => {
    const f = await fixture();
    await f.directory.recoverProtectedActors();
    const committed: Array<{ id: string; scope: Scope }> = [];
    for (const [scope, role] of [["project", 0], ["session", 1]] as const) {
      let receiptId: string | undefined;
      // Real post-admission/pre-projection failure: no CAS, writeBatch, sync or
      // registry injection. The known committed ID must survive caller failure.
      const outcome: unknown = await f.directory.create(f.request(scope, role), { onCommit: id => {
        receiptId = id;
        throw new Error("Caller disconnected after durable native intent");
      } }).catch((error: unknown) => error);
      expect(receiptId).toMatch(/^[a-f0-9]{32}$/);
      expect(outcome).toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN", actorId: receiptId });
      committed.push({ id: receiptId!, scope });
      expect(registryRows(f.actorRoots[scope])).toEqual([]);
      expect(actorDirs(f.actorRoots[scope])).toEqual([]);
    }
    const ledger = f.mesh.listAll(SLOT_ADMISSION_PREFIX, { fresh: true });
    expect(ledger).toHaveLength(1);
    expect(ledger[0]!.value).toMatchObject({ roles: {
      review: { actorId: committed[0]!.id, registryRoot: f.actorRoots.project, state: "admitted" },
      security: { actorId: committed[1]!.id, registryRoot: f.actorRoots.session, state: "admitted" },
    } });
    expect(f.mesh.listAll(`actors/${sessionId}/`)).toEqual([]);
    await closeDirectory(f.directory);
    const freshMesh = openMesh(f.mesh.root);
    const restarted = openDirectory(freshMesh, f.actorRoots);
    expect(restarted.slotRecoveryStatus()).toMatchObject({ ready: false });
    expect(restarted.listOwned()).toEqual([]);
    expect(restarted.presenceBatch(true).actorIds).toEqual([]);
    for (const actor of committed) expect(() => restarted.tell(actor.id, "not before recovery")).toThrow();

    // The fixture changes ONLY trusted policy at its source boundary. A changed
    // generation must close protected gates, not disable the whole local manager.
    f.publishPolicy([{ ...f.binding, authorityGeneration: "native-flow-wrong-generation" }]);
    await expect(restarted.recoverProtectedActors()).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    expect(restarted.slotRecoveryStatus()).toMatchObject({ ready: false, error: { code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" } });
    const ordinary = await restarted.create({ scope: "session", name: "ordinary-while-parked", instructions: "Ordinary work stays usable." });
    const ordinaryInput = restarted.tell(ordinary.id, "accepted during protected outage");
    expect(restarted.status(ordinary.id).queued).toBe(1);
    expect(restarted.presenceBatch(true).actorIds).toEqual([ordinary.id]);
    for (const actor of committed) {
      expect(actorDirs(f.actorRoots[actor.scope])).not.toContain(actor.id);
      expect(() => restarted.tell(actor.id, "still not admitted locally")).toThrow();
    }

    f.publishPolicy([f.binding]);
    await restarted.recoverProtectedActors();
    expect(restarted.slotRecoveryStatus()).toEqual({ ready: true });
    for (const [index, actor] of committed.entries()) {
      expect(restarted.status(actor.id)).toMatchObject({ id: actor.id, scope: actor.scope, name: names[index], queued: 0 });
      expect(registryRows(f.actorRoots[actor.scope]).filter(row => row.id === actor.id)).toHaveLength(1);
      expect(slotAdmissionFor(freshMesh).isCurrent(actor.id, [routes[0]!.topic], f.actorRoots[actor.scope])).toBe(true);
      expect((await restarted.create(f.request(actor.scope, index as 0 | 1))).id).toBe(actor.id);
    }
    expect(restarted.listOwned().map(actor => actor.id).sort()).toEqual([...committed.map(actor => actor.id), ordinary.id].sort());
    expect(restarted.presenceBatch(true).actorIds.sort()).toEqual([...committed.map(actor => actor.id), ordinary.id].sort());
    expect(restarted.status(ordinary.id).queued).toBe(1);
    expect(restarted.messages(ordinary.id)).toContainEqual(expect.objectContaining({ id: ordinaryInput.messageId }));
    expect(freshMesh.listAll(SLOT_ADMISSION_PREFIX, { fresh: true })).toEqual(ledger);
  });

  // KEEP: recorded real catch, #1199 round 2 / P1-a. A durable retired OLD
  // generation poisoned actual monitor admission for NEW on the same topic.
  // Real SQLite/coordinator tombstones; no old projection, worker or removal.
  it.each([false, true])("accepts a new generation's mesh input after restart with a retired tombstone (stale registry: %s)", async staleRegistry => {
    const observed: ActorMeshMonitor[] = [];
    const start = ActorMeshMonitor.prototype.start;
    const observer = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
      observed.push(this); start.call(this);
    });
    const flush = async () => { for (let n = 0; n < 64; n++) await Promise.resolve(); };
    try {
      const f = await fixture();
      let oldId: string | undefined;
      await expect(f.directory.create(f.request("project"), { onCommit: id => {
        oldId = id; throw new Error("Disconnect before projection");
      } })).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN" });
      expect(oldId).toMatch(/^[a-f0-9]{32}$/);
      const slots = slotAdmissionFor(f.mesh);
      const oldSpec = (await slots.recoverable(f.actorRoots.project)).find(intent => intent.actorId === oldId)!.spec;
      await closeDirectory(f.directory);
      // Checked native primitives only: this incarnation never projected or ran,
      // so there is no registry/child cleanup to execute or infer from status.
      expect(registryRows(f.actorRoots.project)).toEqual([]);
      expect(actorDirs(f.actorRoots.project)).toEqual([]);
      expect(launchCounts.every(count => count() === 0)).toBe(true);
      await slots.beginRemoval(oldId!, [routes[0]!.topic], f.actorRoots.project);
      expect(await slots.recoverable(f.actorRoots.project)).toContainEqual({ actorId: oldId, spec: oldSpec, state: "revoking" });
      await slots.retire(oldId!, [routes[0]!.topic], f.actorRoots.project);
      const tombstone = f.mesh.listAll(SLOT_ADMISSION_PREFIX, { fresh: true })[0]!;
      expect(tombstone.value).toMatchObject({ sealed: true, roles: { review: { actorId: oldId, state: "retired" } } });
      const newKey = claimKey.replace(/base$/, "r1");
      const newClaim = await f.mesh.put({ key: newKey, identity, value: {
        ...(f.claim.value as Record<string, unknown>), generation: "r1",
        names: ["slot-native-r1-review-astra", "slot-native-r1-security-astra"],
      } });
      const creator = openDirectory(f.mesh, f.actorRoots);
      const actor = await creator.create({ ...f.request("project"), name: "slot-native-r1-review-astra",
        idempotencyKey: "native-flow-r1-birth", slotClaim: { key: newKey, ifVersion: newClaim.version } });
      await closeDirectory(creator);
      const registry = new ActorRegistryStore(f.actorRoots.project);
      if (staleRegistry) registry.write([...registry.snapshot().actors, oldSpec], { durable: true });
      const freshMesh = openMesh(f.mesh.root);
      const cursorPath = path.join(f.root, "retired-cursor.json");
      fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, ...freshMesh.latestCursor() }));
      vi.useFakeTimers(); // Ingress is enabled below; hold retention until close.
      const restarted = openDirectory(freshMesh, f.actorRoots, false, cursorPath);
      const primary = observed[observed.length - 2]!;
      await restarted.recoverProtectedActors(); await flush();
      expect(restarted.listOwned().map(row => row.id)).toEqual([actor.id]);
      expect(slotAdmissionFor(freshMesh).isCurrent(oldId!, [routes[0]!.topic], f.actorRoots.project)).toBe(false);
      await expect(restarted.create(f.request("project"), { beforeMaterialize: forbidMaterialization }))
        .rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "occupied" });
      const ordinary = await restarted.create({ name: "ordinary-after-retirement", instructions: "Observe", topics: ["ordinary.retired"] });
      const input = await freshMesh.publish({ topic: routes[0]!.topic, from: identity });
      primary.schedule(); await flush(); primary.checkpointForRelease();
      expect(Object.values(queues(f.actorRoots.project, actor.id)).join("\n"), "retired generation must not hold new generation input").toContain(input.id);
      expect(JSON.parse(fs.readFileSync(`${cursorPath}.project`, "utf8")).last.id).toBe(input.id);
      if (staleRegistry) {
        // Exercise the registry reload/watch seam too, without deleting old data.
        registry.write(registry.snapshot().actors.map(row => row.id === oldId ? { ...row, updatedAt: Date.now() + 1 } : row), { durable: true });
        restarted.list();
        const next = await freshMesh.publish({ topic: routes[0]!.topic, from: identity });
        primary.schedule(); await flush(); primary.checkpointForRelease();
        expect(Object.values(queues(f.actorRoots.project, actor.id)).join("\n")).toContain(next.id);
        expect(JSON.parse(fs.readFileSync(`${cursorPath}.project`, "utf8")).last.id).toBe(next.id);
      }
      const unchanged = await freshMesh.publish({ topic: "ordinary.retired", from: identity });
      primary.schedule(); await flush(); primary.checkpointForRelease();
      expect(restarted.messages(ordinary.id).some(message => (message.data as { id?: string })?.id === unchanged.id)).toBe(true);
      expect(freshMesh.get(tombstone.key, { fresh: true })).toEqual(tombstone);
      expect(actorDirs(f.actorRoots.project)).not.toContain(oldId);
    } finally {
      await Promise.all([...directories].map(directory => closeDirectory(directory)));
      observer.mockRestore(); vi.useRealTimers();
    }
  });

  it("refuses a byte-copied SQLite authority through the native local create path, despite copied claim and actor IDs", async () => {
    const f = await fixture();
    const actor = await f.directory.create(f.request("session"));
    await closeDirectory(f.directory);
    closeMesh(f.mesh); // SQLite close checkpoints before copying; no live database replacement.
    const copyRoot = path.join(f.root, "copied-mesh");
    fs.mkdirSync(copyRoot, { mode: 0o700 });
    fs.copyFileSync(path.join(f.binding.meshRoot, "state.db"), path.join(copyRoot, "state.db"));
    fs.copyFileSync(path.join(f.binding.meshRoot, "state.json"), path.join(copyRoot, "state.json"));
    const copied = openMesh(copyRoot);
    const copiedLedger = copied.listAll(SLOT_ADMISSION_PREFIX, { fresh: true });
    expect(copiedLedger).toHaveLength(1);
    expect(copiedLedger[0]!.value).toMatchObject({ roles: { review: { actorId: actor.id } } });
    expect(copied.stateBackendHandle.attestDatabaseFile?.()).not.toEqual(f.binding.databaseFileIdentity);
    const wrong = openDirectory(copied, f.actorRoots);
    expect(wrong.listOwned()).toEqual([]);
    expect(wrong.presenceBatch(true).actorIds).not.toContain(actor.id);
    const before = effects(copied, f.actorRoots);
    await expect(wrong.recoverProtectedActors()).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    await expect(wrong.create(f.request("session"), { beforeMaterialize: forbidMaterialization }))
      .rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    expect(effects(copied, f.actorRoots)).toEqual(before);
    expect(() => wrong.tell(actor.id, "copied authority must not accept input")).toThrow();
    expect(wrong.presenceBatch(true).actorIds).not.toContain(actor.id);
    expect(registryRows(f.actorRoots.session).map(row => row.id)).toEqual([actor.id]);
  });
});
