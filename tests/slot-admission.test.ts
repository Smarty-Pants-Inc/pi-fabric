// Keep: native trust boundary + durable data safety (#7991). Real SQLite transactions, not mocked CAS.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { SqliteStateStore, openNodeSqlite } from "../src/mesh/state-sqlite.js";
import { readHostPolicy } from "../src/host-policy.js";
import { initializeFreshSlotAuthority, slotPhysicalHostIdentity, slotRoutesDigest, slotBindingSeal, NativeSlotAuthority,
  type SlotAuthorityBinding, type SlotRoute } from "../src/actors/slot-authority.js";
import { ActorSlotClaimRefusedError, ActorSlotCreateUncertainError, slotAdmissionFor,
  SLOT_ADMISSION_PREFIX } from "../src/actors/slot-admission.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";

// Only trusted installer policy is supplied by the harness. No state/transaction/commit is mocked.
vi.mock("../src/host-policy.js", () => ({ readHostPolicy: vi.fn() }));
const caller: MeshIdentity = { id: "session:registered-main", name: "Main", kind: "main" };
const routes: SlotRoute[] = [{ repository: "acme/demo", topic: "github.demo.pulls", maxSlot: 8, denominators: [2, 4, 8] }];
const names = ["demo-shards2-review-astra", "demo-shards2-security-astra"];
const key = "factory/review-slots/v1/acme/demo/1/base";
const admissionKey = SLOT_ADMISSION_PREFIX + "acme/demo/1/base";
const roots: string[] = [];
const meshes: MeshStore[] = [];
let document: Record<string, unknown>;
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "slot-native-")); roots.push(root); return root; };
const meshAt = (root: string) => { const mesh = new MeshStore(root, 64 * 1024, 10_000, { stateBackend: "sqlite" }); meshes.push(mesh); return mesh; };
const installPolicy = (authorities: SlotAuthorityBinding[]) => {
  document = { slotAdmission: { protocol: "slot-admission-v1", routes, authorities } };
  vi.mocked(readHostPolicy).mockImplementation(() => ({ status: "valid", document }));
};
const planAt = (root: string): SlotAuthorityBinding => ({ protocol: "slot-admission-v1", meshRoot: path.join(root, "mesh"),
  registryRoots: [path.join(root, "project-actors"), path.join(root, "session-actors")], rootIds: [caller.id],
  authorityGeneration: "fresh-generation-7991-unique", hubRootId: "hub-root-7991", physicalHostIdentity: slotPhysicalHostIdentity(),
  routesDigest: slotRoutesDigest(routes) });
const fresh = async () => {
  const root = temp();
  const plan = planAt(root);
  installPolicy([plan]);
  const binding = await initializeFreshSlotAuthority(plan.meshRoot);
  installPolicy([binding]);
  const mesh = meshAt(plan.meshRoot);
  const coordinator = slotAdmissionFor(mesh);
  const claim = await mesh.put({ key, identity: caller, value: { version: 1, repository: "acme/demo", slot: 1, generation: "base",
    owner: "native-test", hubRootId: binding.hubRootId, names, claimedBy: caller.id } });
  const input = (actorId = "actor:review-1", role = 0, revision = claim.version) => {
    const request: FabricActorRequest & { slotClaim: { key: string; ifVersion: number } } = {
      name: names[role]!, instructions: "Resolved instructions (not a file reference)", topics: [routes[0]!.topic],
      scope: "session", residency: "durable", idempotencyKey: `birth-${role}`, slotClaim: { key, ifVersion: revision },
    };
    return { actorId, request, spec: { ...request, id: actorId, rootId: caller.id, createdAt: Date.now(), updatedAt: Date.now(),
      sessionFile: path.join(root, actorId.replace(":", "-"), "session.jsonl"), messages: [], principalScope: { kind: "root", id: caller.id } },
      registryRoot: binding.registryRoots[1]!, caller };
  };
  return { root, mesh, binding, coordinator, claim, input };
};
afterEach(() => {
  for (const mesh of meshes.splice(0)) mesh.closeState();
  // All fixtures are under the test harness's caller-owned TMPDIR, never fleet roots.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("native fresh authority bootstrap", () => {
  it("publishes no authority from empty existing DB, and leaves one-shot crash orphans blocked (#13036)", async () => {
    const root = temp(), plan = planAt(root);
    installPolicy([plan]);
    fs.mkdirSync(plan.meshRoot);
    fs.writeFileSync(path.join(plan.meshRoot, "state.db"), "", { mode: 0o600 });
    await expect(initializeFreshSlotAuthority(plan.meshRoot)).rejects.toMatchObject({
      code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "unsupported-authority", details: { issue: "smarty-dev#13036" },
    });
    expect(fs.statSync(path.join(plan.meshRoot, "state.db")).size).toBe(0);
    expect(slotAdmissionFor(meshAt(plan.meshRoot)).status()).toMatchObject({ enforcement: "blocked" });
  });

  it("refuses existing authority, prior file artifacts, and nonempty registered registries", async () => {
    for (const artifact of ["state.json", "registry", "empty-sqlite"]) {
      const root = temp(), plan = planAt(root);
      installPolicy([plan]);
      if (artifact === "registry") {
        fs.mkdirSync(plan.registryRoots[0]!);
        fs.writeFileSync(path.join(plan.registryRoots[0]!, "actors.json"), '{"actors":[]}');
      } else if (artifact === "empty-sqlite") {
        const mesh = meshAt(plan.meshRoot);
        await mesh.confirmWritable();
      } else {
        fs.mkdirSync(plan.meshRoot);
        fs.writeFileSync(path.join(plan.meshRoot, artifact), '{"format":1,"entries":{}}');
      }
      await expect(initializeFreshSlotAuthority(plan.meshRoot)).rejects.toMatchObject({ details: { issue: "smarty-dev#13036" } });
    }
  });

  it("requires root policy publication of the completed inode binding and seals fresh provenance outside KV", async () => {
    const root = temp(), plan = planAt(root);
    installPolicy([plan]);
    const binding = await initializeFreshSlotAuthority(plan.meshRoot);
    const before = slotAdmissionFor(meshAt(plan.meshRoot));
    expect(before.status().enforcement).toBe("blocked");
    installPolicy([binding]);
    const mesh = meshAt(plan.meshRoot);
    expect(slotAdmissionFor(mesh)).toBe(slotAdmissionFor(mesh));
    expect(slotAdmissionFor(mesh).status()).toMatchObject({ enforcement: "enforced", authorityGeneration: plan.authorityGeneration });
    expect(mesh.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
    expect(mesh.stateBackendHandle.slotAuthorityBinding?.()).toBe(slotBindingSeal(binding));
    await expect(initializeFreshSlotAuthority(plan.meshRoot)).rejects.toBeInstanceOf(ActorSlotClaimRefusedError);
    // A policy change must not silently unpark a blocked already-attached runtime.
    expect(before.status().enforcement).toBe("blocked");
  });

  it("leaves direct existing SQLite allowed UNENFORCED with an explicit #7991/#13036 gap", async () => {
    installPolicy([]);
    const mesh = meshAt(path.join(temp(), "existing")), coordinator = slotAdmissionFor(mesh);
    const status = coordinator.status();
    expect(status).toMatchObject({ enforcement: "unenforced", reason: "existing-authority", gap: "smarty-dev#7991", issue: "smarty-dev#13036" });
    expect(coordinator.isProtected([routes[0]!.topic])).toBe(true);
    await expect(coordinator.admit({ actorId: "ordinary", request: { name: "legacy", instructions: "", topics: [routes[0]!.topic] },
      spec: {}, registryRoot: path.join(temp(), "legacy"), caller })).resolves.toBeUndefined();
    expect(coordinator.isCurrent("legacy", [routes[0]!.topic], path.join(temp(), "legacy"))).toBe(true);
    coordinator.guardClaimMutation(key);
    expect(coordinator.status()).toEqual(status); // read-only
    expect(coordinator.requiresRecovery(path.join(temp(), "legacy-registry"))).toBe(false);
    document = {};
    const ordinary = slotAdmissionFor(meshAt(path.join(temp(), "ordinary")));
    expect(ordinary.status()).toMatchObject({ enforcement: "unenforced", reason: "not-configured" });
    expect(ordinary.requiresRecovery(path.join(temp(), "ordinary-registry"))).toBe(false);
  });

  it("keeps matching-binding NATS births/lifecycle allowed UNENFORCED without invoking any SQLite gate", async () => {
    // Policy-branch evidence only, not a NATS server/persistence test. The real trusted fresh
    // binding remains configured; the backend surface throws if enforcement tries to use it.
    const f = await fresh(), backendAccess = vi.fn(() => { throw new Error("NATS must not use a SQLite gate"); });
    const nats = { root: f.mesh.root, stateBackend: "nats", get stateBackendHandle() { return backendAccess(); } } as unknown as MeshStore;
    const native = new NativeSlotAuthority(nats), slots = slotAdmissionFor(nats), registry = f.binding.registryRoots[1]!;
    expect(native.enforced).toBe(false);
    expect(native.forOperation(registry, caller.id)).toBe(false);
    expect(slots.status()).toMatchObject({ enforcement: "unenforced", reason: "nats-backend", gap: "smarty-dev#7991" });
    expect(slots.requiresEnforcement([routes[0]!.topic], registry, caller.id)).toBe(false);
    expect(slots.requiresRecovery(registry, caller.id)).toBe(false);
    await expect(slots.admit(f.input())).resolves.toBeUndefined();
    expect(slots.isCurrent("legacy-nats", [routes[0]!.topic], registry)).toBe(true);
    expect(slots.withCurrent("legacy-nats", [routes[0]!.topic], registry, () => "allowed")).toBe("allowed");
    expect(await slots.recoverable(registry)).toEqual([]);
    await slots.beginRemoval("legacy-nats", [routes[0]!.topic], registry);
    await slots.retire("legacy-nats", [routes[0]!.topic], registry);
    slots.guardClaimMutation(key);
    expect(backendAccess).not.toHaveBeenCalled();
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("does not let a separate NATS target's unenforced status downgrade a registered enforcing origin/registry", async () => {
    const f = await fresh(), backendAccess = vi.fn(() => { throw new Error("Wrong target must refuse before backend access"); });
    const nats = { root: path.join(f.root, "separate-nats"), stateBackend: "nats", get stateBackendHandle() { return backendAccess(); } } as unknown as MeshStore;
    const native = new NativeSlotAuthority(nats), slots = slotAdmissionFor(nats), registry = f.binding.registryRoots[1]!;
    expect(native.enforced).toBe(false);
    expect(slots.status()).toMatchObject({ enforcement: "unenforced", reason: "nats-backend" });
    expect(() => native.forOperation(registry, caller.id)).toThrow(/different physical mesh authority/);
    expect(slots.requiresEnforcement([routes[0]!.topic], registry, caller.id)).toBe(true);
    expect(slots.requiresRecovery(registry, caller.id)).toBe(true);
    await expect(slots.admit(f.input())).rejects.toMatchObject({ reason: "authority-unavailable" });
    expect(slots.isCurrent("copied-legacy-row", [routes[0]!.topic], registry)).toBe(false);
    expect(backendAccess).not.toHaveBeenCalled();
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("attests actual native connection fds across independent stores and refuses a replaced inode", async () => {
    const f = await fresh();
    const other = meshAt(f.mesh.root);
    expect(other.stateBackendHandle.attestDatabaseFile?.()).toEqual(f.binding.databaseFileIdentity);
    expect(f.coordinator.status().enforcement).toBe("enforced");
    const database = path.join(f.mesh.root, "state.db");
    fs.renameSync(database, `${database}.old`); // inside owned TMPDIR; no delete of the live file
    fs.copyFileSync(`${database}.old`, database);
    expect(f.coordinator.status().enforcement).toBe("blocked");
    expect(() => f.coordinator.guardClaimMutation(key)).toThrow();
    await expect(f.coordinator.admit(f.input())).rejects.toThrow();
    // All native claim write paths retain a final inode fence, not just the provider's earlier check.
    await expect(f.mesh.put({ key, value: f.claim.value, identity: caller })).rejects.toMatchObject({ reason: "authority-unavailable" });
    await expect(f.mesh.delete({ key })).rejects.toMatchObject({ reason: "authority-unavailable" });
    await expect(f.mesh.writeBatch({ identity: caller, ops: [], prepare: () => [{ kind: "put", key, value: f.claim.value }] }))
      .rejects.toMatchObject({ reason: "authority-unavailable" });
  });

  it("refuses symlink alias, copied DB with copied claim/UUID, and an enforcing origin on an unbound resident", async () => {
    const f = await fresh();
    const alias = path.join(f.root, "alias");
    fs.symlinkSync(f.mesh.root, alias, "dir");
    const aliasMesh = meshAt(alias);
    expect(slotAdmissionFor(aliasMesh).status().enforcement).toBe("blocked");
    const copiedRoot = path.join(f.root, "copied");
    const copied = meshAt(copiedRoot);
    await copied.put({ key, value: f.claim.value, identity: caller });
    const target = slotAdmissionFor(copied);
    expect(target.status().enforcement).toBe("unenforced"); // direct existing remains supported
    expect(target.requiresEnforcement([routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(true);
    expect(target.requiresRecovery(f.binding.registryRoots[1]!)).toBe(true);
    expect(target.requiresEnforcement([routes[0]!.topic], path.join(f.root, "other-registry"), caller.id)).toBe(true);
    expect(target.requiresEnforcement([routes[0]!.topic], path.join(f.root, "unregistered-direct-registry"))).toBe(false);
    expect(target.requiresEnforcement(["ordinary.topic"], f.binding.registryRoots[1]!, caller.id)).toBe(false);
    fs.mkdirSync(f.binding.registryRoots[1]!, { recursive: true, mode: 0o700 });
    const registryAlias = path.join(f.root, "registry-alias");
    fs.symlinkSync(f.binding.registryRoots[1]!, registryAlias, "dir");
    expect(target.requiresEnforcement([routes[0]!.topic], registryAlias)).toBe(true);
    expect(target.isCurrent("legacy-row", [routes[0]!.topic], registryAlias)).toBe(false);
    await expect(target.admit({ ...f.input(), registryRoot: path.join(f.root, "other-registry") })).rejects.toMatchObject({ reason: "authority-unavailable" });
    await expect(target.admit(f.input())).rejects.toMatchObject({ reason: "authority-unavailable" });
    expect(copied.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
  });

  it("rejects adapters without connection attestation even if their configured path is right", async () => {
    const f = await fresh();
    vi.spyOn(f.mesh.stateBackendHandle, "attestDatabaseFile").mockImplementation(() => { throw new Error("unattestable adapter"); });
    expect(f.coordinator.status().enforcement).toBe("blocked");
    expect(f.coordinator.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(false);
    await expect(f.coordinator.admit(f.input())).rejects.toThrow("unattestable adapter");
  });
});

describe("same-transaction permanent actor-birth ledger", () => {
  it("rejects missing, malformed, stale, foreign and multi-slot claims before the acceptance callback or any intent", async () => {
    const f = await fresh();
    const callback = vi.fn();
    const missing = f.input(); delete (missing.request as Partial<typeof missing.request>).slotClaim;
    await expect(f.coordinator.admit({ ...missing, beforeCommit: callback })).rejects.toMatchObject({ reason: "required" });
    for (const revision of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, f.claim.version + 1]) {
      await expect(f.coordinator.admit({ ...f.input("actor:x", 0, revision), beforeCommit: callback })).rejects.toBeInstanceOf(ActorSlotClaimRefusedError);
    }
    const foreign = { ...f.input(), caller: { ...caller, id: "session:foreign" } };
    await expect(f.coordinator.admit({ ...foreign, beforeCommit: callback })).rejects.toMatchObject({ reason: "foreign-root" });
    const multi = f.input(); multi.request.topics!.push("github.demo.pulls.shard2of2");
    await expect(f.coordinator.admit({ ...multi, beforeCommit: callback })).rejects.toMatchObject({ reason: "invalid" });
    expect(callback).not.toHaveBeenCalled();
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toEqual([]);
  });

  it("guards canonical receiver spellings and protects malformed/unsupported shard forms", async () => {
    const f = await fresh();
    for (const topic of ["github.demo.pulls", "github.demo.pulls.shard1", "github.demo.pulls.shard2of4", "github.demo.pulls.shard01", "github.demo.pulls.shard9", "github.demo.pulls.shard1of3"]) {
      expect(f.coordinator.isProtected([topic])).toBe(true);
    }
    expect(f.coordinator.isProtected(["github.other.pulls", "github.demo.observers"])).toBe(false);
    const bad = f.input(); bad.request.topics = ["github.demo.pulls.shard01"];
    await expect(f.coordinator.admit(bad)).rejects.toMatchObject({ reason: "invalid" });
  });

  it("commits one original ID, binds native caller/spec/registry, and passes reused ID to final acceptance", async () => {
    const f = await fresh();
    const callback = vi.fn();
    const born = await f.coordinator.admit({ ...f.input(), beforeCommit: callback });
    expect(born).toMatchObject({ actorId: "actor:review-1", reused: false });
    const originalSpec = born!.spec;
    expect(callback).toHaveBeenLastCalledWith("actor:review-1");
    const retry = await f.coordinator.admit({ ...f.input("actor:discarded-preallocation"), beforeCommit: callback });
    expect(retry).toEqual({ actorId: "actor:review-1", spec: originalSpec, reused: true });
    expect(callback).toHaveBeenLastCalledWith("actor:review-1");
    const changed = f.input("actor:another"); changed.request.instructions = "Changed"; changed.spec.instructions = "Changed";
    await expect(f.coordinator.admit(changed)).rejects.toMatchObject({ reason: "occupied" });
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toHaveLength(1);
    expect(f.coordinator.withCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!, () => "enabled")).toBe("enabled");
  });

  it("uses distinct stores on the same physical database and requires each observer's own sync before enable", async () => {
    const f = await fresh();
    await f.coordinator.admit(f.input());
    const other = meshAt(f.mesh.root), receiver = slotAdmissionFor(other);
    expect(receiver.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(false);
    expect(await receiver.recoverable(f.binding.registryRoots[1]!)).toMatchObject([{ actorId: "actor:review-1", state: "admitted" }]);
    expect(receiver.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(true);
    expect(await receiver.recoverable(f.binding.registryRoots[0]!)).toEqual([]);
    expect(fs.existsSync(path.join(f.binding.registryRoots[1]!, "actors.json"))).toBe(false); // ledger-only replay
  });

  it("serializes competing same-role births and keeps one durable incarnation", async () => {
    const f = await fresh(), other = slotAdmissionFor(meshAt(f.mesh.root));
    const rival = f.input("actor:rival"); rival.request.idempotencyKey = "other-request";
    const results = await Promise.allSettled([f.coordinator.admit(f.input()), other.admit(rival)]);
    expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter(result => result.status === "rejected")).toHaveLength(1);
    expect(f.mesh.listAll(SLOT_ADMISSION_PREFIX)).toHaveLength(1);
  });

  it("fences claim ABA and prevents mixed-partner epochs but reconciles the unchanged original ID", async () => {
    const f = await fresh();
    await f.coordinator.admit(f.input());
    await f.mesh.delete({ key, ifVersion: f.claim.version });
    const replacement = await f.mesh.put({ key, value: f.claim.value, identity: caller });
    expect(replacement.version).toBeGreaterThan(f.claim.version);
    await expect(f.coordinator.admit(f.input("actor:security", 1))).rejects.toMatchObject({ reason: "stale" });
    await expect(f.coordinator.admit(f.input("actor:security", 1, replacement.version))).rejects.toMatchObject({ reason: "occupied" });
    expect(await f.coordinator.admit(f.input("actor:discarded"))).toMatchObject({ actorId: "actor:review-1", reused: true });
  });

  it("classifies sync failure after COMMIT as uncertain/occupied, then replays exact ID after a new barrier", async () => {
    const f = await fresh();
    const sync = vi.spyOn(f.mesh.stateBackendHandle, "sync").mockRejectedValueOnce(new Error("injected sync uncertainty"));
    await expect(f.coordinator.admit(f.input())).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN",
      actorId: "actor:review-1", claimKey: key, claimVersion: f.claim.version, requestKey: "birth-0" });
    expect(f.mesh.get(admissionKey)).toBeDefined();
    expect(f.coordinator.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(false);
    sync.mockRestore();
    const recovered = await slotAdmissionFor(meshAt(f.mesh.root)).recoverable(f.binding.registryRoots[1]!);
    expect(recovered).toMatchObject([{ actorId: "actor:review-1", state: "admitted" }]);
  });

  it("rolls back a failing native acceptance fence and conservatively reports uncertainty with selected ID", async () => {
    const f = await fresh();
    await expect(f.coordinator.admit({ ...f.input(), beforeCommit: () => { throw new Error("acceptance transport unknown"); } }))
      .rejects.toBeInstanceOf(ActorSlotCreateUncertainError);
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("keeps permanent retired occupancy, seals an absent partner, and never lets a new token/key reopen it", async () => {
    const f = await fresh();
    await f.coordinator.admit(f.input());
    await expect(f.coordinator.retire("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).rejects.toMatchObject({ reason: "invalid" });
    await f.coordinator.beginRemoval("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!);
    expect(f.coordinator.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(false);
    await f.coordinator.retire("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!);
    const entry = f.mesh.get(admissionKey)!;
    expect(entry.value).toMatchObject({ sealed: true, roles: { review: { actorId: "actor:review-1", state: "retired" } } });
    await expect(f.coordinator.admit(f.input())).rejects.toMatchObject({ reason: "occupied" });
    const newClaim = await f.mesh.put({ key, value: f.claim.value, identity: caller });
    const newRequest = f.input("actor:new", 1, newClaim.version); newRequest.request.idempotencyKey = "new-security";
    await expect(f.coordinator.admit(newRequest)).rejects.toMatchObject({ reason: "occupied" });
    const restarted = slotAdmissionFor(meshAt(f.mesh.root));
    expect(await restarted.recoverable(f.binding.registryRoots[1]!)).toMatchObject([{ actorId: "actor:review-1", state: "retired" }]);
    expect(restarted.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(false);
    await expect(restarted.beginRemoval("actor:wrong", [routes[0]!.topic], f.binding.registryRoots[1]!)).rejects.toMatchObject({ reason: "occupied" });
    expect(f.mesh.get(admissionKey)).toEqual(entry);
  });

  it("preserves only validated protected routes on binding failure and never parks ordinary topics for unrelated policy failure", async () => {
    const f = await fresh(), registry = f.binding.registryRoots[1]!;
    installPolicy([{ ...f.binding, physicalHostIdentity: "foreign-host" }]);
    const malformed = slotAdmissionFor(meshAt(f.mesh.root));
    expect(malformed.status().enforcement).toBe("blocked");
    expect(malformed.isProtected([routes[0]!.topic])).toBe(true);
    expect(malformed.isProtected(["ordinary.topic"])).toBe(false);
    expect(malformed.requiresEnforcement([routes[0]!.topic], registry, caller.id)).toBe(true);
    expect(malformed.requiresEnforcement(["ordinary.topic"], registry, caller.id)).toBe(false);
    expect(malformed.isCurrent("ordinary", ["ordinary.topic"], registry)).toBe(true);
    const ordinary = { actorId: "ordinary", request: { name: "ordinary", instructions: "Ordinary", topics: ["ordinary.topic"] }, spec: {}, registryRoot: registry, caller };
    await expect(malformed.admit(ordinary)).resolves.toBeUndefined();
    await expect(malformed.admit(f.input())).rejects.toMatchObject({ reason: "authority-unavailable" });

    vi.mocked(readHostPolicy).mockReturnValue({ status: "invalid" });
    const unrelated = slotAdmissionFor(meshAt(path.join(temp(), "unconfigured")));
    expect(unrelated.status().enforcement).toBe("blocked"); // no enforcement grant from unreadable policy
    expect(unrelated.isProtected(["ordinary.topic", routes[0]!.topic])).toBe(false);
    expect(unrelated.requiresRecovery(path.join(temp(), "ordinary-registry"))).toBe(false);
    await expect(unrelated.admit(ordinary)).resolves.toBeUndefined();
    // The ALREADY loaded enforced runtime keeps its known routes and cannot downgrade.
    expect(f.coordinator.isProtected([routes[0]!.topic])).toBe(true);
    expect(f.coordinator.isProtected(["ordinary.topic"])).toBe(false);
    expect(f.coordinator.requiresRecovery(registry, caller.id)).toBe(true);
    await expect(f.coordinator.admit(f.input())).rejects.toMatchObject({ reason: "authority-unavailable" });
    vi.mocked(readHostPolicy).mockReturnValue({ status: "missing" });
    expect(f.coordinator.requiresEnforcement([routes[0]!.topic], registry, caller.id)).toBe(true);
    await expect(f.coordinator.admit(f.input())).rejects.toMatchObject({ reason: "authority-unavailable" });
    await expect(f.coordinator.admit(ordinary)).resolves.toBeUndefined();
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("refuses oversized complete specs before transaction/acceptance without effects", async () => {
    const f = await fresh(), callback = vi.fn(), input = f.input();
    input.request.instructions = "x".repeat(f.mesh.maxEventBytes + 1); input.spec.instructions = input.request.instructions;
    await expect(f.coordinator.admit({ ...input, beforeCommit: callback })).rejects.toMatchObject({ reason: "invalid" });
    expect(callback).not.toHaveBeenCalled();
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("does not let copied mesh policy/provenance authorize a binding or a native policy downgrade", async () => {
    const f = await fresh();
    await f.coordinator.admit(f.input());
    await f.mesh.put({ key: "actors/slot-policy/v1", identity: caller, value: { phase: "enforce", authorityGeneration: "copied" } });
    installPolicy([]);
    expect(f.coordinator.status().enforcement).toBe("blocked");
    expect(f.coordinator.isCurrent("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(false);
  });

  it("rejects actor-spoofed claim provenance and preserves no-effect refusal at the final callback boundary", async () => {
    const f = await fresh(), callback = vi.fn();
    const forged = await f.mesh.put({ key, value: f.claim.value, identity: { ...caller, kind: "actor" } });
    await expect(f.coordinator.admit({ ...f.input("actor:new", 0, forged.version), beforeCommit: callback }))
      .rejects.toMatchObject({ reason: "foreign-root" });
    expect(callback).not.toHaveBeenCalled();
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("refuses a live protected spec hidden behind an unprotected request", async () => {
    const f = await fresh(), input = f.input();
    input.request.topics = ["github.demo.observer"];
    await expect(f.coordinator.admit(input)).rejects.toMatchObject({ reason: "invalid" });
    expect(f.mesh.get(admissionKey)).toBeUndefined();
  });

  it("allows only a deliberate new generation after sealing, and old cleanup cannot touch it", async () => {
    const f = await fresh();
    await f.coordinator.admit(f.input());
    await f.coordinator.beginRemoval("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!);
    await f.coordinator.retire("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!);
    const nextKey = key.replace(/base$/, "r2"), nextNames = ["demo-shards2-r2-review-astra", "demo-shards2-r2-security-astra"];
    const nextClaim = await f.mesh.put({ key: nextKey, identity: caller, value: { ...(f.claim.value as object), generation: "r2", names: nextNames } });
    const next = f.input("actor:next", 0, nextClaim.version);
    next.request.name = nextNames[0]!; next.request.slotClaim.key = nextKey; next.request.idempotencyKey = "next-generation";
    next.spec.name = next.request.name; next.spec.slotClaim = next.request.slotClaim;
    expect(await f.coordinator.admit(next)).toMatchObject({ actorId: "actor:next" });
    const nextEntry = f.mesh.get(SLOT_ADMISSION_PREFIX + "acme/demo/1/r2");
    await f.coordinator.retire("actor:review-1", [routes[0]!.topic], f.binding.registryRoots[1]!);
    expect(f.mesh.get(SLOT_ADMISSION_PREFIX + "acme/demo/1/r2")).toEqual(nextEntry);
    expect(f.coordinator.isCurrent("actor:next", [routes[0]!.topic], f.binding.registryRoots[1]!)).toBe(true);
  });

  it("refuses a role whose combined pair record exceeds the value limit without partial mutation", async () => {
    const f = await fresh();
    const first = f.input(); first.request.instructions = "x".repeat(40_000); first.spec.instructions = first.request.instructions;
    await f.coordinator.admit(first);
    const prior = f.mesh.get(admissionKey), callback = vi.fn();
    const second = f.input("actor:security", 1); second.request.instructions = "y".repeat(40_000); second.spec.instructions = second.request.instructions;
    await expect(f.coordinator.admit({ ...second, beforeCommit: callback })).rejects.toMatchObject({ reason: "invalid" });
    expect(callback).not.toHaveBeenCalled();
    expect(f.mesh.get(admissionKey)).toEqual(prior);
  });

  it("retains ordinary native driver create semantics when the file did not yet exist", () => {
    const file = path.join(temp(), "new.db"), connection = openNodeSqlite(file);
    try { expect(connection.attestDatabaseFile?.().canonicalPath).toBe(file); } finally { connection.close(); }
  });

  it("returns the actual native driver's evidence and refuses an adapter with no evidence", async () => {
    const root = temp();
    const store = await SqliteStateStore.open(root, 4096, 100, { initialize: "create", open: file => {
      const db = openNodeSqlite(file);
      return { exec: sql => db.exec(sql), prepare: sql => db.prepare(sql), close: () => db.close(), get isTransaction() { return db.isTransaction; } };
    } });
    try { expect(() => store.attestDatabaseFile()).toThrow("cannot attest"); } finally { store.close(); }
  });
});
