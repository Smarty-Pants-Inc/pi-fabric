import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { AgentManager } from "../src/agents/manager.js";
import type { FabricActorRequest, FabricActorStorageScope } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep: data safety / #7991 R1-2. Exercise real create, directory routing,
// registry projection and recovery. Only core admission and execution are doubles.
// No worker, product deletion, retention cleanup, process fixture or signal.
const doubles = vi.hoisted(() => ({ coordinator: undefined as unknown }));
vi.mock("../src/actors/slot-admission.js", () => ({
  slotAdmissionFor: () => doubles.coordinator,
  ActorSlotCreateUncertainError: class extends Error {
    code = "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN";
    constructor(readonly details: Record<string, unknown>, cause: unknown) { super("uncertain", { cause }); }
  },
}));
vi.mock("../src/actors/mesh-monitor.js", () => ({
  ActorMeshMonitor: class { start() {} schedule() {} close() {} caughtUp() { return true; } },
}));

const topic = "fleet.review.repo.pull";
const identity = { id: "session:alpha", name: "main", kind: "main" as const, sessionId: "alpha" };
const scopes = ["project", "session"] as const;
const other = (scope: FabricActorStorageScope): FabricActorStorageScope => scope === "project" ? "session" : "project";
const request = (scope: FabricActorStorageScope): FabricActorRequest => ({
  scope, name: "repo-review-astra", instructions: "Review this change.", topics: [topic],
  idempotencyKey: "review-v1", slotClaim: { key: "factory/review-slots/v1/org/repo/1/base", ifVersion: 7 },
});
type Intent = { actorId: string; registryRoot: string; spec: Record<string, unknown>; request: string; state: "admitted" };
let intents: Intent[];
const roots: string[] = [], directories: ActorDirectory[] = [];
const run = vi.fn(() => { throw new Error("Workers are forbidden in this suite"); });

beforeEach(() => {
  intents = [];
  run.mockClear();
  const protectedTopics = (topics: readonly string[]) => topics.includes(topic);
  const current = (id: string, topics: readonly string[], root: string) => !protectedTopics(topics) ||
    intents.some(intent => intent.actorId === id && intent.registryRoot === root);
  doubles.coordinator = {
    isProtected: protectedTopics,
    requiresEnforcement: protectedTopics,
    requiresRecovery: () => true,
    isCurrent: current,
    async admit(input: { actorId: string; registryRoot: string; request: FabricActorRequest; spec: Record<string, unknown>; beforeCommit?: (id: string) => void }) {
      if (!protectedTopics(input.request.topics ?? [])) return undefined;
      const prior = intents.find(intent => intent.request === JSON.stringify(input.request) && intent.registryRoot === input.registryRoot);
      if (!prior && intents.length) throw new Error("occupied");
      const selected: Intent = prior ?? { actorId: input.actorId, registryRoot: input.registryRoot,
        spec: structuredClone(input.spec), request: JSON.stringify(input.request), state: "admitted" };
      input.beforeCommit?.(selected.actorId);
      if (!prior) intents.push(selected);
      return { actorId: selected.actorId, spec: structuredClone(selected.spec), reused: !!prior };
    },
    async recoverable(root: string) { return structuredClone(intents.filter(intent => intent.registryRoot === root)); },
    withCurrent<T>(id: string, topics: readonly string[], root: string, operation: () => T): T {
      if (protectedTopics(topics)) expect(fs.existsSync(path.join(root, "actors.json.lock", "owner"))).toBe(true);
      if (!current(id, topics, root)) throw new Error("not current");
      return operation();
    },
    async beginRemoval() { throw new Error("Product removal is hosted-only"); },
    async retire() { throw new Error("Product removal is hosted-only"); },
  };
});
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  expect(run).not.toHaveBeenCalled();
  for (const root of roots.splice(0)) {
    expect(root.startsWith(path.resolve(process.env.TMPDIR!) + path.sep)).toBe(true);
    fs.rmSync(root, { recursive: true, force: true }); // Assigned TMPDIR fixtures only.
  }
});
function open(root = fs.mkdtempSync(path.join(os.tmpdir(), "slot-names-"))) {
  if (!roots.includes(root)) roots.push(root);
  const actorRoots = { project: path.join(root, "actors"), session: path.join(root, "actors", "alpha") };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = { config: DEFAULT_FABRIC_CONFIG.agents, resolveKernel: () => undefined, defaultModel: () => undefined,
    assertModelAllowed: () => {}, run, actorArchiveSources: () => new Map(), close: async () => {} } as unknown as AgentManager;
  const directory = new ActorDirectory(["alpha", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    persistent: true, rootId: identity.id, claimResidency: "session", releasePaused: true,
    canConsumeMesh: () => false, meshRetentionSweepPath: false, reapDeadSessionPresence: false,
  }], actorRoots, "project");
  directories.push(directory);
  return { root, directory, actorRoots };
}
const rows = (root: string) => new ActorRegistryStore(root).snapshot().actors;
const namedRows = (actorRoots: { project: string; session: string }) => scopes.flatMap(scope => rows(actorRoots[scope])).filter(row => row.name === request("project").name);

// Recorded native-cleanup completion: rewrite only the fixture registry, retaining
// the exact predecessor row and every session/queue/payload file. Never call an
// ActorManager removal API or emulate executable child settlement.
function cleanupDouble(registryRoot: string) {
  const receipts: Array<{ id: string; row: Record<string, unknown> }> = [];
  const complete = vi.fn(async (id: string) => {
    const registry = new ActorRegistryStore(registryRoot);
    await registry.withLock(() => {
      const before = registry.snapshot().actors;
      const row = before.find(row => row.id === id);
      if (!row) throw new Error("No predecessor to record");
      receipts.push({ id, row: structuredClone(row) });
      registry.write(before.filter(row => row.id !== id), { durable: true });
    }, undefined, "test-dry-cleanup");
    return { removed: true as const };
  });
  return { complete, receipts };
}

function queueBytes(registryRoot: string, id: string) {
  return fs.readdirSync(path.join(registryRoot, id)).filter(file => file.startsWith("queue-")).map(file => ({
    file, bytes: fs.readFileSync(path.join(registryRoot, id, file), "utf8"),
  }));
}

describe("ordinary name conflicts remain cross-platform", () => {
  it.each(scopes)("still creates an ordinary %s actor and rejects an active duplicate", async scope => {
    const { directory, actorRoots } = open();
    const ordinary = { scope, name: "ordinary", instructions: "No protected subscription" };
    const actor = await directory.create(ordinary);
    await expect(directory.create(ordinary)).rejects.toThrow(/already active/);
    expect(rows(actorRoots[scope]).map(row => row.id)).toEqual([actor.id]);
    expect(directory.status(actor.id).status).toBe("idle");
    expect(intents).toEqual([]);
  });
});

// Native regression evidence is Linux-scoped, matching slot-admission-native-flow:
// fresh native authority uses /etc/machine-id and /proc. No platform widening.
// The core double deliberately does NOT claim to test that authority binding.
describe.runIf(process.platform === "linux")("protected slot native name conflicts (#7991 R1-2)", () => {
  it.each(scopes)("rejects active same-%s ordinary conflict before intent or receipt effects", async scope => {
    const { directory, actorRoots } = open();
    const predecessor = await directory.create({ name: request(scope).name, instructions: "Ordinary incumbent", scope });
    const accepted = directory.tell(predecessor.id, "Retain accepted ordinary work");
    await directory.setInstructions(predecessor.id, "Ordinary incumbent"); // Join the prior queued-state save.
    const before = new ActorRegistryStore(actorRoots[scope]).snapshot().bytes;
    const queues = queueBytes(actorRoots[scope], predecessor.id);
    const acceptance = vi.fn(), materialization = vi.fn(), receipt = vi.fn();
    const failure = await directory.create(request(scope), { beforeCommit: acceptance, beforeMaterialize: materialization, onCommit: receipt }).catch(error => error);
    expect(failure).toBeInstanceOf(Error);
    expect(intents).toEqual([]);
    expect(failure.message).toMatch(/already active/);
    expect(acceptance).not.toHaveBeenCalled();
    expect(materialization).not.toHaveBeenCalled();
    expect(receipt).not.toHaveBeenCalled();
    expect(new ActorRegistryStore(actorRoots[scope]).snapshot().bytes).toBe(before);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([predecessor.id]);
    expect(queueBytes(actorRoots[scope], predecessor.id)).toEqual(queues);
    expect(directory.messages(predecessor.id)).toContainEqual(expect.objectContaining({ id: accepted.messageId }));
  });

  it.each(scopes)("parks exact %s retry after post-intent interruption until recorded cross-scope cleanup", async scope => {
    const { directory, actorRoots } = open();
    const predecessorScope = other(scope);
    const predecessor = await directory.create({ name: request(scope).name, instructions: "Ordinary predecessor", scope: predecessorScope });
    directory.tell(predecessor.id, "Accepted predecessor work");
    const queues = queueBytes(actorRoots[predecessorScope], predecessor.id);
    expect(queues).toHaveLength(1);
    const cleanup = cleanupDouble(actorRoots[predecessorScope]);
    const removal = vi.spyOn(directory, "remove").mockImplementation(cleanup.complete);
    await expect(directory.create(request(scope), { beforeMaterialize: () => { throw new Error("crash after durable intent, before predecessor hook"); } }))
      .rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN" });
    const id = intents[0]!.actorId;
    await expect(directory.create(request(scope))).rejects.toBeInstanceOf(Error);
    expect(removal).not.toHaveBeenCalled(); // Exact retry does not invent cleanup authority.
    expect(rows(actorRoots[scope]).some(row => row.id === id)).toBe(false);
    expect(fs.existsSync(path.join(actorRoots[scope], id))).toBe(false);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([predecessor.id]);
    expect(queueBytes(actorRoots[predecessorScope], predecessor.id)).toEqual(queues);
    await cleanup.complete(predecessor.id); // External, recorded native completion, no file deletion.
    const retry = await directory.create(request(scope));
    expect(retry.id).toBe(id);
    expect(intents).toHaveLength(1);
    expect(cleanup.receipts.map(receipt => receipt.id)).toEqual([predecessor.id]);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([id]);
    expect(queueBytes(actorRoots[predecessorScope], predecessor.id)).toEqual(queues);
  });

  it.each(scopes)("parks restarted %s recovery after the same interruption, then projects the original ID", async scope => {
    const first = open();
    const predecessorScope = other(scope);
    const predecessor = await first.directory.create({ name: request(scope).name, instructions: "Ordinary predecessor", scope: predecessorScope });
    first.directory.tell(predecessor.id, "Retain predecessor queue through recovery");
    await expect(first.directory.create(request(scope), { beforeMaterialize: () => { throw new Error("interrupted before cleanup"); } }))
      .rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN" });
    const id = intents[0]!.actorId;
    await first.directory.close();
    const queues = queueBytes(first.actorRoots[predecessorScope], predecessor.id);
    const { directory, actorRoots } = open(first.root);
    const cleanup = cleanupDouble(actorRoots[predecessorScope]);
    const removal = vi.spyOn(directory, "remove").mockImplementation(cleanup.complete);
    await expect(directory.recoverProtectedActors()).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    expect(directory.slotRecoveryStatus().ready).toBe(false);
    await expect(directory.create(request(scope))).rejects.toBeInstanceOf(Error);
    expect(removal).not.toHaveBeenCalled();
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([predecessor.id]);
    expect(fs.existsSync(path.join(actorRoots[scope], id))).toBe(false);
    expect(queueBytes(actorRoots[predecessorScope], predecessor.id)).toEqual(queues);
    await cleanup.complete(predecessor.id);
    await directory.recoverProtectedActors();
    expect(directory.status(id).id).toBe(id);
    expect((await directory.create(request(scope))).id).toBe(id);
    expect(intents).toHaveLength(1);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([id]);
    expect(queueBytes(actorRoots[predecessorScope], predecessor.id)).toEqual(queues);
  });

  it.each(scopes)("checks quarantined cross-scope registry rows, not only visible maps, for %s recovery", async scope => {
    const first = open();
    const predecessorScope = other(scope);
    const predecessor = await first.directory.create({ name: request(scope).name, instructions: "Predecessor retained on disk", scope: predecessorScope });
    await expect(first.directory.create(request(scope), { beforeMaterialize: () => { throw new Error("interrupted"); } })).rejects.toBeInstanceOf(Error);
    const id = intents[0]!.actorId;
    await first.directory.close();
    const registry = new ActorRegistryStore(first.actorRoots[predecessorScope]);
    await registry.withLock(() => registry.write(registry.snapshot().actors.map(row => ({ ...row, topics: [topic], rootId: "session:foreign" })), { durable: true }));
    const { directory, actorRoots } = open(first.root);
    expect(directory.listOwned()).toEqual([]);
    expect(directory.list().some(actor => actor.id === predecessor.id)).toBe(false);
    const before = registry.snapshot().bytes;
    await expect(directory.recoverProtectedActors()).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    expect(registry.snapshot().bytes).toBe(before);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([predecessor.id]);
    expect(fs.existsSync(path.join(actorRoots[scope], id))).toBe(false);
    await cleanupDouble(actorRoots[predecessorScope]).complete(predecessor.id);
    await directory.recoverProtectedActors();
    expect(directory.status(id).id).toBe(id);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([id]);
  });

  it.each(scopes)("retains successful %s protected birth with safe cross-scope cleanup and exact retry", async scope => {
    const { directory, actorRoots } = open();
    const predecessorScope = other(scope);
    const predecessor = await directory.create({ name: request(scope).name, instructions: "Stopped predecessor", scope: predecessorScope });
    await directory.stop(predecessor.id);
    const cleanup = cleanupDouble(actorRoots[predecessorScope]);
    const removal = vi.spyOn(directory, "remove").mockImplementation(async id => {
      expect(intents).toHaveLength(1); // No removal before durable intent.
      return cleanup.complete(id);
    });
    const receipts: string[] = [];
    const actor = await directory.create(request(scope), { onCommit: id => { receipts.push(id); } });
    expect(removal).toHaveBeenCalledWith(predecessor.id, { wait: false });
    expect(cleanup.receipts.map(receipt => receipt.id)).toEqual([predecessor.id]);
    expect(namedRows(actorRoots).map(row => row.id)).toEqual([actor.id]);
    const accepted = directory.tell(actor.id, "Accepted protected work");
    const queues = queueBytes(actorRoots[scope], actor.id);
    const history = directory.messages(actor.id);
    const retry = await directory.create(request(scope), { onCommit: id => { receipts.push(id); }, beforeMaterialize: () => { throw new Error("Exact retry must not replace predecessor"); } });
    expect(retry.id).toBe(actor.id);
    expect(receipts).toEqual([actor.id, actor.id]);
    expect(removal).toHaveBeenCalledTimes(1);
    expect(intents).toHaveLength(1);
    expect(queueBytes(actorRoots[scope], actor.id)).toEqual(queues);
    expect(directory.messages(actor.id)).toEqual(history);
    expect(history).toContainEqual(expect.objectContaining({ id: accepted.messageId }));
    expect(directory.status(actor.id).queued).toBe(1);
  });
});
