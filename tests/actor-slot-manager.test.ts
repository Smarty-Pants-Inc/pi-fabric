import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorSlotClaimRefusedError } from "../src/actors/slot-authority.js";
import type { AgentManager, AgentModelAdmission } from "../src/agents/manager.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

// Keep: data safety / native trust boundary, #7991. Real actor registry, queue
// and MeshStore projections; coordinator is a deterministic contract double.
// This is NOT proof of core SQL atomicity/fsync/authority attestation. No worker,
// product removal, retention CLI, process fixture or OS signal is run here.
const doubles = vi.hoisted(() => ({
  coordinator: undefined as unknown,
  monitors: [] as Array<{ onEvent: (event: MeshEvent) => boolean | string | void }>,
}));
vi.mock("../src/actors/slot-admission.js", () => ({
  slotAdmissionFor: () => doubles.coordinator,
  ActorSlotCreateUncertainError: class extends Error {
    code = "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN";
    constructor(readonly details: Record<string, unknown>, cause: unknown) { super("uncertain", { cause }); }
  },
}));
vi.mock("../src/actors/mesh-monitor.js", () => ({
  ActorMeshMonitor: class {
    constructor(_mesh: unknown, _config: unknown, options: { onEvent: (event: MeshEvent) => boolean | string | void }) { doubles.monitors.push(options); }
    start() {} schedule() {} close() {} caughtUp() { return true; }
  },
}));

type Intent = { actorId: string; spec: Record<string, unknown>; state: "admitted" | "revoking" | "retired"; registryRoot: string; key: string; request: string };
const protectedTopic = "fleet.review.repo.pull";
const identity = { id: "session:alpha", name: "main", kind: "main" as const, sessionId: "alpha" };
const request = (scope: "project" | "session" = "session"): FabricActorRequest => ({
  name: "repo-review-astra", instructions: "Review this change.", topics: [protectedTopic], scope,
  idempotencyKey: "slot-review-v1", slotClaim: { key: "factory/review-slots/v1/org/repo/1/base", ifVersion: 7 },
});
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; };
const roots: string[] = [], directories: ActorDirectory[] = [];
let intents: Intent[], enforced: boolean, readable: boolean, refusal: Error | undefined, beforeDurable: (() => Promise<void>) | undefined;
let recover: ((registryRoot: string) => Promise<void>) | undefined;
let runner: ReturnType<typeof vi.fn>;
let resolveModel: (model: string) => string | Promise<string>;

function coreDouble() {
  const isProtected = (topics: readonly string[]) => topics.includes(protectedTopic);
  const current = (id: string, topics: readonly string[], registryRoot: string) => !enforced || !isProtected(topics) ||
    (readable && intents.some(intent => intent.actorId === id && intent.registryRoot === registryRoot && intent.state === "admitted"));
  return {
    isProtected,
    requiresEnforcement: (topics: readonly string[], _registryRoot: string, _callerId?: string) => enforced && isProtected(topics),
    requiresRecovery: () => enforced,
    status: () => ({ protocol: "slot-admission-v1", enforcement: enforced ? "enforced" : "unenforced" }),
    async admit(input: { actorId: string; request: FabricActorRequest; spec: Record<string, unknown>; registryRoot: string; caller: unknown; beforeCommit?: (id: string) => void }) {
      if (!enforced || !isProtected(input.request.topics ?? [])) return undefined;
      if (refusal) throw refusal;
      const prior = intents.find(intent => intent.key === input.request.idempotencyKey);
      const immutable = JSON.stringify(input.request);
      if (prior && (prior.request !== immutable || prior.state !== "admitted" || prior.registryRoot !== input.registryRoot)) throw new Error("occupied");
      const selected = prior ?? { actorId: input.actorId, spec: structuredClone(input.spec), state: "admitted" as const,
        registryRoot: input.registryRoot, key: input.request.idempotencyKey!, request: immutable };
      input.beforeCommit?.(selected.actorId);
      if (!prior) intents.push(selected);
      await beforeDurable?.();
      return { actorId: selected.actorId, spec: structuredClone(selected.spec), reused: !!prior };
    },
    isCurrent: current,
    withCurrent<T>(id: string, topics: readonly string[], registryRoot: string, operation: () => T): T {
      if (!current(id, topics, registryRoot)) throw new Error("not current");
      if (enforced && isProtected(topics)) {
        // All materialization/publication gates in these no-run tests owe registry
        // custody BEFORE core acquires mesh/state custody. Detect recursive helpers.
        expect(fs.existsSync(path.join(registryRoot, "actors.json.lock", "owner"))).toBe(true);
      }
      return operation();
    },
    async recoverable(registryRoot: string) { await recover?.(registryRoot); return intents.filter(intent => intent.registryRoot === registryRoot).map(intent => structuredClone(intent)); },
    async beginRemoval() { throw new Error("hosted-only in manager tests"); },
    async retire() { throw new Error("hosted-only in manager tests"); },
  };
}

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

function open(root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-slot-manager-"))) {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const actorRoots = { project: path.join(root, "actors"), session: path.join(root, "actors", "alpha") };
  runner = vi.fn(() => { throw new Error("A worker must not launch in this safe suite"); });
  const agents = { config: DEFAULT_FABRIC_CONFIG.agents, resolveKernel: () => undefined, defaultModel: () => undefined,
    assertModelAllowed, resolveModelReasonPolicy, run: runner, actorArchiveSources: () => new Map(), close: async () => {} } as unknown as AgentManager;
  const directory = new ActorDirectory(["alpha", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
    persistent: true, rootId: identity.id, claimResidency: "session", releasePaused: true,
    resolvePiModel: model => resolveModel(model),
    canConsumeMesh: () => false, meshRetentionSweepPath: false, reapDeadSessionPresence: false,
  }], actorRoots, "project");
  directories.push(directory);
  return { directory, mesh, actorRoots, root };
}
function seed(registryRoot: string, id: string, scope: "project" | "session", state: Intent["state"] = "admitted"): Intent {
  const req = request(scope);
  return { actorId: id, state, registryRoot, key: `${scope}-seed`, request: JSON.stringify(req), spec: {
    id, name: `${scope}-review-astra`, rootId: identity.id, instructions: req.instructions, topics: req.topics,
    status: "idle", events: ["turn_end"], delivery: "mailbox", responseMode: "text", triggerTurn: false,
    coalesce: false, residency: "session", runner: "pi", requirements: [], messages: [], createdAt: 1, updatedAt: 1,
    sessionFile: path.join(registryRoot, id, "session.jsonl"),
  } };
}
function event(topic = protectedTopic, to?: string): MeshEvent {
  return { id: "input-1", sequence: 1, topic, kind: "work", from: { id: "sender", name: "sender", kind: "main" },
    createdAt: Date.now(), ...(to ? { to } : {}) } as MeshEvent;
}

beforeEach(() => {
  intents = []; enforced = true; readable = true; refusal = undefined; beforeDurable = undefined; recover = undefined;
  resolveModel = model => model;
  doubles.monitors.length = 0;
  doubles.coordinator = coreDouble();
});
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const root of roots.splice(0)) {
    // Every deletion in this suite is fixture cleanup strictly under its assigned
    // TMPDIR (Vitest's isolation creates a child of that root), never a product API.
    expect(root.startsWith(path.resolve(process.env.TMPDIR!) + path.sep)).toBe(true);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("manager protected birth/recovery seam", () => {
  it("keeps the slot model-policy fixture synchronous and rejects unexpected/protected selectors before effects", async () => {
    const { directory } = open();
    for (const model of [undefined, "test-model"]) {
      expect(resolveModelReasonPolicy(model, undefined, "pi", { thinking: "high" }))
        .toEqual({ model, thinking: "high", fallback: false });
    }
    for (const model of ["unexpected-model", "anthropic/claude-opus-4-1"]) {
      expect(() => resolveModelReasonPolicy(model, undefined, "pi")).toThrow("Unexpected slot fixture");
      expect(() => assertModelAllowed(model, "pi")).toThrow("Unexpected slot fixture");
      await expect(directory.create({ name: "rejected-model", instructions: "No effects.", model }))
        .rejects.toThrow("Unexpected slot fixture");
    }
    expect(() => resolveModelReasonPolicy(undefined, undefined, "claude")).toThrow("Unexpected slot fixture");
    expect(directory.listOwned()).toEqual([]);
    expect(intents).toEqual([]);
    expect(runner).not.toHaveBeenCalled();
  });
  it("does not retain unrelated unknown-address input on an unenforced authority before recovery", () => {
    enforced = false;
    open();
    for (const monitor of doubles.monitors) expect(monitor.onEvent(event("ordinary.work", "unrelated-receiver"))).not.toBe(false);
  });
  it.each(["project", "session"] as const)("has no %s actor effects before admission sync, then materializes under custody", async scope => {
    const { directory, actorRoots } = open();
    const durability = deferred(), reached = deferred();
    beforeDurable = () => { reached.resolve(); return durability.promise; };
    const materialize = vi.fn();
    const creating = directory.create(request(scope), { beforeMaterialize: materialize });
    await reached.promise;
    expect(intents).toHaveLength(1);
    const intent = intents[0]!;
    expect(fs.existsSync(path.join(actorRoots[scope], intent.actorId))).toBe(false);
    expect(fs.existsSync(path.join(actorRoots[scope], "actors.json"))).toBe(false);
    expect(directory.listOwned()).toEqual([]);
    expect(materialize).not.toHaveBeenCalled();
    durability.resolve();
    const actor = await creating;
    expect(actor.id).toBe(intent.actorId);
    expect(new ActorRegistryStore(actorRoots[scope]).snapshot().actors.map(row => row.id)).toEqual([actor.id]);
    expect(materialize).toHaveBeenCalledWith(actor.id);
    expect(runner).not.toHaveBeenCalled();
  });

  it.each(["project", "session"] as const)("returns the exact active %s ID on retry and preserves accepted queue/history", async scope => {
    const { directory, actorRoots } = open();
    const receipts: string[] = [];
    const actor = await directory.create(request(scope), { beforeCommit: id => { receipts.push(id); } });
    const queued = directory.tell(actor.id, "accepted work");
    const history = directory.messages(actor.id);
    expect(history).toContainEqual(expect.objectContaining({ id: queued.messageId, data: { message: "accepted work" } }));
    const queueFiles = fs.readdirSync(path.join(actorRoots[scope], actor.id)).filter(file => file.startsWith("queue-"));
    expect(queueFiles).toHaveLength(1);
    const before = fs.readFileSync(path.join(actorRoots[scope], actor.id, queueFiles[0]!), "utf8");
    expect(before).toContain(queued.messageId);
    const retry = await directory.create(request(scope), { beforeCommit: id => { receipts.push(id); },
      beforeMaterialize: () => { throw new Error("retry must not replace predecessor"); } });
    expect(retry.id).toBe(actor.id);
    expect(receipts).toEqual([actor.id, actor.id]);
    expect(directory.status(actor.id).queued).toBe(1);
    expect(fs.readFileSync(path.join(actorRoots[scope], actor.id, queueFiles[0]!), "utf8")).toBe(before);
    expect(directory.messages(actor.id)).toEqual(history);
    expect(intents).toHaveLength(1);
    expect(runner).not.toHaveBeenCalled();
  });

  it.each(["project", "session"] as const)("replays the same %s ID after restart without replacing accepted queue/history", async scope => {
    const { directory, actorRoots, root } = open();
    const actor = await directory.create(request(scope));
    const queued = directory.tell(actor.id, "accepted before restart");
    await directory.close();
    const saved = new ActorRegistryStore(actorRoots[scope]).snapshot().actors.find(row => row.id === actor.id)!;
    expect(saved).toBeDefined();
    const beforeHistory = new ActorRegistryStore(actorRoots[scope]).messages(saved);
    const restarted = open(root).directory;
    expect(restarted.listOwned()).toEqual([]);
    expect(() => restarted.tell(actor.id, "not ready")).toThrow();
    await restarted.recoverProtectedActors();
    expect(restarted.status(actor.id).queued).toBe(1);
    const retry = await restarted.create(request(scope));
    expect(retry.id).toBe(actor.id);
    expect(restarted.messages(actor.id)).toEqual(beforeHistory);
    const files = fs.readdirSync(path.join(actorRoots[scope], actor.id)).filter(file => file.startsWith("queue-"));
    expect(files).toHaveLength(1);
    expect(fs.readFileSync(path.join(actorRoots[scope], actor.id, files[0]!), "utf8")).toContain(queued.messageId);
    expect(runner).not.toHaveBeenCalled();
  });

  it("parks accepted input when authority fails during awaited launch preparation", async () => {
    const { directory, actorRoots } = open();
    const actor = await directory.create({ ...request(), model: "test-model" });
    const binding = deferred(), reached = deferred();
    resolveModel = async model => { reached.resolve(); await binding.promise; return model; };
    const queued = directory.tell(actor.id, "retain across authority loss");
    directory.resumeAfterRelease();
    await reached.promise;
    readable = false;
    binding.resolve();
    await vi.waitFor(() => expect(directory.inFlightCount()).toBe(0));
    expect(runner).not.toHaveBeenCalled();
    const file = fs.readdirSync(path.join(actorRoots.session, actor.id)).find(file => file.startsWith("queue-"))!;
    expect(fs.readFileSync(path.join(actorRoots.session, actor.id, file), "utf8")).toContain(queued.messageId);
    expect(directory.messages(actor.id).every(message => !message.error?.includes("Dropped"))).toBe(true);
    directory.pauseForRelease();
    readable = true;
    await directory.recoverProtectedActors();
    await Promise.resolve();
    expect(directory.status(actor.id).queued).toBe(1);
    expect(runner).not.toHaveBeenCalled();
  });

  it("does not remove a stopped cross-scope predecessor on a claim refusal", async () => {
    const { directory, actorRoots } = open();
    const predecessor = await directory.create({ name: request().name, instructions: "old", scope: "project" });
    await directory.stop(predecessor.id);
    const before = fs.readFileSync(path.join(actorRoots.project, "actors.json"), "utf8");
    refusal = Object.assign(new Error("missing exact claim"), { code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "required" });
    const removal = vi.spyOn(directory, "remove").mockResolvedValue({ removed: true });
    await expect(directory.create(request())).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED" });
    expect(removal).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(actorRoots.project, "actors.json"), "utf8")).toBe(before);
    expect(fs.existsSync(path.join(actorRoots.session, "actors.json"))).toBe(false);
    expect(intents).toEqual([]);
  });

  it("keeps both scopes unpublished until registryless recovery of both has synced", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-slot-ledger-only-"));
    const actorRoots = { project: path.join(root, "actors"), session: path.join(root, "actors", "alpha") };
    intents.push(seed(actorRoots.project, "a".repeat(32), "project"), seed(actorRoots.session, "b".repeat(32), "session"));
    const gate = deferred(), reached = deferred();
    recover = async registryRoot => { if (registryRoot === actorRoots.session) { reached.resolve(); await gate.promise; } };
    const { directory } = open(root);
    expect(directory.listOwned()).toEqual([]);
    expect(doubles.monitors.every(monitor => monitor.onEvent(event()) === "deferred")).toBe(true);
    expect(doubles.monitors.every(monitor => monitor.onEvent(event("fabric.steer", "b".repeat(32))) === "deferred")).toBe(true);
    const pending = directory.recoverProtectedActors();
    await reached.promise;
    expect(directory.listOwned()).toEqual([]);
    expect(fs.existsSync(path.join(actorRoots.session, "actors.json"))).toBe(false);
    // An ordinary actor need not wait for an unrelated protected recovery barrier.
    const ordinary = await directory.create({ name: "ordinary", instructions: "unrelated" });
    expect(directory.listOwned().map(actor => actor.id)).toEqual([ordinary.id]);
    gate.resolve();
    await pending;
    expect(directory.listOwned().map(actor => actor.id).sort()).toEqual([ordinary.id, "a".repeat(32), "b".repeat(32)].sort());
    expect(runner).not.toHaveBeenCalled();
  });

  it("quarantines stale retired registry rows without holding addressed input after recovery", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-slot-retired-"));
    const registryRoot = path.join(root, "actors");
    const intent = seed(registryRoot, "c".repeat(32), "project", "retired");
    intents.push(intent);
    fs.mkdirSync(registryRoot, { recursive: true });
    fs.writeFileSync(path.join(registryRoot, "actors.json"), JSON.stringify({ format: 1, actors: [intent.spec] }));
    const { directory } = open(root);
    expect(directory.listOwned()).toEqual([]);
    expect(doubles.monitors[0]!.onEvent(event("fabric.steer", intent.actorId))).toBe("deferred");
    await directory.recoverProtectedActors();
    directory.resumeQueued();
    // Do NOT call resumeAfterRelease in this no-worker suite: test ingress
    // directly without enabling unrelated native maintenance. Recovery keeps
    // the terminal row quarantined, but it is no longer an active input hold.
    expect(directory.listOwned()).toEqual([]);
    expect(doubles.monitors[0]!.onEvent(event("fabric.steer", intent.actorId))).not.toBe("deferred");
    expect(() => directory.tell(intent.actorId, "must not accept")).toThrow();
    expect(runner).not.toHaveBeenCalled();
  });

  it("rechecks current admission after an awaited materialization hook and reports occupied uncertainty", async () => {
    const { directory, actorRoots } = open();
    await expect(directory.create(request(), { beforeMaterialize: async () => { intents[0]!.state = "revoking"; } }))
      .rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_CREATE_UNCERTAIN", details: { actorId: expect.any(String) } });
    expect(fs.existsSync(path.join(actorRoots.session, intents[0]!.actorId))).toBe(false);
    expect(directory.listOwned()).toEqual([]);
    expect(intents[0]!.state).toBe("revoking");
  });

  it("leaves a foreign root's ledger-only intent occupied for its existing native owner", async () => {
    const { directory, actorRoots } = open();
    const foreign = seed(actorRoots.project, "e".repeat(32), "project");
    foreign.spec.rootId = "session:other-native-owner";
    intents.push(foreign);
    await directory.recoverProtectedActors();
    expect(directory.listOwned()).toEqual([]);
    expect(fs.existsSync(path.join(actorRoots.project, foreign.actorId))).toBe(false);
    expect(intents[0]!.state).toBe("admitted");
    expect(doubles.monitors[0]!.onEvent(event())).toBe("ignored");
    expect(runner).not.toHaveBeenCalled();
  });

  it("keeps both scopes protected-parked on one failed recovery while ordinary actors remain usable", async () => {
    const { directory, actorRoots } = open();
    intents.push(seed(actorRoots.project, "a".repeat(32), "project"));
    recover = async registryRoot => { if (registryRoot === actorRoots.session) throw new Error("authority unavailable"); };
    // This is the exact narrow owner boundary: catch the typed protected stage,
    // expose its diagnostic, then open unrelated ordinary business readiness.
    const failure = await directory.recoverProtectedActors().catch(error => error);
    expect(failure.code).toBe("FABRIC_ACTOR_SLOT_RECOVERY_PARKED");
    expect(directory.slotRecoveryStatus()).toMatchObject({ ready: false, error: { code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED", message: expect.stringContaining("authority unavailable") } });
    expect(directory.listOwned()).toEqual([]);
    expect(doubles.monitors[0]!.onEvent(event())).toBe("deferred");
    const ordinary = await directory.create({ name: "ordinary during outage", instructions: "unrelated" });
    const accepted = directory.tell(ordinary.id, "still usable");
    expect(accepted.queued).toBe(true);
    expect(directory.listOwned().map(actor => actor.id)).toEqual([ordinary.id]);
    expect(runner).not.toHaveBeenCalled();
  });

  it("vetoes an optimistic registry mutation when the slot changes while acquisition awaits", async () => {
    const { directory, actorRoots } = open();
    const actor = await directory.create(request("project"));
    const registry = new ActorRegistryStore(actorRoots.project);
    const before = registry.snapshot().bytes;
    const prepared = deferred();
    const original = ActorRegistryStore.prototype.prepare;
    const prepare = vi.spyOn(ActorRegistryStore.prototype, "prepare").mockImplementation(function (this: ActorRegistryStore, ...args) {
      const value = original.apply(this, args);
      prepared.resolve();
      return value;
    });
    let mutation!: Promise<unknown>;
    let observation!: Promise<void>;
    try {
      await registry.withLock(async () => {
        mutation = directory.setInstructions(actor.id, "must not commit after revocation");
        // Attach the failure observer before releasing custody: no unhandled rejection.
        observation = expect(mutation).rejects.toThrow();
        await prepared.promise;
        intents[0]!.state = "revoking";
      }, undefined, "test-slot-race");
      await observation;
      expect(registry.snapshot().bytes).toBe(before);
      expect(directory.listOwned()).toEqual([]);
      expect(runner).not.toHaveBeenCalled();
    } finally { prepare.mockRestore(); }
  });

  it("a watcher/resume cannot re-enable a role retired after its initial projection", async () => {
    const { directory, actorRoots } = open();
    const actor = await directory.create(request("project"));
    const registry = new ActorRegistryStore(actorRoots.project);
    const stale = registry.snapshot().bytes!;
    intents[0]!.state = "retired";
    fs.writeFileSync(path.join(actorRoots.project, "actors.json"), stale);
    directory.resumeQueued();
    directory.resumeAfterRelease();
    expect(directory.listOwned()).toEqual([]);
    expect(() => directory.tell(actor.id, "must not enqueue")).toThrow("parked");
    expect(doubles.monitors[0]!.onEvent(event())).toBe("deferred");
    expect(runner).not.toHaveBeenCalled();
  });

  it("retains allowed-unenforced behavior without a slot token", async () => {
    enforced = false;
    const { directory } = open();
    const { slotClaim: _claim, idempotencyKey: _key, ...ordinary } = request();
    const actor = await directory.create(ordinary);
    expect(actor.topics).toEqual([protectedTopic]);
    expect(directory.listOwned().map(actor => actor.id)).toEqual([actor.id]);
    expect(intents).toEqual([]);
    expect((await directory.setTopics(actor.id, [])).topics).toEqual([]);
  });

  it.each(["project", "session"] as const)("refuses %s protected topic clear before mutation; restart recovers original slot/topics/queue", async scope => {
    const { directory, actorRoots, root } = open();
    const actor = await directory.create(request(scope));
    const queued = directory.tell(actor.id, "accepted before clear");
    const history = directory.messages(actor.id);
    const row = () => { const { status: _status, ...rest } = new ActorRegistryStore(actorRoots[scope]).snapshot().actors.find(row => row.id === actor.id)!; return rest; };
    // tell() persists queue/history asynchronously; settle it before the baseline.
    await vi.waitFor(() => expect(row()).toMatchObject({ messageHistory: { count: 1 } }));
    const registryBefore = row();
    expect(registryBefore).toMatchObject({ topics: [protectedTopic], rootId: identity.id });
    const updatedAt = directory.status(actor.id).updatedAt;
    for (const topics of [[], [protectedTopic], ["ordinary.work"], [protectedTopic, "ordinary.work"]]) {
      const failure = await directory.setTopics(actor.id, topics).catch(error => error);
      expect(failure).toBeInstanceOf(ActorSlotClaimRefusedError);
      expect(failure).toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "invalid", details: { issue: "smarty-dev#13463" } });
    }
    expect(directory.status(actor.id)).toMatchObject({ topics: [protectedTopic], queued: 1, updatedAt });
    expect(directory.messages(actor.id)).toEqual(history);
    expect(row()).toEqual(registryBefore);
    expect(intents).toHaveLength(1);
    expect(intents[0]).toMatchObject({ actorId: actor.id, state: "admitted", registryRoot: actorRoots[scope] });
    await directory.close();
    const restarted = open(root).directory;
    await restarted.recoverProtectedActors();
    expect(restarted.status(actor.id)).toMatchObject({ topics: [protectedTopic], queued: 1 });
    expect((await restarted.create(request(scope))).id).toBe(actor.id);
    const files = fs.readdirSync(path.join(actorRoots[scope], actor.id)).filter(file => file.startsWith("queue-"));
    expect(files).toHaveLength(1);
    expect(fs.readFileSync(path.join(actorRoots[scope], actor.id, files[0]!), "utf8")).toContain(queued.messageId);
    expect(runner).not.toHaveBeenCalled();
  });

  it("refuses ordinary-to-protected topic upgrade without mutation; ordinary clear still succeeds", async () => {
    const { directory, actorRoots } = open();
    const ordinary = await directory.create({ name: "ordinary-topics", instructions: "unrelated", topics: ["ordinary.work"], scope: "project" });
    const registryBefore = fs.readFileSync(path.join(actorRoots.project, "actors.json"), "utf8");
    const updatedAt = directory.status(ordinary.id).updatedAt;
    const failure = await directory.setTopics(ordinary.id, ["ordinary.work", protectedTopic]).catch(error => error);
    expect(failure).toBeInstanceOf(ActorSlotClaimRefusedError);
    expect(failure).toMatchObject({ code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "invalid", details: { issue: "smarty-dev#13463" } });
    expect(directory.status(ordinary.id)).toMatchObject({ topics: ["ordinary.work"], updatedAt });
    expect(fs.readFileSync(path.join(actorRoots.project, "actors.json"), "utf8")).toBe(registryBefore);
    expect(intents).toEqual([]);
    expect((await directory.setTopics(ordinary.id, [])).topics).toEqual([]);
    expect(runner).not.toHaveBeenCalled();
  });
});
