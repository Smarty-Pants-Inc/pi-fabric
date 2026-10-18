// Keep reason: durable accepted-work data safety and the authenticated wake trust boundary.
// Real ActorManager ingress/checkpoint/restart/drain, fake placement and worker boundary only.
// No CLI, provider, transient unit, real worker, or shared-path cleanup is permitted here.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, it, mock as testMock } from "node:test";
import { ActorManager, ActorQueueCheckpointError } from "../src/actors/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { PlacementQueueError, type PlacementRequest, type PlacementQueue } from "../src/agents/placement-admission.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricPlacementWake } from "../src/topology/control-plane.js";

const SECRET_TASK = "private-task-body-never-placement-metadata";
const SECRET_PROMPT = "private-system-prompt-never-placement-metadata";
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const fixtures: Fixture[] = [];
const wakeFor = (launchId: string, generation = 1, reason: FabricPlacementWake["reason"] = "eligible"): FabricPlacementWake => ({
  version: 1, launchId, generation, reason, at: "2030-01-02T03:04:05.123456Z",
});
function queued(request: AgentRunRequest, retryAfter = 0, interaction: PlacementRequest["interaction"]["class"] = "background-agent") {
  const launchId = request.placement?.launchId ?? "missing-internal-binding";
  const generation = request.placement?.generation ?? 1;
  const placement: PlacementRequest = {
    schema: "smarty.placement.v1", operation: "decide", launch_id: launchId, generation,
    agent_name: "fabric-child:fixture", mode: "enforced", assignment: { org_id: "fixture-org", work_id: "fixture-work" },
    role: "actor-activation", complexity: "complex", required_profile: "fixture-profile",
    interaction: { class: interaction, origin_ref: "host-event:fixture" },
    requirements: { capabilities: [], runtime: "pi-rpc", source_pin: "fixture-source", memory_profile_ref: "fixture-memory" },
  };
  const queue: PlacementQueue = { schema: "smarty.placement.v1", decision: "QUEUE", mode: "enforced", launch_id: launchId,
    generation, queue_id: "protected-writer-queue", reason: "AGENT_CAPACITY_EXHAUSTED", retry_after: retryAfter, evidence: [] };
  return new PlacementQueueError(placement, queue);
}
type Run = AgentManager["run"];
type QueueRecord = { id: string; payload: unknown; attempts: number; preparationAttempts: number; executionStarted: boolean;
  placementQueue?: { launchId: string; generation: number; queueId?: string; reason: string; retryAfter: number; retryAt: number;
    wakes: FabricPlacementWake[]; wakePending?: true; retrying?: true }; };
class Fixture {
  readonly root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "actor-placement-queue-"));
  readonly mesh = new MeshStore(path.join(this.root, "mesh"), 64 * 1024, 100, { stateBackend: "file" });
  readonly agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(this.root, "runs"),
  });
  readonly managers: ActorManager[] = [];
  readonly requests: AgentRunRequest[] = [];
  readonly launches: string[] = [];
  readonly deliveries: unknown[] = [];
  readonly warnings: string[] = [];
  readonly transport = testMock.method(ProcessTransport.prototype, "launch", async () => {
    throw new Error("Real worker launch forbidden by placement fixture");
  });
  readonly run = testMock.method(this.agents, "run", async (...args: Parameters<Run>) => {
    this.requests.push(args[0]);
    return this.decide(...args);
  });
  readonly warn = testMock.method(diagnostics, "warn", (_source: string, ...args: unknown[]) => { this.warnings.push(args.join(" ")); });
  decide: Run = async request => { throw queued(request); };
  actors: ActorManager;
  owned = true;
  constructor(readonly queueLimit = DEFAULT_FABRIC_CONFIG.mesh.actorQueueLimit) {
    fixtures.push(this);
    this.actors = this.manager();
  }
  manager(): ActorManager {
    const manager = new ActorManager("placement-fixture", { id: "session:placement-fixture", name: "main", kind: "main" },
      this.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000, actorQueueLimit: this.queueLimit }, this.agents,
      delivery => { this.deliveries.push(delivery); }, {
        actorRoot: path.join(this.root, "actors"), persistent: true, meshRetentionSweepPath: false,
        reapDeadSessionPresence: false, canManageActor: () => this.owned, preparationRetryMs: 20,
      });
    this.managers.push(manager);
    return manager;
  }
  async actor(name = "placement-waiter") {
    return this.actors.create({ name, instructions: SECRET_PROMPT, responseMode: "text", residency: "durable", coalesce: false });
  }
  checkpoint(sessionFile: string): { file: string; items: QueueRecord[]; snapshot: Record<string, unknown> } {
    const directory = path.dirname(sessionFile);
    const basename = fs.readdirSync(directory).find(file => /^queue-[a-f0-9]{16}\.json$/.test(file));
    assert.ok(basename, "activation queue checkpoint exists");
    const file = path.join(directory, basename);
    const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
    return { file, snapshot, items: snapshot.items };
  }
  async idle(manager: ActorManager = this.actors): Promise<void> {
    await this.until(manager, () => manager.inFlightCount() === 0);
  }
  async until(manager: ActorManager, ready: () => boolean): Promise<void> {
    if (ready()) return;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => { unsubscribe(); reject(new Error("Actor placement state event did not arrive")); }, 3_000);
      const unsubscribe = manager.subscribe(() => {
        if (!ready()) return;
        clearTimeout(timer); unsubscribe(); resolve();
      });
    });
  }
  success: Run = async (request, _signal, onLaunched) => {
    const id = `fake-run-${this.launches.length + 1}`;
    this.launches.push(id);
    onLaunched?.({ id, name: request.name ?? "fake", status: "running", runner: "pi", transport: "process", cwd: this.root });
    return { id, name: request.name ?? "fake", task: request.task, runner: "pi", transport: "process", cwd: this.root,
      status: "completed", startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(),
      turns: 1, toolCalls: 0, inferenceStarted: true, text: "fake completion", usage: {
        input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0,
      } } as AgentRunResult;
  };
  async dispose(): Promise<void> {
    try {
      for (const manager of this.managers) await manager.close();
      await this.agents.close();
      assert.equal(this.transport.mock.calls.length, 0, "no real worker transport entered");
    } finally {
      this.run.mock.restore(); this.transport.mock.restore(); this.warn.mock.restore();
      this.mesh.closeState();
      fs.rmSync(this.root, { recursive: true, force: true });
    }
  }
}
afterEach(async () => { for (const fixture of fixtures.splice(0).reverse()) await fixture.dispose(); });

describe("durable actor placement QUEUE", () => {
  it("checkpoints a prompt-free queue identity without worker/run/outcome evidence and restores it on restart", async () => {
    const f = new Fixture();
    const actor = await f.actor();
    const outcomeSend = { eventId: "queued-send", to: actor.id, from: "session:sender", mode: "followUp" as const };
    const accepted = f.actors.tell(actor.id, SECRET_TASK, undefined, { outcomeSend });
    await f.idle();
    const before = f.checkpoint(actor.sessionFile!);
    assert.equal(before.items.length, 1);
    const item = before.items[0]!;
    assert.equal(item.id, accepted.messageId);
    assert.equal(item.executionStarted, false);
    assert.equal(item.attempts, 0);
    assert.equal(item.preparationAttempts, 0);
    assert.deepEqual(item.placementQueue, { launchId: accepted.messageId, generation: 1, queueId: "protected-writer-queue",
      reason: "AGENT_CAPACITY_EXHAUSTED", retryAfter: 0, retryAt: item.placementQueue!.retryAt, wakes: [] });
    assert.ok(item.placementQueue!.retryAt <= Date.now());
    assert.equal(JSON.stringify(item.placementQueue).includes(SECRET_TASK), false);
    assert.equal(JSON.stringify(item.placementQueue).includes(SECRET_PROMPT), false);
    assert.equal(f.actors.status(actor.id).lastRunId, undefined);
    assert.equal(f.actors.status(actor.id).inFlightRun, undefined);
    assert.equal(f.actors.status(actor.id).preparing, undefined);
    assert.equal(f.actors.status(actor.id).activationBlocked, undefined);
    assert.equal(f.actors.messages(actor.id).filter(message => message.direction === "out").length, 0);
    assert.deepEqual(f.agents.list(), []);
    assert.deepEqual(f.launches, []);
    assert.deepEqual(f.deliveries, []);
    assert.equal(before.snapshot.pendingDeliveryOutcomes, undefined);
    assert.equal(fs.existsSync(path.join(f.mesh.root, "delivery-outcomes")), false, "QUEUE owes neither success nor failure receipt");
    assert.deepEqual(f.mesh.read({ topic: "fabric.actor.output", limit: 20 }), []);
    assert.deepEqual(f.warnings.filter(message => !message.includes("fabric-policy.json is missing")), []);
    await f.actors.close();
    f.actors = f.manager();
    await turn(); await f.idle();
    assert.equal(f.requests.length, 1, "restart alone must not re-decide waiting work");
    assert.deepEqual(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue, item.placementQueue);
    assert.equal(f.checkpoint(actor.sessionFile!).items[0]!.attempts, 0);
    f.decide = f.success;
    assert.equal(f.actors.placementWake(actor.id, wakeFor(accepted.messageId)), true);
    await f.until(f.actors, () => f.actors.inFlightCount() === 0 && f.launches.length === 1);
    assert.deepEqual(f.requests[1]!.placement, { role: "actor-activation",
      actorActivation: { actorId: actor.id, rootId: "session:placement-fixture" }, launchId: accepted.messageId, generation: 1 });
  });

  it("retries a wake arriving after QUEUE but before checkpoint, then launches once after durable acceptance", async () => {
    const f = new Fixture();
    const actor = await f.actor();
    let earlyWake: FabricPlacementWake | undefined;
    let earlyAccepted: boolean | undefined;
    f.decide = async request => {
      const response = queued(request);
      const launchId = request.placement?.launchId;
      const generation = request.placement?.generation;
      assert.ok(launchId);
      assert.ok(generation);
      earlyWake = wakeFor(launchId, generation);
      // The authenticated receiver's first attempt lands after QUEUE exists but before
      // ActorManager has installed or durably checkpointed its queue identity.
      earlyAccepted = f.actors.placementWake(actor.id, earlyWake);
      throw response;
    };

    const accepted = f.actors.tell(actor.id, SECRET_TASK);
    await f.idle();
    assert.equal(earlyAccepted, false);
    assert.equal(earlyWake?.launchId, accepted.messageId);
    assert.equal(f.requests.length, 1);
    assert.deepEqual(f.launches, []);
    const beforeRetry = f.checkpoint(actor.sessionFile!).items[0]!;
    assert.equal(beforeRetry.executionStarted, false);
    assert.equal(beforeRetry.placementQueue?.launchId, accepted.messageId);
    assert.equal(beforeRetry.placementQueue?.generation, 1);
    assert.deepEqual(beforeRetry.placementQueue?.wakes, []);

    f.decide = f.success;
    assert.equal(f.actors.placementWake(actor.id, earlyWake!), true);
    const afterWake = f.checkpoint(actor.sessionFile!).items[0]!;
    assert.deepEqual(afterWake.placementQueue?.wakes, [earlyWake]);
    assert.equal(afterWake.placementQueue?.wakePending, true);
    await f.until(f.actors, () => f.launches.length === 1 && f.actors.inFlightCount() === 0);
    assert.equal(f.requests.length, 2);
    assert.equal(f.launches.length, 1, "the redelivered wake launches the activation exactly once");
    assert.equal(f.requests[1]!.placement?.launchId, accepted.messageId);
    assert.equal(f.requests[1]!.placement?.generation, 1);
  });

  it("requires exact local actor id, launch and generation, with durable idempotent matching receipts", async () => {
    const f = new Fixture(); const actor = await f.actor();
    const accepted = f.actors.tell(actor.id, SECRET_TASK); await f.idle();
    const receipt = wakeFor(accepted.messageId);
    const before = fs.readFileSync(f.checkpoint(actor.sessionFile!).file, "utf8");
    for (const target of [actor.name, "actor:unowned", ""]) assert.equal(f.actors.placementWake(target, receipt), false);
    assert.equal(f.actors.placementWake(actor.id, wakeFor("different-launch")), false);
    assert.equal(f.actors.placementWake(actor.id, wakeFor(accepted.messageId, 2)), false);
    f.owned = false; assert.equal(f.actors.placementWake(actor.id, receipt), false); f.owned = true;
    assert.equal(fs.readFileSync(f.checkpoint(actor.sessionFile!).file, "utf8"), before);
    // Hold ordinary draining, not wake persistence, to inspect the committed receipt.
    f.actors.pauseForRelease();
    let synced = 0;
    const fsync = fs.fsyncSync;
    const sync = testMock.method(fs, "fsyncSync", (fd: number) => { synced++; fsync(fd); });
    try {
      assert.equal(f.actors.placementWake(actor.id, receipt), true);
      assert.ok(synced > 0, "true follows real durable barriers");
      assert.deepEqual(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakes, [receipt]);
      assert.equal(f.actors.placementWake(actor.id, { ...receipt, at: "2031-01-02T00:00:00Z" }), true);
      assert.deepEqual(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakes, [receipt], "stable identity ignores a duplicate's timestamp");
    } finally { sync.mock.restore(); }
    f.decide = async request => { throw queued(request); };
    f.actors.resumeAfterRelease();
    await f.until(f.actors, () => f.requests.length === 2 && f.actors.inFlightCount() === 0);
    assert.equal(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakePending, undefined);
    assert.equal(f.actors.placementWake(actor.id, receipt), true);
    await turn(); await f.idle();
    assert.equal(f.requests.length, 2, "redelivery after re-QUEUE must not cause another decide");
    assert.deepEqual(f.launches, []);
  });

  it("throws instead of returning true on a failed durable wake commit, then accepts redelivery", async () => {
    const f = new Fixture(); const actor = await f.actor();
    const accepted = f.actors.tell(actor.id, SECRET_TASK); await f.idle(); f.actors.pauseForRelease();
    const receipt = wakeFor(accepted.messageId);
    const sync = testMock.method(fs, "fsyncSync", () => { throw new Error("fake-only checkpoint failure"); });
    try { assert.throws(() => f.actors.placementWake(actor.id, receipt), ActorQueueCheckpointError); }
    finally { sync.mock.restore(); }
    assert.deepEqual(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakes, []);
    assert.equal(f.actors.placementWake(actor.id, receipt), true);
    assert.deepEqual(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakes, [receipt]);
    assert.equal(f.requests.length, 1);
    assert.deepEqual(f.launches, []);
  });

  it("persists an early wake across restart and lets its timer re-decide only at retry_after", async () => {
    const f = new Fixture(); f.decide = async request => { throw queued(request, 0.2); };
    const actor = await f.actor(); const accepted = f.actors.tell(actor.id, SECRET_TASK); await f.idle();
    const retryAt = f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.retryAt;
    const receipt = wakeFor(accepted.messageId);
    f.actors.pauseForRelease();
    assert.equal(f.actors.placementWake(actor.id, receipt), true);
    assert.equal(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakePending, true);
    await f.actors.close();
    let decidedAt = 0;
    f.decide = async (...args) => { decidedAt = Date.now(); return f.success(...args); };
    f.actors = f.manager();
    assert.deepEqual(f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!.wakes, [receipt]);
    assert.ok(Date.now() < retryAt, "fixture restart must finish inside the early-wake window");
    await turn();
    assert.equal(f.requests.length, 1, "the received wake cannot bypass the retry lower bound");
    await f.until(f.actors, () => f.launches.length === 1 && f.actors.inFlightCount() === 0);
    assert.ok(decidedAt >= retryAt, `decide ${decidedAt} must be at/after ${retryAt}`);
    assert.equal(f.requests.length, 2);
    assert.equal(f.requests[1]!.placement!.launchId, accepted.messageId);
    assert.equal(f.requests[1]!.placement!.generation, 1);
  });

  it("runs unrelated items behind waiting work even when the runnable item is in overflow", async () => {
    const f = new Fixture(1);
    f.decide = async (...args) => {
      if (args[0].task.includes(SECRET_TASK)) throw queued(args[0], 60);
      return f.success(...args);
    };
    const actor = await f.actor(); const waiting = f.actors.tell(actor.id, SECRET_TASK); await f.idle();
    const unrelated = f.actors.tell(actor.id, "unrelated runnable item");
    await f.until(f.actors, () => f.launches.length === 1 && f.actors.inFlightCount() === 0);
    assert.equal(f.requests.length, 2);
    const records = f.checkpoint(actor.sessionFile!).items;
    assert.equal(records.length, 1); assert.equal(records[0]!.id, waiting.messageId);
    assert.notEqual(records[0]!.id, unrelated.messageId);
    assert.equal(records[0]!.executionStarted, false);
    assert.equal(records[0]!.preparationAttempts, 0);
    assert.equal(f.actors.messages(actor.id).filter(message => message.direction === "out" && message.error).length, 0);
  });

  it("retains background ask work durably without resolving or rejecting it as a QUEUE outcome", async () => {
    const f = new Fixture(); const actor = await f.actor();
    let settled = false;
    const answer = f.actors.ask(actor.id, SECRET_TASK).finally(() => { settled = true; });
    await f.idle();
    assert.equal(settled, false);
    const item = f.checkpoint(actor.sessionFile!).items[0]!;
    assert.ok(item.placementQueue);
    assert.deepEqual(f.launches, []); assert.deepEqual(f.deliveries, []);
    f.decide = f.success;
    assert.equal(f.actors.placementWake(actor.id, wakeFor(item.placementQueue.launchId)), true);
    assert.equal((await answer).text, "fake completion");
    await f.idle(); assert.equal(settled, true);
  });

  it("preserves a non-default restored identity and accepts QUEUE without optional queue_id", async () => {
    const f = new Fixture();
    f.decide = async request => { const error = queued(request); delete error.queue.queue_id; throw error; };
    const actor = await f.actor("supervisor");
    f.actors.tell(actor.id, SECRET_TASK); await f.idle();
    const checkpoint = f.checkpoint(actor.sessionFile!);
    assert.equal(checkpoint.items[0]!.placementQueue!.queueId, undefined);
    assert.equal(f.requests[0]!.placement!.role, "actor-activation", "an actor name is never a placement role");
    await f.actors.close();
    // Model a legitimate older generation already durably owned by this local actor.
    const placement = checkpoint.items[0]!.placementQueue!;
    placement.launchId = "restored-writer-launch"; placement.generation = 7;
    Object.assign(placement, { task: SECRET_TASK, prompt: SECRET_PROMPT });
    fs.writeFileSync(checkpoint.file, JSON.stringify(checkpoint.snapshot));
    f.actors = f.manager(); await turn(); await f.idle();
    assert.equal(f.requests.length, 1);
    assert.equal(f.actors.placementWake(actor.id, wakeFor("restored-writer-launch", 1)), false);
    f.decide = f.success;
    assert.equal(f.actors.placementWake(actor.id, wakeFor("restored-writer-launch", 7)), true);
    const restored = f.checkpoint(actor.sessionFile!).items[0]!.placementQueue!;
    assert.equal(JSON.stringify(restored).includes(SECRET_TASK), false);
    assert.equal(JSON.stringify(restored).includes(SECRET_PROMPT), false);
    await f.until(f.actors, () => f.launches.length === 1 && f.actors.inFlightCount() === 0);
    assert.equal(f.requests[1]!.placement!.launchId, "restored-writer-launch");
    assert.equal(f.requests[1]!.placement!.generation, 7);
  });

  it("restores an accepted wake-driven retry interrupted before decide without needing another wake", async () => {
    const f = new Fixture(); const actor = await f.actor();
    const accepted = f.actors.tell(actor.id, SECRET_TASK); await f.idle();
    let enter!: () => void, release!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; });
    const released = new Promise<void>(resolve => { release = resolve; });
    let interrupted: ReturnType<Fixture["checkpoint"]> | undefined;
    f.decide = async request => {
      interrupted = f.checkpoint(actor.sessionFile!);
      enter(); await released; throw queued(request);
    };
    try {
      assert.equal(f.actors.placementWake(actor.id, wakeFor(accepted.messageId)), true);
      await entered;
      assert.equal(interrupted!.items[0]!.placementQueue!.wakePending, undefined);
      assert.equal(interrupted!.items[0]!.placementQueue!.retrying, true);
      assert.equal(interrupted!.items[0]!.executionStarted, false);
    } finally { release(); }
    await f.idle(); await f.actors.close();
    fs.writeFileSync(interrupted!.file, JSON.stringify(interrupted!.snapshot));
    f.decide = f.success; f.actors = f.manager();
    await f.until(f.actors, () => f.launches.length === 1 && f.actors.inFlightCount() === 0);
    assert.equal(f.requests.length, 3);
    assert.equal(f.requests[2]!.placement!.launchId, accepted.messageId);
    assert.equal(f.requests[2]!.placement!.generation, 1);
  });

  it("forwards a matching wake to the exact actor in ActorDirectory's secondary scope", async () => {
    const f = new Fixture();
    const identity = { id: "session:placement-fixture", name: "main", kind: "main" as const, sessionId: "placement-fixture" };
    const directory = new ActorDirectory([
      "placement-fixture", identity, f.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, f.agents,
      delivery => { f.deliveries.push(delivery); },
      { persistent: true, rootId: identity.id, meshRetentionSweepPath: false, reapDeadSessionPresence: false,
        canManageActor: () => f.owned, preparationRetryMs: 20 },
    ], { project: path.join(f.root, "project-actors"), session: path.join(f.root, "session-actors") }, "project");
    f.managers.push(directory); f.actors = directory;
    f.decide = async request => { throw queued(request, 60); };
    const actor = await directory.create({ scope: "session", name: "secondary-placement-actor", instructions: SECRET_PROMPT,
      responseMode: "text", residency: "durable", coalesce: false });
    const accepted = directory.tell(actor.id, SECRET_TASK);
    await f.until(directory, () => f.requests.length === 1 && directory.inFlightCount() === 0);
    expect(f.requests[0]!.placement).toMatchObject({ role: "actor-activation",
      actorActivation: { actorId: actor.id, rootId: identity.id }, launchId: accepted.messageId, generation: 1 });
    const receipt = wakeFor(accepted.messageId);
    expect(directory.placementWake(actor.name, receipt)).toBe(false);
    expect(directory.placementWake(actor.id, receipt)).toBe(true);
    const checkpoint = f.checkpoint(actor.sessionFile!);
    expect(checkpoint.items[0]!.placementQueue!.wakes).toEqual([receipt]);
    expect(checkpoint.items[0]!.placementQueue!.wakePending).toBe(true);
    expect(f.requests).toHaveLength(1);
    expect(f.launches).toEqual([]);
  });

  it("fails closed immediately for live-human QUEUE instead of checkpointing or scheduling it", async () => {
    const f = new Fixture(); f.decide = async request => {
      const error = queued(request, 60, "live-human");
      error.message = `untrusted placement diagnostics ${SECRET_TASK} ${SECRET_PROMPT}`;
      f.owned = false; // Even simultaneous ownership loss must not park live-human QUEUE.
      throw error;
    };
    const actor = await f.actor();
    try { await assert.rejects(f.actors.ask(actor.id, SECRET_TASK), /Placement/); }
    finally { f.owned = true; }
    await f.idle();
    assert.equal(f.requests.length, 1);
    assert.equal(f.actors.status(actor.id).queued, 0);
    assert.equal(f.actors.status(actor.id).lastRunId, undefined);
    assert.equal(f.actors.status(actor.id).inFlightRun, undefined);
    assert.deepEqual(f.launches, []);
    assert.equal(f.actors.placementWake(actor.id, wakeFor(f.requests[0]!.placement!.launchId!)), false);
    const directory = path.dirname(actor.sessionFile!);
    for (const file of fs.readdirSync(directory).filter(file => file.startsWith("queue-"))) {
      assert.equal(fs.readFileSync(path.join(directory, file), "utf8").includes("placementQueue"), false);
    }
    assert.ok(f.warnings.every(message => !message.includes(SECRET_TASK) && !message.includes(SECRET_PROMPT)));
    const failures = f.actors.messages(actor.id).filter(message => message.direction === "out" && message.error);
    assert.equal(failures.length, 1);
    assert.equal(JSON.stringify(failures).includes(SECRET_TASK), false);
    assert.equal(JSON.stringify(failures).includes(SECRET_PROMPT), false);
  });
});
