import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ACTOR_FAILURE_NOTICE_AFTER, ActorManager, ActorRegistryOwnershipError } from "../src/actors/manager.js";
import type { FabricCapabilityRequirement } from "../src/components/types.js";
import type { FabricCapabilityViewLease } from "../src/core/action-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentDeliveryRequest } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentManager } from "../src/agents/manager.js";

const roots: string[] = [];
const actorManagers: ActorManager[] = [];
const agentManagers: AgentManager[] = [];

const DEFAULT_WAIT_MS = process.env.CI ? 10_000 : 2_000;

const waitFor = async (predicate: () => boolean, timeoutMs = DEFAULT_WAIT_MS): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

const setup = (
  persistent = false,
  canManageActor?: (id: string) => boolean | undefined,
  acquireCapabilityView?: (
    requirements: readonly FabricCapabilityRequirement[],
    signal: AbortSignal,
  ) => Promise<FabricCapabilityViewLease>,
  modelValidation?: {
    preparePiModel?: (model: string | undefined) => Promise<string | void>;
    resolvePiModel?: (model: string) => string;
  },
  meshOverrides: Partial<typeof DEFAULT_FABRIC_CONFIG.mesh> = {},
) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-test-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    runRoot: path.join(root, "runs"),
    ...(modelValidation?.preparePiModel
      ? { preparePiModel: modelValidation.preparePiModel }
      : {}),
  });
  agentManagers.push(agents);
  const identity: MeshIdentity = {
    id: "session:test",
    name: "main",
    kind: "main",
    sessionId: "test",
  };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, ...meshOverrides };
  const deliveries: string[] = [];
  const actors = new ActorManager(
    "test",
    identity,
    mesh,
    meshConfig,
    agents,
    ({ message }) => {
      if (message.text) deliveries.push(message.text);
    },
    {
      actorRoot: path.join(root, "actors"),
      persistent,
      ...(canManageActor ? { canManageActor } : {}),
      ...(acquireCapabilityView ? { acquireCapabilityView } : {}),
      ...(modelValidation?.resolvePiModel
        ? { resolvePiModel: modelValidation.resolvePiModel }
        : {}),
    },
  );
  actorManagers.push(actors);
  return { actors, mesh, deliveries, root, agents, identity, meshConfig };
};

afterEach(async () => {
  await Promise.all(actorManagers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentManagers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// smarty-dev#448: presence writes lost on a contended mesh lock left the mesh without a new
// actor, or with a removed one.
describe("ActorManager presence under a stalled mesh lock", () => {
  const stalledSetup = (presenceRetryMs = 50) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-presence-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { lockTimeoutMs: 150 });
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const identity: MeshIdentity = { id: "session:presence", name: "main", kind: "main", sessionId: "presence" };
    const make = () => {
      const manager = new ActorManager("presence", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
        { actorRoot: path.join(root, "actors"), persistent: true, presenceRetryMs });
      actorManagers.push(manager);
      return manager;
    };
    const lockPath = path.join(mesh.root, ".lock");
    const hold = () => {
      fs.mkdirSync(mesh.root, { recursive: true });
      fs.mkdirSync(lockPath, { mode: 0o700 });
      fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    };
    const release = () => fs.rmSync(lockPath, { recursive: true, force: true });
    return { mesh, make, hold, release, identity };
  };

  // smarty-dev#784: a host with 13 actors re-decided every actor's ownership once per actor for
  // each mesh event, before the topic filter (182 directory reads per event), and its event
  // loop saturated.
  it("decides ownership once per mesh event, not once per actor", async () => {
    const decisions = vi.fn((_id: string) => true as boolean | undefined);
    const { actors, mesh } = setup(false, decisions);
    for (let index = 0; index < 10; index++) {
      await actors.create({ name: `actor-${index}`, instructions: "Watch.", topics: [index === 0 ? "team.pulls" : `team.other-${index}`], responseMode: "text" });
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
    decisions.mockClear();
    const from: MeshIdentity = { id: "session:other", name: "main", kind: "main", sessionId: "other" };
    for (let index = 0; index < 20; index++) await mesh.publish({ topic: "fleet.noise", from, data: { index } });
    await new Promise((resolve) => setTimeout(resolve, 400));                  // several 20 ms polls
    // Before: 20 events x 10 actors x (1 + 10) decisions = 2,200. Now: 10 per event, plus the polls' own.
    expect(decisions.mock.calls.length).toBeGreaterThan(0);
    expect(decisions.mock.calls.length).toBeLessThan(600);
  });

  // review/astra F1 on #72: a matching event also enqueued, started a drain and recorded the
  // message, and each of those re-decided every actor's ownership.
  it("decides ownership once per matching mesh event too, with several subscribers and full queues", async () => {
    const decisions = vi.fn((_id: string) => true as boolean | undefined);
    const { actors, mesh } = setup(false, decisions, undefined, undefined, { actorQueueLimit: 2 });
    for (let index = 0; index < 8; index++) {
      await actors.create({ name: `actor-${index}`, instructions: "Watch.", topics: ["team.pulls"], responseMode: "text", coalesce: false });
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
    decisions.mockClear();
    const from: MeshIdentity = { id: "session:other", name: "main", kind: "main", sessionId: "other" };
    for (let index = 0; index < 20; index++) await mesh.publish({ topic: "team.pulls", from, data: { index } });
    await new Promise((resolve) => setTimeout(resolve, 400));
    // Before: each delivery re-decided all 8 actors 4 times (enqueue, drain, record twice).
    expect(decisions.mock.calls.length).toBeGreaterThan(0);
    expect(decisions.mock.calls.length).toBeLessThan(1_200);
  });

  // smarty-dev#918: callers verify setInstructions by digest, without reading the registry file.
  it("reports the instructions digest and length, and updates them on setInstructions", async () => {
    const { actors } = setup();
    const sha = (text: string) => createHash("sha256").update(text).digest("hex");
    const actor = await actors.create({ name: "digest", instructions: "First.", topics: ["team.pulls"], responseMode: "text" });
    expect(actors.status(actor.id)).toMatchObject({ instructionsDigest: sha("First."), instructionsLength: 6 });
    await actors.setInstructions(actor.id, "Second text.");
    expect(actors.list().find((info) => info.id === actor.id)).toMatchObject({ instructionsDigest: sha("Second text."), instructionsLength: 12 });
    expect(actors.status(actor.id)).not.toHaveProperty("instructions");
  });

  it("publishes a new actor's presence once a stalled lock frees", async () => {
    const { mesh, make, hold, release } = stalledSetup();
    const actors = make();
    hold();
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    expect(mesh.get(`actors/presence/${actor.id}`)).toBeUndefined();
    release();
    await waitFor(() => mesh.get(`actors/presence/${actor.id}`) !== undefined, 5_000);
  }, 20_000);

  it("deletes a removed actor's presence once a stalled lock frees", async () => {
    const { mesh, make, hold, release } = stalledSetup();
    const actors = make();
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await waitFor(() => mesh.get(`actors/presence/${actor.id}`) !== undefined, 5_000);
    hold();
    await actors.remove(actor.id);
    expect(mesh.get(`actors/presence/${actor.id}`)).toBeDefined();
    release();
    await waitFor(() => mesh.get(`actors/presence/${actor.id}`) === undefined, 5_000);
  }, 20_000);

  // review/astra on #46, F1: a presence write held on the lock must not land after a newer one.
  it("never puts a removed actor back when an older presence write lands late", async () => {
    const { mesh, make } = stalledSetup();
    const actors = make();
    const put = mesh.put.bind(mesh);
    let open!: () => void;
    const gate = new Promise<void>((resolve) => { open = resolve; });
    let held = 0;
    vi.spyOn(mesh, "put").mockImplementation(async (input) => {
      if (input.key.startsWith("actors/presence/") && held++ === 0) await gate;   // the create's write sleeps
      return put(input);
    });
    const created = actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await waitFor(() => held === 1);
    const actor = actors.list().find((candidate) => candidate.name === "reviewer")!;
    const removed = actors.remove(actor.id);                   // its delete must wait behind that write
    await new Promise((resolve) => setTimeout(resolve, 100));
    open();
    await created;
    await removed;
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(mesh.get(`actors/presence/${actor.id}`)).toBeUndefined();
  }, 20_000);

  it("lets close() wait for a presence write in flight", async () => {
    const { mesh, make } = stalledSetup();
    const actors = make();
    const put = mesh.put.bind(mesh);
    let open!: () => void;
    const gate = new Promise<void>((resolve) => { open = resolve; });
    let held = 0;
    vi.spyOn(mesh, "put").mockImplementation(async (input) => {
      if (input.key.startsWith("actors/presence/") && held++ === 0) await gate;
      return put(input);
    });
    const created = actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await waitFor(() => held === 1);
    let closed = false;
    const closing = actors.close().then(() => { closed = true; });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(closed).toBe(false);
    open();
    await closing;
    await created.catch(() => undefined);
    expect(closed).toBe(true);
  }, 20_000);

  // review/astra on #46, F2: an orphan delete that failed keeps its version fence on retry.
  it("does not delete an orphan entry that another writer replaced before the retry", async () => {
    const { mesh, make, hold, release, identity } = stalledSetup(1_000);
    await mesh.put({ key: "actors/presence/ghost", value: { id: "ghost", scope: "project" }, identity });
    const actorsDir = path.join(path.dirname(mesh.root), "actors");
    fs.mkdirSync(actorsDir, { recursive: true });
    fs.writeFileSync(path.join(actorsDir, "actors.json"), JSON.stringify({ actors: [] }));
    hold();
    make();                                                    // the reap's delete fails on the lock (150 ms)
    await new Promise((resolve) => setTimeout(resolve, 300));
    release();
    // Before the 1 s retry, another writer replaces the entry.
    const replaced = await mesh.put({ key: "actors/presence/ghost", value: { id: "ghost", scope: "project", again: true }, identity: { ...identity, id: "session:other" } });
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(mesh.get("actors/presence/ghost")?.version).toBe(replaced.version);
  }, 20_000);

  // review/astra on #46, F3: a registry that cannot be read proves nothing; presence stays.
  it.each([["malformed", "{"], ["with an unloadable record", JSON.stringify({ actors: [{ id: "kept" }] })]])(
    "keeps presence when the registry is %s", async (_case, registry) => {
      const { mesh, make, identity } = stalledSetup();
      await mesh.put({ key: "actors/presence/kept", value: { id: "kept", scope: "project" }, identity });
      const actorsDir = path.join(path.dirname(mesh.root), "actors");
      fs.mkdirSync(actorsDir, { recursive: true });
      fs.writeFileSync(path.join(actorsDir, "actors.json"), registry);
      make();
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(mesh.get("actors/presence/kept")).toBeDefined();
    }, 20_000);

  it("reaps this session's orphan presence at start, and nothing of another scope or writer", async () => {
    const { mesh, make, identity } = stalledSetup();
    const other = { ...identity, id: "session:other" };
    await mesh.put({ key: "actors/presence/ghost", value: { id: "ghost", scope: "project" }, identity });
    await mesh.put({ key: "actors/presence/session-scoped", value: { id: "session-scoped", scope: "session" }, identity });
    await mesh.put({ key: "actors/presence/foreign", value: { id: "foreign", scope: "project" }, identity: other });
    // The registry was read and no longer lists "ghost" (a remove whose delete never landed).
    const actorsDir = path.join(path.dirname(mesh.root), "actors");
    fs.mkdirSync(actorsDir, { recursive: true });
    fs.writeFileSync(path.join(actorsDir, "actors.json"), JSON.stringify({ actors: [] }));
    make();                                                    // project scope (the mesh default)
    await waitFor(() => mesh.get("actors/presence/ghost") === undefined, 5_000);
    expect(mesh.get("actors/presence/session-scoped")).toBeDefined();
    expect(mesh.get("actors/presence/foreign")).toBeDefined();
  }, 20_000);
});

// smarty-dev#472: a Main-hosted actor lost every event published while its session reloaded,
// because each runtime started its mesh stream at the tail.
const recordRuns = (agents: AgentManager, hold?: () => Promise<void>) => {
  const runs: Array<{ task: string; startedAt: number; finishedAt?: number }> = [];
  const run = agents.run.bind(agents);
  vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
    const entry: { task: string; startedAt: number; finishedAt?: number } = { task: request.task, startedAt: Date.now() };
    runs.push(entry);
    try {
      const result = await run(request, signal);
      await hold?.();
      return result;
    } finally { entry.finishedAt = Date.now(); }
  });
  return runs;
};

describe("ActorManager across a session reload", () => {
  const reloadable = (root: string, mesh: MeshStore, agents: AgentManager, replayAgeMs = 10 * 60_000, actorQueueLimit?: number) => {
    const manager = new ActorManager(
      "reload", { id: "session:reload", name: "main", kind: "main", sessionId: "reload" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, ...(actorQueueLimit ? { actorQueueLimit } : {}) }, agents, () => {},
      {
        actorRoot: path.join(root, "actors"), persistent: true,
        meshCursorPath: path.join(root, "actors", "mesh-cursor.json"), meshReplayAgeMs: replayAgeMs,
      },
    );
    actorManagers.push(manager);
    return manager;
  };

  it("delivers an event published while the session reloaded, after the reload", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents);
    const actor = await before.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await mesh.publish({ topic: "team.pulls", from, text: "before the reload" });
    await waitFor(() => before.messages(actor.id).some((message) => message.direction === "out" && !message.error));
    await before.close();                                    // the old runtime is gone ...
    await mesh.publish({ topic: "team.pulls", from, text: "during the reload" });
    const after = reloadable(root, mesh, agents);            // ... and the new one starts
    await waitFor(() => after.messages(actor.id).filter((message) => message.direction === "out" && !message.error).length === 2, 15_000);
  }, 30_000);

  // review/astra on #45: catch-up must not overflow a 32-item queue silently.
  it("replays 34 missed events into a resumed idle actor, all of them, each once", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents);
    const actor = await before.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await before.close();
    for (let index = 1; index <= 34; index++) await mesh.publish({ topic: "team.pulls", from, text: `missed ${index}` });
    const after = reloadable(root, mesh, agents);
    const replies = () => after.messages(actor.id).filter((message) => message.direction === "out" && !message.error);
    await waitFor(() => replies().length >= 34, 90_000);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(replies()).toHaveLength(34);                      // every event ran, none twice
  }, 120_000);

  it("offers a deferred catch-up event again only to the actor whose queue was full", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents, undefined, 2);
    const busy = await before.create({ name: "busy", instructions: "Work.", topics: ["busy.only", "shared"], responseMode: "text" });
    const idle = await before.create({ name: "idle", instructions: "Work.", topics: ["shared"], responseMode: "text" });
    await before.close();
    // While down: the busy actor gets a run that never ends plus two queued events (its limit),
    // then one event both actors take.
    for (const text of ["HANG busy", "queued 1", "queued 2"]) await mesh.publish({ topic: "busy.only", from, text });
    await mesh.publish({ topic: "shared", from, text: "shared event" });
    const after = reloadable(root, mesh, agents, undefined, 2);
    const idleReplies = () => after.messages(idle.id).filter((message) => message.direction === "out" && !message.error);
    await waitFor(() => idleReplies().length === 1, 15_000);
    await new Promise((resolve) => setTimeout(resolve, 3_000));  // the busy actor stays full; retries go on
    expect(idleReplies()).toHaveLength(1);
    expect(after.status(busy.id).queued).toBe(2);
    after.haltAll();
  }, 40_000);

  // smarty-dev#878: a relaunch lost 33 queued items: the queue lived only in memory while the
  // mesh cursor already sat past the queued events.
  it.each([false, true])("keeps queued and running items across a restart (cursor replayed from the start: %s)", async (replayAll) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents);
    const actor = await before.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text", coalesce: false });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });   // a 1.5 s run
    await waitFor(() => before.status(actor.id).status === "running", 10_000);
    await mesh.publish({ topic: "team.pulls", from, text: "queued 1" });
    await mesh.publish({ topic: "team.pulls", from, text: "queued 2" });
    await waitFor(() => before.status(actor.id).queued === 2, 10_000);
    await before.close();                                    // the runtime goes mid-run
    if (replayAll) {
      // A crash before the cursor was written: the replay offers every event again.
      fs.writeFileSync(path.join(root, "actors", "mesh-cursor.json"), JSON.stringify({ format: 1, cursor: 0 }));
    }
    const after = reloadable(root, mesh, agents);
    const replies = (text: RegExp) => after.messages(actor.id)
      .filter((message) => message.direction === "out" && !message.error && text.test(String(message.text ?? "")));
    await waitFor(() => replies(/^fake worker complete$/).length >= 2 && replies(/^live attempt/).length >= 1, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(replies(/^fake worker complete$/)).toHaveLength(2);           // each queued item once
    expect(replies(/^live attempt/).length).toBeLessThanOrEqual(2);     // the interrupted run at most twice
    expect(fs.existsSync(path.join(root, "actors", actor.id, "queue.json"))).toBe(false);
  }, 60_000);

  // review/astra F2 on #79: one running plus a full waiting queue must all survive a restart.
  it("keeps one running and a full waiting queue across a restart, dropping none", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const runs = recordRuns(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents, undefined, 2);
    const actor = await before.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text", coalesce: false });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => before.status(actor.id).status === "running", 10_000);
    await mesh.publish({ topic: "team.pulls", from, text: "waiting-a" });
    await mesh.publish({ topic: "team.pulls", from, text: "waiting-b" });                // exactly the limit
    await waitFor(() => before.status(actor.id).queued === 2, 10_000);
    await before.close();
    reloadable(root, mesh, agents, undefined, 2);
    await waitFor(() => runs.filter((run) => /waiting-[ab]/.test(run.task) && run.finishedAt !== undefined).length === 2, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(runs.filter((run) => run.task.includes("waiting-a"))).toHaveLength(1);
    expect(runs.filter((run) => run.task.includes("waiting-b"))).toHaveLength(1);
  }, 60_000);

  // review/astra F3 on #79: freshness state must survive with the queue, or the latest pending
  // activation reads as superseded after a restart.
  it("keeps validWhile freshness across a restart: the latest item runs, older ones stay stale", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const runs = recordRuns(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents);
    const actor = await before.create({
      name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text", coalesce: false,
      validWhile: { version: 1, source: "({ activation, current }) => activation.sequence === current.latestActivationSequence" },
    });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => before.status(actor.id).status === "running", 10_000);
    await mesh.publish({ topic: "team.pulls", from, text: "older" });
    await mesh.publish({ topic: "team.pulls", from, text: "newest" });
    await waitFor(() => before.status(actor.id).queued === 2, 10_000);
    await before.close();
    const after = reloadable(root, mesh, agents);
    await waitFor(() => runs.some((run) => run.task.includes("newest") && run.finishedAt !== undefined), 30_000);
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(runs.filter((run) => run.task.includes("newest"))).toHaveLength(1);
    expect(runs.filter((run) => run.task.includes("older"))).toEqual([]);
    expect(after.messages(actor.id).some((message) => message.stale)).toBe(true);
  }, 60_000);

  // review/astra F1 on #79: a passive view of another host's actor must not delete or replace the
  // owner's persisted queue.
  it("keeps the owner's queue file when a passive host refreshes the actor, and the owner restores it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const runs = recordRuns(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const host = (name: string, owns: boolean) => {
      const manager = new ActorManager(
        name, { id: `session:${name}`, name: "main", kind: "main", sessionId: name }, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
        {
          actorRoot: path.join(root, "actors"), persistent: true, rootId: "session:owner", claimResidency: "session",
          canManageActor: () => owns, lineageAlive: () => true,
          meshCursorPath: path.join(root, `cursor-${name}.json`),
        },
      );
      actorManagers.push(manager);
      return manager;
    };
    const passiveHost = (name: string) => {
      const manager = new ActorManager(
        name, { id: `session:${name}`, name: "main", kind: "main", sessionId: name }, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
        {
          actorRoot: path.join(root, "actors"), persistent: true, rootId: `session:${name}`, claimResidency: "session",
          canManageActor: () => false, lineageAlive: () => true,
        },
      );
      actorManagers.push(manager);
      return manager;
    };
    const owner = host("owner", true);
    const actor = await owner.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text", coalesce: false });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => owner.status(actor.id).status === "running", 10_000);
    await mesh.publish({ topic: "team.pulls", from, text: "held-a" });
    await mesh.publish({ topic: "team.pulls", from, text: "held-b" });
    await waitFor(() => owner.status(actor.id).queued === 2, 10_000);
    const queueFile = path.join(root, "actors", actor.id, "queue.json");
    const passive = passiveHost("passive");                   // loads the foreign actor
    await owner.setInstructions(actor.id, "Review carefully.");          // a registry change ...
    passive.listOwned();                                                // ... that the passive view picks up
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).items).toHaveLength(3);
    await owner.close();
    host("owner", true);                                                // the owner restarts
    await waitFor(() => runs.filter((run) => /held-[ab]/.test(run.task) && run.finishedAt !== undefined).length === 2, 30_000);
  }, 60_000);

  // review/astra F1 (re-review) on #79: the empty queue a new owner parks before it reloads must
  // not delete the dead owner's file.
  it.each([
    ["starts after the owner died", false],
    ["was running before the owner died", true],
  ] as const)("lets an adopter that %s run the dead owner's accepted backlog", async (_order, successorFirst) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const runs = recordRuns(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    let ownerAlive = true;
    const manager = (name: string, owns: () => boolean | undefined) => {
      const value = new ActorManager(
        name, { id: `session:${name}`, name: "main", kind: "main", sessionId: name }, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
        {
          actorRoot: path.join(root, "actors"), persistent: true, rootId: `session:${name}`, claimResidency: "session",
          canManageActor: owns, lineageAlive: (rootId) => rootId !== "session:owner" || ownerAlive,
          adoptionGraceMs: 0, meshCursorPath: path.join(root, `cursor-${name}.json`),
        },
      );
      actorManagers.push(value);
      return value;
    };
    const owner = manager("owner", () => (ownerAlive ? true : undefined));
    const actor = await owner.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text", coalesce: false });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => owner.status(actor.id).status === "running", 10_000);
    await mesh.publish({ topic: "team.pulls", from, text: "backlog-a" });
    await mesh.publish({ topic: "team.pulls", from, text: "backlog-b" });
    await waitFor(() => owner.status(actor.id).queued === 2, 10_000);
    if (successorFirst) manager("successor", () => undefined);  // a passive view, then adopts via refresh
    await owner.close();
    ownerAlive = false;                                         // the owner's lineage is dead
    if (!successorFirst) manager("successor", () => undefined); // adopts during its own start
    // Either way the successor's cursor starts at the log's end: no replay can mask a loss.
    await waitFor(() => runs.filter((run) => /backlog-[ab]/.test(run.task) && run.finishedAt !== undefined).length === 2, 30_000);
  }, 60_000);

  // review/astra F3 (re-review) on #79: a host revision that changed after the last queue write
  // must not roll back on restart and make obsolete work valid again.
  it.each([
    ["taskRevision", "input"],
    ["mainRevision", "main activity"],
  ] as const)("keeps a host activation made stale by %s stale across a restart", async (field, change) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const runs = recordRuns(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents);
    const actor = await before.create({
      name: "watcher", instructions: "Watch.", topics: ["team.pulls"], events: ["tool_error"], responseMode: "text", coalesce: false,
      validWhile: { version: 1, source: `({ activation, current }) => activation.kind !== "hostEvent" || activation.${field} === current.${field}` },
    });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => before.status(actor.id).status === "running", 10_000);
    before.dispatchHostEvent("tool_error", { error: "obsolete-error" });
    await waitFor(() => before.status(actor.id).queued === 1, 10_000);
    if (change === "input") before.dispatchHostEvent("input", { text: "a new task" });   // not subscribed: no queue write
    else before.noteMainActivity();
    await before.close();
    const after = reloadable(root, mesh, agents);
    // The restored host activation is judged, not dropped: it ends as a stale message.
    const judged = () => after.messages(actor.id).filter((message) => message.direction === "out" && message.source === "host:tool_error");
    await waitFor(() => judged().length >= 1, 30_000);
    expect(judged().every((message) => message.stale)).toBe(true);
  }, 60_000);

  it("does not replay events older than the replay window after a long downtime", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reload-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const before = reloadable(root, mesh, agents);
    const actor = await before.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    await before.close();
    await mesh.publish({ topic: "team.pulls", from, text: "long ago" });
    await new Promise((resolve) => setTimeout(resolve, 300));
    const after = reloadable(root, mesh, agents, 100);      // the event is older than the window
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(after.messages(actor.id).filter((message) => message.direction === "out")).toEqual([]);
  }, 30_000);
});

describe("ActorManager", () => {
  it("updates inference context on the same identity, preserves policy/history, and restores it", async () => {
    const s = setup(true);
    const actor = await s.actors.create({
      name: "window", instructions: "Keep quiet on hold; retain task refs.",
      events: ["agent_settled"], topics: ["task.result"], tools: [], extensions: false,
      thinking: "medium", responseMode: "directive", coalesce: false,
    });
    expect(actor.inferenceContext).toBeUndefined();
    const before = s.actors.definition(actor.id);
    const messages = s.actors.messages(actor.id);
    const selected = await s.actors.setInferenceContext(actor.id, "activation");
    expect(selected).toEqual({ ...actor, inferenceContext: "activation", updatedAt: selected.updatedAt });
    expect(s.actors.definition(actor.id)).toEqual({ ...before, inferenceContext: "activation" });
    expect(s.actors.messages(actor.id)).toEqual(messages);
    await s.actors.close();
    const restored = new ActorManager("test", s.identity, s.mesh, s.meshConfig, s.agents, () => {}, {
      actorRoot: path.join(s.root, "actors"), persistent: true,
    });
    actorManagers.push(restored);
    expect(restored.status(actor.id)).toMatchObject({ id: actor.id, inferenceContext: "activation", extensions: false, tools: [] });
    expect((await restored.setInferenceContext(actor.id, "full-history")).inferenceContext).toBe("full-history");
    await expect(restored.setInferenceContext(actor.id, "invalid" as "activation")).rejects.toThrow(/inference context/);
    await expect(restored.create({ name: "unsupported", instructions: "test", runner: "claude", inferenceContext: "activation" })).rejects.toThrow(/Pi runner/);
  });

  it("snapshots the policy per activation without enabling Fabric or changing its task", async () => {
    const { actors, agents } = setup();
    const run = vi.spyOn(agents, "run");
    const actor = await actors.create({ name: "window", instructions: "Keep quiet", extensions: false, tools: [], inferenceContext: "activation" });
    const pending = actors.ask(actor.id, "LIVE_WITH_PROGRESS");
    expect(actors.status(actor.id).status).toBe("running");
    // Change policy during the pre-launch await, not just after spawn.
    await actors.setInferenceContext(actor.id, "full-history");
    await waitFor(() => run.mock.calls.length === 1);
    const first = run.mock.calls[0]![0];
    expect(first).toMatchObject({ inferenceContext: "activation", extensions: false, recursive: false, tools: [], sessionFile: actor.sessionFile, actorId: actor.id });
    await pending;
    await actors.ask(actor.id, "next activation");
    expect(run.mock.calls[1]![0].inferenceContext).toBe("full-history");
    expect(run.mock.calls[1]![0].sessionFile).toBe(first.sessionFile);
  });

  it("uses event monitoring where supported and polling fallback on Windows", async () => {
    const { mesh } = setup();
    const tail = vi.spyOn(mesh, "tail");

    await new Promise((resolve) => setTimeout(resolve, 80));

    if (process.platform === "win32") {
      expect(tail.mock.calls.length).toBeGreaterThan(1);
    } else {
      expect(tail).toHaveBeenCalledTimes(1);
    }
  });

  it("owner-gates execution and project defaults while allowing session bindings", async () => {
    let owns = true;
    const { actors, mesh } = setup(false, () => owns);
    const actor = await actors.create({ name: "leased", instructions: "Observe." });
    await waitFor(() => Boolean(mesh.get(`actors/test/${actor.id}`)));

    owns = false;
    expect(() => actors.tell(actor.id, "do not run")).toThrow("owned by another host");
    expect(actors.dispatchHostEvent("input", { text: "ignored" })).toBe(0);
    await expect(actors.setModel(actor.id, "provider/session-model")).resolves.toMatchObject({
      model: "provider/session-model",
      binding: { model: "provider/session-model", scope: "session" },
      projectDefaults: { scope: "project" },
    });
    await expect(
      actors.setModel(actor.id, "provider/project-model", "project"),
    ).rejects.toThrow("owned by another host");

    owns = true;
    expect(actors.tell(actor.id, "run after takeover")).toMatchObject({ queued: true });
  });

  it("rejects unavailable Pi models on create, setModel, and activation overrides", async () => {
    const resolvePiModel = (model: string): string => {
      if (model !== "provider/visible") {
        throw new Error(`Model ${JSON.stringify(model)} is not available to this Pi session`);
      }
      return model;
    };
    const { actors } = setup(false, undefined, undefined, { resolvePiModel });

    await expect(
      actors.create({
        name: "hidden create",
        instructions: "Do not persist.",
        runner: "pi",
        model: "provider/hidden",
      }),
    ).rejects.toThrow(/not available to this Pi session/);
    const actor = await actors.create({
      name: "visible actor",
      instructions: "Use the visible model.",
      runner: "pi",
      model: "provider/visible",
    });

    await expect(actors.setModel(actor.id, "provider/hidden")).rejects.toThrow(
      /not available to this Pi session/,
    );
    expect(() =>
      actors.tell(actor.id, "Do not queue", undefined, {
        overrides: { model: "provider/hidden" },
      })
    ).toThrow(/not available to this Pi session/);
    await expect(
      actors.ask(actor.id, "Do not run", undefined, undefined, {
        overrides: { model: "provider/hidden" },
      }),
    ).rejects.toThrow(/not available to this Pi session/);
    expect(actors.status(actor.id).queued).toBe(0);
  });

  it("rejects a persisted actor binding that becomes hidden before launch", async () => {
    let visible = true;
    const preparePiModel = async (model: string | undefined): Promise<string | void> => {
      if (!visible || model !== "provider/visible") {
        throw new Error(`Model ${JSON.stringify(model)} is not available to this Pi session`);
      }
      return model;
    };
    const { actors, agents, root } = setup(true, undefined, undefined, { preparePiModel });
    const actor = await actors.create({
      name: "stale binding",
      instructions: "Never launch a newly hidden model.",
      runner: "pi",
    });
    await actors.setModel(actor.id, "provider/visible");
    expect(fs.readdirSync(path.join(root, "actors", "bindings"))).toHaveLength(1);

    visible = false;
    await expect(actors.ask(actor.id, "Do not launch")).rejects.toThrow(
      /not available to this Pi session/,
    );
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(agents.list()).toEqual([]);
    expect(actors.status(actor.id).lastError).toMatch(/not available to this Pi session/);
  });

  it("isolates two live sessions over one shared actor definition", async () => {
    const owner = setup(true);
    const actor = await owner.actors.create({
      name: "shared reviewer",
      instructions: "Review from the caller's bound model.",
      model: "provider/project-default",
      thinking: "medium",
    });
    const peerIdentity: MeshIdentity = {
      id: "session:peer",
      name: "main",
      kind: "main",
      sessionId: "peer",
    };
    const peer = new ActorManager(
      "peer",
      peerIdentity,
      owner.mesh,
      owner.meshConfig,
      owner.agents,
      () => {},
      {
        actorRoot: path.join(owner.root, "actors"),
        persistent: true,
        canManageActor: () => false,
      },
    );
    actorManagers.push(peer);
    const registryPath = path.join(owner.root, "actors", "actors.json");
    const registryBeforeBindings = fs.readFileSync(registryPath, "utf8");

    await owner.actors.setModel(actor.id, "provider/session-a");
    await owner.actors.setThinking(actor.id, "high");
    await peer.setModel(actor.id, "provider/session-b");
    await peer.setThinking(actor.id, "low");

    expect(owner.actors.status(actor.id)).toMatchObject({
      model: "provider/session-a",
      thinking: "high",
      binding: {
        scope: "session",
        sessionId: "test",
        model: "provider/session-a",
        thinking: "high",
      },
      projectDefaults: {
        scope: "project",
        model: "provider/project-default",
        thinking: "medium",
      },
    });
    expect(peer.status(actor.id)).toMatchObject({
      model: "provider/session-b",
      thinking: "low",
      binding: {
        scope: "session",
        sessionId: "peer",
        model: "provider/session-b",
        thinking: "low",
      },
      projectDefaults: {
        scope: "project",
        model: "provider/project-default",
        thinking: "medium",
      },
    });
    expect(() => peer.tell(actor.id, "must route through the owner")).toThrow(
      "owned by another host",
    );

    expect(fs.readFileSync(registryPath, "utf8")).toBe(registryBeforeBindings);
    const bindingDirectory = path.join(owner.root, "actors", "bindings");
    const bindingFiles = fs.readdirSync(bindingDirectory);
    expect(bindingFiles).toHaveLength(2);
    if (process.platform !== "win32") {
      for (const file of bindingFiles) {
        expect(fs.statSync(path.join(bindingDirectory, file)).mode & 0o777).toBe(0o600);
      }
    }
    const registry = JSON.parse(
      fs.readFileSync(registryPath, "utf8"),
    ) as { actors: Array<{ id: string; model?: string; thinking?: string }> };
    expect(registry.actors.find((record) => record.id === actor.id)).toMatchObject({
      model: "provider/project-default",
      thinking: "medium",
    });
  });

  it("pins a queued activation before later session binding changes", async () => {
    const { actors, agents } = setup();
    const runSpy = vi.spyOn(agents, "run");
    const actor = await actors.create({
      name: "binding witness",
      instructions: "Report the pinned model.",
      model: "provider/project-default",
      thinking: "medium",
    });
    await actors.setModel(actor.id, "provider/session-old");
    await actors.setThinking(actor.id, "low");

    const first = actors.ask(actor.id, "first");
    await actors.setModel(actor.id, "provider/session-new");
    await actors.setThinking(actor.id, "high");
    await first;

    expect(runSpy.mock.calls[0]?.[0]).toMatchObject({
      model: "provider/session-old",
      thinking: "low",
    });
    await actors.ask(
      actor.id,
      "one-off",
      undefined,
      undefined,
      { overrides: { model: "provider/one-off", thinking: "xhigh" } },
    );
    expect(runSpy.mock.calls[1]?.[0]).toMatchObject({
      model: "provider/one-off",
      thinking: "xhigh",
    });
    expect(actors.status(actor.id)).toMatchObject({
      model: "provider/session-new",
      thinking: "high",
      projectDefaults: {
        model: "provider/project-default",
        thinking: "medium",
      },
    });
  });
  it("commits and records an exact capability view for every actor run", async () => {
    const release = vi.fn(async () => {});
    const acquire = vi.fn(async (
      _requirements: readonly FabricCapabilityRequirement[],
      _signal: AbortSignal,
    ): Promise<FabricCapabilityViewLease> => ({
      satisfied: true,
      missing: [],
      optionalMissing: ["optional.missing"],
      view: {
        id: "view-1",
        digest: "local-digest",
        semanticDigest: "semantic-digest",
        bindings: {
          "demo.echo": {
            ref: "demo.echo",
            provider: "demo",
            providerBindingId: "binding-1",
            generation: 1,
            descriptorHash: "descriptor-1",
          },
        },
      },
      release,
    }));
    const { actors } = setup(false, undefined, acquire);
    const actor = await actors.create({
      name: "bounded",
      instructions: "Use only committed capabilities.",
      requires: ["demo.echo", { ref: "optional.missing", optional: true }],
    });

    const reply = await actors.ask(actor.id, "Inspect");
    expect(reply.text).toBe("fake worker complete");
    expect(acquire).toHaveBeenCalledOnce();
    expect(acquire.mock.calls[0]?.[0]).toEqual([
      { ref: "demo.echo" },
      { ref: "optional.missing", optional: true },
    ]);
    expect(release).toHaveBeenCalledOnce();
    expect(actors.status(actor.id)).toMatchObject({
      requirements: [
        { ref: "demo.echo" },
        { ref: "optional.missing", optional: true },
      ],
      capabilityDigest: "semantic-digest",
    });
  });

  it("forwards durable actor follow-up capability requirements from a resident host", async () => {
    const acquire = vi.fn(async (
      _requirements: readonly FabricCapabilityRequirement[],
      _signal: AbortSignal,
    ): Promise<FabricCapabilityViewLease> => ({
      satisfied: true,
      missing: [],
      optionalMissing: [],
      view: {
        id: "view-durable",
        digest: "local-durable",
        semanticDigest: "semantic-durable",
        bindings: {
          "agents.followUp": {
            ref: "agents.followUp",
            provider: "agents",
            providerBindingId: "agents",
            generation: 1,
            descriptorHash: "follow-up",
          },
        },
      },
      release: async () => {},
    }));
    const opened = setup(true, undefined, acquire);
    const actor = await opened.actors.create({
      name: "durable follow-up",
      instructions: "Follow up with the remote participant.",
      requires: ["agents.followUp"],
    });
    await opened.actors.close();
    await opened.agents.close();

    const residentAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: path.join(opened.root, "resident-runs"),
    });
    agentManagers.push(residentAgents);
    const resident = new ActorManager(
      "test",
      opened.identity,
      opened.mesh,
      opened.meshConfig,
      residentAgents,
      () => {},
      { actorRoot: path.join(opened.root, "actors"), persistent: true },
    );
    actorManagers.push(resident);
    const run = vi.spyOn(residentAgents, "run");

    await expect(resident.ask(actor.id, "Send the remote follow-up.")).resolves.toMatchObject({
      text: "fake worker complete",
    });
    expect(run).toHaveBeenCalledWith(
      expect.objectContaining({ capabilityRequirements: ["agents.followUp"] }),
      expect.any(AbortSignal),
    );
  });

  it("keeps mailbox work queued until required capabilities become available", async () => {
    let available = false;
    const release = vi.fn(async () => {});
    const acquire = vi.fn(async (): Promise<FabricCapabilityViewLease> => available
      ? {
          satisfied: true,
          missing: [],
          optionalMissing: [],
          view: {
            id: "view-ready",
            digest: "local-ready",
            semanticDigest: "semantic-ready",
            bindings: {
              "demo.echo": {
                ref: "demo.echo",
                provider: "demo",
                providerBindingId: "binding-ready",
                generation: 1,
                descriptorHash: "descriptor-ready",
              },
            },
          },
          release,
        }
      : {
          satisfied: false,
          missing: ["demo.echo"],
          optionalMissing: [],
          release,
        });
    const { actors } = setup(false, undefined, acquire);
    const actor = await actors.create({
      name: "waiting",
      instructions: "Wait for the exact capability.",
      requires: ["demo.echo"],
    });

    const pending = actors.ask(actor.id, "Inspect later");
    await waitFor(() => actors.status(actor.id).missingCapabilities?.[0] === "demo.echo");
    expect(actors.status(actor.id)).toMatchObject({ status: "queued", queued: 1 });
    expect(release).toHaveBeenCalledOnce();

    available = true;
    actors.retryCapabilityWaiters();
    await expect(pending).resolves.toMatchObject({ text: "fake worker complete" });
    expect(acquire).toHaveBeenCalledTimes(2);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.status(actor.id)).toMatchObject({
      status: "idle",
      queued: 0,
      capabilityDigest: "semantic-ready",
    });
    expect(actors.status(actor.id).missingCapabilities).toBeUndefined();
    expect(release).toHaveBeenCalledTimes(2);
  });

  it("returns a cancelled capability-blocked ask to idle", async () => {
    const release = vi.fn(async () => {});
    const acquire = vi.fn(async (): Promise<FabricCapabilityViewLease> => ({
      satisfied: false,
      missing: ["demo.echo"],
      optionalMissing: [],
      release,
    }));
    const { actors } = setup(false, undefined, acquire);
    const actor = await actors.create({
      name: "cancelled waiter",
      instructions: "Wait for the exact capability.",
      requires: ["demo.echo"],
    });
    const controller = new AbortController();
    const pending = actors.ask(actor.id, "Inspect later", undefined, controller.signal);
    await waitFor(() => actors.status(actor.id).missingCapabilities?.[0] === "demo.echo");

    controller.abort();

    await expect(pending).rejects.toThrow("request cancelled");
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(actors.status(actor.id).missingCapabilities).toBeUndefined();
  });

  it("does not claim a durable actor from another root", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "root-bound",
      instructions: "Remain with the creating root.",
      residency: "durable",
    });
    const otherIdentity: MeshIdentity = {
      id: "session:other-root",
      name: "main",
      kind: "main",
      sessionId: "other-root",
    };
    const other = new ActorManager(
      "other-root",
      otherIdentity,
      state.mesh,
      state.meshConfig,
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        claimResidency: "durable",
        rootId: otherIdentity.id,
      },
    );
    actorManagers.push(other);

    expect(other.status(actor.id).rootId).toBe(state.identity.id);
    expect(other.owns(actor.id)).toBe(false);
  });

  it("adopts orphaned project actors after the creating root is gone", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "orphaned",
      instructions: "Survive the creating session.",
      events: ["agent_settled"],
      residency: "session",
    });
    expect(actor.rootId).toBe(state.identity.id);
    await state.actors.close();

    // No live participant-directory owner (undefined) and a dead lineage root:
    // a Main-shaped manager with claimResidency + directory hooks must adopt
    // through the fenced registry write and rebind rootId.
    const successorIdentity: MeshIdentity = {
      id: "session:successor",
      name: "main",
      kind: "main",
      sessionId: "successor",
    };
    const liveOwners = new Map<string, string>();
    const liveRoots = new Set<string>();
    const successor = new ActorManager(
      "successor",
      successorIdentity,
      state.mesh,
      state.meshConfig,
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        claimResidency: "session",
        rootId: successorIdentity.id,
        canManageActor: (id) => {
          const owner = liveOwners.get(id);
          if (!owner) return undefined;
          return owner === successorIdentity.id;
        },
        lineageAlive: (rootId) => liveRoots.has(rootId),
      },
    );
    actorManagers.push(successor);

    // Adoption completes through the fenced registry write; ownership only
    // holds once the rebound rootId is persisted.
    await waitFor(() => successor.owns(actor.id));
    expect(successor.status(actor.id).rootId).toBe(successorIdentity.id);
    expect(successor.tell(actor.id, "run after orphan takeover")).toMatchObject({
      queued: true,
    });
    await successor.setModel(actor.id, "provider/after-takeover", "project");
    expect(successor.status(actor.id).model).toBe("provider/after-takeover");

    // create/import must not be bricked by the orphaned row
    const imported = await successor.create({
      name: "fresh",
      instructions: "Created after orphan takeover.",
      residency: "session",
    });
    expect(imported.rootId).toBe(successorIdentity.id);

    const registry = JSON.parse(
      fs.readFileSync(path.join(state.root, "actors", "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string; rootId: string; model?: string; adoptedAt?: number }> };
    const saved = registry.actors.find((row) => row.id === actor.id);
    expect(saved?.rootId).toBe(successorIdentity.id);
    expect(saved?.model).toBe("provider/after-takeover");
    expect(typeof saved?.adoptedAt).toBe("number");
  });

  it("still refuses takeover while another host live-owns the actor", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "live-owned",
      instructions: "Stay with the live owner.",
      residency: "session",
    });
    const peerIdentity: MeshIdentity = {
      id: "session:peer",
      name: "main",
      kind: "main",
      sessionId: "peer",
    };
    const peer = new ActorManager(
      "peer",
      peerIdentity,
      state.mesh,
      state.meshConfig,
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        claimResidency: "session",
        rootId: peerIdentity.id,
        // Live foreign owner advertised by the participant directory.
        canManageActor: () => false,
      },
    );
    actorManagers.push(peer);

    expect(peer.owns(actor.id)).toBe(false);
    expect(() => peer.tell(actor.id, "blocked")).toThrow("owned by another host");
    await expect(
      peer.create({ name: "blocked-create", instructions: "Should fail." }),
    ).rejects.toThrow(ActorRegistryOwnershipError);
  });

  it("creates via the registry-owner channel while a live foreign actor holds the registry", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "foreign-live-owner-channel",
      instructions: "Owned elsewhere.",
      residency: "session",
    });
    const peerIdentity: MeshIdentity = {
      id: "session:owner-channel",
      name: "main",
      kind: "main",
      sessionId: "owner-channel",
    };
    const peer = new ActorManager(
      "owner-channel",
      peerIdentity,
      state.mesh,
      state.meshConfig,
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        claimResidency: "session",
        rootId: peerIdentity.id,
        // Live foreign owner advertised by the participant directory.
        canManageActor: () => false,
      },
    );
    actorManagers.push(peer);

    // Plain callers stay guarded.
    await expect(
      peer.create({ name: "blocked", instructions: "Guard stays for plain callers." }),
    ).rejects.toThrow("registry is owned by another host");
    // The bypass cannot be used for a session-scoped actor.
    await expect(
      peer.create(
        { name: "blocked-session", instructions: "Keep session creation guarded." },
        { asRegistryOwner: true },
      ),
    ).rejects.toThrow(ActorRegistryOwnershipError);
    // The resident host control channel creates a durable as registry owner.
    const created = await peer.create(
      {
        name: "via-owner-channel",
        instructions: "Created as the registry owner.",
        residency: "durable",
      },
      { asRegistryOwner: true },
    );
    expect(created.name).toBe("via-owner-channel");
    expect(created.id).toBeTruthy();
  });

  it("settles exactly one adopter when concurrent starters race an orphan", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "raced",
      instructions: "Only one host may win.",
      residency: "session",
    });
    await state.actors.close();

    // Worst case: neither starter ever sees the other in the directory.
    // Ownership must still converge through the fenced registry write alone.
    const makeRacer = (name: string) => {
      const identity: MeshIdentity = {
        id: `session:${name}`,
        name: "main",
        kind: "main",
        sessionId: name,
      };
      const manager = new ActorManager(
        name,
        identity,
        state.mesh,
        state.meshConfig,
        state.agents,
        () => {},
        {
          actorRoot: path.join(state.root, "actors"),
          persistent: true,
          claimResidency: "session",
          rootId: identity.id,
          canManageActor: () => undefined,
          lineageAlive: () => false,
        },
      );
      actorManagers.push(manager);
      return { manager, identity };
    };
    const first = makeRacer("racer-a");
    const second = makeRacer("racer-b");

    await waitFor(
      () => [first, second].filter(({ manager }) => manager.owns(actor.id)).length === 1,
    );
    const winner = first.manager.owns(actor.id) ? first : second;
    const loser = winner === first ? second : first;

    // The loser resyncs to the winner's persisted lineage and stays passive.
    await waitFor(() => loser.manager.status(actor.id).rootId === winner.identity.id);
    await new Promise((resolve) => setTimeout(resolve, 120));
    expect([first, second].filter(({ manager }) => manager.owns(actor.id))).toHaveLength(1);
    expect(() => loser.manager.tell(actor.id, "not yours")).toThrow("owned by another host");

    const registry = JSON.parse(
      fs.readFileSync(path.join(state.root, "actors", "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string; rootId: string; adoptedAt?: number }> };
    const rows = registry.actors.filter((row) => row.id === actor.id);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.rootId).toBe(winner.identity.id);
    expect(typeof rows[0]?.adoptedAt).toBe("number");
  });

  it("refuses a late starter while the adopter's lineage root is live", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "claimed",
      instructions: "Stay with the live adopter.",
      residency: "session",
    });
    await state.actors.close();

    const liveRoots = new Set<string>();
    const makeRacer = (name: string) => {
      const identity: MeshIdentity = {
        id: `session:${name}`,
        name: "main",
        kind: "main",
        sessionId: name,
      };
      const manager = new ActorManager(
        name,
        identity,
        state.mesh,
        state.meshConfig,
        state.agents,
        () => {},
        {
          actorRoot: path.join(state.root, "actors"),
          persistent: true,
          claimResidency: "session",
          rootId: identity.id,
          canManageActor: () => undefined,
          lineageAlive: (rootId) => liveRoots.has(rootId),
        },
      );
      actorManagers.push(manager);
      return { manager, identity };
    };
    const winner = makeRacer("racer-live-a");
    await waitFor(() => winner.manager.owns(actor.id));

    // The adopter advertises a live lineage root; a later starter loading the
    // rebound registry must not attempt adoption at all.
    liveRoots.add(winner.identity.id);
    const late = makeRacer("racer-live-b");
    await new Promise((resolve) => setTimeout(resolve, 120));
    expect(late.manager.owns(actor.id)).toBe(false);
    expect(() => late.manager.tell(actor.id, "not yours")).toThrow("owned by another host");

    const registry = JSON.parse(
      fs.readFileSync(path.join(state.root, "actors", "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string; rootId: string }> };
    expect(registry.actors.find((row) => row.id === actor.id)?.rootId).toBe(winner.identity.id);
  });

  it("adopts durable orphans for a resident host while Main stays create-guarded", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "durable-orphan",
      instructions: "Belong to the resident host.",
      events: ["agent_settled"],
      residency: "durable",
    });
    await state.actors.close();

    const makeSuccessor = (name: string, claimResidency: "session" | "durable") => {
      const identity: MeshIdentity = {
        id: `session:${name}`,
        name: "main",
        kind: "main",
        sessionId: name,
      };
      const manager = new ActorManager(
        name,
        identity,
        state.mesh,
        state.meshConfig,
        state.agents,
        () => {},
        {
          actorRoot: path.join(state.root, "actors"),
          persistent: true,
          claimResidency,
          rootId: identity.id,
          canManageActor: () => undefined,
          lineageAlive: () => false,
        },
      );
      actorManagers.push(manager);
      return { manager, identity };
    };

    // Main's claim never matches a durable row: no adoption, and with no
    // manageable rows at all the create/import guard stays up until the
    // resident host takes over.
    const main = makeSuccessor("main-successor", "session");
    await new Promise((resolve) => setTimeout(resolve, 120));
    expect(main.manager.owns(actor.id)).toBe(false);
    await expect(
      main.manager.create({ name: "blocked", instructions: "No shared rows are manageable." }),
    ).rejects.toThrow("registry is owned by another host");

    const resident = makeSuccessor("resident-successor", "durable");
    await waitFor(() => resident.manager.owns(actor.id));
    expect(resident.manager.status(actor.id).rootId).toBe(resident.identity.id);
    expect(main.manager.owns(actor.id)).toBe(false);
    const registry = JSON.parse(
      fs.readFileSync(path.join(state.root, "actors", "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string; rootId: string }> };
    expect(registry.actors.find((row) => row.id === actor.id)?.rootId).toBe(resident.identity.id);
  });

  it("re-adopts once a dead adopter's grace window has elapsed", async () => {
    const state = setup(true);
    const actor = await state.actors.create({
      name: "rehome",
      instructions: "Accept a new root after the previous adopter dies.",
      residency: "session",
    });
    await state.actors.close();

    const makeRacer = (name: string) => {
      const identity: MeshIdentity = {
        id: `session:${name}`,
        name: "main",
        kind: "main",
        sessionId: name,
      };
      const manager = new ActorManager(
        name,
        identity,
        state.mesh,
        state.meshConfig,
        state.agents,
        () => {},
        {
          actorRoot: path.join(state.root, "actors"),
          persistent: true,
          claimResidency: "session",
          rootId: identity.id,
          canManageActor: () => undefined,
          lineageAlive: () => false,
          adoptionGraceMs: 50,
        },
      );
      actorManagers.push(manager);
      return { manager, identity };
    };
    const first = makeRacer("racer-grace-a");
    await waitFor(() => first.manager.owns(actor.id));
    await first.manager.close();

    // The first adopter vanished before any directory advertisement healed;
    // after the grace window the next starter re-adopts the lineage.
    await new Promise((resolve) => setTimeout(resolve, 100));
    const second = makeRacer("racer-grace-b");
    await waitFor(() => second.manager.owns(actor.id));
    expect(second.manager.status(actor.id).rootId).toBe(second.identity.id);
  });

  it("preserves current remote actor records when saving a locally owned actor", async () => {
    let localId: string | undefined;
    const state = setup(true, (id) => localId === undefined || id === localId);
    const local = await state.actors.create({
      name: "local actor",
      instructions: "Local instructions.",
    });
    await state.actors.create({
      name: "remote actor",
      instructions: "Initial remote instructions.",
    });
    localId = local.id;
    const registryPath = path.join(state.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryPath, "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    const remote = registry.actors.find((actor) => actor.id !== local.id);
    expect(remote).toBeDefined();
    const remoteId = String(remote!.id);
    remote!.instructions = "Updated by remote owner.";
    remote!.messages = [
      {
        id: "message:remote",
        actorId: remoteId,
        actorName: "remote actor",
        direction: "out",
        source: "direct",
        createdAt: Date.now(),
        text: "Fresh remote reply.",
      },
    ];
    fs.writeFileSync(registryPath, JSON.stringify(registry, null, 2));

    await state.actors.setModel(local.id, "provider/local", "project");

    const saved = JSON.parse(fs.readFileSync(registryPath, "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    expect(saved.actors.find((actor) => actor.id !== local.id)?.instructions).toBe(
      "Updated by remote owner.",
    );
    expect(state.actors.instructions(remoteId)).toBe("Updated by remote owner.");
    expect(state.actors.messages(remoteId)).toMatchObject([
      { id: "message:remote", text: "Fresh remote reply." },
    ]);
  });

  it("discovers the first actor created after an empty standby starts", async () => {
    let ownerOwns = true;
    let standbyOwns = false;
    const state = setup(true, () => ownerOwns);
    const standbyIdentity: MeshIdentity = {
      id: "session:standby",
      name: "main",
      kind: "main",
      sessionId: "standby",
    };
    const standby = new ActorManager(
      "standby",
      standbyIdentity,
      state.mesh,
      state.meshConfig,
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        canManageActor: () => standbyOwns,
      },
    );
    actorManagers.push(standby);
    expect(standby.list()).toEqual([]);

    const created = await state.actors.create({
      name: "late actor",
      instructions: "Persist after standby startup.",
    });
    await waitFor(() => standby.list().some((actor) => actor.id === created.id));

    ownerOwns = false;
    standbyOwns = true;
    expect(standby.tell(created.id, "continue after takeover")).toMatchObject({
      queued: true,
    });
  });

  it("notifies and releases actor state subscribers", async () => {
    const { actors } = setup();
    const listener = vi.fn();
    const unsubscribe = actors.subscribe(listener);
    const actor = await actors.create({ name: "observer", instructions: "Observe." });
    expect(listener).toHaveBeenCalled();

    const beforeUpdate = listener.mock.calls.length;
    await actors.setModel(actor.id, "provider/model");
    expect(listener.mock.calls.length).toBeGreaterThan(beforeUpdate);

    unsubscribe();
    const beforeUnsubscribedUpdate = listener.mock.calls.length;
    await actors.setThinking(actor.id, "high");
    expect(listener).toHaveBeenCalledTimes(beforeUnsubscribedUpdate);
  });

  it("keeps a persistent actor identity and processes direct mailbox messages", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });

    const reply = await actors.ask(actor.id, "Inspect auth");
    expect(reply.text).toBe("fake worker complete");
    expect(reply.actorId).toBe(actor.id);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", messages: 2 });
    expect(agents.list()).toEqual([]);
    expect(actors.messages(actor.id)).toMatchObject([
      { direction: "in", source: "direct" },
      { direction: "out", source: "direct", text: "fake worker complete" },
    ]);
  });

  it("bounds combined actor text and structured data to one mesh envelope", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({
      name: "large reporter",
      instructions: "Return LARGE_RESULT when asked.",
      responseMode: "text",
    });

    const reply = await actors.ask(actor.id, "LARGE_RESULT");

    expect(Buffer.byteLength(JSON.stringify(reply), "utf8")).toBeLessThanOrEqual(
      mesh.maxEventBytes - 4_096,
    );
    expect(reply.text).toContain("[actor message truncated]");
    expect(reply.data).toMatchObject({ fabricTruncated: true, originalBytes: 100_013 });
    expect(mesh.read({ topic: "fabric.actor.output", limit: 10 })).toHaveLength(1);
  });

  it("delivers schema-validated actor directives through the fixed policy", async () => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "advisor",
      instructions: "Advise only when useful.",
      responseMode: "directive",
      delivery: "steer",
      triggerTurn: false,
    });

    const reply = await actors.ask(actor.id, "Review this turn");
    expect(reply).toMatchObject({
      action: "message",
      text: "fake actor advice",
    });
    expect(deliveries).toEqual(["fake actor advice"]);
  });

  it("requires explicit active delivery intent and rejects impossible trigger policies", async () => {
    const { actors } = setup();

    await expect(
      actors.create({
        name: "ambiguous",
        instructions: "Advise.",
        delivery: "steer",
      }),
    ).rejects.toThrow(/requires explicit triggerTurn/);
    await expect(
      actors.create({
        name: "impossible",
        instructions: "Advise.",
        delivery: "nextTurn",
        triggerTurn: true,
      }),
    ).rejects.toThrow(/never starts Main/);

    const actor = await actors.create({ name: "mailbox", instructions: "Advise." });
    expect(actor).toMatchObject({ delivery: "mailbox", triggerTurn: false });
  });

  it("updates a live actor delivery policy without recreating its history", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "advisor", instructions: "Advise." });
    await actors.tell(actor.id, "remember this");
    await waitFor(() => actors.status(actor.id).status === "idle");
    const messages = actors.status(actor.id).messages;

    await expect(actors.setDeliveryPolicy(actor.id, "mailbox", true)).rejects.toThrow(
      /never starts Main/,
    );
    const active = await actors.setDeliveryPolicy(actor.id, "followUp", true);
    expect(active).toMatchObject({ delivery: "followUp", triggerTurn: true, messages });
    const passive = await actors.setDeliveryPolicy(actor.id, "steer", false);
    expect(passive).toMatchObject({ delivery: "steer", triggerTurn: false, messages });
  });

  // smarty-dev#390: activation-context supervisors failed every activation silently for an hour.
  it("tells the owner's Main once when an actor keeps failing, and again after it recovers and fails", async () => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "supervisor",
      instructions: "Watch and steer only when needed.",
      responseMode: "directive",
      delivery: "mailbox",
      triggerTurn: false,
    });
    const notices = () => deliveries.filter((text) => text.startsWith("Fabric host notice:"));
    for (let run = 1; run < ACTOR_FAILURE_NOTICE_AFTER; run++) await actors.ask(actor.id, "FAIL_DIRECTIVE");
    expect(notices()).toEqual([]);
    await actors.ask(actor.id, "FAIL_DIRECTIVE");
    expect(notices()).toEqual([expect.stringContaining(`actor supervisor failed its last ${ACTOR_FAILURE_NOTICE_AFTER} activations`)]);
    expect(notices()[0]).toContain("Structured agent output was invalid");
    await actors.ask(actor.id, "FAIL_DIRECTIVE");
    expect(notices()).toHaveLength(1);                       // once per streak
    await actors.ask(actor.id, "all good");                  // a completed run ends the streak
    for (let run = 0; run < ACTOR_FAILURE_NOTICE_AFTER; run++) await actors.ask(actor.id, "FAIL_DIRECTIVE");
    expect(notices()).toHaveLength(2);
  }, 60_000);

  // dev-lead review of #34: interrupted activations (ESC) are not failures, and no notice
  // may start a turn while the halt holds.
  it("does not count interrupted activations toward the owner notice", async () => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "supervisor",
      instructions: "Watch and steer only when needed.",
      responseMode: "directive",
      delivery: "mailbox",
      triggerTurn: false,
    });
    for (let run = 0; run < ACTOR_FAILURE_NOTICE_AFTER + 1; run++) {
      const asked = actors.ask(actor.id, "HANG").catch(() => undefined);
      await waitFor(() => actors.status(actor.id).status === "running");
      actors.haltAll();
      await asked;
      await waitFor(() => actors.status(actor.id).status === "idle");
      actors.dispatchHostEvent("input", {});                   // the user resumes
    }
    expect(deliveries.filter((text) => text.startsWith("Fabric host notice:"))).toEqual([]);
  }, 60_000);

  // review/astra on 810b557: in text mode (the default) a stopped run throws into the catch path.
  it("does not count a text actor's run stopped through agents.stop() as the third failure", async () => {
    const { actors, agents, deliveries } = setup();
    const actor = await actors.create({
      name: "watcher",
      instructions: "Answer briefly.",
      delivery: "mailbox",
      triggerTurn: false,
    });
    for (let run = 1; run < ACTOR_FAILURE_NOTICE_AFTER; run++) {
      await expect(actors.ask(actor.id, "FAIL_DIRECTIVE")).rejects.toThrow();
    }
    const asked = actors.ask(actor.id, "HANG").catch((error: unknown) => error);
    const hanging = () => agents.list().find((run) => run.status === "running" && String((run as { task?: string }).task).includes("HANG"));
    await waitFor(() => hanging() !== undefined);
    const hung = hanging()!;
    await agents.stop(hung.id);
    expect(String(await asked)).toMatch(/stopped/i);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(deliveries.filter((text) => text.startsWith("Fabric host notice:"))).toEqual([]);
    await expect(actors.ask(actor.id, "FAIL_DIRECTIVE")).rejects.toThrow(); // a real third failure still alarms
    await waitFor(() => deliveries.some((text) => text.startsWith("Fabric host notice:")));
  }, 60_000);

  it.each(["stop", "close"] as const)("does not count an activation cut off by %s as the third failure", async (how) => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "supervisor",
      instructions: "Watch and steer only when needed.",
      responseMode: "directive",
      delivery: "mailbox",
      triggerTurn: false,
    });
    for (let run = 1; run < ACTOR_FAILURE_NOTICE_AFTER; run++) await actors.ask(actor.id, "FAIL_DIRECTIVE");
    const asked = actors.ask(actor.id, "HANG").catch(() => undefined);
    await waitFor(() => actors.status(actor.id).status === "running");
    if (how === "stop") await actors.stop(actor.id);
    else await actors.close();
    await asked;
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(deliveries.filter((text) => text.startsWith("Fabric host notice:"))).toEqual([]);
  }, 60_000);

  // review/astra on #34: a completed run with an invalid directive is a failure too.
  it("counts completed runs whose directive is invalid toward the owner notice", async () => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "supervisor",
      instructions: "Watch and steer only when needed.",
      responseMode: "directive",
      delivery: "mailbox",
      triggerTurn: false,
    });
    const notices = () => deliveries.filter((text) => text.startsWith("Fabric host notice:"));
    for (let run = 0; run < ACTOR_FAILURE_NOTICE_AFTER; run++) {
      await actors.ask(actor.id, "EMPTY_MESSAGE_DIRECTIVE").catch(() => undefined);
    }
    expect(notices()).toEqual([expect.stringContaining("missing message text")]);
    await actors.ask(actor.id, "all good");                  // a valid message still ends the streak
    for (let run = 0; run < ACTOR_FAILURE_NOTICE_AFTER; run++) {
      await actors.ask(actor.id, "EMPTY_MESSAGE_DIRECTIVE").catch(() => undefined);
    }
    expect(notices()).toHaveLength(2);
  }, 60_000);

  it("stays ambient and retains the failed run when a directive run fails", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({
      name: "supervisor",
      instructions: "Watch and steer only when needed.",
      responseMode: "directive",
      delivery: "steer",
      triggerTurn: false,
    });

    const reply = await actors.ask(actor.id, "FAIL_DIRECTIVE");
    expect(reply).toMatchObject({ action: "silent" });
    expect((reply.data as { runError: string }).runError).toContain(
      "Structured agent output was invalid",
    );

    await waitFor(() => actors.status(actor.id).status === "idle");
    const status = actors.status(actor.id);
    expect(status).toMatchObject({ status: "idle" });
    expect(status.lastError).toBeUndefined();

    // The failed run is retained for debugging (agents.status(lastRunId)), not cleaned up.
    const retained = agents.list();
    expect(retained).toHaveLength(1);
    const run = agents.status(retained[0]!.id);
    expect(run).toMatchObject({
      status: "failed",
      error: expect.stringContaining("Structured agent output was invalid"),
    });

    // Removing the actor releases the retained run.
    await actors.remove(actor.id);
    expect(agents.list()).toEqual([]);
  });

  it("restores persistent ambient actors for the same Pi session", async () => {
    const setupState = setup(true);
    const actor = await setupState.actors.create({
      name: "supervisor",
      instructions: "Watch until the goal is complete.",
      events: ["agent_settled"],
      responseMode: "directive",
    });
    await setupState.actors.close();
    actorManagers.splice(actorManagers.indexOf(setupState.actors), 1);

    const restored = new ActorManager(
      "test",
      setupState.identity,
      setupState.mesh,
      setupState.meshConfig,
      setupState.agents,
      () => {},
      { actorRoot: path.join(setupState.root, "actors"), persistent: true },
    );
    actorManagers.push(restored);

    expect(restored.status(actor.id)).toMatchObject({
      id: actor.id,
      name: "supervisor",
      status: "idle",
      events: ["agent_settled"],
    });
  });

  it("resumes a Claude Code session after a persistent actor is restored", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-actor-"));
    roots.push(root);
    const invocationLog = path.join(root, "claude-args.jsonl");
    process.env.FAKE_CLAUDE_LOG = invocationLog;
    try {
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
      const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
        runRoot: path.join(root, "runs"),
      });
      agentManagers.push(agents);
      const identity: MeshIdentity = {
        id: "session:test",
        name: "main",
        kind: "main",
        sessionId: "test",
      };
      const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
      const actorRoot = path.join(root, "actors");
      const first = new ActorManager(
        "test",
        identity,
        mesh,
        meshConfig,
        agents,
        () => {},
        { actorRoot, persistent: true },
      );
      actorManagers.push(first);
      const actor = await first.create({
        name: "claude-reviewer",
        instructions: "Review each mailbox item.",
        runner: "claude",
        tools: ["read"],
      });

      const firstReply = await first.ask(actor.id, "first message");
      expect(firstReply.text).toContain("fake claude complete");
      await waitFor(() => first.status(actor.id).status === "idle");
      expect(first.status(actor.id)).toMatchObject({ runner: "claude", status: "idle" });
      await first.close();
      actorManagers.splice(actorManagers.indexOf(first), 1);

      const restored = new ActorManager(
        "test",
        identity,
        mesh,
        meshConfig,
        agents,
        () => {},
        { actorRoot, persistent: true },
      );
      actorManagers.push(restored);
      expect(restored.status(actor.id)).toMatchObject({ runner: "claude", status: "idle" });
      const secondReply = await restored.ask(actor.id, "second message");
      expect(secondReply.text).toContain("fake claude complete");

      const invocations = fs
        .readFileSync(invocationLog, "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { argv: string[] });
      expect(invocations).toHaveLength(2);
      expect(invocations[0]!.argv).not.toContain("--resume");
      const resumeAt = invocations[1]!.argv.indexOf("--resume");
      expect(invocations[1]!.argv[resumeAt + 1]).toBe(
        "11111111-1111-4111-8111-111111111111",
      );
      expect(invocations[0]!.argv).not.toContain("--no-session-persistence");
      expect(restored.readLog(actor.id).session.filter((line) => line.parsed)).not.toHaveLength(0);
    } finally {
      delete process.env.FAKE_CLAUDE_LOG;
    }
  });

  it("restores project-scoped actors across different Pi sessions", async () => {
    // Project scope stores actors at a shared root (no sessionId segment), so a
    // new Pi session that points at the same root picks up the roster without
    // redefining actors.
    const scopeDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-scope-"));
    roots.push(scopeDir);
    const sharedRoot = path.join(scopeDir, "actors");
    const firstMesh = new MeshStore(path.join(scopeDir, "mesh"), 64 * 1024, 100);
    const firstAgents = new AgentManager(
      process.cwd(),
      DEFAULT_FABRIC_CONFIG.agents,
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(scopeDir, "runs") },
    );
    agentManagers.push(firstAgents);
    const first = new ActorManager(
      "session-a",
      { id: "session:a", name: "main", kind: "main", sessionId: "session-a" },
      firstMesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      firstAgents,
      () => {},
      { actorRoot: sharedRoot, persistent: true },
    );
    actorManagers.push(first);
    const actor = await first.create({
      name: "advisor",
      instructions: "Watch until the goal is complete.",
      responseMode: "directive",
    });
    await first.close();
    actorManagers.splice(actorManagers.indexOf(first), 1);

    // A brand-new Pi session, same shared actor root.
    const secondMesh = new MeshStore(path.join(scopeDir, "mesh"), 64 * 1024, 100);
    const secondAgents = new AgentManager(
      process.cwd(),
      DEFAULT_FABRIC_CONFIG.agents,
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(scopeDir, "runs") },
    );
    agentManagers.push(secondAgents);
    const restored = new ActorManager(
      "session-b",
      { id: "session:b", name: "main", kind: "main", sessionId: "session-b" },
      secondMesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      secondAgents,
      () => {},
      { actorRoot: sharedRoot, persistent: true },
    );
    actorManagers.push(restored);

    expect(restored.status(actor.id)).toMatchObject({
      id: actor.id,
      name: "advisor",
      status: "idle",
    });
  });

  // smarty-dev#442: an ownership flicker dropped about 20 queued PR events of a busy review
  // actor. Queued mesh events (no caller) must survive it and run once ownership returns.
  it.each([false, true])("keeps queued mesh events across an ownership flicker (persistent %s)", async (persistent) => {
    let owned = true;
    const { actors, mesh } = setup(persistent, () => owned);
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review each event.",
      topics: ["team.pulls"],
      responseMode: "text",
    });
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS event-1" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await mesh.publish({ topic: "team.pulls", from, text: "event-2" });
    await mesh.publish({ topic: "team.pulls", from, text: "event-3" });
    await waitFor(() => actors.status(actor.id).queued === 2);
    actors.listOwned();                                            // ownership observed as true

    owned = false;
    actors.listOwned();                                            // observe the loss: park, abort the run
    expect(actors.status(actor.id).queued).toBe(0);
    owned = true;
    actors.listOwned();                                            // observe the return: queued events come back

    const ran = () => actors.messages(actor.id)
      .filter((message) => message.direction === "out" && message.runId)
      .map((message) => String((message as { text?: string }).text ?? "") + " " + message.source);
    await waitFor(() => ran().length >= 3, 20_000);
    const inbound = actors.messages(actor.id).filter((message) => message.direction === "in").length;
    expect(inbound).toBe(3);
    expect(actors.messages(actor.id).filter((message) => message.error?.startsWith("Dropped a queued event"))).toEqual([]);
  }, 30_000);

  // Records every actor activation (the manager cleans completed runs out of agents.list()).
  // hold, when given, delays each run's result until it resolves (a deferred result).

  // Astra review of #36, R1: a run that completes while ownership is elsewhere is recorded,
  // never parked and run a second time.
  it("records, and does not rerun, an event whose run completed while ownership was away", async () => {
    let owned = true;
    const { actors, mesh, agents } = setup(false, () => owned);
    const runs = recordRuns(agents);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    actors.listOwned();
    await mesh.publish({ topic: "team.pulls", from: { id: "peer", name: "peer", kind: "actor" }, text: "LIVE_WITH_PROGRESS once" });
    // The manager has seen the run's progress, so an abort detaches it and it completes.
    await waitFor(() => agents.list().some((run) => ((run as { turns?: number }).turns ?? 0) > 0));
    owned = false;
    actors.listOwned();                                        // the caller aborts; the progressing run is detached
    await waitFor(() => actors.messages(actor.id).some((message) => message.error?.startsWith("Dropped a queued event")), 10_000);
    owned = true;
    actors.listOwned();
    await new Promise((resolve) => setTimeout(resolve, 2_500));
    expect(runs.filter((run) => run.task.includes("LIVE_WITH_PROGRESS once"))).toHaveLength(1);
  }, 30_000);

  // Astra review of #36, R2: ESC cancels parked events too, even with their restore already scheduled.
  it("cancels parked events on an interrupt that arrives before their restore runs", async () => {
    let owned = true;
    const { actors, mesh, agents } = setup(false, () => owned);
    const runs = recordRuns(agents);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    actors.listOwned();
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    // HANG has no progress, so the ownership loss always aborts it (a progressing run detaches).
    await mesh.publish({ topic: "team.pulls", from, text: "HANG first" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await mesh.publish({ topic: "team.pulls", from, text: "held-2" });
    await mesh.publish({ topic: "team.pulls", from, text: "held-3" });
    await waitFor(() => actors.status(actor.id).queued === 2);
    owned = false;
    actors.listOwned();                                        // held-2 and held-3 park
    owned = true;
    actors.listOwned();                                        // their restore is scheduled ...
    actors.haltAll();                                          // ... and ESC arrives first
    await new Promise((resolve) => setTimeout(resolve, 2_500));
    expect(runs.filter((run) => run.task.includes("held-"))).toEqual([]);
    // Both parked events, and the in-flight one the ownership loss aborted, are recorded dropped.
    expect(actors.messages(actor.id).filter((message) => message.error?.includes("halted by user interrupt"))).toHaveLength(3);
  }, 30_000);

  // Astra review of #36, R2 follow-up: ESC after an ownership loss cancels the aborted in-flight
  // event; the next input must not run it again.
  it("drops, and never reruns, an in-flight event that an ownership loss aborted and ESC cancelled", async () => {
    let owned = true;
    const { actors, mesh, agents } = setup(false, () => owned);
    const runs = recordRuns(agents);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    actors.listOwned();
    await mesh.publish({ topic: "team.pulls", from: { id: "peer", name: "peer", kind: "actor" }, text: "HANG cancel-me" });
    await waitFor(() => actors.status(actor.id).status === "running");
    owned = false;
    actors.listOwned();                                        // ownership loss aborts the run
    actors.haltAll();                                          // ESC before the aborted run returns
    await waitFor(() => actors.messages(actor.id).some((message) => message.error?.includes("halted by user interrupt")), 10_000);
    owned = true;
    actors.listOwned();
    actors.dispatchHostEvent("input", {});                     // the user resumes
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(runs.filter((run) => run.task.includes("cancel-me"))).toHaveLength(1);
  }, 30_000);

  // review/astra on 7b47958, R2 and R4 across a persistent reload: the old drain's deferred
  // result arrives after ownership returned (a new actor object) and after ESC.
  it("cancels, and records on the live actor, a deferred in-flight event across a persistent reload", async () => {
    let owned = true;
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    const { actors, mesh, agents } = setup(true, () => owned);
    const runs = recordRuns(agents, () => held);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    actors.listOwned();
    await mesh.publish({ topic: "team.pulls", from: { id: "peer", name: "peer", kind: "actor" }, text: "HANG deferred" });
    await waitFor(() => actors.status(actor.id).status === "running");
    owned = false;
    actors.listOwned();                                        // aborts the run; its result is held back
    owned = true;
    actors.listOwned();                                        // persistent reload: a new actor object
    actors.haltAll();                                          // ESC while the old drain still waits
    release();
    await waitFor(() => actors.messages(actor.id).some((message) => message.error?.includes("halted by user interrupt")), 10_000);
    actors.dispatchHostEvent("input", {});                     // the user resumes
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(runs.filter((run) => run.task.includes("HANG deferred"))).toHaveLength(1);
  }, 30_000);

  // review/astra on ffc39e8, R5: restored events keep their freshness across a persistent reload.
  it("runs the latest restored event, and keeps older ones stale, across a persistent ownership flip", async () => {
    let owned = true;
    const { actors, mesh, agents } = setup(true, () => owned);
    const runs = recordRuns(agents);
    const actor = await actors.create({
      name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text",
      validWhile: { version: 1, source: "({ activation, current }) => activation.sequence === current.latestActivationSequence" },
    });
    actors.listOwned();
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS older" });
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS newest" });
    await waitFor(() => actors.status(actor.id).queued === 2);
    owned = false;
    actors.listOwned();                                        // both queued events park
    owned = true;
    actors.listOwned();                                        // persistent reload: a new actor object
    await waitFor(() => runs.some((run) => run.task.includes("newest") && run.finishedAt !== undefined), 15_000);
    expect(runs.filter((run) => run.task.includes("newest"))).toHaveLength(1);
    expect(runs.filter((run) => run.task.includes("older"))).toEqual([]);
    expect(actors.messages(actor.id).some((message) => message.stale)).toBe(true);
  }, 30_000);

  // review/astra on ffc39e8, R6: a registry resync that replaces an unowned actor parks the
  // aborted in-flight event for retry, like an ownership refresh does.
  it.each(["every actor", "one of two actors"] as const)("retries an in-flight event aborted by a registry resync after ownership returns (%s unowned)", async (scope) => {
    let owned = true;
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    let keeper: string | undefined;
    // "one of two": another actor stays owned, so the resync takes its partial replacement path.
    const { actors, mesh, agents, root } = setup(true, (id) => owned || id === keeper);
    const runs = recordRuns(agents, () => held);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    if (scope === "one of two actors") {
      keeper = (await actors.create({ name: "keeper", instructions: "Keep.", topics: ["team.other"], responseMode: "text" })).id;
    }
    actors.listOwned();
    await mesh.publish({ topic: "team.pulls", from: { id: "peer", name: "peer", kind: "actor" }, text: "HANG resync" });
    await waitFor(() => actors.status(actor.id).status === "running");
    owned = false;
    const registry = path.join(root, "actors", "actors.json");
    const later = new Date(Date.now() + 5_000);
    fs.utimesSync(registry, later, later);                     // another host rewrote the registry
    actors.listOwned();                                        // the resync replaces the unowned actor
    owned = true;
    actors.listOwned();                                        // ownership returns before the result
    release();
    await waitFor(() => runs.filter((run) => run.task.includes("HANG resync")).length === 2, 15_000);
    actors.haltAll();
  }, 30_000);

  // Astra review of #36, R4: a drop recorded while unowned survives the ownership-return reload.
  it("keeps a drop recorded while unowned across the persistent ownership reload", async () => {
    let owned = true;
    const { actors, mesh, agents } = setup(true, () => owned);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    actors.listOwned();
    await mesh.publish({ topic: "team.pulls", from: { id: "peer", name: "peer", kind: "actor" }, text: "LIVE_WITH_PROGRESS kept" });
    await waitFor(() => agents.list().some((run) => ((run as { turns?: number }).turns ?? 0) > 0));
    owned = false;
    actors.listOwned();                                        // the progressing run is detached and completes unowned
    await waitFor(() => actors.messages(actor.id).some((message) => message.error?.startsWith("Dropped a queued event")), 10_000);
    owned = true;
    actors.listOwned();                                        // persistent reload rebuilds the actor from its registry
    expect(actors.messages(actor.id).some((message) => message.error?.startsWith("Dropped a queued event"))).toBe(true);
  }, 30_000);

  // Astra review of #36, R3: after a reload, restored work waits for the old drain's run.
  it("runs restored events only after the reloaded actor's previous run settles", async () => {
    let owned = true;
    const { actors, mesh, agents } = setup(true, () => owned);
    const runs = recordRuns(agents);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["team.pulls"], responseMode: "text" });
    actors.listOwned();
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    await mesh.publish({ topic: "team.pulls", from, text: "LIVE_WITH_PROGRESS serial-1" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await mesh.publish({ topic: "team.pulls", from, text: "serial-2" });
    await waitFor(() => actors.status(actor.id).queued === 1);
    await waitFor(() => agents.list().some((run) => ((run as { turns?: number }).turns ?? 0) > 0));
    owned = false;
    actors.listOwned();
    owned = true;
    actors.listOwned();                                        // reload: new actor object, serial-2 restored
    await waitFor(() => runs.some((run) => run.task.includes("serial-2") && run.finishedAt !== undefined), 15_000);
    const first = runs.filter((run) => run.task.includes("serial-1"));
    const second = runs.filter((run) => run.task.includes("serial-2"));
    expect(first).toHaveLength(1);                             // the detached run was not restarted
    expect(second).toHaveLength(1);
    expect(second[0]!.startedAt).toBeGreaterThanOrEqual(first[0]!.finishedAt!);
  }, 30_000);

  it("routes host events and durable topic events to subscriptions", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({
      name: "watcher",
      instructions: "Watch parent and team events.",
      events: ["agent_settled"],
      topics: ["team.auth"],
      responseMode: "text",
    });

    expect(actors.dispatchHostEvent("agent_settled", { goal: "ship" })).toBe(1);
    await mesh.publish({
      topic: "team.auth",
      from: { id: "peer", name: "peer", kind: "actor" },
      text: "Need review",
    });

    await waitFor(
      () => actors.messages(actor.id).filter((message) => message.direction === "out").length === 2,
    );
    const sources = actors
      .messages(actor.id)
      .filter((message) => message.direction === "out")
      .map((message) => message.source);
    expect(sources).toEqual(["host:agent_settled", "mesh:team.auth"]);
  });

  it("retains completed-run logs and exposes them via readLog", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    await actors.ask(actor.id, "Inspect auth");
    await waitFor(() => actors.status(actor.id).status === "idle");

    const status = actors.status(actor.id);
    expect(status.sessionFile).toContain("session.jsonl");
    expect(status.logDir).toContain("runs");

    const log = actors.readLog(actor.id, { type: "all" });
    expect(log.actorName).toBe("reviewer");
    expect(log.sessionFile).toContain("session.jsonl");
    const sessionRoles = log.session.map(
      (line) => (line.parsed as { role?: string } | undefined)?.role,
    );
    expect(sessionRoles).toContain("user");
    expect(sessionRoles).toContain("assistant");
    expect(log.run).toBeDefined();
    const eventTypes = log.run!.events.map(
      (line) => (line.parsed as { type?: string } | undefined)?.type,
    );
    expect(eventTypes).toContain("agent_start");
    expect(eventTypes).toContain("message_end");
    expect(log.run!.status?.status).toBe("completed");
    expect(log.retainedRuns).toHaveLength(1);
    // Completed runs are released from the in-memory registry, but the log
    // copy in the actor directory survives.
    expect(agents.list()).toEqual([]);
  });

  it("retains failed-run logs too so readLog can inspect them", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "advisor",
      instructions: "Advise only when useful.",
      responseMode: "directive",
      delivery: "steer",
      triggerTurn: false,
    });
    await actors.ask(actor.id, "FAIL_DIRECTIVE");
    await waitFor(() => actors.status(actor.id).status === "idle");
    const log = actors.readLog(actor.id, { type: "run" });
    expect(log.session).toEqual([]);
    expect(log.run).toBeDefined();
    expect(log.run!.status?.status).toBe("failed");
    const eventTypes = log.run!.events.map(
      (line) => (line.parsed as { type?: string } | undefined)?.type,
    );
    expect(eventTypes).toContain("agent_start");
  });

  it("setModel updates and clears an actor's model and it takes effect on the next run", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    expect(actors.status(actor.id).model).toBeUndefined();

    await actors.setModel(actor.id, "anthropic/claude-sonnet-4-5");
    expect(actors.status(actor.id).model).toBe("anthropic/claude-sonnet-4-5");

    // The new model is forwarded to the agent run launched for the next message.
    await actors.ask(actor.id, "Inspect auth");
    await waitFor(() => actors.status(actor.id).status === "idle");
    const run = actors.readLog(actor.id, { type: "run" });
    expect(run.run?.status?.model).toBe("anthropic/claude-sonnet-4-5");

    // Clearing the override falls back to the Fabric default (no stored model).
    await actors.setModel(actor.id, undefined);
    expect(actors.status(actor.id).model).toBeUndefined();
    await actors.ask(actor.id, "Inspect auth");
    await waitFor(() => actors.status(actor.id).status === "idle");
    const clearedRun = actors.readLog(actor.id, { type: "run" });
    expect(clearedRun.run?.status?.model).toBeUndefined();

    // Whitespace-only values are treated as clearing the override.
    await actors.setModel(actor.id, "  ");
    expect(actors.status(actor.id).model).toBeUndefined();
  });

  it("setModel throws for an unknown actor", async () => {
    const { actors } = setup();
    await expect(actors.setModel("nope", "anthropic/claude-sonnet-4-5")).rejects.toThrow(
      "Unknown Fabric actor",
    );
  });

  it("persists a session model binding without changing the project default", async () => {
    const setupState = setup(true);
    const actor = await setupState.actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    await setupState.actors.setModel(actor.id, "anthropic/claude-sonnet-4-5");
    const registry = JSON.parse(
      fs.readFileSync(path.join(setupState.root, "actors", "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string; model?: string }> };
    expect(registry.actors.find((record) => record.id === actor.id)?.model).toBeUndefined();
    await setupState.actors.close();
    actorManagers.splice(actorManagers.indexOf(setupState.actors), 1);

    const restored = new ActorManager(
      "test",
      setupState.identity,
      setupState.mesh,
      setupState.meshConfig,
      setupState.agents,
      () => {},
      { actorRoot: path.join(setupState.root, "actors"), persistent: true },
    );
    actorManagers.push(restored);

    expect(restored.status(actor.id).model).toBe("anthropic/claude-sonnet-4-5");
  });

  it("setTools normalizes and persists an actor tool allowlist", async () => {
    const setupState = setup(true);
    const actor = await setupState.actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
    });

    await setupState.actors.setTools(actor.id, [" read ", "grep", "read", ""]);
    expect(setupState.actors.status(actor.id).tools).toEqual(["read", "grep"]);
    expect(setupState.actors.definition(actor.id).tools).toEqual(["read", "grep"]);

    await setupState.actors.close();
    actorManagers.splice(actorManagers.indexOf(setupState.actors), 1);
    const restored = new ActorManager(
      "test",
      setupState.identity,
      setupState.mesh,
      setupState.meshConfig,
      setupState.agents,
      () => {},
      { actorRoot: path.join(setupState.root, "actors"), persistent: true },
    );
    actorManagers.push(restored);
    expect(restored.status(actor.id).tools).toEqual(["read", "grep"]);

    await restored.setTools(actor.id, []);
    expect(restored.status(actor.id).tools).toEqual([]);
  });

  it("setThinking updates and clears an actor's thinking and it takes effect on the next run", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    expect(actors.status(actor.id).thinking).toBeUndefined();

    await actors.setThinking(actor.id, "high");
    expect(actors.status(actor.id).thinking).toBe("high");

    // The new thinking is forwarded to the agent run launched for the next message.
    await actors.ask(actor.id, "Inspect auth");
    await waitFor(() => actors.status(actor.id).status === "idle");
    const run = actors.readLog(actor.id, { type: "run" });
    expect(run.run?.status?.thinking).toBe("high");

    // Clearing the override falls back to the Fabric default (medium).
    await actors.setThinking(actor.id, undefined);
    expect(actors.status(actor.id).thinking).toBeUndefined();
    await actors.ask(actor.id, "Inspect auth");
    await waitFor(() => actors.status(actor.id).status === "idle");
    const clearedRun = actors.readLog(actor.id, { type: "run" });
    expect(clearedRun.run?.status?.thinking).toBe("medium");

    // Whitespace-only values are treated as clearing the override.
    await actors.setThinking(actor.id, "  ");
    expect(actors.status(actor.id).thinking).toBeUndefined();
  });

  it("setThinking rejects an invalid thinking level", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    await expect(actors.setThinking(actor.id, "turbo")).rejects.toThrow(
      "Invalid Fabric actor thinking level",
    );
    expect(actors.status(actor.id).thinking).toBeUndefined();
  });

  it("setThinking throws for an unknown actor", async () => {
    const { actors } = setup();
    await expect(actors.setThinking("nope", "high")).rejects.toThrow("Unknown Fabric actor");
  });

  it("persists a session thinking binding across actor manager restarts", async () => {
    const setupState = setup(true);
    const actor = await setupState.actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    await setupState.actors.setThinking(actor.id, "xhigh");
    await setupState.actors.close();
    actorManagers.splice(actorManagers.indexOf(setupState.actors), 1);

    const restored = new ActorManager(
      "test",
      setupState.identity,
      setupState.mesh,
      setupState.meshConfig,
      setupState.agents,
      () => {},
      { actorRoot: path.join(setupState.root, "actors"), persistent: true },
    );
    actorManagers.push(restored);

    expect(restored.status(actor.id).thinking).toBe("xhigh");
  });

  it("haltAll aborts an in-flight run and cancels queued work without tearing actors down", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "supervisor",
      instructions: "Watch and steer only when needed.",
      responseMode: "text",
    });

    // Start a long-running ask (the fake worker hangs until killed). Wait until
    // the run is in flight before queueing a second message, since enqueueing
    // resets the actor status to "queued".
    const askPromise = actors.ask(actor.id, "HANG");
    await waitFor(() => actors.status(actor.id).status === "running");
    actors.tell(actor.id, "queued behind the hanging run");
    expect(actors.status(actor.id).queued).toBe(1);

    expect(actors.haltAll()).toEqual({ halted: 1 });

    // The abort can land before or after the agent process spawns, so the
    // rejection reason is either the semaphore's "Operation aborted" or the
    // transport's "Agent stopped" — both are valid interrupt outcomes.
    await expect(askPromise).rejects.toThrow(/Agent stopped|Operation aborted/);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.status(actor.id).queued).toBe(0);

    // The actor is interrupted, not destroyed: it keeps its identity and can
    // process new messages immediately.
    const reply = await actors.ask(actor.id, "Inspect auth");
    expect(reply.text).toBe("fake worker complete");
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", name: "supervisor" });
  });

  it("haltAll skips idle and stopped actors and leaves them usable", async () => {
    const { actors } = setup();
    const idle = await actors.create({
      name: "idle-advisor",
      instructions: "Advise only when useful.",
      responseMode: "text",
    });
    const stopped = await actors.create({
      name: "stopped-advisor",
      instructions: "Advise only when useful.",
      responseMode: "text",
    });
    await actors.stop(stopped.id);

    // An idle actor with no queued work is not counted as halted.
    expect(actors.haltAll()).toEqual({ halted: 0 });
    expect(actors.status(idle.id)).toMatchObject({ status: "idle" });
    expect(actors.status(stopped.id)).toMatchObject({ status: "stopped" });

    // The idle actor is still responsive after a no-op halt.
    const reply = await actors.ask(idle.id, "Inspect auth");
    expect(reply.text).toBe("fake worker complete");
  });

  it("stop rejects queued asks with the actor identity and cause", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "snapshotter",
      instructions: "Snapshot on demand.",
      responseMode: "text",
    });

    const inFlight = actors.ask(actor.id, "HANG");
    await waitFor(() => actors.status(actor.id).status === "running");
    const queued = actors.ask(actor.id, "queued behind the hanging run");
    expect(actors.status(actor.id).queued).toBe(1);

    await actors.stop(actor.id);

    // The queued ask names the actor and that it was stopped externally; the
    // rejected promise settles synchronously out of stop()'s queue drain.
    await expect(queued).rejects.toThrow(
      `Fabric actor snapshotter (${actor.id}) was stopped while messages were queued`,
    );
    // The in-flight run instead settles with the runner's abort error.
    await inFlight.catch(() => undefined);
    expect(actors.status(actor.id)).toMatchObject({ status: "stopped" });
  });

  it("a self-issued stop directive rejects queued asks with the actor identity and cause", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "one-shot",
      instructions: "Stop when your role is complete.",
      responseMode: "directive",
    });

    const first = actors.ask(actor.id, "STOP_DIRECTIVE");
    const queued = actors.ask(actor.id, "queued behind the stop");
    const queuedRejection = expect(queued).rejects.toThrow(
      `Fabric actor one-shot (${actor.id}) stopped itself with a stop directive while messages were queued`,
    );

    await expect(first).resolves.toMatchObject({ action: "stop" });
    await queuedRejection;
    await waitFor(() => actors.status(actor.id).status === "stopped");
  });

  it("identifies the actor when messaging a stopped actor", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "gone",
      instructions: "Leave when finished.",
      responseMode: "text",
    });
    await actors.stop(actor.id);

    expect(() => actors.tell(actor.id, "hello")).toThrow(
      `Fabric actor gone (${actor.id}) is stopped`,
    );
    expect(() => actors.ask(actor.id, "hello")).toThrow(
      `Fabric actor gone (${actor.id}) is stopped`,
    );
  });

  it("haltAll arms a stop-the-world that suppresses host events until the user resumes", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "watcher",
      instructions: "Watch parent events.",
      events: ["agent_settled"],
      responseMode: "text",
    });

    // Before any halt, host events are delivered normally.
    expect(actors.dispatchHostEvent("agent_settled", { turn: 1 })).toBe(1);
    await waitFor(() => actors.status(actor.id).status === "idle");

    // A halt arms stop-the-world: subsequent host events are suppressed...
    actors.haltAll();
    expect(actors.dispatchHostEvent("agent_settled", { turn: 2 })).toBe(0);

    // ...including other event types, with no time-based expiry.
    expect(actors.dispatchHostEvent("tool_error", { turn: 2 })).toBe(0);
    await new Promise((resolve) => setTimeout(resolve, 60));

    // The user resumes by sending a new message: the "input" host event lifts
    // the halt. The watcher does not subscribe to input, so this dispatches to
    // zero actors but reopens the gate.
    expect(actors.dispatchHostEvent("input", { turn: 3 })).toBe(0);

    // After resume, host-event dispatch is delivered again.
    expect(actors.dispatchHostEvent("agent_settled", { turn: 4 })).toBe(1);
    await waitFor(() => actors.status(actor.id).status === "idle");
  });

  it("delivers mesh messages deferred by stop-the-world immediately after resume", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({
      name: "mesh-watcher",
      instructions: "Watch mesh messages.",
      responseMode: "text",
    });
    actors.haltAll();
    await mesh.publish({
      topic: "fabric.steer",
      kind: "steer",
      from: { id: "peer", name: "peer", kind: "agent" },
      to: actor.id,
      text: "deferred while halted",
    });
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(actors.messages(actor.id)).toEqual([]);

    actors.dispatchHostEvent("input", { resumed: true });
    await waitFor(() => actors.messages(actor.id).some((message) => message.direction === "in"));
    await waitFor(() => actors.status(actor.id).status === "idle");
  });

  it("exposes the stop-the-world gate via halted, lifting it on the next message", async () => {
    const { actors } = setup();

    // The gate starts disarmed.
    expect(actors.halted).toBe(false);

    // haltAll() arms the gate even when no actor had active work to abort.
    expect(actors.haltAll()).toEqual({ halted: 0 });
    expect(actors.halted).toBe(true);

    // A repeated halt is a no-op (the gate is already armed) — the index.ts
    // ESC handler reads halted to avoid re-notifying on a double-Esc.
    expect(actors.haltAll()).toEqual({ halted: 0 });
    expect(actors.halted).toBe(true);

    // The next message ("input") lifts the gate; it can then re-arm.
    expect(actors.dispatchHostEvent("input", { turn: 1 })).toBe(0);
    expect(actors.halted).toBe(false);
    expect(actors.haltAll()).toEqual({ halted: 0 });
    expect(actors.halted).toBe(true);
  });

  it("passes host-event images transiently without recording their base64 in the actor registry", async () => {
    const { actors, root } = setup(true);
    const actor = await actors.create({
      name: "image-observer",
      instructions: "Inspect attached images.",
      events: ["input"],
      responseMode: "directive",
      delivery: "steer",
      triggerTurn: false,
    });
    const data = "cGl4ZWwtc2VjcmV0";
    expect(
      actors.dispatchHostEvent(
        "input",
        {
          signal: {
            payload: {
              type: "input",
              text: "Inspect this image",
              images: [{
                type: "image",
                mediaIndex: 0,
                mimeType: "image/png",
                redacted: true,
              }],
            },
            media: [{ type: "image", mediaIndex: 0, mimeType: "image/png" }],
            idle: false,
          },
        },
        [{ type: "image", data, mimeType: "image/png" }],
      ),
    ).toBe(1);
    await waitFor(() => actors.status(actor.id).status === "idle");

    expect(actors.messages(actor.id).at(-1)).toMatchObject({
      direction: "out",
      action: "message",
      data: { imageCount: 1 },
    });
    const registry = fs.readFileSync(path.join(root, "actors", "actors.json"), "utf8");
    expect(registry).not.toContain(data);
    expect(registry).toContain('"redacted": true');
  });

  it("setEvents replaces an actor's host-event subscriptions and dedupes", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "watcher",
      instructions: "Watch parent events.",
      events: ["agent_settled", "tool_error"],
      responseMode: "text",
    });
    expect(actors.status(actor.id).events).toEqual(["agent_settled", "tool_error"]);

    await actors.setEvents(actor.id, ["input", "turn_end"]);
    expect(actors.status(actor.id).events).toEqual(["input", "turn_end"]);

    // An empty set pauses host-event reactivity without stopping the actor.
    await actors.setEvents(actor.id, []);
    expect(actors.status(actor.id).events).toEqual([]);
    expect(actors.status(actor.id).status).toBe("idle");

    // Duplicates are deduped, preserving first-seen order.
    await actors.setEvents(actor.id, ["agent_settled", "agent_settled"]);
    expect(actors.status(actor.id).events).toEqual(["agent_settled"]);
  });

  it("setEvents rejects an unsupported event", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "watcher",
      instructions: "Watch parent events.",
      responseMode: "text",
    });
    await expect(actors.setEvents(actor.id, ["bogus" as never])).rejects.toThrow(
      "Unsupported Fabric actor event",
    );
    expect(actors.status(actor.id).events).toEqual([]);
  });

  it("setEvents throws for an unknown actor", async () => {
    const { actors } = setup();
    await expect(actors.setEvents("nope", [])).rejects.toThrow("Unknown Fabric actor");
  });

  it("clearMessages resets an actor's recorded history without stopping it", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review messages and reply concisely.",
      responseMode: "text",
    });
    await actors.ask(actor.id, "Inspect auth");
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.messages(actor.id).length).toBeGreaterThan(0);

    await actors.clearMessages(actor.id);
    expect(actors.messages(actor.id)).toEqual([]);
    // The actor is still alive and responsive.
    expect(actors.status(actor.id).status).toBe("idle");
    const reply = await actors.ask(actor.id, "Inspect auth");
    expect(reply.text).toBe("fake worker complete");
  });

  it("clearMessages throws for an unknown actor", async () => {
    const { actors } = setup();
    await expect(actors.clearMessages("nope")).rejects.toThrow("Unknown Fabric actor");
  });

  it("restarts the drain for successive coalesced host events without stranding an item", async () => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "advisor",
      instructions: "Advise only when useful.",
      events: ["agent_settled"],
      responseMode: "directive",
      delivery: "steer",
      triggerTurn: false,
      coalesce: true,
    });
    // Each turn: the actor is idle when the event fires, so a run starts and
    // the drain exits before the next event. A regression in drain restart
    // (the "stuck at queue:1" race) would leave one of these stranded.
    for (let turn = 0; turn < 5; turn++) {
      expect(actors.dispatchHostEvent("agent_settled", { turn })).toBe(1);
      await waitFor(() => actors.status(actor.id).status === "idle");
    }
    expect(deliveries.length).toBe(5);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
  });

  it("processes a host event enqueued while a run is in flight", async () => {
    const { actors, deliveries } = setup();
    const actor = await actors.create({
      name: "advisor",
      instructions: "Advise only when useful.",
      events: ["agent_settled"],
      responseMode: "directive",
      delivery: "steer",
      triggerTurn: false,
      coalesce: true,
    });
    expect(actors.dispatchHostEvent("agent_settled", { turn: 1 })).toBe(1);
    await waitFor(() => actors.status(actor.id).status === "running");
    // A second event arrives while the first run is in flight; the running
    // drain must pick it up on its next loop instead of stranding it.
    expect(actors.dispatchHostEvent("agent_settled", { turn: 2 })).toBe(1);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(deliveries.length).toBe(2);
    expect(actors.status(actor.id).queued).toBe(0);
  });

  it("exposes the portable definition without history", async () => {
    const { actors } = setup();
    const actor = await actors.create({
      name: "reviewer",
      instructions: "Review code.",
      events: ["turn_end"],
      topics: ["team.review"],
      delivery: "steer",
      triggerTurn: false,
      model: "anthropic/sonnet",
    });
    const def = actors.definition(actor.id);
    expect(def).toEqual({
      name: "reviewer",
      instructions: "Review code.",
      events: ["turn_end"],
      topics: ["team.review"],
      delivery: "steer",
      responseMode: "text",
      triggerTurn: false,
      coalesce: true,
      runner: "pi",
      kernel: "typescript",
      pythonRuntime: "monty",
      model: "anthropic/sonnet",
    });
    // history never crosses the global⇄project boundary
    expect(def).not.toHaveProperty("id");
    expect(def).not.toHaveProperty("sessionFile");
    expect(def).not.toHaveProperty("messages");
  });

  it("reads and updates the default instruction", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "advisor", instructions: "Advise." });
    expect(actors.instructions(actor.id)).toBe("Advise.");
    await actors.setInstructions(actor.id, "Advise only when useful.");
    expect(actors.instructions(actor.id)).toBe("Advise only when useful.");
    await expect(actors.setInstructions(actor.id, "   ")).rejects.toThrow(/empty/);
  });
});

describe("ActorManager steering relay", () => {
  const fakeWorker = path.resolve("tests/fixtures/fake-worker.mjs");

  const waitFor = async (predicate: () => boolean, timeoutMs = DEFAULT_WAIT_MS): Promise<void> => {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
      if (Date.now() >= deadline) throw new Error("Timed out waiting for steer relay");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };

  it("steerRemote throws when the mesh is disabled", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-relay-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(root, "runs"),
    });
    agentManagers.push(agents);
    const disabledConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: false, actorPollMs: 20 };
    const actors = new ActorManager(
      "test",
      { id: "session:t", name: "main", kind: "main" },
      mesh,
      disabledConfig,
      agents,
      () => {},
      { actorRoot: path.join(root, "actors") },
    );
    actorManagers.push(actors);
    await expect(actors.steerRemote("anyone", "hi", "steer")).rejects.toThrow(/disabled/);
  });

  it("relays a fabric.steer event across processes to a remote agent", async () => {
    const shared = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-relay-"));
    roots.push(shared);
    const meshPath = path.join(shared, "mesh");
    const meshA = new MeshStore(meshPath, 64 * 1024, 100);
    const meshB = new MeshStore(meshPath, 64 * 1024, 100);
    const agentsA = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(shared, "runsA"),
    });
    const agentsB = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(shared, "runsB"),
    });
    agentManagers.push(agentsA, agentsB);
    const cfg = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const actorsA = new ActorManager(
      "a",
      { id: "session:a", name: "main", kind: "main", sessionId: "a" },
      meshA,
      cfg,
      agentsA,
      () => {},
      { actorRoot: path.join(shared, "actorsA") },
    );
    const actorsB = new ActorManager(
      "b",
      { id: "session:b", name: "main", kind: "main", sessionId: "b" },
      meshB,
      cfg,
      agentsB,
      () => {},
      { actorRoot: path.join(shared, "actorsB") },
    );
    actorManagers.push(actorsA, actorsB);

    // A owns a running agent; B steers it by publishing over the shared mesh.
    const handle = await agentsA.spawn({ task: "HANG", transport: "process" });
    const remote = await actorsB.steerRemote(handle.id, "redirect from B", "steer");
    expect(remote).toEqual({ queued: true, messageId: expect.any(String), routed: "mesh" });
    const steerFile = path.join(agentsA.runDirectory(handle.id)!, "steer.jsonl");
    await waitFor(
      () => fs.existsSync(steerFile) && fs.readFileSync(steerFile, "utf8").includes("redirect from B"),
      3_000,
    );
    const forwarded = fs
      .readFileSync(steerFile, "utf8")
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(forwarded).toHaveLength(1);
    expect(forwarded[0]).toMatchObject({ type: "steer", message: "redirect from B" });
    await agentsA.stop(handle.id);
  });

  it("relays a cross-process follow-up to the owning Main session", async () => {
    const shared = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-relay-"));
    roots.push(shared);
    const meshPath = path.join(shared, "mesh");
    const rootMesh = new MeshStore(meshPath, 64 * 1024, 100);
    const peerMesh = new MeshStore(meshPath, 64 * 1024, 100);
    const rootAgents = new AgentManager(
      process.cwd(),
      DEFAULT_FABRIC_CONFIG.agents,
      { workerPath: fakeWorker, runRoot: path.join(shared, "root-runs") },
    );
    const peerAgents = new AgentManager(
      process.cwd(),
      DEFAULT_FABRIC_CONFIG.agents,
      { workerPath: fakeWorker, runRoot: path.join(shared, "peer-runs") },
    );
    agentManagers.push(rootAgents, peerAgents);
    const deliveries: FabricMainAgentDeliveryRequest[] = [];
    const mainAgent = {
      id: "session:root",
      local: true,
      matches: (id: string) => id === "main" || id === "session:root",
      info: () => ({
        id: "session:root",
        name: "Main" as const,
        kind: "main" as const,
        status: "idle" as const,
        runner: "pi" as const,
        transport: "host" as const,
        cwd: process.cwd(),
        startedAt: 1,
        updatedAt: 1,
        pendingMessages: false,
        local: true,
      }),
      deliverAgent: (request: FabricMainAgentDeliveryRequest) => {
        deliveries.push(request);
        return { queued: true as const, messageId: "main-1", routed: "main" as const };
      },
    };
    const cfg = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const rootActors = new ActorManager(
      "root",
      { id: "session:root", name: "Main", kind: "main", sessionId: "root" },
      rootMesh,
      cfg,
      rootAgents,
      () => {},
      { actorRoot: path.join(shared, "root-actors"), mainAgent },
    );
    const peerActors = new ActorManager(
      "peer",
      { id: "agent:peer", name: "peer", kind: "agent", sessionId: "peer" },
      peerMesh,
      cfg,
      peerAgents,
      () => {},
      { actorRoot: path.join(shared, "peer-actors") },
    );
    actorManagers.push(rootActors, peerActors);

    await peerActors.steerRemote(
      "session:root",
      "summarize after implementation",
      "followUp",
      { requestedBy: "peer" },
    );
    await waitFor(() => deliveries.length === 1, 3_000);
    expect(deliveries).toMatchObject([
      {
        from: { id: "agent:peer", kind: "agent" },
        message: "summarize after implementation",
        delivery: "followUp",
        data: { requestedBy: "peer" },
      },
    ]);
  });

  it("relays a fabric.steer event to a local actor as a mailbox message", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({
      name: "target",
      instructions: "reply",
      responseMode: "text",
    });
    // Simulate a remote peer publishing a steer addressed to this actor.
    await mesh.publish({
      topic: "fabric.steer",
      kind: "steer",
      from: { id: "peer", name: "peer", kind: "actor" },
      to: actor.id,
      text: "from a peer",
    });
    await waitFor(
      () =>
        actors
          .messages(actor.id)
          .some(
            (message) =>
              message.direction === "in" &&
              (message.data as { message?: string } | undefined)?.message === "from a peer",
          ),
      3_000,
    );
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.messages(actor.id).some((message) => message.direction === "out")).toBe(true);
  });
});

describe("ActorManager extensions flag (read-only Pi actors)", () => {
    it("runs a read-only Pi actor (extensions:false) without fabric_exec or recursion", async () => {
      const { actors, agents } = setup();
      const runSpy = vi.spyOn(agents, "run");
      const actor = await actors.create({
        name: "readonly-nav",
        instructions: "Read-only navigator.",
        runner: "pi",
        extensions: false,
        tools: ["read"],
        responseMode: "text",
      });
      expect(actor.extensions).toBe(false);
      await actors.ask(actor.id, "probe");
      const request = runSpy.mock.calls[0]?.[0] as Partial<{ extensions: boolean; recursive: boolean }> | undefined;
      expect(request?.extensions).toBe(false);
      expect(request?.recursive).toBe(false);
    });

    it("defaults to Fabric-enabled (extensions true, recursive true) for a Pi actor", async () => {
      const { actors, agents } = setup();
      const runSpy = vi.spyOn(agents, "run");
      const actor = await actors.create({
        name: "default-nav",
        instructions: "Default navigator.",
        runner: "pi",
        responseMode: "text",
      });
      expect(actor.extensions).toBeUndefined();
      await actors.ask(actor.id, "probe");
      const request = runSpy.mock.calls[0]?.[0] as Partial<{ extensions: boolean; recursive: boolean }> | undefined;
      expect(request?.extensions).toBe(true);
      expect(request?.recursive).toBe(true);
    });

    it("persists extensions:false across close and restore", async () => {
      const setupState = setup(true);
      const created = await setupState.actors.create({
        name: "persistent-readonly",
        instructions: "Survive restart read-only.",
        runner: "pi",
        extensions: false,
        tools: ["read"],
        responseMode: "text",
      });
      await setupState.actors.close();
      actorManagers.splice(actorManagers.indexOf(setupState.actors), 1);
      const restored = new ActorManager(
        "test",
        setupState.identity,
        setupState.mesh,
        setupState.meshConfig,
        setupState.agents,
        () => {},
        { actorRoot: path.join(setupState.root, "actors"), persistent: true },
      );
      actorManagers.push(restored);
      const runSpy = vi.spyOn(setupState.agents, "run");
      expect(restored.list().find((a) => a.name === "persistent-readonly")?.extensions).toBe(false);
      await restored.ask(created.id, "probe");
      const request = runSpy.mock.calls[0]?.[0] as Partial<{ extensions: boolean; recursive: boolean }> | undefined;
      expect(request?.extensions).toBe(false);
      expect(request?.recursive).toBe(false);
    });
});
