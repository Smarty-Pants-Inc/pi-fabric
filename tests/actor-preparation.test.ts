import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ACTOR_PREPARATION_TIMEOUT_MS, ActorManager, ActorPreparationError, ActorPreparationTimeoutError } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import * as predicate from "../src/actors/predicate.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricCapabilityViewLease } from "../src/core/action-registry.js";

const cleanups: Array<() => Promise<void>> = [];
const waitFor = async (predicate: () => boolean, ms = 5_000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for preparation recovery");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
};
type Options = NonNullable<ConstructorParameters<typeof ActorManager>[6]>;
const setup = (options: Options = {}, maxConcurrent = 20) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-preparation-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("preparation", { id: "session:preparation", name: "main", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot: path.join(root, "actors"), persistent: true, preparationTimeoutMs: 80, preparationRetryMs: 60, ...options });
  cleanups.push(async () => { await actors.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  return { actors, agents, mesh, root };
};
const retryEvent = (actors: ActorManager, id: string, phase: string) => actors.messages(id, 500).find((message) =>
  (message.data as { code?: string; phase?: string } | undefined)?.code === "FABRIC_ACTOR_PREPARATION_TIMEOUT" &&
  (message.data as { phase?: string } | undefined)?.phase === phase);
const readQueue = (sessionFile: string) => {
  const directory = path.dirname(sessionFile);
  const queue = fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!;
  return JSON.parse(fs.readFileSync(path.join(directory, queue), "utf8")).items as Array<{ attempts: number; resumed?: boolean }>;
};
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.restoreAllMocks(); });

describe("actor preparation (#3167)", () => {
  it("exposes a typed phase-specific timeout with a conservative production deadline", () => {
    expect(ACTOR_PREPARATION_TIMEOUT_MS).toBe(30_000);
    const error = new ActorPreparationTimeoutError("actor-id", "presence", 80);
    expect(error).toBeInstanceOf(ActorPreparationError);
    expect(error).toMatchObject({ code: "FABRIC_ACTOR_PREPARATION_TIMEOUT", actorId: "actor-id", phase: "presence", timeoutMs: 80 });
  });

  it("recovers an unresolved presence join and drains the queue while that write is still unresolved", async () => {
    const { actors, agents, mesh } = setup();
    const actor = await actors.create({ name: "stalled", instructions: "Reply", responseMode: "text", coalesce: false });
    const gate = deferred<void>();
    const put = mesh.put.bind(mesh);
    let released = false;
    let writes = 0;
    vi.spyOn(mesh, "put").mockImplementation(async (input) => {
      if (input.key.endsWith(actor.id)) { writes++; await gate.promise; }
      return put(input);
    });
    cleanups.push(async () => { released = true; gate.resolve(); });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "first");
    actors.tell(actor.id, "second");
    await waitFor(() => writes === 1 && actors.status(actor.id).preparing?.phase === "presence");
    expect(actors.status(actor.id)).toMatchObject({ status: "preparing", queued: 1, preparing: { attempts: 0 } });
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    await waitFor(() => retryEvent(actors, actor.id, "presence") !== undefined && actors.inFlightCount() === 0);
    expect(run).not.toHaveBeenCalled();
    expect(actors.status(actor.id)).toMatchObject({ status: "queued", queued: 2 });
    expect(readQueue(actor.sessionFile!)[0]).toMatchObject({ attempts: 1 });
    expect(retryEvent(actors, actor.id, "presence")?.data).toMatchObject({ errorType: "ActorPreparationTimeoutError", attempts: 1 });
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(2);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle" });
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    expect(released).toBe(false);
    expect(writes).toBe(1); // Ordering is retained, not broken to force a second write through.
    gate.resolve();
    await waitFor(() => (mesh.get(`actors/preparation/${actor.id}`)?.value as { status?: string })?.status === "idle");
  });

  it.each(["registry", "validity", "binding"] as const)("bounds an unresolved %s await and retries without resolving the old await", async (phase) => {
    const gate = deferred<void>();
    let stalled = false;
    let bindingCalls = 0;
    const { actors, agents } = setup({ resolvePiModel: (model) => stalled && bindingCalls++ === 0 ? gate.promise.then(() => model) : model });
    const actor = await actors.create({ name: phase, instructions: "Reply", responseMode: "text", coalesce: false,
      ...(phase === "binding" ? { model: "provider/test" } : {}),
      ...(phase === "validity" ? { validWhile: { version: 1, source: "() => true" } } : {}),
    });
    cleanups.push(async () => { gate.resolve(); });
    if (phase === "validity") {
      vi.spyOn(predicate, "evaluateActorValidWhile").mockImplementationOnce(() => gate.promise.then(() => ({ valid: true })));
    } else if (phase === "registry") {
      const withLock = ActorRegistryStore.prototype.withLock;
      let calls = 0;
      vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(async function<T>(this: ActorRegistryStore, operation: () => T): Promise<T> {
        // The enqueue's advisory save and then the drain's required save both stall.
        if (++calls <= 2) await gate.promise;
        return withLock.call(this, operation) as Promise<T>;
      });
    } else stalled = true;
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "first");
    await waitFor(() => retryEvent(actors, actor.id, phase) !== undefined);
    expect(retryEvent(actors, actor.id, phase)?.data).toMatchObject({ attempts: 1 });
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("re-arms after a rejected preparation await as well as a timeout", async () => {
    const { actors, agents } = setup();
    const actor = await actors.create({ name: "rejected", instructions: "Reply", responseMode: "text" });
    const withLock = ActorRegistryStore.prototype.withLock;
    let calls = 0;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function<T>(this: ActorRegistryStore, operation: () => T): Promise<T> {
      if (++calls === 2) return Promise.reject(new Error("temporary registry unavailable"));
      return withLock.call(this, operation) as Promise<T>;
    });
    actors.tell(actor.id, "retry me");
    await waitFor(() => actors.messages(actor.id).some((message) => message.error?.includes("temporary registry unavailable")));
    expect(actors.messages(actor.id).find((message) => message.error)?.data).toMatchObject({ code: "FABRIC_ACTOR_PREPARATION_FAILED", attempts: 1, phase: "registry" });
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(agents.list().every((run) => run.status !== "running")).toBe(true);
  });

  it("keeps finite unavailable-model errors terminal instead of retrying them", async () => {
    let unavailable = false;
    const { actors, agents } = setup({ resolvePiModel: (model) => unavailable
      ? Promise.reject(new Error("model no longer available")) : model });
    const actor = await actors.create({ name: "unavailable", instructions: "Reply", model: "provider/test", responseMode: "text" });
    const run = vi.spyOn(agents, "run");
    unavailable = true;
    await expect(actors.ask(actor.id, "do not retry a finite rejection")).rejects.toThrow("model no longer available");
    await waitFor(() => actors.inFlightCount() === 0);
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(run).not.toHaveBeenCalled();
  });

  it("does not strand the next activation on a stalled cleanup presence write or rerun completed work", async () => {
    const { actors, agents, mesh } = setup();
    const actor = await actors.create({ name: "cleanup", instructions: "Reply", responseMode: "text", coalesce: false });
    const gate = deferred<void>();
    cleanups.push(async () => { gate.resolve(); });
    const put = mesh.put.bind(mesh);
    const cleanup = agents.cleanup.bind(agents);
    vi.spyOn(agents, "cleanup").mockImplementationOnce(async (...args) => {
      vi.spyOn(mesh, "put").mockImplementation(async (input) => {
        if (input.key.endsWith(actor.id)) await gate.promise;
        return put(input);
      });
      return cleanup(...args);
    });
    const run = vi.spyOn(agents, "run");
    actors.tell(actor.id, "first"); actors.tell(actor.id, "second");
    await waitFor(() => retryEvent(actors, actor.id, "presence") !== undefined);
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(run).toHaveBeenCalledTimes(2);
    expect(actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error)).toHaveLength(2);
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
  });

  it("bounds capability acquisition, aborts the old acquisition, and releases a late lease", async () => {
    const gate = deferred<FabricCapabilityViewLease>();
    const release = vi.fn(async () => {});
    const lease: FabricCapabilityViewLease = { satisfied: true, missing: [], optionalMissing: [],
      view: { id: "test", digest: "test", semanticDigest: "test", bindings: {} }, release };
    let firstSignal: AbortSignal | undefined;
    let calls = 0;
    const { actors } = setup({ acquireCapabilityView: async (_requirements, signal) => {
      if (++calls === 1) { firstSignal = signal; return gate.promise; }
      return lease;
    } });
    cleanups.push(async () => { gate.resolve(lease); });
    const actor = await actors.create({ name: "capabilities", instructions: "Reply", responseMode: "text", requires: ["demo.echo"] });
    actors.tell(actor.id, "first");
    await waitFor(() => retryEvent(actors, actor.id, "capabilities") !== undefined);
    expect(firstSignal?.aborted).toBe(true);
    await waitFor(() => actors.inFlightCount() === 0 && actors.status(actor.id).queued === 0);
    expect(release).toHaveBeenCalledTimes(1);
    gate.resolve(lease);
    await waitFor(() => release.mock.calls.length === 2);
  });

  it("14 stalled presence writes neither starve four healthy actors nor impose a cap of four", async () => {
    const { actors, agents, mesh } = setup({ preparationTimeoutMs: 250 });
    const all: Awaited<ReturnType<ActorManager["create"]>>[] = [];
    for (let index = 0; index < 18; index++) all.push(await actors.create({ name: `review-${index}`, instructions: "Reply", responseMode: "text" }));
    const stalled = new Set(all.slice(0, 14).map((actor) => actor.id));
    const gate = deferred<void>();
    const put = mesh.put.bind(mesh);
    vi.spyOn(mesh, "put").mockImplementation(async (input) => {
      if (stalled.has(input.key.split("/").at(-1)!)) await gate.promise;
      return put(input);
    });
    cleanups.push(async () => { gate.resolve(); actors.haltAll(); await waitFor(() => actors.inFlightCount() === 0); });
    for (const actor of all) actors.tell(actor.id, "HANG");
    await waitFor(() => all.slice(14).every((actor) => actors.status(actor.id).inFlightRun !== undefined));
    expect(all.slice(14).every((actor) => retryEvent(actors, actor.id, "presence") === undefined)).toBe(true);
    await waitFor(() => all.every((actor) => actors.status(actor.id).inFlightRun !== undefined));
    expect(agents.list().filter((run) => run.status === "running")).toHaveLength(18);
    expect(all.slice(0, 14).every((actor) => retryEvent(actors, actor.id, "presence") !== undefined)).toBe(true);
    expect(all.every((actor) => actors.status(actor.id).status === "running" && actors.status(actor.id).preparing === undefined)).toBe(true);
  }, 20_000);

  it("reports permit waiters with queue positions, and every actor drains as permits free", async () => {
    const { actors, agents } = setup({}, 2);
    const blockers = await Promise.all(Array.from({ length: 2 }, () => agents.spawn({ task: "HANG" })));
    cleanups.push(async () => { await Promise.all(blockers.map((run) => agents.stop(run.id))); });
    const all: Awaited<ReturnType<ActorManager["create"]>>[] = [];
    for (let index = 0; index < 7; index++) all.push(await actors.create({ name: `waiter-${index}`, instructions: "Reply", responseMode: "text" }));
    for (const actor of all) actors.tell(actor.id, "queued behind permits");
    await waitFor(() => all.every((actor) => actors.status(actor.id).status === "waiting"));
    expect(all.map((actor) => actors.status(actor.id).preparing?.queuePosition).sort()).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(all.every((actor) => actors.status(actor.id).inFlightRun === undefined)).toBe(true);
    await pause(200); // A legitimate permit wait is longer than the 80ms setup deadline.
    expect(all.every((actor) => actors.status(actor.id).status === "waiting")).toBe(true);
    await agents.stop(blockers[0]!.id);
    await waitFor(() => all.every((actor) => actors.status(actor.id).lastRunId !== undefined) && actors.inFlightCount() === 0);
    expect(all.every((actor) => actors.status(actor.id).queued === 0 && actors.status(actor.id).inFlightRun === undefined)).toBe(true);
  });
});
