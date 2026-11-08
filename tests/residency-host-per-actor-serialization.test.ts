// Keep reason: recorded resident head-of-line blocking (#19308) and request/commit data safety.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidentHost } from "../src/residency/host.js";
import { abandonResidentRequest, readResidentRequestDecision, residentRoot, RESIDENT_ACTOR_COMMAND_FORMAT, type ResidentCommand, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => installInProcessResidentFence());
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const promptly = async <T>(promise: Promise<T>, timeoutMs = 1_500): Promise<T> => {
  let timer!: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Unrelated actor request blocked behind another actor's mutation")), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
};
const files = (root: string, directory: string) => fs.readdirSync(path.join(root, directory)).filter(name => name.endsWith(".json"));
const fixture = async (modelRegistry?: ConstructorParameters<typeof ResidentHost>[2]) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-actor-serialization-"));
  const identity = { id: "session:actor-serialization", name: "Main", kind: "main" as const, sessionId: "actor-serialization" };
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const mesh = new MeshStore(config.meshRoot, 65_536, 1_000);
  const main = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  main.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
    runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  const host = new ResidentHost(config, undefined, modelRegistry);
  await main.start(); await host.start();
  const client = new ResidentActorClient(config.meshRoot, config.rootId, 5_000);
  const caller = { identity, hostId: identity.id };
  const pending: Promise<unknown>[] = [];
  const observe = <T>(promise: Promise<T>) => {
    // Observe immediately so a deliberately held exchange cannot leak a late rejection.
    pending.push(promise.catch(() => undefined));
    return promise;
  };
  const enqueue = (requestId: string, operation: string, fields: Record<string, unknown> = {}) => {
    const command = { format: RESIDENT_ACTOR_COMMAND_FORMAT, rootId: config.rootId, requestId, operation,
      createdAt: Date.now(), caller, ...fields };
    fs.writeFileSync(path.join(config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify(command));
    return command as ResidentCommand;
  };
  return { root, config, host, client, caller, observe, enqueue,
    close: async () => {
      await Promise.all(pending); await host.close(); await main.close(); mesh.closeState(); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    } };
};

describe("resident per-actor admission serialization", () => {
  it("admits an unrelated durable create during real setModel registry preparation before commit", async () => {
    const entered = deferred(), release = deferred();
    let refreshed = false;
    const f = await fixture({
      getAvailable: () => refreshed ? [{ provider: "fixture", id: "stalled" }] : [],
      refresh: async () => { entered.resolve(); await release.promise; refreshed = true; },
    });
    try {
      const actor = await f.client.createActor({ name: "model-preparation", instructions: "original", residency: "durable" });
      const mutation = f.observe(f.client.setActor({ operation: "setModel", id: actor.id, model: "fixture/stalled", scope: "session" }, undefined, f.caller));
      await promptly(entered.promise);
      const processing = files(f.config.residencyRoot, "processing")[0]!;
      expect(readResidentRequestDecision(f.config.residencyRoot, path.basename(processing, ".json"))).toBeUndefined();
      const creation = f.observe(f.client.createActor({ name: "during-model-preparation", instructions: "independent", residency: "durable" }));
      expect((await promptly(creation)).residency).toBe("durable");
      expect(fs.existsSync(path.join(f.config.residencyRoot, "processing", processing))).toBe(true);
      release.resolve();
      expect(await mutation).toMatchObject({ id: actor.id, model: "fixture/stalled" });
      expect(readResidentRequestDecision(f.config.residencyRoot, path.basename(processing, ".json"))).toMatchObject({ state: "committed", id: actor.id });
    } finally { release.resolve(); await f.close(); }
  });

  it("keeps a stalled setter's id/name/prefix FIFO while unrelated creates and setters complete", async () => {
    const f = await fixture();
    const release = deferred();
    try {
      const actor = await f.client.createActor({ name: "held", instructions: "original", residency: "durable" });
      const other = await f.client.createActor({ name: "other", instructions: "original", residency: "durable" });
      const entered: string[] = [];
      const setter = f.host.actors.setInstructions.bind(f.host.actors);
      vi.spyOn(f.host.actors, "setInstructions").mockImplementation(async (...args) => {
        entered.push(args[1]);
        if (args[1] === "first") await release.promise;
        return setter(...args);
      });
      const first = f.observe(f.client.setActor({ operation: "setInstructions", id: actor.id, instructions: "first", replace: true }, undefined, f.caller));
      await vi.waitFor(() => expect(entered).toEqual(["first"]));
      const processing = files(f.config.residencyRoot, "processing")[0]!;
      expect(JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "processing", processing), "utf8"))).toMatchObject({ operation: "setInstructions", id: actor.id });
      // Deterministic existing disk-pickup order, including two accepted aliases of the same actor.
      f.enqueue("a-name", "setInstructions", { id: actor.name, instructions: "second", replace: true });
      f.enqueue("b-prefix", "setInstructions", { id: actor.id.slice(0, 12), instructions: "third", replace: true });
      const created = f.observe(f.client.createActor({ name: "independent", instructions: "new actor", residency: "durable" }));
      const changed = f.observe(f.client.setActor({ operation: "setInstructions", id: other.id, instructions: "unrelated", replace: true }, undefined, f.caller));
      const [newActor, changedActor] = await promptly(Promise.all([created, changed]));
      expect(newActor.residency).toBe("durable"); expect(changedActor.id).toBe(other.id);
      expect(entered.filter(value => value !== "unrelated")).toEqual(["first"]);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "processing", processing))).toBe(true);
      expect(readResidentRequestDecision(f.config.residencyRoot, path.basename(processing, ".json"))).toBeUndefined();
      release.resolve(); await first;
      await vi.waitFor(() => {
        expect(files(f.config.residencyRoot, "requests")).toEqual([]);
        expect(files(f.config.residencyRoot, "processing")).toEqual([]);
      });
      expect(f.host.actors.instructions(actor.id)).toBe("third");
      expect(entered.filter(value => value !== "unrelated")).toEqual(["first", "second", "third"]);
      for (const requestId of ["a-name", "b-prefix"]) {
        expect(readResidentRequestDecision(f.config.residencyRoot, requestId)).toMatchObject({ state: "committed", id: actor.id });
        expect(JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "responses", `${requestId}.json`), "utf8"))).toMatchObject({ ok: true, actor: { id: actor.id } });
      }
    } finally { release.resolve(); await f.close(); }
  });

  it("lets a same-name successor mutate by canonical ID while a removed predecessor mutation is held", async () => {
    const f = await fixture({ getAvailable: () => [{ provider: "fixture", id: "visible" }] });
    const runEntered = deferred(), releaseRun = deferred();
    const predecessorMutationEntered = deferred(), releasePredecessorMutation = deferred();
    let predecessor: Awaited<ReturnType<typeof f.host.actors.create>> | undefined;
    let runSettled = false;
    let predecessorMutationHeld = false;
    let predecessorMutationSettled = false;
    let successorMutationSettled = false;
    let run: Promise<unknown> | undefined;
    let predecessorMutation: Promise<unknown> | undefined;
    let successorMutation: Promise<unknown> | undefined;
    vi.spyOn(f.host.agents, "run").mockImplementation(async () => {
      runEntered.resolve();
      await releaseRun.promise;
      throw new Error("Held activation released");
    });
    try {
      predecessor = await f.host.actors.create({ name: "replacement", instructions: "predecessor", model: "fixture/visible", residency: "durable" });
      run = f.observe(f.host.actors.ask(predecessor.id, "held activation").finally(() => { runSettled = true; }));
      await promptly(runEntered.promise);

      const removed = await promptly(f.observe(f.client.removeActor(predecessor.id)));
      expect(removed).toMatchObject({ removed: true, pending: expect.any(String) });
      expect(runSettled).toBe(false);
      const successor = await f.observe(f.client.createActor({ name: predecessor.name, instructions: "successor", residency: "durable" }));
      expect(successor.id).not.toBe(predecessor.id);

      const setInstructions = f.host.actors.setInstructions.bind(f.host.actors);
      const setterSpy = vi.spyOn(f.host.actors, "setInstructions").mockImplementation(async (...args) => {
        if (args[0] === predecessor!.id) {
          predecessorMutationHeld = true;
          predecessorMutationEntered.resolve();
          await releasePredecessorMutation.promise;
          predecessorMutationHeld = false;
        }
        return setInstructions(...args);
      });
      predecessorMutation = f.observe(f.client.setActor({ operation: "setInstructions", id: predecessor.id,
        instructions: "held predecessor mutation", replace: true }, undefined, f.caller)
        .finally(() => { predecessorMutationSettled = true; }));
      await promptly(predecessorMutationEntered.promise);
      expect(setterSpy).toHaveBeenCalledWith(predecessor.id, "held predecessor mutation", expect.any(Function));
      expect(predecessorMutationHeld).toBe(true);
      expect(predecessorMutationSettled).toBe(false);

      successorMutation = f.observe(f.client.setActor({ operation: "setInstructions", id: successor.id,
        instructions: "successor update", replace: true }, undefined, f.caller)
        .finally(() => { successorMutationSettled = true; }));
      const updated = await promptly(successorMutation, 800);
      expect(updated).toMatchObject({ id: successor.id });
      expect(f.host.actors.instructions(successor.id)).toBe("successor update");
      expect(successorMutationSettled).toBe(true);
      expect(predecessorMutationHeld).toBe(true);
      expect(predecessorMutationSettled).toBe(false);
      expect(runSettled).toBe(false);

      releasePredecessorMutation.resolve();
      await predecessorMutation;
      expect(predecessorMutationSettled).toBe(true);
      expect(f.host.actors.instructions(predecessor.id)).toBe("held predecessor mutation");

      releaseRun.resolve();
      await run.catch(() => undefined);
      const removal = f.host.actors.removalSettled(predecessor.id);
      if (removal) await removal;
      expect(f.host.actors.status(successor.id).id).toBe(successor.id);
    } finally {
      releasePredecessorMutation.resolve();
      releaseRun.resolve();
      await Promise.all([
        run?.catch(() => undefined),
        predecessorMutation?.catch(() => undefined),
        successorMutation?.catch(() => undefined),
      ]);
      if (predecessor) {
        const removal = f.host.actors.removalSettled(predecessor.id);
        if (removal) await removal.catch(() => undefined);
      }
      await f.close();
    }
  }, 15_000);

  it("keeps whole-host reads before an unrelated durable create independent of a held setter", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    try {
      const actor = await f.client.createActor({ name: "held-before-reads", instructions: "original", residency: "durable" });
      const setter = f.host.actors.setInstructions.bind(f.host.actors);
      vi.spyOn(f.host.actors, "setInstructions").mockImplementation(async (...args) => {
        entered.resolve(); await release.promise;
        return setter(...args);
      });
      const mutation = f.observe(f.client.setActor({ operation: "setInstructions", id: actor.id, instructions: "changed", replace: true }, undefined, f.caller));
      await promptly(entered.promise);
      // Fixed envelope order proves both common-path reads precede the public create.
      f.enqueue("a-actors-read", "actors");
      f.enqueue("b-slot-status-read", "slotStatus");
      f.enqueue("c-same-actor-status", "actorStatus", { id: actor.id });
      const creation = f.observe(f.client.createActor({ name: "after-whole-host-reads", instructions: "independent", residency: "durable" }));
      const readResponse = (requestId: string) => JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "responses", `${requestId}.json`), "utf8"));
      const [created] = await promptly(Promise.all([creation, vi.waitFor(() => {
        expect(readResponse("a-actors-read")).toMatchObject({ ok: true, actors: expect.arrayContaining([expect.objectContaining({ id: actor.id })]) });
        expect(readResponse("b-slot-status-read")).toMatchObject({ ok: true, slotStatus: expect.any(Object) });
      }, { timeout: 3_000 })]));
      expect(created.residency).toBe("durable");
      expect(f.host.actors.instructions(actor.id)).toBe("original");
      expect(fs.existsSync(path.join(f.config.residencyRoot, "responses", "c-same-actor-status.json"))).toBe(false);
      release.resolve(); await mutation;
      await vi.waitFor(() => expect(readResponse("c-same-actor-status")).toMatchObject({ ok: true, actor: { id: actor.id } }));
    } finally { release.resolve(); await f.close(); }
  });

  it("serializes pending creates by name and idempotency key without blocking a different name", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    try {
      const create = f.host.actors.create.bind(f.host.actors);
      const calls: string[] = [];
      vi.spyOn(f.host.actors, "create").mockImplementation(async (...args) => {
        calls.push(args[0].name);
        const actor = await create(...args);
        if (args[0].name === "held-create") { entered.resolve(); await release.promise; }
        return actor;
      });
      const request = { name: "held-create", instructions: "one durable birth", residency: "durable" as const, idempotencyKey: "one-birth" };
      const first = f.observe(f.client.createActor(request));
      await promptly(entered.promise);
      const committed = files(f.config.residencyRoot, "processing")[0]!;
      expect(readResidentRequestDecision(f.config.residencyRoot, path.basename(committed, ".json"))).toMatchObject({ state: "committed", operation: "createActor" });
      const retry = f.observe(f.client.createActor({ ...request, name: "retry-with-different-name" }));
      const duplicate = f.observe(f.client.createActor({ ...request, idempotencyKey: "different-key" }).catch(error => error));
      const independent = f.observe(f.client.createActor({ ...request, name: "independent-create", idempotencyKey: "independent-key" }));
      expect((await promptly(independent)).name).toBe("independent-create");
      expect(calls).toEqual(["held-create", "independent-create"]);
      expect(files(f.config.residencyRoot, "requests")).toHaveLength(2);
      release.resolve();
      const [a, b] = await Promise.all([first, retry]);
      expect(b.id).toBe(a.id);
      expect(await duplicate).toMatchObject({ message: expect.stringContaining("already active") });
      expect(calls).toEqual(["held-create", "independent-create", "held-create"]);
      expect(f.host.actors.listOwned().filter(actor => actor.name === "held-create")).toHaveLength(1);
      expect(f.host.actors.listOwned().some(actor => actor.name === "retry-with-different-name")).toBe(false);
    } finally { release.resolve(); await f.close(); }
  });

  it("does not starve a later poll batch behind more than 32 same-actor requests, and fences abandonment", async () => {
    const f = await fixture();
    const release = deferred();
    try {
      const actor = await f.client.createActor({ name: "held-batch", instructions: "original", residency: "durable" });
      const entered = deferred();
      const setter = f.host.actors.setInstructions.bind(f.host.actors);
      vi.spyOn(f.host.actors, "setInstructions").mockImplementation(async (...args) => {
        if (args[1] === "first") { entered.resolve(); await release.promise; }
        return setter(...args);
      });
      const first = f.observe(f.client.setActor({ operation: "setInstructions", id: actor.id, instructions: "first", replace: true }, undefined, f.caller));
      await promptly(entered.promise);
      for (let i = 0; i < 35; i++) {
        const id = `a-queued-${String(i).padStart(2, "0")}`;
        f.enqueue(id, "setInstructions", { id: actor.name, instructions: `queued-${i}`, replace: true });
      }
      const abandoned = f.enqueue("b-abandoned", "setInstructions", { id: actor.id, instructions: "must not commit", replace: true });
      abandonResidentRequest(path.join(f.config.residencyRoot, "requests"), path.join(f.config.residencyRoot, "responses"), abandoned.requestId);
      // Replay its envelope after abandonment removed the source: the tombstone,
      // not successful unlink, must still prevent a late queued mutation.
      f.enqueue(abandoned.requestId, "setInstructions", { id: actor.id, instructions: "must not commit", replace: true });
      const created = f.observe(f.client.createActor({ name: "later-batch", instructions: "new actor", residency: "durable" }));
      expect((await promptly(created)).name).toBe("later-batch");
      release.resolve(); await first;
      await vi.waitFor(() => expect(files(f.config.residencyRoot, "requests")).toEqual([]), { timeout: 3_000 });
      await vi.waitFor(() => expect(files(f.config.residencyRoot, "processing")).toEqual([]));
      expect(f.host.actors.instructions(actor.id)).toBe("queued-34");
      expect(readResidentRequestDecision(f.config.residencyRoot, abandoned.requestId)).toMatchObject({ state: "abandoned" });
      expect(fs.existsSync(path.join(f.config.residencyRoot, "responses", `${abandoned.requestId}.json`))).toBe(false);
    } finally { release.resolve(); await f.close(); }
  });

  it.each(["releaseChange", "releaseDrain"])("keeps %s a global admission barrier across subsequent poll batches", async operation => {
    const f = await fixture();
    const release = deferred();
    try {
      const actor = await f.client.createActor({ name: "before-barrier", instructions: "original", residency: "durable" });
      const entered = deferred();
      const setter = f.host.actors.setInstructions.bind(f.host.actors);
      vi.spyOn(f.host.actors, "setInstructions").mockImplementation(async (...args) => {
        if (args[1] === "first") { entered.resolve(); await release.promise; }
        return setter(...args);
      });
      const first = f.observe(f.client.setActor({ operation: "setInstructions", id: actor.id, instructions: "first", replace: true }, undefined, f.caller));
      await promptly(entered.promise);
      f.enqueue("a-before-barrier", "setInstructions", { id: actor.name, instructions: "before release", replace: true });
      // Deliberately invalid launch intent: exercises the dispatch barrier without launching/exiting a host.
      f.enqueue("b-release-barrier", operation);
      await vi.waitFor(() => expect(files(f.config.residencyRoot, "requests").length + files(f.config.residencyRoot, "processing").length).toBeGreaterThanOrEqual(3));
      const after = f.observe(f.client.createActor({ name: "after-barrier", instructions: "after", residency: "durable" }));
      await vi.waitFor(() => expect(files(f.config.residencyRoot, "requests").some(file => file.startsWith("r1-"))).toBe(true));
      await new Promise(resolve => setTimeout(resolve, 150)); // Covers multiple real 50 ms polls.
      expect(f.host.actors.listOwned().some(candidate => candidate.name === "after-barrier")).toBe(false);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "responses", "b-release-barrier.json"))).toBe(false);
      release.resolve(); await first; await after;
      expect(f.host.actors.instructions(actor.id)).toBe("before release");
      const beforeResponse = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "responses", "a-before-barrier.json"), "utf8"));
      const barrierResponse = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "responses", "b-release-barrier.json"), "utf8"));
      expect(beforeResponse.ok).toBe(true); expect(barrierResponse.ok).toBe(false);
      expect(barrierResponse.completedAt).toBeGreaterThanOrEqual(beforeResponse.completedAt);
    } finally { release.resolve(); await f.close(); }
  });
});
