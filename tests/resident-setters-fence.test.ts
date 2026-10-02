import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentActorCaller, type ResidentActorMutation, type ResidentHostConfig } from "../src/residency/protocol.js";

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for setter fence probe");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const files = (root: string, directory: string): string[] => {
  try { return fs.readdirSync(path.join(root, directory)).filter(file => file.endsWith(".json")); }
  catch { return []; }
};
const fixture = async (kind: "Main" | "proxy", timeout = 500, deniedDefault = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-setter-fence-"));
  const identity = { id: "session:setter-fence", name: "Main", kind: "main" as const, sessionId: "setter-fence" };
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  participants.registerSource(() => [{
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"],
    cwd: root, sessionId: identity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
  }]);
  await participants.start();
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, ...(deniedDefault ? { model: "fixture/denied", deniedModels: ["fixture/denied"] } : {}) }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const entered = deferred();
  const release = deferred();
  let refreshed = false;
  const host = new ResidentHost(config, () => {}, {
    getAvailable: () => refreshed ? [{ provider: "fixture", id: "slow" }] : [],
    refresh: async () => { entered.resolve(); await release.promise; refreshed = true; },
  });
  await host.start();
  const main = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: timeout,
    mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
  const client = kind === "Main" ? main : new ResidentActorClient(meshRoot, identity.id, timeout);
  const actor = await host.actors.create({ name: "fenced setter", instructions: "Keep this persona", residency: "durable", model: "fixture/visible", tools: ["read"] });
  const caller = { identity, hostId: identity.id, principal: { id: "paul", binding: "voice-call" as const } };
  return { host, config, actor, client, main, mesh, participants, caller, entered, release,
    close: async () => { release.resolve(); await host.close(); await main.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); },
  };
};

it("round 3 Astra 2 preserves public guest policy classification across durable clear", async () => {
  const state = await fixture("Main", 5_000, true);
  const passive = new ActorManager(state.config.sessionId, state.caller.identity, state.mesh, state.config.mesh, state.host.agents, () => {}, {
    actorRoot: state.config.actorRoot, persistent: true, canManageActor: () => false,
  });
  const mainAgent = { id: state.config.rootId, local: true, matches: (id: string) => id === state.config.rootId } as FabricMainAgentTarget;
  const provider = new AgentsProvider(state.host.agents, passive, new GlobalActorRegistry(state.config.cwd, 64 * 1024), mainAgent, state.participants, undefined, state.host.lifecycle, () => false, state.main, false);
  const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.approvals.agent = "allow";
  const registry = new ActionRegistry(); registry.register(provider);
  const service = new FabricExecutionService(registry, config);
  const clear = () => service.execute({ code: `try { await agents.setModel({ id: ${JSON.stringify(state.actor.id)}, scope: "project" }); return { admitted: true }; } catch (error) { return { name: error.name, code: error.code }; }`,
    signal: undefined, parentToolCallId: "round3-durable-clear", context: { cwd: state.config.cwd, hasUI: false, modelRegistry: { getAvailable: () => [{ provider: "fixture", id: "visible" }] } } as unknown as ExtensionContext, onPartial() {},
  });
  try {
    const before = state.host.actors.status(state.actor.id);
    const registryBytes = fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8");
    const result = await clear();
    expect(result.success).toBe(true);
    expect(result.value).toEqual({ name: "FabricModelDeniedError", code: "FABRIC_MODEL_DENIED" });
    expect(state.host.actors.status(state.actor.id)).toEqual(before);
    expect(fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8")).toBe(registryBytes);
    const proxy = new ResidentActorClient(state.config.meshRoot, state.config.rootId, 5_000);
    await expect(proxy.setActor({ operation: "setModel", id: state.actor.id, scope: "project" }, undefined, state.caller)).rejects.toMatchObject({ name: "FabricModelDeniedError", code: "FABRIC_MODEL_DENIED" });
    // Retained allowed session overlay is the real fallback for project clear.
    await state.host.actors.setModel(state.actor.id, "fixture/visible", "session");
    expect((await clear()).value).toEqual({ admitted: true });
    expect(state.host.actors.status(state.actor.id).projectDefaults?.model).toBeUndefined();
  } finally { await passive.close(); await state.close(); }
});

for (const kind of ["Main", "proxy"] as const) {
  describe(`${kind} resident setter commit fence`, () => {
    for (const scope of ["session", "project"] as const) for (const ending of ["abort", "timeout"] as const) {
      it(`setModel ${scope} ${ending} during real refresh abandons before binding mutation`, async () => {
        const state = await fixture(kind);
        const abort = new AbortController();
        try {
          const before = state.host.actors.status(state.actor.id);
          const registry = fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8");
          const result = state.client.setActor({ operation: "setModel", id: state.actor.id, model: "fixture/slow", scope }, abort.signal, state.caller).catch(error => error);
          await state.entered.promise;
          const pickedUp = files(state.config.residencyRoot, "processing");
          expect(pickedUp).toHaveLength(1);
          if (ending === "abort") abort.abort();
          expect(await result).toMatchObject({ message: expect.stringMatching(ending === "abort" ? /aborted/ : /Timed out/) });
          state.release.resolve();
          await waitFor(() => files(state.config.residencyRoot, "processing").length === 0);
          expect(state.host.actors.status(state.actor.id)).toEqual(before);
          expect(fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8")).toBe(registry);
          const decision = JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "decisions", pickedUp[0]!), "utf8"));
          expect(decision).toMatchObject({ state: "abandoned" });
          expect(decision).not.toHaveProperty("principal");
          expect(files(state.config.residencyRoot, "responses")).toEqual([]);
        } finally { abort.abort(); await state.close(); }
      });
    }

    for (const operation of ["setModel", "setThinking"] as const) for (const ending of ["abort", "timeout"] as const) {
      it(`${operation} session ${ending} while binding lock is held abandons before transaction mutation`, async () => {
        const state = await fixture(kind);
        const abort = new AbortController();
        let lockPath: string | undefined;
        try {
          await state.host.actors.setThinking(state.actor.id, "low", "session");
          const bindingsRoot = path.join(state.config.actorRoot, "bindings");
          const bindingFile = path.join(bindingsRoot, fs.readdirSync(bindingsRoot).find(file => file.endsWith(".json"))!);
          const bytes = fs.readFileSync(bindingFile, "utf8");
          const before = state.host.actors.status(state.actor.id);
          lockPath = `${bindingFile}.lock`;
          fs.mkdirSync(lockPath);
          fs.writeFileSync(path.join(lockPath, "owner"), `held-by-test\n${process.pid}\n${Date.now()}\n`);
          const mutation: ResidentActorMutation = operation === "setModel"
            ? { operation, id: state.actor.id, model: "fixture/visible", scope: "session" }
            : { operation, id: state.actor.id, thinking: "high", scope: "session" };
          const result = state.client.setActor(mutation, abort.signal, state.caller).catch(error => error);
          await waitFor(() => files(state.config.residencyRoot, "processing").length === 1);
          const request = files(state.config.residencyRoot, "processing")[0]!;
          // Actual host is blocked in binding-store preparation, not committed.
          await new Promise(resolve => setTimeout(resolve, 30));
          expect(fs.existsSync(path.join(state.config.residencyRoot, "decisions", request))).toBe(false);
          if (ending === "abort") abort.abort();
          expect(await result).toMatchObject({ message: expect.stringMatching(ending === "abort" ? /aborted/ : /Timed out/) });
          fs.rmSync(lockPath, { recursive: true, force: true }); lockPath = undefined;
          await waitFor(() => files(state.config.residencyRoot, "processing").length === 0);
          expect(state.host.actors.status(state.actor.id)).toEqual(before);
          expect(fs.readFileSync(bindingFile, "utf8")).toBe(bytes);
          expect(JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "decisions", request), "utf8"))).toMatchObject({ state: "abandoned" });
          expect(files(state.config.residencyRoot, "responses")).toEqual([]);
        } finally {
          abort.abort();
          if (lockPath) fs.rmSync(lockPath, { recursive: true, force: true });
          await state.close();
        }
      });
    }

    it.each(["actor lineage", "wrong session", "wrong host"])("a carried principal cannot authorize %s before the setter fence", async (invalid) => {
      const state = await fixture(kind);
      try {
        const before = state.host.actors.status(state.actor.id);
        const caller: ResidentActorCaller = { ...state.caller, identity: { ...state.caller.identity } };
        if (invalid === "actor lineage") caller.identity.kind = "actor";
        if (invalid === "wrong session") caller.identity.sessionId = "foreign";
        if (invalid === "wrong host") caller.hostId = "foreign";
        await expect(state.client.setActor({ operation: "setInstructions", id: state.actor.id, instructions: "Forbidden" }, undefined, caller)).rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
        expect(state.host.actors.status(state.actor.id)).toEqual(before);
        // A dispatched rejection leaves the client's abandonment tombstone,
        // but authorization must never reach the committed mutation fence.
        for (const file of files(state.config.residencyRoot, "decisions")) {
          const decision = JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "decisions", file), "utf8"));
          expect(decision).toMatchObject({ state: "abandoned" });
          expect(decision).not.toHaveProperty("principal");
        }
      } finally { await state.close(); }
    });

    const mutations: ResidentActorMutation[] = [
      { operation: "setInstructions", id: "", instructions: "New persona" },
      { operation: "setModel", id: "", model: "fixture/visible", scope: "project" },
      { operation: "setThinking", id: "", thinking: "high", scope: "session" },
      { operation: "setActivationFilter", id: "", activationFilter: null },
      { operation: "setTools", id: "", tools: ["read", "bash"] },
    ];
    for (const mutation of mutations) for (const ending of ["abort", "timeout"] as const) {
      it(`${mutation.operation} ${ending} after commit preserves known-ID receipt and rejects replay`, async () => {
        const state = await fixture(kind);
        const abort = new AbortController();
        const entered = deferred(); const release = deferred();
        // Hold response publication after the real setter's mutation/fence, not at pickup.
        const original = state.host.actors[mutation.operation].bind(state.host.actors) as (...args: unknown[]) => Promise<unknown>;
        const spy = vi.spyOn(state.host.actors, mutation.operation).mockImplementation(async (...args: unknown[]) => {
          const actor = await original(...args); entered.resolve(); await release.promise; return actor as ReturnType<typeof state.host.actors.status>;
        });
        try {
          const result = state.client.setActor({ ...mutation, id: state.actor.id }, abort.signal, state.caller).catch(error => error);
          await entered.promise;
          const pickedUp = files(state.config.residencyRoot, "processing");
          expect(pickedUp).toHaveLength(1);
          const envelope = fs.readFileSync(path.join(state.config.residencyRoot, "processing", pickedUp[0]!), "utf8");
          const requestId = pickedUp[0]!.slice(0, -5);
          expect(JSON.parse(envelope).caller.principal).toEqual(state.caller.principal);
          const decisionPath = path.join(state.config.residencyRoot, "decisions", pickedUp[0]!);
          const decision = fs.readFileSync(decisionPath, "utf8");
          expect(JSON.parse(decision)).toMatchObject({ state: "committed", id: state.actor.id, operation: mutation.operation, principal: state.caller.principal });
          if (ending === "abort") abort.abort();
          expect(await result).toMatchObject({ name: "ResidentOutcomeUnknownError", id: state.actor.id, requestId, operation: mutation.operation,
            residentOutcome: { state: "committed", entityKind: "actor", id: state.actor.id, operation: mutation.operation } });
          release.resolve();
          await waitFor(() => files(state.config.residencyRoot, "processing").length === 0);
          const updated = state.host.actors.status(state.actor.id);
          const responsePath = path.join(state.config.residencyRoot, "responses", pickedUp[0]!);
          expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: true, actor: { id: state.actor.id } });
          fs.rmSync(responsePath);
          fs.writeFileSync(path.join(state.config.residencyRoot, "requests", pickedUp[0]!), envelope);
          await waitFor(() => fs.existsSync(responsePath));
          expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: false, error: expect.stringContaining("already committed") });
          expect(state.host.actors.status(state.actor.id)).toEqual(updated);
          expect(fs.readFileSync(decisionPath, "utf8")).toBe(decision);
        } finally { release.resolve(); abort.abort(); spy.mockRestore(); await state.close(); }
      });
    }
  });
}
