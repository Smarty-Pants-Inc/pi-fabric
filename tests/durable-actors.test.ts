import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { collectAgentToolPreviewNodes } from "../src/providers/agents-progress.js";
import type { FabricKernel } from "../src/runtime/kernel.js";

const roots: string[] = [];
const actorManagers: ActorManager[] = [];
const agentManagers: AgentManager[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map((manager) => manager.close()));
  await Promise.all(actorManagers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentManagers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-durable-actors-"));
  roots.push(root);
  let kernel: FabricKernel = "python";
  const resolvePiModel = vi.fn((model: string) => {
    if (model === "hidden") throw new Error("Model is not available to this Pi session");
    return model.includes("/") ? model : `provider/${model}`;
  });
  const preparePiModel = vi.fn(async (model: string | undefined) => model ?? "provider/inherited");
  const agents = new AgentManager(process.cwd(), {
    ...structuredClone(DEFAULT_FABRIC_CONFIG.agents), runner: "pi-durable",
  }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    kernel: () => kernel, preparePiModel,
  });
  agentManagers.push(agents);
  const run = vi.spyOn(agents, "run");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity = { id: "session:durable", name: "main", kind: "main" as const, sessionId: "durable" };
  const createManager = () => {
    const manager = new ActorManager("durable", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true, resolvePiModel,
      });
    actorManagers.push(manager);
    return manager;
  };
  return { root, agents, run, mesh, identity, createManager, actors: createManager(),
    resolvePiModel, preparePiModel, setKernel: (value: FabricKernel) => { kernel = value; } };
};

describe("durable Pi actors", () => {
  it("inherits the configured runner, pins kernels and activates recursive Pi with inherited models", async () => {
    const { actors, agents, run, preparePiModel, setKernel } = fixture();
    const actor = await actors.create({ name: "durable reviewer", instructions: "Review", residency: "durable" });
    expect(actor).toMatchObject({ runner: "pi-durable", kernel: "python", residency: "durable" });
    setKernel("typescript");
    await actors.ask(actor.id, "check");
    const request = run.mock.calls[0]?.[0];
    expect(request).toMatchObject({ runner: "pi-durable", kernel: "python", recursive: true, extensions: true });
    expect(request?.systemPrompt).toContain("agents.main()");
    expect(request?.systemPrompt).not.toContain("This Claude runner");
    expect(preparePiModel).toHaveBeenCalledWith(undefined);
    const status = agents.status(actors.status(actor.id).lastRunId!);
    expect(status).toMatchObject({ runner: "pi-durable", model: "provider/inherited" });
    if (!("task" in status)) throw new Error("Expected a concrete actor run record");
    expect(collectAgentToolPreviewNodes([status], { tools: () => [] })[0]).toMatchObject({ runner: "pi-durable", owner: "actor" });
    expect(actors.definition(actor.id)).toMatchObject({ runner: "pi-durable", kernel: "python" });
    const next = await actors.create({ name: "new kernel", instructions: "Review" });
    expect(next.kernel).toBe("typescript");
  });

  it("resolves creation, session and activation model bindings and rejects hidden models", async () => {
    const { actors, run } = fixture();
    await expect(actors.create({ name: "hidden", instructions: "Review", model: "hidden" })).rejects.toThrow(/not available/);
    const actor = await actors.create({ name: "bound", instructions: "Review", model: "project" });
    expect(actor.model).toBe("provider/project");
    await actors.setModel(actor.id, "session");
    expect(actors.resolveBinding(actor.id).model).toBe("provider/session");
    await expect(actors.setModel(actor.id, "hidden")).rejects.toThrow(/not available/);
    await expect(actors.ask(actor.id, "check", undefined, undefined, { overrides: { model: "hidden" } })).rejects.toThrow(/not available/);
    await actors.ask(actor.id, "check", undefined, undefined, { overrides: { model: "call" } });
    expect(run.mock.calls[0]?.[0].model).toBe("provider/call");
    expect(actors.resolveBinding(actor.id).model).toBe("provider/session");
  });

  it("rehydrates durable runner and kernel without migrating legacy Pi sessions", async () => {
    const { actors, createManager } = fixture();
    const durable = await actors.create({ name: "durable", instructions: "Review" });
    const legacy = await actors.create({ name: "legacy", instructions: "Review", runner: "pi", kernel: "typescript" });
    await actors.close();
    const restored = createManager();
    expect(restored.status(durable.id)).toMatchObject({ runner: "pi-durable", kernel: "python" });
    expect(restored.status(legacy.id)).toMatchObject({ runner: "pi", kernel: "typescript" });
  });

  it("honors disabled extensions and retains incompatible-kernel validation", async () => {
    const { actors, run } = fixture();
    await expect(actors.create({ name: "invalid", instructions: "Review", extensions: false, kernel: "python" })).rejects.toThrow(/kernel/i);
    const actor = await actors.create({ name: "native", instructions: "Review", extensions: false });
    expect(actor.kernel).toBeUndefined();
    await actors.ask(actor.id, "check");
    expect(run.mock.calls[0]?.[0]).toMatchObject({ runner: "pi-durable", extensions: false, recursive: false });
    expect(run.mock.calls[0]?.[0].systemPrompt).toContain("Do not attempt to call fabric_exec");
    expect(run.mock.calls[0]?.[0].systemPrompt).not.toContain("This Claude runner");
  });

  it("keeps fleet Pi templates and preserves explicit durable runner and kernel through rehydration and import", async () => {
    const { root, actors } = fixture();
    const registry = new GlobalActorRegistry(root, 64 * 1024);
    expect(registry.create({ name: "fleet default", instructions: "Review" }).runner).toBe("pi");
    const durable = registry.create({ name: "template", instructions: "Review", runner: "pi-durable", kernel: "typescript" });
    const legacy = registry.create({ name: "legacy template", instructions: "Review", runner: "pi" });
    expect(durable.runner).toBe("pi-durable");
    const restored = new GlobalActorRegistry(root, 64 * 1024);
    expect(restored.resolve(legacy.id)?.runner).toBe("pi");
    const definition = restored.resolve(durable.id)!;
    expect(definition).toMatchObject({ runner: "pi-durable", kernel: "typescript" });
    expect(await actors.create(restored.toRequest(definition))).toMatchObject({ runner: "pi-durable", kernel: "typescript" });
    expect(() => registry.create({ name: "invalid", instructions: "Review", kernel: "python", extensions: false })).toThrow(/kernel/i);
  });

  it("advertises Fabric for durable legacy actor records without replacing root Main identity", async () => {
    const { mesh, identity } = fixture();
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    directories.push(directory);
    expect(directory.self().runner).toBe("pi");
    const remote = { id: "session:remote", name: "main", kind: "main" as const, sessionId: "remote" };
    await mesh.put({ key: "sessions/remote", identity: remote, value: {
      id: remote.id, sessionId: "remote", cwd: process.cwd(), status: "idle", startedAt: 1,
    } });
    await mesh.put({ key: "actors/remote/actor:durable", identity: remote, value: {
      id: "actor:durable", name: "durable", status: "idle", runner: "pi-durable", createdAt: 1,
    } });
    expect(directory.get("actor:durable")).toMatchObject({ runner: "pi-durable", capabilities: ["steer", "followUp", "fabric"] });
    expect(directory.get(remote.id)?.runner).toBe("pi");
    expect(directory.peers()[0]?.runner).toBe("pi");
  });
});
