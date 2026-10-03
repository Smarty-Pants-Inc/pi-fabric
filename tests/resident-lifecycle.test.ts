import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { residentRoot, RESIDENT_COMMANDS, type ResidentHostConfig } from "../src/residency/protocol.js";

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-lifecycle-"));
  const identity = { id: "session:lifecycle", name: "Main", kind: "main" as const, sessionId: "lifecycle" };
  const meshRoot = path.join(root, "mesh"); const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  await participants.start();
  const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" } };
  const host = new ResidentHost(config); await host.start();
  const mainAgent = { id: identity.id, local: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
  const client = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 5_000, mainAgent });
  const passive = new ActorManager(identity.sessionId, identity, mesh, config.mesh, host.agents, () => {}, { actorRoot: config.actorRoot, persistent: true, canManageActor: () => false });
  const provider = new AgentsProvider(host.agents, passive, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants, undefined, host.lifecycle, () => false, client, false);
  const actor = await host.actors.create({ name: "security", instructions: "Keep persona", residency: "durable", model: "fixture/visible", tools: [] });
  await participants.refresh();
  const context = { cwd: root, signal: undefined, extensionContext: {}, parentToolCallId: "lifecycle", nestedToolCallId: "lifecycle", update() {} } as unknown as FabricInvocationContext;
  return { root, config, host, client, passive, provider, actor, context, identity,
    close: async () => { await passive.close(); await host.close(); await client.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); } };
};

it("advertises resident lifecycle commands", () => {
  expect(RESIDENT_COMMANDS).toContain("resetSession"); expect(RESIDENT_COMMANDS).toContain("stop");
});
it("owning root Main resets an archived session and stops through the public provider", async () => {
  const f = await fixture();
  try {
    const file = f.actor.sessionFile!; fs.mkdirSync(path.dirname(file), { recursive: true });
    const before = JSON.stringify({ type: "session", version: 3, id: "old", cwd: f.root, timestamp: new Date().toISOString() }) + "\n";
    fs.writeFileSync(file, before);
    expect(await f.provider.invoke("resetSession", { id: f.actor.name }, f.context)).toMatchObject({ id: f.actor.id });
    expect(JSON.parse(fs.readFileSync(file, "utf8")).id).not.toBe("old");
    const archive = fs.readdirSync(path.dirname(file)).find(name => name.endsWith(".bak"))!;
    expect(fs.readFileSync(path.join(path.dirname(file), archive), "utf8")).toBe(before);
    expect(await f.provider.invoke("stop", { id: f.actor.id }, f.context)).toMatchObject({ id: f.actor.id, status: "stopped" });
    expect(f.host.actors.instructions(f.actor.id)).toBe("Keep persona");
  } finally { await f.close(); }
});
it.each(["resetSession", "stop"] as const)("%s refuses foreign roots and inherited actor lineage without mutation", async operation => {
  const f = await fixture();
  try {
    const proxy = new ResidentActorClient(f.config.meshRoot, f.config.rootId, 5_000);
    for (const identity of [{ ...f.identity, id: "session:other", sessionId: "other" }, { ...f.identity, kind: "actor" as const }]) {
      const before = f.host.actors.status(f.actor.id);
      await expect(proxy.setActor({ operation, id: f.actor.id } as never, undefined, { identity, hostId: f.identity.id })).rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
      expect(f.host.actors.status(f.actor.id)).toEqual(before);
    }
  } finally { await f.close(); }
});
it.each(["resetSession", "stop"] as const)("public %s refuses another root before control routing", async operation => {
  const f = await fixture();
  try {
    const foreignMain = { id: "session:other", local: true, matches: () => false } as unknown as FabricMainAgentTarget;
    const provider = new AgentsProvider(f.host.agents, f.passive, new GlobalActorRegistry(f.root, 64 * 1024), foreignMain,
      f.client.options.participants, undefined, f.host.lifecycle, () => false, f.client, false);
    const before = f.host.actors.status(f.actor.id);
    await expect(provider.invoke(operation, { id: f.actor.id }, f.context)).rejects.toThrow(/own|forbidden|Main|Unknown Fabric actor/);
    expect(f.host.actors.status(f.actor.id)).toEqual(before);
  } finally { await f.close(); }
});
it.each(["resetSession", "stop"] as const)("%s refuses an older live owner before publishing a request", async operation => {
  const f = await fixture();
  const ownerPath = path.join(f.config.residencyRoot, "owner.json");
  const bytes = fs.readFileSync(ownerPath, "utf8");
  try {
    const owner = JSON.parse(bytes); owner.commands = owner.commands.filter((command: string) => command !== operation);
    fs.writeFileSync(ownerPath, JSON.stringify(owner));
    const requests = path.join(f.config.residencyRoot, "requests");
    const before = f.host.actors.status(f.actor.id); const files = fs.readdirSync(requests);
    await expect(f.provider.invoke(operation, { id: f.actor.id }, f.context)).rejects.toMatchObject({ code: "RESIDENT_COMMAND_UNSUPPORTED" });
    expect(fs.readdirSync(requests)).toEqual(files); expect(f.host.actors.status(f.actor.id)).toEqual(before);
  } finally { fs.writeFileSync(ownerPath, bytes); await f.close(); }
});
it("resident control-plane stop cannot bypass the foreign-root authorization fence", async () => {
  const f = await fixture();
  const identity = { id: "session:foreign", name: "Foreign Main", kind: "main" as const, sessionId: "foreign" };
  const mesh = new MeshStore(f.config.meshRoot, 64 * 1024, 100);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  directory.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Foreign Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: f.root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
  try {
    await directory.start(); control.start(() => ({ accepted: false }));
    const before = f.host.actors.status(f.actor.id);
    await expect(control.request(f.host.hostId, f.actor.id, "stop", {}, f.host.identity.id)).rejects.toThrow(/owning Main/);
    expect(f.host.actors.status(f.actor.id)).toEqual(before);
  } finally { await control.close(); await directory.close(); await f.close(); }
});
it("active resident reset refuses; stop joins the run before a reset can archive", async () => {
  const f = await fixture(); let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const original = f.host.agents.run.bind(f.host.agents);
  const spy = vi.spyOn(f.host.agents, "run").mockImplementation(async (request, signal, ...rest) => {
    entered(); await new Promise<void>(resolve => { if (signal?.aborted) resolve(); else signal?.addEventListener("abort", () => resolve(), { once: true }); });
    return original(request, signal, ...rest);
  });
  try {
    const run = f.host.actors.ask(f.actor.id, "held").catch(error => error); await started;
    await expect(f.provider.invoke("resetSession", { id: f.actor.id }, f.context)).rejects.toThrow(/active|in flight|stop first/i);
    expect(await f.provider.invoke("stop", { id: f.actor.id }, f.context)).toMatchObject({ status: "stopped" }); await run;
    expect(await f.provider.invoke("resetSession", { id: f.actor.id }, f.context)).toMatchObject({ id: f.actor.id });
  } finally { await f.host.actors.stop(f.actor.id); spy.mockRestore(); await f.close(); }
});
