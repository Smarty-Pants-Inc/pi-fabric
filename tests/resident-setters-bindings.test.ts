import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { controlActorBindingOptions } from "../src/topology/control-plane.js";

const roots: string[] = [];
const managers: ActorManager[] = [];
const agentsList: AgentManager[] = [];
const releases: Array<() => void> = [];
const identity = { id: "session:root", name: "Main", kind: "main" as const, sessionId: "root" };
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for binding probe");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const terminal = (request: AgentRunRequest, id: number): AgentRunResult => ({
  id: `probe-${id}`, name: request.name ?? "probe", task: request.task, status: "completed",
  runner: "pi", transport: "process", cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(),
  turns: 1, toolCalls: 0, text: "done", usage: {
    input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
    cost: 0,
  },
});
const setup = (queueLimit = 2) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-binding-drain-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  agentsList.push(agents);
  const launches: AgentRunRequest[] = [];
  let release = () => {};
  const held = new Promise<void>((resolve) => { release = resolve; });
  releases.push(release);
  vi.spyOn(agents, "run").mockImplementation(async (request) => {
    launches.push(structuredClone(request));
    if (launches.length === 1) await held;
    return terminal(request, launches.length);
  });
  const actorRoot = path.join(root, "actors");
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: queueLimit };
  const open = (options: ConstructorParameters<typeof ActorManager>[6] = {}) => {
    const manager = new ActorManager("root", identity, mesh, meshConfig, agents, () => {}, { actorRoot, persistent: true, ...options });
    managers.push(manager); return manager;
  };
  return { root, mesh, agents, launches, release, open, actorRoot };
};
afterEach(async () => {
  for (const release of releases.splice(0)) release();
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentsList.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("resident queued binding defaults", () => {
  it.each(["session", "durable"] as const)("%s: rebinds N queued and overflow items, pins only requested fields, and leaves an in-flight launch unchanged", async (residency) => {
    const state = setup(); const actors = state.open();
    const actor = await actors.create({ name: "serial", instructions: "Watch", model: "provider/old", thinking: "low", coalesce: false, residency });
    actors.tell(actor.id, "in flight"); await waitFor(() => state.launches.length === 1);
    for (let index = 0; index < 6; index++) actors.tell(actor.id, `default-${index}`);
    actors.tell(actor.id, "both explicit", undefined, { overrides: { model: "provider/pinned", thinking: "high" } });
    actors.tell(actor.id, "model explicit", undefined, { overrides: { model: "provider/pinned" } });
    actors.tell(actor.id, "thinking explicit", undefined, { overrides: { thinking: "max" } });
    await actors.setModel(actor.id, "provider/new"); await actors.setThinking(actor.id, "xhigh");
    const file = fs.readdirSync(path.join(state.actorRoot, actor.id)).find((name) => name.startsWith("queue-"))!;
    const saved = JSON.parse(fs.readFileSync(path.join(state.actorRoot, actor.id, file), "utf8"));
    expect(saved.items.slice(0, 7).map((item: { binding: unknown }) => item.binding)).toEqual(Array.from({ length: 7 }, () => ({})));
    state.release(); await waitFor(() => state.launches.length === 10 && actors.status(actor.id).status === "idle");
    expect(state.launches[0]).toMatchObject({ model: "provider/old", thinking: "low" });
    expect(state.launches.slice(1, 7)).toEqual(Array.from({ length: 6 }, () => expect.objectContaining({ model: "provider/new", thinking: "xhigh" })));
    expect(state.launches[7]).toMatchObject({ model: "provider/pinned", thinking: "high" });
    expect(state.launches[8]).toMatchObject({ model: "provider/pinned", thinking: "xhigh" });
    expect(state.launches[9]).toMatchObject({ model: "provider/new", thinking: "max" });
  });

  it("keeps coalesced mesh defaults dynamic, including when the replaced item sits in overflow", async () => {
    const state = setup(1); const actors = state.open();
    const actor = await actors.create({ name: "coalesced", instructions: "Watch", model: "provider/old", thinking: "low", topics: ["work.items"], coalesceKey: "key" });
    actors.tell(actor.id, "in flight"); await waitFor(() => state.launches.length === 1);
    actors.tell(actor.id, "queue slot");
    await state.mesh.publish({ topic: "work.items", from: identity, data: { key: "same", value: "older" } });
    await waitFor(() => actors.status(actor.id).queued === 2);
    await state.mesh.publish({ topic: "work.items", from: identity, data: { key: "same", value: "newer" } });
    await waitFor(() => {
      const file = fs.readdirSync(path.join(state.actorRoot, actor.id)).find((name) => name.startsWith("queue-"))!;
      return fs.readFileSync(path.join(state.actorRoot, actor.id, file), "utf8").includes("newer");
    });
    await actors.setModel(actor.id, "provider/new", "project"); await actors.setThinking(actor.id, "max", "project");
    state.release(); await waitFor(() => state.launches.length === 3 && actors.status(actor.id).status === "idle");
    expect(state.launches[2]).toMatchObject({ model: "provider/new", thinking: "max" });
    expect(state.launches[2]!.task).toContain("newer"); expect(state.launches[2]!.task).not.toContain("older");
  });

  it("fences resident session actors from Main on reload and never claims a plain Main session actor", async () => {
    const state = setup(); state.release();
    const main = state.open({ claimResidency: "session", rootId: identity.id });
    const plain = await main.create({ name: "plain Main", instructions: "Watch", residency: "session" });
    await main.close();
    const host = state.open({ claimResidency: "durable", hostSessionActors: true, rootId: identity.id, lineageAlive: () => true });
    expect(host.owns(plain.id)).toBe(false);
    const resident = await host.create({ name: "resident session", instructions: "Watch", residency: "session" }, { asRegistryOwner: true });
    await host.close();
    const mainReloaded = state.open({ claimResidency: "session", rootId: identity.id });
    expect(mainReloaded.owns(plain.id)).toBe(true); expect(mainReloaded.owns(resident.id)).toBe(false);
    await mainReloaded.close();
    const hostReloaded = state.open({ claimResidency: "durable", hostSessionActors: true, rootId: identity.id, lineageAlive: () => true });
    expect(hostReloaded.owns(plain.id)).toBe(false); expect(hostReloaded.owns(resident.id)).toBe(true);
  });

  it("persists project model/thinking through the existing publishPresence save path", async () => {
    const state = setup(); const actors = state.open(); state.release();
    const actor = await actors.create({ name: "persisted", instructions: "Watch", model: "provider/old", thinking: "low" });
    await actors.setModel(actor.id, "provider/new", "project"); await actors.setThinking(actor.id, "max", "project");
    await actors.close(); const reloaded = state.open();
    expect(reloaded.status(actor.id)).toMatchObject({ model: "provider/new", thinking: "max", projectDefaults: { model: "provider/new", thinking: "max" } });
  });

  it("migrates legacy mesh/host defaults but conservatively preserves legacy direct bindings on reload", async () => {
    const state = setup(10); const actors = state.open(); state.release();
    const actor = await actors.create({ name: "reload", instructions: "Watch", model: "provider/old", thinking: "low" });
    await actors.close();
    const dir = path.join(state.actorRoot, actor.id);
    const key = (await import("node:crypto")).createHash("sha256").update([identity.id, "session"].join("\0")).digest("hex").slice(0, 16);
    const registry = JSON.parse(fs.readFileSync(path.join(state.actorRoot, "actors.json"), "utf8"));
    registry.actors[0].model = "provider/new"; registry.actors[0].thinking = "max";
    fs.writeFileSync(path.join(state.actorRoot, "actors.json"), JSON.stringify(registry));
    const records = ["mesh:work.items", "host:input", "direct", "direct"].map((source, index) => ({
      id: `saved-${index}`, source, payload: { message: `saved-${index}` }, createdAt: Date.now(),
      activation: { kind: "direct", id: `saved-${index}`, source, sequence: index + 1, createdAt: Date.now() },
      binding: index === 3 ? { model: "provider/pinned" } : { model: "provider/old", thinking: "low" },
      ...(index === 3 ? { bindingVersion: 2 } : {}),
    }));
    fs.writeFileSync(path.join(dir, `queue-${key}.json`), JSON.stringify({ format: 1, items: records }));
    const reloaded = state.open(); await waitFor(() => state.launches.length === 4 && reloaded.status(actor.id).status === "idle");
    expect(state.launches.slice(0, 2)).toEqual(Array.from({ length: 2 }, () => expect.objectContaining({ model: "provider/new", thinking: "max" })));
    expect(state.launches[2]).toMatchObject({ model: "provider/old", thinking: "low" });
    expect(state.launches[3]).toMatchObject({ model: "provider/pinned", thinking: "max" });
  });
});

describe("owner-default control provenance", () => {
  it("accepts raw explicit fields from root or validated own-root actors, and rejects foreign promotion", () => {
    const command = { binding: { model: "provider/pinned" }, bindingProvenance: { kind: "owner-defaults" as const, rootId: identity.id } };
    expect(controlActorBindingOptions(command, identity, identity.id, undefined)).toEqual({ overrides: { model: "provider/pinned" } });
    const sender = { id: "actor-a", name: "actor", kind: "actor" as const };
    expect(controlActorBindingOptions(command, sender, identity.id, identity.id)).toEqual({ overrides: command.binding });
    expect(() => controlActorBindingOptions(command, sender, identity.id, "session:foreign")).toThrow("Invalid actor owner-default binding provenance");
    expect(() => controlActorBindingOptions(command, identity, "session:foreign", identity.id)).toThrow("Invalid actor owner-default binding provenance");
    expect(controlActorBindingOptions({ binding: command.binding }, sender, identity.id, "session:foreign")).toEqual({ binding: command.binding });
  });
});
