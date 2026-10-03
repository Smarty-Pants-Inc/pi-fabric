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

const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for resident lifecycle probe");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const promptly = async <T>(promise: Promise<T>): Promise<T> => {
  let timer!: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Resident dispatcher blocked behind activation settlement")), 2_000);
    })]);
  } finally { clearTimeout(timer); }
};
const decisions = (root: string): Array<{ requestId: string; state: string; operation: string; id: string }> => {
  try { return fs.readdirSync(path.join(root, "decisions")).filter(name => name.endsWith(".json"))
    .map(name => JSON.parse(fs.readFileSync(path.join(root, "decisions", name), "utf8"))); }
  catch { return []; }
};

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
  const actor = await host.actors.create({ name: "security", instructions: "Keep persona", residency: "durable", model: "fixture/visible", tools: [], topics: ["repair.events"] });
  await participants.refresh();
  const context = { cwd: root, signal: undefined, extensionContext: {}, parentToolCallId: "lifecycle", nestedToolCallId: "lifecycle", update() {} } as unknown as FabricInvocationContext;
  return { root, config, host, client, passive, provider, actor, context, identity,
    close: async () => { await passive.close(); await host.close(); await client.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); } };
};

it.each(["Main", "proxy"] as const)("public reset leaves status, other actors and terminal stop serviceable during a hanging activation (%s)", async kind => {
  const f = await fixture();
  const pending: Promise<unknown>[] = [];
  const observe = <T>(promise: Promise<T>) => {
    const outcome = promise.then(value => ({ value }), error => ({ error }));
    pending.push(outcome); return outcome;
  };
  try {
    const other = await f.host.actors.create({ name: "unrelated", instructions: "Other persona", residency: "durable", model: "fixture/visible", tools: [] });
    await f.client.options.participants.refresh();
    const run = observe(f.host.actors.ask(f.actor.id, "HANG_WITH_PROGRESS"));
    await waitFor(() => {
      const id = f.host.actors.status(f.actor.id).inFlightRun?.id;
      if (!id) return false;
      const run = f.host.agents.status(id);
      return "turns" in run && run.turns === 3;
    });
    const header = fs.readFileSync(f.actor.sessionFile!, "utf8").split("\n")[0];
    const queued = observe(f.host.actors.ask(f.actor.id, "queued work cancelled only by explicit stop"));
    const proxy = new ResidentActorClient(f.config.meshRoot, f.config.rootId, 5_000);
    const reset = observe(kind === "Main"
      ? f.provider.invoke("resetSession", { id: f.actor.name }, f.context)
      : proxy.setActor({ operation: "resetSession", id: f.actor.id }, undefined, { identity: f.identity, hostId: f.identity.id }));
    await waitFor(() => decisions(f.config.residencyRoot).some(entry => entry.operation === "resetSession" && entry.state === "committed"));
    const resetDecision = decisions(f.config.residencyRoot).find(entry => entry.operation === "resetSession")!;
    // All use the public provider, which routes to this resident's command dispatcher.
    const status = observe(f.provider.invoke("actorStatus", { id: f.actor.id }, f.context));
    const unrelated = observe(f.provider.invoke("setInstructions", { id: other.id, instructions: "Still serviceable" }, f.context));
    const stop = observe(f.provider.invoke("stop", { id: f.actor.id }, f.context));
    const results = await promptly(Promise.all([status, unrelated, stop]));
    expect(results[0]).toMatchObject({ value: { id: f.actor.id } });
    expect(results[1]).toMatchObject({ value: { id: other.id } });
    expect(results[2]).toMatchObject({ value: { id: f.actor.id, status: "stopped" } });
    // No external release: only the owner's public stop can terminate this real child.
    await promptly(run);
    expect(await promptly(reset)).toMatchObject({ error: { name: "ActorSessionResetCancelledError", code: "ACTOR_SESSION_RESET_CANCELLED", id: f.actor.id, requestId: resetDecision.requestId } });
    expect(await queued).toMatchObject({ error: { message: expect.stringContaining("stopped while messages were queued") } });
    expect(fs.readFileSync(f.actor.sessionFile!, "utf8").split("\n")[0]).toBe(header);
    expect(fs.readdirSync(path.dirname(f.actor.sessionFile!)).filter(name => name.endsWith(".bak"))).toEqual([]);
    expect(f.host.actors.messages(f.actor.id, 50).some(message => (message.data as { sessionReset?: unknown })?.sessionReset)).toBe(false);
    expect(f.host.actors.status(f.actor.id).status).toBe("stopped");
    expect(f.host.actors.status(f.actor.id)).not.toHaveProperty("inFlightRun");
    expect(() => f.host.actors.ask(f.actor.id, "not resumed")).toThrow(/stopped/);
    for (const entry of decisions(f.config.residencyRoot).filter(entry => ["resetSession", "stop"].includes(entry.operation))) {
      expect(entry).toMatchObject({ state: "committed", id: f.actor.id });
      expect(fs.existsSync(path.join(f.config.residencyRoot, "acknowledgements", `${entry.requestId}.json`))).toBe(true);
    }
    expect(fs.readdirSync(path.join(f.config.residencyRoot, "processing"))).toEqual([]);
    expect(fs.readdirSync(path.join(f.config.residencyRoot, "responses"))).toEqual([]);
  } finally {
    // Failed assertions on the old dispatcher must not leave the real hanging child alive.
    const actor = f.host.actors.status(f.actor.id);
    const runId = actor.inFlightRun?.id ?? actor.lastRunId;
    if (runId) await f.host.agents.stop(runId);
    await f.host.actors.stop(f.actor.id, undefined, true);
    await Promise.all(pending);
    await f.close();
  }
}, 15_000);

it("public stop joins its activation without blocking resident status or another actor's command", async () => {
  const f = await fixture();
  let entered!: () => void; let aborted!: () => void; let release!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const cancelled = new Promise<void>(resolve => { aborted = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  const spy = vi.spyOn(f.host.agents, "run").mockImplementation(async (_request, signal) => {
    entered(); signal!.addEventListener("abort", aborted, { once: true });
    await gate; throw new Error("Activation cancelled by stop");
  });
  const pending: Promise<unknown>[] = [];
  const observe = <T>(promise: Promise<T>) => {
    const outcome = promise.then(value => ({ value }), error => ({ error }));
    pending.push(outcome); return outcome;
  };
  try {
    const other = await f.host.actors.create({ name: "other during join", instructions: "Other", residency: "durable", model: "fixture/visible", tools: [] });
    await f.client.options.participants.refresh();
    observe(f.host.actors.ask(f.actor.id, "held until stop join")); await started;
    let stopped = false;
    const stop = observe(f.provider.invoke("stop", { id: f.actor.id }, f.context).then(value => { stopped = true; return value; }));
    await cancelled;
    expect(stopped).toBe(false);
    const status = observe(f.provider.invoke("actorStatus", { id: f.actor.id }, f.context));
    const unrelated = observe(f.provider.invoke("setInstructions", { id: other.id, instructions: "Updated while stop joins" }, f.context));
    expect(await promptly(Promise.all([status, unrelated]))).toMatchObject([
      { value: { id: f.actor.id, status: "stopped" } }, { value: { id: other.id } },
    ]);
    expect(stopped).toBe(false);
    release();
    expect(await promptly(stop)).toMatchObject({ value: { status: "stopped" } });
  } finally {
    release(); await f.host.actors.stop(f.actor.id, undefined, true);
    await Promise.all(pending); spy.mockRestore(); await f.close();
  }
}, 15_000);

it.each(["Main", "proxy"] as const)("%s bounded reset wait preserves a committed receipt and a late terminal cancellation", async kind => {
  const f = await fixture();
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const spy = vi.spyOn(f.host.agents, "run").mockImplementation(async (_request, signal) => {
    entered();
    await new Promise<void>(resolve => signal!.addEventListener("abort", () => resolve(), { once: true }));
    throw new Error("Activation cancelled by stop");
  });
  const abort = new AbortController();
  const run = f.host.actors.ask(f.actor.id, "pending activation").catch(error => error);
  try {
    await started;
    f.client.options.commandTimeoutMs = kind === "Main" ? 5_000 : 200;
    const proxy = new ResidentActorClient(f.config.meshRoot, f.config.rootId, 200);
    const reset = (kind === "Main"
      ? f.provider.invoke("resetSession", { id: f.actor.id }, { ...f.context, signal: abort.signal })
      : proxy.setActor({ operation: "resetSession", id: f.actor.id }, undefined, { identity: f.identity, hostId: f.identity.id }))
      .catch(error => error);
    await waitFor(() => decisions(f.config.residencyRoot).some(entry => entry.operation === "resetSession" && entry.state === "committed"));
    const decision = decisions(f.config.residencyRoot).find(entry => entry.operation === "resetSession")!;
    expect(await promptly(f.provider.invoke("actorStatus", { id: f.actor.id }, f.context))).toMatchObject({ id: f.actor.id });
    if (kind === "Main") abort.abort();
    expect(await promptly(reset)).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.actor.id, requestId: decision.requestId,
      residentOutcome: { state: "committed", operation: "resetSession", id: f.actor.id } });
    f.client.options.commandTimeoutMs = 5_000;
    expect(await promptly(f.provider.invoke("stop", { id: f.actor.id }, f.context))).toMatchObject({ status: "stopped" });
    await run;
    await waitFor(() => fs.readdirSync(path.join(f.config.residencyRoot, "processing")).length === 0);
    // The original bounded caller left after commit: retain its eventual definitive reply.
    expect(JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "responses", `${decision.requestId}.json`), "utf8")))
      .toMatchObject({ requestId: decision.requestId, ok: false, errorCode: "ACTOR_SESSION_RESET_CANCELLED" });
    expect(decisions(f.config.residencyRoot).find(entry => entry.requestId === decision.requestId)).toEqual(decision);
    expect(fs.readdirSync(path.dirname(f.actor.sessionFile!)).filter(name => name.endsWith(".bak"))).toEqual([]);
  } finally {
    abort.abort(); await f.host.actors.stop(f.actor.id, undefined, true); await run;
    spy.mockRestore(); await f.close();
  }
}, 15_000);

it("public discovery advertises direct repair rather than a stop prerequisite", async () => {
  const f = await fixture();
  try {
    const descriptor = await f.provider.describe("resetSession", f.context);
    const listed = (await f.provider.list({ query: "resetSession" }, f.context)).find(action => action.name === "resetSession");
    expect(listed).toEqual(descriptor);
    expect(descriptor!.description).not.toMatch(/stop[ -]first|idle run boundary/);
    expect(descriptor!.description).toMatch(/owning Main.*directly/);
    expect(descriptor!.description).toMatch(/activation.*settles.*fenced boundary/);
    expect(descriptor!.description).toMatch(/stop.*cancels work/);
  } finally { await f.close(); }
});

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
    // A history reset is not a resume command for an explicitly terminal actor.
    expect(await f.provider.invoke("resetSession", { id: f.actor.id }, f.context)).toMatchObject({ id: f.actor.id, status: "stopped" });
    expect(() => f.host.actors.ask(f.actor.id, "after terminal stop")).toThrow(/stopped/);
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
it("owning Main repairs an active resident without dropping queued work or stopping its identity", async () => {
  const f = await fixture(); let entered!: () => void; let release!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  const original = f.host.agents.run.bind(f.host.agents);
  const requests: Array<{ task: string; sessionId: string }> = [];
  const spy = vi.spyOn(f.host.agents, "run").mockImplementation(async (request, signal, ...rest) => {
    requests.push({ task: request.task, sessionId: JSON.parse(fs.readFileSync(request.sessionFile!, "utf8").split("\n")[0]!).id });
    if (requests.length === 1) { entered(); await gate; }
    return original(request, signal, ...rest);
  });
  try {
    const before = f.host.actors.status(f.actor.id);
    const run = f.host.actors.ask(f.actor.id, "held"); await started;
    const queued = f.host.actors.ask(f.actor.id, "retained queued delivery");
    void queued.catch(() => undefined);
    let settled = false;
    const reset = f.provider.invoke("resetSession", { id: f.actor.id }, f.context).then(value => { settled = true; return value; });
    // Attach before the baseline refusal to avoid an unhandled rejection in the red run.
    const outcome = reset.then(value => ({ value }), error => ({ error }));
    await new Promise(resolve => setTimeout(resolve, 150));
    expect(settled).toBe(false);
    expect(fs.readdirSync(path.dirname(f.actor.sessionFile!)).some(name => name.endsWith(".bak"))).toBe(false);
    release(); await run;
    const repaired = await outcome;
    expect(repaired).not.toHaveProperty("error");
    expect(repaired).toMatchObject({ value: { id: before.id, model: before.model, topics: before.topics, tools: before.tools } });
    await queued;
    await expect(f.host.actors.ask(f.actor.id, "new delivery")).resolves.toMatchObject({ direction: "out" });
    expect(requests).toHaveLength(3);
    expect(requests[1]!.task).toContain("retained queued delivery");
    expect(requests[2]!.task).toContain("new delivery");
    expect(requests[1]!.sessionId).not.toBe(requests[0]!.sessionId);
    expect(requests[2]!.sessionId).toBe(requests[1]!.sessionId);
    expect(f.host.actors.status(f.actor.id).status).not.toBe("stopped");
    expect(f.host.actors.instructions(f.actor.id)).toBe("Keep persona");
    expect(f.host.actors.messages(f.actor.id, 50).some(message => message.error?.includes("stopped while messages were queued"))).toBe(false);
  } finally { release(); await f.host.actors.stop(f.actor.id, undefined, true); spy.mockRestore(); await f.close(); }
});
