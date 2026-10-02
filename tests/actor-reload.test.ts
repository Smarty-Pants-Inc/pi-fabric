import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";
import { fauxProvider } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ActionRegistry } from "../src/core/action-registry.js";

const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks(); vi.unstubAllEnvs();
});
const waitFor = async (predicate: () => boolean, ms = 10_000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for reload workers");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

describe("actor workers across native in-process extension reload (#3167)", () => {
  it("re-adopts queued and admitted-but-unlaunched work once even when catalog rearm races reload finalization", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-native-reload-")));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      mesh: { enabled: true, announce: true, actorPollMs: 20 },
      agents: { maxConcurrent: 1, transport: "process" },
      memory: { enabled: false }, jev: { enabled: false }, prewalk: { alwaysRearm: false },
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", path.join(root, "mesh"));
    const states: FabricState[] = [];
    const ensure = FabricState.prototype.ensure;
    vi.spyOn(FabricState.prototype, "ensure").mockImplementation(async function (this: FabricState, context) {
      if (!states.includes(this)) states.push(this);
      return ensure.call(this, context);
    });
    const launched: string[] = [];
    const launch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      launched.push(fs.readFileSync(request.workerArguments[request.workerArguments.indexOf("--task-file") + 1]!, "utf8"));
      return launch.call(this, { ...request, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    });
    const faux = fauxProvider();
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "unused-auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{ name: "pi-fabric-source", factory: piFabric }],
    });
    await loader.reload();
    const { session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(root) });
    cleanups.push(async () => { for (const state of states) await state.shutdown("exit"); session.dispose(); });
    const errors: unknown[] = [];
    await session.bindExtensions({ onError: (error) => { errors.push(error); } });
    expect(errors).toEqual([]);
    expect(states).toHaveLength(1);
    const before = states[0]!;
    const oldAgents = before.agents;
    const blocker = await oldAgents.spawn({ task: "HANG blocker", transport: "process" });
    const actors = await Promise.all((["project", "session"] as const).map((scope) => before.actors.create({
      name: `reload-${scope}`, scope, instructions: "Reply", responseMode: "text", coalesce: false, requires: ["agents.list"],
    })));
    for (const actor of actors) {
      before.actors.tell(actor.id, `${actor.name}-admitted`);
      before.actors.tell(actor.id, `${actor.name}-queued`);
    }
    await waitFor(() => actors.every((actor) => before.actors.status(actor.id).status === "waiting" && before.actors.status(actor.id).queued === 1));
    const staleRunIds = actors.map((actor) => before.actors.status(actor.id).preparing!.runId!);
    expect(launched).toHaveLength(1); // Only the blocker has a process in the old host.
    // Restored work can reach a still-building catalog before its provider registers. Hold
    // that first unsatisfied lease's finalization while a real catalog notification arrives.
    // This is the reload lost-wake window: ensureDrain sees the old drain and ignores rearm.
    let release!: () => void;
    const finalization = new Promise<void>((done) => { release = done; });
    cleanups.push(() => release());
    let missed = 0;
    const acquire = ActionRegistry.prototype.acquireCapabilityView;
    vi.spyOn(ActionRegistry.prototype, "acquireCapabilityView").mockImplementation(async function (this: ActionRegistry, requirements, context) {
      if (requirements.some((requirement) => (typeof requirement === "string" ? requirement : requirement.ref) === "agents.list") && missed++ < actors.length) {
        return { satisfied: false, missing: ["agents.list"], optionalMissing: [], release: () => finalization };
      }
      return acquire.call(this, requirements, context);
    });
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ theme: "dark" }));
    await session.reload();
    expect(errors).toEqual([]);
    expect(states).toHaveLength(2);
    const after = states[1]!;
    await waitFor(() => actors.every((actor) => after.actors.status(actor.id).missingCapabilities?.includes("agents.list")));
    await new Promise((resolve) => setTimeout(resolve, 1_000)); // Drain first-use startup catalog notifications before the controlled final one.
    after.registry.notifyCatalogChanged("agents");
    await new Promise((resolve) => setTimeout(resolve, 20)); // Let the retry reach the still-active drains.
    expect(launched).toHaveLength(1);
    expect(after.actors.inFlightCount()).toBe(2); // Both re-adopted drains still own finalization, but no actor worker has launched.
    release();
    await waitFor(() => actors.every((actor) => after.actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error).length === 2));
    await new Promise((resolve) => setTimeout(resolve, 200));
    for (const actor of actors) {
      expect(launched.filter((task) => task.includes(`${actor.name}-admitted`))).toHaveLength(1);
      expect(launched.filter((task) => task.includes(`${actor.name}-queued`))).toHaveLength(1);
      expect(after.actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
      expect(after.actors.status(actor.id).inFlightRun).toBeUndefined();
      expect(fs.readdirSync(path.dirname(actor.sessionFile!)).filter((file) => file.startsWith("queue-"))).toEqual([]);
    }
    for (const id of staleRunIds) expect(oldAgents.status(id).status).toBe("stopped");
    expect(oldAgents.status(blocker.id).status).toBe("stopped");
  }, 60_000);
});
