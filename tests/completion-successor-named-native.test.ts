import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, type AgentSession } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ResidencyClient } from "../src/residency/client.js";
import { AGENT_COMPLETION_MESSAGE_TYPE } from "../src/agents/completion-inbox.js";
import { completionConsumed, saveCompletion, saveWorkerCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";

// #3178, native Pi: real AgentSession + SessionManager name (`pi --name`).
// The old expectation transferred named A's results to equally named B without adoption.
// Names remain metadata; only resuming A's exact persistent session grants access.
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks(); vi.unstubAllEnvs();
});
const waitFor = async (predicate: () => boolean, ms = 10_000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

describe("named lane succession in native Pi sessions", () => {
  it("equal-name new root receives nothing; exact resumed session receives ordinary and durable results once", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-named-native-")));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir, { recursive: true });
    const meshRoot = path.join(root, "mesh");
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      fullCodeMode: false, mesh: { enabled: true, announce: true, actorPollMs: 20 },
      agents: { transport: "process", nice: 19, sessionExport: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
    }));
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
    const states: FabricState[] = [];
    const ensure = FabricState.prototype.ensure;
    vi.spyOn(FabricState.prototype, "ensure").mockImplementation(async function (this: FabricState, context) {
      if (!states.includes(this)) states.push(this);
      return ensure.call(this, context);
    });
    const launch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      return launch.call(this, { ...request, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    });
    const clients: ResidencyClient[] = [];
    const start = ResidencyClient.prototype.start;
    vi.spyOn(ResidencyClient.prototype, "start").mockImplementation(function (this: ResidencyClient) {
      clients.push(this); return start.call(this);
    });
    const faux = fauxProvider();
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "unused-auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const open = async (name?: string, sessionFile?: string) => {
      const loader = new DefaultResourceLoader({ cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true,
        noContextFiles: true, extensionFactories: [{ name: "pi-fabric-source", factory: piFabric }] });
      await loader.reload();
      const sessionManager = sessionFile ? SessionManager.open(sessionFile, path.join(root, "sessions")) : SessionManager.create(root, path.join(root, "sessions"));
      if (name) sessionManager.appendSessionInfo(name);           // what `pi --name` does before binding
      const { session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, model: faux.getModel(),
        resourceLoader: loader, sessionManager });
      const before = states.length;
      await session.bindExtensions({ mode: "rpc" });                // as `pi --mode rpc` binds (interactive lane)
      await waitFor(() => states.length > before);
      const state = states[states.length - 1]!;
      let closed = false;
      const close = async () => { if (closed) return; closed = true; await state.shutdown("exit"); session.dispose(); };
      cleanups.push(close);
      return { session, state, close, sessionFile: sessionManager.getSessionFile()!, id: `session:${sessionManager.getSessionId()}` };
    };
    const bodies = (session: AgentSession) => session.messages.filter((message) =>
      message.role === "custom" && (message as { customType?: string }).customType === AGENT_COMPLETION_MESSAGE_TYPE).map(m => JSON.stringify(m));
    const nudge = async (session: AgentSession) => {
      faux.setResponses(Array.from({ length: 6 }, () => fauxAssistantMessage("ok")));
      await session.prompt("tick").catch(() => undefined);
      await waitFor(() => !session.isStreaming);
      // Async journal reads need a real event-loop turn. Faux prompts can finish
      // entirely in microtasks; spinning them starves I/O until the deadline.
      await new Promise(resolve => setTimeout(resolve, 20));
    };

    const a = await open("probe-lane");
    const runtimeA = (a.state as unknown as { agents: { spawn(r: unknown): Promise<{ id: string }>; wait(id: string): Promise<unknown> } });
    const handle = await runtimeA.agents.spawn({ task: "ordinary", transport: "process" });
    await runtimeA.agents.wait(handle.id);
    const manifest = JSON.parse(fs.readFileSync(path.join(root, "runs", handle.id, "completion-recipient.json"), "utf8")) as { recipient: CompletionRecipient };
    expect(manifest.recipient).toMatchObject({ rootId: a.id, name: "probe-lane" });
    const resident = clients.find(client => client.options.config.rootId === a.id)!;
    expect(resident.options.config.mainName).toBe("probe-lane");
    await nudge(a.session);                                         // flush A's session file like a real lane
    await a.close();

    const result = (id: string, text: string): AgentRunResult => ({ id, name: `child ${id.slice(0, 4)}`, task: "work", status: "completed",
      runner: "pi", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
      text, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
    const ordinary = result("e".repeat(32), "NATIVE_ORDINARY_NAMED");
    const durable = result("f".repeat(32), "NATIVE_DURABLE_NAMED");
    const run = path.join(root, "orphan-run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot, recipient: manifest.recipient, supervisor: { pid: 2147483647 } }));
    saveWorkerCompletion(path.join(run, "status.json"), ordinary);
    saveCompletion(meshRoot, { ...manifest.recipient, name: resident.options.config.mainName!, startedAt: resident.options.config.mainStartedAt! }, durable);

    await new Promise((resolve) => setTimeout(resolve, 20));
    const unnamed = await open();
    const other = await open("other-lane");
    await new Promise((resolve) => setTimeout(resolve, 400));
    await nudge(unnamed.session); await nudge(other.session);
    await new Promise((resolve) => setTimeout(resolve, 300));
    for (const bystander of [unnamed, other]) expect(bystander.session.messages.length > 0 && bodies(bystander.session)).toEqual([]);
    for (const value of [ordinary, durable]) expect(completionConsumed(meshRoot, value.id)).toBe(false);

    const successor = await open("probe-lane");
    // #3178: B is not a successor record; name equality grants neither delivery nor receipt.
    for (let tick = 0; tick < 5; tick++) await nudge(successor.session);
    expect(bodies(successor.session)).toEqual([]);
    for (const value of [ordinary, durable]) expect(completionConsumed(meshRoot, value.id)).toBe(false);

    const returned = await open("renamed-owner", a.sessionFile);
    expect(returned.id).toBe(a.id);
    const deadline = Date.now() + 15_000;
    while (!(bodies(returned.session).some(b => b.includes(ordinary.text!)) && bodies(returned.session).some(b => b.includes(durable.text!)))) {
      if (Date.now() > deadline) throw new Error(`owner did not receive: ${JSON.stringify(bodies(returned.session))}`);
      await nudge(returned.session);
    }
    await waitFor(() => [ordinary, durable].every(value => completionConsumed(meshRoot, value.id)), 15_000);
    await nudge(returned.session); await nudge(successor.session); await nudge(unnamed.session); await nudge(other.session);
    for (const value of [ordinary, durable]) expect(bodies(returned.session).filter(b => b.includes(value.text!))).toHaveLength(1);
    for (const bystander of [unnamed, other, successor]) expect(bodies(bystander.session)).toEqual([]);
  }, 90_000);
});
