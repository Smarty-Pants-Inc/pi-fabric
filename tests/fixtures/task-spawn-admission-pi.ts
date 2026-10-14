// Bundled by tests/task-spawn-process.test.ts into a native executable Pi RPC stand-in (#12187).
// Modeled: only the Pi RPC framing and the "model turn" (a deterministic QuickJS guest program
// carried in the prompt replaces inference). Real: the source worker that launched this process,
// its launch environment, AgentManager, AgentsProvider, ParticipantDirectory, control plane, and
// the QuickJS guest runtime with its host error bridge. Children spawned by the guest run through
// the same real manager -> worker -> this stand-in path.
import readline from "node:readline";
import fs from "node:fs";
import path from "node:path";
import { ActorManager } from "../../src/actors/manager.js";
import { GlobalActorRegistry } from "../../src/actors/global-registry.js";
import { resolveFabricIdentity } from "../../src/main-agent-identity.js";
import { MainAgentController } from "../../src/main-agent.js";
import { MeshStore } from "../../src/mesh/store.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import { AgentsProvider } from "../../src/providers/agents-provider.js";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { QuickJsRuntime } from "../../src/runtime/quickjs-runtime.js";

export const TASK_MARKER = "ADMISSION-TASK:";
const send = (event: unknown) => process.stdout.write(JSON.stringify(event) + "\n");
let running = false;
let model = { provider: "fake", id: "fake" };
let thinkingLevel = "off";

// The manager may decorate the prompt; the deterministic program is one marked JSON line.
type Task = { guest: string; label?: string; directHandoff?: boolean; runDeferredHandoff?: boolean;
  realActorFlow?: boolean; existingActors?: boolean; tamperIdentity?: boolean };
const FAKE_MODEL = { provider: "fake", id: "fake", api: "openai-completions", name: "fake", reasoning: false,
  input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024 };
// Minimal AgentSessionSeed for the modeled outer fabric_exec result boundary (no paid inference).
const seed = (sessionId: string) => ({ sourceSessionId: sessionId, sourceBranchLeafId: "seed-leaf",
  sourceBranch: [{ type: "message", id: "seed-leaf", parentId: null, timestamp: new Date(0).toISOString(),
    message: { role: "user", content: "handoff seed", timestamp: 0 } }],
  sourceModel: { provider: "fake", modelId: "fake" },
  outerToolResult: { role: "toolResult", toolCallId: "seed-call", toolName: "fabric_exec",
    content: [{ type: "text", text: "handoff scheduled" }], isError: false, timestamp: 0 } }) as any;
const refusal = (error: any) => ({ ok: false, name: error?.name, code: error?.code, message: String(error?.message ?? error) });
const parseTask = (message: string): Task => {
  // Real ActorManager wraps direct messages in its JSON payload; decode before reading the marker.
  if (message.startsWith("Fabric actor message from direct:\n\n")) {
    return parseTask(JSON.parse(message.slice(message.indexOf("\n\n") + 2)).payload.message);
  }
  const start = message.indexOf(TASK_MARKER);
  if (start < 0) return { guest: "return null;" };
  const line = message.slice(start + TASK_MARKER.length).split("\n")[0]!;
  return JSON.parse(line);
};

const input = readline.createInterface({ input: process.stdin });
input.on("line", async line => {
  if (running || !line.trim()) return;
  const command = JSON.parse(line);
  if (["get_state", "set_model", "set_thinking_level"].includes(command.type)) {
    if (command.type === "set_model") model = { provider: command.provider, id: command.modelId };
    if (command.type === "set_thinking_level") thinkingLevel = command.level;
    send({ type: "response", id: command.id, command: command.type, success: true,
      data: command.type === "get_state" ? { model, thinkingLevel, isStreaming: false, isCompacting: false } : {} });
    return;
  }
  if (command.type !== "prompt") return;
  running = true;
  send({ type: "response", command: "prompt", success: true });
  send({ type: "agent_start" });
  const task = parseTask(String(command.message));
  const sessionId = `child-${process.pid}`;
  const { identity, mainAgentId } = resolveFabricIdentity(sessionId);
  const outcome: any = {
    label: task.label, identity: { id: identity.id, kind: identity.kind },
    // Launch grant exactly as worker.ts exported it, before the tamper below.
    grantedRisks: process.env.PI_FABRIC_GRANTED_RISKS ?? null,
    parentRun: process.env.PI_FABRIC_PARENT_RUN ?? null,
  };
  const mesh = new MeshStore(process.env.PI_FABRIC_MESH_ROOT!, 64 * 1024, 100);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: mainAgentId, identity });
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 10, acknowledgementTimeoutMs: 3_000 });
  const main = new MainAgentController({} as any, mainAgentId, false, process.cwd());
  // Real nested manager: inherits PI_FABRIC_RUN_ROOT / DEPTH / PI binary from the real worker env.
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [],
    maxDepth: 10, nice: 0, timeoutMs: 20_000 }, {
    workerPath: process.env.PI_FABRIC_TEST_ADMISSION_WORKER!, fabricExtensionPath: process.env.PI_FABRIC_TEST_ADMISSION_EXTENSION!,
    meshRoot: process.env.PI_FABRIC_MESH_ROOT!, mainAgentId, hostId: identity.id, identityId: identity.id, spawnerSessionId: sessionId,
  });
  // MODELED actor lifecycle (Decision C): a recording stand-in, not a real ActorManager. Every
  // method touch is counted so a refused leaf can be shown to reach no actor state at all.
  const actorCalls: string[] = [];
  const actors = new Proxy({ identity, mesh } as any, { get(target, key) {
    if (typeof key !== "string" || key in target || key === "then") return target[key as any];
    return (..._args: unknown[]) => {
      actorCalls.push(key);
      if (key === "owns") return false;
      if (key === "validateDirectMessage") return undefined;
      if (key === "status") throw new Error(`Unknown Fabric actor: ${_args[0]}`);
      throw new Error(`modeled actor stand-in: ${key}`);
    };
  } });
  outcome.actorCalls = actorCalls;
  // realActorFlow: REAL actor lifecycle (no stand-in) - actual ActorManager + GlobalActorRegistry over
  // this task's real AgentManager, persistent actor root in a unique child directory of the run's mesh root.
  let realActors: ActorManager | undefined;
  let globalActors: any = {};
  if (task.realActorFlow) {
    const actorRoot = path.join(process.env.PI_FABRIC_MESH_ROOT!, "..", `actors-${process.pid}-${Date.now()}`);
    fs.mkdirSync(actorRoot, { recursive: true });
    realActors = new ActorManager(sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager,
      () => {}, { actorRoot, persistent: true, actorScope: "project", mainAgent: main as any, rootId: mainAgentId });
    globalActors = new GlobalActorRegistry(actorRoot, 64 * 1024);
    if (task.existingActors) {
      await realActors.create({ name: "actor-existing-12187", instructions: "Do deterministic task", residency: "session", extensions: false });
      await realActors.create({ name: "actor-dormant-12187", instructions: "Do deterministic task", residency: "durable", extensions: false }, { asRegistryOwner: true });
      await realActors.dormantIdleActors();
      outcome.actorsBefore = realActors.list();
    }
  }
  const provider = new AgentsProvider(manager, (realActors ?? actors) as any, globalActors, main, directory, control, {} as any);
  // A task cannot grant itself spawning after launch: the admission snapshot was taken above.
  process.env.PI_FABRIC_GRANTED_RISKS = "agent";
  if (task.tamperIdentity) {
    process.env.PI_FABRIC_ACTOR_ID = "forged-actor";
    delete process.env.PI_FABRIC_PARENT_RUN;
    process.env.PI_FABRIC_SPAWNER_ID = "actor-existing-12187";
  }
  const deferred: Array<Record<string, unknown>> = [];
  const context = { cwd: process.cwd(), update() {}, activity() {},
    deferHandoff: async (args: Record<string, unknown>) => { deferred.push(args); return { scheduled: true }; },
    extensionContext: { cwd: process.cwd(), model: FAKE_MODEL, modelRegistry: { getAvailable: () => [FAKE_MODEL], getAll: () => [FAKE_MODEL],
      find: (provider: string, id: string) => provider === "fake" && id === "fake" ? FAKE_MODEL : undefined },
      sessionManager: { getEntries: () => [], getSessionId: () => sessionId } } } as any;
  try {
    await directory.start();
    outcome.routedReports = [];
    control.start(async (command, from, signal, verification) => {
      const accepted = await provider.acceptControl(command, from, signal, verification);
      if (command.message === "bound-spawner-report") {
        outcome.routedReports.push({ targetId: command.targetId, from: from.id, ...accepted,
          ...(realActors ? { mailbox: realActors.status(command.targetId) } : {}) });
      }
      return accepted;
    });
    const result = await new QuickJsRuntime().execute(task.guest,
      async (ref, args) => {
        if (ref === "fabric.$call") {
          ref = String(args.ref);
          args = (args.args ?? {}) as Record<string, unknown>;
        }
        if (!ref.startsWith("agents.")) throw new Error(`fixture exposes only agents.*, not ${ref}`);
        const action = ref.slice("agents.".length);
        return provider.invoke(action, provider.guardArguments(action, args), context);
      }, { timeoutMs: 40_000, memoryLimitBytes: 64 * 1024 * 1024 });
    outcome.guest = result.value;
    if (result.error) outcome.guestError = result.error;
    outcome.deferredHandoffs = deferred.length;
    // Modeled outer boundary (FabricRuntimeState.runHandoffAtBoundary): the same provider's
    // executeHandoff, invoked after the guest returned. Direct mode probes it with no deferral.
    const boundary = task.directHandoff ? [{ model: "fake/fake", task: "direct handoff" }]
      : task.runDeferredHandoff ? deferred : [];
    if (boundary.length) {
      try {
        const before = new Set(manager.list().map((record: any) => record.id));
        // executeHandoff waits for the child internally; the child id is taken from the real manager.
        const compact: any = await provider.executeHandoff({ ...boundary[0]!, recursive: true }, context, seed(sessionId));
        const id = manager.list().map((record: any) => record.id).find((candidate: string) => !before.has(candidate));
        const waited: any = id ? await manager.wait(id) : {};
        outcome.handoff = { ok: true, id, compactStatus: compact?.status, status: waited.status, text: waited.text, error: waited.error };
      } catch (error) { outcome.handoff = refusal(error); }
    }
    // Host-side observation of children this task's real manager actually created.
    outcome.children = manager.list().map((record: any) => ({ id: record.id, status: record.status }));
    if (realActors) outcome.actorsAfter = realActors.list();
  } catch (error) { outcome.error = (error as Error).message; }
  finally {
    if (realActors) await realActors.close();
    await manager.close(); await control.close(); await directory.close(); main.closeFollowUpDrain();
  }
  const message = { role: "assistant", content: [{ type: "text", text: JSON.stringify(outcome) }],
    provider: model.provider, model: model.id, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 }, stopReason: "stop" };
  send({ type: "message_end", message }); send({ type: "turn_end", message, toolResults: [] });
  send({ type: "agent_end", messages: [message], willRetry: false }); send({ type: "agent_settled" });
});
process.stdin.on("end", () => process.exit(0));
