// Bundled by tests/task-spawn-process.test.ts into a native executable Pi RPC stand-in (#12187).
// Modeled: only the Pi RPC framing and the "model turn" (a deterministic QuickJS guest program
// carried in the prompt replaces inference). Real: the source worker that launched this process,
// its launch environment, AgentManager, AgentsProvider, ParticipantDirectory, control plane, and
// the QuickJS guest runtime with its host error bridge. Children spawned by the guest run through
// the same real manager -> worker -> this stand-in path.
import readline from "node:readline";
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
const parseTask = (message: string): { guest: string; label?: string } => {
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
    meshRoot: process.env.PI_FABRIC_MESH_ROOT!, mainAgentId, identityId: identity.id, spawnerSessionId: sessionId,
  });
  const actors = { identity, mesh, status() { throw new Error("Unknown Fabric actor"); }, validateDirectMessage() {} };
  const provider = new AgentsProvider(manager, actors as any, {} as any, main, directory, control, {} as any);
  // A task cannot grant itself spawning after launch: the admission snapshot was taken above.
  process.env.PI_FABRIC_GRANTED_RISKS = "agent";
  const context = { cwd: process.cwd(), update() {}, activity() {},
    extensionContext: { cwd: process.cwd(), model: undefined, modelRegistry: { getAvailable: () => [], getAll: () => [], find: () => undefined },
      sessionManager: { getEntries: () => [], getSessionId: () => sessionId } } } as any;
  try {
    await directory.start();
    control.start(() => ({ accepted: false }));
    const result = await new QuickJsRuntime().execute(task.guest,
      async (ref, args) => {
        if (!ref.startsWith("agents.")) throw new Error(`fixture exposes only agents.*, not ${ref}`);
        return provider.invoke(ref.slice("agents.".length), args, context);
      }, { timeoutMs: 40_000, memoryLimitBytes: 64 * 1024 * 1024 });
    outcome.guest = result.value;
    if (result.error) outcome.guestError = result.error;
    // Host-side observation of children this task's real manager actually created.
    outcome.children = manager.list().map((record: any) => ({ id: record.id, status: record.status }));
  } catch (error) { outcome.error = (error as Error).message; }
  finally {
    await manager.close(); await control.close(); await directory.close(); main.closeFollowUpDrain();
  }
  const message = { role: "assistant", content: [{ type: "text", text: JSON.stringify(outcome) }],
    provider: model.provider, model: model.id, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 }, stopReason: "stop" };
  send({ type: "message_end", message }); send({ type: "turn_end", message, toolResults: [] });
  send({ type: "agent_end", messages: [message], willRetry: false }); send({ type: "agent_settled" });
});
process.stdin.on("end", () => process.exit(0));
