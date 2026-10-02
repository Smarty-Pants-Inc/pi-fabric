// Bundled by the process regression into a native executable Pi RPC stand-in.
// It invokes the real public provider and publishes through the real control plane.
import readline from "node:readline";
import { resolveFabricIdentity } from "../../src/main-agent-identity.js";
import { MainAgentController } from "../../src/main-agent.js";
import { MeshStore } from "../../src/mesh/store.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import { AgentsProvider } from "../../src/providers/agents-provider.js";

const send = (event: unknown) => process.stdout.write(JSON.stringify(event) + "\n");
let running = false;
let model = { provider: "fake", id: "fake" };
let thinkingLevel = "off";
const input = readline.createInterface({ input: process.stdin });
input.on("line", async line => {
  if (running || !line.trim()) return;
  const command = JSON.parse(line);
  // Resident hosts resolve an exact model before launching; implement only its admission RPCs.
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
  const request = JSON.parse(command.message);
  const sessionId = `child-${process.pid}`;
  const { identity, mainAgentId } = resolveFabricIdentity(sessionId);
  const mesh = new MeshStore(process.env.PI_FABRIC_MESH_ROOT!, 64 * 1024, 100);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: mainAgentId, identity });
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 10, acknowledgementTimeoutMs: 3_000 });
  const main = new MainAgentController({} as any, mainAgentId, false, process.cwd());
  const actors = { identity, status() { throw new Error("Unknown Fabric actor"); }, validateDirectMessage() {} };
  const provider = new AgentsProvider({ cwd: process.cwd(), status() { throw new Error("Unknown Fabric agent"); } } as any, actors as any, {} as any,
    main, directory, control, {} as any);
  const context = { cwd: process.cwd(), update() {}, activity() {} } as any;
  const outcome: any = { env: {
    spawnerId: process.env.PI_FABRIC_SPAWNER_ID, spawnerSessionId: process.env.PI_FABRIC_SPAWNER_SESSION_ID,
    marker: process.env.PI_FABRIC_TASK_PROCESS_CHILD, parentRun: process.env.PI_FABRIC_PARENT_RUN,
    actorId: process.env.PI_FABRIC_ACTOR_ID, actorName: process.env.PI_FABRIC_ACTOR_NAME,
    root: mainAgentId,
  }, sends: [] };
  try {
    await directory.start();
    control.start(() => ({ accepted: false }));
    outcome.main = await provider.invoke("main", {}, context);
    // The runtime must retain its binding even if ambient env changes after startup.
    process.env.PI_FABRIC_SPAWNER_ID = "session:wrong-after-startup";
    process.env.PI_FABRIC_TASK_ESCALATION_TARGETS = '["session:wrong-after-startup"]';
    outcome.mainAgain = await provider.invoke("main", {}, context);
    for (const target of request.targets) {
      try {
        const value = await provider.invoke(target.action, { id: target.id, message: target.message }, context);
        outcome.sends.push({ ...target, ok: true, value });
      } catch (error) {
        outcome.sends.push({ ...target, ok: false, error: (error as Error).message, code: (error as any).code });
      }
    }
  } catch (error) { outcome.error = (error as Error).message; }
  finally { await control.close(); await directory.close(); main.closeFollowUpDrain(); }
  const message = { role: "assistant", content: [{ type: "text", text: JSON.stringify(outcome) }],
    provider: model.provider, model: model.id, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 }, stopReason: "stop" };
  send({ type: "message_end", message }); send({ type: "turn_end", message, toolResults: [] });
  send({ type: "agent_end", messages: [message], willRetry: false }); send({ type: "agent_settled" });
});
process.stdin.on("end", () => process.exit(0));
