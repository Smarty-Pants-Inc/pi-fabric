import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn } from "node:child_process";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { readProcessIdentity, processStartIdentityState, type ProcessIdentity } from "../src/core/process-identity.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { hasUnsettledRecordedProcesses } from "../src/storage/worker-settlement.js";

// Real built worker, real native Pi and its detached native Bash implementation.
// Only the model is substituted with a credential-free loopback SSE endpoint.
it.skipIf(process.platform !== "linux")("S8: native Bash survives abrupt runner death; public successor removal stays unaccepted until durable tool reconciliation", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-tool-debt-"));
  const project = fs.realpathSync(process.cwd());
  const oldId = "session:tool-predecessor";
  const newId = "session:tool-successor";
  const pidFile = path.join(root, "native-bash-pid");
  const toolId = "native-bash-obligation";
  let requests = 0;
  const server = http.createServer((request, response) => {
    request.resume();
    request.on("end", () => {
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const delta = ++requests === 1 ? { role: "assistant", tool_calls: [{ index: 0, id: toolId, type: "function", function: {
        name: "bash", arguments: JSON.stringify({ command: `printf '%s' "$$" > '${pidFile}'; exec sleep 8`, timeout: 30 }),
      } }] } : { role: "assistant", content: "done" };
      const chunk = (delta: unknown, finish_reason: string | null) => response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", created: 1, model: "offline", choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
      chunk(delta, null); chunk({}, requests === 1 ? "tool_calls" : "stop"); response.end("data: [DONE]\n\n");
    });
  });
  const main = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  const mainExited = new Promise<void>(resolve => main.once("exit", () => resolve()));
  let host: ResidentHost | undefined;
  let actors: ActorDirectory | undefined;
  let agents: AgentManager | undefined;
  let client: ResidencyClient | undefined;
  let lifecycle: LifecycleBroker | undefined;
  let ask: Promise<unknown> | undefined;
  let mainIdentity: ProcessIdentity | undefined;
  let runner: ProcessIdentity | undefined;
  let tool: ProcessIdentity | undefined;
  const killOwned = (identity: ProcessIdentity): void => {
    if (processStartIdentityState(identity) === "alive") process.kill(identity.pid, "SIGKILL");
  };
  try {
    await new Promise<void>((resolve, reject) => { main.once("spawn", resolve); main.once("error", reject); });
    mainIdentity = readProcessIdentity(main.pid!)!;
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    const agent = path.join(root, "agent"); fs.mkdirSync(agent);
    fs.writeFileSync(path.join(agent, "models.json"), JSON.stringify({ providers: { "tool-offline": {
      baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions",
      models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    } } }));
    fs.writeFileSync(path.join(agent, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false } }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agent); vi.stubEnv("PI_OFFLINE", "1");
    const rootRecord = (id: string, processIdentity: ProcessIdentity): FabricParticipantInfo => ({
      format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: "main", agentName: "tool-lead", role: "project-agent", project,
      processIdentity, status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"], sessionId: id.slice(8), startedAt: 1, updatedAt: 1,
      controlProtocol: "v1", local: id === newId, stale: id !== newId,
    } as FabricParticipantInfo);
    const caller = rootRecord(newId, readProcessIdentity()!);
    const predecessor = rootRecord(oldId, mainIdentity);
    const meshRoot = path.join(root, "mesh");
    const actorRoot = path.join(meshRoot, "actors");
    const config: ResidentHostConfig = {
      format: 1, rootId: oldId, sessionId: oldId.slice(8), cwd: project, projectRoot: project, project, role: "project-agent", rootOwner: predecessor,
      meshRoot, actorRoot, sessionActorRoot: path.join(actorRoot, oldId.slice(8)), residencyRoot: residentRoot(meshRoot, oldId), fullCodeMode: false,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 30000, retainRuns: true, sessionExport: false, nice: 19 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: findExecutable("pi")!, claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "tool-offline", id: "offline" }], aliases: {} },
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    host = new ResidentHost(config);
    await host.start();
    const actor = await host.actors.create({ name: "native-tool-debt", instructions: "Use bash once.", residency: "durable", scope: "project", model: "tool-offline/offline", tools: ["bash"], extensions: false });
    ask = host.actors.ask(actor.id, "Run the requested native Bash sleep.").catch(error => error);
    const runRoot = path.join(config.residencyRoot, "runs");
    let dir = "";
    try {
      await vi.waitFor(() => {
        dir = path.join(runRoot, fs.readdirSync(runRoot)[0]!);
        expect(fs.readFileSync(path.join(dir, "events.jsonl"), "utf8")).toContain('"tool_execution_start"');
        expect(fs.existsSync(pidFile)).toBe(true);
      }, { timeout: 20000, interval: 50 });
    } catch (error) {
      console.error("native tool fixture", { requests, actor: host.actors.status(actor.id), files: fs.readdirSync(dir), status: fs.readFileSync(path.join(dir, "status.json"), "utf8") });
      throw error;
    }
    const eventsFile = path.join(dir, "events.jsonl");
    const before = fs.readFileSync(eventsFile, "utf8");
    const start = before.trim().split("\n").map(line => JSON.parse(line)).find(event => event.type === "tool_execution_start");
    expect(start.toolName).toBe("bash");
    const journal = fs.readFileSync(path.join(dir, "worker-processes.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)).reverse().find(row => row.runner);
    runner = journal.runner;
    tool = readProcessIdentity(Number(fs.readFileSync(pidFile, "utf8")))!;
    expect(tool).toBeTruthy();
    expect(processStartIdentityState(tool)).toBe("alive");
    const group = (pid: number) => fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]!.split(" ")[2];
    expect(group(tool.pid)).not.toBe(group(runner!.pid)); // Pi's native Bash is independently detached.
    killOwned(runner!);
    await vi.waitFor(() => expect(processStartIdentityState(journal.worker)).toBe("dead"), { timeout: 5000, interval: 25 });
    killOwned(mainIdentity); await mainExited;
    await ask;
    const settledLog = fs.readFileSync(eventsFile, "utf8"); // includes worker-flushed updates, not a tool end
    await host.close(); // exercise real clean-close receipt creation with dead worker + runner
    expect.soft(processStartIdentityState(tool)).toBe("alive");
    expect.soft(hasUnsettledRecordedProcesses(dir)).toBe(true);
    expect.soft(fs.existsSync(path.join(config.residencyRoot, "workers-settled.json"))).toBe(false);
    const unsettledFile = path.join(config.residencyRoot, "workers-unsettled.json");
    expect.soft(fs.existsSync(unsettledFile)).toBe(true);
    if (fs.existsSync(unsettledFile)) expect(JSON.parse(fs.readFileSync(unsettledFile, "utf8")).reason).toContain("tool settlement not proven");
    const mesh = host.mesh;
    const identity = { id: newId, name: "main", kind: "main" as const, sessionId: newId.slice(8) };
    const participants: FabricParticipantSource = { self: () => caller, get: id => id === newId ? caller : undefined,
      list: () => [caller], peers: () => [], refresh: async () => {}, scheduleRefresh() {} };
    agents = new AgentManager(project, config.agents, { workerPath: config.workerPath, runRoot: path.join(root, "successor-runs") });
    actors = new ActorDirectory([identity.sessionId, identity, mesh, config.mesh, agents, () => {},
      { persistent: true, claimResidency: "session", rootId: newId, project, role: "project-agent", canManageActor: () => undefined }],
      { project: actorRoot, session: path.join(actorRoot, identity.sessionId) }, "project");
    const successorConfig = { ...config, rootId: newId, sessionId: identity.sessionId, rootOwner: caller, residencyRoot: residentRoot(meshRoot, newId), sessionActorRoot: path.join(actorRoot, identity.sessionId) };
    const mainAgent = { id: newId, local: true } as FabricMainAgentTarget;
    client = new ResidencyClient({ config: successorConfig, mesh, participants, mainAgent });
    vi.spyOn(client, "removeActor").mockRejectedValue(new Error(`Resident host does not own ${actor.id}`));
    lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants, undefined, lifecycle, () => false, client, true);
    const remove = () => provider.invoke("remove", { id: actor.id, successor: true }, { cwd: project, update() {} } as unknown as FabricInvocationContext);
    const actorDir = path.dirname(actor.sessionFile!);
    const registryRoot = path.dirname(actorDir);
    const ownershipBefore = fs.readFileSync(path.join(registryRoot, "actors.json"), "utf8");
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    try {
      expect.soft(await remove()).toMatchObject({ removed: false, pending: expect.stringContaining("tool settlement not proven") });
      expect.soft(write).not.toHaveBeenCalled();
      expect.soft(fs.existsSync(actorDir)).toBe(true);
      expect.soft(fs.readFileSync(path.join(registryRoot, "actors.json"), "utf8")).toBe(ownershipBefore);
      expect.soft(mesh.get(`actor-removals/${actor.id}`)).toBeUndefined();
      expect.soft(fs.readFileSync(eventsFile, "utf8")).toBe(settledLog);
      if (!fs.existsSync(actorDir)) return; // baseline red still waits for every owned process in finally
      await vi.waitFor(() => expect(processStartIdentityState(tool!)).toBe("dead"), { timeout: 15000, interval: 50 });
      // Process exit alone remains insufficient: production does not infer or scan descendants.
      expect(await remove()).toMatchObject({ removed: false, pending: expect.stringContaining("tool settlement not proven") });
      // Fixture-owned reconciliation: only after the recorded native shell/group has naturally
      // exited (the shell execs sleep, so this is the whole process group), append its
      // exact completion obligation. No status-only or ancestor-PID proof.
      const completion = JSON.stringify({ type: "tool_execution_end", toolCallId: start.toolCallId, toolName: start.toolName, isError: true, result: { content: [{ type: "text", text: "Owned native Bash exited after runner SIGKILL" }] } }) + "\n";
      fs.appendFileSync(eventsFile, completion);
      // Actor retention copied this activation on worker failure. Reconciliation must
      // preserve the same completion in that retained evidence, not discard the archive.
      fs.appendFileSync(path.join(actorDir, "runs", path.basename(dir), "events.jsonl"), completion);
      expect(hasUnsettledRecordedProcesses(dir)).toBe(false);
      await expect(Promise.all([remove(), remove()])).resolves.toEqual([{ removed: true }, { removed: true }]);
      expect(write.mock.calls.filter(([rows]) => !rows.some(row => row.id === actor.id))).toHaveLength(1);
      expect(fs.existsSync(actorDir)).toBe(false);
      expect(mesh.get(`actor-removals/${actor.id}`)?.updatedBy.id).toBe(newId);
    } finally { write.mockRestore(); }
  } finally {
    if (runner) killOwned(runner);
    if (mainIdentity) killOwned(mainIdentity); // never recapture a possibly reused numeric PID
    await mainExited;
    // The native tool is short and inert: let its entire detached group exit naturally, even on red.
    if (tool) await vi.waitFor(() => expect(processStartIdentityState(tool!)).toBe("dead"), { timeout: 15000 });
    await ask;
    await host?.close(); await client?.close(); await lifecycle?.close(); await actors?.close(); await agents?.close();
    await new Promise<void>(resolve => server.close(() => resolve()));
    vi.restoreAllMocks(); vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
}, 45000);
