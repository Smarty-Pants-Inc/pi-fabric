import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import type { AgentRunResult } from "../src/agents/types.js";

const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.unstubAllEnvs(); }
});

// Production compiled activation worker -> native Pi CLI -> production compiled
// Fabric extension -> compiled child worker -> native Pi CLI. Only inference is
// substituted by a loopback endpoint. No transport, session, runtime or shutdown
// callback is mocked. This is offline native evidence, not signed-in fleet proof.
describe("native compiled actor activation-end result custody", () => {
  it.each([false, true])("retains a settled child at native activation end (notify=%s)", async (notify) => {
    expect(fs.existsSync("dist/worker.js"), "fresh build required").toBe(true);
    const selectedNativeBinary = process.env.PI_FABRIC_ACTIVATION_TEST_PI_BINARY;
    const piBinary = selectedNativeBinary ?? findExecutable("pi");
    expect(piBinary, "native Pi CLI required; this proof must not silently skip").toBeTruthy();
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-f10-native-")));
    cleanups.push(() => fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
    const meshRoot = path.join(root, "mesh");
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir);
    const full = { output: "native-private-value-".repeat(1000) };
    let childId = "";
    let releaseChild!: () => void;
    const childGate = new Promise<void>((resolve) => { releaseChild = resolve; });
    cleanups.push(() => releaseChild());
    let store!: ActorChildCompletionStore;
    let activationRequests = 0;
    let childRequests = 0;
    let handoffInferences = 0;
    const errors: unknown[] = [];
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", (data) => { body += data; });
      request.on("end", () => { void (async () => {
        const payload = JSON.parse(body);
        const send = (delta: unknown, finish: string) => {
          response.writeHead(200, { "Content-Type": "text/event-stream" });
          const chunk = (value: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
            id: "f10-offline", object: "chat.completion.chunk", created: 1, model: payload.model,
            choices: [{ index: 0, delta: value, finish_reason }],
          })}\n\n`);
          chunk(delta); chunk({}, finish); response.end("data: [DONE]\n\n");
        };
        if (payload.model === "child") {
          ++childRequests;
          // Complete only AFTER the spawn tool's safe turn boundary: a live
          // delivery would be real consumption and would not exercise F10.
          await childGate;
          send({ role: "assistant", content: JSON.stringify(full) }, "stop");
        } else if (++activationRequests === 1) {
          expect(payload.tools.some((tool: any) => tool.function?.name === "fabric_exec")).toBe(true);
          send({ role: "assistant", tool_calls: [{ index: 0, id: "spawn-private-child", type: "function", function: {
            name: "fabric_exec", arguments: JSON.stringify({ code: `return await agents.spawn({ task: "NATIVE-F10-CHILD", model: "f10-offline/child", transport: "process", extensions: false, tools: [], schema: { type: "object", properties: { output: { type: "string" } }, required: ["output"], additionalProperties: false } });` }),
          } }] }, "tool_calls");
        } else if (activationRequests === 2) {
          releaseChild();
          await vi.waitFor(() => {
            const archives = fs.readdirSync(store.directory).filter((file) => file.endsWith(".result.json"));
            expect(archives).toHaveLength(1);
            childId = archives[0]!.replace(/\.result\.json$/, "");
            const saved = JSON.parse(fs.readFileSync(store.resultFile(childId), "utf8")) as AgentRunResult;
            expect(saved.status, `${saved.error ?? ""} ${saved.logFile ? fs.readFileSync(saved.logFile, "utf8").slice(-4000) : ""}`).toBe("completed");
            expect(saved.value).toEqual(full);
          }, { timeout: 30000 });
          if (notify) {
            // A failed final turn suspends live notices. Native Pi still runs
            // session_shutdown when its one-shot activation closes stdin.
            response.writeHead(400, { "Content-Type": "application/json" });
            response.end(JSON.stringify({ error: { message: "offline activation ended after child settlement", type: "invalid_request_error" } }));
          } else send({ role: "assistant", content: "native activation completed with muted private child" }, "stop");
        } else {
          const current = [...payload.messages].reverse().find((message: any) => message.role === "user");
          const text = typeof current?.content === "string" ? current.content :
            (current?.content ?? []).map((part: any) => part.text ?? "").join("\n");
          if (text.includes(childId)) {
            ++handoffInferences;
            expect(text).toContain(JSON.stringify(store.resultFile(childId)));
            const saved = JSON.parse(fs.readFileSync(store.resultFile(childId), "utf8"));
            expect(saved.text).toBe(JSON.stringify(full));
            expect(saved.value).toEqual(full);
          }
          send({ role: "assistant", content: "native next activation consumed its context" }, "stop");
        }
      })().catch((error) => { errors.push(error); response.writeHead(500); response.end(String(error)); }); });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); }));
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "f10-offline": {
      baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions",
      models: ["activation", "child"].map((id) => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 65536,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { notifyOnComplete: notify, retainRuns: false, nice: 19, sessionExport: false }, mesh: { enabled: true, actorPollMs: 20 },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
    const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60000, sessionExport: false, nice: 19 }, {
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: piBinary!,
      runRoot: path.join(root, "runs"), mainAgentId: "session:native-root", fabricSessionId: "native-root", fullCodeMode: true,
    });
    cleanups.push(() => agents.close());
    const makeOwner = () => new ActorManager("native-root", { id: "session:native-root", name: "owner", kind: "main", sessionId: "native-root" },
      new MeshStore(meshRoot, 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        persistent: true, actorRoot: path.join(meshRoot, "actors", "native-root"), claimResidency: "session", rootId: "session:native-root",
        resolvePiModel: async (model) => model,
      });
    const owner = makeOwner();
    cleanups.push(() => owner.close());
    const actor = await owner.create({ name: "native-f10", instructions: "Keep child results private.", delivery: "mailbox", responseMode: "text",
      transport: "process", residency: "session", model: "f10-offline/activation", tools: [], nice: 19 });
    store = new ActorChildCompletionStore(actor.sessionFile!);
    await owner.close(); // Keep the owner from consuming before the archive assertion.
    const activation = await agents.run({ task: "Start native F10 activation", actorId: actor.id, actorName: actor.name,
      sessionFile: actor.sessionFile!, inferenceContext: "full-history", meshRoot, model: "f10-offline/activation", transport: "process",
      recursive: true, tools: [], extensions: true });
    expect(errors).toEqual([]);
    expect(activation, activation.logFile ? fs.readFileSync(activation.logFile, "utf8").slice(-10000) : JSON.stringify(activation))
      .toMatchObject({ status: notify ? "failed" : "completed" });
    expect(activationRequests).toBe(2);
    expect(childRequests).toBe(1);
    expect(childId).not.toBe("");
    expect(store.received(childId)).toBe(false);
    const saved = JSON.parse(fs.readFileSync(store.resultFile(childId), "utf8")) as AgentRunResult;
    expect(saved).toMatchObject({ status: "completed", text: JSON.stringify(full), value: full,
      spawner: { id: actor.id, kind: "actor", runId: activation.id } });
    expect(store.pending().map(({ result }) => result.id)).toEqual(notify ? [childId] : []);
    // retainRuns:false removes execution artifacts only after production close
    // has joined custody. This proves shutdown ran; process exit alone cannot.
    expect(fs.existsSync(path.dirname(saved.logFile!))).toBe(false);
    const restarted = makeOwner();
    cleanups.push(() => restarted.close());
    await restarted.ask(actor.id, "next native activation after owner restart");
    await vi.waitFor(() => {
      expect(restarted.status(actor.id).status).toBe("idle");
      expect(handoffInferences).toBe(notify ? 1 : 0);
    }, { timeout: 30000 });
    expect(restarted.messages(actor.id).filter((message) => message.id === childId && message.direction === "in")).toHaveLength(notify ? 1 : 0);
    expect(fs.existsSync(store.resultFile(childId))).toBe(!notify);
    await restarted.close();
    const again = makeOwner();
    cleanups.push(() => again.close());
    await again.ask(actor.id, "unrelated native activation after second owner restart");
    await vi.waitFor(() => expect(again.status(actor.id).status).toBe("idle"), { timeout: 30000 });
    expect(handoffInferences).toBe(notify ? 1 : 0);
    expect(errors).toEqual([]);
    expect(fs.existsSync(store.resultFile(childId))).toBe(!notify);
    // No fixture/API evidence is relabeled: this log names the native binary,
    // compiled activation, native session and full archived child sizes.
    console.info("F10 offline native activation-end proof", JSON.stringify({ piBinary, worker: "dist/worker.js", extension: "dist/index.js",
      activationId: activation.id, status: activation.status, actorId: actor.id, childId, nativeSession: activation.runnerSessionId,
      notify, handoffInferences, inferenceContext: "full-history", archivedTextBytes: saved.text.length, archivedValueBytes: full.output.length, received: store.received(childId) }));
  }, 90000);
});
