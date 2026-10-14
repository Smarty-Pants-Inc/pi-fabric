import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { withRootExceptionMetadata, writeModelExceptions } from "./helpers/model-exceptions.js";

const roots: string[] = [];
const actorManagers: ActorManager[] = [];
const agentManagers: AgentManager[] = [];
const hasPosixTrustMetadata = process.platform !== "win32" && typeof process.getuid === "function";
const identity = { id: "session:root", name: "Main", kind: "main" as const, sessionId: "root" };
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for resident activation");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const terminal = (request: AgentRunRequest): AgentRunResult => ({
  id: "safe-first-run", name: request.name ?? "resident", task: request.task, status: "completed",
  runner: "pi", transport: "process", cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(),
  turns: 1, toolCalls: 0, text: "done", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});

afterEach(async () => {
  await Promise.all(actorManagers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentManagers.splice(0).map((manager) => manager.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("resident protected model exception bindings", () => {
  it.runIf(hasPosixTrustMetadata)("keeps model-only owner-default overrides thinking-dynamic through persisted admission", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-model-binding-")); roots.push(root);
    const rootCwd = path.join(root, "privatefixture"); fs.mkdirSync(rootCwd);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    const protectedModel = "anthropic/claude-sonnet-4-5";
    const modelReason = "Bounded resident exception regression #11864";
    const grantPath = writeModelExceptions(agentDir, [protectedModel], rootCwd);
    const grantDocument = JSON.parse(fs.readFileSync(grantPath, "utf8"));
    grantDocument.grants[0].role = "actor:serial";
    grantDocument.grants[0].thinking = "medium";
    fs.writeFileSync(grantPath, JSON.stringify(grantDocument), { mode: 0o600 });

    const agents = new AgentManager(rootCwd, {
      ...DEFAULT_FABRIC_CONFIG.agents, model: "provider/config", thinking: "low", modelExceptionsPath: grantPath,
    }, { runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), rootSessionCwd: () => rootCwd });
    agentManagers.push(agents);
    const realAdmit = agents.admitModelException.bind(agents);
    const denials: unknown[] = [];
    vi.spyOn(agents, "admitModelException").mockImplementation((...args) => {
      try { return withRootExceptionMetadata(grantPath, () => realAdmit(...args)); }
      catch (error) { denials.push(error); throw error; }
    });

    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const actorRoot = path.join(root, "actors");
    const actors = new ActorManager("root", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 10 }, agents, () => {},
      { actorRoot, persistent: true });
    actorManagers.push(actors);
    const requests: AgentRunRequest[] = [];
    const protectedSpawned: AgentRunRequest[] = [];
    let releaseFirst = () => {};
    const firstHeld = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const realRun = agents.run.bind(agents);
    vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned, ...callbacks) => {
      requests.push(structuredClone(request));
      if (requests.length === 1) {
        await firstHeld;
        return terminal(request);
      }
      const recordSpawn = (handle: Parameters<NonNullable<typeof onSpawned>>[0]) => {
        if (request.model === protectedModel) protectedSpawned.push(structuredClone(request));
        onSpawned?.(handle);
      };
      return realRun(request, signal, recordSpawn, ...callbacks);
    });

    try {
      const actor = await actors.create({ name: "serial", instructions: "Review.", model: "provider/safe", thinking: "low", protected: true, residency: "durable" });
      actors.tell(actor.id, "hold safe activation");
      await waitFor(() => requests.length === 1);
      const modelOnly = actors.tell(actor.id, "model-only protected override", undefined, { overrides: { model: protectedModel, modelReason } });
      const explicitLow = actors.tell(actor.id, "explicit-low protected override", undefined, { overrides: { model: protectedModel, modelReason, thinking: "low" } });
      await waitFor(() => actors.status(actor.id).queued === 2);

      const queueFile = fs.readdirSync(path.join(actorRoot, actor.id)).find((name) => name.startsWith("queue-"))!;
      const persisted = JSON.parse(fs.readFileSync(path.join(actorRoot, actor.id, queueFile), "utf8"));
      expect(persisted.items.find((item: { id: string }) => item.id === modelOnly.messageId)?.binding)
        .toEqual({ model: protectedModel, modelReason });
      expect(persisted.items.find((item: { id: string }) => item.id === explicitLow.messageId)?.binding)
        .toEqual({ model: protectedModel, modelReason, thinking: "low" });

      await actors.setThinking(actor.id, "high");
      releaseFirst();
      await waitFor(() => protectedSpawned.length === 1 && actors.status(actor.id).status === "idle");
      expect(denials).toEqual([expect.objectContaining({
        code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "thinking-exceeds", role: "actor:serial", model: protectedModel,
      })]);
      expect(requests).toHaveLength(2);
      expect(requests[1]).toMatchObject({ model: protectedModel, modelReason, thinking: "low" });
      expect(protectedSpawned).toHaveLength(1);
      expect(protectedSpawned[0]).toMatchObject({ model: protectedModel, modelReason, thinking: "low" });
    } finally {
      releaseFirst();
    }
  });
});
