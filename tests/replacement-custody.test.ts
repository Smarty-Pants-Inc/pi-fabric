import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const pause = () => new Promise(resolve => setTimeout(resolve, 100));

describe("A24 replacement-attempt execution custody", () => {
  it.each(["run", "stop", "close", "handleless"] as const)("never uses the predecessor receipt for actor %s", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-replacement-custody-"));
    const onSettled = vi.fn();
    const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 2, budgetUsd: 0,
      retainRuns: false, sessionExport: false, timeoutMs: 30_000 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), onSettled,
    });
    const actors = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" },
      new MeshStore(path.join(root, "mesh"), 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      agents, () => {}, { actorRoot: path.join(root, "actors") });
    let confirmed = false;
    let launchCount = 0;
    let replacementRequest: AgentTransportLaunch | undefined;
    let predecessorRequest: AgentTransportLaunch | undefined;
    let firstRunId = "";
    const sessions: string[] = [];
    const predecessorStop = vi.fn(async () => {});
    const replacementStop = vi.fn(async () => { if (!confirmed) throw new Error("replacement execution exit unconfirmed"); });
    const replacement: AgentTransportHandle = { kind: "process", sessionId: "replacement-attempt", stop: replacementStop,
      isAlive: async () => !confirmed, lostContact: () => confirmed ? undefined : "replacement custody unconfirmed" };
    let blockerAlive = true;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      launchCount++;
      const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      if (args.has("--session-file")) sessions.push(args.get("--session-file")!);
      if (launchCount === 2) {
        expect(predecessorStop).toHaveBeenCalled(); // confirmed predecessor exit already installed
        replacementRequest = request;
        if (mode !== "handleless") request.onCustody?.(replacement);
        request.onUnconfirmedExit?.("replacement custody unconfirmed");
        throw Object.assign(new Error("replacement launch rejected after execution"), { launchOutcome: "unknown" });
      }
      const blocker = fs.readFileSync(args.get("--task-file")!, "utf8").includes("capacity blocker");
      const now = Date.now();
      fs.writeFileSync(args.get("--status-file")!, JSON.stringify({ id: request.id, name: request.name, task: "fixture",
        status: launchCount === 1 ? "failed" : blocker ? "running" : "completed", runner: "pi", transport: "process", cwd: root,
        startedAt: now, updatedAt: now, finishedAt: now, turns: 0, toolCalls: 0, text: "replacement discharged",
        ...(launchCount === 1 ? { error: "Agent transport exited without a result" } : {}),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      if (launchCount === 1) { firstRunId = request.id; predecessorRequest = request; }
      return { kind: "process", sessionId: launchCount === 1 ? "predecessor" : `fixture-${launchCount}`,
        stop: launchCount === 1 ? predecessorStop : async () => { if (blocker) blockerAlive = false; },
        isAlive: async () => blocker && blockerAlive };
    });
    const run = agents.run.bind(agents);
    let runSettlements = 0;
    vi.spyOn(agents, "run").mockImplementation(async (...args) => { const result = await run(...args); runSettlements++; return result; });
    let closing: Promise<void> | undefined;
    let ask: Promise<unknown> | undefined;
    try {
      const actor = await actors.create({ name: "replacement-owner", instructions: "Fixture", transport: "process" });
      ask = actors.ask(actor.id, "first activation").catch(error => error);
      await vi.waitFor(() => expect(replacementRequest).toBeDefined(), { timeout: 10_000 });
      const directory = agents.runDirectory(firstRunId)!;
      actors.tell(actor.id, "next activation against the same native session");
      await pause();
      expect(launchCount).toBe(2);
      expect(actors.status(actor.id).inFlightRun?.id).toBe(firstRunId);
      expect(actors.status(actor.id).queued).toBe(1);
      expect(runSettlements).toBe(0);
      expect(onSettled).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(true);
      // A repeated predecessor receipt/callback must not discharge or relabel the replacement.
      await predecessorStop();
      predecessorRequest!.onUnconfirmedExit?.("stale predecessor callback");
      await expect(agents.stop(firstRunId)).rejects.toThrow(/execution exit unconfirmed/);
      await expect(agents.cleanup(firstRunId)).rejects.toThrow("running agent");
      await expect(agents.checkpointForRelease()).rejects.toThrow(/unresolved worker/);
      expect(fs.existsSync(directory)).toBe(true);
      expect(runSettlements).toBe(0);
      // With capacity >1, an unrelated worker can launch, but the replacement
      // still owns its permit. A third worker must queue rather than reuse it.
      const blocker = await agents.spawn({ task: "capacity blocker", transport: "process" });
      expect(blocker.status).toBe("running");
      const queued = await agents.spawn({ task: "must not use predecessor permit", transport: "process" });
      expect(queued.status).toBe("queued");
      await agents.stop(queued.id);
      if (mode === "stop") await expect(actors.stop(actor.id, undefined, true)).rejects.toThrow(/execution exit unconfirmed/);
      if (mode === "close") {
        let closeSettled = false;
        closing = actors.close().then(() => { closeSettled = true; });
        await expect(agents.close()).rejects.toThrow(/execution exit unconfirmed/);
        await pause();
        expect(closeSettled).toBe(false);
        expect(fs.existsSync(path.join(root, "actors"))).toBe(true);
        expect(fs.existsSync(directory)).toBe(true);
      }
      expect(runSettlements).toBe(0);
      expect(onSettled.mock.calls.some(([result]) => result.id === firstRunId)).toBe(false);
      expect(sessions).toHaveLength(2); // no next activation/session reuse
      confirmed = true;
      if (mode === "handleless") replacementRequest!.onCustody?.(replacement); // exact attempt's late custody receipt
      await agents.stop(firstRunId);
      await ask;
      await vi.waitFor(() => expect(runSettlements).toBeGreaterThanOrEqual(1));
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(false);
      if (mode === "run" || mode === "handleless") {
        await vi.waitFor(() => expect(runSettlements).toBe(2), { timeout: 5000 });
        expect(sessions).toHaveLength(3);
        expect(sessions[2]).toBe(sessions[0]); // reuse only AFTER replacement confirmation
      }
      await agents.stop(blocker.id);
      await closing;
    } finally {
      confirmed = true;
      replacementRequest?.onCustody?.(replacement);
      if (firstRunId) await agents.stop(firstRunId).catch(() => undefined);
      await closing;
      await actors.close();
      await agents.close();
      await ask;
      launch.mockRestore();
      vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
