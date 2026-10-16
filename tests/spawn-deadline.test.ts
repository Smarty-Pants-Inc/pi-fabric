import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { AgentManager, effectiveAgentTimeoutMs } from "../src/agents/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [], managers: AgentManager[] = [], actors: ActorManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(actors.splice(0).map(actor => actor.close()));
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return process.platform !== "linux" || !/^[ZX]$/.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ").at(-1)!.split(" ")[0]!);
  } catch { return false; }
};
const setup = (persistent = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "spawn-deadline-")); roots.push(root);
  const profile = path.join(root, "profile"); fs.mkdirSync(path.join(profile, "extensions"), { recursive: true });
  fs.copyFileSync(path.resolve("tests/fixtures/spawn-deadline-provider.ts"), path.join(profile, "extensions", "provider.ts"));
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false }, defaultProjectTrust: "never" }));
  vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("SPAWN_DEADLINE_ROOT", root);
  const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.agents.timeoutMs = 600000;
  const delivered: AgentRunResult[] = [];
  let deliver!: (result: AgentRunResult) => void;
  const completion = new Promise<AgentRunResult>(resolve => { deliver = resolve; });
  const manager = new AgentManager(root, config.agents, {
    workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    runRoot: path.join(root, "runs"), onBackgroundComplete: result => { delivered.push(result); deliver(result); },
  }); managers.push(manager);
  const identity = { id: "session:deadline-proof", name: "Main", kind: "main" as const, sessionId: "deadline-proof" };
  const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  const main = { id: identity.id, matches: (id: string) => id === identity.id || id === "main", info: () => ({ id: identity.id }), local: true } as FabricMainAgentTarget;
  const participants = { list: () => [], get: () => undefined, self: () => undefined, peers: () => [], scheduleRefresh() {}, async refresh() {} } as unknown as FabricParticipantSource;
  const actor = new ActorManager("deadline-proof", identity, mesh, config.mesh, manager, () => {}, { actorRoot: path.join(root, "actors"), mainAgent: main, persistent, reapDeadSessionPresence: false }); actors.push(actor);
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 100, maxReadEvents: 10 }, async () => {});
  const provider = new AgentsProvider(manager, actor, new GlobalActorRegistry(root, 65536), main, participants, undefined, lifecycle);
  const registry = new ActionRegistry(); registry.register(provider);
  const model = { provider: "provider", id: "session" };
  const context = { hasUI: false, modelRegistry: { getAvailable: () => [model], find: (p: string, id: string) => p === model.provider && id === model.id ? model : undefined } } as unknown as ExtensionContext;
  return { root, manager, actor, mesh, identity, main, config, delivered, completion, service: new FabricExecutionService(registry, config), context };
};

describe("spawn hard deadlines below the configured default (#12302)", () => {
  it("honors five-minute timeboxes and longer overrides while preserving the tiny-timeout guard and unset default", () => {
    expect(effectiveAgentTimeoutMs(3600000, 240000)).toBe(3600000); // b4408352's original tiny-timeout regression.
    expect(effectiveAgentTimeoutMs(1800000, 900000)).toBe(900000); // Authorized change to the one obsolete upstream assertion.
    expect(effectiveAgentTimeoutMs(600000, 300000)).toBe(300000);
    expect(effectiveAgentTimeoutMs(600000, 300001)).toBe(300001);
    expect(effectiveAgentTimeoutMs(600000, 299999)).toBe(600000);
    expect(effectiveAgentTimeoutMs(600000, 30000)).toBe(600000);
    expect(effectiveAgentTimeoutMs(600000)).toBe(600000);
    expect(effectiveAgentTimeoutMs(DEFAULT_FABRIC_CONFIG.agents.timeoutMs)).toBe(86400000);
    expect(effectiveAgentTimeoutMs(600000, 900000)).toBe(900000);
    expect(effectiveAgentTimeoutMs(600000, 100000000)).toBe(86400000);
    for (const timeoutMs of [30000, 300000]) expect(normalizeAgentRunRequest({ task: "box", timeoutMs }, { runner: "pi", timeoutMs: 600000 }).timeoutMs).toBe(timeoutMs);
  });

  it.each([
    [86_400_000, 86_400_000],
    [1_800_000, 7_200_000],
  ])("records a persisted actor's effective deadline with configured default %i", { timeout: 30000 }, async (configured, effective) => {
    // Only inference is synthetic: persistence, actor activation, worker/Pi,
    // process transport, deadline enforcement and terminal records are real.
    const s = setup(true);
    s.manager.config.timeoutMs = 1_800_000;
    const created = await s.service.execute({ code: `return await agents.createActor({ name: "persisted-budget", instructions: "Wait for work.", model: "provider/session", extensions: true, tools: ["write"], timeoutMs: 7200000 });`,
      signal: undefined, parentToolCallId: "actor-deadline-proof", context: s.context, onPartial() {} });
    expect(created.success, created.error).toBe(true);
    const id = (created.value as { id: string }).id;
    expect(s.actor.definition(id).timeoutMs).toBe(7_200_000);
    await s.actor.close();
    s.manager.config.timeoutMs = configured;
    const restored = new ActorManager("deadline-proof", s.identity, s.mesh, s.config.mesh, s.manager, () => {}, {
      actorRoot: path.join(s.root, "actors"), mainAgent: s.main, persistent: true, reapDeadSessionPresence: false,
    }); actors.push(restored);
    expect(restored.definition(id).timeoutMs).toBe(7_200_000);
    const activation = restored.ask(id, "Write the checkpoint, then wait for release.").catch(error => error as Error);
    let runId!: string;
    await vi.waitFor(() => {
      runId = restored.status(id).inFlightRun!.id;
      expect(s.manager.status(runId)).toMatchObject({ status: "running", partialText: "Partial deadline report." });
    }, { timeout: 15000, interval: 20 });
    const now = Date.now;
    let offset = Math.min(configured, 7_200_000) + 1500;
    vi.spyOn(Date, "now").mockImplementation(() => now() + offset);
    // A restored two-hour override must neither shorten a raised day-long default
    // nor lose its ability to extend the original half-hour default.
    await expect(s.manager.wait(runId, { timeoutMs: 400 })).rejects.toThrow("still running");
    offset = effective + 1500;
    const result = await s.manager.wait(runId, { timeoutMs: 8000 });
    expect(result).toMatchObject({ status: "timed_out", actorId: id, error: `Agent timed out after ${effective}ms` });
    expect(JSON.parse(fs.readFileSync(path.join(s.root, "runs", runId, "status.json"), "utf8")))
      .toMatchObject({ status: "timed_out", error: result.error });
    expect(await activation).toBeInstanceOf(Error);
    offset = 0;
    vi.restoreAllMocks();
    if (configured === 86_400_000) {
      const oneShot = await s.service.execute({ code: `return await agents.spawn({ task: "Write the checkpoint, then wait for release.", model: "provider/session", extensions: true, tools: ["write"], timeoutMs: 900000 });`,
        signal: undefined, parentToolCallId: "one-shot-counterexample", context: s.context, onPartial() {} });
      expect(oneShot.success, oneShot.error).toBe(true);
      const handle = oneShot.value as AgentHandleInfo;
      await vi.waitFor(() => expect(s.manager.status(handle.id)).toMatchObject({ status: "running", partialText: "Partial deadline report." }), { timeout: 15000, interval: 20 });
      vi.spyOn(Date, "now").mockImplementation(() => now() + 901500);
      expect(await s.manager.wait(handle.id, { timeoutMs: 8000 }))
        .toMatchObject({ status: "timed_out", error: "Agent timed out after 900000ms" });
    }
  });

  // The clock changes only in the manager's host, after real native readiness.
  // Worker/Pi/descendants, termination, filesystem receipts and delivery are not mocked.
  it.each(["short", "tiny", "unset"] as const)("native public spawn path: %s", { timeout: 30000 }, async mode => {
    const s = setup();
    const timeout = mode === "short" ? ", timeoutMs: 300000" : mode === "tiny" ? ", timeoutMs: 30000" : "";
    const spawned = await s.service.execute({ code: `return await agents.spawn({ task: "Write the checkpoint, then wait for release.", model: "provider/session", transport: "process", extensions: true, tools: ["write"]${timeout} });`,
      signal: undefined, parentToolCallId: "deadline-proof", context: s.context, onPartial() {} });
    expect(spawned.success, spawned.error).toBe(true);
    const handle = spawned.value as AgentHandleInfo;
    const native = path.join(s.root, "native-pids.json");
    await vi.waitFor(() => {
      expect(fs.existsSync(native)).toBe(true);
      expect((s.manager.status(handle.id) as AgentRunResult).partialText).toBe("Partial deadline report.");
    }, { timeout: 15000, interval: 20 });
    const pids = JSON.parse(fs.readFileSync(native, "utf8")) as { pi: number; descendant: number };
    const worker = Number(handle.sessionId);
    expect(alive(pids.pi)).toBe(true); expect(alive(pids.descendant)).toBe(true);
    const now = Date.now; let offset = 301500;
    vi.spyOn(Date, "now").mockImplementation(() => now() + offset);
    if (mode !== "short") {
      // A bounded observation proves both counterexamples outlive the short deadline.
      await expect(s.manager.wait(handle.id, { timeoutMs: 400 })).rejects.toThrow("still running");
      expect(alive(pids.pi)).toBe(true); expect(alive(pids.descendant)).toBe(true);
      fs.writeFileSync(path.join(s.root, "release"), "finish");
    }
    let observation: ReturnType<typeof setTimeout> | undefined;
    const result = await Promise.race([s.completion, new Promise<never>((_, reject) => {
      observation = setTimeout(() => reject(new Error("task outlived its requested hard deadline")), 8000);
    })]).finally(() => clearTimeout(observation));
    expect(result.status, result.error).toBe(mode === "short" ? "timed_out" : "completed");
    expect(result.partialText ?? result.text).toBe("Partial deadline report.");
    expect(result.writeReceipt?.paths).toContain(path.join(s.root, "checkpoint.txt"));
    expect(fs.readFileSync(path.join(s.root, "checkpoint.txt"), "utf8")).toBe("preserved work\n");
    expect(s.delivered).toHaveLength(1);
    expect(await s.manager.wait(handle.id)).toMatchObject({ status: result.status, text: result.text, writeReceipt: result.writeReceipt });
    await vi.waitFor(() => {
      expect(alive(worker)).toBe(false); expect(alive(pids.pi)).toBe(false); expect(alive(pids.descendant)).toBe(false);
    }, { timeout: 5000, interval: 20 });
    if (mode === "tiny") {
      const note = { code: "AGENT_TIMEOUT_BELOW_SHORTENING_FLOOR", requestedTimeoutMs: 30000, effectiveTimeoutMs: 600000, minimumTimeoutMs: 300000 };
      expect(handle.timeoutNote).toEqual(note); expect(result.timeoutNote).toEqual(note);
      expect(fs.readFileSync(path.join(s.root, "runs", handle.id, "events.jsonl"), "utf8")).toContain("run.timeout_ignored");
    } else expect(handle.timeoutNote).toBeUndefined();
    offset = 0;
  });
});
