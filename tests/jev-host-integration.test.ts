import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { registerFabricActorHostEventObservers } from "../src/actors/host-event-observer.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { JevClient } from "../src/jev/client.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { JevRunInfo } from "../src/jev/types.js";

describe("Main lifecycle to Jev observer integration", () => {
  it.each(["deny", "ask", "auto", "allow"] as const)("SR-5 shadow inference requires explicit current network allow: %s", async network => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-shadow-authority-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent")); vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "high", sendMessage: vi.fn(), appendEntry: vi.fn(), on: vi.fn(() => () => {}) } as unknown as ExtensionAPI;
    const available = [{ provider: "test", id: "sol" }];
    const credential = vi.fn(async () => "offline-test-only");
    const http = vi.fn(async () => new Response(JSON.stringify({ model: "jev-latest", answers: { route: { type: "choice", choice: "candidate-0", confidence: 1, probabilities: { "candidate-0": 1 } } }, usage: { input_tokens: 1, output_tokens: 1 } })));
    vi.stubGlobal("fetch", http);
    const context = { cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => available, find: () => available[0], getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "offline-test-only", headers: {} }), getProviderAuthStatus: () => ({ configured: true }), getApiKeyForProvider: credential },
      sessionManager: { getSessionId: () => "shadow-authority", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined }, ui: { setStatus: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({ fullCodeMode: true, mcp: { enabled: false }, mesh: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false }, agents: { enabled: true, nice: 19 }, approvals: { agent: "allow", execute: "allow", read: "allow", network } });
    const fixture = path.join(cwd, "unused.mjs"); fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: fixture, worker: path.resolve("tests/fixtures/fake-worker.mjs"), residentHost: fixture, skills: cwd } });
    // The real registry approves the agent action only. That is not a network grant.
    const approve = vi.fn(async (action: { risk: string }) => { if (action.risk !== "agent" && action.risk !== "read") throw new Error("network not approved"); });
    const invocation = { cwd, signal: undefined, parentToolCallId: "shadow", nestedToolCallId: "shadow", extensionContext: context, update() {}, approve, audits: [], maxResultChars: 32768 };
    const spawn = () => runtime.registry.invoke("agents.spawn", { task: "lookup", model: "auto", pinModel: "test/sol", pinThinking: "high", routeClass: "bounded-lookup", protected: false }, invocation) as Promise<{ id: string; routeDecision: { reasonCode: string } }>;
    try {
      await runtime.initialize(context, config);
      const handle = await spawn();
      expect(handle).toMatchObject({ model: "test/sol", routeDecision: { reasonCode: network === "allow" ? "shadow-choice" : "jev-error", model: "test/sol" } });
      await runtime.registry.invoke("agents.wait", { id: handle.id }, invocation);
      expect(credential).toHaveBeenCalledTimes(network === "allow" ? 1 : 0); expect(http).toHaveBeenCalledTimes(network === "allow" ? 1 : 0);
      const rows = fs.readFileSync(path.join(cwd, "agent/fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows).toHaveLength(2); expect(rows[0].reasonCode).toBe(network === "allow" ? "shadow-choice" : "jev-error"); expect(rows[1].decisionId).toBe(rows[0].decisionId);
      if (network === "allow") {
        // Current policy is consulted again, not cached at provider installation.
        runtime.config.approvals.network = "deny";
        const denied = await spawn(); expect(denied.routeDecision.reasonCode).toBe("jev-error");
        await runtime.registry.invoke("agents.wait", { id: denied.id }, invocation);
        expect(credential).toHaveBeenCalledTimes(1); expect(http).toHaveBeenCalledTimes(1);
      }
    } finally { await runtime.shutdown(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.unstubAllEnvs(); fs.rmSync(cwd, { recursive: true, force: true }); }
  }, 30000);

  it.each(["credential", "inference"])("R2 revokes and joins routing on Jev retirement during %s and permits fresh generation", async phase => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-jev-route-owner-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent")); vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "high", sendMessage: vi.fn(), appendEntry: vi.fn(), on: vi.fn(() => () => {}) } as unknown as ExtensionAPI;
    const available = [{ provider: "test", id: "sol" }];
    let enter!: () => void; const entered = new Promise<void>(resolve => { enter = resolve; });
    let release!: () => void; const gate = new Promise<void>(resolve => { release = resolve; });
    let captured!: AbortSignal; let fetchCalls = 0;
    const evaluation = JevClient.prototype.evaluate;
    vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(function (this: JevClient, request, signal) { captured = signal; return evaluation.call(this, request, signal); });
    vi.stubGlobal("fetch", vi.fn(async (_url, init) => {
      fetchCalls++; if (phase === "inference" && fetchCalls === 1) { captured = init!.signal as AbortSignal; enter(); await new Promise<void>((resolve, reject) => { captured.addEventListener("abort", () => reject(new Error("retired")), { once: true }); void gate.then(resolve); }); }
      return new Response(JSON.stringify({ model: "jev-latest", answers: { route: { type: "choice", choice: "candidate-0", confidence: 1, probabilities: { "candidate-0": 1 } } }, usage: { input_tokens: 1, output_tokens: 1 } }));
    }));
    let authCalls = 0;
    const context = { cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => available, find: () => available[0], getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "offline-test-only", headers: {} }), getProviderAuthStatus: () => ({ configured: true }), getApiKeyForProvider: async () => { if (phase === "credential" && ++authCalls === 1) { enter(); await gate; } return "offline-test-only"; } },
      sessionManager: { getSessionId: () => "route-owner", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined }, ui: { setStatus: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({ fullCodeMode: true, mcp: { enabled: false }, mesh: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false }, agents: { enabled: true }, approvals: { agent: "allow", execute: "allow", read: "allow", network: "allow" } });
    const fixture = path.join(cwd, "unused.mjs"); fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: fixture, worker: path.resolve("tests/fixtures/fake-worker.mjs"), residentHost: fixture, skills: cwd } });
    const invocation = { cwd, signal: undefined, parentToolCallId: "route", nestedToolCallId: "route", extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32768 };
    const launch = vi.spyOn(AgentManager.prototype, "spawn");
    const spawn = () => runtime.registry.invoke("agents.spawn", { task: "lookup", model: "auto", pinModel: "test/sol", pinThinking: "high", routeClass: "bounded-lookup", protected: false }, invocation);
    let pending: Promise<unknown> | undefined;
    let reload: Promise<unknown> | undefined;
    try {
      await runtime.initialize(context, config); pending = spawn(); await entered;
      let reloaded = false;
      reload = runtime.registry.invoke("components.reload", { id: "fabric.provider.jev" }, invocation).then(result => { reloaded = true; return result; });
      await vi.waitFor(() => expect(captured.aborted).toBe(true));
      if (phase === "credential") {
        await new Promise(resolve => setTimeout(resolve, 30));
        expect(reloaded).toBe(false); // Cancellation of the waiter is not settlement of host auth.
        expect(fetchCalls).toBe(0);
        release();
      }
      await reload;
      expect(captured.aborted).toBe(true);
      expect(await pending).toMatchObject({ routeDecision: { reasonCode: "jev-error", model: "test/sol" } });
      release(); await new Promise(resolve => setTimeout(resolve, 5));
      expect(fetchCalls).toBe(phase === "credential" ? 0 : 1);
      expect(await spawn()).toMatchObject({ routeDecision: { reasonCode: "shadow-choice" } }); expect(launch).toHaveBeenCalledTimes(2);
    } finally { release(); await reload?.catch(() => {}); await pending?.catch(() => {}); await runtime.shutdown(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.unstubAllEnvs(); fs.rmSync(cwd, { recursive: true, force: true }); }
  }, 20000);

  it.each(["startup-retry", "resume"])("SR-3 rechecks exact availability, never the similar authenticated model, on %s", async recovery => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-pin-recovery-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent")); vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "high", sendMessage: vi.fn(), appendEntry: vi.fn(), on: vi.fn(() => () => {}) } as unknown as ExtensionAPI;
    let available = [{ provider: "test", id: "sol" }];
    const auth = vi.fn(async () => ({ ok: true, apiKey: "offline-test-only", headers: {} }));
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ model: "jev-latest", answers: { route: { type: "choice", choice: "candidate-0", confidence: 1, probabilities: { "candidate-0": 1 } } }, usage: { input_tokens: 1, output_tokens: 1 } }))));
    const context = { cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => available, refresh: vi.fn(), find: () => available[0], getApiKeyAndHeaders: auth, getProviderAuthStatus: () => ({ configured: true }), getApiKeyForProvider: async () => "offline-test-only" },
      sessionManager: { getSessionId: () => "pin-recovery", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined }, ui: { setStatus: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({ fullCodeMode: true, mcp: { enabled: false }, mesh: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false }, agents: { enabled: true }, approvals: { agent: "allow", execute: "allow", read: "allow", network: "allow" } });
    const fixture = path.join(cwd, "unused.mjs"); fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: fixture, worker: path.resolve(recovery === "startup-retry" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs"), residentHost: fixture, skills: cwd } });
    const invocation = { cwd, signal: undefined, parentToolCallId: "pin", nestedToolCallId: "pin", extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32768 };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      await runtime.initialize(context, config);
      const handle = await runtime.registry.invoke("agents.spawn", { task: recovery === "startup-retry" ? "Recover startup" : "RESUME_AFTER_STOP", model: "auto", pinModel: "test/sol", pinThinking: "high", routeClass: "bounded-lookup", protected: false }, invocation) as { id: string };
      available = [{ provider: "test", id: "sol-similar" }];
      const result = await runtime.registry.invoke("agents.wait", { id: handle.id }, invocation);
      expect(result).toMatchObject({ error: expect.stringMatching(/Role pin.*not available/) });
      expect(launch).toHaveBeenCalledTimes(1);
      expect(auth).toHaveBeenCalledTimes(1);
      expect(auth.mock.calls[0]).toEqual([expect.objectContaining({ id: "sol" })]);
    } finally { await runtime.shutdown(); vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.unstubAllEnvs(); fs.rmSync(cwd, { recursive: true, force: true }); }
  }, 20000);

  it.each([false, true])("delivers real host hooks and cleans up with mesh=%s", async (mesh) => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-jev-observer-"));
    fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const handlers = new Map<string, Array<(event: unknown, context: ExtensionContext) => void>>();
    const sendMessage = vi.fn();
    const pi = {
      events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage,
      on(event: string, handler: (event: unknown, context: ExtensionContext) => void) {
        handlers.set(event, [...(handlers.get(event) ?? []), handler]);
        return () => {
          handlers.set(event, (handlers.get(event) ?? []).filter((entry) => entry !== handler));
        };
      },
    } as unknown as ExtensionAPI;
    const context = {
      cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
      sessionManager: { getSessionId: () => "observer-integration", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({
      fullCodeMode: true, mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: mesh },
      agents: { enabled: false }, memory: { enabled: false }, residency: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false }, approvals: { agent: "allow", execute: "allow", read: "allow", network: "deny" },
    });
    const fixture = path.join(cwd, "unused.mjs");
    fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), {
      paths: { extension: fixture, worker: fixture, residentHost: fixture, skills: cwd },
    });
    registerFabricActorHostEventObservers(pi, (name, event, ctx) => { runtime.dispatchHostEvent(name, event, ctx); });
    const emit = (name: string, event: unknown) => handlers.get(name)?.forEach(handler => handler(event, context));
    const invocation = {
      cwd, signal: undefined, parentToolCallId: "observer", nestedToolCallId: "observer",
      extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32_768,
    };
    const spawn = (code: string, requires = ["jev.advise"]) => runtime.registry.invoke("jev.spawn", {
      input: null, observe: { events: ["turn_end"], delivery: "steer" },
      program: { name: "turn-advisor", code, requires, inputSchema: {}, outputSchema: {} },
    }, invocation) as Promise<JevRunInfo>;
    const wait = (id: string) => runtime.registry.invoke("jev.wait", { id }, invocation);
    try {
      await runtime.initialize(context, config);
      const run = await spawn('const event = await program.nextEvent(); return await program.advise({eventId:event.id,message:"Check <tests> before completion"});');
      emit("turn_end", { type: "turn_end", turnIndex: 0, message: { role: "assistant", content: [{ type: "text", text: "private unselected response" }] }, toolResults: [] });
      expect(await wait(run.id)).toMatchObject({ state: "completed", result: { delivered: true }, observation: { consumed: 1, adviceDelivered: 1 } });
      expect(sendMessage).toHaveBeenCalledWith(expect.objectContaining({
        customType: "pi-fabric-jev", display: true, content: expect.stringContaining("Check &lt;tests&gt;"),
        details: expect.objectContaining({ runId: run.id }),
      }), { deliverAs: "steer", triggerTurn: false });
      expect(JSON.stringify(sendMessage.mock.calls)).not.toContain("private unselected");

      expect(runtime.backgroundWorkCount()).toBe(0);
      const interrupted = await spawn("while(true) await program.nextEvent();");
      // A running observer blocks a self-reload, which would cancel it (smarty-dev#2160).
      expect(runtime.backgroundWorkCount()).toBe(1);
      const interruptedWait = wait(interrupted.id);
      expect(runtime.haltAdvisors()).toBeGreaterThanOrEqual(1);
      expect(runtime.advisorsHalted).toBe(true);
      expect(await interruptedWait).toMatchObject({ state: "cancelled" });
      expect(runtime.backgroundWorkCount()).toBe(0);
      emit("turn_end", { type: "turn_end", turnIndex: 1 });
      expect(sendMessage).toHaveBeenCalledTimes(1);
      emit("input", { type: "input", source: "interactive", text: "continue" });
      expect(runtime.advisorsHalted).toBe(false);

      const pinned = await spawn("while(true) await program.nextEvent();", ["jev.evaluate", "jev.advise"]);
      const pinnedWait = wait(pinned.id);
      await runtime.registry.invoke("components.reload", { id: "fabric.provider.jev" }, invocation);
      expect(await pinnedWait).toMatchObject({ state: "cancelled", observation: { queued: 0 } });
      expect(runtime.componentGraph().components.find(c => c.id === "fabric.provider.jev")?.state).toBe("active");
      const shutdown = await spawn("while(true) await program.nextEvent();");
      const shutdownWait = wait(shutdown.id);
      await runtime.shutdown();
      expect(await shutdownWait).toMatchObject({ state: "cancelled" });
    } finally {
      await runtime.shutdown();
      vi.unstubAllEnvs();
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  }, 20000);
});
