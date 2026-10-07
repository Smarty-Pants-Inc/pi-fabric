import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type {
  FabricInvocationContext,
  FabricParticipantHandle,
  FabricParticipantSpec,
  FabricProvider,
} from "../src/protocol.js";
import type { FabricControlCommand } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import {
  acceptProviderParticipantControl,
  controlProviderParticipant,
  ProviderParticipantRegistry,
} from "../src/topology/provider-participants.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const spec = (overrides: Partial<FabricParticipantSpec> = {}): FabricParticipantSpec => ({
  id: "run-1",
  label: "Delegated run",
  stop: async () => ({ confirmed: true }),
  ...overrides,
});

const roots: string[] = [];
afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ProviderParticipantRegistry", () => {
  it("validates specs, binds the provider into the ref, and refuses duplicate live ids", () => {
    const registry = new ProviderParticipantRegistry();
    const participants = registry.view("delegate", "call-1");
    for (const bad of [
      spec({ id: "" }),
      spec({ id: "has space" }),
      spec({ id: "x".repeat(65) }),
      spec({ label: "  " }),
      spec({ label: "x".repeat(121) }),
      spec({ kind: "Bad Kind" }),
      spec({ stop: undefined as never }),
      spec({ steer: "nope" as never }),
      spec({ detached: "yes" as never }),
    ]) {
      expect(() => participants.register(bad)).toThrow(/Fabric participant/);
    }
    const handle = participants.register(spec({ kind: "delegate" }));
    expect(handle.ref).toBe("provider:delegate:run-1");
    expect(() => participants.register(spec())).toThrow("already registered");
    // Another provider's view cannot collide with or reach this ref.
    expect(registry.view("other", "call-1").register(spec()).ref).toBe("provider:other:run-1");
    handle.settle({ status: "completed", summary: "done" });
    expect(participants.register(spec()).ref).toBe("provider:delegate:run-1");
  });

  it("projects bounded progress and settlement into directory records", () => {
    const registry = new ProviderParticipantRegistry();
    const handle = registry.view("delegate", "call-1").register(spec({
      steer: async () => {},
    }));
    handle.update({ phase: "build", message: "compiling", usage: { input: 10, output: 4 } });
    expect(() => handle.update({ message: "x".repeat(501) })).toThrow("exceeds 500");
    expect(() => handle.update({ usage: { input: -1 } })).toThrow("non-negative");
    expect(() => handle.update({ usage: { tokens: 1 } as never })).toThrow("Unknown");
    const [record] = registry.records("session:root", "host", "identity");
    expect(record).toMatchObject({
      id: "provider:delegate:run-1",
      kind: "provider",
      provider: "delegate",
      name: "Delegated run",
      status: "running",
      parentId: "session:root",
      currentTool: "build: compiling",
      usage: { input: 10, output: 4, cacheRead: 0, cacheWrite: 0, cost: 0 },
      capabilities: ["steer", "stop"],
    });
    expect(record).not.toHaveProperty("runner");
    expect(() => handle.settle({ status: "done" as never })).toThrow("settle status");
    handle.settle({ status: "failed", summary: "compile error" });
    handle.settle({ status: "completed" });
    expect(registry.get(handle.ref)).toMatchObject({ status: "failed", summary: "compile error", capabilities: [] });
    handle.dispose();
    expect(registry.has(handle.ref)).toBe(false);
    // A disposed handle is inert.
    handle.update({ message: "late" });
    handle.settle({ status: "completed" });
    expect(registry.list()).toEqual([]);
  });

  it("routes stop, steer, and followUp to callbacks and reports unsupported controls", async () => {
    const registry = new ProviderParticipantRegistry();
    const steer = vi.fn(async () => {});
    const stop = vi.fn(async () => ({ confirmed: true }));
    const handle = registry.view("delegate", "call-1").register(spec({ stop, steer }));
    const directory = { get: () => undefined };

    await expect(controlProviderParticipant(registry, directory, undefined, handle.ref, "steer", "focus"))
      .resolves.toMatchObject({ queued: true, routed: "local" });
    expect(steer).toHaveBeenCalledWith("focus");
    await expect(controlProviderParticipant(registry, directory, undefined, handle.ref, "followUp", "next"))
      .rejects.toThrow("does not support followUp");
    await expect(controlProviderParticipant(registry, directory, undefined, handle.ref, "steer", "x", { a: 1 }))
      .rejects.toThrow("does not accept message data");
    await expect(controlProviderParticipant(registry, directory, undefined, handle.ref, "stop"))
      .resolves.toEqual({ ref: handle.ref, outcome: "confirmed" });
    expect(stop).toHaveBeenCalledWith("stopped");
    expect(registry.get(handle.ref)?.status).toBe("stopped");
    await expect(controlProviderParticipant(registry, directory, undefined, handle.ref, "stop"))
      .rejects.toThrow("already stopped");
    await expect(controlProviderParticipant(registry, directory, undefined, "provider:delegate:missing", "stop"))
      .rejects.toThrow("Unknown Fabric participant");
  });

  it("forwards remote provider participants over the control plane and accepts owner commands", async () => {
    const remote = {
      id: "provider:delegate:run-9",
      kind: "provider",
      local: false,
      ownerHostId: "host-b",
      ownerIdentityId: "identity-b",
      capabilities: ["stop"],
    } as unknown as FabricParticipantInfo;
    const request = vi.fn(async () => ({ queued: true as const, messageId: "m", routed: "mesh" as const, acknowledged: true as const }));
    const directory = { get: (id: string) => (id === remote.id ? remote : undefined) };
    await controlProviderParticipant(undefined, directory, { request }, remote.id, "stop");
    expect(request).toHaveBeenCalledWith("host-b", remote.id, "stop", {}, "identity-b");
    await expect(controlProviderParticipant(undefined, directory, { request }, remote.id, "steer", "x"))
      .rejects.toThrow("does not support steer");

    const registry = new ProviderParticipantRegistry();
    const followUp = vi.fn(async () => {});
    const handle = registry.view("delegate", "call-1").register(spec({ followUp }));
    const command = (operation: FabricControlCommand["operation"], targetId = handle.ref) =>
      ({ version: 1, commandId: "c1", targetId, operation, message: "more" }) as FabricControlCommand;
    expect(await acceptProviderParticipantControl(registry, command("followUp"))).toEqual({ accepted: true, messageId: "c1" });
    expect(followUp).toHaveBeenCalledWith("more");
    expect(await acceptProviderParticipantControl(registry, command("ask"))).toMatchObject({ accepted: false });
    expect(await acceptProviderParticipantControl(registry, command("stop", "provider:x:y")))
      .toMatchObject({ accepted: false, error: expect.stringContaining("does not control") });
    expect(await acceptProviderParticipantControl(registry, command("stop"))).toMatchObject({
      accepted: true,
      result: { ref: handle.ref, outcome: "confirmed" },
    });
  });

  it("stops only unsettled, non-detached participants of the cancelled invocation within the deadline", async () => {
    const registry = new ProviderParticipantRegistry();
    const view = registry.view("delegate", "call-1");
    const reasons: string[] = [];
    const confirmed = view.register(spec({ id: "a", stop: async (reason) => (reasons.push(reason), { confirmed: true }) }));
    const declined = view.register(spec({ id: "b", stop: async () => ({ confirmed: false }) }));
    const threw = view.register(spec({ id: "c", stop: async () => { throw new Error("boom"); } }));
    const hung = view.register(spec({ id: "d", stop: () => new Promise(() => {}) }));
    const detachedStop = vi.fn(async () => ({ confirmed: true }));
    view.register(spec({ id: "e", detached: true, stop: detachedStop }));
    const settledStop = vi.fn(async () => ({ confirmed: true }));
    view.register(spec({ id: "f", stop: settledStop })).settle({ status: "completed" });
    const otherStop = vi.fn(async () => ({ confirmed: true }));
    registry.view("delegate", "call-2").register(spec({ id: "g", stop: otherStop }));

    const started = Date.now();
    const outcomes = await registry.cancelInvocation("call-1", "program_cancelled", 50);
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(outcomes).toEqual([
      { ref: confirmed.ref, outcome: "confirmed" },
      { ref: declined.ref, outcome: "unconfirmed", detail: "declined" },
      { ref: threw.ref, outcome: "unconfirmed", detail: "error" },
      { ref: hung.ref, outcome: "unconfirmed", detail: "timeout" },
    ]);
    expect(reasons).toEqual(["program_cancelled"]);
    expect(detachedStop).not.toHaveBeenCalled();
    expect(settledStop).not.toHaveBeenCalled();
    expect(otherStop).not.toHaveBeenCalled();
    expect(registry.get(confirmed.ref)?.status).toBe("stopped");
    expect(registry.get(declined.ref)?.status).toBe("running");
  });

  it("releases a withdrawn provider's participants and leaves their handles inert", () => {
    const registry = new ProviderParticipantRegistry();
    const listener = vi.fn();
    registry.subscribe(listener);
    const detached = registry.view("delegate", "call-1").register(spec({ detached: true }));
    registry.view("other", "call-1").register(spec());
    expect(registry.releaseProvider("delegate")).toBe(1);
    expect(registry.has(detached.ref)).toBe(false);
    expect(registry.has("provider:other:run-1")).toBe(true);
    detached.settle({ status: "completed" });
    expect(registry.has(detached.ref)).toBe(false);
    expect(listener).toHaveBeenCalled();
    registry.releaseAll();
    expect(registry.list()).toEqual([]);
  });
});

describe("participant directory", () => {
  it("publishes provider participants through the mesh and validates them on read", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-provider-participants-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const registry = new ProviderParticipantRegistry();
    registry.view("delegate", "call-1").register(spec({ followUp: async () => {} }));
    const directory = new ParticipantDirectory(
      new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000),
      { enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 300 },
    );
    directory.registerSource(() => registry.records(identity.id, identity.id, identity.id));
    try {
      await directory.start();
      expect(directory.get("provider:delegate:run-1")).toMatchObject({
        kind: "provider",
        provider: "delegate",
        status: "running",
        capabilities: ["followUp", "stop"],
        local: true,
      });
    } finally {
      await directory.close();
    }
  });
});

describe("owned-work cancellation in fabric_exec", () => {
  const descriptor = {
    name: "run",
    description: "Start delegated work",
    inputSchema: { type: "object", properties: { detached: { type: "boolean" } }, additionalProperties: false },
    risk: "read" as const,
  };
  const delegateProvider = (
    stops: string[],
    handles: FabricParticipantHandle[],
    hang = true,
  ): FabricProvider => ({
    name: "delegate",
    description: "Delegate-like provider",
    async list() { return [descriptor]; },
    async describe(name) { return name === "run" ? descriptor : undefined; },
    async invoke(_name, args, context: FabricInvocationContext) {
      const id = `run-${handles.length + 1}`;
      const handle = context.participants!.register({
        id,
        label: `Delegated ${id}`,
        detached: args.detached === true,
        stop: async (reason) => {
          stops.push(`${id}:${reason}`);
          return { confirmed: id !== "run-2" };
        },
      });
      handles.push(handle);
      if (args.detached === true || !hang) return { ref: handle.ref };
      await new Promise((resolve) => setTimeout(resolve, 60_000).unref());
      return { ref: handle.ref };
    },
  });

  const service = (provider: FabricProvider, timeoutMs?: number) => {
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.fullCodeMode = false;
    config.approvals.read = "allow";
    if (timeoutMs !== undefined) config.executor.timeoutMs = timeoutMs;
    const participants = new ProviderParticipantRegistry();
    const execution = new FabricExecutionService(registry, config);
    execution.setParticipantRegistry(participants);
    return { execution, participants };
  };

  const context = { cwd: process.cwd(), hasUI: false } as ExtensionContext;

  it("stops owned participants on cancellation, records outcomes in the trace and result, and spares detached work", async () => {
    const stops: string[] = [];
    const handles: FabricParticipantHandle[] = [];
    const { execution, participants } = service(delegateProvider(stops, handles));
    const controller = new AbortController();
    const running = execution.execute({
      code: `await tools.call({ ref: "delegate.run", args: { detached: true } });
        await Promise.all([tools.call({ ref: "delegate.run", args: {} }), tools.call({ ref: "delegate.run", args: {} })]);`,
      signal: controller.signal,
      parentToolCallId: "cancel-test",
      context,
      onPartial() {},
    });
    // The first program type-checks before it runs; slow CI hosts need more than the 1 s default.
    await vi.waitFor(() => expect(handles).toHaveLength(3), { timeout: 10_000 });
    controller.abort();
    const result = await running;

    expect(result.success).toBe(false);
    expect(stops.sort()).toEqual(["run-2:program_cancelled", "run-3:program_cancelled"]);
    expect(result.ownedWork).toEqual([
      { ref: "provider:delegate:run-2", outcome: "unconfirmed", detail: "declined" },
      { ref: "provider:delegate:run-3", outcome: "confirmed" },
    ]);
    expect(result.error).toContain("provider:delegate:run-3 confirmed");
    const stopOperations = result.trace.operations.filter((operation) => operation.ref === "fabric.participant.stop");
    expect(stopOperations).toEqual([
      expect.objectContaining({
        args: { ref: "provider:delegate:run-2", reason: "program_cancelled" },
        outcome: "failed",
        result: { outcome: "unconfirmed", detail: "declined" },
      }),
      expect.objectContaining({
        args: { ref: "provider:delegate:run-3", reason: "program_cancelled" },
        outcome: "succeeded",
        result: { outcome: "confirmed" },
      }),
    ]);
    // Detached participants survive the invocation.
    expect(participants.get("provider:delegate:run-1")?.status).toBe("running");
    expect(participants.get("provider:delegate:run-3")?.status).toBe("stopped");
  });

  it("stops owned participants when the program times out and leaves completed programs alone", async () => {
    const stops: string[] = [];
    const handles: FabricParticipantHandle[] = [];
    const { execution } = service(delegateProvider(stops, handles), 200);
    const timedOut = await execution.execute({
      code: 'await tools.call({ ref: "delegate.run", args: {} });',
      signal: undefined,
      parentToolCallId: "timeout-test",
      context,
      onPartial() {},
    });
    expect(timedOut.trace.outcome).toBe("timed_out");
    expect(stops).toEqual(["run-1:program_cancelled"]);
    expect(timedOut.ownedWork).toEqual([{ ref: "provider:delegate:run-1", outcome: "confirmed" }]);

    const quick = service(delegateProvider(stops, handles, false));
    const completed = await quick.execution.execute({
      code: 'return await tools.call({ ref: "delegate.run", args: {} });',
      signal: undefined,
      parentToolCallId: "complete-test",
      context,
      onPartial() {},
    });
    expect(completed.success).toBe(true);
    expect(completed.ownedWork).toBeUndefined();
    expect(stops).toEqual(["run-1:program_cancelled"]);
  });
});

describe("Fabric runtime provider participants", () => {
  it("routes agents.stop/steer, lists participants, and releases detached work on withdrawal and shutdown", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-provider-participant-runtime-"));
    roots.push(cwd);
    fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const pi = {
      events: { emit: vi.fn() },
      getThinkingLevel: vi.fn(() => "off"),
      sendMessage: vi.fn(),
    } as unknown as ExtensionAPI;
    const context = {
      cwd,
      hasUI: false,
      isProjectTrusted: () => true,
      isIdle: () => true,
      hasPendingMessages: () => false,
      modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
      sessionManager: {
        getSessionId: () => "provider-participants-session",
        getSessionFile: () => undefined,
        getBranch: () => [],
        getLeafId: () => undefined,
      },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({
      capture: { enabled: false },
      mcp: { enabled: false, cache: { enabled: false } },
      mesh: { enabled: false },
      memory: { enabled: false },
      residency: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
      approvals: { read: "allow" },
    });
    const fixture = path.join(cwd, "unused.mjs");
    fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), {
      paths: { extension: fixture, worker: fixture, residentHost: fixture, skills: cwd },
    });
    const descriptor = {
      name: "start",
      description: "Start detached work",
      inputSchema: { type: "object", additionalProperties: false },
      risk: "read" as const,
    };
    const stop = vi.fn(async () => ({ confirmed: true }));
    const steer = vi.fn(async () => {});
    const provider: FabricProvider = {
      name: "delegate",
      description: "Delegate-like provider",
      async list() { return [descriptor]; },
      async describe(name) { return name === "start" ? descriptor : undefined; },
      async invoke(_name, _args, invocation) {
        const first = invocation.participants!.register({ id: "one", label: "One", detached: true, stop, steer });
        invocation.participants!.register({ id: "two", label: "Two", detached: true, stop: async () => ({ confirmed: true }) });
        return first.ref;
      },
    };
    try {
      await runtime.initialize(context, config);
      runtime.registerExternal(provider);
      const started = await runtime.execution.execute({
        code: 'return await tools.call({ ref: "delegate.start", args: {} });',
        context, signal: undefined, parentToolCallId: "runtime-participants", onPartial() {},
      });
      expect(started.success, started.error).toBe(true);
      expect(started.value).toBe("provider:delegate:one");
      await vi.waitFor(() => expect(runtime.participantInfos().map((participant) => participant.id))
        .toEqual(expect.arrayContaining(["provider:delegate:one", "provider:delegate:two"])), { timeout: 10_000 });

      await runtime.queueUserMessage("provider:delegate:one", "focus", "steer");
      expect(steer).toHaveBeenCalledWith("focus");
      await expect(runtime.queueUserMessage("provider:delegate:one", "later", "followUp"))
        .rejects.toThrow("does not support followUp");
      const invocation = {
        cwd, signal: undefined, parentToolCallId: "direct", nestedToolCallId: "direct",
        extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32_768,
      };
      expect(await runtime.registry.invoke("agents.stop", { id: "provider:delegate:one" }, invocation))
        .toEqual({ ref: "provider:delegate:one", outcome: "confirmed" });
      expect(stop).toHaveBeenCalledWith("stopped");

      expect(runtime.withdrawExternal("delegate")).toBe(true);
      await vi.waitFor(() => expect(runtime.participantInfos().map((participant) => participant.id))
        .not.toContain("provider:delegate:two"), { timeout: 10_000 });
      await expect(runtime.stopParticipant("provider:delegate:two")).rejects.toThrow("Unknown Fabric participant");
    } finally {
      await runtime.shutdown();
    }
  });
});
