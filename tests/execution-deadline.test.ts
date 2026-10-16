import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore } from "../src/mesh/store.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";

const memoryLimitBytes = 32 * 1024 * 1024;
const boundMs = 100;
const guestDurationMs = 600;
const busyUntil = (until: number): void => { while (Date.now() < until) { /* Deliberately starve timers. */ } };

const pause = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const meshFrom = { id: "guest", name: "guest", kind: "agent" as const };
const deadlineContext = (id: string): ExtensionContext => ({
  cwd: process.cwd(), mode: "print", sessionManager: { getSessionId: () => id },
} as unknown as ExtensionContext);
const registerMeshPublish = (registry: ActionRegistry, mesh: MeshStore, onInvoke: () => void): void => {
  const descriptor = {
    name: "publish", description: "publish one mesh event",
    inputSchema: { type: "object", additionalProperties: true }, risk: "read" as const,
  };
  registry.register({
    name: "mesh", description: "test mesh", async list() { return [descriptor]; },
    async describe() { return descriptor; },
    async invoke(_name, args) {
      onInvoke();
      return mesh.publish({ topic: String(args.topic), from: meshFrom });
    },
  });
};
const heldMesh = (root: string) => {
  let release!: () => void;
  let acquired!: () => void;
  const ready = new Promise<void>(resolve => { acquired = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  const holder = new MeshLock(root, { lockTimeoutMs: 5_000 }, () => {});
  const settled = holder.withLockAcrossAwait(async () => { acquired(); await gate; });
  return { ready, release, settled };
};

// Recorded failure path: use a real held canonical .lock, not an indefinitely
// pending fake provider. Release is event-driven, independent of cold startup.
describe("execution deadline under microtask starvation", () => {
  beforeEach(() => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
  });
  afterEach(() => vi.unstubAllEnvs());

  it.each([
    ["Main immediate agents.wait", 'await agents.wait({ id: "done" });', true],
    ["Main computed wait", 'await tools.call({ ref: ["agents", "wait"].join("."), args: { id: "done" } });', true],
    ["Main discovery", "await tools.providers();", true],
    ["ordinary immediate call", 'await tools.call({ ref: "demo.done", args: {} });', false],
  ] as const)("stops %s near the bound, not at the guest's finite escape", async (_name, call, main) => {
    const registry = new ActionRegistry();
    let calls = 0;
    let lastCallAt = 0;
    let coldDispatch = true;
    let admit = () => {};
    const descriptor = { name: main ? "wait" : "done", description: "immediate result", inputSchema: { type: "object", additionalProperties: true }, risk: "read" as const };
    registry.register({
      name: main ? "agents" : "demo", description: "immediate provider",
      async list() { return [descriptor]; },
      async describe() {
        // Deterministically model the first-dispatch initialization cost seen in
        // both CI jobs. prewarm alone does not visit actual provider dispatch.
        if (coldDispatch) { coldDispatch = false; busyUntil(Date.now() + boundMs + 50); }
        return descriptor;
      },
      async invoke() {
        admit();
        calls++; lastCallAt = performance.now();
        // Each already-resolved call does modest synchronous work: too few guest
        // instructions for the periodic VM interrupt to rescue a starved timer.
        if (!main) busyUntil(Date.now() + 5);
        return { status: "completed" };
      },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = main ? 500 : boundMs;
    config.executor.mainMaxTimeoutMs = boundMs;
    const context = { cwd: process.cwd(), mode: main ? "rpc" : "print", sessionManager: { getSessionId: () => "deadline-test" } } as unknown as ExtensionContext;
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(context);
    // Initialize the actual descriptor/provider/bridge path under a separate
    // setup budget. Do not charge cold dispatch to the starvation measurement.
    config.executor.timeoutMs = 5_000;
    config.executor.mainMaxTimeoutMs = 5_000;
    const initialized = await service.execute({
      code: `${call} return "initialized";`, context, signal: undefined,
      parentToolCallId: "starvation-setup", onPartial() {},
    });
    expect(initialized.success).toBe(true);
    expect(initialized.value).toBe("initialized");
    if (!call.includes("tools.providers")) expect(calls).toBe(1);
    calls = 0; lastCallAt = 0;
    config.executor.timeoutMs = main ? 500 : boundMs;
    config.executor.mainMaxTimeoutMs = boundMs;
    let began = 0;
    let admitted = false;
    if (call.includes("tools.providers")) {
      const providers = registry.providers.bind(registry);
      vi.spyOn(registry, "providers").mockImplementation(ctx => { admit(); return providers(ctx); });
    }
    try {
      const result = await executeAfterAdmission((signal, startClock) => {
        admit = () => {
          if (admitted) return;
          admitted = true;
          startClock();
          began = performance.now();
        };
        return service.execute({
          code: `const until = Date.now() + ${guestDurationMs}; while (Date.now() < until) { ${call} } return "escaped";`,
          ...(main ? { requestedTimeoutMs: 86_400_000 } : {}),
          context, signal, parentToolCallId: "starvation", onPartial() {},
        });
      }, () => admitted);
      expect(result.typeErrors).toBeUndefined();
      expect(result.success).toBe(false);
      expect(result.trace.outcome).toBe("timed_out");
      expect(result.value).toBeUndefined();
      expect(result.error).toMatch(main ? /MainExecutionCeilingError.*Main ceiling hit/ : /Execution timed out after 100ms/);
      // Wide scheduling margin, but strictly less than the finite exploit's 600ms.
      expect(performance.now() - began).toBeLessThan(guestDurationMs - 100);
      if (!call.includes("tools.providers")) {
        expect(calls).toBeGreaterThan(1);
        expect(lastCallAt - began).toBeLessThan(boundMs + 100);
      }
    } finally { await registry.close(); }
  });

  it.each([true, false])("does not publish a late host result or dispatch a tail call (Main=%s)", async (main) => {
    const registry = new ActionRegistry();
    const descriptor = { name: "done", description: "timer-starving host", inputSchema: { type: "object", additionalProperties: true }, risk: "read" as const };
    let calls = 0;
    let admit = () => {};
    registry.register({
      name: "demo", description: "busy provider", async list() { return [descriptor]; }, async describe() { return descriptor; },
      async invoke() { admit(); calls++; busyUntil(Date.now() + boundMs + 50); return "late"; },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = main ? 500 : boundMs;
    config.executor.mainMaxTimeoutMs = boundMs;
    const context = { cwd: process.cwd(), mode: main ? "rpc" : "print", sessionManager: { getSessionId: () => "late-host" } } as unknown as ExtensionContext;
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(context);
    let admitted = false;
    try {
      const result = await executeAfterAdmission((signal, startClock) => {
        admit = () => { admitted = true; startClock(); };
        return service.execute({
          code: 'try { await tools.call({ ref: "demo.done", args: {} }); } catch {} await tools.providers(); return "escaped";',
          context, signal, parentToolCallId: "late-result", onPartial() {},
        });
      }, () => admitted);
      expect(result.success).toBe(false);
      expect(result.trace.outcome).toBe("timed_out");
      expect(result.error).toMatch(main ? /MainExecutionCeilingError/ : /Execution timed out after 100ms/);
      expect(result.value).toBeUndefined();
      expect(calls).toBe(1);
      expect(result.trace.operations.some(operation => operation.ref === "fabric.discovery.providers")).toBe(false);
    } finally { await registry.close(); }
  });

  it("enforces the ordinary runtime deadline without a service watchdog", async () => {
    const runtime = new QuickJsRuntime();
    await runtime.execute("return 1;", async () => undefined, { timeoutMs: 1_000, memoryLimitBytes });
    let calls = 0;
    let began = 0;
    let admitted = false;
    const result = await executeAfterAdmission((signal, startClock) => runtime.execute(
      `const until = Date.now() + ${guestDurationMs}; while (Date.now() < until) await tools.providers(); return "escaped";`,
      async () => {
        if (!admitted) { admitted = true; startClock(); began = performance.now(); }
        calls++; busyUntil(Date.now() + 5); return [];
      },
      { timeoutMs: boundMs, memoryLimitBytes, signal },
    ), () => admitted);
    expect(result.terminationReason).toBe("timed_out");
    expect(result.error).toBe("Execution timed out after 100ms");
    expect(result.value).toBeUndefined();
    expect(calls).toBeGreaterThan(1);
    expect(performance.now() - began).toBeLessThan(guestDurationMs - 100);
  });

  it("checks immediately before host dispatch, after bridge preparation", async () => {
    let calls = 0;
    const result = await new QuickJsRuntime().execute(
      "return tools.providers();",
      async () => { calls++; return []; },
      {
        timeoutMs: boundMs, memoryLimitBytes,
        minimumTimeoutMsForHostCall() { busyUntil(Date.now() + boundMs + 50); return undefined; },
      },
    );
    expect(result.terminationReason).toBe("timed_out");
    expect(result.error).toBe("Execution timed out after 100ms");
    expect(calls).toBe(0);
  });

  it("keeps the CPU interrupt on the absolute ceiling after a host-call floor", async () => {
    const runtime = new QuickJsRuntime();
    await runtime.execute("return 1;", async () => undefined, { timeoutMs: 1_000, memoryLimitBytes });
    let calls = 0;
    let began = 0;
    let admitted = false;
    const result = await executeAfterAdmission((signal, startClock) => runtime.execute(
      "await tools.providers(); while (true) {}",
      async () => {
        admitted = true; startClock(); began = performance.now();
        calls++; busyUntil(Date.now() + 30); return [];
      },
      {
        timeoutMs: 500, memoryLimitBytes, signal,
        maximumDeadlineAt: Date.now() + boundMs,
        minimumTimeoutMsForHostCall: () => 5_000,
      },
    ), () => admitted);
    expect(result.terminationReason).toBe("timed_out");
    expect(calls).toBe(1);
    expect(performance.now() - began).toBeLessThan(guestDurationMs - 100);
  });

  it("uses Main's absolute deadline to interrupt CPU-only guest work without host calls", async () => {
    const registry = new ActionRegistry();
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = 500;
    config.executor.mainMaxTimeoutMs = boundMs;
    const context = { cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main-cpu" } } as unknown as ExtensionContext;
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(context);
    const began = performance.now();
    try {
      const result = await service.execute({
        code: "while (true) {}", context, signal: undefined, parentToolCallId: "main-cpu", onPartial() {},
      });
      expect(result.trace.outcome).toBe("timed_out");
      expect(result.error).toMatch(/MainExecutionCeilingError/);
      expect(result.trace.operations).toHaveLength(0);
      expect(performance.now() - began).toBeLessThan(guestDurationMs - 100);
    } finally { await registry.close(); }
  });

  it("uses the ordinary deadline to interrupt CPU-only guest work", async () => {
    let calls = 0;
    const result = await new QuickJsRuntime().execute(
      "while (true) {}",
      async () => { calls++; return undefined; },
      { timeoutMs: boundMs, memoryLimitBytes },
    );
    expect(result.terminationReason).toBe("timed_out");
    expect(result.error).toBe("Execution timed out after 100ms");
    expect(calls).toBe(0);
  });

  it.each(["timeout", "release", "abort", "pause"] as const)("bounds a real guest mesh lock wait: %s", async ending => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "execution-mesh-deadline-"));
    const mesh = new MeshStore(base, 65_536, 100, { lockTimeoutMs: 5_000 });
    const registry = new ActionRegistry();
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = ending === "timeout" ? boundMs : ending === "pause" ? 50 : 500;
    config.executor.maxTimeoutMs = 5_000;
    const context = deadlineContext(`guest-mesh-${ending}`);
    const service = new FabricExecutionService(registry, config);
    let admit = () => {};
    let admitted = false;
    const abort = new AbortController();
    if (ending === "pause") {
      config.fullCodeMode = true;
      config.executor.humanWaitRefs = ["extensions.ask"];
      const descriptor = {
        name: "ask", description: "paused extension",
        inputSchema: { type: "object", additionalProperties: true }, risk: "read" as const,
      };
      registry.register({
        name: "extensions", description: "test extension", async list() { return [descriptor]; },
        async describe() { return descriptor; },
        async invoke() {
          admit();
          await pause(120);
          await mesh.publish({ topic: "guest", from: meshFrom });
          return "published";
        },
      });
    } else registerMeshPublish(registry, mesh, () => admit());
    await service.prewarm(context);
    const held = heldMesh(base);
    await held.ready;
    // The ordinary caller is outside the guest's async context and must retain
    // its own budget even while queued ahead of the guest on the same lock.
    const ordinary = ending === "timeout" ? mesh.publish({ topic: "ordinary", from: meshFrom }) : undefined;
    void ordinary?.catch(() => undefined);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const result = await executeAfterAdmission((signal, startClock) => {
        admit = () => {
          if (admitted) return;
          admitted = true;
          startClock();
          if (ending === "release" || ending === "pause") timer = setTimeout(held.release, ending === "pause" ? 180 : 20);
          if (ending === "abort") timer = setTimeout(() => abort.abort(new Error("test abort")), 20);
        };
        return service.execute({
          code: ending === "pause" ? 'return await extensions.ask({});' : ending === "timeout"
            ? 'try { await tools.call({ ref: "mesh.publish", args: { topic: "guest" } }); } catch (error) { if (error?.code !== "FABRIC_MESH_LOCK_TIMEOUT") throw error; } return "continued";'
            : 'await tools.call({ ref: "mesh.publish", args: { topic: "guest" } }); return "published";',
          context, signal: AbortSignal.any([signal, abort.signal]), parentToolCallId: `guest-mesh-${ending}`, onPartial() {},
        });
      }, () => admitted);
      expect(result.typeErrors).toBeUndefined();
      expect(result.success).toBe(ending !== "abort");
      if (ending === "abort") expect(result.trace.outcome).toBe("aborted");
      else expect(result.value).toBe(ending === "timeout" ? "continued" : "published");
      held.release();
      await held.settled;
      if (ordinary) expect((await ordinary).topic).toBe("ordinary");
      // Drain the original aborted provider too: the no-append assertion is
      // after release, so it catches a late commit rather than only a wait.
      await registry.close();
      await pause(30);
      expect(mesh.read({ topic: "guest" })).toHaveLength(ending === "release" || ending === "pause" ? 1 : 0);
    } finally {
      clearTimeout(timer);
      held.release();
      await held.settled;
      await ordinary?.catch(() => undefined);
      await registry.close();
      mesh.closeState();
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  // Data-safety regression: resolved failure values must not authorize a
  // background writer that was already waiting on the real canonical lock.
  it.each([
    { status: "failed", callOutcome: "failed", ending: "timeout", oversized: false },
    { status: "stopped", callOutcome: "aborted", ending: "abort", oversized: false },
    { status: "timed_out", callOutcome: "timed_out", ending: "timeout", oversized: false },
    { status: "failed", callOutcome: "failed", ending: "timeout", oversized: true },
    { status: "completed", callOutcome: "succeeded", ending: "timeout", oversized: false },
    { status: "completed", callOutcome: "succeeded", ending: "abort", oversized: false },
  ] as const)("preserves queued mesh writes only for canonical call success: $status/$ending (oversized=$oversized)", async ({ status, callOutcome, ending, oversized }) => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "execution-mesh-returned-"));
    const mesh = new MeshStore(base, 65_536, 100, { lockTimeoutMs: 5_000 });
    const registry = new ActionRegistry();
    let background: Promise<{ value?: unknown; error?: unknown }> = Promise.resolve({});
    let received: unknown;
    let admit = () => {};
    let admitted = false;
    const abort = new AbortController();
    const returned = status === "completed" ? { status, id: "detached-started" } : {
      status, error: `spawn ${status}`,
      ...(oversized ? { detail: "oversized failure detail ".repeat(100) } : {}),
    };
    const descriptor = {
      name: "spawn", description: "detached actor start with a returned outcome",
      inputSchema: { type: "object", additionalProperties: true }, risk: "read" as const,
    };
    registry.register({
      name: "agents", description: "test agents", async list() { return [descriptor]; },
      async describe() { return descriptor; },
      async invoke() {
        // Queue under the inherited invocation BEFORE resolving the spawn call.
        background = mesh.publish({ topic: "detached", from: meshFrom })
          .then(value => ({ value }), error => ({ error }));
        return returned;
      },
    });
    const receiptDescriptor = { ...descriptor, name: "received", description: "guest result receipt" };
    registry.register({
      name: "demo", description: "guest receipt", async list() { return [receiptDescriptor]; },
      async describe() { return receiptDescriptor; },
      async invoke(_name, args) {
        // Admission here proves the guest received the resolved value, rather
        // than catching a newly thrown error. Cold startup costs no CPU budget.
        received = args.result;
        admit();
        return "received";
      },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = ending === "timeout" ? boundMs : 5_000;
    config.executor.hostCallTimeouts = {};
    if (oversized) config.executor.maxNestedResultChars = 256;
    const context = deadlineContext(`guest-mesh-returned-${status}-${ending}-${oversized}`);
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(context);
    const held = heldMesh(base);
    await held.ready;
    try {
      const result = await executeAfterAdmission((signal, startClock) => {
        admit = () => { admitted = true; startClock(); };
        return service.execute({
          code: `const result = await tools.call({ ref: "agents.spawn", args: {} });
            await tools.call({ ref: "demo.received", args: { result } });
            ${ending === "timeout" ? "while (true) {}" : "await new Promise(() => {});"}`,
          context, signal: AbortSignal.any([signal, abort.signal]),
          parentToolCallId: "guest-mesh-returned", onPartial() {},
        });
      }, () => admitted, () => {
        if (ending === "abort") abort.abort(new Error("test abort after returned result"));
      });
      expect(result.typeErrors).toBeUndefined();
      expect(result.success).toBe(false);
      expect(result.trace.outcome).toBe(ending === "timeout" ? "timed_out" : "aborted");
      if (oversized) {
        expect(received).toMatchObject({ fabricTruncated: true, originalChars: JSON.stringify(returned).length });
        expect(received).not.toHaveProperty("status");
      } else expect(received).toEqual(returned);
      expect(result.audits.find(audit => audit.ref === "agents.spawn")?.success).toBe(status === "completed");
      expect(result.trace.operations.find(operation => operation.ref === "agents.spawn")).toMatchObject({
        outcome: callOutcome,
        ...(status === "completed" ? {} : { failureStage: "invoke" }),
        ...(oversized ? { resultTruncated: true } : {}),
      });
      expect(mesh.read({ topic: "detached" })).toHaveLength(0);
      held.release();
      await held.settled;
      // Drain the original writer AFTER release, so no-append catches a late
      // commit. Confirmed successful spawns still commit despite guest teardown.
      const outcome = await background;
      if (status === "completed") expect(outcome.value).toMatchObject({ topic: "detached" });
      else expect(outcome.error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(mesh.read({ topic: "detached" })).toHaveLength(status === "completed" ? 1 : 0);
    } finally {
      held.release();
      await held.settled;
      await background;
      await registry.close();
      mesh.closeState();
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  it.each([true, false])("preserves detached mesh refresh only after successful agents.spawn: %s", async success => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "execution-mesh-detached-"));
    const mesh = new MeshStore(base, 65_536, 100, { lockTimeoutMs: 5_000 });
    const registry = new ActionRegistry();
    let background: Promise<{ value?: unknown; error?: unknown }> = Promise.resolve({});
    const descriptor = {
      name: "spawn", description: "detached actor start",
      inputSchema: { type: "object", additionalProperties: true }, risk: "read" as const,
    };
    registry.register({
      name: "agents", description: "test agents", async list() { return [descriptor]; },
      async describe() { return descriptor; },
      async invoke() {
        // Start waiting BEFORE the host call returns, not from a later timer.
        background = mesh.publish({ topic: "detached", from: meshFrom })
          .then(value => ({ value }), error => ({ error }));
        if (!success) throw new Error("spawn failed");
        return { id: "detached-started" };
      },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = 500;
    const context = deadlineContext("guest-mesh-detached");
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(context);
    const held = heldMesh(base);
    await held.ready;
    try {
      const result = await service.execute({
        code: 'try { await tools.call({ ref: "agents.spawn", args: {} }); } catch {} return "continued";',
        context, signal: undefined, parentToolCallId: "guest-mesh-detached", onPartial() {},
      });
      expect(result.typeErrors).toBeUndefined();
      expect(result.success).toBe(true);
      expect(result.value).toBe("continued");
      held.release();
      await held.settled;
      const outcome = await background;
      if (success) expect(outcome.value).toMatchObject({ topic: "detached" });
      else expect(outcome.error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(mesh.read({ topic: "detached" })).toHaveLength(success ? 1 : 0);
    } finally {
      held.release();
      await held.settled;
      await background;
      await registry.close();
      mesh.closeState();
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});
