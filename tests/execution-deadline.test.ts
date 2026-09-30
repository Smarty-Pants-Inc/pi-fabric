import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

const memoryLimitBytes = 32 * 1024 * 1024;
const boundMs = 100;
const guestDurationMs = 600;
const busyUntil = (until: number): void => { while (Date.now() < until) { /* Deliberately starve timers. */ } };

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
    const began = performance.now();
    try {
      const result = await service.execute({
        code: `const until = Date.now() + ${guestDurationMs}; while (Date.now() < until) { ${call} } return "escaped";`,
        ...(main ? { requestedTimeoutMs: 86_400_000 } : {}),
        context, signal: undefined, parentToolCallId: "starvation", onPartial() {},
      });
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
    registry.register({
      name: "demo", description: "busy provider", async list() { return [descriptor]; }, async describe() { return descriptor; },
      async invoke() { calls++; busyUntil(Date.now() + boundMs + 50); return "late"; },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = main ? 500 : boundMs;
    config.executor.mainMaxTimeoutMs = boundMs;
    const context = { cwd: process.cwd(), mode: main ? "rpc" : "print", sessionManager: { getSessionId: () => "late-host" } } as unknown as ExtensionContext;
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(context);
    try {
      const result = await service.execute({
        code: 'try { await tools.call({ ref: "demo.done", args: {} }); } catch {} await tools.providers(); return "escaped";',
        context, signal: undefined, parentToolCallId: "late-result", onPartial() {},
      });
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
    const began = performance.now();
    const result = await runtime.execute(
      `const until = Date.now() + ${guestDurationMs}; while (Date.now() < until) await tools.providers(); return "escaped";`,
      async () => { calls++; busyUntil(Date.now() + 5); return []; },
      { timeoutMs: boundMs, memoryLimitBytes },
    );
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
    const began = performance.now();
    const result = await runtime.execute(
      "await tools.providers(); while (true) {}",
      async () => { calls++; busyUntil(Date.now() + 30); return []; },
      {
        timeoutMs: 500, memoryLimitBytes,
        maximumDeadlineAt: Date.now() + boundMs,
        minimumTimeoutMsForHostCall: () => 5_000,
      },
    );
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
});
