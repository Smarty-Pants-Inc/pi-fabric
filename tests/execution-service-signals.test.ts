import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { isMainExecutionCeilingError } from "../src/async-settlement.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import type { FabricHostCall, FabricSandboxOptions, FabricSandboxResult } from "../src/runtime/kernel.js";

const driver = vi.hoisted(() => ({
  kind: "Main" as "Main" | "Main-runtime-first" | "Main-runtime-first-floor" | "Escape" | "Escape-late" | "ordinary-runtime" | "ordinary-runtime-first-late" | "shorter-runtime-late" | "non-Main-runtime",
  escape: undefined as AbortController | undefined,
  escapeReason: undefined as Error | undefined,
  runtimeReason: undefined as Error | undefined,
  mainReason: undefined as Error | undefined,
}));

// Model the native backends' existing lossy outer-abort handlers, including
// their synchronous replacement before the service's dispatch settles.
vi.mock("../src/runtime/typescript-kernel.js", () => ({
  TypeScriptKernelRuntime: class {
    prepare(code: string) { return { code, checked: { errors: [] } }; }
    async execute(_code: string, hostCall: FabricHostCall, options: FabricSandboxOptions): Promise<FabricSandboxResult> {
      const runtimeHost = new AbortController();
      driver.mainReason = options.maximumDeadlineReason;
      if (driver.kind === "Main-runtime-first-floor") {
        // Mirror the real runtime's floor extension of the shared host record.
        options.executionDeadline!.extend(options.minimumTimeoutMsForHostCall!("demo.hold", {}));
      }
      const replaceOuterReason = () => runtimeHost.abort(new Error("Execution cancelled"));
      options.signal?.addEventListener("abort", replaceOuterReason, { once: true });
      const timer = driver.kind === "Main" ? undefined : setTimeout(() => {
        if (["Main-runtime-first", "Main-runtime-first-floor", "shorter-runtime-late", "Escape-late"].includes(driver.kind)) {
          // Cross the absolute deadline without allowing Main's timer to run,
          // then let the runtime watchdog be the first cancellation source.
          const until = Date.now() + (driver.kind === "Main-runtime-first" ? 40 : 140);
          while (Date.now() < until) {}
        }
        if (driver.kind.startsWith("Escape")) driver.escape!.abort(driver.escapeReason);
        else runtimeHost.abort(driver.runtimeReason);
        if (driver.kind === "ordinary-runtime-first-late") {
          const until = Date.now() + 140;
          while (Date.now() < until) {}
        }
      }, driver.kind === "Main-runtime-first" || driver.kind.endsWith("-late") || driver.kind === "Main-runtime-first-floor" ? 0 : 20);
      try {
        const value = await hostCall("demo.hold", {}, runtimeHost.signal);
        return { value, logs: [], terminationReason: "completed" };
      } catch {
        return { value: undefined, logs: [], terminationReason: options.signal?.aborted ? "aborted" : "timed_out", error: options.signal?.aborted ? "Execution cancelled" : driver.runtimeReason!.message };
      } finally {
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", replaceOuterReason);
      }
    }
  },
}));

describe("service provider cancellation reasons", () => {
  beforeEach(() => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
  });
  afterEach(() => vi.unstubAllEnvs());

  it.each(["Main", "Main-runtime-first", "Main-runtime-first-floor", "Escape", "Escape-late", "ordinary-runtime", "ordinary-runtime-first-late", "shorter-runtime-late", "non-Main-runtime"] as const)(
    "preserves %s cancellation at the provider boundary despite lossy runtime forwarding",
    async kind => {
      driver.kind = kind;
      driver.escape = new AbortController();
      driver.escapeReason = new Error("Escape requested");
      driver.runtimeReason = new Error("Execution timed out after 20ms");
      const registry = new ActionRegistry();
      const descriptor = { name: "hold", description: "pending provider", inputSchema: { type: "object", additionalProperties: false }, risk: "read" as const };
      let providerReason: unknown;
      registry.register({
        name: "demo", description: "local provider", async list() { return [descriptor]; }, async describe() { return descriptor; },
        async invoke(_name, _args, context) {
          return new Promise((_resolve, reject) => {
            const abort = () => { providerReason = context.signal!.reason; reject(providerReason); };
            if (context.signal!.aborted) abort();
            else context.signal!.addEventListener("abort", abort, { once: true });
          });
        },
      });
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      config.executor.mainMaxTimeoutMs = kind === "Main" || kind === "Main-runtime-first" ? 20
        : ["Main-runtime-first-floor", "Escape-late", "ordinary-runtime-first-late", "shorter-runtime-late"].includes(kind) ? 100 : 1_000;
      if (kind === "Main-runtime-first-floor" || kind === "shorter-runtime-late") config.executor.timeoutMs = 50;
      if (kind === "Main-runtime-first-floor") config.executor.hostCallTimeouts["demo.hold"] = 1_000;
      const context = { cwd: process.cwd(), mode: kind === "non-Main-runtime" ? "print" : "rpc", sessionManager: { getSessionId: () => "signals" } } as unknown as ExtensionContext;
      try {
        const result = await new FabricExecutionService(registry, config).execute({
          code: 'return tools.call({ ref: "demo.hold", args: {} });',
          context, signal: kind.startsWith("Escape") ? driver.escape.signal : undefined,
          parentToolCallId: `signal-${kind}`, onPartial() {},
        });
        expect(result.success).toBe(false);
        expect(result.trace.outcome).toBe(kind.startsWith("Escape") ? "aborted" : "timed_out");
        if (kind.startsWith("Main")) {
          expect(providerReason).toBeInstanceOf(Error);
          expect(providerReason).toBe(driver.mainReason);
          expect(isMainExecutionCeilingError(providerReason)).toBe(true);
          expect((providerReason as Error).message).toMatch(new RegExp(`^MainExecutionCeilingError: Main ceiling hit after ${config.executor.mainMaxTimeoutMs}ms`));
          expect((providerReason as Error).message).toContain("(executor.mainMaxTimeoutMs)");
        } else {
          expect(providerReason).toBe(kind.startsWith("Escape") ? driver.escapeReason : driver.runtimeReason);
          expect(isMainExecutionCeilingError(providerReason)).toBe(false);
        }
      } finally { await registry.close(); }
    },
  );
});
