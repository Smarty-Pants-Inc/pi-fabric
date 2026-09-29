import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { TypeScriptKernelRuntime } from "../src/runtime/typescript-kernel.js";

afterEach(() => { vi.restoreAllMocks(); });

const context = { cwd: process.cwd(), hasUI: false } as ExtensionContext;

describe("fabric_exec prewarm (smarty-dev#2010)", () => {
  it("builds the checker for the same declarations the first call type-checks", async () => {
    const registry = new ActionRegistry();
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.fullCodeMode = false;
    const service = new FabricExecutionService(registry, config);
    const prepare = vi.spyOn(TypeScriptKernelRuntime.prototype, "prepare");
    await service.prewarm(context);
    expect(prepare).toHaveBeenCalledTimes(1);
    const result = await service.execute({
      code: "return 1 + 1;", signal: undefined, parentToolCallId: "call-1", context, onPartial() {},
    });
    expect(result.success).toBe(true);
    expect(result.value).toBe(2);
    expect(prepare).toHaveBeenCalledTimes(2);
    // Same full-code mode, unavailable providers, guest type sources and core overrides:
    // the same declaration string, so the first call reuses the prewarmed checker.
    expect(prepare.mock.calls[1]!.slice(1)).toEqual(prepare.mock.calls[0]!.slice(1));
  });

  it("does nothing for the Python kernel", async () => {
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.kernel = "python";
    const prepare = vi.spyOn(TypeScriptKernelRuntime.prototype, "prepare");
    await new FabricExecutionService(new ActionRegistry(), config).prewarm(context);
    expect(prepare).not.toHaveBeenCalled();
  });
});
