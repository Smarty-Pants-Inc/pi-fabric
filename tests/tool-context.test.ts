import type { ExtensionContext, ExtensionRunner } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { createCapturedToolContext } from "../src/capture/tool-context.js";
import { wrapRegisteredToolForCapture } from "../src/capture/wrapper.js";
import { Type } from "typebox";

describe("captured tool execution context", () => {
  it("uses the live host context and preserves lazy guards and non-enumerable nested APIs across cwd overrides", async () => {
    let live = true;
    const executeTool = vi.fn(async () => ({ isError: false }));
    const native = Object.defineProperties({}, {
      cwd: { get: () => { if (!live) throw new Error("stale"); return "/original"; }, enumerable: true },
      model: { get: () => { if (!live) throw new Error("stale"); return "live-model"; }, enumerable: true },
      tools: { get: () => ["callable"] },
      executeTool: { value: executeTool },
    });
    const createToolContext = vi.fn(() => native);
    const runner = { createToolContext, createContext: vi.fn(), getActiveTools: () => [] } as unknown as ExtensionRunner;
    const signal = new AbortController().signal;
    const context = createCapturedToolContext(runner, "nested-id", signal, undefined, "/override");
    expect(createToolContext).toHaveBeenCalledWith("nested-id", signal);
    expect(context.cwd).toBe("/override");
    expect(context.tools).toEqual(["callable"]);
    await context.executeTool("target", {});
    expect(executeTool).toHaveBeenCalledWith("target", {});
    live = false;
    expect(() => context.model).toThrow("stale");
    expect(runner.createContext).not.toHaveBeenCalled();
  });

  it("injects a call-bound host context into captured execute", async () => {
    const native = { tools: [], executeTool: vi.fn() };
    const createToolContext = vi.fn(() => native);
    const runner = { createToolContext, getActiveTools: () => [] } as unknown as ExtensionRunner;
    const execute = vi.fn(async (_id: unknown, _params: unknown, _signal: unknown, _update: unknown, _context: { executeTool: unknown }) => ({ content: [], details: {} }));
    const wrapped = wrapRegisteredToolForCapture({
      definition: { name: "nested-tool", label: "nested", description: "fixture", parameters: Type.Object({}), execute },
      sourceInfo: {} as never,
    }, runner);
    const signal = new AbortController().signal;
    await wrapped.execute("fabric/nested", {}, signal, () => {});
    expect(createToolContext).toHaveBeenCalledWith("fabric/nested", signal);
    expect(execute.mock.calls[0]![4].executeTool).toBe(native.executeTool);
  });

  it("keeps legacy hosts working but refuses to invent a nested pipeline", async () => {
    const legacy = { cwd: "/legacy" } as ExtensionContext;
    const context = createCapturedToolContext(undefined, "legacy-id", undefined, legacy);
    expect(context.cwd).toBe("/legacy");
    await expect(context.executeTool("target", {})).rejects.toThrow("host support");
  });
});
