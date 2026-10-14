import type { ExtensionRunner, RegisteredTool } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { wrapRegisteredToolForCapture } from "../src/capture/wrapper.js";

const result = { content: [{ type: "text", text: "ok" }] };

const setup = (withFactory = true) => {
  const executeTool = vi.fn();
  const nativeCtx = {
    cwd: "/native",
    tools: [{ name: "probe" }],
    executeTool,
    sessionManager: { getSessionId: () => "session-native" },
  };
  const fallbackCtx = { cwd: "/fallback" };
  const execute = vi.fn(async (..._args: unknown[]) => result);
  const createToolContext = vi.fn((_id: string, _signal?: AbortSignal) => nativeCtx);
  const createContext = vi.fn(() => fallbackCtx);
  const runner = {
    ...(withFactory ? { createToolContext } : {}),
    createContext,
    getActiveTools: () => ["probe"],
  } as unknown as ExtensionRunner;
  const registered = { definition: { name: "probe", label: "Probe", description: "d", parameters: {}, execute } } as unknown as RegisteredTool;
  return { wrapped: wrapRegisteredToolForCapture(registered, runner), execute, createToolContext, createContext, nativeCtx, fallbackCtx, executeTool };
};

describe("captured tool context", () => {
  it("uses the native createToolContext when no explicit ctx is supplied", async () => {
    const { wrapped, execute, createToolContext, createContext, nativeCtx, executeTool } = setup();
    const signal = new AbortController().signal;
    const params = { q: 1 };
    const out = await wrapped.execute("call-1", params, signal, () => {});
    expect(out).toBe(result);
    expect(createToolContext).toHaveBeenCalledTimes(1);
    expect(createToolContext).toHaveBeenCalledWith("call-1", signal);
    expect(createContext).not.toHaveBeenCalled();
    const ctx = execute.mock.calls[0]![4] as typeof nativeCtx;
    expect(execute.mock.calls[0]![0]).toBe("call-1");
    expect(execute.mock.calls[0]![1]).toBe(params);
    expect(ctx).toBe(nativeCtx);
    expect(ctx.tools).toBe(nativeCtx.tools);
    expect(ctx.executeTool).toBe(executeTool);
  });

  it("explicit ctx wins unchanged; abort never touches execute or factories", async () => {
    const { wrapped, execute, createToolContext, createContext } = setup();
    let cwdReads = 0;
    const base = { sessionManager: { getSessionId: () => "s-shell" } };
    const shellCtx = Object.create(base, { cwd: { get: () => { cwdReads++; return "/judged"; }, enumerable: true } });
    await wrapped.execute("call-2", {}, undefined, () => {}, shellCtx);
    const received = execute.mock.calls[0]![4] as { cwd: string; sessionManager: unknown };
    expect(received).toBe(shellCtx);
    expect(Object.getPrototypeOf(received)).toBe(base);
    expect(received.cwd).toBe("/judged");
    expect(cwdReads).toBe(1);
    expect(createToolContext).not.toHaveBeenCalled();
    expect(createContext).not.toHaveBeenCalled();

    const controller = new AbortController();
    controller.abort(new Error("caller aborted"));
    await expect(wrapped.execute("call-3", {}, controller.signal, () => {})).rejects.toThrow("caller aborted");
    expect(execute).toHaveBeenCalledTimes(1);
    expect(createToolContext).not.toHaveBeenCalled();
    expect(createContext).not.toHaveBeenCalled();
  });

  it("falls back to createContext on an old runner without createToolContext", async () => {
    const { wrapped, execute, createContext, fallbackCtx } = setup(false);
    expect(await wrapped.execute("call-4", {}, undefined, () => {})).toBe(result);
    expect(createContext).toHaveBeenCalledTimes(1);
    expect(execute.mock.calls[0]![4]).toBe(fallbackCtx);
  });
});
