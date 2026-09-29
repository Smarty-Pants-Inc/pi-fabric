import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";

type ExtensionHandler = (event: unknown, context: ExtensionContext) => unknown;

afterEach(() => { vi.restoreAllMocks(); });

const spyRuntime = () => {
  const prewarm = vi.fn(async () => {});
  const ensure = vi.spyOn(FabricState.prototype, "ensure").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "execution", "get").mockReturnValue({ prewarm } as unknown as FabricState["execution"]);
  return { ensure, prewarm };
};

const harness = async () => {
  const handlers = new Map<string, ExtensionHandler[]>();
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []),
    getAllTools: vi.fn(() => []),
    on: vi.fn((event: string, handler: ExtensionHandler) => {
      handlers.set(event, [...(handlers.get(event) ?? []), handler]);
    }),
    registerCommand: vi.fn(),
    registerMessageRenderer: vi.fn(),
    registerTool: vi.fn(),
    setActiveTools: vi.fn(),
  } as unknown as ExtensionAPI;
  await piFabric(pi);
  const context = { cwd: process.cwd(), hasUI: false } as ExtensionContext;
  const stream = (type: string, name: string | undefined): void => {
    const event = {
      type: "message_update",
      assistantMessageEvent: {
        type, contentIndex: 0,
        partial: { role: "assistant", content: [name === undefined ? { type: "text", text: "" } : { type: "toolCall", id: "c", name, arguments: {} }] },
      },
    };
    for (const handler of handlers.get("message_update") ?? []) handler(event, context);
  };
  return { stream };
};

describe("fabric_exec prewarm trigger (smarty-dev#2010)", () => {
  it("prewarms once when the model starts streaming a fabric_exec call", async () => {
    const runtime = spyRuntime();
    const { stream } = await harness();
    stream("text_delta", undefined);
    stream("toolcall_start", "bash");
    expect(runtime.ensure).not.toHaveBeenCalled();
    stream("toolcall_start", "fabric_exec");
    stream("toolcall_delta", "fabric_exec");
    stream("toolcall_start", "fabric_exec");
    await vi.waitFor(() => expect(runtime.prewarm).toHaveBeenCalledTimes(1));
    expect(runtime.ensure).toHaveBeenCalledTimes(1);
  });

  it("swallows a failed prewarm: the call itself reports any error", async () => {
    const runtime = spyRuntime();
    runtime.ensure.mockRejectedValueOnce(new Error("mesh unavailable"));
    const { stream } = await harness();
    stream("toolcall_start", "fabric_exec");
    await vi.waitFor(() => expect(runtime.ensure).toHaveBeenCalledTimes(1));
    expect(runtime.prewarm).not.toHaveBeenCalled();
  });
});
