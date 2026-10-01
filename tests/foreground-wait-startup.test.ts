import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
vi.mock("../src/guards/foreground-wait.js", async original => {
  loads(); return original<typeof import("../src/guards/foreground-wait.js")>();
});

describe("foreground shell parser startup", () => {
  it("stays unloaded through cold import/idle registration and loads on first bash only", async () => {
    vi.resetModules(); loads.mockClear();
    const handlers = new Map<string, Array<(...args: any[]) => unknown>>();
    const pi = { events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      getActiveTools: () => [], getAllTools: () => [], setActiveTools: vi.fn(),
      on: (name: string, handler: (...args: any[]) => unknown) => handlers.set(name, [...handlers.get(name) ?? [], handler]),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    } as unknown as ExtensionAPI;
    try {
      const { default: fabric } = await import("../src/index.js");
      expect(loads).not.toHaveBeenCalled();
      await fabric(pi); await new Promise(resolve => setTimeout(resolve, 0));
      expect(loads).not.toHaveBeenCalled();
      const call = async (toolName: string, input: object) => {
        const results: any[] = [];
        for (const handler of handlers.get("tool_call") ?? []) {
          try { results.push(await handler({ type: "tool_call", toolName, toolCallId: "cold", input }, { cwd: process.cwd(), hasUI: false })); } catch { /* unrelated live-session hooks */ }
        }
        return results.find(result => result?.block);
      };
      expect(await call("read", { path: "unused" })).toBeUndefined(); expect(loads).not.toHaveBeenCalled();
      expect(await call("bash", { command: "sleep 900" })).toMatchObject({ block: true, reason: expect.stringContaining("foreground wait") });
      expect(loads).toHaveBeenCalledTimes(1);
      expect(await call("bash", { command: "sleep 1", timeout: 5 })).toBeUndefined();
      expect(loads).toHaveBeenCalledTimes(1);
    } finally { for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown(); }
  });
});
