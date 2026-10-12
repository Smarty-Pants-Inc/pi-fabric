import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { fabricToolPlacement } from "../src/core/tool-ownership.js";
import piFabric from "../src/index.js";
import {
  FABRIC_TOOL_PLACEMENT_EVENT,
  readFabricToolPlacementRequestV1,
  type FabricToolPlacementResultV1,
} from "../src/protocol.js";

const registered = ["fabric_exec", "read", "bash", "my_tool", "hidden_tool"];

describe("fabricToolPlacement", () => {
  it("declares only fabric_exec to the model in exclusive modes", () => {
    for (const mode of ["full-code", "enforce"] as const) {
      expect(fabricToolPlacement({
        mode,
        registered,
        active: ["fabric_exec", "read", "bash", "my_tool"],
        program: (name) => name === "read" || name === "bash" || (mode === "full-code" && name === "my_tool"),
      })).toEqual({
        version: 1,
        mode,
        tools: {
          fabric_exec: "model",
          read: "program",
          bash: "program",
          my_tool: mode === "full-code" ? "program" : "unavailable",
          hidden_tool: "unavailable",
        },
      });
    }
  });

  it("uses Pi's active set in orchestration mode and model wins over program", () => {
    const result = fabricToolPlacement({
      mode: "orchestration",
      registered,
      active: ["fabric_exec", "read", "my_tool"],
      program: (name) => name === "read",
    });
    expect(result.tools).toEqual({
      fabric_exec: "model",
      read: "model",
      bash: "unavailable",
      my_tool: "model",
      hidden_tool: "unavailable",
    });
  });

  it("answers only the requested tools, including unregistered names", () => {
    const result = fabricToolPlacement({
      mode: "full-code",
      registered,
      active: ["fabric_exec"],
      program: (name) => name === "read",
      tools: ["read", "nope", "read"],
    });
    expect(result.tools).toEqual({ read: "program", nope: "unavailable" });
  });
});

describe("tool placement event", () => {
  it("validates queries", () => {
    const reply = () => undefined;
    expect(readFabricToolPlacementRequestV1({ reply })).toBeDefined();
    expect(readFabricToolPlacementRequestV1({ tools: ["read"], reply })).toBeDefined();
    expect(readFabricToolPlacementRequestV1({ tools: ["read"] })).toBeUndefined();
    expect(readFabricToolPlacementRequestV1({ tools: "read", reply })).toBeUndefined();
    expect(readFabricToolPlacementRequestV1({ tools: [""], reply })).toBeUndefined();
    expect(readFabricToolPlacementRequestV1({ tools: [1], reply })).toBeUndefined();
    expect(readFabricToolPlacementRequestV1({ tools: Array.from({ length: 1_025 }, (_, i) => `t${i}`), reply })).toBeUndefined();
  });

  it("replies synchronously from the extension listener", async () => {
    const listeners = new Map<string, (value: unknown) => unknown>();
    const handlers = new Map<string, Array<(...args: never[]) => unknown>>();
    const pi = {
      events: {
        emit: vi.fn(),
        on: vi.fn((channel: string, handler: (value: unknown) => unknown) => {
          listeners.set(channel, handler);
          return () => listeners.delete(channel);
        }),
      },
      getActiveTools: vi.fn(() => ["fabric_exec", "read"]),
      getAllTools: vi.fn(() => [{ name: "fabric_exec" }, { name: "read" }, { name: "my_tool" }]),
      on: vi.fn((event: string, handler: (...args: never[]) => unknown) => {
        handlers.set(event, [...(handlers.get(event) ?? []), handler]);
      }),
      registerCommand: vi.fn(),
      registerMessageRenderer: vi.fn(),
      registerTool: vi.fn(),
      setActiveTools: vi.fn(),
    } as unknown as ExtensionAPI;
    await piFabric(pi);
    const query = listeners.get(FABRIC_TOOL_PLACEMENT_EVENT)!;
    let result: FabricToolPlacementResultV1 | undefined;
    query({ reply: (value: FabricToolPlacementResultV1) => { result = value; } });
    // Before bootstrap: default full-code policy and no mounted program surface.
    expect(result).toEqual({
      version: 1,
      mode: "full-code",
      tools: { fabric_exec: "model", read: "unavailable", my_tool: "unavailable" },
    });
    expect(() => query({ tools: "read", reply: () => undefined })).toThrow("Invalid Pi Fabric tool placement query");
    for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown();
    expect(listeners.has(FABRIC_TOOL_PLACEMENT_EVENT)).toBe(false);
  });
});
