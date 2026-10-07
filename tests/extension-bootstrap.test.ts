import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Value } from "typebox/value";
import { afterEach, describe, expect, it, vi } from "vitest";

const compilerLoaded = vi.hoisted(() => vi.fn());
vi.mock("../src/entropy/compiler.js", async original => {
  compilerLoaded();
  return original<typeof import("../src/entropy/compiler.js")>();
});

type Handler = (event: unknown, context: ExtensionContext) => unknown;
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
  vi.unstubAllEnvs();
});

const fixture = (kernel = "typescript") => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bootstrap-"));
  const agentDir = path.join(cwd, "agent");
  fs.mkdirSync(agentDir);
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
    executor: { kernel }, prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [],
  }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST"]) vi.stubEnv(key, undefined);
  const handlers = new Map<string, Handler[]>();
  const tools = new Map<string, ToolDefinition>();
  const registerTool = vi.fn((tool: ToolDefinition) => tools.set(tool.name, tool));
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on: vi.fn((event: string, handler: Handler) => {
      handlers.set(event, [...(handlers.get(event) ?? []), handler]);
    }),
    getActiveTools: vi.fn(() => ["fabric_exec"]), getAllTools: vi.fn(() => []),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool,
    setActiveTools: vi.fn(), sendMessage: vi.fn(), getThinkingLevel: vi.fn(() => "off"),
  } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => cwd, getBranch: () => [], getSessionFile: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const emit = async (name: string) => {
    const results = [];
    for (const handler of handlers.get(name) ?? []) results.push(await handler({}, context));
    return results;
  };
  cleanups.push(async () => {
    try { await emit("session_shutdown"); }
    finally { fs.rmSync(cwd, { recursive: true, force: true }); }
  });
  return { pi, context, emit, tools, handlers, registerTool };
};

describe("extension-only bootstrap", () => {
  it("registers the real contract, stays compiler-free at idle, and executes on first use", async () => {
    vi.resetModules();
    compilerLoaded.mockClear();
    const { default: register, FABRIC_MANAGED_HOST_VERSION } = await import("../src/extension-bootstrap.js");
    expect(FABRIC_MANAGED_HOST_VERSION).toBe(1);
    const host = fixture();
    const originalRegister = host.pi.registerTool;
    await register(host.pi);
    expect(host.pi.registerTool).toBe(originalRegister);
    expect(host.pi.registerCommand).toHaveBeenCalledWith("fabric", expect.anything());
    expect(compilerLoaded).not.toHaveBeenCalled();
    const tool = host.tools.get("fabric_exec")!;
    expect(tool.prepareArguments).toBeTypeOf("function");
    expect(tool.prepareLoadout).toBeTypeOf("function");
    expect(Value.Check(tool.parameters, {
      code: "return 7;", payloads: { note: "hello" }, tokenBudget: 100, agentBudget: 1,
      timeoutMs: 1000, resultFormat: "json", display: { name: "probe", description: "first-use check" },
    })).toBe(true);
    expect(Value.Check(tool.parameters, { code: "return 7;", resultFormat: "invalid" })).toBe(false);
    const discovery = await host.emit("resources_discover");
    expect(discovery).toContainEqual({ skillPaths: [path.resolve("skillsets/typescript")] });
    await host.emit("session_start");
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(compilerLoaded).not.toHaveBeenCalled();
    const result = await host.tools.get("fabric_exec")!.execute("first-use", { code: "return 7;", resultFormat: "json" }, undefined, undefined, { ...host.context, tools: [], executeTool: vi.fn() });
    expect(result.content).toContainEqual({ type: "text", text: "7" });
  }, 60_000); // Cold compiler/worker watchdog, not a latency assertion.

  it("keeps registrations and kernel resource discovery local to each host", async () => {
    const { default: register } = await import("../src/extension-bootstrap.js");
    const first = fixture("typescript");
    await register(first.pi);
    await first.emit("resources_discover");
    const second = fixture("python");
    await register(second.pi);
    const resources = await second.emit("resources_discover");
    expect(resources).toContainEqual({ skillPaths: [path.resolve("skillsets/python")] });
    await Promise.all([first.emit("session_start"), second.emit("session_start")]);
    expect(first.tools.get("fabric_exec")).not.toBe(second.tools.get("fabric_exec"));
    expect(first.tools.get("fabric_exec")!.parameters).toMatchObject({ properties: { code: { description: expect.stringContaining("TypeScript") } } });
    expect(second.tools.get("fabric_exec")!.parameters).toMatchObject({ properties: { code: { description: expect.stringContaining("Python") } } });
    expect(first.handlers.get("session_start")!.length).toBeGreaterThan(0);
    expect(second.handlers.get("session_start")).toHaveLength(first.handlers.get("session_start")!.length);
    expect(first.handlers.get("session_start")).not.toBe(second.handlers.get("session_start"));
  });
});
