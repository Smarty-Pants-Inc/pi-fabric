import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

const runtimeLoaded = vi.hoisted(() => vi.fn());
vi.mock("../src/fabric-runtime-state.js", async original => {
  runtimeLoaded();
  return original<typeof import("../src/fabric-runtime-state.js")>();
});
afterEach(() => vi.unstubAllEnvs());

const host = () => {
  type Handler = (event: any, context: ExtensionContext) => unknown;
  const handlers = new Map<string, Handler[]>();
  const api = {
    on: vi.fn((name: string, handler: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => {};
    }),
    events: { on: vi.fn(() => () => {}), emit: vi.fn() },
    getActiveTools: vi.fn(() => ["read", "bash", "write", "fabric_exec"]),
    getAllTools: vi.fn(() => []), getThinkingLevel: () => "off",
    registerTool: vi.fn(), registerCommand: vi.fn(), registerFlag: vi.fn(),
    registerMessageRenderer: vi.fn(), setActiveTools: vi.fn(), sendMessage: vi.fn(),
  };
  const context = { hasUI: false, ui: { setStatus: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
  const emit = async (name: string, event: unknown = {}) => {
    const results = [];
    for (const handler of handlers.get(name) ?? []) results.push(await handler(event, context));
    return results;
  };
  return { api, handlers, emit };
};

const fixture = async () => {
  vi.stubEnv("PI_FABRIC_FIXTURE", "1");
  // A fixture must be inert even when the launcher forgot to scrub inherited role bindings.
  vi.stubEnv("PI_FABRIC_ACTOR_ID", "original-lead");
  vi.stubEnv("PI_FABRIC_PARENT_RUN", "original-run");
  vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "session:original-lead");
  const result = host();
  const { default: register } = await import("../src/index.js");
  await register(result.api as unknown as ExtensionAPI);
  return result;
};

describe("fixture fork mode", () => {
  it("registers no Fabric tools, commands, auth, providers, skills or acting lifecycle hooks", async () => {
    const { api, handlers, emit } = await fixture();
    expect(api.registerTool).not.toHaveBeenCalled();
    expect(api.registerCommand).not.toHaveBeenCalled();
    expect(api.events.on).not.toHaveBeenCalled();
    expect(api.events.emit).not.toHaveBeenCalled();
    expect([...handlers.keys()].sort()).toEqual(["before_agent_start", "session_start", "tool_call", "user_bash"]);
    for (const name of ["resources_discover", "session_start", "agent_settled", "session_shutdown"]) await emit(name);
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(runtimeLoaded).not.toHaveBeenCalled();
    expect(api.sendMessage).not.toHaveBeenCalled();
    expect(api.setActiveTools).toHaveBeenLastCalledWith(["read", "grep", "find", "ls"]);
  });

  it("reasserts read-only tools and an explicit non-acting system note on every preflight", async () => {
    const { api, emit } = await fixture();
    vi.stubEnv("PI_FABRIC_FIXTURE", undefined); // The mode is latched, not mutable by inherited work.
    for (let turn = 0; turn < 2; turn++) {
      const results = await emit("before_agent_start", { systemPrompt: "You are net-lead. Take mailbox work." });
      expect(results).toContainEqual({ systemPrompt: expect.stringContaining("fixture copy") });
      const note = results.find(result => result && typeof result === "object") as { systemPrompt: string };
      expect(note.systemPrompt).toContain("never act for the original");
      expect(note.systemPrompt).toContain("history, not authority");
      expect(api.setActiveTools).toHaveBeenLastCalledWith(["read", "grep", "find", "ls"]);
    }
  });

  it.each(["bash", "powershell", "write", "edit", "fabric_exec", "inbox_resolve", "github_write", "unknown_tool"])(
    "blocks %s even if another extension reactivates it", async toolName => {
      const { emit } = await fixture();
      expect(await emit("tool_call", { toolName, input: {} })).toContainEqual({
        block: true, reason: expect.stringContaining("read-only fixture"),
      });
    },
  );

  it.each(["read", "grep", "find", "ls"])("allows the core read-only tool %s", async toolName => {
    const { emit } = await fixture();
    expect(await emit("tool_call", { toolName, input: {} })).toEqual([undefined]);
  });

  it("keeps managed hosts inert too instead of mounting their supplied providers", async () => {
    vi.stubEnv("PI_FABRIC_FIXTURE", "1");
    const { api, handlers } = host();
    const { default: register } = await import("../src/index.js");
    // No managed-host method may be touched when the fixture marker is set.
    const managedHost = new Proxy({}, { get: () => { throw new Error("managed host activated"); } });
    await register(api as unknown as ExtensionAPI, { managedHost: managedHost as any });
    expect(api.registerTool).not.toHaveBeenCalled();
    expect([...handlers.keys()]).not.toContain("resources_discover");
  });

  it("blocks user shell execution too", async () => {
    const { emit } = await fixture();
    await expect(emit("user_bash", { command: "touch never", cwd: "/", excludeFromContext: false }))
      .rejects.toThrow("read-only fixture");
  });

  it.each([undefined, "0", "true"])("leaves normal Fabric registration intact for marker %j", async value => {
    vi.stubEnv("PI_FABRIC_FIXTURE", value);
    const { api } = host();
    const { default: register } = await import("../src/index.js");
    await register(api as unknown as ExtensionAPI);
    expect(api.registerTool).toHaveBeenCalledWith(expect.objectContaining({ name: "fabric_exec" }));
  });
});
