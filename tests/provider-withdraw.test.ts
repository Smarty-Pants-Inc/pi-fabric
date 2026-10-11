import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricState } from "../src/fabric-state.js";
import {
  FABRIC_PROVIDER_WITHDRAW_EVENT,
  readFabricProviderWithdrawalV1,
  type FabricInvocationContext,
  type FabricProvider,
} from "../src/protocol.js";
import piFabric from "../src/index.js";

const context: FabricInvocationContext = {
  cwd: process.cwd(),
  signal: undefined,
  parentToolCallId: "parent",
  nestedToolCallId: "metadata",
  extensionContext: {} as ExtensionContext,
  update() {},
};

const provider = (value: string, close = vi.fn()): FabricProvider => ({
  name: "demo",
  description: `Demo ${value}`,
  async list() { return []; },
  async describe(name) {
    return name === "echo"
      ? { name: "echo", description: "Echo", inputSchema: { type: "object", additionalProperties: false }, risk: "read" }
      : undefined;
  },
  async invoke() { return value; },
  close,
});

const invoke = (registry: ActionRegistry, invocation: FabricInvocationContext = context): Promise<unknown> =>
  registry.invoke("demo.echo", {}, { ...invocation, approve: async () => {}, audits: [], maxResultChars: 10_000 });

describe("provider withdrawal", () => {
  it("refuses new calls after withdrawal, keeps the host-owned instance open, and allows re-registration", async () => {
    const registry = new ActionRegistry();
    const close = vi.fn();
    const instance = provider("first", close);
    registry.register(instance);
    expect(await invoke(registry)).toBe("first");

    expect(registry.unregister("demo", { provider: instance, keepProviderOpen: true })).toBe(instance);
    expect(registry.has("demo")).toBe(false);
    await expect(invoke(registry)).rejects.toThrow();
    await Promise.resolve();
    expect(close).not.toHaveBeenCalled();

    registry.register(instance);
    expect(await invoke(registry)).toBe("first");
    await registry.close(new Set(["demo"]));
  });

  it("pins withdrawal to a generation, binding id, or provider instance", async () => {
    const registry = new ActionRegistry();
    const instance = provider("first");
    registry.register(instance);
    const binding = registry.providerStatus()[0]!;
    expect(binding.generation).toBe(1);

    expect(registry.unregister("demo", { generation: 2 })).toBeUndefined();
    expect(registry.unregister("demo", { generation: "not-the-binding" })).toBeUndefined();
    expect(registry.unregister("demo", { provider: provider("other") })).toBeUndefined();
    expect(await invoke(registry)).toBe("first");

    expect(registry.unregister("demo", { generation: 1, keepProviderOpen: true })).toBe(instance);
    expect(registry.has("demo")).toBe(false);

    registry.register(instance);
    const second = registry.providerStatus().find((entry) => entry.state === "active")!;
    expect(second.generation).toBe(2);
    await registry.close(new Set(["demo"]));
  });

  it("lets a committed view drain the retained generation", async () => {
    const registry = new ActionRegistry();
    registry.register(provider("first"));
    const pinned = await registry.acquireCapabilityView(["demo.echo"], context);
    registry.unregister("demo", { keepProviderOpen: true });
    await expect(invoke(registry)).rejects.toThrow();
    expect(await invoke(registry, { ...context, capabilityView: pinned.view! })).toBe("first");
    await pinned.release();
    await registry.close();
  });

  it("validates withdrawal payloads", () => {
    expect(readFabricProviderWithdrawalV1({ name: "demo" })).toEqual({ name: "demo" });
    expect(readFabricProviderWithdrawalV1({ name: "demo", generation: 3 })).toEqual({ name: "demo", generation: 3 });
    expect(readFabricProviderWithdrawalV1({ name: "demo", generation: "id" })).toEqual({ name: "demo", generation: "id" });
    expect(readFabricProviderWithdrawalV1({ name: "" })).toBeUndefined();
    expect(readFabricProviderWithdrawalV1({ name: "demo", generation: 0 })).toBeUndefined();
    expect(readFabricProviderWithdrawalV1({ name: "demo", generation: 1.5 })).toBeUndefined();
    expect(readFabricProviderWithdrawalV1({ generation: 1 })).toBeUndefined();
    expect(readFabricProviderWithdrawalV1(null)).toBeUndefined();
  });
});

describe("FabricState provider withdrawal", () => {
  const project = (): string => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-withdraw-"));
    fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ prewalk: { alwaysRearm: false }, mesh: { enabled: false } }));
    return cwd;
  };
  const contextAt = (cwd: string): ExtensionContext => ({
    cwd,
    isProjectTrusted: () => true,
    sessionManager: { getSessionId: () => "session-1" },
    ui: { setStatus: vi.fn() },
  } as unknown as ExtensionContext);

  const harness = () => {
    const instances: FakeRuntime[] = [];
    class FakeRuntime {
      initialized = false;
      widgetDismissedAt = 0;
      readonly providers = new Map<string, FabricProvider>();
      initialize = vi.fn(async () => { this.initialized = true; });
      shutdown = vi.fn(async () => { this.initialized = false; });
      registerExternal = vi.fn((value: FabricProvider) => { this.providers.set(value.name, value); });
      withdrawExternal = vi.fn((name: string, generation?: number | string) =>
        this.providers.has(name) && (generation === undefined || generation === 1) && this.providers.delete(name));
      constructor() { instances.push(this); }
    }
    return { instances, loader: vi.fn(async () => ({ FabricRuntimeState: FakeRuntime })) as never };
  };

  it("forgets a withdrawn registration so reactivation does not remount it", async () => {
    const cwd = project();
    const { instances, loader } = harness();
    const state = new FabricState({} as ExtensionAPI, new CapturedToolCatalog(), { runtimeLoader: loader });
    try {
      const instance = provider("first");
      state.registerExternal(instance);
      expect(state.withdrawExternal("missing")).toBe(false);
      // A generation pin needs a mounted binding.
      expect(state.withdrawExternal("demo", 1)).toBe(false);
      await state.bootstrap(contextAt(cwd));
      await state.ensure(contextAt(cwd));
      expect(instances[0]!.providers.has("demo")).toBe(true);
      expect(state.withdrawExternal("demo", 7)).toBe(false);
      expect(instances[0]!.providers.has("demo")).toBe(true);
      expect(state.withdrawExternal("demo", 1)).toBe(true);
      expect(instances[0]!.providers.has("demo")).toBe(false);
      expect(state.withdrawExternal("demo")).toBe(false);

      // Re-registration after withdrawal is accepted without overwrite.
      state.registerExternal(instance);
      expect(instances[0]!.providers.has("demo")).toBe(true);
    } finally {
      await state.shutdown();
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("ignores unknown withdrawals at the event boundary and rejects malformed ones", async () => {
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
      getActiveTools: vi.fn(() => []),
      getAllTools: vi.fn(() => []),
      on: vi.fn((event: string, handler: (...args: never[]) => unknown) => {
        handlers.set(event, [...(handlers.get(event) ?? []), handler]);
      }),
      registerCommand: vi.fn(),
      registerMessageRenderer: vi.fn(),
      registerTool: vi.fn(),
      setActiveTools: vi.fn(),
    } as unknown as ExtensionAPI;
    await piFabric(pi);
    const withdraw = listeners.get(FABRIC_PROVIDER_WITHDRAW_EVENT)!;
    expect(withdraw).toBeTypeOf("function");
    expect(() => withdraw({ name: "unknown" })).not.toThrow();
    expect(() => withdraw({ generation: 1 })).toThrow("Invalid Pi Fabric provider withdrawal");
    for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown();
    expect(listeners.has(FABRIC_PROVIDER_WITHDRAW_EVENT)).toBe(false);
  });
});
