import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { FABRIC_PROVIDER_REGISTER_EVENT } from "../src/protocol.js";
import {
  registerRootInboxReceipt, rootInboxReceiptIssuer, verifyRootInboxReceipt,
} from "../src/topology/root-inbox-shared.js";

type ExtensionHandler = (...args: never[]) => unknown;
type LifecycleHandler = (event?: unknown, context?: ExtensionContext) => unknown;

afterEach(() => vi.restoreAllMocks());

const lifecycleHarness = async () => {
  const handlers = new Map<string, LifecycleHandler[]>();
  const listeners = new Set<unknown>();
  const pi = {
    events: {
      emit: vi.fn(),
      on: vi.fn((channel: string, handler: unknown) => {
        if (channel === FABRIC_PROVIDER_REGISTER_EVENT) listeners.add(handler);
        return () => listeners.delete(handler);
      }),
    },
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
    on: (name: string, handler: LifecycleHandler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
    },
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    setActiveTools: vi.fn(),
  } as unknown as ExtensionAPI;
  await piFabric(pi);
  return {
    listeners,
    emit: async (name: string, event?: unknown, context?: ExtensionContext) => {
      for (const handler of handlers.get(name) ?? []) await handler(event, context);
    },
  };
};

// Stop startup at its independent bootstrap boundary, after the real lifecycle custody bind.
const bindSession = async (harness: Awaited<ReturnType<typeof lifecycleHarness>>, context: ExtensionContext, reason = "startup") => {
  const boundary = new Error("reached bootstrap");
  const bootstrap = vi.spyOn(FabricState.prototype, "bootstrap").mockRejectedValueOnce(boundary);
  try { await expect(harness.emit("session_start", { reason }, context)).rejects.toBe(boundary); }
  finally { bootstrap.mockRestore(); }
};

describe("Pi Fabric extension shutdown", () => {
  // Keep reasons: the contextless teardown regression and the private receipt trust boundary.
  it("retires delayed work and tears down without an event or context, even with running agents", async () => {
    const harness = await lifecycleHarness();
    vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
    vi.spyOn(FabricState.prototype, "agents", "get").mockReturnValue({ runningCount: () => 2 } as FabricState["agents"]);
    const abort = vi.spyOn(AbortController.prototype, "abort");
    const retire = vi.spyOn(FabricState.prototype, "setRootInboxWake");
    const shutdown = vi.spyOn(FabricState.prototype, "shutdown");
    expect(harness.listeners.size).toBeGreaterThan(0);
    await expect(harness.emit("session_shutdown")).resolves.toBeUndefined();
    expect(retire).toHaveBeenCalledWith(undefined);
    expect(abort).toHaveBeenCalledTimes(1);
    expect(shutdown).toHaveBeenCalledWith("exit", undefined);
    expect(harness.listeners.size).toBe(0);
  });

  it("retires timers before reading shutdown context and releases non-reload receipt custody", async () => {
    const harness = await lifecycleHarness();
    const manager = {};
    const issuer = rootInboxReceiptIssuer(manager, "session");
    const carrier = { content: "shown work", details: { ids: ["work"], receipts: ["receipt"] } };
    registerRootInboxReceipt(issuer, carrier.details, carrier.content);
    const abort = vi.spyOn(AbortController.prototype, "abort");
    const shutdown = vi.spyOn(FabricState.prototype, "shutdown");
    const context = {
      get sessionManager() {
        expect(abort).toHaveBeenCalledTimes(1);
        return manager;
      },
      hasUI: false,
    } as ExtensionContext;
    await harness.emit("session_shutdown", { reason: "switch", targetSessionFile: "next.jsonl" }, context);
    expect(shutdown).toHaveBeenCalledWith("switch", "next.jsonl");
    expect(harness.listeners.size).toBe(0);
    const afterShutdown = rootInboxReceiptIssuer(manager, "session", true);
    expect(afterShutdown).not.toBe(issuer);
    expect(verifyRootInboxReceipt(afterShutdown, carrier.details, carrier, new Set())).toBeUndefined();
  });

  it("allows startup without an optional session manager to reach bootstrap", async () => {
    const harness = await lifecycleHarness();
    try { await bindSession(harness, { hasUI: false } as ExtensionContext); }
    finally { await harness.emit("session_shutdown", { reason: "reload" }); }
  });

  it("retains genuine same-manager/session reload receipts, consumption and queue leases", async () => {
    let sessionId = "session";
    const manager = { getSessionId: () => sessionId };
    const context = { sessionManager: manager, hasUI: false } as ExtensionContext;
    const first = await lifecycleHarness();
    await bindSession(first, context);
    const issuer = rootInboxReceiptIssuer(manager, sessionId, true);
    const carrier = { content: "shown work", details: { ids: ["work"], receipts: ["receipt"] } };
    registerRootInboxReceipt(issuer, carrier.details, carrier.content);
    const snapshot = verifyRootInboxReceipt(issuer, carrier.details, carrier, new Set())!;
    issuer.consumed.add(snapshot);
    issuer.queued.set("work", 7);
    issuer.turn = 7;
    await first.emit("session_shutdown", { reason: "reload" }, context);
    expect(first.listeners.size).toBe(0);
    const reloaded = await lifecycleHarness();
    try {
      await bindSession(reloaded, context, "reload");
      const retained = rootInboxReceiptIssuer(manager, sessionId, true);
      expect(retained).toBe(issuer);
      expect(verifyRootInboxReceipt(retained, carrier.details, JSON.parse(JSON.stringify(carrier)), new Set())).toBe(snapshot);
      expect(retained.consumed.has(snapshot)).toBe(true);
      expect(retained.queued.get("work")).toBe(7);
      expect(retained.turn).toBe(7);
      // A public session id change on the same manager is not a same-session reload.
      sessionId = "replacement";
      await bindSession(reloaded, context, "reload");
      const replacement = rootInboxReceiptIssuer(manager, sessionId, true);
      expect(replacement).not.toBe(issuer);
      expect(verifyRootInboxReceipt(replacement, carrier.details, carrier, new Set())).toBeUndefined();
    } finally { await reloaded.emit("session_shutdown", { reason: "exit" }, context); }
  });

  it("unsubscribes shared provider listeners across reloads", async () => {
    const providerListeners = new Set<(value: unknown) => void>();
    const events = {
      emit: vi.fn(),
      on: vi.fn((channel: string, handler: (value: unknown) => void) => {
        if (channel === FABRIC_PROVIDER_REGISTER_EVENT) providerListeners.add(handler);
        return () => providerListeners.delete(handler);
      }),
    };

    for (let reload = 0; reload < 3; reload++) {
      const handlers = new Map<string, ExtensionHandler[]>();
      const pi = {
        events,
        getActiveTools: vi.fn(() => []),
        getAllTools: vi.fn(() => []),
        on: vi.fn((event: string, handler: ExtensionHandler) => {
          const registered = handlers.get(event) ?? [];
          registered.push(handler);
          handlers.set(event, registered);
        }),
        registerCommand: vi.fn(),
        registerMessageRenderer: vi.fn(),
        registerTool: vi.fn(),
        setActiveTools: vi.fn(),
      } as unknown as ExtensionAPI;

      await piFabric(pi);
      expect(providerListeners.size).toBe(1);

      const shutdownHandlers = handlers.get("session_shutdown") ?? [];
      expect(shutdownHandlers.length).toBeGreaterThan(0);
      for (const shutdown of shutdownHandlers) await shutdown();
      expect(providerListeners.size).toBe(0);
    }
  });
});
