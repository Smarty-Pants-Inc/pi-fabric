import { AsyncLocalStorage } from "node:async_hooks";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const replacementReasons = ["new", "resume", "fork", "switch"] as const;

type Handler = (event: any, context: ExtensionContext) => unknown;

const sessionContext = (sessionId: string) => ({
  mode: "rpc",
  cwd: process.cwd(),
  hasUI: false,
  isIdle: () => true,
  hasPendingMessages: () => false,
  isProjectTrusted: () => false,
  sessionManager: {
    getSessionId: () => sessionId,
    getEntries: () => [],
    getBranch: () => [],
  },
  ui: { setStatus: vi.fn(), notify: vi.fn() },
}) as unknown as ExtensionContext;

const harness = async (sessionReadyEvent: boolean) => {
  const handlers = new Map<string, Handler[]>();
  const inputScope = new AsyncLocalStorage<"replacement-start" | "session-ready">();
  const accepted: unknown[] = [];
  const sendMessage = vi.fn(async (message: unknown) => {
    if (inputScope.getStore() === "replacement-start") {
      throw Object.assign(new Error("INPUT_ADMISSION_FENCED"), { code: "INPUT_ADMISSION_FENCED" });
    }
    accepted.push(message);
  });
  const api = {
    hostCapabilities: { sessionReadyEvent, triggeredMessageQueuesBehindPreflight: true, promptPendingVisible: true },
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), setActiveTools: vi.fn(),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    sendMessage,
    on: (name: string, handler: Handler) => handlers.set(name, [...(handlers.get(name) ?? []), handler]),
  } as unknown as ExtensionAPI;
  await piFabric(api);
  const emit = async (name: string, event: unknown, context: ExtensionContext) => {
    for (const handler of handlers.get(name) ?? []) await handler(event, context);
  };
  const replacementStart = (reason: typeof replacementReasons[number], context: ExtensionContext) =>
    inputScope.run("replacement-start", () => emit("session_start", { reason }, context));
  const sessionReady = (event: unknown, context: ExtensionContext) =>
    inputScope.run("session-ready", () => emit("session_ready", event, context));
  return { accepted, emit, replacementStart, sessionReady, sendMessage };
};

afterEach(() => vi.restoreAllMocks());

const stubConfig = () => vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);

describe("replacement session activation admission", () => {
  it.each(replacementReasons)("starts input-producing work from session_ready after %s", async reason => {
    const h = await harness(true);
    stubConfig();
    const bootstrap = vi.spyOn(FabricState.prototype, "bootstrap").mockResolvedValue(undefined);
    const eager = vi.spyOn(FabricState.prototype, "shouldEagerlyActivate").mockReturnValue(true);
    const ensure = vi.spyOn(FabricState.prototype, "ensure").mockImplementation(async () => {
      await Promise.resolve(); // Async descendants retain the lifecycle scope.
      await h.sendMessage({ customType: "fabric-background-canary" });
      return { current: () => true };
    });
    const rootWake = vi.spyOn(FabricState.prototype, "setRootInboxWake");
    const recordsWake = vi.spyOn(FabricState.prototype, "setRecordsWake");
    const startContext = sessionContext(`replacement-${reason}`);
    const readyContext = sessionContext(`replacement-${reason}`);

    await expect(h.replacementStart(reason, startContext)).resolves.toBeUndefined();
    expect(bootstrap).not.toHaveBeenCalled();
    expect(ensure).not.toHaveBeenCalled();
    // The only pre-ready root-wake call retires the old session; it must not arm a callback.
    expect(rootWake.mock.calls).toEqual([[undefined]]);
    expect(recordsWake.mock.calls).toEqual([[undefined]]);
    expect(h.accepted).toEqual([]);

    await h.sessionReady({ reason, sessionGeneration: "ready-generation" }, readyContext);
    expect(bootstrap).toHaveBeenCalledTimes(1);
    expect(ensure).toHaveBeenCalledTimes(1);
    expect(ensure).toHaveBeenCalledWith(readyContext);
    expect(eager).toHaveBeenCalledTimes(1);
    expect(eager).toHaveBeenCalledWith(readyContext);
    expect(rootWake.mock.calls.at(-1)?.[0]).toEqual(expect.any(Function));
    expect(recordsWake.mock.calls.at(-1)?.[0]).toEqual(expect.any(Function));
    expect(h.accepted).toEqual([{ customType: "fabric-background-canary" }]);
  });

  it.each(["startup", "reload"] as const)("keeps %s activation in session_start", async reason => {
    const h = await harness(true);
    stubConfig();
    const bootstrap = vi.spyOn(FabricState.prototype, "bootstrap").mockResolvedValue(undefined);
    vi.spyOn(FabricState.prototype, "shouldEagerlyActivate").mockReturnValue(true);
    const ensure = vi.spyOn(FabricState.prototype, "ensure").mockResolvedValue({ current: () => true });
    const context = sessionContext(`${reason}-session`);

    await h.emit("session_start", { reason }, context);
    expect(bootstrap).toHaveBeenCalledTimes(1);
    expect(bootstrap).toHaveBeenCalledWith(context);
    expect(ensure).toHaveBeenCalledTimes(1);
    expect(ensure).toHaveBeenCalledWith(context);
  });

  it("keeps the documented session_start fallback on hosts without session_ready", async () => {
    const h = await harness(false);
    stubConfig();
    const bootstrap = vi.spyOn(FabricState.prototype, "bootstrap").mockResolvedValue(undefined);
    vi.spyOn(FabricState.prototype, "shouldEagerlyActivate").mockReturnValue(true);
    const ensure = vi.spyOn(FabricState.prototype, "ensure").mockResolvedValue({ current: () => true });
    const context = sessionContext("legacy-replacement");

    await h.replacementStart("new", context);
    expect(bootstrap).toHaveBeenCalledTimes(1);
    expect(bootstrap).toHaveBeenCalledWith(context);
    expect(ensure).toHaveBeenCalledTimes(1);
    expect(ensure).toHaveBeenCalledWith(context);
  });

  it("invalidates a pending ready continuation on shutdown", async () => {
    const h = await harness(true);
    stubConfig();
    const bootstrap = vi.spyOn(FabricState.prototype, "bootstrap").mockResolvedValue(undefined);
    const ensure = vi.spyOn(FabricState.prototype, "ensure").mockResolvedValue({ current: () => true });
    vi.spyOn(FabricState.prototype, "shutdown").mockResolvedValue(undefined);
    const context = sessionContext("shutdown-before-ready");

    await h.replacementStart("new", context);
    await h.emit("session_shutdown", { reason: "exit" }, context);
    await h.sessionReady({ reason: "new", sessionGeneration: "ready-generation" }, context);
    expect(bootstrap).not.toHaveBeenCalled();
    expect(ensure).not.toHaveBeenCalled();
  });
});
