import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

type Handler = (event: unknown, context: ExtensionContext) => unknown;
type Command = Parameters<ExtensionAPI["registerCommand"]>[1];
type Shortcut = Parameters<NonNullable<ExtensionAPI["registerShortcut"]>>[1];
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); }
  finally {
    vi.doUnmock("../src/ui/controller.js");
    vi.doUnmock("../src/fabric-runtime-state.js");
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
    vi.resetModules();
  }
});

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

// Keep reason: lifecycle ownership and failed optional imports are failure paths
// that a normal UI flow cannot cheaply arrange. Only runtime/module/host I/O is
// replaced: bootstrap, FabricState leases, activation and command handlers are real.
// This is NOT real-widget, collector, rendering, native-Pi or latency proof.
async function fixture(options: { mode?: "tui" | "rpc"; enabled?: boolean } = {}) {
  vi.resetModules();
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ui-firstuse-"));
  fs.mkdirSync(path.join(cwd, "agent"));
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
    prewalk: { alwaysRearm: false }, mesh: { enabled: false }, components: [],
    jev: { enabled: false }, ui: { enabled: options.enabled ?? true, haltOnEscape: false },
  }));
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_DEPTH", "PI_FABRIC_CAPABILITY_REQUIREMENTS", "PI_FABRIC_CAPABILITY_DIGEST", "PI_FABRIC_FIXTURE", "PI_FABRIC_SCOPE", "PI_FABRIC_SCOPE_FILE", "PI_FABRIC_RESIDENT_PROBE_NONCE"]) vi.stubEnv(key, undefined);
  const loads = vi.fn();
  const constructed = vi.fn();
  let gate: ReturnType<typeof deferred> | undefined;
  let fail = false;
  const instances: BoundaryController[] = [];
  class BoundaryController {
    ownsInput = false;
    start = vi.fn((_context: ExtensionContext) => {});
    stop = vi.fn();
    setHostStreaming = vi.fn((_streaming: boolean) => {});
    openConversation = vi.fn(async (_context: ExtensionContext, _query?: string) => {});
    openTasks = vi.fn(async (_context: ExtensionContext, _query?: string) => {});
    openDashboard = vi.fn(async (_context: ExtensionContext) => {});
    snapshot = vi.fn(() => ({
      main: { id: "current-main", name: "Current Main", status: "idle" },
      peers: [], actors: [], agents: [], participants: [],
    }));
    constructor(..._args: unknown[]) { constructed(); instances.push(this); }
  }
  const controllerModule = async () => {
    loads();
    if (gate) await gate.promise;
    if (fail) throw new Error("controller chunk unavailable");
    return { FabricUiController: BoundaryController };
  };
  vi.doMock("../src/ui/controller.js", controllerModule);
  // No workers, mesh, shared database, watchers or actual Pi Main are started.
  const runtimes: BoundaryRuntime[] = [];
  class BoundaryRuntime {
    initialized = false;
    registry = { providers: () => [] };
    execution = { authorizer: undefined };
    agents = { list: () => [], runningCount: () => 0 };
    actors = { list: () => [], inFlightCount: () => 0 };
    initialize = vi.fn(async () => { this.initialized = true; });
    shutdown = vi.fn(async () => { this.initialized = false; });
    observationHealth = () => [];
    publishHostLifecycle = async () => {};
    dispatchHostEvent = () => 0;
    resetSpeculation = () => {};
    constructor(..._args: unknown[]) { runtimes.push(this); }
  }
  vi.doMock("../src/fabric-runtime-state.js", () => ({ FabricRuntimeState: BoundaryRuntime }));
  const handlers = new Map<string, Handler[]>();
  const commands = new Map<string, Command>();
  const shortcuts = new Map<string, Shortcut>();
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on: (name: string, handler: Handler) => handlers.set(name, [...handlers.get(name) ?? [], handler]),
    registerCommand: (name: string, command: Command) => commands.set(name, command),
    registerShortcut: (key: string, shortcut: Shortcut) => shortcuts.set(key, shortcut),
    registerTool: vi.fn(), registerMessageRenderer: vi.fn(),
    getActiveTools: () => ["fabric_exec"], getAllTools: () => [],
    setActiveTools: vi.fn(), sendMessage: vi.fn(), getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const contextAt = (id: string): ExtensionCommandContext => ({
    cwd, hasUI: (options.mode ?? "tui") === "tui", mode: options.mode ?? "tui",
    isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => id, getBranch: () => [], getEntries: () => [],
      getLeafEntry: () => undefined, getEntry: () => undefined, getSessionFile: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn(), setWidget: vi.fn() },
  } as unknown as ExtensionCommandContext);
  const context = contextAt("first-session");
  const emit = async (name: string, ctx: ExtensionContext = context, event: unknown = {}) => {
    const results: unknown[] = [];
    for (const handler of handlers.get(name) ?? []) results.push(await handler(event, ctx));
    return results;
  };
  // Register cleanup before importing: even a cold-load assertion failure cleans up.
  cleanups.push(async () => {
    gate?.resolve();
    try { await emit("session_shutdown"); }
    finally { fs.rmSync(cwd, { recursive: true, force: true }); }
  });
  const { default: register } = await import("../src/extension-bootstrap.js");
  const coldLoads = loads.mock.calls.length;
  await register(pi);
  const uiShortcuts = [...shortcuts.values()].filter(shortcut =>
    shortcut.description === "Open Fabric conversation or return to Main" ||
    shortcut.description === "Inspect Fabric background shell tasks");
  expect(uiShortcuts).toHaveLength(2);
  return {
    coldLoads, loads, constructed, instances, runtimes, context, contextAt, emit, commands, uiShortcuts,
    block() { gate = deferred(); return gate; },
    failImport() { fail = true; },
    repairImport() { fail = false; vi.doMock("../src/ui/controller.js", controllerModule); },
    command: (text: string, ctx = context) => commands.get("fabric")!.handler(text, ctx),
  };
}

const expectUnloaded = (h: Awaited<ReturnType<typeof fixture>>) => {
  expect(h.coldLoads, "controller evaluated during cold entry import").toBe(0);
  expect(h.loads, "controller evaluated before actual UI activation").not.toHaveBeenCalled();
  expect(h.constructed, "controller constructed before actual UI activation").not.toHaveBeenCalled();
};

describe("registered Fabric UI controller first use", () => {
  it("does not evaluate or construct at cold import, registration, discovery or nonactivated idle", async () => {
    const h = await fixture();
    expectUnloaded(h);
    expect(h.commands.has("fabric")).toBe(true);
    expect(h.commands.get("fabric")!.getArgumentCompletions?.("cha")).toEqual([{ value: "chat", label: "chat" }]);
    await h.emit("resources_discover");
    await h.emit("session_start");
    await new Promise<void>(resolve => setImmediate(resolve));
    expectUnloaded(h);
    expect(h.runtimes).toHaveLength(0);
  });

  it.each([{ mode: "rpc" as const }, { enabled: false }])("keeps non-UI status unloaded (%j)", async options => {
    const h = await fixture(options);
    expectUnloaded(h);
    await h.emit("resources_discover");
    await h.emit("session_start");
    await h.command("status");
    expect(h.context.ui.notify).toHaveBeenCalledWith(expect.stringContaining("cwd:"), "info");
    expectUnloaded(h);
    expect(h.runtimes).toHaveLength(1);
    if (options.mode === "rpc") {
      for (const shortcut of h.uiShortcuts) await shortcut.handler(h.context);
      expectUnloaded(h);
    }
  });

  it("awaits first activation once and resolves chat, tasks, shortcuts and synchronous completions to that controller", async () => {
    const h = await fixture();
    expectUnloaded(h);
    await h.emit("session_start");
    const gate = h.block();
    const chat = h.command("chat current-main");
    const tasks = h.command("tasks job-1");
    await vi.waitFor(() => expect(h.loads).toHaveBeenCalledOnce());
    expect(h.constructed).not.toHaveBeenCalled();
    expect(h.commands.get("fabric")!.getArgumentCompletions?.("chat Current")).toBeNull();
    gate.resolve();
    await Promise.all([chat, tasks]);
    expect(h.instances).toHaveLength(1);
    const controller = h.instances[0]!;
    expect(controller.start).toHaveBeenCalledExactlyOnceWith(h.context);
    expect(controller.openConversation).toHaveBeenCalledWith(h.context, "current-main");
    expect(controller.openTasks).toHaveBeenCalledWith(h.context, "job-1");
    for (const shortcut of h.uiShortcuts) await shortcut.handler(h.context);
    expect(controller.openConversation).toHaveBeenCalledTimes(2);
    expect(controller.openTasks).toHaveBeenCalledTimes(2);
    expect(h.commands.get("fabric")!.getArgumentCompletions?.("chat Current")).toContainEqual(expect.objectContaining({ value: "current-main", label: "Current Main" }));
    await h.command("dashboard");
    expect(controller.openDashboard).toHaveBeenCalledWith(h.context);
    expect(h.loads).toHaveBeenCalledOnce();
    expect(controller.start).toHaveBeenCalledOnce();
    expect(h.context.ui.notify).not.toHaveBeenCalledWith(expect.anything(), "error");
  });

  it.each(["shutdown", "replacement"] as const)("does not mount a stale delayed import after %s; a fresh session can activate", async boundary => {
    const h = await fixture();
    expectUnloaded(h);
    await h.emit("session_start");
    const gate = h.block();
    // Attach rejection handling immediately: shutdown can retire ensure().
    const first = h.command("chat old-target").then(() => undefined, error => error);
    await vi.waitFor(() => expect(h.loads).toHaveBeenCalledOnce());
    const successor = h.contextAt("second-session");
    const retiring = boundary === "shutdown" ? h.emit("session_shutdown") : h.emit("session_start", successor);
    gate.resolve();
    await Promise.all([first, retiring]);
    for (const controller of h.instances) {
      expect(controller.start.mock.calls.some(([ctx]) => ctx === h.context)).toBe(false);
      expect(controller.openConversation.mock.calls.some(([ctx]) => ctx === h.context)).toBe(false);
    }
    if (boundary === "shutdown") await h.emit("session_start", successor);
    await h.command("chat new-target", successor);
    const starts = h.instances.flatMap(controller => controller.start.mock.calls);
    expect(starts).toEqual([[successor]]);
    expect(h.instances.some(controller => controller.openConversation.mock.calls.some(([ctx, query]) => ctx === successor && query === "new-target"))).toBe(true);
  });

  it("retries a failed controller import through the same registered command", async () => {
    const h = await fixture();
    expectUnloaded(h);
    await h.emit("session_start");
    h.failImport();
    const failure = await h.command("chat").then(() => undefined, error => error);
    expect(failure).toBeInstanceOf(Error);
    expect(h.loads).toHaveBeenCalledOnce();
    expect(h.constructed).not.toHaveBeenCalled();
    h.repairImport();
    await h.command("chat recovered");
    expect(h.loads).toHaveBeenCalledTimes(2);
    expect(h.instances).toHaveLength(1);
    expect(h.instances[0]!.start).toHaveBeenCalledExactlyOnceWith(h.context);
    expect(h.instances[0]!.openConversation).toHaveBeenCalledWith(h.context, "recovered");
  });

  it("attributes host streaming received during import to the controller started for that session", async () => {
    const h = await fixture();
    expectUnloaded(h);
    await h.emit("session_start");
    const gate = h.block();
    const activation = h.command("chat");
    await vi.waitFor(() => expect(h.loads).toHaveBeenCalledOnce());
    await h.emit("agent_start");
    expect(h.constructed).not.toHaveBeenCalled();
    gate.resolve();
    await activation;
    const controller = h.instances[0]!;
    expect(controller.start).toHaveBeenCalledExactlyOnceWith(h.context);
    expect(controller.setHostStreaming).toHaveBeenLastCalledWith(true);
    expect(controller.setHostStreaming.mock.invocationCallOrder.at(-1)).toBeGreaterThan(controller.start.mock.invocationCallOrder[0]!);
  });
});
