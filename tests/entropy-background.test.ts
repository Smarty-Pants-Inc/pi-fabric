import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

// Gate each background compile at its evidence read: `resolve()` releases it.
const scanControl = vi.hoisted(() => ({
  calls: 0,
  files: [] as string[][],
  resolvers: [] as Array<() => void>,
  signals: [] as Array<AbortSignal | undefined>,
}));

vi.mock("../src/entropy/sessions.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/entropy/sessions.js")>();
  return {
    ...actual,
    machineSessionFilesAsync: () => {
      throw new Error("the background compile must not scan the machine's sessions (smarty-dev#2010)");
    },
    sessionWindowEvidenceAsync: (
      files: readonly string[],
      options: Parameters<typeof actual.sessionWindowEvidenceAsync>[1] = {},
    ) => {
      scanControl.calls += 1;
      scanControl.files.push([...files]);
      scanControl.signals.push(options.signal);
      return new Promise<void>((resolve) => scanControl.resolvers.push(resolve))
        .then(() => actual.sessionWindowEvidenceAsync(files, options));
    },
  };
});

vi.mock("../src/fabric-runtime-state.js", () => ({
  FabricRuntimeState: class {
    initialized = true;
    widgetDismissedAt = 0;
    registry = { list: vi.fn(async () => []) };
    repairs = {
      repairs: [],
      status: () => ({
        enabled: true,
        catalogDigest: "test",
        repairCount: 0,
        applyHits: 0,
        invocationErrors: 0,
        effectDropped: 0,
        fingerprints: [],
        repairs: [],
      }),
    };
    async initialize(): Promise<void> {}
    async shutdown(): Promise<void> {}
    async publishHostLifecycle(): Promise<void> {}
    async settleComponents(): Promise<void> {}
    noteMainActivity(): void {}
    resetSpeculation(): void {}
    dispatchHostEvent(): number { return 0; }
    registerExternal(): void {}
    registerExternalComponent(): void {}
    mcpSlice(): never[] { return []; }
  },
}));

import piFabric from "../src/index.js";
import { BackgroundEntropyCompiler } from "../src/entropy/compiler.js";
import * as poolStore from "../src/entropy/pool-store.js";
import { SessionObservationCache } from "../src/entropy/pool.js";

type ExtensionHandler = (event: unknown, context: ExtensionContext) => unknown;

const tempRoots: string[] = [];
afterEach(() => {
  scanControl.calls = 0;
  scanControl.files.length = 0;
  scanControl.resolvers.length = 0;
  scanControl.signals.length = 0;
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  for (const root of tempRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const createHarness = () => {
  const handlers = new Map<string, ExtensionHandler[]>();
  let command: ((args: string, context: ExtensionContext) => Promise<void>) | undefined;
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []),
    getAllTools: vi.fn(() => []),
    on: vi.fn((event: string, handler: ExtensionHandler) => {
      const values = handlers.get(event) ?? [];
      values.push(handler);
      handlers.set(event, values);
    }),
    registerCommand: vi.fn((name: string, definition: { handler: typeof command }) => {
      if (name === "fabric") command = definition.handler;
    }),
    registerMessageRenderer: vi.fn(),
    registerTool: vi.fn(),
    setActiveTools: vi.fn(),
  } as unknown as ExtensionAPI;
  return { pi, handlers, command: () => command! };
};

const emit = async (
  handlers: Map<string, ExtensionHandler[]>,
  name: string,
  event: unknown,
  context: ExtensionContext,
): Promise<void> => {
  for (const handler of handlers.get(name) ?? []) await handler(event, context);
};

describe("entropy background scheduler", () => {
  it("reads only this session's file, compiles each turn and skips unchanged pool writes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-entropy-background-"));
    tempRoots.push(root);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, "");
    const compile = vi.spyOn(BackgroundEntropyCompiler.prototype, "compile");
    const save = vi.spyOn(poolStore, "saveObservationPoolAsync");
    const harness = createHarness();
    await piFabric(harness.pi);
    const context = {
      mode: "code", cwd: root, hasUI: false, isProjectTrusted: () => true,
      ui: { setStatus: vi.fn(), notify: vi.fn() },
      sessionManager: { getBranch: () => [], getSessionId: () => "cached", getSessionFile: () => file },
    } as unknown as ExtensionContext;
    await harness.command()("repairs", context);
    const trigger = async () => {
      await emit(harness.handlers, "tool_execution_end", { toolName: "fabric_exec", isError: false }, context);
      await emit(harness.handlers, "turn_end", {}, context);
    };
    await trigger();
    await vi.waitFor(() => expect(scanControl.calls).toBe(1));
    scanControl.resolvers.shift()!();
    await vi.waitFor(() => expect(compile).toHaveBeenCalledTimes(1));
    await trigger();
    await vi.waitFor(() => expect(scanControl.calls).toBe(2));
    scanControl.resolvers.shift()!();
    await vi.waitFor(() => expect(compile).toHaveBeenCalledTimes(2));
    await emit(harness.handlers, "session_shutdown", {}, context);
    expect(scanControl.files).toEqual([[file], [file]]);
    expect(save).toHaveBeenCalledTimes(1);
    expect(compile.mock.calls[1]![0].windows).toEqual([{ file, traces: [] }]);
  });

  it("aborts an in-flight compile at shutdown instead of waiting for it (smarty-dev#2010)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-entropy-background-"));
    tempRoots.push(root);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const compile = vi.spyOn(BackgroundEntropyCompiler.prototype, "compile");
    const save = vi.spyOn(poolStore, "saveObservationPoolAsync");
    const harness = createHarness();
    await piFabric(harness.pi);
    const context = {
      mode: "code", cwd: root, hasUI: false, isProjectTrusted: () => true,
      ui: { setStatus: vi.fn(), notify: vi.fn() },
      sessionManager: { getBranch: () => [], getSessionId: () => "abort-session" },
    } as unknown as ExtensionContext;
    await harness.command()("repairs", context);
    await emit(harness.handlers, "tool_execution_end", { toolName: "fabric_exec", isError: false }, context);
    await emit(harness.handlers, "turn_end", {}, context);
    await vi.waitFor(() => expect(scanControl.calls).toBe(1), { timeout: 1_000 });
    // A second fabric_exec turn is pending when the session ends: it is dropped, not flushed.
    await emit(harness.handlers, "tool_execution_end", { toolName: "fabric_exec", isError: false }, context);
    // The scan never settles on its own: shutdown must not wait for it.
    await emit(harness.handlers, "session_shutdown", {}, context);
    expect(scanControl.signals[0]?.aborted).toBe(true);
    // A late scan result after shutdown writes nothing and compiles nothing.
    scanControl.resolvers.shift()!();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(scanControl.calls).toBe(1);
    expect(compile).not.toHaveBeenCalled();
    expect(save).not.toHaveBeenCalled();
  });

  it("cancels an observation merge in flight at shutdown and never writes after it (smarty-dev#2010)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-entropy-background-"));
    tempRoots.push(root);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, "");
    const save = vi.spyOn(poolStore, "saveObservationPoolAsync");
    const compile = vi.spyOn(BackgroundEntropyCompiler.prototype, "compile");
    const original = SessionObservationCache.prototype.merge;
    const mergeSignals: Array<AbortSignal | undefined> = [];
    let releaseMerge!: () => void;
    const mergeGate = new Promise<void>((resolve) => { releaseMerge = resolve; });
    vi.spyOn(SessionObservationCache.prototype, "merge").mockImplementation(async function (
      this: SessionObservationCache, pool, windows, signal,
    ) {
      mergeSignals.push(signal);
      await mergeGate;
      // Resolve with a result, as a merge that ignored cancellation would.
      return original.call(this, pool, windows);
    });
    const harness = createHarness();
    await piFabric(harness.pi);
    const context = {
      mode: "code", cwd: root, hasUI: false, isProjectTrusted: () => true,
      ui: { setStatus: vi.fn(), notify: vi.fn() },
      sessionManager: { getBranch: () => [], getSessionId: () => "merge-session", getSessionFile: () => file },
    } as unknown as ExtensionContext;
    await harness.command()("repairs", context);
    await emit(harness.handlers, "tool_execution_end", { toolName: "fabric_exec", isError: false }, context);
    await emit(harness.handlers, "turn_end", {}, context);
    await vi.waitFor(() => expect(scanControl.calls).toBe(1), { timeout: 1_000 });
    scanControl.resolvers.shift()!();
    await vi.waitFor(() => expect(mergeSignals).toHaveLength(1), { timeout: 1_000 });
    await emit(harness.handlers, "session_shutdown", {}, context);
    expect(mergeSignals[0]?.aborted).toBe(true);
    releaseMerge();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(save).not.toHaveBeenCalled();
    expect(compile).not.toHaveBeenCalled();
  });

  it("returns turn hooks immediately, coalesces pending turns, and ends them on shutdown", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-entropy-background-"));
    tempRoots.push(root);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    const harness = createHarness();
    await piFabric(harness.pi);
    const context = {
      mode: "code",
      cwd: root,
      hasUI: false,
      isProjectTrusted: () => true,
      ui: { setStatus: vi.fn(), notify: vi.fn() },
      sessionManager: { getBranch: () => [], getSessionId: () => "background-session" },
    } as unknown as ExtensionContext;
    await harness.command()("repairs", context);

    const trigger = async (): Promise<void> => {
      await emit(
        harness.handlers,
        "tool_execution_end",
        { toolName: "fabric_exec", isError: false },
        context,
      );
      await emit(harness.handlers, "turn_end", {}, context);
    };

    await trigger();
    expect(scanControl.calls).toBe(0);
    await vi.waitFor(() => expect(scanControl.calls).toBe(1), { timeout: 1_000 });
    await trigger();
    await trigger();
    expect(scanControl.calls).toBe(1);

    scanControl.resolvers.shift()!();
    await vi.waitFor(() => expect(scanControl.calls).toBe(2), { timeout: 1_000 });
    expect(scanControl.resolvers).toHaveLength(1);
    scanControl.resolvers.shift()!();
    await emit(harness.handlers, "session_shutdown", {}, context);
    expect(scanControl.calls).toBe(2);
  });
});
