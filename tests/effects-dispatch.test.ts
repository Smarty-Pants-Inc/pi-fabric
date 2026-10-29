import { mkdtempSync, existsSync, writeFileSync, mkdirSync, readFileSync, statSync, chmodSync } from "node:fs";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createSyntheticSourceInfo, defineTool,
  type AgentToolResult, type ExtensionAPI, type ExtensionContext, type ExtensionToolContext, type ExtensionRunner, type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { runToolCall, type AgentTool } from "@earendil-works/pi-agent-core";
import { Type } from "typebox";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry, type FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { FabricProviderBindings } from "../src/core/provider-bindings.js";
import { ProviderOperations } from "../src/core/provider-operations.js";
import {
  bindCertifiedEffectRuntime, createCertifiedEffectRecordPort, createCertifiedEffectRuntime,
  type CertifiedDispatchPolicy, type CertifiedToolDispatcher, type CertifiedEffectRecordOptions,
  type CertifiedEffectRuntimeOptions,
} from "../src/effects/extension.js";
import type { AcceptedInput, EffectSlot, RecoveryOwner } from "../src/effects/contracts.js";
import type { EffectFailpointName, EffectFailpointReceipt } from "../src/effects/failpoints.js";
import { EffectStore } from "../src/effects/store.js";
import { canonicalEncode, manifestDigest, mintEffectKey, sha256Bytes } from "../src/effects/slots.js";
import * as fileEffects from "../src/effects/file.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { applyRunBashDefaults } from "../src/guards/actor-bash-timeout.js";
import { prepareShellEffect, shellEffectCommand } from "../src/effects/shell.js";
import * as worktree from "../src/agents/bash-worktree-add.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import type { FabricActionDescriptor, FabricProvider } from "../src/protocol.js";
import type { FabricSpeculationRuntime } from "../src/speculation/types.js";

// Keep reason: data-safety/trust boundary. These tests use the actual registry,
// tickets and providers; the destination is a recorded-call probe, not a claim
// of filesystem durability or crash recovery (covered by the adapter/flow lanes).
const context = (cwd = process.cwd()): FabricRegistryInvocationContext => ({
  cwd, signal: undefined, parentToolCallId: "parent", nestedToolCallId: "nested",
  extensionContext: { cwd } as ExtensionContext,
  update: vi.fn(), approve: vi.fn(async () => {}), audits: [], maxResultChars: 100_000,
});
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const result = (text = "certified"): AgentToolResult<unknown> => ({
  content: [{ type: "text", text }], details: { certified: true },
});
const refuse: CertifiedDispatchPolicy = {
  assertDispatch() { throw new Error("uncertified provider"); },
};
const pin = (provider: FabricProvider): CertifiedDispatchPolicy => ({
  assertDispatch(request) {
    if (request.provider !== provider) throw new Error("uncertified provider identity");
  },
});
function probeProvider(scoped = false) {
  const destination: string[] = [];
  const descriptor: FabricActionDescriptor = {
    name: "effect", description: "Dispatch probe", risk: "read",
    inputSchema: { type: "object", properties: { value: { type: "string" } }, required: ["value"], additionalProperties: false },
    effect: scoped ? { kind: "scoped", ordering: "ordered", resources: ["probe"] } : { kind: "none", ordering: "commutative" },
  };
  const dispose = vi.fn(async () => { destination.push("disposed"); });
  const provider: FabricProvider = {
    name: "probe", description: "Probe",
    list: vi.fn(async () => [descriptor]), describe: vi.fn(async () => descriptor),
    prepareArguments: vi.fn((_name, args) => ({ ...args, value: `${String(args.value)}:prepared` })),
    invoke: vi.fn(async (_name, args) => { destination.push(String(args.value)); return args.value; }),
    acquire: vi.fn(async (_name, args) => { destination.push(String(args.value)); return { value: args.value, dispose }; }),
  };
  return { provider, descriptor, destination, dispose };
}
const speculation = (tryServe: FabricSpeculationRuntime["tryServe"]): FabricSpeculationRuntime => ({
  tryServe, reset: vi.fn(), bumpEpoch: vi.fn(),
});

describe("certified registry final dispatch", () => {
  it("pins provider identity and refuses a same-ref replacement before prepare", async () => {
    const original = probeProvider();
    const replacement = probeProvider();
    const registry = new ActionRegistry();
    registry.register(original.provider);
    registry.setCertifiedDispatchPolicy(pin(original.provider));
    registry.register(replacement.provider, { overwrite: true });
    await expect(registry.invoke("probe.effect", { value: "input" }, context())).rejects.toThrow("identity");
    expect(replacement.provider.prepareArguments).not.toHaveBeenCalled();
    expect(replacement.destination).toEqual([]);
    await registry.close();
  });

  it.each([false, true])("checks the live policy after final observation (scoped=%s)", async scoped => {
    const f = probeProvider(scoped);
    const registry = new ActionRegistry();
    registry.register(f.provider);
    const observed: Array<Parameters<CertifiedDispatchPolicy["assertDispatch"]>[0]> = [];
    registry.setCertifiedDispatchPolicy({ assertDispatch(request) { observed.push(request); pin(f.provider).assertDispatch(request); } });
    const describe = f.provider.describe;
    let descriptions = 0;
    f.provider.describe = async (...args) => {
      if (++descriptions === 2) registry.setCertifiedDispatchPolicy(refuse);
      return describe(...args);
    };
    const operation = scoped
      ? registry.acquireScoped("probe.effect", { value: "input" }, context())
      : registry.invoke("probe.effect", { value: "input" }, context());
    await expect(operation).rejects.toThrow("uncertified provider");
    expect(observed[0]).toMatchObject({ ref: "probe.effect", mode: scoped ? "acquire" : "invoke" });
    expect(observed[0]?.provider).toBe(f.provider);
    expect(f.destination).toEqual([]);
    expect(f.dispose).not.toHaveBeenCalled();
    await registry.close();
  });

  it.each([false, true])("passes prepared arguments and actual provider to final checks (scoped=%s)", async scoped => {
    const f = probeProvider(scoped);
    const registry = new ActionRegistry();
    registry.register(f.provider);
    const checked: Array<{ value: unknown; provider: unknown; mode: string }> = [];
    registry.setCertifiedDispatchPolicy({ assertDispatch(request) {
      checked.push({ value: request.args.value, provider: request.provider, mode: request.mode });
      request.args.value = "policy cannot rewrite execution";
    } });
    if (scoped) {
      const resource = await registry.acquireScoped("probe.effect", { value: "input" }, context());
      expect(resource.value).toBe("input:prepared");
      await resource.dispose(); await resource.dispose();
      expect(f.dispose).toHaveBeenCalledOnce();
      expect(f.destination).toEqual(["input:prepared", "disposed"]);
    } else {
      expect(await registry.invoke("probe.effect", { value: "input" }, context())).toBe("input:prepared");
      expect(f.destination).toEqual(["input:prepared"]);
    }
    expect(checked).toEqual([
      { value: "input", provider: f.provider, mode: scoped ? "acquire" : "invoke" },
      { value: "input:prepared", provider: f.provider, mode: scoped ? "acquire" : "invoke" },
    ]);
    await registry.close();
  });

  it("rechecks a successor policy when binding changes during an awaited final check", async () => {
    const f = probeProvider();
    const registry = new ActionRegistry(); registry.register(f.provider);
    const entered = deferred(); const release = deferred();
    registry.setCertifiedDispatchPolicy({ async assertDispatch(request) {
      if (request.args.value === "input:prepared") { entered.resolve(); await release.promise; }
    } });
    const operation = registry.invoke("probe.effect", { value: "input" }, context());
    const outcome = operation.catch(error => error);
    await entered.promise;
    registry.setCertifiedDispatchPolicy(refuse); release.resolve();
    expect(await outcome).toMatchObject({ message: "uncertified provider" });
    expect(f.destination).toEqual([]);
    await registry.close();
  });

  it("disables speculative launch and serve, including prepared pre-profile tickets", async () => {
    const f = probeProvider(); const registry = new ActionRegistry(); registry.register(f.provider);
    const runtime = speculation(vi.fn(async () => ({ hit: false as const, reason: "absent" as const })));
    registry.setSpeculation(runtime, () => true);
    const prepared = await registry.speculate("probe.effect", { value: "input" }, context(), {});
    expect(prepared).toBeDefined();
    registry.setCertifiedDispatchPolicy(pin(f.provider));
    await expect(prepared!.execute(undefined)).rejects.toThrow("disables speculation");
    expect(await registry.speculate("probe.effect", { value: "input" }, context(), {})).toBeUndefined();
    expect(f.destination).toEqual([]);
    expect(await registry.invoke("probe.effect", { value: "input" }, context())).toBe("input:prepared");
    expect(runtime.tryServe).not.toHaveBeenCalled();
    expect(runtime.reset).toHaveBeenCalled();
    await registry.close();
  });

  it("rejects replay if the certified profile binds while a speculative result is awaited", async () => {
    const f = probeProvider(); const registry = new ActionRegistry(); registry.register(f.provider);
    registry.setSpeculation(speculation(vi.fn(async () => {
      registry.setCertifiedDispatchPolicy(pin(f.provider));
      return { hit: true as const, value: "cached success", replay: {} };
    })), () => true);
    await expect(registry.invoke("probe.effect", { value: "input" }, context())).rejects.toThrow("speculative replay");
    expect(f.destination).toEqual([]);
    await registry.close();
  });

  it("leaves normal-profile speculative replay unchanged", async () => {
    const f = probeProvider(); const registry = new ActionRegistry(); registry.register(f.provider);
    const runtime = speculation(vi.fn(async () => ({ hit: true as const, value: "cached success", replay: {} })));
    registry.setSpeculation(runtime, () => true);
    expect(await registry.invoke("probe.effect", { value: "input" }, context())).toBe("cached success");
    expect(f.destination).toEqual([]);
    expect(runtime.tryServe).toHaveBeenCalledOnce();
    await registry.close();
  });

  it.each(["invoke", "acquire", "speculate", "replay"] as const)("prepared %s operations consult the live final callback", async mode => {
    const f = probeProvider(mode === "acquire"); const bindings = new FabricProviderBindings();
    bindings.mount(f.provider); const binding = bindings.current("probe")!;
    const operations = new ProviderOperations(bindings);
    let policy: CertifiedDispatchPolicy | undefined;
    const execute = operations.prepare({ binding, ref: "probe.effect", action: "effect", mode,
      descriptor: "unchanged", args: { value: "prepared" }, context: context(), replayValue: "old result",
      observe: async () => ({ ref: "probe.effect", descriptor: "unchanged" }),
      assertDispatch: request => policy?.assertDispatch(request),
    });
    const seen = vi.fn((request: Parameters<CertifiedDispatchPolicy["assertDispatch"]>[0]) => {
      expect(request.provider).toBe(f.provider); expect(request.mode).toBe(mode);
      throw new Error("late binding denied");
    });
    policy = { assertDispatch: seen };
    await expect(execute()).rejects.toThrow("late binding denied");
    expect(seen).toHaveBeenCalledOnce(); expect(f.destination).toEqual([]);
    await expect(execute()).rejects.toThrow("spent ticket");
    await operations.close();
  });
});

type Hook = (input: Record<string, unknown>) => void | Promise<void>;
function toolsFixture(name = "write", prepare = false) {
  const cwd = mkdtempSync(join(tmpdir(), "fabric-effects-dispatch-"));
  const destination = new Map<string, string>();
  const unsafeDestination: unknown[] = [];
  const execute = vi.fn(async (_id: string, args: Record<string, unknown>) => {
    unsafeDestination.push(structuredClone(args)); return result("ordinary tool");
  });
  const prepareArguments = vi.fn((args: unknown) => args as { command: string; path: string; content: string });
  const definition = defineTool({ name, label: name, description: "Certified fixture identity",
    parameters: name === "bash" ? Type.Object({ command: Type.String() }) : Type.Object({ path: Type.String(), content: Type.String() }),
    ...(prepare ? { prepareArguments } : {}), execute,
  });
  let hook: Hook = () => {};
  let patch: { content?: AgentToolResult<unknown>["content"]; details?: unknown } | undefined;
  const lifecycle: string[] = [];
  const runner = {
    createContext: () => ({ cwd }), getActiveTools: () => [name],
    emit: vi.fn(async (event: { type: string }) => { lifecycle.push(event.type); }),
    emitToolCall: vi.fn(async (event: { input: Record<string, unknown> }) => {
      lifecycle.push("tool_call"); await hook(event.input); return undefined;
    }),
    emitToolResult: vi.fn(async () => { lifecycle.push("tool_result"); return patch; }),
  } as unknown as ExtensionRunner;
  const catalog = new CapturedToolCatalog();
  const replace = (replacement: ToolDefinition<any, any, any> = definition) => catalog.replace([
    { definition: replacement, sourceInfo: createSyntheticSourceInfo("/extensions/certified.ts", { source: "test" }) },
  ], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
  replace();
  const captured = new CapturedToolsProvider(catalog);
  const pi = new PiToolsProvider(cwd, catalog, captured);
  const dispatcher: CertifiedToolDispatcher = {
    assertTool(toolName, supplied) {
      if (!["write", "edit", "bash"].includes(toolName) ||
        (supplied !== undefined && (supplied as { execute?: unknown }).execute !== definition.execute)) {
        throw new Error("uncertified tool identity");
      }
    },
    execute: vi.fn(async (toolName, id, args) => {
      if (args.content === "denied" || args.command === "unapproved command") throw new Error("uncertified final arguments");
      destination.set(String(args.path ?? toolName), String(args.content ?? args.command));
      return { ...result(), details: { certified: true, id } };
    }),
  };
  const ctx = context(cwd);
  const nativeExecute = vi.fn(async (toolName: string, args: unknown, options: { signal?: AbortSignal } = {}) => {
    const entry = catalog.require(toolName);
    // Use Pi's actual validation/beforeToolCall/execute/afterToolCall boundary,
    // rather than pretending native execute checks its cancellation signal.
    return runToolCall({ type: "toolCall", id: "parent/1", name: toolName, arguments: args as Parameters<typeof runToolCall>[0]["arguments"] }, {
      tools: [entry.wrappedTool as AgentTool],
      context: { messages: [], tools: [entry.wrappedTool as AgentTool] },
      assistantMessage: {} as Parameters<typeof runToolCall>[1]["assistantMessage"],
      ...(options.signal ? { signal: options.signal } : {}),
      beforeToolCall: async call => {
        const preflight = await runner.emitToolCall({ type: "tool_call", toolName, toolCallId: call.toolCall.id, input: call.args as Record<string, unknown> });
        return preflight;
      },
    });
  });
  const nativeContext = () => ({ ...ctx, extensionContext: {
    cwd, tools: [catalog.require(name).wrappedTool], executeTool: nativeExecute,
  } as unknown as ExtensionContext });
  return { cwd, destination, unsafeDestination, execute, prepareArguments, definition, runner, catalog, captured, pi, dispatcher,
    ctx, nativeContext, nativeExecute, lifecycle, replace,
    setHook(next: Hook) { hook = next; }, setPatch(next: typeof patch) { patch = next; },
    async close() { await pi.close(); await captured.close(); },
  };
}

describe("certified Pi and captured execution lifecycle", () => {
  it("routes stock write through the adapter after hook rewrites, preserving lifecycle/output", async () => {
    const f = toolsFixture(); f.catalog.replace([], f.runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
    f.pi.setCertifiedToolDispatcher(f.dispatcher);
    const target = join(f.cwd, "must-not-be-stock-written.txt");
    f.setHook(args => { args.path = target; args.content = "hook bytes"; });
    f.setPatch({ content: [{ type: "text", text: "patched display" }], details: { patched: true } });
    const registry = new ActionRegistry(); registry.register(f.pi); registry.setCertifiedDispatchPolicy(pin(f.pi));
    expect(await registry.invoke("pi.write", { path: "initial.txt", content: "initial bytes" }, f.ctx)).toMatchObject({ ok: true, output: "patched display", details: { patched: true } });
    expect(f.destination.get(target)).toBe("hook bytes");
    expect(existsSync(target)).toBe(false);
    expect(f.lifecycle).toEqual(["tool_execution_start", "tool_call", "tool_result", "tool_execution_end"]);
    expect(f.dispatcher.execute).toHaveBeenCalledWith("write", expect.stringContaining("fabric"), { path: target, content: "hook bytes" }, expect.objectContaining({ parentToolCallId: "parent" }));
    await registry.close(); await f.close();
  });

  it.each(["pi", "captured"] as const)("uses a hook's replacement input object at the final %s adapter boundary", async route => {
    const f = toolsFixture();
    if (route === "pi") f.catalog.replace([], f.runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
    f.pi.setCertifiedToolDispatcher(f.dispatcher);
    const emit = f.runner.emitToolCall.bind(f.runner);
    vi.mocked(f.runner.emitToolCall).mockImplementationOnce(async event => {
      await emit(event);
      event.input = { path: "replacement", content: "replacement bytes" };
      return undefined;
    });
    const provider = route === "pi" ? f.pi : f.captured;
    await provider.invoke("write", { path: "initial", content: "initial bytes" }, f.ctx);
    expect([...f.destination]).toEqual([["replacement", "replacement bytes"]]);
    expect(f.unsafeDestination).toEqual([]); expect(existsSync(join(f.cwd, "initial"))).toBe(false);
    await f.close();
  });

  it("rejects malformed final Pi hook arguments before the adapter or stock writer", async () => {
    const f = toolsFixture(); f.catalog.replace([], f.runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
    f.pi.setCertifiedToolDispatcher(f.dispatcher);
    f.setHook(args => { args.content = 123; });
    await expect(f.pi.invoke("write", { path: "target", content: "before hook" }, f.ctx)).rejects.toThrow("Invalid final arguments");
    expect(f.destination.size).toBe(0); expect(f.dispatcher.execute).not.toHaveBeenCalled();
    expect(existsSync(join(f.cwd, "target"))).toBe(false);
    expect(f.lifecycle.at(-1)).toBe("tool_execution_end");
    await f.close();
  });

  it("adapts stock edit/bash without invoking their ordinary implementations", async () => {
    const f = toolsFixture(); f.catalog.replace([], f.runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
    f.pi.setCertifiedToolDispatcher(f.dispatcher);
    await expect(f.pi.invoke("edit", { path: "nonexistent", edits: [{ oldText: "old", newText: "new" }] }, f.ctx)).resolves.toMatchObject({ ok: true, output: "certified" });
    await expect(f.pi.invoke("bash", { command: "unapproved command" }, f.ctx)).rejects.toThrow("uncertified final arguments");
    expect(f.dispatcher.execute).toHaveBeenCalledTimes(2);
    expect(existsSync(join(f.cwd, "nonexistent"))).toBe(false);
    expect(f.pi.shellJobs.live()).toEqual([]);
    await f.close();
  });

  it.each(["stock", "pi-captured", "extensions"] as const)("preserves the exact certified shell grammar on %s, without host defaults or dropped caller flags", async route => {
    const f = toolsFixture("bash", true);
    const target = join(f.cwd, "read-only.txt"); writeFileSync(target, "checked bytes");
    const sha256 = createHash("sha256").update("checked bytes").digest("hex");
    const command = shellEffectCommand("read-only.txt", sha256);
    if (route === "stock") f.catalog.replace([], f.runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
    f.dispatcher.execute = vi.fn(async (name, _id, args) => {
      expect(name).toBe("bash");
      await prepareShellEffect(f.cwd, args, { kind: "read-sha256-v1", path: target, sha256 });
      f.destination.set("shell", args.command as string); return result("read checked");
    });
    f.pi.setCertifiedToolDispatcher(f.dispatcher);
    f.setHook(args => { applyRunBashDefaults({ PI_FABRIC_BASH_IDLE_S: "1" }, args); });
    const registry = new ActionRegistry(); registry.register(f.pi); registry.register(f.captured);
    registry.setCertifiedDispatchPolicy({ assertDispatch(request) {
      if (request.provider !== f.pi && request.provider !== f.captured) throw new Error("wrong provider");
    } });
    const ref = route === "extensions" ? "extensions.bash" : "pi.bash";
    expect(await registry.invoke(ref, { command }, f.ctx)).toMatchObject(route === "extensions" ? { text: "read checked" } : { ok: true, output: "read checked" });
    expect(f.destination.get("shell")).toBe(command);
    expect(f.prepareArguments).not.toHaveBeenCalled(); expect(f.nativeExecute).not.toHaveBeenCalled();
    expect(f.unsafeDestination).toEqual([]); expect(f.pi.shellJobs.live()).toEqual([]);
    // In particular durable:false must not disappear in Pi's captured delegation.
    for (const flags of [{ timeout: 1 }, { cwd: f.cwd }, { durable: false }]) {
      await expect(registry.invoke(ref, { command, ...flags }, f.ctx)).rejects.toThrow();
    }
    f.setHook(args => { args.command = "unapproved command"; });
    await expect(registry.invoke(ref, { command }, f.ctx)).rejects.toThrow("command must exactly match");
    expect(f.destination.get("shell")).toBe(command); expect(f.unsafeDestination).toEqual([]);
    await registry.close(); await f.close();
  });

  it("never uses the git-worktree intercept, including a binding installed by preflight", async () => {
    const intercept = vi.spyOn(worktree, "tryExecuteGitWorktreeAdd").mockResolvedValue({ content: [{ type: "text", text: "unsafe intercept" }], details: { exitCode: 0, cloned: false } });
    try {
      for (const late of [false, true]) {
        const f = toolsFixture(); f.catalog.replace([], f.runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
        f.dispatcher.execute = vi.fn(async () => { throw new Error("not the fixed read command"); });
        if (late) f.setHook(() => f.pi.setCertifiedToolDispatcher(f.dispatcher));
        else f.pi.setCertifiedToolDispatcher(f.dispatcher);
        await expect(f.pi.invoke("bash", { command: "git worktree add /fabric-certified-never-created" }, f.ctx)).rejects.toThrow("not the fixed read command");
        expect(f.pi.shellJobs.live()).toEqual([]); expect(f.unsafeDestination).toEqual([]);
        await f.close();
      }
      expect(intercept).not.toHaveBeenCalled();
    } finally { intercept.mockRestore(); }
  });

  it.each([false, true])("adapts captured calls after hooks, even with native execute available=%s", async native => {
    const f = toolsFixture(); f.pi.setCertifiedToolDispatcher(f.dispatcher);
    f.setHook(args => { args.content = "final hook bytes"; });
    f.setPatch({ content: [{ type: "text", text: "redacted result" }] });
    const registry = new ActionRegistry(); registry.register(f.captured); registry.register(f.pi);
    registry.setCertifiedDispatchPolicy({ assertDispatch(request) {
      if (request.provider !== f.pi && request.provider !== f.captured) throw new Error("wrong provider");
    } });
    for (const ref of ["extensions.write", "pi.write"]) {
      const ctx = native ? f.nativeContext() : context(f.cwd);
      expect(await registry.invoke(ref, { path: ref, content: "before hook" }, ctx)).toMatchObject(ref.startsWith("pi.") ? { ok: true, output: "redacted result" } : { text: "redacted result", isError: false });
      expect(f.destination.get(ref)).toBe("final hook bytes");
    }
    expect(f.nativeExecute).not.toHaveBeenCalled(); expect(f.unsafeDestination).toEqual([]);
    expect(f.dispatcher.execute).toHaveBeenCalledTimes(2);
    expect(f.lifecycle).toEqual(Array(2).fill(["tool_execution_start", "tool_call", "tool_result", "tool_execution_end"]).flat());
    await registry.close(); await f.close();
  });

  it.each([false, true])("keeps normal captured execution unchanged (native=%s)", async native => {
    const f = toolsFixture();
    f.setHook(args => { args.content = "normal hook bytes"; });
    expect(await f.captured.invoke("write", { path: "normal", content: "input" }, native ? f.nativeContext() : f.ctx)).toMatchObject({ text: "ordinary tool", isError: false });
    expect(f.unsafeDestination).toEqual([{ path: "normal", content: "normal hook bytes" }]);
    expect(f.nativeExecute).toHaveBeenCalledTimes(native ? 1 : 0);
    expect(f.destination.size).toBe(0);
    await f.close();
  });

  it("refuses opaque same-name overrides before prepareArguments or native dispatch", async () => {
    const f = toolsFixture("write", true); f.pi.setCertifiedToolDispatcher(f.dispatcher);
    const opaque = defineTool({ ...f.definition, async execute() { f.unsafeDestination.push("opaque"); return result(); } });
    f.replace(opaque);
    const registry = new ActionRegistry(); registry.register(f.pi); registry.setCertifiedDispatchPolicy(pin(f.pi));
    await expect(registry.invoke("pi.write", { path: "target", content: "input" }, f.nativeContext())).rejects.toThrow("identity");
    expect(f.prepareArguments).not.toHaveBeenCalled(); expect(f.nativeExecute).not.toHaveBeenCalled();
    expect(f.unsafeDestination).toEqual([]); expect(f.destination.size).toBe(0);
    await registry.close(); await f.close();
  });

  it("refuses a captured definition replaced by a hook before destination execution", async () => {
    const f = toolsFixture(); f.pi.setCertifiedToolDispatcher(f.dispatcher);
    f.setHook(() => f.replace(defineTool({ ...f.definition, async execute() { f.unsafeDestination.push("opaque"); return result(); } })));
    await expect(f.captured.invoke("write", { path: "target", content: "input" }, f.nativeContext())).rejects.toThrow("identity");
    expect(f.dispatcher.execute).not.toHaveBeenCalled(); expect(f.nativeExecute).not.toHaveBeenCalled();
    expect(f.unsafeDestination).toEqual([]); expect(f.destination.size).toBe(0);
    expect(f.lifecycle).toEqual(["tool_execution_start", "tool_call", "tool_result", "tool_execution_end"]);
    await f.close();
  });

  it.each([false, true])("checks certified captured final arguments after hooks (native available=%s)", async native => {
    const f = toolsFixture(); f.captured.setCertifiedToolDispatcher(f.dispatcher);
    f.setHook(args => { args.content = "denied"; });
    await expect(f.captured.invoke("write", { path: "target", content: "input" }, native ? f.nativeContext() : f.ctx)).rejects.toThrow("uncertified final arguments");
    expect(f.unsafeDestination).toEqual([]); expect(f.destination.size).toBe(0);
    expect(f.nativeExecute).not.toHaveBeenCalled();
    await f.close();
  });

  it("uses a dispatcher bound during fallback hooks instead of the captured writer", async () => {
    const f = toolsFixture(); f.setHook(args => { args.content = "late binding"; f.captured.setCertifiedToolDispatcher(f.dispatcher); });
    expect(await f.captured.invoke("write", { path: "target", content: "input" }, f.ctx)).toMatchObject({ text: "certified" });
    expect(f.destination.get("target")).toBe("late binding"); expect(f.unsafeDestination).toEqual([]);
    await f.close();
  });

  it("retires native calls if the profile binds inside the native pre-execute hook", async () => {
    const f = toolsFixture(); f.setHook(() => f.captured.setCertifiedToolDispatcher(f.dispatcher));
    await expect(f.captured.invoke("write", { path: "target", content: "input" }, f.nativeContext())).rejects.toThrow("binding changed");
    expect(f.nativeExecute).toHaveBeenCalledOnce(); expect(f.unsafeDestination).toEqual([]);
    expect(f.destination.size).toBe(0);
    await f.close();
  });

  it("preserves native result projection and allows host removal of the profile", async () => {
    const f = toolsFixture(); f.pi.setCertifiedToolDispatcher(f.dispatcher);
    expect(await f.captured.invoke("write", { path: "target", content: "input" }, { ...f.ctx, nativeToolResult: true })).toBe("certified");
    f.pi.setCertifiedToolDispatcher(undefined);
    expect(await f.captured.invoke("write", { path: "target", content: "normal" }, f.nativeContext())).toMatchObject({ text: "ordinary tool" });
    expect(f.unsafeDestination).toEqual([{ path: "target", content: "normal" }]);
    await f.close();
  });
});

// Keep reason: data safety + trust boundary. The tests below cross the REAL
// runtime -> registry/providers -> concrete record port -> EffectStore -> file
// sink boundary. Registration/admission/owner/native-assistant callbacks are
// explicitly host receipt probes: B3/B5/B6 and the native CLI are NOT qualified.
// All retained files live in unique test-owned scratch. No cleanup/deletion runs.
type SlotSpec = {
  id: string;
  tool: "write" | "edit" | "bash";
  args: Record<string, unknown>;
  target?: string;
  predecessors?: readonly string[];
  adapterVersion?: string;
  phase?: string;
  constraints?: EffectSlot["constraints"];
};
function provisionRecords(specs: readonly SlotSpec[]) {
  const root = mkdtempSync(join(tmpdir(), "fabric-effects-record-dispatch-"));
  const workspaceRoot = join(root, "workspace");
  const sessionRoot = join(root, "session");
  mkdirSync(workspaceRoot, { mode: 0o700 });
  mkdirSync(sessionRoot, { mode: 0o700 });
  const slots: EffectSlot[] = specs.map((spec, ordinal) => ({
    id: spec.id, ordinal, kind: { write: "file-write", edit: "file-edit", bash: "shell-check" }[spec.tool],
    canonicalRef: `pi.${spec.tool}`, adapterVersion: spec.adapterVersion ?? "1",
    canonicalTarget: resolve(workspaceRoot, spec.target ?? String(spec.args.path ?? "file.txt")),
    canonicalArguments: JSON.parse(canonicalEncode(spec.args)),
    predecessors: spec.predecessors ?? [], constraints: spec.constraints ?? {},
    ...(spec.phase === undefined ? {} : { phase: spec.phase }),
  }));
  const manifest = { version: 1 as const, slots };
  const acceptedInput: AcceptedInput = {
    id: "accepted-original", digest: sha256Bytes("original accepted input"), nativeEntryId: "input-receipt-probe",
    manifest, manifestDigest: manifestDigest(manifest),
  };
  return { root, workspaceRoot, sessionRoot, acceptedInput };
}
type RecordProvision = ReturnType<typeof provisionRecords>;
function receiptProbeOwner(storage: RecordProvision, incarnation = "first"): RecoveryOwner {
  return {
    orgId: "record-dispatch-org", logicalSessionId: "record-dispatch-session",
    sessionRoot: storage.sessionRoot, workspaceRoot: storage.workspaceRoot,
    sessionFile: join(storage.sessionRoot, "native-session-receipt-probe.jsonl"), pvcUID: "retained-volume-probe",
    exclusionReceipt: `host-exclusion-receipt-probe:${incarnation}`,
    attempt: { podUID: `pod-${incarnation}`, bootId: `boot-${incarnation}`,
      ownerIncarnation: incarnation, authorityRevision: `revision-${incarnation}` },
    signal: new AbortController().signal,
    assertLive() { /* host capability probe, not a B3 old-writer fence */ },
  };
}
function realRecordsFixture(storage: RecordProvision, options: {
  incarnation?: string;
  failpoint?: CertifiedEffectRecordOptions["failpoint"];
  admissionForCall?: CertifiedEffectRecordOptions["admissionForCall"];
  readNativeToolResult?: CertifiedEffectRecordOptions["readNativeToolResult"];
  programs?: CertifiedEffectRuntimeOptions["programs"];
  stock?: boolean;
} = {}) {
  const owner = receiptProbeOwner(storage, options.incarnation);
  const store = new EffectStore(owner);
  const selected = new Map<string, unknown>((options.programs ?? []).map(program => [program.toolName, program.definition]));
  const handlers = new Map<string, (event: Record<string, unknown>) => unknown | Promise<unknown>>();
  const events: Record<string, unknown>[] = [];
  let resultHook: ExtensionRunner["emitToolResult"] = async () => undefined;
  const extensionContext = { cwd: storage.workspaceRoot } as ExtensionContext;
  const toolContext: ExtensionToolContext = { ...extensionContext, tools: [],
    async executeTool() { throw new Error("native tool execution is not part of this host receipt probe"); } };
  const admissionForCall = vi.fn<CertifiedEffectRecordOptions["admissionForCall"]>(options.admissionForCall ?? (() => ({
    acceptedInput: storage.acceptedInput, nativeAssistantEntryId: "assistant-durability-receipt-probe",
  })));
  // This probe only establishes that dispatch awaits the supplied port; it is
  // NOT Pi's missing B6 post-append native durability acknowledgement.
  const confirmNativeAssistant = vi.fn<CertifiedEffectRecordOptions["confirmNativeAssistant"]>(async () => {});
  const assertManifestConstraints = vi.fn<CertifiedEffectRecordOptions["assertManifestConstraints"]>((slot, prepared) => {
    const constraints = slot.constraints as { postSha256?: string };
    if (constraints.postSha256 !== undefined && (prepared.format !== "fabric-file-effect-v1" || prepared.post.sha256 !== constraints.postSha256)) {
      throw new Error("manifest post hash constraint refused");
    }
  });
  const records = createCertifiedEffectRecordPort({
    owner, store, admissionForCall, confirmNativeAssistant, assertManifestConstraints,
    ...(options.failpoint === undefined ? {} : { failpoint: options.failpoint }),
    ...(options.readNativeToolResult === undefined ? {} : { readNativeToolResult: options.readNativeToolResult }),
  });
  const runner = {
    createContext: () => extensionContext,
    getActiveTools: () => [...selected.keys()],
    emit: async (event: Record<string, unknown>) => { events.push(event); },
    emitToolCall: async (event: Record<string, unknown>) => { await handlers.get("tool_call")?.(event); return undefined; },
    emitToolResult: (event: Parameters<ExtensionRunner["emitToolResult"]>[0]) => resultHook(event),
  } as unknown as ExtensionRunner;
  const catalog = new CapturedToolCatalog();
  const captured = new CapturedToolsProvider(catalog);
  const pi = new PiToolsProvider(storage.workspaceRoot, catalog, captured);
  const registry = new ActionRegistry(); registry.register(pi); registry.register(captured);
  let executionProfile = "quickjs";
  const runtime = createCertifiedEffectRuntime({
    logicalSessionId: owner.logicalSessionId, workspaceRoot: owner.workspaceRoot, records,
    shellProfile: { kind: "read-sha256-v1", path: "file.txt", sha256: sha256Bytes("checked bytes") },
    assertLive: () => owner.assertLive("new-intent"),
    assertExecutionProfile() { if (executionProfile !== "quickjs") throw new Error("unsupported actual execution profile"); },
    getToolDefinition: name => selected.get(name), providers: { pi, captured },
    ...(options.programs === undefined ? {} : { programs: options.programs }),
  });
  runtime.register({
    registerTool(definition: ToolDefinition<any>) { selected.set(definition.name, definition); },
    on(name: string, handler: (event: Record<string, unknown>) => unknown | Promise<unknown>) {
      handlers.set(name, handler); return () => { handlers.delete(name); };
    },
  } as unknown as ExtensionAPI);
  const replaceCatalog = () => catalog.replace((options.stock ? [] : ["write", "edit", "bash"]).map(name => ({
    definition: selected.get(name) as ToolDefinition<any>,
    sourceInfo: createSyntheticSourceInfo(join(storage.root, "certified-registration-probe.ts"), { source: "test" }),
  })), runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
  replaceCatalog();
  bindCertifiedEffectRuntime(runtime, { registry, piTools: pi, capturedTools: captured });
  return {
    ...storage, owner, store, records, runtime, registry, pi, captured, catalog, selected, replaceCatalog,
    admissionForCall, confirmNativeAssistant, assertManifestConstraints, events,
    setResultHook(hook: ExtensionRunner["emitToolResult"]) { resultHook = hook; },
    setExecutionProfile(value: string) { executionProfile = value; },
    hook: async (name: string, event: Record<string, unknown>) => await handlers.get(name)?.(event),
    async direct(name: "write" | "edit" | "bash", toolCallId: string, args: Record<string, unknown>) {
      await handlers.get("tool_call")?.({ type: "tool_call", toolName: name, toolCallId, input: args });
      return runtime.getToolDefinition(name)!.execute(toolCallId, args, undefined, undefined, toolContext);
    },
    invoke(ref: string, args: Record<string, unknown>, parentToolCallId = "outer-program") {
      return registry.invoke(ref, args, { ...context(storage.workspaceRoot), parentToolCallId });
    },
    ledger: () => store.withSession("settle", transaction => transaction.list()),
    async close() { runtime.close(); await registry.close(); await pi.close(); await captured.close(); },
  };
}
const fileIdentity = (file: string) => {
  const stat = statSync(file, { bigint: true });
  return { dev: stat.dev, ino: stat.ino, size: stat.size, mode: stat.mode, mtimeNs: stat.mtimeNs, ctimeNs: stat.ctimeNs };
};

// Finite, event-driven safety net for a missing boundary/child-close event; never
// a polling interval and never a substitute for the real process close receipt.
async function boundedEvent<T>(event: Promise<T>, milliseconds: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([event, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} exceeded ${milliseconds}ms`)), milliseconds);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}

describe("concrete record dispatch integration (host receipt probes, not B3/B5/B6/native CLI)", () => {
  it("joins the registered definition and pi.write/extensions.write into one K with fresh physical aliases", async () => {
    const args = { path: "file.txt", content: "joined original bytes" };
    const reached = deferred(); const release = deferred();
    const points: EffectFailpointName[] = [];
    const f = realRecordsFixture(provisionRecords([{ id: "write-once", tool: "write", args }]), {
      async failpoint(point) {
        points.push(point);
        if (point === "intent-durable") { reached.resolve(); await release.promise; }
      },
    });
    let first: Promise<AgentToolResult<unknown>> | undefined;
    try {
      first = f.direct("write", "registered-first", args);
      // Attach immediately: a failed preparation must not become an unhandled
      // rejection while the independent event deadline reports the missing cut.
      const observedFirst = first.catch(error => { reached.resolve(); throw error; });
      void observedFirst.catch(() => {});
      await boundedEvent(reached.promise, 5_000, "real INTENT barrier");
      const viaPi = f.invoke("pi.write", args, "pi-parent");
      const viaExtensions = f.invoke("extensions.write", args, "extensions-parent");
      release.resolve();
      const [direct, piResult, capturedResult] = await Promise.all([observedFirst, viaPi, viaExtensions]);
      expect(piResult).toMatchObject({ ok: true, output: direct.content[0]?.type === "text" ? direct.content[0].text : "" });
      expect(capturedResult).toMatchObject({ content: direct.content, details: direct.details, isError: false });
      expect(readFileSync(join(f.workspaceRoot, "file.txt"), "utf8")).toBe(args.content);
      const [record] = await f.ledger();
      expect(await f.ledger()).toHaveLength(1);
      expect(record!.intent.key).toBe(mintEffectKey([1, f.owner.orgId, f.owner.logicalSessionId, f.acceptedInput.id, "write-once", "file-write"]));
      expect(record!.intent.tool).toMatchObject({ originalToolCallId: "registered-first", canonicalRef: "pi.write" });
      const aliases = record!.transitions.filter(row => row.state === "ALIAS");
      expect(aliases).toHaveLength(2);
      expect(new Set([record!.intent.tool.originalToolCallId, ...aliases.map(row => row.toolCallId)]).size).toBe(3);
      expect(aliases.map(row => row.parentToolCallId).sort()).toEqual(["extensions-parent", "pi-parent"]);
      expect(record!.transitions.filter(row => row.state === "RESULT")).toHaveLength(1);
      expect(f.assertManifestConstraints).toHaveBeenCalledOnce();
      expect(f.confirmNativeAssistant).toHaveBeenCalledTimes(3);
      expect(points.filter(point => point === "file-renamed")).toHaveLength(1);
    } finally {
      release.resolve();
      await first?.catch(() => {});
      await f.close();
    }
  });

  it("returns a historical write after a later legitimate edit without preparing, renaming or reverting that edit", async () => {
    const write = { path: "file.txt", content: "original body" };
    const edit = { path: "file.txt", edits: [{ oldText: "original", newText: "later legitimate" }] };
    const points: EffectFailpointName[] = [];
    const f = realRecordsFixture(provisionRecords([
      { id: "initial-write", tool: "write", args: write },
      { id: "later-edit", tool: "edit", args: edit, predecessors: ["initial-write"] },
    ]), { failpoint(point) { points.push(point); } });
    const prepare = vi.spyOn(fileEffects, "prepareFileEffect");
    try {
      const originalResult = await f.direct("write", "write-original", write);
      await f.invoke("extensions.edit", edit, "later-edit-parent");
      const target = join(f.workspaceRoot, "file.txt");
      const completed = fileIdentity(target);
      const before = await f.ledger();
      const writeRecord = before.find(row => row.intent.slot.id === "initial-write")!;
      const resultRow = writeRecord.transitions.find(row => row.state === "RESULT")!;
      const preparations = prepare.mock.calls.length;
      const previousPoints = [...points];
      expect(await f.direct("write", "write-historical-fresh", write)).toEqual(originalResult);
      expect(await f.invoke("pi.write", write, "historical-parent")).toMatchObject({ ok: true, output: originalResult.content[0]?.type === "text" ? originalResult.content[0].text : "" });
      expect(readFileSync(target, "utf8")).toBe("later legitimate body");
      expect(fileIdentity(target)).toEqual(completed);
      expect(prepare).toHaveBeenCalledTimes(preparations);
      expect(points).toEqual(previousPoints);
      expect(f.assertManifestConstraints).toHaveBeenCalledTimes(2);
      const after = await f.ledger();
      expect(after).toHaveLength(2);
      const historical = after.find(row => row.intent.key === writeRecord.intent.key)!;
      expect(historical.intentDigest).toBe(writeRecord.intentDigest);
      expect(historical.transitions.find(row => row.state === "RESULT")).toEqual(resultRow);
      expect(historical.transitions.slice(writeRecord.transitions.length).every(row => row.state === "ALIAS")).toBe(true);
    } finally { prepare.mockRestore(); await f.close(); }
  });

  it("reports changed-request conflict with the saved original outcome and changes neither destination nor ledger", async () => {
    const args = { path: "file.txt", content: "authorized" };
    const f = realRecordsFixture(provisionRecords([{ id: "bound-write", tool: "write", args }]));
    try {
      const saved = await f.direct("write", "original", args);
      const before = await f.ledger();
      const target = join(f.workspaceRoot, "file.txt"); const identity = fileIdentity(target);
      await expect(f.direct("write", "changed-direct", { ...args, content: "changed request" })).rejects.toMatchObject({
        code: "EFFECT_SLOT_CONFLICT", original: { key: before[0]!.intent.key, state: "RESULT", result: saved },
      });
      for (const ref of ["pi.write", "extensions.write"]) {
        const error = await f.invoke(ref, { ...args, content: "changed request" }, "changed-parent").catch((failure: unknown) => failure);
        expect(error).toMatchObject({ name: "EFFECT_SLOT_CONFLICT", code: "EFFECT_SLOT_CONFLICT",
          message: expect.stringContaining(`saved effect ${before[0]!.intent.key} (RESULT)`),
          original: { key: before[0]!.intent.key, state: "RESULT" } });
        expect(error).toHaveProperty("original", { key: before[0]!.intent.key, state: "RESULT" });
      }
      const { QuickJsRuntime } = await import("../src/runtime/quickjs-runtime.js");
      const guest = await new QuickJsRuntime().execute(
        'try { await pi.write({path:"file.txt",content:"changed request"}); return {wrongSuccess:true}; } catch (error) { return {name:error.name,message:error.message}; }',
        (ref, supplied) => f.invoke(ref, supplied), { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 });
      expect(guest.error).toBeUndefined();
      expect(guest.value).toMatchObject({ name: "EFFECT_SLOT_CONFLICT", message: expect.stringContaining(before[0]!.intent.key) });
      expect(await f.ledger()).toEqual(before);
      expect(fileIdentity(target)).toEqual(identity);
      expect(readFileSync(target, "utf8")).toBe(args.content);
      expect(f.assertManifestConstraints).toHaveBeenCalledOnce();
    } finally { await f.close(); }
  });

  it.each([
    { label: "stock Pi", ref: "pi.write", stock: true },
    { label: "Pi captured", ref: "pi.write", stock: false },
    { label: "captured", ref: "extensions.write", stock: false },
  ])("keeps $label certified conflicts failed through final redaction/success patches and host unbind", async ({ ref, stock }) => {
    const args = { path: "file.txt", content: "authorized original" };
    const f = realRecordsFixture(provisionRecords([{ id: "bound", tool: "write", args }]), { stock });
    try {
      await f.direct("write", "saved-original", args);
      const before = await f.ledger();
      const target = join(f.workspaceRoot, "file.txt"); const identity = fileIdentity(target);
      for (const content of [[{ type: "text" as const, text: "redacted refusal" }], []]) {
        f.setResultHook(async () => ({ content, isError: false }));
        const error = await f.invoke(ref, { ...args, content: "must-not-execute" }).catch((failure: unknown) => failure);
        expect(error).toMatchObject({ name: "EFFECT_SLOT_CONFLICT", code: "EFFECT_SLOT_CONFLICT",
          message: content.length ? "redacted refusal" : "", original: { key: before[0]!.intent.key, state: "RESULT" } });
        expect(error).toHaveProperty("original", { key: before[0]!.intent.key, state: "RESULT" });
        expect(f.events.filter(event => event.type === "tool_execution_end").at(-1)).toMatchObject({ isError: true });
      }
      f.setResultHook(async () => {
        f.pi.setCertifiedToolDispatcher(undefined); f.captured.setCertifiedToolDispatcher(undefined);
        return { content: [{ type: "text", text: "redacted after host unbind" }], isError: false };
      });
      await expect(f.invoke(ref, { ...args, content: "still-must-not-execute" })).rejects.toMatchObject({
        name: "EFFECT_SLOT_CONFLICT", code: "EFFECT_SLOT_CONFLICT", message: "redacted after host unbind",
        original: { key: before[0]!.intent.key, state: "RESULT" },
      });
      expect(f.events.filter(event => event.type === "tool_execution_end").at(-1)).toMatchObject({ isError: true });
      expect(await f.ledger()).toEqual(before);
      expect(fileIdentity(target)).toEqual(identity);
      expect(readFileSync(target, "utf8")).toBe(args.content);
    } finally { await f.close(); }
  });

  it("enforces the actual admitted manifest constraint before binding INTENT or touching the destination", async () => {
    const args = { path: "file.txt", content: "wrong desired hash" };
    const f = realRecordsFixture(provisionRecords([{
      id: "constrained", tool: "write", args, constraints: { postSha256: sha256Bytes("different approved bytes") },
    }]));
    try {
      await expect(f.invoke("pi.write", args)).rejects.toThrow("manifest post hash constraint refused");
      expect(await f.ledger()).toEqual([]);
      expect(existsSync(join(f.workspaceRoot, "file.txt"))).toBe(false);
      expect(f.assertManifestConstraints).toHaveBeenCalledOnce();
    } finally { await f.close(); }
  });

  it.each(["predecessor", "version", "cancelled"] as const)("refuses %s admission before file preparation", async reason => {
    const write = { path: "file.txt", content: "authorized" };
    const edit = { path: "file.txt", edits: [{ oldText: "authorized", newText: "later" }] };
    const storage = provisionRecords(reason === "predecessor" ? [
      { id: "write", tool: "write", args: write },
      { id: "edit", tool: "edit", args: edit, predecessors: ["write"] },
    ] : [{ id: "write", tool: "write", args: write, ...(reason === "version" ? { adapterVersion: "2" } : {}) }]);
    const f = realRecordsFixture(storage, reason === "cancelled" ? { admissionForCall: () => ({
      acceptedInput: { ...storage.acceptedInput, cancellationRef: "durable-cancel-probe" },
      nativeAssistantEntryId: "assistant-receipt-probe",
    }) } : {});
    const prepare = vi.spyOn(fileEffects, "prepareFileEffect");
    try {
      const operation = reason === "predecessor" ? f.direct("edit", "not-started", edit) : f.direct("write", "not-started", write);
      await expect(operation).rejects.toMatchObject({ code: {
        predecessor: "EFFECT_SLOT_PREDECESSOR", version: "EFFECT_ADAPTER_VERSION_UNSUPPORTED", cancelled: "EFFECT_INPUT_CLOSED",
      }[reason] });
      expect(prepare).not.toHaveBeenCalled();
      expect(await f.ledger()).toEqual([]);
      expect(existsSync(join(f.workspaceRoot, "file.txt"))).toBe(false);
    } finally { prepare.mockRestore(); await f.close(); }
  });

  it("refuses unsupported read-risk providers, acquisition, and same-ref replacement without destination effects", async () => {
    const args = { path: "file.txt", content: "authorized" };
    const f = realRecordsFixture(provisionRecords([{ id: "write", tool: "write", args }]));
    const unsupported = probeProvider(true); f.registry.register(unsupported.provider);
    const replacement = new PiToolsProvider(f.workspaceRoot);
    try {
      await expect(f.invoke("probe.effect", { value: "read-risk is not certification" })).rejects.toThrow("EFFECT_UNSUPPORTED: probe.effect");
      await expect(f.registry.acquireScoped("probe.effect", { value: "scoped" }, context(f.workspaceRoot))).rejects.toThrow("EFFECT_UNSUPPORTED: acquire");
      await expect(f.invoke("pi.read", { path: "file.txt" })).rejects.toThrow("EFFECT_UNSUPPORTED");
      f.registry.register(replacement, { overwrite: true });
      await expect(f.invoke("pi.write", args)).rejects.toThrow("unbound tool provider");
      expect(unsupported.destination).toEqual([]);
      expect(await f.ledger()).toEqual([]);
      expect(existsSync(join(f.workspaceRoot, "file.txt"))).toBe(false);
    } finally { await replacement.close(); await f.close(); }
  });

  it("pins complete frozen QuickJS program inputs/definition and refuses RPC bash and actual backend changes", async () => {
    const args = { path: "file.txt", content: "frozen program bytes" };
    const program = {
      code: 'const written = await pi.write({path:"file.txt",content:"frozen program bytes"}); return {written, process:typeof process, fetch:typeof fetch};',
      payloads: { approved: "original payload" }, timeoutMs: 5_000,
    };
    const originalInput = JSON.parse(canonicalEncode(program)) as typeof program;
    const definition = { name: "fabric_exec", receiptProbe: "selected outer executor identity" };
    const f = realRecordsFixture(provisionRecords([{ id: "write", tool: "write", args }]), {
      programs: [{ toolName: "fabric_exec", input: program, definition }],
    });
    try {
      // Runtime approval snapshots input bytes, not the mutable provisioner's
      // object. The actual QuickJS engine below calls the real registry/sink.
      program.code = "return 'mutated after provisioning'";
      program.payloads.approved = "mutated payload";
      await f.runtime.assertProgram("fabric_exec", originalInput, definition);
      await f.hook("tool_call", { toolName: "fabric_exec", toolCallId: "frozen-outer", input: originalInput });
      const { QuickJsRuntime } = await import("../src/runtime/quickjs-runtime.js");
      const executed = await new QuickJsRuntime().execute(originalInput.code, (ref, supplied) => f.invoke(ref, supplied, "frozen-outer"), {
        timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024, strings: originalInput.payloads,
      });
      expect(executed.error).toBeUndefined();
      expect(executed.value).toMatchObject({ written: { ok: true }, process: "undefined", fetch: "undefined" });
      const ledger = await f.ledger();
      const target = join(f.workspaceRoot, "file.txt"); const identity = fileIdentity(target);
      for (const changed of [program, { ...originalInput, timeoutMs: 10_000 }, { ...originalInput, kernel: "python" },
        { ...originalInput, ref: "saved-program@different" }, { ...originalInput, artifact: { path: "escape" } }]) {
        await expect(f.runtime.assertProgram("fabric_exec", changed, definition)).rejects.toThrow("unapproved fabric_exec program or execution options");
      }
      await expect(f.runtime.assertProgram("fabric_exec", originalInput, { ...definition })).rejects.toThrow("unapproved");
      await expect(f.hook("user_bash", { command: "echo must-not-run" })).rejects.toThrow("user/RPC bash");
      f.setExecutionProfile("node-process");
      await expect(f.runtime.assertProgram("fabric_exec", originalInput, definition)).rejects.toThrow("actual execution profile");
      await expect(f.invoke("pi.write", args)).rejects.toThrow("actual execution profile");
      expect(await f.ledger()).toEqual(ledger);
      expect(fileIdentity(target)).toEqual(identity);
    } finally { await f.close(); }
  });

  it("runs only the frozen fixed read/hash command through the real runtime/provider/records sink", async () => {
    const args = { command: shellEffectCommand("file.txt", sha256Bytes("checked bytes")) };
    const f = realRecordsFixture(provisionRecords([{ id: "bounded-read", tool: "bash", args }]));
    const target = join(f.workspaceRoot, "file.txt"); writeFileSync(target, "checked bytes");
    const identity = fileIdentity(target);
    try {
      expect(await f.invoke("pi.bash", args)).toMatchObject({ ok: true, output: `${sha256Bytes("checked bytes")}  file.txt\n` });
      const ledger = await f.ledger();
      expect(ledger).toHaveLength(1);
      expect(ledger[0]!.intent.adapterData).toMatchObject({ format: "fabric-read-sha256-v1", command: args.command });
      await expect(f.direct("bash", "changed-shell-direct", { command: "echo must-not-run" })).rejects.toMatchObject({ code: "EFFECT_SLOT_CONFLICT" });
      await expect(f.invoke("extensions.bash", { command: "echo must-not-run" })).rejects.toThrow("Changed request for a bound effect slot");
      expect(await f.ledger()).toEqual(ledger);
      expect(fileIdentity(target)).toEqual(identity);
      expect(f.pi.shellJobs.live()).toEqual([]);
    } finally { await f.close(); }
  });

  it.each(["runtime-first", "defaults-first", "defaults-disabled-first"] as const)("uses actual runToolCall validation with explicit native bash hook wiring: %s", async order => {
    const args = { command: shellEffectCommand("file.txt", sha256Bytes("checked bytes")) };
    const f = realRecordsFixture(provisionRecords([{ id: "native-read", tool: "bash", args }]));
    const target = join(f.workspaceRoot, "file.txt"); writeFileSync(target, "checked bytes");
    const identity = fileIdentity(target);
    let beforeKeys: string[] | undefined, beforeSymbols: symbol[] | undefined;
    let afterKeys: string[] | undefined, afterCommand: unknown;
    let afterHolder: PropertyDescriptor | undefined;
    let defaultsChangedInput = false;
    try {
      const tool = f.catalog.require("bash").wrappedTool as AgentTool;
      const native = await runToolCall({ type: "toolCall", id: "native-prepared-bash", name: "bash", arguments: args }, {
        tools: [tool], context: { messages: [], tools: [tool] },
        assistantMessage: {} as Parameters<typeof runToolCall>[1]["assistantMessage"],
        beforeToolCall: async call => {
          const input = call.args as Record<string, unknown>;
          // validateToolArguments really structuredClone()s preparation output:
          // no test-injected metadata may stand in for native hook registration.
          beforeKeys = Object.keys(input); beforeSymbols = Object.getOwnPropertySymbols(input);
          const defaults = () => {
            const off = order === "defaults-disabled-first";
            applyRunBashDefaults({ PI_FABRIC_BASH_IDLE_S: off ? "0" : "1", PI_FABRIC_ACTOR_BASH_TIMEOUT_S: off ? "0" : "2" }, input);
            defaultsChangedInput = input.command !== args.command || input.timeout !== undefined;
          };
          if (order !== "runtime-first") defaults();
          await f.hook("tool_call", { type: "tool_call", toolName: "bash", toolCallId: call.toolCall.id, input });
          if (order === "runtime-first") defaults();
          afterKeys = Object.keys(input); afterCommand = input.command;
          afterHolder = Object.getOwnPropertyDescriptor(input, Symbol.for("pi-fabric.bash-defaults.deferred"));
          return undefined;
        },
      });
      // Assertions stay OUTSIDE hooks: runToolCall correctly captures hook
      // exceptions as tool failures, which must not conceal a failed assertion.
      expect(beforeKeys).toEqual(["command"]); expect(beforeSymbols).toEqual([]);
      if (order === "defaults-first") {
        expect(defaultsChangedInput).toBe(true);
        expect(native.isError, JSON.stringify(native.result)).toBe(true);
        expect(native.result.content).toEqual([expect.objectContaining({ type: "text", text: expect.stringMatching(/Symbol|unsupported|Invalid|not authorized/i) })]);
        expect(await f.ledger()).toEqual([]);
        expect(f.confirmNativeAssistant).not.toHaveBeenCalled();
      } else {
        expect(defaultsChangedInput).toBe(false);
        expect(afterKeys).toEqual(["command"]); expect(afterCommand).toBe(args.command);
        expect(afterHolder).toMatchObject({ enumerable: false, writable: false, configurable: false, value: expect.any(Object) });
        expect(native.isError, JSON.stringify(native.result)).toBe(false);
        expect(native.result.content).toEqual([{ type: "text", text: `${sha256Bytes("checked bytes")}  file.txt\n` }]);
        const [record] = await f.ledger();
        expect(record!.intent.canonicalArguments).toEqual(args);
        expect(record!.intent.tool.originalToolCallId).toBe("native-prepared-bash");
      }
      expect(fileIdentity(target)).toEqual(identity);
      expect(f.pi.shellJobs.live()).toEqual([]);
    } finally { await f.close(); }
  });

  it("blocks further dispatch after real sink uncertainty until explicit successful settlement", async () => {
    const first = { path: "file.txt", content: "recorded post-state" };
    const next = { path: "next.txt", content: "later legitimate effect" };
    const storage = provisionRecords([
      { id: "first", tool: "write", args: first }, { id: "next", tool: "write", args: next },
    ]);
    const target = join(storage.workspaceRoot, "file.txt"); writeFileSync(target, "pre-state");
    let inject = true;
    const f = realRecordsFixture(storage, { failpoint(point) {
      if (point === "file-temp-synced" && inject) {
        inject = false;
        // Deliberate out-of-band conflict probe strictly inside our temp root.
        writeFileSync(target, "neither recorded pre nor post");
        throw new Error("injected before-rename uncertainty");
      }
    } });
    try {
      await expect(f.direct("write", "uncertain-original", first)).rejects.toMatchObject({ code: "EFFECT_UNCERTAIN" });
      const before = await f.ledger();
      expect(before[0]!.transitions.at(-1)?.state).toBe("UNCERTAIN");
      await expect(f.invoke("pi.write", next)).rejects.toThrow("requires explicit B3 settlement");
      await expect(f.direct("write", "uncertain-retry", first)).rejects.toMatchObject({ code: "EFFECT_SETTLEMENT_REQUIRED" });
      expect(await f.ledger()).toEqual(before);
      const unresolved = await f.runtime.settlePendingIntents(f.owner.logicalSessionId, "settle-still-conflicting");
      expect(typeof unresolved.uncertain).toBe("number"); expect(unresolved.uncertain).toBe(1);
      expect(unresolved.settled).toEqual([{ key: before[0]!.intent.key, toolCallId: "uncertain-original", outcome: "uncertain" }]);
      await expect(f.runtime.readToolResult(unresolved.settled[0]!)).rejects.toMatchObject({ code: "EFFECT_RESULT_UNAVAILABLE" });
      writeFileSync(target, "pre-state");
      await expect(f.invoke("extensions.write", next)).rejects.toThrow("requires explicit B3 settlement");
      const settled = await f.runtime.settlePendingIntents(f.owner.logicalSessionId, "settle-explicitly-corrected");
      expect(settled.uncertain).toBe(0);
      expect(readFileSync(target, "utf8")).toBe(first.content);
      const completed = fileIdentity(target);
      expect(await f.direct("write", "uncertain-now-historical", first)).toMatchObject({ content: [{ type: "text" }] });
      expect(fileIdentity(target)).toEqual(completed);
      await f.invoke("pi.write", next);
      expect(readFileSync(join(f.workspaceRoot, "next.txt"), "utf8")).toBe(next.content);
      expect(await f.ledger()).toHaveLength(2);
    } finally { await f.close(); }
  });

  it("labels adapter originals without inventing timestamps, preserves original alias metadata and refuses orphan parents/forged hashes", async () => {
    const args = { path: "file.txt", content: "original adapter result" };
    const f = realRecordsFixture(provisionRecords([{ id: "write", tool: "write", args }]));
    try {
      const original = await f.direct("write", "direct-original", args);
      await f.direct("write", "fresh-direct-alias", args);
      await f.invoke("extensions.write", args, "interrupted-outer-js");
      const { settled, uncertain } = await f.runtime.settlePendingIntents(f.owner.logicalSessionId, "reader-generation");
      expect(uncertain).toBe(0); expect(typeof uncertain).toBe("number");
      expect(settled).toHaveLength(3);
      const direct = settled.find(row => row.toolCallId === "direct-original")!;
      const alias = settled.find(row => row.toolCallId === "fresh-direct-alias")!;
      const nested = settled.find(row => row.toolCallId !== direct.toolCallId && row.toolCallId !== alias.toolCallId)!;
      const read = await f.runtime.readToolResult(direct);
      expect(read).toEqual({ source: "adapter-original", postHook: false, message: {
        role: "toolResult", toolCallId: "direct-original", toolName: "write", content: original.content,
        details: original.details, isError: false,
      } });
      expect(read.message).not.toHaveProperty("timestamp");
      expect(await f.runtime.readToolResult(alias)).toEqual(read);
      const record = (await f.ledger())[0]!;
      const saved = record.transitions.find(row => row.state === "RESULT")!;
      expect(direct.resultBytesHash).toBe(sha256Bytes(canonicalEncode(saved.result)));
      await expect(f.runtime.readToolResult({ ...direct, resultBytesHash: sha256Bytes("forged hash") })).rejects.toMatchObject({ code: "EFFECT_RESULT_UNAVAILABLE" });
      await expect(f.runtime.readToolResult(nested)).rejects.toMatchObject({ code: "EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR" });
      await expect(f.runtime.readToolResult({ ...direct, toolCallId: "interrupted-outer-js" })).rejects.toMatchObject({ code: "EFFECT_CALL_ALIAS_MISMATCH" });
      const extended = await f.records.reconcilePending("labelled-parent-repairs");
      expect(extended.repairs).toContainEqual(expect.objectContaining({
        key: direct.key, toolCallId: nested.toolCallId, parentToolCallId: "interrupted-outer-js", outcome: "committed",
      }));
      expect(extended.repairs.some(row => row.toolCallId === "interrupted-outer-js")).toBe(false);
      expect(extended.settled.some(row => row.toolCallId === "interrupted-outer-js")).toBe(false);
    } finally { await f.close(); }
  });

  it("distinguishes a supplied native-reader receipt from adapter-original fallback (receipt probe, not B6)", async () => {
    const args = { path: "file.txt", content: "native-reader probe" };
    let nativeAvailable = false;
    const f = realRecordsFixture(provisionRecords([{ id: "write", tool: "write", args }]), {
      async readNativeToolResult(effect) {
        if (!nativeAvailable) return undefined;
        // A deliberately patched envelope whose hash differs from the adapter
        // original. Only the injected host port claims its native association.
        return { role: "toolResult", toolCallId: effect.toolCallId, toolName: "write", isError: false,
          content: [{ type: "text", text: "post-hook native receipt probe" }], details: { nativeProbe: true }, timestamp: 123_456 };
      },
    });
    try {
      const original = await f.direct("write", "native-reader-original", args);
      await f.direct("write", "native-reader-alias", args);
      const fallbackEffects = (await f.runtime.settlePendingIntents(f.owner.logicalSessionId, "native-reader-generation")).settled;
      expect(fallbackEffects).toHaveLength(2);
      const fallbackByCall = new Map(fallbackEffects.map(effect => [effect.toolCallId, effect]));
      const fallback = await f.runtime.readToolResult(fallbackByCall.get("native-reader-original")!);
      expect(fallback).toMatchObject({ source: "adapter-original", postHook: false, message: {
        role: "toolResult", toolCallId: "native-reader-original", toolName: "write", isError: false,
        content: original.content, details: original.details,
      } });
      expect(fallback.message).not.toHaveProperty("timestamp");
      expect(await f.runtime.readToolResult(fallbackByCall.get("native-reader-alias")!)).toEqual(fallback);

      nativeAvailable = true;
      for (const effect of fallbackEffects) {
        await expect(f.runtime.readToolResult(effect)).rejects.toMatchObject({ code: "EFFECT_RESULT_UNAVAILABLE" });
      }

      // A fresh settlement must project a native hash independently for each
      // physical alias; the earlier canonical adapter hash cannot authorize it.
      const nativeEffects = (await f.runtime.settlePendingIntents(f.owner.logicalSessionId, "native-reader-generation")).settled;
      expect(nativeEffects).toHaveLength(2);
      for (const effect of nativeEffects) {
        const read = await f.runtime.readToolResult(effect);
        expect(read).toMatchObject({ source: "native-entry", postHook: true, message: {
          role: "toolResult", toolCallId: effect.toolCallId, toolName: "write", isError: false,
          content: [{ type: "text", text: "post-hook native receipt probe" }], details: { nativeProbe: true }, timestamp: 123_456,
        } });
        expect(effect.resultBytesHash).toBe(sha256Bytes(JSON.stringify(read.message)));
        expect(effect.resultBytesHash).not.toBe(fallbackByCall.get(effect.toolCallId)!.resultBytesHash);
        expect(sha256Bytes(canonicalEncode(read.message))).not.toBe(effect.resultBytesHash);
      }
      expect(new Set(nativeEffects.map(effect => effect.resultBytesHash)).size).toBe(2);
      await expect(f.runtime.readToolResult({ ...nativeEffects[0]!, resultBytesHash: sha256Bytes("forged raw original") })).rejects.toMatchObject({ code: "EFFECT_RESULT_UNAVAILABLE" });
    } finally { await f.close(); }
  });
});

// Actual source runtime + concrete EffectStore bridge in a separate Bun process.
// No bespoke intent JSON, fabricated durability marker, ordinary Pi writer, or
// regenerated edit preparation is used. The three selected callbacks originate
// at the real fsynced INTENT, destination barrier, and fsynced RESULT boundaries.
const RECORD_PROCESS_CHILD = `
import { createSyntheticSourceInfo } from '@earendil-works/pi-coding-agent';
import { createCertifiedEffectRecordPort, createCertifiedEffectRuntime, bindCertifiedEffectRuntime } from ${JSON.stringify(resolve("src/effects/extension.ts"))};
import { EffectStore } from ${JSON.stringify(resolve("src/effects/store.ts"))};
import { createEffectFailpoints } from ${JSON.stringify(resolve("src/effects/failpoints.ts"))};
import { CapturedToolCatalog } from ${JSON.stringify(resolve("src/capture/catalog.ts"))};
import { CapturedToolsProvider } from ${JSON.stringify(resolve("src/providers/captured-tools-provider.ts"))};
import { PiToolsProvider } from ${JSON.stringify(resolve("src/providers/pi-tools-provider.ts"))};
import { ActionRegistry } from ${JSON.stringify(resolve("src/core/action-registry.ts"))};
import { DEFAULT_FABRIC_CONFIG } from ${JSON.stringify(resolve("src/config.ts"))};
const [serialized, selectedPoint] = process.argv.slice(1);
const packet = JSON.parse(serialized);
const owner = { ...packet.owner, signal: new AbortController().signal,
  assertLive() { /* explicitly a host owner/exclusion capability probe, not B3 */ } };
const store = new EffectStore(owner);
const selected = new Map(); const handlers = new Map();
const context = { cwd: owner.workspaceRoot };
const runner = {
  createContext: () => context, getActiveTools: () => [...selected.keys()],
  emit: async () => {}, emitToolResult: async () => undefined,
  emitToolCall: async event => { await handlers.get('tool_call')?.(event); return undefined; },
};
const catalog = new CapturedToolCatalog();
const captured = new CapturedToolsProvider(catalog);
const pi = new PiToolsProvider(owner.workspaceRoot, catalog, captured);
const registry = new ActionRegistry(); registry.register(pi); registry.register(captured);
const failpoint = createEffectFailpoints({
  attemptFile: packet.attemptFile,
  identity: { logicalSessionId: owner.logicalSessionId, podUID: owner.attempt.podUID,
    bootId: owner.attempt.bootId, ownerIncarnation: owner.attempt.ownerIncarnation },
  points: [selectedPoint],
  onReached: receipt => new Promise((resolve, reject) => {
    process.stdout.write(JSON.stringify(receipt) + '\\n', error => error ? reject(error) : resolve());
  }),
  pause: async () => {
    // Keep the blocked child alive. ONLY the external test process sends SIGKILL.
    process.stdin.resume(); await new Promise(() => {});
  },
});
const records = createCertifiedEffectRecordPort({ owner, store, failpoint,
  admissionForCall: () => ({ acceptedInput: packet.acceptedInput, nativeAssistantEntryId: 'assistant-durability-RECEIPT-PROBE-NOT-B6' }),
  confirmNativeAssistant: async () => { /* labelled receipt probe; no native post-append proof */ },
  assertManifestConstraints: () => {},
});
const runtime = createCertifiedEffectRuntime({ logicalSessionId: owner.logicalSessionId,
  workspaceRoot: owner.workspaceRoot, records,
  shellProfile: { kind: 'read-sha256-v1', path: 'file.txt', sha256: '0'.repeat(64) },
  assertLive: () => owner.assertLive('new-intent'), assertExecutionProfile() {},
  getToolDefinition: name => selected.get(name), providers: { pi, captured },
});
runtime.register({
  registerTool: definition => selected.set(definition.name, definition),
  on: (name, handler) => { handlers.set(name, handler); return () => handlers.delete(name); },
});
catalog.replace([...selected.values()].map(definition => ({ definition,
  sourceInfo: createSyntheticSourceInfo('/extensions/record-runtime-receipt-probe.ts', {source:'test'}),
})), runner, DEFAULT_FABRIC_CONFIG.capture, '/extensions/fabric.ts');
bindCertifiedEffectRuntime(runtime, { registry, piTools: pi, capturedTools: captured });
await registry.invoke('extensions.edit', packet.args, {
  cwd: owner.workspaceRoot, parentToolCallId: 'crash-parent', nestedToolCallId: 'unused-registry-replaces-this',
  extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 100000,
});
throw new Error('real selected bridge failpoint did not pause');
`;

async function killRecordRuntimeAt(storage: RecordProvision, args: Record<string, unknown>, point: EffectFailpointName) {
  const owner = receiptProbeOwner(storage, "killed-old-owner");
  const { signal: _signal, assertLive: _assertLive, ...serializedOwner } = owner;
  const attemptFile = join(storage.root, "raw-fsynced-attempts.jsonl");
  const child = spawn("bun", ["--eval", RECORD_PROCESS_CHILD, JSON.stringify({
    owner: serializedOwner, acceptedInput: storage.acceptedInput, args, attemptFile,
  }), point], { cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.setEncoding("utf8").on("data", (text: string) => { stderr = (stderr + text).slice(-8_192); });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(done => {
    child.once("close", (code, signal) => done({ code, signal }));
  });
  const lines = createInterface({ input: child.stdout });
  try {
    const reached = new Promise<EffectFailpointReceipt>((done, reject) => {
      lines.once("line", line => {
        try { done(JSON.parse(line) as EffectFailpointReceipt); } catch (error) { reject(error); }
      });
      child.once("error", reject);
      child.once("exit", (code, signal) => reject(new Error(`Bun exited before ${point}: ${code}/${signal}: ${stderr}`)));
    });
    const receipt = await boundedEvent(reached, 15_000, `Bun real ${point} stdout receipt`);
    expect(receipt).toMatchObject({ format: "fabric.effect-failpoint.v1", point,
      logicalSessionId: owner.logicalSessionId, podUID: owner.attempt.podUID,
      bootId: owner.attempt.bootId, ownerIncarnation: owner.attempt.ownerIncarnation,
      details: { parentToolCallId: "crash-parent" },
    });
    expect(receipt.key).toMatch(/^[a-f0-9]{64}$/);
    expect(receipt.toolCallId).toBeTruthy();
    expect(JSON.parse(readFileSync(attemptFile, "utf8").trim())).toEqual(receipt);
    expect(statSync(attemptFile).mode & 0o777).toBe(0o600);
    expect(child.kill("SIGKILL")).toBe(true);
    expect(await boundedEvent(closed, 5_000, "owned killed Bun close")).toEqual({ code: null, signal: "SIGKILL" });
    return { receipt, owner, attemptFile };
  } finally {
    lines.close(); child.stdin.destroy();
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    // Every exit/error/assertion path waits for this exact owned child. The only
    // timer is a finite safety net for a missing OS close event, never a sleep.
    await boundedEvent(closed, 5_000, "owned Bun final close");
  }
}

describe("real bridge record-level SIGKILL cuts (not B3/B5/B6/native-CLI/power-loss qualification)", () => {
  it.each(["intent-durable", "destination-committed", "result-durable"] as const)("settles immutable stored edit data after external SIGKILL at %s and replays without rename", async point => {
    const args = { path: "file.txt", edits: [{ oldText: "old", newText: "stored successor bytes" }] };
    const storage = provisionRecords([{ id: "edit-once", tool: "edit", args }]);
    const target = join(storage.workspaceRoot, "file.txt");
    writeFileSync(target, "old"); chmodSync(target, 0o640);
    const originalIdentity = fileIdentity(target);
    const { receipt, owner: oldOwner } = await killRecordRuntimeAt(storage, args, point);
    // The old owner is a separate now-confirmed-SIGKILLed process. The fresh
    // successor runtime/store below can only observe its retained ledger; it has
    // no old in-memory intent, prepared arguments or operation tickets.
    let permitNewAdmission = false;
    const points: EffectFailpointName[] = [];
    const f = realRecordsFixture(storage, {
      incarnation: "fresh-successor",
      admissionForCall() {
        if (!permitNewAdmission) throw new Error("recovery tried to regenerate/admit edit arguments");
        return { acceptedInput: storage.acceptedInput, nativeAssistantEntryId: "successor-assistant-RECEIPT-PROBE-NOT-B6" };
      },
      failpoint(nextPoint) { points.push(nextPoint); },
    });
    const prepare = vi.spyOn(fileEffects, "prepareFileEffect");
    try {
      const before = (await f.ledger())[0]!;
      expect(before.intent.key).toBe(receipt.key);
      expect(before.intent.tool).toMatchObject({ originalToolCallId: receipt.toolCallId, parentToolCallId: "crash-parent",
        nativeAssistantEntryId: "assistant-durability-RECEIPT-PROBE-NOT-B6" });
      expect(before.intent.originAttempt).toEqual(oldOwner.attempt);
      expect(f.owner.attempt).not.toEqual(oldOwner.attempt);
      expect(before.intent.adapterData).toMatchObject({ format: "fabric-file-effect-v1", tool: "edit", replacementCount: 1,
        desiredBase64: Buffer.from("stored successor bytes").toString("base64"),
        pre: { sha256: sha256Bytes("old"), mode: 0o640 }, post: { sha256: sha256Bytes("stored successor bytes"), mode: 0o640 },
      });
      expect(before.transitions.some(row => row.state === "RESULT")).toBe(point === "result-durable");
      expect(readFileSync(target, "utf8")).toBe(point === "intent-durable" ? "old" : "stored successor bytes");
      const cutIdentity = fileIdentity(target);
      if (point === "intent-durable") expect(cutIdentity).toEqual(originalIdentity);
      const settled = await f.runtime.settlePendingIntents(f.owner.logicalSessionId, `successor-${point}`);
      expect(settled.uncertain).toBe(0);
      expect(settled.settled).toEqual([{ key: receipt.key, toolCallId: receipt.toolCallId, outcome: "committed", resultBytesHash: expect.stringMatching(/^[a-f0-9]{64}$/) }]);
      expect(f.admissionForCall).not.toHaveBeenCalled();
      expect(f.confirmNativeAssistant).not.toHaveBeenCalled();
      expect(f.assertManifestConstraints).not.toHaveBeenCalled();
      expect(prepare).not.toHaveBeenCalled();
      expect(readFileSync(target, "utf8")).toBe("stored successor bytes");
      expect(statSync(target).mode & 0o7777).toBe(0o640);
      if (point !== "intent-durable") expect(fileIdentity(target)).toEqual(cutIdentity);
      expect(points.filter(row => row === "file-renamed")).toHaveLength(point === "intent-durable" ? 1 : 0);
      const after = (await f.ledger())[0]!;
      expect(after.intent).toEqual(before.intent);
      expect(after.intentDigest).toBe(before.intentDigest);
      expect(after.transitions.filter(row => row.state === "RESULT")).toHaveLength(1);
      if (point === "result-durable") expect(after.transitions).toEqual(before.transitions);
      else expect(after.transitions.find(row => row.state === "RESULT")?.attempt).toEqual(f.owner.attempt);
      const completed = fileIdentity(target); const completedPoints = [...points];
      permitNewAdmission = true;
      // A changed regenerated model request is a conflict, never the bytes used
      // during settlement. The exact historical request then gets a fresh alias.
      const regenerated = { path: "file.txt", edits: [{ oldText: "old", newText: "WRONG regenerated bytes" }] };
      await expect(f.direct("edit", "changed-successor-direct", regenerated)).rejects.toMatchObject({ code: "EFFECT_SLOT_CONFLICT" });
      await expect(f.invoke("pi.edit", regenerated, "regenerated-parent")).rejects.toThrow("Changed request for a bound effect slot");
      expect(await f.invoke("extensions.edit", args, "successor-replay-parent")).toMatchObject({ text: "Successfully replaced 1 block(s) in file.txt.", isError: false });
      expect(prepare).not.toHaveBeenCalled();
      expect(points).toEqual(completedPoints);
      expect(fileIdentity(target)).toEqual(completed);
      expect(readFileSync(target, "utf8")).toBe("stored successor bytes");
      const replayed = (await f.ledger())[0]!;
      expect(replayed.intentDigest).toBe(before.intentDigest);
      expect(replayed.transitions.filter(row => row.state === "RESULT")).toEqual(after.transitions.filter(row => row.state === "RESULT"));
      expect(replayed.transitions.at(-1)).toMatchObject({ state: "ALIAS", parentToolCallId: "successor-replay-parent", attempt: f.owner.attempt });
      const all = await f.runtime.settlePendingIntents(f.owner.logicalSessionId, `successor-post-replay-${point}`);
      expect(all.settled).toHaveLength(2); expect(all.uncertain).toBe(0);
      await expect(f.runtime.readToolResult(all.settled[0]!)).rejects.toMatchObject({ code: "EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR" });
      expect(all.settled.some(row => row.toolCallId === "crash-parent" || row.toolCallId === "successor-replay-parent")).toBe(false);
    } finally { prepare.mockRestore(); await f.close(); }
  }, 30_000);
});
