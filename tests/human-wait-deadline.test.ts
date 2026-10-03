import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { availablePythonBackends } from "./fixtures/python-backends.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { HumanWaitDeadlinePause } from "../src/runtime/deadline-pause.js";

const registries: ActionRegistry[] = [];
const controllers: AbortController[] = [];
beforeEach(() => {
  // Only the parent's deadline clock is virtual. Child startup/IPC remain real;
  // tests advance time after a host-call handshake, never after a guessed delay.
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
});
afterEach(async () => {
  for (const controller of controllers.splice(0)) controller.abort();
  vi.useRealTimers();
  await Promise.all(registries.splice(0).map(registry => registry.close()));
});

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
type PendingCall = { name: string; answer(): void; fail(): void };
const fixture = (configure: (config: FabricConfig) => void) => {
  const registry = new ActionRegistry();
  registries.push(registry);
  const aborted: string[] = [];
  const calls: Array<ReturnType<typeof deferred<PendingCall>>> = [];
  const slot = (index: number) => calls[index] ??= deferred<PendingCall>();
  let invoked = 0;
  let observed = 0;
  const descriptor = {
    name: "ask",
    description: "human question stub",
    inputSchema: { type: "object", properties: {}, additionalProperties: true },
    risk: "read" as const,
  };
  registry.register({
    name: "extensions",
    description: "fake extensions",
    async list() { return [descriptor, { ...descriptor, name: "slow" }]; },
    async describe(name) { return name === "ask" || name === "slow" ? { ...descriptor, name } : undefined; },
    async invoke(name, _args, context) {
      const response = deferred<unknown>();
      const abort = () => { aborted.push(name); response.reject(new Error("aborted")); };
      context.signal?.addEventListener("abort", abort, { once: true });
      if (context.signal?.aborted) abort();
      slot(invoked++).resolve({
        name,
        answer: () => response.resolve({ answer: name }),
        fail: () => response.reject(new Error("question failed")),
      });
      try { return await response.promise; }
      finally { context.signal?.removeEventListener("abort", abort); }
    },
  });
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = true;
  config.approvals.read = "allow";
  configure(config);
  const service = new FabricExecutionService(registry, config);
  const controller = new AbortController();
  controllers.push(controller);
  const run = (code: string) => service.execute({
    code, signal: controller.signal, parentToolCallId: "human-wait",
    context: { cwd: process.cwd(), hasUI: false, sessionManager: {
      getSessionId: () => "human-wait-test", getSessionFile: () => undefined,
    } } as unknown as ExtensionContext,
    onPartial() {},
  });
  return { run, aborted, controller, nextCall: () => slot(observed++).promise };
};

describe("HumanWaitDeadlinePause", () => {
  it("suspends once, resumes once with the saved budget, and applies raised floors", () => {
    const calls: string[] = [];
    let remaining = 300;
    const pause = new HumanWaitDeadlinePause({
      remainingMs: () => remaining,
      suspend: () => calls.push("suspend"),
      resume: (ms) => calls.push(`resume:${ms}`),
    });
    pause.leave();
    expect(calls).toEqual([]);
    pause.enter();
    remaining = 5;
    pause.enter();
    expect(pause.paused).toBe(true);
    pause.leave();
    expect(calls).toEqual(["suspend"]);
    pause.raise(100);
    pause.leave();
    expect(pause.paused).toBe(false);
    expect(calls).toEqual(["suspend", "resume:300"]);
    pause.enter();
    pause.raise(1_000.7);
    pause.leave();
    expect(calls.at(-1)).toBe("resume:1000");
  });
});

describe("executor.humanWaitRefs config", () => {
  it("defaults to extensions.ask and normalizes exact refs", () => {
    expect(normalizeFabricConfig({}).executor.humanWaitRefs).toEqual(["extensions.ask"]);
    expect(normalizeFabricConfig({ executor: { humanWaitRefs: [] } }).executor.humanWaitRefs).toEqual([]);
    expect(normalizeFabricConfig({
      executor: { humanWaitRefs: [" extensions.ask ", "extensions.ask", "", 7, "mcp.q.ask"] },
    }).executor.humanWaitRefs).toEqual(["extensions.ask", "mcp.q.ask"]);
    expect(normalizeFabricConfig({ executor: { humanWaitRefs: "extensions.ask" } }).executor.humanWaitRefs)
      .toEqual(["extensions.ask"]);
  });
});

const runtimes = ["quickjs", "node-process", "monty", "cpython"] as const;
describe.each(runtimes)("%s human-wait deadline pause", (runtime) => {
  const python = runtime === "monty" || runtime === "cpython";
  const runTest = it.skipIf(python && !availablePythonBackends[runtime]);
  const configure = (config: FabricConfig): void => {
    config.executor = { ...normalizeFabricConfig({ executor: {
      ...(python ? { kernel: "python", pythonRuntime: runtime } : { runtime }),
      memoryLimitBytes: 256 * 1024 * 1024,
    } }).executor, timeoutMs: 1_000 };
  };
  const call = (name: string, generic = false) => python
    ? `await tools.call(ref="extensions.${name}", args={})`
    : generic ? `await tools.call({ ref: "extensions.${name}", args: {} })` : `await extensions.${name}({})`;

  runTest.each([false, true])("waits past the deadline (generic=%s)", async (generic) => {
    const { run, nextCall, aborted } = fixture(configure);
    const pending = run(`return ${call("ask", generic)}`);
    const question = await nextCall();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(aborted).toEqual([]);
    question.answer();
    expect(await pending).toMatchObject({ success: true, value: { answer: "ask" } });
  });

  runTest.each(["unlisted", "disabled"])("keeps the normal deadline when %s", async (mode) => {
    const { run, nextCall, aborted } = fixture(config => {
      configure(config);
      if (mode === "disabled") config.executor.humanWaitRefs = [];
    });
    const name = mode === "disabled" ? "ask" : "slow";
    const pending = run(`return ${call(name)}`);
    await nextCall();
    await vi.advanceTimersByTimeAsync(1_001);
    const result = await pending;
    expect(result.success).toBe(false);
    expect(result.error).toContain("timed out");
    expect(aborted).toContain(name);
  });

  runTest.each(["answer", "fail"] as const)("resumes the saved budget after %s", async (settle) => {
    const { run, nextCall } = fixture(configure);
    const code = python
      ? `try:\n    ${call("ask")}\nexcept Exception:\n    pass\nreturn ${call("slow")}`
      : `try { ${call("ask")}; } catch {} return ${call("slow")};`;
    const pending = run(code);
    const question = await nextCall();
    await vi.advanceTimersByTimeAsync(60_000);
    question[settle]();
    expect((await nextCall()).name).toBe("slow");
    await vi.advanceTimersByTimeAsync(1_001);
    const result = await pending;
    expect(result.success).toBe(false);
    expect(result.error).toContain("timed out");
  });

  runTest("cancels an entered human wait without waiting for a timer", async () => {
    const { run, nextCall, controller, aborted } = fixture(configure);
    const pending = run(`return ${call("ask")}`);
    await nextCall();
    controller.abort(new Error("user cancelled"));
    const result = await pending;
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/cancelled|abort/i);
    expect(aborted).toContain("ask");
  });

  runTest("remains paused until all overlapping questions settle", async () => {
    const { run, nextCall, aborted } = fixture(configure);
    const code = python
      ? 'return await asyncio.gather(tools.call(ref="extensions.ask", args={}), tools.call(ref="extensions.ask", args={}))'
      : 'return await Promise.all([extensions.ask({}), extensions.ask({})]);';
    const pending = run(code);
    const first = await nextCall();
    const second = await nextCall();
    first.answer();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(aborted).toEqual([]);
    second.answer();
    expect(await pending).toMatchObject({ success: true, value: [{ answer: "ask" }, { answer: "ask" }] });
  });
});
