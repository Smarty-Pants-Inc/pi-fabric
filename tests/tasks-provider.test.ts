import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";
import { ShellEventInbox } from "../src/core/shell-inbox.js";
import { ResultConsumption } from "../src/result-consumption.js";
import { createMainExecutionCeilingError } from "../src/async-settlement.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";
import { captureRuntimeDeadline } from "./helpers/early-runtime-deadline.js";
import { captureMontyTransport } from "./helpers/monty-transport.js";
const stores: FabricShellJobStore[] = [];
const context = {} as FabricInvocationContext;
afterEach(async () => { for (const store of stores.splice(0)) await store.close(); });
const setup = () => { const store = new FabricShellJobStore(); stores.push(store); return { store, provider: new TasksProvider(store) }; };

const pendingInbox = (store: FabricShellJobStore) => {
  const handlers = new Map<string, (...args: any[]) => unknown>();
  const sendMessage = vi.fn();
  const inboxContext = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
  const inbox = new ShellEventInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, inboxContext, store);
  return { inbox, sendMessage, boundary: () => handlers.get("turn_end")?.({ message: { stopReason: "stop" } }, inboxContext) };
};

describe("shell task observation receipts", () => {
  it.each((["get", "wait"] as const).flatMap(action => (["output read", "publication"] as const).map(stage => [action, stage] as const)))(
    "keeps exactly one pending completed-spilled-task notification when tasks.%s expires during %s", async (action, stage) => {
      const { store, provider } = setup();
      const { inbox, sendMessage, boundary } = pendingInbox(store);
      const job = store.begin("bash", "completed spilled output"); job.spill(); job.append(Buffer.from("retained evidence")); await job.finish(0);
      expect(inbox.pendingCount()).toBe(1);
      const registry = new ActionRegistry(); registry.register(provider);
      const ceiling = createMainExecutionCeilingError(700);
      let expired = false;
      const output = job.outputText.bind(job);
      const read = vi.spyOn(job, "outputText").mockImplementation(async () => {
        const value = await output();
        if (stage === "output read") expired = true;
        return value;
      });
      try {
        await expect(registry.invoke(`tasks.${action}`, { id: job.id }, {
          ...context, cwd: process.cwd(), parentToolCallId: "task-rejected", nestedToolCallId: "task-nested", update() {},
          extensionContext: {} as ExtensionContext, audits: [], maxResultChars: 100_000, async approve() {},
          checkExecutionBudget() { if (expired) throw ceiling; },
          observeInvocation(event) { if (stage === "publication" && event.type === "call_end" && event.success) expired = true; },
        })).rejects.toBe(ceiling);
        expect(job.info().unread).toBe(true); expect(inbox.pendingCount()).toBe(1);
        boundary(); boundary();
        expect(sendMessage).toHaveBeenCalledOnce(); expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([job.id]);
      } finally { read.mockRestore(); inbox.close(); await registry.close(); }
    },
  );

  it.each((["get", "wait"] as const).flatMap(action => (["quickjs", "node-process", "cpython", "monty"] as const).map(backend => [action, backend] as const)))(
    "keeps exactly one pending completed-spilled-task notification when tasks.%s expires during %s encoding", async (action, backend) => {
      vi.stubEnv("PI_FABRIC_PARENT_RUN", ""); vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
      const { store, provider } = setup();
      const { inbox, sendMessage, boundary } = pendingInbox(store);
      const job = store.begin("bash", "completed spilled encoding"); job.spill(); job.append(Buffer.from("retained evidence")); await job.finish(0);
      expect(inbox.pendingCount()).toBe(1);
      const registry = new ActionRegistry(); registry.register(provider);
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      const python = backend === "cpython" || backend === "monty";
      if (python) { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
      else config.executor.runtime = backend;
      config.executor.mainMaxTimeoutMs = 60_000;
      const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
      const invoke = registry.invoke.bind(registry);
      const encode = vi.fn((deadlineAt: number) => { clock.mockReturnValue(deadlineAt + 1); return "late"; });
      const encoding = vi.spyOn(registry, "invoke").mockImplementation(async (ref, args, ctx) => {
        const value = await invoke(ref, args, ctx);
        Object.defineProperty(value, "encoding", { enumerable: true, get: () => encode(ctx.mainDeadlineAt!) });
        return value;
      });
      try {
        const result = await new FabricExecutionService(registry, config).execute({
          code: python ? `return await tools.call(ref="tasks.${action}", args={"id": ${JSON.stringify(job.id)}})` : `return tools.call({ref:"tasks.${action}",args:{id:${JSON.stringify(job.id)}}});`,
          context: { cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "task-receipt-main" } } as unknown as ExtensionContext,
          signal: undefined, parentToolCallId: `task-encoding-${backend}-${action}`, onPartial() {},
        });
        expect(encode).toHaveBeenCalled(); expect(result.error).toMatch(/MainExecutionCeilingError/);
        expect(job.info().unread).toBe(true); expect(inbox.pendingCount()).toBe(1);
        boundary(); boundary();
        expect(sendMessage).toHaveBeenCalledOnce(); expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([job.id]);
      } finally { encoding.mockRestore(); clock.mockRestore(); inbox.close(); await registry.close(); vi.unstubAllEnvs(); }
    }, 30_000,
  );

  it.each(["get", "wait"] as const)("acknowledges a successfully delivered tasks.%s result once, not on admission", async action => {
    const { store, provider } = setup(); const { inbox } = pendingInbox(store);
    const job = store.begin("bash", "completed receipt"); job.spill(); await job.finish(0);
    const receipts = [new ResultConsumption(), new ResultConsumption()];
    const acknowledged = vi.fn(); store.subscribe(event => { if (event.type === "acknowledged") acknowledged(); });
    try {
      for (const receipt of receipts) await provider.invoke(action, { id: job.id }, { ...context, deferResultConsumption: receipt.defer });
      expect(acknowledged).not.toHaveBeenCalled(); expect(inbox.pendingCount()).toBe(1);
      for (const receipt of receipts) { receipt.commit(); receipt.commit(); }
      expect(acknowledged).toHaveBeenCalledOnce(); expect(job.info().unread).toBe(false); expect(inbox.pendingCount()).toBe(0);
    } finally { inbox.close(); }
  });

  it.each(["event", "finish"] as const)("does not consume a newer shell %s version at delayed delivery", async version => {
    vi.useFakeTimers();
    const { store, provider } = setup(); const { inbox, boundary, sendMessage } = pendingInbox(store);
    const job = store.begin("bash", "live version", { monitor: { delivery: "wake", intervalMs: 1000, timeoutMs: 300000 } }); job.spill();
    job.append(Buffer.from("first event\n")); await vi.advanceTimersByTimeAsync(1000);
    const receipt = new ResultConsumption();
    try {
      await provider.invoke("get", { id: job.id }, { ...context, deferResultConsumption: receipt.defer });
      if (version === "event") { job.append(Buffer.from("new unseen event\n")); await vi.advanceTimersByTimeAsync(1000); }
      else await job.finish(0);
      receipt.commit();
      expect(job.info().unread).toBe(true); expect(inbox.pendingCount()).toBe(1);
      boundary(); boundary(); expect(sendMessage).toHaveBeenCalledOnce();
    } finally { inbox.close(); vi.useRealTimers(); }
  });
});

describe.skipIf(process.platform !== "linux")("Monty returned-but-unadmitted spilled-task observations", () => {
  it.each((["get", "wait"] as const).flatMap(action =>
    (["expiry", "closure", "normal ack", "ack then expiry"] as const).map(outcome => [action, outcome] as const)),
  )("completed spilled tasks.%s after Monty return before admission: %s", async (action, outcome) => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", ""); vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const { store, provider } = setup();
    const { inbox, sendMessage, boundary } = pendingInbox(store);
    const job = store.begin("bash", "Monty admission spilled output"); job.spill(); job.append(Buffer.from("retained evidence")); await job.finish(0);
    const acknowledged = vi.fn(); store.subscribe(event => { if (event.type === "acknowledged") acknowledged(); });
    const registry = new ActionRegistry(); registry.register(provider);
    let countAtReturn = -1;
    let deadlineAt = 0;
    let ackCount = 0;
    const timer = captureRuntimeDeadline("monty");
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
    const control = await captureMontyTransport(() => { countAtReturn = acknowledged.mock.calls.length; }, ack => {
      ackCount++;
      ack(1, 0); ack(0, 1);
      expect(acknowledged, "uncorrelated confirmations").not.toHaveBeenCalled();
      ack(); ack();
      expect(acknowledged).toHaveBeenCalledOnce();
      if (outcome === "ack then expiry") timer.fireAt(deadlineAt);
    }, outcome === "expiry" || outcome === "closure");
    const invoke = registry.invoke.bind(registry);
    const invocation = vi.spyOn(registry, "invoke").mockImplementation(async (ref, args, ctx) => {
      deadlineAt = ctx.mainDeadlineAt!;
      return invoke(ref, args, ctx);
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.kernel = "python"; config.executor.pythonRuntime = "monty"; config.executor.mainMaxTimeoutMs = 60_000;
    const controller = new AbortController();
    const execution = new FabricExecutionService(registry, config).execute({
      code: `return await tools.call(ref="tasks.${action}", args={"id": ${JSON.stringify(job.id)}})`,
      context: { cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "monty-task-admission-main" } } as unknown as ExtensionContext,
      signal: controller.signal, parentToolCallId: `monty-task-${action}-${outcome}`, onPartial() {},
    });
    try {
      await Promise.race([control.responseReturned, execution.then(result => { throw new Error(`Ended before callback returned: ${result.error}`); })]);
      expect(countAtReturn, "callback return is not guest admission").toBe(0);
      if (outcome === "expiry") timer.fireAt(deadlineAt);
      if (outcome === "closure") control.closeReceiver();
      const result = await execution;
      expect(result.success).toBe(outcome === "normal ack");
      if (outcome === "expiry" || outcome === "ack then expiry") expect(result.error).toMatch(/MainExecutionCeilingError/);
      control.staleAck();
      const admitted = outcome === "normal ack" || outcome === "ack then expiry";
      expect(acknowledged).toHaveBeenCalledTimes(admitted ? 1 : 0);
      expect(ackCount).toBe(admitted ? 1 : 0);
      expect(job.info().unread).toBe(!admitted); expect(inbox.pendingCount()).toBe(admitted ? 0 : 1);
      boundary(); boundary(); expect(sendMessage).toHaveBeenCalledTimes(admitted ? 0 : 1);
      if (!admitted) expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([job.id]);
    } finally {
      controller.abort(); control.closeReceiver(); await execution;
      invocation.mockRestore(); control.restore(); clock.mockRestore(); timer.restore();
      inbox.close(); await registry.close(); vi.unstubAllEnvs();
    }
  }, 45_000);
});

describe("tasks provider", () => {
  it("leaves a detached task alive and its completion unread at the Main program ceiling", async () => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const { store, provider } = setup();
    const job = store.begin("bash", "detached work");
    job.spill();
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.mainMaxTimeoutMs = 50;
    // Observe the actual waiter subscription, not merely provider dispatch.
    const subscribed = vi.spyOn(store, "subscribe");
    try {
      const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
        code: `return tools.call({ ref: "tasks.wait", args: { id: ${JSON.stringify(job.id)}, timeoutMs: 300_000 } });`,
        signal, parentToolCallId: "main-task-ceiling",
        context: { cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
        onPartial() {},
      }), () => subscribed.mock.calls.length > 0);
      expect(result.error).toMatch(/MainExecutionCeilingError.*Main ceiling hit/);
      expect(job.abort.signal.aborted).toBe(false);
      expect(job.info().status).toBe("spilled");
      await job.finish(0);
      expect(job.info()).toMatchObject({ status: "exited", unread: true });
    } finally { vi.unstubAllEnvs(); }
  });
  it("waits for an exit and returns bounded evidence, not a success claim", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "work"); job.spill();
    const result = provider.invoke("wait", { id: job.id, timeoutMs: 1000 }, context);
    job.append(Buffer.alloc(16000, 97)); job.append(Buffer.from("failure evidence")); await job.finish(7);
    expect(await result).toMatchObject({ timedOut: false, task: { status: "failed", exitCode: 7, unread: false }, output: expect.stringContaining("failure evidence") });
    expect(await provider.invoke("wait", { id: job.id }, context)).toMatchObject({ timedOut: false, task: { status: "failed" } });
    expect(((await result) as {output:string}).output.length).toBeLessThan(8100);
  });
  it("timeouts and cancellation affect only the waiter, and release subscriptions", async () => {
    const { store, provider } = setup(); const job = store.begin("bash", "work");
    const unsubscribed = vi.fn(); const subscribe = store.subscribe.bind(store);
    const subscriptions = vi.spyOn(store, "subscribe").mockImplementation(listener => { const remove = subscribe(listener); return () => { unsubscribed(); remove(); }; });
    expect(await provider.invoke("wait", { id: job.id, timeoutMs: 1 }, context)).toMatchObject({ timedOut: true, task: { status: "running" } });
    const controller = new AbortController();
    const pending = provider.invoke("wait", { id: job.id }, { ...context, signal: controller.signal });
    const rejected = expect(pending).rejects.toThrow("caller stopped");
    await Promise.resolve();
    controller.abort(new Error("caller stopped")); await rejected;
    expect(job.abort.signal.aborted).toBe(false);
    // The pre-aborted call need not subscribe; every actual subscription is released.
    expect(unsubscribed.mock.calls.length).toBe(subscriptions.mock.calls.length);
  });
  it("watches retained batches with a loss cursor, then waits for new events or exit", async () => {
    vi.useFakeTimers();
    try {
      const { store, provider } = setup();
      const job = store.begin("bash", "watch", { monitor: { delivery: "ui", match: "READY:", intervalMs: 1000, timeoutMs: 300000 } });
      job.append(Buffer.from("ignored\nREADY: old\n")); await vi.advanceTimersByTimeAsync(1000);
      job.append(Buffer.from(Array.from({ length: 12 }, (_, i) => `READY: ${i}\n`).join(""))); await vi.advanceTimersByTimeAsync(1000);
      expect(await provider.invoke("watch", { id: job.id }, context)).toMatchObject({ reason: "event", nextCursor: 13, omitted: 5, lines: Array.from({ length: 8 }, (_, i) => `READY: ${i + 4}`) });
      expect(await provider.invoke("watch", { id: job.id, after: 11 }, context)).toMatchObject({ omitted: 0, lines: ["READY: 10", "READY: 11"], nextCursor: 13 });
      const next = provider.invoke("watch", { id: job.id, after: 13 }, context);
      job.append(Buffer.from("READY: fresh\n")); await vi.advanceTimersByTimeAsync(1000);
      expect(await next).toMatchObject({ reason: "event", nextCursor: 14, lines: ["READY: fresh"], omitted: 0 });
      const timeout = provider.invoke("watch", { id: job.id, after: 14, timeoutMs: 10 }, context);
      await vi.advanceTimersByTimeAsync(10);
      expect(await timeout).toMatchObject({ reason: "timeout", nextCursor: 14, lines: [], omitted: 0 });
      expect(job.abort.signal.aborted).toBe(false);
      const end = provider.invoke("watch", { id: job.id, after: 14 }, context);
      await job.finish(0);
      expect(await end).toMatchObject({ reason: "finished", nextCursor: 14, lines: [] });
    } finally { vi.useRealTimers(); }
  });
  it("cancels an in-flight watch without stopping its monitor or consuming an event", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "watch", { monitor: { delivery: "ui", intervalMs: 1000, timeoutMs: 300000 } });
    const controller = new AbortController();
    const pending = expect(provider.invoke("watch", { id: job.id }, { ...context, signal: controller.signal })).rejects.toThrow("watch cancelled");
    await Promise.resolve();
    controller.abort(new Error("watch cancelled")); await pending;
    expect(job.abort.signal.aborted).toBe(false); expect(job.eventCount).toBe(0);
    job.append(Buffer.from("last event\n")); await job.finish(0);
    expect(await provider.invoke("watch", { id: job.id }, context)).toMatchObject({ reason: "event", lines: ["last event"], nextCursor: 1 });
  });
  it("rejects invalid controls, future cursors and watches without an opt-in monitor", async () => {
    const { store, provider } = setup(); const job = store.begin("bash", "work");
    for (const args of [{ timeoutMs: 0 }, { timeoutMs: 300001 }, { timeoutMs: 1.5 }, { extra: true }])
      await expect(provider.invoke("wait", { id: job.id, ...args }, context)).rejects.toThrow("Invalid tasks.wait");
    await expect(provider.invoke("watch", { id: job.id }, context)).rejects.toThrow("requires a task started with monitor");
    const monitor = store.begin("bash", "watch", { monitor: { delivery: "ui", intervalMs: 1000, timeoutMs: 300000 } });
    await expect(provider.invoke("watch", { id: monitor.id, after: 1 }, context)).rejects.toThrow("existing event cursor");
    await expect(provider.invoke("wait", { id: "another-session" }, context)).rejects.toThrow("Unknown shell task");
  });
  it("rejects pending waits/watches at store shutdown without leaking observers", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "work", { monitor: { delivery: "ui", intervalMs: 1000, timeoutMs: 300000 } });
    const pending = ["wait", "watch"].map(name => expect(provider.invoke(name, { id: job.id }, context)).rejects.toThrow("Shell job store is closed"));
    await Promise.resolve(); await store.close(); await Promise.all(pending);
  });

  it("rejects a ninth monitor before spawning another shell", async () => {
    const { store } = setup();
    for (let i = 0; i < 8; i++) store.begin("bash", "existing watch", { monitor: { delivery: "ui", intervalMs: 1000, timeoutMs: 300000 } });
    const native = new PiToolsProvider(process.cwd(), undefined, undefined, { shellJobs: store, powerShellToolDefinitionFactory: undefined });
    await expect(native.invoke("bash", { command: "echo forbidden", monitor: { delivery: "wake" } }, context)).rejects.toThrow("At most 8 monitors");
    expect(store.list()).toHaveLength(8);
  });
  it("registers discoverable list/get/wait/watch/stop contracts", async () => {
    const { provider } = setup();
    expect((await provider.list({})).map(d => d.name)).toEqual(["list", "get", "wait", "watch", "stop"]);
    expect(await provider.describe("stop")).toMatchObject({ risk: "execute", inputSchema: { additionalProperties: false } });
  });
  it("returns terminal metadata, bounded output, and acknowledges a consumed result", async () => {
    const { store, provider } = setup(); const events = vi.fn(); store.subscribe(events);
    const job = store.begin("bash", "test", { cwd: "/work", ownerId: "session" }); job.spill(); job.append(Buffer.alloc(16000, 97)); await job.finish(7);
    const result = await provider.invoke("get", { id: job.id }, context) as { task: any; output: string };
    expect(result.task).toMatchObject({ id: job.id, cwd: "/work", ownerId: "session", status: "failed", exitCode: 7, unread: false });
    expect(result.output.length).toBeLessThan(8100);
    expect(events.mock.calls.at(-1)![0].type).toBe("acknowledged");
  });
  it("stops only owned handles; another session cannot access or kill them", async () => {
    const a = setup(), b = setup(); const job = a.store.begin("bash", "sleep");
    await expect(b.provider.invoke("stop", { id: job.id }, context)).rejects.toThrow("Unknown shell task");
    expect(job.abort.signal.aborted).toBe(false);
    expect(await a.provider.invoke("stop", { id: job.id }, context)).toMatchObject({ stopped: true, task: { stopping: true } });
    expect(await a.provider.invoke("stop", { id: job.id }, context)).toMatchObject({ stopped: false });
  });
  it("isolates listener errors and closes jobs without emitting completion wakeups", async () => {
    const { store } = setup(); store.subscribe(() => { throw new Error("bad observer"); });
    const events = vi.fn(); store.subscribe(events);
    const job = store.begin("bash", "work"); job.spill();
    events.mockClear(); await store.close();
    expect(job.abort.signal.aborted).toBe(true); expect(events).not.toHaveBeenCalled();
  });
});
