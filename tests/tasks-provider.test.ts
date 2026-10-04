import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { SHELL_READ_MAX_BYTES } from "../src/core/shell-jobs.js";
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
      expect(await provider.invoke("watch", { id: job.id }, context)).toMatchObject({ reason: "event", nextCursor: 13, omitted: 0, losses: [], more: false, lines: ["READY: old", ...Array.from({ length: 12 }, (_, i) => `READY: ${i}`)] });
      // Agent-facing delivery keeps its eight newest previews; replay keeps every line.
      expect(job.info().lastEvent).toMatchObject({ omitted: 4, lines: Array.from({ length: 8 }, (_, i) => `READY: ${i + 4}`) });
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
  it("pages replay and discloses burst and eviction losses as cursor ranges", async () => {
    vi.useFakeTimers();
    try {
      const { store, provider } = setup();
      const job = store.begin("bash", "watch", { monitor: { delivery: "ui", intervalMs: 1000, timeoutMs: 300000 } });
      job.append(Buffer.from(Array.from({ length: 300 }, (_, i) => `line ${i}\n`).join(""))); await vi.advanceTimersByTimeAsync(1000);
      const first = await provider.invoke("watch", { id: job.id }, context);
      expect(first).toMatchObject({ reason: "event", losses: [{ after: 0, next: 44, reason: "burst" }], omitted: 44, more: true, nextCursor: 108 });
      expect((first as { lines: string[] }).lines).toEqual(Array.from({ length: 64 }, (_, i) => `line ${i + 44}`));
      const second = await provider.invoke("watch", { id: job.id, after: 108 }, context) as { lines: string[] };
      expect(second).toMatchObject({ omitted: 0, losses: [], more: true, nextCursor: 172 });
      expect(second.lines[0]).toBe("line 108");
      // The ring keeps the newest 256 positions; a lagging reader sees an eviction record.
      job.append(Buffer.from(Array.from({ length: 100 }, (_, i) => `more ${i}\n`).join(""))); await vi.advanceTimersByTimeAsync(1000);
      const lagging = await provider.invoke("watch", { id: job.id, after: 108 }, context) as { lines: string[]; losses: unknown[] };
      expect(lagging.losses).toEqual([{ after: 108, next: 144, reason: "evicted" }]);
      expect(lagging.lines[0]).toBe("line 144");
    } finally { vi.useRealTimers(); }
  });
  it("reads combined output by byte offset without splitting UTF-8, and discloses evicted bytes", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "work"); job.spill();
    const bytes = Buffer.from("héllo\n");
    job.append(bytes.subarray(0, 2));
    // The first byte of "é" is held back until its continuation arrives.
    expect(await provider.invoke("read", { id: job.id }, context)).toMatchObject({ offset: 0, bytes: 1, text: "h", next: 1, eof: false, omittedBytes: 0, stream: "output", state: "spilled" });
    job.append(bytes.subarray(2));
    expect(await provider.invoke("read", { id: job.id, offset: 1 }, context)).toMatchObject({ text: "éllo\n", next: 7 });
    expect(await provider.invoke("read", { id: job.id, offset: 0, encoding: "base64" }, context)).toMatchObject({ data: bytes.toString("base64"), next: 7 });
    await expect(provider.invoke("read", { id: job.id, offset: 8 }, context)).rejects.toThrow("past the task's output");
    job.append(Buffer.alloc(1024 * 1024, 97));
    const lagging = await provider.invoke("read", { id: job.id, offset: 0, max: 4 }, context);
    expect(lagging).toMatchObject({ offset: 7, omittedBytes: 7, bytes: 4 });
    await job.finish(0);
    // After exit a 32 KiB window stays readable at the same offsets.
    const tail = await provider.invoke("read", { id: job.id, offset: 0 }, context) as { offset: number; omittedBytes: number; next: number; eof: boolean };
    expect(tail).toMatchObject({ offset: 7 + 1024 * 1024 - 32 * 1024, eof: true, next: 7 + 1024 * 1024 });
  });
  it("long-polls a read for new bytes or exit, without stopping the task", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "work"); job.spill();
    const pending = provider.invoke("read", { id: job.id, offset: 0, waitMs: 5000 }, context);
    job.append(Buffer.from("ready\n"));
    expect(await pending).toMatchObject({ text: "ready\n", next: 6 });
    expect(await provider.invoke("read", { id: job.id, offset: 6, waitMs: 5 }, context)).toMatchObject({ bytes: 0, next: 6, eof: false });
    const ending = provider.invoke("read", { id: job.id, offset: 6, waitMs: 5000 }, context);
    await job.finish(0);
    expect(await ending).toMatchObject({ bytes: 0, eof: true, state: "exited" });
    expect(job.abort.signal.aborted).toBe(false);
  });
  it("watches any task for a literal chosen at watch time, by byte cursor", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "server"); job.spill();
    job.append(Buffer.from("booting\nlistening on :3000\nGET / 200\npartial READY"));
    const first = await provider.invoke("watch", { id: job.id, match: "listening" }, context) as { reason: string; lines: string[]; nextCursor: number };
    expect(first).toMatchObject({ reason: "event", lines: ["listening on :3000"], more: true });
    // The unterminated line is not consumed until it ends or the task exits.
    const idle = await provider.invoke("watch", { id: job.id, match: "READY", after: first.nextCursor, timeoutMs: 10 }, context) as { reason: string; nextCursor: number };
    expect(idle).toMatchObject({ reason: "timeout", lines: [] });
    const pending = provider.invoke("watch", { id: job.id, match: "READY", after: idle.nextCursor, timeoutMs: 5000 }, context);
    job.append(Buffer.from("\n"));
    expect(await pending).toMatchObject({ reason: "event", lines: ["partial READY"], more: false });
    await job.finish(0);
    const done = await provider.invoke("watch", { id: job.id, match: "nothing" }, context);
    expect(done).toMatchObject({ reason: "finished", lines: [], more: false });
    expect(job.abort.signal.aborted).toBe(false);
  });
  it("A21 keeps malformed UTF-8 line cursors in raw bytes and sees appended matches", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "invalid utf8"); job.spill();
    job.append(Buffer.from([0xff, 0x78, 0x0a]));
    const first = await provider.invoke("watch", { id: job.id, match: "x" }, context) as { nextCursor: number };
    expect(first).toMatchObject({ lines: ["�x"], nextCursor: 3, omittedBytes: 0, more: false });
    await expect(provider.invoke("watch", { id: job.id, match: "x", after: first.nextCursor, timeoutMs: 5 }, context))
      .resolves.toMatchObject({ lines: [], nextCursor: 3, reason: "timeout" });
    job.append(Buffer.from("xx\n"));
    await expect(provider.invoke("watch", { id: job.id, match: "x", after: first.nextCursor }, context))
      .resolves.toMatchObject({ lines: ["xx"], nextCursor: 6, omittedBytes: 0 });
  });
  it("A21 clips malformed UTF-8 pending lines by bytes, not replacement text length", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "invalid clipped utf8"); job.spill();
    job.append(Buffer.concat([Buffer.from([0xff]), Buffer.alloc(SHELL_READ_MAX_BYTES - 1, 120)]));
    const first = await provider.invoke("watch", { id: job.id, match: "x" }, context) as { nextCursor: number };
    expect(first).toMatchObject({ nextCursor: SHELL_READ_MAX_BYTES, more: false });
    job.append(Buffer.from("NEXT x\n"));
    await expect(provider.invoke("watch", { id: job.id, match: "NEXT", after: first.nextCursor }, context))
      .resolves.toMatchObject({ lines: ["NEXT x"], nextCursor: SHELL_READ_MAX_BYTES + 7, omittedBytes: 0 });
  });
  it("does not spin on incomplete UTF-8 tails or page-boundary multibyte output", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "utf8"); job.spill();
    const encoded = Buffer.from("READY: 😀\n");
    job.append(encoded.subarray(0, encoded.length - 2));
    await expect(provider.invoke("watch", { id: job.id, match: "READY", timeoutMs: 10 }, context))
      .resolves.toMatchObject({ reason: "timeout", lines: [] });
    const pending = provider.invoke("watch", { id: job.id, match: "READY", timeoutMs: 5000 }, context);
    job.append(encoded.subarray(encoded.length - 2));
    expect(await pending).toMatchObject({ reason: "event", lines: ["READY: 😀"] });

    const boundary = store.begin("bash", "boundary"); boundary.spill();
    boundary.append(Buffer.concat([Buffer.alloc(SHELL_READ_MAX_BYTES - 1, 120), Buffer.from("😀NEEDLE")]));
    const clipped = await provider.invoke("watch", { id: boundary.id, match: "never", timeoutMs: 10 }, context) as { nextCursor: number };
    expect(clipped).toMatchObject({ reason: "timeout", nextCursor: SHELL_READ_MAX_BYTES - 1 });
    const resumed = provider.invoke("watch", { id: boundary.id, match: "NEEDLE", after: clipped.nextCursor, timeoutMs: 5000 }, context);
    boundary.append(Buffer.from([10]));
    await expect(resumed).resolves.toMatchObject({ reason: "event", lines: ["😀NEEDLE"], nextCursor: boundary.written });
  });

  it("checks cancellation and deadlines while draining many raw output pages", async () => {
    const { store, provider } = setup();
    const job = store.begin("bash", "many pages"); job.spill();
    job.append(Buffer.alloc(SHELL_READ_MAX_BYTES * 4, 120));
    const abort = new AbortController();
    const original = job.read.bind(job);
    const spy = vi.spyOn(job, "read").mockImplementation((...args) => {
      const page = original(...args);
      abort.abort(new Error("cancel draining"));
      return page;
    });
    try {
      await expect(provider.invoke("watch", { id: job.id, match: "never" }, { ...context, signal: abort.signal })).rejects.toThrow("cancel draining");
      expect(spy).toHaveBeenCalledOnce();
    } finally { spy.mockRestore(); }
    const clock = vi.spyOn(Date, "now");
    clock.mockReturnValueOnce(0).mockReturnValue(100);
    try {
      const reader = vi.spyOn(job, "read");
      await expect(provider.invoke("watch", { id: job.id, match: "never", timeoutMs: 10 }, context)).resolves.toMatchObject({ reason: "timeout" });
      expect(reader).toHaveBeenCalledOnce();
      reader.mockRestore();
    } finally { clock.mockRestore(); }
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
    await expect(provider.invoke("watch", { id: job.id }, context)).rejects.toThrow("needs a match literal");
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
    expect((await provider.list({})).map(d => d.name)).toEqual(["list", "get", "wait", "read", "watch", "stop"]);
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
