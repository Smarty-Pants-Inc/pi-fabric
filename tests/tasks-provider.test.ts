import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";
const stores: FabricShellJobStore[] = [];
const context = {} as FabricInvocationContext;
afterEach(async () => { for (const store of stores.splice(0)) await store.close(); });
const setup = () => { const store = new FabricShellJobStore(); stores.push(store); return { store, provider: new TasksProvider(store) }; };

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
