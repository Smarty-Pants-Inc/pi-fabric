import { afterEach, describe, expect, it, vi } from "vitest";
import type { FabricInvocationContext } from "../src/protocol.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";
const stores: FabricShellJobStore[] = [];
const context = {} as FabricInvocationContext;
afterEach(async () => { for (const store of stores.splice(0)) await store.close(); });
const setup = () => { const store = new FabricShellJobStore(); stores.push(store); return { store, provider: new TasksProvider(store) }; };

describe("tasks provider", () => {
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
