// Keep: H1/H2 data-safety failure interleavings from .local/C2-REVIEW.md.
// Deterministic event gates, not native process/crash acceptance or production proof.
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { setImmediate } from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import { c2Lifecycle, type C2Child, type C2Terminal } from "./helpers/b5-c2-lifecycle.js";

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const owner = () => {
  let cleanup!: () => Promise<void>;
  const lifecycle = c2Lifecycle(close => { cleanup = close; });
  return { lifecycle, cleanup: () => cleanup() };
};
const fakeChild = (pid: number | undefined = 123) => {
  const emitter = new EventEmitter();
  const child = Object.assign(emitter, { pid, stdout: new PassThrough(), stderr: new PassThrough(), kill: vi.fn(() => true) });
  // Only Node's event/pipe ordering is substituted. The real harness uses this same lifecycle.
  return { child: child as unknown as C2Child, emit: emitter.emit.bind(emitter), stdout: child.stdout, stderr: child.stderr, kill: child.kill };
};
const observe = <T>(promise: Promise<T>) => {
  const state = { settled: false };
  const result = promise.then(value => { state.settled = true; return { value }; }, error => { state.settled = true; return { error }; });
  return { state, result };
};

describe("C2 deterministic H1/H2 lifecycle regressions", () => {
  it("owns pending context creation before first await; late resolution after cancellation disposes without rebuilding or launching receivers", async () => {
    const { lifecycle, cleanup } = owner(), creating = deferred<{ rebuild(): Promise<void>; cancel(): Promise<void>; dispose(): Promise<void> }>();
    const context = { rebuild: vi.fn(async () => {}), cancel: vi.fn(async () => {}), dispose: vi.fn(async () => {}) };
    const start = vi.fn(() => fakeChild().child), modes: string[] = [];
    const work = observe((async () => {
      await lifecycle.bundle(() => creating.promise);
      // Review H1's abandoned bootstrap → read → finish-new publication → admit path.
      for (const mode of ["crash", "read", "finish-new", "admit"]) { modes.push(mode); await lifecycle.run(start); }
    })());
    const closed = observe(cleanup()); // Vitest afterEach after the existing test deadline.
    await setImmediate();
    expect(work.state.settled).toBe(false); expect(closed.state.settled).toBe(false);
    creating.resolve(context);
    expect(await work.result).toMatchObject({ error: expect.objectContaining({ message: expect.stringContaining("scenario ended") }) });
    expect(await closed.result).toHaveProperty("value");
    expect(context.rebuild).not.toHaveBeenCalled(); expect(context.cancel).toHaveBeenCalledOnce(); expect(context.dispose).toHaveBeenCalledOnce();
    expect(start).not.toHaveBeenCalled(); expect(modes).toEqual([]);
    await expect(lifecycle.run(start)).rejects.toThrow("scenario ended"); expect(start).not.toHaveBeenCalled();
  });

  it("cancels and joins pending rebuild; a late successful bundle cannot launch a child and disposal must finish before cleanup", async () => {
    const { lifecycle, cleanup } = owner(), rebuilding = deferred<void>(), entered = deferred<void>(), disposed = deferred<void>();
    const context = {
      rebuild: vi.fn(() => { entered.resolve(); return rebuilding.promise; }),
      cancel: vi.fn(async () => {}), dispose: vi.fn(() => disposed.promise),
    };
    const start = vi.fn(() => fakeChild().child);
    const work = observe((async () => { await lifecycle.bundle(async () => context); await lifecycle.run(start); })());
    await entered.promise;
    const closed = observe(cleanup()); await setImmediate();
    expect(context.cancel).toHaveBeenCalledOnce(); expect(context.dispose).toHaveBeenCalledOnce();
    expect(closed.state.settled).toBe(false);
    rebuilding.resolve(); await setImmediate();
    expect(start).not.toHaveBeenCalled(); expect(work.state.settled).toBe(false); expect(closed.state.settled).toBe(false);
    disposed.resolve();
    expect(await work.result).toHaveProperty("error"); expect(await closed.result).toHaveProperty("value");
    expect(start).not.toHaveBeenCalled(); expect(context.dispose).toHaveBeenCalledOnce();
  });

  it.each(["read", "retry-new", "admit"])("retains final %s JSON and stderr when native exit arrives before output/close; no successor starts early", async mode => {
    const { lifecycle, cleanup } = owner(), first = fakeChild(), successor = fakeChild(124);
    const response = mode === "admit" ? { ack: { queued: true }, state: { inputs: [{ inputId: "original" }] }, nativeDeliveries: 0 }
      : mode === "retry-new" ? { code: "RECOVERY_CONFLICT" } : { revision: 0, complete: true, inputs: [], recoveryGenerationPins: [] };
    const terminals: C2Terminal[] = [], launchSuccessor = vi.fn(() => successor.child);
    const work = observe((async () => {
      const result = await lifecycle.run(() => first.child, terminal => { terminals.push(terminal); });
      const reopened = lifecycle.run(launchSuccessor);
      successor.emit("exit", 0, null); successor.emit("close", 0, null); await reopened;
      return result;
    })());
    first.emit("exit", 0, null); await setImmediate();
    expect(work.state.settled).toBe(false); expect(terminals).toEqual([]); expect(launchSuccessor).not.toHaveBeenCalled();
    const json = JSON.stringify(response), split = Math.floor(json.length / 2);
    first.stdout.write(json.slice(0, split)); first.stdout.end(json.slice(split) + "\n"); first.stderr.end("final diagnostic\n");
    await setImmediate(); expect(work.state.settled).toBe(false); expect(launchSuccessor).not.toHaveBeenCalled();
    first.emit("close", 0, null);
    const result = await work.result;
    expect(result).toHaveProperty("value");
    if (!("value" in result)) throw result.error;
    expect(JSON.parse(result.value.stdout)).toEqual(response); expect(result.value.stderr).toBe("final diagnostic\n");
    expect(terminals).toEqual([{ pid: 123, nativeExit: { code: 0, signal: null }, stdout: json + "\n", stderr: "final diagnostic\n", error: null }]);
    expect(launchSuccessor).toHaveBeenCalledOnce(); await cleanup();
  });

  it("allows the normal success counterexample: active build is disposed, stdout precedes exit, then close authorizes the next receiver", async () => {
    const { lifecycle, cleanup } = owner();
    const context = { rebuild: vi.fn(async () => {}), cancel: vi.fn(async () => {}), dispose: vi.fn(async () => {}) };
    await lifecycle.bundle(async () => context);
    expect(context.rebuild).toHaveBeenCalledOnce(); expect(context.dispose).toHaveBeenCalledOnce(); expect(context.cancel).not.toHaveBeenCalled();
    const child = fakeChild(); const work = observe(lifecycle.run(() => child.child));
    child.stdout.end('{"complete":true}\n'); child.stderr.end(); child.emit("exit", 0, null); child.emit("close", 0, null);
    expect(await work.result).toMatchObject({ value: { code: 0, signal: null, stdout: '{"complete":true}\n', stderr: "" } });
    await cleanup(); expect(child.kill).not.toHaveBeenCalled();
  });

  it("cancellation cannot use SIGKILL request or an error as death; cleanup and output wait for actual exit AND close", async () => {
    const { lifecycle, cleanup } = owner(), child = fakeChild(); const terminals: C2Terminal[] = [];
    const work = observe(lifecycle.run(() => child.child, terminal => { terminals.push(terminal); }));
    const closed = observe(cleanup()); expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    child.emit("error", new Error("kill diagnostic, not death")); await setImmediate();
    expect(work.state.settled).toBe(false); expect(closed.state.settled).toBe(false);
    child.emit("exit", null, "SIGKILL"); await setImmediate();
    expect(work.state.settled).toBe(false); expect(closed.state.settled).toBe(false);
    child.stdout.end('{"code":"RECOVERY_HISTORY_INCOMPLETE"}\n'); child.stderr.end("late stderr\n"); await setImmediate();
    child.emit("close", null, "SIGKILL");
    expect(await work.result).toMatchObject({ error: expect.objectContaining({ message: "kill diagnostic, not death" }) });
    expect(await closed.result).toHaveProperty("value");
    expect(terminals).toMatchObject([{ nativeExit: { code: null, signal: "SIGKILL" }, stderr: "late stderr\n" }]);
    expect(JSON.parse(terminals[0]!.stdout)).toEqual({ code: "RECOVERY_HISTORY_INCOMPLETE" });
  });

  it("cancellation after native exit keeps custody through pipe close, never signals an already-dead child or launches the next receiver", async () => {
    const { lifecycle, cleanup } = owner(), child = fakeChild(), next = vi.fn(() => fakeChild(124).child);
    const work = observe((async () => { await lifecycle.run(() => child.child); await lifecycle.run(next); })());
    child.emit("exit", 0, null); const closed = observe(cleanup()); await setImmediate();
    expect(child.kill).not.toHaveBeenCalled(); expect(closed.state.settled).toBe(false);
    child.stdout.end('{"complete":true}\n'); child.emit("close", 0, null);
    expect(await work.result).toMatchObject({ error: expect.objectContaining({ message: expect.stringContaining("scenario ended") }) });
    expect(await closed.result).toHaveProperty("value"); expect(next).not.toHaveBeenCalled();
  });

  it("spawn failure drains at close without inventing native exit, including cancellation while that close is pending", async () => {
    const { lifecycle, cleanup } = owner(), child = fakeChild();
    // Native spawn failure has no PID and no exit event.
    Object.defineProperty(child.child, "pid", { value: undefined });
    const terminals: C2Terminal[] = [], error = Object.assign(new Error("spawn ENOENT"), { code: "ENOENT" });
    const work = observe(lifecycle.run(() => child.child, terminal => { terminals.push(terminal); }));
    child.emit("error", error); const closed = observe(cleanup()); await setImmediate();
    expect(work.state.settled).toBe(false); expect(closed.state.settled).toBe(false); expect(child.kill).not.toHaveBeenCalled();
    child.stderr.end("spawn diagnostic\n"); child.emit("close", -2, null);
    expect(await work.result).toEqual({ error }); expect(await closed.result).toHaveProperty("value");
    expect(terminals).toEqual([{ pid: null, nativeExit: null, stdout: "", stderr: "spawn diagnostic\n", error: "spawn ENOENT" }]);
  });

  it("rejects close-only evidence for a child with a PID even when close claims success", async () => {
    const { lifecycle, cleanup } = owner(), child = fakeChild();
    const work = observe(lifecycle.run(() => child.child));
    const closed = observe(cleanup()); child.emit("close", 0, null);
    expect(await work.result).toMatchObject({ error: expect.objectContaining({ message: expect.stringContaining("without actual native exit") }) });
    expect(await closed.result).toMatchObject({ error: expect.any(AggregateError) });
  });

  it("accepts synchronous spawn throws as failures, with no child to abandon", async () => {
    const { lifecycle, cleanup } = owner();
    await expect(lifecycle.run(() => { throw new Error("synchronous spawn error"); })).rejects.toThrow("synchronous spawn error");
    await cleanup();
  });
});
