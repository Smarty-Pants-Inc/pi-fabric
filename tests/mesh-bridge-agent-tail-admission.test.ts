import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { Worker } from "node:worker_threads";
import { buildSync } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { BRIDGE_PROTOCOL_VERSION, RemoteBridgeSide, type BridgeRead } from "../src/mesh/bridge.js";
import { serveBridgeAgentWorker } from "../src/mesh/bridge-agent.js";

// Keep harness deadlines real when the completion-before-grace case controls ONLY
// the agent's response timer. No assumption that a busy host schedules IPC in <5 ms.
const realSetTimeout = setTimeout;
const realClearTimeout = clearTimeout;
const bounded = async <T>(promise: Promise<T>, what: string, ms = 5_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<T>((_resolve, reject) => {
      timer = realSetTimeout(() => reject(new Error(`Timed out: ${what}`)), ms);
    })]);
  } finally { if (timer !== undefined) realClearTimeout(timer); }
};
const pause = (ms: number): Promise<void> => new Promise(resolve => realSetTimeout(resolve, ms));
interface Frame {
  id?: number | null;
  ok?: boolean;
  result?: unknown;
  error?: string;
  notification?: string;
  kind?: string;
}
type Page = BridgeRead & { threadId: number; ordinal: number };
class Wire {
  readonly frames: Frame[] = [];
  readonly requests: Array<{ id: number; op: string; args?: unknown }> = [];
  readonly waiters = new Set<() => void>();
  #next = 1;
  constructor(readonly input: PassThrough, readonly output: PassThrough) {
    let incoming = "", outgoing = "";
    input.on("data", chunk => {
      incoming += String(chunk);
      let end: number;
      while ((end = incoming.indexOf("\n")) >= 0) {
        this.requests.push(JSON.parse(incoming.slice(0, end)));
        incoming = incoming.slice(end + 1);
      }
    });
    output.on("data", chunk => {
      outgoing += String(chunk);
      let end: number;
      while ((end = outgoing.indexOf("\n")) >= 0) {
        this.frames.push(JSON.parse(outgoing.slice(0, end)) as Frame);
        outgoing = outgoing.slice(end + 1);
        for (const check of [...this.waiters]) check();
      }
    });
  }
  async until(ready: () => boolean, what: string): Promise<void> {
    if (ready()) return;
    let check!: () => void;
    const promise = new Promise<void>(resolve => {
      check = () => { if (ready()) resolve(); };
      this.waiters.add(check);
    });
    try { await bounded(promise, what); }
    finally { this.waiters.delete(check); }
  }
  async call(op: string, args?: unknown): Promise<Frame> {
    const id = this.#next++;
    const response = this.until(() => this.frames.some(frame => frame.id === id), `${op} response`);
    this.input.write(`${JSON.stringify({ id, op, ...(args === undefined ? {} : { args }) })}\n`);
    await response;
    return this.frames.find(frame => frame.id === id)!;
  }
  get changes(): number {
    return this.frames.filter(frame => frame.notification === "change" && frame.kind === "events").length;
  }
  count(op: string): number { return this.requests.filter(request => request.op === op).length; }
}
interface Harness {
  root: string;
  control: Int32Array;
  worker: Worker;
  input: PassThrough;
  output: PassThrough;
  wire: Wire;
  serving: Promise<void>;
}
let bundleRoot: string;
let bundle: string;
const agents: Harness[] = [];
const start = (options: { failHello?: boolean; failTail?: boolean } = {}): Harness => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-tail-admission-"));
  const control = new Int32Array(new SharedArrayBuffer(3 * Int32Array.BYTES_PER_ELEMENT));
  const worker = new Worker(bundle, { workerData: { root, control: control.buffer, ...options } });
  const input = new PassThrough(), output = new PassThrough();
  const wire = new Wire(input, output);
  const serving = serveBridgeAgentWorker(worker, input, output);
  void serving.catch(() => undefined);
  const agent = { root, control, worker, input, output, wire, serving };
  agents.push(agent);
  return agent;
};
const release = (agent: Harness, index: number): void => {
  Atomics.store(agent.control, index, 3);
  Atomics.notify(agent.control, index);
};
const entered = (agent: Harness, index: number): Promise<void> => agent.wire.until(
  () => agent.wire.frames.some(frame => frame.notification === "fixture-gate" && frame.kind === String(index)),
  "worker gate entry",
);
const hello = (agent: Harness, busyTail = true): Promise<Frame> => agent.wire.call("hello", {
  version: BRIDGE_PROTOCOL_VERSION, changes: true, ...(busyTail ? { busyTail: true } : {}),
});
const workerPage = (agent: Harness, frame: Frame, text = "page-one"): Page => {
  expect(frame.ok).toBe(true);
  const page = frame.result as Page;
  expect(page.busy).not.toBe(true);
  expect(page.events.map(event => event.text)).toEqual([text]);
  // Required legacy counterexample: the completed read really ran OFF the stdio thread.
  expect(page.threadId).toBeGreaterThan(0);
  expect(page.threadId).toBe(agent.worker.threadId);
  return page;
};

beforeAll(() => {
  bundleRoot = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-tail-admission-bundle-"));
  fs.symlinkSync(path.resolve("node_modules"), path.join(bundleRoot, "node_modules"),
    process.platform === "win32" ? "junction" : "dir");
  bundle = path.join(bundleRoot, "worker.mjs");
  buildSync({ entryPoints: [path.resolve("tests/fixtures/bridge-agent-tail-admission-worker.mjs")],
    outfile: bundle, bundle: true, platform: "node", format: "esm", target: "node24", packages: "external" });
}, 30_000);
afterEach(async () => {
  vi.useRealTimers();
  for (const agent of agents.splice(0)) {
    release(agent, 0); release(agent, 1);
    agent.input.end();
    try { await bounded(agent.serving, "worker cleanup"); }
    finally {
      if (agent.worker.threadId !== -1) await agent.worker.terminate();
      agent.output.destroy();
      fs.rmSync(agent.root, { recursive: true, force: true });
    }
  }
});
afterAll(() => { fs.rmSync(bundleRoot, { recursive: true, force: true }); });

describe("worker tail admission / capability rollout (P2-2)", () => {
  // Keep: actual transport contract + the per-page extra-RPC regression from #1611.
  it("opts in through RemoteBridgeSide and returns idle pages in one RPC without a synthetic wake", async () => {
    const agent = start();
    const remote = new RemoteBridgeSide(agent.output, agent.input);
    try {
      await bounded(remote.hello(), "updated hub hello");
      await bounded(remote.subscribeChanges(() => undefined, () => undefined), "updated hub subscription");
      expect(agent.wire.requests.filter(request => request.op === "hello").map(request => request.args))
        .toEqual([{ version: BRIDGE_PROTOCOL_VERSION, busyTail: true },
          { version: BRIDGE_PROTOCOL_VERSION, busyTail: true, changes: true }]);
      expect(agent.wire.frames.filter(frame => frame.id === 1 || frame.id === 2))
        .toEqual(expect.arrayContaining([expect.objectContaining({ ok: true, result: expect.objectContaining({ busyTail: true }) })]));
      expect(await remote.watchdogPing!(1_000)).toEqual({ busy: false });
      const changes = agent.wire.changes;
      // Freeze only timeout progression: this checks completion BEFORE grace through
      // a real port, without turning CPU contention into an intermittent busy result.
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      const first = await bounded(remote.tail(0), "idle first tail page");
      workerPage(agent, { ok: true, result: first });
      expect(agent.wire.count("tail")).toBe(1);
      const next = await bounded(remote.tail(first.through, first.offset), "idle second tail page");
      workerPage(agent, { ok: true, result: next }, "page-two");
      expect(agent.wire.count("tail")).toBe(2);
      expect(Atomics.load(agent.control, 2)).toBe(2);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(agent.wire.changes).toBe(changes);
      expect(agent.wire.frames.filter(frame => frame.id === 4)).toHaveLength(1);
      expect(agent.wire.frames.filter(frame => frame.id === 5)).toHaveLength(1);
    } finally { vi.useRealTimers(); remote.closeGracefully(); }
  });

  it("answers busy immediately for existing worker work and drains that exact retained page", async () => {
    const agent = start();
    expect((await hello(agent)).result).toMatchObject({ busyTail: true });
    Atomics.store(agent.control, 1, 1);
    const presence = agent.wire.call("presence");
    await entered(agent, 1);
    const changes = agent.wire.changes;
    // A paused grace clock must not delay the already-pending admission.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const busy = await bounded(agent.wire.call("tail", { after: 0, offset: 0 }), "immediate pending-work busy", 1_000);
    expect(busy).toMatchObject({ ok: true, result: { events: [], through: 0, offset: 0, busy: true } });
    expect(busy.result).toEqual({ events: [], through: 0, offset: 0, busy: true });
    expect(Atomics.load(agent.control, 2)).toBe(0);
    expect((await agent.wire.call("ping")).result).toEqual({ busy: true });
    vi.useRealTimers();
    release(agent, 1);
    expect((await presence).ok).toBe(true);
    await agent.wire.until(() => agent.wire.changes > changes, "retained tail completion wake");
    const page = workerPage(agent, await agent.wire.call("tail", { after: 0, offset: 0 }));
    expect(page.ordinal).toBe(1);
    expect(Atomics.load(agent.control, 2)).toBe(1);
    expect(agent.wire.count("tail")).toBe(2);
  });

  it("falls back within one second if an idle-admitted tail itself stalls, then wakes and drains once", async () => {
    const agent = start();
    await hello(agent);
    expect((await agent.wire.call("ping")).result).toEqual({ busy: false });
    Atomics.store(agent.control, 0, 1);
    const changes = agent.wire.changes;
    const startedAt = performance.now();
    const tail = agent.wire.call("tail", { after: 0 });
    await entered(agent, 0);
    const busy = await bounded(tail, "bounded idle fallback", 1_000);
    expect(performance.now() - startedAt).toBeLessThan(1_000);
    expect(busy.result).toEqual({ events: [], through: 0, busy: true });
    expect((await bounded(agent.wire.call("ping"), "ping during slow tail", 1_000)).result).toEqual({ busy: true });
    expect(agent.wire.changes).toBe(changes);
    release(agent, 0);
    await agent.wire.until(() => agent.wire.changes > changes, "slow tail completion wake");
    const page = workerPage(agent, await agent.wire.call("tail", { after: 0 }));
    expect(page.ordinal).toBe(1);
    expect(Atomics.load(agent.control, 2)).toBe(1);
    expect(agent.wire.frames.filter(frame => frame.id === busy.id)).toHaveLength(1);
    expect((await agent.wire.call("ping")).result).toEqual({ busy: false });
  });

  it("waits the full 50 ms grace before marking an idle-admitted read busy", async () => {
    const agent = start();
    await hello(agent);
    Atomics.store(agent.control, 0, 1);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let settled = false;
    const tail = agent.wire.call("tail", { after: 0 }).then(frame => { settled = true; return frame; });
    await entered(agent, 0);
    await vi.advanceTimersByTimeAsync(49);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect((await tail).result).toEqual({ events: [], through: 0, busy: true });
    expect(agent.wire.changes).toBe(0);
    vi.useRealTimers();
    release(agent, 0);
    await agent.wire.until(() => agent.wire.changes === 1, "grace-expired completion wake");
    expect(workerPage(agent, await agent.wire.call("tail", { after: 0 })).ordinal).toBe(1);
  });

  it.each(["legacy", "failed hello"] as const)("returns only a completed WORKER page after %s, even past grace", async mode => {
    const agent = start({ failHello: mode === "failed hello" });
    const response = await hello(agent, mode !== "legacy");
    if (mode === "failed hello") {
      expect(response).toMatchObject({ ok: false, error: "Injected first hello preparation failure" });
    } else {
      expect(response.ok).toBe(true);
      expect(response.result).not.toHaveProperty("busyTail");
    }
    Atomics.store(agent.control, 0, 1);
    let settled = false;
    const tail = agent.wire.call("tail", { after: 0 }).then(frame => { settled = true; return frame; });
    await entered(agent, 0);
    await pause(100); // Deliberately beyond the 50 ms opted-in response grace.
    expect(settled).toBe(false);
    expect(agent.wire.frames.filter(frame => frame.id === 2)).toHaveLength(0);
    expect((await agent.wire.call("ping")).result).toEqual({ busy: true });
    release(agent, 0);
    const page = workerPage(agent, await tail);
    expect(page.ordinal).toBe(1);
    expect(agent.wire.count("tail")).toBe(1);
    expect(agent.wire.changes).toBe(0);
  });

  it("preserves worker error encoding for a direct idle completion", async () => {
    const agent = start({ failTail: true });
    await hello(agent);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const response = await agent.wire.call("tail", { after: 0 });
    expect(response).toEqual({ id: 2, ok: false, error: "Injected worker tail failure" });
    expect(agent.wire.changes).toBe(0);
    expect(Atomics.load(agent.control, 2)).toBe(1);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(agent.wire.frames.filter(frame => frame.id === 2)).toHaveLength(1);
  });
});
