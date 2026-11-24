import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { Worker } from "node:worker_threads";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { buildSync } from "esbuild";
import { BRIDGE_LEASE_MS, BRIDGE_PROTOCOL_VERSION, BridgeDispatchDeferredError, DEFAULT_CALL_TIMEOUT_MS, MeshBridge, RemoteBridgeSide,
  serveBridgeAgent, StoreBridgeSide, type BridgePresence, type BridgeRead } from "../src/mesh/bridge.js";
import { serveBridgeAgentWorker } from "../src/mesh/bridge-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease } from "../src/topology/host-leases.js";

const MAX_EVENT_BYTES = 64 * 1024;
const PEER = "hub";
const FROM: MeshIdentity = { id: "agent:bridge-worker-test", name: "bridge worker test", kind: "agent" };
const roots: string[] = [];
const agents: AgentHarness[] = [];
const stores: MeshStore[] = [];
let workerBundle: string;
let buildRoot: string;

interface RpcFrame {
  id?: number | null;
  ok?: boolean;
  result?: unknown;
  error?: string;
  code?: string;
  notification?: string;
  kind?: string;
}

interface AgentHarness {
  root: string;
  nativeStore: MeshStore;
  armPath: string;
  enteredPath: string;
  completedPath: string;
  input: PassThrough;
  output: PassThrough;
  worker: Worker;
  remote: RemoteBridgeSide;
  wire: AgentWire;
  serving: Promise<void>;
}

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};

const withTimeout = async <T>(promise: Promise<T>, timeoutMs: number, what: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out waiting for ${what}`)), timeoutMs);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};

type Outcome<T> = { ok: true; value: T } | { ok: false; error: Error };
const observe = <T>(promise: Promise<T>): Promise<Outcome<T>> => promise.then(
  value => ({ ok: true, value }),
  error => ({ ok: false, error: error instanceof Error ? error : new Error(String(error)) }),
);
const valueOf = <T>(outcome: Outcome<T>): T => {
  if (!outcome.ok) throw outcome.error;
  return outcome.value;
};

async function waitForJsonFile<T>(file: string, timeoutMs: number, description: string,
  accept: (value: T) => boolean = () => true): Promise<T> {
  const read = (): T | undefined => {
    try {
      const value = JSON.parse(fs.readFileSync(file, "utf8")) as T;
      return accept(value) ? value : undefined;
    }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT" || error instanceof SyntaxError) return undefined;
      throw error;
    }
  };
  const initial = read();
  if (initial !== undefined) return initial;

  return new Promise<T>((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const finish = (action: () => void): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      watcher?.close();
      action();
    };
    const check = (): void => {
      try {
        const value = read();
        if (value !== undefined) finish(() => resolve(value));
      } catch (error) { finish(() => reject(error)); }
    };
    try {
      watcher = fs.watch(path.dirname(file), (_event, filename) => {
        if (filename === null || String(filename) === path.basename(file)) check();
      });
      watcher.on("error", error => finish(() => reject(error)));
    } catch (error) {
      finish(() => reject(error));
      return;
    }
    timer = setTimeout(() => finish(() => reject(new Error(`Timed out waiting for ${description} (${file})`))), timeoutMs);
    // Recheck after watcher installation to close the create-between-read-and-watch race.
    check();
  });
}

class AgentWire {
  readonly frames: RpcFrame[] = [];
  readonly requests: Array<{ id?: number; op?: string }> = [];
  #outputBuffer = "";
  #inputBuffer = "";
  #changeWaiters: Array<{ count: number; resolve: () => void }> = [];
  eventChanges = 0;
  readonly #onOutput = (chunk: unknown): void => {
    this.#outputBuffer += String(chunk);
    let newline: number;
    while ((newline = this.#outputBuffer.indexOf("\n")) >= 0) {
      const line = this.#outputBuffer.slice(0, newline);
      this.#outputBuffer = this.#outputBuffer.slice(newline + 1);
      if (!line.trim()) continue;
      const frame = JSON.parse(line) as RpcFrame;
      this.frames.push(frame);
      if (frame.notification === "change" && frame.kind === "events") {
        this.eventChanges += 1;
        const ready = this.#changeWaiters.filter(waiter => this.eventChanges >= waiter.count);
        this.#changeWaiters = this.#changeWaiters.filter(waiter => this.eventChanges < waiter.count);
        for (const waiter of ready) waiter.resolve();
      }
    }
  };
  readonly #onInput = (chunk: unknown): void => {
    this.#inputBuffer += String(chunk);
    let newline: number;
    while ((newline = this.#inputBuffer.indexOf("\n")) >= 0) {
      const line = this.#inputBuffer.slice(0, newline);
      this.#inputBuffer = this.#inputBuffer.slice(newline + 1);
      if (!line.trim()) continue;
      const request = JSON.parse(line) as { id?: number; op?: string };
      this.requests.push(request);
    }
  };

  constructor(readonly input: PassThrough, readonly output: PassThrough) {
    output.on("data", this.#onOutput);
    input.on("data", this.#onInput);
  }

  response(id: number): RpcFrame {
    const frame = this.frames.find(candidate => candidate.id === id);
    if (!frame) throw new Error(`No RPC response ${id} has arrived`);
    return frame;
  }

  waitForEventChanges(count: number, timeoutMs = 5_000): Promise<void> {
    if (this.eventChanges >= count) return Promise.resolve();
    const waiter = deferred<void>();
    const entry = { count, resolve: () => waiter.resolve(undefined) };
    this.#changeWaiters.push(entry);
    return withTimeout(waiter.promise, timeoutMs, `${count} events-change notifications`).finally(() => {
      this.#changeWaiters = this.#changeWaiters.filter(candidate => candidate !== entry);
    });
  }

  countRequests(op: string): number { return this.requests.filter(request => request.op === op).length; }

  dispose(): void {
    this.input.off("data", this.#onInput);
    this.output.off("data", this.#onOutput);
    this.#changeWaiters.splice(0).forEach(waiter => waiter.resolve());
  }
}

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-agent-worker-"));
  roots.push(root);
  return root;
};

async function startAgent(options: {
  mode?: "normal" | "commit" | "scan";
  delayMs?: number;
  maxReadEvents?: number;
  callTimeoutMs?: number;
} = {}): Promise<AgentHarness> {
  const root = scratch();
  fs.mkdirSync(path.join(root, "participants"), { recursive: true });
  // Open the second real SQLite store BEFORE the worker can stall a transaction:
  // constructor/schema setup must never block the stdio-owning test thread.
  const nativeStore = new MeshStore(root, MAX_EVENT_BYTES, 100, { stateBackend: "sqlite" });
  stores.push(nativeStore);
  await nativeStore.prepareLock();
  const armPath = path.join(root, "arm-test-hook");
  const enteredPath = path.join(root, "test-hook-entered.json");
  const completedPath = path.join(root, "test-hook-completed.json");
  const input = new PassThrough();
  const output = new PassThrough();
  const wire = new AgentWire(input, output);
  const worker = new Worker(workerBundle, { workerData: {
    root, peer: PEER, mode: options.mode ?? "normal", delayMs: options.delayMs ?? 0,
    maxReadEvents: options.maxReadEvents ?? 100, armPath, enteredPath, completedPath,
  } });
  const serving = serveBridgeAgentWorker(worker, input, output);
  // Keep intentional crash/termination cases handled until the assertion awaits the original promise.
  void serving.catch(() => undefined);
  const remote = options.callTimeoutMs === undefined
    ? new RemoteBridgeSide(output, input)
    : new RemoteBridgeSide(output, input, options.callTimeoutMs);
  const agent = { root, nativeStore, armPath, enteredPath, completedPath, input, output, worker, remote, wire, serving };
  agents.push(agent);

  await withTimeout(remote.hello(), 5_000, "worker-backed hello");
  expect(wire.response(1)).toMatchObject({ ok: true, result: {
    version: BRIDGE_PROTOCOL_VERSION, responsivePing: true,
  } });
  expect(remote.watchdogPing).toBeDefined();
  await withTimeout(remote.subscribeChanges(() => undefined, () => undefined), 5_000,
    "worker-backed change subscription");
  return agent;
}

async function tailWhenReady(agent: AgentHarness, after: number, offset?: number): Promise<BridgeRead> {
  for (let attempt = 0; attempt < 20; attempt++) {
    const before = agent.wire.eventChanges;
    const page = await withTimeout(agent.remote.tail(after, offset), 2_000, "tail page or busy response");
    if (page.busy !== true) return page;
    await agent.wire.waitForEventChanges(before + 1);
  }
  throw new Error(`Tail boundary (${after}, ${String(offset)}) stayed busy without completing`);
}

const publish = (text: string) => ({
  topic: "fleet.work.test",
  kind: "probe",
  from: FROM,
  to: PEER,
  text,
  data: { bridge: { from: "fixture-origin", id: `bridge-agent-${text}` } },
});

const publishNative = (agent: AgentHarness, text: string) => agent.nativeStore.publish({
  topic: "fleet.work.test", kind: "probe", from: FROM, to: PEER, text, durable: true,
});

function invalidateParticipantDirectoryCache(agent: AgentHarness): void {
  const directory = path.join(agent.root, "participants");
  const before = fs.statSync(directory);
  const futureMtimeMs = Math.max(Date.now(), before.mtimeMs) + 1_000;
  fs.utimesSync(directory, before.atime, futureMtimeMs / 1_000);
  expect(fs.statSync(directory).mtimeMs).toBeGreaterThan(before.mtimeMs);
}

async function holdWorkerAtParticipantScan(agent: AgentHarness): Promise<{ presence: Promise<Outcome<BridgePresence>> }> {
  fs.writeFileSync(agent.armPath, "armed\n");
  invalidateParticipantDirectoryCache(agent);
  const presence = observe(agent.remote.presence());
  const entry = await waitForJsonFile<{ threadId: number }>(agent.enteredPath, 5_000,
    "worker participant-directory scan barrier");
  expect(entry.threadId).toBeGreaterThan(0);
  return { presence };
}

const mirrorSample = (at: number, startedAt = at): BridgePresence => {
  const identity: MeshIdentity = { id: "session:receipt-clock-00000000", name: "receipt clock", kind: "main" };
  const record = { format: 1 as const, id: identity.id, rootId: identity.id, identity,
    startedAt, updatedAt: at, expiresAt: at + 60_000 };
  return { hosts: [{ record, expiresAt: record.expiresAt, observedAt: at }], participants: [], reserved: [] };
};

async function closeAgent(agent: AgentHarness): Promise<void> {
  try { agent.remote.close(new Error("bridge worker test cleanup")); } catch { /* already closed */ }
  try { agent.input.end(); } catch { /* already ended */ }
  agent.output.destroy();
  await withTimeout(agent.serving.then(() => undefined, () => undefined), 3_000, "worker runner cleanup").catch(() => undefined);
  if (agent.worker.threadId !== -1) await agent.worker.terminate().catch(() => 1);
  agent.wire.dispose();
}

afterEach(async () => {
  for (const agent of agents.splice(0).reverse()) await closeAgent(agent);
  for (const store of stores.splice(0).reverse()) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

beforeAll(() => {
  buildRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-agent-worker-bundle-"));
  fs.symlinkSync(path.resolve("node_modules"), path.join(buildRoot, "node_modules"),
    process.platform === "win32" ? "junction" : "dir");
  workerBundle = path.join(buildRoot, "bridge-agent-sqlite-worker.mjs");
  buildSync({
    entryPoints: [path.resolve("tests/fixtures/bridge-agent-sqlite-worker.mjs")],
    outfile: workerBundle,
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node24",
    packages: "external",
  });
}, 60_000);

afterAll(() => {
  fs.rmSync(buildRoot, { recursive: true, force: true });
});

describe("worker-backed bridge agent responsiveness (#16874)", () => {
  // Data-safety regression: exercise the actual topology RPC FIFO, mirror commit,
  // lease file and holds() expiry check, not just a callback invocation count.
  it.each(["native", "mocked"] as const)("looks up the live receipt clock after a %s startup clock is replaced", async startup => {
    const originalNow = Date.now;
    const bootAt = originalNow();
    const store = new MeshStore(scratch(), MAX_EVENT_BYTES, 100, { stateBackend: "sqlite" });
    stores.push(store);
    const side = new StoreBridgeSide(store, PEER, () => Date.now());
    const input = new PassThrough(), output = new PassThrough();
    if (startup === "mocked") Date.now = () => bootAt - 60_000;
    const serving = serveBridgeAgent(side, input, output);
    const remote = new RemoteBridgeSide(output, input);
    const entered = deferred<void>(), release = deferred<void>();
    try {
      await remote.hello();
      let wall = bootAt + 60_000;
      // Replace the PROPERTY, not the implementation of the already captured mock.
      Date.now = () => wall;
      const presence = side.presence.bind(side);
      side.presence = async () => { entered.resolve(undefined); await release.promise; return presence(); };
      const heldPresence = observe(remote.presence());
      await withTimeout(entered.promise, 2_000, "held topology service");
      const receivedAt = wall;
      await expect(remote.mirror(mirrorSample(receivedAt, bootAt))).rejects.toMatchObject({ sent: false, reason: "traffic-in-flight" });
      // The hub now refuses this queueing. Inject the raw RECEIVER test frame deliberately:
      // receipt-clock conservation must still hold for old hubs and direct wire senders.
      const queuedMirror = observe(new Promise<void>((resolve, reject) => {
        const onReply = (chunk: unknown): void => {
          for (const line of String(chunk).trim().split("\n")) {
            const reply = JSON.parse(line) as { id?: number; ok?: boolean; error?: string };
            if (reply.id !== 100_000) continue;
            output.off("data", onReply);
            if (reply.ok) resolve(); else reject(new Error(reply.error));
          }
        };
        output.on("data", onReply);
        input.write(`${JSON.stringify({ id: 100_000, op: "mirror", args: mirrorSample(receivedAt, bootAt) })}\n`);
      }));
      wall += BRIDGE_LEASE_MS + 1;
      release.resolve(undefined);
      valueOf(await heldPresence);
      valueOf(await queuedMirror);

      const id = mirrorSample(receivedAt).hosts[0]!.record.id;
      const oldLease = readHostLease(store.root, id)!;
      expect(oldLease.mirror?.observedAt).toBe(receivedAt);
      expect(oldLease.expiresAt).toBe(receivedAt + BRIDGE_LEASE_MS);
      expect(oldLease.expiresAt).toBeLessThan(wall);
      expect(side.holds(id)).toBe(false); // A slow queue may not freshen the old sample.

      // Subsequent requests must keep following the current clock, not a saved
      // startup function or a receipt from the preceding request.
      for (let renewal = 0; renewal < 2; renewal++) {
        wall += 1_000;
        await remote.mirror(mirrorSample(wall, bootAt));
        expect(readHostLease(store.root, id)?.mirror?.observedAt).toBe(wall);
        expect(readHostLease(store.root, id)?.expiresAt).toBe(wall + BRIDGE_LEASE_MS);
        expect(side.holds(id)).toBe(true);
      }
    } finally {
      Date.now = originalNow;
      release.resolve(undefined);
      remote.close();
      input.end();
      await serving;
      output.end();
    }
  });

  it("retains an exact busy-tail cursor without spinning and resumes on a change notification", async () => {
    const root = scratch();
    const localStore = new MeshStore(path.join(root, "local"), MAX_EVENT_BYTES, 100, { stateBackend: "sqlite" });
    const remoteStore = new MeshStore(path.join(root, "remote"), MAX_EVENT_BYTES, 100, { stateBackend: "sqlite" });
    stores.push(localStore, remoteStore);
    // Filtered events still advance a real byte/sequence cursor once a page is ready.
    await remoteStore.publish({ topic: "private.unbridged", kind: "anchor", from: FROM, durable: true });
    const local = new StoreBridgeSide(localStore, "far"), remote = new StoreBridgeSide(remoteStore, PEER);
    let changed: ((kind: "events") => void) | undefined;
    local.subscribeChanges = () => () => undefined;
    remote.subscribeChanges = callback => { changed = callback; return () => { changed = undefined; }; };
    const cursorPath = path.join(root, "cursor.json");
    const bridge = new MeshBridge({ localName: PEER, remoteName: "far", local, remote, cursorPath,
      membershipRetryMs: 30_000, stopMs: 1_000 });
    await bridge.start();
    const before = fs.readFileSync(cursorPath, "utf8");
    const cursor = (JSON.parse(before) as { toLocal: { after: number; offset?: number } }).toLocal;
    expect(cursor.offset).toEqual(expect.any(Number));
    const appended = await remoteStore.publish({ topic: "private.unbridged", kind: "next", from: FROM, durable: true });
    let busy = true, tailCalls = 0;
    const entered = deferred<void>();
    const tail = remote.tail.bind(remote);
    remote.tail = async (after, offset) => {
      tailCalls++;
      if (!busy) return tail(after, offset);
      expect({ after, offset }).toEqual({ after: cursor.after, offset: cursor.offset });
      entered.resolve(undefined);
      return { events: [], through: after, ...(offset === undefined ? {} : { offset }), busy: true };
    };
    const running = observe(bridge.run());
    try {
      await withTimeout(entered.promise, 2_000, "MeshBridge busy tail");
      // Normal wake source: events notification. The retained-work safety net is
      // 30 s; this bounded negative-observation window checks it does not hot-spin.
      await new Promise(resolve => setTimeout(resolve, 75));
      expect(tailCalls).toBe(1);
      expect(fs.readFileSync(cursorPath, "utf8")).toBe(before);
      const advanced = waitForJsonFile<{ toLocal: { after: number; offset?: number } }>(cursorPath, 2_000,
        "cursor after busy-tail release", value => value.toLocal.after === appended.sequence);
      busy = false;
      expect(changed).toBeDefined();
      changed!("events");
      const checkpoint = await advanced;
      expect(checkpoint.toLocal.offset).toBeGreaterThan(cursor.offset!);
      expect(tailCalls).toBeGreaterThan(1);
      expect(localStore.read()).toEqual([]); // Filtered source event was consumed, not delivered.
    } finally {
      await bridge.stop();
      valueOf(await withTimeout(running, 2_000, "busy-tail bridge shutdown"));
    }
  });

  it("keeps legacy peers on the latestSequence watchdog fallback", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const operations: string[] = [];
    let buffered = "";
    input.on("data", chunk => {
      buffered += String(chunk);
      let newline: number;
      while ((newline = buffered.indexOf("\n")) >= 0) {
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        if (!line.trim()) continue;
        const request = JSON.parse(line) as { id: number; op: string };
        operations.push(request.op);
        const result = request.op === "hello" ? { version: 1, tail: true }
          : request.op === "latestSequence" ? 7 : { events: [], through: 7 };
        output.write(`${JSON.stringify({ id: request.id, ok: true, result })}\n`);
      }
    });
    const remote = new RemoteBridgeSide(output, input);
    try {
      await remote.hello();
      expect(remote.protocolVersion).toBe(1);
      expect(remote.watchdogPing).toBeUndefined();
      await expect(remote.watchdogHead(1_000)).resolves.toBe(7);
      expect(operations).toEqual(["hello", "latestSequence"]);
    } finally { remote.close(); }
  });

  it("answers tail and ping while JS Atomics.wait holds a write transaction before COMMIT, then drains ordered durable writes", async () => {
    const agent = await startAgent({ mode: "commit", delayMs: 35_000, callTimeoutMs: 60_000 });
    await publishNative(agent, "tail-anchor");
    const initial = await agent.remote.latestCursor();
    // These native events are exportable. Inbound bridged work is deliberately NOT
    // an echo source, and opening/seeding another store during COMMIT could block.
    const outgoing = [await publishNative(agent, "outgoing-1"), await publishNative(agent, "outgoing-2"),
      await publishNative(agent, "outgoing-3")];
    fs.writeFileSync(agent.armPath, "armed\n");

    const first = observe(agent.remote.publish(publish("ordered-1")));
    const commitEntry = await waitForJsonFile<{ startedAt: number; threadId: number; writeChanges: number }>(agent.enteredPath,
      5_000, "JS Atomics.wait entry before SQLite COMMIT");
    expect(commitEntry.threadId).toBeGreaterThan(0);
    expect(commitEntry.writeChanges).toBeGreaterThan(0);
    let queuedWriteSettled = false;
    const second = observe(agent.remote.publish(publish("ordered-2"))).then(outcome => {
      queuedWriteSettled = true;
      return outcome;
    });
    const third = observe(agent.remote.publish(publish("ordered-3")));

    const tailStartedAt = performance.now();
    const busyTail = await withTimeout(agent.remote.tail(initial.through, initial.offset), 1_000, "responsive busy tail");
    const tailMs = performance.now() - tailStartedAt;
    expect(busyTail).toEqual({ events: [], through: initial.through,
      ...(initial.offset === undefined ? {} : { offset: initial.offset }), busy: true });

    const pingStartedAt = performance.now();
    const ping = await withTimeout(agent.remote.watchdogPing!(1_000), 1_000, "responsive watchdog ping");
    const pingMs = performance.now() - pingStartedAt;
    expect(ping).toEqual({ busy: true });
    expect(agent.wire.countRequests("ping")).toBe(1);
    expect(tailMs).toBeLessThan(1_000);
    expect(pingMs).toBeLessThan(1_000);
    expect(queuedWriteSettled).toBe(false);

    const outcomes = await withTimeout(Promise.all([first, second, third]), 45_000, "three queued durable publish receipts");
    const receipts = outcomes.map(valueOf);
    const commitDone = await waitForJsonFile<{ blockedMs: number }>(agent.completedPath, 2_000, "JS Atomics.wait release before SQLite COMMIT");
    expect(commitDone.blockedMs).toBeGreaterThanOrEqual(34_000);
    expect(receipts.map(receipt => receipt.sequence)).toEqual([
      outgoing[2]!.sequence + 1, outgoing[2]!.sequence + 2, outgoing[2]!.sequence + 3,
    ]);

    const drained = await tailWhenReady(agent, initial.through, initial.offset);
    expect(drained.events.map(event => event.text)).toEqual(["outgoing-1", "outgoing-2", "outgoing-3"]);
    expect(drained.events.map(event => event.sequence)).toEqual(outgoing.map(event => event.sequence));
    expect(drained.through).toBe(receipts[2]!.sequence);
    const idle = await tailWhenReady(agent, drained.through, drained.offset);
    expect(idle.events).toEqual([]);
    expect(idle.through).toBe(drained.through);

    // Reopen after a clean worker EOF: committed events must survive worker/store shutdown.
    agent.input.end();
    await withTimeout(agent.serving, 5_000, "clean worker EOF after commit");
    const verifier = new MeshStore(agent.root, MAX_EVENT_BYTES, 100, { stateBackend: "sqlite" });
    try {
      const persisted = verifier.read({ after: initial.through });
      expect(persisted.map(event => event.text))
        .toEqual(["outgoing-1", "outgoing-2", "outgoing-3", "ordered-1", "ordered-2", "ordered-3"]);
      const inbound = persisted.filter(event => event.verification === "bridge");
      expect(inbound.map(event => event.sequence)).toEqual(receipts.map(receipt => receipt.sequence));
      expect(inbound.map(event => event.text)).toEqual(["ordered-1", "ordered-2", "ordered-3"]);
      expect(inbound.map(event => (event.data as { bridge: { from: string } }).bridge.from))
        .toEqual([PEER, PEER, PEER]);
      expect(verifier.get("test/bridge-agent-before-publish", { fresh: true })?.value)
        .toEqual({ ordinal: 3, text: "ordered-3" });
    } finally { verifier.closeState(); }

    console.info(`[bridge-worker-js-wait-35s] held=${commitDone.blockedMs}ms tail=${tailMs.toFixed(1)}ms ` +
      `ping=${pingMs.toFixed(1)}ms inboundOrdered=${receipts.length} nativeExported=${drained.events.length}`);
  }, 70_000);

  it("records the unchanged default 30s fatal publication deadline without replay", async () => {
    expect(DEFAULT_CALL_TIMEOUT_MS).toBe(30_000);
    const agent = await startAgent({ mode: "commit", delayMs: 35_000 });
    fs.writeFileSync(agent.armPath, "armed\n");
    // An independently SENT publication (no mirror) owns the unchanged fatal deadline.
    // The JS Atomics fixture is not proof of a native COMMIT stall.
    const startedAt = performance.now();
    const pendingPublish = observe(agent.remote.publish(publish("deadline-residual")));
    const commitEntry = await waitForJsonFile<{ threadId: number; writeChanges: number }>(agent.enteredPath, 5_000,
      "JS Atomics.wait entry for independently sent publication");
    expect(commitEntry.threadId).toBeGreaterThan(0);
    expect(commitEntry.writeChanges).toBeGreaterThan(0);

    const closed = await withTimeout(agent.remote.closed, 34_000, "fatal default publication deadline");
    const elapsedMs = performance.now() - startedAt;
    expect(closed.message).toBe("Bridge agent did not answer publish within 30000 ms");
    const publishOutcome = await withTimeout(pendingPublish, 1_000, "default-deadline publish rejection");
    expect(publishOutcome.ok).toBe(false);
    if (publishOutcome.ok) throw new Error("The publication unexpectedly completed after the fatal deadline");
    expect(publishOutcome.error.message).toBe(closed.message);
    expect(elapsedMs).toBeGreaterThanOrEqual(29_000);
    expect(elapsedMs).toBeLessThan(34_000);
    expect(agent.wire.countRequests("publish")).toBe(1); // fatal close; no automatic replay
    expect(agent.wire.countRequests("mirror")).toBe(0);
    expect(agent.nativeStore.read().filter(event => event.text === "deadline-residual")).toEqual([]);
    // These assertions exercise shutdown with the fixture stalled in JS Atomics.wait;
    // they do not establish a hard termination bound for a native SQLite call.
    expect(fs.existsSync(agent.completedPath)).toBe(false);
    await withTimeout(agent.serving.then(() => undefined, error => { throw error; }), 5_000,
      "transport close and worker shutdown from the JS Atomics.wait fixture");

    console.info(`[bridge-worker-default-deadline] close=${elapsedMs.toFixed(0)}ms ` +
      `publishRequests=${agent.wire.countRequests("publish")} jsWaitCompletionMarkerAbsent=true replayed=false`);
  }, 42_000);

  it("refuses publication during a worker mirror, keeps busy/ping responsive, then delivers exactly once", async () => {
    const agent = await startAgent({ mode: "commit", delayMs: 1_500 });
    fs.writeFileSync(agent.armPath, "armed\n");
    const mirror = observe(agent.remote.mirror(mirrorSample(Date.now())));
    await waitForJsonFile(agent.enteredPath, 5_000, "worker mirror JS wait entry");
    const started = performance.now();
    const refused = await agent.remote.publish(publish("mirror-retained")).catch(error => error);
    expect(refused).toBeInstanceOf(BridgeDispatchDeferredError);
    expect(refused).toMatchObject({ sent: false, reason: "mirror-pending" });
    expect(agent.wire.countRequests("publish")).toBe(0);
    await expect(agent.remote.watchdogPing!(1_000)).resolves.toEqual({ busy: true });
    const busy = await agent.remote.tail(0);
    expect(busy).toMatchObject({ events: [], through: 0, busy: true });
    valueOf(await mirror);
    expect(performance.now() - started).toBeGreaterThan(1_000); // real policy cost, NOT instantaneous delivery
    await agent.remote.mirror(mirrorSample(Date.now())); // current source observation, never late-ACK renewal
    const receipt = await agent.remote.publish(publish("mirror-retained"));
    expect(receipt.sequence).toBeGreaterThan(0);
    expect(agent.wire.countRequests("publish")).toBe(1);
    expect(agent.nativeStore.read().filter(event => event.text === "mirror-retained")).toHaveLength(1);
  }, 15_000);

  it("invalidates a completed empty page on a real append and preserves a cached nonempty prefix", async () => {
    const emptyAgent = await startAgent({ mode: "scan", delayMs: 1_500, maxReadEvents: 1 });
    const emptyBoundary = await emptyAgent.remote.latestCursor();
    const { presence: emptyScan } = await holdWorkerAtParticipantScan(emptyAgent);
    const emptyReadyAfter = emptyAgent.wire.eventChanges + 1;
    const emptyBusy = await emptyAgent.remote.tail(emptyBoundary.through, emptyBoundary.offset);
    expect(emptyBusy).toMatchObject({ events: [], through: emptyBoundary.through, busy: true });
    await emptyAgent.wire.waitForEventChanges(emptyReadyAfter);
    valueOf(await emptyScan);

    const appendChangedAfter = emptyAgent.wire.eventChanges + 1;
    await publishNative(emptyAgent, "after-empty");
    await emptyAgent.wire.waitForEventChanges(appendChangedAfter);
    const refreshed = await tailWhenReady(emptyAgent, emptyBoundary.through, emptyBoundary.offset);
    expect(refreshed.events.map(event => event.text)).toEqual(["after-empty"]);

    const prefixAgent = await startAgent({ mode: "scan", delayMs: 1_500, maxReadEvents: 1 });
    const prefixBoundary = await prefixAgent.remote.latestCursor();
    const prefixObservedAfter = prefixAgent.wire.eventChanges + 1;
    await publishNative(prefixAgent, "prefix");
    await prefixAgent.wire.waitForEventChanges(prefixObservedAfter);
    const { presence: prefixScan } = await holdWorkerAtParticipantScan(prefixAgent);
    const prefixReadyAfter = prefixAgent.wire.eventChanges + 1;
    const prefixBusy = await prefixAgent.remote.tail(prefixBoundary.through, prefixBoundary.offset);
    expect(prefixBusy.busy).toBe(true);
    await prefixAgent.wire.waitForEventChanges(prefixReadyAfter);
    valueOf(await prefixScan);

    const followingChangedAfter = prefixAgent.wire.eventChanges + 1;
    await publishNative(prefixAgent, "following");
    await prefixAgent.wire.waitForEventChanges(followingChangedAfter);

    const prefix = await tailWhenReady(prefixAgent, prefixBoundary.through, prefixBoundary.offset);
    expect(prefix.events.map(event => event.text)).toEqual(["prefix"]);
    const successor = await tailWhenReady(prefixAgent, prefix.through, prefix.offset);
    expect(successor.events.map(event => event.text)).toEqual(["following"]);
    expect(successor.events[0]!.sequence).toBe(prefix.events[0]!.sequence + 1);
  }, 20_000);

  it("keeps ping and tail responsive during a synchronous topology directory scan", async () => {
    const agent = await startAgent({ mode: "scan", delayMs: 1_500 });
    const cursor = await agent.remote.latestCursor();
    fs.writeFileSync(agent.armPath, "armed\n");
    invalidateParticipantDirectoryCache(agent);
    const scanning = agent.remote.presence();
    const scanEntry = await waitForJsonFile<{ threadId: number }>(agent.enteredPath, 5_000,
      "topology directory scan entry");
    expect(scanEntry.threadId).toBeGreaterThan(0);

    const tailStartedAt = performance.now();
    const tail = await withTimeout(agent.remote.tail(cursor.through, cursor.offset), 1_000, "tail during topology scan");
    const tailMs = performance.now() - tailStartedAt;
    const pingStartedAt = performance.now();
    const ping = await withTimeout(agent.remote.watchdogPing!(1_000), 1_000, "responsive ping during scan");
    const pingMs = performance.now() - pingStartedAt;
    expect(tail).toMatchObject({ events: [], through: cursor.through, busy: true });
    expect(ping).toEqual({ busy: true });
    expect(agent.wire.countRequests("ping")).toBe(1);
    expect(tailMs).toBeLessThan(1_000);
    expect(pingMs).toBeLessThan(1_000);

    await scanning;
    await waitForJsonFile(agent.completedPath, 2_000, "topology scan completion");
    const page = await tailWhenReady(agent, cursor.through, cursor.offset);
    expect(page.events).toEqual([]);
  }, 10_000);

  it("closes watches on EOF/output close and rejects a pending call on worker failure", async () => {
    const eofAgent = await startAgent();
    eofAgent.input.end();
    await withTimeout(eofAgent.serving, 5_000, "clean EOF worker cleanup");
    expect(eofAgent.worker.threadId).toBe(-1);

    const outputAgent = await startAgent();
    outputAgent.output.destroy();
    await withTimeout(outputAgent.serving, 5_000, "output-close worker cleanup");
    expect(outputAgent.worker.threadId).toBe(-1);

    const failedAgent = await startAgent({ mode: "commit", delayMs: 10_000 });
    fs.writeFileSync(failedAgent.armPath, "armed\n");
    const pendingPublish = observe(failedAgent.remote.publish(publish("worker-failure")));
    const failureEntry = await waitForJsonFile<{ threadId: number }>(failedAgent.enteredPath, 5_000,
      "worker failure injection point");
    expect(failureEntry.threadId).toBeGreaterThan(0);
    await failedAgent.worker.terminate();
    const publishOutcome = await withTimeout(pendingPublish, 2_000, "worker-failure publish rejection");
    expect(publishOutcome.ok).toBe(false);
    if (publishOutcome.ok) throw new Error("The publication unexpectedly completed after worker failure");
    expect(publishOutcome.error.message).toMatch(/closed|exited|transport/i);
    await expect(failedAgent.serving).rejects.toThrow(/unexpectedly/);
    await expect(failedAgent.remote.closed).resolves.toBeInstanceOf(Error);
    expect(failedAgent.wire.countRequests("publish")).toBe(1);
  }, 20_000);
});
