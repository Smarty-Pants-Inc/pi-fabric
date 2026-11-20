import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BRIDGE_LEASE_MS, BridgeDispatchDeferredError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide,
  type BridgePublish,
} from "../src/mesh/bridge.js";
import { watchBridgeStore } from "../src/mesh/change-notifier.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Keep reason: the real stdio/file-mesh trust boundary and the ingress HOL failure path.
// No publication, ownership, lease, subscription, or control admission is mocked.
//
// Optional, bounded benchmark (one JSON line, four 50-command cases, <=10 outstanding):
//   FABRIC_TEST_INGRESS_BENCH=1 bunx vitest run tests/mesh-bridge-ingress.test.ts
// Held-barrier characterization / future isolation gate, using the SAME real path:
//   FABRIC_TEST_INGRESS_BARRIER=blocked     ...   # characterize the old implementation
//   FABRIC_TEST_INGRESS_BARRIER=independent ...  # red before an isolation fix, green after
// Default "independent" asserts progress with a PRESENCE service held. #16378
// intentionally supersedes mirror-service independence: even a worker-free async
// mirror blocks protected publication/read/head RPCs until its original slot settles.
// Requires serveBridgeAgent's optional fourth-argument debug callback for benchmark timings.
const COMMANDS = 50, IN_FLIGHT = 10, WAVES = COMMANDS / IN_FLIGHT;
const SEED = 0x64779751;
const barrierMode = process.env.FABRIC_TEST_INGRESS_BARRIER ?? "independent";
if (!["observe", "blocked", "independent"].includes(barrierMode)) {
  throw new Error("FABRIC_TEST_INGRESS_BARRIER must be observe, blocked, or independent");
}
const cleanups: Array<() => Promise<unknown> | void> = [];
const roots: string[] = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
// Event sources below are fs.watch, incoming JSON lines, and the real control handler.
// Timers are finite observation deadlines or explicitly injected service delays, never polling.
const within = async <T>(work: Promise<T>, label: string, ms = 8_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Ingress harness timed out: ${label}`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const journal = <T>() => {
  const entries: T[] = [];
  const listeners = new Set<() => void>();
  return {
    entries,
    push(entry: T) { entries.push(entry); for (const changed of [...listeners]) changed(); },
    async wait(check: (entry: T) => boolean, label: string, ms?: number): Promise<T> {
      let changed!: () => void;
      const work = new Promise<T>(resolve => {
        changed = () => { const match = entries.find(check); if (match !== undefined) resolve(match); };
        listeners.add(changed);
        changed();
      });
      try { return await within(work, label, ms); }
      finally { listeners.delete(changed); }
    },
  };
};
const dataOf = (event: MeshEvent): Record<string, unknown> => event.data as Record<string, unknown>;
const commandIdOf = (event: MeshEvent): string => String(dataOf(event).commandId);
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;

// Same native file-participant + host/lease setup as spokes/latency/notifications tests.
const native = async (store: MeshStore, name: string) => {
  const now = Date.now();
  const identity: MeshIdentity = { id: `session:ingress-${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const ownerIncarnation = `fixture:${identity.id}`;
  const host = { format: 1, id: identity.id, rootId: identity.id, identity,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 };
  await store.put({ key: key("hosts", identity.id), identity, value: host });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt: now, updatedAt: now, expiresAt: host.expiresAt });
  const entry = { key: key("participants", identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    ownerIncarnation, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } };
  writeParticipantFile(store.root, entry);
  return { identity, ownerIncarnation, entry };
};
type Native = Awaited<ReturnType<typeof native>>;
interface Sample {
  id: string;
  submittedAt: number;
  publishedAt?: number;
  dispatchedAt?: number;
  acknowledgedAt?: number;
  mirrorDeferrals: number;
  admissions: number;
  expectedSender?: string;
}
interface RpcFrame { id: number; op: string; args?: unknown; sentAt: number }
interface RpcTiming {
  id: number;
  op: string;
  kind: string;
  queuedAt: number;
  startedAt: number;
  endedAt: number;
  queueMs: number;
  runMs: number;
  queueDepth: number;
  requestBytes: number;
  hosts?: number;
  participants?: number;
  presenceBytes?: number;
}

const observeStore = (store: MeshStore) => {
  const log = journal<MeshEvent>();
  let after = 0, failure: Error | undefined;
  const scan = () => {
    try {
      for (;;) {
        const page = store.read({ after, limit: 256 });
        if (!page.length) return;
        for (const event of page) { after = event.sequence; log.push(event); }
      }
    } catch (error) { failure = error instanceof Error ? error : new Error(String(error)); }
  };
  const close = watchBridgeStore(store.root, kind => { if (kind === "events") scan(); }, error => { failure = error; });
  scan();
  return { ...log, close, failure: () => failure, scan };
};

// The only injected seam. Await the gate BEFORE entering the actual StoreBridgeSide
// presence/mirror method (and therefore before its real MeshStore critical section).
// In held tests an unrelated native publish completes while the gate remains shut.
const holdService = (side: StoreBridgeSide, op: "presence" | "mirror") => {
  const entered = deferred<void>(), released = deferred<void>();
  const presence = side.presence.bind(side), mirror = side.mirror.bind(side);
  let first = true, timer: ReturnType<typeof setTimeout> | undefined;
  const pause = async () => {
    if (!first) return;
    first = false;
    entered.resolve();
    await released.promise;
  };
  if (op === "presence") side.presence = async () => { await pause(); return presence(); };
  else side.mirror = async (...args) => { await pause(); await mirror(...args); };
  return {
    entered: entered.promise,
    release() { if (timer !== undefined) clearTimeout(timer); released.resolve(); },
    releaseAfter(ms: number) { timer = setTimeout(() => released.resolve(), ms); },
    restore() { side.presence = presence; side.mirror = mirror; },
  };
};
type Hold = ReturnType<typeof holdService>;

const fixture = async (legacyRpc = false, callTimeoutMs = 15_000) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-ingress-"));
  roots.push(root);
  // Explicit file backend: never inherit a developer's SQLite/live mesh configuration.
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 256, { stateBackend: "file" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 256, { stateBackend: "file" });
  // Dynamic clocks let the expiry regression advance wall time without fake RPC timers.
  const local = new StoreBridgeSide(hub, "far", () => Date.now()), agent = new StoreBridgeSide(far, "hub", () => Date.now());
  const input = new PassThrough(), output = new PassThrough();
  const wire = journal<RpcFrame>(), timings = journal<RpcTiming>(), logs = journal<string>();
  const responses = journal<{ id: number; op: string; ok: boolean; eventIds: string[] }>();
  let buffered = "", responseBuffer = "";
  input.setEncoding("utf8");
  input.on("data", (chunk: string) => {
    buffered += chunk;
    let newline: number;
    while ((newline = buffered.indexOf("\n")) >= 0) {
      const line = buffered.slice(0, newline); buffered = buffered.slice(newline + 1);
      if (line.trim()) wire.push({ ...JSON.parse(line), sentAt: performance.now() } as RpcFrame);
    }
  });
  output.setEncoding("utf8");
  output.on("data", (chunk: string) => {
    responseBuffer += chunk;
    let newline: number;
    while ((newline = responseBuffer.indexOf("\n")) >= 0) {
      const line = responseBuffer.slice(0, newline); responseBuffer = responseBuffer.slice(newline + 1);
      if (!line.trim()) continue;
      const frame = JSON.parse(line) as { id?: number; ok?: boolean; result?: { events?: MeshEvent[] } };
      const request = wire.entries.find(request => request.id === frame.id);
      if (request) responses.push({ id: request.id, op: request.op, ok: frame.ok === true,
        eventIds: (request.op === "read" || request.op === "tail") && Array.isArray(frame.result?.events)
          ? frame.result.events.map(event => event.id) : [] });
    }
  });
  // Compatible with the three-argument base revision too; the benchmark explicitly
  // refuses missing telemetry instead of inventing server queue/start measurements.
  const serveWithDebug: (side: StoreBridgeSide, input: PassThrough, output: PassThrough,
    debug?: (message: string) => void) => Promise<void> = serveBridgeAgent;
  const servedOutput = legacyRpc ? new PassThrough() : output;
  if (legacyRpc) servedOutput.on("data", chunk => {
    // A genuine old hello lacks the additive scheduling capability. Keep the rest
    // of the real agent/store path, but require the client to retain its old gate.
    const frame = JSON.parse(String(chunk));
    if (frame.result?.independentRpc === true) delete frame.result.independentRpc;
    output.write(`${JSON.stringify(frame)}\n`);
  });
  const serving = serveWithDebug(agent, input, servedOutput, message => {
    if (message.startsWith("bridge rpc ")) timings.push(JSON.parse(message.slice("bridge rpc ".length)) as RpcTiming);
  });
  const remote = new RemoteBridgeSide(output, input, callTimeoutMs);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(root, "cursor.json"), stopMs: 100, log: message => logs.push(message) });
  const holds: Hold[] = [], planes: FabricControlPlane[] = [];
  const acceptanceGates = new Map<string, Promise<void>>();
  let running: Promise<void> | undefined, runFailure: unknown, closed = false;
  let hubLog: ReturnType<typeof observeStore> | undefined, farLog: ReturnType<typeof observeStore> | undefined;
  const close = async () => {
    if (closed) return;
    closed = true;
    for (const hold of holds.slice().reverse()) { hold.release(); hold.restore(); }
    hubLog?.close(); farLog?.close();
    await bridge.stop();
    if (running) await running;
    for (const plane of planes) await plane.close();
    remote.close(); input.end(); await serving; output.end();
    hub.closeState(); far.closeState();
  };
  cleanups.push(close);
  const lead = await native(hub, "hub"), lane = await native(far, "far");
  await remote.hello();
  await bridge.start();
  expect(local.holds(lane.identity.id, lane.identity)).toBe(true);
  expect(agent.holds(lead.identity.id, lead.identity)).toBe(true);
  hubLog = observeStore(hub); farLog = observeStore(far);
  const samples = new Map<string, Sample>();
  const admitted = journal<string>();
  for (const [store, owner] of [[hub, lead], [far, lane]] as const) {
    const plane = new FabricControlPlane(store, owner.identity, { enabled: true, hostId: owner.identity.id,
      ownerIncarnation: owner.ownerIncarnation, controlIncarnationFence: "enforce", acknowledgementTimeoutMs: 30_000 });
    plane.start(async (command, from) => {
      const sample = samples.get(command.commandId);
      if (!sample) throw new Error(`Unexpected ingress command ${command.commandId}`);
      expect(from.id).toBe(sample.expectedSender ?? (store === far ? lead.identity.id : lane.identity.id));
      expect(from.verified).toBe("bridge");
      sample.dispatchedAt = performance.now(); sample.admissions++;
      admitted.push(command.commandId);
      await acceptanceGates.get(command.commandId);
      return { accepted: true, messageId: `ingress:${command.commandId}` };
    });
    planes.push(plane);
  }
  return {
    root, hub, far, local, agent, remote, bridge, lead, lane, hubLog, farLog, wire, timings, logs, responses, samples, admitted, acceptanceGates, close,
    hold(op: "presence" | "mirror", side = agent) { const hold = holdService(side, op); holds.push(hold); return hold; },
    async launch() {
      running = bridge.run().catch(error => { runFailure = error; });
      // A direction's first actual wire read occurs only after startup subscriptions
      // and presence settle. No settle sleeps or manual step() drive the live run path.
      await wire.wait(frame => frame.op === "tail" || frame.op === "read", "run direction started");
      await remote.latestSequence(); // FIFO marker: startup read telemetry has settled too.
    },
    assertHealthy() {
      expect(runFailure).toBeUndefined(); expect(hubLog!.failure()).toBeUndefined(); expect(farLog!.failure()).toBeUndefined();
    },
  };
};
type Fixture = Awaited<ReturnType<typeof fixture>>;

const command = (source: Native, target: Native, id: string) => ({
  topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
  data: { version: 1, commandId: id, targetId: target.identity.id, operation: "followUp", replyTo: source.identity.id,
    ownerIncarnation: target.ownerIncarnation, message: `ingress ${id}`, requestedAt: Date.now(), deadlineAt: Date.now() + 30_000 },
});
const bridged = (event: MeshEvent, from: string): BridgePublish => ({ topic: event.topic, kind: event.kind,
  from: event.from, to: event.to!, data: { ...dataOf(event), bridge: { from, id: event.id } } });
const newSample = (f: Fixture, id: string): Sample => {
  const sample = { id, submittedAt: performance.now(), mirrorDeferrals: 0, admissions: 0 };
  f.samples.set(id, sample); return sample;
};
const ackFor = (id: string) => (event: MeshEvent) => event.topic === "fabric.control.ack" && commandIdOf(event) === id;
const verifyAck = (ack: MeshEvent, target: Native, source: Native, id: string) => {
  expect(ack).toMatchObject({ from: { id: target.identity.id }, to: source.identity.id, verification: "bridge",
    data: { commandId: id, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation,
      accepted: true, messageId: `ingress:${id}` } });
};
const isDispatchDeferred = (error: unknown): boolean =>
  error instanceof BridgeDispatchDeferredError && error.code === "FABRIC_BRIDGE_DISPATCH_DEFERRED" && error.sent === false;

// Direct RPC: native source event -> RemoteBridgeSide.publish -> real owner handler/ACK
// -> remote read -> local publication with the real sender ownership fence.
const directCommand = async (f: Fixture, id: string, serviceDone: Promise<unknown>) => {
  const sample = newSample(f, id);
  const origin = await f.hub.publish(command(f.lead, f.lane, id));
  sample.publishedAt = performance.now();
  try { await f.remote.publish(bridged(origin, "hub")); }
  catch (error) {
    if (!isDispatchDeferred(error)) throw error;
    sample.mirrorDeferrals++;
    await serviceDone; // one bounded retry of the SAME valid event, not a polling loop
    await f.remote.publish(bridged(origin, "hub"));
  }
  await f.farLog.wait(ackFor(id), `owner ACK ${id}`);
  // Mirror's client-side gate also covers reads; wait for this finite presence pass.
  await serviceDone;
  const page = await f.remote.read(0);
  const ack = page.events.find(ackFor(id));
  expect(ack, `real ACK in stdio read for ${id}`).toBeDefined();
  await f.local.publish(bridged(ack!, "far"), [f.lane.identity.id]);
  // Fresh durable reads, not only watcher delivery or mock calls.
  f.hubLog.scan();
  const delivered = await f.hubLog.wait(ackFor(id), `hub ACK ${id}`);
  verifyAck(delivered, f.lane, f.lead, id);
  sample.acknowledgedAt = performance.now();
};
const inboundCommand = async (f: Fixture, id: string) => {
  const sample = newSample(f, id);
  await f.far.publish(command(f.lane, f.lead, id));
  sample.publishedAt = performance.now();
  const ack = await f.farLog.wait(ackFor(id), `inbound round-trip ACK ${id}`);
  verifyAck(ack, f.lead, f.lane, id);
  sample.acknowledgedAt = performance.now();
};
const churn = (f: Fixture, wave: number) => writeParticipantFile(f.hub.root, {
  ...f.lead.entry, updatedAt: Date.now() + wave + 1, value: { ...f.lead.entry.value,
    label: `HUB ingress wave ${wave}`, status: wave % 2 ? "idle" : "running", updatedAt: Date.now() + wave + 1 },
});
const directPresence = async (f: Fixture) => {
  const claimed = await f.remote.presence();
  await f.local.mirror(claimed);
  await f.remote.mirror(await f.local.presence());
};

const stats = (values: number[]) => {
  expect(values.length).toBeGreaterThan(0);
  expect(values.every(value => Number.isFinite(value) && value >= 0)).toBe(true);
  const sorted = values.slice().sort((a, b) => a - b);
  return { n: sorted.length, p95Ms: +sorted[Math.ceil(sorted.length * .95) - 1]!.toFixed(2),
    maxMs: +sorted.at(-1)!.toFixed(2) };
};
const summarize = (f: Fixture, path_: "direct-rpc" | "run-inbound", delay: "none" | "seeded", peak: number, delays: number[]) => {
  const samples = [...f.samples.values()];
  expect(samples).toHaveLength(COMMANDS);
  expect(samples.every(sample => sample.admissions === 1 && sample.dispatchedAt !== undefined && sample.acknowledgedAt !== undefined)).toBe(true);
  expect(peak).toBe(IN_FLIGHT);
  const measured = f.timings.entries.filter(timing => f.wire.entries.some(frame => frame.id === timing.id &&
    (path_ === "direct-rpc" ? frame.op === "publish" &&
      (frame.args as BridgePublish).topic === "fabric.control.command" : frame.op === "tail" || frame.op === "read")));
  expect(measured.length, "serveBridgeAgent debug callback must emit 'bridge rpc '+JSON; no synthesized queue timings").toBeGreaterThan(0);
  const rpcKinds: Record<string, number> = {};
  for (const timing of f.timings.entries) {
    // Host instrumentation exposes scheduling metadata only, never command/ACK bodies.
    const required = ["id", "op", "kind", "queuedAt", "startedAt", "endedAt", "queueMs", "runMs", "queueDepth", "requestBytes"];
    expect(Object.keys(timing)).toEqual(expect.arrayContaining(required));
    expect(Object.keys(timing).every(key => [...required, "hosts", "participants", "presenceBytes"].includes(key))).toBe(true);
    expect([timing.queuedAt, timing.startedAt, timing.endedAt, timing.queueMs, timing.runMs]
      .every(value => Number.isFinite(value) && value >= 0)).toBe(true);
    expect(Number.isInteger(timing.queueDepth) && timing.queueDepth >= 0).toBe(true);
    expect(Number.isInteger(timing.requestBytes) && timing.requestBytes > 0).toBe(true);
    expect(timing.queuedAt).toBeLessThanOrEqual(timing.startedAt);
    expect(timing.startedAt).toBeLessThanOrEqual(timing.endedAt);
    rpcKinds[timing.kind] = (rpcKinds[timing.kind] ?? 0) + 1;
  }
  if (path_ === "direct-rpc") expect(measured).toHaveLength(COMMANDS);
  f.assertHealthy();
  return { path: path_, delay, commands: COMMANDS, maxInFlight: IN_FLIGHT, peakInFlight: peak,
    presenceChurn: { waves: WAVES, nativeParticipantWrites: WAVES, injectedServiceMs: delays },
    rpcKinds, maxRpcQueueDepth: Math.max(...f.timings.entries.map(timing => timing.queueDepth)),
    mirrorShape: f.timings.entries.filter(timing => timing.op === "mirror").map(({ hosts, participants, presenceBytes }) => ({ hosts, participants, presenceBytes })),
    mirrorDeferrals: samples.reduce((n, sample) => n + sample.mirrorDeferrals, 0),
    rpcSelection: path_ === "direct-rpc" ? "command publish" : "inbound read/tail (pages, not commands)",
    queue: stats(measured.map(timing => timing.queueMs)), run: stats(measured.map(timing => timing.runMs)),
    presenceService: stats(f.timings.entries.filter(timing => timing.op === "presence" || timing.op === "mirror").map(timing => timing.runMs)),
    wireToStart: stats(measured.map(timing => timing.startedAt - f.wire.entries.find(frame => frame.id === timing.id)!.sentAt)),
    submitToDispatch: stats(samples.map(sample => sample.dispatchedAt! - sample.submittedAt)),
    submitToAck: stats(samples.map(sample => sample.acknowledgedAt! - sample.submittedAt)),
    sourcePublication: stats(samples.map(sample => sample.publishedAt! - sample.submittedAt)),
  };
};

const benchmarkCase = async (path_: "direct-rpc" | "run-inbound", delay: "none" | "seeded") => {
  const f = await fixture();
  if (path_ === "run-inbound") await f.launch();
  // Skip startup telemetry; keep only the bounded work window, not stop()/withdrawal.
  f.wire.entries.length = 0; f.timings.entries.length = 0;
  let seed = SEED, active = 0, peak = 0;
  const delays: number[] = [];
  for (let wave = 0; wave < WAVES; wave++) {
    seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
    const ms = delay === "seeded" ? 100 + (seed >>> 0) % 201 : 0;
    delays.push(ms);
    // Direct alternates slow presence and slow mirror, exposing FIFO vs local guard.
    // run() always holds the outbound mirror while native far commands arrive inbound.
    const hold = f.hold(path_ === "direct-rpc" && wave % 2 === 0 ? "presence" : "mirror");
    churn(f, wave); // exactly one real native participant-file change per wave
    let service: Promise<unknown> | undefined = path_ === "direct-rpc" ? directPresence(f) : undefined;
    void service?.catch(() => undefined);
    // For run-inbound, only the real file notification starts this presence pass.
    await within(hold.entered, "presence service entered");
    service ??= f.bridge.syncPresence(); // join the already-held run() pass, never drive step()
    void service.catch(() => undefined);
    const serviceDone = service;
    hold.releaseAfter(ms); // Explicitly INJECTED 100-300 ms service delay, not fleet latency.
    try {
      await Promise.all(Array.from({ length: IN_FLIGHT }, async (_, index) => {
        active++; peak = Math.max(peak, active);
        try {
          const id = `${path_}-${delay}-${wave * IN_FLIGHT + index}`;
          if (path_ === "direct-rpc") await directCommand(f, id, serviceDone);
          else await inboundCommand(f, id);
        } finally { active--; }
      }));
      await serviceDone;
    } finally { hold.release(); hold.restore(); }
  }
  expect(active).toBe(0);
  // Check persisted commands/ACKs and exactly-once handler delivery for all 50, not only timings.
  const destination = path_ === "direct-rpc" ? f.far : f.hub;
  const source = path_ === "direct-rpc" ? f.hub : f.far;
  const commands = destination.read({ topic: "fabric.control.command", after: 0, limit: 256 });
  const acks = source.read({ topic: "fabric.control.ack", after: 0, limit: 256 });
  expect(commands).toHaveLength(COMMANDS); expect(acks).toHaveLength(COMMANDS);
  expect(commands.map(commandIdOf)).toEqual(source.read({ topic: "fabric.control.command", after: 0, limit: 256 }).map(commandIdOf));
  expect(new Set(commands.map(commandIdOf)).size).toBe(COMMANDS);
  expect(new Set(acks.map(commandIdOf))).toEqual(new Set(commands.map(commandIdOf)));
  expect(commands.every(event => event.verification === "bridge")).toBe(true);
  const summary = summarize(f, path_, delay, peak, delays);
  await f.close(); // No preceding live run/watch/lease-renewal can contaminate the next case.
  return summary;
};

// This probe does not bake the existing failure into CI. "blocked" is a deliberate
// pre-fix characterization; "independent" holds the barrier until delivery and is a
// deterministic regression gate. Default requires progress before releasing.
const heldObservation = async (f: Fixture, id: string) => {
  if (barrierMode === "independent") {
    await f.admitted.wait(value => value === id, "dispatch must not await held presence service", 1_000);
  } else {
    // Reach another event-loop check phase AFTER the valid source publication/wire
    // admission. No timing threshold is used as a performance assertion.
    await turn(); await turn();
    if (barrierMode === "blocked") expect(f.samples.get(id)!.admissions).toBe(0);
  }
};

describe("mesh bridge ingress: real file meshes and stdio", () => {
  it("refuses protected legacy traffic UNSENT while the original mirror owns admission", async () => {
    const f = await fixture(true), hold = f.hold("mirror");
    const pending = f.remote.mirror(await f.local.presence());
    void pending.catch(() => undefined);
    try {
      await within(hold.entered, "legacy mirror pending");
      const before = f.wire.entries.length;
      await expect(f.remote.read(0)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false, reason: "mirror-pending" });
      expect(f.wire.entries).toHaveLength(before);
    } finally { hold.release(); hold.restore(); await pending; }
    f.assertHealthy();
  });

  it.each(["presence", "mirror"] as const)("direct remote: held %s permits presence parallelism, but mirror retains UNSENT command until release", async op => {
    const f = await fixture(), hold = f.hold(op);
    const snapshot = await f.local.presence();
    const service = op === "presence" ? f.remote.presence() : f.remote.mirror(snapshot);
    void service.catch(() => undefined);
    await within(hold.entered, `${op} held outside store lock`);
    const id = `held-direct-${op}`, sample = newSample(f, id);
    try {
      const origin = await f.hub.publish(command(f.lead, f.lane, id));
      // A real native write to the SERVED store succeeds while its service is held:
      // the delay cannot be attributed to a held MeshStore critical section.
      await f.far.publish({ topic: "ingress.lock-probe", kind: "probe", from: f.lane.identity });
      const attempt = f.remote.publish(bridged(origin, "hub"));
      const result = attempt.then(() => ({ delivered: true as const }), error => ({ delivered: false as const, error }));
      // A protected presence is not a mirror: other protected calls remain concurrent.
      // A pending mirror instead refuses the command before any frame or handler entry.
      if (op === "presence") await f.wire.wait(frame => frame.op === "publish", "valid control queued on stdio");
      if (op === "mirror") {
        const outcome = await within(result, "UNSENT mirror scheduling refusal", 1_000);
        expect(outcome.delivered).toBe(false);
        if (!outcome.delivered) expect(isDispatchDeferred(outcome.error)).toBe(true);
        expect(f.wire.entries.some(frame => frame.op === "publish")).toBe(false);
        expect(sample.admissions).toBe(0);
        expect(f.far.read({ topic: "fabric.control.command", limit: 256 })).toHaveLength(0);
      } else {
        if (barrierMode === "independent") expect((await within(result, "control during held presence", 1_000)).delivered).toBe(true);
        await heldObservation(f, id);
      }
      hold.release(); await service;
      const outcome = await result;
      if (!outcome.delivered) {
        expect(isDispatchDeferred(outcome.error)).toBe(true);
        sample.mirrorDeferrals++;
        await f.remote.publish(bridged(origin, "hub"));
      }
      await f.farLog.wait(ackFor(id), "real owner ACK after held service");
      const ack = (await f.remote.read(0)).events.find(ackFor(id));
      expect(ack).toBeDefined();
      await f.local.publish(bridged(ack!, "far"), [f.lane.identity.id]);
      verifyAck(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })[0]!, f.lane, f.lead, id);
      expect(sample.admissions).toBe(1);
      expect(f.far.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(1);
      f.assertHealthy();
    } finally { hold.release(); hold.restore(); await service; }
  });

  it("run(): retains inbound native command behind an async outbound mirror, then delivers/ACKs after release", async () => {
    const f = await fixture();
    await f.launch();
    const hold = f.hold("mirror");
    // Real file notification triggers the normal presence path. No manual step() or
    // mocked subscription is needed to reproduce the shared-presence ingress stall.
    churn(f, 0);
    await within(hold.entered, "run outbound mirror held");
    const id = "held-run-inbound";
    const pending = inboundCommand(f, id);
    void pending.catch(() => undefined);
    try {
      await f.farLog.wait(event => event.topic === "fabric.control.command" && commandIdOf(event) === id,
        "native inbound command committed while outbound mirror held");
      await f.logs.wait(message => message.startsWith("bridge dispatch deferred (toLocal)"), "protected source read refused UNSENT");
      expect(f.samples.get(id)!.admissions).toBe(0);
      expect(f.hub.read({ topic: "fabric.control.command", limit: 256 })).toHaveLength(0);
      expect(f.far.read({ topic: "fabric.control.ack", limit: 256 })).toHaveLength(0);
      hold.release();
      await pending;
      expect(f.samples.get(id)!.admissions).toBe(1);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(1);
      f.assertHealthy();
    } finally { hold.release(); hold.restore(); }
  });

  it("refreshes a newcomer after an older successful pass, without admitting it from an uncommitted claim", async () => {
    const f = await fixture();
    await f.launch();
    const hold = f.hold("mirror");
    churn(f, 0);
    await within(hold.entered, "old presence snapshot awaiting outbound mirror");
    const newcomer = await native(f.far, "new-during-old-pass");
    const id = "new-root-during-successful-pass", sample = newSample(f, id);
    sample.expectedSender = newcomer.identity.id;
    const before = f.wire.entries.at(-1)!.id;
    try {
      await f.far.publish(command(newcomer, f.lead, id));
      await f.logs.wait(message => message.startsWith("bridge dispatch deferred (toLocal)"), "newcomer read retained by pending mirror");
      expect(f.wire.entries.some(frame => frame.id > before && (frame.op === "tail" || frame.op === "read"))).toBe(false);
      expect(f.local.holds(newcomer.identity.id)).toBe(false);
      expect(sample.admissions).toBe(0);
      // This pass succeeds normally, well before the call deadline: no timeout/late
      // response may be needed to trigger a genuinely new membership observation.
      hold.release();
      const ack = await f.farLog.wait(ackFor(id), "newcomer ACK after a fresh committed observation");
      verifyAck(ack, f.lead, newcomer, id);
      expect(sample.admissions).toBe(1);
      expect(f.local.holds(newcomer.identity.id, newcomer.identity)).toBe(true);
      f.assertHealthy();
    } finally { hold.release(); hold.restore(); }
  });

  it("run(): fast outbound command followed by a remote-to-hub ACK behind the held mirror", async () => {
    const f = await fixture();
    await f.launch();
    const id = "held-run-ack-ingress", sample = newSample(f, id), accept = deferred<void>();
    // A real asynchronous owner handler lets us place its ACK AFTER the outbound
    // command arrived and AFTER the mirror began. Publication itself is untouched.
    f.acceptanceGates.set(id, accept.promise);
    cleanups.push(() => accept.resolve());
    await f.hub.publish(command(f.lead, f.lane, id));
    await f.admitted.wait(value => value === id, "outbound control delivered before slow mirror");
    const hold = f.hold("mirror");
    churn(f, 0);
    await within(hold.entered, "outbound mirror held before remote ACK");
    try {
      accept.resolve();
      const nativeAck = await f.farLog.wait(ackFor(id), "real far owner ACK committed while mirror held");
      expect(nativeAck.verification).toBe("mesh");
      await f.logs.wait(message => message.startsWith("bridge dispatch deferred (toLocal)"), "durable ACK read blocked by mirror");
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.responses.entries.some(response => response.eventIds.includes(nativeAck.id))).toBe(false);
      hold.release();
      verifyAck(await f.hubLog.wait(ackFor(id), "remote-to-hub ACK after barrier release"), f.lane, f.lead, id);
      expect(sample.admissions).toBe(1);
      f.assertHealthy();
    } finally { accept.resolve(); hold.release(); hold.restore(); }
  });

  it("run(): post-deadline inbound command is UNSENT/retained until the ORIGINAL late mirror ACK, then delivers once", async () => {
    const f = await fixture(false, 500);
    await f.launch();
    const hold = f.hold("mirror");
    churn(f, 0);
    await within(hold.entered, "outbound mirror held past its deadline");
    const mirrorId = f.wire.entries.filter(frame => frame.op === "mirror").at(-1)!.id;
    try {
      // The real mirror timer settles the pass, not the retained response slot.
      await f.logs.wait(message => message.startsWith("bridge mirror degraded"), "real mirror deadline logged");
      await expect(f.remote.presence()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(true);
      await expect(f.remote.latestSequence()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      const id = "post-deadline-inbound";
      const delivery = inboundCommand(f, id);
      void delivery.catch(() => undefined);
      await f.farLog.wait(event => event.topic === "fabric.control.command" && commandIdOf(event) === id, "native source retained before late mirror ACK");
      expect(f.samples.get(id)!.admissions).toBe(0);
      expect(f.hub.read({ topic: "fabric.control.command", limit: 256 })).toHaveLength(0);
      expect(f.hub.read({ topic: "fabric.control.ack", limit: 256 })).toHaveLength(0);
      expect(f.responses.entries.some(response => response.id === mirrorId)).toBe(false);
      hold.release();
      await delivery;
      expect(f.samples.get(id)!.admissions).toBe(1);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(1);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toMatchObject([
        { verification: "mesh", data: { commandId: id, accepted: true } },
      ]);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(1);
      expect(f.responses.entries.some(response => response.id === mirrorId)).toBe(true);
      f.assertHealthy();
    } finally { hold.release(); hold.restore(); }
  }, 15_000);

  it("run(): real far-owner ACK is already durable after the deadline, but its protected read waits for mirror release", async () => {
    const f = await fixture(false, 500);
    await f.launch();
    const id = "post-deadline-ack", sample = newSample(f, id), accept = deferred<void>();
    f.acceptanceGates.set(id, accept.promise);
    cleanups.push(() => accept.resolve());
    // Deliver the actual outbound command first; only its real handler's completion is held.
    await f.hub.publish(command(f.lead, f.lane, id));
    await f.admitted.wait(value => value === id, "outbound command delivered before mirror deadline");
    const hold = f.hold("mirror");
    churn(f, 0);
    await within(hold.entered, "outbound mirror held before ACK completion");
    const mirrorId = f.wire.entries.filter(frame => frame.op === "mirror").at(-1)!.id;
    try {
      await f.logs.wait(message => message.startsWith("bridge mirror degraded"), "real mirror deadline before ACK");
      await expect(f.remote.presence()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(true);
      accept.resolve();
      const nativeAck = await f.farLog.wait(ackFor(id), "real far ACK committed after mirror deadline");
      expect(nativeAck.verification).toBe("mesh");
      expect(f.hub.read({ topic: "fabric.control.ack", limit: 256 })).toHaveLength(0);
      expect(f.responses.entries.some(response => response.eventIds.includes(nativeAck.id))).toBe(false);
      expect(f.responses.entries.some(response => response.id === mirrorId)).toBe(false);
      hold.release();
      verifyAck(await f.hubLog.wait(ackFor(id), "already-durable ACK ingress after original mirror release"), f.lane, f.lead, id);
      await f.responses.wait(response => response.ok && response.eventIds.includes(nativeAck.id), "actual far ACK read response");
      expect(sample.admissions).toBe(1);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(1);
      expect(f.responses.entries.some(response => response.id === mirrorId)).toBe(true);
      f.assertHealthy();
    } finally { accept.resolve(); hold.release(); hold.restore(); }
  }, 15_000);

  it("run(): a root born after a timed-out snapshot waits for a fresh local mirror commit", async () => {
    const f = await fixture(false, 500);
    await f.launch();
    const oldMirror = f.hold("mirror");
    churn(f, 0);
    await within(oldMirror.entered, "old snapshot held before newcomer exists");
    let freshLocal: Hold | undefined;
    try {
      await f.logs.wait(message => message.startsWith("bridge mirror degraded"), "old snapshot's real mirror deadline");
      await expect(f.remote.presence()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      // This root cannot have appeared in either half of the old snapshot.
      const newcomer = await native(f.far, "new-after-deadline");
      const id = "post-deadline-new-root", sample = newSample(f, id);
      sample.expectedSender = newcomer.identity.id;
      freshLocal = f.hold("mirror", f.local);
      const origin = await f.far.publish(command(newcomer, f.lead, id));
      await expect(f.remote.read(0)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      expect(f.responses.entries.some(response => response.eventIds.includes(origin.id))).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "cursor.json"), "utf8")).toLocal.after).toBeLessThan(origin.sequence);
      expect(sample.admissions).toBe(0);
      expect(f.local.holds(newcomer.identity.id, newcomer.identity)).toBe(false);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);

      oldMirror.release();
      await within(freshLocal.entered, "fresh newcomer claims held before local store critical section");
      // New claims alone cannot authorize traffic: no committed local mirror yet.
      expect(sample.admissions).toBe(0);
      expect(f.local.holds(newcomer.identity.id, newcomer.identity)).toBe(false);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      freshLocal.release();
      verifyAck(await f.farLog.wait(ackFor(id), "newcomer ACK after fresh local commit"), f.lead, newcomer, id);
      await f.responses.wait(response => response.ok && response.eventIds.includes(origin.id), "new root read only after original mirror release");
      expect(sample.admissions).toBe(1);
      expect(f.local.holds(newcomer.identity.id, newcomer.identity)).toBe(true);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(1);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toMatchObject([
        { verification: "mesh", data: { commandId: id, accepted: true } },
      ]);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(1);
      f.assertHealthy();
    } finally { freshLocal?.release(); freshLocal?.restore(); oldMirror.release(); oldMirror.restore(); }
  }, 15_000);

  it("run(): expired authority and the late old sample wait for a fresh local mirror commit", async () => {
    const f = await fixture(false, 500);
    await f.launch();
    const oldMirror = f.hold("mirror");
    churn(f, 0);
    await within(oldMirror.entered, "old mirror received before expiry jump");
    let freshLocal: Hold | undefined, freshRemote: Hold | undefined;
    try {
      await f.logs.wait(message => message.startsWith("bridge mirror degraded"), "real mirror deadline before expiry jump");
      await expect(f.remote.presence()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(true);
      const beforeJump = Date.now(), realNow = Date.now.bind(Date);
      vi.spyOn(Date, "now").mockImplementation(() => realNow() + BRIDGE_LEASE_MS + 1);
      // Only wall time moves, AFTER the real RPC deadline. Native 120 s leases stay live.
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(false);
      expect(f.agent.holds(f.lead.identity.id, f.lead.identity)).toBe(false);
      freshLocal = f.hold("mirror", f.local);
      // Hold the next agent mirror too, so its fresh commit cannot hide whether the
      // late old mirror retained the agent's original pre-hold receivedAt stamp.
      freshRemote = f.hold("mirror");
      const id = "post-deadline-expired-root", sample = newSample(f, id);
      const origin = await f.far.publish(command(f.lane, f.lead, id)); // New, valid 30 s command AFTER the jump.
      await expect(f.remote.read(0)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_DISPATCH_DEFERRED", sent: false });
      expect(f.responses.entries.some(response => response.eventIds.includes(origin.id))).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "cursor.json"), "utf8")).toLocal.after).toBeLessThan(origin.sequence);
      expect(sample.admissions).toBe(0);
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(false);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);

      oldMirror.release();
      // Reciprocal admission can refuse the fresh remote mirror while a protected
      // read is still in flight. Do not demand simultaneous entry (that would deadlock
      // this harness): local commit is held, remote commit is either gated or held.
      await within(freshLocal.entered, "fresh local claims held before local mirror critical section");
      const lateLease = readHostLease(f.far.root, f.lead.identity.id)!;
      expect(lateLease.mirror!.observedAt).toBeLessThanOrEqual(beforeJump);
      expect(lateLease.expiresAt).toBeLessThanOrEqual(beforeJump + BRIDGE_LEASE_MS);
      expect(lateLease.expiresAt).toBeLessThan(Date.now());
      expect(f.agent.holds(f.lead.identity.id, f.lead.identity)).toBe(false);
      expect(sample.admissions).toBe(0);
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(false);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(0);
      freshRemote.release(); freshLocal.release();
      verifyAck(await f.farLog.wait(ackFor(id), "expired root ACK only after fresh local commit"), f.lead, f.lane, id);
      await f.responses.wait(response => response.ok && response.eventIds.includes(origin.id), "expired root read after original mirror release");
      expect(sample.admissions).toBe(1);
      expect(f.local.holds(f.lane.identity.id, f.lane.identity)).toBe(true);
      expect(f.hub.read({ topic: "fabric.control.command", after: 0, limit: 256 })).toHaveLength(1);
      expect(f.hub.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toMatchObject([
        { verification: "mesh", data: { commandId: id, accepted: true } },
      ]);
      expect(f.far.read({ topic: "fabric.control.ack", after: 0, limit: 256 })).toHaveLength(1);
      f.assertHealthy();
    } finally {
      freshRemote?.release(); freshRemote?.restore(); freshLocal?.release(); freshLocal?.restore();
      oldMirror.release(); oldMirror.restore();
    }
  }, 15_000);

  it.skipIf(process.env.FABRIC_TEST_INGRESS_BENCH !== "1")("50/10: compare queue/start/dispatch before and during injected presence service delay", async () => {
    const cases = [];
    // Sequential independent fixtures avoid letting one benchmark contaminate another.
    for (const path_ of ["direct-rpc", "run-inbound"] as const) {
      for (const delay of ["none", "seeded"] as const) cases.push(await benchmarkCase(path_, delay));
    }
    const kinds = new Set(cases.flatMap(result => Object.keys(result.rpcKinds)));
    for (const kind of ["control", "ack", "mirror", "presence", "read"]) expect(kinds.has(kind), `RPC timing classification: ${kind}`).toBe(true);
    console.log(JSON.stringify({ benchmark: "mesh-bridge-ingress", commands: COMMANDS, maxInFlight: IN_FLIGHT,
      seed: SEED, clock: "performance.now", injectedDelay: "100-300ms service delay BEFORE the real store critical section",
      cases }));
  }, 60_000);
});
