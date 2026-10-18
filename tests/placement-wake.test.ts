// Keep reason: authenticated Path B trust boundary and durable-before-ACK data safety.
// Real FabricControlPlane.start/inbox/close and on-disk MeshStore; fake broker deliveries,
// not a live NATS authorization/credential test. Event gates drive checks; timers only bound failures.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { afterEach, beforeEach, describe, it } from "node:test";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane, type FabricControlPlaneOptions, type FabricPlacementWake } from "../src/topology/control-plane.js";
import type { ParticipantView } from "../src/topology/participant-view.js";
import { TransportUnsupportedError, type FabricTransport, type TransportDelivery, type TransportEnvelope,
  type TransportKv, type TransportSent } from "../src/transport/types.js";

const HOST = "runtime:placement-owner";
const TARGET = "actor:placement-target";
const SOURCE = "placement-writer";
const LAUNCH = "semantic-launch-123";
const GENERATION = 7;
const WAKE_KEY = "tests/placement-wake";
const identity: MeshIdentity = { id: "session:owner", kind: "main", name: "owner" };
const wake = (): FabricPlacementWake => ({ version: 1, launchId: LAUNCH, generation: GENERATION,
  reason: "eligible", at: "2030-01-02T03:04:05.123456Z" });
const idFor = (body: Record<string, unknown>): string =>
  `placement-wake:${String(body.launchId)}:${String(body.generation)}:${String(body.reason)}`;
function envelope(body: unknown = wake(), patch: Partial<TransportEnvelope> = {}): TransportEnvelope {
  return { id: idFor((body ?? {}) as Record<string, unknown>), kind: "command", operation: "placement.wake",
    to: { machine: "owner-machine", hostId: HOST }, targetId: TARGET,
    // Deliberately absent from ParticipantView. This label is not placement authority.
    from: { id: "unregistered-json-label", kind: "main", name: "not-authority" }, body, ...patch };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("placement delivery event did not settle")), 3_000);
    })]);
  } finally { clearTimeout(timer); }
}
type Disposition = "ack" | "nak" | "term";
class FakeNatsTransport implements FabricTransport {
  readonly kind = "nats" as const;
  readonly self = { machine: "owner-machine", hostId: HOST };
  readonly sent: TransportEnvelope[] = [];
  readonly queue = new Readable({ objectMode: true, read() {} });
  sequence = 0;
  deliver(payload: TransportEnvelope, sourceMachine = SOURCE, beforeAck?: () => void, attempt = 1) {
    const done = deferred<Disposition>();
    const state = { acks: 0, naks: [] as Array<number | undefined>, terms: [] as string[],
      outcome: done.promise };
    const delivery: TransportDelivery<TransportEnvelope> = {
      payload, sourceMachine, verification: "bridge", attempt, sequence: ++this.sequence, working() {},
      async ack() { beforeAck?.(); state.acks++; done.resolve("ack"); },
      nak(delay) { state.naks.push(delay); done.resolve("nak"); },
      async term(reason) { state.terms.push(reason); done.resolve("term"); },
    };
    this.queue.push(delivery);
    return state;
  }
  async *inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>> {
    const abort = () => { if (!this.queue.destroyed && !this.queue.readableEnded) this.queue.push(null); };
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    try { for await (const delivery of this.queue as AsyncIterable<TransportDelivery<TransportEnvelope>>) yield delivery; }
    finally { signal.removeEventListener("abort", abort); }
  }
  async send<T>(input: TransportEnvelope<T>): Promise<TransportSent<T>> {
    const createdAt = Date.now();
    const body = typeof input.body === "function" ? (input.body as (at: number) => T)(createdAt) : input.body;
    const resolved = { ...input, body, createdAt };
    this.sent.push(resolved);
    return { id: input.id, sequence: ++this.sequence, duplicate: false, envelope: resolved };
  }
  kv(): TransportKv { throw new TransportUnsupportedError("Wake fixture does not use KV"); }
  async drainState() { return { pending: 0, ackPending: 0 }; }
  async close(): Promise<void> { this.queue.destroy(); }
}
const noParticipantLookup: ParticipantView = {
  leaseLive: () => true,
  resolve() { throw new Error("Placement wakes must not resolve an ordinary participant sender/target"); },
  async whenChanged() { throw new Error("No participant watch expected"); },
};
const fixtures: Fixture[] = [];
class Fixture {
  readonly root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "placement-wake-"));
  readonly mesh = new MeshStore(this.root, 64 * 1024, 1_000, { stateBackend: "file" });
  readonly transport = new FakeNatsTransport();
  readonly plane: FabricControlPlane;
  ordinaryCalls = 0;
  constructor(options: Partial<FabricControlPlaneOptions> = {}) {
    fixtures.push(this);
    this.plane = new FabricControlPlane(this.mesh, identity, { enabled: true, hostId: HOST,
      ownerIncarnation: "placement-owner-v1", transport: this.transport, participants: noParticipantLookup,
      relayOperatorMachine: "fabric-ops", placementWakeSourceMachine: SOURCE, ...options });
    this.plane.start(() => { this.ordinaryCalls++; return { accepted: true, messageId: "ordinary-result" }; });
  }
  async persist(targetId: string, received: FabricPlacementWake): Promise<boolean> {
    if (targetId !== TARGET || received.launchId !== LAUNCH || received.generation !== GENERATION) return false;
    await this.mesh.put({ key: WAKE_KEY, value: { targetId, wake: received }, identity });
    return true;
  }
  diskWake(): unknown {
    const fresh = new MeshStore(this.root, 64 * 1024, 1_000, { stateBackend: "file" });
    try { return fresh.get(WAKE_KEY, { fresh: true })?.value; }
    finally { fresh.closeState(); }
  }
  async dispose(): Promise<void> {
    await this.plane.close();
    await this.transport.close();
    this.mesh.closeState();
    fs.rmSync(this.root, { recursive: true, force: true });
  }
}
let warnings: string[] = [];
const originalWarn = diagnostics.warn;
beforeEach(() => { warnings = []; diagnostics.warn = (_source, ...args) => { warnings.push(args.join(" ")); }; });
afterEach(async () => {
  try { for (const fixture of fixtures.splice(0)) await fixture.dispose(); }
  finally { diagnostics.warn = originalWarn; }
});

async function ackWithoutControl(f: Fixture, payload: TransportEnvelope, source = SOURCE, attempt = 5): Promise<void> {
  const receipt = f.transport.deliver(payload, source, undefined, attempt);
  assert.equal(await bounded(receipt.outcome), "ack");
  assert.equal(receipt.acks, 1);
  assert.deepEqual(receipt.naks, []);
  assert.deepEqual(receipt.terms, []);
  assert.equal(f.ordinaryCalls, 0);
  assert.deepEqual(f.transport.sent, []); // Never emit an ordinary control ACK.
}

describe("Path B placement.wake receiver", () => {
  it("delivers an authenticated wake directly and persists the exact owner record before ACK", async () => {
    const entered = deferred<void>(), commit = deferred<void>(), events: string[] = [];
    const f: Fixture = new Fixture({ onPlacementWake: async (targetId, received) => {
      assert.equal(targetId, TARGET);
      assert.deepEqual(received, wake());
      entered.resolve();
      await commit.promise;
      const owned = await f.persist(targetId, received);
      events.push("durable-write");
      return owned;
    } });
    const receipt = f.transport.deliver(envelope(), SOURCE, () => {
      assert.deepEqual(f.diskWake(), { targetId: TARGET, wake: wake() });
      events.push("ack");
    });
    try {
      await bounded(entered.promise);
      assert.equal(receipt.acks, 0);
      assert.equal(f.diskWake(), undefined);
      commit.resolve();
      assert.equal(await bounded(receipt.outcome), "ack");
      assert.deepEqual(events, ["durable-write", "ack"]);
      assert.deepEqual(receipt.naks, []);
      assert.equal(f.ordinaryCalls, 0);
      assert.deepEqual(f.transport.sent, []);
      assert.deepEqual(warnings, []);
    } finally { commit.resolve(); }
  });

  for (const reason of ["eligible", "released-never-started", "released-exited"] as const) {
    it(`accepts the stable ${reason} wake identity`, async () => {
      const f: Fixture = new Fixture({ onPlacementWake: (targetId, received) => f.persist(targetId, received) });
      const body = { ...wake(), reason, at: "2024-02-29T00:00:00Z" };
      await ackWithoutControl(f, envelope(body));
      assert.deepEqual(f.diskWake(), { targetId: TARGET, wake: body });
      assert.deepEqual(warnings, []);
    });
  }

  it("ACK-drops spoofed delivery sources even when envelope.from claims the configured writer", async () => {
    let calls = 0;
    const f = new Fixture({ onPlacementWake: () => { calls++; return true; } });
    const forged = envelope(wake(), { from: { id: SOURCE, name: SOURCE, kind: "main" } });
    await ackWithoutControl(f, forged, "untrusted-machine");
    await ackWithoutControl(f, forged, "fabric-ops"); // The special operation never enters relay refusal handling.
    assert.equal(calls, 0);
    assert.equal(f.diskWake(), undefined);
    assert.match(warnings[0]!, /reason=source count=1$/);
  });

  it("ACK-drops another host/machine destination without calling the owner callback", async () => {
    let calls = 0;
    const f = new Fixture({ onPlacementWake: () => { calls++; return true; } });
    await ackWithoutControl(f, envelope(wake(), { to: { machine: "owner-machine", hostId: "runtime:other" } }));
    await ackWithoutControl(f, envelope(wake(), { to: { machine: "other-machine", hostId: HOST } }));
    assert.equal(calls, 0);
    assert.equal(f.diskWake(), undefined);
    assert.match(warnings[0]!, /reason=target count=1$/);
  });

  it("is disabled without a configured authenticated source", async () => {
    let calls = 0;
    const f = new Fixture({ placementWakeSourceMachine: undefined, onPlacementWake: () => { calls++; return true; } });
    await ackWithoutControl(f, envelope());
    assert.equal(calls, 0);
    assert.match(warnings[0]!, /reason=source count=1$/);
  });

  it("ACK-drops a missing callback and bounds body-free counted diagnostics", async () => {
    const f = new Fixture();
    for (let index = 0; index < 17; index++) await ackWithoutControl(f, envelope());
    assert.deepEqual(warnings, [1, 2, 4, 8, 16].map(count =>
      `[pi-fabric] Dropped NATS placement wake reason=unowned count=${count}`));
    assert.ok(warnings.every(message => !message.includes(LAUNCH) && !message.includes(TARGET) && !message.includes(wake().at)));
    assert.equal(f.diskWake(), undefined);
  });

  it("requires the callback to match targetId AND the exact queued launchId+generation", async () => {
    let calls = 0;
    const f: Fixture = new Fixture({ onPlacementWake: (targetId, received) => { calls++; return f.persist(targetId, received); } });
    await ackWithoutControl(f, envelope(wake(), { targetId: "actor:not-owned" }));
    await ackWithoutControl(f, envelope({ ...wake(), launchId: "not-queued" }));
    await ackWithoutControl(f, envelope({ ...wake(), generation: GENERATION + 1 }));
    assert.equal(calls, 3);
    assert.equal(f.diskWake(), undefined);
    assert.match(warnings[0]!, /reason=unowned count=1$/);
  });

  it("NAKs a pre-checkpoint wake, then accepts the same identity after the durable queue commit", async () => {
    let committed = false, calls = 0;
    const f: Fixture = new Fixture({ onPlacementWake: (targetId, received): boolean | Promise<boolean> => {
      calls++;
      return committed ? f.persist(targetId, received) : false;
    } });
    const payload = envelope();
    const early = f.transport.deliver(payload, SOURCE, undefined, 1);
    assert.equal(await bounded(early.outcome), "nak");
    assert.equal(early.acks, 0);
    assert.deepEqual(early.naks, [250]);
    assert.deepEqual(early.terms, []);
    assert.equal(f.diskWake(), undefined);

    committed = true;
    const retry = f.transport.deliver(payload, SOURCE, () => {
      assert.deepEqual(f.diskWake(), { targetId: TARGET, wake: wake() });
    }, 2);
    assert.equal(await bounded(retry.outcome), "ack");
    assert.equal(retry.acks, 1);
    assert.deepEqual(retry.naks, []);
    assert.equal(calls, 2);
    assert.equal(f.ordinaryCalls, 0);
    assert.deepEqual(f.transport.sent, []);
    assert.deepEqual(warnings, []);
  });

  it("counts and drops an unknown launch after bounded NAK redeliveries", async () => {
    let calls = 0;
    const f = new Fixture({ onPlacementWake: () => { calls++; return false; } });
    const payload = envelope({ ...wake(), launchId: "never-checkpointed" });
    for (let attempt = 1; attempt < 5; attempt++) {
      const retry = f.transport.deliver(payload, SOURCE, undefined, attempt);
      assert.equal(await bounded(retry.outcome), "nak");
      assert.equal(retry.acks, 0);
      assert.deepEqual(retry.naks, [250]);
      assert.deepEqual(retry.terms, []);
    }
    const final = f.transport.deliver(payload, SOURCE, undefined, 5);
    assert.equal(await bounded(final.outcome), "ack");
    assert.equal(final.acks, 1);
    assert.deepEqual(final.naks, []);
    assert.equal(calls, 5);
    assert.deepEqual(warnings, ["[pi-fabric] Dropped NATS placement wake reason=unowned count=1"]);
    assert.ok(warnings.every(message => !message.includes("never-checkpointed") && !message.includes(TARGET)));
    assert.equal(f.diskWake(), undefined);
  });

  it("NAKs a persistence exception without ACK, then retries the same identity successfully", async () => {
    let failing = true, calls = 0;
    const f: Fixture = new Fixture({ onPlacementWake: async (targetId, received) => {
      calls++;
      if (failing) throw new Error("private storage failure must not be logged");
      return f.persist(targetId, received);
    } });
    const payload = envelope();
    const first = f.transport.deliver(payload);
    assert.equal(await bounded(first.outcome), "nak");
    assert.equal(first.acks, 0);
    assert.deepEqual(first.naks, [1_000]);
    assert.deepEqual(first.terms, []);
    assert.equal(f.diskWake(), undefined);
    failing = false;
    const retry = f.transport.deliver(payload, SOURCE, () => {
      assert.deepEqual(f.diskWake(), { targetId: TARGET, wake: wake() });
    }, 2);
    assert.equal(await bounded(retry.outcome), "ack");
    assert.equal(calls, 2);
    assert.equal(retry.acks, 1);
    assert.equal(f.ordinaryCalls, 0);
    assert.deepEqual(f.transport.sent, []);
    assert.deepEqual(warnings, []);
  });

  it("ACK-drops malformed wakes without callback, including repaired dates and unstable message ids", async () => {
    let calls = 0;
    const f = new Fixture({ onPlacementWake: () => { calls++; return true; } });
    const invalidBodies: unknown[] = [
      null, [], "wake", { ...wake(), version: 2 }, { ...wake(), version: "1" }, { ...wake(), extra: true },
      { ...wake(), launchId: "" }, { ...wake(), launchId: "x".repeat(201) }, { ...wake(), launchId: "bad\u0000id" },
      { ...wake(), launchId: "bad\u007fid" }, { ...wake(), generation: 0 }, { ...wake(), generation: -1 },
      { ...wake(), generation: 1.5 }, { ...wake(), generation: String(GENERATION) },
      { ...wake(), generation: Number.MAX_SAFE_INTEGER + 1 }, { ...wake(), generation: Infinity },
      { ...wake(), reason: "released" }, { ...wake(), reason: "eligible\n" },
      ...[0, Date.now(), "2030-01-02", "2030-01-02T03:04:05+00:00", "2030-01-02T03:04:05z",
        "2030-01-02T03:04:05Z\n", "2030-01-02T03:04:05.1234567Z", "2030-01-02T24:00:00Z",
        "2030-01-02T03:04:60Z", "2030-02-29T00:00:00Z", "2030-04-31T00:00:00Z"].map(at => ({ ...wake(), at })),
    ];
    for (const body of invalidBodies) await ackWithoutControl(f, envelope(body));
    const { at: _at, ...missingAt } = wake();
    await ackWithoutControl(f, envelope(missingAt));
    for (const patch of [{ id: "arbitrary-message-id" }, { id: `${envelope().id}\n` }, { kind: "ack" as const },
      { targetId: "" }, { targetId: "x".repeat(201) }, { targetId: "bad\u0001target" }]) {
      await ackWithoutControl(f, envelope(wake(), patch));
    }
    assert.equal(calls, 0);
    assert.equal(f.diskWake(), undefined);
    assert.ok(warnings.every(message => /reason=malformed count=\d+$/.test(message)));
  });

  it("preserves ordinary participant command handling and control ACK publication", async () => {
    let wakeCalls = 0;
    const caller = "session:ordinary-caller", now = Date.now();
    const participants: ParticipantView = { leaseLive: () => true,
      resolve(id) { return { machine: id === caller ? SOURCE : "owner-machine", ownerHostId: id === caller ? "runtime:caller" : HOST,
        ownerIdentityId: id === caller ? caller : identity.id, ownerIncarnation: "placement-owner-v1", capabilities: ["followUp"], live: true }; },
      async whenChanged() { throw new Error("No directory watch expected"); } };
    const f = new Fixture({ participants, onPlacementWake: () => { wakeCalls++; return true; } });
    const command = { version: 1, commandId: "ordinary-command", targetId: TARGET, operation: "followUp",
      ownerIncarnation: "placement-owner-v1", requestedAt: now, deadlineAt: now + 30_000, replyTo: "runtime:caller", message: "ordinary" };
    const receipt = f.transport.deliver(envelope(command, { id: "ordinary-event", operation: "followUp",
      from: { id: caller, kind: "main", name: "caller" } }));
    assert.equal(await bounded(receipt.outcome), "ack");
    assert.equal(f.ordinaryCalls, 1);
    assert.equal(wakeCalls, 0);
    assert.equal(f.transport.sent.length, 1);
    assert.equal(f.transport.sent[0]!.kind, "ack");
    assert.deepEqual((f.transport.sent[0]!.body as Record<string, unknown>).accepted, true);
    assert.equal(f.diskWake(), undefined);
  });
});
