import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { FabricControlPlane, FabricControlHandler } from "../src/topology/control-plane.js";
import type { AgentManager } from "../src/agents/manager.js";
import type { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import {
  adapterKinds, boundedReceipt, commandEnvelope, contractTiming, createStubBus, faultGate, FaultHooks,
  identity, loadControlPlane, loseNextPublishAck, openRealAdapterPair, owner, resolvedRetry, withFaults,
  type AdapterKind, type CommandBody, type FabricTransport, type RealBrokerOutage, type RealCapacityBoundary, type RealKvReadiness, type RealPairOptions,
  type StubTransport, type TransportBucket, type TransportDelivery, type TransportEnvelope,
  type TransportKvEntry, type TransportKvEvent,
} from "./helpers/transport-harness.js";
import type { ParticipantRoute as Route, ParticipantView as View } from "../src/topology/participant-view.js";

/**
 * FROZEN CONTRACT-v2 §§0,1,3,4,5,7,8,10.3,11 + frozen errata; T1–T22: inherited draft §7.
 * Keep reason: data safety and cross-machine trust boundary. No fake claim/control/admission
 * implementation: the helper's stub models §1 carrier/KV ONLY. Control tests instantiate the
 * actual class returned by loadControlPlane, with an actual carrier-constructor capability probe.
 *
 * E2: real server is Intel 1 / 2.11.17 / sync_interval always; E3: WA/WB Ryzen 2/Ryzen 4.
 * No fleet deployment, unit control or production credential creation is performed here. B's
 * local fixture owns its real server and ephemeral signed per-machine runtime credentials.
 * E12 is enforced by the actual factory/connection USER.INFO check, never verifyMachine:false.
 * The existing state-server helper is deliberately NOT imported (different server/auth surface).
 *
 * Expected skips / coverage metadata, not green E2E evidence:
 * - FILE/NATS use the current product factory. NATS uses B's createPathBHarness with fresh
 *   server/keys/state per pair, bootstrap-only resource creation, then two distinct runtime JWTs.
 *   T11 opens that matching default policy FIRST, then explicitly faults only its isolated
 *   stream's byte/message-size ceilings. This is not tiny production-default policy coverage.
 *   Missing real-server prerequisites and actual A1 NATS dispatch are explicit dependency skips.
 * - Lost-PubAck injection is STUB ONLY. Successful real PubAck followed by a wrapper throw is
 *   not B1 loss evidence. Real T3 below checks resolved same-id retry bytes, not lost PubAck.
 * - Real T6 NAK replay is NOT lost-ACK or ackWait/WPI-extension proof.
 * - Stub working/nak/replay are SCRIPTED carrier operations, not a broker timer/delivery engine.
 * - T7 authenticated operator relay/DLQ is DEFERRED-E19: the operator is report-only.
 *   T8 NATS now owns a real transparent TCP cut + same-store server restart, retaining the
 *   original factory policy and live iterator. FILE/stub have no broker outage simulation.
 *   E25 T11 uses the real sender's typed refusal PLUS actual report-only operator bytes/max_bytes;
 *   no separate alarm emitter is required (follow-on #13951). T13/T16/T21/T22 still need optional
 *   integrations. Missing fixtures are skipped, never replaced by a synthetic admission/lease/
 *   operator/rollback implementation.
 * - T4/T5 use a native-custody test callback invoked by the REAL control plane. This tests its
 *   claim/outcome/ACK machinery, NOT Main/Pi consumption, AgentManager fsync or power-loss safety.
 * - T20 is focused control parity; unchanged control-plane/router suites must ALSO run. It does
 *   not spawn other tests. STATE migration is not control-carrier rollback evidence.
 * - SIGKILL/SIGSTOP, partitions, B3–B5 (same-host completion per E4), T13b/c/d, T16b, T-HOL,
 *   T-MIX and E20 nonzero-drain refusal remain tests/transport-faults/ and canary obligations.
 *   T-LOCK below covers real A1 control dispatch through a native test custody callback, not the
 *   additional real Main/AgentManager canary obligation. E14 retry/digest gates also run in
 *   transport-faults/e14-control.test.ts; E19 defers authenticated operator relay tests.
 * - §10.3 N={1,4,16,64}, 1 cmd/s/sender, 10 min/step, per-target/N p50/p95/p99, KV write rate,
 *   PubAck p95 and separate B3 latency belong to T-P95, not CI/stub latency assertions.
 * - E6 T-READY on the stub proves §1 event ordering ONLY. Its snapshot is synchronously queued;
 *   changing storage while that queue is consumed is not a real in-flight broker snapshot.
 *   Scripted resetKvWatches() is NOT ordered-consumer reset/reconnect proof. Real NATS reset
 *   deletes this pair's actual active ordered consumer. Pre-consumption delete/15s lease-expiry
 *   races use B's real NatsKvStore on the same machine-B runtime credentials and its official
 *   consume() scheduling seam, NOT a replacement for receiver.kv() or a synthetic watch engine.
 *   The stronger marker-after-snapshot-data hold hook is unavailable; those separate cases
 *   remain explicit gaps (fail in required mode), never relabelled as pre-consumption coverage.
 * - E6 supersedes draft §4.4's isUpdate/keys() readiness recipe. Only the explicit ready event
 *   completes a snapshot; neither its last put nor a keys()/get() result is a readiness barrier.
 *   Actual A2 KvParticipantDirectory/TransportHostLease APIs ARE present: manually-composed
 *   authenticated ready/reset/view coverage is in transport-faults/presence-leases.test.ts.
 *   Installed application-root composition coverage remains separate and unproven here.
 * - No events port, automatic TTL takeover, file fallback or same-host isolation claim.
 */

type Skip = (reason: string) => never;
type Message = TransportDelivery<TransportEnvelope>;
interface Pair {
  sender: FabricTransport; receiver: FabricTransport;
  hooks: FaultHooks; stub?: { sender: StubTransport; receiver: StubTransport };
  bus?: ReturnType<typeof createStubBus>;
  raw?: { readonly sender: FabricTransport; readonly receiver: FabricTransport };
  reopenReceiver?(): Promise<FabricTransport>;
  kvReadiness?: RealKvReadiness;
  brokerOutage?: RealBrokerOutage;
  capacityBoundary?: RealCapacityBoundary;
  now(): number; advance(ms: number): void;
  close(): Promise<void>;
}
/** Small OPTIONAL composition seam, not extra behavior required of the terminal helper.
 * Parent may supply real fixtures here after integration. Every callback must drive real product
 * classes/broker operations; none may implement claims, authorization, admission or dedupe.
 * The registry wires actual carriers; advanced fault/readiness fixtures remain explicit gaps.
 */
interface Integration {
  /** Fresh isolated cohort per test, including initially empty presence/leases buckets. */
  pair(skip: Skip, options?: RealPairOptions): Promise<Pair>;
  /** B-owned fixture operations on this pair's REAL ordered push watch, never event synthesis.
   * These are test composition hooks, not guessed production exports. Missing hooks are explicit
   * skipped coverage, or failures for mandatory NATS T-READY with FABRIC_TRANSPORT_REQUIRED=1.
   * Hooks must honor their finite signal; releasing a held boundary must also be bounded.
   */
  kvReadiness?: {
    /** Arm before watch(): hold its true snapshot-complete boundary, allowing snapshot puts out.
     * Preserve the ordered source (including concurrent tombstones); do not replace/wrap the
     * adapter with a second watch engine. release() is idempotent, including after signal abort.
     */
    holdSnapshot?(bucket: TransportBucket, prefix: string, signal: AbortSignal): Promise<{ release(): Promise<void> }>;
    /** Reset the active ordered consumer for this bucket/prefix, not just the inbox connection.
     * Resolve after injecting the real reset; public reset/snapshot/ready receipts are read below.
     */
    reset?(bucket: TransportBucket, prefix: string, signal: AbortSignal): Promise<void>;
    /** Await actual server MaxAge expiry of this revision while the snapshot boundary is held.
     * No fabricated gone, explicit delete, bucket-policy change or local fake timer as TTL proof.
     */
    expire?(bucket: TransportBucket, key: string, revision: number, signal: AbortSignal): Promise<void>;
  };
  broker?: {
    restart(): Promise<void>; disconnect(): Promise<void>; reconnect(): Promise<void>;
    probe(verb: string, subject: string): Promise<"allowed" | "denied">;
    foreignMachine: string; foreignUser: string; ownInbox: string;
  };
  spawn?: { manager: AgentManager; directory: ParticipantDirectory; router: AgentMessageRouter;
    sender: FabricControlPlane; receiver: FabricControlPlane; request: Parameters<AgentManager["spawn"]>[0] };
  lease?: { directory: ParticipantDirectory; pause(): Promise<void>; resume(): Promise<void>; successor(): Promise<boolean>; proveExit(): Promise<void> };
  flip?: (carrier: "file" | "nats") => Promise<{ sender: FabricControlPlane; receiver: FabricControlPlane }>;
}
const integrations: Partial<Record<AdapterKind, Integration>> = {
  file: { pair: (skip, options) => realPair("file", skip, options) },
  nats: { pair: (skip, options) => realPair("nats", skip, options) },
};
// Explicit required mode still fails missing capabilities; the real constructor is always probed.
const transportIntegrated = process.env.PI_FABRIC_TRANSPORT_CONTROL_INTEGRATED === "1";
const required = process.env.FABRIC_TRANSPORT_REQUIRED === "1";
const TIMEOUT = 120_000;
let serial = 0;
const nextId = (test: string) => `${test}-${++serial}`;

async function next<T>(iterator: AsyncIterator<T>, timeoutMs = 5_000, label = "next delivery/watch event"): Promise<T> {
  const result = await boundedReceipt(iterator.next(), label, timeoutMs);
  expect(result.done, "iterator ended before required receipt").not.toBe(true);
  return result.value as T;
}
/** createdAt is guaranteed by TransportSent, not TransportDelivery<TransportEnvelope>.
 * Compare every frozen envelope field (especially body/from/principal), without demanding
 * that a legacy FILE delivery invent send-receipt metadata it does not carry.
 */
function payloadMinimum(envelope: TransportEnvelope & { readonly createdAt?: number }): TransportEnvelope {
  const { createdAt: _sentMetadata, ...payload } = envelope;
  return payload;
}
const pairInboxes = new WeakMap<Pair, Set<AsyncIterator<Message>>>();
async function returnPairInboxes(pair: Pair): Promise<void> {
  const iterators = pairInboxes.get(pair);
  if (!iterators) return;
  const results = await Promise.allSettled([...iterators].map(iterator =>
    boundedReceipt(Promise.resolve().then(() => iterator.return?.()), "return test-owned inbox after actual pair.close")));
  pairInboxes.delete(pair);
  const failed = results.find(result => result.status === "rejected");
  if (failed?.status === "rejected") throw failed.reason;
}
async function ready(iterator: AsyncIterator<TransportKvEvent>): Promise<TransportKvEvent[]> {
  const result: TransportKvEvent[] = [];
  for (let i = 0; i < 100; i++) {
    const event = await next(iterator); result.push(event);
    if (event.type === "ready") return result;
  }
  throw new Error("watch did not become ready within 100 receipts");
}
/** Exact snapshot oracle, not a directory or a readiness implementation. No data order assumed.
 * Last-per-subject NATS snapshots include retained tombstones as well as live puts. Both are
 * DATA: require every expected receipt, then a separate ready; never filter another watched key.
 */
async function snapshotThenReady(iterator: AsyncIterator<TransportKvEvent>, expected: readonly TransportKvEntry[],
  tombstones: readonly Extract<TransportKvEvent, { type: "gone" }>[] = []): Promise<void> {
  const data: TransportKvEvent[] = [];
  const expectedData: TransportKvEvent[] = [...expected.map(entry => ({ type: "put" as const, entry })), ...tombstones];
  for (let i = 0; i < expectedData.length; i++) {
    const event = await next(iterator);
    expect(["put", "gone"], `snapshot data ${i + 1}/${expectedData.length} must precede ready`).toContain(event.type);
    data.push(event);
  }
  const key = (event: TransportKvEvent) => event.type === "put" ? event.entry.key : event.type === "gone" ? event.key : "";
  const byKey = (a: TransportKvEvent, b: TransportKvEvent) => key(a).localeCompare(key(b));
  expect(data.sort(byKey)).toEqual(expectedData.sort(byKey));
  expect(await next(iterator), "snapshot completion is its own receipt").toEqual({ type: "ready" });
}
function stream(pair: Pair, signal: AbortSignal, port = pair.receiver): AsyncIterator<Message> {
  const iterator = port.inbox(signal)[Symbol.asyncIterator]();
  const tracked = pairInboxes.get(pair) ?? new Set<AsyncIterator<Message>>();
  tracked.add(iterator); pairInboxes.set(pair, tracked);
  return iterator;
}
function command(pair: Pair, id = nextId("command"), patch: Partial<Parameters<typeof commandEnvelope>[0]> = {}) {
  return commandEnvelope({ id, to: pair.receiver.self, from: identity(pair.sender.self.hostId),
    replyTo: pair.sender.self.hostId, message: "bounded conformance input", ...patch });
}

async function realPair(kind: "file" | "nats", skip: Skip, options?: RealPairOptions): Promise<Pair> {
  const result = await openRealAdapterPair(kind, required, options);
  if (result.status === "skip") return skip(result.reason);
  const raw = result.pair, hooks = new FaultHooks();
  // Never wrap NATS with synthetic PubAck/connection failures. FILE port-only probes may use
  // hooks; its control plane receives the untouched concrete FileTransport instances below.
  const wrap = (port: FabricTransport) => kind === "file" ? withFaults(port, hooks) : port;
  const pair: Pair = { sender: wrap(raw.sender), receiver: wrap(raw.receiver), raw, hooks,
    ...(raw.kvReadiness ? { kvReadiness: raw.kvReadiness } : {}),
    ...(raw.brokerOutage ? { brokerOutage: raw.brokerOutage } : {}),
    ...(raw.capacityBoundary ? { capacityBoundary: raw.capacityBoundary } : {}),
    now: Date.now, advance: () => { throw new Error("real carrier fixture has no virtual clock capability"); },
    reopenReceiver: async () => { pair.receiver = wrap(await raw.reopenReceiver()); return pair.receiver; },
    close: () => raw.close() };
  return pair;
}

async function openPair(kind: AdapterKind, skip: Skip, options?: RealPairOptions): Promise<Pair> {
  if (integrations[kind]) return integrations[kind]!.pair(skip, options);
  if (kind === "stub") {
    let time = Date.now();
    const bus = createStubBus(() => time), hooks = new FaultHooks();
    const sender = bus.open(owner("test-a", "session:sender")), receiver = bus.open(owner("test-b", "session:receiver"));
    return { sender: withFaults(sender, hooks), receiver: withFaults(receiver, hooks), hooks,
      stub: { sender, receiver }, bus, now: () => time, advance: ms => { time += ms; bus.expireKv(); },
      close: async () => { await sender.close(); await receiver.close(); } };
  }
  throw new Error(`No real integration registered for ${kind}`);
}

/** Actual CP, real owner-local MeshStore claims. The view is fixture DATA conforming to A2's
 * exact resolve/whenChanged/leaseLive contract, not a directory/authorization implementation.
 * Native callback is a probe at the production handler boundary, not a fake control plane.
 */
async function control(pair: Pair, skip: Skip, handler?: FabricControlHandler) {
  const [{ MeshStore }, { FileTransport }, { writeJsonAtomic }] = await Promise.all([
    import("../src/mesh/store.js"), import("../src/transport/file.js"), import("../src/core/atomic-write.js"),
  ]);
  const root = mkdtempSync(join(tmpdir(), "fabric-transport-control-")), senderId = pair.sender.self.hostId, receiverId = pair.receiver.self.hostId;
  const ports = pair.raw ?? pair;
  const ownedMeshes: import("../src/mesh/store.js").MeshStore[] = [];
  const meshFor = (port: FabricTransport, side: string) => {
    // FILE's native watcher, reader and publisher must share the ACTUAL adapter mesh.
    if (port instanceof FileTransport) return port.mesh;
    const mesh = new MeshStore(join(root, side), 1_048_576, 100, { stateBackend: "file" });
    ownedMeshes.push(mesh); return mesh;
  };
  const senderMesh = meshFor(ports.sender, "sender"), receiverMesh = meshFor(ports.receiver, "receiver");
  const cleanup = () => { for (const mesh of ownedMeshes) mesh.closeState(); rmSync(root, { recursive: true, force: true }); };
  const routes = new Map<string, Route>();
  const route = (machine: string, hostId: string): Route => ({ machine, ownerHostId: hostId,
    ownerIdentityId: hostId, ownerIncarnation: "incarnation-1", capabilities: ["followUp", "steer", "stop"], live: true });
  routes.set(senderId, route(pair.sender.self.machine, senderId));
  routes.set(receiverId, route(pair.receiver.self.machine, receiverId));
  const view: View = { resolve: id => routes.get(id), leaseLive: () => true,
    whenChanged: (_id, signal) => new Promise<void>((_resolve, reject) => {
      if (signal.aborted) { reject(signal.reason); return; }
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }) };
  const options = (hostId: string, transport: FabricTransport) => ({ enabled: true, hostId, transport, participants: view,
    ownerIncarnation: "incarnation-1", controlIncarnationFence: "enforce" as const,
    inboxLanes: 8, acknowledgementTimeoutMs: 60_000,
    ...(transport.kind === "nats" ? { relayOperatorMachine: "fabric-ops" } : {}) });
  let sender: FabricControlPlane | undefined, receiver: FabricControlPlane | undefined;
  let dependency: Awaited<ReturnType<typeof loadControlPlane>>;
  try {
    dependency = await loadControlPlane({ transportIntegrated, required, probe: Class => {
      // An actual NATS transport reaches this constructor. A getter alone cannot claim dispatch.
      sender = new Class(senderMesh, identity(senderId), options(senderId, ports.sender));
      receiver = new Class(receiverMesh, identity(receiverId), options(receiverId, ports.receiver));
    } });
    if (dependency.status === "unavailable") {
      await sender?.close(); await receiver?.close(); cleanup(); return skip(dependency.reason);
    }
  } catch (error) { await sender?.close(); await receiver?.close(); cleanup(); throw error; }
  const Class = dependency.module.FabricControlPlane;
  const actualSender = sender!, actualReceiver = receiver!;
  expect(Reflect.get(actualSender, "transportKind")).toBe(pair.sender.kind);
  expect(Reflect.get(actualReceiver, "transportKind")).toBe(pair.receiver.kind);
  const admitted: string[] = [];
  const native: FabricControlHandler = handler ?? (cmd => {
    // Real durable journal write at the test's native boundary; do not implement claims/dedupe here.
    writeJsonAtomic(join(root, "native", `${cmd.commandId}.json`), cmd, { durable: true });
    admitted.push(cmd.commandId); return { accepted: true, messageId: cmd.commandId };
  });
  actualReceiver.start(native);
  // Start the sender only for actual request calls. Raw-envelope negative tests own its inbox.
  let senderStarted = false;
  const startSender = () => { if (!senderStarted) { senderStarted = true; actualSender.start(() => ({ accepted: false })); } };
  return { sender: actualSender, receiver: actualReceiver, routes, admitted, Class, native,
    request: (key: string, op: "followUp" | "steer" | "stop" = "followUp") => {
      startSender();
      return actualSender.request(receiverId, receiverId, op,
        { ownerIncarnation: "incarnation-1", message: "bounded input" }, receiverId, { idempotencyKey: key });
    },
    close: async () => {
      try {
        await boundedReceipt(actualSender.close(), "close actual sender control plane");
        await boundedReceipt(actualReceiver.close(), "close actual receiver control plane");
      } finally { cleanup(); }
    } };
}

describe("T-READY exact snapshot oracle", () => {
  const entry: TransportKvEntry = { key: "machine-b.p.live", machine: "machine-b", owner: "owner#snapshot", value: { live: true }, revision: 2 };
  const deleted: Extract<TransportKvEvent, { type: "gone" }> = {
    type: "gone", key: "machine-b.p.deleted", machine: "machine-b", revision: 4, reason: "deleted",
  };
  const iterator = (events: TransportKvEvent[]) => (async function* () { yield* events; })()[Symbol.asyncIterator]();
  it("requires exact live entries and retained tombstones, in either data order, before a separate marker", async () => {
    for (const data of [[{ type: "put" as const, entry }, deleted], [deleted, { type: "put" as const, entry }]]) {
      await snapshotThenReady(iterator([...data, { type: "ready" }]), [entry], [deleted]);
    }
  });
  it("rejects early ready, missing or unexpected tombstones, and stale tombstone revisions", async () => {
    await expect(snapshotThenReady(iterator([{ type: "put", entry }, { type: "ready" }]), [entry], [deleted])).rejects.toThrow();
    await expect(snapshotThenReady(iterator([{ type: "put", entry }, deleted, { type: "ready" }]), [entry])).rejects.toThrow();
    await expect(snapshotThenReady(iterator([{ type: "put", entry }, { ...deleted, revision: 3 }, { type: "ready" }]), [entry], [deleted])).rejects.toThrow();
    await expect(snapshotThenReady(iterator([{ type: "ready" }, { type: "put", entry }, deleted]), [entry], [deleted])).rejects.toThrow();
  });
});

describe.each(adapterKinds)("transport conformance / %s", kind => {
  function readinessCapability<T>(value: T | undefined, skip: Skip, reason: string): T {
    if (value !== undefined) return value;
    if (kind === "nats" && required) throw new Error(`mandatory NATS T-READY fixture missing: ${reason}`);
    return skip(reason);
  }
  function test(name: string, run: (pair: Pair, signal: AbortSignal, skip: Skip) => Promise<void>, timeoutMs = TIMEOUT, options?: RealPairOptions) {
    it(name, async context => {
      const started = Date.now();
      const skip: Skip = reason => { context.skip(reason); throw new Error("Vitest skip unexpectedly returned"); };
      const pair = await openPair(kind, skip, options), abort = new AbortController();
      try {
        // Receipts drive progress. Reserve cleanup time BEFORE Vitest's outer deadline; a
        // failing real iterator must never strand this pair's server until the worker is killed.
        await boundedReceipt(run(pair, abort.signal, skip), name, Math.max(1, timeoutMs - (Date.now() - started) - 30_000));
      } finally {
        abort.abort(); vi.restoreAllMocks();
        // Ordinary ownership cleanup follows actual close, never a pre-close workaround.
        try { await pair.close(); }
        finally { await returnPairInboxes(pair); }
      }
    }, timeoutMs);
  }

  test("T1 send → inbox → ack: ordered/authenticated; E21 serial FILE, out-of-order stub/NATS final ACK", async (p, signal) => {
    const a = await p.sender.send(command(p)), b = await p.sender.send(command(p));
    expect(a.sequence).toBeGreaterThan(0); expect(b.sequence).toBeGreaterThan(a.sequence);
    const iterator = stream(p, signal), one = await next(iterator);
    let two: Message;
    if (kind === "file") {
      // E21: zero-behaviour FILE parity wins. Its legacy serial inbox must not yield
      // another delivery until the first settles; out-of-order ACK is stub/NATS only.
      let secondYielded = false;
      const pending = iterator.next().then(result => { secondYielded = true; return result; });
      void pending.catch(() => undefined);
      expect(await p.receiver.drainState()).toEqual({ pending: 1, ackPending: 1 });
      expect(secondYielded).toBe(false);
      await one.ack();
      two = await next({ next: () => pending });
    } else {
      two = await next(iterator, 5_000, "T1 stub/NATS second delivery BEFORE first ACK");
    }
    expect(one).toMatchObject({ sourceMachine: p.sender.self.machine, attempt: 1, sequence: a.sequence });
    expect(payloadMinimum(one.payload)).toEqual(payloadMinimum(a.envelope));
    expect(one.verification).toBe(p.sender.self.machine === p.receiver.self.machine ? "mesh" : "bridge");
    expect(payloadMinimum(two.payload)).toEqual(payloadMinimum(b.envelope));
    await two.ack();
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: kind === "file" ? 0 : 1 });
    if (kind !== "file") await one.ack();
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
  });

  test("T2 same id: stub/NATS carrier duplicate; FILE application admission/handler/ACK once (E1)", async (p, signal, skip) => {
    if (kind === "file") {
      // Lead ruling: E1 permits multiple appended file events. Never require duplicate:true here.
      const cp = await control(p, skip);
      try {
        const key = nextId("T2-file"), first = await cp.request(key), again = await cp.request(key);
        expect(first).toEqual(again); expect(first.acknowledged).toBe(true);
        expect(cp.admitted).toHaveLength(1); // actual CP/claim implementation, not a stub admission model
      } finally { await cp.close(); }
      return;
    }
    const original = command(p), first = await p.sender.send(original);
    if (kind === "stub") p.advance(1);
    let stamps = 0;
    const retry = await p.sender.send({ ...original, body: at => { stamps++; return { ...first.envelope.body, requestedAt: at }; } });
    expect(stamps).toBe(1); expect(retry).toMatchObject({ duplicate: true, sequence: first.sequence });
    expect(retry.envelope.body.requestedAt).toBe(retry.envelope.createdAt);
    const delivered = await next(stream(p, signal)); expect(payloadMinimum(delivered.payload)).toEqual(payloadMinimum(first.envelope));
    await delivered.ack(); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
  });

  test("T3 resolved retry uses actual carrier: same logical id and byte-identical body", async (p, signal) => {
    const original = command(p), first = await p.sender.send(original);
    const frozenBytes = JSON.stringify(first.envelope.body);
    const retry = await p.sender.send(resolvedRetry(original, first.envelope.body));
    expect(JSON.stringify(retry.envelope.body)).toBe(frozenBytes);
    // E1: FILE may append a new native event; only stub/NATS dedupe inside the broker window.
    if (kind !== "file") expect(retry).toMatchObject({ duplicate: true, sequence: first.sequence });
    const delivered = await next(stream(p, signal));
    expect(JSON.stringify(delivered.payload.body)).toBe(frozenBytes); await delivered.ack();
  });

  test("T3 lost PubAck STUB SCRIPT only: uncertain bytes, not real B1 fault evidence", async (p, signal, skip) => {
    if (!p.stub) skip("real lost-PubAck boundary injection absent; a post-success wrapper throw is not lost PubAck");
    const original = command(p); loseNextPublishAck(p.hooks);
    const error: unknown = await p.sender.send(original).catch((reason: unknown) => reason);
    expect(error).toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN", id: original.id, resolvedBody: expect.any(Object) });
    expect(error).not.toHaveProperty("persisted", false);
    const retry = await p.sender.send(resolvedRetry(original, (error as { resolvedBody: CommandBody }).resolvedBody));
    if (kind !== "file") expect(retry.duplicate).toBe(true); // E1 allows a fresh file event
    const delivered = await next(stream(p, signal)); expect(delivered.payload.body).toEqual((error as { resolvedBody: unknown }).resolvedBody);
    await delivered.ack();
  });

  test("T4 STUB delivery/ACK-loss script: real control-plane claim prevents a second handler call", async (p, _signal, skip) => {
    if (!p.stub) skip("real lost-consumer-ACK injection/ackWait receipt absent; NAK replay does not cover ACK loss");
    const cp = await control(p, skip);
    try {
      // Drop BEFORE forwarding final broker ack, not after a confirmed ack (which would not redeliver).
      const remove = p.hooks.set("ack:before", context => {
        if (context.id?.startsWith("ack:")) return;
        remove(); throw new Error("injected consumer ack loss");
      });
      const key = nextId("T4"); expect(await cp.request(key)).toMatchObject({ acknowledged: true });
      const record = p.stub!.receiver.records.find(row => row.delivery.payload.kind === "command")!;
      const replayAcked = faultGate();
      const observe = p.hooks.set("ack:after", context => {
        if (context.id === record.delivery.payload.id) replayAcked.release();
      });
      try {
        p.stub!.receiver.enqueue(record.delivery.payload, { sourceMachine: p.sender.self.machine,
          verification: "bridge", attempt: 2, sequence: record.delivery.sequence });
        await replayAcked.wait(AbortSignal.timeout(5_000));
        expect(cp.admitted).toHaveLength(1); expect(cp.receiver).toBeInstanceOf(cp.Class);
      } finally { observe(); }
      // This is scripted consumer replay through the REAL claim/outcome code. It makes no
      // B2 crash claim. E14 sender re-observation has its separate real-broker gate.
    } finally { await cp.close(); }
  });

  test("T5 crash after real claim before outcome: real recovery refuses indeterminate, never re-executes", async (p, _signal, skip) => {
    const cp = await control(p, skip);
    try {
      // The carrier cannot observe a claim. Wire the hook to the REAL owner-local store mutation.
      const { MeshStore } = await import("../src/mesh/store.js");
      const originalPut = MeshStore.prototype.put, originalBatch = MeshStore.prototype.writeBatch;
      let claimed = false;
      const claimGate = faultGate(), sawClaim = faultGate();
      const afterClaim = async (root: string, create: boolean) => {
        if (!claimed && root.includes("control-seen") && create) {
          claimed = true; sawClaim.release(); await claimGate.wait(AbortSignal.timeout(5_000));
          throw new Error("injected halt after actual claim write");
        }
      };
      const putSpy = vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: import("../src/mesh/store.js").MeshStore, input) {
        const result = await originalPut.call(this, input);
        await afterClaim(this.root, input.ifVersion === 0); return result;
      });
      // The integrated A1 NATS dispatcher uses a fenced writeBatch for the actual claim;
      // FILE still uses put. Observe both real boundaries, never seed a synthetic claim.
      const batchSpy = vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: import("../src/mesh/store.js").MeshStore, input) {
        const result = await originalBatch.call(this, input);
        await afterClaim(this.root, input.ops.some(op => op.kind === "put" && op.ifVersion === 0)); return result;
      });
      const restore = () => { putSpy.mockRestore(); batchSpy.mockRestore(); };
      const pending = cp.request(nextId("T5")).catch((error: unknown) => error);
      try {
        await sawClaim.wait(AbortSignal.timeout(5_000)); cp.receiver.pause(); claimGate.release(); restore();
        // Production restart replay must see the real claim without an outcome, not a seeded fake.
        cp.receiver.resume();
        const result = await pending;
        expect(result).toBeInstanceOf(Error); expect(String(result)).toMatch(/unknown|indeterminate|interrupted/i);
        expect(cp.admitted).toEqual([]);
      } finally { claimGate.release(); restore(); }
      // Actual SIGKILL/new-process restart is T-B2a; this seam is not process-crash proof.
    } finally { await cp.close(); }
  });

  test("T6 actual working()/NAK replay (not lost-ACK or timed WPI-extension evidence)", async (p, signal) => {
    const sent = await p.sender.send(command(p)), iterator = stream(p, signal), first = await next(iterator);
    for (let i = 0; i < 4; i++) first.working();
    if (p.stub) expect(p.stub.receiver.records[0]!.working).toBe(4);
    first.nak(25);
    if (p.stub) {
      expect(p.stub.receiver.records[0]!.naks).toEqual([25]);
      // Stub ONLY: scripted carrier receipt, never broker timing/authentication proof.
      p.stub.receiver.enqueue(sent.envelope, { sourceMachine: p.sender.self.machine, verification: "bridge", attempt: 2, sequence: sent.sequence });
    }
    const replay = await next(iterator);
    expect(replay.attempt).toBe(kind === "file" ? 1 : 2); // FILE always reports attempt 1 (§1).
    expect(replay.sequence).toBe(first.sequence); expect(replay.payload.body).toEqual(first.payload.body);
    await replay.ack();
    if (p.stub) await first.ack(); // settle the separate scripted record; real NAK settled no ACK.
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
  });

  // E19 supersedes the historical operator relay/DLQ requirement. No body read,
  // relay publish or consumer mutation is authorized by the report-only credential.
  it.skip("T7 DEFERRED-E19: authenticated operator relay/DLQ; report-only permissions covered by B T-OPS-NOREAD", () => {});

  test("T8 broker restart/network cut: live iterator resumes from ack floor; outage send never silently succeeds", async (p, signal, skip) => {
    const broker = p.brokerOutage ?? integrations[kind]?.broker;
    if (!broker) skip("real broker restart/network-cut dependency absent; stub does not simulate a server");
    if (kind === "nats" && !p.brokerOutage) throw new Error("T8 requires its pair-owned real TCP/broker receipts, not a missing fixture");
    const iterator = stream(p, signal), prime = await p.sender.send(command(p));
    await (await next(iterator)).ack();
    const outstanding = await p.sender.send(command(p)); const held = await next(iterator);
    expect(held).toMatchObject({ sequence: outstanding.sequence, attempt: 1,
      sourceMachine: p.sender.self.machine, verification: "bridge" });
    expect(payloadMinimum(held.payload)).toEqual(payloadMinimum(outstanding.envelope));
    const before = await p.brokerOutage?.state();
    if (before) expect(before).toMatchObject({ ackFloor: prime.sequence, delivered: outstanding.sequence, pending: 0, ackPending: 1 });
    // Cut actual owned sockets/listener and await real runtime disconnect status BEFORE send.
    // The carrier, its reconnect policy, USER.INFO check and 30s ack_wait remain untouched.
    await broker!.disconnect();
    // UNCERTAIN explicitly permits a queued publish to reach the broker after reconnect.
    // Address this separate outage probe to the sender's OWN inbox, so it cannot overtake
    // the receiver's held redelivery. Do not assert that an uncertain attempt never persisted.
    await expect(boundedReceipt(p.sender.send(command(p, nextId("T8-outage"), { to: p.sender.self })), "T8 real outage publish outcome", 10_000))
      .rejects.toMatchObject({ code: expect.stringMatching(/^FABRIC_TRANSPORT_(UNAVAILABLE|UNCERTAIN)$/) });
    const pending = iterator.next(); void pending.catch(() => undefined);
    await broker!.restart(); await broker!.reconnect();
    if (before) {
      const after = await p.brokerOutage!.state();
      expect(after.serverId, "actual nats-server incarnation changed, same persistent store").not.toBe(before.serverId);
      expect(after).toMatchObject({ durable: before.durable, filter: before.filter,
        ackFloor: before.ackFloor, pending: 0, ackPending: 1 });
    }
    // SAME iterator, no NAK/working/fabricated replay/reopen. The unchanged broker ack_wait
    // is the event source; allow a production-policy redelivery after the actual restart.
    const replay = await next({ next: () => pending }, contractTiming.ackWaitMs + 15_000, "T8 same live iterator broker replay");
    expect(replay.attempt).toBeGreaterThanOrEqual(2);
    expect(replay).toMatchObject({ sourceMachine: held.sourceMachine, verification: held.verification });
    expect(replay.sequence).toBe(outstanding.sequence); expect(payloadMinimum(replay.payload)).toEqual(payloadMinimum(held.payload));
    expect(JSON.stringify(replay.payload), "entire resolved envelope is byte-identical").toBe(JSON.stringify(held.payload));
    await replay.ack();
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
    if (p.brokerOutage) expect(await p.brokerOutage.state()).toMatchObject({ ackFloor: outstanding.sequence, pending: 0, ackPending: 0 });
  }, TIMEOUT, { brokerOutage: true });

  test("T9 real NATS durable reopen: stable hostId replays unacked work, confirmed ACK survives reopen", async (p, signal, skip) => {
    if (kind !== "nats") skip("durable broker cursor is NATS-only; FILE owner restart/control-seen parity is tested separately");
    if (!p.reopenReceiver) skip("real carrier reopen fixture missing; scripted replay is not restart proof");
    const sent = await p.sender.send(command(p)), host = { ...p.receiver.self };
    const initialAbort = new AbortController();
    const initial = stream(p, initialAbort.signal);
    const held = await next(initial); expect(held.sequence).toBe(sent.sequence);
    initialAbort.abort(); await initial.return?.(); // no ACK/NAK: keep real work outstanding
    await p.reopenReceiver!(); expect(p.receiver.self).toEqual(host);
    const replayAbort = new AbortController();
    const replayIterator = stream(p, replayAbort.signal);
    const replay = await next(replayIterator, contractTiming.ackWaitMs + 10_000);
    expect(replay).toMatchObject({ sequence: sent.sequence, attempt: 2, sourceMachine: p.sender.self.machine });
    expect(replay.payload.body).toEqual(held.payload.body); await replay.ack();
    replayAbort.abort(); await replayIterator.return?.();
    await p.reopenReceiver!(); expect(p.receiver.self).toEqual(host);
    expect(await p.receiver.drainState(signal)).toEqual({ pending: 0, ackPending: 0 });
    // Carrier close/reopen only, not SIGKILL or control-seen admission recovery.
  });

  if (kind === "nats") test("T-CLOSE-SUSPENDED actual close ends a yielded inbox without caller return or ACK/NAK; durable redelivers", async (p, signal) => {
    if (!p.reopenReceiver) throw new Error("T-CLOSE-SUSPENDED requires actual durable reopen, not a skip or scripted replay");
    const [{ ProtocolHandler }, { transportInboxStream, transportDurableName }] = await Promise.all([
      import("@nats-io/nats-core/internal"), import("../src/transport/nats/subjects.js"),
    ]);
    const streamName = transportInboxStream(p.receiver.self.machine), durable = transportDurableName(p.receiver.self.hostId);
    const settlements: string[] = [];
    const publish = ProtocolHandler.prototype.publish;
    // Observe actual official-client wire publishes, forwarding unchanged. Spying only on
    // delivery.ack()/nak() would miss a close implementation that settled its JsMsg directly.
    const settlementSpy = vi.spyOn(ProtocolHandler.prototype, "publish").mockImplementation(function (this: InstanceType<typeof ProtocolHandler>, subject, payload, options) {
      const tokens = subject.split(".");
      if (subject.startsWith("$JS.ACK.") && tokens.includes(streamName) && tokens.includes(durable)) {
        settlements.push(typeof payload === "string" ? payload : new TextDecoder().decode(payload));
      }
      return publish.call(this, subject, payload, options);
    });
    const inboxAbort = new AbortController();
    let iterator: AsyncIterator<Message> | undefined, closing: Promise<void> | undefined;
    try {
      const sent = await p.sender.send(command(p)), host = { ...p.receiver.self };
      iterator = stream(p, inboxAbort.signal);
      const held = await next(iterator);
      expect(held.sequence).toBe(sent.sequence);
      expect(payloadMinimum(held.payload)).toEqual(payloadMinimum(sent.envelope));
      expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 1 });
      // NO pending next(), abort or return(): public generator is suspended at its yield.
      // The known defect queues stop in ConsumerMessages.close() and waits for iterClosed,
      // which cannot advance while the outer generator still owns that inner yield.
      closing = p.receiver.close();
      await boundedReceipt(closing, "T-CLOSE-SUSPENDED: actual close must finish while delivery is in hand, without caller return", 2_000);
      expect(settlements, "close must not publish ACK, NAK, TERM or WPI for in-hand work").toEqual([]);
      expect(await boundedReceipt(iterator.next(), "closed public inbox ends without caller return", 2_000))
        .toEqual({ done: true, value: undefined });
      await p.reopenReceiver(); expect(p.receiver.self).toEqual(host);
      const replay = await next(stream(p, signal), contractTiming.ackWaitMs + 10_000);
      expect(replay).toMatchObject({ sequence: held.sequence, attempt: 2, sourceMachine: p.sender.self.machine });
      expect(payloadMinimum(replay.payload)).toEqual(payloadMinimum(held.payload));
      expect(settlements, "unacked durable work survived close without any implicit settlement").toEqual([]);
      await replay.ack();
      expect(settlements, "positive control: observer sees the replay's real double ACK").toContain("+ACK");
      expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
    } finally {
      // Cleanup is deliberately AFTER the hard-failing close receipt. Returning here may
      // unblock the bad close, but cannot resolve/replace its already rejected deadline.
      inboxAbort.abort();
      try {
        if (iterator) await boundedReceipt(Promise.resolve().then(() => iterator!.return?.()), "T-CLOSE-SUSPENDED cleanup: return actual yielded iterator");
        if (closing) await boundedReceipt(closing, "T-CLOSE-SUSPENDED cleanup: join original actual close after return");
      } finally { settlementSpy.mockRestore(); }
      // Outer finally closes the actual ports and stops B's owned real server, then returns
      // remaining test-owned readers even if either cleanup operation above fails.
    }
  });

  test("T9 control-plane owner restart: real stable hostId/control-seen prevents repeated admission", async (p, _signal, skip) => {
    const cp = await control(p, skip);
    try {
      const key = nextId("T9"); expect(await cp.request(key)).toMatchObject({ acknowledged: true });
      await cp.receiver.close();
      // The stub models one iterator per process instance. A restart opens a fresh
      // instance on the same bus; it is not an implicit redelivery or claim engine.
      const replacement = p.bus ? p.bus.open(p.receiver.self) : p.raw?.receiver ?? p.receiver;
      const restartOptions = { ...cp.receiver.options, transport: replacement };
      const restarted = new cp.Class(cp.receiver.mesh, cp.receiver.identity, restartOptions);
      expect(restarted.options.hostId).toBe(cp.receiver.options.hostId);
      const calls: string[] = []; restarted.start(cmd => { calls.push(cmd.commandId); return { accepted: true }; });
      try {
        // A fresh carrier event carries the same logical commandId (E1/E14). Only the
        // production restarted control-seen reader may recover/dedupe its prior outcome.
        expect(await cp.request(key)).toMatchObject({ acknowledged: true }); expect(calls).toEqual([]);
      } finally { await restarted.close(); if (p.bus) await replacement.close(); }
    } finally { await cp.close(); }
  });

  test("T10 actual abort and stamp throw before persistence: exact refusal, nothing stored", async (p) => {
    const original = command(p), abort = new AbortController(); abort.abort(); let stamps = 0;
    await expect(p.sender.send({ ...original, body: at => { stamps++; return { version: 1, commandId: original.id,
      targetId: original.targetId, operation: original.operation, ownerIncarnation: "incarnation-1", replyTo: "sender", requestedAt: at, deadlineAt: at + 1000 }; } }, abort.signal))
      .rejects.toMatchObject({ name: "AbortError" });
    expect(stamps).toBe(0); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
    const reason = new Error("stamp refused"); await expect(p.sender.send({ ...original, body: () => { throw reason; } })).rejects.toBe(reason);
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
    // A real post-publish abort race/lost PubAck needs its own boundary injection (T-B1).
    // Do not manufacture that evidence by throwing after a successful product send.
  });

  test("T11 E25 isolated byte-ceiling FAULT: real capacity refusal + report-only operator limit, no eviction", async (p, signal, skip) => {
    if (kind !== "nats") skip("T11 E25 requires a real JetStream capacity refusal/operator report; no FILE/stub capacity model");
    const boundary = p.capacityBoundary;
    if (!boundary) throw new Error("T11 requires its pair-owned real capacity fault/report fixture, not a synthetic alarm");
    const { TransportCapacityError } = await import("../src/transport/types.js");
    // Both factory carriers already opened against the exact bootstrap-provisioned 1GiB
    // policy with signed machine JWT + USER.INFO verification. No direct-open override or
    // runtime privilege expansion. ONLY the following explicit isolated-stream fault is tiny.
    const kept = await p.sender.send(command(p, "T11-kept-0001"));
    const before = await boundary.report();
    expect(before).toMatchObject({ max_bytes: 1_073_741_824, max_msg_size: 1_048_576,
      discard: "new", max_age: 0, messages: 1, first_seq: kept.sequence, last_seq: kept.sequence });
    expect(before.bytes).toBeGreaterThan(0); expect(before.bytes).toBeLessThan(before.max_bytes);
    const fault = await boundary.limitToStoredBytes(); // admin UPDATE + INFO, closed before send
    expect(fault).toEqual({ ...before, max_bytes: before.bytes,
      max_msg_size: Math.min(before.max_msg_size, before.bytes) });
    const full = await boundary.report(); // actual E19 maintainer STREAM.INFO, connection closed
    expect(full).toEqual(fault); expect(full.bytes).toBe(full.max_bytes);

    // Same normal-size resolved envelope, distinct same-length id: capacity refusal must NOT
    // be an oversized-message probe. No body restamp/fill loop, no dedupe hit or payload inflation.
    const refused = { ...kept.envelope, id: "T11-deny-0002",
      body: { ...kept.envelope.body, commandId: "T11-deny-0002" } };
    const wireBytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
    expect(wireBytes(refused)).toBe(wireBytes(kept.envelope));
    expect(wireBytes(refused)).toBeLessThanOrEqual(full.max_msg_size);
    const refusal = p.sender.send(refused);
    await expect(refusal).rejects.toBeInstanceOf(TransportCapacityError);
    await expect(refusal).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_CAPACITY", persisted: false });
    // Real server state: no second persistence and no DiscardOld eviction, BEFORE consuming.
    const after = await boundary.report();
    expect(after).toEqual(full); expect(after.bytes).toBe(after.max_bytes);
    const delivered = await next(stream(p, signal));
    expect(delivered).toMatchObject({ sequence: kept.sequence, attempt: 1,
      sourceMachine: p.sender.self.machine, verification: "bridge" });
    expect(payloadMinimum(delivered.payload)).toEqual(payloadMinimum(kept.envelope));
    expect(JSON.stringify(delivered.payload)).toBe(JSON.stringify(kept.envelope));
    await delivered.ack(); // real adapter's confirmed broker ACK, not an application ACK model
    expect(await p.receiver.drainState(signal)).toEqual({ pending: 0, ackPending: 0 });
    // E25 = sender refusal + E15 metadata report. Production alarm emitter is #13951;
    // this fault policy is NOT evidence of a tiny production factory configuration.
  }, TIMEOUT, { capacityBoundary: true });

  test("T12 KV lease CAS: bucket-wide revisions, renew/release/conflict, last-write TTL; no takeover authority", async (p, _signal, skip) => {
    if (kind === "file") skip("file kv() is explicitly unsupported; file directory parity is T20");
    const kv = p.receiver.kv("leases"), key = `${p.receiver.self.machine}.h.one`, other = `${p.receiver.self.machine}.h.two`;
    expect(kv.ttlMs).toBe(contractTiming.leaseTtlMs); expect(p.receiver.kv("presence").ttlMs).toBe(contractTiming.presenceTtlMs);
    const first = await kv.put(key, "owner#one", { n: 1 }, 0);
    await expect(kv.put(key, "other#two", {}, 0)).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT", key, expected: 0 });
    const different = await kv.put(other, "owner#one", {}, 0); expect(different).toBeGreaterThan(first);
    const renewed = await kv.put(key, "owner#one", { n: 2 }, first); expect(renewed).toBeGreaterThan(different);
    expect(await kv.delete(key, first)).toBe(false); expect(await kv.delete(key, renewed)).toBe(true);
    const successor = await kv.put(key, "owner#released", {}, 0); expect(successor).toBeGreaterThan(renewed);
    await expect(kv.put(key, "owner#one", {}, renewed)).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT" });
    if (p.bus) { p.advance(kv.ttlMs + 1); expect(await kv.get(key)).toBeUndefined(); }
    // A raw create after expiry is NOT host takeover; A2 requires clean release or local exit proof.
  });

  test("T13 paused live holder beyond TTL: actual A2 stops consumption; no successor until local exit proof", async (_p, _signal, skip) => {
    const dependency = await loadControlPlane({ transportIntegrated, required }); if (dependency.status === "unavailable") skip(dependency.reason);
    const lease = integrations[kind]?.lease;
    if (!lease) skip("actual A2 host-lease/pause fixture absent; no fake lease/admission implementation");
    const { ParticipantDirectory } = await import("../src/topology/participant-directory.js");
    expect(lease!.directory).toBeInstanceOf(ParticipantDirectory);
    await lease!.pause(); expect(await lease!.successor()).toBe(false); await lease!.resume();
    expect(lease!.directory.canConsumeMesh()).toBe(false);
    expect(await lease!.successor()).toBe(false); await lease!.proveExit(); expect(await lease!.successor()).toBe(true);
  });

  test("T14 presence watch: empty/nonempty ready, live put, reset→snapshot→ready", async (p, signal, skip) => {
    if (kind === "file") skip("file adapter has no KV capability");
    const kv = p.receiver.kv("presence"), key = `${p.receiver.self.machine}.p.one`;
    // The stub and real adapter use the same whole-token watch filter (§4.4).
    const prefix = `${p.receiver.self.machine}.>`;
    const iterator = kv.watch(prefix, signal)[Symbol.asyncIterator](); expect(await ready(iterator)).toEqual([{ type: "ready" }]);
    const revision = await kv.put(key, "owner#one", { id: "one" }, 0);
    expect(await next(iterator)).toMatchObject({ type: "put", entry: { key, revision } });
    if (!p.bus) skip("T14 legacy stub reset script only; real ordered-consumer deletion is covered by T-READY reset, not broker reconnect proof");
    p.bus!.resetKvWatches(); expect(await next(iterator)).toEqual({ type: "reset" });
    expect(await ready(iterator)).toEqual([
      { type: "put", entry: { key, machine: p.receiver.self.machine, owner: "owner#one", value: { id: "one" }, revision } }, { type: "ready" },
    ]);
    // An open/put race uses the actual KV iterator: pending next and put, no readiness sleep.
    const pending = iterator.next(); const newer = await kv.put(key, "owner#one", { id: "two" }, revision);
    expect(await next({ next: () => pending })).toMatchObject({ type: "put", entry: { revision: newer } });
  });

  test("T-READY empty bucket: explicit ready, then live puts without another ready", async (p, signal, skip) => {
    if (kind === "file") skip("file adapter has no KV capability; T-READY is not file directory parity");
    const kv = p.receiver.kv("presence"), machine = p.receiver.self.machine;
    // Integration.pair() must supply empty isolated buckets; no keys() emptiness inference.
    const iterator = kv.watch(`${machine}.>`, signal)[Symbol.asyncIterator]();
    expect(await next(iterator), "an empty snapshot still has a completion receipt").toEqual({ type: "ready" });
    const key = `${machine}.p.${nextId("ready-empty")}`, writer = "owner#ready-empty";
    const first = await kv.put(key, writer, { generation: 1 }, 0, signal);
    expect(await next(iterator)).toEqual({ type: "put", entry: { key, machine, owner: writer, value: { generation: 1 }, revision: first } });
    const second = await kv.put(key, writer, { generation: 2 }, first, signal);
    expect(await next(iterator)).toEqual({ type: "put", entry: { key, machine, owner: writer, value: { generation: 2 }, revision: second } });
    // Live receipts fence the one marker without a silence/sleep assertion or background reader.
  });

  test("T-READY final snapshot entry is data: full snapshot, separate ready exactly once, then live", async (p, signal, skip) => {
    if (kind === "file") skip("file adapter has no KV capability");
    const kv = p.receiver.kv("presence"), machine = p.receiver.self.machine, writer = "owner#ready-snapshot";
    const expected: TransportKvEntry[] = [];
    for (let i = 0; i < 3; i++) {
      const key = `${machine}.p.${nextId("ready-snapshot")}`, value = { id: i };
      const revision = await kv.put(key, writer, value, 0, signal);
      expected.push({ key, machine, owner: writer, value, revision });
    }
    const iterator = kv.watch(`${machine}.>`, signal)[Symbol.asyncIterator]();
    await snapshotThenReady(iterator, expected);
    const key = `${machine}.p.${nextId("ready-live")}`, revision = await kv.put(key, writer, { live: true }, 0, signal);
    expect(await next(iterator), "no duplicate ready between snapshot and first live change")
      .toEqual({ type: "put", entry: { key, machine, owner: writer, value: { live: true }, revision } });
    expect(await kv.delete(key, revision, signal)).toBe(true);
    expect(await next(iterator), "no extra ready after entering live mode").toMatchObject({ type: "gone", key, reason: "deleted" });
  });

  test("T-READY reset: reset precedes a fresh full snapshot and one new ready, never continuous readiness", async (p, signal, skip) => {
    if (kind === "file") skip("file adapter has no KV capability");
    const fixture = integrations[kind]?.kvReadiness;
    const reset: (bucket: TransportBucket, prefix: string, signal: AbortSignal) => Promise<void> =
      p.bus ? async () => { p.bus!.resetKvWatches(); }
      : readinessCapability(p.kvReadiness?.reset.bind(p.kvReadiness) ?? fixture?.reset?.bind(fixture), skip,
        "B real ordered-consumer reset hook absent; scripted stub reset is §1 ordering only");
    const kv = p.receiver.kv("presence"), machine = p.receiver.self.machine, prefix = `${machine}.>`, writer = "owner#ready-reset";
    const key = `${machine}.p.${nextId("ready-reset")}`, value = { generation: 1 };
    const first = await kv.put(key, writer, value, 0, signal);
    const iterator = kv.watch(prefix, signal)[Symbol.asyncIterator]();
    await snapshotThenReady(iterator, [{ key, machine, owner: writer, value, revision: first }]);
    const updated = { generation: 2 }, second = await kv.put(key, writer, updated, first, signal);
    expect(await next(iterator)).toMatchObject({ type: "put", entry: { key, revision: second, value: updated } });
    const other = `${machine}.p.${nextId("ready-reset-other")}`, otherValue = { generation: 3 };
    const third = await kv.put(other, writer, otherValue, 0, signal);
    expect(await next(iterator)).toMatchObject({ type: "put", entry: { key: other, revision: third } });
    await reset("presence", prefix, AbortSignal.any([signal, AbortSignal.timeout(5_000)]));
    expect(await next(iterator, kind === "nats" ? 75_000 : 5_000), "ready must be invalidated before any replacement snapshot data").toEqual({ type: "reset" });
    await snapshotThenReady(iterator, [
      { key, machine, owner: writer, value: updated, revision: second },
      { key: other, machine, owner: writer, value: otherValue, revision: third },
    ]);
    expect(await kv.delete(other, third, signal)).toBe(true);
    const deleted = await next(iterator);
    expect(deleted, "the replacement ready occurs once, not again on a live delete")
      .toMatchObject({ type: "gone", key: other, machine, reason: "deleted" });
    if (deleted.type !== "gone") throw new Error("live deletion must yield its own tombstone receipt");
    expect(deleted.revision).toBeGreaterThan(third);
    // A second reset must snapshot current storage, not replay the deleted old snapshot member.
    await reset("presence", prefix, AbortSignal.any([signal, AbortSignal.timeout(5_000)]));
    expect(await next(iterator, kind === "nats" ? 75_000 : 5_000)).toEqual({ type: "reset" });
    // The real last-per-subject snapshot retains the exact newer DEL for `other`. The stub
    // snapshots only live entries. Require the broker tombstone rather than silently skipping
    // it (or filtering to `key`), so stale resurrection and missing ready still fail.
    await snapshotThenReady(iterator, [{ key, machine, owner: writer, value: updated, revision: second }], kind === "nats" ? [deleted] : []);
    const final = await kv.put(key, writer, { generation: 4 }, second, signal);
    expect(await next(iterator)).toMatchObject({ type: "put", entry: { key, revision: final, value: { generation: 4 } } });
    // These are PORT receipts. Real A2 reset→not-ready→new ready view composition is covered
    // separately in transport-faults/presence-leases.test.ts (hosted reset opt-in). That public
    // class proof is not installed application-root wiring coverage.
    // Each real deletion can take two missed 30s heartbeats. Both have separate 75s receipt
    // deadlines and leave room for setup/cleanup; no consumer-info polling or fake reset.
  }, kind === "nats" ? 210_000 : TIMEOUT);

  if (kind === "nats") for (const reason of ["deleted", "expired"] as const) {
    test(`T-READY real ${reason} after ordered snapshot creation before consumption: broker gone→ready→live`, async (p, signal) => {
      if (!p.kvReadiness) throw new Error("B real-broker KV scheduling probe is required, not a synthetic watch fallback");
      // This is B's demonstrated before-consume seam, NOT the unavailable stronger hold
      // after snapshot puts have escaped. The marker-after-data cases below stay separate.
      const bucket = reason === "expired" ? "leases" : "presence";
      const kv = p.receiver.kv(bucket), machine = p.receiver.self.machine, writer = "owner#ready-broker-race";
      const key = `${machine}.${bucket === "leases" ? "h" : "p"}.${nextId("ready-broker-race")}`;
      if (reason === "expired") expect(kv.ttlMs, "real contract lease policy, never a shortened test TTL or 600s presence wait").toBe(15_000);
      const revision = await kv.put(key, writer, { generation: 1 }, 0, signal);
      const bounded = AbortSignal.any([signal, AbortSignal.timeout(30_000)]);
      const probe = await p.kvReadiness.holdConsumption(bucket, key, bounded);
      const watchAbort = new AbortController();
      const iterator = probe.store.watch(key, AbortSignal.any([signal, watchAbort.signal]))[Symbol.asyncIterator]();
      let first: Promise<IteratorResult<TransportKvEvent>> | undefined;
      try {
        first = iterator.next(); void first.catch(() => undefined);
        await boundedReceipt(probe.created, "real ordered snapshot consumer reached consume boundary");
        let markerRevision: number | undefined;
        if (reason === "deleted") expect(await kv.delete(key, revision, bounded)).toBe(true);
        else markerRevision = await probe.expiry(key, revision, bounded);
        await probe.release();
        const initial = await boundedReceipt(first, "broker removal during held snapshot consumption");
        expect(initial.done).not.toBe(true);
        expect(initial.value).toMatchObject({ type: "gone", key, machine, reason });
        if (initial.value?.type !== "gone") throw new Error("real snapshot lost its broker tombstone/expiry marker");
        expect(initial.value.revision).toBeGreaterThan(revision);
        if (markerRevision !== undefined) expect(initial.value.revision).toBe(markerRevision);
        expect(await next(iterator), "only a separate official snapshot-complete receipt makes the watch ready").toEqual({ type: "ready" });
        expect(await kv.get(key, bounded)).toBeUndefined(); // storage check only, NOT a barrier
        const replacement = { generation: 2 }, replaced = await kv.put(key, writer, replacement, 0, bounded);
        expect(replaced).toBeGreaterThan(initial.value.revision);
        expect(await next(iterator), "no stale snapshot put or duplicate ready after the broker gone")
          .toEqual({ type: "put", entry: { key, machine, owner: writer, value: replacement, revision: replaced } });
        expect(await kv.delete(key, replaced, bounded)).toBe(true);
        expect(await next(iterator), "live delete fences the one ready and replacement")
          .toMatchObject({ type: "gone", key, machine, reason: "deleted" });
      } finally {
        watchAbort.abort();
        await probe.release();
        try {
          await boundedReceipt(Promise.resolve().then(() => iterator.return?.()), "return real gated KV watch");
          if (first) await boundedReceipt(first, "join first gated watch receipt during cleanup");
        } finally { await boundedReceipt(probe.close(), "close real gated KV runtime connection"); }
      }
    });
  }

  for (const reason of ["deleted", "expired"] as const) {
    test(`T-READY ${reason} during snapshot: gone is retained before live puts, stale data cannot resurrect`, async (p, signal, skip) => {
      if (kind === "file") skip("file adapter has no KV capability");
      const fixture = integrations[kind]?.kvReadiness;
      const holdSnapshot = p.bus ? undefined : readinessCapability(fixture?.holdSnapshot?.bind(fixture), skip,
        "B ordered-push snapshot-boundary hold hook absent; no real concurrent snapshot proof");
      const expire = reason === "expired" && !p.bus ? readinessCapability(fixture?.expire?.bind(fixture), skip,
        "B real MaxAge-during-snapshot receipt hook absent; no synthetic expiry as broker proof") : undefined;
      const bucket = reason === "expired" ? "leases" : "presence";
      const kv = p.receiver.kv(bucket), machine = p.receiver.self.machine, prefix = `${machine}.>`;
      const writer = "owner#ready-race", token = bucket === "leases" ? "h" : "p";
      const expected: TransportKvEntry[] = [];
      for (let i = 0; i < 3; i++) {
        const key = `${machine}.${token}.${nextId("ready-race")}`, value = { generation: 1, id: i };
        const revision = await kv.put(key, writer, value, 0, signal);
        expected.push({ key, machine, owner: writer, value, revision });
      }
      // For a real adapter, arm B's actual boundary gate BEFORE opening the watch. For the stub,
      // this is only a queued-snapshot interleaving: no gate, server, hidden-reset or timer proof.
      const bounded = AbortSignal.any([signal, AbortSignal.timeout(Math.min(kv.ttlMs + 10_000, 30_000))]);
      const held = holdSnapshot ? await holdSnapshot(bucket, prefix, bounded) : undefined;
      try {
        const iterator = kv.watch(prefix, signal)[Symbol.asyncIterator]();
        const first = await next(iterator);
        expect(first.type, "race must begin with snapshot data, not ready").toBe("put");
        if (first.type !== "put") throw new Error("no snapshot entry available for concurrent mutation");
        expect(expected).toContainEqual(first.entry);
        // Mutate a DIFFERENT, still-unconsumed snapshot key. Mutating only the first key would
        // miss an adapter replaying stale buffered data after applying a tombstone.
        const victim = expected.find(entry => entry.key !== first.entry.key)!;
        if (reason === "deleted") expect(await kv.delete(victim.key, victim.revision, signal)).toBe(true);
        else if (p.bus) p.advance(kv.ttlMs + 1);
        else await expire!(bucket, victim.key, victim.revision, bounded);
        const liveKey = `${machine}.${token}.${nextId("ready-after-gone")}`, liveValue = { live: true };
        const liveRevision = await kv.put(liveKey, writer, liveValue, 0, signal);
        await held?.release();
        const events: TransportKvEvent[] = [first];
        let sawLive = false;
        for (let i = 0; i < 20; i++) {
          const event = await next(iterator); events.push(event);
          expect(event.type, "a mutation during a held snapshot is not a watch reset").not.toBe("reset");
          if (event.type === "put" && event.entry.key === liveKey) {
            expect(event.entry).toEqual({ key: liveKey, machine, owner: writer, value: liveValue, revision: liveRevision });
            sawLive = true; break;
          }
        }
        expect(sawLive, "bounded live-put fence was not delivered").toBe(true);
        expect(events.filter(event => event.type === "ready"), "one completion marker despite concurrent removal")
          .toEqual([{ type: "ready" }]);
        const goneAt = events.findIndex(event => event.type === "gone" && event.key === victim.key);
        expect(goneAt, "gone must not be lost or deferred past the subsequent live put").toBeGreaterThan(0);
        const gone = events[goneAt];
        expect(gone).toMatchObject({ type: "gone", key: victim.key, machine, reason });
        if (gone?.type !== "gone") throw new Error("missing snapshot-race tombstone");
        expect(gone.revision).toBeGreaterThan(victim.revision);
        expect(liveRevision).toBeGreaterThan(gone.revision);
        expect(events.slice(goneAt + 1).filter(event => event.type === "put" && event.entry.key === victim.key),
          "no buffered snapshot put may resurrect a key after its gone").toEqual([]);
        expect(await kv.get(victim.key, signal)).toBeUndefined(); // Storage check, NOT readiness.
        const replacement = { generation: 2, afterGone: true };
        const replacementRevision = await kv.put(victim.key, writer, replacement, 0, signal);
        expect(replacementRevision).toBeGreaterThan(liveRevision);
        expect(await next(iterator), "only a new successful put may bring the removed key back")
          .toEqual({ type: "put", entry: { key: victim.key, machine, owner: writer, value: replacement, revision: replacementRevision } });
        const fenceRevision = await kv.put(liveKey, writer, { live: "after-replacement" }, liveRevision, signal);
        expect(await next(iterator), "no late stale snapshot data or second ready after the replacement")
          .toMatchObject({ type: "put", entry: { key: liveKey, revision: fenceRevision, value: { live: "after-replacement" } } });
        expect(await kv.get(victim.key, signal)).toEqual({ key: victim.key, machine, owner: writer, value: replacement, revision: replacementRevision });
      } finally { await held?.release(); }
    });
  }

  test("T15 delete/MaxAge expiry markers: gone reasons, machine and monotonic revisions", async (p, signal, skip) => {
    if (kind === "file") skip("file adapter has no KV capability");
    const kv = p.receiver.kv("leases"), key = `${p.receiver.self.machine}.h.one`;
    const iterator = kv.watch(`${p.receiver.self.machine}.>`, signal)[Symbol.asyncIterator]();
    await ready(iterator); const first = await kv.put(key, "owner#one", {}, 0); await next(iterator);
    expect(await kv.delete(key, first)).toBe(true);
    const deleted = await next(iterator); expect(deleted).toMatchObject({ type: "gone", key, machine: p.receiver.self.machine, reason: "deleted" });
    const second = await kv.put(key, "owner#two", {}, 0); expect(second).toBeGreaterThan(first); await next(iterator);
    if (p.bus) p.advance(kv.ttlMs + 1);
    // Real server MaxAge marker is the event source; this finite receipt deadline is not polling.
    const expired = await next(iterator, p.bus ? 5_000 : kv.ttlMs + 10_000);
    expect(expired).toMatchObject({ type: "gone", key, machine: p.receiver.self.machine, reason: "expired" });
    if (expired.type === "gone") expect(expired.revision).toBeGreaterThan(second);
    expect(await kv.get(key)).toBeUndefined();
  });

  // T16 is supplied by B's real server suites in the required hosted nats-auth job:
  // tests/nats-auth-path-b-cli.test.ts:236-263 uses a CLI-minted machine-A JWT against
  // foreign FABRIC_IN_machine-b CREATE/INFO/MSG.NEXT/DELETE and a LIVE foreign ACK reply,
  // plus forged source, MSG.GET, foreign KV and private inbox. Companion server suite
  // T-OPS-NOREAD adds DIRECT.GET, report-only operator and stream lifecycle denials.
  // PermissionOracle checks real PermissionViolationError.operation/subject, not a local
  // allow-list. This standalone optional probe is not independently made green by citation.
  test("T16 real permission negatives: source spoof/foreign consumers/ACK/raw reads/KV/inbox/stream lifecycle", async (p, _signal, skip) => {
    const b = integrations[kind]?.broker;
    if (!b || kind !== "nats") skip("T16 real JWT matrix is owned by tests/nats-auth-path-b-cli.test.ts:236-263 and server T-OPS-NOREAD, both required hosted nats-auth suites; no stub allow-list proof");
    const foreign = `FABRIC_IN_${b!.foreignMachine}`;
    const denied: [string, string][] = [
      ["publish", `fabric.in.${b!.foreignMachine}.owner.cmd.target.${b!.foreignMachine}`],
      ...["CREATE", "MSG.NEXT", "INFO", "DELETE"].map(v => ["request", `$JS.API.CONSUMER.${v}.${foreign}.rt_foreign`] as [string, string]),
      ["publish", `$JS.ACK.${foreign}.rt_foreign.1.1.1.1.0`],
      ...[foreign, `FABRIC_IN_${p.sender.self.machine}`].flatMap(stream => [
        ["request", `$JS.API.STREAM.MSG.GET.${stream}`], ["request", `$JS.API.DIRECT.GET.${stream}`],
        ["request", `$JS.API.DIRECT.GET.${stream}.fabric.in.test`],
      ] as [string, string][]),
      ...["CREATE", "UPDATE", "DELETE", "PURGE"].map(v => ["request", `$JS.API.STREAM.${v}.${foreign}`] as [string, string]),
      ["publish", `$KV.fabric_presence.${b!.foreignMachine}.p.one`], ["publish", `$KV.fabric_leases.${b!.foreignMachine}.h.one`],
      ["subscribe", `_INBOX.${b!.foreignUser}.probe`],
    ];
    for (const [verb, subject] of denied) expect(await b!.probe(verb, subject), subject).toBe("denied");
    expect(await b!.probe("subscribe", b!.ownInbox)).toBe("allowed");
  });

  test("T17 forged sender: E23 FILE same-UID parity; stub/NATS presence/source verification refuses notRun", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      const forged = command(p, nextId("T17"), { from: identity("session:unknown"), operation: "stop" });
      await p.sender.send(forged);
      const ack = await next(stream(p, signal, p.sender));
      if (kind === "file") {
        // E23 + D10 / smarty-dev#9641: labels on the same-UID FILE mesh are not a
        // same-host trust boundary. Preserve legacy acceptance, not NATS verification.
        expect(ack.payload.body).toMatchObject({ commandId: forged.id, accepted: true });
        expect(cp.admitted).toEqual([forged.id]);
      } else {
        expect(ack.payload.body).toMatchObject({ commandId: forged.id, accepted: false, notRun: true, errorCode: "FABRIC_SENDER_UNVERIFIED" });
        expect(cp.admitted).toEqual([]);
      }
      await ack.ack();
    } finally { await cp.close(); }
  });

  test("T18 stale incarnation: actual holder refuses; E23 FILE legacy ACK, stub/NATS notRun", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      const stale = command(p, nextId("T18"), { ownerIncarnation: "superseded", operation: "stop" }); await p.sender.send(stale);
      const ack = await next(stream(p, signal, p.sender));
      expect(ack.payload.body).toMatchObject({ accepted: false, errorCode: "FABRIC_CONTROL_STALE_INCARNATION" });
      if (kind === "file") {
        // E23: e931dbc8 control-plane.ts:1897–1899 emits this legacy refusal without
        // notRun. A carrier-only change must not add fields to the FILE ACK shape.
        expect(ack.payload.body).not.toHaveProperty("notRun");
      } else expect(ack.payload.body).toHaveProperty("notRun", true);
      expect(cp.admitted).toEqual([]); await ack.ack();
    } finally { await cp.close(); }
  });

  test("T19 expiry: actual production durable refusal; carrier stamp preserves full delivery budget", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      const id = nextId("T19");
      const expired = command(p, id, { operation: "stop", body: at => ({ version: 1, commandId: id, targetId: p.receiver.self.hostId,
        operation: "stop", replyTo: p.sender.self.hostId, ownerIncarnation: "incarnation-1", requestedAt: at - 1000, deadlineAt: at - 1 }) });
      await p.sender.send(expired); const ack = await next(stream(p, signal, p.sender));
      expect(ack.payload.body).toMatchObject({ accepted: false, notRun: true, error: "Fabric control command expired" });
      expect(cp.admitted).toEqual([]); await ack.ack();
      const sent = await p.sender.send(command(p));
      expect(sent.envelope.body.requestedAt).toBe(sent.envelope.createdAt);
      expect(sent.envelope.body.deadlineAt - sent.envelope.body.requestedAt).toBe(60_000);
      // P4 commit-lock delay regression still needs real lock contention in T-FENCE/T19 integration.
    } finally { await cp.close(); }
  });

  test("A1 actual control dispatch: fresh followUp/steer/stop requests reach real durable custody", async (p, _signal, skip) => {
    const cp = await control(p, skip);
    try {
      for (const operation of ["followUp", "steer", "stop"] as const) {
        expect(await cp.request(nextId(`A1-${operation}`), operation)).toMatchObject({ acknowledged: true });
      }
      expect(cp.admitted).toHaveLength(3); expect(new Set(cp.admitted).size).toBe(3);
    } finally { await cp.close(); }
  });

  test("T-LOCK actual NATS A1: delivery→custody→ACK PubAck→confirmed consumer ACK uses zero global mesh locks", async (p, signal, skip) => {
    if (kind !== "nats") skip("I-LOCK is NATS-only; FILE parity does not promise lock-free dispatch");
    const { MeshLock } = await import("../src/mesh/mesh-lock.js");
    const locks: Array<{ root: string; commandInHand: readonly string[] }> = [];
    const inHand = new Set<string>();
    const confirmed = new Map<string, Promise<void>>();
    const receiver = p.raw?.receiver ?? p.receiver;
    const inbox = receiver.inbox.bind(receiver);
    // Observation only: every byte, provenance, working/NAK/TERM and final ACK still belongs
    // to the real adapter. Resolve only AFTER its real broker double ACK has completed.
    const receiptSpy = vi.spyOn(receiver, "inbox").mockImplementation(async function* (abort) {
      for await (const delivery of inbox(abort)) {
        if (delivery.payload.kind !== "command") { yield delivery; continue; }
        const id = delivery.payload.id;
        let resolve!: () => void;
        const commandId = (delivery.payload.body as { commandId: string }).commandId;
        expect(typeof commandId, "actual delivery carries its logical native-custody id").toBe("string");
        confirmed.set(commandId, new Promise<void>(done => { resolve = done; }));
        inHand.add(id);
        yield { ...delivery, ack: async () => {
          await delivery.ack(); inHand.delete(id); resolve();
        } };
      }
    });
    const withLock = MeshLock.prototype.withLock;
    const lockSpy = vi.spyOn(MeshLock.prototype, "withLock").mockImplementation(function (this: InstanceType<typeof MeshLock>, operation, timeout, lockClass, bounded) {
      // Record actual critical-section entry, not just a refused lock attempt. The whole active
      // dispatch window is observed, stronger than relying on ALS propagated through an iterator.
      return withLock.call(this, () => {
        locks.push({ root: this.root, commandInHand: [...inHand] }); return operation();
      }, timeout, lockClass, bounded);
    });
    let cp: Awaited<ReturnType<typeof control>> | undefined;
    try {
      cp = await control(p, skip);
      const globalRoots = new Set([cp.sender.mesh.root, cp.receiver.mesh.root]);
      for (const operation of ["followUp", "steer", "stop"] as const) {
        const result = await cp.request(nextId(`T-LOCK-${operation}`), operation);
        expect(result.acknowledged).toBe(true);
        const completion = confirmed.get(result.messageId);
        expect(completion, "real command delivery was observed before its admission ACK").toBeDefined();
        await next({ next: async () => { await completion!; return { done: false, value: true }; } });
      }
      expect(inHand.size, "all observed commands have a confirmed real consumer ACK").toBe(0);
      expect(cp.admitted).toHaveLength(3);
      expect(locks.some(row => row.root.startsWith(join(cp!.receiver.mesh.root, "control-seen"))),
        "positive control: the real owner-local claim/outcome mutex was exercised").toBe(true);
      expect(locks.filter(row => globalRoots.has(row.root)), "I-LOCK: no global .lock over the entire active dispatch window").toEqual([]);
      expect(await receiver.drainState(signal)).toEqual({ pending: 0, ackPending: 0 });
      // E4: no completion envelope is a transport target. Real Main/task native-custody probes
      // remain an additional gate; this test does not claim to run models or AgentManager.
    } finally {
      try { await cp?.close(); }
      finally { lockSpy.mockRestore(); receiptSpy.mockRestore(); }
    }
  });

  test("T20 file parity: actual control-plane operations/idempotency; explicit unsupported KV", async (p, _signal, skip) => {
    if (kind !== "file") skip("file-only parity; no stub implementation of file control admission");
    const cp = await control(p, skip);
    try {
      for (const bucket of ["presence", "leases"] as const) expect(() => p.receiver.kv(bucket)).toThrowError(expect.objectContaining({ code: "FABRIC_TRANSPORT_UNSUPPORTED" }));
      for (const operation of ["followUp", "steer", "stop"] as const) {
        const key = nextId("T20"), first = await cp.request(key, operation), again = await cp.request(key, operation);
        expect(first).toEqual(again); expect(first.acknowledged).toBe(true);
      }
      expect(cp.admitted).toHaveLength(3);
      // Existing unchanged router/control tests are an ADDITIONAL gate, not claimed executed here.
    } finally { await cp.close(); }
  });

  test("T21 actual AgentManager spawn: presence before launch/return, immediate remote followUp addressable", async (_p, _signal, skip) => {
    const dependency = await loadControlPlane({ transportIntegrated, required });
    if (dependency.status === "unavailable") return skip(dependency.reason);
    const spawn = integrations[kind]?.spawn; if (!spawn) skip("actual A2 AgentManager/directory/router composition dependency absent");
    const [{ AgentManager }, { ParticipantDirectory }, { AgentMessageRouter }] = await Promise.all([
      import("../src/agents/manager.js"), import("../src/topology/participant-directory.js"), import("../src/providers/agents-message-router.js"),
    ]);
    expect(spawn!.manager).toBeInstanceOf(AgentManager); expect(spawn!.directory).toBeInstanceOf(ParticipantDirectory);
    expect(spawn!.router).toBeInstanceOf(AgentMessageRouter);
    expect(spawn!.sender).toBeInstanceOf(dependency.module.FabricControlPlane);
    expect(spawn!.receiver).toBeInstanceOf(dependency.module.FabricControlPlane);
    spawn!.receiver.start(spawn!.router.acceptControl.bind(spawn!.router));
    spawn!.sender.start(() => ({ accepted: false }));
    const native = vi.spyOn(spawn!.manager, "followUp");
    const run = await spawn!.manager.spawn(spawn!.request); expect(run.id).toMatch(/^[a-f0-9]{32}$/);
    expect(spawn!.directory.get(run.id)).toMatchObject({ id: run.id });
    expect(await spawn!.sender.request(spawn!.receiver.options.hostId, run.id, "followUp",
      { ownerIncarnation: spawn!.receiver.incarnation, message: "immediate" })).toMatchObject({ acknowledged: true });
    expect(native).toHaveBeenCalledTimes(1);
    // T-SPAWN owns launch-order tracing and definite/uncertain KV failures; no session:<runId> surrogate.
  });

  test("T22 real rollback/re-enable: pause, 0/0 drain, same-commandId/control-seen reconciliation", async (p, _signal, skip) => {
    const cp = await control(p, skip);
    try {
      const flip = integrations[kind]?.flip; if (!flip) skip("real A1/A2 composition flip dependency absent; no fake rollback state machine");
      const key = nextId("T22"), first = await cp.request(key); expect(first.acknowledged).toBe(true);
      cp.sender.pause(); cp.receiver.pause(); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
      const file = await flip!("file");
      expect(file.sender).toBeInstanceOf(cp.Class); expect(file.receiver).toBeInstanceOf(cp.Class);
      expect(file.sender.options.hostId).toBe(cp.sender.options.hostId);
      expect(file.receiver.options.hostId).toBe(cp.receiver.options.hostId);
      expect(file.receiver.mesh.root).toBe(cp.receiver.mesh.root); // same carrier-independent control-seen root
      file.receiver.start(cp.native); file.sender.start(() => ({ accepted: false }));
      const retry = await file.sender.request(file.receiver.options.hostId, file.receiver.options.hostId, "followUp",
        { ownerIncarnation: "incarnation-1", message: "bounded input" }, file.receiver.options.hostId, { idempotencyKey: key });
      expect(retry).toEqual(first); expect(cp.admitted).toHaveLength(1);
      const nats = await flip!("nats");
      expect(nats.receiver).toBeInstanceOf(cp.Class);
      nats.receiver.start(cp.native); nats.sender.start(() => ({ accepted: false }));
      expect(await nats.sender.request(nats.receiver.options.hostId, nats.receiver.options.hostId, "followUp",
        { ownerIncarnation: "incarnation-1", message: "bounded input" }, nats.receiver.options.hostId,
        { idempotencyKey: nextId("T22-fresh") })).toMatchObject({ acknowledged: true });
      // E20's canary stops at any nonzero INFO counter; it never reads bodies or retries.
      // This optional source-level same-key probe does not authorize a canary retry. Atomic
      // FILE+48h hold and old-writer stop proof remain T-ROLLBACK/canary gates. E4 completion
      // journals remain same-host and never travel on either carrier during rollback.
    } finally { await cp.close(); }
  });

  test("T-FILE-POSTCOMMIT (E1) append then lost wake/receipt: resolved/uncertain, one real handler/ACK", async (p, _signal, skip) => {
    if (kind !== "file") skip("requires actual file adapter and integrated production control plane");
    const cp = await control(p, skip);
    try {
      const [{ FileTransport }, { TransportUncertainError }] = await Promise.all([
        import("../src/transport/file.js"), import("../src/transport/types.js"),
      ]);
      // FILE CP deliberately uses legacy publish, not generic send reconciliation.
      // Inject only AFTER the real shared mesh append; never pre-label the error uncertain.
      const mesh = cp.sender.mesh, publish = mesh.publish.bind(mesh);
      const postAppendError = new Error("injected post-append wake/receipt timeout");
      let injected = false, original: Awaited<ReturnType<typeof publish>> | undefined;
      const spy = vi.spyOn(mesh, "publish").mockImplementation(async input => {
        const event = await publish(input);
        if (!injected && input.topic === "fabric.control.command" && input.kind !== "cancel") {
          expect(mesh.read({ topic: input.topic }).some(stored => stored.id === event.id)).toBe(true);
          original ??= event;
          injected = true; throw postAppendError;
        }
        return event;
      });
      try {
        const key = nextId("E1");
        await expect(cp.request(key)).rejects.toBe(postAppendError);
        expect(injected).toBe(true); expect(original).toBeDefined();
        const stored = original!;
        const body = stored.data as import("../src/topology/control-plane.js").FabricControlCommand;
        const adapter = p.raw!.sender;
        expect(adapter).toBeInstanceOf(FileTransport);
        expect((adapter as InstanceType<typeof FileTransport>).mesh).toBe(mesh);
        // A fresh failure exercises actual send's append fence/readback using the CP's
        // real immutable body and envelope. E1 permits a fresh native event id.
        injected = false;
        const portResult = await adapter.send({ id: body.commandId, kind: "command", operation: stored.kind,
          to: p.raw!.receiver.self, targetId: body.targetId, from: stored.from,
          ...(stored.principal ? { principal: stored.principal } : {}),
          ...(stored.sender ? { sender: stored.sender } : {}), body }).then(
          value => ({ value, error: undefined }), (error: unknown) => ({ value: undefined, error }));
        expect(injected).toBe(true);
        if (portResult.error !== undefined) {
          expect(portResult.error).toBeInstanceOf(TransportUncertainError);
          expect(portResult.error).toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN", id: body.commandId, resolvedBody: body });
          expect(portResult.error).not.toHaveProperty("persisted", false);
          expect(portResult.error).not.toHaveProperty("notDelivered", true);
        } else {
          expect(portResult.value?.sequence).toBeGreaterThan(0);
          expect(portResult.value?.envelope.body).toEqual(body);
        }
        spy.mockRestore();
        const first = await cp.request(key);
        expect(injected).toBe(true); expect(first).toMatchObject({ acknowledged: true });
        expect(await cp.request(key)).toEqual(first); expect(cp.admitted).toHaveLength(1);
        const commands = mesh.read({ topic: "fabric.control.command", limit: 100 }).filter(event => event.kind !== "cancel");
        expect(commands).toHaveLength(4); // legacy failure, send, CP retry, equal-outcome retry.
        expect(new Set(commands.map(event => event.id)).size).toBe(4);
        expect(new Set(commands.map(event => (event.data as typeof body).commandId))).toEqual(new Set([body.commandId]));
        const acknowledgements = mesh.read({ topic: "fabric.control.ack", limit: 100 });
        expect(acknowledgements.length).toBeGreaterThan(0);
        const receipts = acknowledgements.map(event => event.data as { commandId: string; messageId: string });
        expect(new Set(receipts.map(receipt => receipt.commandId))).toEqual(new Set([body.commandId]));
        expect(new Set(receipts.map(receipt => receipt.messageId))).toEqual(new Set([first.messageId]));
      } finally { spy.mockRestore(); }
    } finally { await cp.close(); }
  });

  test("§1 capability floor: term logs reason, close/abort end iterators without settling in-hand work", async (p, signal) => {
    await p.sender.send(command(p)); const iterator = stream(p, signal), held = await next(iterator);
    await held.term("malformed test envelope");
    if (p.stub) expect(p.stub.receiver.records[0]!.terms).toEqual(["malformed test envelope"]);
    await p.sender.send(command(p)); const unacked = await next(iterator), pending = iterator.next();
    await p.receiver.close(); expect((await pending).done).toBe(true);
    if (p.stub) {
      const record = p.stub.receiver.records.find(row => row.delivery.sequence === unacked.sequence)!;
      expect(record.acks).toBe(0); expect(record.naks).toEqual([]); expect(record.settled).toBe(false);
    }
  });
});
