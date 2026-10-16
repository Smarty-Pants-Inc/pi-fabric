import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { FabricControlPlane, FabricControlHandler } from "../src/topology/control-plane.js";
import type { AgentManager } from "../src/agents/manager.js";
import type { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import {
  adapterKinds, commandEnvelope, contractTiming, createStubBus, faultGate, FaultHooks,
  identity, loadControlPlane, loseNextPublishAck, openAdapter, owner, resolvedRetry, withFaults,
  type AdapterKind, type AdapterSpec, type CommandBody, type FabricTransport,
  type StubTransport, type TransportDelivery, type TransportEnvelope, type TransportKvEvent,
} from "./helpers/transport-harness.js";
import type { ParticipantRoute as Route, ParticipantView as View } from "../src/topology/participant-view.js";

/**
 * FROZEN CONTRACT-v2 §§0,1,3,4,5,7,8,10.3,11 + E1–E3; T1–T22: inherited draft §7.
 * Keep reason: data safety and cross-machine trust boundary. No fake claim/control/admission
 * implementation: the helper's stub models §1 carrier/KV ONLY. Control tests instantiate the
 * actual class returned by loadControlPlane, and skip until A1 integration is explicitly confirmed.
 *
 * E2: real server is Intel 1 / 2.11.17 / sync_interval always; E3: WA/WB Ryzen 2/Ryzen 4.
 * No deployment, process/unit control or credential creation is performed here. Real NATS needs
 * explicit fixture options with distinct per-machine runtime credentials, never admin credentials.
 * The existing state-server helper is deliberately NOT imported (different server/auth surface).
 *
 * Expected skips / coverage metadata, not green E2E evidence:
 * - Missing file/NATS factories and real-server prerequisites are explicit dependency skips.
 * - Stub working/nak/replay are SCRIPTED carrier operations, not a broker timer/delivery engine.
 * - T7/T8/T11/T13/T16/T21/T22 need optional real integration fixtures, below. Missing fixtures
 *   are skipped, never replaced by a synthetic admission/lease/operator/rollback implementation.
 * - T4/T5 use a native-custody test callback invoked by the REAL control plane. This tests its
 *   claim/outcome/ACK machinery, NOT Main/Pi consumption, AgentManager fsync or power-loss safety.
 * - T20 is focused control parity; unchanged control-plane/router suites must ALSO run. It does
 *   not spawn other tests. STATE migration is not control-carrier rollback evidence.
 * - SIGKILL/SIGSTOP, partitions, B3–B5, exact-parent completion, T13b/c/d, T16b, T-HOL, T-LOCK,
 *   T-MIX and nonzero-drain classification remain tests/transport-faults/ and canary obligations.
 * - §10.3 N={1,4,16,64}, 1 cmd/s/sender, 10 min/step, per-target/N p50/p95/p99, KV write rate,
 *   PubAck p95 and separate B3 latency belong to T-P95, not CI/stub latency assertions.
 * - No events port, automatic TTL takeover, file fallback or same-host isolation claim.
 */

type Skip = (reason: string) => never;
type Message = TransportDelivery<TransportEnvelope>;
interface Pair {
  sender: FabricTransport; receiver: FabricTransport;
  hooks: FaultHooks; stub?: { sender: StubTransport; receiver: StubTransport };
  bus?: ReturnType<typeof createStubBus>;
  now(): number; advance(ms: number): void;
  close(): Promise<void>;
}
/** Small OPTIONAL composition seam, not extra behavior required of the terminal helper.
 * Parent may supply real fixtures here after integration. Every callback must drive real product
 * classes/broker operations; none may implement claims, authorization, admission or dedupe.
 * The default registry is empty: these dependencies are deliberately visible as skipped coverage.
 */
interface Integration {
  pair(): Promise<Pair>;
  broker?: {
    restart(): Promise<void>; disconnect(): Promise<void>; reconnect(): Promise<void>;
    setCapacity(bytes: number): Promise<void>; alarmCodes(): Promise<readonly string[]>;
    operator(online: boolean): Promise<void>; reconcile(): Promise<void>;
    records(): Promise<{ advisory: string[]; dlq: string[]; relay: string[]; retained: string[] }>;
    probe(verb: string, subject: string): Promise<"allowed" | "denied">;
    foreignMachine: string; foreignUser: string; ownInbox: string;
  };
  spawn?: { manager: AgentManager; directory: ParticipantDirectory; router: AgentMessageRouter;
    sender: FabricControlPlane; receiver: FabricControlPlane; request: Parameters<AgentManager["spawn"]>[0] };
  lease?: { directory: ParticipantDirectory; pause(): Promise<void>; resume(): Promise<void>; successor(): Promise<boolean>; proveExit(): Promise<void> };
  flip?: (carrier: "file" | "nats") => Promise<{ sender: FabricControlPlane; receiver: FabricControlPlane }>;
}
const integrations: Partial<Record<AdapterKind, Integration>> = {};
// Parent sets this only after checking actual A1 integration, not merely presence of port types.
const transportIntegrated = process.env.PI_FABRIC_TRANSPORT_CONTROL_INTEGRATED === "1";
const required = process.env.FABRIC_TRANSPORT_REQUIRED === "1";
const TIMEOUT = 120_000;
let serial = 0;
const nextId = (test: string) => `${test}-${++serial}`;

async function next<T>(iterator: AsyncIterator<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([iterator.next(), new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("bounded transport receipt deadline")), 5_000);
    })]);
    expect(result.done, "iterator ended before required receipt").not.toBe(true);
    return result.value as T;
  } finally { if (timer !== undefined) clearTimeout(timer); }
}
async function ready(iterator: AsyncIterator<TransportKvEvent>): Promise<TransportKvEvent[]> {
  const result: TransportKvEvent[] = [];
  for (let i = 0; i < 100; i++) {
    const event = await next(iterator); result.push(event);
    if (event.type === "ready") return result;
  }
  throw new Error("watch did not become ready within 100 receipts");
}
function stream(pair: Pair, signal: AbortSignal): AsyncIterator<Message> {
  return pair.receiver.inbox(signal)[Symbol.asyncIterator]();
}
function command(pair: Pair, id = nextId("command"), patch: Partial<Parameters<typeof commandEnvelope>[0]> = {}) {
  return commandEnvelope({ id, to: pair.receiver.self, from: identity(pair.sender.self.hostId),
    replyTo: pair.sender.self.hostId, message: "bounded conformance input", ...patch });
}

async function openPair(kind: AdapterKind, skip: Skip): Promise<Pair> {
  if (integrations[kind]) return integrations[kind]!.pair();
  if (kind === "stub") {
    let time = Date.now();
    const bus = createStubBus(() => time), hooks = new FaultHooks();
    const sender = bus.open(owner("test-a", "session:sender")), receiver = bus.open(owner("test-b", "session:receiver"));
    return { sender: withFaults(sender, hooks), receiver: withFaults(receiver, hooks), hooks,
      stub: { sender, receiver }, bus, now: () => time, advance: ms => { time += ms; bus.expireKv(); },
      close: async () => { await sender.close(); await receiver.close(); } };
  }
  // Ask the actual helper first; no requested entry means an honest adapter-level skip.
  const entry = new URL("../src/transport/index.ts", import.meta.url);
  if (!existsSync(entry)) {
    const result = await openAdapter({ kind, exportName: "openFabricTransport", required });
    if (result.status === "skip") return skip(result.reason);
    await result.transport.close();
    throw new Error("unexpected factory without source entry; supply an explicit integrated pair");
  }
  if (kind === "nats") {
    const result = await openAdapter({ kind, exportName: "openFabricTransport", natsReady: false, required });
    if (result.status === "skip") return skip(result.reason);
    throw new Error("real NATS requires a caller-owned per-machine credential pair");
  }
  const [{ DEFAULT_FABRIC_CONFIG }, { MeshStore }] = await Promise.all([import("../src/config.js"), import("../src/mesh/store.js")]);
  const root = mkdtempSync(join(tmpdir(), "fabric-transport-conformance-")), hooks = new FaultHooks();
  const config = { ...DEFAULT_FABRIC_CONFIG.mesh, transport: "file" as const };
  const mesh = new MeshStore(root, config.maxEventBytes, config.maxReadEvents, { stateBackend: "file", canWakeResidents: () => false });
  const opened: FabricTransport[] = [];
  try {
    for (const hostId of ["session:sender", "session:receiver"]) {
      const spec: AdapterSpec = { kind, exportName: "openFabricTransport", options: { config, mesh, hostId }, required };
      const result = await openAdapter(spec);
      if (result.status === "skip") return skip(result.reason);
      opened.push(result.transport);
    }
    const sender = opened[0]!, receiver = opened[1]!;
    return { sender: withFaults(sender, hooks), receiver: withFaults(receiver, hooks), hooks,
      now: Date.now, advance: () => { throw new Error("file fixture has no virtual clock capability"); },
      close: async () => { await Promise.all(opened.map(port => port.close())); mesh.closeState(); rmSync(root, { recursive: true, force: true }); } };
  } catch (error) {
    await Promise.allSettled(opened.map(port => port.close())); mesh.closeState(); rmSync(root, { recursive: true, force: true }); throw error;
  }
}

/** Actual CP, real owner-local MeshStore claims. The view is fixture DATA conforming to A2's
 * exact resolve/whenChanged/leaseLive contract, not a directory/authorization implementation.
 * Native callback is a probe at the production handler boundary, not a fake control plane.
 */
async function control(pair: Pair, skip: Skip, handler?: FabricControlHandler) {
  const dependency = await loadControlPlane({ transportIntegrated, required });
  if (dependency.status === "unavailable") return skip(dependency.reason);
  const [{ MeshStore }, { writeJsonAtomic }] = await Promise.all([import("../src/mesh/store.js"), import("../src/core/atomic-write.js")]);
  const root = mkdtempSync(join(tmpdir(), "fabric-transport-control-")), senderId = pair.sender.self.hostId, receiverId = pair.receiver.self.hostId;
  const senderMesh = new MeshStore(join(root, "sender"), 1_048_576, 100, { stateBackend: "file" });
  const receiverMesh = new MeshStore(join(root, "receiver"), 1_048_576, 100, { stateBackend: "file" });
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
    ownerIncarnation: "incarnation-1", controlIncarnationFence: "enforce" as const, acknowledgementTimeoutMs: 60_000 });
  const Class = dependency.module.FabricControlPlane;
  const sender = new Class(senderMesh, identity(senderId), options(senderId, pair.sender));
  const receiver = new Class(receiverMesh, identity(receiverId), options(receiverId, pair.receiver));
  expect(Reflect.get(sender, "transportKind")).toBe(pair.sender.kind);
  expect(Reflect.get(receiver, "transportKind")).toBe(pair.receiver.kind);
  const admitted: string[] = [];
  const native: FabricControlHandler = handler ?? (cmd => {
    // Real durable journal write at the test's native boundary; do not implement claims/dedupe here.
    writeJsonAtomic(join(root, "native", `${cmd.commandId}.json`), cmd, { durable: true });
    admitted.push(cmd.commandId); return { accepted: true, messageId: cmd.commandId };
  });
  receiver.start(native);
  // Start the sender only for actual request calls. Raw-envelope negative tests own its inbox.
  let senderStarted = false;
  const startSender = () => { if (!senderStarted) { senderStarted = true; sender.start(() => ({ accepted: false })); } };
  return { sender, receiver, routes, admitted, Class, native,
    request: (key: string, op: "followUp" | "steer" | "stop" = "followUp") => {
      startSender();
      return sender.request(receiverId, receiverId, op,
        { ownerIncarnation: "incarnation-1", message: "bounded input" }, receiverId, { idempotencyKey: key });
    },
    close: async () => { await sender.close(); await receiver.close(); senderMesh.closeState(); receiverMesh.closeState(); rmSync(root, { recursive: true, force: true }); } };
}

describe.each(adapterKinds)("transport conformance / %s", kind => {
  function test(name: string, run: (pair: Pair, signal: AbortSignal, skip: Skip) => Promise<void>) {
    it(name, async context => {
      const skip: Skip = reason => { context.skip(reason); throw new Error("Vitest skip unexpectedly returned"); };
      const pair = await openPair(kind, skip), abort = new AbortController();
      try { await run(pair, abort.signal, skip); }
      finally { abort.abort(); vi.restoreAllMocks(); await pair.close(); }
    }, TIMEOUT);
  }

  test("T1 send → inbox → ack: ordered per inbox, authenticated source, out-of-order final ack", async (p, signal) => {
    const a = await p.sender.send(command(p)), b = await p.sender.send(command(p));
    expect(a.sequence).toBeGreaterThan(0); expect(b.sequence).toBeGreaterThan(a.sequence);
    const iterator = stream(p, signal), one = await next(iterator), two = await next(iterator);
    expect(one).toMatchObject({ payload: a.envelope, sourceMachine: p.sender.self.machine, attempt: 1, sequence: a.sequence });
    expect(one.verification).toBe(p.sender.self.machine === p.receiver.self.machine ? "mesh" : "bridge");
    expect(two.payload).toEqual(b.envelope);
    await two.ack(); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 1 });
    await one.ack(); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
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
    const delivered = await next(stream(p, signal)); expect(delivered.payload).toEqual(first.envelope);
    await delivered.ack(); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
  });

  test("T3 lost PubAck: uncertain carries resolved bytes; same-id byte-identical retry", async (p, signal) => {
    const original = command(p); loseNextPublishAck(p.hooks);
    const error: unknown = await p.sender.send(original).catch((reason: unknown) => reason);
    expect(error).toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN", id: original.id, resolvedBody: expect.any(Object) });
    expect(error).not.toHaveProperty("persisted", false);
    const retry = await p.sender.send(resolvedRetry(original, (error as { resolvedBody: CommandBody }).resolvedBody));
    if (kind !== "file") expect(retry.duplicate).toBe(true); // E1 allows a fresh file event
    const delivered = await next(stream(p, signal)); expect(delivered.payload.body).toEqual((error as { resolvedBody: unknown }).resolvedBody);
    await delivered.ack();
  });

  test("T4 lost consumer ack after actual ACK persistence: real control-plane claim prevents a second handler call", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      // Drop BEFORE forwarding final broker ack, not after a confirmed ack (which would not redeliver).
      const remove = p.hooks.set("ack:before", () => { remove(); throw new Error("injected consumer ack loss"); });
      const key = nextId("T4"); expect(await cp.request(key)).toMatchObject({ acknowledged: true });
      if (p.stub) {
        const record = p.stub.receiver.records.find(row => row.delivery.payload.kind === "command")!;
        p.stub.receiver.enqueue(record.delivery.payload, { sourceMachine: p.sender.self.machine, verification: "bridge", attempt: 2, sequence: record.delivery.sequence });
      } else if (!integrations[kind]?.broker) skip("real redelivery/ackWait fixture missing; retained claim alone is not crash proof");
      // Same logical sender key joins/reads the real persisted outcome; no new handler admission.
      expect(await cp.request(key)).toMatchObject({ acknowledged: true });
      expect(cp.admitted).toHaveLength(1);
      expect(cp.receiver).toBeInstanceOf(cp.Class);
      void signal;
    } finally { await cp.close(); }
  });

  test("T5 crash after real claim before outcome: real recovery refuses indeterminate, never re-executes", async (p, _signal, skip) => {
    const cp = await control(p, skip);
    try {
      // The carrier cannot observe a claim. Wire the hook to the REAL owner-local store mutation.
      const { MeshStore } = await import("../src/mesh/store.js");
      const original = MeshStore.prototype.put;
      let claimed = false;
      const claimGate = faultGate(), sawClaim = faultGate();
      const spy = vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: import("../src/mesh/store.js").MeshStore, input) {
        const result = await original.call(this, input);
        if (!claimed && this.root.includes(`${join("control-seen", "")}`) && input.ifVersion === 0) {
          claimed = true; sawClaim.release(); await claimGate.wait(AbortSignal.timeout(5_000));
          throw new Error("injected halt after actual claim write");
        }
        return result;
      });
      const pending = cp.request(nextId("T5")).catch((error: unknown) => error);
      try {
        await sawClaim.wait(AbortSignal.timeout(5_000)); cp.receiver.pause(); claimGate.release(); spy.mockRestore();
        // Production restart replay must see the real claim without an outcome, not a seeded fake.
        cp.receiver.resume();
        const result = await pending;
        expect(result).toBeInstanceOf(Error); expect(String(result)).toMatch(/unknown|indeterminate|interrupted/i);
        expect(cp.admitted).toEqual([]);
      } finally { claimGate.release(); spy.mockRestore(); }
      // Actual SIGKILL/new-process restart is T-B2a; this seam is not process-crash proof.
    } finally { await cp.close(); }
  });

  test("T6 working/nak: all in-hand deliveries heartbeat; scripted stub replay is not broker timer proof", async (p, signal, skip) => {
    const sent = await p.sender.send(command(p)), iterator = stream(p, signal), first = await next(iterator);
    for (let i = 0; i < 4; i++) first.working();
    if (!p.stub) skip("requires real ackWait/WPI timing fixture; file working is a no-op");
    expect(p.stub!.receiver.records[0]!.working).toBe(4);
    first.nak(25); expect(p.stub!.receiver.records[0]!.naks).toEqual([25]);
    // Script ONLY the delivery supplied by a broker; no automatic redelivery engine in the stub.
    p.stub!.receiver.enqueue(sent.envelope, { sourceMachine: p.sender.self.machine, verification: "bridge", attempt: 2, sequence: sent.sequence });
    const replay = await next(iterator); expect(replay.attempt).toBe(2); expect(replay.sequence).toBe(first.sequence);
    await replay.ack(); await first.ack();
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
  });

  test("T7 max_deliver/operator offline: retained advisory, one relay/DLQ, original retained", async (p, signal, skip) => {
    const broker = integrations[kind]?.broker;
    if (!broker || kind !== "nats") skip("requires real NATS operator/advisory dependency, not a stub DLQ model");
    const b = broker!; await b.operator(false);
    const original = command(p); await p.sender.send(original); const iterator = stream(p, signal);
    for (let attempt = 1; attempt <= 5; attempt++) { const d = await next(iterator); expect(d.attempt).toBe(attempt); d.nak(0); }
    expect((await b.records()).advisory).toContain(original.id);
    expect((await b.records()).dlq).not.toContain(original.id);
    await b.operator(true); await b.reconcile(); await b.reconcile();
    const records = await b.records();
    expect(records.dlq.filter(id => id === original.id)).toHaveLength(1);
    expect(records.relay.filter(id => id === `relay:${original.id}`)).toHaveLength(1);
    expect(records.retained).toContain(original.id);
  });

  test("T8 broker restart/network cut: live iterator resumes from ack floor; outage send never silently succeeds", async (p, signal, skip) => {
    const broker = integrations[kind]?.broker;
    if (!broker) skip("real broker restart/network-cut dependency absent; stub does not simulate a server");
    const iterator = stream(p, signal); await p.sender.send(command(p)); await (await next(iterator)).ack();
    const outstanding = await p.sender.send(command(p)); const held = await next(iterator);
    await broker!.disconnect();
    await expect(p.sender.send(command(p))).rejects.toMatchObject({ code: expect.stringMatching(/^FABRIC_TRANSPORT_(UNAVAILABLE|UNCERTAIN)$/) });
    const pending = iterator.next(); void pending.catch(() => undefined);
    await broker!.restart(); await broker!.reconnect();
    const replay = await next({ next: () => pending });
    expect(replay.sequence).toBe(outstanding.sequence); expect(replay.payload).toEqual(held.payload); await replay.ack();
  });

  test("T9 owner restart: real stable hostId/control-seen prevents repeated admission", async (p, _signal, skip) => {
    const cp = await control(p, skip);
    try {
      const key = nextId("T9"); expect(await cp.request(key)).toMatchObject({ acknowledged: true });
      await cp.receiver.close();
      const replacement = p.bus ? p.bus.open(p.receiver.self) : p.receiver;
      const restartOptions = { ...cp.receiver.options, transport: replacement };
      const restarted = new cp.Class(cp.receiver.mesh, cp.receiver.identity, restartOptions);
      expect(restarted.options.hostId).toBe(cp.receiver.options.hostId);
      const calls: string[] = []; restarted.start(cmd => { calls.push(cmd.commandId); return { accepted: true }; });
      try {
        // Carrier replay is scripted; recovery/admission is exclusively the REAL restarted class.
        if (p.stub) {
          const record = p.stub.receiver.records.find(row => row.delivery.payload.kind === "command")!;
          (replacement as StubTransport).enqueue(record.delivery.payload, { sourceMachine: p.sender.self.machine,
            verification: "bridge", attempt: 2, sequence: record.delivery.sequence });
        }
        expect(await cp.request(key)).toMatchObject({ acknowledged: true }); expect(calls).toEqual([]);
      } finally { await restarted.close(); if (p.bus) await replacement.close(); }
    } finally { await cp.close(); }
  });

  test("T10 abort and stamp throw before persistence: exact refusal; after-publish fault is uncertain", async (p) => {
    const original = command(p), abort = new AbortController(); abort.abort(); let stamps = 0;
    await expect(p.sender.send({ ...original, body: at => { stamps++; return { version: 1, commandId: original.id,
      targetId: original.targetId, operation: original.operation, ownerIncarnation: "incarnation-1", replyTo: "sender", requestedAt: at, deadlineAt: at + 1000 }; } }, abort.signal))
      .rejects.toMatchObject({ name: "AbortError" });
    expect(stamps).toBe(0); expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
    const reason = new Error("stamp refused"); await expect(p.sender.send({ ...original, body: () => { throw reason; } })).rejects.toBe(reason);
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
    // withFaults can model a lost reply AFTER send, not abort races inside the real client.
    loseNextPublishAck(p.hooks); await expect(p.sender.send(original)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN", id: original.id });
  });

  test("T11 DiscardNew capacity: typed not-persisted refusal and alarm, no eviction", async (p, signal, skip) => {
    const broker = integrations[kind]?.broker;
    if (!broker) skip("real isolated stream-capacity dependency absent; §1 stub has no fake capacity/alarm engine");
    await broker!.setCapacity(2048);
    const kept = await p.sender.send(command(p));
    await expect(p.sender.send(command(p, nextId("T11"), { message: "x".repeat(4096) })))
      .rejects.toMatchObject({ code: "FABRIC_TRANSPORT_CAPACITY", persisted: false });
    expect(await broker!.alarmCodes()).toContain("FABRIC_TRANSPORT_CAPACITY");
    const delivered = await next(stream(p, signal)); expect(delivered.payload).toEqual(kept.envelope); await delivered.ack();
    expect(await p.receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
  });

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
    if (!p.bus) skip("real watch-gap/reset fixture absent; readiness above is not reconnect proof");
    p.bus!.resetKvWatches(); expect(await next(iterator)).toEqual({ type: "reset" });
    expect(await ready(iterator)).toEqual([
      { type: "put", entry: { key, machine: p.receiver.self.machine, owner: "owner#one", value: { id: "one" }, revision } }, { type: "ready" },
    ]);
    // An open/put race uses the actual KV iterator: pending next and put, no readiness sleep.
    const pending = iterator.next(); const newer = await kv.put(key, "owner#one", { id: "two" }, revision);
    expect(await next({ next: () => pending })).toMatchObject({ type: "put", entry: { revision: newer } });
  });

  test("T15 delete/MaxAge expiry markers: gone reasons, machine and monotonic revisions", async (p, signal, skip) => {
    if (kind === "file") skip("file adapter has no KV capability");
    const kv = p.receiver.kv("leases"), key = `${p.receiver.self.machine}.h.one`;
    const iterator = kv.watch(`${p.receiver.self.machine}.>`, signal)[Symbol.asyncIterator]();
    await ready(iterator); const first = await kv.put(key, "owner#one", {}, 0); await next(iterator);
    expect(await kv.delete(key, first)).toBe(true);
    const deleted = await next(iterator); expect(deleted).toMatchObject({ type: "gone", key, machine: p.receiver.self.machine, reason: "deleted" });
    const second = await kv.put(key, "owner#two", {}, 0); expect(second).toBeGreaterThan(first); await next(iterator);
    if (!p.bus) skip("real bucket TTL observation fixture absent");
    p.advance(kv.ttlMs + 1); const expired = await next(iterator);
    expect(expired).toMatchObject({ type: "gone", key, machine: p.receiver.self.machine, reason: "expired" });
    if (expired.type === "gone") expect(expired.revision).toBeGreaterThan(second);
    expect(await kv.get(key)).toBeUndefined();
  });

  test("T16 real permission negatives: source spoof/foreign consumers/ACK/raw reads/KV/inbox/stream lifecycle", async (p, _signal, skip) => {
    const b = integrations[kind]?.broker;
    if (!b || kind !== "nats") skip("real per-machine runtime credentials required; no stub allow-list proof");
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

  test("T17 forged sender: actual production presence/source verification refuses notRun", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      const forged = command(p, nextId("T17"), { from: identity("session:unknown"), operation: "stop" });
      await p.sender.send(forged);
      const ack = await next(p.sender.inbox(signal)[Symbol.asyncIterator]());
      expect(ack.payload.body).toMatchObject({ commandId: forged.id, accepted: false, notRun: true, errorCode: "FABRIC_SENDER_UNVERIFIED" });
      expect(cp.admitted).toEqual([]); await ack.ack();
    } finally { await cp.close(); }
  });

  test("T18 stale incarnation: actual live production holder refuses notRun without handler admission", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      const stale = command(p, nextId("T18"), { ownerIncarnation: "superseded", operation: "stop" }); await p.sender.send(stale);
      const ack = await next(p.sender.inbox(signal)[Symbol.asyncIterator]());
      expect(ack.payload.body).toMatchObject({ accepted: false, notRun: true, errorCode: "FABRIC_CONTROL_STALE_INCARNATION" });
      expect(cp.admitted).toEqual([]); await ack.ack();
    } finally { await cp.close(); }
  });

  test("T19 expiry: actual production durable refusal; carrier stamp preserves full delivery budget", async (p, signal, skip) => {
    const cp = await control(p, skip);
    try {
      const id = nextId("T19");
      const expired = command(p, id, { operation: "stop", body: at => ({ version: 1, commandId: id, targetId: p.receiver.self.hostId,
        operation: "stop", replyTo: p.sender.self.hostId, ownerIncarnation: "incarnation-1", requestedAt: at - 1000, deadlineAt: at - 1 }) });
      await p.sender.send(expired); const ack = await next(p.sender.inbox(signal)[Symbol.asyncIterator]());
      expect(ack.payload.body).toMatchObject({ accepted: false, notRun: true, error: "Fabric control command expired" });
      expect(cp.admitted).toEqual([]); await ack.ack();
      const sent = await p.sender.send(command(p));
      expect(sent.envelope.body.requestedAt).toBe(sent.envelope.createdAt);
      expect(sent.envelope.body.deadlineAt - sent.envelope.body.requestedAt).toBe(60_000);
      // P4 commit-lock delay regression still needs real lock contention in T-FENCE/T19 integration.
    } finally { await cp.close(); }
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
      // Nonzero drain queued/admitted/unknown/terminal classification, operator dispositions,
      // retained completion replay and old-writer stop proof remain T-ROLLBACK/canary, not this 0/0 case.
    } finally { await cp.close(); }
  });

  test("T-FILE-POSTCOMMIT (E1) append then lost wake/receipt: resolved/uncertain, one real handler/ACK", async (p, _signal, skip) => {
    if (kind !== "file") skip("requires actual file adapter and integrated production control plane");
    const cp = await control(p, skip);
    try {
      // Carrier wrapper's after-publish loss is a lower-bound E1 check. Actual wake-timeout injection
      // INSIDE MeshStore.publish is an additional T-FILE-POSTCOMMIT fault-suite obligation.
      loseNextPublishAck(p.hooks); const key = nextId("E1");
      const first: unknown = await cp.request(key).catch((error: unknown) => error);
      if (first instanceof Error) expect(first).not.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE", persisted: false });
      expect(await cp.request(key)).toMatchObject({ acknowledged: true }); expect(cp.admitted).toHaveLength(1);
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
