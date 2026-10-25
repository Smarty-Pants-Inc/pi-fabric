// L1 (smarty-dev#15310): the raw NATS port of the A' bridge (L3's BridgeNatsPort). The link identity,
// presence read for the identity gate, the mirror set (roots, allow-listed actors, link identity),
// send's pre-publish refusals, and the machine-wide inbox's strict decode and settle-only-on-request
// behavior, against the in-memory KV and a scripted inbox source.
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  decodeNatsBridgeMessage,
  NatsBridgePort,
  natsBridgeAdmit,
  type NatsBridgeDelivery,
  type NatsBridgeInboxMessage,
  type NatsBridgeInboxSource,
} from "../src/mesh/bridge-nats-port.js";
import { bridgeNatsLinkIdentity, type BridgeNatsMirrorSet, type BridgeNatsPort } from "../src/mesh/bridge-nats-types.js";
import type { NatsFileActorRead } from "../src/mesh/bridge-nats-actors.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { BridgeHost } from "../src/mesh/bridge.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { TransportDeniedError, TransportUncertainError, type TransportEnvelope, type TransportSent } from "../src/transport/types.js";
import { advance, hKey, kvWorld, natsLease, pKey, seedNative, type KvWorld } from "./helpers/bridge-nats-kv.js";

const BRIDGE = "ryzen1-bridge";
const V2 = "v2-org";
const LINK = "v2";
const LINK_IDENTITY = bridgeNatsLinkIdentity(LINK);
const LINK_ID = LINK_IDENTITY.id;
const FILE_ROOT = "session:11111111-1111-4111-8111-111111111111";
const NATS_ROOT = "session:22222222-2222-4222-8222-222222222222";
const SUPERVISOR = "0123456789abcdef0123456789abcdef";
const PROOF = { pid: 4242, pidStartTime: "777", bootId: "12345678-1234-4234-8234-123456789abc" };

interface FakeMessage extends NatsBridgeInboxMessage { calls: string[] }

/** A scripted inbox: push messages; the port pulls them in order. */
class ScriptedInbox implements NatsBridgeInboxSource {
  readonly #queue: FakeMessage[] = [];
  #wake: (() => void) | undefined;
  verified = true;
  push(message: FakeMessage): void { this.#queue.push(message); this.#wake?.(); }
  isVerified(): boolean { return this.verified; }
  async *consume(signal: AbortSignal): AsyncIterable<NatsBridgeInboxMessage> {
    while (!signal.aborted) {
      const next = this.#queue.shift() ?? await new Promise<FakeMessage | undefined>(resolve => {
        const abort = (): void => resolve(undefined);
        this.#wake = () => { this.#wake = undefined; signal.removeEventListener("abort", abort); resolve(this.#queue.shift()); };
        signal.addEventListener("abort", abort, { once: true });
      });
      if (!next) return;
      yield next;
    }
  }
}

function message(options: { id: string; kind?: "command" | "ack"; toHostId?: string; targetId?: string; source?: string;
  subjectSource?: string; toMachine?: string; subjectMachine?: string; body?: Record<string, unknown>; sequence?: number;
  attempt?: number; principal?: unknown; raw?: string }): FakeMessage {
  const kind = options.kind ?? "command";
  const toHostId = options.toHostId ?? FILE_ROOT;
  const targetId = options.targetId ?? FILE_ROOT;
  const source = options.source ?? V2;
  const body = options.body ?? { commandId: "c".repeat(64), targetId, operation: "followUp", message: "hi", replyTo: NATS_ROOT,
    requestedAt: 1, deadlineAt: 2, ownerIncarnation: "file-inc-1" };
  const envelope = { id: options.id, kind, to: { machine: options.toMachine ?? BRIDGE, hostId: toHostId }, targetId,
    operation: kind === "command" ? "followUp" : "accepted", from: { id: NATS_ROOT, name: "v2-main", kind: "main" },
    ...(options.principal !== undefined ? { principal: options.principal } : {}), body, createdAt: 123 };
  const calls: string[] = [];
  return {
    calls,
    subject: `fabric.in.${options.subjectMachine ?? BRIDGE}.${hashToken(toHostId)}.${kind === "command" ? "cmd" : "ack"}.${hashToken(targetId)}.${options.subjectSource ?? source}`,
    data: Buffer.from(options.raw ?? JSON.stringify({ format: 1, sourceMachine: source, envelope })),
    info: { streamSequence: options.sequence ?? 1, deliveryCount: options.attempt ?? 1 },
    async ackAck() { calls.push("ack"); return true; },
    nak(delay?: number) { calls.push(`nak:${delay ?? ""}`); },
    term(reason?: string) { calls.push(`term:${reason ?? ""}`); },
    working() { calls.push("working"); },
  };
}

const closers: Array<{ close(): Promise<void> }> = [];
afterEach(async () => { await Promise.allSettled(closers.splice(0).map(item => item.close())); });

function port(w: KvWorld, options: { send?: (envelope: TransportEnvelope) => Promise<TransportSent>; fileActors?: () => NatsFileActorRead;
  allow?: Parameters<typeof natsBridgeAdmit>[0]; onClose?: () => Promise<void>; proof?: typeof PROOF; predecessorExited?: () => boolean;
  takeoverMs?: number } = {}) {
  const clock = { wall: 1_000_000, mono: 50_000 };
  const sent: TransportEnvelope[] = [];
  const inbox = new ScriptedInbox();
  const transport = w.transport(BRIDGE, LINK_ID, async (envelope) => {
    sent.push(envelope as TransportEnvelope);
    if (options.send) return await options.send(envelope as TransportEnvelope) as never;
    return { id: envelope.id, sequence: 41, duplicate: false, envelope: { ...envelope, createdAt: clock.wall } } as never;
  });
  const allow = options.allow ?? { file: [{ kind: "root" as const, id: FILE_ROOT }], nats: [{ kind: "root" as const, id: NATS_ROOT }] };
  const instance = new NatsBridgePort({ transport, inbox, link: LINK, admit: natsBridgeAdmit(allow), processProof: options.proof ?? PROOF,
    predecessorExited: options.predecessorExited ?? (() => false), now: () => clock.wall, monotonic: () => clock.mono,
    ...(options.fileActors ? { fileActors: options.fileActors } : {}), ...(options.onClose ? { onClose: options.onClose } : {}),
    ...(options.takeoverMs !== undefined ? { takeoverMs: options.takeoverMs } : {}) });
  closers.push(instance);
  return { port: instance, inbox, sent, clock };
}

function natsRoot(id = NATS_ROOT): FabricParticipantRecord {
  return { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "nats-inc-1",
    name: "v2-main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    controlProtocol: "v1", startedAt: 999_000, updatedAt: 1_000_000, sessionId: id.slice(8) };
}
function fileRoot(id = FILE_ROOT): FabricParticipantRecord {
  return { ...natsRoot(id), ownerIncarnation: "file-inc-1", name: "file-lead" };
}
function fileActor(overrides: Partial<FabricParticipantRecord> = {}): FabricParticipantRecord {
  return { format: 1, id: SUPERVISOR, kind: "actor", rootId: FILE_ROOT, ownerHostId: "resident:0123456789abcdef01234567",
    ownerIdentityId: FILE_ROOT, ownerIncarnation: "resident-inc-1", parentId: FILE_ROOT, name: "fleet-supervisor", status: "idle",
    runner: "pi", transport: "host", capabilities: ["steer", "followUp", "ask", "fabric"], residency: "durable",
    controlProtocol: "v1", startedAt: 900_000, updatedAt: 1_000_000, ...overrides };
}
const fileHost = (id: string, at: number, expiresAt = at + 60_000): BridgeHost => ({ record: { format: 1, id, rootId: id,
  identity: { id, name: "file-lead", kind: "main" }, startedAt: 900_000, updatedAt: at, expiresAt }, expiresAt, observedAt: at });
const mirrorSet = (at: number, participants: FabricParticipantRecord[] = [fileRoot()], hosts = [fileHost(FILE_ROOT, at)]): BridgeNatsMirrorSet =>
  ({ hosts, participants, linkIdentity: LINK_IDENTITY });

const command = (overrides: Partial<TransportEnvelope> = {}): TransportEnvelope => ({
  id: "bnf1-abc", kind: "command", to: { machine: V2, hostId: NATS_ROOT }, targetId: NATS_ROOT, operation: "followUp",
  from: { id: FILE_ROOT, name: "file-lead", kind: "main" }, body: { commandId: "x", targetId: NATS_ROOT, operation: "followUp" }, ...overrides,
});

async function next(iterator: AsyncIterator<NatsBridgeDelivery>): Promise<NatsBridgeDelivery> {
  const step = await iterator.next();
  if (step.done) throw new Error("inbox ended");
  return step.value;
}

async function receiver(w: KvWorld): Promise<KvParticipantDirectory> {
  const view = new KvParticipantDirectory({ transport: w.transport(V2, "session:receiver"),
    lease: { live: () => true, signal: new AbortController().signal }, identityId: "session:receiver",
    ownerIncarnation: "r", rootId: "session:receiver" });
  closers.push(view);
  view.start();
  await vi.waitFor(() => expect(view.ready).toBe(true));
  return view;
}

describe("NATS bridge port: contract shape", () => {
  it("is L3's BridgeNatsPort", () => {
    const w = kvWorld();
    const typed: BridgeNatsPort = port(w).port;
    expect(typed.machine).toBe(BRIDGE);
  });
});

describe("NATS bridge port: link identity and presence", () => {
  it("holds the link identity live in KV under the bridge machine, as a receiver sees it", async () => {
    const w = kvWorld();
    const { port: p } = port(w);
    await p.start();
    expect(p.selfRecord).toMatchObject({ id: LINK_ID, kind: "root", ownerHostId: LINK_ID, ownerIdentityId: LINK_ID,
      name: "mesh-bridge", capabilities: [], controlProtocol: "v1" });
    expect((await receiver(w)).resolve(LINK_ID)).toMatchObject({ machine: BRIDGE, live: true, capabilities: [] });
    await p.withdraw();
    expect(w.presence.values.size + w.leases.values.size).toBe(0);
  });

  it("releases its link identity on a clean stop, so a restart within the lease TTL starts at once", async () => {
    const w = kvWorld();
    const first = port(w).port;
    await first.start();
    await first.close();
    expect(w.presence.values.has(pKey(BRIDGE, LINK_ID))).toBe(false);
    expect(w.leases.values.has(hKey(BRIDGE, LINK_ID))).toBe(false);
    const restarted = port(w, { proof: { pid: 5151, pidStartTime: "888", bootId: PROOF.bootId }, takeoverMs: 0 }).port;
    await restarted.start();
    expect((await receiver(w)).resolve(LINK_ID)).toMatchObject({ machine: BRIDGE, live: true });
  });

  it("after a crash, takes over its own link identity at once from a provably exited predecessor", async () => {
    const w = kvWorld();
    const first = port(w).port;
    await first.start();
    const incarnation = first.selfRecord!.ownerIncarnation;
    await first.fence(); // A crash: the rows stay and nothing renews them.
    expect(w.presence.values.has(pKey(BRIDGE, LINK_ID))).toBe(true);
    const next = { pid: 5151, pidStartTime: "888", bootId: PROOF.bootId };
    const alive = port(w, { proof: next, predecessorExited: () => false, takeoverMs: 0 }).port;
    await expect(alive.start()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_LEASE_CONFLICT" });
    const successor = port(w, { proof: next, predecessorExited: () => true }).port;
    await successor.start();
    expect(successor.selfRecord?.ownerIncarnation).toBe(incarnation);
    expect(w.leases.values.get(hKey(BRIDGE, LINK_ID))?.value).toMatchObject({ pid: 5151 });
    expect((await receiver(w)).resolve(LINK_ID)).toMatchObject({ machine: BRIDGE, live: true });
  });

  it("refuses to start when the link identity already has a row on another machine", async () => {
    const w = kvWorld();
    const { sessionId: _sessionId, ...squatter } = natsRoot(LINK_ID);
    seedNative(w, V2, squatter, 1_000_000);
    const { port: p } = port(w);
    await expect(p.start()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_ID_ON_ANOTHER_MACHINE" });
  });

  it("answers 'unknown' (DirectoryNotReadyError), never 'absent', before it has a KV snapshot", async () => {
    const w = kvWorld();
    const { port: p } = port(w);
    expect(() => p.resolve(NATS_ROOT)).toThrow(expect.objectContaining({ code: "FABRIC_DIRECTORY_NOT_READY" }));
    expect(() => p.presenceView().records(NATS_ROOT)).toThrow(expect.objectContaining({ code: "FABRIC_DIRECTORY_NOT_READY" }));
    expect(() => p.presenceView().list?.()).toThrow(expect.objectContaining({ code: "FABRIC_DIRECTORY_NOT_READY" }));
    await p.start();
    expect(p.resolve(NATS_ROOT)).toBeUndefined();
  });

  it("resolves the one KV record for an id with the identity from that record", async () => {
    const w = kvWorld();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { port: p } = port(w);
    await p.start();
    expect(p.resolve(NATS_ROOT)).toEqual({
      route: { machine: V2, ownerHostId: NATS_ROOT, ownerIdentityId: NATS_ROOT, ownerIncarnation: "nats-inc-1",
        capabilities: ["steer", "followUp", "fabric"], live: true },
      identity: { id: NATS_ROOT, name: "v2-main", kind: "main", sessionId: NATS_ROOT.slice(8) },
    });
    expect(p.resolve("session:absent-0000")).toBeUndefined();
  });

  it("lists every record for an id, on every machine, for the identity gate", async () => {
    const w = kvWorld();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { port: p, clock } = port(w);
    await p.start();
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    const view = p.presenceView();
    expect(view.side).toBe("nats");
    expect(view.records(NATS_ROOT)).toEqual([{ id: NATS_ROOT, name: "v2-main", kind: "main", rootId: NATS_ROOT,
      sessionId: NATS_ROOT.slice(8), ownerHostId: NATS_ROOT, ownerIdentityId: NATS_ROOT, ownerIncarnation: "nats-inc-1",
      machine: V2, live: true, observedAt: clock.wall }]);
    // The link's own mirror is a record under the bridge machine; mirroredBy stays unset on NATS.
    expect(view.records(FILE_ROOT)).toEqual([expect.objectContaining({ machine: BRIDGE, live: true })]);
    expect(view.records(FILE_ROOT)[0]).not.toHaveProperty("mirroredBy");
    seedNative(w, "v3-org", { ...natsRoot(), ownerIncarnation: "other" }, 1_000_000);
    await vi.waitFor(() => expect(view.records(NATS_ROOT)).toHaveLength(2));
    expect(p.resolve(NATS_ROOT)).toBeUndefined();
    // The full listing (dry run) is every record on every machine, the link identity included.
    expect(view.list?.().map(record => `${record.id}@${record.machine}`)).toEqual([
      `${LINK_ID}@${BRIDGE}`, `${FILE_ROOT}@${BRIDGE}`, `${NATS_ROOT}@v2-org`, `${NATS_ROOT}@v3-org`]);
  });

  it("returns only admitted roots from other machines, and wakes only on another machine's change", async () => {
    const w = kvWorld();
    const other = "session:33333333-3333-4333-8333-333333333333";
    seedNative(w, V2, natsRoot(), 1_000_000);
    seedNative(w, V2, natsRoot(other), 1_000_000);
    const { port: p, clock } = port(w);
    await p.start();
    const changes: number[] = [];
    p.watchPresence(() => { changes.push(1); }, () => { throw new Error("unexpected failure"); });
    expect((await p.presence()).hosts.map(host => host.record.id)).toEqual([NATS_ROOT]);
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    expect(changes).toEqual([]);
    seedNative(w, "v3-org", natsRoot("session:44444444-4444-4444-8444-444444444444"), clock.wall);
    await vi.waitFor(() => expect(changes.length).toBe(2));
  });
});

describe("NATS bridge port: with L3's identity gate", () => {
  it("verifies a NATS sender on its source machine and refuses this link's own mirrors, from the port's presenceView", async () => {
    const w = kvWorld();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { port: p, clock } = port(w);
    await p.start();
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    // L3 (audit): every allow.nats entry pins its machine.
    const config = parseBridgeNatsAllowSection({ file: [{ kind: "root", id: FILE_ROOT }], nats: [{ kind: "root", id: NATS_ROOT, machine: V2 }] },
      { link: LINK, natsMachine: BRIDGE });
    const gate = createBridgeNatsIdentityGate(config, { pins: { seats: new Map(), bound: new Map() }, clock: () => clock.wall });
    const fileView = { side: "file" as const, records: () => [] };
    const nats = p.presenceView();
    expect(gate.verifySender("nats", NATS_ROOT, nats, fileView, V2)).toMatchObject({ ok: true,
      identity: { side: "nats", id: NATS_ROOT, machine: V2, ownerIncarnation: "nats-inc-1" } });
    expect(gate.verifySender("nats", NATS_ROOT, nats, fileView, "v3-org")).toMatchObject({ ok: false });
    expect(gate.verifyTarget("nats", NATS_ROOT, nats, fileView)).toMatchObject({ ok: true });
    expect(gate.verifySender("nats", FILE_ROOT, nats, fileView, V2)).toMatchObject({ ok: false, refuse: "BRIDGE_NATS_SIDE_MISMATCH" });
  });

  it("treats a v2 root whose lease awaits a root re-check as not live in resolve, presence and presenceView, until it is rewritten", async () => {
    const w = kvWorld();
    seedNative(w, V2, natsRoot(), 1_000_000);
    const { port: p, clock } = port(w);
    await p.start();
    const config = parseBridgeNatsAllowSection({ file: [{ kind: "root", id: FILE_ROOT }], nats: [{ kind: "root", id: NATS_ROOT, machine: V2 }] },
      { link: LINK, natsMachine: BRIDGE });
    const gate = createBridgeNatsIdentityGate(config, { pins: { seats: new Map(), bound: new Map() }, clock: () => clock.wall });
    const fileView = { side: "file" as const, records: () => [] };
    expect(gate.verifyTarget("nats", NATS_ROOT, p.presenceView(), fileView)).toMatchObject({ ok: true });
    // The v2 owner's lease lapsed in KV and was created again with rootRecheck (host-leases.ts).
    const key = hKey(V2, NATS_ROOT);
    const owner = `${NATS_ROOT}#nats-inc-1`;
    w.leases.expire(key);
    w.leases.put(key, owner, { ...natsLease(NATS_ROOT, "nats-inc-1", clock.wall), rootRecheck: true }, 0);
    await vi.waitFor(() => expect(p.kv.reader.lease(V2, NATS_ROOT)?.entry.value.rootRecheck).toBe(true));
    // Kept, not absent (resolve is a fact, never undefined here), but not routable: as native routing.
    expect(p.resolve(NATS_ROOT)?.route).toMatchObject({ machine: V2, ownerIncarnation: "nats-inc-1", live: false });
    expect((await receiver(w)).resolve(NATS_ROOT)).toMatchObject({ machine: V2, live: false });
    const view = p.presenceView();
    expect(view.records(NATS_ROOT)).toEqual([expect.objectContaining({ id: NATS_ROOT, machine: V2, live: false })]);
    expect(view.list?.().find(record => record.id === NATS_ROOT)).toMatchObject({ live: false });
    // The gate's live input is false: neither a sender nor a target.
    expect(gate.verifySender("nats", NATS_ROOT, view, fileView, V2)).toMatchObject({ ok: false });
    expect(gate.verifyTarget("nats", NATS_ROOT, view, fileView)).toMatchObject({ ok: false });
    // Not mirrored as live, and the snapshot is partial: unknown is never withdrawal evidence.
    const during = await p.presence();
    expect(during.hosts).toEqual([]);
    expect(during.partial).toBe(true);
    // Rewritten without rootRecheck: routable again, and the snapshot is complete.
    w.leases.put(key, owner, natsLease(NATS_ROOT, "nats-inc-1", clock.wall), w.leases.values.get(key)!.revision);
    await vi.waitFor(() => expect(p.resolve(NATS_ROOT)?.route.live).toBe(true));
    expect(gate.verifyTarget("nats", NATS_ROOT, p.presenceView(), fileView)).toMatchObject({ ok: true });
    expect(gate.verifySender("nats", NATS_ROOT, p.presenceView(), fileView, V2)).toMatchObject({ ok: true });
    const after = await p.presence();
    expect(after.partial).toBeUndefined();
    expect(after.hosts.map(host => host.record.id)).toEqual([NATS_ROOT]);
  });
});

describe("NATS bridge port: mirror set", () => {
  it("holds allow-listed file roots and actors, bounded by their samples, and keeps the link identity", async () => {
    const w = kvWorld();
    const resident = fileActor().ownerHostId;
    let listing: NatsFileActorRead = { actors: [fileActor()], sampledAt: 1_000_000 };
    const { port: p, clock } = port(w, {
      fileActors: () => listing,
      allow: { file: [{ kind: "root", id: FILE_ROOT }, { kind: "actor", rootId: FILE_ROOT, name: "fleet-supervisor" }] },
    });
    await p.start();
    const stranger = fileActor({ id: "fedcba9876543210fedcba9876543210", name: "other-actor" });
    await p.mirror(mirrorSet(clock.wall, [fileRoot(), stranger]), clock.wall);
    expect(w.presence.values.has(pKey(BRIDGE, FILE_ROOT))).toBe(true);
    expect(w.presence.values.has(pKey(BRIDGE, SUPERVISOR))).toBe(true);
    expect(w.presence.values.has(pKey(BRIDGE, stranger.id))).toBe(false);
    expect(w.leases.values.get(hKey(BRIDGE, resident))?.value).toMatchObject({ id: resident, rootId: FILE_ROOT,
      identityId: FILE_ROOT, incarnation: "resident-inc-1" });
    const view = await receiver(w);
    expect(view.resolve(SUPERVISOR)).toMatchObject({ machine: BRIDGE, ownerHostId: resident, live: true });
    expect(view.resolve(LINK_ID)?.live).toBe(true);
    // The actor's authority is the listing's: 15 s from the read, renewed only by a newer read.
    advance(clock, 15_000);
    await p.kv.writer.renew();
    expect(w.leases.values.has(hKey(BRIDGE, resident))).toBe(false);
    listing = { actors: [fileActor()], sampledAt: clock.wall };
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    expect(w.leases.values.has(hKey(BRIDGE, resident))).toBe(true);
    // An actor gone from a readable listing is withdrawn; an unreadable listing withdraws nothing.
    listing = { actors: [], sampledAt: clock.wall };
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    expect(w.presence.values.has(pKey(BRIDGE, SUPERVISOR))).toBe(false);
    listing = { actors: [fileActor()], sampledAt: clock.wall };
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    const broken = (): NatsFileActorRead => { throw new Error("mesh busy"); };
    (p.options as { fileActors?: () => NatsFileActorRead }).fileActors = broken;
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    expect(w.presence.values.has(pKey(BRIDGE, SUPERVISOR))).toBe(true);
  });

  it("holds an actor that is both in the set and in the native listing once", async () => {
    const w = kvWorld();
    const { port: p, clock } = port(w, {
      fileActors: () => ({ actors: [fileActor()], sampledAt: clock.wall }),
      allow: { file: [{ kind: "root", id: FILE_ROOT }, { kind: "actor", rootId: FILE_ROOT, name: "fleet-supervisor" }] },
    });
    await p.start();
    await p.mirror(mirrorSet(clock.wall, [fileRoot(), fileActor()]), clock.wall);
    expect(w.presence.values.has(pKey(BRIDGE, SUPERVISOR))).toBe(true);
  });

  it("refuses a mirror set that names another link identity, and any mirror once fenced", async () => {
    const w = kvWorld();
    const { port: p, clock } = port(w);
    await p.start();
    await expect(p.mirror({ ...mirrorSet(clock.wall), linkIdentity: bridgeNatsLinkIdentity("other") }, clock.wall))
      .rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
    await p.fence();
    await expect(p.mirror(mirrorSet(clock.wall), clock.wall)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_FENCED" });
  });
});

describe("NATS bridge port: presence admission from the allow section", () => {
  const admit = natsBridgeAdmit({
    file: [{ kind: "root", id: FILE_ROOT }, { kind: "actor", rootId: FILE_ROOT, name: "fleet-supervisor", id: SUPERVISOR }],
    nats: [{ kind: "root", id: NATS_ROOT, machine: V2 }],
  });
  it("admits exact ids per direction and honors a machine pin", () => {
    expect(admit(fileRoot(), "toNats")).toBe(true);
    expect(admit(fileRoot(), "toFile")).toBe(false);
    expect(admit(natsRoot(), "toFile", V2)).toBe(true);
    expect(admit(natsRoot(), "toFile", "v3-org")).toBe(false);
    expect(admit(natsRoot(), "toNats")).toBe(false);
    expect(admit(fileActor(), "toNats")).toBe(true);
    expect(admit(fileActor({ id: "fedcba9876543210fedcba9876543210" }), "toNats")).toBe(false);
    expect(admit(fileActor({ rootId: NATS_ROOT }), "toNats")).toBe(false);
    expect(admit(fileActor(), "toFile")).toBe(false);
    // A root entry never admits an actor that happens to carry the root's id as its own.
    expect(admit({ ...fileActor(), id: FILE_ROOT }, "toNats")).toBe(false);
  });
});

describe("NATS bridge port: send", () => {
  it("sends a command from a held identity or the link identity, unchanged", async () => {
    const w = kvWorld();
    const { port: p, sent, clock } = port(w);
    await p.start();
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    const envelope = command({ principal: { kind: "human", id: "principal-a" } as unknown as TransportEnvelope["principal"] });
    expect(await p.send(envelope)).toMatchObject({ id: "bnf1-abc", sequence: 41, duplicate: false });
    expect(sent).toEqual([envelope]);
    expect((await p.send(command({ id: "bnf1-link", from: LINK_IDENTITY }))).id).toBe("bnf1-link");
  });

  it("refuses a command from an identity this link does not hold, before any publish; ACKs are not presence-gated", async () => {
    const w = kvWorld();
    const { port: p, sent } = port(w);
    await p.start();
    await expect(p.send(command())).rejects.toMatchObject({ code: "FABRIC_SENDER_UNVERIFIED" });
    await expect(p.send(command({ from: { id: 7 } as unknown as TransportEnvelope["from"] }))).rejects.toMatchObject({ code: "FABRIC_SENDER_UNVERIFIED" });
    expect((await p.send(command({ kind: "ack", id: "ack:n1", operation: "accepted" }))).id).toBe("ack:n1");
    expect(sent.map(envelope => envelope.id)).toEqual(["ack:n1"]);
  });

  it("refuses this machine as a destination and ids whose prefix contradicts the kind", async () => {
    const w = kvWorld();
    const { port: p, sent } = port(w);
    await p.start();
    for (const bad of [command({ from: LINK_IDENTITY, to: { machine: BRIDGE, hostId: FILE_ROOT } }),
      command({ from: LINK_IDENTITY, id: "ack:x" }), command({ kind: "ack", id: "bnf1-x", operation: "accepted" }),
      command({ from: LINK_IDENTITY, id: "relay:x" }), command({ from: LINK_IDENTITY, id: " padded" })]) {
      await expect(p.send(bad)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
    }
    expect(sent).toEqual([]);
  });

  it("waits out a predecessor lease from another boot instead of trusting its exit proof", async () => {
    const w = kvWorld();
    const first = port(w).port;
    await first.start();
    await first.fence();
    const otherHost = { pid: 5151, pidStartTime: "888", bootId: "ffffffff-1234-4234-8234-123456789abc" };
    const early = port(w, { proof: otherHost, predecessorExited: () => true, takeoverMs: 0 }).port;
    await expect(early.start()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_LEASE_CONFLICT" });
    const waiting = port(w, { proof: otherHost, predecessorExited: () => true }).port;
    const started = waiting.start();
    await new Promise(resolve => setTimeout(resolve, 50));
    w.leases.expire(hKey(BRIDGE, LINK_ID)); // The broker expires the old lease (15 s TTL).
    await started;
    expect(w.leases.values.get(hKey(BRIDGE, LINK_ID))?.value).toMatchObject({ pid: 5151, bootId: otherHost.bootId });
  });

  it("reports an outage as uncertain (same id and bytes may be resent), never as a refusal", async () => {
    const w = kvWorld();
    const down = Object.assign(new Error("NATS transport is temporarily unavailable"), { code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    const { port: p, clock } = port(w, { send: async () => { throw down; } });
    await p.start();
    const outage = command({ from: LINK_IDENTITY });
    await expect(p.send(outage)).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN", id: outage.id, resolvedBody: outage.body, cause: down });
    // A held sender whose lease writes have failed past the window is held for a resend too, not refused.
    await p.mirror(mirrorSet(clock.wall), clock.wall);
    const put = w.leases.put.bind(w.leases);
    w.leases.put = () => { throw Object.assign(new Error("broker down"), { code: "FABRIC_TRANSPORT_UNAVAILABLE" }); };
    advance(clock, 5_000);
    await p.kv.writer.renew().catch(() => undefined);
    advance(clock, 9_000);
    await expect(p.send(command())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN", id: "bnf1-abc" });
    w.leases.put = put;
  });

  it("passes an uncertain outcome and a definite refusal through unchanged", async () => {
    const w = kvWorld();
    let failure: Error = new TransportUncertainError("bnf1-abc", {});
    const { port: p } = port(w, { send: async () => { throw failure; } });
    await p.start();
    await expect(p.send(command({ from: LINK_IDENTITY }))).rejects.toBe(failure);
    failure = new TransportDeniedError("denied");
    await expect(p.send(command({ from: LINK_IDENTITY }))).rejects.toBe(failure);
    await p.fence();
    await expect(p.send(command({ from: LINK_IDENTITY }))).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_FENCED" });
  });
});

describe("NATS bridge port: machine-wide inbox", () => {
  it("yields a decoded command unsettled until the caller acks it", async () => {
    const w = kvWorld();
    const { port: p, inbox } = port(w);
    const iterator = p.inbox(new AbortController().signal)[Symbol.asyncIterator]();
    const principal = { kind: "human", id: "principal-b" };
    const sent = message({ id: "n-1", sequence: 7, attempt: 2, principal });
    inbox.push(sent);
    const delivery = await next(iterator);
    expect(delivery).toMatchObject({ sourceMachine: V2, toHostId: FILE_ROOT, streamSequence: 7, attempt: 2,
      envelope: { id: "n-1", kind: "command", createdAt: 123, to: { machine: BRIDGE, hostId: FILE_ROOT } } });
    expect(delivery.bodyDigest).toBe(controlBodyFingerprint(delivery.envelope.body, principal));
    expect(sent.calls).toEqual([]);
    delivery.working();
    await delivery.ack();
    await delivery.ack();
    delivery.nak();
    expect(sent.calls).toEqual(["working", "ack"]);
  });

  it("TERMs and NAKs only on request, once", async () => {
    const w = kvWorld();
    const { port: p, inbox } = port(w);
    const iterator = p.inbox(new AbortController().signal)[Symbol.asyncIterator]();
    const first = message({ id: "n-2" }), second = message({ id: "n-3" });
    inbox.push(first); inbox.push(second);
    const a = await next(iterator), b = await next(iterator);
    await a.term("refused by the allow-list");
    await a.ack();
    b.nak(500);
    b.working();
    expect(first.calls).toEqual(["term:refused by the allow-list"]);
    expect(second.calls).toEqual(["nak:500"]);
  });

  it("TERMs a message whose subject, payload or destination disagree, without yielding it", async () => {
    const w = kvWorld();
    const { port: p, inbox } = port(w);
    const iterator = p.inbox(new AbortController().signal)[Symbol.asyncIterator]();
    const bad = [
      message({ id: "s-1", subjectSource: "v3-org" }),                         // subject source token != payload sourceMachine
      message({ id: "s-2", toMachine: "v3-org" }),                              // payload destination is another machine
      message({ id: "s-3", subjectMachine: "v3-org", toMachine: "v3-org" }),    // not this machine's subject
      message({ id: "s-4", raw: "{not json" }),
      message({ id: " s-5" }),                                                 // not a header-safe id
    ];
    for (const item of bad) inbox.push(item);
    const good = message({ id: "ok" });
    inbox.push(good);
    expect((await next(iterator)).envelope.id).toBe("ok");
    for (const item of bad) expect(item.calls).toEqual([expect.stringMatching(/^term:/)]);
  });

  it("never settles anything while the connection awaits re-verification", async () => {
    const w = kvWorld();
    const { port: p, inbox } = port(w);
    const iterator = p.inbox(new AbortController().signal)[Symbol.asyncIterator]();
    inbox.verified = false;
    const malformed = message({ id: "m", raw: "{" });
    const ok = message({ id: "v" });
    inbox.push(malformed); inbox.push(ok);
    const delivery = await next(iterator);
    expect(malformed.calls).toEqual([]);
    await expect(delivery.ack()).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    expect(() => delivery.nak()).toThrow();
    expect(ok.calls).toEqual([]);
    inbox.verified = true;
    await delivery.ack();
    expect(ok.calls).toEqual(["ack"]);
  });

  it("allows one inbox iterator, ends it on close and runs the owner's close hook", async () => {
    const w = kvWorld();
    let closed = 0;
    const { port: p } = port(w, { onClose: async () => { closed += 1; } });
    const first = p.inbox(new AbortController().signal)[Symbol.asyncIterator]();
    const pending = first.next();
    await new Promise(resolve => setImmediate(resolve));
    await expect(p.inbox(new AbortController().signal)[Symbol.asyncIterator]().next()).rejects.toThrow(/Only one/);
    await p.close();
    await p.close();
    expect((await pending).done).toBe(true);
    expect(closed).toBe(1);
  });

  it("decodes only this machine's subjects", () => {
    const sample = message({ id: "d", kind: "ack", targetId: FILE_ROOT });
    expect(decodeNatsBridgeMessage(BRIDGE, sample)).toMatchObject({ sourceMachine: V2, envelope: { id: "d", kind: "ack" } });
    expect(decodeNatsBridgeMessage(BRIDGE, sample).bodyDigest).toBeUndefined();
    expect(() => decodeNatsBridgeMessage("other", sample)).toThrow(/mismatch/);
  });
});
