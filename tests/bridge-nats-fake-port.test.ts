// The in-memory BridgeNatsPort fake that L2's NatsCarrierSide unit tests use (smarty-dev#15310, A' L4).
// These cases pin the fake to the real port's semantics the carrier depends on, so a carrier test that
// passes on the fake is not passing because the fake is lenient.
import { describe, expect, it } from "vitest";
import { bridgeNatsLinkIdentity } from "../src/mesh/bridge-nats-types.js";
import { TransportUncertainError, TransportUnavailableError, type TransportEnvelope } from "../src/transport/types.js";
import { FakeNatsBroker, type FakeBridgeNatsPort } from "./helpers/bridge-nats-fake-port.js";

const BRIDGE = "ryzen1-bridge", V2 = "v2-pod";
const LEAD = "session:15310b22-bbbb-4000-8000-000000000002", FILE_ROOT = "session:15310f11-aaaa-4000-8000-000000000001";

const command = (id: string, to: { machine: string; hostId: string }, body: unknown = { commandId: id }): TransportEnvelope =>
  ({ id, kind: "command", to, targetId: to.hostId, operation: "followUp", from: { id: LEAD, name: "lead", kind: "main" }, body });

async function take(port: FakeBridgeNatsPort, signal: AbortSignal, count: number) {
  const out = [];
  for await (const delivery of port.inbox(signal)) { out.push(delivery); if (out.length === count) break; }
  return out;
}

describe("FakeBridgeNatsPort", () => {
  it("sends as its own machine, dedupes by envelope id, and stores a lost-PubAck send exactly once", async () => {
    const broker = new FakeNatsBroker(), port = broker.port(BRIDGE);
    let stamps = 0;
    const sent = await port.send({ ...command("e1", { machine: V2, hostId: LEAD }), body: (at: number) => ({ stamped: at, n: ++stamps }) });
    expect(sent).toMatchObject({ id: "e1", duplicate: false, sequence: 1, envelope: { body: { n: 1 } } });
    expect(broker.messagesTo(V2)).toEqual([expect.objectContaining({ sourceMachine: BRIDGE, envelope: expect.objectContaining({ body: { stamped: expect.any(Number), n: 1 } }) })]);
    await expect(port.send(command("e1", { machine: V2, hostId: LEAD }))).resolves.toMatchObject({ duplicate: true, sequence: 1 });
    port.failNextSend("uncertainStored");
    await expect(port.send(command("e2", { machine: V2, hostId: LEAD }))).rejects.toBeInstanceOf(TransportUncertainError);
    await expect(port.send(command("e2", { machine: V2, hostId: LEAD }))).resolves.toMatchObject({ duplicate: true });
    port.failNextSend("uncertainLost");
    await expect(port.send(command("e3", { machine: V2, hostId: LEAD }))).rejects.toBeInstanceOf(TransportUncertainError);
    port.failNextSend("unavailable");
    await expect(port.send(command("e3", { machine: V2, hostId: LEAD }))).rejects.toBeInstanceOf(TransportUnavailableError);
    await expect(port.send(command("e3", { machine: V2, hostId: LEAD }))).resolves.toMatchObject({ duplicate: false });
    expect(broker.messagesTo(V2).map((message) => message.envelope.id)).toEqual(["e1", "e2", "e3"]);
    expect(broker.sends.map((attempt) => attempt.outcome)).toEqual(["stored", "duplicate", "uncertainStored", "duplicate", "uncertainLost", "unavailable", "stored"]);
  });

  it("delivers its inbox in stream order with the credential source machine; ack/term are final, nak and AckWait redeliver", async () => {
    const broker = new FakeNatsBroker(), port = broker.port(BRIDGE);
    broker.publish(V2, command("a", { machine: BRIDGE, hostId: FILE_ROOT }));
    broker.publish(V2, command("b", { machine: BRIDGE, hostId: FILE_ROOT }));
    broker.publish(BRIDGE, command("elsewhere", { machine: V2, hostId: LEAD }));
    const abort = new AbortController();
    const [a, b] = await take(port, abort.signal, 2);
    expect([a!.envelope.id, a!.sourceMachine, a!.attempt, b!.envelope.id]).toEqual(["a", V2, 1, "b"]);
    await a!.ack();
    b!.nak();
    const [again] = await take(port, abort.signal, 1);
    expect(again).toMatchObject({ streamSequence: b!.streamSequence, attempt: 2 });
    expect(broker.expireAckWait(BRIDGE)).toBe(1);
    const [third] = await take(port, abort.signal, 1);
    expect(third).toMatchObject({ streamSequence: b!.streamSequence, attempt: 3 });
    await third!.term("refused");
    await expect(a!.ack()).rejects.toThrow("not in flight");
    expect(broker.messagesTo(BRIDGE).map((message) => message.state)).toEqual(["acked", "termed"]);
    const second = port.inbox(abort.signal)[Symbol.asyncIterator]();
    const first = port.inbox(abort.signal)[Symbol.asyncIterator]();
    const pending = second.next();
    await expect(first.next()).rejects.toThrow("one inbox iterator");
    abort.abort();
    await expect(pending).resolves.toMatchObject({ done: true });
  });

  it("routes only a live presence + lease pair; an id on two machines is ambiguous; bridge-machine rows are not NATS presence", async () => {
    const broker = new FakeNatsBroker(), port = broker.port(BRIDGE);
    broker.putRoot(V2, LEAD, "epoch-1");
    broker.putRoot(BRIDGE, FILE_ROOT, "file-epoch");
    expect(port.resolve(LEAD)).toMatchObject({ route: { machine: V2, ownerIncarnation: "epoch-1", live: true }, identity: { id: LEAD, kind: "main" } });
    expect((await port.presence()).hosts.map((host) => host.record.id)).toEqual([LEAD]);
    broker.dropLease(V2, LEAD);
    expect(port.resolve(LEAD)?.route.live).toBe(false);
    expect((await port.presence()).hosts).toEqual([]);
    broker.putRoot(V2, LEAD, "epoch-2");
    broker.putRoot("other-pod", LEAD, "epoch-x");
    expect(port.resolve(LEAD)).toBeUndefined();
    expect(port.presenceView().records(LEAD).map((record) => record.machine).sort()).toEqual(["other-pod", V2]);
    expect((await port.presence()).hosts).toEqual([]);
  });

  it("mirrors under its own machine only, with the link identity, refuses an id another machine holds, and stops after fence", async () => {
    const broker = new FakeNatsBroker(), port = broker.port(BRIDGE);
    broker.putRoot(V2, LEAD, "epoch-1");
    const root = (id: string) => ({ format: 1 as const, id, rootId: id, kind: "root" as const, ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: "file-epoch", name: "lead", status: "idle" as const, runner: "pi" as const, transport: "host" as const,
      capabilities: ["followUp" as const], controlProtocol: "v1" as const, startedAt: 1, updatedAt: 1 });
    const linkIdentity = bridgeNatsLinkIdentity("v2");
    let changes = 0;
    const unwatch = port.watchPresence(() => { changes++; }, () => {});
    await port.mirror({ hosts: [], participants: [root(FILE_ROOT), root(LEAD)], linkIdentity }, Date.now());
    unwatch();
    expect(broker.rowsOn(BRIDGE).map((row) => row.record.id).sort()).toEqual([linkIdentity.id, FILE_ROOT].sort());
    expect(port.refusedMirror).toEqual([LEAD]);
    expect(changes).toBeGreaterThan(0);
    await port.fence();
    await expect(port.mirror({ hosts: [], participants: [], linkIdentity }, Date.now())).rejects.toThrow("fence");
    await port.withdraw();
    expect(broker.rowsOn(BRIDGE)).toEqual([]);
  });
});
