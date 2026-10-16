// Keep reason: data safety under interrupted persisted admission and cross-machine ACK trust.
// Source integration only. Explicit replay is carrier stimulus, not a simulated broker/dispatcher.
// B2a/b/c below inject logical interruptions at real boundaries; they do NOT claim SIGKILL/power-loss proof.
// Deliberately NOT registered as passing scenarios: T-X3e needs the real broker's retained
// max_deliver advisory; E19's operator is report-only, with NO inbox reads or relay publisher.
// E20 rollback requires a 0/0 drain, never a relay disposition. T-MIX needs A2's carrier-filtered
// directory/host-switch integration. Full T-ROLLBACK additionally needs the admission-drain
// and atomic 0/0-only host flip surface (E20). T-LOCK task custody needs a
// real pre-admitted AgentManager run, not a synthetic task journal. Native 40 s synchronous
// write blocking and OS process-death boundaries are not proven by the X3d/B2 slices here.
import path from "node:path";
import { setImmediate as immediate } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../../src/mesh/store.js";
import { FabricControlPlane, type FabricControlCommand, type FabricControlPlaneOptions } from "../../src/topology/control-plane.js";
import { MeshConsumptionPausedError } from "../../src/topology/mesh-consumption.js";
import { TransportUnavailableError, type TransportEnvelope, type TransportSent } from "../../src/transport/types.js";
import { bodyDigest, contractTiming, identity, type CommandBody, type ResolvedEnvelope } from "../helpers/transport-harness.js";
import {
  A1_REASON, ControlFixture, Receipts, assertA1, carrierKind, fromOwner, latch, laneTargetA, laneTargetB, actorTargetA, actorTargetB,
  objectBody, persistedBoundary, resolved, seenKey, seenRoot, transportIntegrated, transportRequired,
} from "./control-fixture.js";

const fixtures: ControlFixture[] = [];
const cleanup: Array<() => void | Promise<void>> = [];
function fixture(lanes = 8): ControlFixture { const f = new ControlFixture(lanes); fixtures.push(f); return f; }
// Missing A1 is an explicit skip, NEVER an early-return pass. Required CI runs the exact same
// cases and fails assertA1 before creating any stores. A broken/incomplete A1 is not skipped.
const controlIt = (name: string, run: () => Promise<void>) =>
  it.skipIf(!transportIntegrated && !transportRequired)(`${name}${!transportIntegrated ? ` — ${A1_REASON}` : ""}`, { timeout: 15_000 }, async () => {
    assertA1(); await run();
  });
afterEach(async () => {
  try {
    // Close auxiliary FILE planes BEFORE their fixture removes the shared temporary root.
    try { for (const dispose of cleanup.splice(0)) await dispose(); }
    finally { for (const f of fixtures.splice(0)) await f.dispose(); }
  } finally { vi.restoreAllMocks(); vi.useRealTimers(); }
});
function virtualClock(): void {
  // Normal progress is driven by carrier events and explicit gates. Fake time exercises ONLY
  // the product's bounded deadline/working timers; the receipt safety deadline stays real.
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  vi.setSystemTime(new Date(1_800_000_000_000));
}
function ackRecords(f: ControlFixture, commandEventId: string) {
  return f.senderCarrier.raw.records.filter(record => record.delivery.payload.id === `ack:${commandEventId}`);
}
function commandRecords(f: ControlFixture) {
  return f.receiverCarrier.raw.records.filter(record => record.delivery.payload.kind === "command" && record.delivery.payload.operation !== "cancel");
}
function hasAck(f: ControlFixture, commandEventId: string): boolean {
  return f.receiverCarrier.sent.items.some(sent => sent.id === `ack:${commandEventId}`);
}

// Counterexamples are deliberately part of each scenario: a fence must not ban a healthy
// independent target, unchanged restamped retry, correct owner ACK, or fresh-incarnation send.
describe("real FabricControlPlane / injected carrier faults", () => {
  controlIt("A1 seam selects the injected carrier and retains native Main custody before final ack", async () => {
    const f = fixture(); f.start();
    const input = { ownerIncarnation: f.receiver.incarnation, message: "native held followUp",
      data: { work: { id: "semantic-input", values: [1, false, null] } }, triggerTurn: false };
    const send = () => f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      input, f.receiverOwner.hostId, { idempotencyKey: "native-positive", timeoutMs: 120_000 });
    const result = await send();
    expect(result).toMatchObject({ acknowledged: true, queued: true });
    const sent = f.senderCarrier.sent.items.find(sent => sent.envelope.kind === "command")!;
    const wire = objectBody(sent.envelope), commandId = wire.commandId as string;
    expect(commandId).toBeTypeOf("string"); expect(commandId).not.toBe(sent.id);
    expect(wire).toMatchObject({ ...input, version: 1, targetId: f.receiverOwner.hostId,
      operation: "followUp", replyTo: f.senderOwner.hostId,
      requestedAt: expect.any(Number), requestCreatedAt: expect.any(Number), deadlineAt: expect.any(Number) });
    expect(f.admissions.items).toEqual([expect.objectContaining({ ...input, commandId, eventId: sent.id })]);
    await f.settled(sent.id);
    expect(carrierKind(f.receiver)).toBe("nats");
    expect(f.journalItems()).toEqual([expect.objectContaining({ deliveryId: commandId, message: input.message, data: input.data })]);
    expect(f.seen(commandId)?.acceptance).toMatchObject({ accepted: true, messageId: result.messageId });
    expect(f.seen(commandId)?.bodyDigest).toBe(bodyDigest(wire, sent.envelope.principal));
    expect(await f.ack(sent.id)).toMatchObject({ commandId, bodyDigest: bodyDigest(wire, sent.envelope.principal) });
    const nativeItems = f.journalItems();
    // An immediate same-key caller retry is a NEW event, not a broker duplicate; only
    // the logical control-seen claim prevents a second native admission.
    expect(await send()).toMatchObject({ acknowledged: true, messageId: result.messageId });
    const retry = f.senderCarrier.sent.items.filter(receipt => receipt.envelope.kind === "command")[1]!;
    expect(retry.id).not.toBe(sent.id); expect(retry.duplicate).toBe(false);
    expect(retry.sequence).toBeGreaterThan(sent.sequence);
    expect(objectBody(retry.envelope).commandId).toBe(commandId);
    await f.settled(retry.id);
    expect(await f.ack(retry.id)).toMatchObject({ commandId, accepted: true, messageId: result.messageId });
    expect(f.admissions.items).toHaveLength(1); expect(f.journalItems()).toEqual(nativeItems);
    // The same semantic input with an independent logical key must still reach custody.
    await expect(f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp", input,
      f.receiverOwner.hostId, { idempotencyKey: "native-independent", timeoutMs: 120_000 }))
      .resolves.toMatchObject({ acknowledged: true });
    const independent = f.senderCarrier.sent.items.filter(receipt => receipt.envelope.kind === "command")[2]!;
    await f.settled(independent.id);
    expect(objectBody(independent.envelope).commandId).not.toBe(commandId);
    expect(f.admissions.items).toHaveLength(2); expect(f.journalItems()).toHaveLength(2);
    expect(f.nativeSends).not.toHaveBeenCalled(); // B2, not B3/native consumption.
    expect(f.receiver.mesh.read({ topic: "fabric.control.command" })).toEqual([]);
    expect(f.receiver.mesh.read({ topic: "fabric.control.ack" })).toEqual([]);
  });

  controlIt("T-B2a: interrupt AFTER the real owner-local create-only claim, then replay without admission", async () => {
    const f = fixture(), interrupted = new Receipts<string>();
    let armed = true;
    const restore = persistedBoundary(seenRoot(f.meshRoot, f.receiverOwner.hostId), (phase, value) => {
      if (phase === "after" && value.commandId === "claim-interruption" && !value.acceptance && armed) {
        armed = false; f.receiver.pause(); interrupted.emit(value.commandId);
        throw new MeshConsumptionPausedError();
      }
    });
    f.keep(restore); f.start();
    const commandId = "claim-interruption";
    const envelope = await f.publish(f.raw(commandId, { eventId: "claim-interruption-event" }));
    expect(objectBody(envelope).commandId).toBe(commandId); expect(envelope.id).not.toBe(commandId);
    await interrupted.wait(id => id === commandId, "persisted claim interruption");
    const claim = f.seen(commandId);
    expect(claim).toMatchObject({ format: 1, commandId, hostId: f.receiverOwner.hostId,
      bodyDigest: bodyDigest(envelope.body, envelope.principal), carrier: "nats", writtenAt: expect.any(Number) });
    expect(claim).not.toHaveProperty("acceptance");
    expect(f.journalItems()).toEqual([]);
    expect(f.admissions.items).toEqual([]);
    expect(f.receiverCarrier.raw.records[0]?.acks).toBe(0);
    // Cleanly retire the interrupted object while PAUSED; not a claim that close == SIGKILL.
    await f.restart(); restore(); f.start();
    f.replay(envelope);
    expect(await f.ack(envelope.id)).toMatchObject({ accepted: false, error: expect.stringMatching(/indeterminate|unknown/i) });
    await f.settled(envelope.id);
    expect(f.admissions.items).toEqual([]);
    expect(f.journalItems()).toEqual([]);
    expect(f.seen(commandId)?.commandId).toBe(claim?.commandId);
    expect(ackRecords(f, envelope.id)).toHaveLength(1); // One persisted disposition, no invented acceptance.
    const retry = await f.publish(f.raw(commandId, { eventId: "claim-interruption-retry" }));
    await f.settled(retry.id);
    expect(await f.ack(retry.id)).toMatchObject({ commandId, accepted: false, error: expect.stringMatching(/indeterminate|unknown/i) });
    expect(f.admissions.items).toEqual([]); expect(f.journalItems()).toEqual([]);
    await f.publish(f.raw("after-claim-new-id", { target: laneTargetB })); await f.settled("after-claim-new-id");
    expect(f.journalItems()).toHaveLength(1); // A different logical command must remain admissible.
  });

  controlIt("T-B2b: real Main journal committed, outcome commit interrupted, replay is indeterminate and never re-admits", async () => {
    const f = fixture(), interrupted = new Receipts<string>();
    const restore = persistedBoundary(seenRoot(f.meshRoot, f.receiverOwner.hostId), (phase, value) => {
      if (phase === "before" && value.commandId === "custody-interruption" && value.acceptance) {
        // This surrounds the REAL persisted outcome boundary, after deliverAgent's durable
        // native journal return. Keep rejecting the shutdown fallback too: no graceful save
        // is allowed to manufacture the missing outcome this scenario is testing.
        f.receiver.pause(); interrupted.emit(value.commandId); throw new MeshConsumptionPausedError();
      }
    });
    f.keep(restore); f.start();
    const commandId = "custody-interruption";
    const envelope = await f.publish(f.raw(commandId, { eventId: "custody-interruption-event" }));
    expect(objectBody(envelope).commandId).toBe(commandId); expect(envelope.id).not.toBe(commandId);
    await interrupted.wait(id => id === commandId, "outcome write interrupted after Main custody");
    const nativeItems = f.journalItems();
    expect(nativeItems).toEqual([expect.objectContaining({ deliveryId: commandId })]);
    expect(f.admissions.items).toEqual([expect.objectContaining({ commandId, eventId: envelope.id })]);
    expect(f.seen(commandId)).not.toHaveProperty("acceptance");
    expect(ackRecords(f, envelope.id)).toEqual([]);
    expect(commandRecords(f)[0]?.acks).toBe(0);
    const close = f.receiver.close.bind(f.receiver);
    // This logical interruption rejects the real shutdown outcome write too; unlike
    // the OS-crash suite, a rejected close is expected evidence, not successful custody.
    vi.spyOn(f.receiver, "close").mockImplementationOnce(async () => {
      await expect(close()).rejects.toBeInstanceOf(MeshConsumptionPausedError);
    });
    await f.restart(); restore(); f.start(); f.replay(envelope);
    const ack = await f.ack(envelope.id); await f.settled(envelope.id);
    expect(ack).toMatchObject({ accepted: false, error: expect.stringMatching(/indeterminate|unknown/i) });
    expect(ack).not.toHaveProperty("notRun", true); // Custody EXISTS; never tell the sender it is safe to mint a new id.
    expect(f.admissions.items.filter(command => command.commandId === commandId)).toHaveLength(1);
    expect(f.journalItems()).toEqual(nativeItems);
    expect(f.nativeSends).not.toHaveBeenCalled();
    expect(ackRecords(f, envelope.id)).toHaveLength(1);
    expect(objectBody(ackRecords(f, envelope.id)[0]!.delivery.payload)).toMatchObject(ack);
    const retry = await f.publish(f.raw(commandId, { eventId: "custody-interruption-retry" }));
    await f.settled(retry.id);
    expect(await f.ack(retry.id)).toMatchObject(ack);
    expect(f.admissions.items).toHaveLength(1); expect(f.journalItems()).toEqual(nativeItems);
    await f.publish(f.raw("after-custody-new-id", { target: laneTargetB })); await f.settled("after-custody-new-id");
    expect(f.admissions.items).toHaveLength(2);
    expect(f.journalItems()).toEqual([...nativeItems, expect.objectContaining({ deliveryId: "after-custody-new-id" })]);
  });

  controlIt("T-B2c: interrupt after ACK PubAck but BEFORE broker ack; replay returns stored outcome and one sender settlement", async () => {
    const f = fixture(), interrupted = new Receipts<string>();
    let settlements = 0;
    const request = f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation, message: "one native admission" }, f.receiverOwner.hostId,
      { idempotencyKey: "ack-puback-interruption", timeoutMs: 120_000 }).then(value => { settlements++; return value; });
    void request.catch(() => undefined);
    const sent = await f.senderCarrier.sent.wait(receipt => receipt.envelope.kind === "command", "real request publish");
    const commandId = objectBody(sent.envelope).commandId as string;
    expect(commandId).not.toBe(sent.id);
    const record = commandRecords(f).find(record => record.delivery.payload.id === sent.id)!;
    const spy = vi.spyOn(record.delivery, "ack").mockImplementation(async () => {
      expect(f.seen(commandId)?.acceptance).toMatchObject({ accepted: true });
      expect(ackRecords(f, sent.id)).toHaveLength(1); // ACK already persisted by the carrier.
      f.receiver.pause(); interrupted.emit(sent.id); throw new MeshConsumptionPausedError();
    });
    f.keep(() => spy.mockRestore()); f.start();
    await interrupted.wait(id => id === sent.id, "post ACK PubAck interruption");
    const result = await request;
    expect(record.acks).toBe(0); expect(settlements).toBe(1);
    const outcome = f.seen(commandId)?.acceptance;
    await f.restart(); f.start(); f.replay(sent.envelope);
    expect(await f.ack(sent.id)).toMatchObject(outcome!); await f.settled(sent.id);
    expect(f.receiverCarrier.sent.items.find(receipt => receipt.id === `ack:${sent.id}`)?.duplicate).toBe(true);
    expect(f.journalItems()).toEqual([expect.objectContaining({ deliveryId: commandId, id: result.messageId })]);
    expect(f.admissions.items.filter(command => command.commandId === commandId)).toHaveLength(1);
    expect(ackRecords(f, sent.id)).toHaveLength(1);
    expect(f.senderCarrier.delivered.items.filter(receipt => receipt.id === `ack:${sent.id}`)).toHaveLength(1);
    expect(settlements).toBe(1);
  });

  controlIt("T-X3a: +11 minute same-id SAME-BODY restamped retry outlives broker dedupe and retains the 24 h claim", async () => {
    let clock = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => clock);
    const f = fixture(); f.start();
    const commandId = "retry-after-window";
    const first = await f.publish(f.raw(commandId)); await f.settled(first.id);
    const saved = f.seen(commandId)!;
    expect(saved.expiresAt).toBeGreaterThanOrEqual(Number(objectBody(first).deadlineAt) + contractTiming.retryHorizonMs);
    expect(saved.bodyDigest).toBe(bodyDigest(first.body as CommandBody, first.principal));
    clock += 11 * 60_000;
    const retry = await f.publish(f.raw(commandId));
    expect(objectBody(retry).requestedAt).not.toBe(objectBody(first).requestedAt);
    expect(objectBody(retry).deadlineAt).not.toBe(objectBody(first).deadlineAt);
    expect(f.senderCarrier.sent.items.filter(receipt => receipt.id === first.id).map(receipt => receipt.duplicate)).toEqual([false, false]);
    const ack = await f.ack(first.id, 2); await f.settled(first.id, 2);
    expect(ack).toMatchObject(saved.acceptance!);
    expect(f.admissions.items.filter(command => command.commandId === commandId)).toHaveLength(1);
    expect(f.journalItems()).toHaveLength(1);
    expect(f.seen(commandId)?.bodyDigest).toBe(saved.bodyDigest);
  });

  controlIt("T-X3b: ACK carrier outage after durable outcome; explicit redelivery recovers one real request", async () => {
    const f = fixture(), lost = new Receipts<string>();
    const send = f.receiverCarrier.transport.send.bind(f.receiverCarrier.transport);
    const spy = vi.spyOn(f.receiverCarrier.transport, "send").mockImplementation(async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      if (envelope.kind === "ack") {
        f.receiver.pause(); lost.emit(envelope.id); throw new TransportUnavailableError("injected ACK publish outage");
      }
      return send(envelope, signal);
    });
    f.keep(() => spy.mockRestore()); f.start();
    let settlements = 0;
    const request = f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation, message: "ACK outage" }, f.receiverOwner.hostId,
      { idempotencyKey: "lost-ack", timeoutMs: 120_000 }).then(value => { settlements++; return value; });
    void request.catch(() => undefined);
    const sent = await f.senderCarrier.sent.wait(receipt => receipt.envelope.kind === "command", "real command publish");
    const commandId = objectBody(sent.envelope).commandId as string;
    expect(commandId).not.toBe(sent.id);
    await lost.wait(id => id === `ack:${sent.id}`, "ACK outage");
    expect(f.seen(commandId)?.acceptance).toMatchObject({ accepted: true });
    expect(ackRecords(f, sent.id)).toEqual([]); expect(settlements).toBe(0);
    spy.mockRestore(); await f.restart(); f.start(); f.replay(sent.envelope);
    expect(await request).toMatchObject({ acknowledged: true });
    await f.receiverCarrier.acked.wait(receipt => receipt.id === sent.id && receipt.attempt === 2, "redelivery final ack");
    expect(f.admissions.items.filter(command => command.commandId === commandId)).toHaveLength(1);
    expect(f.journalItems()).toHaveLength(1); expect(ackRecords(f, sent.id)).toHaveLength(1); expect(settlements).toBe(1);
  });

  controlIt("T-X3c: a 60 s real ASK handler wait and in-flight duplicate both receive working() and settle from one admission", async () => {
    virtualClock(); const f = fixture(), gate = latch(); f.keep(gate.release);
    f.start(async command => { await gate.promise; return { accepted: true, result: { answer: 42 }, messageId: command.commandId }; });
    const result = f.sender.requestResult(f.receiverOwner.hostId, actorTargetA, "ask",
      { ownerIncarnation: f.receiver.incarnation, message: "bounded long ASK" }, f.receiverOwner.hostId,
      { idempotencyKey: "long-ask", timeoutMs: 120_000 });
    void result.catch(() => undefined);
    const command = await f.admissions.wait(() => true, "ASK admission");
    const commandEventId = command.eventId!;
    expect(commandEventId).toBeTypeOf("string"); expect(commandEventId).not.toBe(command.commandId);
    const original = commandRecords(f).find(record => record.delivery.payload.id === commandEventId)!;
    await vi.advanceTimersByTimeAsync(30_000);
    const duplicate = f.replay(original.delivery.payload as ResolvedEnvelope);
    await f.receiverCarrier.delivered.wait(receipt => receipt.id === commandEventId && receipt.attempt === 2, "in-flight duplicate pulled");
    await vi.advanceTimersByTimeAsync(30_000);
    expect(original.working).toBeGreaterThanOrEqual(5);
    expect(duplicate.working).toBeGreaterThanOrEqual(2);
    const ticks = f.receiverCarrier.working.items.filter(receipt => receipt.id === commandEventId && receipt.attempt === 1).map(receipt => receipt.at);
    expect(ticks[0]! - command.requestedAt).toBeLessThanOrEqual(contractTiming.workingEveryMs);
    for (let index = 1; index < ticks.length; index++) expect(ticks[index]! - ticks[index - 1]!).toBeLessThanOrEqual(contractTiming.workingEveryMs);
    expect(Date.now() - ticks.at(-1)!).toBeLessThanOrEqual(contractTiming.workingEveryMs);
    expect(original.acks).toBe(0); expect(duplicate.acks).toBe(0);
    expect(f.admissions.items).toHaveLength(1);
    gate.release(); expect(await result).toEqual({ answer: 42 });
    await f.settled(commandEventId, 2);
    expect(original.acks).toBe(1); expect(duplicate.acks).toBe(1);
    expect(ackRecords(f, commandEventId)).toHaveLength(1);
    expect(f.seen(command.commandId)?.acceptance).toMatchObject({ accepted: true, result: { answer: 42 } });
    // The stub records WPI calls only. Real nats-server T6 must separately prove no automatic redelivery.
  });

  controlIt("T-X3d (logical B2 persistence slice): a real control-seen outcome commit delayed 40 s gets working() until commit", async () => {
    virtualClock(); const f = fixture(), gate = latch(), entered = new Receipts<string>(); f.keep(gate.release);
    const commandId = "slow-outcome-write", commandEventId = "slow-outcome-event";
    const root = seenRoot(f.meshRoot, f.receiverOwner.hostId), key = seenKey(f.receiverOwner.hostId, commandId);
    const batch = MeshStore.prototype.writeBatch;
    // Delay entry to the ACTUAL outcome CAS batch outside its lock. Do not evaluate
    // its value callback early, or claim that JS timers progress during synchronous IO.
    const spy = vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
      if (path.resolve(this.root) === path.resolve(root) && input.ops.some(op =>
        op.kind === "put" && op.key === key && op.ifVersion !== undefined && op.ifVersion > 0)) {
        entered.emit(commandId); await gate.promise;
      }
      return batch.call(this, input);
    });
    f.keep(() => spy.mockRestore()); f.start();
    await f.publish(f.raw(commandId, { eventId: commandEventId }));
    await entered.wait(id => id === commandId, "real outcome batch before commit");
    expect(f.journalItems()).toHaveLength(1); expect(f.seen("slow-outcome-write")).not.toHaveProperty("acceptance");
    await vi.advanceTimersByTimeAsync(40_000);
    const record = commandRecords(f)[0]!;
    expect(record.working).toBeGreaterThanOrEqual(3); expect(record.acks).toBe(0);
    expect(record.delivery.payload.id).toBe(commandEventId);
    expect(hasAck(f, commandEventId)).toBe(false);
    gate.release(); await f.settled(commandEventId);
    expect(f.seen("slow-outcome-write")?.acceptance).toMatchObject({ accepted: true });
    expect(f.admissions.items).toHaveLength(1); expect(f.journalItems()).toHaveLength(1);
  });

  for (const changed of ["message", "data"] as const) {
    controlIt(`T-IDCONFLICT: changed ${changed} is notRun, but unchanged restamped retries remain observable`, async () => {
      let clock = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => clock);
      const f = fixture(); f.start();
      const commandId = `conflict-${changed}`;
      const first = await f.publish(f.raw(commandId)); await f.settled(first.id);
      const saved = f.seen(commandId)!;
      clock += 11 * 60_000;
      await f.publish(f.raw(commandId)); await f.settled(first.id, 2);
      expect(await f.ack(first.id, 2)).toMatchObject(saved.acceptance!);
      const beforeConflict = f.seen(commandId)!; // An equal-body retry may extend the retention horizon.
      // Expire BOTH command and ACK broker dedupe windows. Observing THIS attempt's
      // TransportSent.envelope when duplicate=true would not prove a persisted refusal.
      clock += 11 * 60_000;
      const attempt = resolved(f.raw(commandId));
      const changedBody = { ...objectBody(attempt), [changed]: changed === "message" ? "different immutable input" : { changed: true } };
      await f.publish({ ...attempt, body: changedBody });
      const refusal = await f.ack(first.id, 3); await f.settled(first.id, 3);
      expect(refusal).toMatchObject({ accepted: false, notRun: true, errorCode: "FABRIC_CONTROL_ID_CONFLICT" });
      expect(objectBody(ackRecords(f, first.id).at(-1)!.delivery.payload)).toMatchObject(refusal);
      expect(f.seen(commandId)).toEqual(beforeConflict); // Conflict may NOT overwrite the old outcome or digest.
      expect(f.admissions.items.filter(command => command.commandId === commandId)).toHaveLength(1);
      expect(f.journalItems()).toHaveLength(1);
      await f.publish(f.raw(`${commandId}-new-logical-id`, { message: "different immutable input" }));
      await f.settled(`${commandId}-new-logical-id`);
      expect(f.journalItems()).toHaveLength(2);
    });
  }

  for (const lanes of [8, 1]) {
    controlIt(`T-HOL: target B ${lanes === 8 ? "escapes" : "shares"} a 20 s target A stall with inboxLanes=${lanes}`, async () => {
      virtualClock(); const f = fixture(lanes), gate = latch(); f.keep(gate.release);
      const order: string[] = [];
      f.start(async command => {
        order.push(command.commandId);
        if (command.targetId === laneTargetA) await gate.promise;
        return { accepted: true, messageId: command.commandId };
      });
      await f.publish(f.raw("stalled-a", { target: laneTargetA }));
      await f.admissions.wait(command => command.commandId === "stalled-a", "A handler entered");
      await f.publish(f.raw("independent-b", { target: laneTargetB }));
      await f.receiverCarrier.delivered.wait(receipt => receipt.id === "independent-b", "B in hand");
      await vi.advanceTimersByTimeAsync(20_000);
      expect(hasAck(f, "stalled-a")).toBe(false);
      if (lanes === 8) {
        await f.settled("independent-b");
        expect(order).toEqual(["stalled-a", "independent-b"]);
        expect(f.seen("independent-b")?.acceptance).toMatchObject({ accepted: true });
      } else {
        expect(order).toEqual(["stalled-a"]); expect(hasAck(f, "independent-b")).toBe(false);
        const queued = commandRecords(f).find(record => record.delivery.payload.id === "independent-b")!;
        expect(queued.working).toBeGreaterThanOrEqual(1); expect(queued.acks).toBe(0);
      }
      gate.release(); await f.settled("stalled-a"); await f.settled("independent-b");
      expect(order).toEqual(["stalled-a", "independent-b"]);
      expect(f.receiverCarrier.acked.items.map(receipt => receipt.id)).toEqual(lanes === 8
        ? ["independent-b", "stalled-a"] : ["stalled-a", "independent-b"]);
      // This proves the concurrency knob, NOT a source-runner p95/real-host latency claim.
    });
  }

  controlIt("T-HOL actor serial: distinct actor stop targets share one lane while an independent Main target progresses", async () => {
    virtualClock(); const f = fixture(), gate = latch(); f.keep(gate.release);
    const order: string[] = [];
    f.start(async command => {
      order.push(command.commandId);
      if (command.commandId === "actor-alpha-stop") await gate.promise;
      return { accepted: true, messageId: command.commandId };
    });
    await f.publish(f.raw("actor-alpha-stop", { target: actorTargetA, operation: "stop" }));
    await f.admissions.wait(command => command.commandId === "actor-alpha-stop", "first actor stop");
    await f.publish(f.raw("actor-beta-stop", { target: actorTargetB, operation: "stop" }));
    await f.publish(f.raw("non-actor", { target: f.receiverOwner.hostId }));
    await f.settled("non-actor"); await vi.advanceTimersByTimeAsync(20_000);
    expect(order).toEqual(["actor-alpha-stop", "non-actor"]);
    expect(hasAck(f, "actor-beta-stop")).toBe(false);
    expect(commandRecords(f).find(record => record.delivery.payload.id === "actor-beta-stop")?.working).toBeGreaterThanOrEqual(1);
    gate.release(); await f.settled("actor-alpha-stop"); await f.settled("actor-beta-stop");
    expect(order).toEqual(["actor-alpha-stop", "non-actor", "actor-beta-stop"]);
  });

  controlIt("T-EXPIRE-QUEUED: a same-target queued command expires out of lane order with durable notRun; fresh work still runs", async () => {
    virtualClock(); const f = fixture(), gate = latch(); f.keep(gate.release);
    f.start(async command => {
      if (command.commandId === "lane-head") await gate.promise;
      return f.nativeAdmission(command, identity(f.senderOwner.hostId), new AbortController().signal, "bridge");
    });
    await f.publish(f.raw("lane-head"));
    await f.admissions.wait(command => command.commandId === "lane-head", "stalled lane head");
    const expiredCommandId = "queued-expired";
    const expired = await f.publish(f.raw(expiredCommandId, { eventId: "queued-expired-event", timeoutMs: 12_000 }));
    await f.receiverCarrier.delivered.wait(receipt => receipt.id === expired.id, "queued delivery");
    await vi.advanceTimersByTimeAsync(13_000);
    expect(await f.ack(expired.id)).toMatchObject({ accepted: false, error: "Fabric control command expired", notRun: true });
    await f.settled(expired.id);
    expect(hasAck(f, "lane-head")).toBe(false);
    expect(f.admissions.items.map(command => command.commandId)).toEqual(["lane-head"]);
    expect(f.seen(expiredCommandId)?.acceptance).toMatchObject({ accepted: false, notRun: true });
    expect(f.journalItems()).toEqual([]);
    await f.publish(f.raw("queued-fresh"));
    gate.release(); await f.settled("lane-head"); await f.settled("queued-fresh");
    expect(f.admissions.items.map(command => command.commandId)).toEqual(["lane-head", "queued-fresh"]);
    expect(f.journalItems().map(item => item.deliveryId)).toEqual(["lane-head", "queued-fresh"]);
  });

  controlIt("T-FENCE-1: settlement before the real command stamp prevents persistence, without suppressing a live request", async () => {
    const f = fixture(); f.start();
    await expect(f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation }, f.receiverOwner.hostId, { idempotencyKey: "live-before-race" }))
      .resolves.toMatchObject({ acknowledged: true });
    const before = commandRecords(f).length;
    const entered = latch(), gate = latch(); f.keep(gate.release);
    const transport = f.senderCarrier.transport;
    // Keep the carrier OPEN after close so a closed-carrier error cannot masquerade as P1.
    const closeSpy = vi.spyOn(transport, "close").mockResolvedValue(undefined); f.keep(() => closeSpy.mockRestore());
    const send = transport.send.bind(transport);
    let stampCalls = 0;
    const spy = vi.spyOn(transport, "send").mockImplementation(async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      if (envelope.kind !== "command" || envelope.operation === "cancel") return send(envelope, signal);
      entered.release(); await gate.promise;
      const body = envelope.body;
      expect(typeof body, "P1 must be checked in a real commit-time stamp").toBe("function");
      return send({ ...envelope, body: (at: number) => { stampCalls++; return (body as (at: number) => T)(at); } }, signal);
    });
    f.keep(() => spy.mockRestore());
    const pending = f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation }, f.receiverOwner.hostId, { idempotencyKey: "closed-before-stamp" });
    const outcome = pending.catch((error: unknown) => error);
    await entered.promise;
    const closing = f.sender.close();
    // The close event owns pending settlement; observe it before allowing the uncommitted stamp.
    expect(await outcome).toBeInstanceOf(Error);
    gate.release(); await closing; await immediate();
    expect(stampCalls).toBe(1);
    expect(commandRecords(f)).toHaveLength(before);
    expect(f.journalItems()).toHaveLength(1);
  });

  controlIt("T-FENCE-3: incarnation changes after persistence but before PubAck; successor refuses notRun and fresh epoch succeeds", async () => {
    const f = fixture(), entered = latch(), gate = latch(); f.keep(gate.release);
    const send = f.senderCarrier.transport.send.bind(f.senderCarrier.transport);
    let held = false;
    const spy = vi.spyOn(f.senderCarrier.transport, "send").mockImplementation(async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      // Bytes have LEFT the sender before the lease/epoch changes. A send-time route
      // recheck therefore cannot turn this into a cheaper pre-persistence refusal.
      const receipt = await send(envelope, signal);
      if (envelope.kind === "command" && envelope.operation !== "cancel" && !held) {
        held = true; entered.release(); await gate.promise;
      }
      return receipt;
    });
    f.keep(() => spy.mockRestore());
    const old = f.receiver.incarnation;
    const pending = f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: old }, f.receiverOwner.hostId, { idempotencyKey: "old-incarnation-race" });
    const outcome = pending.catch((error: unknown) => error);
    await entered.promise;
    // Script the view's lease loss. This is P3 receiver fencing, not a process-exit/takeover proof.
    f.receiverRoutes.live = false;
    await f.restart("successor-epoch"); f.receiverRoutes.live = true; f.start();
    gate.release();
    expect(await outcome).toMatchObject({ code: "FABRIC_CONTROL_STALE_INCARNATION" });
    const oldCommand = f.senderCarrier.sent.items.find(receipt => receipt.envelope.kind === "command" && receipt.envelope.operation !== "cancel")!;
    expect(await f.ack(oldCommand.id)).toMatchObject({ accepted: false, notRun: true, errorCode: "FABRIC_CONTROL_STALE_INCARNATION" });
    expect(f.admissions.items).toEqual([]); expect(f.journalItems()).toEqual([]);
    const oldCommandId = objectBody(oldCommand.envelope).commandId as string;
    expect(oldCommandId).not.toBe(oldCommand.id);
    expect(f.seen(oldCommandId)).toBeUndefined();
    await expect(f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation }, f.receiverOwner.hostId, { idempotencyKey: "fresh-incarnation" }))
      .resolves.toMatchObject({ acknowledged: true });
    expect(f.journalItems()).toHaveLength(1);
  });

  controlIt("T-FENCE-5: pause at the real ACK stamp holds publication and broker ack, but preserves the already-durable outcome", async () => {
    const f = fixture(), stamped = new Receipts<string>();
    const send = f.receiverCarrier.transport.send.bind(f.receiverCarrier.transport);
    let armed = true;
    const spy = vi.spyOn(f.receiverCarrier.transport, "send").mockImplementation(async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      if (envelope.kind !== "ack" || !armed) return send(envelope, signal);
      const body = envelope.body;
      expect(typeof body, "P5 is a commit-time predicate, not merely a preflight check").toBe("function");
      return send({ ...envelope, body: (at: number) => {
        armed = false; f.receiver.pause(); stamped.emit(envelope.id);
        return (body as (at: number) => T)(at);
      } }, signal);
    });
    f.keep(() => spy.mockRestore()); f.start();
    const commandId = "pause-ack-fence";
    const command = await f.publish(f.raw(commandId, { eventId: "pause-ack-fence-event" }));
    await stamped.wait(id => id === `ack:${command.id}`, "pause in ACK stamp"); await immediate();
    expect(ackRecords(f, command.id)).toEqual([]);
    expect(commandRecords(f)[0]?.acks).toBe(0);
    expect(f.seen(commandId)?.acceptance).toMatchObject({ accepted: true });
    expect(f.journalItems()).toHaveLength(1);
    // Reopen a carrier endpoint for reactivation: the frozen helper permits only ONE
    // iterator per endpoint. Do not emulate a second iterator or an inbox dispatcher here.
    spy.mockRestore(); await f.restart(); f.start(); f.replay(command);
    expect(await f.ack(command.id)).toMatchObject({ accepted: true });
    await f.receiverCarrier.acked.wait(receipt => receipt.id === command.id && receipt.attempt === 2, "ACK after reactivation");
    expect(f.admissions.items).toHaveLength(1); expect(f.journalItems()).toHaveLength(1);
    expect(ackRecords(f, command.id)).toHaveLength(1);
  });

  for (const [label, reason] of [
    ["Error", new Error("E11 own-host lease lost")],
    ["symbol", Symbol("E11 own-host lease cutoff")],
    ["object", Object.freeze({ code: "E11_LEASE_LOST", source: "composition-notification" })],
  ] as const) {
    controlIt(`T-E11-PAUSE-REASON ${label} [SAFE local]: exact inbox abort reason precedes final ACK; durable custody survives endpoint reactivation`, async () => {
      const f = fixture(), bound = new Receipts<AbortSignal>(), aborted = new Receipts<unknown>();
      const order: string[] = [];
      // Observe the signal supplied by the REAL #bindNatsInbox. Delegate the exact
      // signal/iterator to the existing carrier observer; never create a dispatcher.
      const inbox = f.receiverCarrier.transport.inbox.bind(f.receiverCarrier.transport);
      const inboxSpy = vi.spyOn(f.receiverCarrier.transport, "inbox").mockImplementation(signal => {
        bound.emit(signal);
        signal.addEventListener("abort", () => { order.push("inbox-abort"); aborted.emit(signal.reason); }, { once: true });
        return inbox(signal);
      });
      f.keep(() => inboxSpy.mockRestore());
      const send = f.receiverCarrier.transport.send.bind(f.receiverCarrier.transport);
      let armed = true;
      const ackSpy = vi.spyOn(f.receiverCarrier.transport, "send").mockImplementation(async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
        if (envelope.kind !== "ack" || !armed) return send(envelope, signal);
        const body = envelope.body;
        expect(typeof body).toBe("function");
        return send({ ...envelope, body: (at: number) => {
          armed = false; order.push("ack-stamp");
          f.receiver.pause(reason);
          // The original P5 stamp still decides publication; no invented ACK result.
          return (body as (at: number) => T)(at);
        } }, signal);
      });
      f.keep(() => ackSpy.mockRestore()); f.start();
      const inboxSignal = await bound.wait(() => true, "real receiver inbox signal");
      expect(inboxSignal.aborted).toBe(false);
      const commandId = `e11-reason-${label}`;
      const envelope = await f.publish(f.raw(commandId, { eventId: `${commandId}-event` }));
      expect(await aborted.wait(value => value === reason, "synchronous E11 inbox abort")).toBe(reason);
      expect(inboxSignal.aborted).toBe(true); expect(inboxSignal.reason).toBe(reason);
      await immediate();
      expect(order).toEqual(["ack-stamp", "inbox-abort"]);
      const original = commandRecords(f).find(record => record.delivery.payload.id === envelope.id)!;
      expect(original.acks).toBe(0); expect(original.naks).toEqual([]); expect(original.terms).toEqual([]);
      expect(ackRecords(f, envelope.id)).toEqual([]);
      const outcome = f.seen(commandId)!;
      expect(outcome.acceptance).toMatchObject({ accepted: true });
      const custody = f.journalItems();
      expect(custody).toEqual([expect.objectContaining({ deliveryId: commandId })]);
      expect(f.admissions.items).toEqual([expect.objectContaining({ commandId, eventId: envelope.id })]);
      // A named new endpoint is required by the frozen stub's single-iterator rule.
      // This is explicit CP reactivation, NOT an E8/A2 lease-reacquisition proof.
      ackSpy.mockRestore(); inboxSpy.mockRestore(); await f.restart(); f.start(); f.replay(envelope);
      await f.settled(envelope.id);
      expect(await f.ack(envelope.id)).toMatchObject(outcome.acceptance!);
      expect(original.acks).toBe(0); // Only the explicit redelivery was acknowledged.
      expect(f.admissions.items).toHaveLength(1); expect(f.journalItems()).toEqual(custody);
      await f.publish(f.raw(`${commandId}-healthy`, { target: laneTargetB }));
      await f.settled(`${commandId}-healthy`);
      expect(f.admissions.items).toHaveLength(2); expect(f.journalItems()).toHaveLength(2);
    });
  }

  for (const strict of [true, false]) {
    controlIt(`T-E11-FILE-PAUSE ${strict} [SAFE local]: legacy boolean pause retains in-flight completion, holds new admission, and resume admits fresh work`, async () => {
      const f = fixture(), entered = new Receipts<AbortSignal>(), appended = new Receipts<string>(), gate = latch();
      f.keep(gate.release);
      const receiverMesh = new MeshStore(f.meshRoot, 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 });
      const senderMesh = new MeshStore(f.meshRoot, 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 });
      const options = (hostId: string, ownerIncarnation: string): FabricControlPlaneOptions => ({
        enabled: true, hostId, ownerIncarnation, controlIncarnationFence: "enforce", pollMs: 20, acknowledgementTimeoutMs: 120_000,
      });
      const receiver = new FabricControlPlane(receiverMesh, fromOwner(f), options(f.receiverOwner.hostId, f.receiver.incarnation));
      const sender = new FabricControlPlane(senderMesh, identity(f.senderOwner.hostId), options(f.senderOwner.hostId, f.sender.incarnation));
      cleanup.push(async () => { gate.release(); receiver.pause(); sender.pause(); await Promise.allSettled([receiver.close(), sender.close()]); receiverMesh.closeState(); senderMesh.closeState(); });
      const publish = senderMesh.publish.bind(senderMesh);
      vi.spyOn(senderMesh, "publish").mockImplementation(async input => {
        const event = await publish(input);
        if (event.topic === "fabric.control.command" && event.kind === "followUp") appended.emit(event.id);
        return event;
      });
      let admissions = 0;
      receiver.start(async (...args) => {
        const acceptance = await f.nativeAdmission(...args);
        if (++admissions === 1) { entered.emit(args[2]); await gate.promise; }
        return acceptance;
      });
      sender.start(() => ({ accepted: false }));
      const request = (key: string) => sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
        { ownerIncarnation: receiver.incarnation, message: key }, f.receiverOwner.hostId,
        { idempotencyKey: key, timeoutMs: 120_000 });
      const first = request(`file-pause-${strict}-inflight`); void first.catch(() => undefined);
      const activeSignal = await entered.wait(() => true, "actual FILE handler signal after native custody");
      receiver.pause(strict);
      expect(activeSignal.aborted).toBe(false); expect(f.journalItems()).toHaveLength(1);
      gate.release(); await expect(first).resolves.toMatchObject({ acknowledged: true });
      expect(activeSignal.aborted).toBe(false); // Boolean pause never became NATS-style cancellation.
      const second = request(`file-pause-${strict}-held`); void second.catch(() => undefined);
      await appended.wait(() => true, "second real FILE append while paused", 2);
      expect(admissions).toBe(1); expect(f.journalItems()).toHaveLength(1);
      receiver.resume(); await expect(second).resolves.toMatchObject({ acknowledged: true });
      expect(admissions).toBe(2); expect(f.journalItems()).toHaveLength(2);
      expect(carrierKind(receiver)).toBe("file");
    });
  }

  controlIt("T-FENCE-6: forged machine and same-machine wrong-runtime ACKs are dropped; exact owner ACK still settles", async () => {
    const f = fixture(), gate = latch(); f.keep(gate.release);
    f.start(async (...args) => { await gate.promise; return f.nativeAdmission(...args); });
    let settled = false;
    const request = f.sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation, message: "authentic owner only" }, f.receiverOwner.hostId,
      { idempotencyKey: "ack-provenance", timeoutMs: 120_000 }).finally(() => { settled = true; });
    void request.catch(() => undefined);
    const command = await f.admissions.wait(() => true, "pending real request");
    const commandEventId = command.eventId!;
    const sent = await f.senderCarrier.sent.wait(receipt => receipt.id === commandEventId, "correlated real request publish");
    const digest = bodyDigest(sent.envelope.body, sent.envelope.principal);
    expect(commandEventId).not.toBe(command.commandId);
    // Both negatives are otherwise valid E14 ACKs for THIS event and logical body.
    // Invalid ids/missing digests would only test shape rejection, not P6 provenance.
    const ack = (fromId: string): TransportEnvelope => ({ id: `ack:${commandEventId}`, kind: "ack", to: f.senderOwner,
      targetId: command.targetId, operation: "accepted", from: identity(fromId), body: {
        version: 1, commandId: command.commandId, targetId: command.targetId, ownerIncarnation: f.receiver.incarnation,
        bodyDigest: digest, accepted: true, messageId: "forged-result",
      } });
    const wrongMachine = f.senderCarrier.raw.enqueue(resolved(ack(f.receiverOwner.hostId)),
      { sourceMachine: "test-attacker", verification: "bridge" });
    await f.senderCarrier.acked.wait(receipt => receipt.id === `ack:${commandEventId}` && receipt.sequence === wrongMachine.delivery.sequence,
      "forged ACK dropped/acked");
    expect(wrongMachine.acks).toBe(1); expect(settled).toBe(false);
    const wrongRuntime = f.senderCarrier.raw.enqueue(resolved(ack("session:other-runtime")),
      { sourceMachine: f.receiverOwner.machine, verification: "bridge" });
    await f.senderCarrier.acked.wait(receipt => receipt.id === `ack:${commandEventId}` && receipt.sequence === wrongRuntime.delivery.sequence,
      "wrong runtime ACK dropped/acked");
    expect(wrongRuntime.acks).toBe(1); expect(settled).toBe(false);
    gate.release(); const result = await request; await f.settled(commandEventId);
    expect(await f.ack(commandEventId)).toMatchObject({ commandId: command.commandId, bodyDigest: digest, accepted: true });
    expect(result).toMatchObject({ acknowledged: true }); expect(result.messageId).not.toBe("forged-result");
    expect(f.seen(command.commandId)?.acceptance?.messageId).toBe(result.messageId);
    expect(f.journalItems()).toHaveLength(1);
  });

  for (const operation of ["followUp", "steer", "stop"] as const) {
    controlIt(`T-LOCK Main ${operation}: ALS delivery→confirmed-ack span sees owner-local locks but ZERO global MeshLock acquisitions`, async () => {
      const f = fixture(), restore = f.locks.instrument(); f.keep(restore);
      // Positive control: prove the spy observes an ACTUAL acquisition on the exact global
      // root (a different process's or a different root's locks must not be attributed here).
      const probe = { finished: false, locks: [] as string[], globalAttempts: [] as string[], localLocks: [] as string[] };
      await f.locks.storage.run(probe, () => f.receiver.mesh.put({ key: "fault-suite/lock-probe", identity: fromOwner(f), value: 1 }));
      expect(probe.locks).toEqual([path.join(f.meshRoot, ".lock")]);
      f.start();
      const commandId = `lock-${operation}`;
      const envelope = await f.publish(f.raw(commandId, { eventId: `lock-${operation}-event`, operation }));
      await f.settled(envelope.id);
      const spans = f.locks.spans.filter(span => span.id === envelope.id);
      expect(spans).toHaveLength(1);
      expect(spans[0]?.finished).toBe(true);
      expect(spans[0]?.localLocks.length, "a zero from an unpropagated/empty ALS span is NOT evidence").toBeGreaterThan(0);
      expect(spans[0]?.locks).toEqual([]);
      expect(spans[0]?.globalAttempts, "even a failed global lock wait violates I-LOCK").toEqual([]);
      expect(f.seen(commandId)?.acceptance).toMatchObject({ accepted: true });
      if (operation === "followUp") expect(f.journalItems()).toHaveLength(1);
      if (operation === "steer") expect(f.nativeSends).toHaveBeenCalledOnce();
      if (operation === "stop") expect(f.nativeAbort).toHaveBeenCalledOnce();
    });
  }

  // E4 voids the completion control-op/I-LOCK row: completion custody stays on the
  // unchanged same-host journal. Native B4/B5 belong to native-boundaries.test.ts.

  controlIt("T-FILE-POSTCOMMIT (E1): real mesh append followed by wake timeout is uncertain/resolved, and same-key retry admits once", async () => {
    const f = fixture(); // Owns the actual Main custody handler and isolated temporary root.
    // Both FILE planes use the same real mesh, with NO fake file carrier or translated bus.
    const mesh = new MeshStore(f.meshRoot, 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 });
    const senderMesh = new MeshStore(f.meshRoot, 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 });
    const options = (hostId: string, ownerIncarnation: string): FabricControlPlaneOptions => ({
      enabled: true, hostId, ownerIncarnation, controlIncarnationFence: "enforce", pollMs: 20, acknowledgementTimeoutMs: 120_000,
    });
    const receiver = new FabricControlPlane(mesh, fromOwner(f), options(f.receiverOwner.hostId, f.receiver.incarnation));
    const sender = new FabricControlPlane(senderMesh, identity(f.senderOwner.hostId), options(f.senderOwner.hostId, f.sender.incarnation));
    cleanup.push(async () => { receiver.pause(); sender.pause(); await Promise.allSettled([receiver.close(), sender.close()]); mesh.closeState(); senderMesh.closeState(); });
    // Prevent the unused injected owner from opening its inbox; the FILE receiver alone admits.
    expect(carrierKind(receiver)).toBe("file"); expect(carrierKind(sender)).toBe("file");
    const admissions = vi.fn(f.nativeAdmission);
    receiver.start(admissions); sender.start(() => ({ accepted: false }));
    const publish = senderMesh.publish.bind(senderMesh);
    let injected = false;
    const spy = vi.spyOn(senderMesh, "publish").mockImplementation(async input => {
      const event = await publish(input);
      if (input.topic === "fabric.control.command" && input.kind !== "cancel" && !injected) {
        injected = true;
        // ACTUAL append completed. This is the public MeshStore postcommit/wake-return
        // boundary, not a transport wrapper that pre-labels its own error "uncertain".
        expect(senderMesh.read({ topic: input.topic }).some(stored => stored.id === event.id)).toBe(true);
        throw Object.assign(new Error("injected resident wake timeout AFTER append"), { code: "ETIMEDOUT" });
      }
      return event;
    });
    const send = () => sender.request(f.receiverOwner.hostId, f.receiverOwner.hostId, "followUp",
      { ownerIncarnation: f.receiver.incarnation, message: "file postcommit" }, f.receiverOwner.hostId,
      { idempotencyKey: "file-postcommit", timeoutMs: 120_000 });
    const first = await send().then(value => ({ value, error: undefined }), (error: unknown) => ({ value: undefined, error }));
    expect(injected).toBe(true);
    if (first.error !== undefined) {
      expect(first.error).toMatchObject({ code: "FABRIC_TRANSPORT_UNCERTAIN" });
      expect(first.error).not.toHaveProperty("persisted", false);
      expect(first.error).not.toHaveProperty("notDelivered", true);
    } else expect(first.value).toMatchObject({ acknowledged: true });
    spy.mockRestore();
    const result = await send();
    expect(result).toMatchObject({ acknowledged: true });
    const commands = senderMesh.read({ topic: "fabric.control.command", limit: 100 }).filter(event => event.kind !== "cancel");
    expect(commands).toHaveLength(2);
    expect(commands[0]?.id).not.toBe(commands[1]?.id);
    expect((commands[0]?.data as FabricControlCommand).commandId).toBe((commands[1]?.data as FabricControlCommand).commandId);
    expect(f.journalItems()).toEqual([expect.objectContaining({ id: result.messageId })]);
    expect(admissions).toHaveBeenCalledOnce();
    const acknowledgements = senderMesh.read({ topic: "fabric.control.ack", limit: 100 });
    expect(acknowledgements.length).toBeGreaterThan(0);
    const receipts = acknowledgements.map(event => event.data as { commandId: string; messageId: string });
    expect(new Set(receipts.map(receipt => receipt.commandId))).toEqual(new Set([(commands[0]!.data as FabricControlCommand).commandId]));
    expect(new Set(receipts.map(receipt => receipt.messageId))).toEqual(new Set([result.messageId]));
  });

  controlIt("T-ROLLBACK reconciliation slice: a NATS-admitted id retried through the actual FILE carrier returns the same local outcome", async () => {
    const f = fixture(); f.start();
    const commandId = "rollback-admitted";
    const envelope = await f.publish(f.raw(commandId, { eventId: "rollback-admitted-event" })); await f.settled(envelope.id);
    const saved = f.seen(commandId)!;
    expect(saved.acceptance).toMatchObject({ accepted: true });
    expect(await f.receiverCarrier.transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    f.receiver.pause(); await f.receiver.close(); await f.receiverCarrier.raw.close();
    const mesh = new MeshStore(f.meshRoot, 64 * 1024, 1_000, { stateBackend: "file", lockProtocol: 1 });
    const receiver = new FabricControlPlane(mesh, fromOwner(f), { enabled: true, hostId: f.receiverOwner.hostId,
      ownerIncarnation: f.receiver.incarnation, controlIncarnationFence: "enforce", pollMs: 20, acknowledgementTimeoutMs: 120_000 });
    cleanup.push(async () => { receiver.pause(); await receiver.close(); mesh.closeState(); });
    expect(carrierKind(receiver)).toBe("file");
    const admissions = vi.fn(f.nativeAdmission); receiver.start(admissions);
    const acked = new Receipts<unknown>();
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const event = await publish(input); if (event.topic === "fabric.control.ack") acked.emit(event.data); return event;
    });
    await mesh.publish({ topic: "fabric.control.command", kind: envelope.operation, to: f.receiverOwner.hostId,
      from: envelope.from, data: { ...objectBody(envelope), requestedAt: Date.now(), deadlineAt: Date.now() + 120_000 } });
    const ack = await acked.wait(value => typeof value === "object" && value !== null && Reflect.get(value, "commandId") === commandId, "FILE replay ACK");
    expect(ack).toMatchObject(saved.acceptance!);
    expect(admissions).not.toHaveBeenCalled();
    expect(f.journalItems()).toEqual([expect.objectContaining({ deliveryId: commandId })]);
    expect(f.seen(commandId)).toEqual(saved);
    // This is the carrier-independent control-seen reconciliation boundary ONLY. It does not
    // claim the host admission switch, INFO 0/0 drain, atomic 48h hold propagation,
    // bridge restart, or an installed-host rollback rehearsal.
  });
});
