// Keep reason: E14 closes false non-delivery, duplicate effects and ACK trust-boundary failures.
// Accepted .local/contract/CONTRACT-v2-E14.md supersedes v2 §§3.2/3.3/P6/8.3.
// Source/protocol/runtime failures stay HARD. Only an absent optional nats-server skips
// broker-backed registrations; NATS_AUTH_REQUIRED=1 makes that prerequisite mandatory.
// An explicitly invalid NATS_BIN is never hidden. E19 relay deferrals remain explicit skips.
// Existing real-deletion/log-rewrite release/cleanup cases are HOSTED/AUTHOR-only, NOT
// authorized local runs. The separately named SAFE local probes below record/block product
// deletion batches and do not rewrite log evidence or claim installed-root release proof.
// Broker-window claims below use a REAL Path B JWT broker, machine-a != machine-b, factory
// openFabricTransport, runtime-only connections. Bootstrap provisions once and is then closed.
// Unit ACK cases are explicitly labelled: scripted ingress is NOT broker/ACL/dedupe proof.
// Relay tests exercise the real CP callback with broker-authenticated source tokens. The origin
// labels do NOT claim to manufacture a max_deliver advisory or perform orphan disposition:
// E15/E19 make the operator report-only; authenticated relay publication is deferred.
// Rollback is the real CP same-root FILE slice, not an installed-host/A2 composition-root test.
// The unchanged FILE floor is separate; product-deletion cases run only in hosted CI.
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { TransportEnvelope, TransportSent } from "../../src/transport/types.js";
import { identity } from "../helpers/transport-harness.js";
import { resolveNatsBinary } from "../helpers/nats-cluster.js";
import { MeshStore } from "../../src/mesh/store.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import { controlBodyFingerprint } from "../../src/mesh/control-idempotency.js";
import { normalizeFabricConfig } from "../../src/config.js";
import { createNatsOperator, createNatsAccount, mintNatsUser, NatsMintError } from "../../src/mesh/nats-auth/mint.js";
import { drainApi, lastWrite, receiptFor, receiptFromDrain, writeDrain } from "./e14-fixtures/drain.js";
import { latch, seenRoot } from "./control-fixture.js";
import {
  BrokerScene, FileScene, Receipts, blocked, noDefiniteNonDelivery, objectBody,
  ownerAck, relay, stubScene, injectUnitAck, readSeen, seenStore, seenKey,
  senderOwner, receiverOwner, ownerEpoch, OPERATOR_MACHINE, HOLD_MS, HOUR, writeRollbackFlip,
  type ControlScene, type SeenRecord,
} from "./e14-fixtures/control.js";

// resolveNatsBinary validates an explicit NATS_BIN synchronously. Do not catch its
// configuration error or turn an installed broker's source/runtime failure into a skip.
const natsBinary = resolveNatsBinary();
const natsRequired = process.env.NATS_AUTH_REQUIRED === "1";
const optionalBrokerMissing = !natsBinary && !natsRequired;
const brokerDescribe = describe.skipIf(optionalBrokerMissing);
const brokerIt = it.skipIf(optionalBrokerMissing);
const dispose: Array<() => Promise<void>> = [];
afterEach(async () => {
  const failures: unknown[] = [];
  try { for (const close of dispose.splice(0).reverse()) { try { await close(); } catch (error) { failures.push(error); } } }
  finally { vi.restoreAllMocks(); vi.useRealTimers(); }
  if (failures.length) throw new AggregateError(failures, "E14 fixture teardown failed");
});
async function broker(): Promise<BrokerScene> {
  blocked(natsBinary, "E14 real Path B broker prerequisite missing: NATS_AUTH_REQUIRED=1 requires nats-server (or an absolute executable NATS_BIN)");
  const f = await BrokerScene.open(); dispose.push(() => f.close()); return f;
}
function files(root?: string, holdUntil?: number): FileScene {
  const f = new FileScene(root, holdUntil);
  dispose.push(async () => { await f.close(); if (root === undefined) fs.rmSync(f.root, { recursive: true, force: true }); });
  return f;
}
function logical(sent: TransportSent): string { return String(objectBody(sent.envelope).commandId); }
function e14Event(sent: TransportSent): void {
  blocked(sent.id !== logical(sent), "A1 E14.1 command Nats-Msg-Id must be per-request commandEventId, NOT logical commandId");
}
function digest(sent: TransportSent): string {
  return controlBodyFingerprint(objectBody(sent.envelope), sent.envelope.principal);
}
function withinWindow(first: TransportSent, last: TransportSent): void {
  expect(last.envelope.createdAt - first.envelope.createdAt).toBeGreaterThanOrEqual(0);
  expect(last.envelope.createdAt - first.envelope.createdAt).toBeLessThan(10 * 60_000);
}
function captured<T>(promise: Promise<T>) {
  let done = false;
  const outcome = promise.then(value => { done = true; return { value, error: undefined }; },
    (error: unknown) => { done = true; return { value: undefined, error }; });
  return { outcome, done: () => done };
}
async function stored(f: ControlScene, sent: TransportSent): Promise<SeenRecord> {
  await f.consumed(sent.id);
  const record = f.seen(logical(sent));
  expect(record?.acceptance).toMatchObject({ accepted: true });
  blocked(record?.carrier === "nats", "A1 E14.3 every NATS claim/outcome must persist carrier:nats");
  expect(record.bodyDigest).toBe(digest(sent));
  blocked(typeof record.writtenAt === "number", "A1 E17 NATS outcome must carry writtenAt on every write");
  const ack = await f.ack(sent.id);
  blocked(objectBody(ack.envelope).bodyDigest === digest(sent), "A1 E14.1 ACK must carry the answered wire bodyDigest");
  return record;
}

/** RECORDED-CALL guard at the product's real cleanup primitive, not a replacement
 * release/expiry predicate. Preserve every requested op/CAS field and reject the
 * WHOLE batch before calling the original. Never return a fabricated deletion result. */
function recordControlDeletes(root: string, onDelete: () => void = () => undefined) {
  const calls = new Receipts<{ root: string; input: Parameters<MeshStore["writeBatch"]>[0] }>();
  const localRoot = seenRoot(root, receiverOwner.hostId), current = MeshStore.prototype.writeBatch;
  // Vitest reuses an existing spy object. Capture its implementation, not that
  // mutable spy, when FileScene already observes this primitive (avoids recursion).
  const batch = vi.isMockFunction(current) ? current.getMockImplementation() as MeshStore["writeBatch"] | undefined : current;
  blocked(batch, "RECORDED-CALL guard requires the existing concrete writeBatch implementation");
  vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
    if ((this.root === root || this.root === localRoot) && input.ops.some(op => op.kind === "delete")) {
      calls.emit({ root: this.root, input: { ...input, ops: input.ops.map(op => ({ ...op })) } });
      onDelete();
      throw new Error("RECORDED-CALL: product control deletion batch withheld; no writes/deletes performed");
    }
    return batch.call(this, input);
  });
  return calls;
}

// All timing here is real: fake process time cannot prove a real server's 10-minute dedupe.
brokerDescribe("E14.1 real broker / REAL FabricControlPlane", () => {
  for (const changed of ["message", "data"] as const) {
    it(`T-IDCONFLICT-WINDOW ${changed}: same key + different body inside 10m reaches owner, notRun, handler once`, { timeout: 90_000 }, async () => {
      const f = await broker(); f.start();
      const first = await f.request(`conflict-${changed}`), one = await f.command(); e14Event(one);
      const saved = await stored(f, one);
      const input = { ownerIncarnation: ownerEpoch, message: changed === "message" ? "changed immutable body" : "E14 immutable input",
        ...(changed === "data" ? { data: { changed: true } } : {}) };
      const refusal = captured(f.sender.request(receiverOwner.hostId, receiverOwner.hostId, "followUp", input,
        receiverOwner.hostId, { idempotencyKey: `conflict-${changed}`, timeoutMs: 30_000 }));
      const two = await f.command(2); e14Event(two); withinWindow(one, two);
      expect(two.id).not.toBe(one.id); expect(two.duplicate).toBe(false);
      expect(logical(two)).toBe(logical(one)); expect(digest(two)).not.toBe(digest(one));
      const answer = await f.ack(two.id); await f.consumed(two.id);
      expect(objectBody(answer.envelope)).toMatchObject({ accepted: false, notRun: true,
        errorCode: "FABRIC_CONTROL_ID_CONFLICT", bodyDigest: digest(two) });
      expect((await refusal.outcome).error).toMatchObject({ code: "FABRIC_CONTROL_ID_CONFLICT", notRun: true });
      expect(f.seen(logical(one))).toEqual(saved); // Refusal must NOT overwrite the original claim/outcome.
      expect(f.admissions.items).toHaveLength(1);
      expect(await f.request(`conflict-${changed}`)).toEqual(first); // Correct same-body counterexample.
      const three = await f.command(3); withinWindow(one, three); expect(three.id).not.toBe(one.id);
      await f.consumed(three.id); expect(f.admissions.items).toHaveLength(1);
      await expect(f.request(`fresh-${changed}`, "independent changed command")).resolves.toMatchObject({ acknowledged: true });
      expect(f.admissions.items).toHaveLength(2); // Conflict must not ban a fresh logical command.
    });
  }

  for (const field of ["priority", "binding", "bindingProvenance", "triggerTurn"] as const) {
    it(`T-DIGEST-FIELDS ${field}: reused key with changed semantic field conflicts; unchanged retry keeps stored outcome`, { timeout: 90_000 }, async () => {
      const f = await broker(); f.start();
      const operation = "steer" as const;
      const initialInput = { ownerIncarnation: ownerEpoch, message: "semantic field input", triggerTurn: false,
        binding: { model: "model-a", thinking: "high" as const }, bindingProvenance: { kind: "owner-defaults" as const, rootId: "root-a" } };
      const changed = { ...initialInput, ...(field === "priority" ? { priority: "interrupt" as const }
        : field === "binding" ? { binding: { model: "model-b", thinking: "high" as const } }
        : field === "bindingProvenance" ? { bindingProvenance: { kind: "owner-defaults" as const, rootId: "root-b" } }
        : { triggerTurn: true }) };
      const sends = () => f.senderCarrier.sent.items.filter(sent => sent.envelope.kind === "command" && sent.envelope.operation === operation);
      const request = (input: typeof changed) => f.sender.request(receiverOwner.hostId, receiverOwner.hostId, operation,
        input, receiverOwner.hostId, { idempotencyKey: `digest-${field}`, timeoutMs: 30_000 });
      const first = await request(initialInput), one = sends()[0]!; e14Event(one);
      const saved = await stored(f, one);
      const refused = captured(request(changed));
      const two = await f.senderCarrier.sent.wait(sent => sent.envelope.kind === "command" && sent.envelope.operation === operation,
        "changed semantic field publish", 2);
      expect(logical(two)).toBe(logical(one)); withinWindow(one, two);
      expect(digest(two), `controlBodyFingerprint must bind ${field}`).not.toBe(digest(one));
      expect((await refused.outcome).error).toMatchObject({ code: "FABRIC_CONTROL_ID_CONFLICT", notRun: true });
      expect(objectBody((await f.ack(two.id)).envelope)).toMatchObject({ bodyDigest: digest(two), notRun: true });
      await f.consumed(two.id); expect(f.seen(logical(one))).toEqual(saved);
      expect(await request(initialInput)).toEqual(first); expect(f.admissions.items).toHaveLength(1);
    });
  }

  it("T-IDCONFLICT-WINDOW in-flight: different digest does not join; equal digest joiners get one owner outcome", { timeout: 90_000 }, async () => {
    const f = await broker(), gate = latch(); f.releases.push(gate.release);
    f.start(async command => { await gate.promise; return { accepted: true, messageId: `stored:${command.commandId}` }; });
    const waiting = captured(f.request("inflight-digest"));
    const one = await f.command(); e14Event(one);
    await f.admissions.wait(() => true, "original handler admission");
    // Same valid owner callback, but a NEW carrier message with one changed semantic field.
    // Use raw carrier stimulus because sender intentionally permits only one local pending/key.
    const two = await f.senderCarrier.transport.send({ ...one.envelope, id: randomUUID(),
      body: { ...objectBody(one.envelope), message: "foreign in-flight input" } });
    withinWindow(one, two);
    const refused = await f.ack(two.id); await f.consumed(two.id);
    expect(objectBody(refused.envelope)).toMatchObject({ accepted: false, errorCode: "FABRIC_CONTROL_ID_CONFLICT",
      notRun: true, bodyDigest: digest(two) });
    await f.consumed(refused.id, f.senderCarrier);
    expect(waiting.done()).toBe(false); // A P6-valid conflict for the OTHER digest cannot settle attempt one.
    expect(f.admissions.items).toHaveLength(1);
    const equal = await f.senderCarrier.transport.send({ ...one.envelope, id: randomUUID() });
    await f.ownerCarrier.delivered.wait(delivery => delivery.payload.id === equal.id, "equal-digest joiner pulled");
    gate.release(); expect((await waiting.outcome).value).toMatchObject({ acknowledged: true });
    await f.consumed(one.id); await f.consumed(equal.id);
    const successes = f.ownerCarrier.sent.items.filter(sent => sent.envelope.kind === "ack" && objectBody(sent.envelope).accepted === true);
    expect(successes).toHaveLength(1); // Joined set gets ONE ACK; no second admission.
    expect(f.admissions.items).toHaveLength(1);
    expect(f.seen(logical(one))?.bodyDigest).toBe(digest(one));
  });

  it("T-LATEACK-REPEAT: timeout, late ACK consumed/dropped, <10m same-key caller retry observes stored outcome", { timeout: 100_000 }, async () => {
    const f = await broker(), gate = latch(); f.releases.push(gate.release);
    f.start(async command => { await gate.promise; return { accepted: true, messageId: `late-stored:${command.commandId}` }; });
    const first = captured(f.request("late-ack"));
    const one = await f.command(); e14Event(one);
    await f.admissions.wait(() => true, "admitted before sender timeout");
    const timedOut = (await first.outcome).error; noDefiniteNonDelivery(timedOut);
    expect(String(timedOut)).toMatch(/outcome is unknown/i);
    // Handler deliberately ignores observation cancellation: B2 can finish after the sender leaves.
    gate.release(); const late = await f.ack(one.id);
    await f.consumed(late.id, f.senderCarrier); await f.consumed(one.id);
    const saved = f.seen(logical(one))!;
    expect(saved.acceptance).toMatchObject({ accepted: true, messageId: `late-stored:${logical(one)}` });
    expect(f.sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
    // Block THIS retry's owner ACK at its real send boundary. A late-ACK cache would settle
    // prematurely; dropping the old ACK must leave us pending until the new stored-outcome answer.
    const retryAckGate = latch(), retryAnswer = new Receipts<string>(); f.releases.push(retryAckGate.release);
    let retryEventId: string | undefined;
    const publish = f.senderCarrier.transport.send.bind(f.senderCarrier.transport);
    vi.spyOn(f.senderCarrier.transport, "send").mockImplementation(<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal) => {
      if (envelope.kind === "command" && envelope.operation === "followUp") retryEventId = envelope.id;
      return publish(envelope, signal);
    });
    const send = f.ownerCarrier.transport.send.bind(f.ownerCarrier.transport);
    vi.spyOn(f.ownerCarrier.transport, "send").mockImplementation(async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal) => {
      if (envelope.kind === "ack" && envelope.id === `ack:${retryEventId}`) {
        retryAnswer.emit(envelope.id); await retryAckGate.promise;
      }
      return send(envelope, signal);
    });
    const again = captured(f.request("late-ack")); const two = await f.command(2);
    withinWindow(one, two); expect(two.id).not.toBe(one.id); expect(two.duplicate).toBe(false);
    expect(logical(two)).toBe(logical(one));
    await retryAnswer.wait(id => id === `ack:${two.id}`, "new-attempt ACK from durable claim");
    expect(again.done()).toBe(false);
    retryAckGate.release(); expect((await again.outcome).value).toMatchObject({ acknowledged: true, messageId: saved.acceptance!.messageId });
    await f.consumed(two.id); const ack = await f.ack(two.id);
    expect(ack.duplicate).toBe(false); expect(ack.id).not.toBe(late.id);
    expect(f.admissions.items).toHaveLength(1);
  });

  it("T-B1 amended: same-request byte-identical republish stores ONE carrier message; caller retry stores a SECOND; claim alone dedupes", { timeout: 90_000 }, async () => {
    const f = await broker(), gate = latch(); f.releases.push(gate.release);
    f.start(async command => { await gate.promise; return { accepted: true, messageId: `one-effect:${command.commandId}` }; });
    const before = await f.streamState(), pending = captured(f.request("b1-repeat"));
    const one = await f.command(); e14Event(one);
    await f.admissions.wait(() => true, "B1 handler held");
    // Explicit republish of the resolved bytes of THIS request, as after uncertain PubAck.
    // This is not a sender SIGKILL claim and does not invent a second request/retry engine.
    const repeat = await f.republishExact(one);
    expect(repeat).toMatchObject({ seq: one.sequence, duplicate: true });
    expect((await f.streamState()).messages - before.messages).toBe(1);
    gate.release(); const first = (await pending.outcome).value; expect(first).toMatchObject({ acknowledged: true });
    await f.consumed(one.id);
    const second = await f.request("b1-repeat"), two = await f.command(2); // Raw byte-identical republish is not another CP send.
    withinWindow(one, two); expect(two.id).not.toBe(one.id); expect(two.sequence).toBeGreaterThan(one.sequence);
    expect(two.duplicate).toBe(false); expect(logical(two)).toBe(logical(one)); expect(second).toEqual(first);
    await f.consumed(two.id);
    expect((await f.streamState()).messages - before.messages).toBe(2);
    expect(f.admissions.items).toHaveLength(1);
    expect(f.seen(logical(one))?.acceptance?.messageId).toBe(second.messageId);
  });
});

// Narrow deterministic trust-boundary checks ONLY. No stub case earns broker-window/ACL proof.
describe("E14 unit / same valid ACK ingress callback (NOT real-broker proof)", () => {
  function unit() {
    const { scene: f, inbox } = stubScene();
    dispose.push(async () => { await f.close(); fs.rmSync(f.root, { recursive: true, force: true }); });
    return { f, inbox };
  }
  it("T-ACK-DIGEST unit: P6-valid foreign/missing/malformed digest drops; exact owner+digest at identical callback settles", { timeout: 15_000 }, async () => {
    const { f, inbox } = unit(), gate = latch(); f.releases.push(gate.release);
    f.start(async () => { await gate.promise; return { accepted: true, messageId: "real-later" }; });
    const pending = captured(f.request("digest-trust")), command = await f.command(); e14Event(command);
    const valid = ownerAck(command, { bodyDigest: digest(command), messageId: "unit-positive" });
    const cases: Array<{ label: string; envelope: TransportEnvelope; machine: string }> = [
      { label: "wrong-machine", envelope: valid, machine: "machine-attacker" },
      { label: "wrong-runtime", envelope: { ...valid, from: identity("session:other-owner") }, machine: receiverOwner.machine },
      ...["0".repeat(64), undefined, "not-a-digest", "F".repeat(64)].map(value => ({ label: `digest-${String(value)}`,
        envelope: ownerAck(command, { bodyDigest: value }), machine: receiverOwner.machine })),
    ];
    let count = 0;
    for (const candidate of cases) {
      injectUnitAck(inbox, candidate.envelope, candidate.machine);
      await f.consumed(valid.id, f.senderCarrier, ++count);
      expect(pending.done(), `${candidate.label} must drop at the SAME valid pending callback`).toBe(false);
    }
    injectUnitAck(inbox, valid, receiverOwner.machine); await f.consumed(valid.id, f.senderCarrier, ++count);
    expect((await pending.outcome).value).toMatchObject({ acknowledged: true, messageId: "unit-positive" });
    gate.release(); await f.consumed(command.id);
    expect(f.sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
  });

  it("T-RELAY-FORGE unit: runtime relay, operator success/notRun/extra keys, owner relay codes drop; real-owner ACK remains allowed", { timeout: 15_000 }, async () => {
    const { f, inbox } = unit(), gate = latch(); f.releases.push(gate.release);
    f.start(async () => { await gate.promise; return { accepted: true, messageId: "later" }; });
    const pending = captured(f.request("relay-forge")), command = await f.command(); e14Event(command);
    const valid = relay(command);
    expect(Object.keys(objectBody(valid)).sort()).toEqual([
      "version", "commandId", "commandEventId", "targetId", "accepted", "error", "errorCode", "retryable",
    ].sort());
    const variants: Array<{ envelope: TransportEnvelope; machine: string; label: string }> = [
      { envelope: valid, machine: senderOwner.machine, label: "runtime-a relay forgery" },
      { envelope: valid, machine: receiverOwner.machine, label: "owner runtime relay forgery" },
      ...[{ accepted: true }, { notRun: true }, { notRun: false }, { notDelivered: true }, { ownerIncarnation: ownerEpoch },
        { result: 1 }, { errorCode: "FABRIC_IDEMPOTENCY_KEY_REUSED" }, { retryable: false },
        { version: 2 }, { error: "different error" }, { surplus: null }, { targetId: "wrong-target" },
        { commandId: "wrong-command" }, { commandEventId: "previous-attempt" }].map(patch => ({
        envelope: relay(command, patch), machine: OPERATOR_MACHINE, label: JSON.stringify(patch),
      })),
      { envelope: { ...valid, id: `relay:${logical(command)}` }, machine: OPERATOR_MACHINE, label: "logical-id not event-id" },
      { envelope: { ...valid, kind: "command" }, machine: OPERATOR_MACHINE, label: "wrong envelope kind" },
      ...["FABRIC_RELAY_DESTINATION_UNAVAILABLE", "FABRIC_IDEMPOTENCY_KEY_REUSED"].map(errorCode => ({
        envelope: ownerAck(command, { accepted: false, bodyDigest: digest(command), errorCode, error: "Relay destination unavailable" }),
        machine: receiverOwner.machine, label: `owner must not borrow operator code ${errorCode}`,
      })),
      // Even an ordinary P6-valid success from ops MUST take relay-only handling, never P6.
      { envelope: ownerAck(command, { bodyDigest: digest(command) }), machine: OPERATOR_MACHINE, label: "operator owner impersonation" },
    ];
    const counts = new Map<string, number>();
    for (const variant of variants) {
      const count = (counts.get(variant.envelope.id) ?? 0) + 1; counts.set(variant.envelope.id, count);
      injectUnitAck(inbox, variant.envelope, variant.machine);
      // E14's relay-only ingress drops/logs invalid operator traffic before ordinary
      // command validation, including wrong-kind traffic. Its confirmed ACK is a receipt,
      // never a successful control outcome or a native admission.
      await f.consumed(variant.envelope.id, f.senderCarrier, count);
      expect(pending.done(), variant.label).toBe(false);
    }
    // E19 defers authenticated operator-relay success. Do not fabricate that authority here.
    // The counterexample is a valid owner ACK through this SAME live pending callback.
    const positive = ownerAck(command, { bodyDigest: digest(command), messageId: "owner-positive" });
    const count = (counts.get(positive.id) ?? 0) + 1;
    injectUnitAck(inbox, positive, receiverOwner.machine); await f.consumed(positive.id, f.senderCarrier, count);
    expect((await pending.outcome).value).toMatchObject({ acknowledged: true, messageId: "owner-positive" });
    gate.release(); const late = await f.ack(command.id);
    await f.consumed(late.id, f.senderCarrier, count + 1);
    expect(f.sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
  });
});

brokerDescribe("E14.2 real broker / operator source is credential-bound", () => {
  for (const origin of ["max_deliver", "orphan-compatible relay (E15 remains report-only)"]) {
    it.skip(`T-RELAY-OP [deferred-by-E19: no relay publish grant] ${origin}: unknown, later owner ACK dropped, same-key caller retry reads real stored outcome`, { timeout: 90_000 }, async () => {
      const f = await broker(); f.operatorUser();
      const gate = latch(); f.releases.push(gate.release);
      f.start(async command => { await gate.promise; return { accepted: true, messageId: `relay-stored:${command.commandId}` }; });
      const pending = captured(f.request(`relay-${origin}`)), command = await f.command(); e14Event(command);
      await f.admissions.wait(() => true, "owner already admitted command");
      const refusal = relay(command); await f.publishAck(refusal, "operator");
      await f.consumed(refusal.id, f.senderCarrier);
      const error = (await pending.outcome).error;
      blocked(error instanceof Error && Reflect.get(error, "code") === "FABRIC_RELAY_DESTINATION_UNAVAILABLE",
        "A1 must recognize E14.2 broker-authenticated operatorMachine relay");
      expect(error).toMatchObject({ name: "FabricRelayDestinationUnavailableError", retryable: true });
      noDefiniteNonDelivery(error); expect(String(error)).toMatch(/outcome is unknown/i);
      gate.release(); const late = await f.ack(command.id);
      await f.consumed(late.id, f.senderCarrier); await f.consumed(command.id);
      expect(f.sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
      const saved = await stored(f, command);
      const retry = await f.request(`relay-${origin}`), next = await f.command(2);
      expect(next.id).not.toBe(command.id); expect(logical(next)).toBe(logical(command)); withinWindow(command, next);
      expect(retry).toMatchObject({ acknowledged: true, messageId: saved.acceptance!.messageId });
      await f.consumed(next.id); expect(f.admissions.items).toHaveLength(1);
    });
  }

  it("T-RELAY-FORGE broker: machine-a JWT can publish relay-shaped bytes but cannot gain operator authority; timeout stays unknown", { timeout: 100_000 }, async () => {
    const f = await broker(), gate = latch(); f.releases.push(gate.release);
    f.start(async () => { await gate.promise; return { accepted: true, messageId: "owner-only" }; });
    const pending = captured(f.request("relay-runtime-forge")), command = await f.command(); e14Event(command);
    await f.admissions.wait(() => true, "real owner handler");
    const forged = relay(command); await f.publishAck(forged, "machine-a");
    await f.consumed(forged.id, f.senderCarrier); expect(pending.done()).toBe(false);
    const error = (await pending.outcome).error; noDefiniteNonDelivery(error);
    expect(String(error)).toMatch(/outcome is unknown/i);
    expect(error).not.toHaveProperty("code", "FABRIC_RELAY_DESTINATION_UNAVAILABLE");
    gate.release(); await f.consumed(command.id);
    expect(f.admissions.items).toHaveLength(1);
  });

  it.skip("T-RELAY-ATTEMPT [deferred-by-E19: no relay publish grant]: delayed relay for timed-out attempt 1 cannot settle attempt 2; exact attempt 2 relay can", { timeout: 100_000 }, async () => {
    const f = await broker(); f.operatorUser();
    const gate = latch(); f.releases.push(gate.release);
    f.start(async command => { await gate.promise; return { accepted: true, messageId: `attempt-stored:${command.commandId}` }; });
    const first = captured(f.request("relay-attempt")), one = await f.command(); e14Event(one);
    await f.admissions.wait(() => true, "attempt one admitted");
    noDefiniteNonDelivery((await first.outcome).error);
    const second = captured(f.request("relay-attempt")), two = await f.command(2); e14Event(two);
    withinWindow(one, two); expect(two.id).not.toBe(one.id); expect(logical(two)).toBe(logical(one));
    const stale = relay(one); await f.publishAck(stale, "operator");
    await f.consumed(stale.id, f.senderCarrier); expect(second.done()).toBe(false);
    const valid = relay(two); await f.publishAck(valid, "operator"); await f.consumed(valid.id, f.senderCarrier);
    expect((await second.outcome).error).toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    gate.release(); await f.consumed(one.id); await f.consumed(two.id);
    const ownerAnswers = f.ownerCarrier.sent.items.filter(sent => sent.envelope.kind === "ack" && logical(sent) === logical(one));
    for (const ack of ownerAnswers) await f.consumed(ack.id, f.senderCarrier);
    expect(f.sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
    await expect(f.request("relay-attempt")).resolves.toMatchObject({ acknowledged: true, messageId: `attempt-stored:${logical(one)}` });
    expect(f.admissions.items).toHaveLength(1);
  });
});

describe("E16b / A2 config and B mint namespace public dependencies", () => {
  it("T-CONFIG-OPS-MISBIND: runtime machine/missing/invalid operator config is typed refusal; reserved operator config is accepted", () => {
    const config = (operatorMachine: unknown, machine = "machine-a") => ({ mesh: { enabled: true,
      transport: "nats", hostTransport: "nats", nats: { replicas: 1, servers: ["tls://localhost:4222"],
        custodyStream: "E14_CUSTODY", livenessStream: "E14_LIVE", pathBMachines: ["machine-a", "machine-b"],
        tls: { caFile: "/e14-fixture-not-opened/ca.pem" },
        credentials: "/e14-fixture-not-opened/runtime.creds", machine,
        ...(operatorMachine === undefined ? {} : { operatorMachine }) } } });
    for (const operator of ["fabric-ops", "fabric-ops-canary1"]) {
      const parsed = normalizeFabricConfig(config(operator));
      blocked(Reflect.get(parsed.mesh, "transport") === "nats" && Reflect.get(parsed.mesh.nats, "operatorMachine") === operator,
        "A2 E16b config parser must expose validated NATS operatorMachine, not silently discard the fields");
    }
    for (const [operator, machine] of [[undefined, "machine-a"], ["machine-a", "machine-a"], ["machine-b", "machine-a"],
      ["fabric-ops", "fabric-ops"], ["fabric-ops-", "machine-a"], ["fabric-ops-a-b", "machine-a"], ["FABRIC-OPS", "machine-a"]] as const) {
      let failure: unknown;
      try { normalizeFabricConfig(config(operator, machine)); } catch (error) { failure = error; }
      blocked(failure instanceof Error && typeof Reflect.get(failure, "code") === "string",
        "A2 E16b invalid/misbound operatorMachine must produce a TYPED config error");
    }
    // Forged relay from the runtime named in the bad config is exercised at the actual CP
    // callback in T-RELAY-FORGE (unit and real-JWT broker), not by a test-local validator.
  });

  it("T-OPS-MINT E16b: actual public minter cannot issue runtime reserved namespace or operator outside it", () => {
    const clock = { nowSeconds: () => 1_800_000_000 };
    const operator = createNatsOperator({ operatorName: "e14-fixture" }, clock);
    const text = (artifacts: readonly { name: string; bytes: Uint8Array }[], name: string): string => {
      const artifact = artifacts.find(item => item.name === name); blocked(artifact, "B mint fixture artifact API");
      return new TextDecoder().decode(artifact.bytes).trim(); // Memory only, NEVER printed or written.
    };
    const operatorJwt = text(operator.artifacts, "operator.jwt"), operatorSigningSeed = text(operator.artifacts, "operator-signing.nk");
    const attempt = (machine: string, role: "agent" | "operator") => {
      const pathBMachines = role === "agent" ? [machine] : ["machine-a", "machine-b"];
      const account = createNatsAccount({ name: "e14-account", operatorJwt, operatorSigningSeed,
        jetstream: { memStorage: 0, diskStorage: 4_294_967_296, streams: 64, consumers: 256 },
        custodyStream: "E14_CUSTODY", livenessStream: "E14_LIVE", pathBMachines }, clock);
      const common = { operatorJwt, accountPublicKey: account.metadata.account.publicKey,
        accountJwt: text(account.artifacts, "account.jwt"), signingSeed: text(account.artifacts, `signing-${role}.nk`),
        custodyStream: "E14_CUSTODY", livenessStream: "E14_LIVE", pathBMachines };
      return mintNatsUser(role === "agent" ? { ...common, role, host: machine, lane: "e14", session: "fixture",
        release: "candidate", meshes: ["a".repeat(32)] }
        : { ...common, role, machine, job: "migration", runId: "e14-fixture" }, clock);
    };
    // Healthy controls prevent an unrelated broken key/template setup from earning negative proof.
    expect(attempt("machine-a", "agent").metadata.role).toBe("agent");
    expect(attempt("fabric-ops", "operator").metadata.role).toBe("operator");
    for (const machine of ["fabric-ops", "fabric-ops-canary1"]) {
      let failure: unknown; try { attempt(machine, "agent"); } catch (error) { failure = error; }
      blocked(failure instanceof NatsMintError, "B E16b runtime mint/account inventory must refuse the entire reserved fabric-ops namespace");
    }
    for (const machine of ["machine-a", "ryzen1", "fabric-ops-a-b"]) {
      let failure: unknown; try { attempt(machine, "operator"); } catch (error) { failure = error; }
      blocked(failure instanceof NatsMintError, "B E16b messaging operator minter must refuse machine outside ^fabric-ops(-[a-z0-9]+)?$");
    }
  });
});

describe("E17 / frozen receipt API dependency gate", () => {
  it("T-DRAIN-RECEIPT-API: source and built public reader/writer/path exports are mandatory", async () => {
    await drainApi("source"); await drainApi("dist");
  });
});

describe("E22/E17 RECORDED-CALL / SAFE local public CP predicates (NOT resident-root release proof)", () => {
  it("T-E22-RELEASE RECORDED-CALL [SAFE local]: FILE event-sequence/ACK evidence survives NATS flip and FILE rollback; NATS outcomes still need fresh 0/0 receipt", { timeout: 15_000 }, async () => {
    const { scene: n } = stubScene();
    dispose.push(async () => { await n.close(); fs.rmSync(n.root, { recursive: true, force: true }); });
    const root = n.ownerMesh.root, f = files(root);
    // Generate ALL command/ACK/outcome evidence through real FILE CPs first.
    f.start(); await f.request("e22-original"); const first = await f.command();
    const fileId = (first.data as { commandId: string }).commandId;
    const fileRecord = readSeen(root, fileId)!;
    expect(fileRecord).not.toHaveProperty("carrier"); expect(fileRecord.sequence).toBe(first.sequence);
    await f.request("e22-original"); const repeat = await f.command(2);
    expect((repeat.data as { commandId: string }).commandId).toBe(fileId);
    expect(repeat.sequence).not.toBe(first.sequence); // Same logical ID is NOT original input-sequence evidence.
    await f.request("e22-independent"); const independent = await f.command(3);
    expect((independent.data as { commandId: string }).commandId).not.toBe(fileId);
    await f.close();
    // Install AFTER FileScene retires its own prototype spies, so its teardown
    // cannot remove this guard before the release predicates are exercised.
    const deletions = recordControlDeletes(root);
    // Actual NATS CP, same local seen-store identity/root; carrier-only stub,
    // not a factory/JWT/host-config/A2 composition claim.
    n.start(); await n.request("e22-nats"); const sent = await n.command();
    const outcome = await stored(n, sent); n.owner.pause();
    expect(await n.ownerCarrier.transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    const rollback = new FabricControlPlane(n.ownerMesh, identity(receiverOwner.hostId), {
      enabled: true, hostId: receiverOwner.hostId, ownerIncarnation: ownerEpoch, pollMs: 20,
      controlIncarnationFence: "enforce", withdrawHoldUntil: Date.now() + HOLD_MS,
      expectedNatsInboxStream: `FABRIC_IN_${receiverOwner.machine}`,
    });
    rollback.pause(); dispose.push(() => rollback.close());
    const planes = [n.owner, rollback];
    expect(planes.map(plane => plane.transportKind)).toEqual(["nats", "file"]);
    // A clean in-memory/broker drain is not an E17 persisted last-write receipt.
    for (const plane of planes) await expect(plane.checkpointForRelease()).rejects.toThrow(/unproven drain receipt|retain A/i);
    const directory = seenRoot(root, receiverOwner.hostId), writtenAt = lastWrite([outcome]);
    const built = await drainApi("dist");
    const complete = receiptFor([outcome], { flippedAt: writtenAt, completedAt: writtenAt });
    expect(complete.consumer).toMatchObject({ numPending: 0, numAckPending: 0 });
    expect(complete.dispositions).toEqual([]); // E19/E20: no canary relay/disposition shortcut.
    for (const [label, patch] of [
      ["foreign-host", { hostId: "session:foreign-host" }],
      ["incomplete", { complete: false }],
      ["stale", { flippedAt: writtenAt - 1, completedAt: writtenAt - 1 }],
    ] as const) {
      built.writeNatsDrainReceipt(directory, { ...complete, ...patch });
      for (const plane of planes) await expect(plane.checkpointForRelease(), label).rejects.toThrow(/unproven drain receipt|retain A/i);
    }
    built.writeNatsDrainReceipt(directory, complete); // Equality covers the last real outcome write.
    let omitted: "none" | "original-input" | "matching-acks" = "none";
    const tail = n.ownerMesh.tail.bind(n.ownerMesh);
    vi.spyOn(n.ownerMesh, "tail").mockImplementation((...args) => {
      const page = tail(...args);
      // Disk-read fault stimulus ONLY: omit original persisted events, never rewrite
      // events.jsonl, invent an ACK/claim, or substitute a release-state predicate.
      return { ...page, events: page.events.filter(event => omitted === "original-input" ? event.id !== first.id
        : omitted === "matching-acks" ? event.topic !== "fabric.control.ack" ||
          (event.data as { commandId?: string } | undefined)?.commandId !== fileId : true) };
    });
    for (const plane of planes) {
      omitted = "original-input";
      // The REAL repeat with the same logical id but another event sequence remains.
      await expect(plane.checkpointForRelease()).rejects.toThrow(/input disk readback.*unavailable|retain A/i);
      omitted = "matching-acks";
      // Real independent command/ACK evidence remains; it cannot cover fileId.
      await expect(plane.checkpointForRelease()).rejects.toThrow(/ACK disk readback.*unavailable|retain A/i);
      omitted = "none";
      await expect(plane.checkpointForRelease()).resolves.toBeUndefined();
      expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    }
    expect(readSeen(root, fileId)).toEqual(fileRecord); expect(readSeen(root, outcome.commandId)).toEqual(outcome);
    expect(deletions.items).toEqual([]); // Exact recorded product deletion operations: NONE at checkpoint.
    // This permits a CP checkpoint receipt, NOT physical release/deletion of a resident root.
    // The following probe positively exercises the guard with a real cleanup request.
  });

  it("T-ROLLBACK-CLEANUP RECORDED-CALL [SAFE local]: public FILE drain requests exact expired NATS CAS deletion but performs no deletion; FILE log record retained", { timeout: 15_000 }, async () => {
    const { scene: n } = stubScene();
    dispose.push(async () => { await n.close(); fs.rmSync(n.root, { recursive: true, force: true }); });
    n.start(); await n.request("recorded-cleanup-nats"); const sent = await n.command();
    const outcome = await stored(n, sent); await n.retire();
    const root = n.ownerMesh.root, f = files(root);
    f.start(); await f.request("recorded-cleanup-file"); const event = await f.command();
    const fileId = (event.data as { commandId: string }).commandId, fileRecord = readSeen(root, fileId)!;
    expect(fileRecord).not.toHaveProperty("carrier"); await f.close();
    const local = seenStore(root), key = seenKey(receiverOwner.hostId, outcome.commandId);
    const version = local.get(key, { fresh: true })!.version; local.closeState();
    const now = Math.max(outcome.expiresAt, fileRecord.expiresAt) + 1;
    vi.spyOn(Date, "now").mockReturnValue(now);
    const sweep = files(root), deletions = recordControlDeletes(root, () => sweep.owner.pause());
    sweep.start(); // REAL public FILE drain -> #cleanupSeen -> writeBatch, never a private-policy call.
    const requested = await deletions.wait(call => call.root === seenRoot(root, receiverOwner.hostId), "actual withheld cleanup deletion batch");
    await sweep.owner.close(); // Join the paused drain; no retry or successful deletion receipt.
    expect(requested.input.identity).toEqual(identity(receiverOwner.hostId));
    expect(requested.input.ops).toEqual([{ kind: "delete", key, ifVersion: version, onConflict: "skip" }]);
    expect(deletions.items).toHaveLength(1);
    expect(readSeen(root, outcome.commandId)).toEqual(outcome); expect(readSeen(root, fileId)).toEqual(fileRecord);
    expect(fileRecord.expiresAt).toBeLessThan(now); expect(sweep.mesh.oldestSequence()).toBeLessThanOrEqual(fileRecord.sequence!);
    // RECORDED-CALL only: the actual delete was BLOCKED, not a local cleanup PASS.
  });
});

describe("E14-X1 / actual public FILE withdraw, release and cleanup", () => {
  it("T-E14-FILEONLY withdraw predicate: absent hold preserves #6729 definite non-delivery and prevents later execution", { timeout: 15_000 }, async () => {
    const f = files(), pending = captured(f.request("file-withdraw"));
    const event = await f.command(), command = event.data as { commandId: string };
    const error = (await pending.outcome).error;
    expect(error).toMatchObject({ code: "FABRIC_CONTROL_NOT_DELIVERED", notDelivered: true });
    expect(f.mesh.get(seenKey(receiverOwner.hostId, command.commandId), { fresh: true })?.value).toMatchObject({
      acceptance: { accepted: false, notRun: true, error: "Fabric control command expired" },
    });
    f.start(); await f.events.wait(ack => ack.topic === "fabric.control.ack" &&
      Reflect.get(ack.data as object, "commandId") === command.commandId, "withdrawn FILE command refused on resume");
    expect(f.admissions.items).toEqual([]);
  });

  it("T-E14-FILEONLY release predicate: no-carrier real outcome needs its command AND correlated ACK; missing ACK retains A", { timeout: 15_000 }, async () => {
    const f = files(); f.start();
    await f.request("file-release"); const command = await f.command();
    const id = (command.data as { commandId: string }).commandId;
    const saved = readSeen(f.root, id)!; expect(saved).not.toHaveProperty("carrier");
    const readback = await f.readbackPlane(); dispose.push(() => readback.close());
    await expect(readback.checkpointForRelease()).resolves.toBeUndefined();
    expect(readback.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    // Actual disk evidence removed, not a replacement release-state predicate.
    await f.mesh.exclusive(() => {
      const file = path.join(f.root, "events.jsonl");
      const lines = fs.readFileSync(file, "utf8").split("\n").filter(Boolean);
      const retained = lines.filter(line => {
        const event = JSON.parse(line) as { topic: string; data?: { commandId?: string } };
        return event.topic !== "fabric.control.ack" || event.data?.commandId !== id;
      });
      expect(retained.length).toBeLessThan(lines.length);
      fs.writeFileSync(file, `${retained.join("\n")}\n`);
    });
    await expect(readback.checkpointForRelease()).rejects.toThrow(/ACK disk readback.*unavailable|retain A/i);
  });

  it("T-E14-FILEONLY cleanup predicate table: no carrier retains the e931dbc8 expiry + file-log sequence decisions", { timeout: 15_000 }, async () => {
    const f = files(), now = Date.now();
    await f.mesh.publish({ topic: "e14.floor", from: identity("fixture"), text: "oldest retained event" });
    const local = seenStore(f.root); dispose.push(async () => { local.closeState(); });
    const oldest = f.mesh.oldestSequence()!;
    const rows = [
      { id: "expired-before-log", expiresAt: now - 1, sequence: oldest - 1, remove: true },
      { id: "expired-in-log", expiresAt: now - 1, sequence: oldest, remove: false },
      { id: "expired-without-sequence", expiresAt: now - 1, remove: false },
      { id: "unexpired-before-log", expiresAt: now + HOUR, sequence: oldest - 1, remove: false },
    ];
    for (const row of rows) await local.put({ key: seenKey(receiverOwner.hostId, row.id), identity: identity(receiverOwner.hostId), value: {
      format: 1, hostId: receiverOwner.hostId, commandId: row.id, targetId: receiverOwner.hostId,
      expiresAt: row.expiresAt, ...(row.sequence === undefined ? {} : { sequence: row.sequence }),
      acceptance: { accepted: true, messageId: row.id },
    } });
    f.start(); await f.deleted.wait(key => key === seenKey(receiverOwner.hostId, rows[0]!.id), "actual FILE cleanup commit");
    f.owner.pause();
    // checkpoint joins the owned drain even though synthetic table rows intentionally lack ACK evidence.
    await f.owner.checkpointForRelease().catch(() => undefined);
    for (const row of rows) expect(local.get(seenKey(receiverOwner.hostId, row.id), { fresh: true }) === undefined, row.id).toBe(row.remove);
  });

  brokerIt("T-ROLLBACK-WITHDRAW: real NATS outcome, same-root FILE flip + atomic 48h hold, timeout unknown, stored answer once", { timeout: 90_000 }, async () => {
    const n = await broker(); n.start();
    const initial = await n.request("rollback-withdraw"), sent = await n.command(); e14Event(sent);
    const saved = await stored(n, sent);
    expect(await n.ownerCarrier.transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    await n.retire();
    const flipAt = Date.now(); await writeDrain(n.ownerMesh.root, receiptFor([saved], { flippedAt: flipAt }));
    const flip = writeRollbackFlip(n.ownerMesh.root, flipAt);
    expect(flip.holdUntil - flipAt).toBe(HOLD_MS);
    const f = files(n.ownerMesh.root, flip.holdUntil); // SAME root, host ids, epoch, logical key.
    const unknown = captured(f.request("rollback-withdraw")), event = await f.command();
    expect((event.data as { commandId: string }).commandId).toBe(logical(sent));
    const error = (await unknown.outcome).error;
    blocked(!(error instanceof Error && Reflect.get(error, "code") === "FABRIC_CONTROL_NOT_DELIVERED"),
      "A1 E14.3 withdrawHoldUntil must suppress shared-claim withdrawal after rollback");
    noDefiniteNonDelivery(error); expect(String(error)).toMatch(/outcome is unknown/i);
    expect(f.mesh.get(seenKey(receiverOwner.hostId, logical(sent)), { fresh: true })).toBeUndefined();
    expect(readSeen(f.root, logical(sent))?.acceptance).toEqual(saved.acceptance);
    f.start();
    await f.events.wait(ack => ack.topic === "fabric.control.ack" &&
      (ack.data as { commandId?: string }).commandId === logical(sent), "FILE late stored-outcome ACK");
    expect(await f.request("rollback-withdraw")).toEqual(initial);
    expect(f.admissions.items).toEqual([]); expect(n.admissions.items).toHaveLength(1);
    // The hold is a full 48h bound, not merely the old record's remaining 24h horizon.
    f.owner.pause(); await f.owner.checkpointForRelease();
    let clock = flip.holdUntil - 1;
    vi.spyOn(Date, "now").mockImplementation(() => clock);
    const beforeEnd = (await captured(f.request("fresh-just-before-hold-end")).outcome).error;
    noDefiniteNonDelivery(beforeEnd); expect(String(beforeEnd)).toMatch(/outcome is unknown/i);
    clock = flip.holdUntil;
    const atEnd = (await captured(f.request("fresh-at-hold-end")).outcome).error;
    expect(atEnd).toMatchObject({ code: "FABRIC_CONTROL_NOT_DELIVERED", notDelivered: true });
  });

  brokerIt("T-ROLLBACK-RELEASE E16c/E17: generated NATS outcome + interrupted claim RETAIN absent/partial receipt, release only with complete durable receipt", { timeout: 90_000 }, async () => {
    const n = await broker(); n.start();
    await n.request("release-nats-outcome"); const sent = await n.command(); e14Event(sent);
    const outcome = await stored(n, sent), claim = await n.claimWithoutOutcome();
    const complete = await receiptFromDrain([outcome, claim], n.ownerCarrier.transport);
    await n.retire();
    expect(outcome).toMatchObject({ carrier: "nats", acceptance: { accepted: true } });
    expect(claim).toMatchObject({ carrier: "nats" }); expect(claim).not.toHaveProperty("acceptance");
    const flip = writeRollbackFlip(n.ownerMesh.root), f = files(n.ownerMesh.root, flip.holdUntil);
    // No receipt: NATS records are NOT an unconditional release bypass (E16c).
    f.owner.pause();
    await expect(f.owner.checkpointForRelease()).rejects.toMatchObject({ code: "FABRIC_NATS_DRAIN_UNPROVEN" });
    await writeDrain(f.root, { ...complete, complete: false });
    await expect(f.owner.checkpointForRelease()).rejects.toMatchObject({ code: "FABRIC_NATS_DRAIN_UNPROVEN" });
    await writeDrain(f.root, { ...complete,
      consumer: { stream: "FABRIC_IN_machine-b", numPending: 0, numAckPending: 1 },
      dispositions: [{ commandEventId: claim.commandId, disposition: "relay" }] });
    await expect(f.owner.checkpointForRelease()).rejects.toMatchObject({ code: "FABRIC_NATS_DRAIN_UNPROVEN" });
    // Canary rollback uses actual sampled 0/0 with no disposition shortcut.
    await writeDrain(f.root, complete);
    try { await f.owner.checkpointForRelease(); }
    catch (cause) { throw new Error("BLOCKED-ON-SOURCE: E17 complete correct-host drain receipt must cover NATS records through writtenAt", { cause }); }
    expect(f.owner.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    expect(readSeen(f.root, claim.commandId)).toEqual(claim); // No fabricated outcome to make release pass.
    expect(f.mesh.read({ topic: "fabric.control.command" })).toEqual([]);
    expect(f.mesh.read({ topic: "fabric.control.ack" })).toEqual([]);
    // Contrast with a real FILE-era record on the same root: ACK removal must still retain A.
    f.start(); await f.request("release-file-outcome"); const event = await f.command();
    const fileId = (event.data as { commandId: string }).commandId;
    const fileRecord = readSeen(f.root, fileId)!; expect(fileRecord).not.toHaveProperty("carrier");
    const readback = await f.readbackPlane(); dispose.push(() => readback.close());
    await expect(readback.checkpointForRelease()).resolves.toBeUndefined();
    await f.mesh.exclusive(() => {
      const file = path.join(f.root, "events.jsonl");
      const lines = fs.readFileSync(file, "utf8").split("\n").filter(Boolean);
      const kept = lines.filter(line => {
        const item = JSON.parse(line) as { topic: string; data?: { commandId?: string } };
        return item.topic !== "fabric.control.ack" || item.data?.commandId !== fileId;
      });
      expect(kept.length).toBeLessThan(lines.length); fs.writeFileSync(file, `${kept.join("\n")}\n`);
    });
    await expect(readback.checkpointForRelease()).rejects.toThrow(/ACK disk readback.*unavailable|retain A/i);
  });

  brokerIt("T-DRAIN-RECEIPT E17: absent/torn/invalid/foreign-host/incomplete/stale proof retains generated claims; complete covering proof releases", { timeout: 90_000 }, async () => {
    const n = await broker(); n.start();
    await n.request("drain-outcome"); const sent = await n.command(); e14Event(sent);
    const outcome = await stored(n, sent), claim = await n.claimWithoutOutcome();
    const records = [outcome, claim], complete = await receiptFromDrain(records, n.ownerCarrier.transport);
    await n.retire();
    const writtenAt = lastWrite(records), root = n.ownerMesh.root;
    const source = await drainApi("source"), built = await drainApi("dist");
    const local = seenStore(root); const directory = local.root; local.closeState();
    const file = built.natsDrainReceiptPath(directory);
    expect(file).toBe(path.join(directory, "nats-drain-receipt.json"));
    expect(source.natsDrainReceiptPath(directory)).toBe(file);
    const f = files(root, writeRollbackFlip(root).holdUntil); f.owner.pause();
    const variants: Array<{ label: string; corrupt?: string; patch?: Record<string, unknown> }> = [
      { label: "absent" },
      { label: "torn", corrupt: '{"version":1,"hostId":' },
      { label: "invalid-json", corrupt: "this is not JSON" },
      { label: "invalid-schema", patch: { consumer: { stream: "FABRIC_IN_machine-b", numPending: "zero", numAckPending: 0 } } },
      { label: "unknown-version", patch: { version: 2 } },
      { label: "foreign-host", patch: { hostId: "session:foreign-host" } },
      { label: "incomplete", patch: { complete: false, consumer: { stream: "FABRIC_IN_machine-b", numPending: 1, numAckPending: 1 }, dispositions: [] } },
      { label: "stale", patch: { flippedAt: writtenAt - 2, completedAt: writtenAt - 1 } },
      { label: "null-consumer-dispositions", patch: { consumer: null,
        dispositions: [{ commandEventId: claim.commandId, disposition: "relay" }] } },
      { label: "ack-pending-disposition", patch: {
        consumer: { stream: "FABRIC_IN_machine-b", numPending: 0, numAckPending: 1 },
        dispositions: [{ commandEventId: claim.commandId, disposition: "relay" }] } },
      { label: "zero-with-disposition", patch: { dispositions: [{ commandEventId: claim.commandId, disposition: "completed" }] } },
      { label: "wrong-stream", patch: { consumer: { stream: "FABRIC_IN_machine-a", numPending: 0, numAckPending: 0 } } },
      { label: "reversed-timeline", patch: { flippedAt: complete.completedAt + 1 } },
      { label: "future-timeline", patch: { flippedAt: Date.now() + HOUR, completedAt: Date.now() + HOUR } },
    ];
    for (const variant of variants) {
      // Legitimate receipt setup ALWAYS uses the built writer. Raw writes ONLY inject on-disk
      // corruption/unknown schema, never stand in for the production atomic+fsync writer.
      built.writeNatsDrainReceipt(directory, complete);
      if (variant.label === "absent") fs.rmSync(file);
      else if (variant.corrupt !== undefined) fs.writeFileSync(file, variant.corrupt);
      else if (["invalid-schema", "unknown-version"].includes(variant.label)) fs.writeFileSync(file, JSON.stringify({ ...complete, ...variant.patch }));
      else built.writeNatsDrainReceipt(directory, { ...complete, ...variant.patch });
      if (["absent", "torn", "invalid-json", "invalid-schema", "unknown-version"].includes(variant.label)) {
        expect(source.readNatsDrainReceipt(directory), variant.label).toBeUndefined();
      }
      await expect(f.owner.checkpointForRelease(), variant.label).rejects.toMatchObject({ code: "FABRIC_NATS_DRAIN_UNPROVEN" });
      expect(() => f.owner.releaseCheckpointReceipt(), variant.label).toThrow();
      expect(readSeen(root, claim.commandId)).toEqual(claim);
    }
    // Equality is authorized: complete completedAt === record's latest writtenAt covers it.
    const equality = { ...complete, flippedAt: writtenAt, completedAt: writtenAt };
    built.writeNatsDrainReceipt(directory, equality);
    expect(source.readNatsDrainReceipt(directory)).toEqual(equality);
    await expect(f.owner.checkpointForRelease()).resolves.toBeUndefined();
    expect(f.owner.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    expect(readSeen(root, claim.commandId)).toEqual(claim);
  });

  brokerIt("T-ROLLBACK-CLEANUP: real NATS records expire without FILE sequence; file-era records retain their log rule", { timeout: 90_000 }, async () => {
    const n = await broker(); n.start();
    await n.request("cleanup-nats-outcome"); const sent = await n.command(); e14Event(sent);
    const outcome = await stored(n, sent), claim = await n.claimWithoutOutcome();
    const complete = await receiptFromDrain([outcome, claim], n.ownerCarrier.transport);
    await n.retire();
    await writeDrain(n.ownerMesh.root, complete);
    const flip = writeRollbackFlip(n.ownerMesh.root), f = files(n.ownerMesh.root, flip.holdUntil);
    // Generate a genuine file-era outcome, not a seeded acceptance/claim substitute.
    f.start(); await f.request("cleanup-file-outcome"); const fileEvent = await f.command();
    const fileId = (fileEvent.data as { commandId: string }).commandId;
    const fileRecord = readSeen(f.root, fileId)!; expect(fileRecord).not.toHaveProperty("carrier");
    f.owner.pause(); await f.owner.checkpointForRelease(); await f.close();
    const expiry = Math.max(outcome.expiresAt, claim.expiresAt), local = seenStore(f.root);
    dispose.push(async () => { local.closeState(); });
    // Boundary = strictly expiresAt < now. At equality, the last NATS record stays.
    let clock = expiry; vi.spyOn(Date, "now").mockImplementation(() => clock);
    const control = new FileScene(f.root, flip.holdUntil); dispose.push(() => control.close());
    control.start();
    // A fresh FILE request is a real drain/ACK barrier, not a direct call of #cleanupSeen.
    await control.request("cleanup-at-equality"); control.owner.pause();
    await control.owner.checkpointForRelease();
    const lastId = outcome.expiresAt >= claim.expiresAt ? outcome.commandId : claim.commandId;
    expect(local.get(seenKey(receiverOwner.hostId, lastId), { fresh: true })).toBeDefined();
    await control.close();
    clock = expiry + 1_000;
    const sweep = new FileScene(f.root, flip.holdUntil); dispose.push(() => sweep.close());
    sweep.start();
    const toRemove = [outcome, claim].filter(record => local.get(seenKey(receiverOwner.hostId, record.commandId), { fresh: true }));
    const deleted = toRemove.map(record => sweep.deleted.wait(key => key === seenKey(receiverOwner.hostId, record.commandId), "NATS expiry cleanup through FILE drain"));
    try { await Promise.all(deleted); }
    catch (cause) { throw new Error("BLOCKED-ON-SOURCE: E14.3 FILE cleanup must expire carrier:nats without sequence/log evidence", { cause }); }
    sweep.owner.pause(); await sweep.owner.checkpointForRelease();
    expect(local.get(seenKey(receiverOwner.hostId, outcome.commandId), { fresh: true })).toBeUndefined();
    expect(local.get(seenKey(receiverOwner.hostId, claim.commandId), { fresh: true })).toBeUndefined();
    expect(local.get(seenKey(receiverOwner.hostId, fileId), { fresh: true })?.value).toEqual(fileRecord);
    expect(fileRecord.expiresAt).toBeLessThan(clock); // Expired, but its command is STILL in the FILE log.
    expect(sweep.mesh.oldestSequence()).toBeLessThanOrEqual(fileRecord.sequence!);
  });
});
