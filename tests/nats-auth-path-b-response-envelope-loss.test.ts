// expected-skip: no NATS_BIN/PATH executable, non-Linux host, or no A' link entry in this checkout.
// Keep reason: authenticated wire/fault trust boundary and a failure path that needs a real broker.
// COMPONENT counter-test ONLY: mixed harness UI/model delivery is a fixture, not native/frozen/app
// commit, business dedupe, reconstruction or N8 acceptance. All commands and response ACKs below
// come from real public agents.followUp/control calls; actual transport.send is record-only wrapped
// by the existing harness, never replaced with a fabricated success or a manufactured ASK/ACK.
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { JetStreamManager } from "@nats-io/jetstream";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { transportInboxStream, transportInboxSubject } from "../src/transport/nats/subjects.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import { openJetStream, resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, E2E_WAIT_MS, NATS_MAIN_MACHINE, natsLinkPresent, waitForValue,
  type MixedHarness, type MixedMain, type WireEvent,
} from "./helpers/nats-file-mixed.js";
import type { FrameMatch } from "./helpers/nats-wire-faults.js";

const binary = resolveNatsBinary(), linked = natsLinkPresent();
const supported = !!binary && linked && process.platform === "linux";
if (!supported && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: response-envelope counter-test needs Linux, NATS_BIN and the A' link");
const INBOX = transportInboxStream(NATS_MAIN_MACHINE);
// A finite negative-delivery observation, not a retry scheduler or a changed product timeout.
const OBSERVATION_MS = 500;
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
const mentions = (main: MixedMain, token: string) => main.delivered().filter(row => row.text.includes(token));
const outcome = <T>(work: Promise<T>) => work.then(value => ({ value }), (error: unknown) => ({ error: String(error) }));
const decode = (body: string) => JSON.parse(body) as { format: number; sourceMachine: string; envelope: TransportEnvelope<Record<string, unknown>> };

describe.skipIf(!supported)("response envelope loss is not broker PubAck loss (real broker component)", () => {
  let h: MixedHarness, manager: JetStreamManager, ackSubject: string;
  beforeAll(async () => {
    h = await createMixedHarness({ binary: binary! });
    ackSubject = transportInboxSubject(NATS_MAIN_MACHINE, h.natsMain.id, "ack", h.fileMain.id, BRIDGE_MACHINE);
    // Runtime credentials intentionally cannot core-subscribe to fabric.in or read stored
    // inbox bodies. The harness's own authenticated bootstrap client can inspect real storage;
    // it never fabricates an envelope or consumes/ACKs the Main's JetStream deliveries.
    manager = (await openJetStream(await h.broker.connect(h.broker.users.bootstrap))).manager;
  }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  const sourceFor = async (mark: number, token: string): Promise<WireEvent> => {
    const commands = () => h.wire.since(mark).filter(event => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token);
    await h.wire.until(() => commands().length > 0, "actual source command publish confirmation", E2E_WAIT_MS);
    expect(commands()).toHaveLength(1);
    return commands()[0]!;
  };
  const responsesFor = (mark: number, commandId: unknown, phase: WireEvent["phase"]) => h.wire.since(mark).filter(event =>
    event.phase === phase && event.envelope.kind === "ack" && bodyOf(event).commandId === commandId);
  const complete = async (mark: number, token: string) => {
    const source = await sourceFor(mark, token), commandId = bodyOf(source).commandId;
    await h.wire.until(() => responsesFor(mark, commandId, "send").length === 1 &&
      responsesFor(mark, commandId, "broker-ack").length === 1, "actual response send and receiver broker ACK", E2E_WAIT_MS);
    const sent = responsesFor(mark, commandId, "send")[0]!;
    const deliveries = responsesFor(mark, commandId, "deliver");
    expect(sent.machine).toBe(BRIDGE_MACHINE);
    expect(sent.envelope.id).toBe(`ack:${source.envelope.id}`);
    expect(sent.envelope).toMatchObject({ kind: "ack", operation: "accepted", targetId: h.fileMain.id });
    expect(bodyOf(sent)).toMatchObject({ version: 1, commandId, accepted: true });
    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]).toMatchObject({ hostId: h.natsMain.id, attempt: 1, sequence: sent.sequence });
    expect(mentions(h.fileMain, token)).toHaveLength(1);
    expect(h.fileEvents({ topic: "fabric.control.command" }).filter(event =>
      (event.data as Record<string, unknown>)?.message === token)).toHaveLength(1);
    const stored = await manager.streams.getMessage(INBOX, { seq: sent.sequence! });
    expect(stored).not.toBeNull();
    expect(decode(stored!.string()).envelope).toMatchObject({ id: sent.envelope.id, kind: "ack", body: { commandId } });
    return { sent, commandId };
  };

  it("lost PubAck still stores/delivers the genuine response; lost response does not, then clear/exhaustion recover", async () => {
    try {
      for (const count of [0, -1, 1.5, NaN, Infinity, 1_001, Number.MAX_SAFE_INTEGER]) {
        expect(() => h.faults.loseResponseEnvelopes(() => true, count)).toThrow("Response envelope drop count");
      }
      const ackDrops = h.faults.ackDrops, pubAckDrops = h.faults.pubAckDrops, responseDrops = h.faults.responseEnvelopeDrops;
      const responseToCaller: FrameMatch = frame => frame.subject === ackSubject;
      const pubToken = `RESPONSE_PUBACK_${randomUUID()}`, pubMark = h.wire.mark();
      h.faults.losePubAcks(responseToCaller);
      await expect(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: pubToken, idempotencyKey: randomUUID() }))
        .resolves.toMatchObject({ acknowledged: true });
      await waitForValue(() => h.faults.pubAckDrops === pubAckDrops + 1 ? true : undefined, "actual broker PubAck dropped", E2E_WAIT_MS);
      // No fake TransportSent: wait for actual.send's normal same-id retry to receive a real
      // PubAck. The broker had already stored and delivered the first response envelope.
      const pub = await complete(pubMark, pubToken);
      expect(pub.sent.duplicate).toBe(true);
      expect(h.faults.responseEnvelopeDrops).toBe(responseDrops);
      expect(h.faults.ackDrops).toBe(ackDrops);

      // A deliberately broad response matcher must still let an outbound COMMAND through the
      // relay and its genuine returning (inbound) application ACK/+ACK path complete normally.
      h.faults.loseResponseEnvelopes(() => true, 1_000);
      const unrelated = `UNRELATED_COMMAND_${randomUUID()}`;
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: unrelated, idempotencyKey: randomUUID() }))
        .resolves.toMatchObject({ acknowledged: true });
      expect(mentions(h.natsMain, unrelated)).toHaveLength(1);
      expect(h.faults.responseEnvelopeDrops).toBe(responseDrops);

      const lostToken = `RESPONSE_CLEAR_${randomUUID()}`, lostMark = h.wire.mark();
      const lastBefore = (await manager.streams.info(INBOX)).state.last_seq;
      let settled = false;
      const pending = outcome(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: lostToken, idempotencyKey: randomUUID() }))
        .then(result => { settled = true; return result; });
      await waitForValue(() => h.faults.responseEnvelopeDrops > responseDrops ? true : undefined, "actual outbound response suppressed", E2E_WAIT_MS);
      const dropped = h.faults.droppedResponseEnvelopes.at(-1)!;
      const wire = decode(dropped.body), source = await sourceFor(lostMark, lostToken), commandId = bodyOf(source).commandId;
      expect(dropped.subject).toBe(ackSubject);
      expect(h.faults.inboxFrames).toContain(dropped);
      expect(wire).toMatchObject({ format: 1, sourceMachine: BRIDGE_MACHINE,
        envelope: { id: `ack:${source.envelope.id}`, kind: "ack", operation: "accepted", body: { commandId, accepted: true } } });
      expect(mentions(h.fileMain, lostToken)).toHaveLength(1); // real command consumed, genuine ACK produced
      // The active bounded rule suppresses retries too. No inferred loss from a thrown send or
      // missing UI message: check real broker stream state and actual receiver delivery.
      await delay(OBSERVATION_MS);
      expect(settled).toBe(false);
      expect((await manager.streams.info(INBOX)).state.last_seq).toBe(lastBefore);
      expect(responsesFor(lostMark, commandId, "deliver")).toEqual([]);
      expect(responsesFor(lostMark, commandId, "broker-ack")).toEqual([]);
      expect(h.faults.pubAckDrops).toBe(pubAckDrops + 1); // no PubAck exists for the suppressed publish
      expect(h.faults.ackDrops).toBe(ackDrops);
      const clearedDropCount = h.faults.responseEnvelopeDrops;
      expect(clearedDropCount).toBeGreaterThan(responseDrops);
      expect(clearedDropCount - responseDrops).toBeLessThan(1_000); // still pending, not exhausted
      h.faults.clearFaults(); // removes the remaining drops, without erasing evidence
      expect(h.faults.responseEnvelopeDrops).toBe(clearedDropCount);
      expect(h.faults.droppedResponseEnvelopes).toContain(dropped);
      expect(await pending).toMatchObject({ value: { acknowledged: true } });
      const cleared = await complete(lostMark, lostToken);
      expect(cleared.sent.duplicate).toBe(false); // first actual broker storage is AFTER clear
      expect(h.faults.responseEnvelopeDrops).toBe(clearedDropCount);

      // Correlate a counted rule using the real synthetic payload, and let it exhaust WITHOUT
      // clearFaults. No protocol command or ACK is manufactured: latch the next genuine response.
      let selectedCommandId: unknown;
      const selected: FrameMatch = frame => {
        if (frame.subject !== ackSubject) return false;
        const response = decode(frame.body.toString("utf8")).envelope;
        if (response.kind !== "ack" || typeof response.body !== "object" || response.body === null) return false;
        selectedCommandId ??= response.body.commandId;
        return response.body.commandId === selectedCommandId;
      };
      const exhaustedToken = `RESPONSE_EXHAUST_${randomUUID()}`, exhaustedMark = h.wire.mark();
      h.faults.loseResponseEnvelopes(selected, 1);
      await expect(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: exhaustedToken, idempotencyKey: randomUUID() }))
        .resolves.toMatchObject({ acknowledged: true });
      const exhausted = await complete(exhaustedMark, exhaustedToken);
      expect(selectedCommandId).toBe(exhausted.commandId);
      expect(exhausted.sent.duplicate).toBe(false);
      expect(h.faults.responseEnvelopeDrops).toBe(clearedDropCount + 1);
      const exhaustedDrop = h.faults.droppedResponseEnvelopes.at(-1)!;
      expect(exhaustedDrop.subject).toBe(ackSubject);
      expect(decode(exhaustedDrop.body).envelope.id).toBe(exhausted.sent.envelope.id);
      expect(h.faults.pubAckDrops).toBe(pubAckDrops + 1);
      expect(h.faults.ackDrops).toBe(ackDrops);
      const recoveryToken = `RESPONSE_RECOVERED_${randomUUID()}`, recoveryMark = h.wire.mark();
      await expect(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: recoveryToken, idempotencyKey: randomUUID() }))
        .resolves.toMatchObject({ acknowledged: true });
      await complete(recoveryMark, recoveryToken);
      expect(h.faults.responseEnvelopeDrops).toBe(clearedDropCount + 1);
    } finally { h.faults.clearFaults(); }
  }, E2E_WAIT_MS * 3);
});
