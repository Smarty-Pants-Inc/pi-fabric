// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' exactly-once across the hop on a real broker. Every fault is real and hits
// the link only, through its TLS wire relay: a forced disconnect (outage), a lost PubAck (the broker stored the
// command, the link cannot know), a lost broker +ACK (the link appended, the broker redelivers after the real
// 30 s AckWait) across a link restart, and a link restart with a command in flight. Each ends with exactly one
// consumption and the sender's ACK. Official assumptions: AckWait is a redelivery deadline and Nats-Msg-Id
// dedupe is bounded by the stream window (https://docs.nats.io/nats-concepts/jetstream/consumers,
// https://docs.nats.io/nats-concepts/jetstream/headers). Only the Pi UI/model boundary is a fixture.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { MeshEvent } from "../src/mesh/store.js";
import { transportInboxStream } from "../src/transport/nats/subjects.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, linkTail, NATS_MAIN_MACHINE, natsLinkPresent, waitForValue,
  type MixedHarness, type MixedMain, type WireEvent,
} from "./helpers/nats-file-mixed.js";
import { parseInboxSubject, publishTo } from "./helpers/nats-wire-faults.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
if (!linked) console.warn("A' bridge fault E2E skipped: src/mesh/bridge-nats-link.ts (lane L2) is not in this checkout");

const ACK_WAIT_MS = 30_000;
const CONTROL = "fabric.control.command";
const LINK_INBOX = transportInboxStream(BRIDGE_MACHINE);
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
const dataOf = (event: MeshEvent) => (event.data ?? {}) as Record<string, unknown>;
const mentions = (main: MixedMain, token: string) => main.delivered().filter((row) => row.text.includes(token));

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link faults on a real broker", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = await createMixedHarness({ binary: binary!, natsPeer: false }); }, 90_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  const settle = <T>(work: Promise<T>) => work.catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); });
  /** Observe the outcome now (no unhandled rejection if an earlier assertion fails); assert it later. */
  const outcome = <T>(work: Promise<T>) => settle(work).then((value) => ({ value }), (error: unknown) => ({ error: String(error) }));
  const natsCommandsFor = (mark: number, token: string) => h.wire.since(mark).filter((event) => event.phase === "deliver" &&
    event.hostId === h.natsMain.id && event.envelope.kind === "command" && bodyOf(event).message === token);
  const fileCommandsFor = (token: string) => h.fileEvents({ topic: CONTROL }).filter((event) => dataOf(event).message === token);

  it("a forced disconnect of the link holds traffic both ways; after reconnect each message is consumed exactly once", async () => {
    const f2n = `OUTAGE_F2N_${randomUUID()}`, n2f = `OUTAGE_N2F_${randomUUID()}`;
    const mark = h.wire.mark(), refusedBefore = h.faults.refused;
    h.faults.block();
    // Both sends are issued during the outage: the file command is in the hub log, the NATS command in the broker.
    const toNats = outcome(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: f2n, idempotencyKey: `o-${randomUUID()}` }));
    const toFile = outcome(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: n2f, idempotencyKey: `o-${randomUUID()}` }));
    await waitForValue(() => fileCommandsFor(f2n).length ? true : undefined, "file command appended during the outage", 10_000);
    await h.wire.until(() => h.wire.since(mark).some((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      bodyOf(event).message === n2f), "NATS command stored during the outage");
    // The link really tried to come back and was refused at least once.
    await waitForValue(() => h.faults.refused > refusedBefore ? true : undefined, "link reconnect attempt refused", 15_000);
    expect(mentions(h.natsMain, f2n)).toEqual([]);
    expect(mentions(h.fileMain, n2f)).toEqual([]);
    h.faults.unblock();
    expect(await toNats).toMatchObject({ value: { acknowledged: true } });
    expect(await toFile).toMatchObject({ value: { acknowledged: true } });
    expect(mentions(h.natsMain, f2n)).toHaveLength(1);
    expect(mentions(h.fileMain, n2f)).toHaveLength(1);
    expect(natsCommandsFor(mark, f2n)).toHaveLength(1);
  }, 90_000);

  it("a lost PubAck on the link's publish: the stored command is consumed once, and any resend is the same id", async () => {
    const token = `LOST_PUBACK_${randomUUID()}`;
    const mark = h.wire.mark(), drops = h.faults.pubAckDrops, published = h.faults.inboxFrames.length;
    h.faults.losePubAcks(publishTo(NATS_MAIN_MACHINE, "cmd"));
    await expect(settle(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: `p-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    expect(h.faults.pubAckDrops - drops).toBe(1);
    // The owner's ACK may settle it before any PubAck timeout; a resend (now or later) must reuse the
    // first envelope id so the broker's Nats-Msg-Id window stores one copy.
    const barrier = `LOST_PUBACK_BARRIER_${randomUUID()}`;
    await expect(settle(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: barrier, idempotencyKey: `b-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    // Wire format (src/transport/nats/inbox.ts #wire): { format: 1, sourceMachine, envelope }.
    const ids = new Set(h.faults.publishedWith(token, published).map((frame) => (JSON.parse(frame.body) as { envelope?: { id?: string } }).envelope?.id));
    expect(ids.size).toBe(1);
    expect(natsCommandsFor(mark, token)).toHaveLength(1);
    expect(mentions(h.natsMain, token)).toHaveLength(1);
  }, 90_000);

  it("a lost broker +ACK after the durable file append, then a link restart: the real redelivery is not consumed again", async () => {
    const token = `LOST_ACK_${randomUUID()}`;
    const drops = h.faults.ackDrops;
    // Drop every broker +ACK the running link sends for NATS commands delivered to it (machine-b -> machine-a
    // inbox), its own re-ACK attempts included, until it is restarted: the broker must redeliver to the new link.
    h.faults.loseAckOf((delivery) => delivery.stream === LINK_INBOX && parseInboxSubject(delivery.subject)?.kind === "cmd" &&
      parseInboxSubject(delivery.subject)?.sourceMachine === NATS_MAIN_MACHINE, 1_000);
    const firstAt = Date.now();
    await expect(settle(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: `a-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    await waitForValue(() => h.faults.ackDrops > drops ? true : undefined, "the link's broker +ACK was sent and lost", 10_000);
    const lost = h.faults.droppedAcks[drops]!;
    expect(lost).toMatchObject({ stream: LINK_INBOX, delivered: 1 });
    expect(mentions(h.fileMain, token)).toHaveLength(1);
    expect(fileCommandsFor(token)).toHaveLength(1);
    // Restart the link while the delivery is still pending in the broker (ack pending, not redelivered yet).
    await h.stopLink();
    h.faults.clearFaults();
    await h.startLinkNow();
    await h.waitMirrored();
    // The broker redelivers the same stored message after AckWait; the new link must settle it without a re-append.
    const redelivered = await waitForValue(() => h.faults.deliveries.find((delivery) => delivery.stream === LINK_INBOX &&
      delivery.streamSequence === lost.streamSequence && delivery.delivered >= 2), "real broker redelivery to the restarted link", ACK_WAIT_MS + 20_000, 200);
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(ACK_WAIT_MS - 1_000);
    const original = h.faults.deliveries.find((delivery) => delivery.stream === LINK_INBOX &&
      delivery.streamSequence === lost.streamSequence && delivery.delivered === 1)!;
    expect(redelivered.subject).toBe(original.subject);
    await waitForValue(() => h.faults.acks.find((ack) => ack.stream === LINK_INBOX && ack.streamSequence === lost.streamSequence),
      "the restarted link settled the redelivery with a broker +ACK", 15_000, 100);
    expect(mentions(h.fileMain, token)).toHaveLength(1);
    expect(fileCommandsFor(token)).toHaveLength(1);
  }, ACK_WAIT_MS + 60_000);

  it("a link restart with a file command read but not yet published: it crosses once after the restart", async () => {
    const token = `RESTART_MID_FLOW_${randomUUID()}`;
    const mark = h.wire.mark();
    h.faults.block();
    const pending = settle(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: `r-${randomUUID()}` }));
    await waitForValue(() => fileCommandsFor(token).length ? true : undefined, "file command appended", 10_000);
    // Stop the link while it cannot reach the broker: whatever it had in hand never reached NATS.
    await h.stopLink();
    h.faults.unblock();
    expect(natsCommandsFor(mark, token)).toEqual([]);
    await h.startLinkNow();
    await expect(pending).resolves.toMatchObject({ acknowledged: true });
    expect(mentions(h.natsMain, token)).toHaveLength(1);
    expect(natsCommandsFor(mark, token)).toHaveLength(1);
  }, 90_000);
});
