// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' exactly-once across the hop on a real broker. Every fault is real and hits
// the link only, through its TLS wire relay: a forced disconnect (outage; each send must have one target-side
// delivery or persisted notRun expiry), a lost PubAck (the broker stored the command, the link cannot know), a
// lost broker +ACK (the link appended, the broker redelivers after the real 30 s AckWait) across a link restart,
// and a link restart with a command in flight. The latter fault cases end with one consumption and sender ACK.
// Official assumptions: AckWait is a redelivery deadline and Nats-Msg-Id
// dedupe is bounded by the stream window (https://docs.nats.io/nats-concepts/jetstream/consumers,
// https://docs.nats.io/nats-concepts/jetstream/headers). Only the Pi UI/model boundary is a fixture.
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { hashToken, transportInboxStream } from "../src/transport/nats/subjects.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, E2E_WAIT_MS, linkTail, NATS_MAIN_MACHINE, natsLinkPresent, waitForValue,
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

const CONTROL_SEEN_PREFIX = "topology/control-seen/";
const CONTROL_COMMAND_EXPIRED = "Fabric control command expired";
type ControlSeenRecord = {
  format?: unknown;
  hostId?: unknown;
  commandId?: unknown;
  targetId?: unknown;
  acceptance?: { accepted?: unknown; messageId?: unknown; error?: unknown; notRun?: unknown };
};
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const controlSeenStores = new Map<string, MeshStore>();
const controlSeenKey = (hostId: string, commandId: string) => CONTROL_SEEN_PREFIX +
  createHash("sha256").update(`${hostId}\0${commandId}`).digest("hex");
const controlSeenStoreFor = (main: MixedMain): MeshStore => {
  let store = controlSeenStores.get(main.id);
  if (!store) {
    store = new MeshStore(path.join(main.runtime.mesh.root, "control-seen", hashToken(main.id)),
      main.runtime.mesh.maxEventBytes, main.runtime.mesh.maxReadEvents,
      { lockProtocol: main.runtime.mesh.lockProtocol, stateBackend: "file" });
    controlSeenStores.set(main.id, store);
  }
  return store;
};
const controlSeenRecordsFor = (main: MixedMain, commandId: string) =>
  controlSeenStoreFor(main).listAll(CONTROL_SEEN_PREFIX, { fresh: true }).flatMap((entry) => {
    if (!isRecord(entry.value) || entry.value.hostId !== main.id || entry.value.commandId !== commandId) return [];
    return [{ key: entry.key, record: entry.value as ControlSeenRecord }];
  });
const waitForControlOutcome = (main: MixedMain, commandId: string) => waitForValue(() => {
  const records = controlSeenRecordsFor(main, commandId);
  return records.some(({ record }) => record.acceptance !== undefined) ? records : undefined;
}, `persisted control outcome for ${main.id}/${commandId}`, E2E_WAIT_MS);
const deliveriesForCommand = (main: MixedMain, token: string, commandId: string) =>
  mentions(main, token).flatMap((row) => {
    const items = Array.isArray(row.details.items) ? row.details.items : [row.details];
    return items.flatMap((item) => isRecord(item) && item.deliveryId === commandId && typeof item.id === "string"
      ? [{ messageId: item.id }] : []);
  });
const assertOutageTerminalOutcome = async (main: MixedMain, commandId: string, token: string): Promise<void> => {
  const records = await waitForControlOutcome(main, commandId);
  expect(records).toHaveLength(1);
  const { key, record } = records[0]!;
  expect(key).toBe(controlSeenKey(main.id, commandId));
  expect(record).toMatchObject({ format: 1, hostId: main.id, commandId, targetId: main.id });

  const deliveries = deliveriesForCommand(main, token, commandId);
  const exactlyOneDelivery = deliveries.length === 1;
  const expiredAcceptance = record.acceptance?.accepted === false &&
    record.acceptance.error === CONTROL_COMMAND_EXPIRED && record.acceptance.notRun === true;
  expect(deliveries.length > 0 && expiredAcceptance).toBe(false);
  // XOR: the target's persisted acceptance and deliveryId-correlation must prove one terminal outcome.
  expect(exactlyOneDelivery).not.toBe(expiredAcceptance);
  if (exactlyOneDelivery) {
    expect(record.acceptance).toMatchObject({ accepted: true, messageId: deliveries[0]!.messageId });
  } else {
    expect(record.acceptance).toMatchObject({ accepted: false, error: CONTROL_COMMAND_EXPIRED, notRun: true });
    expect(deliveries).toEqual([]);
  }
};
const closeControlSeenStores = (): void => {
  for (const store of controlSeenStores.values()) store.closeState();
  controlSeenStores.clear();
};

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link faults on a real broker", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = await createMixedHarness({ binary: binary!, natsPeer: false }); }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { try { closeControlSeenStores(); } finally { await h?.stop(); } }, 60_000);

  const settle = <T>(work: Promise<T>) => work.catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); });
  /** Observe the raw outcome now (no unhandled rejection); outage sends may time out without proving non-delivery. */
  type SendOutcome<T> = { value: T } | { error: unknown };
  const outcome = <T>(work: Promise<T>): Promise<SendOutcome<T>> => work.then(
    (value) => ({ value }), (error: unknown) => ({ error }),
  );
  const assertOutageOutcome = (result: SendOutcome<unknown>, targetId: string) => {
    if ("value" in result) {
      expect(result.value).toMatchObject({ acknowledged: true });
      return;
    }
    const error = result.error;
    if (!(error instanceof Error) || error.constructor !== Error || error.name !== "Error") {
      throw new Error(`Unexpected outage-send error class: ${String(error)}\n${linkTail(h)}`);
    }
    const prefix = "Fabric mesh bridge to remote host ";
    const suffix = ` is not responding for ${targetId}; the outcome is unknown and it may still be delivered, so a retry can deliver it twice.`;
    const host = error.message.startsWith(prefix) && error.message.endsWith(suffix)
      ? error.message.slice(prefix.length, error.message.length - suffix.length)
      : "";
    const ownerTimeout = `Timed out waiting for the remote Fabric owner to acknowledge ${targetId}; ` +
      "the outcome is unknown and it may still be delivered, so a retry can deliver it twice.";
    if ((!host || /[;\r\n]/.test(host)) && error.message !== ownerTimeout) {
      throw new Error(`Unexpected outage-send error message: ${error.message}\n${linkTail(h)}`);
    }
  };
  const natsCommandsFor = (mark: number, token: string) => h.wire.since(mark).filter((event) => event.phase === "deliver" &&
    event.hostId === h.natsMain.id && event.envelope.kind === "command" && bodyOf(event).message === token);
  const fileCommandsFor = (token: string) => h.fileEvents({ topic: CONTROL }).filter((event) => dataOf(event).message === token);

  it("after a forced disconnect, outage sends have one terminal outcome and reconnect sends are delivered once", async () => {
    const f2n = `OUTAGE_F2N_${randomUUID()}`, n2f = `OUTAGE_N2F_${randomUUID()}`;
    const f2nKey = `o-${randomUUID()}`, n2fKey = `o-${randomUUID()}`;
    const mark = h.wire.mark(), refusedBefore = h.faults.refused;
    h.faults.block();
    // Keep both sends inside the outage; outcomes may be ACKed or the exact documented unknown timeout.
    const toNatsDuringOutage = outcome(h.fileMain.invoke("agents.followUp", {
      id: h.natsMain.id, message: f2n, idempotencyKey: f2nKey,
    }));
    const toFileDuringOutage = outcome(h.natsMain.invoke("agents.followUp", {
      id: h.fileMain.id, message: n2f, idempotencyKey: n2fKey,
    }));
    await waitForValue(() => fileCommandsFor(f2n).length ? true : undefined,
      "file command appended during the outage", E2E_WAIT_MS);
    await h.wire.until(() => h.wire.since(mark).some((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      bodyOf(event).message === n2f), "NATS command stored during the outage", E2E_WAIT_MS);
    // Neither outage send may be consumed before the verified reconnect.
    expect(mentions(h.natsMain, f2n)).toEqual([]);
    expect(mentions(h.fileMain, n2f)).toEqual([]);
    // The link really tried to come back and was refused at least once.
    await waitForValue(() => h.faults.refused > refusedBefore ? true : undefined, "link reconnect attempt refused", E2E_WAIT_MS);
    const reconnectMark = h.linkLog.length;
    h.faults.unblock();
    // Wait for the bridge port's own verified-reconnect callback, not another connection's recovery.
    await waitForValue(() => h.linkLog.slice(reconnectMark).includes("nats port: verified-reconnect") ? true : undefined,
      () => `reconnect not verified\n${linkTail(h)}`, 30_000);
    // Post-signal route-readiness barrier: waitMirrored/liveRow reads participantInfos({ fresh: true }) only now.
    try {
      await h.waitMirrored(30_000);
    } catch (error) {
      throw new Error(`Route readiness after verified reconnect failed: ${String(error)}\n${linkTail(h)}`, { cause: error });
    }
    assertOutageOutcome(await toNatsDuringOutage, h.natsMain.id);
    assertOutageOutcome(await toFileDuringOutage, h.fileMain.id);

    const f2nFileCommand = fileCommandsFor(f2n).find((event) => dataOf(event).targetId === h.natsMain.id);
    expect(f2nFileCommand).toBeDefined();
    expect(typeof dataOf(f2nFileCommand!).commandId).toBe("string");
    const f2nCommandId = bridgeNatsMappedCommandId(h.linkName, "toNats", h.fileMain.id,
      String(dataOf(f2nFileCommand!).commandId));
    const n2fFileCommand = await waitForValue(() => fileCommandsFor(n2f).find((event) => dataOf(event).targetId === h.fileMain.id),
      "NATS-to-file command appended to the target hub after reconnect", E2E_WAIT_MS);
    expect(typeof dataOf(n2fFileCommand).commandId).toBe("string");
    const n2fCommandId = String(dataOf(n2fFileCommand).commandId);
    await assertOutageTerminalOutcome(h.natsMain, f2nCommandId, f2n);
    await assertOutageTerminalOutcome(h.fileMain, n2fCommandId, n2f);

    const postF2n = `POST_RECONNECT_F2N_${randomUUID()}`, postN2f = `POST_RECONNECT_N2F_${randomUUID()}`;
    const postToNats = settle(h.fileMain.invoke("agents.followUp", {
      id: h.natsMain.id, message: postF2n, idempotencyKey: `p-${randomUUID()}`,
    }));
    const postToFile = settle(h.natsMain.invoke("agents.followUp", {
      id: h.fileMain.id, message: postN2f, idempotencyKey: `p-${randomUUID()}`,
    }));
    await expect(postToNats).resolves.toMatchObject({ acknowledged: true });
    await expect(postToFile).resolves.toMatchObject({ acknowledged: true });
    await waitForValue(() => mentions(h.natsMain, postF2n).length > 0 ? true : undefined,
      "post-reconnect file-to-NATS command consumed", E2E_WAIT_MS);
    await waitForValue(() => mentions(h.fileMain, postN2f).length > 0 ? true : undefined,
      "post-reconnect NATS-to-file command consumed", E2E_WAIT_MS);
    expect(mentions(h.natsMain, postF2n)).toHaveLength(1);
    expect(mentions(h.fileMain, postN2f)).toHaveLength(1);
    expect(natsCommandsFor(mark, postF2n)).toHaveLength(1);
  }, E2E_WAIT_MS * 4);

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
  }, E2E_WAIT_MS * 2);

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
    await waitForValue(() => h.faults.ackDrops > drops ? true : undefined, "the link's broker +ACK was sent and lost", E2E_WAIT_MS);
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
      "the restarted link settled the redelivery with a broker +ACK", E2E_WAIT_MS, 100);
    expect(mentions(h.fileMain, token)).toHaveLength(1);
    expect(fileCommandsFor(token)).toHaveLength(1);
  }, ACK_WAIT_MS + E2E_WAIT_MS * 3);

  it("a link restart with a file command read but not yet published: it crosses once after the restart", async () => {
    const token = `RESTART_MID_FLOW_${randomUUID()}`;
    const mark = h.wire.mark();
    h.faults.block();
    const pending = settle(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: `r-${randomUUID()}` }));
    await waitForValue(() => fileCommandsFor(token).length ? true : undefined, "file command appended", E2E_WAIT_MS);
    // Stop the link while it cannot reach the broker: whatever it had in hand never reached NATS.
    await h.stopLink();
    h.faults.unblock();
    expect(natsCommandsFor(mark, token)).toEqual([]);
    await h.startLinkNow();
    await expect(pending).resolves.toMatchObject({ acknowledged: true });
    expect(mentions(h.natsMain, token)).toHaveLength(1);
    expect(natsCommandsFor(mark, token)).toHaveLength(1);
  }, E2E_WAIT_MS * 3);
});
