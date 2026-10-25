// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' (D3 gate 3) real-broker E2E. A real FILE-transport Main (Ryzen 1 lead)
// and a real NATS-transport Main (moved v2 lead) reach each other only through the in-process link:
// mirrored presence both ways, one keyed followUp each way, steer each way, and a commandId collision
// that must never hand one sender another sender's outcome. Only the Pi UI/model boundary is a fixture.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import type { MeshEvent } from "../src/mesh/store.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, linkTail, liveRow, NATS_MAIN_MACHINE, natsLinkPresent, selfRecord, waitForValue,
  type MixedHarness, type MixedMain, type WireEvent,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
if (!linked) console.warn("A' bridge E2E skipped: src/mesh/bridge-nats-link.ts (lane L2) is not in this checkout");

const CONTROL = "fabric.control.command", ACK = "fabric.control.ack";
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
const dataOf = (event: MeshEvent) => (event.data ?? {}) as Record<string, unknown>;
const mentions = (main: MixedMain, token: string) => main.delivered().filter((row) => row.text.includes(token));
let current: MixedHarness | undefined;
/** Attach the link's log tail to a failure. */
const withLog = <T>(work: Promise<T>): Promise<T> => work.catch((error: unknown) => { throw new Error(`${String(error)}\n${current ? linkTail(current) : ""}`); });

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link: FILE Main <-> NATS Main on a real broker", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = current = await createMixedHarness({ binary: binary!, natsPeer: true }); }, 90_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  it("mirrors each allow-listed root live onto the other side, under the link's name and the bridge machine", async () => {
    const { fileView, natsView } = await h.waitMirrored();
    const nats = selfRecord(h.natsMain)!, file = selfRecord(h.fileMain)!;
    // Ryzen 1 readers (smarty-send, smarty-org-target, review_gate live_roots) see a live root with link provenance.
    expect(fileView).toMatchObject({ id: h.natsMain.id, kind: "root", rootId: h.natsMain.id, sessionId: h.natsMain.sessionId,
      ownerIncarnation: nats.ownerIncarnation, remoteHost: h.linkName, local: false, stale: false });
    // v2 routes to the file root through presence + lease the bridge machine holds, at the file Main's exact epoch.
    expect(natsView).toMatchObject({ id: h.fileMain.id, kind: "root", ownerIncarnation: file.ownerIncarnation,
      machine: BRIDGE_MACHINE, local: false, stale: false });
    const kv = await h.kvRoot(BRIDGE_MACHINE, h.fileMain.id);
    expect(kv?.presence).toMatchObject({ id: h.fileMain.id, ownerIncarnation: file.ownerIncarnation });
    expect(kv?.lease).toMatchObject({ id: h.fileMain.id, incarnation: file.ownerIncarnation });
    // Never mirrored back the other way, and never written under the NATS Main's machine.
    expect(await h.kvRoot(NATS_MAIN_MACHINE, h.fileMain.id)).toBeUndefined();
    expect(await h.kvRoot(BRIDGE_MACHINE, h.natsMain.id)).toBeUndefined();
  }, 30_000);

  it("file Main -> NATS Main: one keyed followUp, one consumption, outer from, exact epoch, ACK; a same-key resend replays", async () => {
    const token = `FILE_TO_NATS_${randomUUID()}`, key = `k-${randomUUID()}`;
    const mark = h.wire.mark();
    const first = await h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: key })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); }) as Record<string, unknown>;
    expect(first).toMatchObject({ acknowledged: true, routed: "mesh" });
    const got = mentions(h.natsMain, token);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ from: { id: h.fileMain.id, kind: "main" }, deliverAs: "followUp" });
    // The command that reached the NATS Main: credential-bound from the bridge machine, sender = the file
    // root's verified identity, fenced to the NATS Main's live epoch; its commandId is the per-hop mapped one.
    const fileCommand = h.fileEvents({ topic: CONTROL, to: h.natsMain.id }).find((event) => dataOf(event).message === token)!;
    expect(fileCommand).toMatchObject({ from: { id: h.fileMain.id }, data: { destinationRemoteHost: h.linkName } });
    const crossed = h.wire.since(mark).filter((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token);
    expect(crossed).toHaveLength(1);
    expect(crossed[0]).toMatchObject({ sourceMachine: BRIDGE_MACHINE, attempt: 1,
      envelope: { from: { id: h.fileMain.id }, targetId: h.natsMain.id, operation: "followUp",
        body: { ownerIncarnation: selfRecord(h.natsMain)!.ownerIncarnation } } });
    // Per-hop mapping: distinct verified senders can never collide on the NATS owner's seen-claims.
    expect(bodyOf(crossed[0]!).commandId).toBe(bridgeNatsMappedCommandId(h.linkName, "toNats", h.fileMain.id, String(dataOf(fileCommand).commandId)));
    // The owner's ACK returned into the file mesh to the sender.
    const acks = h.fileEvents({ topic: ACK, to: h.fileMain.id }).filter((event) => dataOf(event).commandId === dataOf(fileCommand).commandId);
    expect(acks.length).toBeGreaterThanOrEqual(1);
    expect(acks[0]).toMatchObject({ kind: "accepted", from: { id: h.natsMain.id }, data: { accepted: true, messageId: first.messageId } });
    // The same key and input again: the receiver replays its stored outcome; nothing is consumed twice.
    const again = await withLog(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: key })) as Record<string, unknown>;
    expect(again).toMatchObject({ acknowledged: true, messageId: first.messageId });
    expect(mentions(h.natsMain, token)).toHaveLength(1);
  }, 60_000);

  it("NATS Main -> file Main: one keyed followUp, one consumption, outer from, exact epoch, ACK; a same-key resend replays", async () => {
    const token = `NATS_TO_FILE_${randomUUID()}`, key = `k-${randomUUID()}`;
    const mark = h.wire.mark();
    const first = await h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: key })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); }) as Record<string, unknown>;
    expect(first).toMatchObject({ acknowledged: true });
    const got = mentions(h.fileMain, token);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ from: { id: h.natsMain.id, kind: "main" }, deliverAs: "followUp" });
    const sent = h.wire.since(mark).find((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token)!;
    expect(sent.envelope).toMatchObject({ to: { machine: BRIDGE_MACHINE, hostId: h.fileMain.id } });
    // The link appended exactly one command event: sender from v2's verified presence, the file Main's
    // exact epoch, the link's bridge stamp, and a per-hop mapped commandId.
    const appended = h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter((event) => dataOf(event).message === token);
    expect(appended).toHaveLength(1);
    expect(appended[0]).toMatchObject({ from: { id: h.natsMain.id, kind: "main" },
      data: { ownerIncarnation: selfRecord(h.fileMain)!.ownerIncarnation, bridge: { from: h.linkName } } });
    expect(dataOf(appended[0]!).commandId).toBe(bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(bodyOf(sent).commandId)));
    // The file owner's ACK went back to the NATS sender as a credential-bound ACK from the bridge machine.
    const ack = h.wire.since(mark).find((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "ack" && bodyOf(event).commandId === bodyOf(sent).commandId);
    expect(ack).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { id: `ack:${sent.envelope.id}`, from: { id: h.fileMain.id },
      body: { accepted: true } } });
    const again = await withLog(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: key })) as Record<string, unknown>;
    expect(again).toMatchObject({ acknowledged: true, messageId: first.messageId });
    expect(mentions(h.fileMain, token)).toHaveLength(1);
    expect(h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter((event) => dataOf(event).message === token).length).toBeLessThanOrEqual(2);
  }, 60_000);

  it("steers a non-seat Main each way", async () => {
    const toNats = `STEER_FILE_TO_NATS_${randomUUID()}`, toFile = `STEER_NATS_TO_FILE_${randomUUID()}`;
    await expect(h.fileMain.invoke("agents.steer", { id: h.natsMain.id, message: toNats, idempotencyKey: `s-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    await expect(h.natsMain.invoke("agents.steer", { id: h.fileMain.id, message: toFile, idempotencyKey: `s-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    expect(mentions(h.natsMain, toNats)).toEqual([expect.objectContaining({ deliverAs: "steer", from: expect.objectContaining({ id: h.fileMain.id }) })]);
    expect(mentions(h.fileMain, toFile)).toEqual([expect.objectContaining({ deliverAs: "steer", from: expect.objectContaining({ id: h.natsMain.id }) })]);
  }, 60_000);

  it("a second verified NATS sender re-using another sender's commandId never receives that sender's outcome", async () => {
    const peer = h.natsPeer!;
    await waitForValue(() => liveRow(peer, h.fileMain.id), "file root routable from the NATS peer", 10_000);
    const token = `COLLISION_${randomUUID()}`;
    const mark = h.wire.mark();
    const original = await h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: `c-${randomUUID()}` }) as Record<string, unknown>;
    expect(original).toMatchObject({ acknowledged: true });
    const sent = h.wire.since(mark).find((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token)!;
    // Same machine credential (machine-b), a verified allow-listed sender (the peer root), and the original's
    // exact commandId and body. Only the sender and its reply address differ.
    const raw = await h.openPeer(NATS_MAIN_MACHINE, `collision-raw-${randomUUID()}`);
    const copy: TransportEnvelope = { ...sent.envelope, id: randomUUID(), from: { id: peer.id, name: peer.label, kind: "main" },
      body: { ...bodyOf(sent), replyTo: peer.id } };
    await raw.send(copy);
    // Prove the link processed the peer's traffic: a later ordinary peer followUp crosses and is acknowledged.
    const barrier = `COLLISION_BARRIER_${randomUUID()}`;
    await expect(peer.invoke("agents.followUp", { id: h.fileMain.id, message: barrier, idempotencyKey: `b-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    const toPeer = h.wire.since(mark).filter((event) => event.phase === "deliver" && event.hostId === peer.id && event.envelope.kind === "ack");
    // Whatever the peer got for the copied commandId, it is never the original sender's stored outcome:
    // the two senders' commands map to different file-side ids.
    expect(bridgeNatsMappedCommandId(h.linkName, "toFile", peer.id, String(bodyOf(sent).commandId)))
      .not.toBe(bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(bodyOf(sent).commandId)));
    for (const ack of toPeer.filter((event) => bodyOf(event).commandId === bodyOf(sent).commandId)) {
      expect(bodyOf(ack).messageId).not.toBe(original.messageId);
    }
    const peerCopies = h.fileEvents({ topic: "fabric.control.command", to: h.fileMain.id })
      .filter((event) => dataOf(event).message === token && event.from.id === peer.id);
    for (const copy of peerCopies) {
      expect(dataOf(copy).commandId).toBe(bridgeNatsMappedCommandId(h.linkName, "toFile", peer.id, String(bodyOf(sent).commandId)));
    }
    // The original sender's message was consumed once, from the original sender only.
    expect(mentions(h.fileMain, token).filter((row) => row.from?.id === h.natsMain.id)).toHaveLength(1);
    expect(mentions(h.fileMain, token).every((row) => row.from?.id === h.natsMain.id || row.from?.id === peer.id)).toBe(true);
  }, 60_000);
});
