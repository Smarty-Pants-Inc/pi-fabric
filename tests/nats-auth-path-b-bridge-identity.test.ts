// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' Tier 1 identity on the real path. The link maps a sender only from the
// verified presence of the side it came from: rewritten senders (either side), a live but unlisted file root,
// a stale target epoch (either side) and steer to a principal seat are all refused, and nothing is consumed.
// Each refusal is followed by an ordinary message on the same ordered path, so "nothing crossed" is decided,
// not timed out. Only the Pi UI/model boundary is a fixture.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, linkTail, liveRow, NATS_MAIN_MACHINE, natsLinkPresent, selfRecord, waitForValue,
  type MixedHarness, type MixedMain, type WireEvent,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
if (!linked) console.warn("A' bridge identity E2E skipped: src/mesh/bridge-nats-link.ts (lane L2) is not in this checkout");

const CONTROL = "fabric.control.command", ACK = "fabric.control.ack";
const STALE = "FABRIC_CONTROL_STALE_INCARNATION";
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
const dataOf = (event: MeshEvent) => (event.data ?? {}) as Record<string, unknown>;
const mentions = (main: MixedMain, token: string) => main.delivered().filter((row) => row.text.includes(token));
const identity = (main: MixedMain): MeshIdentity => ({ id: main.id, name: main.label, kind: "main", sessionId: main.sessionId });
const hex64 = () => (randomUUID() + randomUUID()).replaceAll("-", "").slice(0, 64);

/** A well-formed NATS control command body, exactly as control-plane.ts #requestNatsAcceptance stamps one. */
function natsCommand(target: string, replyTo: string, ownerIncarnation: string, message: string, operation = "followUp") {
  const now = Date.now();
  return { version: 1, commandId: hex64(), targetId: target, operation, ownerIncarnation, message, replyTo,
    requestCreatedAt: now, requestedAt: now, deadlineAt: now + 60_000 };
}

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link identity on a real broker", () => {
  let h: MixedHarness;
  beforeAll(async () => {
    // natsPeer is a principal seat: AGENTS lets a principal's org instance receive followUp (and cancel) only.
    h = await createMixedHarness({ binary: binary!, outsider: true, natsPeer: true, seat: true });
  }, 90_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  /** Ordered barrier on the file -> NATS path: a later legitimate followUp is acknowledged. */
  const fileToNatsBarrier = async () => {
    const token = `BARRIER_F2N_${randomUUID()}`;
    await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: `b-${randomUUID()}` })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); })).resolves.toMatchObject({ acknowledged: true });
  };
  /** Ordered barrier on the NATS -> file path (same consumer as the refused deliveries). */
  const natsToFileBarrier = async () => {
    const token = `BARRIER_N2F_${randomUUID()}`;
    await expect(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: `b-${randomUUID()}` })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); })).resolves.toMatchObject({ acknowledged: true });
  };
  /** Wire-level: anything the link published to an inbox carrying the token, or any delivery of it to the v2 Main. */
  const crossedToNats = (mark: { wire: number; relay: number }, token: string) => [
    ...h.faults.publishedWith(token, mark.relay),
    ...h.wire.since(mark.wire).filter((event) => event.phase === "deliver" && event.hostId === h.natsMain.id && bodyOf(event).message === token),
  ];

  it("NATS side: a delivery whose from.id is not live on its credential's machine is refused and never consumed", async () => {
    const fileEpoch = selfRecord(h.fileMain)!.ownerIncarnation!;
    const raw = await h.openPeer(NATS_MAIN_MACHINE, `rewriter-${randomUUID()}`);
    const claims: Array<[string, MeshIdentity]> = [
      ["file-native id", identity(h.fileMain)], // native to the file side: a forged origin / loop
      ["bridge identity", { id: `bridge:${h.linkName}`, name: "mesh-bridge", kind: "main" }],
      ["unknown root", { id: `session:${randomUUID()}`, name: "nobody", kind: "main" }],
    ];
    const tokens: string[] = [];
    for (const [label, from] of claims) {
      const token = `REWRITTEN_NATS_${label.replaceAll(" ", "_")}_${randomUUID()}`;
      tokens.push(token);
      const envelope: TransportEnvelope = { id: randomUUID(), kind: "command", to: { machine: BRIDGE_MACHINE, hostId: h.fileMain.id },
        targetId: h.fileMain.id, operation: "followUp", from, body: natsCommand(h.fileMain.id, raw.self.hostId, fileEpoch, token) };
      await raw.send(envelope);
    }
    await natsToFileBarrier();
    for (const token of tokens) {
      expect(mentions(h.fileMain, token), token).toEqual([]);
      expect(h.fileEvents({ topic: CONTROL }).filter((event) => dataOf(event).message === token), token).toEqual([]);
    }
  }, 60_000);

  it("file side: a command whose sender is not a live allow-listed native file root never crosses", async () => {
    const natsEpoch = selfRecord(h.natsMain)!.ownerIncarnation!;
    const store = h.fileMain.runtime.mesh;
    const mark = { wire: h.wire.mark(), relay: h.faults.inboxFrames.length };
    const forged: Array<[string, MeshIdentity]> = [
      ["mirrored NATS root", identity(h.natsMain)], // native to NATS: the hub must not speak for it back
      ["unknown root", { id: `session:${randomUUID()}`, name: "nobody", kind: "main" }],
    ];
    const tokens: string[] = [];
    for (const [label, from] of forged) {
      const token = `REWRITTEN_FILE_${label.replaceAll(" ", "_")}_${randomUUID()}`;
      tokens.push(token);
      await store.publish({ topic: CONTROL, kind: "followUp", from, to: h.natsMain.id,
        data: { ...natsCommand(h.natsMain.id, from.id, natsEpoch, token), destinationRemoteHost: h.linkName } });
    }
    // A real, live, native file root that is simply not on the allow-list.
    const outsider = h.outsider!;
    await waitForValue(() => liveRow(outsider, h.natsMain.id), "mirrored NATS root visible to the outsider", 10_000);
    const outsiderToken = `UNLISTED_FILE_ROOT_${randomUUID()}`;
    tokens.push(outsiderToken);
    // A verified sender is ALWAYS answered (integrator note 10, F): a notRun refusal ACK from bridge:<link>
    // with a stable BRIDGE_NATS_* code, well inside its 30 s bridge window. An abort at 15 s is no answer.
    const unlisted = outsider.invoke("agents.followUp", { id: h.natsMain.id, message: outsiderToken, idempotencyKey: `o-${randomUUID()}` },
      AbortSignal.timeout(15_000)).then(() => "accepted", (error: unknown) => /BRIDGE_NATS_/.test(String(error)) ? "refused" : String(error));
    await fileToNatsBarrier();
    expect(await unlisted).toBe("refused");
    for (const token of tokens) {
      expect(mentions(h.natsMain, token), token).toEqual([]);
      expect(crossedToNats(mark, token), token).toEqual([]);
    }
  }, 60_000);

  it("file -> NATS: a stale target epoch crosses unchanged, the NATS owner refuses it notRun, and that ACK returns", async () => {
    const token = `STALE_TO_NATS_${randomUUID()}`;
    const command = { ...natsCommand(h.natsMain.id, h.fileMain.id, `stale-${randomUUID()}`, token), destinationRemoteHost: h.linkName };
    await h.fileMain.runtime.mesh.publish({ topic: CONTROL, kind: "followUp", from: identity(h.fileMain), to: h.natsMain.id, data: command });
    const ack = await waitForValue(() => h.fileEvents({ topic: ACK, to: h.fileMain.id }).find((event) => dataOf(event).commandId === command.commandId),
      "stale refusal ACK back in the file mesh", 30_000);
    expect(ack).toMatchObject({ kind: "rejected", from: { id: h.natsMain.id },
      data: { accepted: false, notRun: true, errorCode: STALE, targetId: h.natsMain.id } });
    expect(mentions(h.natsMain, token)).toEqual([]);
  }, 45_000);

  it("NATS -> file: a stale target epoch is refused notRun by the file owner, the ACK returns, nothing is consumed", async () => {
    const token = `STALE_TO_FILE_${randomUUID()}`;
    const raw = await h.openPeer(NATS_MAIN_MACHINE, `stale-sender-${randomUUID()}`);
    const mark = h.wire.mark();
    // The real v2 Main's verified identity on its own machine credential; only the target epoch is old.
    const body = natsCommand(h.fileMain.id, h.natsMain.id, `stale-${randomUUID()}`, token);
    await raw.send({ id: randomUUID(), kind: "command", to: { machine: BRIDGE_MACHINE, hostId: h.fileMain.id },
      targetId: h.fileMain.id, operation: "followUp", from: identity(h.natsMain), body });
    await h.wire.until(() => h.wire.since(mark).some((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "ack" && bodyOf(event).commandId === body.commandId), "stale refusal ACK at the NATS sender", 30_000);
    const ack = h.wire.since(mark).find((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "ack" && bodyOf(event).commandId === body.commandId)!;
    expect(ack).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { operation: "rejected", from: { id: h.fileMain.id } } });
    expect(bodyOf(ack)).toMatchObject({ accepted: false, notRun: true });
    expect(String(bodyOf(ack).errorCode)).toMatch(/STALE_INCARNATION/);
    expect(mentions(h.fileMain, token)).toEqual([]);
  }, 45_000);

  it("a principal seat across the link receives followUp but never steer", async () => {
    const seat = h.natsPeer!;
    await waitForValue(() => liveRow(h.fileMain, seat.id), "seat mirrored into the file mesh", 15_000);
    const steer = `SEAT_STEER_${randomUUID()}`, followUp = `SEAT_FOLLOWUP_${randomUUID()}`;
    // Refused before publish by destination class and answered with the stable code, never left to time out.
    await expect(h.fileMain.invoke("agents.steer", { id: seat.id, message: steer, idempotencyKey: `s-${randomUUID()}` }, AbortSignal.timeout(15_000)))
      .rejects.toThrow(/BRIDGE_NATS_DESTINATION_CLASS_REFUSED/);
    await expect(h.fileMain.invoke("agents.followUp", { id: seat.id, message: followUp, idempotencyKey: `f-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    expect(mentions(seat, steer)).toEqual([]);
    expect(mentions(seat, followUp)).toEqual([expect.objectContaining({ deliverAs: "followUp", from: expect.objectContaining({ id: h.fileMain.id }) })]);
  }, 60_000);
});
