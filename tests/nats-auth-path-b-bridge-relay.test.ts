// expected-skip: no NATS_BIN/PATH executable, no A' link entry, or no spoke relay (allow `viaSpoke`) in this checkout.
// Keep reason: smarty-dev#15310 A' PR 2 relay (T8) on a real broker. A work-host lane on a spoke file mesh reaches a
// moved v2 lead only spoke -> hub -> NATS: the hub's existing file<->file bridge relays to the A' link as one more
// hub link (MeshBridge's relay seam, in-process here; the UDS relay in production). Identity still comes only from
// the verified presence of the side a message came from: spoofed spoke senders and a NATS credential claiming the
// spoke root are refused and never consumed.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { MeshIdentity } from "../src/mesh/store.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  allowEntrySupported, allowRoot, BRIDGE_MACHINE, createMixedHarness, E2E_WAIT_MS, HUB_NAME, linkTail, liveRow, NATS_MAIN_MACHINE,
  natsLinkPresent, rootId, selfRecord, SPOKE_LINK_NAME, SPOKE_MAIN_SESSION, waitForValue, type MixedHarness, type MixedMain, type WireEvent,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
const relay = linked && allowEntrySupported("file", allowRoot(rootId(SPOKE_MAIN_SESSION), { viaSpoke: SPOKE_LINK_NAME }));
if (!relay) console.warn("A' relay E2E skipped: no link entry or no allow.file viaSpoke (lane L3, PR 2) in this checkout");

const CONTROL = "fabric.control.command";
const identity = (main: MixedMain): MeshIdentity => ({ id: main.id, name: main.label, kind: "main", sessionId: main.sessionId });
const mentions = (main: MixedMain, token: string) => main.delivered().filter((row) => row.text.includes(token));
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;

describe.skipIf(!binary || process.platform !== "linux" || !relay)("A' relay: a work-host lane <-> a moved v2 lead through the hub", () => {
  let h: MixedHarness;
  let spoke: MixedMain;
  beforeAll(async () => {
    h = await createMixedHarness({ binary: binary!, spoke: true, allow: (allow, ids) => {
      allow.file = [...allow.file, allowRoot(ids.spokeMain, { viaSpoke: SPOKE_LINK_NAME })];
    } });
    spoke = h.spoke!.main;
  }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  const withLog = <T>(work: Promise<T>): Promise<T> =>
    work.catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}\n--- spoke bridge:\n${h.spoke!.log.slice(-20).join("\n")}`); });

  it("the link joins the hub's relay, the v2 lead is re-exported to the spoke, and the spoke root is in NATS KV", async () => {
    expect(h.relays.has(h.linkName)).toBe(true);
    const lead = await waitForValue(() => liveRow(spoke, h.natsMain.id), "v2 lead visible on the spoke", E2E_WAIT_MS);
    expect(lead).toMatchObject({ remoteHost: h.linkName, bridgeVia: HUB_NAME, ownerIncarnation: selfRecord(h.natsMain)!.ownerIncarnation });
    const lane = await waitForValue(() => liveRow(h.natsMain, spoke.id), "spoke root routable from v2", E2E_WAIT_MS);
    expect(lane).toMatchObject({ machine: BRIDGE_MACHINE, ownerIncarnation: selfRecord(spoke)!.ownerIncarnation });
  }, E2E_WAIT_MS * 3);

  it("spoke lane -> v2 lead: one keyed followUp, one consumption from the lane's verified identity, and the ACK back", async () => {
    const token = `SPOKE_TO_V2_${randomUUID()}`;
    const mark = h.wire.mark();
    await expect(withLog(spoke.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: `r-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    expect(mentions(h.natsMain, token)).toEqual([expect.objectContaining({ deliverAs: "followUp", from: expect.objectContaining({ id: spoke.id }) })]);
    const crossed = h.wire.since(mark).filter((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token);
    expect(crossed).toHaveLength(1);
    expect(crossed[0]).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { from: { id: spoke.id } } });
  }, E2E_WAIT_MS * 2);

  it("v2 lead -> spoke lane: one keyed followUp, one consumption, and the ACK back", async () => {
    const token = `V2_TO_SPOKE_${randomUUID()}`;
    await expect(withLog(h.natsMain.invoke("agents.followUp", { id: spoke.id, message: token, idempotencyKey: `l-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    expect(mentions(spoke, token)).toEqual([expect.objectContaining({ deliverAs: "followUp", from: expect.objectContaining({ id: h.natsMain.id }) })]);
  }, E2E_WAIT_MS * 2);

  it("a spoofed spoke sender is refused and never consumed, on the spoke mesh and from a NATS credential", async () => {
    const lead = selfRecord(h.natsMain)!.ownerIncarnation!, lane = selfRecord(spoke)!.ownerIncarnation!;
    const now = Date.now();
    const command = (target: string, replyTo: string, epoch: string, message: string) => ({ version: 1, commandId: randomUUID().replaceAll("-", ""),
      targetId: target, operation: "followUp", ownerIncarnation: epoch, message, replyTo, requestCreatedAt: now, requestedAt: now,
      deadlineAt: now + 60_000, destinationRemoteHost: h.linkName });
    const tokens: string[] = [];
    // On the spoke mesh: an id live nowhere, and the hub lead's id claimed from the spoke.
    for (const [label, from] of [["unknown", { id: `session:${randomUUID()}`, name: "nobody", kind: "main" }], ["hub root", identity(h.fileMain)]] as const) {
      const token = `SPOOFED_SPOKE_${label.replace(" ", "_")}_${randomUUID()}`;
      tokens.push(token);
      await spoke.runtime.mesh.publish({ topic: CONTROL, kind: "followUp", from, to: h.natsMain.id, data: command(h.natsMain.id, from.id, lead, token) });
    }
    // From the v2 machine's credential: a NATS delivery claiming to BE the spoke lane (its presence is the bridge machine's).
    const raw = await h.openPeer(NATS_MAIN_MACHINE, `spoke-spoofer-${randomUUID()}`);
    const natsToken = `SPOOFED_SPOKE_FROM_NATS_${randomUUID()}`;
    const envelope: TransportEnvelope = { id: randomUUID(), kind: "command", to: { machine: BRIDGE_MACHINE, hostId: spoke.id }, targetId: spoke.id,
      operation: "followUp", from: identity(spoke), body: { ...command(spoke.id, raw.self.hostId, lane, natsToken), destinationRemoteHost: undefined } };
    await raw.send(envelope);
    // Ordered barriers on both paths: later legitimate messages cross, so the spoofed ones were decided.
    await expect(withLog(spoke.invoke("agents.followUp", { id: h.natsMain.id, message: `RELAY_BARRIER_${randomUUID()}`, idempotencyKey: `b-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    await expect(withLog(h.natsMain.invoke("agents.followUp", { id: spoke.id, message: `RELAY_BARRIER_${randomUUID()}`, idempotencyKey: `b-${randomUUID()}` })))
      .resolves.toMatchObject({ acknowledged: true });
    for (const token of tokens) {
      expect(mentions(h.natsMain, token), token).toEqual([]);
      expect(h.faults.publishedWith(token), token).toEqual([]);
    }
    expect(mentions(spoke, natsToken)).toEqual([]);
  }, E2E_WAIT_MS * 3);
});
