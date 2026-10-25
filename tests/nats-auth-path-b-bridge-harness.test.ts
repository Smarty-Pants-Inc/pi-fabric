// expected-skip: no NATS_BIN/PATH executable: a private authenticated loopback broker is required.
// Keep reason: smarty-dev#15310 A' (D3 gate 3). Self-test of the mixed FILE <-> NATS harness that every
// tests/nats-auth-path-b-bridge-*.test.ts E2E uses: both real Mains boot on one real broker and stay
// isolated with no link, and the link's fault relay really disconnects and really loses a PubAck.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { TransportEnvelope } from "../src/transport/types.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import { BRIDGE_MACHINE, createMixedHarness, NATS_MAIN_MACHINE, selfRecord, waitForValue, type MixedHarness } from "./helpers/nats-file-mixed.js";
import { publishTo } from "./helpers/nats-wire-faults.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the mixed bridge harness needs NATS_BIN");

describe.skipIf(!binary || process.platform !== "linux")("A' mixed FILE <-> NATS harness (no link)", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = await createMixedHarness({ binary: binary!, startLink: null }); }, 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  it("boots a real FILE Main and a real NATS Main on one broker, each visible only on its own side", async () => {
    const file = selfRecord(h.fileMain), nats = selfRecord(h.natsMain);
    expect(file).toMatchObject({ id: h.fileMain.id, kind: "root", stale: false, ownerIdentityId: h.fileMain.id, sessionId: h.fileMain.sessionId });
    expect(file!.remoteHost).toBeUndefined();
    expect(nats).toMatchObject({ id: h.natsMain.id, kind: "root", stale: false, machine: NATS_MAIN_MACHINE, sessionId: h.natsMain.sessionId });
    // The NATS Main's own KV presence and lease, under its credential-bound machine only.
    const kv = await waitForValue(async () => {
      const row = await h.kvRoot(NATS_MAIN_MACHINE, h.natsMain.id);
      return row?.presence && row.lease ? row : undefined;
    }, "NATS Main presence + lease in KV", 5_000);
    expect(kv.presence).toMatchObject({ id: h.natsMain.id, kind: "root", ownerIncarnation: nats!.ownerIncarnation });
    expect(kv.lease).toMatchObject({ id: h.natsMain.id, incarnation: nats!.ownerIncarnation });
    // No link: neither root crosses. Nothing stands in for the carrier.
    expect(await h.kvRoot(BRIDGE_MACHINE, h.fileMain.id)).toBeUndefined();
    expect(h.fileMain.runtime.participantInfos({ fresh: true, includeStale: true }).some((row) => row.id === h.natsMain.id)).toBe(false);
    expect(h.natsMain.runtime.participantInfos({ fresh: true, includeStale: true }).some((row) => row.id === h.fileMain.id)).toBe(false);
    await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: "NO_LINK_FILE_TO_NATS" }))
      .rejects.toThrow(`Unknown Fabric participant: ${h.natsMain.id}`);
    expect(h.natsMain.delivered()).toEqual([]);
    expect(h.fileMain.delivered()).toEqual([]);
  }, 20_000);

  it("the link's relay severs a real authenticated connection, which reconnects, and loses a real PubAck once", async () => {
    const sender = await h.openPeer(BRIDGE_MACHINE, `relay-probe-${randomUUID()}`, true);
    const receiver = await h.openPeer(NATS_MAIN_MACHINE, `relay-sink-${randomUUID()}`);
    await h.faults.whenClients(1);
    const envelope = (id: string): TransportEnvelope => ({ id, kind: "command", to: receiver.self, targetId: `target:${id}`,
      operation: "followUp", from: { id: sender.self.hostId, name: "relay-probe", kind: "main" }, body: { probe: id } });
    const accepts = h.faults.accepts;
    expect(h.faults.sever()).toBeGreaterThan(0);
    await h.faults.whenClients(1);
    // The product client reconnects through the same relay and re-verifies; a send then succeeds.
    const first = await waitForValue(async () => {
      try { return await sender.send(envelope(`after-sever-${randomUUID()}`)); } catch { return undefined; }
    }, "send after a forced disconnect", 15_000, 200);
    expect(first.duplicate).toBe(false);
    expect(h.faults.accepts).toBeGreaterThan(accepts);
    // A lost PubAck: the broker stores the message but its receipt never reaches the sender. The
    // product transport resends the frozen bytes under the same id and the broker reports a duplicate.
    h.faults.losePubAcks(publishTo(NATS_MAIN_MACHINE, "cmd"));
    const lost = envelope(`lost-puback-${randomUUID()}`);
    const before = h.faults.inboxPublishes.length;
    const sent = await sender.send(lost);
    expect(sent).toMatchObject({ id: lost.id, duplicate: true });
    expect(h.faults.pubAckDrops).toBe(1);
    expect(h.faults.inboxPublishes.length - before).toBe(2);
  }, 40_000);
});
