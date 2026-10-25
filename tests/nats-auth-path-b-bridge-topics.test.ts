// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' N3 (T7/T9/T15) on a real broker. A moved v2 lead's RootInbox reads only its
// local log, so Ryzen 1 work topics addressed to it (smarty-report lane reports, smarty-task completions,
// ops.owner notices) reach it only as a keyed followUp the link sends from its own verified KV identity,
// keyed by the file event id. Unlisted topics, unaddressed broadcasts and other recipients never cross.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { bridgeNatsCommandId, bridgeNatsLinkIdentity, bridgeNatsTopicIdempotencyKey } from "../src/mesh/bridge-nats-types.js";
import type { MeshIdentity } from "../src/mesh/store.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  allowRoot, BRIDGE_MACHINE, createMixedHarness, linkTail, NATS_MAIN_MACHINE, natsLinkPresent, waitForValue, type MixedHarness, type WireEvent,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
if (!linked) console.warn("A' bridge topic E2E skipped: src/mesh/bridge-nats-link.ts (lane L2) is not in this checkout");

const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
/** The Ryzen 1 publishers are tools, not participants (bin/smarty-report relay, smarty-task watcher, paul_inbox.py). */
const REPORT_RELAY: MeshIdentity = { id: "fleet-tool:smarty-report", name: "smarty-report-relay", kind: "main" };
const OWNER_INBOX: MeshIdentity = { id: "fleet-tool:paul-inbox", name: "paul-inbox", kind: "main" };

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link: addressed Ryzen 1 work topics reach a v2 lead", () => {
  let h: MixedHarness;
  beforeAll(async () => {
    h = await createMixedHarness({ binary: binary!, natsPeer: true, allow: (allow, ids) => {
      allow.nats = allow.nats.map((entry) => entry.kind === "root" && entry.id === ids.natsMain
        ? allowRoot(ids.natsMain, { machine: NATS_MAIN_MACHINE, receiveTopics: ["fleet.work.*", "ops.owner"] }) : entry);
    } });
  }, 90_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  const link = () => bridgeNatsLinkIdentity(h.linkName);
  const publish = (from: MeshIdentity, topic: string, kind: string, to: string | undefined, text: string) =>
    h.fileMain.runtime.mesh.publish({ topic, kind, from, ...(to === undefined ? {} : { to }), text });

  it("a lane report addressed to the moved lead arrives once as a followUp from the link, keyed by the file event id", async () => {
    const text = `READY lane L4 ${randomUUID()}`;
    const mark = h.wire.mark();
    const event = await publish(REPORT_RELAY, "fleet.work.lanes", "report", h.natsMain.id, text);
    const row = await waitForValue(() => h.natsMain.delivered().find((entry) => entry.text.includes(text)),
      `lane report delivered to the v2 lead\n${linkTail(h)}`, 20_000);
    expect(row).toMatchObject({ deliverAs: "followUp", from: { id: link().id } });
    expect(row.text).toContain(`[mesh fleet.work.lanes/report from ${REPORT_RELAY.id}]`);
    // The link's own command: from its verified KV identity on the bridge machine, keyed by the event id.
    const command = h.wire.since(mark).find((entry) => entry.phase === "deliver" && entry.hostId === h.natsMain.id &&
      entry.envelope.kind === "command" && String(bodyOf(entry).message).includes(text))!;
    expect(command).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { from: { id: link().id }, operation: "followUp" } });
    expect(bodyOf(command).commandId).toBe(bridgeNatsCommandId({ hostId: link().id, ownerHostId: h.natsMain.id, ownerIdentityId: h.natsMain.id,
      targetId: h.natsMain.id, operation: "followUp", idempotencyKey: bridgeNatsTopicIdempotencyKey(h.linkName, event.id) }));
    expect(h.natsMain.delivered().filter((entry) => entry.text.includes(text))).toHaveLength(1);
  }, 45_000);

  it("an ops.owner alarm crosses; pr.wake, an unlisted topic, a broadcast and a report to an unlisted recipient never do", async () => {
    const notice = `inbox age alarm ${randomUUID()}`, github = `GITHUB_${randomUUID()}`, broadcast = `BROADCAST_${randomUUID()}`,
      other = `OTHER_${randomUUID()}`, wake = `PR_WAKE_${randomUUID()}`;
    const relayMark = h.faults.inboxFrames.length;
    await publish(REPORT_RELAY, "github.pull_request", "opened", h.natsMain.id, github);
    await publish(REPORT_RELAY, "fleet.work.lanes", "report", undefined, broadcast);
    await publish(REPORT_RELAY, "fleet.work.lanes", "report", h.natsPeer!.id, other); // natsPeer has no receiveTopics
    // pr.wake registers a PR with the hub factory; it is not Main work (L2 bridge-nats-topics.ts OWNER_KINDS).
    await publish(OWNER_INBOX, "ops.owner", "pr.wake", h.natsMain.id, wake);
    await publish(OWNER_INBOX, "ops.owner", "inbox.age.alarm", h.natsMain.id, notice);
    // Ordered barrier: the later allowed notice arrived, so every earlier event was already decided.
    await waitForValue(() => h.natsMain.delivered().find((entry) => entry.text.includes(notice)), `ops.owner notice delivered\n${linkTail(h)}`, 20_000);
    expect(h.natsMain.delivered().filter((entry) => [github, broadcast, wake].some((token) => entry.text.includes(token)))).toEqual([]);
    expect(h.natsPeer!.delivered().filter((entry) => entry.text.includes(other))).toEqual([]);
    // Nothing for them was even published by the link (decided before the barrier, in file-log order).
    for (const token of [github, broadcast, other, wake]) expect(h.faults.publishedWith(token, relayMark), token).toEqual([]);
    expect(h.faults.publishedWith(notice, relayMark)).toHaveLength(1);
  }, 45_000);
});
