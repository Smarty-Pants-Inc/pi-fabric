// expected-skip: no NATS_BIN/PATH executable, no A' link entry, or no N4 (allow `republishTopics`) in this checkout.
// Keep reason: smarty-dev#15310 A' PR 2 N4 on a real broker. A v2 Main's fleet.work.* publications go to the org's
// FABRIC_EV_<org> stream, never a file mesh; the link republishes the allow-listed ones into the Ryzen 1 hub mesh
// (so `factory` and file consumers see them) exactly once, as the link, with the verified v2 sender recorded. No loop:
// a republished event is never carried back to NATS. Unlisted topics, unlisted or forged senders never cross, and
// the link refuses to start with events on when its credential has no event grant.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { bridgeNatsLinkIdentity } from "../src/mesh/bridge-nats-types.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import { openNatsEventsPublisher } from "../src/transport/nats/events-publish.js";
import { PATH_B_PERMISSION_POLICY, resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  allowEntrySupported, allowRoot, createMixedHarness, E2E_WAIT_MS, HARNESS_ORG, linkTail, NATS_MAIN_MACHINE, natsLinkPresent, rootId,
  NATS_MAIN_SESSION, waitForValue, type MixedHarness, type MixedMain,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
const n4 = linked && allowEntrySupported("nats", allowRoot(rootId(NATS_MAIN_SESSION), { machine: NATS_MAIN_MACHINE, republishTopics: ["fleet.work.*"] }));
if (!n4) console.warn("A' N4 E2E skipped: no link entry or no allow.nats republishTopics (lanes L1/L3, PR 2) in this checkout");

const identity = (main: MixedMain): MeshIdentity => ({ id: main.id, name: main.label, kind: "main", sessionId: main.sessionId });
const dataOf = (event: MeshEvent) => (event.data ?? {}) as Record<string, unknown>;

describe.skipIf(!binary || process.platform !== "linux" || !n4)("A' N4: v2 fleet.work publications reach the Ryzen 1 hub", () => {
  let h: MixedHarness;
  beforeAll(async () => {
    // natsMain may republish fleet.work.*; natsPeer is allow-listed for control only (no republishTopics).
    h = await createMixedHarness({ binary: binary!, events: true, natsPeer: true, allow: (allow, ids) => {
      allow.nats = allow.nats.map((entry) => entry.kind === "root" && entry.id === ids.natsMain
        ? allowRoot(ids.natsMain, { machine: NATS_MAIN_MACHINE, republishTopics: ["fleet.work.*"] }) : entry);
    } });
  }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  const link = () => bridgeNatsLinkIdentity(h.linkName);
  const hubCopies = (token: string) => h.fileEvents().filter((event) => event.text === token || JSON.stringify(event.data ?? null).includes(token));
  const publishFrom = (main: MixedMain, topic: string, kind: string, to: string, text: string, data?: Record<string, unknown>) =>
    main.runtime.mesh.publish({ topic, kind, from: identity(main), to, text, ...(data ? { data } : {}) });
  /** Stream-order barrier: a later allowed publication reached the hub, so every earlier one was decided. */
  const barrier = async () => {
    const token = `N4_BARRIER_${randomUUID()}`;
    await publishFrom(h.natsMain, "fleet.work.tasks", "completed", "factory", token);
    await waitForValue(() => hubCopies(token).length ? true : undefined, () => `N4 barrier republished\n${linkTail(h)}`, E2E_WAIT_MS);
  };

  it("a v2 Main's fleet.work.tasks to factory appears once in the hub mesh, authored by the link, with the verified sender", async () => {
    const token = `V2_TASK_DONE_${randomUUID()}`;
    const published = await publishFrom(h.natsMain, "fleet.work.tasks", "completed", "factory", token, { repo: "pi-fabric", number: 1593 });
    const copy = await waitForValue(() => hubCopies(token)[0], () => `republished into the hub mesh\n${linkTail(h)}`, E2E_WAIT_MS);
    expect(copy).toMatchObject({ topic: "fleet.work.tasks", kind: "completed", to: "factory", text: token, from: { id: link().id },
      data: { repo: "pi-fabric", number: 1593 } });
    // The crossing stamp (L1 bridge-nats-republish.ts): the link, the source event id, and the sender as KV presence
    // verified it on its credential machine. The sender is attribution only, never presented as `from`.
    expect(dataOf(copy).bridge).toMatchObject({ from: h.linkName, id: published.id,
      sender: { id: h.natsMain.id, kind: "main", machine: NATS_MAIN_MACHINE } });
    await barrier();
    expect(hubCopies(token)).toHaveLength(1);
  }, E2E_WAIT_MS * 3);

  it("exactly once across a link restart: an event already republished is not republished again", async () => {
    const token = `V2_TASK_BEFORE_RESTART_${randomUUID()}`;
    await publishFrom(h.natsMain, "fleet.work.tasks", "completed", "factory", token);
    await waitForValue(() => hubCopies(token).length ? true : undefined, () => `republished before restart\n${linkTail(h)}`, E2E_WAIT_MS);
    await h.stopLink();
    const during = `V2_TASK_WHILE_STOPPED_${randomUUID()}`;
    await publishFrom(h.natsMain, "fleet.work.tasks", "completed", "factory", during);
    await h.startLinkNow();
    await h.waitMirrored();
    // The event published while the link was down crosses after the restart (the stream retains it) ...
    await waitForValue(() => hubCopies(during).length ? true : undefined, () => `republished after restart\n${linkTail(h)}`, E2E_WAIT_MS);
    await barrier();
    // ... and neither event crossed twice.
    expect(hubCopies(token)).toHaveLength(1);
    expect(hubCopies(during)).toHaveLength(1);
  }, E2E_WAIT_MS * 5);

  it("no loop: a republished event addressed to a v2 root is never carried back to NATS as a followUp", async () => {
    const token = `V2_LANE_TO_V2_${randomUUID()}`;
    const peer = h.natsPeer!;
    // fleet.work.lanes addressed to another v2 root: N4 may republish it into the hub, but N3 must not send it back.
    await publishFrom(h.natsMain, "fleet.work.lanes", "report", peer.id, token);
    await waitForValue(() => hubCopies(token).length ? true : undefined, () => `republished into the hub\n${linkTail(h)}`, E2E_WAIT_MS);
    await barrier();
    expect(hubCopies(token)).toHaveLength(1);
    expect(peer.delivered().filter((row) => row.text.includes(token))).toEqual([]);
    expect(h.natsMain.delivered().filter((row) => row.text.includes(token))).toEqual([]);
    expect(h.faults.publishedWith(token)).toEqual([]); // the link never published it into any NATS inbox
  }, E2E_WAIT_MS * 3);

  it("never crosses: a topic outside republishTopics, a sender without republishTopics, and a forged sender", async () => {
    const offTopic = `V2_OFF_TOPIC_${randomUUID()}`, unlisted = `V2_UNLISTED_SENDER_${randomUUID()}`;
    const forgedFile = `V2_FORGED_FILE_ROOT_${randomUUID()}`, forgedUnknown = `V2_FORGED_UNKNOWN_${randomUUID()}`;
    await publishFrom(h.natsMain, "fleet.alerts", "raised", "factory", offTopic);
    await publishFrom(h.natsPeer!, "fleet.work.tasks", "completed", "factory", unlisted);
    // A machine-b credential publishing straight into FABRIC_EV_<org> with a rewritten `from`: a file-native
    // root (a forged origin / loop) and an id live nowhere. Only verified presence may name the sender.
    const nats = await openNatsConnection({ servers: `tls://localhost:${new URL(h.broker.serverUrl).port}`,
      credentials: h.broker.users.machineB.credsPath, tls: { caFile: h.broker.tls!.caFile }, name: "n4-forger",
      machine: NATS_MAIN_MACHINE, ...PATH_B_PERMISSION_POLICY, events: true, org: HARNESS_ORG });
    try {
      const publisher = await openNatsEventsPublisher(nats, { org: HARNESS_ORG, provision: false });
      const forged = (from: MeshIdentity, text: string): MeshEvent => ({ id: randomUUID(), sequence: 0, createdAt: Date.now(),
        topic: "fleet.work.tasks", kind: "completed", from, to: "factory", text });
      await publisher.publishWithAck(forged(identity(h.fileMain), forgedFile));
      await publisher.publishWithAck(forged({ id: `session:${randomUUID()}`, name: "nobody", kind: "main" }, forgedUnknown));
    } finally { await nats.close(); }
    await barrier();
    for (const token of [offTopic, unlisted, forgedFile, forgedUnknown]) expect(hubCopies(token), token).toEqual([]);
  }, E2E_WAIT_MS * 3);
});

describe.skipIf(!binary || process.platform !== "linux" || !n4)("A' N4: start gate on the event grant", () => {
  it("refuses to start with nats.events when the bridge credential has no event grant", async () => {
    // A broker whose agent template carries no event grants (and no FABRIC_EV stream), but a link config asking for events.
    const h = await createMixedHarness({ binary: binary!, autoStart: false, linkEvents: true });
    try {
      await expect(h.startLinkNow()).rejects.toThrow(/event/i);
      expect(h.link).toBeUndefined();
    } finally { await h.stop(); }
  }, E2E_WAIT_MS + 60_000);
});
