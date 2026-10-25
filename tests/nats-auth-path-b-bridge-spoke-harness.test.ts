// expected-skip: no NATS_BIN/PATH executable: a private authenticated loopback broker is required.
// Keep reason: smarty-dev#15310 A' PR 2. Self-test of the two fixtures the PR 2 E2E builds on, with no A' link:
// a work-host spoke whose real lane Main crosses a real hub<->spoke file bridge, and a v2 Main whose fleet.work
// publication really lands in the org's Fabric event stream (the N4 source).
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";
import { openBootstrapNatsConnection } from "../src/transport/nats/connection.js";
import { natsEventsStreamName } from "../src/transport/nats/provision.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  createMixedHarness, E2E_WAIT_MS, HUB_NAME, liveRow, SPOKE_LINK_NAME, waitForValue, type MixedHarness, type MixedMain,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the mixed bridge harness needs NATS_BIN");
const mentions = (main: MixedMain, token: string) => main.delivered().filter((row) => row.text.includes(token));

describe.skipIf(!binary || process.platform !== "linux")("A' PR 2 fixtures: spoke bridge and org event stream (no link)", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = await createMixedHarness({ binary: binary!, startLink: null, spoke: true, events: true }); }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  it("the spoke lane root and the hub lead reach each other over the real hub<->spoke file bridge", async () => {
    const spoke = h.spoke!.main;
    await waitForValue(() => liveRow(h.fileMain, spoke.id)?.remoteHost === SPOKE_LINK_NAME ? true : undefined, "spoke root mirrored into the hub", E2E_WAIT_MS);
    await waitForValue(() => liveRow(spoke, h.fileMain.id)?.remoteHost === HUB_NAME ? true : undefined, "hub root mirrored into the spoke", E2E_WAIT_MS);
    const toSpoke = `HUB_TO_SPOKE_${randomUUID()}`, toHub = `SPOKE_TO_HUB_${randomUUID()}`;
    await expect(h.fileMain.invoke("agents.followUp", { id: spoke.id, message: toSpoke, idempotencyKey: `s-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    await expect(spoke.invoke("agents.followUp", { id: h.fileMain.id, message: toHub, idempotencyKey: `h-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    expect(mentions(spoke, toSpoke)).toEqual([expect.objectContaining({ from: expect.objectContaining({ id: h.fileMain.id }) })]);
    expect(mentions(h.fileMain, toHub)).toEqual([expect.objectContaining({ from: expect.objectContaining({ id: spoke.id }) })]);
  }, E2E_WAIT_MS * 3);

  it("a v2 Main's fleet.work.tasks publication lands once in FABRIC_EV_<org>", async () => {
    const token = `V2_TASK_DONE_${randomUUID()}`;
    const from: MeshIdentity = { id: h.natsMain.id, name: h.natsMain.label, kind: "main", sessionId: h.natsMain.sessionId };
    const published = await h.natsMain.runtime.mesh.publish({ topic: "fleet.work.tasks", kind: "completed", from, to: "factory", text: token });
    const bootstrap = await openBootstrapNatsConnection({ servers: `tls://localhost:${new URL(h.broker.serverUrl).port}`,
      credentials: h.broker.users.bootstrap.credsPath, tls: { caFile: h.broker.tls!.caFile }, name: "spoke-harness-events-reader" });
    try {
      const stream = natsEventsStreamName(h.org!);
      const stored = await waitForValue(async () => {
        const last = (await bootstrap.manager.streams.info(stream)).state.last_seq;
        const found: MeshEvent[] = [];
        for (let seq = 1; seq <= last; seq++) {
          const message = await bootstrap.manager.streams.getMessage(stream, { seq }).catch(() => undefined);
          const event = message?.json<MeshEvent>();
          if (event?.text === token) found.push(event);
        }
        return found.length ? found : undefined;
      }, "the v2 publication in the org event stream", E2E_WAIT_MS, 200);
      expect(stored).toHaveLength(1);
      expect(stored[0]).toMatchObject({ id: published.id, topic: "fleet.work.tasks", kind: "completed", to: "factory", from: { id: h.natsMain.id } });
    } finally { await bootstrap.close(); }
  }, E2E_WAIT_MS * 2);
});
