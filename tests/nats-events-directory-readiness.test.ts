// Keep: smarty-dev#17152 dropped review events at the directory/JetStream boundary.
// Use the nats-auth real-broker harness; no live fleet state or inference is involved.
// Expected-skip: no nats-server executable: NATS_AUTH_REQUIRED=1 makes missing broker fatal.
import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AckPolicy, DeliverPolicy, ReplayPolicy } from "@nats-io/jetstream";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { openBootstrapNatsConnection } from "../src/transport/nats/connection.js";
import { startNatsActorEventsConsumer } from "../src/transport/nats/events-consume.js";
import { openNatsEventsPublisher } from "../src/transport/nats/events-publish.js";
import { ensureNatsEventsStream } from "../src/transport/nats/provision.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { createPathBHarness, PATH_B_PERMISSION_POLICY, resolveNatsBinary } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  try { for (const close of cleanup.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const waitFor = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 8_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for directory/JetStream recovery");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const event = (id: string): Omit<MeshEvent, "sequence"> => ({
  id, topic: "review.ready", kind: "message", from: { id: "review-service", name: "review-service", kind: "agent" },
  data: { id }, createdAt: Date.now(),
});
const fixture = async () => {
  const org = `directory-${randomUUID().slice(0, 8)}`;
  const server = await createPathBHarness({ binary, tls: {}, org, events: true });
  cleanup.push(() => server.stop());
  const connection = await openBootstrapNatsConnection({
    servers: `tls://localhost:${new URL(server.serverUrl).port}`, credentials: server.users.bootstrap.credsPath,
    tls: { caFile: server.tls!.caFile },
  });
  cleanup.push(() => connection.close());
  await ensureNatsEventsStream(connection.manager, { org, provision: true });
  const publisher = await openNatsEventsPublisher(connection, { org, provision: false });
  // Capture standard broker advisories in a stream (the scoped bootstrap user
  // deliberately cannot subscribe directly to arbitrary core-NATS subjects).
  await connection.manager.streams.add({ name: "DIRECTORY_ADVISORIES", subjects: ["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"] });
  const maxDeliveries = async () => (await connection.manager.streams.info("DIRECTORY_ADVISORIES")).state.messages;
  return { org, server, connection, publisher, maxDeliveries };
};

describe.skipIf(!binary)("directory readiness recovery on the nats-auth real broker", () => {
  it("reconciles an old max-deliver=5 durable with no backoff, gates pulls, alarms once and handles its backlog exactly once", async () => {
    const { org, connection, publisher, maxDeliveries } = await fixture();
    const actorId = `${org}:reviewer`;
    const stream = publisher.stream;
    const durable = `ev_${hashToken(actorId)}`;
    await connection.manager.consumers.add(stream, {
      durable_name: durable, filter_subjects: [`fabric.ev.${org}.review.ready`, `fabric.ev.${org}.wake.${actorId}`].sort(),
      ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
      ack_wait: 1_000_000_000, max_deliver: 5, max_ack_pending: 128, inactive_threshold: 0,
    });
    await publisher.publish(event("already-acked"));
    const legacy = await connection.js.consumers.get(stream, durable);
    const oldMessage = await legacy.next({ expires: 1_000 });
    expect(oldMessage).not.toBeNull();
    expect(await oldMessage!.ackAck()).toBe(true);
    const before = await connection.manager.consumers.info(stream, durable);
    expect(before.config.max_deliver).toBe(5);
    expect(before.config.backoff ?? []).toEqual([]);
    const inputs = ["review-1", "review-2", "review-3"];
    for (const id of inputs) await publisher.publish(event(id));
    let ready = false;
    const seen: string[] = [];
    const alarms: unknown[] = [];
    const consumer = await startNatsActorEventsConsumer(connection, {
      org, actorId, topics: ["review.ready"], directoryReady: () => ready, directoryReadyRecheckMs: 20,
      directoryStallAlarmMs: 50, onDirectoryStall: stall => { alarms.push(stall); },
      onMessage: async received => { seen.push(received.id); return "accepted"; },
    });
    cleanup.push(() => consumer.close(false));
    await waitFor(() => alarms.length === 1);
    await new Promise(resolve => setTimeout(resolve, 250));
    const gated = await connection.manager.consumers.info(stream, durable);
    expect(gated.config.max_deliver).toBe(-1);
    expect(gated.config.backoff).toEqual([1, 5, 15, 30, 30].map(seconds => seconds * 1_000_000_000));
    expect(gated.delivered).toEqual(before.delivered);
    expect(gated.ack_floor).toEqual(before.ack_floor);
    expect(gated.num_pending).toBe(3);
    expect(gated.num_ack_pending).toBe(0);
    expect(seen).toEqual([]);
    expect(alarms).toHaveLength(1);
    ready = true;
    await waitFor(async () => (await connection.manager.consumers.info(stream, durable)).num_ack_pending === 0 && seen.length === 3);
    expect(seen).toEqual(inputs);
    const settled = await connection.manager.consumers.info(stream, durable);
    expect(settled.delivered.consumer_seq).toBe(4);
    expect(settled.ack_floor.consumer_seq).toBe(4);
    expect(settled.num_pending).toBe(0);
    expect(await maxDeliveries()).toBe(0);
  }, 30_000);

  it("delays an in-hand directory refusal and stops pulling until ready, without exhausting delivery attempts", async () => {
    const { org, connection, publisher, maxDeliveries } = await fixture();
    const actorId = `${org}:in-hand`;
    const durable = `ev_${hashToken(actorId)}`;
    let ready = true;
    let refused = false;
    const accepted: string[] = [];
    const consumer = await startNatsActorEventsConsumer(connection, {
      org, actorId, topics: ["review.ready"], retryDelayScheduleMs: [50],
      directoryReady: () => ready, directoryReadyRecheckMs: 20,
      onMessage: async received => {
        if (!refused) {
          refused = true;
          ready = false;
          throw Object.assign(new Error("directory unavailable during authorization"), { code: "FABRIC_DIRECTORY_NOT_READY" });
        }
        accepted.push(received.id);
        return "accepted";
      },
    });
    cleanup.push(() => consumer.close(false));
    for (const id of ["in-hand", "behind-in-hand"]) await publisher.publish(event(id));
    await waitFor(() => refused);
    // Longer than five immediate NAK roundtrips in the incident, and five injected backoff slots.
    await new Promise(resolve => setTimeout(resolve, 350));
    const gated = await connection.manager.consumers.info(publisher.stream, durable);
    expect(gated.delivered.consumer_seq).toBe(1);
    expect(gated.num_ack_pending).toBe(1);
    expect(accepted).toEqual([]);
    expect(await maxDeliveries()).toBe(0);
    ready = true;
    await waitFor(async () => accepted.length === 2 && (await connection.manager.consumers.info(publisher.stream, durable)).num_ack_pending === 0);
    expect(accepted).toEqual(["in-hand", "behind-in-hand"]);
    expect((await connection.manager.consumers.info(publisher.stream, durable)).delivered.consumer_seq).toBe(3);
    expect(await maxDeliveries()).toBe(0);
  }, 30_000);

  it("retries setup before any durable exists, raises one diagnostic alarm, and recovers the same manager without restart", async () => {
    const { org, server, connection, publisher, maxDeliveries } = await fixture();
    const root = path.join(server.root, "actor-runtime");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const identity: MeshIdentity = { id: "session:directory-test", name: "directory-test", kind: "main", sessionId: "directory-test" };
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    cleanup.push(() => agents.close());
    let ready = true;
    let directoryRefusals = 0;
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, transport: "nats" as const,
      nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, ...PATH_B_PERMISSION_POLICY, org,
        servers: [`tls://localhost:${new URL(server.serverUrl).port}`], credentials: server.users.machineA.credsPath,
        machine: "machine-a", tls: { caFile: server.tls!.caFile }, events: false } };
    const actors = new ActorManager("directory-test", identity, mesh, meshConfig, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, releasePaused: true,
      canManageActor: () => true, canConsumeMesh: () => ready, meshOutageAlarmMs: 50,
      snapshotActorOwnership: () => {
        if (!ready) {
          directoryRefusals++;
          throw Object.assign(new Error("private directory detail"), { code: "FABRIC_DIRECTORY_NOT_READY" });
        }
        return new Map();
      },
    });
    cleanup.push(async () => { ready = true; await actors.close(); });
    const actor = await actors.create({ name: "reviewer", instructions: "Wait for review events.", topics: ["review.ready"],
      coalesce: false, residency: "durable", extensions: false });
    const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    ready = false;
    meshConfig.nats.events = true;
    await mesh.publish({ topic: "test.setup-wake", kind: "test", from: identity });
    await waitFor(() => directoryRefusals > 0);
    for (const id of ["setup-review-1", "setup-review-2"]) await publisher.publish(event(id));
    const alarms = () => warnings.mock.calls.filter(([, text]) => String(text).startsWith("fabric.alarm.mesh-outage "));
    await waitFor(() => alarms().length === 1);
    expect(directoryRefusals).toBeGreaterThanOrEqual(2);
    const durable = `ev_${hashToken(actor.id)}`;
    await expect(connection.manager.consumers.info(publisher.stream, durable)).rejects.toThrow();
    expect(warnings).toHaveBeenCalledTimes(1);
    expect(String(alarms()[0]![1])).toContain('"reason":"FABRIC_DIRECTORY_NOT_READY"');
    ready = true;
    const queued = (): { items: Array<{ payload: MeshEvent }>; natsEventReceipts: string[] } | undefined => {
      const dir = path.join(root, "actors", actor.id);
      const file = fs.readdirSync(dir).find(name => /^queue-.+\.json$/.test(name));
      return file ? JSON.parse(fs.readFileSync(path.join(dir, file), "utf8")) : undefined;
    };
    await waitFor(() => queued()?.items.length === 2);
    await waitFor(async () => (await connection.manager.consumers.info(publisher.stream, durable)).num_ack_pending === 0);
    expect(queued()!.items.map(item => item.payload.id)).toEqual(["setup-review-1", "setup-review-2"]);
    expect(queued()!.natsEventReceipts).toEqual(["setup-review-1", "setup-review-2"]);
    const settled = await connection.manager.consumers.info(publisher.stream, durable);
    expect(settled.delivered.consumer_seq).toBe(2);
    expect(settled.num_pending).toBe(0);
    expect(await maxDeliveries()).toBe(0);
    expect(alarms()).toHaveLength(1);
  }, 30_000);
});
