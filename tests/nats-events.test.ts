import { randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { afterEach, describe, expect, it } from "vitest";
import { openBootstrapNatsConnection, openNatsConnection } from "../src/transport/nats/connection.js";
import { NatsEventsIdConflictError, openNatsEventsPublisher } from "../src/transport/nats/events-publish.js";
import { startNatsActorEventsConsumer } from "../src/transport/nats/events-consume.js";
import type { MeshEvent } from "../src/mesh/event-log.js";
import { ensureNatsEventsStream, natsEventsStreamName, natsGithubStreamName } from "../src/transport/nats/provision.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { DiscardPolicy, RetentionPolicy, StorageType, type StreamConfig } from "@nats-io/jetstream";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const fixtures: PathBHarness[] = [];
afterEach(async () => { await Promise.all(fixtures.splice(0).map(fixture => fixture.stop())); });
const waitFor = async (predicate: () => boolean, timeoutMs = 8_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for real NATS event delivery");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const fixture = async (): Promise<PathBHarness> => {
  const value = await createPathBHarness({ binary, tls: {} });
  fixtures.push(value);
  return value;
};
const open = async (server: PathBHarness, org: string) => {
  const connection = await openBootstrapNatsConnection({
    servers: `tls://localhost:${new URL(server.serverUrl).port}`,
    credentials: server.users.bootstrap.credsPath,
    tls: { caFile: server.tls!.caFile },
    name: `nats-events-${org}`,
  });
  await ensureNatsEventsStream(connection.manager, { org, provision: true });
  const publisher = await openNatsEventsPublisher(connection, { org, provision: false });
  return { connection, publisher };
};
const event = (id: string, topic: string, createdAt = Date.now()): Omit<MeshEvent, "sequence"> => ({
  id, topic, kind: "message", from: { id: "mail-service", name: "mail-service", kind: "agent" },
  data: { id }, origin: "external", untrusted: true, createdAt,
});

describe("NATS event stream names", () => {
  it("normalizes GitHub login slugs with uppercase and underscores", () => {
    expect(natsEventsStreamName("smarty-pants-inc")).toBe("FABRIC_EV_SMARTY_PANTS_INC");
    expect(natsEventsStreamName("versehq")).toBe("FABRIC_EV_VERSEHQ");
    expect(natsGithubStreamName("smarty-pants-inc")).toBe("GITHUB_SMARTY_PANTS_INC");
    expect(natsGithubStreamName("versehq")).toBe("GITHUB_VERSEHQ");
    expect(() => natsEventsStreamName("sys")).toThrow();
  });
});

describe.skipIf(!binary)("NATS Mesh events on a real JetStream server", () => {
  it("uses opt-in agent grants for stream inspection, publishing, and actor consumption", async () => {
    const org = `events-${randomUUID().slice(0, 8)}`;
    const server = await createPathBHarness({ binary, tls: {}, org, events: true });
    fixtures.push(server);
    const port = new URL(server.serverUrl).port;
    const admin = await openBootstrapNatsConnection({
      servers: `tls://localhost:${port}`,
      credentials: server.users.bootstrap.credsPath,
      tls: { caFile: server.tls!.caFile },
      name: `nats-events-admin-${org}`,
    });
    let runtime: Awaited<ReturnType<typeof openNatsConnection>> | undefined;
    let consumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      await ensureNatsEventsStream(admin.manager, { org, provision: true });
      const githubStream = natsGithubStreamName(org);
      await admin.manager.streams.add({
        name: githubStream, subjects: [`github.${org}.>`], storage: StorageType.File,
        retention: RetentionPolicy.Limits, discard: DiscardPolicy.New, max_bytes: 1_048_576,
        max_age: 0, max_msg_size: 1_048_576, max_msgs: -1, max_msgs_per_subject: -1,
        duplicate_window: 120_000_000_000, num_replicas: 1, no_ack: false, allow_direct: false,
      } as StreamConfig);
      runtime = await openNatsConnection({
        ...server.runtimeOptions(server.users.machineA),
        events: true,
        org,
      });
      const publisher = await openNatsEventsPublisher(runtime, { org, provision: false });
      const received: string[] = [];
      const githubSubject = `github.${org}.repo.pulls.shard1of1`;
      consumer = await startNatsActorEventsConsumer(runtime, {
        org,
        actorId: `${org}:agent-runtime`,
        topics: ["mail.runtime", githubSubject],
        onMessage: async (message, _messageId, ack) => { received.push(message.id); await ack(); },
      });
      await publisher.publish(event("agent-runtime-event", "mail.runtime"));
      await admin.js.publish(githubSubject, Buffer.from(JSON.stringify(event("agent-github-event", githubSubject))));
      await waitFor(() => received.length === 2);
      expect(received).toEqual(expect.arrayContaining(["agent-runtime-event", "agent-github-event"]));
    } finally {
      await consumer?.close(false);
      await runtime?.close();
      await admin.close();
    }
  }, 30_000);

  it("provisions the bounded org stream, dedupes one ID, and fans out to per-actor durables", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    const consumers: Array<{ close(remove?: boolean): Promise<void> }> = [];
    try {
      const info = await connection.manager.streams.info(publisher.stream);
      expect(info.config.subjects).toEqual([`fabric.ev.${org}.>`]);
      expect(info.config.retention).toBe("limits");
      expect(info.config.storage).toBe("file");
      expect(info.config.discard).toBe("new");
      expect(info.config.max_age).toBe(0);
      expect(info.config.max_bytes).toBe(2_147_483_648);
      expect(info.config.duplicate_window).toBe(120_000_000_000);
      expect(info.config.num_replicas).toBe(1);

      const stableEnvelope = event("stable-mail-event", "mail.inbound");
      const first = await publisher.publishWithAck(stableEnvelope);
      const retry = await publisher.publishWithAck({ ...stableEnvelope, createdAt: stableEnvelope.createdAt + 1_000 });
      expect(first).toMatchObject({ duplicate: false, event: { id: "stable-mail-event", sequence: 1 } });
      expect(retry).toMatchObject({ duplicate: true, event: { id: "stable-mail-event", sequence: 1 } });
      expect(retry.event.createdAt).toBe(stableEnvelope.createdAt);
      await expect(publisher.publishWithAck({ ...stableEnvelope, data: { id: "changed" } }))
        .rejects.toBeInstanceOf(NatsEventsIdConflictError);

      const deliveredA: Array<{ id: string; seq: number }> = [];
      const deliveredB: Array<{ id: string; seq: number }> = [];
      consumers.push(await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:actor-a`, topics: ["mail.inbound"],
        onMessage: async (received, messageId, ack) => { deliveredA.push({ id: `${received.id}:${messageId}`, seq: received.sequence }); await ack(); },
      }));
      consumers.push(await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:actor-b`, topics: ["mail.inbound"],
        onMessage: async (received, messageId, ack) => { deliveredB.push({ id: `${received.id}:${messageId}`, seq: received.sequence }); await ack(); },
      }));
      await waitFor(() => deliveredA.length === 1 && deliveredB.length === 1);
      expect(deliveredA).toEqual([{ id: "stable-mail-event:stable-mail-event", seq: 1 }]);
      expect(deliveredB).toEqual([{ id: "stable-mail-event:stable-mail-event", seq: 1 }]);
      const eventConsumerInfo = await connection.manager.consumers.info(publisher.stream, `ev_${hashToken(`${org}:actor-a`)}`);
      expect(eventConsumerInfo.config.filter_subjects).toEqual([`fabric.ev.${org}.mail.inbound`, `fabric.ev.${org}.wake.${org}:actor-a`].sort());
      await publisher.publish(event("wake-for-actor-a", `wake.${org}:actor-a`));
      await waitFor(() => deliveredA.length === 2);
      expect(deliveredA[1]).toEqual({ id: "wake-for-actor-a:wake-for-actor-a", seq: 2 });
      expect(deliveredB).toHaveLength(1);

      const noIdReceived: string[] = [];
      consumers.push(await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:actor-no-header-id`, topics: ["mail.paul"],
        onMessage: async (received, messageId, ack) => { noIdReceived.push(`${received.id}:${messageId}`); await ack(); },
      }));
      const raw = event("event-without-msg-id", "mail.paul");
      const rawAck = await connection.js.publish(`fabric.ev.${org}.mail.paul`, Buffer.from(JSON.stringify(raw)));
      await waitFor(() => noIdReceived.length === 1);
      expect(noIdReceived).toEqual([`event-without-msg-id:${rawAck.seq}`]);
    } finally {
      await Promise.all(consumers.map(consumer => consumer.close(false)));
      await connection.close();
    }
  }, 30_000);

  it("uses stream-wide sequence CAS across concurrent publisher connections", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const first = await open(server, org);
    const second = await open(server, org);
    try {
      const inputs = Array.from({ length: 4 }, (_, index) => event(`concurrent-${index}`, `mail.concurrent.${index}`));
      const results = await Promise.all(inputs.map((input, index) =>
        (index % 2 === 0 ? first.publisher : second.publisher).publishWithAck(input)));
      expect(results.map(result => result.event.sequence).sort((a, b) => a - b)).toEqual([1, 2, 3, 4]);
      expect(results.every(result => !result.duplicate)).toBe(true);
      const info = await first.connection.manager.streams.info(first.publisher.stream);
      expect(info.state.last_seq).toBe(4);
    } finally {
      await Promise.all([first.connection.close(), second.connection.close()]);
    }
  }, 30_000);

  it("routes GitHub projections to the existing GitHub stream and keeps wake filters on Fabric durables", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    let consumer: { close(remove?: boolean): Promise<void> } | undefined;
    try {
      const githubStream = natsGithubStreamName(org);
      await connection.manager.streams.add({
        name: githubStream, subjects: [`github.${org}.>`], storage: StorageType.File,
        retention: RetentionPolicy.Limits, discard: DiscardPolicy.New, max_bytes: 1_048_576,
        max_age: 0, max_msg_size: 1_048_576, max_msgs: -1, max_msgs_per_subject: -1,
        duplicate_window: 120_000_000_000, num_replicas: 1, no_ack: false, allow_direct: false,
      } as StreamConfig);
      const actorId = `${org}:mixed-actor`;
      const received: string[] = [];
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.inbound", `github.${org}.repo.pulls.shard1of1`],
        onMessage: async (receivedEvent, messageId, ack) => { received.push(`${receivedEvent.topic}:${messageId}`); await ack(); },
      });
      const evInfo = await connection.manager.consumers.info(natsEventsStreamName(org), `ev_${hashToken(actorId)}`);
      const ghInfo = await connection.manager.consumers.info(githubStream, `gh_${hashToken(actorId)}`);
      expect(evInfo.config.filter_subjects).toEqual([`fabric.ev.${org}.mail.inbound`, `fabric.ev.${org}.wake.${actorId}`].sort());
      expect(ghInfo.config.filter_subjects).toEqual([`github.${org}.repo.pulls.shard1of1`]);

      await publisher.publish(event("fabric-copy", "mail.inbound"));
      const subject = `github.${org}.repo.pulls.shard1of1`;
      const githubEvent = event("github-copy", subject);
      const githubAck = await connection.js.publish(subject, Buffer.from(JSON.stringify(githubEvent)));
      await waitFor(() => received.length === 2);
      expect(received).toEqual(expect.arrayContaining([`mail.inbound:fabric-copy`, `${subject}:${githubAck.seq}`]));
    } finally {
      await consumer?.close(false);
      await connection.close();
    }
  }, 30_000);

  it("updates an actor's exact Fabric filter on the same durable", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    const actorId = `${org}:topic-update`;
    const seen: string[] = [];
    let consumer: { updateTopics(topics: readonly string[], active?: boolean): Promise<void>; close(remove?: boolean): Promise<void> } | undefined;
    try {
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.before"],
        onMessage: async (received, _messageId, ack) => { seen.push(received.topic); await ack(); },
      });
      await publisher.publish(event("before-update", "mail.before"));
      await waitFor(() => seen.length === 1);
      await consumer.updateTopics(["mail.after"]);
      const info = await connection.manager.consumers.info(natsEventsStreamName(org), `ev_${hashToken(actorId)}`);
      expect(info.config.filter_subjects).toEqual([`fabric.ev.${org}.mail.after`, `fabric.ev.${org}.wake.${actorId}`].sort());
      await publisher.publish(event("old-filter", "mail.before"));
      await publisher.publish(event("new-filter", "mail.after"));
      await waitFor(() => seen.length === 2);
      expect(seen).toEqual(["mail.before", "mail.after"]);
    } finally {
      await consumer?.close(false);
      await connection.close();
    }
  }, 30_000);

  it("applies eventsStartAt only to a new durable and redelivers an unaccepted event", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    let consumer: { close(remove?: boolean): Promise<void> } | undefined;
    try {
      await publisher.publish(event("before-start", "mail.paul"));
      const startAt = new Date(Date.now() + 100).toISOString();
      const seen: string[] = [];
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:start-floor`, topics: ["mail.paul"], eventsStartAt: startAt,
        onMessage: async (received, _messageId, ack) => {
          seen.push(received.id);
          if (received.id === "after-start" && seen.filter(id => id === "after-start").length === 1) {
            throw new Error("simulate failure before durable queue acceptance");
          }
          await ack();
        },
      });
      await new Promise(resolve => setTimeout(resolve, 150));
      await publisher.publish(event("after-start", "mail.paul"));
      await waitFor(() => seen.filter(id => id === "after-start").length >= 2);
      expect(seen).toEqual(["after-start", "after-start"]);

      await consumer.close(false);
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:start-floor`, topics: ["mail.paul"], eventsStartAt: new Date(0).toISOString(),
        onMessage: async (received, _messageId, ack) => { seen.push(received.id); await ack(); },
      });
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(seen).toEqual(["after-start", "after-start"]);
    } finally {
      await consumer?.close(false);
      await connection.close();
    }
  }, 30_000);
});
