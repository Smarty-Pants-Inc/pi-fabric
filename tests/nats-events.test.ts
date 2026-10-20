import { randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { afterEach, describe, expect, it } from "vitest";
import { openBootstrapNatsConnection, openNatsConnection } from "../src/transport/nats/connection.js";
import { NatsEventsIdConflictError, openNatsEventsPublisher } from "../src/transport/nats/events-publish.js";
import { NATS_EVENT_RETRY_DELAY_SCHEDULE_MS, startNatsActorEventsConsumer } from "../src/transport/nats/events-consume.js";
import type { MeshEvent } from "../src/mesh/event-log.js";
import { ensureNatsEventsStream, ensureTransportStream, natsEventsStreamName, natsGithubStreamName, transportStreamConfig } from "../src/transport/nats/provision.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { createPathBHarness, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType, type StreamConfig, type StreamInfo } from "@nats-io/jetstream";

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
  it("uses a bounded default transient retry schedule", () => {
    expect(NATS_EVENT_RETRY_DELAY_SCHEDULE_MS).toEqual([1_000, 5_000, 15_000, 30_000]);
  });

  it("normalizes GitHub login slugs with uppercase and underscores", () => {
    expect(natsEventsStreamName("smarty-pants-inc")).toBe("FABRIC_EV_SMARTY_PANTS_INC");
    expect(natsEventsStreamName("versehq")).toBe("FABRIC_EV_VERSEHQ");
    expect(natsGithubStreamName("smarty-pants-inc")).toBe("GITHUB_SMARTY_PANTS_INC");
    expect(natsGithubStreamName("versehq")).toBe("GITHUB_VERSEHQ");
    expect(() => natsEventsStreamName("sys")).toThrow();
  });

  it("accepts an unbounded event stream but retains the 1 MiB application publish cap", async () => {
    const org = "smarty-pants-inc";
    const stream = natsEventsStreamName(org);
    const policy = {
      name: stream, subjects: [`fabric.ev.${org}.>`], maxBytes: 2_147_483_648, maxAgeMs: 0,
      maxMessageBytes: 1_048_576, duplicateWindowMs: 120_000, replicas: 1,
      discard: DiscardPolicy.New, allowUnboundedMaxMessageBytes: true,
    } as const;
    const config = { ...transportStreamConfig(policy), max_msg_size: -1 };
    let publishes = 0;
    const info = { config, state: { last_seq: 0 } } as StreamInfo;
    const manager = { streams: { info: async () => info } };
    const js = { publish: async () => { publishes += 1; return { stream, seq: 1, duplicate: false }; } };
    const connection = { js, manager } as unknown as Parameters<typeof openNatsEventsPublisher>[0];
    const publisher = await openNatsEventsPublisher(connection, { org, provision: false });
    await expect(publisher.publish({ ...event("oversized", "mail.large"), data: { value: "x".repeat(1_048_600) } }))
      .rejects.toThrow("exceeds 1048576 bytes");
    expect(publishes).toBe(0);

    const ordinary = { ...policy, name: "FABRIC_IN_sandbox1", subjects: ["fabric.in.sandbox1.>"], allowUnboundedMaxMessageBytes: false };
    const ordinaryInfo = { config: { ...config, name: ordinary.name, subjects: [...ordinary.subjects] }, state: { last_seq: 0 } } as StreamInfo;
    const ordinaryManager = { streams: { info: async () => ordinaryInfo } } as unknown as Parameters<typeof ensureTransportStream>[0];
    await expect(ensureTransportStream(ordinaryManager, ordinary, false)).rejects.toThrow("configuration mismatch");
  });
});

describe.skipIf(!binary)("NATS Mesh events on a real JetStream server", () => {
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
        onMessage: async (received, messageId, ack) => { deliveredA.push({ id: `${received.id}:${messageId}`, seq: received.sequence }); await ack(); return "accepted"; },
      }));
      consumers.push(await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:actor-b`, topics: ["mail.inbound"],
        onMessage: async (received, messageId, ack) => { deliveredB.push({ id: `${received.id}:${messageId}`, seq: received.sequence }); await ack(); return "accepted"; },
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
        onMessage: async (received, messageId, ack) => { noIdReceived.push(`${received.id}:${messageId}`); await ack(); return "accepted"; },
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
        onMessage: async (receivedEvent, messageId, ack) => { received.push(`${receivedEvent.topic}:${messageId}`); await ack(); return "accepted"; },
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
        onMessage: async (received, _messageId, ack) => { seen.push(received.topic); await ack(); return "accepted"; },
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
    const actorId = `${org}:start-floor`;
    let consumer: { close(remove?: boolean): Promise<void> } | undefined;
    try {
      await publisher.publish(event("before-start", "mail.paul"));
      const startAt = new Date(Date.now() + 100).toISOString();
      const seen: string[] = [];
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.paul"], eventsStartAt: startAt,
        onMessage: async (received, _messageId, ack) => {
          seen.push(received.id);
          if (received.id === "after-start" && seen.filter(id => id === "after-start").length === 1) {
            throw new Error("simulate failure before durable queue acceptance");
          }
          await ack();
          return "accepted";
        },
      });
      const firstDurable = await connection.manager.consumers.info(natsEventsStreamName(org), `ev_${hashToken(actorId)}`);
      expect(firstDurable.config.deliver_policy).toBe(DeliverPolicy.StartTime);
      expect(Date.parse(firstDurable.config.opt_start_time ?? "")).toBe(Date.parse(startAt));
      await new Promise(resolve => setTimeout(resolve, 150));
      await publisher.publish(event("after-start", "mail.paul"));
      await waitFor(() => seen.filter(id => id === "after-start").length >= 2);
      expect(seen).toEqual(["after-start", "after-start"]);

      await consumer.close(false);
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.paul"], eventsStartAt: new Date(0).toISOString(),
        onMessage: async (received, _messageId, ack) => { seen.push(received.id); await ack(); return "accepted"; },
      });
      const reopenedDurable = await connection.manager.consumers.info(natsEventsStreamName(org), `ev_${hashToken(actorId)}`);
      expect(reopenedDurable.config.deliver_policy).toBe(DeliverPolicy.StartTime);
      expect(reopenedDurable.config.opt_start_time).toBe(startAt);
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(seen).toEqual(["after-start", "after-start"]);
    } finally {
      await consumer?.close(false);
      await connection.close();
    }
  }, 30_000);

  it("keeps transient ownership refusals retrying beyond five deliveries", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    const actorId = `${org}:ownership-retry`;
    const stream = natsEventsStreamName(org);
    const durable = `ev_${hashToken(actorId)}`;
    let owned = false;
    const attempts: string[] = [];
    const accepted: string[] = [];
    let consumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.retry"], retryDelayScheduleMs: [50],
        onMessage: async (received, _messageId, ack) => {
          attempts.push(received.id);
          if (!owned) return "retry";
          accepted.push(received.id);
          await ack();
          return "accepted";
        },
      });
      const initial = await connection.manager.consumers.info(stream, durable);
      expect(initial.config.max_deliver).toBe(-1);
      expect(initial.config.backoff).toEqual([50, 50].map(ms => ms * 1_000_000));
      await publisher.publish(event("ownership-blip", "mail.retry"));
      await waitFor(() => attempts.length >= 6, 8_000);
      expect(accepted).toEqual([]);
      const refused = await connection.manager.consumers.info(stream, durable);
      expect(refused.delivered?.consumer_seq).toBe(attempts.length);

      owned = true;
      await waitFor(() => accepted.length === 1, 5_000);
      await new Promise(resolve => setTimeout(resolve, 100));
      const settled = await connection.manager.consumers.info(stream, durable);
      expect(settled.delivered?.consumer_seq).toBe(attempts.length);
      expect(accepted).toEqual(["ownership-blip"]);
      expect(attempts.every(id => id === "ownership-blip")).toBe(true);
    } finally {
      await consumer?.close(false);
      await connection.close();
    }
  }, 15_000);

  it("terminates malformed poison envelopes without invoking the handler", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection } = await open(server, org);
    const actorId = `${org}:poison-event`;
    let handlerCalls = 0;
    let consumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.poison"],
        onMessage: async (_received, _messageId, ack) => { handlerCalls++; await ack(); return "accepted"; },
      });
      const stream = natsEventsStreamName(org);
      const durable = `ev_${hashToken(actorId)}`;
      const initial = await connection.manager.consumers.info(stream, durable);
      expect(initial.config.max_deliver).toBe(-1);
      await connection.js.publish(`fabric.ev.${org}.mail.poison`, Buffer.from("{"), { msgID: "malformed-poison" });
      const deadline = Date.now() + 5_000;
      let terminated = initial;
      while ((terminated.delivered?.consumer_seq ?? 0) < 1 || terminated.num_ack_pending !== 0 || terminated.num_pending !== 0) {
        if (Date.now() >= deadline) throw new Error("malformed poison event was not terminated");
        await new Promise(resolve => setTimeout(resolve, 25));
        terminated = await connection.manager.consumers.info(stream, durable);
      }
      await new Promise(resolve => setTimeout(resolve, 1_100));
      terminated = await connection.manager.consumers.info(stream, durable);
      expect(terminated.delivered?.consumer_seq).toBe(1);
      expect(terminated.num_ack_pending).toBe(0);
      expect(terminated.num_pending).toBe(0);
      expect(handlerCalls).toBe(0);
    } finally {
      await consumer?.close(false);
      await connection.close();
    }
  }, 15_000);

  it("redelivers an event after ownership is lost mid-delivery, while ACKing the still-owned case", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    const actorId = `${org}:ownership-handoff`;
    let oldOwner = true;
    let resolveOldEntered!: () => void;
    let resolveOldDecision!: () => void;
    let resolveOldRetry!: () => void;
    const oldEntered = new Promise<void>(resolve => { resolveOldEntered = resolve; });
    const oldDecision = new Promise<void>(resolve => { resolveOldDecision = resolve; });
    const oldRetry = new Promise<void>(resolve => { resolveOldRetry = resolve; });
    const oldSeen: string[] = [];
    const newSeen: string[] = [];
    let oldConsumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    let newConsumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      oldConsumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.owner"], retryDelayScheduleMs: [50, 100],
        onMessage: async (received, _messageId, ack) => {
          oldSeen.push(received.id);
          if (received.id === "ownership-lost") {
            resolveOldEntered();
            await oldDecision;
            if (!oldOwner) {
              resolveOldRetry();
              // A normal return without an accepted outcome must be NAKed, never ACKed.
              return undefined as never;
            }
          }
          await ack();
          return "accepted";
        },
      });
      await publisher.publish(event("still-owned", "mail.owner"));
      await waitFor(() => oldSeen.filter(id => id === "still-owned").length === 1);

      await publisher.publish(event("ownership-lost", "mail.owner"));
      await oldEntered;
      newConsumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.owner"], retryDelayScheduleMs: [50, 100],
        onMessage: async (received, _messageId, ack) => {
          newSeen.push(received.id);
          await ack();
          return "accepted";
        },
      });
      oldOwner = false;
      resolveOldDecision();
      await oldRetry;
      await oldConsumer.close(false);
      oldConsumer = undefined;
      await waitFor(() => newSeen.includes("ownership-lost"));
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(oldSeen.filter(id => id === "still-owned")).toHaveLength(1);
      expect(oldSeen.filter(id => id === "ownership-lost")).toHaveLength(1);
      expect(newSeen).toEqual(["ownership-lost"]);
    } finally {
      await Promise.all([oldConsumer?.close(false), newConsumer?.close(false)]);
      await connection.close();
    }
  }, 30_000);

  it("starts Fabric and GitHub event paths with a minted Path B runtime credential", async () => {
    const org = `events-${randomUUID().slice(0, 8)}`;
    const server = await createPathBHarness({ binary, tls: {}, org, events: true });
    fixtures.push(server);
    const bootstrap = await openBootstrapNatsConnection({
      servers: `tls://localhost:${new URL(server.serverUrl).port}`,
      credentials: server.users.bootstrap.credsPath,
      tls: { caFile: server.tls!.caFile },
      name: `nats-events-bootstrap-${org}`,
    });
    let runtime: Awaited<ReturnType<typeof openNatsConnection>> | undefined;
    let consumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      await ensureNatsEventsStream(bootstrap.manager, { org, provision: true });
      const githubStream = natsGithubStreamName(org);
      await bootstrap.manager.streams.add({
        name: githubStream, subjects: [`github.${org}.>`], storage: StorageType.File,
        retention: RetentionPolicy.Limits, discard: DiscardPolicy.New, max_bytes: 1_048_576,
        max_age: 0, max_msg_size: 1_048_576, max_msgs: -1, max_msgs_per_subject: -1,
        duplicate_window: 120_000_000_000, num_replicas: 1, no_ack: false, allow_direct: false,
      } as StreamConfig);
      runtime = await openNatsConnection({
        servers: `tls://localhost:${new URL(server.serverUrl).port}`,
        credentials: server.users.machineA.credsPath,
        tls: { caFile: server.tls!.caFile },
        name: `nats-events-minted-${org}`,
        machine: "machine-a",
        ...PATH_B_PERMISSION_POLICY,
        events: true,
        org,
      });
      const publisher = await openNatsEventsPublisher(runtime, { org, provision: false });
      const githubTopic = `github.${org}.repo.pulls.shard1of1`;
      const seen: string[] = [];
      consumer = await startNatsActorEventsConsumer(runtime, {
        org, actorId: `${org}:minted-runtime`, topics: ["mail.before", githubTopic],
        onMessage: async (received, _messageId, ack) => {
          seen.push(received.topic);
          await ack();
          return "accepted";
        },
      });
      await consumer.updateTopics(["mail.after", githubTopic]);
      await publisher.publishWithAck(event("minted-fabric-event", "mail.after"));
      await bootstrap.js.publish(githubTopic, Buffer.from(JSON.stringify(event("minted-github-event", githubTopic))));
      await waitFor(() => seen.length === 2);
      expect(seen).toEqual(expect.arrayContaining(["mail.after", githubTopic]));
    } finally {
      await consumer?.close(false);
      await runtime?.close();
      await bootstrap.close();
    }
  }, 30_000);
});
