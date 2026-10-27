import { randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { afterEach, describe, expect, it, vi } from "vitest";
import { openBootstrapNatsConnection, openNatsConnection } from "../src/transport/nats/connection.js";
import { NatsEventsIdConflictError, openNatsEventsPublisher } from "../src/transport/nats/events-publish.js";
import { NATS_EVENT_RETRY_DELAY_SCHEDULE_MS, startNatsActorEventsConsumer, type NatsActorEventsSetupFailure } from "../src/transport/nats/events-consume.js";
import type { MeshEvent } from "../src/mesh/event-log.js";
import { ensureNatsEventsStream, ensureTransportStream, natsEventsStreamName, natsGithubStreamName, transportStreamConfig } from "../src/transport/nats/provision.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { createPathBHarness, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { AckPolicy, DeliverPolicy, DiscardPolicy, JetStreamApiError, JetStreamError, ReplayPolicy, RetentionPolicy, StorageType, type ConsumerInfo, type JsMsg, type StreamConfig, type StreamInfo } from "@nats-io/jetstream";
import { AuthorizationError, InvalidArgumentError, NoRespondersError, PermissionViolationError, RequestError, TimeoutError } from "@nats-io/transport-node";

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
const resolvesWithin = async <T>(promise: Promise<T>, timeoutMs = 1_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`operation did not resolve within ${timeoutMs}ms`)), timeoutMs);
    })]);
  } finally { if (timer) clearTimeout(timer); }
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

// Keep: setup failures and broker leader transitions cannot be reached cheaply by the real-server flow.
describe("NATS event consumer setup failures with fake JetStream", () => {
  const org = "smarty-pants-inc";
  const actorId = `${org}:setup/actor`;
  const topics = ["mail.inbound", `github.${org}.repo.pulls.shard1of1`];
  type SetupStep = "info" | "add" | "update" | "get" | "consume";
  type Kind = "ev" | "gh";
  const resources = (kind: Kind) => ({
    stream: kind === "ev" ? natsEventsStreamName(org) : natsGithubStreamName(org),
    durable: `${kind}_${hashToken(actorId)}`,
  });
  const missingConsumer = () => new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
  const apiError = (status: number, code: number, description = "private broker detail") =>
    new JetStreamApiError({ code: status, err_code: code, description });
  const statusError = (code: number) => Object.assign(new JetStreamError("private status detail"), { name: "JetStreamStatusError", code });
  const wrapper = (cause?: unknown) => Object.assign(new Error("private request detail", { cause }), { name: "JetStreamNotEnabled" });
  const fakeJs = ({ kind = "ev", missing = false, recoverCreate = false, incompatible = false, oldRetryPolicy = false, pending = 0, hold = false, deliver }: {
    kind?: Kind; missing?: boolean; recoverCreate?: boolean; incompatible?: boolean; oldRetryPolicy?: boolean; pending?: number; hold?: boolean; deliver?: JsMsg;
  } = {}) => {
    const target = resources(kind);
    const calls: Array<{ step: SetupStep | "close" | "delete"; stream: string; durable: string }> = [];
    const updates: Array<Record<string, unknown>> = [];
    let failureStep: SetupStep | undefined;
    let failureCause: unknown;
    let created = false;
    const checkpoint = (step: SetupStep, stream: string, durable: string) => {
      calls.push({ step, stream, durable });
      if (stream === target.stream && step === failureStep) throw failureCause;
    };
    const info = (durable: string) => ({
      config: { durable_name: incompatible && durable === target.durable ? "wrong-durable" : durable,
        ack_policy: AckPolicy.Explicit, replay_policy: ReplayPolicy.Instant,
        ack_wait: oldRetryPolicy && durable === target.durable ? 30_000_000_000 : 60_000_000_000,
        max_ack_pending: oldRetryPolicy && durable === target.durable ? 1 : 128,
        inactive_threshold: oldRetryPolicy && durable === target.durable ? 604_800_000_000_000 : 0,
        ...(oldRetryPolicy && durable === target.durable ? { max_deliver: 5 } : {
          max_deliver: -1,
          backoff: [1_000_000_000, 5_000_000_000, 15_000_000_000, 30_000_000_000, 30_000_000_000],
        }) },
      num_pending: pending,
      num_ack_pending: 0,
    }) as ConsumerInfo;
    const connection = {
      isVerified: () => true,
      manager: { consumers: {
        info: async (stream: string, durable: string) => {
          checkpoint("info", stream, durable);
          if (missing && stream === target.stream && !created) throw missingConsumer();
          return info(durable);
        },
        add: async (stream: string, config: { durable_name: string }) => {
          if (recoverCreate) created = true;
          checkpoint("add", stream, config.durable_name);
          created = true;
          return info(config.durable_name);
        },
        update: async (stream: string, durable: string, config: Record<string, unknown>) => {
          updates.push(config);
          checkpoint("update", stream, durable);
          return info(durable);
        },
        delete: async (stream: string, durable: string) => { calls.push({ step: "delete", stream, durable }); return true; },
      } },
      js: { consumers: { get: async (stream: string, durable: string) => {
        checkpoint("get", stream, durable);
        return { consume: async () => {
          checkpoint("consume", stream, durable);
          if (deliver) {
            let sent = false;
            let releaseNext!: (result: IteratorResult<JsMsg>) => void;
            const done = { done: true, value: undefined as never } as IteratorResult<JsMsg>;
            const iterator = {
              [Symbol.asyncIterator]() { return this; },
              next: (): Promise<IteratorResult<JsMsg>> => sent
                ? new Promise(resolve => { releaseNext = resolve; })
                : (sent = true, Promise.resolve({ done: false, value: deliver } as IteratorResult<JsMsg>)),
              return: async () => { releaseNext?.(done); return done; },
            };
            return {
              [Symbol.asyncIterator]: () => iterator,
              closed: async () => undefined,
              close: async () => { calls.push({ step: "close", stream, durable }); releaseNext?.(done); },
            };
          }
          if (hold) {
            let release!: () => void;
            const pending = new Promise<void>(resolve => { release = resolve; });
            return {
              async *[Symbol.asyncIterator]() { await pending; },
              closed: async () => undefined,
              close: async () => { calls.push({ step: "close", stream, durable }); release(); },
            };
          }
          return {
            async *[Symbol.asyncIterator]() { /* No deliveries are needed to exercise setup. */ },
            closed: async () => undefined,
            close: async () => { calls.push({ step: "close", stream, durable }); },
          };
        } };
      } } },
    } as unknown as Parameters<typeof startNatsActorEventsConsumer>[0];
    return { connection, calls, updates, setFailure(step: SetupStep, cause: unknown) { failureStep = step; failureCause = cause; } };
  };
  const start = (connection: Parameters<typeof startNatsActorEventsConsumer>[0]) =>
    startNatsActorEventsConsumer(connection, { org, actorId, topics, onMessage: async () => "accepted" });
  const expectFailure = async (promise: Promise<unknown>, cause: unknown, kind: Kind, code: string, retryable: boolean) => {
    const error: NatsActorEventsSetupFailure | undefined = await promise.then(() => undefined, error => error);
    expect(error).toBeInstanceOf(Error);
    expect(error?.cause).toBe(cause);
    expect(error?.setup).toEqual({ ...resources(kind), code, retryable });
  };

  it.each((["ev", "gh"] as const).flatMap(kind =>
    (["info", "add", "update", "get", "consume"] as const).map(step => ({ kind, step }))))(
    "preserves the cause and exact $kind resources when $step fails", async ({ kind, step }) => {
      const fake = fakeJs({ kind, missing: step === "add" });
      const cause = new TimeoutError();
      fake.setFailure(step, cause);
      await expectFailure(start(fake.connection), cause, kind, "TIMEOUT", true);
      expect(fake.calls).toContainEqual({ step, ...resources(kind) });
      expect(fake.calls.some(call => call.step === "delete")).toBe(false);
    });

  const permission = new PermissionViolationError("private permission detail", "publish", "private.subject");
  const cyclicRequest = new RequestError("private cyclic detail");
  cyclicRequest.cause = cyclicRequest;
  it.each([
    { label: "timeout", cause: new TimeoutError(), code: "TIMEOUT", retryable: true },
    { label: "no responders", cause: new NoRespondersError("private.subject"), code: "NO_RESPONDERS", retryable: true },
    { label: "wrapped timeout", cause: new RequestError("private", { cause: new TimeoutError() }), code: "TIMEOUT", retryable: true },
    { label: "nested no responders", cause: wrapper(new RequestError("private", { cause: new NoRespondersError("private.subject") })), code: "NO_RESPONDERS", retryable: true },
    { label: "API timeout", cause: apiError(408, 10000), code: "JS_API_10000", retryable: true },
    { label: "API unavailable", cause: apiError(503, 10008), code: "JS_API_10008", retryable: true },
    { label: "API status fallback", cause: apiError(503, 0), code: "JS_STATUS_503", retryable: true },
    { label: "cluster unavailable", cause: apiError(500, 10008), code: "JS_API_10008", retryable: true },
    { label: "cluster leader transition", cause: apiError(500, 10009), code: "JS_API_10009", retryable: true },
    { label: "not-ready status", cause: apiError(500, 10000, "consumer is not yet current"), code: "JS_API_10000", retryable: true },
    { label: "meta leader transition", cause: apiError(500, 10000, "no JetStream meta leader"), code: "JS_API_10000", retryable: true },
    { label: "pull timeout status", cause: statusError(408), code: "JS_STATUS_408", retryable: true },
    { label: "pull unavailable status", cause: statusError(503), code: "JS_STATUS_503", retryable: true },
    { label: "permission denial", cause: permission, code: "PermissionViolationError", retryable: false },
    { label: "wrapped permission denial", cause: wrapper(new RequestError("private", { cause: permission })), code: "PermissionViolationError", retryable: false },
    { label: "authorization veto", cause: new AuthorizationError("private", { cause: new TimeoutError() }), code: "AuthorizationError", retryable: false },
    { label: "API permission denial", cause: apiError(403, 10076, "cluster not ready"), code: "JS_API_10076", retryable: false },
    { label: "API authorization veto", cause: apiError(401, 10008), code: "JS_API_10008", retryable: false },
    { label: "account disabled", cause: wrapper(apiError(503, 10039)), code: "JS_API_10039", retryable: false },
    { label: "account permission veto", cause: apiError(503, 10076), code: "JS_API_10076", retryable: false },
    { label: "malformed config", cause: apiError(400, 10093), code: "JS_API_10093", retryable: false },
    { label: "malformed config veto", cause: apiError(400, 10000, "consumer is not yet current"), code: "JS_API_10000", retryable: false },
    { label: "missing stream", cause: apiError(404, 10059), code: "JS_API_10059", retryable: false },
    { label: "invalid config option", cause: new TypeError("private invalid option"), code: "TypeError", retryable: false },
    { label: "SDK invalid config", cause: new InvalidArgumentError("private invalid config", { cause: new TimeoutError() }), code: "InvalidArgumentError", retryable: false },
    { label: "unknown API failure", cause: apiError(500, 10000), code: "JS_API_10000", retryable: false },
    { label: "unknown error", cause: new Error("timeout: cluster not ready"), code: "UNKNOWN", retryable: false },
    { label: "unrecognized wrapper", cause: new Error("private", { cause: new TimeoutError() }), code: "UNKNOWN", retryable: false },
    { label: "empty request wrapper", cause: new RequestError("private"), code: "UNKNOWN", retryable: false },
    { label: "empty JS wrapper", cause: wrapper(), code: "UNKNOWN", retryable: false },
    { label: "cyclic request wrapper", cause: cyclicRequest, code: "UNKNOWN", retryable: false },
    { label: "untrusted name and code", cause: Object.assign(new Error("private"), { name: "private tenant detail", code: "private broker detail" }), code: "UNKNOWN", retryable: false },
    { label: "non-Error throw", cause: "private timeout detail", code: "UNKNOWN", retryable: false },
    { label: "unknown status", cause: statusError(409), code: "JS_STATUS_409", retryable: false },
  ])("classifies $label without exposing broker messages as codes", async ({ cause, code, retryable }) => {
    const fake = fakeJs();
    fake.setFailure("info", cause);
    await expectFailure(start(fake.connection), cause, "ev", code, retryable);
  });

  it.each((["ev", "gh"] as const).flatMap(kind =>
    (["update", "get", "consume"] as const).map(step => ({ kind, step }))))(
    "annotates $kind $step failures while updating topics", async ({ kind, step }) => {
      const fake = fakeJs({ kind });
      const consumer = await start(fake.connection);
      try {
        const cause = new TimeoutError();
        fake.setFailure(step, cause);
        await expectFailure(consumer.updateTopics(topics), cause, kind, "TIMEOUT", true);
      } finally { await consumer.close(false); }
    });

  it("keeps incompatible existing consumers terminal without updating or resetting them", async () => {
    const fake = fakeJs({ incompatible: true });
    const error: NatsActorEventsSetupFailure | undefined = await start(fake.connection).then(() => undefined, error => error);
    expect(error).toBeInstanceOf(Error);
    expect(error?.cause).toBeInstanceOf(Error);
    expect((error?.cause as Error).message).toContain("Incompatible NATS actor events consumer");
    expect(error?.setup).toEqual({ ...resources("ev"), code: "UNKNOWN", retryable: false });
    expect(fake.calls.map(call => call.step)).toEqual(["info"]);
  });

  it("repairs every bounded consumer setting on an old durable without deleting it", async () => {
    const fake = fakeJs({ oldRetryPolicy: true, pending: 4 });
    const consumer = await start(fake.connection);
    try {
      expect(fake.updates[0]).toEqual({
        ack_wait: 60_000_000_000, max_ack_pending: 128, inactive_threshold: 0,
        max_deliver: -1,
        backoff: [1_000_000_000, 5_000_000_000, 15_000_000_000, 30_000_000_000, 30_000_000_000],
      });
      expect(fake.calls.some(call => call.step === "delete")).toBe(false);
      expect(fake.calls.filter(call => call.stream === resources("ev").stream).map(call => call.step))
        .toEqual(["info", "update", "update", "get", "consume"]);
    } finally { await consumer.close(false); }
  });

  it("does not pull while the directory is unready, NAKs an in-hand message with delay, and resumes it", async () => {
    let directoryReady = false;
    let pullCount = 0;
    let queuedRetry = false;
    let resolveFirst!: (result: IteratorResult<JsMsg>) => void;
    const received: string[] = [];
    const nakDelays: number[] = [];
    const ackAck = vi.fn(async () => true);
    const rawEvent = event("directory-retry", "mail.retry");
    const message = {
      data: Buffer.from(JSON.stringify(rawEvent)), subject: `fabric.ev.${org}.mail.retry`,
      info: { deliveryCount: 1, streamSequence: 1 }, ackAck,
      nak: (delay?: number) => { nakDelays.push(delay ?? 0); queuedRetry = true; },
      term: vi.fn(),
    } as unknown as JsMsg;
    const messages = {
      [Symbol.asyncIterator]() { return this; },
      next: () => {
        pullCount++;
        if (queuedRetry) { queuedRetry = false; return Promise.resolve({ done: false, value: message } as IteratorResult<JsMsg>); }
        return new Promise<IteratorResult<JsMsg>>(resolve => { resolveFirst = resolve; });
      },
      closed: async () => undefined,
      close: async () => { resolveFirst?.({ done: true, value: undefined as never }); },
    };
    const connection = {
      isVerified: () => true,
      manager: { consumers: {
        info: async (_stream: string, durable: string) => ({ config: {
          durable_name: durable, ack_policy: AckPolicy.Explicit, replay_policy: ReplayPolicy.Instant,
          max_deliver: -1, backoff: [1_000_000_000, 5_000_000_000, 15_000_000_000, 30_000_000_000, 30_000_000_000],
        }, num_pending: 1, num_ack_pending: 0 }) as ConsumerInfo,
        add: async (_stream: string, config: { durable_name: string }) => ({ config: {
          durable_name: config.durable_name, ack_policy: AckPolicy.Explicit, replay_policy: ReplayPolicy.Instant,
          max_deliver: -1, backoff: [1_000_000_000, 5_000_000_000, 15_000_000_000, 30_000_000_000, 30_000_000_000],
        } }) as ConsumerInfo,
        update: async (_stream: string, durable: string) => ({ config: {
          durable_name: durable, ack_policy: AckPolicy.Explicit, replay_policy: ReplayPolicy.Instant,
          max_deliver: -1, backoff: [1_000_000_000, 5_000_000_000, 15_000_000_000, 30_000_000_000, 30_000_000_000],
        } }) as ConsumerInfo,
      } },
      js: { consumers: { get: async () => ({ consume: async () => messages }) } },
    } as unknown as Parameters<typeof startNatsActorEventsConsumer>[0];
    const consumer = await startNatsActorEventsConsumer(connection, {
      org, actorId, topics: ["mail.retry"], directoryReady: () => directoryReady, directoryReadyRecheckMs: 20,
      onMessage: async receivedEvent => { received.push(receivedEvent.id); return "accepted"; },
    });
    try {
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(pullCount).toBe(0);
      directoryReady = true;
      await waitFor(() => pullCount === 1);
      directoryReady = false;
      resolveFirst({ done: false, value: message });
      await waitFor(() => nakDelays.length === 1);
      expect(nakDelays[0]).toBeGreaterThan(0);
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(pullCount).toBe(1);
      expect(received).toEqual([]);
      directoryReady = true;
      await waitFor(() => received.length === 1);
      expect(received).toEqual(["directory-retry"]);
      expect(ackAck).toHaveBeenCalledOnce();
    } finally { await consumer.close(false); }
  });

  it("preserves the not-ready alarm deadline across topic updates", async () => {
    const fake = fakeJs({ pending: 3, hold: true });
    let firstNotReadyAt = 0;
    const alarm = vi.fn();
    const consumer = await startNatsActorEventsConsumer(fake.connection, {
      org, actorId, topics: ["mail.inbound"], directoryReady: () => { firstNotReadyAt ||= Date.now(); return false; },
      directoryReadyRecheckMs: 20, directoryStallAlarmMs: 500, onDirectoryStall: alarm,
      onMessage: async () => "accepted",
    });
    try {
      await new Promise(resolve => setTimeout(resolve, 250));
      await consumer.updateTopics(["mail.updated"]);
      await waitFor(() => alarm.mock.calls.length === 1, 1_000);
      expect(alarm).toHaveBeenCalledTimes(1);
      const stall = alarm.mock.calls[0]?.[0];
      expect(stall).toBeDefined();
      if (!stall) throw new Error("directory stall alarm was not recorded");
      expect(stall.since).toBeGreaterThanOrEqual(firstNotReadyAt);
      expect(stall.since).toBeLessThan(firstNotReadyAt + 100);
    } finally { await consumer.close(false); }
  });

  it("closes promptly during a not-ready wait and cancels its shared recheck", async () => {
    const fake = fakeJs();
    let readinessChecks = 0;
    const consumer = await startNatsActorEventsConsumer(fake.connection, {
      org, actorId, topics: ["mail.inbound"], directoryReady: () => { readinessChecks++; return false; },
      directoryReadyRecheckMs: 20, onMessage: async () => "accepted",
    });
    await new Promise(resolve => setTimeout(resolve, 50));
    const checksAtClose = readinessChecks;
    expect(checksAtClose).toBeGreaterThan(1);
    await resolvesWithin(consumer.close(false));
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(readinessChecks).toBe(checksAtClose);
  });

  it("updates topics and closes promptly while onMessage is still running", async () => {
    let entered!: () => void;
    const handlerEntered = new Promise<void>(resolve => { entered = resolve; });
    const message = {
      data: Buffer.from(JSON.stringify(event("close-during-handler", "mail.close"))),
      subject: `fabric.ev.${org}.mail.close`, info: { deliveryCount: 1, streamSequence: 1 },
      ackAck: vi.fn(async () => true), nak: vi.fn(), term: vi.fn(),
    } as unknown as JsMsg;
    const fake = fakeJs({ deliver: message });
    const consumer = await startNatsActorEventsConsumer(fake.connection, {
      org, actorId, topics: ["mail.close"],
      onMessage: async () => { entered(); await new Promise<void>(() => {}); return "accepted"; },
    });
    await handlerEntered;
    await resolvesWithin(consumer.updateTopics(["mail.changed"]));
    await resolvesWithin(consumer.close(false));
    expect(message.ackAck).not.toHaveBeenCalled();
  });

  it("alarms once after a prolonged not-ready episode and re-arms after recovery", async () => {
    const fake = fakeJs({ pending: 3, hold: true });
    let directoryReady = false;
    const alarm = vi.fn();
    const consumer = await startNatsActorEventsConsumer(fake.connection, {
      org, actorId, topics: ["mail.inbound"], directoryReady: () => directoryReady, directoryReadyRecheckMs: 20,
      directoryStallAlarmMs: 25, onDirectoryStall: alarm,
      onMessage: async () => "accepted",
    });
    try {
      await waitFor(() => alarm.mock.calls.length === 1);
      expect(alarm).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
        ...resources("ev"), pendingCount: 3, since: expect.any(Number),
      }));
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(alarm).toHaveBeenCalledOnce();
      directoryReady = true;
      await new Promise(resolve => setTimeout(resolve, 150));
      directoryReady = false;
      await waitFor(() => alarm.mock.calls.length === 2);
      expect(alarm).toHaveBeenCalledTimes(2);
    } finally { await consumer.close(false); }
  });

  it("recovers an expected 404 by creating the same durable without updating its new cursor", async () => {
    const fake = fakeJs({ missing: true });
    const consumer = await start(fake.connection);
    try {
      expect(fake.calls.filter(call => call.stream === resources("ev").stream).map(call => call.step))
        .toEqual(["info", "add", "get", "consume"]);
    } finally { await consumer.close(false); }
  });

  it("does not create a missing GitHub consumer when there are no GitHub topics", async () => {
    const fake = fakeJs({ kind: "gh", missing: true });
    const consumer = await startNatsActorEventsConsumer(fake.connection, {
      org, actorId, topics: ["mail.inbound"], onMessage: async () => "accepted",
    });
    try {
      expect(fake.calls.filter(call => call.stream === resources("gh").stream))
        .toEqual([{ step: "info", ...resources("gh") }]);
    } finally { await consumer.close(false); }
  });

  it("recovers a concurrent creation failure by reopening, never recreating the durable", async () => {
    const fake = fakeJs({ missing: true, recoverCreate: true });
    fake.setFailure("add", apiError(400, 10013, "consumer already exists"));
    const consumer = await start(fake.connection);
    try {
      expect(fake.calls.filter(call => call.stream === resources("ev").stream).map(call => call.step))
        .toEqual(["info", "add", "info", "update", "get", "consume"]);
    } finally { await consumer.close(false); }
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

  it("closes a real JetStream consumer while its handler is blocked", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection, publisher } = await open(server, org);
    let enterHandler!: () => void;
    let releaseHandler!: () => void;
    const handlerEntered = new Promise<void>(resolve => { enterHandler = resolve; });
    const handlerBlocked = new Promise<void>(resolve => { releaseHandler = resolve; });
    let consumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId: `${org}:blocked-close`, topics: ["mail.blocked-close"],
        onMessage: async () => { enterHandler(); await handlerBlocked; return "accepted"; },
      });
      await publisher.publish(event("blocked-close", "mail.blocked-close"));
      await handlerEntered;
      await resolvesWithin(consumer.close(false));
      releaseHandler();
      await new Promise(resolve => setTimeout(resolve, 50));
      const info = await connection.manager.consumers.info(natsEventsStreamName(org), `ev_${hashToken(`${org}:blocked-close`)}`);
      expect(info.num_ack_pending).toBe(1);
    } finally {
      releaseHandler();
      await consumer?.close(false);
      await connection.close();
    }
  }, 30_000);

  it("repairs legacy durable delivery and inactivity settings on the real server", async () => {
    const server = await fixture();
    const org = `events-${randomUUID().slice(0, 8)}`;
    const { connection } = await open(server, org);
    const actorId = `${org}:legacy-settings`;
    const stream = natsEventsStreamName(org);
    const durable = `ev_${hashToken(actorId)}`;
    let consumer: Awaited<ReturnType<typeof startNatsActorEventsConsumer>> | undefined;
    try {
      await connection.manager.consumers.add(stream, {
        durable_name: durable, filter_subject: "", filter_subjects: [`fabric.ev.${org}.mail.legacy`, `fabric.ev.${org}.wake.${actorId}`].sort(),
        ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
        ack_wait: 30_000_000_000, max_deliver: -1, max_ack_pending: 1,
        backoff: [30_000_000_000, 30_000_000_000], inactive_threshold: 604_800_000_000_000,
      });
      consumer = await startNatsActorEventsConsumer(connection, {
        org, actorId, topics: ["mail.legacy"], onMessage: async () => "accepted",
      });
      const repaired = await connection.manager.consumers.info(stream, durable);
      expect(repaired.config).toMatchObject({
        ack_wait: 60_000_000_000, max_ack_pending: 128, inactive_threshold: 0, max_deliver: -1,
        backoff: [1_000_000_000, 5_000_000_000, 15_000_000_000, 30_000_000_000, 30_000_000_000],
        filter_subjects: [`fabric.ev.${org}.mail.legacy`, `fabric.ev.${org}.wake.${actorId}`].sort(),
      });
    } finally {
      await consumer?.close(false);
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
      expect(Date.parse(reopenedDurable.config.opt_start_time ?? "")).toBe(Date.parse(startAt));
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
      // The injected 50 ms schedule is explicit; the final entry repeats for NATS backoff.
      expect(initial.config.backoff).toEqual([50_000_000, 50_000_000]);
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
