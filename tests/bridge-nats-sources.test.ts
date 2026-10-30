// L1 (smarty-dev#15310): the real JetStream sources of the A' bridge NATS side, against a fake
// consumer API. Pins the durable names, filters and delivery policies the agent grant must allow,
// that a delivery is admitted only on a verified connection, and that an existing cursor is reused.
import { JetStreamApiError, AckPolicy, DeliverPolicy, ReplayPolicy } from "@nats-io/jetstream";
import { describe, expect, it, vi } from "vitest";
import { natsBridgeInboxSource } from "../src/mesh/bridge-nats-port.js";
import { natsBridgeEventSource } from "../src/mesh/bridge-nats-events.js";
import type { NatsConnectionHandle, NatsStatusListener } from "../src/transport/nats/connection.js";
import { hashToken } from "../src/transport/nats/subjects.js";

const notFound = (): JetStreamApiError => new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });

interface Fake {
  connection: NatsConnectionHandle;
  added: Array<{ stream: string; config: Record<string, unknown> }>;
  updated: Array<{ stream: string; durable: string; config: Record<string, unknown> }>;
  gets: Array<{ stream: string; durable: string }>;
  snapshot(durable: string): { config: Record<string, unknown>; state?: Record<string, unknown> } | undefined;
  push(message: unknown): void;
  end(): void;
  status(type: "disconnect" | "reconnect"): void;
  readonly statusListeners: number;
  verified: boolean;
}

function fakeConnection(existing?: Record<string, unknown>, existingState?: Record<string, unknown>): Fake {
  const consumers = new Map<string, Record<string, unknown>>();
  const states = new Map<string, Record<string, unknown>>();
  if (existing) {
    const durable = String(existing.durable_name);
    consumers.set(durable, existing);
    if (existingState) states.set(durable, existingState);
  }
  const fake: Fake = { added: [], updated: [], gets: [], verified: true, push: () => undefined, end: () => undefined,
    snapshot: (durable: string) => {
      const config = consumers.get(durable);
      const state = states.get(durable);
      return config ? { config, ...(state ? { state } : {}) } : undefined;
    } } as unknown as Fake;
  const listeners = new Set<NatsStatusListener>();
  fake.status = type => { for (const listener of listeners) listener({ type, server: "test:4222" }); };
  Object.defineProperty(fake, "statusListeners", { get: () => listeners.size });
  const messages = (callback?: (message: unknown) => void) => {
    const queue: unknown[] = [];
    let wake: (() => void) | undefined;
    let ended = false;
    let resolveClosed!: (error: Error | void) => void;
    const closed = new Promise<Error | void>(resolve => { resolveClosed = resolve; });
    fake.push = message => { if (callback) callback(message); else { queue.push(message); wake?.(); } };
    const end = (): void => { ended = true; wake?.(); resolveClosed(); };
    fake.end = end;
    return {
      async close() { end(); },
      closed: () => closed,
      status: () => ({ async *[Symbol.asyncIterator]() { await closed; } }),
      async *[Symbol.asyncIterator]() {
        for (;;) {
          const next = queue.shift();
          if (next !== undefined) { yield next; continue; }
          if (ended) return;
          await new Promise<void>(resolve => { wake = resolve; });
          wake = undefined;
        }
      },
    };
  };
  fake.connection = {
    isVerified: () => fake.verified,
    onStatus: (listener: NatsStatusListener) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    manager: { consumers: {
      async info(stream: string, durable: string) {
        const config = consumers.get(durable);
        if (!config) throw notFound();
        const state = states.get(durable);
        return { stream_name: stream, name: durable, config, ...(state ? { state } : {}) };
      },
      async add(stream: string, config: Record<string, unknown>) {
        fake.added.push({ stream, config });
        consumers.set(String(config.durable_name), config);
        return { stream_name: stream, name: config.durable_name, config };
      },
      async update(stream: string, durable: string, config: Record<string, unknown>) {
        fake.updated.push({ stream, durable, config });
        const current = consumers.get(durable);
        if (!current) throw notFound();
        Object.assign(current, config);
        const state = states.get(durable);
        return { stream_name: stream, name: durable, config: current, ...(state ? { state } : {}) };
      },
    } },
    js: { consumers: {
      async get(stream: string, durable: string) {
        fake.gets.push({ stream, durable });
        return { async consume(options: { callback?: (message: unknown) => void }) {
          return messages(options.callback);
        } };
      },
    } },
  } as unknown as NatsConnectionHandle;
  return fake;
}

const validBridgeInboxConsumerConfig = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  durable_name: `br_${hashToken("bridge:v2")}`,
  description: JSON.stringify({ fabricBridgeLink: "v2", machine: "ryzen1-bridge" }),
  filter_subject: "fabric.in.ryzen1-bridge.>",
  ack_policy: AckPolicy.Explicit,
  deliver_policy: DeliverPolicy.All,
  replay_policy: ReplayPolicy.Instant,
  ack_wait: 30_000_000_000,
  max_deliver: 5,
  backoff: Array.from({ length: 5 }, () => 30_000_000_000),
  max_ack_pending: 128,
  max_waiting: 1,
  inactive_threshold: 0,
  ...overrides,
});

describe("natsBridgeInboxSource", () => {
  it("creates one machine-wide durable with the transport inbox policy and yields only verified deliveries", async () => {
    const fake = fakeConnection();
    const source = natsBridgeInboxSource(fake.connection, { machine: "ryzen1-bridge", link: "v2" });
    const abort = new AbortController();
    const iterator = source.consume(abort.signal)[Symbol.asyncIterator]();
    const first = iterator.next();
    const durable = `br_${hashToken("bridge:v2")}`;
    await vi.waitFor(() => expect(fake.gets).toHaveLength(1), { timeout: 5_000 });
    expect(fake.added).toHaveLength(1);
    expect(fake.added[0]).toMatchObject({ stream: "FABRIC_IN_ryzen1-bridge", config: {
      durable_name: durable, filter_subject: "fabric.in.ryzen1-bridge.>", ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant, max_ack_pending: 128, max_waiting: 1,
      inactive_threshold: 604_800_000_000_000,
      description: JSON.stringify({ fabricBridgeLink: "v2", machine: "ryzen1-bridge" }) } });
    expect(fake.gets).toEqual([{ stream: "FABRIC_IN_ryzen1-bridge", durable }]);
    fake.verified = false;
    fake.push({ id: "dropped-while-unverified" });
    fake.verified = true;
    fake.push({ id: "m1" });
    expect((await first).value).toEqual({ id: "m1" });
    abort.abort();
    expect((await iterator.next()).done).toBe(true);
  });

  // Keep: a native reconnect resets its pull before USER.INFO passes, then silently backs off.
  it("reopens a silent inbox after verified reconnect, retaining its durable and disposing status listeners", async () => {
    const durable = `br_${hashToken("bridge:v2")}`;
    const config = validBridgeInboxConsumerConfig({ inactive_threshold: 604_800_000_000_000 });
    const cursor = { delivered: { consumer_seq: 23, stream_seq: 81 }, ack_floor: { consumer_seq: 19, stream_seq: 76 } };
    const fake = fakeConnection(config, cursor);
    const abort = new AbortController();
    const iterator = natsBridgeInboxSource(fake.connection, { machine: "ryzen1-bridge", link: "v2" })
      .consume(abort.signal)[Symbol.asyncIterator]();
    let pending = iterator.next();
    try {
      await vi.waitFor(() => expect(fake.gets).toHaveLength(1), { timeout: 5_000 });
      for (let generation = 1; generation <= 2; generation++) {
        fake.verified = false;
        fake.status("disconnect");
        fake.push({ id: "replayed-before-USER.INFO" });
        // No messages or consumer statuses arrive on the old iterator, even after verification.
        fake.verified = true;
        fake.status("reconnect");
        await vi.waitFor(() => expect(fake.gets).toHaveLength(generation + 1), { timeout: 2_000 });
        expect(fake.statusListeners).toBe(1);
        fake.push({ id: `verified-${generation}` });
        expect((await pending).value).toEqual({ id: `verified-${generation}` });
        pending = iterator.next();
      }
      expect(fake.added).toEqual([]);
      expect(fake.updated).toEqual([]);
      expect(fake.snapshot(durable)).toEqual({ config, state: cursor });
    } finally {
      abort.abort();
      await pending;
      await iterator.return?.();
    }
    expect(fake.statusListeners).toBe(0);
  });

  it("heals a compatible legacy durable in place without resetting its cursor or unrelated config", async () => {
    const durable = `br_${hashToken("bridge:v2")}`;
    const legacy = { ...validBridgeInboxConsumerConfig(), metadata: { retained: "yes" } };
    const cursor = { delivered: { consumer_seq: 23, stream_seq: 81 }, ack_floor: { consumer_seq: 19, stream_seq: 76 } };
    const fake = fakeConnection(legacy, cursor);
    const abort = new AbortController();
    const iterator = natsBridgeInboxSource(fake.connection, { machine: "ryzen1-bridge", link: "v2" }).consume(abort.signal)[Symbol.asyncIterator]();
    const pending = iterator.next();
    await vi.waitFor(() => expect(fake.gets).toHaveLength(1), { timeout: 5_000 });
    expect(fake.added).toEqual([]);
    expect(fake.updated).toEqual([{ stream: "FABRIC_IN_ryzen1-bridge", durable,
      config: { inactive_threshold: 604_800_000_000_000 } }]);
    expect(fake.snapshot(durable)).toEqual({
      config: { ...legacy, inactive_threshold: 604_800_000_000_000 }, state: cursor,
    });
    abort.abort();
    expect((await pending).done).toBe(true);
  });

  it("refuses fully configured wrong-owner or broader-filter durables without updating or adding", async () => {
    const incompatible = [
      validBridgeInboxConsumerConfig({ description: JSON.stringify({ fabricBridgeLink: "another-link", machine: "ryzen1-bridge" }) }),
      validBridgeInboxConsumerConfig({ filter_subject: "fabric.in.>" }),
    ];
    for (const config of incompatible) {
      const fake = fakeConnection(config);
      const source = natsBridgeInboxSource(fake.connection, { machine: "ryzen1-bridge", link: "v2" });
      await expect(source.consume(new AbortController().signal)[Symbol.asyncIterator]().next())
        .rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
      expect(fake.updated).toEqual([]);
      expect(fake.added).toEqual([]);
    }
  });
});

describe("natsBridgeEventSource", () => {
  const ORG = "example-org";
  it("creates its durable at NEW messages over the exact filters, one in flight, unlimited redelivery", async () => {
    const fake = fakeConnection();
    const source = natsBridgeEventSource(fake.connection, { org: ORG, link: "v2", topics: ["ops.owner", "fleet.work.>"] });
    const abort = new AbortController();
    const iterator = source.consume(abort.signal)[Symbol.asyncIterator]();
    const first = iterator.next();
    await vi.waitFor(() => expect(fake.gets).toHaveLength(1), { timeout: 5_000 });
    expect(fake.added).toHaveLength(1);
    expect(fake.added[0]).toMatchObject({ stream: "FABRIC_EV_EXAMPLE_ORG", config: {
      durable_name: `bev_${hashToken("bridge:v2")}`, deliver_policy: DeliverPolicy.New, ack_policy: AckPolicy.Explicit,
      inactive_threshold: 0, max_deliver: -1, max_ack_pending: 1,
      filter_subjects: [`fabric.ev.${ORG}.fleet.work.>`, `fabric.ev.${ORG}.ops.owner`] } });
    fake.push({ subject: "x" });
    expect((await first).value).toEqual({ subject: "x" });
    abort.abort();
    expect((await iterator.next()).done).toBe(true);
  });

  it("reuses an existing durable without resetting its cursor, and refuses one with other filters", async () => {
    const durable = `bev_${hashToken("bridge:v2")}`;
    const existing = { durable_name: durable, ack_policy: AckPolicy.Explicit, filter_subjects: [`fabric.ev.${ORG}.ops.owner`] };
    const reused = fakeConnection(existing);
    const abort = new AbortController();
    const iterator = natsBridgeEventSource(reused.connection, { org: ORG, link: "v2", topics: ["ops.owner"] }).consume(abort.signal)[Symbol.asyncIterator]();
    const pending = iterator.next();
    await vi.waitFor(() => expect(reused.gets).toHaveLength(1), { timeout: 5_000 });
    expect(reused.added).toEqual([]);
    abort.abort();
    expect((await pending).done).toBe(true);
    const other = fakeConnection(existing);
    await expect(natsBridgeEventSource(other.connection, { org: ORG, link: "v2", topics: ["fleet.work.>"] })
      .consume(new AbortController().signal)[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
  });
});
