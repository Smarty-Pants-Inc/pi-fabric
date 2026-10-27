// L1 (smarty-dev#15310): the real JetStream sources of the A' bridge NATS side, against a fake
// consumer API. Pins the durable names, filters and delivery policies the agent grant must allow,
// that a delivery is admitted only on a verified connection, and that an existing cursor is reused.
import { JetStreamApiError, AckPolicy, DeliverPolicy, ReplayPolicy } from "@nats-io/jetstream";
import { describe, expect, it, vi } from "vitest";
import { natsBridgeInboxSource } from "../src/mesh/bridge-nats-port.js";
import { natsBridgeEventSource } from "../src/mesh/bridge-nats-events.js";
import type { NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { hashToken } from "../src/transport/nats/subjects.js";

const notFound = (): JetStreamApiError => new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });

interface Fake {
  connection: NatsConnectionHandle;
  added: Array<{ stream: string; config: Record<string, unknown> }>;
  gets: Array<{ stream: string; durable: string }>;
  push(message: unknown): void;
  end(): void;
  verified: boolean;
}

function fakeConnection(existing?: Record<string, unknown>): Fake {
  const consumers = new Map<string, Record<string, unknown>>();
  if (existing) consumers.set(String(existing.durable_name), existing);
  const fake: Fake = { added: [], gets: [], verified: true, push: () => undefined, end: () => undefined } as unknown as Fake;
  let callback: ((message: unknown) => void) | undefined;
  const queue: unknown[] = [];
  let wake: (() => void) | undefined;
  let ended = false;
  let resolveClosed!: (error: Error | void) => void;
  const closed = new Promise<Error | void>(resolve => { resolveClosed = resolve; });
  fake.push = message => { if (callback) callback(message); else { queue.push(message); wake?.(); } };
  fake.end = () => { ended = true; wake?.(); resolveClosed(); };
  const messages = {
    async close() { fake.end(); },
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
  fake.connection = {
    isVerified: () => fake.verified,
    manager: { consumers: {
      async info(stream: string, durable: string) {
        const config = consumers.get(durable);
        if (!config) throw notFound();
        return { stream_name: stream, name: durable, config };
      },
      async add(stream: string, config: Record<string, unknown>) {
        fake.added.push({ stream, config });
        consumers.set(String(config.durable_name), config);
        return { stream_name: stream, name: config.durable_name, config };
      },
    } },
    js: { consumers: {
      async get(stream: string, durable: string) {
        fake.gets.push({ stream, durable });
        return { async consume(options: { callback?: (message: unknown) => void }) {
          callback = options.callback;
          return messages;
        } };
      },
    } },
  } as unknown as NatsConnectionHandle;
  return fake;
}

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

  it("refuses a durable of the same name with another owner or filter", async () => {
    const fake = fakeConnection({ durable_name: `br_${hashToken("bridge:v2")}`, filter_subject: "fabric.in.fleet-bridge.x.>",
      description: "{}", ack_policy: AckPolicy.Explicit });
    const source = natsBridgeInboxSource(fake.connection, { machine: "fleet-bridge", link: "v2" });
    await expect(source.consume(new AbortController().signal)[Symbol.asyncIterator]().next()).rejects.toBeTruthy();
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
      max_deliver: -1, max_ack_pending: 1, filter_subjects: [`fabric.ev.${ORG}.fleet.work.>`, `fabric.ev.${ORG}.ops.owner`] } });
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
