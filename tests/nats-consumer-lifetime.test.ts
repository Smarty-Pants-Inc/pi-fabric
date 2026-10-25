// Failure-path/data-safety checks; real server behavior and runtime grants are proved in
// nats-messaging.test.ts. In particular no migration may mutate an incompatible/foreign cursor.
import { describe, expect, it, vi } from "vitest";
import { AckPolicy, DeliverPolicy, JetStreamApiError, ReplayPolicy, type ConsumerAPI, type ConsumerConfig, type ConsumerInfo } from "@nats-io/jetstream";
import { ensureTransportConsumer } from "../src/transport/nats/provision.js";

const WEEK_NS = 7 * 24 * 60 * 60 * 1_000_000_000;
const policy = {
  stream: "FABRIC_IN_machine-a", durable: "policy-test", filterSubject: "fabric.in.machine-a.policy-test.>",
  description: "policy test owner", maxAckPending: 128,
};
const consumerInfo = (overrides: Partial<ConsumerConfig> = {}): ConsumerInfo => ({
  config: {
    durable_name: policy.durable, description: policy.description, filter_subject: policy.filterSubject,
    ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
    ack_wait: 30_000_000_000, max_deliver: 5, backoff: Array.from({ length: 5 }, () => 30_000_000_000),
    max_ack_pending: 128, max_waiting: 1, inactive_threshold: 0, ...overrides,
  },
} as ConsumerInfo);
const fake = (initial: ConsumerInfo) => {
  const info = vi.fn(async () => initial);
  const add = vi.fn(async () => initial);
  const update = vi.fn(async () => initial);
  return { info, add, update, api: { info, add, update } as unknown as ConsumerAPI };
};
const missing = () => new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });

describe("bounded inbox consumer migration failure paths", () => {
  it("does not update an already compatible bounded durable", async () => {
    const f = fake(consumerInfo({ inactive_threshold: WEEK_NS }));
    await expect(ensureTransportConsumer(f.api, policy)).resolves.toMatchObject({ config: { inactive_threshold: WEEK_NS } });
    expect(f.update).not.toHaveBeenCalled();
    expect(f.add).not.toHaveBeenCalled();
  });

  it.each([undefined, 0, 60 * 60 * 1_000_000_000])("repairs legacy/wrong lifetime %s with a one-field delta and reads it back", async (threshold) => {
    const initial = consumerInfo();
    if (threshold === undefined) delete initial.config.inactive_threshold;
    else initial.config.inactive_threshold = threshold;
    const f = fake(initial);
    const repaired = consumerInfo({ inactive_threshold: WEEK_NS });
    f.info.mockResolvedValueOnce(initial).mockResolvedValueOnce(repaired);
    // The update response is deliberately stale; the separate info read is authoritative.
    await expect(ensureTransportConsumer(f.api, policy)).resolves.toBe(repaired);
    expect(f.update).toHaveBeenCalledExactlyOnceWith(policy.stream, policy.durable, { inactive_threshold: WEEK_NS });
    expect(f.info).toHaveBeenCalledTimes(2);
    expect(f.add).not.toHaveBeenCalled();
  });

  it("keeps the explicit ephemeral lifetime override", async () => {
    const threshold = 300 * 1_000_000_000;
    const f = fake(consumerInfo({ inactive_threshold: threshold }));
    await expect(ensureTransportConsumer(f.api, { ...policy, inactiveThresholdNs: threshold }))
      .resolves.toMatchObject({ config: { inactive_threshold: threshold } });
    expect(f.update).not.toHaveBeenCalled();
  });

  const incompatible: Array<[string, Partial<ConsumerConfig>]> = [
    ["durable", { durable_name: "foreign" }],
    ["filter", { filter_subject: "fabric.in.machine-a.>" }],
    ["multiple filters", { filter_subjects: [policy.filterSubject] }],
    ["push delivery", { deliver_subject: "push.inbox" }],
    ["headers only", { headers_only: true }],
    ["ack policy", { ack_policy: AckPolicy.None }],
    ["replay policy", { replay_policy: ReplayPolicy.Original }],
    ["max waiting", { max_waiting: 2 }],
    ["delivery policy", { deliver_policy: DeliverPolicy.New }],
    ["ack wait", { ack_wait: 1_000_000_000 }],
    ["max deliveries", { max_deliver: 6 }],
    ["max ack pending", { max_ack_pending: 129 }],
    ["backoff", { backoff: [30_000_000_000] }],
    ["owner", { description: "foreign owner" }],
  ];
  it.each(incompatible)("validates %s before mutating an unbounded durable", async (_name, overrides) => {
    const f = fake(consumerInfo(overrides));
    await expect(ensureTransportConsumer(f.api, policy)).rejects.toThrow(/configuration mismatch|owner description mismatch/);
    expect(f.update).not.toHaveBeenCalled();
    expect(f.add).not.toHaveBeenCalled();
  });

  it("propagates update permission failure without recreating the durable", async () => {
    const f = fake(consumerInfo());
    const denied = new Error("permissions violation");
    f.update.mockRejectedValue(denied);
    await expect(ensureTransportConsumer(f.api, policy)).rejects.toBe(denied);
    expect(f.add).not.toHaveBeenCalled();
    expect(f.info).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["lifetime", { inactive_threshold: 0 }],
    ["owner", { inactive_threshold: WEEK_NS, description: "foreign owner" }],
    ["filter", { inactive_threshold: WEEK_NS, filter_subject: "fabric.in.machine-a.>" }],
  ] satisfies Array<[string, Partial<ConsumerConfig>]>)("rejects a post-update %s mismatch", async (_name, overrides) => {
    const initial = consumerInfo();
    const f = fake(initial);
    f.info.mockResolvedValueOnce(initial).mockResolvedValueOnce(consumerInfo(overrides));
    await expect(ensureTransportConsumer(f.api, policy)).rejects.toThrow(/configuration mismatch|owner description mismatch/);
    expect(f.add).not.toHaveBeenCalled();
    expect(f.update).toHaveBeenCalledTimes(1);
  });

  it("propagates a failed post-update read rather than trusting the update response", async () => {
    const initial = consumerInfo();
    const f = fake(initial);
    const unavailable = new Error("broker unavailable");
    f.info.mockResolvedValueOnce(initial).mockRejectedValueOnce(unavailable);
    await expect(ensureTransportConsumer(f.api, policy)).rejects.toBe(unavailable);
    expect(f.add).not.toHaveBeenCalled();
  });

  it("does not mutate a foreign durable that wins a create race", async () => {
    const f = fake(consumerInfo({ description: "foreign owner" }));
    f.info.mockRejectedValueOnce(missing());
    f.add.mockRejectedValueOnce(new Error("another opener won"));
    await expect(ensureTransportConsumer(f.api, policy)).rejects.toThrow("owner description mismatch");
    expect(f.update).not.toHaveBeenCalled();
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 1.5, Number.MAX_SAFE_INTEGER + 1])("rejects invalid explicit lifetime %s before any API call", async (threshold) => {
    const f = fake(consumerInfo());
    await expect(ensureTransportConsumer(f.api, { ...policy, inactiveThresholdNs: threshold })).rejects.toThrow("positive safe integer");
    expect(f.info).not.toHaveBeenCalled();
    expect(f.add).not.toHaveBeenCalled();
    expect(f.update).not.toHaveBeenCalled();
  });
});
