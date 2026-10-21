import { describe, expect, it } from "vitest";
import type { JetStreamManager, StreamInfo } from "@nats-io/jetstream";
import { assertTransportStream, ensureTransportStream, INBOX_DUPLICATE_WINDOW_MS, transportStreamConfig, type TransportStreamPolicy } from "../src/transport/nats/provision.js";

const policy: TransportStreamPolicy = {
  name: "FABRIC_IN_machine-a",
  subjects: ["fabric.in.machine-a.>"],
  maxBytes: 1_073_741_824,
  maxAgeMs: 24 * 60 * 60_000,
  maxMessageBytes: 1_048_576,
  duplicateWindowMs: INBOX_DUPLICATE_WINDOW_MS,
  replicas: 1,
};

const streamInfo = (maxAge: number): StreamInfo => ({
  config: { ...transportStreamConfig(policy), max_age: maxAge },
} as unknown as StreamInfo);

describe("NATS inbox stream age and deduplication policy", () => {
  it("sets the bounded max_age and preserves the ten-minute duplicate window", () => {
    expect(transportStreamConfig(policy).max_age).toBe(24 * 60 * 60_000 * 1_000_000);
    expect(INBOX_DUPLICATE_WINDOW_MS).toBe(10 * 60_000);
    expect(transportStreamConfig(policy).duplicate_window).toBe(10 * 60_000 * 1_000_000);
    expect(() => assertTransportStream(streamInfo(transportStreamConfig(policy).max_age!), transportStreamConfig(policy))).not.toThrow();
  });

  it("accepts the existing ten-minute duplicate window and unbounded legacy age without updating it", async () => {
    let adds = 0;
    const manager = {
      streams: {
        info: async () => streamInfo(0),
        add: async () => { adds++; return streamInfo(0); },
      },
    } as unknown as JetStreamManager;
    const result = await ensureTransportStream(manager, { ...policy, allowUnboundedMaxAge: true }, false);
    expect(result.config.max_age).toBe(0);
    expect(result.config.duplicate_window).toBe(10 * 60_000 * 1_000_000);
    expect(adds).toBe(0);
    expect(() => assertTransportStream(streamInfo(0), transportStreamConfig(policy))).toThrow("configuration mismatch");
  });

  it("still refuses a different nonzero age", () => {
    const wanted = transportStreamConfig(policy);
    expect(() => assertTransportStream(streamInfo(wanted.max_age! - 1), wanted, false, true))
      .toThrow("configuration mismatch");
  });
});
