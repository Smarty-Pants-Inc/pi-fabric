import { beforeEach, describe, expect, it, vi } from "vitest";
import { JetStreamApiCodes, JetStreamApiError } from "@nats-io/jetstream";
import { NatsInboxResourceNotFoundError, openNatsTransport } from "../src/transport/nats/inbox.js";

const mocks = vi.hoisted(() => ({
  consumerSetupAttempts: 0,
  failure: undefined as unknown,
  connection: {
    manager: { consumers: {} },
    js: {},
    nc: { isClosed: () => false, isDraining: () => false },
    isVerified: () => true,
    onStatus: () => () => {},
    closed: () => new Promise<void | Error>(() => {}),
    close: vi.fn(async () => {}),
  },
}));

vi.mock("../src/transport/nats/connection.js", () => ({
  openNatsConnection: async () => mocks.connection,
}));
vi.mock("../src/transport/nats/kv.js", () => ({
  LEASE_TTL_MS: 15_000,
  PRESENCE_TTL_MS: 600_000,
  PLACEMENT_TERMINAL_BUCKET: "FABRIC_PLACEMENT_TERMINAL",
  PLACEMENT_TERMINAL_TTL_MS: 86_400_000,
  openNatsKv: vi.fn(),
  openNatsKvForMachine: async (_js: unknown, bucket: "leases" | "presence") => ({
    ttlMs: bucket === "leases" ? 15_000 : 600_000,
  }),
}));
vi.mock("../src/transport/nats/provision.js", () => ({
  ensureTransportStream: async () => undefined,
  ensureTransportConsumer: async (_api: unknown, policy: { description: string }) => {
    mocks.consumerSetupAttempts++;
    if (mocks.consumerSetupAttempts > 1) throw mocks.failure;
    return { config: { description: policy.description } };
  },
}));

beforeEach(() => {
  mocks.consumerSetupAttempts = 0;
  mocks.failure = undefined;
  mocks.connection.close.mockClear();
});

describe("NATS inbox terminal missing-resource errors", () => {
  it.each([
    { label: "HTTP 404", status: 404, code: 0 },
    { label: "consumer not found", status: 500, code: JetStreamApiCodes.ConsumerNotFound },
    { label: "stream not found", status: 500, code: JetStreamApiCodes.StreamNotFound },
  ])("stops the inbox with a typed error for JetStream $label", async ({ status, code }) => {
    const brokerError = new JetStreamApiError({ code: status, err_code: code, description: "private broker detail" });
    mocks.failure = brokerError;
    const transport = await openNatsTransport({
      servers: "tls://localhost:4222",
      machine: "machine-a",
      hostId: "inbox-terminal-test",
      credentials: "unused-by-fake-connection",
      custodyStream: "FABRIC_CUSTODY",
      livenessStream: "FABRIC_LIVENESS",
      pathBMachines: ["machine-a"],
      provision: false,
    });
    try {
      const iterator = transport.inbox(new AbortController().signal)[Symbol.asyncIterator]();
      const error = await iterator.next().then(() => undefined, caught => caught);
      expect(error).toBeInstanceOf(NatsInboxResourceNotFoundError);
      expect(error).toMatchObject({
        name: "NatsInboxResourceNotFoundError",
        code: "FABRIC_TRANSPORT_INVALID",
        message: "NATS inbox JetStream stream or consumer was not found",
        cause: brokerError,
      });
      // One successful open-time setup plus one failed inbox entry: no retry loop follows the 404.
      expect(mocks.consumerSetupAttempts).toBe(2);
    } finally {
      await transport.close();
    }
  });
});
