import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

// pi-fabric#937 round 4: the real client's connect() gets only the validated, pinned servers, with cluster gossip
// (INFO connect_urls) ignored, so a loopback seed cannot steer a reconnect to an unchecked peer. The two @nats-io
// modules are replaced: what is checked is exactly what connectJetStream hands to nats.js.
const connected = vi.hoisted(() => [] as Record<string, unknown>[]);
vi.mock("@nats-io/transport-node", () => ({
  connect: async (options: Record<string, unknown>) => {
    connected.push(options);
    return { info: { version: "2.14.7", auth_required: false, max_payload: 1024 * 1024 }, drain: async () => undefined };
  },
}));
vi.mock("@nats-io/jetstream", () => ({
  DeliverPolicy: {}, JetStreamApiError: class extends Error {}, StorageType: {}, jetstream: () => ({}), jetstreamManager: async () => ({}),
}));

const { openNatsStateBackend } = await import("../src/mesh/state-backend-nats.js");

describe("NATS client connect options (pi-fabric#937 round 4)", () => {
  it("connects only to the validated, pinned servers and ignores cluster gossip", async () => {
    const lookup = async (host: string) => host === "localhost" ? [{ address: "127.0.0.1", family: 4 }] : [{ address: "10.1.1.1", family: 4 }];
    // The open goes on to refuse (no sync_always from the fake); only what reached connect() matters here.
    await openNatsStateBackend({ root: path.join(os.tmpdir(), `nats-connect-${Math.random().toString(36).slice(2)}`), servers: ["nats://localhost:4222", "tls://nats.example.com:4222"], lookup })
      .catch(() => undefined);
    expect(connected).toHaveLength(1);
    expect(connected[0]).toMatchObject({ servers: ["nats://127.0.0.1:4222", "tls://nats.example.com:4222"], ignoreClusterUpdates: true });
  });
});
