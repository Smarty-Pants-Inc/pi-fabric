import { describe, expect, it } from "vitest";
import type { NatsConnection } from "@nats-io/transport-node";
import { NatsSessionChannelManager } from "../src/transport/nats/session-channel.js";

const identity = {
  org: "smarty-pants-inc",
  hostId: "session-channel-host",
  sessionId: "11111111-2222-4333-8444-555555555555",
  sessionGeneration: "generation-1",
};

// Keep reason: code-lead #3110 reproduced a real reconnect failure where a synchronous
// first-bind error left a channel record behind and permanently blocked retries.
describe("NatsSessionChannelManager bind failures", () => {
  it("removes the channel record when the first bind throws synchronously", async () => {
    let failFirstSubscribe = true;
    const connection = {
      isClosed: () => false,
      isDraining: () => false,
      flush: async () => undefined,
      subscribe: () => {
        if (failFirstSubscribe) {
          failFirstSubscribe = false;
          throw new Error("subscribe failed");
        }
        return { unsubscribe: () => undefined };
      },
    } as unknown as NatsConnection;
    const manager = new NatsSessionChannelManager(connection);

    await expect(manager.open(identity, { service: "smarty-code-ordinary", version: 1 })).rejects.toThrow("subscribe failed");
    await expect(manager.open(identity, { service: "smarty-code-ordinary", version: 1 })).resolves.toBeDefined();
    manager.close();
  });
});
