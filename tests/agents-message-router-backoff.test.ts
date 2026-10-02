import { describe, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import type { FabricControlCommand } from "../src/topology/control-plane.js";

const from = { id: "session:sender", name: "sender", kind: "main" as const };
const reason = "provider-backoff until 2026-10-01T08:35:16.663Z";

describe("AgentMessageRouter provider backoff receipts", () => {
  it.each(["followUp", "steer"] as const)("carries a held %s admission reason into the control ACK", async operation => {
    const deliverAgent = vi.fn(() => ({ queued: true as const, messageId: "held-message", routed: "main" as const,
      triggered: false, reason, pendingFollowUps: 1, oldestAgeS: 0 }));
    const router = new AgentMessageRouter(
      {} as never, { identity: from } as never,
      { id: "session:receiver", local: true, interactive: true, matches: id => id === "session:receiver", deliverAgent },
      { get: () => undefined } as never, undefined, binding => binding,
    );
    const command: FabricControlCommand = { version: 1, commandId: "durable-command", targetId: "session:receiver",
      operation, replyTo: from.id, requestedAt: Date.now(), message: "resume", triggerTurn: true };
    expect(await router.acceptControl(command, from, undefined, "bridge")).toMatchObject({
      accepted: true, messageId: "held-message", triggered: false, reason, pendingFollowUps: 1,
    });
    expect(deliverAgent).toHaveBeenCalledExactlyOnceWith({ from, verification: "bridge", message: "resume",
      delivery: operation, deliveryId: command.commandId, triggerTurn: true });
  });
});
