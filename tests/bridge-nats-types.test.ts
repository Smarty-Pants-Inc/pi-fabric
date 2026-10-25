import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  BRIDGE_NATS_CLASS_RECEIVE,
  BRIDGE_NATS_SEAT_RECEIVE,
  bridgeNatsDestinationClass,
  BRIDGE_NATS_DESTINATION_SIDE,
  BRIDGE_NATS_MESH_DIRECTION,
  BRIDGE_NATS_OPERATIONS,
  BRIDGE_NATS_SESSION_ID,
  BRIDGE_NATS_SOURCE_SIDE,
  bridgeNatsAckEnvelopeId,
  bridgeNatsCommandId,
  bridgeNatsEnvelopeId,
  bridgeNatsLinkIdentity,
  bridgeNatsMappedCommandId,
  bridgeNatsMeshIdentity,
  bridgeNatsTopicIdempotencyKey,
  type BridgeNatsVerifiedIdentity,
} from "../src/mesh/bridge-nats-types.js";

// The NATS transport's message-id rule (src/transport/nats/inbox.ts validMessageId) and the control
// plane's event-id rule (control-plane.ts isNatsControlEventId), restated: the minted id must pass both.
const validMessageId = (id: string): boolean => id.length > 0 && id === id.trim() && !/[\u0000-\u001f\u007f]/.test(id);
const isNatsControlEventId = (id: string): boolean => id.length > 0 && !/[\s\u0000-\u001f\u007f]/.test(id);

// Fictional ids only: this repository is public.
const UUID_A = "00000000-0000-4000-8000-00000000000a";
const UUID_B = "00000000-0000-4000-8000-00000000000b";
const SEAT = `session:${UUID_B}`;

describe("bridge-nats contract ids", () => {
  it("mints a deterministic, link-scoped NATS envelope id that NATS and the control plane accept", () => {
    const a = bridgeNatsEnvelopeId("nats-intel1", "0b8f6a52-6a0e-4f43-9a51-7c1d3f0e2a11");
    expect(a).toBe(bridgeNatsEnvelopeId("nats-intel1", "0b8f6a52-6a0e-4f43-9a51-7c1d3f0e2a11"));
    expect(a).not.toBe(bridgeNatsEnvelopeId("nats-other", "0b8f6a52-6a0e-4f43-9a51-7c1d3f0e2a11"));
    expect(a).not.toBe(bridgeNatsEnvelopeId("nats-intel1", "0b8f6a52-6a0e-4f43-9a51-7c1d3f0e2a12"));
    expect(a).toMatch(/^bnf1-[0-9a-f]{48}$/);
    expect(validMessageId(a) && isNatsControlEventId(a)).toBe(true);
    // Reserved NATS id spaces: a minted command id is never an ACK or an operator relay refusal.
    expect(a.startsWith("ack:") || a.startsWith("relay:")).toBe(false);
    // A separator inside a field cannot make two different (link, event) pairs collide.
    expect(bridgeNatsEnvelopeId("a\0b", "c")).not.toBe(bridgeNatsEnvelopeId("a", "b\0c"));
  });

  it("answers a NATS command with the ACK id a native NATS owner would send", () => {
    const id = bridgeNatsEnvelopeId("link", "event");
    expect(bridgeNatsAckEnvelopeId(id)).toBe(`ack:${id}`);
    expect(bridgeNatsAckEnvelopeId("4f1c2c4e-native")).toBe("ack:4f1c2c4e-native");
  });

  it("derives commandId exactly as the control plane does for both carriers", () => {
    const input = { hostId: "bridge:nats-intel1", ownerHostId: SEAT, ownerIdentityId: SEAT, targetId: SEAT,
      operation: "followUp" as const, idempotencyKey: "k" };
    // control-plane.ts: JSON.stringify([hostId, ownerHostId, ownerIdentityId, targetId, operation, idempotencyKey, notRunAttempt])
    const expected = createHash("sha256").update(JSON.stringify([input.hostId, input.ownerHostId, input.ownerIdentityId,
      input.targetId, input.operation, input.idempotencyKey, 0])).digest("hex");
    expect(bridgeNatsCommandId(input)).toBe(expected);
    expect(bridgeNatsCommandId({ ...input, notRunAttempt: 1 })).not.toBe(expected);
  });

  it("maps a commandId per hop so two senders re-using one id never collide on the far side", () => {
    const original = "c0ffee";
    const a = bridgeNatsMappedCommandId("link", "toFile", SEAT, original);
    expect(a).toMatch(/^[0-9a-f]{64}$/);
    expect(a).toBe(bridgeNatsMappedCommandId("link", "toFile", SEAT, original));
    // Another verified sender, the other direction or another link: a different far-side id.
    expect(a).not.toBe(bridgeNatsMappedCommandId("link", "toFile", `session:${UUID_A}`, original));
    expect(a).not.toBe(bridgeNatsMappedCommandId("link", "toNats", SEAT, original));
    expect(a).not.toBe(bridgeNatsMappedCommandId("link2", "toFile", SEAT, original));
    expect(a).not.toBe(original);
    expect(isNatsControlEventId(a)).toBe(true);
  });

  it("names the link's own identity like the file bridge's refusal sender", () => {
    expect(bridgeNatsLinkIdentity("nats-intel1")).toEqual({ id: "bridge:nats-intel1", name: "mesh-bridge", kind: "main" });
  });

  it("keys N3 topic followUps by link and file event id", () => {
    const key = bridgeNatsTopicIdempotencyKey("link", "event-1");
    expect(key).toMatch(/^[0-9a-f]{64}$/);
    expect(key).toBe(bridgeNatsTopicIdempotencyKey("link", "event-1"));
    expect(key).not.toBe(bridgeNatsTopicIdempotencyKey("link", "event-2"));
    expect(key).not.toBe(bridgeNatsTopicIdempotencyKey("link2", "event-1"));
  });

  it("states the destination class table from the entry alone", () => {
    expect(bridgeNatsDestinationClass({ kind: "root", side: "nats", principal: "Kate" })).toBe("seat");
    expect(bridgeNatsDestinationClass({ kind: "root", side: "file", principal: "Paul" })).toBe("seat");
    expect(bridgeNatsDestinationClass({ kind: "root", side: "nats" })).toBe("movedLead");
    expect(bridgeNatsDestinationClass({ kind: "root", side: "file" })).toBe("fileParticipant");
    expect(bridgeNatsDestinationClass({ kind: "actor", side: "file" })).toBe("fileParticipant");
    expect(bridgeNatsDestinationClass({ kind: "actor", side: "nats" })).toBe("refused");
    expect(BRIDGE_NATS_CLASS_RECEIVE).toEqual({ seat: ["followUp", "cancel"], movedLead: ["followUp", "steer", "ask", "cancel"],
      fileParticipant: ["followUp", "steer", "ask", "cancel"], spokeRoot: ["followUp", "steer", "ask", "cancel"], refused: [] });
    expect(bridgeNatsDestinationClass({ kind: "root", side: "file", viaSpoke: "ryzen2" })).toBe("spokeRoot");
    expect(BRIDGE_NATS_CLASS_RECEIVE.seat).toBe(BRIDGE_NATS_SEAT_RECEIVE);
    expect(Object.isFrozen(BRIDGE_NATS_CLASS_RECEIVE) && Object.isFrozen(BRIDGE_NATS_CLASS_RECEIVE.movedLead)).toBe(true);
  });

  it("maps directions to sides and MeshBridge directions", () => {
    expect(BRIDGE_NATS_SOURCE_SIDE.toNats).toBe("file");
    expect(BRIDGE_NATS_DESTINATION_SIDE.toNats).toBe("nats");
    expect(BRIDGE_NATS_SOURCE_SIDE.toFile).toBe("nats");
    expect(BRIDGE_NATS_DESTINATION_SIDE.toFile).toBe("file");
    expect(BRIDGE_NATS_MESH_DIRECTION).toEqual({ toNats: "toRemote", toFile: "toLocal" });
    expect(Object.isFrozen(BRIDGE_NATS_OPERATIONS)).toBe(true);
  });

  it("accepts only the canonical session id form smarty-dev org_target.canonical() produces", () => {
    expect(BRIDGE_NATS_SESSION_ID.test(SEAT)).toBe(true);
    expect(BRIDGE_NATS_SESSION_ID.test(UUID_B)).toBe(false);
    expect(BRIDGE_NATS_SESSION_ID.test(`session:${UUID_B.toUpperCase()}`)).toBe(false);
    expect(BRIDGE_NATS_SESSION_ID.test(`${SEAT} `)).toBe(false);
  });

  it("builds the crossing MeshIdentity only from the verified identity", () => {
    const identity = { side: "nats", id: SEAT, name: "org-kate", kind: "main",
      sessionId: UUID_B, ownerHostId: "h", ownerIdentityId: "i", machine: "kate-v2",
      entry: {} as BridgeNatsVerifiedIdentity["entry"], verifiedAt: 1 } satisfies BridgeNatsVerifiedIdentity;
    expect(bridgeNatsMeshIdentity(identity)).toEqual({ id: identity.id, name: "org-kate", kind: "main", sessionId: UUID_B });
  });
});
