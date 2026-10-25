// A' PR 2 identity rules (smarty-dev#15310): spoke roots reached through the hub's relay (viaSpoke), and the
// N4 republish of a v2 Main's own FABRIC_EV events (republishTopics, presence-only senders).
import { describe, expect, it } from "vitest";
import {
  BridgeNatsAllowListError,
  createBridgeNatsIdentityGate,
  parseBridgeNatsAllowSection,
  readBridgeNatsSeatPins,
  type BridgeNatsIdentity,
} from "../src/mesh/bridge-nats-identity.js";
import {
  BRIDGE_NATS_REFUSAL,
  bridgeNatsDestinationClass,
  bridgeNatsEnvelopeId,
  bridgeNatsRelayEnvelopeId,
  type BridgeNatsPresenceRecord,
  type BridgeNatsPresenceView,
  type BridgeNatsSide,
  type BridgeNatsVerified,
  type BridgeNatsVerifiedIdentity,
} from "../src/mesh/bridge-nats-types.js";

// Fictional ids only: this repository is public.
const PAUL_SEAT = "session:00000000-0000-4000-8000-00000000000a";
const LANE = "session:00000000-0000-4000-8000-000000000011"; // a work-host lane on spoke "ryzen2"
const MOVED = "session:00000000-0000-4000-8000-000000000002"; // a moved lead on v2
const OTHER = "session:00000000-0000-4000-8000-000000000003";
const LINK = "v2";
const SPOKE = "ryzen2";
const BRIDGE = "ryzen1-bridge";
const NOW = 1_800_000_000_000;

const PINS = readBridgeNatsSeatPins({
  productOwner: { name: "Paul" },
  principals: [{ name: "Paul", orgInstance: { herdrAgent: "org", herdrWorkspaceLabel: "org (Paul)", fabricSession: PAUL_SEAT.slice(8) } }],
});

const ALLOW = {
  file: [{ kind: "root", id: LANE, viaSpoke: SPOKE, send: ["followUp", "steer"], receive: ["followUp", "steer"] }],
  nats: [{ kind: "root", id: MOVED, machine: "lead-v2", send: ["followUp", "steer"], receive: ["followUp", "steer"],
    republishTopics: ["fleet.work.*"] }],
};

/** The hub's configured spoke links. */
const OPTIONS = { link: LINK, natsMachine: BRIDGE, spokes: [SPOKE, "ryzen3"] } as const;

const gateAt = (allow: unknown = ALLOW, clock = { now: NOW }): BridgeNatsIdentity =>
  createBridgeNatsIdentityGate(parseBridgeNatsAllowSection(allow, OPTIONS), { pins: PINS, clock: () => clock.now });

const record = (fields: Partial<BridgeNatsPresenceRecord> & { id: string }): BridgeNatsPresenceRecord => ({
  name: `root-${fields.id.slice(-2)}`, kind: "main", rootId: fields.id, ownerHostId: `host-${fields.id.slice(-2)}`,
  ownerIdentityId: fields.id, ownerIncarnation: "inc-1", live: true, observedAt: NOW, ...fields,
});
const view = (side: BridgeNatsSide, records: BridgeNatsPresenceRecord[]): BridgeNatsPresenceView =>
  ({ side, records: id => records.filter(entry => entry.id === id), list: () => records });

/** The hub's file mesh: the lane as spoke ryzen2's direct mirror; the moved lead as this link's mirror. */
const hub = (extra: BridgeNatsPresenceRecord[] = [], lane = true) => view("file", [
  ...(lane ? [record({ id: LANE, mirroredBy: SPOKE })] : []),
  record({ id: MOVED, mirroredBy: LINK }),
  ...extra,
]);
const nats = (extra: BridgeNatsPresenceRecord[] = [], base = true) => view("nats", [
  ...(base ? [record({ id: MOVED, machine: "lead-v2" }), record({ id: LANE, machine: BRIDGE })] : []),
  ...extra,
]);

const ok = (verified: BridgeNatsVerified): BridgeNatsVerifiedIdentity => {
  if (!verified.ok) throw new Error(`expected verified, got ${verified.refuse}: ${verified.reason}`);
  return verified.identity;
};
const code = (value: { ok: boolean; refuse?: string }): string | undefined => value.ok ? undefined : value.refuse;

describe("allow-list: viaSpoke and republishTopics", () => {
  it("refuses viaSpoke when no spoke links are configured, and a malformed spoke list", () => {
    expect(() => parseBridgeNatsAllowSection({ file: [{ kind: "root", id: LANE, viaSpoke: SPOKE }] }, { link: LINK, natsMachine: BRIDGE }))
      .toThrow(/configured spoke link/);
    for (const spokes of [[LINK], ["bad name"], [SPOKE, SPOKE]]) {
      expect(() => parseBridgeNatsAllowSection({}, { link: LINK, natsMachine: BRIDGE, spokes })).toThrow(BridgeNatsAllowListError);
    }
    expect(parseBridgeNatsAllowSection({ nats: [{ kind: "root", id: MOVED, machine: "lead-v2", republishTopics: ["ops.owner", "fleet.work.tasks"] }] },
      OPTIONS).entries).toHaveLength(1);
  });

  it("parses a spoke root and a republishing NATS root, and classes the spoke root", () => {
    const parsed = parseBridgeNatsAllowSection(ALLOW, OPTIONS);
    const lane = parsed.entries.find(entry => entry.kind === "root" && entry.id === LANE)!;
    expect(lane).toMatchObject({ side: "file", viaSpoke: SPOKE });
    expect(bridgeNatsDestinationClass(lane)).toBe("spokeRoot");
    expect(parsed.entries.find(entry => entry.kind === "root" && entry.id === MOVED)).toMatchObject({ republishTopics: ["fleet.work.*"] });
  });

  it.each([
    ["viaSpoke on a NATS root", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", viaSpoke: SPOKE }] }, "allow.nats[0].viaSpoke"],
    ["viaSpoke naming this link", { file: [{ kind: "root", id: LANE, viaSpoke: LINK }] }, "allow.file[0].viaSpoke"],
    ["a bad spoke name", { file: [{ kind: "root", id: LANE, viaSpoke: "bad name" }] }, "allow.file[0].viaSpoke"],
    ["viaSpoke on a principal seat", { file: [{ kind: "root", id: PAUL_SEAT, principal: "Paul", viaSpoke: SPOKE }] }, "allow.file[0].viaSpoke"],
    ["viaSpoke on an actor", { file: [{ kind: "actor", rootId: LANE, name: "a", viaSpoke: SPOKE }] }, "allow.file[0]"],
    ["republishTopics on a file root", { file: [{ kind: "root", id: OTHER, republishTopics: ["fleet.work.*"] }] }, "allow.file[0].republishTopics"],
    ["republishTopics on an actor", { nats: [{ kind: "actor", rootId: MOVED, name: "a", machine: "lead-v2", receive: [], republishTopics: ["x"] }] }, "allow.nats[0]"],
    ["a bad republish pattern", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", republishTopics: ["fleet.>"] }] }, "allow.nats[0].republishTopics"],
    ["an unconfigured spoke link", { file: [{ kind: "root", id: LANE, viaSpoke: "ryzen9" }] }, "allow.file[0].viaSpoke"],
    ["a republish topic outside fleet.work and ops", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", republishTopics: ["github.*"] }] }, "allow.nats[0].republishTopics"],
    ["a bare fleet prefix", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", republishTopics: ["fleet.*"] }] }, "allow.nats[0].republishTopics"],
    ["fleet.work without a segment", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", republishTopics: ["fleet.work"] }] }, "allow.nats[0].republishTopics"],
    ["a spoke root that may receive stop", { file: [{ kind: "root", id: LANE, viaSpoke: SPOKE, receive: ["stop"] }] }, "allow.file[0].receive"],
  ])("refuses %s", (_name, raw, field) => {
    try {
      parseBridgeNatsAllowSection(raw, OPTIONS);
      expect.unreachable("parsed");
    } catch (error) {
      expect(error).toBeInstanceOf(BridgeNatsAllowListError);
      expect((error as BridgeNatsAllowListError).field).toBe(field);
    }
  });
});

describe("spoke roots: verified only as the hub's direct mirror held by their spoke link", () => {
  it("verifies a spoke root from its spoke link's mirror, as sender and as target", () => {
    const gate = gateAt();
    expect(ok(gate.verifySender("file", LANE, hub(), nats()))).toMatchObject({ side: "file", id: LANE, viaSpoke: SPOKE });
    expect(ok(gate.verifyTarget("file", LANE, hub(), nats()))).toMatchObject({ viaSpoke: SPOKE });
  });

  it.each([
    ["only a native hub record (no spoke mirror)", () => hub([record({ id: LANE })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
    ["a mirror held by another spoke link", () => hub([record({ id: LANE, mirroredBy: "ryzen3" })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
    ["a re-exported (bridgeVia) mirror", () => hub([record({ id: LANE, mirroredBy: SPOKE, bridgeVia: "hub" })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
    ["the spoke mirror next to a native record", () => hub([record({ id: LANE })]), BRIDGE_NATS_REFUSAL.ambiguous],
    ["the spoke mirror next to another link's mirror", () => hub([record({ id: LANE, mirroredBy: "ryzen3" })]), BRIDGE_NATS_REFUSAL.ambiguous],
    ["a stale spoke mirror", () => hub([record({ id: LANE, mirroredBy: SPOKE, observedAt: NOW - 15_001 })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
    ["a spoke mirror that is not live", () => hub([record({ id: LANE, mirroredBy: SPOKE, live: false })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
  ])("refuses %s", (_name, presence, expected) => {
    expect(code(gateAt().verifySender("file", LANE, presence(), nats()))).toBe(expected);
  });

  it("refuses a spoke root whose id also answers natively on NATS (a spoofed spoke sender)", () => {
    const spoofed = nats([record({ id: LANE, machine: "other-v2" })]);
    expect(code(gateAt().verifySender("file", LANE, hub(), spoofed))).toBe(BRIDGE_NATS_REFUSAL.ambiguous);
  });

  it("keeps PR 1's rule for entries without viaSpoke: another link's mirror is no sender", () => {
    const gate = gateAt({ file: [{ kind: "root", id: OTHER }] });
    expect(code(gate.verifySender("file", OTHER, view("file", [record({ id: OTHER, mirroredBy: SPOKE })]), nats([], false))))
      .toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
  });

  it("applies the spoke-root class both ways", () => {
    const gate = gateAt();
    const lane = ok(gate.verifySender("file", LANE, hub(), nats()));
    const moved = ok(gate.verifyTarget("nats", MOVED, nats(), hub()));
    expect(gate.allow({ direction: "toNats", sender: lane, target: moved, subject: { type: "control", operation: "followUp" } }).ok).toBe(true);
    const movedSender = ok(gate.verifySender("nats", MOVED, nats(), hub(), "lead-v2"));
    const laneTarget = ok(gate.verifyTarget("file", LANE, hub(), nats()));
    expect(gate.allow({ direction: "toFile", sender: movedSender, target: laneTarget, subject: { type: "control", operation: "steer" } }).ok).toBe(true);
    expect(code(gate.allow({ direction: "toFile", sender: movedSender, target: laneTarget, subject: { type: "control", operation: "stop" } })))
      .toBe(BRIDGE_NATS_REFUSAL.destinationClassRefused);
  });
});

describe("N4: an event's claimed sender is presence-checked only, and republished as the link", () => {
  it("verifies the claimed sender on its pinned machine and marks it presence-only", () => {
    const sender = ok(gateAt().verifyEventSender(MOVED, nats(), hub()));
    expect(sender).toMatchObject({ side: "nats", id: MOVED, machine: "lead-v2", presenceOnly: true });
  });

  it.each([
    ["on another machine than its pin", () => nats([record({ id: MOVED, machine: "other-v2" })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
    ["only under the bridge machine (a file identity echoed back)", () => nats([record({ id: MOVED, machine: BRIDGE })], false), BRIDGE_NATS_REFUSAL.sideMismatch],
    ["on two machines", () => nats([record({ id: MOVED, machine: "other-v2", live: false })]), BRIDGE_NATS_REFUSAL.ambiguous],
    ["not live", () => nats([record({ id: MOVED, machine: "lead-v2", live: false })], false), BRIDGE_NATS_REFUSAL.senderUnverified],
  ])("refuses a claimed sender %s", (_name, presence, expected) => {
    expect(code(gateAt().verifyEventSender(MOVED, presence(), hub()))).toBe(expected);
  });

  it("refuses an id that is not on allow.nats, and a file identity's id", () => {
    expect(code(gateAt().verifyEventSender(OTHER, nats([record({ id: OTHER, machine: "lead-v2" })]), hub()))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
    expect(code(gateAt().verifyEventSender(LANE, nats(), hub()))).toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
  });

  it("allows republish only on the sender's republishTopics, authored as bridge:<link>", () => {
    const gate = gateAt();
    const sender = ok(gate.verifyEventSender(MOVED, nats(), hub()));
    expect(gate.allowRepublish({ sender, topic: "fleet.work.pi-fabric.15310", kind: "wait" }))
      .toMatchObject({ ok: true, author: { id: `bridge:${LINK}`, name: "mesh-bridge", kind: "main" }, sender: { id: MOVED } });
    for (const topic of ["fleet.work", "fleet.workx", "ops.owner", "github.pull"]) {
      expect(code(gate.allowRepublish({ sender, topic, kind: "wait" }))).toBe(BRIDGE_NATS_REFUSAL.topicNotAllowed);
    }
  });

  it("refuses republish for a credential-verified, forged, stale or unlisted sender", () => {
    const clock = { now: NOW };
    const gate = gateAt(ALLOW, clock);
    const credential = ok(gate.verifySender("nats", MOVED, nats(), hub(), "lead-v2"));
    expect(code(gate.allowRepublish({ sender: credential, topic: "fleet.work.x.1", kind: "wait" }))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    const sender = ok(gate.verifyEventSender(MOVED, nats(), hub()));
    expect(code(gate.allowRepublish({ sender: { ...sender }, topic: "fleet.work.x.1", kind: "wait" }))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    clock.now = NOW + 15_001;
    expect(code(gate.allowRepublish({ sender, topic: "fleet.work.x.1", kind: "wait" }))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    const quiet = gateAt({ nats: [{ kind: "root", id: MOVED, machine: "lead-v2" }] });
    const unlisted = ok(quiet.verifyEventSender(MOVED, nats(), hub()));
    expect(code(quiet.allowRepublish({ sender: unlisted, topic: "fleet.work.x.1", kind: "wait" }))).toBe(BRIDGE_NATS_REFUSAL.topicNotAllowed);
  });

  it("never lets a presence-only identity send a control command or a topic route", () => {
    const gate = gateAt();
    const sender = ok(gate.verifyEventSender(MOVED, nats(), hub()));
    const lane = ok(gate.verifyTarget("file", LANE, hub(), nats()));
    expect(code(gate.allow({ direction: "toFile", sender, target: lane, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    expect(code(gate.allow({ direction: "toFile", sender, target: lane, subject: { type: "topic", topic: "fleet.work.x.1", kind: "wait" } })))
      .toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
  });
});

describe("relay envelope ids", () => {
  it("are deterministic per link, spoke and spoke event, and never collide with hub event ids", () => {
    const id = bridgeNatsRelayEnvelopeId(LINK, SPOKE, "evt-1");
    expect(id).toMatch(/^bnr1-[0-9a-f]{48}$/);
    expect(id).toBe(bridgeNatsRelayEnvelopeId(LINK, SPOKE, "evt-1"));
    expect(id).not.toBe(bridgeNatsRelayEnvelopeId(LINK, "ryzen3", "evt-1"));
    expect(id).not.toBe(bridgeNatsRelayEnvelopeId("v3", SPOKE, "evt-1"));
    expect(id.slice(5)).not.toBe(bridgeNatsEnvelopeId(LINK, "evt-1").slice(5));
  });
});
