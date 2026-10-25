import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  BridgeNatsAllowListError,
  BridgeNatsSeatPinsError,
  bridgeNatsDryRun,
  checkBridgeNatsSeatPins,
  createBridgeNatsIdentityGate,
  loadBridgeNatsSeatPins,
  parseBridgeNatsAllowSection,
  readBridgeNatsSeatPins,
  type BridgeNatsIdentity,
} from "../src/mesh/bridge-nats-identity.js";
import {
  BRIDGE_NATS_REFUSAL,
  type BridgeNatsCommandRecord,
  type BridgeNatsPresenceRecord,
  type BridgeNatsPresenceView,
  type BridgeNatsSide,
  type BridgeNatsVerifiedIdentity,
} from "../src/mesh/bridge-nats-types.js";

// Fictional ids only: this repository is public.
const PAUL_SEAT = "session:00000000-0000-4000-8000-00000000000a";
const KATE_SEAT = "session:00000000-0000-4000-8000-00000000000b";
const KATE_ROTATION = "session:00000000-0000-4000-8000-0000000000bb";
const LEAD = "session:00000000-0000-4000-8000-000000000001"; // a Ryzen 1 lead (file)
const MOVED = "session:00000000-0000-4000-8000-000000000002"; // a moved lead (NATS)
const STRANGER = "session:00000000-0000-4000-8000-000000000099";
const SUPERVISOR = "0123456789abcdef0123456789abcdef"; // a file actor owned by LEAD
const LINK = "v2";
const BRIDGE = "ryzen1-bridge";
const NOW = 1_800_000_000_000;

const ORG_JSON = {
  productOwner: { name: "Paul" },
  principals: [
    { name: "Paul", orgInstance: { herdrAgent: "org", herdrWorkspaceLabel: "org (Paul)", fabricSession: PAUL_SEAT.slice(8) } },
    { name: "Kate", orgInstance: { herdrAgent: "org-kate", herdrWorkspaceLabel: "org (Kate)", fabricSession: KATE_SEAT.slice(8) } },
  ],
  keeper: { pins: [{ name: "org-kate", session: KATE_ROTATION.slice(8) }, { name: "someone-else", session: STRANGER.slice(8) }] },
};
const PINS = readBridgeNatsSeatPins(ORG_JSON);

const ALLOW = {
  file: [
    { kind: "root", id: LEAD, send: ["followUp", "steer"], receive: ["followUp", "steer"], principals: ["herdr:paul"] },
    { kind: "root", id: PAUL_SEAT, principal: "Paul", send: ["followUp"], receive: ["followUp", "cancel"] },
    { kind: "actor", rootId: LEAD, name: "supervisor", send: ["steer", "followUp"], receive: [] },
  ],
  nats: [
    { kind: "root", id: MOVED, machine: "lead-v2", send: ["followUp", "steer", "cancel"], receive: ["followUp", "steer"], receiveTopics: ["fleet.work.*", "ops.owner"] },
    { kind: "root", id: KATE_SEAT, principal: "Kate", machine: "kate-v2", receive: ["followUp", "cancel"] },
  ],
};

const config = () => parseBridgeNatsAllowSection(ALLOW, { link: LINK, natsMachine: BRIDGE });

const record = (side: BridgeNatsSide, fields: Partial<BridgeNatsPresenceRecord> & { id: string }): BridgeNatsPresenceRecord => ({
  name: fields.id.startsWith("session:") ? `root-${fields.id.slice(-3)}` : "supervisor",
  kind: fields.id.startsWith("session:") ? "main" : "actor",
  rootId: fields.id.startsWith("session:") ? fields.id : LEAD,
  ownerHostId: `host-${fields.id.slice(-3)}`, ownerIdentityId: fields.id.startsWith("session:") ? fields.id : LEAD,
  ownerIncarnation: "inc-1", live: true, observedAt: NOW,
  ...(side === "nats" ? { machine: "lead-v2" } : {}),
  ...fields,
});

const view = (side: BridgeNatsSide, records: BridgeNatsPresenceRecord[]): BridgeNatsPresenceView => ({
  side, records: id => records.filter(entry => entry.id === id), list: () => records,
});

const fileView = (extra: BridgeNatsPresenceRecord[] = []) => view("file", [
  record("file", { id: LEAD }), record("file", { id: PAUL_SEAT }), record("file", { id: SUPERVISOR }),
  // NATS identities this link mirrors into the file mesh.
  record("file", { id: MOVED, mirroredBy: LINK }), record("file", { id: KATE_SEAT, mirroredBy: LINK }),
  ...extra,
]);
const natsView = (extra: BridgeNatsPresenceRecord[] = [], base = true) => view("nats", [
  ...(base ? [
    record("nats", { id: MOVED, machine: "lead-v2" }), record("nats", { id: KATE_SEAT, machine: "kate-v2" }),
    // File identities this link mirrors into KV, under the bridge machine.
    record("nats", { id: LEAD, machine: BRIDGE }), record("nats", { id: PAUL_SEAT, machine: BRIDGE }),
  ] : []),
  ...extra,
]);

const gateAt = (clock = { now: NOW }): BridgeNatsIdentity => createBridgeNatsIdentityGate(config(), { pins: PINS, clock: () => clock.now });

const identity = (verified: ReturnType<BridgeNatsIdentity["verifySender"]>): BridgeNatsVerifiedIdentity => {
  if (!verified.ok) throw new Error(`expected a verified identity, got ${verified.refuse}: ${verified.reason}`);
  return verified.identity;
};
const refusal = (verified: { ok: boolean; refuse?: string }): string | undefined => verified.ok ? undefined : verified.refuse;

describe("parseBridgeNatsAllowSection", () => {
  it("normalizes, attaches the home side, applies defaults and freezes", () => {
    const parsed = config();
    expect(parsed).toMatchObject({ version: 1, link: LINK, natsMachine: BRIDGE });
    expect(parsed.entries.map(entry => entry.side)).toEqual(["file", "file", "file", "nats", "nats"]);
    const kate = parsed.entries.find(entry => entry.kind === "root" && entry.id === KATE_SEAT)!;
    expect(kate.send).toEqual(["followUp"]); // default
    expect(Object.isFrozen(parsed) && Object.isFrozen(parsed.entries) && Object.isFrozen(kate) && Object.isFrozen(kate.send)).toBe(true);
    expect(() => (parsed.entries as unknown[]).push({})).toThrow();
    expect(parseBridgeNatsAllowSection(undefined, { link: LINK, natsMachine: BRIDGE }).entries).toEqual([]);
  });

  it.each([
    ["an unknown top key", { file: [], extra: [] }, "allow"],
    ["an unknown entry key", { file: [{ kind: "root", id: LEAD, label: "x" }] }, "allow.file[0]"],
    ["a non-canonical root id (upper case)", { file: [{ kind: "root", id: "session:0000000A-0000-4000-8000-00000000000F" }] }, "allow.file[0].id"],
    ["a bare uuid", { file: [{ kind: "root", id: LEAD.slice(8) }] }, "allow.file[0].id"],
    ["a trailing newline", { file: [{ kind: "root", id: `${LEAD}\n` }] }, "allow.file[0].id"],
    ["a duplicate in one list", { file: [{ kind: "root", id: LEAD }, { kind: "root", id: LEAD }] }, "allow.file[1]"],
    ["one identity on both sides", { file: [{ kind: "root", id: LEAD }], nats: [{ kind: "root", id: LEAD, machine: "lead-v2" }] }, "allow.nats[0]"],
    ["a NATS root without a machine pin", { nats: [{ kind: "root", id: MOVED }] }, "allow.nats[0].machine"],
    ["a NATS seat without a machine pin", { nats: [{ kind: "root", id: KATE_SEAT, principal: "Kate" }] }, "allow.nats[0].machine"],
    ["a NATS actor without a machine pin", { nats: [{ kind: "actor", rootId: MOVED, name: "r", receive: [] }] }, "allow.nats[0].machine"],
    ["a duplicate actor id pin", { file: [{ kind: "actor", rootId: LEAD, name: "a", id: SUPERVISOR }, { kind: "actor", rootId: LEAD, name: "b", id: SUPERVISOR }] }, "allow.file[1]"],
    ["a machine pin in the file list", { file: [{ kind: "root", id: LEAD, machine: "x" }] }, "allow.file[0].machine"],
    ["a machine pin on the bridge machine", { nats: [{ kind: "root", id: MOVED, machine: BRIDGE }] }, "allow.nats[0].machine"],
    ["a seat that may receive steer", { nats: [{ kind: "root", id: KATE_SEAT, principal: "Kate", machine: "kate-v2", receive: ["followUp", "steer"] }] }, "allow.nats[0].receive"],
    ["a file seat that may receive ask", { file: [{ kind: "root", id: PAUL_SEAT, principal: "Paul", receive: ["ask"] }] }, "allow.file[0].receive"],
    ["a moved lead that may receive stop", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", receive: ["followUp", "stop"] }] }, "allow.nats[0].receive"],
    ["a file root that may receive setModel", { file: [{ kind: "root", id: LEAD, receive: ["setModel"] }] }, "allow.file[0].receive"],
    ["a file actor that may receive setThinking", { file: [{ kind: "actor", rootId: LEAD, name: "s", receive: ["setThinking"] }] }, "allow.file[0].receive"],
    ["a NATS actor that may receive anything", { nats: [{ kind: "actor", rootId: MOVED, name: "r", machine: "lead-v2", receive: ["followUp"] }] }, "allow.nats[0].receive"],
    ["topics on an actor", { file: [{ kind: "actor", rootId: LEAD, name: "s", receiveTopics: ["ops.owner"] }] }, "allow.file[0].receiveTopics"],
    ["a principal on an actor", { file: [{ kind: "actor", rootId: LEAD, name: "s", principal: "Paul" }] }, "allow.file[0]"],
    ["an unknown operation", { file: [{ kind: "root", id: LEAD, send: ["tell"] }] }, "allow.file[0].send"],
    ["a repeated operation", { file: [{ kind: "root", id: LEAD, send: ["followUp", "followUp"] }] }, "allow.file[0].send"],
    ["a bad topic pattern", { nats: [{ kind: "root", id: MOVED, machine: "lead-v2", receiveTopics: ["fleet.*.x"] }] }, "allow.nats[0].receiveTopics"],
    ["an unknown kind", { file: [{ kind: "agent", id: LEAD }] }, "allow.file[0].kind"],
  ])("refuses %s", (_name, raw, field) => {
    try {
      parseBridgeNatsAllowSection(raw, { link: LINK, natsMachine: BRIDGE });
      expect.unreachable("parsed");
    } catch (error) {
      expect(error).toBeInstanceOf(BridgeNatsAllowListError);
      expect((error as BridgeNatsAllowListError).field).toBe(field);
      expect((error as Error).message).not.toContain(LEAD.slice(8)); // never echoes a value
    }
  });

  it("refuses a bad link name or an operator bridge machine", () => {
    expect(() => parseBridgeNatsAllowSection({}, { link: "bad link", natsMachine: BRIDGE })).toThrow(BridgeNatsAllowListError);
    expect(() => parseBridgeNatsAllowSection({}, { link: LINK, natsMachine: "fabric-ops" })).toThrow(BridgeNatsAllowListError);
  });
});

describe("org.json seat pins", () => {
  it("reads seats and every bound session, smarty-send style", () => {
    expect(PINS.seats.get("Kate")).toBe(KATE_SEAT);
    expect(PINS.bound.get(KATE_ROTATION)).toBe("Kate"); // keeper pin under a protected Herdr name
    expect(PINS.bound.has(STRANGER)).toBe(false); // keeper pin under an unprotected name
  });

  it.each([
    ["no product owner", { ...ORG_JSON, productOwner: {} }],
    ["a product owner without a seat", { ...ORG_JSON, productOwner: { name: "Marisela" } }],
    ["an incomplete orgInstance", { ...ORG_JSON, principals: [ORG_JSON.principals[0], { name: "Kate", orgInstance: { herdrAgent: "org-kate", fabricSession: KATE_SEAT } }] }],
    ["an upper-case session", { ...ORG_JSON, principals: [ORG_JSON.principals[0], { name: "Kate", orgInstance: { ...ORG_JSON.principals[1]!.orgInstance, fabricSession: KATE_SEAT.slice(8).toUpperCase() } }] }],
    ["two principals sharing a session", { ...ORG_JSON, principals: [ORG_JSON.principals[0], { name: "Kate", orgInstance: { ...ORG_JSON.principals[1]!.orgInstance, fabricSession: PAUL_SEAT } }] }],
    ["no principals", { ...ORG_JSON, principals: [] }],
  ])("fails closed on %s", (_name, doc) => {
    expect(() => readBridgeNatsSeatPins(doc)).toThrow(BridgeNatsSeatPinsError);
  });

  it("requires a seat entry to equal its principal's fabricSession exactly", () => {
    const wrong = parseBridgeNatsAllowSection({ nats: [{ kind: "root", id: MOVED, principal: "Kate", machine: "kate-v2" }] }, { link: LINK, natsMachine: BRIDGE });
    expect(() => checkBridgeNatsSeatPins(wrong, PINS)).toThrow(/orgInstance.fabricSession/);
    const unknown = parseBridgeNatsAllowSection({ nats: [{ kind: "root", id: KATE_SEAT, principal: "Marisela", machine: "kate-v2" }] }, { link: LINK, natsMachine: BRIDGE });
    expect(() => checkBridgeNatsSeatPins(unknown, PINS)).toThrow(/not a principal/);
  });

  it("refuses a principal's bound session listed without declaring the principal, rotation included", () => {
    for (const id of [KATE_SEAT, KATE_ROTATION]) {
      const undeclared = parseBridgeNatsAllowSection({ nats: [{ kind: "root", id, machine: "kate-v2" }] }, { link: LINK, natsMachine: BRIDGE });
      expect(() => createBridgeNatsIdentityGate(undeclared, { pins: PINS })).toThrow(/bound org session/);
    }
    // A rotation can never be a declared seat either: the seat is the static fabricSession only.
    const rotation = parseBridgeNatsAllowSection({ nats: [{ kind: "root", id: KATE_ROTATION, principal: "Kate", machine: "kate-v2" }] }, { link: LINK, natsMachine: BRIDGE });
    expect(() => checkBridgeNatsSeatPins(rotation, PINS)).toThrow(/orgInstance.fabricSession/);
  });

  describe("loading from a file", () => {
    let dir: string | undefined;
    afterEach(() => { if (dir) fs.rmSync(dir, { recursive: true, force: true }); dir = undefined; });

    it("loads a private file and refuses a group/other-writable one or a missing one", () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-identity-"));
      const file = path.join(dir, "org.json");
      fs.writeFileSync(file, JSON.stringify(ORG_JSON), { mode: 0o600 });
      expect(loadBridgeNatsSeatPins(file)).toMatchObject({ source: file });
      if (process.geteuid) {
        fs.chmodSync(file, 0o666);
        expect(() => loadBridgeNatsSeatPins(file)).toThrow(/writable/);
      }
      expect(() => loadBridgeNatsSeatPins(path.join(dir!, "missing.json"))).toThrow(/not found/);
    });
  });
});

describe("BridgeNatsIdentity: senders and targets come only from presence", () => {
  it("verifies a NATS sender on the delivery's source machine and builds its identity from presence", () => {
    const gate = gateAt();
    const verified = identity(gate.verifySender("nats", MOVED, natsView(), fileView(), "lead-v2"));
    expect(verified).toMatchObject({ side: "nats", id: MOVED, name: "root-002", kind: "main", machine: "lead-v2",
      ownerHostId: "host-002", ownerIdentityId: MOVED, ownerIncarnation: "inc-1" });
    expect(Object.isFrozen(verified)).toBe(true);
  });

  it.each([
    ["another source machine", () => natsView(), "other-v2", BRIDGE_NATS_REFUSAL.senderUnverified],
    ["no source machine", () => natsView(), undefined, BRIDGE_NATS_REFUSAL.senderUnverified],
    ["the bridge's own machine as source", () => natsView(), BRIDGE, BRIDGE_NATS_REFUSAL.sideMismatch],
    ["a presence that is not live", () => natsView([record("nats", { id: MOVED, live: false })], false), "lead-v2", BRIDGE_NATS_REFUSAL.senderUnverified],
    ["a stale observation", () => natsView([record("nats", { id: MOVED, observedAt: NOW - 15_001 })], false), "lead-v2", BRIDGE_NATS_REFUSAL.senderUnverified],
    ["the id on two machines (even a dead record)", () => natsView([record("nats", { id: MOVED, machine: "other-v2", live: false })]), "lead-v2", BRIDGE_NATS_REFUSAL.ambiguous],
    ["the id also under the bridge machine", () => natsView([record("nats", { id: MOVED, machine: BRIDGE })]), "lead-v2", BRIDGE_NATS_REFUSAL.ambiguous],
    ["no presence at all", () => natsView([], false), "lead-v2", BRIDGE_NATS_REFUSAL.senderUnverified],
  ])("refuses a NATS sender with %s", (_name, views, machine, code) => {
    expect(refusal(gateAt().verifySender("nats", MOVED, views(), fileView(), machine))).toBe(code);
  });

  it("refuses a file identity echoed back from its own KV mirror (a loop)", () => {
    // LEAD exists on NATS only as this link's mirror under the bridge machine.
    expect(refusal(gateAt().verifySender("nats", LEAD, natsView(), fileView(), BRIDGE))).toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
    expect(refusal(gateAt().verifySender("nats", LEAD, natsView(), fileView(), "lead-v2"))).toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
  });

  it("refuses an id that answers natively on both sides", () => {
    const split = fileView([record("file", { id: MOVED })]).records(MOVED).filter(entry => entry.mirroredBy === undefined);
    expect(split).toHaveLength(1);
    expect(refusal(gateAt().verifySender("nats", MOVED, natsView(), view("file", split), "lead-v2"))).toBe(BRIDGE_NATS_REFUSAL.ambiguous);
  });

  it("refuses an identity that is not allow-listed, or listed on the other side", () => {
    const stranger = natsView([record("nats", { id: STRANGER, machine: "lead-v2" })]);
    expect(refusal(gateAt().verifySender("nats", STRANGER, stranger, fileView(), "lead-v2"))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
    // LEAD is allow-listed on file; a native NATS record for it is a different identity on the wrong side.
    const impostor = natsView([record("nats", { id: LEAD, machine: "lead-v2" })], false);
    expect(refusal(gateAt().verifySender("nats", LEAD, impostor, view("file", []), "lead-v2"))).toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
  });

  it("enforces a NATS machine pin", () => {
    const moved = natsView([record("nats", { id: KATE_SEAT, machine: "elsewhere" })], false);
    expect(refusal(gateAt().verifySender("nats", KATE_SEAT, moved, view("file", []), "elsewhere"))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    expect(refusal(gateAt().verifyTarget("nats", KATE_SEAT, moved, view("file", [])))).toBe(BRIDGE_NATS_REFUSAL.targetUnavailable);
  });

  it("verifies file senders only from native file presence", () => {
    const gate = gateAt();
    expect(identity(gate.verifySender("file", LEAD, fileView(), natsView())).side).toBe("file");
    // MOVED exists in the file mesh only as this link's mirror: it never sends file -> NATS.
    expect(refusal(gate.verifySender("file", MOVED, fileView(), natsView()))).toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
    // Another link's mirror is no sender for this link (PR 1 has no relay).
    const relayed = view("file", [record("file", { id: STRANGER, mirroredBy: "ryzen2" })]);
    expect(refusal(gate.verifySender("file", STRANGER, relayed, natsView()))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
  });

  it("matches an actor by owner root + name, requires its root live with it, and honours an id pin", () => {
    const gate = gateAt();
    expect(identity(gate.verifySender("file", SUPERVISOR, fileView(), natsView())).kind).toBe("actor");
    const orphan = view("file", [record("file", { id: SUPERVISOR })]);
    expect(refusal(gate.verifySender("file", SUPERVISOR, orphan, natsView()))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    const renamed = fileView().records(SUPERVISOR).map(entry => ({ ...entry, name: "impostor" }));
    expect(refusal(gate.verifySender("file", SUPERVISOR, view("file", [record("file", { id: LEAD }), ...renamed]), natsView())))
      .toBe(BRIDGE_NATS_REFUSAL.notAllowed);
    const pinned = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({
      file: [{ kind: "root", id: LEAD }, { kind: "actor", rootId: LEAD, name: "supervisor", id: "f".repeat(32) }],
    }, { link: LINK, natsMachine: BRIDGE }), { pins: PINS, clock: () => NOW });
    expect(refusal(pinned.verifySender("file", SUPERVISOR, fileView(), natsView()))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
  });

  it("refuses NATS actors whose root runs on another machine", () => {
    const gate = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({
      nats: [{ kind: "root", id: MOVED, machine: "lead-v2" }, { kind: "actor", rootId: MOVED, name: "review", machine: "other-v2" }],
    }, { link: LINK, natsMachine: BRIDGE }), { pins: PINS, clock: () => NOW });
    const actor = record("nats", { id: "a".repeat(32), name: "review", rootId: MOVED, kind: "actor", machine: "other-v2" });
    const views = natsView([record("nats", { id: MOVED, machine: "lead-v2" }), actor], false);
    expect(refusal(gate.verifySender("nats", actor.id, views, view("file", []), "other-v2"))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
  });

  it("never lets an actor record resolve to a session id (a root's, a seat's, a rotation's)", () => {
    const gate = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({
      file: [{ kind: "root", id: PAUL_SEAT, principal: "Paul" }, { kind: "root", id: LEAD },
        { kind: "actor", rootId: LEAD, name: "worker", receive: ["followUp", "steer"] }],
      nats: [{ kind: "root", id: MOVED, machine: "lead-v2" },
        { kind: "actor", rootId: MOVED, name: "worker", machine: "lead-v2", send: ["followUp"], receive: [] }],
    }, { link: LINK, natsMachine: BRIDGE }), { pins: PINS, clock: () => NOW });
    // The audit's F1 sequence: an unpinned NATS actor entry; a fresh, live record on its machine claims the
    // name of that actor but carries a principal's seat session as its id.
    for (const forged of [KATE_SEAT, KATE_ROTATION, PAUL_SEAT, LEAD]) {
      const nats = natsView([record("nats", { id: MOVED, machine: "lead-v2" }),
        record("nats", { id: forged, kind: "actor", name: "worker", rootId: MOVED, ownerIdentityId: MOVED, machine: "lead-v2" })], false);
      expect(refusal(gate.verifySender("nats", forged, nats, view("file", []), "lead-v2"))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
    }
    // The same on the file side: an actor record with a seat's id never gets the actor's (wider) class.
    const file = view("file", [record("file", { id: LEAD }), record("file", { id: PAUL_SEAT, kind: "actor", name: "worker", rootId: LEAD })]);
    expect(refusal(gate.verifyTarget("file", PAUL_SEAT, file, natsView([], false)))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
    // Counterexample that must keep working: a real actor id with its live owner root.
    const good = natsView([record("nats", { id: MOVED, machine: "lead-v2" }),
      record("nats", { id: "b".repeat(32), kind: "actor", name: "worker", rootId: MOVED, machine: "lead-v2" })], false);
    expect(identity(gate.verifySender("nats", "b".repeat(32), good, view("file", []), "lead-v2")).kind).toBe("actor");
  });

  it("requires an actor's owner root observation to be fresh too", () => {
    const gate = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({
      nats: [{ kind: "actor", rootId: MOVED, name: "worker", machine: "lead-v2", receive: [] }],
    }, { link: LINK, natsMachine: BRIDGE }), { pins: PINS, clock: () => NOW });
    const actor = record("nats", { id: "c".repeat(32), kind: "actor", name: "worker", rootId: MOVED, machine: "lead-v2" });
    const stale = natsView([record("nats", { id: MOVED, machine: "lead-v2", observedAt: NOW - 15_001 }), actor], false);
    expect(refusal(gate.verifySender("nats", actor.id, stale, view("file", []), "lead-v2"))).toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    const fresh = natsView([record("nats", { id: MOVED, machine: "lead-v2" }), actor], false);
    expect(identity(gate.verifySender("nats", actor.id, fresh, view("file", []), "lead-v2")).id).toBe(actor.id);
  });

  it("refuses agents and non-canonical roots even when an id matches", () => {
    const agent = view("file", [record("file", { id: LEAD, kind: "agent" })]);
    expect(refusal(gateAt().verifySender("file", LEAD, agent, natsView([], false)))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
    const off = view("file", [record("file", { id: LEAD, rootId: STRANGER })]);
    expect(refusal(gateAt().verifySender("file", LEAD, off, natsView([], false)))).toBe(BRIDGE_NATS_REFUSAL.notAllowed);
  });
});

describe("BridgeNatsIdentity.allow", () => {
  const pair = (clock = { now: NOW }) => {
    const gate = gateAt(clock);
    return {
      gate,
      lead: identity(gate.verifySender("file", LEAD, fileView(), natsView())),
      supervisor: identity(gate.verifySender("file", SUPERVISOR, fileView(), natsView())),
      moved: identity(gate.verifyTarget("nats", MOVED, natsView(), fileView())),
      kate: identity(gate.verifyTarget("nats", KATE_SEAT, natsView(), fileView())),
      movedSender: identity(gate.verifySender("nats", MOVED, natsView(), fileView(), "lead-v2")),
      leadTarget: identity(gate.verifyTarget("file", LEAD, fileView(), natsView())),
    };
  };

  it("allows an operation in the sender's send set and the target's receive set", () => {
    const { gate, lead, moved, movedSender, leadTarget, supervisor } = pair();
    expect(gate.allow({ direction: "toNats", sender: lead, target: moved, subject: { type: "control", operation: "steer" } }))
      .toMatchObject({ ok: true, principal: "none" });
    expect(gate.allow({ direction: "toFile", sender: movedSender, target: leadTarget, subject: { type: "control", operation: "followUp" } }).ok).toBe(true);
    expect(gate.allow({ direction: "toNats", sender: supervisor, target: moved, subject: { type: "control", operation: "steer" } }).ok).toBe(true);
  });

  it("refuses an operation outside either set; a seat never receives steer", () => {
    const { gate, lead, moved, kate, movedSender, leadTarget } = pair();
    // Outside the destination class: refused before the entry lists are even consulted.
    expect(refusal(gate.allow({ direction: "toNats", sender: lead, target: moved, subject: { type: "control", operation: "stop" } })))
      .toBe(BRIDGE_NATS_REFUSAL.destinationClassRefused);
    expect(refusal(gate.allow({ direction: "toNats", sender: lead, target: kate, subject: { type: "control", operation: "steer" } })))
      .toBe(BRIDGE_NATS_REFUSAL.destinationClassRefused);
    // Inside the class but not in the sender's send list.
    expect(refusal(gate.allow({ direction: "toNats", sender: lead, target: moved, subject: { type: "control", operation: "ask" } })))
      .toBe(BRIDGE_NATS_REFUSAL.operationNotAllowed);
    expect(gate.allow({ direction: "toNats", sender: lead, target: kate, subject: { type: "control", operation: "followUp" } }).ok).toBe(true);
    expect(refusal(gate.allow({ direction: "toFile", sender: movedSender, target: leadTarget, subject: { type: "control", operation: "cancel" } })))
      .toBe(BRIDGE_NATS_REFUSAL.operationNotAllowed);
  });

  it("applies the destination class table from the entry alone", () => {
    const gate = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({
      file: [{ kind: "root", id: LEAD, send: ["followUp", "steer", "ask", "cancel", "stop", "setModel", "setThinking"] },
        { kind: "root", id: PAUL_SEAT, principal: "Paul", receive: ["followUp", "cancel"] }],
      nats: [{ kind: "root", id: MOVED, machine: "lead-v2", send: ["followUp", "ask"], receive: ["followUp", "steer", "ask", "cancel"] },
        { kind: "actor", rootId: MOVED, name: "review", machine: "lead-v2" }],
    }, { link: LINK, natsMachine: BRIDGE }), { pins: PINS, clock: () => NOW });
    const actor = record("nats", { id: "a".repeat(32), name: "review", rootId: MOVED, kind: "actor", machine: "lead-v2" });
    const nats = natsView([actor]);
    const lead = identity(gate.verifySender("file", LEAD, fileView(), nats));
    const moved = identity(gate.verifyTarget("nats", MOVED, nats, fileView()));
    const natsActor = identity(gate.verifyTarget("nats", actor.id, nats, fileView()));
    const seat = identity(gate.verifyTarget("file", PAUL_SEAT, fileView(), nats));
    const movedSender = identity(gate.verifySender("nats", MOVED, nats, fileView(), "lead-v2"));
    const send = (target: BridgeNatsVerifiedIdentity, operation: "followUp" | "steer" | "ask" | "cancel" | "stop" | "setModel" | "setThinking", sender = lead) =>
      gate.allow({ direction: target.side === "nats" ? "toNats" : "toFile", sender, target, subject: { type: "control", operation } });
    for (const operation of ["followUp", "steer", "ask", "cancel"] as const) expect(send(moved, operation).ok).toBe(true);
    for (const operation of ["stop", "setModel", "setThinking"] as const) {
      expect(refusal(send(moved, operation))).toBe(BRIDGE_NATS_REFUSAL.destinationClassRefused);
    }
    // anything else (a NATS actor): refused before publish, for every operation
    for (const operation of ["followUp", "steer", "ask", "cancel"] as const) {
      expect(refusal(send(natsActor, operation))).toBe(BRIDGE_NATS_REFUSAL.destinationClassRefused);
    }
    // a principal seat on the file side: followUp and cancel only
    expect(send(seat, "followUp", movedSender).ok).toBe(true);
    expect(refusal(send(seat, "ask", movedSender))).toBe(BRIDGE_NATS_REFUSAL.destinationClassRefused);
  });

  it("refuses identities it did not issue, however they are shaped, and stale ones", () => {
    const clock = { now: NOW };
    const { gate, lead, moved } = pair(clock);
    const forged = { ...lead } as BridgeNatsVerifiedIdentity;
    expect(refusal(gate.allow({ direction: "toNats", sender: forged, target: moved, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.senderUnverified);
    const other = pair().moved; // issued by another gate instance
    expect(refusal(gate.allow({ direction: "toNats", sender: lead, target: other, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.targetUnavailable);
    clock.now = NOW + 15_001;
    expect(refusal(gate.allow({ direction: "toNats", sender: lead, target: moved, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.targetUnavailable);
  });

  it("refuses identities on the wrong side for the direction", () => {
    const { gate, lead, moved, movedSender } = pair();
    expect(refusal(gate.allow({ direction: "toFile", sender: lead, target: moved, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
    expect(refusal(gate.allow({ direction: "toNats", sender: movedSender, target: moved, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.sideMismatch);
  });

  it("carries a principal only when the sender's entry lists it, else strips it", () => {
    const { gate, lead, moved } = pair();
    const subject = { type: "control", operation: "followUp" } as const;
    expect(gate.allow({ direction: "toNats", sender: lead, target: moved, subject, principal: { id: "herdr:paul", binding: "herdr-client" } }))
      .toMatchObject({ ok: true, principal: "carry" });
    expect(gate.allow({ direction: "toNats", sender: lead, target: moved, subject, principal: { id: "herdr:kate", binding: "herdr-client" } }))
      .toMatchObject({ ok: true, principal: "strip" });
    expect(gate.allow({ direction: "toNats", sender: lead, target: moved, subject,
      principal: { id: "herdr:paul", binding: "forged" } as never })).toMatchObject({ ok: true, principal: "strip" });
  });

  it("routes topics file -> NATS only, to roots that list them; a native publisher never sends control", () => {
    const { gate, lead, moved, kate, movedSender, leadTarget } = pair();
    const factory = { side: "file", native: true, fromId: "factory-host" } as const;
    const topic = (name: string) => ({ type: "topic", topic: name, kind: "pr.wake" } as const);
    expect(gate.allow({ direction: "toNats", sender: factory, target: moved, subject: topic("fleet.work.pi-fabric.15310") }).ok).toBe(true);
    expect(gate.allow({ direction: "toNats", sender: factory, target: moved, subject: topic("ops.owner") }).ok).toBe(true);
    expect(gate.allow({ direction: "toNats", sender: lead, target: moved, subject: topic("fleet.work.x"), principal: { id: "p", binding: "org-agent" } }))
      .toMatchObject({ ok: true, principal: "strip" });
    for (const name of ["fleet.work", "fleet.workx", "ops.owner.x", "github.pull"]) {
      expect(refusal(gate.allow({ direction: "toNats", sender: factory, target: moved, subject: topic(name) }))).toBe(BRIDGE_NATS_REFUSAL.topicNotAllowed);
    }
    expect(refusal(gate.allow({ direction: "toNats", sender: factory, target: kate, subject: topic("ops.owner") }))).toBe(BRIDGE_NATS_REFUSAL.topicNotAllowed);
    expect(refusal(gate.allow({ direction: "toFile", sender: movedSender, target: leadTarget, subject: topic("ops.owner") }))).toBe(BRIDGE_NATS_REFUSAL.topicNotAllowed);
    expect(refusal(gate.allow({ direction: "toNats", sender: factory, target: moved, subject: { type: "control", operation: "followUp" } })))
      .toBe(BRIDGE_NATS_REFUSAL.notAllowed);
  });
});

describe("BridgeNatsIdentity.verifyAck", () => {
  const claim = (fields: Partial<BridgeNatsCommandRecord> = {}): BridgeNatsCommandRecord => ({
    v: 1, state: "sent", direction: "toNats", origin: "control", originalCommandId: "orig", commandId: "mapped", sourceEventId: "file-event",
    envelopeId: "bnf1-abc", bodyDigest: "d".repeat(64), sourceMachine: BRIDGE, requesterId: LEAD, replyTo: "host-001",
    from: { id: LEAD, name: "lead", kind: "main" },
    targetId: MOVED, operation: "followUp", ownerHostId: "host-002", ownerIdentityId: MOVED, ownerIncarnation: "inc-1",
    targetMachine: "lead-v2", deadlineAt: NOW + 30_000, claimedAt: NOW, expiresAt: NOW + 86_400_000 + 30_000, ...fields,
  });
  const ack = { side: "nats" as const, fromId: MOVED, targetId: MOVED, commandId: "mapped", sourceMachine: "lead-v2",
    bodyDigest: "d".repeat(64), eventId: "ack:bnf1-abc" };

  it("accepts only the claimed owner's ACK, from its machine, for the same body and envelope", () => {
    const gate = gateAt();
    expect(gate.verifyAck(claim(), ack)).toEqual({ ok: true });
    for (const bad of [{ sourceMachine: "other-v2" }, { bodyDigest: "e".repeat(64) }, { eventId: "ack:bnf1-other" },
      { fromId: STRANGER }, { commandId: "orig" }, { targetId: KATE_SEAT }, { side: "file" as const }]) {
      expect(refusal(gate.verifyAck(claim(), { ...ack, ...bad }))).toBe(BRIDGE_NATS_REFUSAL.ackUnbound);
    }
  });

  it("binds a file ACK to the claimed owner and target", () => {
    const gate = gateAt();
    const { envelopeId: _envelope, targetMachine: _machine, ...base } = claim({ direction: "toFile", targetId: LEAD, ownerIdentityId: LEAD });
    const toFile: BridgeNatsCommandRecord = base;
    expect(gate.verifyAck(toFile, { side: "file", fromId: LEAD, targetId: LEAD, commandId: "mapped", eventId: "evt" })).toEqual({ ok: true });
    expect(refusal(gate.verifyAck(toFile, { side: "file", fromId: SUPERVISOR, targetId: LEAD, commandId: "mapped", eventId: "evt" })))
      .toBe(BRIDGE_NATS_REFUSAL.ackUnbound);
  });
});

describe("bridgeNatsDryRun", () => {
  it("reports what each entry resolves to without writing anything", () => {
    const report = bridgeNatsDryRun(gateAt(), { file: fileView(), nats: natsView() }, NOW);
    expect(report).toMatchObject({ v: 1, link: LINK, natsMachine: BRIDGE, live: 5, refused: 0 });
    expect(report.rows.find(row => row.key === `actor:${LEAD}/supervisor`)).toMatchObject({ status: "live", id: SUPERVISOR, mirrorTo: "nats" });
    expect(report.rows.find(row => row.key === KATE_SEAT)).toMatchObject({ status: "live", machine: "kate-v2", principal: "Kate", mirrorTo: "file" });
    const down = bridgeNatsDryRun(gateAt(), { file: fileView(), nats: natsView([record("nats", { id: MOVED, machine: "other-v2" })]) }, NOW);
    expect(down.rows.find(row => row.key === MOVED)).toMatchObject({ status: BRIDGE_NATS_REFUSAL.ambiguous });
  });
});
