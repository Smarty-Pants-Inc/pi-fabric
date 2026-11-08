// Lane L2 (smarty-dev#15310): the link composition with L3's REAL allow gate, under the real
// MeshBridge and a real file MeshStore; the NATS port is a recording fake (L4 owns real-broker E2E).
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFileActorDirectory, readFileActorsForMirror } from "../src/mesh/bridge-nats-actors.js";
import type { NatsCarrierRecord } from "../src/mesh/bridge-nats-carrier.js";
import { createNatsFileLink, NatsCarrierDeferredError, type NatsCarrierSide } from "../src/mesh/bridge-nats-file.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection, type BridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { BRIDGE_NATS_LEDGER_LOCK, openBridgeNatsLedger, type FileBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsLinkAllowListConfig, parseNatsLinkConfig, type NatsLinkConfig } from "../src/mesh/bridge-nats-config.js";
import { NatsBridgePort, natsBridgeAdmit, type NatsBridgeInboxSource, type NatsBridgeSideConfig } from "../src/mesh/bridge-nats-port.js";
import { natsCarrierLedger, natsLinkAuthorizers, natsLinkFileTargetAllowed, natsLinkMirrorAllowed, startNatsLink } from "../src/mesh/bridge-nats-link.js";
import { bridgeNatsMappedCommandId, type BridgeNatsAllowSection } from "../src/mesh/bridge-nats-types.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { FabricControlPlane, type FabricControlCommand } from "../src/topology/control-plane.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import { kvWorld, seedNative } from "./helpers/bridge-nats-kv.js";
import {
  BRIDGE_MACHINE, FakePort, LINK, POD_MACHINE, fileLead, linkIdentity, natsLead, route, settle,
} from "./helpers/bridge-nats-l2-fakes.js";

const roots: string[] = [];
const stores: MeshStore[] = [];
const closers: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const ledgerIn = async (root: string): Promise<FileBridgeNatsLedger> => {
  // L3's real crash-durable ledger, in this test's own temp directory.
  const ledger = await openBridgeNatsLedger({ dir: path.join(root, "ledger") });
  closers.push(() => ledger.close());
  return ledger;
};
const NOW = Date.now();
const offList: MeshIdentity = { id: "session:55555555-5555-4555-8555-555555555555", name: "not-listed", kind: "main" };
const host = (identity: MeshIdentity): FabricHostRecord => ({
  format: 1, id: identity.id, rootId: identity.id, identity, startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 86_400_000,
});
const participant = (identity: MeshIdentity, ownerIncarnation: string): FabricParticipantRecord => ({
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  ownerIncarnation, name: identity.name, label: identity.name, status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp"], sessionId: identity.id.slice("session:".length), startedAt: NOW - 1_000, updatedAt: NOW,
  controlProtocol: "v1",
});
const pins: BridgeNatsSeatPins = { seats: new Map(), bound: new Map() };
const ALLOW: BridgeNatsAllowSection = {
  file: [{ kind: "root", id: fileLead.id, send: ["followUp", "ask", "cancel", "stop"], receive: ["followUp", "ask", "cancel"] }],
  nats: [{ kind: "root", id: natsLead.id, send: ["followUp", "ask", "cancel", "steer"], receive: ["followUp", "steer", "ask", "cancel"],
    receiveTopics: ["fleet.work.*"], machine: POD_MACHINE }],
};

const meshRoot = async (root: string): Promise<MeshStore> => {
  const store = new MeshStore(path.join(root, "hub"), 256 * 1024, 1_000);
  stores.push(store);
  for (const identity of [fileLead, offList]) {
    await store.put({ key: key("topology/hosts/", identity.id), value: host(identity), identity });
    await store.put({ key: key("topology/participants/", identity.id), value: participant(identity, `inc-${identity.name}`), identity });
  }
  return store;
};

const fakePort = (): FakePort => {
  const port = new FakePort();
  port.natsPresence = { hosts: [{ record: host(natsLead), expiresAt: NOW + 86_400_000, observedAt: NOW }],
    participants: [participant(natsLead, "inc-moved-lead")], reserved: [natsLead.id] };
  for (const [identity, machine, incarnation] of [[natsLead, POD_MACHINE, "inc-moved-lead"], [fileLead, BRIDGE_MACHINE, "inc-file-lead"],
    [linkIdentity, BRIDGE_MACHINE, "inc-link"]] as const) port.routes.set(identity.id, route(identity, machine, incarnation));
  return port;
};

const gatedFixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-link-"));
  roots.push(root);
  const store = await meshRoot(root);
  const port = fakePort();
  const allow = parseBridgeNatsAllowSection(ALLOW, { link: LINK, natsMachine: BRIDGE_MACHINE });
  const gate = createBridgeNatsIdentityGate(allow, { pins });
  let remote: NatsCarrierSide | undefined;
  const { authorize, authorizeTopic } = natsLinkAuthorizers(gate, { file: () => remote!.filePresence.view(), nats: () => port.presenceView() });
  const logs: string[] = [];
  const ledger = await ledgerIn(root);
  const link = createNatsFileLink({ store, localName: "ryzen1", link: LINK, cursorPath: path.join(root, "cursor.json"), port,
    ledger: natsCarrierLedger(ledger, LINK), authorize, authorizeTopic, mirrorAllowed: natsLinkMirrorAllowed(allow),
    waitForTarget: natsLinkFileTargetAllowed(allow, () => remote!.filePresence.view()),
    log: line => logs.push(line), resendDelayMs: 0, bridge: { presenceMs: 60_000 } });
  remote = link.remote;
  closers.push(() => link.bridge.stop());
  await link.bridge.start();
  const events = (topic: string): MeshEvent[] => store.read({ after: 0, limit: store.maxReadEvents }).filter(event => event.topic === topic);
  return { root, store, port, link, logs, ledger, events };
};

const fileCommand = (store: MeshStore, operation: string, commandId: string, extra: Record<string, unknown> = {}) => store.publish({
  topic: "fabric.control.command", kind: operation, from: fileLead, to: natsLead.id,
  data: { version: 1, commandId, targetId: natsLead.id, operation, replyTo: fileLead.id, ownerIncarnation: "inc-moved-lead",
    message: "hello", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, destinationRemoteHost: LINK, ...extra },
});

const natsCommand = (id: string, operation: string, commandId: string, target: MeshIdentity = fileLead) => ({
  id, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: target.id }, targetId: target.id, operation, from: natsLead,
  body: { version: 1, commandId, targetId: target.id, operation, ownerIncarnation: `inc-${target.name}`, message: "from v2",
    replyTo: natsLead.id, requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
});

/** Run with process.platform reporting `platform` (the refusal reads it at call time). */
const onPlatform = async <T>(platform: NodeJS.Platform, work: () => Promise<T>): Promise<T> => {
  const original = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { ...original, value: platform });
  try { return await work(); } finally { Object.defineProperty(process, "platform", original); }
};
/** Stated skip: start refuses on Windows by design (its refusal is tested below on every platform). */
const WINDOWS = process.platform === "win32";

describe("startNatsLink composition with L3's allow gate", () => {
  it("mirrors only allow-listed file identities into KV", async () => {
    const { port } = await gatedFixture();
    const ids = port.mirrors.at(-1)!.participants.map(entry => entry.id);
    expect(ids).toContain(fileLead.id);
    expect(ids).not.toContain(offList.id);
  });

  it("file -> NATS: the gate verifies both sides from presence; stop never crosses (destination class)", async () => {
    const { store, port, link, logs } = await gatedFixture();
    await fileCommand(store, "followUp", "c-follow");
    await fileCommand(store, "stop", "c-stop");
    const step = await link.bridge.step();
    expect(step.toRemote).toBe(1);
    expect(step.dropped).toBe(1);
    expect(port.sent.map(envelope => envelope.operation)).toEqual(["followUp"]);
    expect(logs.some(line => /BRIDGE_NATS_DESTINATION_CLASS_REFUSED|never receives stop/.test(line))).toBe(true);
    // The verified sender is answered at once (never a timeout): a rejected ACK from the owner the link
    // mirrors, with the link's stamp, notRun and the stable code, matched to its original commandId.
    const answer = store.read({ after: 0, limit: store.maxReadEvents }).find(event => event.topic === "fabric.control.ack" &&
      (event.data as Record<string, unknown>).commandId === "c-stop");
    expect(answer).toMatchObject({ kind: "rejected", to: fileLead.id, verification: "bridge", from: { id: natsLead.id },
      data: { accepted: false, notRun: true, errorCode: "BRIDGE_NATS_DESTINATION_CLASS_REFUSED", targetId: natsLead.id,
        ownerIncarnation: "inc-moved-lead", bridge: { from: LINK } } });
    // An off-list file root never sends, even to an allow-listed target.
    await store.publish({ topic: "fabric.control.command", kind: "followUp", from: offList, to: natsLead.id,
      data: { version: 1, commandId: "c-off", targetId: natsLead.id, operation: "followUp", replyTo: offList.id, message: "x",
        requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, destinationRemoteHost: LINK } });
    expect((await link.bridge.step()).toRemote).toBe(0);
    expect(port.sent).toHaveLength(1);
  });

  it("NATS -> file: an allowed ask is appended; an op the file root may not receive gets a notRun answer, never the file mesh", async () => {
    const { port, link, events } = await gatedFixture();
    const ask = port.deliver(natsCommand("env-ask", "ask", "n-ask"), POD_MACHINE, 1);
    const steer = port.deliver(natsCommand("env-steer", "steer", "n-steer"), POD_MACHINE, 2);
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    expect(events("fabric.control.command").map(event => event.kind)).toEqual(["ask"]);
    expect((events("fabric.control.command")[0]!.data as Record<string, unknown>).commandId)
      .toBe(bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "n-ask"));
    expect(ask.acked).toBe(1);
    expect(steer.acked).toBe(1);
    expect(port.sent.find(envelope => envelope.id === "ack:env-steer")).toMatchObject({ kind: "ack", operation: "rejected",
      to: { machine: POD_MACHINE, hostId: natsLead.id }, body: { accepted: false, notRun: true, commandId: "n-steer" } });
  });

  it("N3 topics cross only into the target's receiveTopics", async () => {
    const { store, port, link, logs } = await gatedFixture();
    const tool = { id: "fleet-tool:smarty-task-ryzen2", name: "smarty-task-ryzen2", kind: "main" as const };
    await store.publish({ topic: "fleet.work.tasks", kind: "answer", from: tool, to: natsLead.id, text: "done" });
    await store.publish({ topic: "ops.owner", kind: "inbox.age.alarm", from: tool, to: natsLead.id, text: "old inbox" });
    const step = await link.bridge.step();
    expect(step.toRemote).toBe(1);
    expect(port.sent.map(envelope => (envelope.body.data as { mesh: { topic: string } }).mesh.topic)).toEqual(["fleet.work.tasks"]);
    expect(logs.some(line => /BRIDGE_NATS_TOPIC_NOT_ALLOWED|may not receive ops\.owner/.test(line))).toBe(true);
  });

  it("the L3 ledger adapter round-trips a carrier record and maps claim statuses", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-ledger-"));
    roots.push(root);
    const ledger = await ledgerIn(root);
    const port = natsCarrierLedger(ledger, LINK);
    const record: NatsCarrierRecord = {
      v: 1, direction: "toNats", origin: "control", link: LINK, operation: "followUp", sourceCommandId: "orig",
      destinationCommandId: "f".repeat(64), sourceEventId: "file-event", destinationEventId: "bnf1-abc", targetId: natsLead.id,
      ownerHostId: natsLead.id, ownerIdentityId: natsLead.id, ownerMachine: POD_MACHINE, requesterId: fileLead.id, from: fileLead,
      principal: { id: "principal:paul", binding: "herdr-client" },
      sourceMachine: BRIDGE_MACHINE, replyTo: fileLead.id, bodyDigest: "a".repeat(64), deadlineAt: NOW + 1_000, expiresAt: NOW + 2_000,
      ownerIncarnation: "inc", body: { version: 1, commandId: "f".repeat(64) },
    };
    const first = await port.claim(record);
    expect(first.created).toBe(true);
    const { expiresAt: _expires, ...rest } = record;
    expect(first.record).toMatchObject(rest);
    expect((await port.claim(record)).created).toBe(false);
    expect(await port.lookup("toNats", record.destinationCommandId)).toMatchObject(rest);
    expect(await port.lookup("toFile", record.destinationCommandId)).toBeUndefined();
    await port.sent!("toNats", record.destinationCommandId, { sequence: 9 });
    expect(ledger.lookup("toNats", record.destinationCommandId)).toMatchObject({ state: "sent", destinationSequence: 9 });
    await port.settle("toNats", record.destinationCommandId, { accepted: true, at: NOW, fromId: natsLead.id, ackEventId: "ack:bnf1-abc", data: { accepted: true } });
    await port.forwarded!("toNats", record.destinationCommandId);
    expect(ledger.lookup("toNats", record.destinationCommandId)).toMatchObject({ state: "ackForwarded", ack: { accepted: true, fromId: natsLead.id } });
    const conflict = await port.claim({ ...record, bodyDigest: "b".repeat(64) });
    expect(conflict).toMatchObject({ created: false, record: { bodyDigest: "a".repeat(64) } });
    const topic = await port.claim({ ...record, origin: "topic", destinationCommandId: "e".repeat(64), requesterId: linkIdentity.id, from: linkIdentity });
    expect(topic.record.origin).toBe("topic");
    // The cancel mapping is kept both ways.
    const cancel = await port.claim({ ...record, operation: "cancel", destinationCommandId: "d".repeat(64), sourceCommandId: "orig-cancel",
      sourceCancelCommandId: "orig", cancelCommandId: "f".repeat(64) });
    expect(cancel.record).toMatchObject({ sourceCancelCommandId: "orig", cancelCommandId: "f".repeat(64) });
  });

  it("NATS -> file: a target not on allow.file is refused at once (notRun, stable code); an allow-listed one waits for KV", async () => {
    const { port, link, events } = await gatedFixture();
    // offList is a live native file root, but not allow-listed: never mirrored, never waited for.
    const stranger = port.deliver(natsCommand("env-stranger", "followUp", "n-stranger", offList), POD_MACHINE, 1);
    await settle();
    await link.bridge.step();
    expect(stranger.acked).toBe(1);
    expect(port.sent.find(envelope => envelope.id === "ack:env-stranger")).toMatchObject({ kind: "ack", operation: "rejected",
      to: { machine: POD_MACHINE, hostId: natsLead.id }, body: { accepted: false, notRun: true, errorCode: "BRIDGE_NATS_NOT_ALLOWED",
        commandId: "n-stranger" } });
    // The allow-listed file root missing from the link's KV view for a moment: a bounded wait, not a refusal.
    const route = port.routes.get(fileLead.id)!;
    port.routes.delete(fileLead.id);
    const listed = port.deliver(natsCommand("env-listed", "followUp", "n-listed"), POD_MACHINE, 2);
    await settle();
    await expect(link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(listed.termed).toEqual([]);
    expect(port.sent.find(envelope => envelope.id === "ack:env-listed")).toBeUndefined();
    port.routes.set(fileLead.id, route);
    await link.bridge.step();
    await link.bridge.step();
    expect(listed.acked).toBe(1);
    expect(events("fabric.control.command").map(event => (event.data as { bridge: { id: string } }).bridge.id)).toEqual(["env-listed"]);
  });

  // Keep: a trust boundary across actual config -> startNatsLink -> carrier -> port admission.
  // This fake still records wire sends, but applies production natsBridgeAdmit to mirror candidates
  // and exposes a KV route only for admitted rows. A dropped flag at either filter breaks the flow.
  describe.skipIf(WINDOWS)("fileFleetRoots through the production start pipeline", () => {
    const movedSeat: MeshIdentity = { id: "session:66666666-6666-4666-8666-666666666666", name: "org-kate", kind: "main" };
    const fileSeat: MeshIdentity = { id: "session:77777777-7777-4777-8777-777777777777", name: "org-paul", kind: "main" };
    const fileRotation: MeshIdentity = { id: "session:88888888-8888-4888-8888-888888888888", name: "org-paul-rotation", kind: "main" };
    const actor: MeshIdentity = { id: "a".repeat(32), name: "unlisted-worker", kind: "actor" };
    const fleetPins: BridgeNatsSeatPins = {
      seats: new Map([["Kate", movedSeat.id], ["Paul", fileSeat.id]]),
      bound: new Map([[movedSeat.id, "Kate"], [fileSeat.id, "Paul"], [fileRotation.id, "Paul"]]),
    };
    const actorRecord = (): FabricParticipantRecord => ({ ...participant(offList, "inc-not-listed"), id: actor.id,
      kind: "actor", rootId: offList.id, ownerHostId: offList.id, ownerIdentityId: offList.id, name: actor.name });
    const fleetFixture = async (fileFleetRoots: boolean | undefined, normalized = false) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-fleet-roots-"));
      roots.push(root);
      const initial = await meshRoot(root);
      for (const identity of [fileSeat, fileRotation]) {
        await initial.put({ key: key("topology/hosts/", identity.id), value: host(identity), identity });
        await initial.put({ key: key("topology/participants/", identity.id), value: participant(identity, `inc-${identity.name}`), identity });
      }
      await initial.put({ key: key("topology/participants/", actor.id), value: actorRecord(), identity: offList });
      initial.closeState();
      stores.pop();
      const config = parseNatsLinkConfig({ version: 1, name: "ryzen1", link: LINK,
        file: { mesh: path.join(root, "hub"), lockProtocol: 1 }, stateDir: path.join(root, "state"),
        allow: { ...ALLOW, ...(fileFleetRoots === undefined ? {} : { fileFleetRoots }), nats: [...ALLOW.nats!,
          { kind: "root", id: movedSeat.id, principal: "Kate", machine: POD_MACHINE,
            send: ["followUp", "steer", "ask", "cancel"], receive: ["followUp", "cancel"] }] },
        nats: { servers: ["tls://nats.invalid:4222"], machine: BRIDGE_MACHINE, credentials: path.join(root, "user.creds"),
          caFile: path.join(root, "ca.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
          pathBMachines: [BRIDGE_MACHINE, POD_MACHINE], operatorMachine: "fabric-ops" } });
      const port = fakePort();
      port.natsPresence = {
        hosts: [...port.natsPresence.hosts, { record: host(movedSeat), expiresAt: NOW + 86_400_000, observedAt: NOW }],
        participants: [...port.natsPresence.participants, participant(movedSeat, `inc-${movedSeat.name}`)],
        reserved: [...port.natsPresence.reserved, movedSeat.id],
      };
      port.routes.set(movedSeat.id, route(movedSeat, POD_MACHINE));
      let portConfig: NatsBridgeSideConfig | undefined;
      let fileStore: MeshStore | undefined;
      let admit: ReturnType<typeof natsBridgeAdmit> | undefined;
      const candidates: FabricParticipantRecord[][] = [];
      const controller = new AbortController();
      const logs: string[] = [];
      const handle = await startNatsLink(normalized ? { ...config, allow: natsLinkAllowListConfig(config) } : config, {
        signal: controller.signal, log: line => logs.push(line), factories: { seatPins: () => fleetPins,
          openPort: async (captured, options) => {
            portConfig = captured;
            fileStore = options.fileStore;
            admit = natsBridgeAdmit(captured.allow);
            const recordMirror = port.mirror.bind(port);
            port.mirror = async set => {
              candidates.push([...set.participants]); // Before L1's filter: a correct port must not hide an overbroad carrier selector.
              const participants = set.participants.filter(entry => admit!(entry, "toNats"));
              await recordMirror({ ...set, participants });
              for (const entry of set.participants) port.routes.delete(entry.id);
              for (const entry of participants) {
                const identity = set.hosts.find(host => host.record.id === entry.ownerHostId)?.record.identity;
                if (identity) port.routes.set(entry.id, route(identity, BRIDGE_MACHINE, entry.ownerIncarnation));
              }
            };
            return port;
          } },
      });
      closers.push(async () => { controller.abort(); await handle.closed; });
      const events = (topic: string) => fileStore!.read({ after: 0, limit: fileStore!.maxReadEvents }).filter(event => event.topic === topic);
      const publish = (operation: string, commandId: string, target = natsLead, from = offList, extra: Record<string, unknown> = {}) => fileStore!.publish({
        topic: "fabric.control.command", kind: operation, from, to: target.id,
        principal: { id: "herdr:paul", binding: "herdr-client" },
        data: { version: 1, commandId, targetId: target.id, operation, replyTo: from.id,
          ownerIncarnation: `inc-${target.name}`, message: "fleet message", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
          destinationRemoteHost: LINK, ...extra },
      });
      const answer = (commandId: string) => events("fabric.control.ack").find(event => (event.data as { commandId?: string }).commandId === commandId);
      const inbound = () => events("fabric.control.command").filter(event => event.verification === "bridge");
      const select = natsLinkMirrorAllowed(natsLinkAllowListConfig(config), fleetPins);
      return { port, portConfig: portConfig!, admit: admit!, select, candidates, publish, answer, inbound, logs };
    };

    it.each([undefined, false])("does not admit an unlisted live file root when fileFleetRoots is %s", async flag => {
      const f = await fleetFixture(flag);
      expect(f.admit(participant(offList, "inc-not-listed"), "toNats")).toBe(false);
      expect(f.port.mirrors.at(-1)!.participants.map(entry => entry.id)).not.toContain(offList.id);
      await f.publish("followUp", "fleet-off", movedSeat);
      await vi.waitFor(() => expect(f.answer("fleet-off")).toMatchObject({ kind: "rejected",
        data: { notRun: true, errorCode: "BRIDGE_NATS_NOT_ALLOWED" } }));
      expect(f.port.sent.filter(envelope => envelope.kind === "command")).toEqual([]);
    });

    it.each([false, true])("sends to a moved seat and receives followUp+steer with flag on (normalized=%s)", async normalized => {
      const f = await fleetFixture(true, normalized);
      expect(f.portConfig.allow).toMatchObject({ fileFleetRoots: true });
      expect(f.admit(participant(offList, "inc-not-listed"), "toNats")).toBe(true);
      expect(f.port.mirrors.at(-1)!.participants.map(entry => entry.id)).toContain(offList.id);
      await f.publish("followUp", "fleet-follow-seat", movedSeat);
      await vi.waitFor(() => expect(f.port.sent.find(envelope => envelope.kind === "command" && envelope.targetId === movedSeat.id))
        .toMatchObject({ from: offList, operation: "followUp", body: {
          commandId: bridgeNatsMappedCommandId(LINK, "toNats", offList.id, "fleet-follow-seat") } }));
      expect(f.port.sent.find(envelope => envelope.kind === "command" && envelope.targetId === movedSeat.id)?.principal).toBeUndefined();
      const follow = f.port.deliver(natsCommand("fleet-in-follow", "followUp", "fleet-in-follow", offList));
      const steer = f.port.deliver(natsCommand("fleet-in-steer", "steer", "fleet-in-steer", offList));
      await vi.waitFor(() => expect(f.inbound().map(event => event.kind)).toEqual(["followUp", "steer"]));
      await vi.waitFor(() => expect([follow.acked, steer.acked]).toEqual([1, 1]));
      expect(f.inbound().map(event => (event.data as { commandId: string }).commandId)).toEqual([
        bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "fleet-in-follow"),
        bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "fleet-in-steer"),
      ]);
      expect(f.inbound().every(event => event.to === offList.id && event.from.id === natsLead.id)).toBe(true);
      await f.publish("steer", "fleet-steer-seat", movedSeat);
      await vi.waitFor(() => expect(f.answer("fleet-steer-seat")).toMatchObject({ kind: "rejected",
        data: { notRun: true, errorCode: "BRIDGE_NATS_DESTINATION_CLASS_REFUSED" } }));
      expect(f.port.sent.filter(envelope => envelope.kind === "command").map(envelope => envelope.operation)).toEqual(["followUp"]);
    });

    it("both production KV filters exclude actors, pinned principal seats and rotations, not ordinary fleet roots", async () => {
      const f = await fleetFixture(true);
      const selected = f.candidates.at(-1)!.map(entry => entry.id);
      const mirrored = f.port.mirrors.at(-1)!.participants.map(entry => entry.id);
      expect(selected).toContain(offList.id);
      expect(mirrored).toContain(offList.id);
      expect(mirrored).toContain(fileLead.id); // Explicit file entries remain admitted.
      expect(f.select(participant(offList, "inc-not-listed"))).toBe(true);
      expect(f.select(participant(fileLead, "inc-file-lead"))).toBe(true);
      for (const candidate of [actorRecord(), participant(fileSeat, "inc-seat"), participant(fileRotation, "inc-rotation"),
        participant(movedSeat, "inc-moved-seat"), { ...participant(offList, "inc-bad"),
          id: "session:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", rootId: fileLead.id },
        { ...participant(offList, "inc-bad"), id: `${offList.id}\n`, rootId: `${offList.id}\n` }]) {
        expect(f.admit(candidate, "toNats"), candidate.id).toBe(false);
        expect(f.select(candidate), candidate.id).toBe(false);
        expect(selected, candidate.id).not.toContain(candidate.id);
        expect(mirrored, candidate.id).not.toContain(candidate.id);
      }
      expect(f.admit(participant(offList, "inc-not-listed"), "toFile", POD_MACHINE)).toBe(false);
    });

    it("does not dynamically grant ask or cancel in either carrier direction", async () => {
      const f = await fleetFixture(true);
      for (const operation of ["ask", "cancel"] as const) {
        const extra = operation === "cancel" ? { cancelCommandId: "fleet-original" } : {};
        await f.publish(operation, `fleet-out-${operation}`, natsLead, offList, extra);
        await vi.waitFor(() => expect(f.answer(`fleet-out-${operation}`)).toMatchObject({ kind: "rejected",
          data: { notRun: true, errorCode: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" } }));
        const envelope = natsCommand(`fleet-in-${operation}`, operation, `fleet-in-${operation}`, offList);
        const delivery = f.port.deliver({ ...envelope, body: { ...envelope.body, ...extra } });
        await vi.waitFor(() => expect(delivery.acked).toBe(1));
        expect(f.port.sent.find(sent => sent.id === `ack:fleet-in-${operation}`)).toMatchObject({ kind: "ack",
          body: { notRun: true, errorCode: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" } });
      }
      expect(f.port.sent.filter(envelope => envelope.kind === "command")).toEqual([]);
      expect(f.inbound()).toEqual([]);
    });

    it("keeps restrictive explicit entries authoritative at the carrier despite the fleet opt-in", async () => {
      const f = await fleetFixture(true);
      // ALLOW explicitly permits fileLead to send/receive followUp, but not steer.
      await f.publish("steer", "fleet-listed-steer", natsLead, fileLead);
      await vi.waitFor(() => expect(f.answer("fleet-listed-steer")).toMatchObject({ kind: "rejected",
        data: { notRun: true, errorCode: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" } }));
      const inbound = f.port.deliver(natsCommand("fleet-listed-in-steer", "steer", "fleet-listed-in-steer"));
      await vi.waitFor(() => expect(inbound.acked).toBe(1));
      expect(f.port.sent.find(envelope => envelope.id === "ack:fleet-listed-in-steer"))
        .toMatchObject({ kind: "ack", body: { notRun: true, errorCode: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" } });
      expect(f.inbound()).toEqual([]);
      await f.publish("followUp", "fleet-listed-follow", natsLead, fileLead);
      await vi.waitFor(() => expect(f.port.sent.filter(envelope => envelope.kind === "command").map(envelope => envelope.operation)).toEqual(["followUp"]));
    });
  });

  // Keep: a trust boundary. Real start, MeshBridge, native actor directory, production port/KV
  // admission, durable ledger and file owner control plane. Only broker KV and wire I/O are fake.
  describe.skipIf(WINDOWS)("fileAskTargets through start and the port's own fileActors path", () => {
    const seat: MeshIdentity = { id: "session:99999999-9999-4999-8999-999999999999", name: "org-kate", kind: "main" };
    const actor: MeshIdentity = { id: "b".repeat(32), name: "inbox", kind: "actor" };
    const otherActor: MeshIdentity = { id: "c".repeat(32), name: actor.name, kind: "actor" };
    const actorParticipant = (identity: MeshIdentity): FabricParticipantRecord => {
      const { sessionId: _session, ...owner } = participant(offList, "inc-not-listed");
      return { ...owner, id: identity.id, kind: "actor", name: identity.name, parentId: offList.id,
        capabilities: ["ask", "followUp", "steer", "fabric"] };
    };

    it.each([false, true])("returns a seat's actor ask as a bound ACK, never a new seat operation (normalized=%s)", async normalized => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-inbox-ask-"));
      roots.push(root);
      const initial = await meshRoot(root);
      for (const identity of [actor, otherActor]) await initial.put({
        key: key("topology/participants/", identity.id), value: actorParticipant(identity), identity: offList,
      });
      initial.closeState();
      stores.pop();
      const fileAskTargets = [{ actorId: actor.id, rootId: offList.id }];
      const config = parseNatsLinkConfig({ version: 1, name: "ryzen1", link: LINK,
        file: { mesh: path.join(root, "hub"), lockProtocol: 1 }, stateDir: path.join(root, "state"),
        allow: { fileFleetRoots: true, fileAskTargets, nats: [{ kind: "root", id: seat.id, principal: "Kate", machine: POD_MACHINE,
          send: ["ask", "followUp", "steer", "cancel"], receive: ["followUp", "cancel"] }] },
        nats: { servers: ["tls://nats.invalid:4222"], machine: BRIDGE_MACHINE, credentials: path.join(root, "user.creds"),
          caFile: path.join(root, "ca.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
          pathBMachines: [BRIDGE_MACHINE, POD_MACHINE], operatorMachine: "fabric-ops" } });
      const world = kvWorld();
      seedNative(world, POD_MACHINE, participant(seat, `inc-${seat.name}`), Date.now());
      const wire = new FakePort();
      // Scripted broker inbox, decoded by the real NatsBridgePort (including subject/source binding).
      const inbox: NatsBridgeInboxSource = { isVerified: () => true, async *consume(signal) {
        for await (const delivery of wire.inbox(signal)) {
          const envelope = delivery.envelope;
          yield { subject: `fabric.in.${BRIDGE_MACHINE}.${hashToken(envelope.to.hostId)}.${envelope.kind === "command" ? "cmd" : "ack"}.${hashToken(envelope.targetId)}.${delivery.sourceMachine}`,
            data: Buffer.from(JSON.stringify({ format: 1, sourceMachine: delivery.sourceMachine, envelope })),
            info: { streamSequence: delivery.streamSequence, deliveryCount: delivery.attempt },
            ackAck: async () => { await delivery.ack(); return true; }, nak: () => delivery.nak(),
            term: (reason?: string) => { void delivery.term(reason ?? "refused"); }, working: () => delivery.working() };
        }
      } };
      let captured: NatsBridgeSideConfig | undefined;
      let store: MeshStore | undefined;
      let port: NatsBridgePort | undefined;
      let ledger: FileBridgeNatsLedger | undefined;
      const rootMirrorActors: string[][] = [];
      const nativeActors: string[][] = [];
      const controller = new AbortController();
      const handle = await startNatsLink(normalized ? { ...config, allow: natsLinkAllowListConfig(config) } : config, {
        signal: controller.signal, log: () => undefined, factories: {
          seatPins: () => ({ seats: new Map([["Kate", seat.id]]), bound: new Map([[seat.id, "Kate"]]) }),
          openLedger: async dir => { ledger = await openBridgeNatsLedger({ dir }); return ledger; },
          openPort: async (portConfig, options) => {
            captured = portConfig;
            store = options.fileStore;
            const directory = await createFileActorDirectory(store, LINK);
            port = new NatsBridgePort({ link: LINK, inbox, admit: natsBridgeAdmit(portConfig.allow),
              transport: world.transport(BRIDGE_MACHINE, linkIdentity.id, envelope => wire.send(envelope)),
              fileActors: () => { const read = readFileActorsForMirror(directory); nativeActors.push(read.actors.map(entry => entry.id)); return read; },
              onClose: () => wire.close() });
            const mirror = port.mirror.bind(port);
            vi.spyOn(port, "mirror").mockImplementation(async (set, at) => {
              rootMirrorActors.push(set.participants.filter(entry => entry.kind === "actor").map(entry => entry.id));
              await mirror(set, at);
            });
            await port.start(options.signal);
            return port;
          },
        },
      });
      let owner: FabricControlPlane | undefined;
      closers.push(async () => { await owner?.close(); controller.abort(); await handle.closed; });
      const events = (topic: string) => store!.read({ after: 0, limit: store!.maxReadEvents }).filter(event => event.topic === topic);
      expect(captured!.allow).toMatchObject({ fileFleetRoots: true, fileAskTargets });
      expect(rootMirrorActors.length).toBeGreaterThan(0);
      expect(rootMirrorActors.every(ids => ids.length === 0)).toBe(true); // start still uses mirrorActors:false.
      expect(nativeActors.some(ids => ids.includes(actor.id) && ids.includes(otherActor.id))).toBe(true);
      await vi.waitFor(() => expect(port!.resolve(actor.id)).toMatchObject({ identity: actor,
        route: { machine: BRIDGE_MACHINE, ownerHostId: offList.id, ownerIdentityId: offList.id, live: true } }));
      expect(port!.resolve(otherActor.id)).toBeUndefined(); // Same owner AND name is still not an exact id pin.

      const receive = vi.fn((_command: FabricControlCommand) => ({ accepted: true, messageId: "actor-answer", result: { text: "inbox is clear" } }));
      owner = new FabricControlPlane(store!, offList, { enabled: true, hostId: offList.id, ownerIncarnation: "inc-not-listed", pollMs: 10 });
      owner.start(receive);
      const request = (id: string, operation: string, target = actor) => ({
        id, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: offList.id }, targetId: target.id, operation, from: seat,
        body: { version: 1, commandId: id, targetId: target.id, operation, replyTo: seat.id, ownerIncarnation: "inc-not-listed",
          message: "check the inbox", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
      });
      const ask = request("seat-inbox-ask", "ask");
      const delivery = wire.deliver(ask);
      await vi.waitFor(() => expect(wire.sent.find(sent => sent.id === `ack:${ask.id}`)).toMatchObject({
        kind: "ack", operation: "accepted", from: offList, targetId: actor.id,
        to: { machine: POD_MACHINE, hostId: seat.id }, body: { accepted: true, commandId: ask.body.commandId,
          targetId: actor.id, bodyDigest: controlBodyFingerprint(ask.body), result: { text: "inbox is clear" } },
      }));
      await vi.waitFor(() => expect(delivery.acked).toBe(1));
      expect(receive).toHaveBeenCalledOnce();
      const mapped = bridgeNatsMappedCommandId(LINK, "toFile", seat.id, ask.body.commandId);
      expect(receive.mock.calls[0]?.[0]).toMatchObject({ operation: "ask", commandId: mapped, targetId: actor.id });
      expect(events("fabric.control.command")).toHaveLength(1);
      expect(events("fabric.control.command")[0]).toMatchObject({ verification: "bridge", kind: "ask", from: seat, to: offList.id,
        data: { commandId: mapped, targetId: actor.id } });
      expect(events("fabric.control.ack")).toContainEqual(expect.objectContaining({ from: offList, to: seat.id,
        data: expect.objectContaining({ accepted: true, commandId: mapped, targetId: actor.id, result: { text: "inbox is clear" } }) }));
      await vi.waitFor(() => expect(ledger!.lookup("toFile", mapped)).toMatchObject({ state: "ackForwarded", operation: "ask",
        originalCommandId: ask.body.commandId, requesterId: seat.id, replyTo: seat.id, sourceMachine: POD_MACHINE,
        targetId: actor.id, bodyDigest: controlBodyFingerprint(ask.body), ack: { accepted: true, fromId: offList.id } }));

      const steer = wire.deliver(request("seat-inbox-steer", "steer"));
      const unlisted = wire.deliver(request("seat-other-inbox-ask", "ask", otherActor));
      await vi.waitFor(() => expect([steer.acked, unlisted.acked]).toEqual([1, 1]));
      expect(wire.sent.find(sent => sent.id === "ack:seat-inbox-steer")).toMatchObject({ kind: "ack",
        body: { accepted: false, notRun: true, errorCode: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" } });
      expect(wire.sent.find(sent => sent.id === "ack:seat-other-inbox-ask")).toMatchObject({ kind: "ack",
        body: { accepted: false, notRun: true, errorCode: "BRIDGE_NATS_NOT_ALLOWED" } });
      expect(receive).toHaveBeenCalledOnce();
      // The receive-only exception never turns the actor into an initiating sender. The real ACK
      // above still succeeds because it is an answer to the durable claim, not an inbound seat op.
      await store!.publish({ topic: "fabric.control.command", kind: "followUp", from: actor, to: seat.id,
        data: { version: 1, commandId: "actor-must-not-initiate", targetId: seat.id, operation: "followUp", replyTo: offList.id,
          ownerIncarnation: `inc-${seat.name}`, message: "unsolicited", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
          destinationRemoteHost: LINK } });
      await vi.waitFor(() => expect(events("fabric.control.ack")).toContainEqual(expect.objectContaining({
        data: expect.objectContaining({ commandId: "actor-must-not-initiate", accepted: false, notRun: true,
          errorCode: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" }) })));
      expect(wire.sent.filter(sent => sent.kind === "command")).toEqual([]);
      expect(events("fabric.control.command").filter(event => event.verification === "bridge").map(event => event.kind)).toEqual(["ask"]);
    });
  });

  it("startNatsLink refuses on Windows by name, before anything opens", async () => {
    await onPlatform("win32", async () => {
      await expect(startNatsLink({} as NatsLinkConfig, { signal: new AbortController().signal, log: () => undefined, factories: {
        openPort: async () => { throw new Error("opened"); } } }))
        .rejects.toMatchObject({ code: "BRIDGE_NATS_UNSUPPORTED_PLATFORM", message: "nats-link runs only on Linux and macOS" });
    });
  });

  it.skipIf(WINDOWS)("startNatsLink links (L5's config shape, L3's default ledger, the store handed to the port) and stops cleanly", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-start-"));
    roots.push(root);
    (await meshRoot(root)).closeState();
    stores.pop();
    const config: NatsLinkConfig = { version: 1, name: "ryzen1", link: LINK, file: { mesh: path.join(root, "hub"), lockProtocol: 1 },
      stateDir: path.join(root, "state"), allow: ALLOW, callTimeoutMs: 30_000, logLevel: "info",
      nats: { servers: ["tls://nats.invalid:4222"], machine: BRIDGE_MACHINE, credentials: path.join(root, "user.creds"),
        caFile: path.join(root, "ca.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
        pathBMachines: [BRIDGE_MACHINE, POD_MACHINE], operatorMachine: "fabric-ops", events: false, inboxLanes: 8 } };
    const controller = new AbortController();
    const logs: string[] = [];
    const port = fakePort();
    let handedStore: MeshStore | undefined;
    const handle = await startNatsLink(config, { signal: controller.signal, log: line => logs.push(line), factories: {
      openPort: async (_config, options) => { handedStore = options.fileStore; return port; }, seatPins: () => pins,
    } });
    expect(handedStore?.root).toBe(path.join(root, "hub"));
    expect(logs).toContain(`linked ryzen1 <-> ${LINK} (NATS machine ${BRIDGE_MACHINE})`);
    expect(port.mirrors.at(-1)!.participants.map(entry => entry.id)).toEqual([fileLead.id]);
    controller.abort();
    await handle.closed;
    expect(port.closed).toBe(true);
    // The default ledger is L3's, in the state dir, and its writer lock was released on stop.
    expect(fs.existsSync(path.join(root, "state", "ledger", BRIDGE_NATS_LEDGER_LOCK))).toBe(false);
    // ...so the next start takes it again.
    const again = await openBridgeNatsLedger({ dir: path.join(root, "state", "ledger") });
    await again.close();
    // A bad allow-list never opens anything.
    await expect(startNatsLink({ ...config, allow: { nats: [{ kind: "root", id: "session:not-a-uuid" }] } },
      { signal: new AbortController().signal, log: () => undefined, factories: { openPort: async () => { throw new Error("opened"); },
        seatPins: () => pins } })).rejects.toThrow(/allow/);
  });
});
