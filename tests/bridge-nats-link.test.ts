// Lane L2 (smarty-dev#15310): the link composition with L3's REAL allow gate, under the real
// MeshBridge and a real file MeshStore; the NATS port is a recording fake (L4 owns real-broker E2E).
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { NatsCarrierRecord } from "../src/mesh/bridge-nats-carrier.js";
import { createNatsFileLink, type NatsCarrierSide } from "../src/mesh/bridge-nats-file.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection, type BridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { BRIDGE_NATS_LEDGER_LOCK, openBridgeNatsLedger, type FileBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import type { NatsLinkConfig } from "../src/mesh/bridge-nats-config.js";
import { natsCarrierLedger, natsLinkAuthorizers, natsLinkMirrorAllowed, startNatsLink } from "../src/mesh/bridge-nats-link.js";
import { bridgeNatsMappedCommandId, type BridgeNatsAllowSection } from "../src/mesh/bridge-nats-types.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
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

const natsCommand = (id: string, operation: string, commandId: string) => ({
  id, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: fileLead.id, operation, from: natsLead,
  body: { version: 1, commandId, targetId: fileLead.id, operation, ownerIncarnation: "inc-file-lead", message: "from v2",
    replyTo: natsLead.id, requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
});

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

  it("startNatsLink links (L5's config shape, L3's default ledger, the store handed to the port) and stops cleanly", async () => {
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
