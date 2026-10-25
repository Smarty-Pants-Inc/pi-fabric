// A' PR 2 relay (smarty-dev#15310, lane L2b): the NATS link as one more hub link in the relay between
// bridges, under the real MeshBridge, a real file MeshStore, L3's real gate and ledger, and a recording
// fake NATS port. A spoke root on the hub is a mirror another link holds (remoteHost = the spoke link).
// The UDS relay itself is L3's (bridge-relay.ts) and the real-broker relay E2E is L4's.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { BridgeRelayUnavailableError } from "../src/mesh/bridge-relay.js";
import type { BridgePublish } from "../src/mesh/bridge.js";
import type { NatsLinkConfig } from "../src/mesh/bridge-nats-config.js";
import { readNatsLinkCounters, type NatsLinkCounterName } from "../src/mesh/bridge-nats-counters.js";
import { createNatsFileLink, type NatsCarrierSide } from "../src/mesh/bridge-nats-file.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection, type BridgeNatsSeatPins } from "../src/mesh/bridge-nats-identity.js";
import { openBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import {
  natsCarrierLedger, natsLinkAuthorizers, natsLinkBinPath, natsLinkFileTargetAllowed, natsLinkMirrorAllowed, startNatsLink,
  type NatsLinkFactories,
} from "../src/mesh/bridge-nats-link.js";
import { bridgeNatsMappedCommandId, bridgeNatsRelayEnvelopeId, type BridgeNatsAllowSection } from "../src/mesh/bridge-nats-types.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import {
  BRIDGE_MACHINE, FakePort, LINK, POD_MACHINE, fileLead, linkIdentity, natsLead, route, settle,
} from "./helpers/bridge-nats-l2-fakes.js";

const HUB = "hub-a";
const SPOKE = "spoke-a";
const CONTROL = "fabric.control.command";
const ACK = "fabric.control.ack";
/** A work-host lane on the spoke, allow-listed through its spoke link; and one on the same spoke that is not. */
const spokeLane: MeshIdentity = { id: "session:88888888-8888-4888-8888-888888888888", name: "spoke-lane", kind: "main" };
const unlisted: MeshIdentity = { id: "session:99999999-9999-4999-8999-999999999999", name: "spoke-other", kind: "main" };

const roots: string[] = [];
const closers: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
/** Stated skip: the link refuses to start on Windows by design (bridge-nats-link.test.ts tests the refusal). */
const WINDOWS = process.platform === "win32";

const NOW = Date.now();
const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
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
  file: [
    { kind: "root", id: fileLead.id, send: ["followUp", "ask", "cancel"], receive: ["followUp", "ask", "cancel"] },
    { kind: "root", id: spokeLane.id, viaSpoke: SPOKE, send: ["followUp", "ask", "cancel"], receive: ["followUp", "steer", "ask", "cancel"] },
  ],
  nats: [{ kind: "root", id: natsLead.id, send: ["followUp", "ask", "cancel", "steer"], receive: ["followUp", "steer", "ask", "cancel"],
    machine: POD_MACHINE }],
};

/** The hub: a native file root, and two roots the spoke link mirrors here directly (as `mesh-bridge run --remote spoke-a` does). */
const hubStore = async (root: string): Promise<MeshStore> => {
  const store = new MeshStore(path.join(root, "hub"), 256 * 1024, 1_000);
  closers.push(() => store.closeState());
  for (const [identity, stored] of [[fileLead, {}], [spokeLane, { remoteHost: SPOKE }], [unlisted, { remoteHost: SPOKE }]] as const) {
    await store.put({ key: key("topology/hosts/", identity.id), value: { ...host(identity), ...stored }, identity });
    await store.put({ key: key("topology/participants/", identity.id), value: { ...participant(identity, `inc-${identity.name}`), ...stored },
      identity });
  }
  return store;
};

const fakePort = (): FakePort => {
  const port = new FakePort();
  port.natsPresence = { hosts: [{ record: host(natsLead), expiresAt: NOW + 86_400_000, observedAt: NOW }],
    participants: [participant(natsLead, "inc-moved-lead")], reserved: [natsLead.id] };
  for (const [identity, machine] of [[natsLead, POD_MACHINE], [fileLead, BRIDGE_MACHINE], [linkIdentity, BRIDGE_MACHINE],
    [spokeLane, BRIDGE_MACHINE]] as const) port.routes.set(identity.id, route(identity, machine, `inc-${identity.name}`));
  return port;
};

class Counters {
  readonly counts = new Map<NatsLinkCounterName, number>();
  add(name: NatsLinkCounterName): void { this.counts.set(name, (this.counts.get(name) ?? 0) + 1); }
  get(name: NatsLinkCounterName): number { return this.counts.get(name) ?? 0; }
}

const relayFixture = async (relay: (destination: string, event: BridgePublish) => Promise<void> = async () => undefined) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-relay-"));
  roots.push(root);
  const store = await hubStore(root);
  const port = fakePort();
  const allow = parseBridgeNatsAllowSection(ALLOW, { link: LINK, natsMachine: BRIDGE_MACHINE, spokes: [SPOKE] });
  const gate = createBridgeNatsIdentityGate(allow, { pins });
  let remote: NatsCarrierSide | undefined;
  const { authorize, authorizeTopic } = natsLinkAuthorizers(gate, { file: () => remote!.filePresence.view(), nats: () => port.presenceView() });
  const ledger = await openBridgeNatsLedger({ dir: path.join(root, "ledger") });
  closers.push(() => ledger.close());
  const logs: string[] = [];
  const counters = new Counters();
  const relayed: Array<{ destination: string; event: BridgePublish }> = [];
  const link = createNatsFileLink({ store, localName: HUB, link: LINK, cursorPath: path.join(root, "cursor.json"), port,
    ledger: natsCarrierLedger(ledger, LINK), authorize, authorizeTopic, mirrorAllowed: natsLinkMirrorAllowed(allow),
    waitForTarget: natsLinkFileTargetAllowed(allow, () => remote!.filePresence.view()), counters,
    relay: async (destination, event) => { relayed.push({ destination, event }); await relay(destination, event); },
    log: line => logs.push(line), resendDelayMs: 0, bridge: { presenceMs: 60_000 } });
  remote = link.remote;
  closers.push(() => link.bridge.stop());
  await link.bridge.start();
  const hubEvents = (topic: string): MeshEvent[] => store.read({ after: 0, limit: store.maxReadEvents }).filter(event => event.topic === topic);
  return { root, store, port, link, logs, counters, relayed, hubEvents };
};

/** A spoke lane's command as MeshBridge hands it over the relay: the spoke link's stamp, not yet the hub hop. */
const spokeCommand = (commandId: string, eventId: string, from: MeshIdentity = spokeLane, spoke = SPOKE): BridgePublish => ({
  topic: CONTROL, kind: "followUp", from, to: natsLead.id,
  data: { version: 1, commandId, targetId: natsLead.id, operation: "followUp", replyTo: from.id, ownerIncarnation: "inc-moved-lead",
    message: "from the spoke", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, destinationRemoteHost: LINK,
    bridge: { from: spoke, id: eventId } },
});
/** The same, as the carrier receives it from MeshBridge (deliverRelay adds the hub hop). */
const viaHub = (pub: BridgePublish): BridgePublish => ({ ...pub, data: { ...pub.data, bridge: { ...pub.data.bridge, via: HUB } } });

const natsCommand = (id: string, commandId: string, target: MeshIdentity) => ({
  id, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: target.id }, targetId: target.id, operation: "followUp", from: natsLead,
  body: { version: 1, commandId, targetId: target.id, operation: "followUp", ownerIncarnation: `inc-${target.name}`, message: "from v2",
    replyTo: natsLead.id, requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
});

describe.skipIf(WINDOWS)("nats-link relay: spoke roots in the KV mirror", () => {
  it("holds an allow-listed spoke root as the bridge's own row (origin and hop left out); never an unlisted one", async () => {
    const { port } = await relayFixture();
    const set = port.mirrors.at(-1)!;
    expect(set.participants.map(entry => entry.id).sort()).toEqual([fileLead.id, spokeLane.id].sort());
    const lane = set.participants.find(entry => entry.id === spokeLane.id)!;
    for (const field of ["remoteHost", "bridgeVia", "mirrorLease"]) expect(lane).not.toHaveProperty(field);
    expect(lane).toMatchObject({ ownerIncarnation: "inc-spoke-lane", controlProtocol: "v1" });
    const laneHost = set.hosts.find(entry => entry.record.id === spokeLane.id)!;
    expect(laneHost.record).not.toHaveProperty("remoteHost");
    expect(laneHost.record).not.toHaveProperty("bridgeVia");
    expect(set.hosts.map(entry => entry.record.id)).not.toContain(unlisted.id);
  });

  it("matches a root's stored origin to its allow entry: native for a hub root, its own spoke for a spoke root", () => {
    const allowed = natsLinkMirrorAllowed(parseBridgeNatsAllowSection(ALLOW, { link: LINK, natsMachine: BRIDGE_MACHINE, spokes: [SPOKE] }));
    const row = (identity: MeshIdentity, remoteHost?: string) => ({ ...participant(identity, "inc"), ...(remoteHost ? { remoteHost } : {}) });
    expect(allowed(row(fileLead))).toBe(true);
    expect(allowed(row(fileLead, SPOKE))).toBe(false);
    expect(allowed(row(spokeLane, SPOKE))).toBe(true);
    expect(allowed(row(spokeLane))).toBe(false);
    expect(allowed(row(spokeLane, "spoke-b"))).toBe(false);
    expect(allowed(row(unlisted, SPOKE))).toBe(false);
  });
});

describe.skipIf(WINDOWS)("nats-link relay: spoke -> NATS", () => {
  it("verifies a relayed command as its spoke link's direct mirror and sends it once, under the relay envelope id", async () => {
    const { port, link, counters } = await relayFixture();
    await link.bridge.deliverRelay(SPOKE, spokeCommand("s-1", "spoke-ev-1"));
    expect(port.sent).toHaveLength(1);
    expect(port.sent[0]).toMatchObject({
      id: bridgeNatsRelayEnvelopeId(LINK, SPOKE, "spoke-ev-1"), kind: "command", operation: "followUp",
      to: { machine: POD_MACHINE, hostId: natsLead.id }, from: { id: spokeLane.id, name: spokeLane.name },
      body: { commandId: bridgeNatsMappedCommandId(LINK, "toNats", spokeLane.id, "s-1"), replyTo: spokeLane.id, message: "from the spoke" },
    });
    // Routing metadata and stamps never cross.
    expect(port.sent[0]!.body).not.toHaveProperty("destinationRemoteHost");
    expect(port.sent[0]!.body).not.toHaveProperty("bridge");
    expect(counters.get("relay.toNats")).toBe(1);
    expect(counters.get("control.toNats")).toBe(0);
    // A relay retry of the same spoke event sends nothing new; MeshBridge answers the hop itself.
    await expect(link.bridge.deliverRelay(SPOKE, spokeCommand("s-1", "spoke-ev-1"))).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    expect(port.sent).toHaveLength(1);
  });

  it("refuses a relayed sender that is not the source spoke's direct mirror, and a spoke root that claims to be native", async () => {
    const { port, link, counters } = await relayFixture();
    // Through another spoke link than the one holding its mirror.
    await expect(link.remote.publish(viaHub(spokeCommand("s-2", "spoke-ev-2", spokeLane, "spoke-b")))).rejects.toThrow(/not allowed/);
    // Without the hub hop: a spoke root is never a native file participant.
    await expect(link.remote.publish(spokeCommand("s-3", "spoke-ev-3"))).rejects.toThrow(/not allowed/);
    // A hub-native root does not become relayable by naming a spoke.
    await expect(link.remote.publish(viaHub(spokeCommand("s-4", "spoke-ev-4", fileLead)))).rejects.toThrow(/not allowed/);
    expect(port.sent).toEqual([]);
    expect(counters.get("relay.refused")).toBe(2);
    expect(counters.get("control.refused")).toBe(1);
  });

  it("answers an off-list spoke root's relayed command with a notRun refusal, through the hub to that root", async () => {
    const { port, link, hubEvents } = await relayFixture();
    await expect(link.remote.publish(viaHub(spokeCommand("s-5", "spoke-ev-5", unlisted)))).rejects.toThrow(/not allowed/);
    expect(port.sent).toEqual([]);
    const answer = hubEvents(ACK).find(event => (event.data as Record<string, unknown>).commandId === "s-5");
    expect(answer).toMatchObject({ to: unlisted.id, from: { id: natsLead.id }, verification: "bridge",
      data: { accepted: false, notRun: true, errorCode: "BRIDGE_NATS_SENDER_UNVERIFIED",
        bridge: { from: LINK, id: `refused:${bridgeNatsRelayEnvelopeId(LINK, SPOKE, "spoke-ev-5")}` } } });
  });
});

describe.skipIf(WINDOWS)("nats-link relay: NATS -> spoke", () => {
  it("relays a command for an allow-listed spoke root to its spoke (destinationRemoteHost), never appends it on the hub", async () => {
    const { port, link, relayed, hubEvents, counters } = await relayFixture();
    const delivery = port.deliver(natsCommand("env-to-spoke", "n-spoke", spokeLane), POD_MACHINE, 1);
    await settle();
    await link.bridge.step();
    expect(relayed).toHaveLength(1);
    expect(relayed[0]).toMatchObject({ destination: SPOKE, event: { topic: CONTROL, kind: "followUp", to: spokeLane.id,
      from: { id: natsLead.id }, data: { destinationRemoteHost: SPOKE, targetId: spokeLane.id, message: "from v2",
        commandId: bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "n-spoke"), bridge: { from: LINK, id: "env-to-spoke" } } } });
    expect(hubEvents(CONTROL).filter(event => (event.data as Record<string, unknown> | undefined)?.message === "from v2")).toEqual([]);
    // Broker ACK only once MeshBridge read past it (the relay receipt is recorded on the hub).
    await link.bridge.step();
    expect(delivery.acked).toBe(1);
    expect(counters.get("relay.toFile")).toBe(1);
  });

  it("carries the spoke owner's ACK back from its spoke mirror; the same ACK from anywhere else is refused", async () => {
    const { port, link } = await relayFixture();
    port.deliver(natsCommand("env-ask-spoke", "n-ack", spokeLane), POD_MACHINE, 1);
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    const ack = (bridge: Record<string, string>): BridgePublish => ({ topic: ACK, kind: "accepted", from: spokeLane, to: natsLead.id,
      data: { version: 1, commandId: bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "n-ack"), targetId: spokeLane.id,
        accepted: true, messageId: "m-1", bridge: bridge as BridgePublish["data"]["bridge"] } });
    // Not relayed (as if the spoke root were a hub native), or relayed by a link that does not hold it.
    await expect(link.remote.publish(ack({ from: HUB, id: "native-ack" }))).rejects.toThrow(/not allowed/);
    await expect(link.remote.publish(ack({ from: "spoke-b", id: "other-ack", via: HUB }))).rejects.toThrow(/not allowed/);
    expect(port.sent.filter(envelope => envelope.kind === "ack")).toEqual([]);
    await link.remote.publish(ack({ from: SPOKE, id: "spoke-ack-1", via: HUB }));
    expect(port.sent.filter(envelope => envelope.kind === "ack")).toEqual([expect.objectContaining({
      id: "ack:env-ask-spoke", operation: "accepted", to: { machine: POD_MACHINE, hostId: natsLead.id }, from: expect.objectContaining({ id: spokeLane.id }),
      body: expect.objectContaining({ commandId: "n-ack", targetId: spokeLane.id, accepted: true, messageId: "m-1" }),
    })]);
  });

  it("a relay MeshBridge cannot complete is never sent to NATS and never ends the link", async () => {
    const { store, port, link, logs, counters } = await relayFixture(async () => { throw new BridgeRelayUnavailableError(); });
    const delivery = port.deliver(natsCommand("env-down", "n-down", spokeLane), POD_MACHINE, 1);
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    expect(delivery.acked).toBe(1);
    expect(port.sent).toEqual([]);
    expect(counters.get("relay.refused")).toBe(1);
    expect(logs.some(line => /relay of .* not completed \(FABRIC_RELAY_DESTINATION_UNAVAILABLE\)/.test(line))).toBe(true);
    // The link still carries ordinary traffic.
    await store.publish({ topic: CONTROL, kind: "followUp", from: fileLead, to: natsLead.id,
      data: { version: 1, commandId: "c-after", targetId: natsLead.id, operation: "followUp", replyTo: fileLead.id,
        ownerIncarnation: "inc-moved-lead", message: "still linked", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
        destinationRemoteHost: LINK } });
    expect((await link.bridge.step()).toRemote).toBe(1);
    expect(port.sent.map(envelope => envelope.body.message)).toEqual(["still linked"]);
    expect(counters.get("control.toNats")).toBe(1);
  });
});

describe.skipIf(WINDOWS)("startNatsLink: relay registration, counters, config checks", () => {
  const startConfig = (root: string, overrides: Partial<NatsLinkConfig> = {}): NatsLinkConfig => ({
    version: 1, name: HUB, link: LINK, file: { mesh: path.join(root, "hub"), lockProtocol: 1 }, stateDir: path.join(root, "state"),
    allow: ALLOW, relay: { spokes: [SPOKE] }, callTimeoutMs: 30_000, logLevel: "info",
    nats: { servers: ["tls://nats.invalid:4222"], machine: BRIDGE_MACHINE, credentials: path.join(root, "user.creds"),
      caFile: path.join(root, "ca.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
      pathBMachines: [BRIDGE_MACHINE, POD_MACHINE], operatorMachine: "fabric-ops", events: false, inboxLanes: 8 },
    ...overrides,
  });
  const fixtureRoot = async (): Promise<string> => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-relay-start-"));
    roots.push(root);
    (await hubStore(root)).closeState();
    closers.pop(); // the hub store is released; the link opens its own
    return root;
  };
  const recordingRelay = () => {
    const calls: Array<Parameters<NonNullable<NatsLinkFactories["startRelay"]>>[0]> = [];
    let closed = 0;
    const startRelay: NonNullable<NatsLinkFactories["startRelay"]> = async (options) => {
      calls.push(options);
      return { send: async () => undefined, close: async () => { closed += 1; } };
    };
    return { calls, startRelay, closed: () => closed };
  };
  const start = async (config: NatsLinkConfig, extra: { relay?: (d: string, e: BridgePublish) => Promise<void>;
    startRelay?: NonNullable<NatsLinkFactories["startRelay"]> } = {}) => {
    const controller = new AbortController();
    const logs: string[] = [];
    const handle = await startNatsLink(config, { signal: controller.signal, log: line => logs.push(line),
      ...(extra.relay ? { relay: extra.relay } : {}),
      factories: { openPort: async () => fakePort(), seatPins: () => pins, ...(extra.startRelay ? { startRelay: extra.startRelay } : {}) } });
    closers.push(async () => { controller.abort(); await handle.closed.catch(() => undefined); });
    return { handle, logs, stop: async () => { controller.abort(); await handle.closed; } };
  };

  it("joins the hub relay as this link from bin/nats-link with relay.spokes, and leaves it on stop", async () => {
    const root = await fixtureRoot();
    const relay = recordingRelay();
    const { handle, logs, stop } = await start(startConfig(root), { startRelay: relay.startRelay });
    expect(relay.calls).toHaveLength(1);
    expect(relay.calls[0]).toMatchObject({ localName: HUB, remoteName: LINK, meshRoot: path.join(root, "hub"),
      bridgeScriptPath: fs.realpathSync(natsLinkBinPath()) });
    expect(relay.calls[0]!.bridgeScriptPath.endsWith(path.join("bin", "nats-link"))).toBe(true);
    expect(typeof relay.calls[0]!.deliver).toBe("function");
    // Inbound relays arrive over the UDS relay only: the handle offers no in-process door.
    expect(handle.deliverRelay).toBeUndefined();
    expect(logs).toContain(`relay: joined the hub relay as ${LINK} (spokes ${SPOKE})`);
    await stop();
    expect(relay.closed()).toBe(1);
  });

  it("the in-process test seam registers NO UDS relay and offers deliverRelay; no spokes, no relay", async () => {
    const seamRoot = await fixtureRoot();
    const relay = recordingRelay();
    const seam = await start(startConfig(seamRoot), { relay: async () => undefined, startRelay: relay.startRelay });
    expect(relay.calls).toEqual([]);
    expect(typeof seam.handle.deliverRelay).toBe("function");
    await seam.stop();
    const plainRoot = await fixtureRoot();
    const plain = await start(startConfig(plainRoot, { relay: { spokes: [] }, allow: { file: [ALLOW.file![0]!], nats: ALLOW.nats! } }),
      { startRelay: relay.startRelay });
    expect(relay.calls).toEqual([]);
    expect(plain.handle.deliverRelay).toBeUndefined();
    await plain.stop();
  });

  it("a refused relay registration costs only the relay: the link starts and says so", async () => {
    const root = await fixtureRoot();
    const { logs, stop } = await start(startConfig(root), { startRelay: async () => { throw new BridgeRelayUnavailableError(); } });
    expect(logs).toContain(`relay unavailable; hub-local link remains active; commands between ${SPOKE} and ${LINK} fail closed`);
    expect(logs).toContain(`linked ${HUB} <-> ${LINK} (NATS machine ${BRIDGE_MACHINE})`);
    await stop();
  });

  it("opens L5's counters in the state dir and writes the final snapshot on stop", async () => {
    const root = await fixtureRoot();
    const { stop } = await start(startConfig(root), { relay: async () => undefined });
    expect(readNatsLinkCounters(path.join(root, "state"))).toMatchObject({ v: 1, link: LINK });
    await stop();
    const snapshot = readNatsLinkCounters(path.join(root, "state"))!;
    expect(Object.keys(snapshot.counters)).toEqual(expect.arrayContaining(["relay.toNats", "relay.toFile", "relay.refused"]));
    expect(fs.statSync(path.join(root, "state", "counters.json")).mode & 0o777).toBe(0o600);
  });

  it("refuses before opening anything: a viaSpoke entry without relay.spokes, or republishTopics without nats.events", async () => {
    const root = await fixtureRoot();
    const never = { openPort: async () => { throw new Error("opened"); }, seatPins: () => pins };
    const options = { signal: new AbortController().signal, log: () => undefined, factories: never };
    await expect(startNatsLink({ ...startConfig(root), relay: { spokes: [] } }, options)).rejects.toThrow(/viaSpoke/);
    const republish: BridgeNatsAllowSection = { file: ALLOW.file!,
      nats: [{ kind: "root", id: natsLead.id, machine: POD_MACHINE, republishTopics: ["fleet.work.*"] }] };
    await expect(startNatsLink(startConfig(root, { allow: republish }), options)).rejects.toThrow(/republishTopics.*nats\.events/);
    expect(fs.existsSync(path.join(root, "state"))).toBe(false);
  });
});
