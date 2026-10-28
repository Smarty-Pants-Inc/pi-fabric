// Lane L2 (smarty-dev#15310): NatsCarrierSide under the REAL MeshBridge and a real file MeshStore.
// The NATS port is a recording fake (L4 owns the real-broker E2E); every file-side check, cursor,
// held fence and dedupe receipt is the production one.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { BridgePresence } from "../src/mesh/bridge.js";
import { BridgeMirrorDeferredError } from "../src/mesh/bridge.js";
import { createNatsFileLink, NatsCarrierDeferredError } from "../src/mesh/bridge-nats-file.js";
import { TransportUnavailableError } from "../src/transport/types.js";
import { bridgeNatsEnvelopeId, bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import {
  BRIDGE_MACHINE, FakePort, LINK, MemoryLedger, POD_MACHINE, fileLead, linkIdentity, natsLead, route, settle,
} from "./helpers/bridge-nats-l2-fakes.js";

const roots: string[] = [];
const stores: MeshStore[] = [];
const closers: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0)) await close();
  for (const store of stores.splice(0)) store.closeState();
  // Only this test's own mkdtemp fixtures.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const NOW = Date.now();
const host = (identity: MeshIdentity): FabricHostRecord => ({
  format: 1, id: identity.id, rootId: identity.id, identity,
  incarnationToken: identity.id === fileLead.id ? "inc-file-lead" : "inc-moved-lead",
  startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 86_400_000,
});
const participant = (identity: MeshIdentity, ownerIncarnation: string): FabricParticipantRecord => ({
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  ownerIncarnation, name: identity.name, label: identity.name, status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp"], sessionId: identity.id.slice("session:".length), startedAt: NOW - 1_000, updatedAt: NOW,
  controlProtocol: "v1",
});

const fixture = async (options: { resendAttempts?: number; now?: () => number } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-file-"));
  roots.push(root);
  const store = new MeshStore(path.join(root, "hub"), 256 * 1024, 1_000);
  stores.push(store);
  await store.put({ key: key("topology/hosts/", fileLead.id), value: host(fileLead), identity: fileLead });
  await store.put({ key: key("topology/participants/", fileLead.id), value: participant(fileLead, "inc-file-lead"), identity: fileLead });
  const newPort = (): FakePort => {
    const port = new FakePort();
    port.natsPresence = { hosts: [{ record: host(natsLead), expiresAt: NOW + 86_400_000, observedAt: NOW }],
      participants: [participant(natsLead, "inc-moved-lead")], reserved: [natsLead.id] } satisfies BridgePresence;
    for (const [identity, machine, incarnation] of [[natsLead, POD_MACHINE, "inc-moved-lead"], [fileLead, BRIDGE_MACHINE, "inc-file-lead"],
      [linkIdentity, BRIDGE_MACHINE, "inc-link"]] as const) port.routes.set(identity.id, route(identity, machine, incarnation));
    if (options.now) port.presence = async () => ({ ...port.natsPresence,
      hosts: port.natsPresence.hosts.map(entry => ({ ...entry, observedAt: options.now!() })) });
    return port;
  };
  const port = newPort();
  const ledger = new MemoryLedger();
  const logs: string[] = [];
  const make = (port: FakePort) => createNatsFileLink({
    store, localName: "ryzen1", link: LINK, cursorPath: path.join(root, "cursor.json"), port, ledger,
    authorize: () => ({ ok: true, principal: "carry" }), authorizeTopic: () => ({ ok: true, principal: "none" }),
    mirrorAllowed: () => true, log: line => logs.push(line), resendDelayMs: 0,
    ...(options.now ? { now: options.now, filePresenceTtlMs: 0 } : {}),
    waitForTarget: id => id === fileLead.id,
    ...(options.resendAttempts !== undefined ? { resendAttempts: options.resendAttempts } : {}),
    bridge: { presenceMs: 60_000 },
  });
  const link = make(port);
  closers.push(() => link.bridge.stop());
  await link.bridge.start();
  const events = (topic: string): MeshEvent[] => store.read({ after: 0, limit: store.maxReadEvents }).filter(event => event.topic === topic);
  return { root, store, port, ledger, logs, link, make, newPort, events };
};

const fileFollowUp = (store: MeshStore, commandId = "file-cmd-1", data: Record<string, unknown> = {}) => store.publish({
  topic: "fabric.control.command", kind: (data.operation as string | undefined) ?? "followUp", from: fileLead, to: natsLead.id,
  data: { version: 1, commandId, targetId: natsLead.id, operation: "followUp", replyTo: fileLead.id, ownerIncarnation: "inc-moved-lead",
    message: "hello moved lead", requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
    destinationRemoteHost: LINK, ...data },
});

const natsAsk = (id = "nats-env-1", body: Record<string, unknown> = {}) => ({
  id, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: fileLead.id, operation: "ask", from: natsLead,
  body: { version: 1, commandId: "nats-cmd-1", targetId: fileLead.id, operation: "ask", ownerIncarnation: "inc-file-lead",
    message: "status?", replyTo: natsLead.id, requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, ...body },
});

describe("NatsCarrierSide under MeshBridge", () => {
  it("mirrors both ways at start: the NATS root into the file mesh, file roots and the link identity into KV", async () => {
    const { link, port } = await fixture();
    expect(link.local.holds(natsLead.id)).toBe(true);
    const mirrored = port.mirrors.at(-1)!;
    expect(mirrored.linkIdentity).toEqual(linkIdentity);
    expect(mirrored.participants.map(entry => entry.id)).toContain(fileLead.id);
    expect(mirrored.hosts.map(entry => entry.record.id)).toContain(fileLead.id);
    // The NATS root MeshBridge mirrored into the file mesh is never mirrored back.
    expect(mirrored.participants.map(entry => entry.id)).not.toContain(natsLead.id);
  });

  it("file followUp -> NATS envelope after the cursor moves; the owner's ACK lands for the file sender before the broker ACK", async () => {
    const { store, port, link, events, ledger } = await fixture();
    const command = await fileFollowUp(store);
    const step = await link.bridge.step();
    expect(step.toRemote).toBe(1);
    expect(port.sent).toHaveLength(1);
    const envelope = port.sent[0]!;
    const mapped = bridgeNatsMappedCommandId(LINK, "toNats", fileLead.id, "file-cmd-1");
    expect(envelope).toMatchObject({ id: bridgeNatsEnvelopeId(LINK, command.id), kind: "command", operation: "followUp",
      to: { machine: POD_MACHINE, hostId: natsLead.id }, from: fileLead, body: { commandId: mapped, replyTo: fileLead.id } });
    let appendedBeforeAck = false;
    const delivery = port.deliver({ id: `ack:${envelope.id}`, kind: "ack", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id },
      targetId: natsLead.id, operation: "accepted", from: natsLead,
      body: { accepted: true, messageId: "m-1", triggered: true, version: 1, commandId: mapped, targetId: natsLead.id,
        bodyDigest: controlBodyFingerprint(envelope.body, envelope.principal), ownerIncarnation: "inc-moved-lead" } },
    POD_MACHINE, 1, () => { appendedBeforeAck = events("fabric.control.ack").length === 1; });
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    const [ack] = events("fabric.control.ack");
    expect(ack).toMatchObject({ topic: "fabric.control.ack", kind: "accepted", to: fileLead.id, verification: "bridge",
      from: { id: natsLead.id, name: natsLead.name, kind: "main" } });
    expect(ack!.data).toMatchObject({ version: 1, commandId: "file-cmd-1", targetId: natsLead.id, accepted: true, messageId: "m-1",
      ownerIncarnation: "inc-moved-lead", bridge: { from: LINK, id: `ack:${envelope.id}` } });
    expect(delivery.acked).toBe(1);
    expect(appendedBeforeAck).toBe(true);
    expect(ledger.settled.get(`toNats:${mapped}`)).toMatchObject({ accepted: true });
  });

  it("NATS ask -> file command from the verified KV identity; the owner's answer goes back as ack:<envelope>", async () => {
    const { store, port, link, events } = await fixture();
    let appendedBeforeAck = false;
    const delivery = port.deliver({ ...natsAsk(), from: { ...natsLead, name: "forged" } }, POD_MACHINE, 1,
      () => { appendedBeforeAck = events("fabric.control.command").length === 1; });
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    const mapped = bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cmd-1");
    const [command] = events("fabric.control.command");
    expect(command).toMatchObject({ kind: "ask", to: fileLead.id, verification: "bridge", from: { id: natsLead.id, name: natsLead.name } });
    expect(command!.data).toMatchObject({ commandId: mapped, replyTo: natsLead.id, message: "status?", bridge: { from: LINK, id: "nats-env-1" } });
    expect(delivery.acked).toBe(1);
    expect(appendedBeforeAck).toBe(true);
    // The file owner answers its (mapped) command; the link maps it back for the NATS sender.
    await store.publish({ topic: "fabric.control.ack", kind: "accepted", from: fileLead, to: natsLead.id,
      data: { version: 1, commandId: mapped, targetId: fileLead.id, ownerIncarnation: "inc-file-lead", accepted: true, messageId: "a-1",
        result: { answer: "green" } } });
    expect((await link.bridge.step()).toRemote).toBe(1);
    expect(port.sent.at(-1)).toMatchObject({ id: "ack:nats-env-1", kind: "ack", operation: "accepted", from: { id: fileLead.id },
      to: { machine: POD_MACHINE, hostId: natsLead.id }, body: { commandId: "nats-cmd-1", result: { answer: "green" },
        bodyDigest: controlBodyFingerprint(natsAsk().body, undefined) } });
  });

  it("TERMs a refused delivery in order, appends the next, and never shows the refused one to MeshBridge", async () => {
    const { port, link, events, logs } = await fixture();
    const forged = port.deliver(natsAsk("nats-env-bad"), "evil-machine", 1);
    const good = port.deliver(natsAsk("nats-env-good", { commandId: "nats-cmd-2" }), POD_MACHINE, 2);
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    expect(forged.termed).toHaveLength(1);
    expect(forged.acked).toBe(0);
    expect(good.acked).toBe(1);
    expect(events("fabric.control.command").map(event => (event.data as { bridge: { id: string } }).bridge.id)).toEqual(["nats-env-good"]);
    expect(logs.some(line => line.includes("refused nats-env-bad"))).toBe(true);
  });

  it("an uncertain publish resends the same id and bytes, holds the cursor, and goes once it is sent", async () => {
    const { store, port, link } = await fixture({ resendAttempts: 2 });
    await fileFollowUp(store);
    port.uncertain = 2;
    // MeshBridge's own retryable class: run() backs off and retries; it never ends the link.
    const deferred = await link.bridge.step().catch((error: unknown) => error);
    expect(deferred).toBeInstanceOf(BridgeMirrorDeferredError);
    expect(deferred).toBeInstanceOf(NatsCarrierDeferredError);
    expect((deferred as Error).message).toMatch(/outcome is unknown/);
    expect(port.sent).toHaveLength(0);
    expect(new Set(port.attempts).size).toBe(1);
    expect(port.attempts).toHaveLength(2);
    expect((await link.bridge.step()).toRemote).toBe(1);
    expect(port.sent).toHaveLength(1);
    expect(port.sent[0]!.id).toBe(port.attempts[0]);
  });

  it("a NATS outage (unavailable transport, a KV gap) defers the direction; the command crosses once it ends", async () => {
    const { store, port, link } = await fixture();
    await fileFollowUp(store);
    const send = port.send.bind(port);
    port.send = async () => { throw new TransportUnavailableError("NATS connection closed"); };
    await expect(link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    // The target's KV route is gone for a moment as well: still a wait, never a drop.
    const route = port.routes.get(natsLead.id)!;
    port.routes.delete(natsLead.id);
    await expect(link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    port.routes.set(natsLead.id, route);
    port.send = send;
    expect((await link.bridge.step()).toRemote).toBe(1);
    expect(port.sent).toHaveLength(1);
  });

  it("an inbound command whose target this link's KV view does not show yet waits, then is TERMed after the bound", async () => {
    const { port, link, events } = await fixture();
    const route = port.routes.get(fileLead.id)!;
    port.routes.delete(fileLead.id);
    const waiting = port.deliver(natsAsk(), POD_MACHINE, 1);
    await settle();
    await expect(link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(waiting.termed).toEqual([]);
    // The view catches up: the same delivery crosses.
    port.routes.set(fileLead.id, route);
    await link.bridge.step();
    await link.bridge.step();
    expect(events("fabric.control.command")).toHaveLength(1);
    expect(waiting.acked).toBe(1);
  });

  // Keep: incidents #120/#126, command loss at the KV/file ownership boundary. Advance only
  // the clock; use the real carrier, local mirror fences, journal, append and ACK ordering.
  it("retains a missing own KV target for 35 s, then appends once without importing target time into the sender fence", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const target = f.port.routes.get(fileLead.id)!;
    f.port.routes.delete(fileLead.id);
    const delivery = f.port.deliver(natsAsk("lag-35s"), POD_MACHINE, 1);
    await settle();
    await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 35_000;
    await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(delivery.termed).toEqual([]); expect(delivery.acked).toBe(0);
    expect(f.ledger.records.size).toBe(0); expect(f.events("fabric.control.command")).toEqual([]);
    f.port.routes.set(fileLead.id, target);
    expect((await f.link.bridge.step()).toLocal).toBe(1);
    await f.link.bridge.step();
    expect(f.events("fabric.control.command")).toHaveLength(1);
    expect(delivery.acked).toBe(1); expect(delivery.termed).toEqual([]);
  });

  it("refuses a genuinely absent target at 30 s, without a blanket 120 s extension", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    f.port.routes.delete(fileLead.id);
    await f.store.put({ key: key("topology/hosts/", fileLead.id), identity: fileLead,
      value: { ...host(fileLead), expiresAt: time - 1 } });
    const delivery = f.port.deliver(natsAsk("gone-30s"), POD_MACHINE, 1);
    await settle();
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 29_999;
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 1;
    expect((await f.link.remote.read(0)).events).toEqual([]);
    expect(delivery.termed).toHaveLength(1); expect(f.ledger.records.size).toBe(0);
  });

  it("charges only the remaining active target budget after independently sampled lag ends", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    f.port.routes.delete(fileLead.id);
    // A missing native target is not mirror lag. Restore it after 20 s of chargeable wait.
    await f.store.put({ key: key("topology/hosts/", fileLead.id), identity: fileLead,
      value: { ...host(fileLead), expiresAt: time - 1 } });
    const delivery = f.port.deliver(natsAsk("active-budget"), POD_MACHINE, 1);
    await settle();
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 20_000;
    await f.store.put({ key: key("topology/hosts/", fileLead.id), identity: fileLead, value: host(fileLead) });
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 40_000;
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    await f.store.put({ key: key("topology/hosts/", fileLead.id), identity: fileLead,
      value: { ...host(fileLead), expiresAt: time - 1 } });
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 9_999;
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(delivery.termed).toEqual([]);
    time += 1; await f.link.remote.read(0);
    expect(delivery.termed).toHaveLength(1);
  });

  it("enforces an immutable 120 s cap before route recovery or a ledger claim, including redelivery", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const target = f.port.routes.get(fileLead.id)!; f.port.routes.delete(fileLead.id);
    const delivery = f.port.deliver(natsAsk("cap-120s"), POD_MACHINE, 1);
    await settle();
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 119_999;
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(delivery.termed).toEqual([]);
    time += 1; f.port.routes.set(fileLead.id, target);
    expect((await f.link.remote.read(0)).events).toEqual([]);
    expect(delivery.termed).toHaveLength(1); expect(f.ledger.records.size).toBe(0);
    const again = f.port.deliver(natsAsk("cap-120s"), POD_MACHINE, 1);
    await settle(); await f.link.remote.read(0);
    expect(again.termed).toHaveLength(1); expect(f.ledger.records.size).toBe(0);
  });

  it("keeps a real sender's 30 s lapse independent of a paused target wait", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const target = f.port.routes.get(fileLead.id)!, sender = f.port.routes.get(natsLead.id)!;
    f.port.routes.delete(fileLead.id);
    const delivery = f.port.deliver(natsAsk("sender-during-lag"), POD_MACHINE, 1);
    await settle(); await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 35_000;
    f.port.routes.set(natsLead.id, { ...sender, route: { ...sender.route, live: false }, leaseLapsed: true });
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 29_999;
    await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(delivery.termed).toEqual([]);
    time += 1; f.port.routes.set(natsLead.id, sender); f.port.routes.set(fileLead.id, target);
    expect((await f.link.remote.read(0)).events).toEqual([]);
    expect(delivery.termed).toHaveLength(1); expect(f.ledger.records.size).toBe(0);
  });

  it("#126 retries an expired own FILE sender mirror beyond 30 s while exact KV authority is live", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const publish = f.link.local.publish.bind(f.link.local);
    let lag = true;
    vi.spyOn(f.link.local, "publish").mockImplementation(async (...args) => {
      if (lag && args[0].topic === "fabric.control.command") {
        const lease = readHostLease(f.store.root, natsLead.id)!;
        writeHostLease(f.store.root, { ...lease, expiresAt: time - 1 });
      }
      return publish(...args);
    });
    const delivery = f.port.deliver(natsAsk("projection-lag"), POD_MACHINE, 1);
    await settle();
    expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    time += 35_000;
    expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(delivery.acked).toBe(0); expect(delivery.termed).toEqual([]);
    expect(f.events("fabric.control.command")).toEqual([]);
    lag = false; time += 1; await f.link.bridge.syncPresence();
    expect((await f.link.bridge.step()).toLocal).toBe(1);
    await f.link.bridge.step();
    expect(f.events("fabric.control.command")).toHaveLength(1); expect(delivery.acked).toBe(1);
    expect(f.logs.filter(line => line.includes("projection-lag") && /refused|dropped/.test(line))).toEqual([]);
  });

  it("rechecks the immutable cap under the append fence after a lock wait crosses it", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const target = f.port.routes.get(fileLead.id)!; f.port.routes.delete(fileLead.id);
    const d = f.port.deliver(natsAsk("cap-at-append"), POD_MACHINE, 1);
    await settle(); await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    time += 119_999; f.port.routes.set(fileLead.id, target);
    const publish = f.link.local.publish.bind(f.link.local);
    vi.spyOn(f.link.local, "publish").mockImplementation(async (...args) => {
      time = NOW + 120_000; // The next continuation is the real store admission/append.
      return publish(...args);
    });
    await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
    expect(f.events("fabric.control.command")).toEqual([]); expect(d.acked).toBe(0); expect(d.termed).toEqual([]);
    await f.link.bridge.step();
    expect(d.termed).toHaveLength(1); expect(f.events("fabric.control.command")).toEqual([]);
  });

  it("#126's own FILE projection pause has the same immutable 120 s cap", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const publish = f.link.local.publish.bind(f.link.local);
    let lag = true;
    vi.spyOn(f.link.local, "publish").mockImplementation(async (...args) => {
      if (lag) writeHostLease(f.store.root, { ...readHostLease(f.store.root, natsLead.id)!, expiresAt: time - 1 });
      return publish(...args);
    });
    const d = f.port.deliver(natsAsk("projection-cap"), POD_MACHINE, 1); await settle();
    expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    time += 119_999;
    expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 }); expect(d.termed).toEqual([]);
    time += 1; lag = false; await f.link.bridge.syncPresence();
    expect((await f.link.bridge.step()).toLocal).toBe(0);
    expect(f.events("fabric.control.command")).toEqual([]); expect(d.termed).toHaveLength(1); expect(d.acked).toBe(0);
  });

  it("never treats a known foreign target owner as own-mirror lag", async () => {
    let time = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => time);
    const f = await fixture({ now: () => time });
    const original = f.port.routes.get(fileLead.id)!;
    f.port.routes.set(fileLead.id, { ...original, route: { ...original.route, ownerHostId: natsLead.id } });
    const d = f.port.deliver(natsAsk("foreign-target"), POD_MACHINE, 1); await settle();
    await f.link.remote.read(0);
    expect(d.termed).toHaveLength(1); expect(f.ledger.records.size).toBe(0);
    time += 35_000; f.port.routes.set(fileLead.id, original);
    const again = f.port.deliver(natsAsk("foreign-target-2"), POD_MACHINE, 2); await settle();
    expect((await f.link.remote.read(0)).events).toHaveLength(1); expect(again.termed).toEqual([]);
  });

  it("a redelivery after a restart is appended once (dedupe receipt) and ACKed", async () => {
    const { port, link, make, newPort, events } = await fixture();
    port.deliver(natsAsk(), POD_MACHINE, 1);
    await settle();
    await link.bridge.step();
    await link.bridge.step();
    await link.bridge.stop();
    // A new process and connection: the broker redelivers the same stream sequence (its first ACK was lost).
    const port2 = newPort();
    const restarted = make(port2);
    closers.push(() => restarted.bridge.stop());
    await restarted.bridge.start();
    const again = port2.deliver(natsAsk(), POD_MACHINE, 1);
    await settle();
    await restarted.bridge.step();
    await restarted.bridge.step();
    expect(events("fabric.control.command")).toHaveLength(1);
    expect(again.acked).toBe(1);
  });

  it("N3: a task completion for the moved lead becomes one followUp from the link identity", async () => {
    const { store, port, link } = await fixture();
    const event = await store.publish({ topic: "fleet.work.tasks", kind: "answer", to: natsLead.id,
      from: { id: "fleet-tool:smarty-task-ryzen2", name: "smarty-task-ryzen2", kind: "main" },
      text: "RYZEN2_TASK_COMPLETE t-1 rc=0", data: { key: "ryzen2-task:t-1" } });
    expect((await link.bridge.step()).toRemote).toBe(1);
    expect(port.sent[0]).toMatchObject({ id: bridgeNatsEnvelopeId(LINK, event.id), operation: "followUp", from: linkIdentity,
      to: { machine: POD_MACHINE, hostId: natsLead.id },
      body: { message: "[mesh fleet.work.tasks/answer from fleet-tool:smarty-task-ryzen2]\nRYZEN2_TASK_COMPLETE t-1 rc=0",
        replyTo: linkIdentity.id, triggerTurn: true, data: { key: "ryzen2-task:t-1", bridge: { from: LINK, id: event.id } } } });
  });
});
