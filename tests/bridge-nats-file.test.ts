// Lane L2 (smarty-dev#15310): NatsCarrierSide under the REAL MeshBridge and a real file MeshStore.
// The NATS port is a recording fake (L4 owns the real-broker E2E); every file-side check, cursor,
// held fence and dedupe receipt is the production one.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BridgePresence } from "../src/mesh/bridge.js";
import { BridgeMirrorDeferredError } from "../src/mesh/bridge.js";
import { createNatsFileLink, NatsCarrierDeferredError } from "../src/mesh/bridge-nats-file.js";
import { MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { TransportUnavailableError } from "../src/transport/types.js";
import { bridgeNatsEnvelopeId, bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import {
  BRIDGE_MACHINE, FakePort, LINK, MemoryLedger, POD_MACHINE, fileLead, linkIdentity, natsLead, route, settle,
} from "./helpers/bridge-nats-l2-fakes.js";

const roots: string[] = [];
const stores: MeshStore[] = [];
const closers: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  for (const store of stores.splice(0)) store.closeState();
  // Only this test's own mkdtemp fixtures.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const NOW = Date.now();
const host = (identity: MeshIdentity): FabricHostRecord => ({
  format: 1, id: identity.id, rootId: identity.id, identity, startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 86_400_000,
});
const participant = (identity: MeshIdentity, ownerIncarnation: string): FabricParticipantRecord => ({
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  ownerIncarnation, name: identity.name, label: identity.name, status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp"], sessionId: identity.id.slice("session:".length), startedAt: NOW - 1_000, updatedAt: NOW,
  controlProtocol: "v1",
});

const fixture = async (options: { resendAttempts?: number } = {}) => {
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
    return port;
  };
  const port = newPort();
  const ledger = new MemoryLedger();
  const logs: string[] = [];
  const make = (port: FakePort) => createNatsFileLink({
    store, localName: "ryzen1", link: LINK, cursorPath: path.join(root, "cursor.json"), port, ledger,
    authorize: () => ({ ok: true, principal: "carry" }), authorizeTopic: () => ({ ok: true, principal: "none" }),
    mirrorAllowed: () => true, log: line => logs.push(line), resendDelayMs: 0,
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
  it("resamples a short source lease before its translated mirror deadline without a KV watch event", async () => {
    const { store, port, link } = await fixture();
    const renewal = vi.spyOn(link.local, "renewMirroredLeases");
    const lease = () => readHostLease(store.root, natsLead.id)!;
    const token = "pod-1";
    const hostKey = key("topology/hosts/", natsLead.id);
    const heldHost = store.get(hostKey, { fresh: true })!;
    await store.put({ key: hostKey, identity: heldHost.updatedBy,
      value: { ...(heldHost.value as Record<string, unknown>), incarnationToken: token } });
    writeHostLease(store.root, { ...lease(), incarnationToken: token });
    const originalPresence = port.natsPresence;
    let samples = 0;
    port.presence = async () => {
      samples++;
      const observedAt = Date.now();
      const expiresAt = observedAt + 600;
      return { ...originalPresence, hosts: originalPresence.hosts.map(host => ({ ...host,
        record: { ...host.record, incarnationToken: token, updatedAt: observedAt, expiresAt }, expiresAt, observedAt })) };
    };
    let notify: (() => void) | undefined;
    port.watchPresence = (changed: () => void, _failed: (error: Error) => void) => {
      notify = () => changed();
      return () => { notify = undefined; };
    };
    const unwatch = link.remote.subscribeChanges(() => undefined, error => { throw error; });
    closers.push(unwatch);
    const before = lease().mirror!.observedAt;
    notify?.();
    await new Promise<void>(resolve => setTimeout(resolve, 30));
    const first = lease().mirror!.observedAt;
    expect(first).toBeGreaterThanOrEqual(before);
    await new Promise<void>(resolve => setTimeout(resolve, 350));
    expect(samples).toBeGreaterThan(1);
    expect(renewal.mock.calls.at(-1)?.[0][0]?.observedAt).toBeGreaterThan(first);
    expect(lease().mirror!.observedAt).toBeGreaterThan(first);
  });

  it("does not renew a source lease when a horizon resample proves it expired", async () => {
    const { store, port, link } = await fixture();
    await settle();
    const renewal = vi.spyOn(link.local, "renewMirroredLeases");
    const lease = () => readHostLease(store.root, natsLead.id)!;
    const token = "pod-1";
    const hostKey = key("topology/hosts/", natsLead.id);
    const heldHost = store.get(hostKey, { fresh: true })!;
    await store.put({ key: hostKey, identity: heldHost.updatedBy,
      value: { ...(heldHost.value as Record<string, unknown>), incarnationToken: token } });
    writeHostLease(store.root, { ...lease(), incarnationToken: token });
    const originalPresence = port.natsPresence;
    let samples = 0;
    let expiredSampled = false;
    let secondSampleStarted = false;
    port.presence = async () => {
      samples++;
      if (samples === 1) {
        const observedAt = Date.now();
        const expiresAt = observedAt + 1_000;
        return { ...originalPresence, hosts: originalPresence.hosts.map(host => ({ ...host,
          record: { ...host.record, incarnationToken: token, updatedAt: observedAt, expiresAt }, expiresAt, observedAt })) };
      }
      // The horizon timer's live KV read returns after the sampled source lease has expired.
      secondSampleStarted = true;
      await new Promise<void>(resolve => setTimeout(resolve, 1_100));
      expiredSampled = true;
      return { ...originalPresence, hosts: [], participants: [] };
    };
    const unwatch = link.remote.subscribeChanges(() => undefined, error => { throw error; });
    closers.push(unwatch);
    const sampleDeadline = Date.now() + 1_500;
    while (!secondSampleStarted && Date.now() < sampleDeadline) await new Promise<void>(resolve => setTimeout(resolve, 10));
    expect(secondSampleStarted).toBe(true);
    const liveMirror = lease().mirror!;
    expect(liveMirror.sourceExpiresAt - liveMirror.sourceObservedAt).toBe(1_000);
    const deadline = Date.now() + 3_000;
    while ((!expiredSampled || !renewal.mock.calls.some(call => call[0].length === 0)) && Date.now() < deadline) {
      await new Promise<void>(resolve => setTimeout(resolve, 10));
    }
    expect(expiredSampled).toBe(true);
    const renewals = renewal.mock.calls;
    expect(renewals.some(call => call[0].length === 0)).toBe(true);
    expect(lease().mirror!.observedAt).toBe(liveMirror.observedAt);
    expect(lease().mirror!.sourceExpiresAt).toBe(liveMirror.sourceExpiresAt);
  });

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

  it("defers a real on-disk ownerless custody refusal in mirror renewal without failing link listeners", async () => {
    const { link, port, store, logs } = await fixture();
    const actualLeaseCustody = store.leaseCustody.bind(store);
    const attemptedLocks: string[] = [];
    // Wrap, but never replace, the production operation. Plant the ownerless canonical directory at the exact
    // per-file gate it selects, so the negative-control reclaim mutation would let a real renewal proceed.
    vi.spyOn(store, "leaseCustody").mockImplementation((async (file, operation, timeoutMs, options) => {
      const hash = createHash("sha256").update(path.basename(file)).digest("hex");
      const legacy = path.join(store.root, "host-lease-commits", hash);
      const domain = fs.existsSync(legacy) ? legacy : path.join(store.root, "host-lease-commits", hash.slice(0, 16));
      const lock = path.join(domain, MESH_CUSTODY_LOCK_NAME);
      fs.mkdirSync(domain, { recursive: true, mode: 0o700 });
      // The renewal loop retries on a bounded backoff, so plant the ownerless directory once and keep it.
      if (!fs.existsSync(lock)) fs.mkdirSync(lock, { mode: 0o700 });
      attemptedLocks.push(lock);
      return actualLeaseCustody(file, operation, timeoutMs, options);
    }) as typeof store.leaseCustody);
    const failures: Error[] = [];
    const unwatch = link.remote.subscribeChanges(() => {}, error => failures.push(error));
    try {
      port.notifyPresenceChange();
      await settle();
      expect(attemptedLocks.length).toBeGreaterThan(0);
      for (const lock of attemptedLocks) {
        expect(fs.existsSync(lock)).toBe(true);
        expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
        expect(fs.readdirSync(lock)).toEqual([]);
      }
      expect(failures).toEqual([]);
      expect(logs.some(line => line.includes("NATS file mirror renewal deferred:") && line.includes("has no valid owner pid; recovery refused"))).toBe(true);
      await expect(link.bridge.step()).resolves.toMatchObject({ toRemote: 0 });
    } finally { unwatch(); }
  }, 60_000);

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
