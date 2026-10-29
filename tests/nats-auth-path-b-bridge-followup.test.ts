// expected-skip: no NATS_BIN/PATH executable, or no A' link entry (src/mesh/bridge-nats-link.ts) in this checkout.
// Keep reason: smarty-dev#15310 A' (D3 gate 3) real-broker E2E. A real FILE-transport Main (Ryzen 1 lead)
// and a real NATS-transport Main (moved v2 lead) reach each other only through the in-process link:
// mirrored presence both ways, one keyed followUp each way, steer each way, and a commandId collision
// that must never hand one sender another sender's outcome. Only the Pi UI/model boundary is a fixture.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { startNatsLink, type NatsLinkStartConfig } from "../src/mesh/bridge-nats-link.js";
import { openNatsBridgeSide, type NatsBridgePort } from "../src/mesh/bridge-nats-port.js";
import { NatsPresenceReader, natsLeaseKey, natsPresenceKey } from "../src/mesh/bridge-nats-presence.js";
import { hostLeasePath, mirroredOwnerRoutingStatus, readHostLease } from "../src/topology/host-leases.js";
import { bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import type { MeshEvent } from "../src/mesh/store.js";
import type { TransportEnvelope, TransportKvEntry } from "../src/transport/types.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { resolveNatsBinary } from "./helpers/nats-cluster.js";
import {
  BRIDGE_MACHINE, createMixedHarness, E2E_WAIT_MS, linkTail, liveRow, NATS_MAIN_MACHINE, NATS_MAIN_SESSION, natsLinkPresent, pauseMixedLeaseWatch, rootId, selfRecord, waitForValue,
  type MixedHarness, type MixedMain, type StartMixedLink, type WireEvent,
} from "./helpers/nats-file-mixed.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the A' bridge E2E needs NATS_BIN");
const linked = natsLinkPresent();
if (!linked) console.warn("A' bridge E2E skipped: src/mesh/bridge-nats-link.ts (lane L2) is not in this checkout");

const CONTROL = "fabric.control.command", ACK = "fabric.control.ack";
const bodyOf = (event: Pick<WireEvent, "envelope">) => event.envelope.body as Record<string, unknown>;
const dataOf = (event: MeshEvent) => (event.data ?? {}) as Record<string, unknown>;
const mentions = (main: MixedMain, token: string) => main.delivered().filter((row) => row.text.includes(token));
let current: MixedHarness | undefined;
/** Attach the link's log tail to a failure. */
const withLog = <T>(work: Promise<T>): Promise<T> => work.catch((error: unknown) => { throw new Error(`${String(error)}\n${current ? linkTail(current) : ""}`); });

const NATS_ROOT = rootId(NATS_MAIN_SESSION);
const barrier = () => {
  let resolve!: () => void, resolved = false;
  const promise = new Promise<void>(done => { resolve = () => { resolved = true; done(); }; });
  return { promise, resolve, wait: (label: string) => waitForValue(() => resolved ? true : undefined, label, 5_000) };
};
const topologyKey = (kind: "hosts" | "participants", id: string) =>
  `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;

/** The production startNatsLink factories and notification run are unchanged. These observers
 * delegate to the actual installer/port; the only injected behavior is lease-watch I/O delay and
 * an unresolved outbound port.mirror. In particular this never calls MeshBridge.step(). */
function omissionProbe() {
  let port!: NatsBridgePort, local!: StoreBridgeSide;
  let gap!: ReturnType<typeof pauseMixedLeaseWatch>;
  let mode: "none" | "aged" | "deleted" | "always" = "none";
  let outboundReturned = false, stalledCalls = 0;
  const entered = barrier(), release = barrier(), inboundApplied = barrier();
  const restore: Array<() => void> = [];
  const syncJoins: Array<{ held: boolean; stack: string | undefined }> = [];
  const mirrorFailures: unknown[] = [];
  const mirror = StoreBridgeSide.prototype.mirror;
  const syncPresence = MeshBridge.prototype.syncPresence;
  const startLink: StartMixedLink = async (raw, options) => {
    const config = raw as NatsLinkStartConfig;
    const syncing = vi.spyOn(MeshBridge.prototype, "syncPresence").mockImplementation(function(this: MeshBridge) {
      if (stalledCalls > 0 && !outboundReturned) syncJoins.push({ held: local.holds(NATS_ROOT), stack: new Error("active-sync join").stack });
      return syncPresence.call(this);
    });
    restore.push(() => syncing.mockRestore());
    const observer = vi.spyOn(StoreBridgeSide.prototype, "mirror").mockImplementation(function(this: StoreBridgeSide, ...args) {
      if (this.store.root === config.file.mesh) local = this;
      const result = mirror.apply(this, args);
      void result.catch(error => { mirrorFailures.push(error); });
      if (this.store.root === config.file.mesh && mode !== "none") void result.then(() => {
        const input = args[0];
        if (!input.hosts.some(host => host.record.id === NATS_ROOT) &&
            (mode === "aged" ? input.partial === true : mode === "deleted" && input.partial !== true)) inboundApplied.resolve();
      }, () => undefined);
      return result;
    });
    restore.push(() => observer.mockRestore());
    return startNatsLink(config, { ...options, factories: {
      openPort: async (portConfig, portOptions) => {
        const start = NatsPresenceReader.prototype.start;
        const starting = vi.spyOn(NatsPresenceReader.prototype, "start").mockImplementation(function(this: NatsPresenceReader) {
          if (this.machine === BRIDGE_MACHINE) gap = pauseMixedLeaseWatch(this, NATS_ROOT);
          return start.call(this);
        });
        try { port = await openNatsBridgeSide(portConfig, portOptions); }
        finally { starting.mockRestore(); }
        const openedGap = gap;
        restore.push(() => openedGap.restore());
        const outbound = port.mirror.bind(port);
        const stalling = vi.spyOn(port, "mirror").mockImplementation(async (...args) => {
          const route = port.resolve(NATS_ROOT);
          if (mode === "always" || mode === "aged" && route?.leaseLapsed || mode === "deleted" && !route) {
            stalledCalls++;
            entered.resolve();
            await release.promise;
            const result = await outbound(...args);
            outboundReturned = true;
            return result;
          }
          return outbound(...args);
        });
        restore.push(() => stalling.mockRestore());
        return port;
      },
    } });
  };
  return { startLink, syncJoins, mirrorFailures, get port() { return port; }, get local() { return local; }, get gap() { return gap; },
    stall(next: typeof mode) { mode = next; }, entered, inboundApplied,
    assertStalled() { expect(stalledCalls).toBe(1); expect(outboundReturned).toBe(false); },
    release() { mode = "none"; release.resolve(); },
    restore() { for (const stop of restore.reverse()) stop(); },
  };
}

const canonical = (h: MixedHarness, kind: "hosts" | "participants") =>
  h.fileMain.runtime.mesh.get(topologyKey(kind, h.natsMain.id), { fresh: true });
const expectMissing = (h: MixedHarness, probe: ReturnType<typeof omissionProbe>, leaseMissing = true) => {
  expect(canonical(h, "hosts")).toBeUndefined();
  expect(canonical(h, "participants")).toBeUndefined();
  if (leaseMissing) expect(readHostLease(h.fileMeshRoot, h.natsMain.id)).toBeUndefined();
  expect(liveRow(h.fileMain, h.natsMain.id)).toBeUndefined();
  expect(probe.local.holds(h.natsMain.id)).toBe(false);
};

// Aged lease evidence was treated as complete omission, deleting a live root; this real-root probe
// checks recovery while the production link's membership notification is stalled.
describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' real followUp/ACK across the omission gap and outbound presence stall", () => {
  it("retains expired membership, refuses stale traffic, then reinstalls and delivers once each way before the SAME outbound mirror returns", async () => {
    const probe = omissionProbe();
    const h = await createMixedHarness({ binary: binary!, startLink: probe.startLink });
    let pending: Promise<unknown> | undefined;
    let phase = "warm public round trip", crossingToken: string | undefined;
    try {
      // A real control round trip is also the barrier that notification drains have started.
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: `WARM_${randomUUID()}` }))
        .resolves.toMatchObject({ acknowledged: true });
      const epoch = selfRecord(h.natsMain)!.ownerIncarnation;
      const fileEpoch = selfRecord(h.fileMain)!.ownerIncarnation;
      const hostVersion = canonical(h, "hosts")!.version;
      const participantVersion = canonical(h, "participants")!.version;
      phase = "source-reader lease gap and same-pass partial retention";
      probe.gap.pause();
      probe.stall("aged");
      // Source heartbeats really reach the broker but not this reader. Its retained lease
      // loses receipt-age authority naturally (no global clock/receiver/routing replacement).
      await waitForValue(() => probe.port.resolve(h.natsMain.id)?.leaseLapsed ? true : undefined,
        "real link reader's source lease aged out", 20_000);
      await probe.entered.wait("production outbound mirror entered the aged-source stall");
      await probe.inboundApplied.wait("production inbound partial mirror completed during the stall");
      expect(probe.gap.intercepted()).toBeGreaterThan(0);
      expect(await probe.port.presence()).toMatchObject({ hosts: [], participants: [], partial: true });
      expect(canonical(h, "hosts")!.version).toBe(hostVersion);
      expect(canonical(h, "participants")!.version).toBe(participantVersion);
      const expired = readHostLease(h.fileMeshRoot, h.natsMain.id)!;
      const expiredBytes = fs.readFileSync(hostLeasePath(h.fileMeshRoot, h.natsMain.id), "utf8");
      expect(expired.expiresAt).toBeLessThanOrEqual(Date.now());
      expect(mirroredOwnerRoutingStatus({ ...expired, remoteHost: h.linkName })).toBe("bridge-stale");
      expect(probe.local.holds(h.natsMain.id)).toBe(false);
      // Directory retention is deliberately lease-independent; bridgeStale, not its generic
      // stale field, denies routing. The public invocation below checks the actual refusal.
      expect(liveRow(h.fileMain, h.natsMain.id)).toMatchObject({ bridgeStale: true });
      probe.assertStalled();

      const mark = h.wire.mark();
      const stale = `STALE_FILE_${randomUUID()}`;
      // The public router must not turn retained expired rows into new publication.
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: stale }, AbortSignal.timeout(300))).rejects.toThrow();
      expect(mentions(h.natsMain, stale)).toHaveLength(0);
      expect(h.fileEvents({ topic: CONTROL, to: h.natsMain.id }).filter(event => dataOf(event).message === stale)).toHaveLength(0);
      expect(fs.readFileSync(hostLeasePath(h.fileMeshRoot, h.natsMain.id), "utf8")).toBe(expiredBytes);

      // Seed exactly the historical complete-omission outcome through the actual installer,
      // not a source presence deletion. The production run's SAME outbound sync stays held.
      await probe.local.mirror({ hosts: [], participants: [] });
      expectMissing(h, probe);
      const token = crossingToken = `GAP_NATS_TO_FILE_${randomUUID()}`, key = `gap-${randomUUID()}`;
      phase = "public NATS followUp held on stale authority";
      pending = h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: key }, AbortSignal.timeout(10_000));
      void pending.catch(() => undefined);
      await waitForValue(() => h.linkLog.some(line => line.includes("sender-live defer") && line.includes("predicate=expired")) ? true : undefined,
        "actual carrier holds public NATS followUp on expired source authority", 5_000);
      expect(mentions(h.fileMain, token)).toHaveLength(0);
      expectMissing(h, probe);
      probe.assertStalled();

      phase = "fresh watch reinstalls canonical membership";
      probe.gap.release(); // Only fresh real watch authority is released; NEVER the outbound mirror.
      await waitForValue(() => liveRow(h.fileMain, h.natsMain.id), "fresh watch reinstalls canonical membership during the stall", 5_000);
      expect(canonical(h, "hosts")!.updatedBy.id).toBe(h.natsMain.id);
      expect(canonical(h, "participants")!.updatedBy.id).toBe(h.natsMain.id);
      expect(canonical(h, "participants")!.value).toMatchObject({ ownerIncarnation: epoch, remoteHost: h.linkName });
      expect(readHostLease(h.fileMeshRoot, h.natsMain.id)).toMatchObject({ incarnationToken: epoch,
        mirrorLease: 1, mirror: { remoteHost: h.linkName, bridgeProcess: { link: h.linkName, pid: process.pid } } });
      expect(probe.local.holds(h.natsMain.id)).toBe(true);
      probe.assertStalled();
      phase = "public NATS followUp/ACK after verified recovery, outbound still blocked";
      const result = await pending as Record<string, unknown>;
      expect(result).toMatchObject({ acknowledged: true });
      expect(mentions(h.fileMain, token)).toEqual([expect.objectContaining({ from: expect.objectContaining({ id: h.natsMain.id, kind: "main" }), deliverAs: "followUp" })]);
      const sent = h.wire.since(mark).find(event => event.phase === "send" && event.hostId === h.natsMain.id &&
        event.envelope.kind === "command" && bodyOf(event).message === token)!;
      expect(sent.envelope).toMatchObject({ from: { id: h.natsMain.id }, to: { machine: BRIDGE_MACHINE, hostId: h.fileMain.id }, body: { ownerIncarnation: fileEpoch } });
      const appended = h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter(event => dataOf(event).message === token);
      expect(appended).toHaveLength(1);
      expect(appended[0]).toMatchObject({ from: { id: h.natsMain.id, kind: "main" }, data: { ownerIncarnation: fileEpoch, bridge: { from: h.linkName } } });
      expect(h.wire.since(mark).find(event => event.phase === "deliver" && event.hostId === h.natsMain.id &&
        event.envelope.kind === "ack" && bodyOf(event).commandId === bodyOf(sent).commandId))
        .toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { from: { id: h.fileMain.id }, body: { accepted: true } } });
      probe.assertStalled();
      await expect(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: key }, AbortSignal.timeout(5_000)))
        .resolves.toMatchObject({ acknowledged: true, messageId: result.messageId });
      expect(mentions(h.fileMain, token)).toHaveLength(1);

      phase = "public FILE followUp/ACK, outbound still blocked";
      const reverse = `GAP_FILE_TO_NATS_${randomUUID()}`, reverseKey = `gap-${randomUUID()}`;
      const answer = await h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: reverse, idempotencyKey: reverseKey }, AbortSignal.timeout(5_000)) as Record<string, unknown>;
      expect(answer).toMatchObject({ acknowledged: true, routed: "mesh" });
      expect(mentions(h.natsMain, reverse)).toEqual([expect.objectContaining({ from: expect.objectContaining({ id: h.fileMain.id, kind: "main" }), deliverAs: "followUp" })]);
      const command = h.fileEvents({ topic: CONTROL, to: h.natsMain.id }).find(event => dataOf(event).message === reverse)!;
      expect(h.wire.since(mark).filter(event => event.phase === "deliver" && event.hostId === h.natsMain.id && bodyOf(event).message === reverse))
        .toEqual([expect.objectContaining({ sourceMachine: BRIDGE_MACHINE, envelope: expect.objectContaining({ body: expect.objectContaining({ ownerIncarnation: epoch }) }) })]);
      expect(h.fileEvents({ topic: ACK, to: h.fileMain.id }).find(event => dataOf(event).commandId === dataOf(command).commandId))
        .toMatchObject({ kind: "accepted", from: { id: h.natsMain.id }, data: { accepted: true, messageId: answer.messageId } });
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: reverse, idempotencyKey: reverseKey }, AbortSignal.timeout(5_000)))
        .resolves.toMatchObject({ acknowledged: true, messageId: answer.messageId });
      expect(mentions(h.natsMain, reverse)).toHaveLength(1);
      probe.assertStalled();
    } catch (error) {
      // Capture BEFORE teardown releases the stall: a timeout is a blocker, not proof of ACK.
      const diagnostic = { phase, route: probe.port.resolve(h.natsMain.id),
        host: canonical(h, "hosts"), participant: canonical(h, "participants"),
        lease: readHostLease(h.fileMeshRoot, h.natsMain.id), held: probe.local.holds(h.natsMain.id),
        delivered: crossingToken ? mentions(h.fileMain, crossingToken) : [],
        appended: crossingToken ? h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter(event => dataOf(event).message === crossingToken) : [],
        syncJoins: probe.syncJoins };
      probe.assertStalled();
      throw new Error(`${String(error)}\naged-lease omission pre-teardown evidence: ${JSON.stringify(diagnostic)}\n${linkTail(h, 100)}`, { cause: error });
    }
    finally {
      // Release only for teardown, after the last delivery/ACK assertion (or a recorded failure).
      probe.gap.release(); probe.release();
      await pending?.catch(() => undefined);
      await h.stop(); probe.restore();
    }
  }, 60_000);

  it("a verified presence deletion withdraws on the production notification pass; lease-only evidence cannot reinstall or deliver", async () => {
    const probe = omissionProbe();
    const h = await createMixedHarness({ binary: binary!, startLink: probe.startLink });
    try {
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: `DELETE_WARM_${randomUUID()}` })).resolves.toMatchObject({ acknowledged: true });
      const leaseKey = natsLeaseKey(NATS_MAIN_MACHINE, h.natsMain.id);
      const source = (await h.inspector.kv("leases").get<FabricKvHostLease>(leaseKey))!;
      probe.stall("deleted");
      await h.natsMain.stop(); // The real runtime's verified native withdrawal, not an empty projection fixture.
      await probe.entered.wait("production outbound mirror entered the verified-deletion stall");
      await probe.inboundApplied.wait("production inbound complete mirror applied verified deletion");
      expectMissing(h, probe);
      expect(probe.port.resolve(h.natsMain.id)).toBeUndefined();
      probe.assertStalled();
      const now = Date.now(), current = await h.inspector.kv("leases").get(leaseKey);
      const revision = await h.inspector.kv("leases").put(leaseKey, source.owner,
        { ...source.value, updatedAt: now, expiresAt: now + 15_000 }, current?.revision ?? 0);
      await waitForValue(() => probe.port.kv.reader.lease(NATS_MAIN_MACHINE, h.natsMain.id)?.entry.revision === revision ? true : undefined,
        "real lease-only watch following explicit deletion", 5_000);
      expect((await probe.port.presence()).partial).not.toBe(true);
      expectMissing(h, probe);
      const token = `DELETED_${randomUUID()}`;
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token }, AbortSignal.timeout(2_000)))
        .rejects.toThrow(/Unknown Fabric participant|routing authority changed/);
      expect(mentions(h.natsMain, token)).toHaveLength(0);
      expect(h.fileEvents({ topic: CONTROL, to: h.natsMain.id }).filter(event => dataOf(event).message === token)).toHaveLength(0);
      probe.assertStalled();
    } catch (error) { throw new Error(`${String(error)}\n${linkTail(h, 100)}`, { cause: error }); }
    finally { probe.release(); await h.stop(); probe.restore(); }
  }, 40_000);

  it("a superseded native incarnation cannot recover missing canonical rows or deliver through the public router", async () => {
    const probe = omissionProbe();
    const h = await createMixedHarness({ binary: binary!, startLink: probe.startLink });
    try {
      const presenceKey = natsPresenceKey(NATS_MAIN_MACHINE, h.natsMain.id), leaseKey = natsLeaseKey(NATS_MAIN_MACHINE, h.natsMain.id);
      const oldPresence = (await h.inspector.kv("presence").get<FabricParticipantRecord>(presenceKey))!;
      const oldLease = (await h.inspector.kv("leases").get<FabricKvHostLease>(leaseKey))!;
      // A real owner restart, with the old link stopped so its canonical mirror/history is retained.
      await h.stopLink();
      await h.natsMain.stop(); await h.natsMain.start();
      const epoch = selfRecord(h.natsMain)!.ownerIncarnation;
      expect(epoch).not.toBe(oldPresence.value.ownerIncarnation);
      await h.startLinkNow(); await h.waitMirrored();
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: `NEW_EPOCH_${randomUUID()}` })).resolves.toMatchObject({ acknowledged: true });
      expect(readHostLease(h.fileMeshRoot, h.natsMain.id)).toMatchObject({ incarnationToken: epoch,
        bridgeSupersededTokens: expect.arrayContaining([oldLease.value.incarnation]) });
      probe.stall("always");
      const currentPresence = (await h.inspector.kv("presence").get<FabricParticipantRecord>(presenceKey))!;
      expect(currentPresence.value.ownerIncarnation).toBe(epoch);
      await h.inspector.kv("presence").put(presenceKey, currentPresence.owner,
        { ...currentPresence.value, updatedAt: Date.now() }, currentPresence.revision);
      await probe.entered.wait("production outbound mirror stalled before old-incarnation replay");
      const newerBytes = fs.readFileSync(hostLeasePath(h.fileMeshRoot, h.natsMain.id), "utf8");
      await probe.local.store.delete({ key: topologyKey("hosts", h.natsMain.id) });
      await probe.local.store.delete({ key: topologyKey("participants", h.natsMain.id) });
      expectMissing(h, probe, false);
      const replay = async <V>(bucket: "presence" | "leases", saved: TransportKvEntry<V>, value: V) => {
        const kv = h.inspector.kv(bucket), current = await kv.get(saved.key);
        return kv.put(saved.key, saved.owner, value, current?.revision ?? 0);
      };
      const now = Date.now();
      await replay("leases", oldLease, { ...oldLease.value, updatedAt: now, expiresAt: now + 15_000 });
      await replay("presence", oldPresence, oldPresence.value);
      await waitForValue(() => probe.local.ingressFenced ? true : undefined, "ordinary installer refuses the superseded token", 5_000);
      expect(probe.mirrorFailures).toContainEqual(expect.objectContaining({ code: "FABRIC_BRIDGE_STALE_INCARNATION" }));
      expectMissing(h, probe, false);
      expect(fs.readFileSync(hostLeasePath(h.fileMeshRoot, h.natsMain.id), "utf8")).toBe(newerBytes);
      const token = `SUPERSEDED_${randomUUID()}`;
      await expect(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token }, AbortSignal.timeout(2_000)))
        .rejects.toThrow(/Unknown Fabric participant|routing authority changed/);
      expect(mentions(h.natsMain, token)).toHaveLength(0);
      expect(h.fileEvents({ topic: CONTROL, to: h.natsMain.id }).filter(event => dataOf(event).message === token)).toHaveLength(0);
      probe.assertStalled();
    } catch (error) { throw new Error(`${String(error)}\n${linkTail(h, 100)}`, { cause: error }); }
    finally { probe.release(); await h.stop(); probe.restore(); }
  }, 60_000);
});

describe.skipIf(!binary || process.platform !== "linux" || !linked)("A' link: FILE Main <-> NATS Main on a real broker", () => {
  let h: MixedHarness;
  beforeAll(async () => { h = current = await createMixedHarness({ binary: binary!, natsPeer: true }); }, E2E_WAIT_MS + 60_000);
  afterAll(async () => { await h?.stop(); }, 60_000);

  it("mirrors each allow-listed root live onto the other side, under the link's name and the bridge machine", async () => {
    const { fileView, natsView } = await h.waitMirrored();
    const nats = selfRecord(h.natsMain)!, file = selfRecord(h.fileMain)!;
    // Ryzen 1 readers (smarty-send, smarty-org-target, review_gate live_roots) see a live root with link provenance.
    expect(fileView).toMatchObject({ id: h.natsMain.id, kind: "root", rootId: h.natsMain.id, sessionId: h.natsMain.sessionId,
      ownerIncarnation: nats.ownerIncarnation, remoteHost: h.linkName, local: false, stale: false });
    // v2 routes to the file root through presence + lease the bridge machine holds, at the file Main's exact epoch.
    expect(natsView).toMatchObject({ id: h.fileMain.id, kind: "root", ownerIncarnation: file.ownerIncarnation,
      machine: BRIDGE_MACHINE, local: false, stale: false });
    const kv = await h.kvRoot(BRIDGE_MACHINE, h.fileMain.id);
    expect(kv?.presence).toMatchObject({ id: h.fileMain.id, ownerIncarnation: file.ownerIncarnation });
    expect(kv?.lease).toMatchObject({ id: h.fileMain.id, incarnation: file.ownerIncarnation });
    // Never mirrored back the other way, and never written under the NATS Main's machine.
    expect(await h.kvRoot(NATS_MAIN_MACHINE, h.fileMain.id)).toBeUndefined();
    expect(await h.kvRoot(BRIDGE_MACHINE, h.natsMain.id)).toBeUndefined();
  }, E2E_WAIT_MS + 30_000);

  it("file Main -> NATS Main: one keyed followUp, one consumption, outer from, exact epoch, ACK; a same-key resend replays", async () => {
    const token = `FILE_TO_NATS_${randomUUID()}`, key = `k-${randomUUID()}`;
    const mark = h.wire.mark();
    const first = await h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: key })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); }) as Record<string, unknown>;
    expect(first).toMatchObject({ acknowledged: true, routed: "mesh" });
    const got = mentions(h.natsMain, token);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ from: { id: h.fileMain.id, kind: "main" }, deliverAs: "followUp" });
    // The command that reached the NATS Main: credential-bound from the bridge machine, sender = the file
    // root's verified identity, fenced to the NATS Main's live epoch; its commandId is the per-hop mapped one.
    const fileCommand = h.fileEvents({ topic: CONTROL, to: h.natsMain.id }).find((event) => dataOf(event).message === token)!;
    expect(fileCommand).toMatchObject({ from: { id: h.fileMain.id }, data: { destinationRemoteHost: h.linkName } });
    const crossed = h.wire.since(mark).filter((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token);
    expect(crossed).toHaveLength(1);
    expect(crossed[0]).toMatchObject({ sourceMachine: BRIDGE_MACHINE, attempt: 1,
      envelope: { from: { id: h.fileMain.id }, targetId: h.natsMain.id, operation: "followUp",
        body: { ownerIncarnation: selfRecord(h.natsMain)!.ownerIncarnation } } });
    // Per-hop mapping: distinct verified senders can never collide on the NATS owner's seen-claims.
    expect(bodyOf(crossed[0]!).commandId).toBe(bridgeNatsMappedCommandId(h.linkName, "toNats", h.fileMain.id, String(dataOf(fileCommand).commandId)));
    // The owner's ACK returned into the file mesh to the sender.
    const acks = h.fileEvents({ topic: ACK, to: h.fileMain.id }).filter((event) => dataOf(event).commandId === dataOf(fileCommand).commandId);
    expect(acks.length).toBeGreaterThanOrEqual(1);
    expect(acks[0]).toMatchObject({ kind: "accepted", from: { id: h.natsMain.id }, data: { accepted: true, messageId: first.messageId } });
    // The same key and input again: the receiver replays its stored outcome; nothing is consumed twice.
    const again = await withLog(h.fileMain.invoke("agents.followUp", { id: h.natsMain.id, message: token, idempotencyKey: key })) as Record<string, unknown>;
    expect(again).toMatchObject({ acknowledged: true, messageId: first.messageId });
    expect(mentions(h.natsMain, token)).toHaveLength(1);
  }, 60_000);

  it("NATS Main -> file Main: one keyed followUp, one consumption, outer from, exact epoch, ACK; a same-key resend replays", async () => {
    const token = `NATS_TO_FILE_${randomUUID()}`, key = `k-${randomUUID()}`;
    const mark = h.wire.mark();
    const first = await h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: key })
      .catch((error: unknown) => { throw new Error(`${String(error)}\n${linkTail(h)}`); }) as Record<string, unknown>;
    expect(first).toMatchObject({ acknowledged: true });
    const got = mentions(h.fileMain, token);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatchObject({ from: { id: h.natsMain.id, kind: "main" }, deliverAs: "followUp" });
    const sent = h.wire.since(mark).find((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token)!;
    expect(sent.envelope).toMatchObject({ to: { machine: BRIDGE_MACHINE, hostId: h.fileMain.id } });
    // The link appended exactly one command event: sender from v2's verified presence, the file Main's
    // exact epoch, the link's bridge stamp, and a per-hop mapped commandId.
    const appended = h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter((event) => dataOf(event).message === token);
    expect(appended).toHaveLength(1);
    expect(appended[0]).toMatchObject({ from: { id: h.natsMain.id, kind: "main" },
      data: { ownerIncarnation: selfRecord(h.fileMain)!.ownerIncarnation, bridge: { from: h.linkName } } });
    expect(dataOf(appended[0]!).commandId).toBe(bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(bodyOf(sent).commandId)));
    // The file owner's ACK went back to the NATS sender as a credential-bound ACK from the bridge machine.
    const ack = h.wire.since(mark).find((event) => event.phase === "deliver" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "ack" && bodyOf(event).commandId === bodyOf(sent).commandId);
    expect(ack).toMatchObject({ sourceMachine: BRIDGE_MACHINE, envelope: { id: `ack:${sent.envelope.id}`, from: { id: h.fileMain.id },
      body: { accepted: true } } });
    const again = await withLog(h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: key })) as Record<string, unknown>;
    expect(again).toMatchObject({ acknowledged: true, messageId: first.messageId });
    expect(mentions(h.fileMain, token)).toHaveLength(1);
    expect(h.fileEvents({ topic: CONTROL, to: h.fileMain.id }).filter((event) => dataOf(event).message === token).length).toBeLessThanOrEqual(2);
  }, 60_000);

  it("steers a non-seat Main each way", async () => {
    const toNats = `STEER_FILE_TO_NATS_${randomUUID()}`, toFile = `STEER_NATS_TO_FILE_${randomUUID()}`;
    await expect(h.fileMain.invoke("agents.steer", { id: h.natsMain.id, message: toNats, idempotencyKey: `s-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    await expect(h.natsMain.invoke("agents.steer", { id: h.fileMain.id, message: toFile, idempotencyKey: `s-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    expect(mentions(h.natsMain, toNats)).toEqual([expect.objectContaining({ deliverAs: "steer", from: expect.objectContaining({ id: h.fileMain.id }) })]);
    expect(mentions(h.fileMain, toFile)).toEqual([expect.objectContaining({ deliverAs: "steer", from: expect.objectContaining({ id: h.natsMain.id }) })]);
  }, 60_000);

  it("a second verified NATS sender re-using another sender's commandId never receives that sender's outcome", async () => {
    const peer = h.natsPeer!;
    await waitForValue(() => liveRow(peer, h.fileMain.id), "file root routable from the NATS peer", E2E_WAIT_MS);
    const token = `COLLISION_${randomUUID()}`;
    const mark = h.wire.mark();
    const original = await h.natsMain.invoke("agents.followUp", { id: h.fileMain.id, message: token, idempotencyKey: `c-${randomUUID()}` }) as Record<string, unknown>;
    expect(original).toMatchObject({ acknowledged: true });
    const sent = h.wire.since(mark).find((event) => event.phase === "send" && event.hostId === h.natsMain.id &&
      event.envelope.kind === "command" && bodyOf(event).message === token)!;
    // Same machine credential (machine-b), a verified allow-listed sender (the peer root), and the original's
    // exact commandId and body. Only the sender and its reply address differ.
    const raw = await h.openPeer(NATS_MAIN_MACHINE, `collision-raw-${randomUUID()}`);
    const copy: TransportEnvelope = { ...sent.envelope, id: randomUUID(), from: { id: peer.id, name: peer.label, kind: "main" },
      body: { ...bodyOf(sent), replyTo: peer.id } };
    await raw.send(copy);
    // Prove the link processed the peer's traffic: a later ordinary peer followUp crosses and is acknowledged.
    const barrier = `COLLISION_BARRIER_${randomUUID()}`;
    await expect(peer.invoke("agents.followUp", { id: h.fileMain.id, message: barrier, idempotencyKey: `b-${randomUUID()}` }))
      .resolves.toMatchObject({ acknowledged: true });
    const toPeer = h.wire.since(mark).filter((event) => event.phase === "deliver" && event.hostId === peer.id && event.envelope.kind === "ack");
    // Whatever the peer got for the copied commandId, it is never the original sender's stored outcome:
    // the two senders' commands map to different file-side ids.
    expect(bridgeNatsMappedCommandId(h.linkName, "toFile", peer.id, String(bodyOf(sent).commandId)))
      .not.toBe(bridgeNatsMappedCommandId(h.linkName, "toFile", h.natsMain.id, String(bodyOf(sent).commandId)));
    for (const ack of toPeer.filter((event) => bodyOf(event).commandId === bodyOf(sent).commandId)) {
      expect(bodyOf(ack).messageId).not.toBe(original.messageId);
    }
    const peerCopies = h.fileEvents({ topic: "fabric.control.command", to: h.fileMain.id })
      .filter((event) => dataOf(event).message === token && event.from.id === peer.id);
    for (const copy of peerCopies) {
      expect(dataOf(copy).commandId).toBe(bridgeNatsMappedCommandId(h.linkName, "toFile", peer.id, String(bodyOf(sent).commandId)));
    }
    // The original sender's message was consumed once, from the original sender only.
    expect(mentions(h.fileMain, token).filter((row) => row.from?.id === h.natsMain.id)).toHaveLength(1);
    expect(mentions(h.fileMain, token).every((row) => row.from?.id === h.natsMain.id || row.from?.id === peer.id)).toBe(true);
  }, E2E_WAIT_MS * 3);
});
