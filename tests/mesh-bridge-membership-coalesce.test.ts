import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide, type BridgeSide } from "../src/mesh/bridge.js";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";

// Issue #16879 records the trust-boundary reason for this work: a recipient miss may mean
// membership changed after the last sample, so freshness is rechecked before advancing the
// source cursor. Misses share a trailing sample; a sample or cache entry is never routing
// authority, and unknown canonical recipients retain their source boundary until it commits.
const roots: string[] = [];
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  try {
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  } finally {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const sid = (name: string): string => `session:${name}-00000000`;
const hostKey = (id: string): string => `topology/hosts/${hash(id)}`;
const participantKey = (id: string): string => `topology/participants/${hash(id)}`;

interface Fixture {
  root: string;
  hub: MeshStore;
  far: MeshStore;
  local: StoreBridgeSide;
  remote: StoreBridgeSide;
  cursorPath: string;
}

const fixture = async (now: () => number = Date.now): Promise<Fixture> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "refresh2-mesh-bridge-membership-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 500, { stateBackend: "sqlite" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 500, { stateBackend: "sqlite" });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  return {
    root, hub, far, local: new StoreBridgeSide(hub, "far", now),
    remote: new StoreBridgeSide(far, "hub", now), cursorPath: path.join(root, "cursor.json"),
  };
};

const addNative = async (
  store: MeshStore,
  name: string,
  expiresAt: number,
  id = sid(name),
): Promise<MeshIdentity> => {
  const now = Date.now();
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const startedAt = now - 1_000;
  await store.put({ key: hostKey(id), identity, value: {
    format: 1, id, rootId: id, identity, startedAt, updatedAt: now, expiresAt,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt, updatedAt: now, expiresAt });
  await store.put({ key: participantKey(id), identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt, updatedAt: now, controlProtocol: "v1",
  } });
  return identity;
};

/** A malformed native row makes this real source snapshot partial, preserving omitted mirror attribution. */
const addPartialMarker = async (store: MeshStore): Promise<void> => {
  const marker = sid("partial-marker");
  const identity: MeshIdentity = { id: marker, name: "main", kind: "main", sessionId: "partial-marker" };
  const now = Date.now();
  // Deliberately valid-looking data under the wrong key: the real store scanner reports a dropped row.
  await store.put({ key: `topology/hosts/${hash(`${marker}-wrong-key`)}`, identity, value: {
    format: 1, id: marker, rootId: marker, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000,
  } });
};

const bridgeFor = (f: Fixture, remote: BridgeSide = f.remote, debug?: (line: string) => void): MeshBridge =>
  new MeshBridge({ localName: "hub", remoteName: "far", local: f.local, remote, cursorPath: f.cursorPath,
    presenceMs: 60_000, membershipRetryMs: 1_000, stopMs: 1_000, ...(debug ? { debug } : {}) });

const work = (store: MeshStore, from: MeshIdentity, to: string, text: string): Promise<MeshEvent> =>
  store.publish({ topic: "fleet.work.refresh2.1", kind: "ask", from, to, text });

const cursor = (file: string): { toRemote: { after: number }; toLocal: { after: number } } =>
  JSON.parse(fs.readFileSync(file, "utf8")) as { toRemote: { after: number }; toLocal: { after: number } };

const bridgeStampId = (event: MeshEvent): string | undefined =>
  (event.data as { bridge?: { id?: string } } | undefined)?.bridge?.id;

const deliveredFromHub = (store: MeshStore): MeshEvent[] => store.read({ after: 0, limit: 500 }).filter(event =>
  event.verification === "bridge" && (event.data as { bridge?: { from?: string } } | undefined)?.bridge?.from === "hub");

const until = async (check: () => boolean, message: string, timeoutMs = 5_000): Promise<void> => {
  const deadline = performance.now() + timeoutMs;
  while (!check() && performance.now() < deadline) await sleep(5);
  expect(check(), message).toBe(true);
};

const startNotifyingBridge = async (f: Fixture, remote: BridgeSide = f.remote, debug?: (line: string) => void) => {
  const bridge = bridgeFor(f, remote, debug);
  const syncs = vi.spyOn(bridge, "syncPresence");
  const localTail = vi.spyOn(f.local, "tail");
  const remoteTail = vi.spyOn(f.remote, "tail");
  let failure: unknown;
  const running = bridge.run().catch(error => { failure = error; });
  cleanups.push(async () => { await bridge.stop().catch(() => undefined); await running; });
  await until(() => localTail.mock.calls.length > 0 && remoteTail.mock.calls.length > 0,
    "both real-store bridge directions entered their notification drains");
  // Settle startup and any notification it emitted before recording per-event counts.
  await sleep(100);
  await bridge.syncPresence();
  await quiet(() => syncs.mock.calls.length);
  expect(failure).toBeUndefined();
  return { bridge, syncs, localTail, remoteTail, running, failure: () => failure };
};

const quiet = async (value: () => number, quietMs = 80): Promise<void> => {
  const deadline = performance.now() + 3_000;
  let previous = value();
  let unchangedSince = performance.now();
  while (performance.now() < deadline) {
    await sleep(10);
    const next = value();
    if (next !== previous) { previous = next; unchangedSince = performance.now(); }
    if (performance.now() - unchangedSince >= quietMs) return;
  }
  throw new Error("bridge did not reach a quiet observation window");
};

describe("real-store bridge membership refresh coalescing", () => {
  it("coalesces a pure burst for a proven known-other recipient and stops when idle", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture();
    const hubLead = await addNative(f.hub, "hub-lead", now + 120_000);
    const knownOther = await addNative(f.hub, "hub-recipient", now + 120_000);
    const run = await startNotifyingBridge(f);
    await quiet(() => run.syncs.mock.calls.length);
    expect((await f.local.presence()).reserved).toContain(knownOther.id);
    expect((await f.local.owned()).hosts.map(host => host.record.id)).not.toContain(knownOther.id);
    expect(f.local.mirrored(knownOther.id)).toBe(false);
    expect(f.local.holds(knownOther.id)).toBe(false);

    const K = 128;
    const actualSamples = vi.spyOn(f.remote, "presence");
    const syncBefore = run.syncs.mock.calls.length;
    const samplesBefore = actualSamples.mock.calls.length;
    const events = await Promise.all(Array.from({ length: K }, (_, index) =>
      work(f.hub, hubLead, knownOther.id, `known-other-${index}`)));
    const lastSequence = Math.max(...events.map(event => event.sequence));
    await until(() => cursor(f.cursorPath).toRemote.after >= lastSequence,
      "pure known-other burst reached its durable source cursor without an inline sample");
    expect(actualSamples.mock.calls.length - samplesBefore).toBe(0);
    now += 15_000;
    await run.bridge.step();
    await quiet(() => run.syncs.mock.calls.length);

    const syncRequestCount = run.syncs.mock.calls.length - syncBefore;
    const presenceSyncCount = actualSamples.mock.calls.length - samplesBefore;
    expect(deliveredFromHub(f.far)).toEqual([]);
    expect(cursor(f.cursorPath).toRemote.after).toBe(lastSequence);
    expect(syncRequestCount).toBeGreaterThan(0);
    expect(presenceSyncCount).toBeGreaterThan(0);
    expect(syncRequestCount).toBeLessThanOrEqual(4);
    expect(presenceSyncCount).toBeLessThanOrEqual(4);
    expect(f.local.mirrored(knownOther.id)).toBe(false);
    expect(f.local.holds(knownOther.id)).toBe(false);
    expect(run.failure()).toBeUndefined();

    // A bounded follow-up is allowed; an unchanged idle link must not turn it into a poll.
    const settledSyncs = run.syncs.mock.calls.length;
    const settledSamples = actualSamples.mock.calls.length;
    await sleep(1_050);
    expect(run.syncs.mock.calls.length).toBe(settledSyncs);
    expect(actualSamples.mock.calls.length).toBe(settledSamples);
  }, 30_000);

  it("bounds known-negative refreshes in a mixed burst and reports a reproducible drain measurement", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const hubLead = await addNative(f.hub, "hub-lead", now + 120_000);
    const knownOther = await addNative(f.hub, "hub-recipient", now + 120_000);
    const routed = await addNative(f.far, "routed-member", now + 120_000);
    for (let index = 0; index < 63; index += 1) {
      await addNative(f.far, `member-${String(index).padStart(2, "0")}`, now + 120_000);
    }
    const membershipSize = (await f.remote.presence()).hosts.length;
    expect(membershipSize).toBe(64);
    const run = await startNotifyingBridge(f);
    await quiet(() => run.syncs.mock.calls.length);
    expect((await f.local.presence()).reserved).toContain(knownOther.id);
    expect((await f.local.owned()).hosts.map(host => host.record.id)).not.toContain(knownOther.id);
    expect(f.local.mirrored(knownOther.id)).toBe(false);
    expect(f.local.holds(knownOther.id)).toBe(false);
    expect(f.local.holds(routed.id)).toBe(true);

    const K = 128;
    const ADDRESSED = 8;
    const totalEvents = K + ADDRESSED;
    const workload: Array<{ to: string; addressed: boolean; text: string }> = [];
    for (let index = 0; index < K; index += 1) {
      workload.push({ to: knownOther.id, addressed: false, text: `negative-${index}` });
      if ((index + 1) % (K / ADDRESSED) === 0) {
        workload.push({ to: routed.id, addressed: true, text: `routed-${index}` });
      }
    }
    expect(workload).toHaveLength(totalEvents);

    const actualSamples = vi.spyOn(f.remote, "presence");
    const syncBefore = run.syncs.mock.calls.length;
    const samplesBefore = actualSamples.mock.calls.length;
    const started = performance.now();
    const published = await Promise.all(workload.map(item => work(f.hub, hubLead, item.to, item.text)));
    const originals = published.map((event, index) => ({ event, addressed: workload[index]!.addressed }))
      .sort((left, right) => left.event.sequence - right.event.sequence);
    const lastEvent = originals.at(-1)!;
    await until(() => cursor(f.cursorPath).toRemote.after >= lastEvent.event.sequence,
      "mixed known-negative burst reached its durable source cursor");
    const outboundDrainMs = performance.now() - started;
    await quiet(() => run.syncs.mock.calls.length);

    const actual = deliveredFromHub(f.far);
    const expected = originals.filter(item => item.addressed).map(item => item.event.id);
    const deliveredIds = actual.map(bridgeStampId);
    const orderOk = JSON.stringify(deliveredIds) === JSON.stringify(expected);
    const presenceSyncCount = actualSamples.mock.calls.length - samplesBefore;
    const syncRequestCount = run.syncs.mock.calls.length - syncBefore;

    // Keep this line stable: the parent can run the exact same real-store workload on baseline and candidate.
    console.info("refresh2-membership-measurement " + JSON.stringify({
      K, totalEvents, membershipSize, presenceSyncCount, syncRequestCount,
      outboundDrainMs: Number(outboundDrainMs.toFixed(2)), orderOk,
    }));

    expect(orderOk).toBe(true);
    expect(actual).toHaveLength(expected.length);
    expect(originals.filter(item => !item.addressed).every(item => !actual.some(event => bridgeStampId(event) === item.event.id))).toBe(true);
    expect(cursor(f.cursorPath).toRemote.after).toBe(lastEvent.event.sequence);
    // A miss does not flush before later addressed events or batches.
    expect(presenceSyncCount).toBe(0);
    expect(syncRequestCount).toBe(0);
    expect(f.local.mirrored(knownOther.id)).toBe(false);
    expect(f.local.holds(knownOther.id)).toBe(false);
    expect(run.failure()).toBeUndefined();
  });

  it("retains an expired peer mirror until the coalesced sample proves absence", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const hubLead = await addNative(f.hub, "hub-lead", now + 120_000);
    const expiredPeer = sid("expired-peer");
    await addNative(f.far, "expired-peer", now + 500, expiredPeer);
    await addPartialMarker(f.far);
    const run = await startNotifyingBridge(f);
    await quiet(() => run.syncs.mock.calls.length);
    now += 501;
    expect(f.local.mirrored(expiredPeer)).toBe(true);
    expect(f.local.holds(expiredPeer)).toBe(false);
    expect((await f.local.owned()).hosts.map(host => host.record.id)).not.toContain(expiredPeer);

    const cursorAtSample: number[] = [];
    const originalPresence = f.remote.presence.bind(f.remote);
    const actualSamples = vi.spyOn(f.remote, "presence").mockImplementation(async () => {
      cursorAtSample.push(cursor(f.cursorPath).toRemote.after);
      return originalPresence();
    });
    const syncBefore = run.syncs.mock.calls.length;
    const samplesBefore = actualSamples.mock.calls.length;
    const event = await work(f.hub, hubLead, expiredPeer, "expired-peer-must-refresh");
    await until(() => run.bridge.options.local.mirrored?.(expiredPeer) === true &&
      cursor(f.cursorPath).toRemote.after < event.sequence, "expired peer remains unread");
    now += 15_000;
    await run.bridge.step();
    expect(run.syncs.mock.calls.length).toBeGreaterThan(syncBefore);
    expect(actualSamples.mock.calls.length).toBeGreaterThan(samplesBefore);
    await until(() => cursor(f.cursorPath).toRemote.after >= event.sequence,
      "expired-peer event reached its durable source cursor");

    expect(cursorAtSample.some(after => after < event.sequence)).toBe(true);
    expect(deliveredFromHub(f.far).some(delivered => bridgeStampId(delivered) === event.id)).toBe(false);
    expect(cursor(f.cursorPath).toRemote.after).toBe(event.sequence);
    expect(f.local.mirrored(expiredPeer)).toBe(true);
    expect(f.local.holds(expiredPeer)).toBe(false);
    expect(run.failure()).toBeUndefined();
  });

  it("coalesces a 50-event notification-mode page for a foreign session and delivers after it joins this link", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const elsewhere = await fixture(() => now);
    const hubLead = await addNative(f.hub, "hub-lead", now + 120_000);
    const recipient = await addNative(elsewhere.far, "foreign-session", now + 120_000);
    let localNotice!: (kind: BridgeChange) => void;
    let remoteNotice!: (kind: BridgeChange) => void;
    // Deterministic notification delivery; run() still sets notificationMode itself.
    f.local.subscribeChanges = changed => { localNotice = changed; return () => undefined; };
    f.remote.subscribeChanges = changed => { remoteNotice = changed; return () => undefined; };
    const run = await startNotifyingBridge(f);
    const samples = vi.spyOn(f.remote, "presence");
    const syncBefore = run.syncs.mock.calls.length;
    const inputs = Array.from({ length: 50 }, (_, index) => ({ topic: "fabric.control.ack", kind: "ack",
      from: hubLead, to: recipient.id, data: { version: 1, commandId: `foreign-${index}`, targetId: hubLead.id, accepted: true } }));
    const events = await f.hub.publishBatch(inputs);
    expect(events).toHaveLength(50);
    expect((await f.local.tail(0)).events.map(event => event.id)).toEqual(events.map(event => event.id));
    expect((await f.local.presence()).reserved).not.toContain(recipient.id);
    expect(f.local.mirrored(recipient.id)).toBe(false);
    expect(f.local.holds(recipient.id)).toBe(false);
    localNotice("events");
    await quiet(() => run.localTail.mock.calls.length);
    expect(samples).not.toHaveBeenCalled();
    expect(cursor(f.cursorPath).toRemote.after).toBeLessThan(events[0]!.sequence);
    expect(deliveredFromHub(f.far)).toEqual([]);
    await addNative(f.far, "foreign-session", now + 120_000, recipient.id);
    now += 15_000;
    remoteNotice("presence");
    await until(() => cursor(f.cursorPath).toRemote.after >= events.at(-1)!.sequence,
      "the committed later membership sample releases the retained page", 10_000);
    expect(samples).toHaveBeenCalledTimes(1);
    expect(run.syncs.mock.calls.length - syncBefore).toBe(1);
    expect(deliveredFromHub(f.far).map(bridgeStampId)).toEqual(events.map(event => event.id));
    expect(run.failure()).toBeUndefined();
  }, 30_000);

  // Keep reason: incident regression and trust boundary; exercise real tail/mirror/publish,
  // not just call counts. A missing projection must never authorize a control delivery.
  it("coalesces 50 unaddressed events in one page and delivers a deferred recipient that joins later", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const hubLead = await addNative(f.hub, "hub-lead", now + 120_000);
    const bridge = bridgeFor(f);
    await bridge.start();
    cleanups.push(() => bridge.stop());
    const samples = vi.spyOn(f.remote, "presence");
    const syncs = vi.spyOn(bridge, "syncPresence");
    const events: MeshEvent[] = [];
    for (let index = 0; index < 50; index++) {
      events.push(await f.hub.publish({ topic: "fabric.control.ack", kind: "ack", from: hubLead,
        to: sid(`unknown-${index}`), data: { version: 1, commandId: `burst-${index}`, targetId: hubLead.id, accepted: true } }));
    }
    expect((await f.local.tail(0)).events.map(event => event.id)).toEqual(events.map(event => event.id));
    expect(await bridge.step()).toMatchObject({ toRemote: 0, dropped: 0 });
    expect(samples).not.toHaveBeenCalled();
    expect(cursor(f.cursorPath).toRemote.after).toBeLessThan(events[0]!.sequence);
    expect(deliveredFromHub(f.far)).toEqual([]);
    await addNative(f.far, "later-member", now + 120_000, events[0]!.to!);
    now += 14_999;
    await bridge.step();
    expect(samples).not.toHaveBeenCalled();
    expect(deliveredFromHub(f.far)).toEqual([]);
    now += 1;
    expect(await bridge.step()).toMatchObject({ toRemote: 1, dropped: 0 });
    expect(samples).toHaveBeenCalledTimes(1);
    expect(syncs).toHaveBeenCalledTimes(1);
    expect(deliveredFromHub(f.far).map(bridgeStampId)).toEqual([events[0]!.id]);
    expect(cursor(f.cursorPath).toRemote.after).toBe(events.at(-1)!.sequence);
    await bridge.step();
    expect(samples).toHaveBeenCalledTimes(1);
    expect(deliveredFromHub(f.far).map(bridgeStampId)).toEqual([events[0]!.id]);
  });

  it("waits out an in-flight pre-change sample, then routes after a delayed membership notice", async () => {
    const f = await fixture();
    const hubLead = await addNative(f.hub, "hub-lead", Date.now() + 120_000);
    const delayedKinds: BridgeChange[] = [];
    let releaseDelayed: ((kind: BridgeChange) => void) | undefined;
    const delayedRemote = new Proxy(f.remote, {
      get(target, property) {
        if (property === "subscribeChanges") return (changed: (kind: BridgeChange) => void, failed: (error: Error) => void) => {
          releaseDelayed = changed;
          return target.subscribeChanges(kind => {
            if (kind === "presence" || kind === "lease") delayedKinds.push(kind);
            else changed(kind);
          }, failed);
        };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as BridgeSide;
    const run = await startNotifyingBridge(f, delayedRemote);
    await quiet(() => run.syncs.mock.calls.length);
    delayedKinds.length = 0;

    const sampled = deferred<unknown>();
    const releaseSample = deferred<void>();
    cleanups.push(() => { releaseSample.resolve(); });
    const originalPresence = f.remote.presence.bind(f.remote);
    let gateNext = true;
    const sourceSamples = vi.spyOn(f.remote, "presence").mockImplementation(async () => {
      const snapshot = await originalPresence();
      if (gateNext) {
        gateNext = false;
        sampled.resolve(snapshot); // Snapshot is fixed before the membership change below.
        await releaseSample.promise;
      }
      return snapshot;
    });
    const beforeSyncs = run.syncs.mock.calls.length;
    const preChange = run.bridge.syncPresence();
    await sampled.promise;

    const newcomerId = sid("arriving-member");
    await addNative(f.far, "arriving-member", Date.now() + 120_000, newcomerId);
    await until(() => delayedKinds.includes("presence"), "the source watch observed the membership change but held its notice");
    const event = await work(f.hub, hubLead, newcomerId, "arrived-after-old-sample");

    // The event retains its boundary without joining the old pass inline.
    await until(() => run.localTail.mock.calls.length > 1,
      "the unknown canonical recipient woke the source drain");
    await sleep(30);
    expect(deliveredFromHub(f.far).some(delivered => bridgeStampId(delivered) === event.id)).toBe(false);
    expect(cursor(f.cursorPath).toRemote.after).toBeLessThan(event.sequence);

    releaseSample.resolve();
    await preChange;
    expect(run.syncs.mock.calls.length - beforeSyncs).toBe(1);
    await run.bridge.syncPresence(); // a later post-miss sample, without releasing the delayed notice
    await until(() => deliveredFromHub(f.far).some(delivered => bridgeStampId(delivered) === event.id),
      "a fresh post-change sample admitted and delivered the event");
    expect(sourceSamples.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(f.local.holds(newcomerId)).toBe(true);
    expect(cursor(f.cursorPath).toRemote.after).toBe(event.sequence);
    expect(deliveredFromHub(f.far).filter(delivered => bridgeStampId(delivered) === event.id)).toHaveLength(1);

    // Deliver the delayed notice only after success; it is a wake, not authority or a second delivery.
    for (const kind of delayedKinds.splice(0)) releaseDelayed?.(kind);
    await quiet(() => run.syncs.mock.calls.length);
    expect(deliveredFromHub(f.far).filter(delivered => bridgeStampId(delivered) === event.id)).toHaveLength(1);
    expect(run.failure()).toBeUndefined();
  }, 12_000);

  it("does not route through a mirrored identity after a native takeover", async () => {
    const f = await fixture();
    const hubLead = await addNative(f.hub, "hub-lead", Date.now() + 120_000);
    const targetId = sid("takeover-target");
    await addNative(f.far, "far-owner", Date.now() + 120_000, targetId);
    const bridge = bridgeFor(f);
    await bridge.start();
    expect(f.local.holds(targetId)).toBe(true);
    expect(f.local.mirrored(targetId)).toBe(true);

    // Same canonical id now belongs to a native hub owner. This real store write supersedes the
    // mirror; a prior membership sample cannot override the native reservation or commit fence.
    await addNative(f.hub, "native-takeover", Date.now() + 120_000, targetId);
    expect(f.local.mirrored(targetId)).toBe(false);
    expect(f.local.holds(targetId)).toBe(false);
    expect((await f.local.owned()).hosts.map(host => host.record.id)).not.toContain(targetId);

    const event = await work(f.hub, hubLead, targetId, "must-not-cross-native-takeover");
    const result = await bridge.step();
    expect(result.toRemote).toBe(0);
    expect(deliveredFromHub(f.far).some(delivered => bridgeStampId(delivered) === event.id)).toBe(false);
    expect(f.hub.get(hostKey(targetId))?.updatedBy.id).toBe(targetId);
    expect(cursor(f.cursorPath).toRemote.after).toBe(event.sequence);
  });

  it("checkpoints coalesced misses before addressed batch admission without an inline refresh", async () => {
    const now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const hubLead = await addNative(f.hub, "hub-lead", now + 120_000);
    const knownOther = await addNative(f.hub, "hub-recipient", now + 120_000);
    const routedId = sid("checkpoint-routed");
    await addNative(f.far, "checkpoint-routed", now + 120_000, routedId);
    const releaseTail = deferred<void>();
    let tailEntered = false;
    const originalTail = f.local.tail.bind(f.local);
    f.local.tail = async (after, offset, pageBytes) => {
      if (!tailEntered) { tailEntered = true; await releaseTail.promise; }
      return originalTail(after, offset, pageBytes);
    };
    let run: Awaited<ReturnType<typeof startNotifyingBridge>>;
    try { run = await startNotifyingBridge(f); }
    finally { cleanups.push(() => { releaseTail.resolve(); }); }
    expect((await f.local.presence()).reserved).toContain(knownOther.id);
    expect(f.local.mirrored(knownOther.id)).toBe(false);
    expect(f.local.holds(knownOther.id)).toBe(false);
    expect(f.local.holds(routedId)).toBe(true);

    const inputs: Parameters<MeshStore["publishBatch"]>[0] = Array.from({ length: 130 }, (_, index) => ({
      topic: "fleet.work.refresh2.1", kind: "ask", from: hubLead,
      to: index < 128 ? knownOther.id : routedId, text: `checkpoint-coalesce-${index}`,
    }));
    const events: MeshEvent[] = [];
    for (let offset = 0; offset < inputs.length;) {
      const committed = await f.hub.publishBatch(inputs.slice(offset));
      expect(committed.length).toBeGreaterThan(0);
      events.push(...committed);
      offset += committed.length;
    }
    expect(events).toHaveLength(130);
    const prefix = events.slice(0, 128);
    const addressed = events.slice(128);
    const page = await f.local.tail(0);
    expect(page.events.map(event => event.id)).toEqual(events.map(event => event.id));
    expect(page.events).toHaveLength(130);
    expect(page.through).toBe(events.at(-1)!.sequence);

    const releaseRefresh = deferred<void>();
    cleanups.push(() => { releaseRefresh.resolve(); });
    const samples = vi.spyOn(f.remote, "presence");
    const publish = f.remote.publishBatchWithReceipt.bind(f.remote);
    let publishEntered = false;
    f.remote.publishBatchWithReceipt = async (...args) => {
      publishEntered = true;
      await releaseRefresh.promise;
      return publish(...args);
    };
    try {
      releaseTail.resolve();
      await until(() => publishEntered, "addressed batch admission reached its commit seam without a presence sample");
      const gatedCursor = cursor(f.cursorPath).toRemote.after;
      expect(gatedCursor).toBe(prefix.at(-1)!.sequence);
      expect(gatedCursor).toBeLessThan(addressed[0]!.sequence);
      expect(samples.mock.calls).toHaveLength(0); // A pending miss is not a pre-publication flush.
      expect(deliveredFromHub(f.far)).toEqual([]);

      releaseRefresh.resolve();
      const finalOffset = (await f.local.latestCursor()).offset;
      await until(() => {
        const saved = JSON.parse(fs.readFileSync(f.cursorPath, "utf8")) as {
          toRemote: { after: number; offset?: number };
        };
        return saved.toRemote.after === events.at(-1)!.sequence && saved.toRemote.offset === finalOffset;
      }, "the held projection admitted the batch and completed its page", 10_000);
      expect(samples.mock.calls.length).toBe(0);
      expect(f.local.holds(routedId)).toBe(true);
      const delivered = deliveredFromHub(f.far);
      expect(delivered.map(bridgeStampId)).toEqual(addressed.map(event => event.id));
      expect(delivered).toHaveLength(2);
      const saved = JSON.parse(fs.readFileSync(f.cursorPath, "utf8")) as {
        toRemote: { after: number; offset?: number };
      };
      expect(saved.toRemote.after).toBe(events.at(-1)!.sequence);
      expect(saved.toRemote.offset).toBe((await f.local.latestCursor()).offset);
      expect(run.failure()).toBeUndefined();
    } finally {
      releaseRefresh.resolve();
      f.remote.publishBatchWithReceipt = publish;
    }
  }, 12_000);
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
