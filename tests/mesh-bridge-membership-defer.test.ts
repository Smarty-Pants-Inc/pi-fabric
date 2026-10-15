// Real-store admission probes, not a claim that plain sender absence is red on 014315d.
// Only mirror timing is injected. Scratch evidence is retained for the parent; no rm/withdraw.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BRIDGE_LEASE_MS, BridgeMirrorDeferredError, BridgeOwnershipError, MeshBridge, RemoteBridgeSide,
  serveBridgeAgent, StoreBridgeSide, type BridgePublish,
} from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const sid = (name: string) => `session:${name}-00000000`;
const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const until = async (check: () => boolean) => {
  const deadline = performance.now() + 4_000;
  while (!check() && performance.now() < deadline) await sleep(5);
  expect(check(), "bounded bridge observation").toBe(true);
};
const bounded = async <T,>(work: Promise<T>): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("bridge seam did not settle within 4 s")), 4_000);
    })]);
  } finally { if (timer) clearTimeout(timer); }
};
const native = async (store: MeshStore, name: string, ttl = 120_000) => {
  const now = Date.now(), id = sid(name);
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  await store.put({ key: key("hosts", id), identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + ttl,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + ttl });
  writeParticipantFile(store.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: `fixture:${id}`, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return identity;
};
const events = (store: MeshStore) => store.read({ after: 0, limit: 100 });
const controls = async (store: MeshStore, from: MeshIdentity, to: MeshIdentity, tag: string) => [
  await store.publish({ topic: "fabric.control.command", kind: "followUp", from, to: to.id, data: {
    version: 1, commandId: `${tag}-command`, targetId: to.id, operation: "followUp", replyTo: from.id,
    requestedAt: Date.now(), deadlineAt: Date.now() + 300_000, ownerIncarnation: `fixture:${to.id}`, message: tag,
    destinationRemoteHost: "hub",
  } }),
  await store.publish({ topic: "fabric.control.ack", kind: "ack", from, to: to.id, data: {
    version: 1, commandId: `${tag}-ack`, targetId: from.id, accepted: true, messageId: tag,
  } }),
];

const fixture = async (clock?: () => number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-membership-"));
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100, { stateBackend: "file" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 100, { stateBackend: "file" });
  const local = new StoreBridgeSide(hub, "far", clock), agent = new StoreBridgeSide(far, "hub", clock);
  const input = new PassThrough(), output = new PassThrough();
  const requests: string[] = [], logs: string[] = [];
  input.on("data", chunk => {
    for (const line of String(chunk).trim().split("\n")) requests.push(JSON.parse(line).op);
  });
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, 500);
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote, cursorPath,
    presenceMs: 60_000, walCapRetryMinMs: 25, log: line => logs.push(line) });
  let running: Promise<void> | undefined, failure: unknown;
  cleanups.push(async () => {
    // Closing the transport releases run() and both subscriptions without deleting mirrors.
    remote.close(); input.end();
    await bounded(Promise.all([serving, running]).then(() => undefined));
    output.end(); hub.closeState(); far.closeState();
  });
  const lead = await native(hub, "lead");
  await remote.hello();
  await bridge.start();
  const launch = async () => {
    running = bridge.run().catch(error => { failure = error; });
    await until(() => requests.includes("tail") || failure !== undefined);
    expect(failure).toBeUndefined();
    await sleep(60); // let startup's own mirror notifications settle before fault injection
  };
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toLocal: { after: number } };
  return { hub, far, local, agent, remote, bridge, lead, requests, logs, launch, cursor, failure: () => failure };
};

type Snapshot = Parameters<StoreBridgeSide["mirror"]>[0];
const holdMirror = (side: StoreBridgeSide, matches: (snapshot: Snapshot) => boolean = () => true) => {
  const entered = deferred<Snapshot>(), release = deferred<void>();
  const mirror = side.mirror.bind(side);
  let armed = true;
  vi.spyOn(side, "mirror").mockImplementation(async (...args) => {
    if (armed && matches(args[0])) {
      armed = false;
      entered.resolve(args[0]);
      await release.promise;
    }
    return mirror(...args); // preserve requested-at/fromHub; never fabricate admission
  });
  cleanups.push(async () => { release.resolve(); });
  return { entered: entered.promise, release: () => release.resolve() };
};
const stampId = (event: MeshEvent) => (event.data as { bridge?: { id: string } } | undefined)?.bridge?.id;

describe("canonical membership admission while mirrors are deferred", () => {
  it.each(["hub admission", "degraded stdio"] as const)(
    "holds a new root's command and ACK until %s lands, then delivers each once without a mirror RPC queue",
    async seam => {
      const f = await fixture();
      await f.launch();
      const mirrorCount = () => f.requests.filter(op => op === "mirror").length;
      const before = mirrorCount();
      const held = holdMirror(seam === "hub admission" ? f.local : f.agent,
        snapshot => seam !== "hub admission" || snapshot.hosts.some(host => host.record.id === sid("new-root")));
      let newcomer: MeshIdentity;
      let refresh: Promise<unknown>;
      if (seam === "degraded stdio") {
        // The old snapshot is already in the agent's serial FIFO when the new native appears.
        refresh = f.bridge.syncPresence().catch(error => error);
        await bounded(held.entered);
        newcomer = await native(f.far, "new-root");
      } else {
        newcomer = await native(f.far, "new-root");
        refresh = f.bridge.syncPresence().catch(error => error);
        await bounded(held.entered);
      }
      const sent = await controls(f.far, newcomer, f.lead, "new-membership");
      if (seam === "degraded stdio") await until(() => f.logs.some(line => line.includes("bridge mirror degraded")));
      await sleep(120); // cover several 25/50/100 ms retry opportunities while still held
      expect(f.local.holds(newcomer.id)).toBe(false);
      expect(f.hub.get(key("participants", newcomer.id))).toBeUndefined();
      expect(events(f.hub)).toEqual([]);
      expect(mirrorCount()).toBe(before + 1); // one pending wire mirror; retries never enqueue another
      expect(f.failure()).toBeUndefined();

      held.release();
      await bounded(refresh);
      await until(() => events(f.hub).length === 2);
      expect(f.local.holds(newcomer.id)).toBe(true);
      expect(events(f.hub).map(stampId)).toEqual(sent.map(event => event.id));
      for (const event of events(f.hub)) expect(event).toMatchObject({ verification: "bridge",
        from: { ...newcomer, verified: "bridge" }, to: f.lead.id, data: { bridge: { from: "far" } } });

      // Positive proof of a foreign namespace and a hub reservation must remain a denial,
      // not be reclassified as the new canonical root's transient missing projection.
      const foreign: MeshIdentity = { id: "factory-host:foreign", name: "foreign", kind: "agent" };
      for (const [sender, reason] of [[foreign, "sender is not a live participant of the remote"],
        [f.lead, "sender claims a hub identity"]] as const) {
        const denied = await controls(f.far, sender, f.lead, sender.id);
        await until(() => denied.every(event => f.logs.some(line => line.includes(event.id) && line.endsWith(reason))));
      }
      // Stamped work from another origin is intentionally rejected, NOT a legitimate control loss.
      const echo = await new StoreBridgeSide(f.far, "elsewhere").publish({ topic: "fleet.work.echo.1", kind: "ask",
        from: newcomer, to: f.lead.id, data: { bridge: { from: "elsewhere", id: "intentional-work-echo" } } });
      const echoId = events(f.far).find(event => event.sequence === echo.sequence)!.id;
      await until(() => f.logs.some(line => line.includes(echoId) && line.endsWith(": not allowed")));
      const barrier = await f.far.publish({ topic: "unrelated", kind: "barrier", from: newcomer });
      await until(() => f.cursor().toLocal.after >= barrier.sequence);
      expect(events(f.hub).map(stampId)).toEqual(sent.map(event => event.id));
      expect(f.logs.some(line => sent.some(event => line.includes(event.id) && /dropped|refused/.test(line)))).toBe(false);
      expect(f.failure()).toBeUndefined();
    }, 12_000,
  );

  it("defers a new root's command and ACK when sampled mirrors land stale despite a continuously renewing origin", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const newcomer = await native(f.far, "renewing-root");
    const entered = deferred<void>(), release = deferred<void>();
    cleanups.push(async () => { release.resolve(); });
    const mirror = f.local.mirror.bind(f.local);
    let induceLatency = true, first = true, delayed = 0;
    const before = f.requests.filter(op => op === "mirror").length;
    vi.spyOn(f.local, "mirror").mockImplementation(async (...args) => {
      if (induceLatency && args[0].hosts.some(host => host.record.id === newcomer.id)) {
        delayed++;
        if (first) { first = false; entered.resolve(); await release.promise; }
        // Source heartbeats stay live throughout the simulated wait. Each captured sample
        // nonetheless lands AFTER its original translated 15 s deadline, including retries.
        for (const elapsed of [5_000, 5_000, 5_001]) {
          now += elapsed;
          writeHostLease(f.far.root, { ...readHostLease(f.far.root, newcomer.id)!, updatedAt: now, expiresAt: now + BRIDGE_LEASE_MS });
        }
      }
      return mirror(...args);
    });
    const refreshing = f.bridge.syncPresence().catch(error => error);
    await bounded(entered.promise);
    const sent = await controls(f.far, newcomer, f.lead, "renewed-during-mirror");
    expect(events(f.hub)).toEqual([]);
    release.resolve();
    const firstPass = await bounded(refreshing);
    if (firstPass !== undefined) expect(firstPass).toBeInstanceOf(BridgeMirrorDeferredError);
    const result = await f.bridge.step().catch(error => {
      expect(error).toBeInstanceOf(BridgeMirrorDeferredError);
      return undefined;
    });
    // RED on base: #syncPresence succeeds, owned() is empty, and the final sender
    // refusal drops BOTH events and checkpoints past them instead of classifying stale authority.
    if (result) expect(result).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(f.cursor().toLocal.after).toBeLessThan(sent[0]!.sequence);
    expect(events(f.hub)).toEqual([]);
    expect(readHostLease(f.far.root, newcomer.id)!.expiresAt).toBeGreaterThan(now);
    expect(f.local.holds(newcomer.id)).toBe(false);
    expect(delayed).toBeLessThanOrEqual(5);
    expect(f.requests.filter(op => op === "mirror").length - before).toBeLessThanOrEqual(5);
    expect(f.logs.some(line => sent.some(event => line.includes(event.id) && /dropped|refused/.test(line)))).toBe(false);
    induceLatency = false;
    await f.bridge.syncPresence(); // a completed fresh mirror wakes the retained boundary
    expect(await f.bridge.step()).toMatchObject({ toLocal: 2, dropped: 0 });
    expect(events(f.hub).map(stampId)).toEqual(sent.map(event => event.id));
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(events(f.hub).map(stampId)).toEqual(sent.map(event => event.id));
  });

  it("releases a pending boundary when a fresh mirror proves its sender is absent", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now);
    const newcomer = await native(f.far, "later-absent", BRIDGE_LEASE_MS);
    const mirror = f.local.mirror.bind(f.local);
    let induceLatency = true;
    vi.spyOn(f.local, "mirror").mockImplementation(async (...args) => {
      if (induceLatency && args[0].hosts.some(host => host.record.id === newcomer.id)) {
        for (const elapsed of [5_000, 5_000, 5_001]) {
          now += elapsed;
          writeHostLease(f.far.root, { ...readHostLease(f.far.root, newcomer.id)!, updatedAt: now, expiresAt: now + BRIDGE_LEASE_MS });
        }
      }
      return mirror(...args);
    });
    await f.bridge.syncPresence();
    const sent = await controls(f.far, newcomer, f.lead, "later-absent");
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(f.cursor().toLocal.after).toBeLessThan(sent[0]!.sequence);
    // The source now really stops renewing. A new successful sample omits it, so its
    // prior pending status must not poison this direction forever or grant authority.
    induceLatency = false;
    now += BRIDGE_LEASE_MS + 1;
    await f.bridge.syncPresence();
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 2 });
    expect(f.cursor().toLocal.after).toBeGreaterThanOrEqual(sent[1]!.sequence);
    expect(events(f.hub)).toEqual([]);
    expect(await f.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
  });

  it("does not turn a late expired snapshot into sender authority or renew it by replaying the same sample", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await fixture(() => now); // isolate landing before any fresh snapshot can hide it
    const newcomer = await native(f.far, "short-root", 1_000);
    const requestedAt = now;
    const held = holdMirror(f.local, snapshot => snapshot.hosts.some(host => host.record.id === newcomer.id));
    const before = f.requests.filter(op => op === "mirror").length;
    const pass = f.bridge.syncPresence().catch(error => error);
    const snapshot = await bounded(held.entered);
    now += BRIDGE_LEASE_MS + 1; // no renewal at the origin; even the bridge cap has elapsed
    const spoofed = await controls(f.far, newcomer, f.lead, "after-source-expiry");
    held.release();
    const outcome = await bounded(pass);
    if (outcome !== undefined) expect(outcome).toBeInstanceOf(BridgeMirrorDeferredError);
    // An attribution projection can exist without live authority. Checking just its presence
    // (or caching snapshot-admitted senders) would authorize this stale identity incorrectly.
    expect(f.local.mirrored(newcomer.id)).toBe(true);
    expect(f.local.holds(newcomer.id)).toBe(false);
    expect((await f.local.owned()).hosts.map(host => host.record.id)).not.toContain(newcomer.id);
    expect(readHostLease(f.hub.root, newcomer.id)?.expiresAt).toBe(requestedAt + 1_000);
    await f.local.mirror(snapshot, requestedAt);
    expect(readHostLease(f.hub.root, newcomer.id)?.expiresAt).toBe(requestedAt + 1_000);
    expect(f.local.holds(newcomer.id)).toBe(false);
    const publications: BridgePublish[] = spoofed.map(event => ({ topic: event.topic, kind: event.kind,
      from: newcomer, to: f.lead.id, data: { ...(event.data as Record<string, unknown>),
        bridge: { from: "far", id: event.id } } }));
    for (const publication of publications) {
      await expect(f.local.publish(publication, [newcomer.id])).rejects.toBeInstanceOf(BridgeOwnershipError);
    }
    await expect(f.local.publishBatch(publications.map(event => ({ event, held: [newcomer.id] }))))
      .rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(events(f.hub)).toEqual([]);
    expect(f.requests.filter(op => op === "mirror")).toHaveLength(before + 1);
  });
});
