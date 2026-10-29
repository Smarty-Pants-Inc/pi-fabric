// Keep reason: smarty-dev#17511 / pi-fabric#1711 P2-1, a lease hint stalled real outbound delivery on full membership I/O.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const cleanups: Array<() => void | Promise<unknown>> = [];
afterEach(async () => {
  // Release test-owned gates before joining the real bridge and RPC workers.
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
});
const deferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const bounded = async <T>(promise: Promise<T>, label: string, ms = 3_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out: ${label}`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const native = async (store: MeshStore, name: string, leaseFile = true) => {
  const now = Date.now(), id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id, rootId: id, identity, incarnationToken: name,
    startedAt: now, updatedAt: now, expiresAt: now + 60_000 };
  await store.put({ key: key("hosts", id), value: host, identity });
  const entry: MeshStateEntry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name, status: "idle", runner: "pi", transport: "host", capabilities: [], sessionId: name,
    startedAt: now, updatedAt: now, controlProtocol: "v1",
  } };
  writeParticipantFile(store.root, entry);
  const lease = { id, rootId: id, identityId: id, startedAt: now, incarnationToken: name,
    updatedAt: now, expiresAt: host.expiresAt };
  if (leaseFile) writeHostLease(store.root, lease);
  return { id, identity, host, lease };
};

const fixture = async (leaseFiles = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lease-hint-forward-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 500, { stateBackend: "sqlite" });
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 500, { stateBackend: "file" });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const local = new StoreBridgeSide(hub, "far"), agent = new StoreBridgeSide(far, "hub");
  const lead = await native(hub, "lease-hint-lead", leaseFiles), lane = await native(far, "lease-hint-lane", leaseFiles);
  const input = new PassThrough(), output = new PassThrough();
  const requests: string[] = [];
  input.on("data", chunk => { for (const line of String(chunk).trim().split("\n")) requests.push(JSON.parse(line).op); });
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  cleanups.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  await remote.hello(); // CLI negotiates receipt publication before bootstrap.
  const notices: BridgeChange[] = [];
  const waiters = new Map<BridgeChange, ReturnType<typeof deferred<void>>>();
  const subscribe = local.subscribeChanges.bind(local);
  vi.spyOn(local, "subscribeChanges").mockImplementation((changed, failed) => subscribe(kind => {
    notices.push(kind);
    changed(kind); // Observe actual fs.watch classification, never inject a notice or authority.
    waiters.get(kind)?.resolve();
  }, failed));
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(hub.root, "bridge-cursor.json"), stopMs: 100 });
  const sync = vi.spyOn(bridge, "syncPresence");
  const tail = vi.spyOn(local, "tail");
  await bridge.start(); // Production CLI bootstrap precedes notification subscriptions.
  let failure: unknown;
  const running = bridge.run().catch(error => { failure = error; });
  cleanups.push(async () => { await bridge.stop(); await running; });
  // Only bootstrap settles here; the regression assertions below are driven by notifications,
  // actual append receipts and an explicit membership gate, not by a sleep-only latency check.
  const bootstrapDeadline = performance.now() + 3_000;
  for (;;) {
    const calls = sync.mock.calls.length;
    await bounded(Promise.all(sync.mock.results.map(result => result.value)), "bootstrap sync");
    await sleep(40);
    if (tail.mock.calls.length > 0 && sync.mock.calls.length === calls) break;
    if (failure) throw failure;
    if (performance.now() >= bootstrapDeadline) throw new Error("Bootstrap mirror notifications did not settle");
  }
  expect(local.holds(lane.id)).toBe(true);
  sync.mockClear(); requests.length = 0; notices.length = 0;
  return { hub, far, local, agent, bridge, lead, lane, sync, requests, notices, failure: () => failure,
    nextNotice: (kind: BridgeChange) => {
      const next = deferred(); waiters.set(kind, next); return next.promise;
    } };
};
const gateMembership = (agent: StoreBridgeSide) => {
  const entered = deferred(), release = deferred();
  const presence = agent.presence.bind(agent);
  const calls = vi.spyOn(agent, "presence").mockImplementation(async () => {
    entered.resolve();
    await release.promise;
    return presence(); // A released pass still uses the real membership snapshot.
  });
  cleanups.push(() => { release.resolve(); });
  return { entered: entered.promise, release: () => release.resolve(), calls };
};
const observeDelivery = (f: Awaited<ReturnType<typeof fixture>>) => {
  const delivered = deferred<number>();
  const publish = StoreBridgeSide.prototype.publishWithReceipt.bind(f.agent);
  vi.spyOn(f.agent, "publishWithReceipt").mockImplementation(async (...args) => {
    const receipt = await publish(...args);
    delivered.resolve(receipt.sequence);
    return receipt;
  });
  return delivered.promise;
};
const send = (f: Awaited<ReturnType<typeof fixture>>) => f.hub.publish({ topic: "fleet.work.lease-hint-forward",
  kind: "ask", from: f.lead.identity, to: f.lane.id, text: "deliver without unrelated membership I/O" });
const assertDelivery = (f: Awaited<ReturnType<typeof fixture>>, sequence: number, sourceId: string) => {
  const delivered = f.far.read({ after: sequence - 1, limit: 1 })[0]!;
  expect(delivered).toMatchObject({ sequence, topic: "fleet.work.lease-hint-forward", to: f.lane.id,
    from: f.lead.identity, verification: "bridge", data: { bridge: { from: "hub", id: sourceId } } });
  expect(f.failure()).toBeUndefined();
};

describe.sequential("outbound forwarding after a lease-only hint (#17511)", () => {
  it.each([
    { side: "local", leaseFiles: true }, { side: "remote", leaseFiles: true },
    { side: "local", leaseFiles: false }, { side: "remote", leaseFiles: false },
  ] as const)("publishes after a real $side TTL hint without starting full membership (bootstrap lease files: $leaseFiles)", async ({ side, leaseFiles }) => {
    const f = await fixture(leaseFiles), gate = gateMembership(f.agent), delivered = observeDelivery(f);
    const readdir = vi.spyOn(fs, "readdirSync"), readFile = vi.spyOn(fs, "readFileSync");
    let remoteLease!: Promise<void>;
    if (side === "remote") {
      const notice = deferred();
      outputNotice(f, notice.resolve);
      remoteLease = notice.promise;
    }
    const hinted = side === "local" ? f.nextNotice("lease") : remoteLease;
    const store = side === "local" ? f.hub : f.far, owner = side === "local" ? f.lead : f.lane;
    const existing = readHostLease(store.root, owner.id);
    if (!leaseFiles) expect(existing, "bootstrap authority is the genuine native host row, not a lease file").toBeUndefined();
    const lease = existing ?? owner.lease;
    writeHostLease(store.root, { ...lease, updatedAt: lease.updatedAt + 1_000, expiresAt: lease.expiresAt + 1_000 });
    await bounded(hinted, "real lease hint reaches MeshBridge before the outbound event");
    expect(f.sync).not.toHaveBeenCalled();
    // Count only the forwarding interval: the hint's own canonical snapshot is expected.
    readdir.mockClear(); readFile.mockClear();
    const started = performance.now(), source = await send(f);
    const first = await bounded(Promise.race([
      delivered.then(sequence => ({ kind: "published", sequence } as const)),
      gate.entered.then(() => ({ kind: "full-membership" } as const)),
    ]), "outbound append without full membership", 1_000);
    expect(first.kind, "a TTL hint must not enqueue a full membership RPC ahead of publish").toBe("published");
    if (first.kind !== "published") return;
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(f.sync).not.toHaveBeenCalled();
    expect(gate.calls).not.toHaveBeenCalled();
    expect(f.requests).not.toContain("presence");
    const membershipPath = (value: unknown, root: string) => {
      const file = String(value);
      return file.startsWith(path.join(root, "participants")) || file.startsWith(path.join(root, "host-leases"));
    };
    expect(readdir.mock.calls.filter(([file]) => membershipPath(file, f.far.root))).toEqual([]);
    expect(readFile.mock.calls.filter(([file]) => membershipPath(file, f.far.root))).toEqual([]);
    // Existing hub presence()/owned()/holds() reads remain canonical, rather than cached away.
    console.log(JSON.stringify({ side, leaseFiles, publishMs: Math.round(performance.now() - started),
      localDirectoryReads: readdir.mock.calls.filter(([file]) => membershipPath(file, f.hub.root)).length,
      localFileReads: readFile.mock.calls.filter(([file]) => membershipPath(file, f.hub.root)).length }));
    assertDelivery(f, first.sequence, source.id);
  });

  it.each([
    { side: "local", leaseFiles: true, mutation: "history" },
    { side: "remote", leaseFiles: true, mutation: "history" },
    { side: "local", leaseFiles: false, mutation: "history" },
    { side: "remote", leaseFiles: false, mutation: "history" },
    { side: "local", leaseFiles: false, mutation: "incarnation" },
    { side: "remote", leaseFiles: false, mutation: "incarnation" },
    { side: "local", leaseFiles: false, mutation: "binding" },
    { side: "remote", leaseFiles: false, mutation: "binding" },
  ] as const)("classifies changed $side lease $mutation as presence before actual publication (bootstrap lease files: $leaseFiles, LH-AUDIT-001)", async ({ side, leaseFiles, mutation }) => {
    const f = await fixture(leaseFiles), gate = gateMembership(f.agent), delivered = observeDelivery(f);
    const notice = deferred<BridgeChange>();
    const subscription = f.bridge.options.remote.subscribeChanges!(kind => {
      if (kind !== "events") notice.resolve(kind);
    }, error => { throw error; });
    cleanups.push(async () => { (await subscription)?.(); });
    const hinted = side === "local" ? f.nextNotice("presence") : notice.promise;
    const store = side === "local" ? f.hub : f.far, owner = side === "local" ? f.lead : f.lane;
    const existing = readHostLease(store.root, owner.id);
    if (!leaseFiles) expect(existing, "a first file must still be compared with consulted bootstrap authority").toBeUndefined();
    const lease = existing ?? owner.lease;
    // Real first-file changes must not hide behind bootstrap equivalence. The remote
    // cases cross StoreBridgeSide -> stdio notice -> RemoteBridgeSide -> MeshBridge;
    // history, incarnation and binding remain authority even without a prior file.
    const changed = mutation === "history" ? { bridgeSupersededTokens: ["retired-incarnation"],
      bridgeSupersededTokenSeen: { "retired-incarnation": 1 } }
      : mutation === "incarnation" ? { incarnationToken: "replacement-incarnation" }
        : { rootId: "session:changed-binding-00000000" };
    writeHostLease(store.root, { ...lease, ...changed,
      updatedAt: lease.updatedAt + 1_000, expiresAt: lease.expiresAt + 1_000 });
    const observed = await bounded<void | BridgeChange>(hinted, "authority-changing source reports presence");
    if (side === "remote") expect(observed).toBe("presence");
    else expect(f.notices).toContain("presence");
    const source = await send(f);
    const first = await bounded(Promise.race([gate.entered.then(() => "membership"), delivered.then(() => "published")]),
      "changed authority forces a full sample before publishing");
    expect(first).toBe("membership");
    expect(f.sync).toHaveBeenCalled();
    expect(f.far.read({ after: 0 }).filter(event => event.topic === "fleet.work.lease-hint-forward")).toEqual([]);
    gate.release();
    assertDelivery(f, await bounded(delivered, "delivery after authority refresh"), source.id);
  });

  it("keeps a TTL-only hint pending for the half-authority deadline after a fast publish", async () => {
    const f = await fixture(), gate = gateMembership(f.agent), delivered = observeDelivery(f);
    const hinted = f.nextNotice("lease"), lease = readHostLease(f.hub.root, f.lead.id)!;
    writeHostLease(f.hub.root, { ...lease, updatedAt: lease.updatedAt + 1_000, expiresAt: lease.expiresAt + 1_000 });
    await bounded(hinted, "TTL hint before the first event");
    const source = await send(f);
    const first = await bounded(Promise.race([
      delivered.then(sequence => ({ kind: "published", sequence } as const)),
      gate.entered.then(() => ({ kind: "membership" } as const)),
    ]), "first event bypasses full membership");
    expect(first.kind).toBe("published");
    if (first.kind !== "published") return;
    assertDelivery(f, first.sequence, source.id);
    expect(f.sync).not.toHaveBeenCalled();
    const secondDelivery = observeDelivery(f);
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 7_501);
    const second = await send(f);
    expect(await bounded(Promise.race([gate.entered.then(() => "membership"), secondDelivery.then(() => "published")]),
      "pending TTL renewals flush at half the capped 15-second authority")).toBe("membership");
    expect(f.sync).toHaveBeenCalled();
    gate.release();
    assertDelivery(f, await bounded(secondDelivery, "delivery after half-authority flush"), second.id);
  });

  it("still refreshes expired sampled authority before publishing", async () => {
    const f = await fixture(), gate = gateMembership(f.agent), delivered = observeDelivery(f);
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 15_100);
    const source = await send(f);
    const first = await bounded(Promise.race([gate.entered.then(() => "membership"), delivered.then(() => "published")]),
      "expiry forces a full sample before publishing");
    expect(first).toBe("membership");
    expect(f.sync).toHaveBeenCalled();
    expect(f.far.read({ after: 0 }).filter(event => event.topic === "fleet.work.lease-hint-forward")).toEqual([]);
    gate.release();
    assertDelivery(f, await bounded(delivered, "delivery after expiry refresh"), source.id);
  });
});

// Observe the real RemoteBridgeSide receiver's decoded notice without replacing transport or
// snapshot logic. The production subscription is already installed by fixture().
const outputNotice = (f: Awaited<ReturnType<typeof fixture>>, resolve: () => void) => {
  const close = f.bridge.options.remote.subscribeChanges!((kind) => { if (kind === "lease") resolve(); }, error => { throw error; });
  cleanups.push(async () => { (await close)?.(); });
};
