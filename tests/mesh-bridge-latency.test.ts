import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY, mirroredLeaseStatus, mirroredOwnerRoutingStatus, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// smarty-dev#6477: slow presence passes must retain spoke attribution within bounded latency,
// without resurrecting source authority whose original translated deadline has passed.
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");

// A fleet Main as deployed: file-only participant written once at start, state host record from
// startup, and a file lease with 15 s TTL last renewed 5 s ago (10 s of life left).
const liveMain = async (store: MeshStore, name: string, now: number) => {
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const startedAt = now - 600_000;
  await store.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files", participants: "files" } });
  await store.put({ key: key("topology/hosts/", identity.id), identity, value: {
    format: 1, livenessLeaseFiles: 1, id: identity.id, rootId: identity.id, identity, startedAt, updatedAt: startedAt, expiresAt: startedAt + 15_000,
  } });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt, updatedAt: now - 5_000, expiresAt: now + 10_000 });
  writeParticipantFile(store.root, { key: key("topology/participants/", identity.id), version: 1, updatedAt: startedAt, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "main", label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt, updatedAt: startedAt, controlProtocol: "v1", livenessLeaseFiles: 1,
  } });
  return identity;
};

describe("bridge presence latency (smarty-dev#6477)", () => {
  // Mixed-release trust boundary: a 6ce-shaped source has no per-host observedAt. A current
  // directory must accept its bounded legacy projection, not require evidence it never supplied.
  it.each([false, true])("keeps legacy projections routable without weakening sampled freshness (sampled=%s)", async sampled => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-compat-"));
    roots.push(root);
    const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lead = await liveMain(hub, "lead", now);
    const lane = await liveMain(far, "lane", now);
    const snapshot = await new StoreBridgeSide(far, "dev1").presence();
    if (!sampled) snapshot.hosts = snapshot.hosts.map(({ observedAt: _observedAt, ...host }) => host);
    const side = new StoreBridgeSide(hub, "ryzen3");
    await side.mirror(snapshot, now);
    const lease = readHostLeases(hub.root).get(lane.id)!;
    const host = hub.get(key("topology/hosts/", lane.id))!.value;
    if (sampled) {
      expect(host).toHaveProperty("mirrorLease", 1);
      expect(lease).toMatchObject({ mirrorLease: 1, mirror: { remoteHost: "ryzen3", observedAt: now } });
    } else {
      expect(host).not.toHaveProperty("mirrorLease");
      expect(lease).not.toHaveProperty("mirrorLease");
      expect(lease).not.toHaveProperty("mirror");
    }
    expect(side.holds(lane.id)).toBe(true);
    const directory = new ParticipantDirectory(hub, { enabled: true, hostId: lead.id, rootId: lead.id, identity: lead });
    const owner = () => directory.mirroredControlOwner(lane.id, lane.id, lane.id)!;
    expect(owner()).toMatchObject({ remoteHost: "ryzen3", expiresAt: now + 10_000 });
    expect(owner()).not.toHaveProperty("bridgeStale");
    expect(mirroredOwnerRoutingStatus(owner(), now)).toBe("live");
    // Absence of a sample is not permission to keep an expired legacy deadline alive.
    // Conversely, a sampled projection still requires fresh evidence; do not relax the reader.
    now += BRIDGE_LEASE_MS;
    expect(side.holds(lane.id)).toBe(false);
    expect(mirroredOwnerRoutingStatus(owner(), now)).toBe("bridge-stale");
    if (sampled) expect(mirroredLeaseStatus("ryzen3", owner().mirror, now)).toBe("bridge-stale");
  });

  it("retains a spoke root's attribution after a slow pass without reviving its expired authority", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
    roots.push(root);
    const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lane = await liveMain(far, "lane", now);
    const spoke = new StoreBridgeSide(far, "dev1");
    const observe = spoke.presence.bind(spoke);
    // The spoke answers with a live snapshot; the hub only commits 11 s later (a slow hub
    // presence read, transport and queueing), past the 10 s the lease had left.
    spoke.presence = async () => {
      const snapshot = await observe();
      now += 11_000;
      return snapshot;
    };
    const input = new PassThrough();
    const output = new PassThrough();
    const serving = serveBridgeAgent(spoke, input, output);
    const remote = new RemoteBridgeSide(output, input);
    closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    const hubSide = new StoreBridgeSide(hub, "ryzen3");
    const requestedAt = now;
    const bridge = new MeshBridge({ localName: "dev1", remoteName: "ryzen3", local: hubSide,
      remote, cursorPath: path.join(root, "cursor.json") });
    await bridge.syncPresence();
    expect(hub.get(key("topology/participants/", lane.id))?.value).toMatchObject({ id: lane.id, kind: "root", remoteHost: "ryzen3" });
    expect(hub.get(key("topology/hosts/", lane.id))?.value).toMatchObject({ id: lane.id, remoteHost: "ryzen3" });
    // Attribution survives (#6477/#2004), but the commit cannot add its latency to authority.
    const lease = readHostLeases(hub.root).get(lane.id)!;
    expect(lease.expiresAt).toBe(requestedAt + 10_000);
    expect(lease.expiresAt).toBeLessThan(now);
    expect(lease.expiresAt).toBeLessThanOrEqual(now + BRIDGE_LEASE_MS);
    // Source evidence remains fresh (#10068); that is not permission to extend its deadline.
    expect(hubSide.holds(lane.id)).toBe(false);
    expect(lease.mirror?.observedAt).toBe(requestedAt);
    expect(mirroredLeaseStatus("ryzen3", lease.mirror, now)).toBe("live");
    expect(mirroredOwnerRoutingStatus({ ...lease, remoteHost: "ryzen3" }, now)).toBe("bridge-stale");
  });

  it("still refuses a host whose lease had already lapsed when the hub asked", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
    roots.push(root);
    const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lane = await liveMain(far, "lane", now);
    now += 11_000;
    // The source itself no longer offers a lease that lapsed on its own clock.
    expect((await new StoreBridgeSide(far, "dev1", () => now).presence()).hosts).toHaveLength(0);
    // A peer that claims it anyway (an older or lying peer). With a source sample the lapse is
    // judged on the SOURCE clock, whatever the hub clock or skew (#10068); a legacy peer without
    // one is judged when the hub asked, as before (#6477).
    const claimed = await new StoreBridgeSide(far, "dev1", () => now - 11_000).presence();
    expect(claimed.hosts).toHaveLength(1);
    const sourceLapsed = claimed.hosts.map(host => ({ ...host, observedAt: host.expiresAt + 1_000 }));
    const legacy = claimed.hosts.map(({ observedAt: _sampledAt, ...host }) => host);
    for (const [name, hosts] of [["source-lapsed", sourceLapsed], ["legacy", legacy]] as const) {
      const store = new MeshStore(path.join(root, `hub-${name}`), 64 * 1024, 100);
      const hubSide = new StoreBridgeSide(store, "ryzen3");
      await hubSide.mirror({ ...claimed, hosts }, now);
      expect(store.get(key("topology/hosts/", lane.id)), name).toBeUndefined();
      expect(store.get(key("topology/participants/", lane.id)), name).toBeUndefined();
      expect(readHostLeases(store.root).has(lane.id), name).toBe(false);
      expect(hubSide.holds(lane.id), name).toBe(false);
    }
    expect(hub.get(key("topology/hosts/", lane.id))).toBeUndefined();
  });

  // Independent review P2: an uncapped delay kept a dead origin alive 60 s past its expiry.
  // Preserve the bounded ceiling, and require the stronger original-deadline trust boundary.
  it("keeps a dead origin's mirror at most one BRIDGE_LEASE_MS past it, even after a 60 s stall", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
    roots.push(root);
    const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lane = await liveMain(far, "lane", now);
    // The origin dies right after its snapshot: its lease file is never renewed again.
    const originExpiry = readHostLeases(far.root).get(lane.id)!.expiresAt;
    const spoke = new StoreBridgeSide(far, "dev1");
    const observe = spoke.presence.bind(spoke);
    spoke.presence = async () => {
      const snapshot = await observe();
      now += 60_000;
      return snapshot;
    };
    const input = new PassThrough();
    const output = new PassThrough();
    const serving = serveBridgeAgent(spoke, input, output);
    const remote = new RemoteBridgeSide(output, input);
    closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    const hubSide = new StoreBridgeSide(hub, "ryzen3");
    const bridge = new MeshBridge({ localName: "dev1", remoteName: "ryzen3", local: hubSide,
      remote, cursorPath: path.join(root, "cursor.json") });
    await bridge.syncPresence();
    // Uncapped, the mirror lived until now + 10 s, i.e. 60 s past the origin's own expiry.
    expect(originExpiry + BRIDGE_LEASE_MS).toBeLessThan(now);
    const lease = readHostLeases(hub.root).get(lane.id);
    expect(lease === undefined || lease.expiresAt === originExpiry).toBe(true);
    expect(lease === undefined || lease.expiresAt <= originExpiry + BRIDGE_LEASE_MS).toBe(true);
    // #10068 may retain the root for a precise bridge-stale routing error, but grants it no
    // authority: its lease lapsed before the commit and the stalled sample is bridge-stale.
    expect(lease === undefined || lease.expiresAt <= now).toBe(true);
    expect(hubSide.holds(lane.id)).toBe(false);
    if (lease) expect(mirroredLeaseStatus("ryzen3", lease.mirror, now)).toBe("bridge-stale");
  });

  it("keeps slow-pass authority at the original deadline within the #6477 bounded latency ceiling", async () => {
    for (const stall of [11_000, 20_000, 30_000]) {
      let now = 1_800_000_000_000;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
      roots.push(root);
      const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
      const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
      const lane = await liveMain(far, "lane", now);
      const originExpiry = readHostLeases(far.root).get(lane.id)!.expiresAt;
      const observedAt = now;
      const snapshot = await new StoreBridgeSide(far, "dev1").presence();
      now += stall;
      const hubSide = new StoreBridgeSide(hub, "ryzen3");
      await hubSide.mirror(snapshot, observedAt);
      const lease = readHostLeases(hub.root).get(lane.id);
      const until = originExpiry;
      // A sampled mirror retains attribution (#2004), never expired authority.
      expect(lease?.expiresAt, `stall ${stall}`).toBe(until);
      // Authority also needs a fresh sample: a stall of one BRIDGE_LEASE_MS or more is bridge-stale.
      const fresh = now - observedAt < BRIDGE_LEASE_MS;
      expect(hubSide.holds(lane.id), `stall ${stall}`).toBe(until > now && fresh);
      if (lease) {
        expect(mirroredLeaseStatus("ryzen3", lease.mirror, now), `stall ${stall}`).toBe(fresh ? "live" : "bridge-stale");
        expect(mirroredOwnerRoutingStatus({ ...lease, remoteHost: "ryzen3" }, now), `stall ${stall}`).toBe("bridge-stale");
      }
      expect(lease === undefined || lease.expiresAt - originExpiry <= BRIDGE_LEASE_MS, `stall ${stall}`).toBe(true);
      vi.restoreAllMocks();
    }
  });

  // Recorded Astra catch / a6d1154f: moving the deadline after the write-lock wait resurrected
  // expired remote authority. Keep this unit at the trust boundary: cold recovery and warm early
  // plans (#10391) may commit retained attribution, never an expired lease AS LIVE.
  it.each([
    ["cold", 1_000], ["warm", 1_000], ["cold", 16_000], ["warm", 16_000],
  ] as const)("never commits expired authority as live after a blocked state write (%s, %i ms wait)", async (path_, wait) => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
    roots.push(root);
    const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lane = await liveMain(far, "lane", now);
    const hubSide = new StoreBridgeSide(hub, "ryzen3", () => now);
    // Warm: one committed pass ends recovery, so the next plans early, before the write lock.
    if (path_ === "warm") await hubSide.mirror({ hosts: [], participants: [] }, now);
    // 500 ms of source-clock life remaining when sampled at t0.
    const lease0 = readHostLeases(far.root).get(lane.id)!;
    writeHostLease(far.root, { ...lease0, expiresAt: now + 500 });
    const t0 = now;
    const snapshot = await new StoreBridgeSide(far, "dev1", () => now).presence();
    const write = hub.writeBatch.bind(hub);
    const batches = vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      now += wait; // the write lock is held elsewhere for `wait` ms; no real sleep
      return write(input);
    });
    const stateViews = vi.spyOn(hub, "stateView");
    await hubSide.mirror(snapshot, t0);
    expect(batches).toHaveBeenCalledTimes(1);
    expect(stateViews).toHaveBeenCalledTimes(path_ === "warm" ? 1 : 0);
    const until = t0 + 500;
    const lease = readHostLeases(hub.root).get(lane.id)!;
    expect(lease.expiresAt).toBe(until);
    expect(hub.get(key("topology/hosts/", lane.id))?.value).toMatchObject({ expiresAt: until, remoteHost: "ryzen3" });
    expect(lease.mirror?.observedAt).toBe(t0);
    const fresh = wait < BRIDGE_LEASE_MS;
    expect(hubSide.holds(lane.id)).toBe(false);
    expect(mirroredLeaseStatus("ryzen3", lease.mirror, now)).toBe(fresh ? "live" : "bridge-stale");
    expect(mirroredOwnerRoutingStatus({ ...lease, remoteHost: "ryzen3" }, now)).toBe("bridge-stale");
    expect(until).toBeLessThanOrEqual(now);
  });

  // Independent review P2: observedAt was used unchecked. NaN admitted every lapsed host (and wrote
  // a NaN lease); -Infinity resurrected one for a full BRIDGE_LEASE_MS. Non-finite counts as now.
  it.each([Number.NaN, Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY])("treats a non-finite observedAt (%s) as now", async (observedAt) => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
    roots.push(root);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lane = await liveMain(far, "lane", now);
    const originExpiry = readHostLeases(far.root).get(lane.id)!.expiresAt;
    // A live host: mirrored with the origin's own expiry (no pass-delay shift).
    const live = new MeshStore(path.join(root, "hub-live"), 64 * 1024, 100);
    await new StoreBridgeSide(live, "ryzen3").mirror(await new StoreBridgeSide(far, "dev1").presence(), observedAt);
    expect(readHostLeases(live.root).get(lane.id)?.expiresAt).toBe(originExpiry);
    expect(live.get(key("topology/participants/", lane.id))?.value).toMatchObject({ id: lane.id, remoteHost: "ryzen3" });
    // A host whose lease lapsed before this pass: refused, as with observedAt = now.
    const lapsed = new MeshStore(path.join(root, "hub-lapsed"), 64 * 1024, 100);
    expect(readHostLeases(live.root).get(lane.id)?.mirror?.observedAt).toBe(now);
    const claimed = await new StoreBridgeSide(far, "dev1").presence();
    now += 11_000;
    // Lapsed on the hub clock (a legacy peer, no sample) or on the source clock (sampled): the
    // non-finite observedAt admits neither, and source time never meets the hub clock (#10068).
    const legacy = claimed.hosts.map(({ observedAt: _sampledAt, ...host }) => host);
    const sourceLapsed = claimed.hosts.map(host => ({ ...host, observedAt: host.expiresAt + 1_000 }));
    await new StoreBridgeSide(lapsed, "ryzen3").mirror({ ...claimed, hosts: legacy }, observedAt);
    await new StoreBridgeSide(lapsed, "ryzen3").mirror({ ...claimed, hosts: sourceLapsed }, observedAt);
    expect(lapsed.get(key("topology/hosts/", lane.id))).toBeUndefined();
    expect(lapsed.get(key("topology/participants/", lane.id))).toBeUndefined();
    expect(readHostLeases(lapsed.root).has(lane.id)).toBe(false);
  });
});
