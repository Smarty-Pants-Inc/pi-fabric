import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#6477: a spoke's live Main (15 s file lease, renewed every 5 s) was never mirrored
// on the hub when one presence pass took longer than the lease's remaining life.
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
  it("mirrors a spoke root observed live even when the pass outlasts its remaining lease", async () => {
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
    const bridge = new MeshBridge({ localName: "dev1", remoteName: "ryzen3", local: new StoreBridgeSide(hub, "ryzen3"),
      remote, cursorPath: path.join(root, "cursor.json") });
    await bridge.syncPresence();
    expect(hub.get(key("topology/participants/", lane.id))?.value).toMatchObject({ id: lane.id, kind: "root", remoteHost: "ryzen3" });
    expect(hub.get(key("topology/hosts/", lane.id))?.value).toMatchObject({ id: lane.id, remoteHost: "ryzen3" });
    // The mirror lives one bounded TTL from the hub's commit, never past it.
    const lease = readHostLeases(hub.root).get(lane.id)!;
    expect(lease.expiresAt).toBeGreaterThan(now);
    expect(lease.expiresAt).toBeLessThanOrEqual(now + 15_000);
  });

  it("still refuses a host whose lease had already lapsed when the hub asked", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-latency-"));
    roots.push(root);
    const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
    const far = new MeshStore(path.join(root, "spoke"), 64 * 1024, 100);
    const lane = await liveMain(far, "lane", now);
    const hubSide = new StoreBridgeSide(hub, "ryzen3");
    now += 11_000;
    // A spoke that claims the lapsed lease anyway (an older or lying peer).
    const claimed = await new StoreBridgeSide(far, "dev1", () => now - 11_000).presence();
    expect(claimed.hosts).toHaveLength(1);
    await hubSide.mirror(claimed, now);
    expect(hub.get(key("topology/hosts/", lane.id))).toBeUndefined();
    expect(hub.get(key("topology/participants/", lane.id))).toBeUndefined();
  });

  // Independent review P2: the pass-delay shift was uncapped, so a pass that stalled 60 s kept a
  // dead origin's mirror alive 60 s past the origin. The shift is capped at one BRIDGE_LEASE_MS.
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
    const bridge = new MeshBridge({ localName: "dev1", remoteName: "ryzen3", local: new StoreBridgeSide(hub, "ryzen3"),
      remote, cursorPath: path.join(root, "cursor.json") });
    await bridge.syncPresence();
    // Uncapped, the mirror lived until now + 10 s, i.e. 60 s past the origin's own expiry.
    expect(originExpiry + BRIDGE_LEASE_MS).toBeLessThan(now);
    const lease = readHostLeases(hub.root).get(lane.id);
    expect(lease === undefined || lease.expiresAt <= originExpiry + BRIDGE_LEASE_MS).toBe(true);
    expect(hub.get(key("topology/hosts/", lane.id))).toBeUndefined();
    expect(hub.get(key("topology/participants/", lane.id))).toBeUndefined();
  });

  it("bounds the shift of a slow pass's still-live mirror by one BRIDGE_LEASE_MS past its origin", async () => {
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
      await new StoreBridgeSide(hub, "ryzen3").mirror(snapshot, observedAt);
      const lease = readHostLeases(hub.root).get(lane.id);
      const until = Math.min(originExpiry + Math.min(stall, BRIDGE_LEASE_MS), now + BRIDGE_LEASE_MS);
      if (until > now) expect(lease?.expiresAt, `stall ${stall}`).toBe(until);
      else expect(lease, `stall ${stall}`).toBeUndefined();
      expect(lease === undefined || lease.expiresAt - originExpiry <= BRIDGE_LEASE_MS, `stall ${stall}`).toBe(true);
      vi.restoreAllMocks();
    }
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
    const claimed = await new StoreBridgeSide(far, "dev1").presence();
    now += 11_000;
    await new StoreBridgeSide(lapsed, "ryzen3").mirror(claimed, observedAt);
    expect(lapsed.get(key("topology/hosts/", lane.id))).toBeUndefined();
    expect(lapsed.get(key("topology/participants/", lane.id))).toBeUndefined();
    expect(readHostLeases(lapsed.root).has(lane.id)).toBe(false);
  });
});
