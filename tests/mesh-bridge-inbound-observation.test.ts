import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, BridgeOwnershipError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease } from "../src/topology/host-leases.js";

// Keep reason: data-safety boundary and #13645 incident. Exercise real stream RPC,
// two private stores, mirror lease files and the held inbound publication fence.
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const native = async (store: MeshStore, name: string) => {
  const now = Date.now();
  const id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name, kind: "main", sessionId: name };
  await store.put({ key: key("hosts", id), identity,
    value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000 } });
  await store.put({ key: key("participants", id), identity,
    value: { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
      name, label: name, status: "idle", runner: "pi", transport: "host", capabilities: [],
      startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return identity;
};
const setup = async () => {
  let wall = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => wall);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-inbound-observation-"));
  cleanups.push(() => fs.rmSync(root, { force: true, recursive: true }));
  const hub = new MeshStore(path.join(root, "hub"), 65536, 256);
  const far = new MeshStore(path.join(root, "far"), 65536, 256);
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const lead = await native(hub, "lead"), lane = await native(far, "lane");
  const local = new StoreBridgeSide(hub, "far"), agent = new StoreBridgeSide(far, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  cleanups.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  await remote.hello();
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(root, "cursor.json"), stopMs: 100 });
  cleanups.push(() => bridge.stop());
  await bridge.start();
  const publication = () => local.publishWithReceipt({ topic: "fabric.control.ack", kind: "accepted",
    from: lane, to: lead.id, data: { version: 1, commandId: "inbound", targetId: lane.id, accepted: true,
      bridge: { from: "far", id: "source-ack" } } }, [lane.id]);
  return { hub, local, agent, bridge, lane, publication, now: () => wall, advance: (ms: number) => { wall += ms; } };
};

describe("inbound source observation excludes hub work before the presence RPC", () => {
  it("can publish under the fresh source lease after a slow synchronous hub scan", async () => {
    const f = await setup();
    const presence = f.local.presence.bind(f.local);
    // StoreBridgeSide.presence has a synchronous filesystem scan before returning its
    // Promise. Inject 20 s there, without sleeping or weakening either store's fence.
    vi.spyOn(f.local, "presence").mockImplementationOnce(() => {
      f.advance(20_000);
      return presence();
    });
    await f.bridge.syncPresence();
    // Baseline fails on the real publication fence, not just a timestamp assertion.
    const receipt = await f.publication();
    const lease = readHostLease(f.hub.root, f.lane.id)!;
    expect(lease.mirror?.sourceObservedAt).toBe(f.now());
    expect(lease.mirror?.observedAt).toBe(f.now());
    expect(lease.expiresAt).toBe(f.now() + BRIDGE_LEASE_MS);
    const delivered = f.hub.read({ after: receipt.sequence - 1, limit: 1 });
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toMatchObject({ topic: "fabric.control.ack", from: { id: f.lane.id },
      data: { commandId: "inbound", bridge: { from: "far", id: "source-ack" } } });
  });

  it.each(["reply", "commit"] as const)("still refuses a source sample aged by a slow %s after the request", async (phase) => {
    const f = await setup();
    const sampledAt = f.now();
    if (phase === "reply") {
      const presence = f.agent.presence.bind(f.agent);
      vi.spyOn(f.agent, "presence").mockImplementationOnce(async () => {
        const sampled = await presence();
        f.advance(20_000);
        return sampled;
      });
    } else {
      const mirror = f.local.mirror.bind(f.local);
      vi.spyOn(f.local, "mirror").mockImplementationOnce((...args) => {
        f.advance(20_000);
        return mirror(...args);
      });
    }
    await f.bridge.syncPresence();
    const lease = readHostLease(f.hub.root, f.lane.id)!;
    expect(lease.mirror?.observedAt).toBe(sampledAt);
    expect(lease.expiresAt).toBeLessThanOrEqual(f.now());
    await expect(f.publication()).rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(f.hub.read().filter(event => event.topic === "fabric.control.ack")).toEqual([]);
  });
});
