// Keep reason: source-deadline and owner/version trust boundary for lease-only watch refresh.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type BridgeHost } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";
import { mirroredOwnerRoutingStatus, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const ROOT = "session:11111111-1111-4111-8111-111111111111";
const LINK = "test-link";
const identity = { id: ROOT, name: "remote-root", kind: "main" as const };
const key = (prefix: string) => prefix + createHash("sha256").update(ROOT).digest("hex");
const closers: Array<() => void> = [];
afterEach(() => { vi.restoreAllMocks(); for (const close of closers.splice(0)) close(); });

const fixture = async () => {
  let now = 1_800_000_000_000;
  const startedAt = now - 60_000;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lease-renewal-"));
  const store = new MeshStore(dir, 64 * 1024, 100);
  closers.push(() => { store.closeState(); fs.rmSync(dir, { recursive: true, force: true }); });
  const side = new StoreBridgeSide(store, LINK, () => now);
  const host = (remaining = 7_000, incarnationToken = "owner-1"): BridgeHost => ({ record: {
    format: 1, id: ROOT, rootId: ROOT, identity, incarnationToken, startedAt,
    updatedAt: now, expiresAt: now + remaining,
  }, expiresAt: now + remaining, observedAt: now });
  await side.mirror({ hosts: [host()], participants: [{
    format: 1, id: ROOT, kind: "root", rootId: ROOT, ownerHostId: ROOT, ownerIdentityId: ROOT, ownerIncarnation: "owner-1",
    name: identity.name, status: "idle", runner: "pi", transport: "host", capabilities: ["followUp"], startedAt, updatedAt: now, controlProtocol: "v1",
  }] }, now);
  const lease = () => readHostLease(dir, ROOT)!;
  const routing = () => mirroredOwnerRoutingStatus({ ...lease(), remoteHost: LINK }, now);
  return { store, side, host, lease, routing, now: () => now, advance: (ms: number) => { now += ms; } };
};

describe("lease-only mirror renewal", () => {
  it("updates both freshness and source deadline without any membership/state write, then expires", async () => {
    const f = await fixture();
    const hostRow = f.store.get(key("topology/hosts/"), { fresh: true });
    const participant = f.store.get(key("topology/participants/"), { fresh: true });
    const batch = vi.spyOn(f.store, "writeBatch");
    for (let tick = 0; tick < 4; tick++) {
      f.advance(5_000);
      await f.side.renewMirroredLeases([f.host(13_000)], f.now());
      expect(f.routing()).toBe("live");
      expect(f.side.holds(ROOT)).toBe(true);
      expect(f.lease().mirror?.observedAt).toBe(f.now());
      expect(f.lease().expiresAt).toBe(f.now() + 13_000);
    }
    expect(batch).not.toHaveBeenCalled();
    expect(f.store.get(key("topology/hosts/"), { fresh: true })).toEqual(hostRow);
    expect(f.store.get(key("topology/participants/"), { fresh: true })).toEqual(participant);
    f.advance(13_000);
    expect(f.routing()).toBe("bridge-stale");
    expect(f.side.holds(ROOT)).toBe(false);
  });

  it.each(["expired-source", "expired-translation", "missing-sample", "future-observation", "nonfinite-observation"] as const)(
    "never renews from an unusable sample (%s)", async bad => {
      const f = await fixture();
      const prior = f.lease();
      const source = f.host(500);
      let observedAt = f.now();
      if (bad === "expired-source") source.observedAt = source.expiresAt;
      if (bad === "expired-translation") f.advance(501);
      if (bad === "missing-sample") delete source.observedAt;
      if (bad === "future-observation") observedAt += 1;
      if (bad === "nonfinite-observation") observedAt = Number.NaN;
      await f.side.renewMirroredLeases([source], observedAt);
      expect(f.lease()).toEqual(prior);
    });

  it("cannot install a missing root or change its incarnation, even from a live source", async () => {
    const f = await fixture();
    const prior = f.lease();
    f.advance(1_000);
    await f.side.renewMirroredLeases([f.host(13_000, "owner-2")], f.now());
    expect(f.lease()).toEqual(prior);
    await f.side.mirror({ hosts: [], participants: [] }, f.now());
    await f.side.renewMirroredLeases([f.host(13_000)], f.now());
    expect(readHostLease(f.store.root, ROOT)).toBeUndefined();
    expect(f.store.get(key("topology/hosts/"), { fresh: true })).toBeUndefined();
  });

  it("does not overwrite a native participant claim or native lease authority", async () => {
    const f = await fixture();
    f.advance(1_000);
    const prior = f.lease();
    writeParticipantFile(f.store.root, { key: key("topology/participants/"), version: 1, updatedAt: f.now(), updatedBy: identity,
      value: { format: 1, id: ROOT, kind: "root", rootId: ROOT, ownerHostId: ROOT, ownerIdentityId: ROOT,
        ownerIncarnation: "native", name: "native-root", status: "idle", runner: "pi", transport: "host", capabilities: [],
        startedAt: f.now(), updatedAt: f.now(), controlProtocol: "v1" } });
    await f.side.renewMirroredLeases([f.host(13_000)], f.now());
    expect(f.lease()).toEqual(prior);
    const g = await fixture();
    const native = { ...g.lease(), incarnationToken: "native", updatedAt: g.now(), expiresAt: g.now() + 60_000,
      session: { id: ROOT, startedAt: g.now(), updatedAt: g.now(), expiresAt: g.now() + 15_000 } };
    writeHostLease(g.store.root, native);
    await g.side.renewMirroredLeases([g.host(13_000)], g.now());
    expect(g.lease()).toEqual(native);
  });

  it("does not renew after a fence, including a renewal queued just before the fence", async () => {
    const f = await fixture();
    const prior = f.lease();
    f.advance(1_000);
    const pending = f.side.renewMirroredLeases([f.host(13_000)], f.now());
    await f.side.fence();
    await pending;
    await f.side.renewMirroredLeases([f.host(13_000)], f.now());
    expect(f.lease()).toEqual(prior);
  });
});
