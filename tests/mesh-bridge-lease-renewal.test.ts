// Keep reason: source-deadline and owner/version trust boundary for lease-only watch refresh.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeMirrorDeferredError, StoreBridgeSide, type BridgeHost } from "../src/mesh/bridge.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { NatsCarrierSide } from "../src/mesh/bridge-nats-file.js";
import type { BridgeNatsPort } from "../src/mesh/bridge-nats-types.js";
import { MeshStore } from "../src/mesh/store.js";
import { hostLeasePath, mirroredOwnerRoutingStatus, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { FakePort, MemoryLedger } from "./helpers/bridge-nats-l2-fakes.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const ROOT = "session:11111111-1111-4111-8111-111111111111";
const LINK = "test-link";
const identity = { id: ROOT, name: "remote-root", kind: "main" as const };
const key = (prefix: string) => prefix + createHash("sha256").update(ROOT).digest("hex");
const closers: Array<() => void> = [];
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); for (const close of closers.splice(0)) close(); });
const barrier = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const turn = () => new Promise<void>(resolve => setImmediate(resolve));

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

  it("skips overlapping lease-only ticks instead of queuing behind the same process's held gate", async () => {
    const f = await fixture();
    const entered = barrier(), release = barrier();
    const custody = f.store.leaseCustody.bind(f.store);
    let starts = 0, active = 0, maximum = 0;
    vi.spyOn(f.store, "leaseCustody").mockImplementation((file, operation, ...args) => custody(file, async () => {
      starts++;
      maximum = Math.max(maximum, ++active);
      try { entered.resolve(); await release.promise; return await operation(); }
      finally { active--; }
    }, ...args));
    f.advance(1_000);
    const first = f.side.renewMirroredLeases([f.host(13_000)], f.now());
    await entered.promise;
    const ticks: Promise<void>[] = [];
    try {
      for (let tick = 0; tick < 4; tick++) {
        f.advance(1_000);
        ticks.push(f.side.renewMirroredLeases([f.host(13_000)], f.now()));
        expect(f.routing()).toBe("live");
      }
      release.resolve();
      await Promise.all([first, ...ticks]);
      expect({ starts, maximum, active }).toEqual({ starts: 1, maximum: 1, active: 0 });
      f.advance(3_000); // Past the original 7 s expiry, but before the confirmed renewal's deadline.
      expect(f.routing()).toBe("live");
      expect(f.side.holds(ROOT)).toBe(true);
    } finally { release.resolve(); await Promise.allSettled([first, ...ticks]); }
  });

  it("a full mirror waits for the active watch renewal before taking the same host custody", async () => {
    const f = await fixture();
    const entered = barrier(), release = barrier();
    const custody = f.store.leaseCustody.bind(f.store);
    let held = false, overlapping = 0;
    vi.spyOn(f.store, "leaseCustody").mockImplementation((file, operation, ...args) => {
      if (file !== hostLeasePath(f.store.root, ROOT)) return custody(file, operation, ...args);
      if (held) overlapping++;
      return custody(file, async () => {
        held = true;
        try { entered.resolve(); await release.promise; return await operation(); }
        finally { held = false; }
      }, ...args);
    });
    f.advance(1_000);
    const renewing = f.side.renewMirroredLeases([f.host(13_000)], f.now());
    await entered.promise;
    f.advance(1_000);
    let settled = false;
    const mirroring = f.side.mirror({ hosts: [f.host(13_000)], participants: [] }, f.now())
      .finally(() => { settled = true; });
    void mirroring.catch(() => undefined);
    try {
      await turn();
      expect(overlapping).toBe(0);
      expect(settled).toBe(false);
      f.advance(3_000);
      expect(f.routing()).toBe("live");
      release.resolve();
      await renewing;
      await mirroring;
      f.advance(3_000);
      expect(f.routing()).toBe("live");
    } finally { release.resolve(); await Promise.allSettled([renewing, mirroring]); }
  });

  it("defers rather than queues a watch tick while a full mirror holds custody", async () => {
    const f = await fixture();
    const entered = barrier(), release = barrier();
    const write = f.store.writeBatch.bind(f.store);
    vi.spyOn(f.store, "writeBatch").mockImplementation(async (...args) => {
      entered.resolve(); await release.promise; return write(...args);
    });
    f.advance(1_000);
    const mirroring = f.side.mirror({ hosts: [f.host(13_000)], participants: [] }, f.now());
    await entered.promise;
    try {
      const custody = vi.spyOn(f.store, "leaseCustody");
      await expect(f.side.renewMirroredLeases([f.host(13_000)], f.now())).rejects.toBeInstanceOf(BridgeMirrorDeferredError);
      expect(custody).not.toHaveBeenCalled();
    } finally { release.resolve(); await mirroring; }
  });

  it("retries a deferred NATS watch renewal before expiry without another watch event", async () => {
    const f = await fixture();
    vi.useFakeTimers({ now: f.now(), toFake: ["Date", "setTimeout", "clearTimeout"] });
    const port: BridgeNatsPort = new FakePort();
    let changed!: () => void;
    vi.spyOn(port, "watchPresence").mockImplementation(listener => { changed = listener; return () => undefined; });
    vi.spyOn(port, "presence").mockImplementation(async () => ({ hosts: [f.host(13_000)], participants: [], reserved: [] }));
    const logs: string[] = [], failures: Error[] = [];
    const carrier = new NatsCarrierSide({ cursorPath: path.join(f.store.root, "cursor.json"), port, file: f.side, link: LINK,
      ledger: new MemoryLedger(), authorize: () => ({ ok: true, principal: "carry" }),
      authorizeTopic: () => ({ ok: true, principal: "none" }), now: f.now, log: line => logs.push(line) });
    const stop = carrier.subscribeChanges(() => undefined, error => failures.push(error));
    const custody = vi.spyOn(f.store, "leaseCustody");
    custody.mockRejectedValueOnce(new MeshLockTimeoutError(" test renewal gate", 1, 0));
    try {
      f.advance(5_000);
      changed();
      await turn();
      expect(logs.some(line => line.startsWith("NATS file mirror renewal deferred:"))).toBe(true);
      expect(f.lease().mirror?.observedAt).toBe(f.now() - 5_000);
      f.advance(999);
      await vi.advanceTimersByTimeAsync(999);
      expect(custody).toHaveBeenCalledTimes(1);
      expect(f.routing()).toBe("live");
      f.advance(1);
      await vi.advanceTimersByTimeAsync(1);
      await turn();
      expect(f.lease().mirror?.observedAt).toBe(f.now());
      expect(f.lease().expiresAt).toBe(f.now() + 13_000);
      f.advance(2_000);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(f.routing()).toBe("live");
      expect(failures).toEqual([]);
    } finally { stop(); await carrier.fence(); carrier.close(); await turn(); }
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
