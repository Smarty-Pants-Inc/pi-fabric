// expected-skip: no NATS_BIN/PATH executable: an authenticated loopback broker is required.
// Keep reason: source-lease trust boundary and smarty-dev#16876's omission/re-import regression.
// Real KV watches, file custody/leases, directory routing and NATS delivery; deterministic barriers
// hold outbound work or the receiver before custody without granting any source authority.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { createNatsFileLink } from "../src/mesh/bridge-nats-file.js";
import { openNatsBridgeSide } from "../src/mesh/bridge-nats-port.js";
import { natsLeaseKey, natsPresenceKey } from "../src/mesh/bridge-nats-presence.js";
import { MeshStore } from "../src/mesh/store.js";
import { mirroredOwnerRoutingStatus, readHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsTransport } from "../src/transport/nats/index.js";
import { MemoryLedger } from "./helpers/bridge-nats-l2-fakes.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: mirror renewal needs NATS_BIN");
const FILE = "session:11111111-1111-4111-8111-111111111111";
const POD = "session:22222222-2222-4222-8222-222222222222";
const LINK = "test-link";
const identity = { id: FILE, name: "file-root", kind: "main" as const };
const stateKey = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const STARTED_AT = Date.now() - 10_000;
const barrier = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const rootRecord = (id: string, incarnation: string): FabricParticipantRecord => ({
  format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
  name: id === FILE ? "file-root" : "remote-root", status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: STARTED_AT, updatedAt: Date.now(),
});
let broker: PathBHarness;
beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
}, 30_000);
afterAll(async () => { await broker?.stop(); });

const fixture = async (options: { subscribe?: boolean } = {}) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-renewal-"));
  const store = new MeshStore(path.join(dir, "mesh"), 256 * 1024, 1_000);
  const pod = await openNatsTransport({ ...broker.runtimeOptions(broker.users.machineB), hostId: POD });
  const leaseKey = natsLeaseKey("machine-b", POD);
  const presenceKey = natsPresenceKey("machine-b", POD);
  let leaseRevision = (await pod.kv("leases").get(leaseKey))?.revision ?? 0;
  let presenceRevision = (await pod.kv("presence").get(presenceKey))?.revision ?? 0;
  const renew = async (incarnation = "pod-1", ageMs = 0, rootRecheck = false) => {
    const updatedAt = Date.now() - ageMs;
    const owner = `${POD}#${incarnation}`;
    leaseRevision = await pod.kv("leases").put(leaseKey, owner, {
      id: POD, rootId: POD, identityId: POD, incarnation, incarnationToken: incarnation,
      pid: process.pid, pidStartTime: "1", bootId: "12345678-1234-4234-8234-123456789abc",
      updatedAt, expiresAt: updatedAt + 15_000, ...(rootRecheck ? { rootRecheck: true } : {}),
    }, leaseRevision);
    return updatedAt;
  };
  const publishRoot = async (incarnation = "pod-1") => {
    presenceRevision = await pod.kv("presence").put(presenceKey, `${POD}#${incarnation}`, rootRecord(POD, incarnation), presenceRevision);
  };
  await renew();
  await publishRoot();
  const runtime = broker.runtimeOptions(broker.users.machineA);
  const port = await openNatsBridgeSide({ link: LINK,
    nats: { servers: [String(runtime.servers)], machine: "machine-a", credentials: runtime.credentials,
      caFile: broker.tls!.caFile, ...PATH_B_PERMISSION_POLICY },
    allow: { file: [{ kind: "root", id: FILE }], nats: [{ kind: "root", id: POD, machine: "machine-b" }] },
  });
  const now = Date.now();
  await store.put({ key: stateKey("topology/hosts/", FILE), identity, value: {
    format: 1, id: FILE, rootId: FILE, identity, incarnationToken: "file-1", startedAt: now - 1_000,
    updatedAt: now, expiresAt: now + 60_000,
  } });
  await store.put({ key: stateKey("topology/participants/", FILE), identity, value: rootRecord(FILE, "file-1") });
  let receiverAgeMs = 0;
  const link = createNatsFileLink({ store, port, localName: "file", link: LINK, cursorPath: path.join(dir, "cursor.json"),
    now: () => Date.now() + receiverAgeMs,
    ledger: new MemoryLedger(), authorize: () => ({ ok: true, principal: "carry" }),
    authorizeTopic: () => ({ ok: true, principal: "none" }), mirrorAllowed: () => true,
  });
  const initialUpdate = await renew("pod-1", 6_000); // About 7 s remains after the safety margin.
  await vi.waitFor(() => expect(port.kv.reader.lease("machine-b", POD)?.entry.value.updatedAt).toBe(initialUpdate));
  await link.bridge.start();
  const failures: Error[] = [];
  // Subscribe exactly as MeshBridge.run does, but leave full resampling/draining under test control.
  let unwatch: (() => void) | undefined;
  const subscribe = () => { unwatch = link.remote.subscribeChanges(() => undefined, error => failures.push(error)); };
  if (options.subscribe !== false) subscribe();
  const reader = new ParticipantDirectory(store, { enabled: true, hostId: "reader", rootId: "reader",
    identity: { id: "reader", name: "reader", kind: "main" }, reapDeadHosts: false });
  const listed = () => reader.list({ scope: "project", kinds: ["root"], fresh: true }).find(row => row.id === POD);
  const lease = () => readHostLease(store.root, POD)!;
  const status = () => mirroredOwnerRoutingStatus({ ...lease(), remoteHost: LINK });
  const close = async () => {
    unwatch?.();
    await link.bridge.stop();
    await port.close();
    await pod.kv("leases").delete(leaseKey, leaseRevision);
    await pod.kv("presence").delete(presenceKey, presenceRevision);
    await pod.close();
    store.closeState();
    fs.rmSync(dir, { recursive: true, force: true });
  };
  return { store, pod, port, link, renew, publishRoot, listed, lease, status, failures, close, subscribe,
    ageReceiverBy: (ms: number) => { receiverAgeMs += ms; },
    deleteRoot: () => pod.kv("presence").delete(presenceKey, presenceRevision),
  };
};

type Fixture = Awaited<ReturnType<typeof fixture>>;
const hostRow = (f: Fixture) => f.store.get(stateKey("topology/hosts/", POD), { fresh: true });
const participantRow = (f: Fixture) => f.store.get(stateKey("topology/participants/", POD), { fresh: true });
const expectWithdrawn = (f: Fixture) => {
  expect(hostRow(f)).toBeUndefined();
  expect(participantRow(f)).toBeUndefined();
  expect(readHostLease(f.store.root, POD)).toBeUndefined();
  expect(f.listed()).toBeUndefined();
  expect(f.link.local.holds(POD)).toBe(false);
};
const ageSource = async (f: Fixture) => {
  const updatedAt = await f.renew("pod-1", 20_000);
  await vi.waitFor(() => {
    expect(f.port.kv.reader.lease("machine-b", POD)?.entry.value.updatedAt).toBe(updatedAt);
    expect(f.port.resolve(POD)?.route.live ?? false).toBe(false);
  });
};
const withdrawOldMirror = async (f: Fixture) => {
  await ageSource(f);
  // Seed the pre-fix complete-omission outcome, not a source presence deletion. The root
  // remains admitted in the real reader, but has no live lease evidence until renewed.
  await f.link.local.mirror({ hosts: [], participants: [] });
  expectWithdrawn(f);
};
const stallOutboundMirror = (f: Fixture) => {
  const entered = barrier(), release = barrier();
  const mirror = f.port.mirror.bind(f.port);
  const spy = vi.spyOn(f.port, "mirror").mockImplementation(async (...args) => {
    entered.resolve();
    await release.promise;
    return mirror(...args);
  });
  let settled = false;
  const syncing = f.link.bridge.syncPresence().finally(() => { settled = true; });
  return { entered, release, syncing, settled: () => settled, restore: () => spy.mockRestore() };
};

describe.skipIf(!binary)("NATS watch renews the file mirror independently of drain passes", () => {
  it("keeps a live root routable during a 20 s blocked drain with 5 s source renewals", async () => {
    const f = await fixture();
    const entered = barrier(), release = barrier();
    const send = f.port.send.bind(f.port);
    vi.spyOn(f.port, "send").mockImplementation(async (envelope, signal) => {
      const sent = await send(envelope, signal);
      entered.resolve();
      await release.promise; // Hold the drain's publish receipt, not the source's authority checks.
      return sent;
    });
    let draining: Promise<unknown> | undefined;
    try {
      const initialExpiry = f.lease().expiresAt;
      await f.store.publish({ topic: "fabric.control.command", kind: "followUp", from: identity, to: POD, data: {
        version: 1, commandId: "slow-drain", targetId: POD, operation: "followUp", replyTo: FILE, ownerIncarnation: "pod-1",
        message: "slow drain", requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
        destinationRemoteHost: LINK,
      } });
      draining = f.link.bridge.step();
      await entered.promise;
      const hostVersion = f.store.get(stateKey("topology/hosts/", POD), { fresh: true })!.version;
      const participantVersion = f.store.get(stateKey("topology/participants/", POD), { fresh: true })!.version;
      await f.renew();
      for (let tick = 0; tick < 4; tick++) {
        await sleep(5_000);
        expect(f.port.resolve(POD)?.route.live).toBe(true);
        expect(f.status()).toBe("live");
        await f.renew();
      }
      expect(f.store.get(stateKey("topology/hosts/", POD), { fresh: true })!.version).toBe(hostVersion);
      expect(f.store.get(stateKey("topology/participants/", POD), { fresh: true })!.version).toBe(participantVersion);
      expect(Date.now()).toBeGreaterThan(initialExpiry);
      expect(f.listed()).toMatchObject({ id: POD, remoteHost: LINK, ownerIncarnation: "pod-1", stale: false });
      expect(f.status()).toBe("live");
      expect(f.link.local.holds(POD)).toBe(true);
      expect(f.failures).toEqual([]);
      release.resolve();
      await expect(draining).resolves.toMatchObject({ toRemote: 1, dropped: 0 });
      const abort = new AbortController();
      try {
        for await (const delivery of f.pod.inbox(AbortSignal.any([abort.signal, AbortSignal.timeout(5_000)]))) {
          expect(delivery.payload).toMatchObject({ targetId: POD, body: { ownerIncarnation: "pod-1", message: "slow drain" } });
          await delivery.ack();
          break;
        }
      } finally { abort.abort(); }
    } finally { release.resolve(); await draining?.catch(() => undefined); await f.close(); }
  }, 60_000);

  it("expires a stopped source within its TTL despite unrelated watch events", async () => {
    const f = await fixture();
    try {
      const before = f.lease().expiresAt;
      const lastUpdate = await f.renew();
      await vi.waitFor(() => expect(f.lease().expiresAt).toBeGreaterThan(before));
      expect(f.lease().expiresAt).toBeLessThanOrEqual(lastUpdate + 15_000);
      const deadline = lastUpdate + 15_000;
      while (Date.now() <= deadline + 100) {
        // Presence-only watch events cannot grant authority after the last live lease.
        await f.publishRoot();
        await sleep(Math.min(1_000, Math.max(1, deadline + 101 - Date.now())));
      }
      expect(f.port.resolve(POD)?.route.live ?? false).toBe(false);
      expect(f.status()).toBe("bridge-stale");
      expect(f.listed()).toMatchObject({ bridgeStale: true });
      expect(f.link.local.holds(POD)).toBe(false);
      expect(f.lease().expiresAt).toBeLessThanOrEqual(deadline);
      expect(f.failures).toEqual([]);
    } finally { await f.close(); }
  }, 40_000);

  it("preserves incarnation replacement and fences watch-driven writes on stop", async () => {
    const f = await fixture();
    try {
      await f.renew("pod-2");
      await f.publishRoot("pod-2");
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route).toMatchObject({ ownerIncarnation: "pod-2", live: true }));
      // Lease-only refresh cannot replace owners; the normal full sync still owns that transition.
      expect(f.lease().incarnationToken).toBe("pod-1");
      await f.link.bridge.syncPresence();
      expect(f.listed()).toMatchObject({ ownerIncarnation: "pod-2", stale: false });
      expect(f.lease()).toMatchObject({ incarnationToken: "pod-2", bridgeSupersededTokens: ["pod-1"] });
      await f.renew("pod-1");
      await f.publishRoot("pod-1");
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route).toMatchObject({ ownerIncarnation: "pod-1", live: true }));
      expect(f.lease().incarnationToken).toBe("pod-2");
      await expect(f.link.bridge.syncPresence()).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
      await f.link.bridge.stop();
      const stopped = f.lease();
      await f.renew("pod-2");
      await f.publishRoot("pod-2");
      await sleep(100);
      expect(f.lease()).toEqual(stopped);
      expect(f.failures).toEqual([]);
    } finally { await f.close(); }
  }, 15_000);
});

describe.skipIf(!binary)("NATS watch recovers withdrawn file mirrors without a bidirectional sync", () => {
  it("holds an aged mirror's original deadline and recovers a prior omission while outbound sync is stalled", async () => {
    const f = await fixture();
    let stalled: ReturnType<typeof stallOutboundMirror> | undefined;
    try {
      const hostVersion = hostRow(f)!.version;
      const participantVersion = participantRow(f)!.version;
      await ageSource(f);
      // Capture after the stale watch arrived: an in-flight bootstrap renewal may have
      // legitimately finished before that event, but no uncertain observation can extend it.
      const original = f.lease();
      f.ageReceiverBy(30_000);
      expect(original.expiresAt).toBeLessThan(f.link.local.now());
      expect(mirroredOwnerRoutingStatus({ ...original, remoteHost: LINK }, f.link.local.now())).toBe("bridge-stale");
      expect(f.link.local.holds(POD)).toBe(false);
      const uncertain = await f.port.presence();
      expect(uncertain).toMatchObject({ hosts: [], participants: [], partial: true });
      await f.link.bridge.syncPresence();
      // Holding omission is not renewal: source uncertainty must preserve the old bytes/deadline.
      expect(hostRow(f)!.version).toBe(hostVersion);
      expect(participantRow(f)!.version).toBe(participantVersion);
      expect(f.lease()).toEqual(original);
      f.ageReceiverBy(-30_000);

      await f.link.local.mirror({ hosts: [], participants: [] });
      expectWithdrawn(f);
      stalled = stallOutboundMirror(f);
      await stalled.entered.promise;
      // Another stale lease/presence observation cannot restore membership or create a lease.
      const processed = barrier();
      const renew = f.link.local.renewMirroredLeases.bind(f.link.local);
      const renewal = vi.spyOn(f.link.local, "renewMirroredLeases").mockImplementation(async (...args) => {
        const result = await renew(...args);
        if (args[0].length === 0) processed.resolve();
        return result;
      });
      try {
        await f.publishRoot();
        await processed.promise;
        expectWithdrawn(f);
      } finally { renewal.mockRestore(); }

      const sourceUpdatedAt = await f.renew();
      await vi.waitFor(() => {
        expect(f.listed()).toMatchObject({ id: POD, ownerIncarnation: "pod-1", remoteHost: LINK, stale: false });
        expect(f.link.local.holds(POD)).toBe(true);
      });
      expect(stalled.settled()).toBe(false);
      expect(hostRow(f)!.updatedBy.id).toBe(POD);
      expect(participantRow(f)!.updatedBy.id).toBe(POD);
      expect(f.lease()).toMatchObject({ incarnationToken: "pod-1", mirrorLease: 1, mirror: { remoteHost: LINK } });
      expect(f.lease().expiresAt).toBeLessThanOrEqual(sourceUpdatedAt + 15_000);
      // The ordinary installer also supplies local writer attribution on recovered leases.
      expect(f.lease().mirror?.bridgeProcess).toMatchObject({ link: LINK, pid: process.pid });
      expect(f.failures).toEqual([]);
    } finally {
      stalled?.release.resolve();
      await stalled?.syncing.catch(() => undefined);
      stalled?.restore();
      await f.close();
    }
  }, 20_000);

  it("bootstraps an already-observed fresh root independently of a stalled outbound sync", async () => {
    const f = await fixture({ subscribe: false });
    let stalled: ReturnType<typeof stallOutboundMirror> | undefined;
    try {
      await withdrawOldMirror(f);
      stalled = stallOutboundMirror(f);
      await stalled.entered.promise;
      const updatedAt = await f.renew();
      await vi.waitFor(() => {
        expect(f.port.kv.reader.lease("machine-b", POD)?.entry.value.updatedAt).toBe(updatedAt);
        expect(f.port.resolve(POD)?.route.live).toBe(true);
      });
      expectWithdrawn(f); // No watch subscription has run on the receiver yet.
      f.subscribe();
      await vi.waitFor(() => expect(f.listed()).toMatchObject({ id: POD, remoteHost: LINK, stale: false }));
      expect(stalled.settled()).toBe(false);
      expect(f.link.local.holds(POD)).toBe(true);
      expect(f.failures).toEqual([]);
    } finally {
      stalled?.release.resolve();
      await stalled?.syncing.catch(() => undefined);
      stalled?.restore();
      await f.close();
    }
  }, 15_000);

  it("withdraws explicit presence deletion and cannot reimport it from subsequent lease-only updates", async () => {
    const f = await fixture();
    try {
      expect(await f.deleteRoot()).toBe(true);
      await vi.waitFor(() => expect(f.port.resolve(POD)).toBeUndefined());
      expect(await f.port.presence()).toMatchObject({ hosts: [], participants: [] });
      expect((await f.port.presence()).partial).not.toBe(true);
      await f.link.bridge.syncPresence();
      expectWithdrawn(f);
      const processed = barrier();
      const renew = f.link.local.renewMirroredLeases.bind(f.link.local);
      const renewal = vi.spyOn(f.link.local, "renewMirroredLeases").mockImplementation(async (...args) => {
        const result = await renew(...args);
        processed.resolve();
        return result;
      });
      try {
        await f.renew();
        await processed.promise;
        expectWithdrawn(f);
        expect(f.failures).toEqual([]);
      } finally { renewal.mockRestore(); }
    } finally { await f.close(); }
  }, 15_000);

  it.each(["expired sample", "commit deadline", "lease lapse", "changed incarnation", "explicit deletion", "link fence", "native reservation"] as const)(
    "cancels queued watch recovery after %s", async reason => {
      const f = await fixture();
      const entered = barrier(), release = barrier(), completed = barrier();
      const prepare = f.store.prepareLock.bind(f.store);
      const mirror = f.link.local.mirror.bind(f.link.local);
      let blocked = false;
      let fencing: Promise<void> | undefined;
      let preparation: ReturnType<typeof vi.spyOn> | undefined;
      let recovery: ReturnType<typeof vi.spyOn> | undefined;
      let committing: ReturnType<typeof vi.spyOn> | undefined;
      try {
        await withdrawOldMirror(f);
        preparation = vi.spyOn(f.store, "prepareLock").mockImplementation(async () => {
          if (!blocked) {
            blocked = true;
            entered.resolve();
            await release.promise;
          }
          return prepare();
        });
        recovery = vi.spyOn(f.link.local, "mirror").mockImplementation((...args) => {
          const result = mirror(...args);
          void result.then(() => completed.resolve(), () => completed.resolve());
          return result;
        });
        await f.renew();
        await entered.promise;
        // This is a real partial mirror queued at the file receiver, before its custody/commit.
        if (reason === "expired sample") f.ageReceiverBy(30_000);
        else if (reason === "commit deadline") {
          const write = f.store.writeBatch.bind(f.store);
          committing = vi.spyOn(f.store, "writeBatch").mockImplementation(input => write({ ...input, beforeCommit: () => {
            f.ageReceiverBy(30_000);
            input.beforeCommit?.();
          } }));
        } else if (reason === "lease lapse") await ageSource(f);
        else if (reason === "changed incarnation") {
          await f.renew("pod-2");
          await f.publishRoot("pod-2");
          await vi.waitFor(() => expect(f.port.resolve(POD)?.route).toMatchObject({ ownerIncarnation: "pod-2", live: true }));
        } else if (reason === "explicit deletion") {
          expect(await f.deleteRoot()).toBe(true);
          await vi.waitFor(() => expect(f.port.resolve(POD)).toBeUndefined());
        } else if (reason === "link fence") fencing = f.link.remote.fence();
        else {
          const now = Date.now();
          await f.store.put({ key: stateKey("topology/hosts/", POD), identity: { id: POD, name: "native", kind: "main" }, value: {
            format: 1, id: POD, rootId: POD, identity: { id: POD, name: "native", kind: "main" },
            incarnationToken: "native", startedAt: now, updatedAt: now, expiresAt: now + 60_000,
          } });
        }
        release.resolve();
        await completed.promise;
        await fencing;
        if (reason === "native reservation") {
          expect(hostRow(f)!.value).toMatchObject({ incarnationToken: "native", identity: { name: "native" } });
          expect((hostRow(f)!.value as { remoteHost?: string }).remoteHost).toBeUndefined();
          expect(participantRow(f)).toBeUndefined();
          expect(readHostLease(f.store.root, POD)).toBeUndefined();
          expect(f.link.local.holds(POD)).toBe(false);
        } else if (reason === "changed incarnation") {
          // Only a resampled pod-2 may install: admitting the queued pod-1 first would
          // strand it, because the watch's live lease-only path cannot replace that owner.
          await vi.waitFor(() => expect(f.listed()).toMatchObject({ ownerIncarnation: "pod-2", stale: false }));
          expect(f.lease().incarnationToken).toBe("pod-2");
          expect(f.link.local.holds(POD)).toBe(true);
        } else expectWithdrawn(f);
        expect(f.failures).toEqual([]);
      } finally {
        release.resolve();
        await fencing?.catch(() => undefined);
        preparation?.mockRestore();
        recovery?.mockRestore();
        committing?.mockRestore();
        await f.close();
      }
    }, 15_000,
  );

  it("refuses a superseded source token when membership is missing but newer lease authority remains", async () => {
    const f = await fixture();
    try {
      await f.renew("pod-2");
      await f.publishRoot("pod-2");
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route).toMatchObject({ ownerIncarnation: "pod-2", live: true }));
      await f.link.bridge.syncPresence();
      const newer = f.lease();
      expect(newer).toMatchObject({ incarnationToken: "pod-2", bridgeSupersededTokens: ["pod-1"] });
      await f.store.delete({ key: stateKey("topology/hosts/", POD) });
      await f.store.delete({ key: stateKey("topology/participants/", POD) });
      await f.renew("pod-1");
      await f.publishRoot("pod-1");
      await vi.waitFor(() => expect(f.failures).toEqual([
        expect.objectContaining({ code: "FABRIC_BRIDGE_STALE_INCARNATION" }),
      ]));
      expect(hostRow(f)).toBeUndefined();
      expect(participantRow(f)).toBeUndefined();
      expect(f.lease()).toEqual(newer);
      expect(f.link.local.holds(POD)).toBe(false);
    } finally { await f.close(); }
  }, 15_000);
});
