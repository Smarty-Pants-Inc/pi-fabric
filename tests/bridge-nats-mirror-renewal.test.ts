// expected-skip: no NATS_BIN/PATH executable: an authenticated loopback broker is required.
// Keep reason: source-lease trust boundary and a slow-drain regression. Real KV watches, file
// custody/leases, directory routing and NATS delivery; only the outbound send is held at a barrier.
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

const fixture = async () => {
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
  const link = createNatsFileLink({ store, port, localName: "file", link: LINK, cursorPath: path.join(dir, "cursor.json"),
    ledger: new MemoryLedger(), authorize: () => ({ ok: true, principal: "carry" }),
    authorizeTopic: () => ({ ok: true, principal: "none" }), mirrorAllowed: () => true,
  });
  const initialUpdate = await renew("pod-1", 6_000); // About 7 s remains after the safety margin.
  await vi.waitFor(() => expect(port.kv.reader.lease("machine-b", POD)?.entry.value.updatedAt).toBe(initialUpdate));
  await link.bridge.start();
  const failures: Error[] = [];
  // Subscribe exactly as MeshBridge.run does, but leave full resampling/draining under test control.
  const unwatch = link.remote.subscribeChanges(() => undefined, error => failures.push(error));
  const reader = new ParticipantDirectory(store, { enabled: true, hostId: "reader", rootId: "reader",
    identity: { id: "reader", name: "reader", kind: "main" }, reapDeadHosts: false });
  const listed = () => reader.list({ scope: "project", kinds: ["root"], fresh: true }).find(row => row.id === POD);
  const lease = () => readHostLease(store.root, POD)!;
  const status = () => mirroredOwnerRoutingStatus({ ...lease(), remoteHost: LINK });
  const close = async () => {
    unwatch();
    await link.bridge.stop();
    await port.close();
    await pod.kv("leases").delete(leaseKey, leaseRevision);
    await pod.kv("presence").delete(presenceKey, presenceRevision);
    await pod.close();
    store.closeState();
    fs.rmSync(dir, { recursive: true, force: true });
  };
  return { store, pod, port, link, renew, publishRoot, listed, lease, status, failures, close };
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
