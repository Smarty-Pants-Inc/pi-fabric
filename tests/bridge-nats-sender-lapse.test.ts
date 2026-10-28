// expected-skip: no NATS_BIN/PATH executable: an authenticated loopback broker is required.
// Keep: incident #17213, data safety and the credential-bound sender trust boundary.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { createNatsFileLink, NatsCarrierDeferredError } from "../src/mesh/bridge-nats-file.js";
import { openBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger } from "../src/mesh/bridge-nats-link.js";
import { openNatsBridgeSide } from "../src/mesh/bridge-nats-port.js";
import { natsLeaseKey, natsPresenceKey } from "../src/mesh/bridge-nats-presence.js";
import type { BridgeNatsPortDelivery } from "../src/mesh/bridge-nats-types.js";
import { MeshStore } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsTransport } from "../src/transport/nats/index.js";
import { DirectoryNotReadyError, type TransportEnvelope } from "../src/transport/types.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: sender lapse needs NATS_BIN");
const FILE = "session:11111111-1111-4111-8111-111111111111";
const POD = "session:22222222-2222-4222-8222-222222222222";
const OTHER = "session:33333333-3333-4333-8333-333333333333";
const FILE2 = "session:44444444-4444-4444-8444-444444444444";
const LINK = "sender-lapse-test";
const REFUSAL = "sender is not live on its source machine";
const WINDOW = 30_000;
const identity = (id: string) => ({ id, name: id, kind: "main" as const });
const stateKey = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const record = (id: string, incarnation = "inc-1"): FabricParticipantRecord => ({
  format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
  name: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
  controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now(),
});
let broker: PathBHarness;
beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
  try { expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0); }
  finally { vi.unstubAllEnvs(); }
}, 60_000);
afterAll(async () => { await broker?.stop(); });

const fixture = async (maxInHand?: number) => {
  // Isolate this fixture from retained Limits-stream history, not the delivery being tested.
  const admin = await broker.connect(broker.users.bootstrap);
  try {
    const { manager } = await openJetStream(admin);
    await manager.streams.purge("FABRIC_IN_machine-a"); await manager.streams.purge("FABRIC_IN_machine-b");
  } finally { await admin.close(); }
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-sender-lapse-"));
  const cursorPath = path.join(dir, "cursor.json"), lapsePath = `${cursorPath}.sender-lease-lapse`;
  const linkName = `${LINK}-${randomUUID().slice(0, 8)}`;
  let time = Date.now();
  const clock = () => time;
  const store = new MeshStore(path.join(dir, "mesh"), 256 * 1024, 1_000, { stateBackend: "file" });
  const pod = await openNatsTransport({ ...broker.runtimeOptions(broker.users.machineB), hostId: POD });
  const lease = async (id: string, expired = false, extra: Record<string, unknown> = {}) => {
    const key = natsLeaseKey("machine-b", id), now = Date.now();
    const revision = (await pod.kv("leases").get(key))?.revision ?? 0;
    await pod.kv("leases").put(key, `${id}#inc-1`, {
      id, rootId: id, identityId: id, incarnation: "inc-1", incarnationToken: "inc-1", pid: process.pid,
      pidStartTime: "1", bootId: "12345678-1234-4234-8234-123456789abc",
      updatedAt: now - (expired ? 15_000 : 0), expiresAt: now + (expired ? -1 : 15_000), ...extra,
    }, revision);
  };
  const presence = async (id: string, incarnation = "inc-1") => {
    const key = natsPresenceKey("machine-b", id);
    const revision = (await pod.kv("presence").get(key))?.revision ?? 0;
    await pod.kv("presence").put(key, `${id}#${incarnation}`, record(id, incarnation), revision);
  };
  for (const id of [POD, OTHER]) { await lease(id); await presence(id); }
  for (const id of [FILE, FILE2]) {
    const now = Date.now();
    await store.put({ key: stateKey("topology/hosts/", id), identity: identity(id), value: {
      format: 1, id, rootId: id, identity: identity(id), incarnationToken: "inc-1", startedAt: now - 1_000,
      updatedAt: now, expiresAt: now + 600_000,
    } });
    await store.put({ key: stateKey("topology/participants/", id), identity: identity(id), value: record(id) });
  }
  let ledger = await openBridgeNatsLedger({ dir: path.join(dir, "ledger") });
  const runtime = broker.runtimeOptions(broker.users.machineA);
  const deliveries: BridgeNatsPortDelivery[] = [];
  let hiddenTarget = false;
  const openPort = async () => {
    const opened = await openNatsBridgeSide({ link: linkName,
    nats: { servers: [String(runtime.servers)], machine: "machine-a", credentials: runtime.credentials,
      caFile: broker.tls!.caFile, ...PATH_B_PERMISSION_POLICY },
    allow: { file: [FILE, FILE2].map(id => ({ kind: "root" as const, id })),
      nats: [POD, OTHER].map(id => ({ kind: "root" as const, id, machine: "machine-b" })) },
    });
    const resolve = opened.resolve.bind(opened);
    opened.resolve = id => hiddenTarget && id === FILE ? undefined : resolve(id);
    const inbox = opened.inbox.bind(opened);
    opened.inbox = async function* (signal) {
      for await (const delivery of inbox(signal)) { deliveries.push(delivery); yield delivery; }
    };
    return opened;
  };
  let port = await openPort();
  const logs: string[] = [];
  const make = () => createNatsFileLink({ store, port, localName: "file", link: linkName, cursorPath,
    now: clock, ledger: natsCarrierLedger(ledger, linkName), ...(maxInHand === undefined ? {} : { maxInHand }),
    authorize: () => ({ ok: true, principal: "carry" }),
    authorizeTopic: () => ({ ok: true, principal: "none" }), mirrorAllowed: () => true, log: line => logs.push(line),
  });
  let link = make();
  await vi.waitFor(() => expect(port.resolve(POD)?.route.live).toBe(true));
  await link.bridge.start();
  const receive = vi.fn(() => ({ accepted: true, messageId: "once" }));
  const owners = [FILE, FILE2].map(id => new FabricControlPlane(store, identity(id), {
    enabled: true, hostId: id, ownerIncarnation: "inc-1", pollMs: 10,
  }));
  for (const owner of owners) owner.start(receive);
  const envelope = (id = randomUUID(), sender = POD, target = FILE, commandId = "same-key"): TransportEnvelope => ({
    id, kind: "command", to: { machine: "machine-a", hostId: target }, targetId: target, operation: "followUp",
    from: identity(sender), body: { version: 1, commandId, targetId: target, operation: "followUp", ownerIncarnation: "inc-1",
      message: "same-body", replyTo: sender, requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 300_000 },
  });
  const abort = new AbortController(), replies: TransportEnvelope[] = [];
  const reading = (async () => { for await (const reply of pod.inbox(abort.signal)) { replies.push(reply.payload); await reply.ack(); } })();
  const pending = async (count = 1) => vi.waitFor(async () => expect(await link.remote.latestSequence()).toBeGreaterThanOrEqual(count), { timeout: 15_000 });
  const lapse = async () => {
    await lease(POD, true);
    await vi.waitFor(() => expect(port.resolve(POD)?.route.live).toBe(false));
    expect(port.resolve(POD)).toMatchObject({ leaseLapsed: true, presenceFailure: "expired", remainingAuthorityMs: expect.any(Number) });
  };
  const restart = async (accelerate = true) => {
    // A real broker NAK accelerates crash redelivery, without changing its ID/sequence or dedupe.
    if (accelerate) for (const delivery of deliveries) delivery.nak(250);
    await link.bridge.stop(); await port.close(); await ledger.close();
    ledger = await openBridgeNatsLedger({ dir: path.join(dir, "ledger") });
    port = await openPort(); link = make(); await link.bridge.start();
    if (accelerate) await pending();
  };
  const close = async () => {
    abort.abort(); await reading;
    for (const owner of owners) await owner.close();
    await link.bridge.stop(); await port.close(); await ledger.close();
    for (const id of [POD, OTHER]) {
      const leaseKey = natsLeaseKey("machine-b", id), presenceKey = natsPresenceKey("machine-b", id);
      const currentLease = await pod.kv("leases").get(leaseKey), currentPresence = await pod.kv("presence").get(presenceKey);
      if (currentLease) await pod.kv("leases").delete(leaseKey, currentLease.revision);
      if (currentPresence) await pod.kv("presence").delete(presenceKey, currentPresence.revision);
    }
    await pod.close(); store.closeState();
  };
  return { get port() { return port; }, get link() { return link; }, store, pod, logs, receive, replies, envelope, lease, presence,
    clock, linkName, deliveries, lapsePath, cursorPath, pending, lapse, restart, close, advance: (ms: number) => { time += ms; },
    hideTarget: (hidden: boolean) => { hiddenTarget = hidden; },
    events: () => store.read({ topic: "fabric.control.command", after: 0, limit: 100 }),
  };
};

describe.skipIf(!binary)("NATS carrier sender lease-lapse", () => {
  it("broker ack-floor reclaims terminal history at capacity across a real link restart", async () => {
    const f = await fixture(2);
    const ids: string[] = [];
    try {
      for (let index = 0; index < 4; index++) {
        if (index === 2) { await f.restart(false); await f.link.remote.latestSequence(); }
        await f.lease(POD); await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(true));
        const original = f.envelope(); ids.push(original.id);
        await f.pod.send(original); await f.pending(); await f.lapse();
        const delivery = f.deliveries.find(item => item.envelope.id === original.id)!;
        expect(delivery.publishedAt).toEqual(expect.any(Number));
        // advance(WINDOW) moves only the carrier clock, not the real broker clock.
        // Prior windows are terminal; align this fresh window with its broker publish
        // so later iterations do not accidentally test an already-expired delivery.
        f.advance(delivery.publishedAt! - f.clock());
        await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
        f.advance(WINDOW); await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
        // Unlike term()'s void result, the consumer floor is broker evidence of settlement.
        await vi.waitFor(async () => expect(await f.port.ackFloor()).toBeGreaterThanOrEqual(delivery.streamSequence));
        const saved = JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses as Array<{ id: string }>;
        expect(saved.length).toBeLessThanOrEqual(2);
        if (index >= 2) expect(saved.some(lapse => lapse.id === ids[0])).toBe(false);
      }
      expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled();
      expect(f.logs.filter(line => line.includes("retaining this lapse in memory only"))).toEqual([]);
    } finally { await f.close(); }
  }, 60_000);

  it.each([false, true])("holds the original, survives restart=%s, then admits same-key retry exactly once from LIVE presence", async restart => {
    const f = await fixture();
    const writes = vi.spyOn(atomic, "writeJsonAtomic");
    try {
      const original = f.envelope();
      await f.pod.send(original); await f.pending(); await f.lapse();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled();
      expect(f.logs).toContainEqual(expect.stringMatching(/sender-live defer; predicate=expired sender=session:.* remainingAuthorityMs=-\d+/));
      const retained = fs.readFileSync(f.lapsePath, "utf8");
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(fs.readFileSync(f.lapsePath, "utf8")).toBe(retained);
      if (restart) { await f.restart(); await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError); }
      expect(writes.mock.calls.filter(call => call[0] === f.lapsePath)).toHaveLength(1);
      // A fresh envelope with the same logical command/body is a genuine client retry.
      const retry = { ...original, id: randomUUID() };
      await f.pod.send(retry); await f.lease(POD);
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(true));
      await f.link.bridge.syncPresence();
      await vi.waitFor(async () => { await f.link.bridge.step(); expect(f.receive).toHaveBeenCalledOnce(); expect(f.replies.length).toBeGreaterThan(0); }, { timeout: 10_000 });
      await f.link.bridge.step();
      expect(f.events()).toHaveLength(1);
      expect(f.events()[0]!.data).toMatchObject({ bridge: { id: original.id } });
      await f.lease(POD); // Keep the healthy source renewing through a slow real-broker replay check.
      const replay = { ...original, id: randomUUID() }; await f.pod.send(replay);
      await vi.waitFor(async () => { await f.link.bridge.step(); expect(f.replies).toContainEqual(expect.objectContaining({ id: `ack:${replay.id}`, body: expect.objectContaining({ accepted: true }) })); }, { timeout: 10_000 });
      expect(f.receive).toHaveBeenCalledOnce(); expect(f.events()).toHaveLength(1);
      expect(f.logs.filter(line => line.includes(original.id) && line.includes("refused"))).toEqual([]);
    } finally { writes.mockRestore(); await f.close(); }
  }, 60_000);

  it.each([false, true])("refuses at the original deadline across restart=%s; unrelated recipient waits only at this HOL boundary", async restart => {
    const f = await fixture();
    const now = vi.spyOn(Date, "now").mockImplementation(f.clock);
    try {
      const held = f.envelope(); const suffix = f.envelope(randomUUID(), OTHER, FILE2, "unrelated");
      await f.pod.send(held); await f.pod.send(suffix); await f.pending(2); await f.lapse();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      const started = JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses[0].startedAt as number;
      if (restart) await f.restart();
      f.advance(WINDOW - 1 - (f.clock() - started));
      await f.lease(OTHER);
      await vi.waitFor(() => expect(f.port.resolve(OTHER)?.route.live).toBe(true), { timeout: 10_000 });
      await f.link.bridge.syncPresence();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled();
      f.advance(2);
      await vi.waitFor(async () => { await f.link.bridge.step(); expect(f.events()).toHaveLength(1); }, { timeout: 10_000 });
      expect(f.events()[0]!.to).toBe(FILE2);
      expect(f.logs.filter(line => line.includes(held.id) && line.includes(REFUSAL))).toHaveLength(1);
      expect(f.clock() - started).toBeGreaterThanOrEqual(WINDOW);
      expect(f.clock() - started).toBeLessThan(WINDOW + 5_000);
      console.log(`HOL restart=${restart}: unrelated recipient released at ${f.clock() - started} ms (30 s budget + scheduling)`);
      await f.link.bridge.step();
      expect(f.logs.filter(line => line.includes(held.id) && line.includes(REFUSAL))).toHaveLength(1);
    } finally { now.mockRestore(); await f.close(); }
  }, 30_000);

  it.each(["wrong-machine", "wrong-identity", "absent", "lease-identity", "root-recheck"])("refuses %s immediately, never starting a lapse window", async reason => {
    const f = await fixture();
    const resolve = f.port.resolve.bind(f.port);
    const seam = reason === "wrong-machine" || reason === "wrong-identity" ? vi.spyOn(f.port, "resolve").mockImplementation(id => {
      const entry = resolve(id);
      return id !== POD || !entry ? entry : { ...entry,
        ...(reason === "wrong-identity" ? { identity: identity(OTHER) } : {}),
        route: { ...entry.route, live: false, ...(reason === "wrong-machine" ? { machine: "machine-c" } : {}) },
        leaseLapsed: true,
      };
    }) : undefined;
    try {
      await f.pod.send(f.envelope(randomUUID(), reason === "absent" ? "session:missing" : POD)); await f.pending();
      if (reason === "lease-identity" || reason === "root-recheck") {
        await f.lease(POD, reason === "lease-identity", reason === "lease-identity" ? { identityId: OTHER } : { rootRecheck: true });
        await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(false));
      }
      await f.link.bridge.step();
      expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled(); expect(fs.existsSync(f.lapsePath)).toBe(false);
      expect(f.logs.filter(line => line.includes(REFUSAL))).toHaveLength(1);
      const predicate = reason === "wrong-machine" ? "machine-mismatch" : reason === "wrong-identity" || reason === "lease-identity"
        ? "identity-mismatch" : reason === "root-recheck" ? "root-recheck" : "no-route";
      expect(f.logs).toContainEqual(expect.stringContaining(`predicate=${predicate}`));
    } finally { seam?.mockRestore(); await f.close(); }
  }, 30_000);

  it("a new incarnation resumes only with the same LIVE source machine and identity (today's live check)", async () => {
    const f = await fixture();
    try {
      await f.pod.send(f.envelope()); await f.pending(); await f.lapse();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      await f.presence(POD, "inc-2");
      const key = natsLeaseKey("machine-b", POD), old = await f.pod.kv("leases").get(key);
      const now = Date.now();
      // The same authenticated machine restarts the same participant under a fresh epoch.
      const revision = await f.pod.kv("leases").put(key, `${POD}#inc-2`, { ...(old!.value as object), incarnation: "inc-2", incarnationToken: "inc-2", updatedAt: now, expiresAt: now + 15_000 }, old!.revision);
      await vi.waitFor(() => expect(f.port.resolve(POD)).toMatchObject({ identity: { id: POD }, route: { live: true, machine: "machine-b", ownerIncarnation: "inc-2" } }));
      await f.link.bridge.syncPresence();
      await vi.waitFor(async () => { await f.link.bridge.step(); expect(f.receive).toHaveBeenCalledOnce(); }, { timeout: 10_000 });
      expect(f.events()).toHaveLength(1);
      // Restore the fixture's lease revision/owner for cleanup.
      await f.pod.kv("leases").delete(key, revision); await f.lease(POD);
    } finally { await f.close(); }
  }, 30_000);

  it.each([false, true])("allowed-target KV wait shares the durable deadline across restart=%s", async restart => {
    const f = await fixture();
    const now = vi.spyOn(Date, "now").mockImplementation(f.clock);
    try {
      const original = f.envelope(); await f.pod.send(original); await f.pending(); f.hideTarget(true);
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      const retained = fs.readFileSync(f.lapsePath, "utf8");
      if (restart) await f.restart();
      f.advance(29_999); await f.lease(POD);
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(true), { timeout: 10_000 });
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(fs.readFileSync(f.lapsePath, "utf8")).toBe(retained);
      f.advance(1); await f.link.bridge.step();
      expect(f.events()).toEqual([]);
      expect(f.logs.filter(line => line.includes(original.id) && line.includes("refused"))).toHaveLength(1);
      expect(f.deliveries.every(delivery => delivery.streamSequence === f.deliveries[0]!.streamSequence)).toBe(true);
    } finally { now.mockRestore(); await f.close(); }
  }, 30_000);

  it("an allowed target absent from this link's KV view reappears within the original deadline", async () => {
    const f = await fixture();
    const now = vi.spyOn(Date, "now").mockImplementation(f.clock);
    try {
      const original = f.envelope(); await f.pod.send(original); await f.pending(); f.hideTarget(true);
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      const retained = fs.readFileSync(f.lapsePath, "utf8");
      expect(f.events()).toEqual([]);
      f.advance(10_000); f.hideTarget(false); await f.lease(POD);
      await f.link.bridge.syncPresence();
      await vi.waitFor(async () => { await f.link.bridge.step(); expect(f.receive).toHaveBeenCalledOnce(); }, { timeout: 10_000 });
      expect(f.events()).toHaveLength(1); expect(f.events()[0]!.data).toMatchObject({ bridge: { id: original.id } });
      expect(fs.readFileSync(f.lapsePath, "utf8")).toBe(retained);
      expect(f.logs.filter(line => line.includes(original.id) && line.includes("refused"))).toEqual([]);
    } finally { now.mockRestore(); await f.close(); }
  }, 30_000);

  it("sender lapse -> target KV wait cannot start a second deadline", async () => {
    const f = await fixture();
    const now = vi.spyOn(Date, "now").mockImplementation(f.clock);
    try {
      const original = f.envelope(); await f.pod.send(original); await f.pending(); await f.lapse();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      const retained = fs.readFileSync(f.lapsePath, "utf8");
      f.advance(20_000); f.hideTarget(true); await f.lease(POD);
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(true), { timeout: 10_000 });
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(fs.readFileSync(f.lapsePath, "utf8")).toBe(retained);
      f.advance(10_000); await f.link.bridge.step();
      expect(f.events()).toEqual([]);
      expect(f.logs.filter(line => line.includes(original.id) && line.includes("refused"))).toHaveLength(1);
    } finally { now.mockRestore(); await f.close(); }
  }, 30_000);

  it("re-checks source liveness even after conversion; an unready view logs no unverified identity", async () => {
    const f = await fixture();
    try {
      await f.pod.send(f.envelope()); await f.pending();
      expect((await f.link.remote.read(0)).events).toHaveLength(1);
      const unavailable = vi.spyOn(f.port, "resolve").mockImplementation(() => { throw new DirectoryNotReadyError("watch gap"); });
      try { await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError); }
      finally { unavailable.mockRestore(); }
      expect(f.logs).toContain("toFile: sender-live wait predicate=not-ready remainingAuthorityMs=unknown");
      await f.lapse(); await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(f.events()).toEqual([]);
      await f.lease(POD); await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(true));
      await f.link.bridge.syncPresence();
      await vi.waitFor(async () => { await f.link.bridge.step(); expect(f.events()).toHaveLength(1); }, { timeout: 10_000 });
    } finally { await f.close(); }
  }, 30_000);

  it("a carrier lapse followed by a real local append-fence lapse ends at the FIRST deadline", async () => {
    const f = await fixture();
    const now = vi.spyOn(Date, "now").mockImplementation(f.clock);
    const publish = f.link.local.publish.bind(f.link.local);
    let attempts = 0;
    const seam = vi.spyOn(f.link.local, "publish").mockImplementation(async (...args) => {
      if (args[0].topic === "fabric.control.command") {
        attempts++;
        const lease = readHostLease(f.store.root, POD)!;
        writeHostLease(f.store.root, { ...lease, expiresAt: f.clock() - 1 });
      }
      return publish(...args);
    });
    try {
      const original = f.envelope(); await f.pod.send(original); await f.pending(); await f.lapse();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      const first = JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses[0].startedAt;
      f.advance(20_000); await f.lease(POD);
      await vi.waitFor(() => expect(f.port.resolve(POD)?.route.live).toBe(true));
      await f.link.bridge.syncPresence();
      expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
      expect(attempts).toBeGreaterThanOrEqual(1); expect(attempts).toBeLessThanOrEqual(2);
      expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toLocal.leaseLapse.startedAt).toBe(first);
      f.advance(10_000);
      expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: 1 });
      expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled();
      expect(f.clock() - first).toBe(WINDOW);
      expect(f.logs.filter(line => line.includes(original.id) && /refused|dropped/.test(line))).toHaveLength(1);
    } finally { seam.mockRestore(); now.mockRestore(); await f.close(); }
  }, 30_000);

  it("retaining a returned prefix does not erase a later carrier boundary's first lapse", async () => {
    const f = await fixture();
    try {
      const prefix = f.envelope(randomUUID(), OTHER, FILE2, "prefix"), boundary = f.envelope();
      await f.pod.send(prefix); await f.pod.send(boundary); await f.pending(2); await f.lapse();
      const page = await f.link.remote.read(0);
      expect(page.events).toHaveLength(1); expect(page.events[0]!.id).toBe(prefix.id);
      const before = JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses[0].startedAt;
      // This is the same hook used by the REAL append-fence test above, on its returned prefix.
      f.link.remote.retainLeaseLapse(page.events[0]!, f.clock());
      const saved = JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses as Array<{ id: string; startedAt: number }>;
      expect(saved.map(lapse => lapse.id).sort()).toEqual([prefix.id, boundary.id].sort());
      await f.link.remote.read(0);
      expect(JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses.find((lapse: { id: string }) => lapse.id === boundary.id).startedAt).toBe(before);
      expect(f.events()).toEqual([]);
    } finally { await f.close(); }
  }, 30_000);

  it("restart with later-before-earlier redelivery keeps the original deadline and refuses at it", async () => {
    const f = await fixture();
    let refresh: ReturnType<typeof setInterval> | undefined;
    try {
      const original = f.envelope();
      await f.pod.send(original); await f.pending(); await f.lapse();
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      refresh = setInterval(() => { void f.lease(POD, true); }, 5_000);
      const saved = () => JSON.parse(fs.readFileSync(f.lapsePath, "utf8")).lapses as Array<{ id: string; startedAt: number }>;
      const first = saved().find(lapse => lapse.id === original.id)!.startedAt;
      // Reuse the review's real delayed NAK: B arrives while A is still pending at the broker.
      const originalDelivery = f.deliveries.find(delivery => delivery.envelope.id === original.id)!;
      originalDelivery.nak(20_000);
      await new Promise(resolve => setTimeout(resolve, 100));
      await f.restart(false); await f.link.remote.latestSequence();
      f.advance(10_000); f.hideTarget(true); await f.lease(OTHER);
      await vi.waitFor(() => expect(f.port.resolve(OTHER)?.route.live).toBe(true));
      const later = f.envelope(randomUUID(), OTHER, FILE, "later-wait");
      await f.pod.send(later);
      await vi.waitFor(() => expect(f.deliveries.some(delivery => delivery.envelope.id === later.id)).toBe(true), { timeout: 10_000 });
      expect(f.deliveries.filter(delivery => delivery.envelope.id === original.id)).toHaveLength(1);
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      // Continue through redelivery even on the broken base, to observe its renewed window too.
      expect.soft(saved().find(lapse => lapse.id === original.id)?.startedAt).toBe(first);
      await vi.waitFor(() => expect(f.deliveries.filter(delivery => delivery.envelope.id === original.id)).toHaveLength(2), { timeout: 25_000 });
      const redelivered = f.deliveries.filter(delivery => delivery.envelope.id === original.id)[1]!;
      await f.lease(OTHER);
      await vi.waitFor(() => expect(f.port.resolve(OTHER)?.route.live).toBe(true));
      expect(redelivered.streamSequence).toBe(originalDelivery.streamSequence);
      const term = vi.spyOn(redelivered, "term");
      f.advance(WINDOW - 1 - (f.clock() - first));
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect.soft(saved().find(lapse => lapse.id === original.id)?.startedAt).toBe(first);
      expect(term).not.toHaveBeenCalled();
      f.advance(1);
      // B still waits; A must nevertheless be terminal at its own original deadline.
      await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      expect(term).toHaveBeenCalledExactlyOnceWith(REFUSAL);
      expect(f.clock() - first).toBe(WINDOW);
      expect(f.logs.filter(line => line.includes(original.id) && line.includes("refused"))).toHaveLength(1);
      expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled();
    } finally { clearInterval(refresh); await f.close(); }
  }, 60_000);

  it("directory-not-ready uses the same deadline, including a first gap and a restarted established lapse", async () => {
    for (const established of [true, false]) {
      const f = await fixture();
      let unavailable: ReturnType<typeof vi.spyOn> | undefined;
      try {
        const original = f.envelope(); await f.pod.send(original); await f.pending();
        if (established) {
          await f.lapse();
          await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
          await f.restart();
        }
        unavailable = vi.spyOn(f.port, "resolve").mockImplementation(() => { throw new DirectoryNotReadyError("watch gap"); });
        await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
        expect(fs.existsSync(f.lapsePath), "an initial directory gap must persist its first lapse too").toBe(true);
        const retained = fs.readFileSync(f.lapsePath, "utf8");
        const first = JSON.parse(retained).lapses[0].startedAt as number;
        const delivery = f.deliveries.filter(delivery => delivery.envelope.id === original.id).at(-1)!;
        const term = vi.spyOn(delivery, "term");
        f.advance(WINDOW - 1 - (f.clock() - first));
        await expect(f.link.remote.read(0)).rejects.toBeInstanceOf(NatsCarrierDeferredError);
        expect(term).not.toHaveBeenCalled();
        expect(fs.readFileSync(f.lapsePath, "utf8")).toBe(retained);
        f.advance(1);
        await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
        expect(term).toHaveBeenCalledOnce();
        expect(f.clock() - first).toBe(WINDOW);
        expect(f.logs.filter(line => line.includes(original.id) && line.includes("refused") && line.includes("predicate=not-ready"))).toHaveLength(1);
        f.advance(60_000);
        await expect(f.link.remote.read(0)).resolves.toMatchObject({ events: [] });
        expect(term).toHaveBeenCalledOnce();
        expect(f.events()).toEqual([]); expect(f.receive).not.toHaveBeenCalled();
      } finally { unavailable?.mockRestore(); await f.close(); }
    }
  }, 60_000);

  it("working-extends beyond AckWait without exhausting MaxDeliver; expiry TERMs once with no redelivery loop", async () => {
    const f = await fixture();
    const nc = await broker.connect(broker.users.bootstrap); const { manager } = await openJetStream(nc);
    const durable = `br_${createHash("sha256").update(`bridge:${f.linkName}`).digest("hex").slice(0, 32)}`;
    let refresh: ReturnType<typeof setInterval> | undefined;
    try {
      const held = f.envelope(); await f.pod.send(held); await f.pending(); await f.lapse();
      await expect(f.link.bridge.step()).rejects.toBeInstanceOf(NatsCarrierDeferredError);
      const info = await manager.consumers.info("FABRIC_IN_machine-a", durable);
      expect(info.config).toMatchObject({ ack_wait: 30_000_000_000, max_deliver: 5, max_ack_pending: 128 });
      // Keep the validated, expired lease row present while testing a full real-time hold.
      refresh = setInterval(() => { void f.lease(POD, true); }, 5_000);
      await new Promise(resolve => setTimeout(resolve, 31_000));
      const before = await manager.consumers.info("FABRIC_IN_machine-a", durable);
      expect(before.num_redelivered).toBe(0); expect(before.num_ack_pending).toBe(1);
      f.advance(WINDOW);
      await f.link.bridge.step();
      await vi.waitFor(async () => expect((await manager.consumers.info("FABRIC_IN_machine-a", durable)).num_ack_pending).toBe(0));
      await f.link.bridge.step();
      expect(f.logs.filter(line => line.includes(held.id) && line.includes(REFUSAL))).toHaveLength(1);
      expect(f.events()).toEqual([]);
    } finally { clearInterval(refresh); await nc.close(); await f.close(); }
  }, 45_000);
});
