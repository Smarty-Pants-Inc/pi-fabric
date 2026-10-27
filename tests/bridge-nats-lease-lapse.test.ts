// expected-skip: no NATS_BIN/PATH executable: an authenticated loopback broker is required.
// Keep: command loss/data safety at the NATS -> FILE ownership fence. Real broker, carrier,
// durable ledger, append receipts and owner control plane; only the local mirror expiry is injected.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { createNatsFileLink } from "../src/mesh/bridge-nats-file.js";
import { openBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger } from "../src/mesh/bridge-nats-link.js";
import { openNatsBridgeSide } from "../src/mesh/bridge-nats-port.js";
import { natsLeaseKey, natsPresenceKey } from "../src/mesh/bridge-nats-presence.js";
import { MeshStore } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsTransport } from "../src/transport/nats/index.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: lease lapse needs NATS_BIN");
const FILE = "session:11111111-1111-4111-8111-111111111111";
const POD = "session:22222222-2222-4222-8222-222222222222";
const LINK = "test-link";
const identity = { id: FILE, name: "recipient", kind: "main" as const };
const stateKey = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const rootRecord = (id: string, incarnation: string): FabricParticipantRecord => ({
  format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
  name: id === FILE ? "recipient" : "sender", status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now(),
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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-lease-lapse-"));
  const store = new MeshStore(path.join(dir, "mesh"), 256 * 1024, 1_000, { stateBackend: "file" });
  const pod = await openNatsTransport({ ...broker.runtimeOptions(broker.users.machineB), hostId: POD });
  const leaseKey = natsLeaseKey("machine-b", POD), presenceKey = natsPresenceKey("machine-b", POD);
  let leaseRevision = (await pod.kv("leases").get(leaseKey))?.revision ?? 0;
  const priorPresence = (await pod.kv("presence").get(presenceKey))?.revision ?? 0;
  const renew = async () => {
    const now = Date.now();
    leaseRevision = await pod.kv("leases").put(leaseKey, `${POD}#pod-1`, {
      id: POD, rootId: POD, identityId: POD, incarnation: "pod-1", incarnationToken: "pod-1",
      pid: process.pid, pidStartTime: "1", bootId: "12345678-1234-4234-8234-123456789abc",
      updatedAt: now, expiresAt: now + 15_000,
    }, leaseRevision);
  };
  await renew();
  const presenceRevision = await pod.kv("presence").put(presenceKey, `${POD}#pod-1`, rootRecord(POD, "pod-1"), priorPresence);
  const runtime = broker.runtimeOptions(broker.users.machineA);
  const port = await openNatsBridgeSide({ link: LINK,
    nats: { servers: [String(runtime.servers)], machine: "machine-a", credentials: runtime.credentials,
      caFile: broker.tls!.caFile, ...PATH_B_PERMISSION_POLICY },
    allow: { file: [{ kind: "root", id: FILE }], nats: [{ kind: "root", id: POD, machine: "machine-b" }] },
  });
  const now = Date.now();
  await store.put({ key: stateKey("topology/hosts/", FILE), identity, value: {
    format: 1, id: FILE, rootId: FILE, identity, incarnationToken: "file-1", startedAt: now - 1_000,
    updatedAt: now, expiresAt: now + 600_000,
  } });
  await store.put({ key: stateKey("topology/participants/", FILE), identity, value: rootRecord(FILE, "file-1") });
  const ledger = await openBridgeNatsLedger({ dir: path.join(dir, "ledger") });
  const logs: string[] = [], cursorPath = path.join(dir, "cursor.json");
  const link = createNatsFileLink({ store, port, localName: "file", link: LINK, cursorPath,
    ledger: natsCarrierLedger(ledger, LINK), authorize: () => ({ ok: true, principal: "carry" }),
    authorizeTopic: () => ({ ok: true, principal: "none" }), mirrorAllowed: () => true, log: line => logs.push(line),
  });
  await vi.waitFor(() => expect(port.resolve(POD)?.route.live).toBe(true));
  await link.bridge.start();
  const receive = vi.fn(() => ({ accepted: true, messageId: "received-once" }));
  const owner = new FabricControlPlane(store, identity, { enabled: true, hostId: FILE, ownerIncarnation: "file-1", pollMs: 10 });
  owner.start(receive);
  const events = () => store.read({ topic: "fabric.control.command", after: 0, limit: 100 });
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toLocal: { after: number } };
  let lapsed = true, attempts = 0;
  const publish = link.local.publish.bind(link.local);
  const seam = vi.spyOn(link.local, "publish").mockImplementation(async (...args) => {
    if (lapsed && args[0].topic === "fabric.control.command") {
      attempts++;
      const lease = readHostLease(store.root, POD)!;
      // The source KV lease remains live. Fail the real local append fence, not a fabricated error.
      writeHostLease(store.root, { ...lease, expiresAt: Date.now() - 1 });
    }
    return publish(...args);
  });
  const envelope = (id: string, message = "same-key-body"): TransportEnvelope => ({
    id, kind: "command", to: { machine: "machine-a", hostId: FILE }, targetId: FILE, operation: "followUp",
    from: { id: POD, name: "sender", kind: "main" }, body: {
      version: 1, commandId: "same-logical-command", targetId: FILE, operation: "followUp", ownerIncarnation: "file-1",
      message, replyTo: POD, requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 300_000,
    },
  });
  const abort = new AbortController(), replies: TransportEnvelope[] = [];
  const reading = (async () => {
    for await (const reply of pod.inbox(abort.signal)) {
      replies.push(reply.payload);
      await reply.ack();
    }
  })();
  const close = async () => {
    seam.mockRestore(); abort.abort(); await reading;
    await owner.close(); await link.bridge.stop(); await port.close(); await ledger.close();
    await pod.kv("leases").delete(leaseKey, leaseRevision);
    await pod.kv("presence").delete(presenceKey, presenceRevision);
    await pod.close(); store.closeState();
  };
  return { store, port, pod, link, ledger, logs, events, cursor, receive, envelope, replies, renew, close,
    recover: () => { lapsed = false; }, attempts: () => attempts };
};

describe.skipIf(!binary)("NATS lease-lapse same-key delivery", () => {
  it.each([false, true])("retries the same logical command after a lapse (historical consumed cursor=%s), then replays success", async historical => {
    const f = await fixture();
    // Reproduce a pre-upgrade consumed refusal without changing cursor bytes or disabling dedupe.
    const classifier = historical && typeof f.link.local.leaseLapsed === "function"
      ? vi.spyOn(f.link.local, "leaseLapsed").mockReturnValue(false) : undefined;
    try {
      const originalId = randomUUID();
      await f.pod.send(f.envelope(originalId));
      await vi.waitFor(async () => expect(await f.link.remote.latestSequence()).toBeGreaterThan(0), { timeout: 5_000, interval: 25 });
      const before = f.cursor().toLocal.after;
      expect(await f.link.bridge.step()).toMatchObject({ toLocal: 0, dropped: historical ? 1 : 0 });
      expect(f.attempts()).toBe(2);
      expect(f.cursor().toLocal.after).toBe(historical ? before + 1 : before);
      expect(f.events()).toEqual([]);
      expect(f.receive).not.toHaveBeenCalled();
      classifier?.mockRestore();
      // A genuine control-plane retry uses a new envelope id with the SAME commandId/key.
      const retryId = randomUUID();
      await f.pod.send(f.envelope(retryId));
      f.recover(); await f.renew(); await f.link.bridge.syncPresence();
      await vi.waitFor(async () => {
        await f.link.bridge.step();
        expect(f.replies.some(reply => (reply.body as { accepted?: boolean })?.accepted === true)).toBe(true);
      }, { timeout: 5_000, interval: 25 });
      expect(f.events()).toHaveLength(1);
      expect(f.receive).toHaveBeenCalledOnce();
      expect(f.events()[0]!.data).toMatchObject({ message: "same-key-body",
        bridge: { id: historical ? retryId : originalId } });
      // The durable carrier ledger replays the owner's answer for a post-success same-key retry.
      const replayId = randomUUID();
      await f.pod.send(f.envelope(replayId));
      await vi.waitFor(async () => {
        await f.link.bridge.step();
        expect(f.replies).toContainEqual(expect.objectContaining({ id: `ack:${replayId}`,
          body: expect.objectContaining({ accepted: true, messageId: "received-once" }) }));
      }, { timeout: 5_000, interval: 25 });
      expect(f.receive).toHaveBeenCalledOnce();
      expect(f.events()).toHaveLength(1);
      expect(f.logs.filter(line => line.includes(originalId) && /refused|dropped/.test(line))).toHaveLength(historical ? 1 : 0);
    } finally { classifier?.mockRestore(); await f.close(); }
  }, 30_000);
});
