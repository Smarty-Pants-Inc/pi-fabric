// L1 (smarty-dev#15310): the NATS side's presence() snapshot through the REAL file-mesh admission and
// mirror (MeshBridge admitPresence + StoreBridgeSide.mirror), read back with the real participant
// directory: a live v2 root appears on the hub as a live mirrored root of the link, nothing is dropped,
// lease uncertainty retains its original deadline, and verified presence deletion withdraws it.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NatsBridgePresence } from "../src/mesh/bridge-nats-presence.js";
import { admitPresence, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { hKey, kvWorld, natsLease, pKey, seedNative } from "./helpers/bridge-nats-kv.js";

const V2 = "v2-org";
const LINK = "v2";
const NATS_ROOT = "session:22222222-2222-4222-8222-222222222222";
const PROOF = { pid: 4242, pidStartTime: "777", bootId: "12345678-1234-4234-8234-123456789abc" };
const temps: string[] = [];
const closers: Array<{ close(): Promise<void> | void }> = [];
afterEach(async () => {
  await Promise.allSettled(closers.splice(0).map(item => item.close()));
  for (const dir of temps.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

const natsRoot = (now: number): FabricParticipantRecord => ({ format: 1, id: NATS_ROOT, kind: "root", rootId: NATS_ROOT,
  ownerHostId: NATS_ROOT, ownerIdentityId: NATS_ROOT, ownerIncarnation: "nats-inc-1", name: "v2-main", status: "idle", runner: "pi",
  transport: "host", capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: now - 1_000, updatedAt: now,
  sessionId: NATS_ROOT.slice(8), cwd: "/work/v2" });

describe("NATS presence through the real file mirror", () => {
  it("holds the original mirror deadline when its lease is gone, and withdraws on verified presence deletion", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-presence-file-"));
    temps.push(dir);
    const mesh = new MeshStore(path.join(dir, "mesh"), 65_536, 1_000);
    closers.push({ close: () => mesh.closeState() });
    const w = kvWorld();
    seedNative(w, V2, natsRoot(Date.now()), Date.now());
    const side = new NatsBridgePresence({ transport: w.transport("ryzen1-bridge", "bridge:v2"), link: LINK,
      admit: (record, direction) => direction === "toFile" && record.id === NATS_ROOT, processProof: PROOF });
    closers.push(side);
    side.start();
    await side.whenReady();

    const claimed = await side.presence();
    const admitted = admitPresence(claimed, new Set(claimed.reserved), LINK);
    expect(admitted.partial).toBeUndefined();
    expect(admitted.droppedCount).toBeUndefined();
    expect(admitted.hosts.map(host => host.record.id)).toEqual([NATS_ROOT]);
    expect(admitted.participants.map(participant => participant.id)).toEqual([NATS_ROOT]);

    const hub = new StoreBridgeSide(mesh, LINK);
    await hub.mirror(admitted, Date.now());
    const reader = new ParticipantDirectory(mesh, { enabled: true, hostId: "reader", rootId: "reader",
      identity: { id: "reader", name: "reader", kind: "main" }, reapDeadHosts: false });
    const listed = reader.list({ scope: "project", kinds: ["root"], fresh: true }).find(participant => participant.id === NATS_ROOT);
    expect(listed).toMatchObject({ id: NATS_ROOT, remoteHost: LINK, ownerIncarnation: "nats-inc-1", stale: false, name: "v2-main" });
    expect((await hub.owned()).participants.map(participant => participant.id)).toEqual([NATS_ROOT]);

    const original = (await hub.owned()).hosts.find(host => host.record.id === NATS_ROOT)!;
    // Lease disappearance alone is not membership withdrawal: hold the mirror without renewing it.
    w.leases.expire(hKey(V2, NATS_ROOT));
    await vi.waitFor(() => expect(side.resolve(NATS_ROOT)?.live).toBe(false));
    const uncertain = await side.presence();
    expect(uncertain.hosts).toEqual([]);
    expect(uncertain.partial).toBe(true);
    await hub.mirror(admitPresence(uncertain, new Set(uncertain.reserved), LINK), Date.now());
    const held = await hub.owned();
    expect(held.participants.map(participant => participant.id)).toEqual([NATS_ROOT]);
    expect(held.hosts.find(host => host.record.id === NATS_ROOT)?.expiresAt).toBe(original.expiresAt);

    // A verified presence deletion still permits complete omission and canonical withdrawal.
    w.presence.expire(pKey(V2, NATS_ROOT));
    await vi.waitFor(() => expect(side.resolve(NATS_ROOT)).toBeUndefined());
    const deleted = await side.presence();
    expect(deleted.partial).toBeUndefined();
    expect(deleted.hosts).toEqual([]);
    await hub.mirror(admitPresence(deleted, new Set(deleted.reserved), LINK), Date.now());
    expect(reader.list({ scope: "project", kinds: ["root"], fresh: true }).some(participant => participant.id === NATS_ROOT)).toBe(false);
  });

  it("holds the hub's mirror of a v2 root whose re-created lease awaits a root re-check, and renews it once the flag is gone", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-nats-presence-file-"));
    temps.push(dir);
    const mesh = new MeshStore(path.join(dir, "mesh"), 65_536, 1_000);
    closers.push({ close: () => mesh.closeState() });
    const w = kvWorld();
    seedNative(w, V2, natsRoot(Date.now()), Date.now());
    const side = new NatsBridgePresence({ transport: w.transport("ryzen1-bridge", "bridge:v2"), link: LINK,
      admit: (record, direction) => direction === "toFile" && record.id === NATS_ROOT, processProof: PROOF });
    closers.push(side);
    side.start();
    await side.whenReady();
    const hub = new StoreBridgeSide(mesh, LINK);
    const first = await side.presence();
    await hub.mirror(admitPresence(first, new Set(first.reserved), LINK), Date.now());
    expect((await hub.owned()).participants.map(participant => participant.id)).toEqual([NATS_ROOT]);

    // The v2 lease lapsed in KV and its owner created it again with rootRecheck: not routable, not gone.
    const key = hKey(V2, NATS_ROOT);
    const owner = `${NATS_ROOT}#nats-inc-1`;
    w.leases.expire(key);
    w.leases.put(key, owner, { ...natsLease(NATS_ROOT, "nats-inc-1", Date.now()), rootRecheck: true }, 0);
    await vi.waitFor(() => expect(side.reader.lease(V2, NATS_ROOT)?.entry.value.rootRecheck).toBe(true));
    expect(side.resolve(NATS_ROOT)?.live).toBe(false);
    const during = await side.presence();
    expect(during.hosts).toEqual([]);
    const admitted = admitPresence(during, new Set(during.reserved), LINK);
    expect(admitted.partial).toBe(true);
    const held = (await hub.owned()).hosts.find(host => host.record.id === NATS_ROOT)!;
    await hub.mirror(admitted, Date.now());
    // No withdrawal: the mirror stays, unrenewed (its own deadline still bounds it).
    const kept = await hub.owned();
    expect(kept.participants.map(participant => participant.id)).toEqual([NATS_ROOT]);
    expect(kept.hosts.find(host => host.record.id === NATS_ROOT)?.expiresAt).toBe(held.expiresAt);

    // The owner re-checked its root and rewrote the lease without the flag: mirrored live again.
    await new Promise(resolve => setTimeout(resolve, 5));
    w.leases.put(key, owner, natsLease(NATS_ROOT, "nats-inc-1", Date.now()), w.leases.values.get(key)!.revision);
    await vi.waitFor(() => expect(side.resolve(NATS_ROOT)?.live).toBe(true));
    const after = await side.presence();
    expect(after.partial).toBeUndefined();
    expect(after.hosts.map(host => host.record.id)).toEqual([NATS_ROOT]);
    await hub.mirror(admitPresence(after, new Set(after.reserved), LINK), Date.now());
    const reader = new ParticipantDirectory(mesh, { enabled: true, hostId: "reader", rootId: "reader",
      identity: { id: "reader", name: "reader", kind: "main" }, reapDeadHosts: false });
    expect(reader.list({ scope: "project", kinds: ["root"], fresh: true }).find(participant => participant.id === NATS_ROOT))
      .toMatchObject({ id: NATS_ROOT, remoteHost: LINK, stale: false });
  });
});
