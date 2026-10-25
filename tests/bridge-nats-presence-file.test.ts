// L1 (smarty-dev#15310): the NATS side's presence() snapshot through the REAL file-mesh admission and
// mirror (MeshBridge admitPresence + StoreBridgeSide.mirror), read back with the real participant
// directory: a live v2 root appears on the hub as a live mirrored root of the link, nothing is dropped,
// and an expired NATS lease withdraws it on the next complete snapshot.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NatsBridgePresence } from "../src/mesh/bridge-nats-presence.js";
import { admitPresence, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { hKey, kvWorld, seedNative } from "./helpers/bridge-nats-kv.js";

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
  it("lands a live v2 root on the hub as the link's live mirrored root, and withdraws it when its lease is gone", async () => {
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

    // The v2 lease expires on the broker: the next complete snapshot omits the root and the hub withdraws it.
    w.leases.expire(hKey(V2, NATS_ROOT));
    await vi.waitFor(() => expect(side.resolve(NATS_ROOT)?.live).toBe(false));
    const later = await side.presence();
    expect(later.hosts).toEqual([]);
    await hub.mirror(admitPresence(later, new Set(later.reserved), LINK), Date.now());
    expect(reader.list({ scope: "project", kinds: ["root"], fresh: true }).some(participant => participant.id === NATS_ROOT)).toBe(false);
  });
});
