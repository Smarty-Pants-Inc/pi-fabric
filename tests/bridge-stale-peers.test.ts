import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MIRROR_FRESH_MS, readHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");

// Keep reason: recorded stale-routing incident; peers must not hide the state exposed by get/sessions (#19315).
describe("bridge-stale peer diagnostics (#19315)", () => {
  const roots: string[] = [];
  const stores: MeshStore[] = [];
  const directories: ParticipantDirectory[] = [];
  afterEach(async () => {
    try {
      await Promise.all(directories.splice(0).map(directory => directory.close()));
    } finally {
      try {
        for (const store of stores.splice(0)) store.closeState();
      } finally {
        for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
      }
    }
  });

  it("retains mirror staleness on peers until renewal, but drops a fresh source-expired root", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-stale-peers-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 0);
    stores.push(mesh);
    const remoteId = "session:remote-bridge-root";
    const nativeId = "session:native-peer";
    const observerId = "session:observer";
    const remoteIdentity: MeshIdentity = { id: remoteId, name: "remote", kind: "main", sessionId: "remote-bridge-root" };
    const nativeIdentity: MeshIdentity = { id: nativeId, name: "native", kind: "main", sessionId: "native-peer" };
    let now = Date.now();
    const startedAt = now - 120_000;
    const remoteParticipant: FabricParticipantRecord = {
      format: 1,
      id: remoteId,
      kind: "root",
      rootId: remoteId,
      ownerHostId: remoteId,
      ownerIdentityId: remoteId,
      name: "remote",
      status: "running",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "fabric"],
      controlProtocol: "v1",
      cwd: "/workspace/remote",
      sessionId: "remote-bridge-root",
      startedAt,
      updatedAt: now,
    };
    const nativeParticipant: FabricParticipantRecord = {
      format: 1,
      id: nativeId,
      kind: "root",
      rootId: nativeId,
      ownerHostId: nativeId,
      ownerIdentityId: nativeId,
      name: "native",
      status: "running",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "fabric"],
      controlProtocol: "v1",
      cwd: "/workspace/native",
      sessionId: "native-peer",
      startedAt,
      updatedAt: now,
    };
    const nativeDirectory = new ParticipantDirectory(mesh, {
      enabled: true,
      hostId: nativeId,
      rootId: nativeId,
      identity: nativeIdentity,
      heartbeatMs: 60_000,
      leaseMs: 120_000,
    });
    directories.push(nativeDirectory);
    nativeDirectory.registerSource(() => [nativeParticipant]);
    await nativeDirectory.refresh();

    const remoteHost = {
      format: 1 as const,
      id: remoteId,
      rootId: remoteId,
      identity: remoteIdentity,
      incarnationToken: "remote-incarnation-1",
      startedAt,
      updatedAt: now,
      expiresAt: now + 300_000,
    };
    const presence = (observedAt: number, sourceExpiresAt: number): BridgePresence => ({
      hosts: [{
        record: { ...remoteHost, updatedAt: observedAt, expiresAt: sourceExpiresAt },
        expiresAt: sourceExpiresAt,
        observedAt,
      }],
      participants: [{ ...remoteParticipant, updatedAt: observedAt }],
      reserved: [],
    });
    const bridge = new StoreBridgeSide(mesh, "forge", () => now);
    const directory = new ParticipantDirectory(mesh, {
      enabled: true,
      hostId: observerId,
      rootId: observerId,
      identity: { id: observerId, name: "observer", kind: "main" },
      reapDeadHosts: false,
    });
    directories.push(directory);
    const findSession = (id: string) => directory.sessions(now).find(participant => participant.id === id);
    const findPeer = (id: string) => directory.peers(now).find(peer => peer.id === id);

    await bridge.mirror(presence(now, now + 300_000), now);
    expect(directory.get(remoteId, now, { fresh: true })).toMatchObject({ status: "running", stale: false });
    expect(findSession(remoteId)).not.toHaveProperty("bridgeStale");
    expect(findPeer(remoteId)).toMatchObject({ id: remoteId, status: "running", host: "forge" });
    expect(findPeer(remoteId)).not.toHaveProperty("bridgeStale");
    expect(findPeer(nativeId)).toMatchObject({ id: nativeId, status: "running" });
    expect(findPeer(nativeId)).not.toHaveProperty("bridgeStale");

    // A short translated deadline can expire while the bridge observation is still fresh.
    // At the exact deadline it is uncertainty, not source death or a routable live root.
    now += 1;
    const observedAt = now;
    const translatedExpiry = now + 1_000;
    await bridge.mirror(presence(observedAt, translatedExpiry), observedAt);
    expect(readHostLease(mesh.root, remoteId)).toMatchObject({ expiresAt: translatedExpiry });
    now = translatedExpiry;
    expect(now - observedAt).toBeLessThan(MIRROR_FRESH_MS);
    expect(directory.get(remoteId, now, { fresh: true })).toMatchObject({ status: "running", stale: false, bridgeStale: true });
    expect(findSession(remoteId)).toMatchObject({ status: "running", stale: false, bridgeStale: true });
    expect(findPeer(remoteId)).toMatchObject({ status: "running", bridgeStale: true });
    expect(findPeer(nativeId)).toMatchObject({ status: "running" });
    expect(findPeer(nativeId)).not.toHaveProperty("bridgeStale");

    now += 1;
    await bridge.mirror(presence(now, now + 300_000), now);
    expect(directory.get(remoteId, now, { fresh: true })).toMatchObject({ status: "running", stale: false });
    expect(findSession(remoteId)).not.toHaveProperty("bridgeStale");
    expect(findPeer(remoteId)).not.toHaveProperty("bridgeStale");
    expect(findPeer(nativeId)).not.toHaveProperty("bridgeStale");

    now += MIRROR_FRESH_MS + 1;
    expect(directory.get(remoteId, now, { fresh: true })).toMatchObject({ status: "running", stale: false, bridgeStale: true });
    expect(findSession(remoteId)).toMatchObject({ status: "running", stale: false, bridgeStale: true });
    expect(findPeer(remoteId)).toMatchObject({ status: "running", bridgeStale: true });
    expect(findPeer(nativeId)).not.toHaveProperty("bridgeStale");

    now += 1;
    await bridge.mirror(presence(now, now - 1), now);
    expect(directory.get(remoteId, now, { fresh: true })).toBeUndefined();
    expect(findSession(remoteId)).toBeUndefined();
    expect(findPeer(remoteId)).toBeUndefined();
    expect(findPeer(nativeId)).not.toHaveProperty("bridgeStale");

    now += 1_000;
    await bridge.mirror(presence(now, now + 300_000), now);
    expect(directory.get(remoteId, now, { fresh: true })).toMatchObject({ status: "running", stale: false });
    expect(findSession(remoteId)).not.toHaveProperty("bridgeStale");
    expect(findPeer(remoteId)).toMatchObject({ status: "running", host: "forge" });
    expect(findPeer(remoteId)).not.toHaveProperty("bridgeStale");
    expect(findPeer(nativeId)).not.toHaveProperty("bridgeStale");

    expect(mesh.get(key("topology/participants/", nativeId), { fresh: true })).toBeDefined();
  });
});
