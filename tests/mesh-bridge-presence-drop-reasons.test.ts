import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { admitPresence, boundPresence, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { PRESENCE_DROP_REASONS, PRESENCE_KEY_CLASSES, PresenceDropReasons } from "../src/mesh/presence-drop-reasons.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Keep: data safety and the real raw-scan -> RPC -> partial-sync diagnostic path.
const roots: string[] = [];
const stores: MeshStore[] = [];
const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closes.splice(0)) await close();
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (family: string, id: string) => `topology/${family}/${createHash("sha256").update(id).digest("hex")}`;
const open = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-drop-reasons-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
  stores.push(store);
  return store;
};
const rootRecord = () => {
  const now = Date.now();
  const identity: MeshIdentity = { id: "session:PRIVATE-session-000001", name: "PRIVATE-actor", kind: "main", sessionId: "PRIVATE-session" };
  const host: FabricHostRecord = { format: 1, id: identity.id, rootId: identity.id, identity,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 };
  const participant: FabricParticipantRecord = { format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, ownerIncarnation: "PRIVATE-token", name: identity.name,
    label: "PRIVATE-label", status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
    sessionId: "PRIVATE-session", startedAt: now, updatedAt: now, controlProtocol: "v1" };
  return { identity, host, participant, presence: { hosts: [{ record: host, expiresAt: host.expiresAt }], participants: [participant] } };
};
const install = async (store: MeshStore, root: ReturnType<typeof rootRecord>) => {
  await store.put({ key: key("hosts", root.host.id), identity: root.identity, value: root.host });
  await store.put({ key: key("participants", root.participant.id), identity: root.identity, value: root.participant });
};
const link = () => {
  const hub = open(), far = open();
  const input = new PassThrough(), output = new PassThrough();
  const local = new StoreBridgeSide(hub, "PRIVATE-peer");
  const agent = new StoreBridgeSide(far, "PRIVATE-hub", () => Date.now(), false);
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  const logs: string[] = [];
  const bridge = new MeshBridge({ localName: "PRIVATE-hub", remoteName: "PRIVATE-peer", local, remote,
    cursorPath: path.join(hub.root, "cursor.json"), log: message => logs.push(message) });
  closes.push(async () => { await bridge.stop(); remote.close(); input.end(); await serving; output.end(); });
  return { hub, far, local, agent, remote, bridge, logs };
};

describe("value-free presence drop diagnostics", () => {
  it.each(["hosts", "participants"])("carries raw malformed %s through the actual RPC and preserves the log prefix", async family => {
    const f = link();
    await f.far.put({ key: `topology/${family}/PRIVATE-key`, identity: rootRecord().identity,
      value: { PRIVATE: "PRIVATE-payload" } });
    await f.bridge.syncPresence();
    expect(f.logs).toEqual([
      `presence: 1 dropped records; this sync is partial, omissions held; reasons: malformed:topology/${family}=1`,
    ]);
    expect(f.logs[0]).toMatch(/^presence: 1 dropped records; this sync is partial/);
    expect(f.logs[0]).not.toContain("PRIVATE");
  });

  it.each(["hosts", "participants"])("reports raw %s writer mismatches without writer identities", async family => {
    const store = open(), root = rootRecord();
    await install(store, root);
    await store.put({ key: key(family, root.identity.id), identity: { ...root.identity, id: "PRIVATE-writer" },
      value: family === "hosts" ? root.host : root.participant });
    const result = await new StoreBridgeSide(store, "PRIVATE-peer").presence();
    expect(result.droppedReasons).toContainEqual({ keyClass: `topology/${family}`, reason: "writer-mismatch", count: 1 });
    expect(JSON.stringify(result.droppedReasons)).not.toContain("PRIVATE");
  });

  it("reports conflicting shared-state/participant-file copies", async () => {
    const store = open(), root = rootRecord();
    await install(store, root);
    writeParticipantFile(store.root, { key: key("participants", root.identity.id), version: 1,
      updatedAt: root.participant.updatedAt + 1, updatedBy: root.identity,
      value: { ...root.participant, name: "PRIVATE-other-name", updatedAt: root.participant.updatedAt + 1 } });
    const result = await new StoreBridgeSide(store, "PRIVATE-peer").presence();
    expect(result.droppedReasons).toEqual([{ keyClass: "topology/participants", reason: "conflicting-copy", count: 1 }]);
  });

  it("reports raw participant/owner mismatches", async () => {
    const store = open(), root = rootRecord();
    await install(store, root);
    await store.put({ key: key("participants", root.identity.id), identity: root.identity,
      value: { ...root.participant, rootId: "PRIVATE-wrong-root" } });
    const result = await new StoreBridgeSide(store, "PRIVATE-peer").presence();
    expect(result.droppedReasons).toEqual([{ keyClass: "topology/participants", reason: "owner-mismatch", count: 1 }]);
  });

  it("reports admission failures and a participant missing its owner", () => {
    const root = rootRecord();
    expect(admitPresence({ hosts: [], participants: [root.participant] }, new Set()).droppedReasons)
      .toEqual([{ keyClass: "topology/participants", reason: "owner-mismatch", count: 1 }]);
    expect(admitPresence({ hosts: [null], participants: [null] } as never, new Set()).droppedReasons)
      .toEqual([{ keyClass: "topology/hosts", reason: "malformed", count: 1 },
        { keyClass: "topology/participants", reason: "malformed", count: 1 }]);
  });

  it("reports frame-budget drops without the oversized payload", () => {
    const result = boundPresence(rootRecord().presence, 1);
    expect(result.dropped).toBe(1);
    expect(result.presence.droppedReasons).toEqual([{ keyClass: "topology/hosts", reason: "frame-budget", count: 1 }]);
  });

  it("reports conflicting-root drops on the real sync path", async () => {
    const f = link(), root = rootRecord();
    await install(f.hub, root);
    const presence = f.local.presence.bind(f.local);
    vi.spyOn(f.local, "presence").mockImplementation(async () => {
      const value = await presence();
      return { ...value, hosts: [...value.hosts, ...value.hosts] };
    });
    await f.bridge.syncPresence();
    expect(f.logs).toEqual(["presence: 3 dropped records; this sync is partial, omissions held; reasons: " +
      "conflicting-root:topology/hosts=2 conflicting-root:topology/participants=1"]);
  });

  it("labels older-peer counts unattributed and rejects arbitrary peer labels/counts", () => {
    const value = { hosts: [], participants: [], droppedCount: 4, droppedReasons: [
      { keyClass: "topology/hosts/PRIVATE-id", reason: "malformed", count: 1 },
      { keyClass: "topology/hosts", reason: "PRIVATE-payload", count: 1 },
      { keyClass: "topology/hosts", reason: "malformed", count: -1 },
      { keyClass: "topology/hosts", reason: "malformed", count: Infinity },
    ] };
    const result = admitPresence(value as never, new Set());
    expect(result.droppedReasons).toEqual([{ keyClass: "presence", reason: "unattributed", count: 4 }]);
    expect(new PresenceDropReasons(result.droppedReasons, 4).summary()).toBe("unattributed:presence=4");
    expect(new PresenceDropReasons([{ keyClass: "presence", reason: "unattributed", count: 999 }], 4).summary())
      .toBe("unattributed:presence=4");
  });

  it("caps the log summary at eight enum/key classes, with an exact overflow class count", () => {
    const reasons = new PresenceDropReasons();
    for (const keyClass of PRESENCE_KEY_CLASSES) for (const reason of PRESENCE_DROP_REASONS) reasons.add(keyClass, reason);
    expect(reasons.entries()).toHaveLength(21);
    expect(reasons.summary().split(" ").slice(0, 8)).toEqual(reasons.entries().slice(0, 8)
      .map(({ keyClass, reason }) => `${reason}:${keyClass}=1`));
    expect(reasons.summary()).toMatch(/=1 \+13 more$/);
    expect(reasons.summary().match(/=1/g)).toHaveLength(8);
  });
});
