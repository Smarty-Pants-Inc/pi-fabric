import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";

const roots: string[] = [];
const stores: MeshStore[] = [];
const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closes.splice(0)) await close();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (family: string, id: string) => `topology/${family}/${createHash("sha256").update(id).digest("hex")}`;
const record = (tag: string) => {
  const now = Date.now();
  const id = `session:conflict-test-${tag}`;
  const identity: MeshIdentity = { id, name: tag, kind: "main", sessionId: `conflict-test-${tag}` };
  const host: FabricHostRecord = { format: 1, id, rootId: id, identity, incarnationToken: "synthetic-owner-token",
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 };
  const participant: FabricParticipantRecord = { format: 1, id, kind: "root", rootId: id,
    ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "synthetic-owner-token", name: tag,
    status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], sessionId: `conflict-test-${tag}`,
    startedAt: now, updatedAt: now, controlProtocol: "v1" };
  return { identity, host, participant };
};
const install = async (store: MeshStore, root: ReturnType<typeof record>) => {
  await store.put({ key: key("hosts", root.host.id), identity: root.identity, value: root.host });
  await store.put({ key: key("participants", root.participant.id), identity: root.identity, value: root.participant });
};
const writeOwnerLease = (store: MeshStore, root: ReturnType<typeof record>, expiresAt = Date.now() + 120_000,
  sessionExpiresAt = expiresAt) => {
  const updatedAt = Date.now();
  writeHostLease(store.root, { id: root.host.id, rootId: root.host.rootId, identityId: root.identity.id,
    incarnationToken: root.host.incarnationToken!, startedAt: root.host.startedAt, updatedAt, expiresAt,
    session: { id: root.participant.sessionId!, startedAt: root.participant.startedAt, updatedAt, expiresAt: sessionExpiresAt } });
};
const fixture = () => {
  const open = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-copy-conflict-main-"));
    roots.push(root);
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
    stores.push(store);
    return store;
  };
  const hub = open(), far = open();
  const input = new PassThrough(), output = new PassThrough();
  const local = new StoreBridgeSide(hub, "far", Date.now, false);
  const agent = new StoreBridgeSide(far, "hub", Date.now, false);
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(hub.root, "cursor.json") });
  closes.push(async () => { await bridge.stop(); remote.close(); input.end(); await serving; output.end(); });
  return { hub, far, agent, bridge };
};

describe("lease-authorized root status conflicts", () => {
  it("exports the latest status-only copy while its owner lease is fresh", async () => {
    const f = fixture(), root = record("status-authority");
    await install(f.far, root);
    writeOwnerLease(f.far, root);
    const participantKey = key("participants", root.participant.id);
    const entry = f.far.get(participantKey)!;
    writeParticipantFile(f.far.root, { ...entry, version: entry.version + 1, updatedAt: entry.updatedAt + 1,
      value: { ...root.participant, status: "running" } });

    await f.bridge.syncPresence();
    expect(f.hub.get(participantKey)?.value).toMatchObject({ id: root.participant.id, status: "running" });
  });

  it("still omits an identity/incarnation conflict", async () => {
    const f = fixture(), root = record("identity-authority");
    await install(f.far, root);
    writeOwnerLease(f.far, root);
    const participantKey = key("participants", root.participant.id);
    const entry = f.far.get(participantKey)!;
    writeParticipantFile(f.far.root, { ...entry, version: entry.version + 1, updatedAt: entry.updatedAt + 1,
      value: { ...root.participant, ownerIncarnation: "different-owner-incarnation" } });

    await f.bridge.syncPresence();
    expect(f.hub.get(participantKey)).toBeUndefined();
  });

  it("does not export when a different identity has a second fresh lease for the root", async () => {
    const f = fixture(), root = record("ambiguous-distinct-identity");
    await install(f.far, root);
    writeOwnerLease(f.far, root);
    const secondLeaseId = "session:competing-owner";
    const otherIdentity: MeshIdentity = { id: secondLeaseId, name: "other", kind: "main", sessionId: "competing-session" };
    await f.far.put({ key: key("hosts", secondLeaseId), identity: otherIdentity, value: {
      format: 1, id: secondLeaseId, rootId: root.host.rootId, identity: otherIdentity,
      startedAt: Date.now() - 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000,
    } satisfies FabricHostRecord });
    writeHostLease(f.far.root, { id: secondLeaseId, rootId: root.host.rootId,
      identityId: otherIdentity.id, incarnationToken: "competing-token", startedAt: Date.now() - 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 120_000 });
    const participantKey = key("participants", root.participant.id);
    const entry = f.far.get(participantKey)!;
    writeParticipantFile(f.far.root, { ...entry, version: entry.version + 1, updatedAt: entry.updatedAt + 1,
      value: { ...root.participant, status: "running" } });

    await f.bridge.syncPresence();
    expect(f.hub.get(participantKey)).toBeUndefined();
  });

  it("does not export after the lease expires even while the host record remains live", async () => {
    const f = fixture(), root = record("expired-owner");
    await install(f.far, root);
    writeOwnerLease(f.far, root, Date.now() - 1, Date.now() + 120_000);
    const ownerLease = readHostLeaseCurrent(f.far.root, root.host.id)!;
    expect(ownerLease.expiresAt).toBeLessThanOrEqual(Date.now());
    expect(ownerLease.session?.expiresAt).toBeGreaterThan(Date.now());
    // Check independent host liveness before creating a conflict: once a conflicting copy is
    // found, presence intentionally suppresses both that participant and its same-id host.
    const sourcePresence = await f.agent.presence();
    expect(sourcePresence.hosts.map(host => host.record.id)).toContain(root.host.id);

    const participantKey = key("participants", root.participant.id);
    const entry = f.far.get(participantKey)!;
    writeParticipantFile(f.far.root, { ...entry, version: entry.version + 1, updatedAt: entry.updatedAt + 1,
      value: { ...root.participant, status: "running" } });
    await f.bridge.syncPresence();
    expect(f.hub.get(participantKey)).toBeUndefined();
  });
});
