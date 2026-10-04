import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import { projectOf } from "../src/topology/project-identity.js";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import { LIVENESS_POLICY_KEY, readHostLeases, removeHostLease, STATE_LEASE_RENEW_MS, writeHostLease } from "../src/topology/host-leases.js";
import { MainAgentController } from "../src/main-agent.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { awaitPeerSettle, type PeerSettleResult } from "../src/topology/peer-settle.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];

const rootRecord = (
  id: string,
  hostId: string,
  sessionId: string,
): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "root",
  rootId: id,
  ownerHostId: hostId,
  ownerIdentityId: hostId,
  name: "main",
  status: "idle",
  runner: "pi",
  transport: "host",
  capabilities: ["steer", "followUp", "fabric"],
  cwd: "/tmp/project",
  sessionId,
  startedAt: 1,
  updatedAt: 2,
  pendingMessages: false,
  controlProtocol: "v1",
});

const agentRecord = (
  id: string,
  rootId: string,
  hostId: string,
  parentId: string,
): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "agent",
  rootId,
  ownerHostId: hostId,
  ownerIdentityId: hostId,
  parentId,
  name: id,
  status: "running",
  runner: "pi",
  transport: "process",
  capabilities: ["steer", "followUp", "stop"],
  cwd: "/tmp/project",
  startedAt: 3,
  updatedAt: 4,
  controlProtocol: "v1",
});

const createDirectory = (
  meshRoot: string,
  identity: MeshIdentity,
  rootId: string,
  source: () => FabricParticipantRecord[],
  // smarty-dev#883: a test that does not exercise expiry uses a lease a starved Windows runner, whose
  // timers stall, cannot outlast between a write and the read that follows it. The directory makes
  // the lease at least two heartbeats.
  lease: { heartbeatMs: number; leaseMs: number } = { heartbeatMs: 100, leaseMs: 300 },
): ParticipantDirectory => {
  const hostId = identity.kind === "main" ? identity.id : "runtime:" + identity.sessionId;
  const directory = new ParticipantDirectory(
    new MeshStore(meshRoot, 64 * 1024, 1_000),
    { enabled: true, hostId, rootId, identity, ...lease },
  );
  directory.registerSource(source);
  directories.push(directory);
  return directory;
};

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("#3662 ParticipantDirectory lineage liveness", () => {
  it.each([null, {}, { format: 1, id: "session:lineage", kind: "invalid" }])(
    "S1 retains invalid raw shared-state lineage %j until withdrawal", async (value) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-raw-"));
      roots.push(root);
      const identity: MeshIdentity = { id: "observer", name: "main", kind: "main" };
      const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => []);
      const id = "session:lineage";
      const key = "topology/participants/" + createHash("sha256").update(id).digest("hex");
      await directory.mesh.put({ key, identity, value });
      expect(directory.get(id, Date.now(), { fresh: true })).toBeUndefined();
      expect(directory.lastKnown(id)).toBeUndefined();
      expect(directory.lineageAlive(id)).toBe(true);
      await directory.mesh.delete({ key });
      expect(directory.lineageAlive(id)).toBe(true); // Removal alone is not a close receipt.
    },
  );

  it("S1 treats a failed raw lineage read as unknown, never dead", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-read-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "observer", name: "main", kind: "main" };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => []);
    const read = vi.spyOn(directory.mesh, "get").mockImplementation(() => { throw new Error("unreadable state"); });
    try {
      expect(directory.lineageAlive("session:lineage")).toBe(true);
    } finally {
      read.mockRestore();
    }
    expect(directory.lineageAlive("session:lineage")).toBe(true);
  });

  it("does not treat lease expiry as lineage death, but observes withdrawal", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-"));
    roots.push(root);
    const id = "session:lineage";
    const directory = createDirectory(path.join(root, "mesh"), { id, name: "main", kind: "main" }, id,
      () => [rootRecord(id, id, "lineage")]);
    await directory.refresh();
    const expiredAt = Date.now() + 120_000;
    expect(directory.get(id, expiredAt)).toBeUndefined();
    expect(directory.lastKnown(id, expiredAt)?.participant.stale).toBe(true);
    expect(directory.lineageAlive(id, expiredAt)).toBe(true);
    expect(directory.lineageAlive("session:unknown", expiredAt)).toBe(true);
    await directory.closeLineage();
    expect(directory.lineageAlive(id)).toBe(false);
  });
});
describe("ParticipantDirectory.mirroredControlOwner", () => {
  const keyFor = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
  const setup = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mirror-owner-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const writer = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const reader = new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: 60_000 });
    const localIdentity: MeshIdentity = { id: "local-identity", name: "main", kind: "main" };
    const identity: MeshIdentity = { id: "remote-identity", name: "main", kind: "main" };
    const directory = new ParticipantDirectory(reader, {
      enabled: true, hostId: "local-host", rootId: "local-root", identity: localIdentity,
    });
    const participant = {
      ...rootRecord("remote-root", "remote-host", "remote-session"),
      ownerIdentityId: identity.id, remoteHost: "forge",
    };
    const host = {
      format: 1, id: "remote-host", rootId: participant.id, identity,
      startedAt: 1, updatedAt: 2, expiresAt: Date.now() + 15_000, remoteHost: "forge",
    };
    const participantKey = keyFor("topology/participants/", participant.id);
    const hostKey = keyFor("topology/hosts/", host.id);
    const putParticipant = (patch: Record<string, unknown> = {}, author = identity) => writer.put({
      key: participantKey, value: { ...participant, ...patch }, identity: author,
    });
    const putHost = (patch: Record<string, unknown> = {}, author = identity) => writer.put({
      key: hostKey, value: { ...host, ...patch }, identity: author,
    });
    await putHost();
    await putParticipant();
    const read = () => directory.mirroredControlOwner(host.id, identity.id, participant.id);
    return { meshRoot, writer, reader, directory, identity, participant, host, participantKey, hostKey, putParticipant, putHost, read };
  };

  it("exposes the exact public read port and retains fresh expiry knowledge from first send through lapse", async () => {
    const { directory, reader, host, putHost, read } = await setup();
    expect(typeof ParticipantDirectory.prototype.mirroredControlOwner).toBe("function");
    const port: (host: string, identity: string | undefined, target: string) =>
      { remoteHost: string; expiresAt: number } | undefined = directory.mirroredControlOwner.bind(directory);
    expect(port(host.id, undefined, host.rootId)).toBeUndefined();
    const publish = vi.spyOn(reader, "publish");
    const put = vi.spyOn(reader, "put");
    try {
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: host.expiresAt });
      const renewed = host.expiresAt + 30_000;
      await putHost({ expiresAt: renewed });
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: renewed });
      const expired = Date.now() - 1_000;
      await putHost({ expiresAt: expired });
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: expired });
      expect(directory.get(host.rootId, Date.now(), { fresh: true })).toBeUndefined();
      expect(publish).not.toHaveBeenCalled();
      expect(put).not.toHaveBeenCalled();
    } finally {
      publish.mockRestore();
      put.mockRestore();
    }
  });

  it("uses later matching file expiry, and observes file renewals and removal", async () => {
    const { meshRoot, host, read } = await setup();
    const lease = {
      id: host.id, rootId: host.rootId, identityId: host.identity.id,
      updatedAt: Date.now(), expiresAt: host.expiresAt + 30_000,
    };
    const leaseFile = path.join(meshRoot, "host-leases",
      createHash("sha256").update(host.id).digest("hex").slice(0, 32) + ".json");
    const replaceLease = (expiresAt: number) => {
      writeHostLease(meshRoot, { ...lease, updatedAt: Date.now(), expiresAt });
    };
    replaceLease(lease.expiresAt);
    expect(read()).toEqual({ remoteHost: "forge", expiresAt: lease.expiresAt });
    replaceLease(lease.expiresAt + 100_000);
    expect(read()?.expiresAt).toBe(lease.expiresAt + 100_000);
    replaceLease(host.expiresAt - 1);
    expect(read()?.expiresAt).toBe(host.expiresAt);
    // Removal must discard a cached lease that still outranks state, not merely a shorter one.
    replaceLease(lease.expiresAt + 100_000);
    expect(read()?.expiresAt).toBe(lease.expiresAt + 100_000);
    removeHostLease(meshRoot, host.id);
    expect(fs.existsSync(leaseFile)).toBe(false);
    expect(read()?.expiresAt).toBe(host.expiresAt);
  });

  it.each(["rootId", "identityId"])("ignores a later file lease with mismatched %s", async (field) => {
    const { meshRoot, host, read } = await setup();
    writeHostLease(meshRoot, {
      id: host.id, rootId: host.rootId, identityId: host.identity.id,
      updatedAt: Date.now(), expiresAt: host.expiresAt + 30_000, [field]: "wrong",
    });
    expect(read()).toEqual({ remoteHost: "forge", expiresAt: host.expiresAt });
  });

  it("requires exact caller target, owner host and identity bindings", async () => {
    const { directory, host } = await setup();
    expect(directory.mirroredControlOwner(host.id, "wrong", host.rootId)).toBeUndefined();
    expect(directory.mirroredControlOwner("wrong", host.identity.id, host.rootId)).toBeUndefined();
    expect(directory.mirroredControlOwner(host.id, host.identity.id, "wrong")).toBeUndefined();
    directory.options.enabled = false;
    expect(directory.mirroredControlOwner(host.id, host.identity.id, host.rootId)).toBeUndefined();
  });

  it.each([
    { rootId: "wrong" }, { ownerHostId: "wrong" }, { ownerIdentityId: "wrong" },
    { remoteHost: "other" }, { remoteHost: undefined }, { remoteHost: "bad/name" },
    { remoteHost: 42 }, { kind: "agent" }, { format: 2 }, { id: "wrong" },
    { capabilities: ["unknown"] }, { role: 42 },
  ])("rejects invalid or conflicting participant metadata %j", async (patch) => {
    const { putParticipant, read } = await setup();
    await putParticipant(patch);
    expect(read()).toBeUndefined();
  });

  it.each([
    { rootId: "wrong" }, { identity: { id: "wrong", name: "main", kind: "main" } },
    { remoteHost: "other" }, { remoteHost: undefined }, { remoteHost: "bad/name" },
    { remoteHost: 42 }, { expiresAt: "wrong" }, { expiresAt: null },
    { format: 2 }, { id: "wrong" },
  ])("rejects invalid, native or conflicting host metadata %j", async (patch) => {
    const { putHost, read } = await setup();
    await putHost(patch);
    expect(read()).toBeUndefined();
  });

  it("rejects records whose entry author does not match the claimed owner", async () => {
    const { identity, putParticipant, putHost, read } = await setup();
    const stranger = { ...identity, id: "stranger" };
    await putParticipant({}, stranger);
    expect(read()).toBeUndefined();
    await putParticipant();
    await putHost({}, stranger);
    expect(read()).toBeUndefined();
  });

  it("does not attribute a missing participant or host", async () => {
    const { writer, participantKey, hostKey, putParticipant, read } = await setup();
    await writer.delete({ key: participantKey });
    expect(read()).toBeUndefined();
    await putParticipant();
    await writer.delete({ key: hostKey });
    expect(read()).toBeUndefined();
  });

  it("keeps a native participant file ahead of a newer state mirror", async () => {
    const { meshRoot, writer, participantKey, participant, putParticipant, read } = await setup();
    const entry = writer.get(participantKey, { fresh: true })!;
    writeParticipantFile(meshRoot, {
      ...entry, updatedAt: 0, value: { ...participant, remoteHost: undefined },
    });
    await putParticipant();
    expect(read()).toBeUndefined();
  });

  it.each(["id", "rootId", "identity"])("refuses collisions with a native host's %s without publishing", async (field) => {
    const { writer, reader, host, read } = await setup();
    const identity: MeshIdentity = { id: field === "identity" ? host.rootId : "native-identity", name: "main", kind: "main" };
    const nativeId = field === "id" ? host.rootId : "native-host";
    await writer.put({
      key: keyFor("topology/hosts/", nativeId), identity,
      value: {
        ...host, id: nativeId, identity, remoteHost: undefined,
        rootId: field === "rootId" ? host.rootId : "native-root",
      },
    });
    const publish = vi.spyOn(reader, "publish");
    try {
      expect(read()).toBeUndefined();
      expect(publish).not.toHaveBeenCalled();
    } finally {
      publish.mockRestore();
    }
  });

  it.each(["root", "identity"])("refuses a mirror colliding with this directory's %s", async (field) => {
    const { directory, participant, read } = await setup();
    if (field === "root") directory.options.rootId = participant.id;
    else directory.options.identity = { ...directory.options.identity, id: participant.id };
    expect(read()).toBeUndefined();
  });

  it("refuses a mirrored owner claiming this directory's host id", async () => {
    const { writer, identity, participant, host, putParticipant, read } = await setup();
    await writer.put({
      key: keyFor("topology/hosts/", "local-host"), identity,
      value: { ...host, id: "local-host" },
    });
    await putParticipant({ ownerHostId: "local-host" });
    const directory = new ParticipantDirectory(writer, {
      enabled: true, hostId: "local-host", rootId: "local-root",
      identity: { id: "local-identity", name: "main", kind: "main" },
    });
    expect(directory.mirroredControlOwner("local-host", identity.id, participant.id)).toBeUndefined();
    expect(read()).toBeUndefined();
  });
});

// smarty-dev#816: heartbeats were 78% of all writes under the one mesh lock, each a rewrite of
// the whole shared state. Hosts also renew a file lease of their own, without the lock.
describe("ParticipantDirectory host leases", () => {
  const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
  const setup = async (policy: boolean) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    if (policy) {
      await store.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner") });
    }
    const alpha = createDirectory(meshRoot, identityOf("alpha"), "session:alpha", () => [rootRecord("session:alpha", "session:alpha", "alpha")]);
    const beta = createDirectory(meshRoot, identityOf("beta"), "session:beta", () => [rootRecord("session:beta", "session:beta", "beta")]);
    await alpha.start();
    await beta.start();
    const hostEntry = () => store.listAll("topology/hosts/", { fresh: true })
      .find((entry) => (entry.value as { id?: string }).id === "session:alpha");
    return { meshRoot, store, alpha, beta, hostEntry };
  };
  const alphaBatches = (spy: { mock: { calls: unknown[][] } }) =>
    spy.mock.calls.filter((call) => (call[0] as { identity: MeshIdentity }).identity.id === "session:alpha").length;

  it("renew a file lease on every heartbeat, and still the shared record without the fleet owner's policy", async () => {
    const { meshRoot, hostEntry } = await setup(false);
    const before = hostEntry()!.updatedAt;
    await new Promise((resolve) => setTimeout(resolve, 450));      // several 100 ms heartbeats
    expect(readHostLeases(meshRoot).get("session:alpha")?.expiresAt).toBeGreaterThan(Date.now());
    expect(hostEntry()!.updatedAt).toBeGreaterThan(before);
  });

  it("under the policy, renew only the file lease, and peers still see the host live", async () => {
    const { beta, hostEntry } = await setup(true);
    const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
    const shared = hostEntry()!;
    await new Promise((resolve) => setTimeout(resolve, 600));      // twice the 300 ms lease
    expect(alphaBatches(batches)).toBe(0);                         // no locked write for a renewal
    expect(hostEntry()!.version).toBe(shared.version);
    expect((hostEntry()!.value as { expiresAt: number }).expiresAt).toBeLessThan(Date.now());
    // includeStale keeps the participant's own record, not the legacy session entry (15 s lease).
    expect(beta.list({ scope: "project", includeStale: true }).find((participant) => participant.id === "session:alpha"))
      .toMatchObject({ stale: false });
    batches.mockRestore();
  });

  // review/astra F1 on #68: a file-only renewal must not certify that the shared state is
  // writable; otherwise a live peer blocked behind a held lock reads as departed (#24).
  it("under the policy, never settle a peer that lapsed behind a held lock, and report the stall", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({
      key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner"),
    });
    let peerStatus: "idle" | "running" = "idle";
    const make = (name: string, timing: { heartbeatMs: number; leaseMs: number }) => {
      // A non-main peer: a main also writes a legacy session entry with a fixed 15 s lease.
      const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: name === "peer" ? "actor" : "main", sessionId: name };
      const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 10_000 }), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, ...timing,
      });
      directory.registerSource(() => [{ ...rootRecord(identity.id, identity.id, name), ...(name === "peer" ? { status: peerStatus } : {}) }]);
      directories.push(directory);
      return directory;
    };
    const reader = make("reader", { heartbeatMs: 100, leaseMs: 2_000 });
    const peer = make("peer", { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([reader.start(), peer.start()]);
    const seesPeer = () => reader.peers().some((candidate) => candidate.id === "session:peer");
    await vi.waitFor(() => expect(seesPeer()).toBe(true), { timeout: 5_000, interval: 20 });
    const lockPath = path.join(meshRoot, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    try {
      peerStatus = "running";
      void peer.refresh().catch(() => undefined);                 // renews its file, then blocks
      let result: PeerSettleResult | undefined;
      void awaitPeerSettle({
        poll: () => reader.peers(),
        stalled: () => reader.writeStalled(),
        confirmedAt: () => reader.confirmedAt(),
        selector: "session:peer",
        settledForMs: 60_000,
        pollMs: 20,
      }).then((settled) => { result = settled; });
      await vi.waitFor(() => expect(seesPeer()).toBe(false), { timeout: 3_000, interval: 10 });   // its lease lapsed
      await vi.waitFor(() => expect(result).toBeDefined(), { timeout: 5_000, interval: 20 });
      expect(result).toEqual({ ok: false, error: expect.stringMatching(/peer lease lapsed/) });
    } finally {
      fs.rmSync(lockPath, { recursive: true, force: true });
    }
  });

  // review/astra F2 on #68: production stores cache reads (2 s). A confirmation must not leave a
  // snapshot from before it, or peer-settle certifies a peer list that is already out of date.
  it("under the policy, read peers after a file-only confirmation, not from an earlier cache", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({
      key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner"),
    });
    const make = (name: string, status: "idle" | "running") => {
      const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: name === "observer" ? "main" : "actor", sessionId: name };
      const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: 60_000 }), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 2_000,
      });
      directory.registerSource(() => [{ ...rootRecord(identity.id, identity.id, name), status }]);
      directories.push(directory);
      return directory;
    };
    const observer = make("observer", "idle");
    await observer.start();
    await new Promise((resolve) => setTimeout(resolve, 300));        // file-only heartbeats now
    expect(observer.peers()).toEqual([]);                           // primes the 60 s cached view
    const joined = make("joined", "running");
    await joined.start();                                           // a running peer joins
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({
      poll: () => observer.peers(),
      stalled: () => observer.writeStalled(),
      confirmedAt: () => observer.confirmedAt(),
      settledForMs: 60_000,
      pollMs: 20,
    }).then((settled) => { result = settled; });
    await vi.waitFor(() => expect(observer.peers().map((peer) => peer.id)).toEqual(["session:joined"]), { timeout: 2_000, interval: 20 });
    await new Promise((resolve) => setTimeout(resolve, 400));        // several confirmed heartbeats
    expect(result).toBeUndefined();                                 // still waiting for the running peer
  });

  // #411 R1: a lock acquisition can outlast the pre-confirmation renewal decision.
  it.each([
    { crossing: "host threshold", leaseMs: 4_000, delayMs: 2_001, policy: false },
    { crossing: "host expiry", leaseMs: 4_000, delayMs: 4_001, policy: false },
    { crossing: "legacy threshold", leaseMs: 60_000, delayMs: 7_501, policy: false },
    { crossing: "legacy expiry", leaseMs: 60_000, delayMs: 15_001, policy: false },
    { crossing: "policy threshold", leaseMs: 60_000, delayMs: STATE_LEASE_RENEW_MS, policy: true },
  ])("renews after delayed confirmation crosses $crossing", async ({ leaseMs, delayMs, policy }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-delayed-confirm-"));
    roots.push(root);
    const identity = identityOf("delayed");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 5_000 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 1_000, leaseMs, reapDeadHosts: false,
    });
    directories.push(directory);
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "delayed")]);
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const lockPath = path.join(mesh.root, ".lock");
    try {
      if (policy) await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity });
      await directory.refresh(); // no heartbeat timer: only this awaited refresh can renew
      const hostBefore = mesh.listAll("topology/hosts/")[0]!;
      const legacyBefore = mesh.get("sessions/delayed")!;
      const participantBefore = mesh.listAll("topology/participants/")[0]!;
      const writes = vi.spyOn(mesh, "writeBatch");
      const confirm = mesh.confirmWritable.bind(mesh);
      let entered!: () => void;
      const confirming = new Promise<void>((resolve) => { entered = resolve; });
      const confirmations = vi.spyOn(mesh, "confirmWritable").mockImplementation(async () => {
        const pending = confirm(); // the real acquisition waits behind a live lock
        entered();
        await pending;
      });
      fs.mkdirSync(lockPath, { mode: 0o700 });
      fs.writeFileSync(path.join(lockPath, "owner"), `delayed\n${process.pid}\n${now}\n`);
      let settled = false;
      const refresh = directory.refresh().then(() => { settled = true; });
      await confirming;
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(settled).toBe(false);
      now += delayMs; // deterministic elapsed time during the actual lock wait
      fs.rmSync(lockPath, { recursive: true, force: true });
      await refresh;
      expect(confirmations).toHaveBeenCalledOnce();
      expect.soft(writes).toHaveBeenCalledOnce(); // host + legacy in one locked commit
      const hostAfter = mesh.listAll("topology/hosts/", { fresh: true })[0]!;
      expect.soft(hostAfter.version).toBeGreaterThan(hostBefore.version);
      expect.soft(hostAfter.value).toMatchObject({ updatedAt: now, expiresAt: now + leaseMs });
      const legacyAfter = mesh.get("sessions/delayed", { fresh: true })!;
      expect.soft(legacyAfter.version).toBeGreaterThan(legacyBefore.version);
      expect.soft(legacyAfter.updatedAt).toBe(now);
      expect.soft(legacyAfter.value).toMatchObject({ updatedAt: now });
      expect(mesh.listAll("topology/participants/", { fresh: true })[0]).toEqual(participantBefore);
      expect(directory.confirmedAt()).toBe(now);
    } finally {
      fs.rmSync(lockPath, { recursive: true, force: true });
      clock.mockRestore();
      vi.restoreAllMocks();
    }
  });

  it("under the policy, still renew the shared record every STATE_LEASE_RENEW_MS", async () => {
    const { hostEntry } = await setup(true);
    const shared = hostEntry()!;
    const now = Date.now;
    vi.spyOn(Date, "now").mockImplementation(() => now() + STATE_LEASE_RENEW_MS);
    await vi.waitFor(() => expect(hostEntry()!.version).toBeGreaterThan(shared.version), { timeout: 2_000, interval: 20 });
    vi.restoreAllMocks();
  });
});

// smarty-dev#816: more than half of the participant rewrites of the shared state carried only
// activity counters. Those ride along with another write, or at most once a minute.
describe("ParticipantDirectory activity counters", () => {
  const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
  const setup = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner") });
    const worker = { status: "running", turns: 0, toolCalls: 0, calls: 0 };
    // Every read of the source is new model activity: the counters and the current tool move.
    // Actor queue/mailbox counts are operational state, covered separately below.
    const alpha = createDirectory(meshRoot, identityOf("alpha"), "session:alpha", () => {
      worker.calls += 1;
      worker.turns += 1;
      worker.toolCalls += 2;
      return [
        rootRecord("session:alpha", "session:alpha", "alpha"),
        {
          ...agentRecord("agent:worker", "session:alpha", "session:alpha", "session:alpha"),
          status: worker.status,
          currentTool: `tool-${worker.calls}`,
          turns: worker.turns,
          toolCalls: worker.toolCalls,
          usage: { input: worker.turns * 10, output: worker.turns, cacheRead: 0, cacheWrite: 0, cost: worker.turns / 100 },
          updatedAt: Date.now(),
        },
      ];
    });
    const beta = createDirectory(meshRoot, identityOf("beta"), "session:beta", () => [rootRecord("session:beta", "session:beta", "beta")]);
    await alpha.start();
    await beta.start();
    const shared = () => store.listAll("topology/participants/", { fresh: true })
      .map((entry) => entry.value as FabricParticipantRecord)
      .find((participant) => participant.id === "agent:worker")!;
    const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
    const alphaBatches = () => batches.mock.calls
      .filter((call) => (call[0] as { identity: MeshIdentity }).identity.id === "session:alpha").length;
    return { alpha, beta, worker, shared, batches, alphaBatches };
  };

  it("writes nothing for counter-only changes over many heartbeats and change refreshes", async () => {
    const { alpha, worker, shared, batches, alphaBatches } = await setup();
    const before = shared();
    const calls = worker.calls;
    for (let index = 0; index < 8; index++) {
      alpha.scheduleRefresh();
      await new Promise((resolve) => setTimeout(resolve, 100));    // 100 ms heartbeats
    }
    expect(worker.calls - calls).toBeGreaterThanOrEqual(5);        // the heartbeats did read new counters
    expect(alphaBatches()).toBe(0);
    expect(shared()).toEqual(before);
    batches.mockRestore();
  });

  it("writes a status change at once, with the latest counters", async () => {
    const { alpha, worker, shared, batches, alphaBatches } = await setup();
    worker.status = "completed";
    await alpha.refresh();
    expect(alphaBatches()).toBe(1);
    expect(shared()).toMatchObject({ status: "completed", turns: worker.turns, toolCalls: worker.toolCalls });
    batches.mockRestore();
  });

  it("carries the counters to a peer within ACTIVITY_REFRESH_MS", async () => {
    const { beta, worker, shared, batches, alphaBatches } = await setup();
    const before = shared().turns!;
    const now = Date.now;
    vi.spyOn(Date, "now").mockImplementation(() => now() + 60_000);
    await vi.waitFor(() => expect(shared().turns).toBeGreaterThan(before), { timeout: 2_000, interval: 20 });
    const seen = beta.get("agent:worker")!;
    expect(seen).toMatchObject({ stale: false, status: "running" });
    expect(seen.turns).toBeGreaterThan(before);
    expect(seen.currentTool).toMatch(/^tool-\d+$/);
    expect(seen.usage?.input).toBe(seen.turns! * 10);
    // One carrying write, then quiet again for the next minute.
    const carried = alphaBatches();
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(alphaBatches()).toBe(carried);
    vi.restoreAllMocks();
    batches.mockRestore();
  });
});

// #2726 / Astra F1: queue and mailbox counts are operational state, not model activity.
// Both provider reads consume this fresh row; their overlay mapping is covered separately in
// agents-provider.test.ts. Exercise the real mapper, publisher and filesystem reader here.
describe("ParticipantDirectory actor operational counters", () => {
  describe.each([
    { version: 1, hostLeases: "files" },
    { version: 1, hostLeases: "files", participants: "files" },
  ])("policy %j", (policy) => {
    const setup = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-counters-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const identity: MeshIdentity = { id: "session:owner", name: "main", kind: "main", sessionId: "owner" };
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.put({ key: LIVENESS_POLICY_KEY, value: policy, identity });
      const actor: FabricActorInfo = {
        filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null },
        id: "actor:running", scope: "project", name: "running", rootId: identity.id,
        status: "running", residency: "durable", runner: "pi", events: [], topics: [],
        delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: false,
        queued: 0, messages: 1, createdAt: 1, updatedAt: 2,
        inFlightRun: { id: "stable-run", startedAt: 2, ageS: 0 },
      };
      // No clock jump, root transition, child or new run can incidentally flush counts.
      // The heartbeat is a minute away; only the awaited ordinary refreshes publish updates.
      const timing = { heartbeatMs: 60_000, leaseMs: 120_000 };
      const owner = createDirectory(meshRoot, identity, identity.id, () => [
        rootRecord(identity.id, identity.id, "owner"),
        actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id),
      ], timing);
      const readerIdentity: MeshIdentity = { id: "session:reader", name: "main", kind: "main", sessionId: "reader" };
      // A separate store/directory, with no owner-memory shortcut or stubbed get().
      const reader = createDirectory(meshRoot, readerIdentity, readerIdentity.id, () => [], timing);
      await owner.start();
      const read = () => reader.get(actor.id, undefined, { fresh: true });
      const stable = {
        id: actor.id, kind: "actor", status: "running", ownerHostId: identity.id,
        local: false, stale: false, actorRun: { id: "stable-run", startedAt: 2 },
      };
      expect(read()).toMatchObject({ ...stable, actorQueued: 0, actorMessages: 1 });
      // Assert that the requested policy really selected state+file vs file-only publication.
      expect(store.listAll("topology/participants/", { fresh: true }).length)
        .toBe(policy.participants === "files" ? 0 : 2);
      return { actor, owner, read, stable };
    };

    it("#4444 publishes and clears host admission state through a separate reader", async () => {
      const { actor, owner, read } = await setup();
      actor.status = "waiting";
      actor.hostQueue = { position: 2, waitingSince: Date.now(), limit: 4 };
      actor.updatedAt += 1;
      await owner.refresh();
      expect(read()).toMatchObject({ status: "waiting", actorHostQueue: actor.hostQueue });
      actor.status = "running";
      delete actor.hostQueue;
      actor.updatedAt += 1;
      await owner.refresh();
      expect(read()?.actorHostQueue).toBeUndefined();
    });

    it.each([
      { change: "queue/message", counts: [[1, 2], [2, 3]] },
      { change: "message-only", counts: [[0, 2], [0, 3]] },
      { change: "queue-only", counts: [[1, 1], [2, 1]] },
    ])("publishes two successive $change updates on the same running activation", async ({ counts }) => {
      const { actor, owner, read, stable } = await setup();
      for (const [queued, messages] of counts) {
        actor.queued = queued!;
        actor.messages = messages!;
        actor.updatedAt += 1;
        await owner.refresh();
        // Soft assertions retain evidence for BOTH refreshes even when the first is RED.
        expect.soft(read()).toMatchObject({ ...stable, actorQueued: queued, actorMessages: messages });
      }
    });
  });
});

// smarty-dev#784: actor ownership checks call get() per actor; it listed and cloned the whole
// directory each time.
describe("ParticipantDirectory.get", () => {
  it("reads one live participant without listing the directory, with the same answer as the list", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ]);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")]);
    await alpha.start();
    await beta.start();
    const expected = (id: string) => beta.list({ scope: "project" }).find((participant) => participant.id === id);
    const listed = vi.spyOn(beta, "list");
    for (const id of ["agent:alpha-child", "session:alpha", "session:beta"]) {
      listed.mockClear();
      const found = beta.get(id);
      expect(listed).not.toHaveBeenCalled();
      listed.mockRestore();
      expect(found).toEqual(expected(id));
      vi.spyOn(beta, "list");
    }
    const fallback = vi.spyOn(beta, "list");
    expect(beta.get("agent:unknown")).toBeUndefined();                // absent: the full list decides
    expect(fallback).toHaveBeenCalled();
    fallback.mockRestore();
  });

  it("finds a participant whose host is alive only by its file lease, without listing", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const owner: MeshIdentity = { id: "session:owner", name: "main", kind: "main", sessionId: "owner" };
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: owner });
    const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ]);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")]);
    await alpha.start();
    await beta.start();
    await new Promise((resolve) => setTimeout(resolve, 600));        // past alpha's 300 ms shared lease
    // A heartbeat just now renews only the file lease; a stalled runner timer must not let it lapse
    // before the read (pi-fabric#142 CI: failed once on ubuntu-latest).
    await alpha.refresh();
    const listed = vi.spyOn(beta, "list");
    expect(beta.get("agent:alpha-child")).toMatchObject({ id: "agent:alpha-child", stale: false });
    expect(listed).not.toHaveBeenCalled();
    listed.mockRestore();
  });
});

// smarty-dev#784: roots publish their role and project, and peers show them.
describe("ParticipantDirectory role and project", () => {
  it("publishes normalized origin and marks print/JSON roots as discovery-only observers", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-origin-presence-"));
    roots.push(dir);
    const git = (...args: string[]) => execFileSync("git", args, { cwd: dir, stdio: "ignore" });
    git("init", "-q");
    git("remote", "add", "origin", "git@github.com:Smarty-Pants-Inc/pi-fabric.git");
    const identity: MeshIdentity = { id: "session:audit", name: "main", kind: "main", sessionId: "audit" };
    let alpha: ParticipantDirectory;
    const info = { id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: dir, sessionId: "audit", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true } as const;
    alpha = createDirectory(path.join(dir, "mesh"), identity, identity.id, () => [alpha.root(info, false)]);
    await alpha.start();
    expect(alpha.get(identity.id)).toMatchObject({ repository: "github.com/smarty-pants-inc/pi-fabric", interactive: false, capabilities: ["fabric"] });
    expect(alpha.root(info, true)).toMatchObject({ interactive: true, capabilities: ["steer", "followUp", "fabric"], mainBindings: false });
    const reader = createDirectory(path.join(dir, "mesh"), { id: "session:reader", name: "main", kind: "main" }, "session:reader", () => []);
    expect(reader.peers()).toEqual([expect.objectContaining({ id: identity.id, repository: "github.com/smarty-pants-inc/pi-fabric", interactive: false })]);
  });


  it("publishes a root's role and project, and shows them on peers", async () => {
    const previous = process.env.SMARTY_ROLE;
    process.env.SMARTY_ROLE = "project-agent@5358e96a418f";
    try {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
      const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
      let alpha: ParticipantDirectory | undefined;
      alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [alpha!.root({
        id: alphaIdentity.id, name: "main", kind: "main", status: "idle", runner: "pi", transport: "host",
        cwd: process.cwd(), sessionId: "alpha", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true,
      } as never)]);
      const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")]);
      await alpha.start();
      await beta.start();
      expect(beta.peers().find((peer) => peer.id === "session:alpha")).toMatchObject({
        role: "project-agent", project: projectOf(process.cwd()),
      });
    } finally {
      if (previous === undefined) delete process.env.SMARTY_ROLE;
      else process.env.SMARTY_ROLE = previous;
    }
  });
});

describe("ParticipantDirectory root project", () => {
  // smarty-dev#977: a lead whose cwd is a worktree of another repository publishes the project it
  // names, so it is not taken for a project agent of that repository.
  it("publishes PI_FABRIC_PROJECT as the root's project instead of its cwd's", async () => {
    const previous = { role: process.env.SMARTY_ROLE, project: process.env.PI_FABRIC_PROJECT };
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const own = path.join(root, "own-project");
    fs.mkdirSync(own);
    // A lane-local TMPDIR may sit inside another checkout: make this fixture a distinct project.
    fs.mkdirSync(path.join(own, ".git"));
    process.env.SMARTY_ROLE = "project-agent@5358e96a418f";
    process.env.PI_FABRIC_PROJECT = own;
    try {
      const meshRoot = path.join(root, "mesh");
      const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
      const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
      let alpha: ParticipantDirectory | undefined;
      alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [alpha!.root({
        id: alphaIdentity.id, name: "main", kind: "main", status: "idle", runner: "pi", transport: "host",
        cwd: process.cwd(), sessionId: "alpha", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true,
      } as never)]);
      const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")]);
      await alpha.start();
      await beta.start();
      const peer = beta.peers().find((candidate) => candidate.id === "session:alpha");
      expect(peer).toMatchObject({ role: "project-agent", project: projectOf(own), cwd: process.cwd() });
      expect(peer?.project).not.toBe(projectOf(process.cwd()));
    } finally {
      for (const [key, value] of [["SMARTY_ROLE", previous.role], ["PI_FABRIC_PROJECT", previous.project]] as const) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
});

describe("ParticipantDirectory", () => {
  it("builds one project topology while preserving local ownership and lineage", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const alphaIdentity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const betaIdentity: MeshIdentity = {
      id: "session:beta",
      name: "main",
      kind: "main",
      sessionId: "beta",
    };
    const recursiveIdentity: MeshIdentity = {
      id: "agent:recursive",
      name: "recursive",
      kind: "agent",
      sessionId: "recursive-session",
    };
    // This tests live ownership and lineage, not expiry: keep all owners live across slow CI.
    const lease = { heartbeatMs: 60_000, leaseMs: 120_000 };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ], lease);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [
      rootRecord(betaIdentity.id, betaIdentity.id, "beta"),
    ], lease);
    const recursive = createDirectory(meshRoot, recursiveIdentity, alphaIdentity.id, () => [
      agentRecord(
        "agent:grandchild",
        alphaIdentity.id,
        "runtime:recursive-session",
        recursiveIdentity.id,
      ),
    ], lease);

    await alpha.start();
    await beta.start();
    await recursive.start();

    expect(alpha.list({ scope: "project" }).map(({ id }) => id)).toEqual([
      "session:alpha",
      "session:beta",
      "agent:alpha-child",
      "agent:grandchild",
    ]);
    expect(alpha.list({ scope: "local" }).map(({ id }) => id)).toEqual([
      "session:alpha",
      "agent:alpha-child",
    ]);
    expect(alpha.list({ scope: "lineage" }).map(({ id }) => id)).toEqual([
      "session:alpha",
      "agent:alpha-child",
      "agent:grandchild",
    ]);
    expect(alpha.sessions()).toMatchObject([
      { id: "session:alpha", kind: "root", local: true },
      { id: "session:beta", kind: "root", local: false },
    ]);
    expect(alpha.peers()).toMatchObject([
      { id: "session:beta", name: "PRO-2", label: "PRO-2", kind: "peer", local: false },
    ]);
    expect(recursive.self()).toMatchObject({
      id: "agent:recursive",
      kind: "agent",
      rootId: "session:alpha",
    });
  });

  it("withdraws control capabilities before releasing its live host lease", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:quiesce",
      name: "main",
      kind: "main",
      sessionId: "quiesce",
    };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "quiesce"),
      agentRecord("agent:quiesce", identity.id, identity.id, identity.id),
    ]);

    await directory.start();
    await directory.quiesce();

    expect(directory.get("agent:quiesce")).toMatchObject({
      capabilities: [],
      local: true,
      stale: false,
    });
    // A root that is shutting down says so, for a clear steer error (smarty-dev#1113).
    expect(directory.get(identity.id)).toMatchObject({ capabilities: [], status: "stopping" });
    // Other roots still list it as a peer while it shuts down.
    const observerIdentity: MeshIdentity = { id: "session:observer", name: "main", kind: "main", sessionId: "observer" };
    const observer = createDirectory(path.join(root, "mesh"), observerIdentity, observerIdentity.id, () => [
      rootRecord(observerIdentity.id, observerIdentity.id, "observer"),
    ]);
    await observer.start();
    expect(observer.peers().map((peer) => peer.id)).toContain(identity.id);
    await observer.close();
    expect(directory.mesh.get("sessions/quiesce")).toBeUndefined();
  });

  it("stamps the host lease after participant writes that outlast it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:slow",
      name: "main",
      kind: "main",
      sessionId: "slow",
    };
    // A lease of 1 s, and a write that outlasts it: the read after the commit keeps a 1 s margin,
    // which a starved Windows runner used up with a 300 ms lease (smarty-dev#883). The lease is
    // at least two heartbeats, so the heartbeat is 500 ms.
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "slow"),
      agentRecord("agent:slow", identity.id, identity.id, identity.id),
    ], { heartbeatMs: 500, leaseMs: 1_000 });
    const write = directory.mesh.writeBatch.bind(directory.mesh);
    // A contended Windows mesh makes the heartbeat write outlast the lease;
    // the host lease is stamped at commit, so it must still be fresh afterwards.
    vi.spyOn(directory.mesh, "writeBatch").mockImplementation(async (input) => {
      await new Promise((resolve) => setTimeout(resolve, 1_100));
      return write(input);
    });

    await directory.start();

    expect(directory.get("agent:slow")).toMatchObject({
      capabilities: ["steer", "followUp", "stop"],
      local: true,
      stale: false,
    });
  });

  it("does not claim an actor still owned by a live legacy root", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const oldIdentity: MeshIdentity = {
      id: "session:old",
      name: "main",
      kind: "main",
      sessionId: "old-session",
    };
    const session = await mesh.put({
      key: "sessions/old-session",
      value: {
        id: oldIdentity.id,
        name: "Peer old-sess",
        kind: "peer",
        status: "idle",
        runner: "pi",
        transport: "host",
        cwd: "/tmp/project",
        sessionId: "old-session",
        startedAt: 1,
        updatedAt: Date.now(),
        pendingMessages: false,
        local: false,
      },
      identity: oldIdentity,
    });
    await mesh.put({
      key: "actors/old-session/actor:legacy",
      value: {
        id: "actor:legacy",
        name: "legacy actor",
        status: "idle",
        runner: "pi",
        createdAt: 1,
      },
      identity: oldIdentity,
    });
    const identity: MeshIdentity = {
      id: "session:new",
      name: "main",
      kind: "main",
      sessionId: "new-session",
    };
    const directory = createDirectory(meshRoot, identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "new-session"),
      {
        ...agentRecord("actor:legacy", identity.id, identity.id, identity.id),
        kind: "actor",
        transport: "host",
      },
    ]);

    await directory.start();
    expect(directory.get("actor:legacy")).toMatchObject({
      ownerHostId: oldIdentity.id,
      controlProtocol: "legacy",
      local: false,
    });

    await mesh.delete({ key: session.key, ifVersion: session.version });
    await directory.refresh();
    expect(directory.get("actor:legacy")).toMatchObject({
      ownerHostId: identity.id,
      controlProtocol: "v1",
      local: true,
    });
  });

  it("never shares agent prompts, results, or errors in participant state", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:private",
      name: "main",
      kind: "main",
      sessionId: "private",
    };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "private"),
      {
        ...agentRecord("agent:private", identity.id, identity.id, identity.id),
        task: "secret prompt",
        text: "secret result",
        error: "secret failure",
      } as FabricParticipantRecord,
      agentRecord("agent:wrong-lineage", "session:foreign", identity.id, identity.id),
    ], { heartbeatMs: 100, leaseMs: 30_000 });                          // no expiry here (smarty-dev#883)

    await directory.start();
    expect(directory.mesh.get("sessions/private")?.value).toMatchObject({
      id: identity.id,
      kind: "peer",
    });
    const participant = directory.get("agent:private") as unknown as Record<string, unknown>;
    expect(participant).not.toHaveProperty("task");
    expect(participant).not.toHaveProperty("text");
    expect(participant).not.toHaveProperty("error");
    expect(directory.get("agent:wrong-lineage")).toBeUndefined();
    expect(
      JSON.stringify(directory.mesh.listAll("topology/participants/")),
    ).not.toContain("secret");

    await directory.mesh.put({
      key: "topology/participants/not-a-canonical-hash",
      value: {
        ...agentRecord("agent:forged", identity.id, identity.id, identity.id),
        ownerIdentityId: identity.id,
      },
      identity,
    });
    expect(directory.get("agent:forged")).toBeUndefined();
    await directory.mesh.put({
      key: "sessions/claimed",
      value: {
        id: "session:victim",
        sessionId: "claimed",
        cwd: "/tmp/project",
        status: "idle",
        startedAt: 1,
      },
      identity,
    });
    expect(directory.get("session:victim")).toBeUndefined();
  });

  it("keeps one live execution owner for a colliding participant id", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const alphaIdentity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const betaIdentity: MeshIdentity = {
      id: "session:beta",
      name: "main",
      kind: "main",
      sessionId: "beta",
    };
    const shared = (identity: MeshIdentity): FabricParticipantRecord => ({
      ...agentRecord("actor:shared", identity.id, identity.id, identity.id),
      kind: "actor",
      transport: "host",
    });
    // The takeover follows alpha's close, not its expiry, so a slow runner must not expire alpha's
    // lease first and hand beta the record early (smarty-dev#883).
    const lease = { heartbeatMs: 100, leaseMs: 30_000 };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      shared(alphaIdentity),
    ], lease);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [
      rootRecord(betaIdentity.id, betaIdentity.id, "beta"),
      shared(betaIdentity),
    ], lease);

    await alpha.start();
    // Mesh writes can lag a fresh enumeration on CI-bound filesystems
    // (Windows readdir caching): beta's first refresh relies on the occupancy
    // guard seeing alpha's records, so wait until beta's on-disk mesh view
    // actually contains alpha before contention starts — otherwise a blind
    // first write can converge on the wrong owner and never self-correct.
    await vi.waitFor(
      () => {
        expect(beta.mesh.listAll("topology/participants/").length).toBeGreaterThanOrEqual(2);
      },
      { timeout: 5_000, interval: 50 },
    );
    await beta.start();
    expect(beta.get("actor:shared")).toMatchObject({ ownerHostId: "session:alpha" });

    await alpha.close();
    // The takeover itself is another write-then-readback hop: poll refresh +
    // read until the delete propagates and beta claims the record.
    await vi.waitFor(
      async () => {
        await beta.refresh();
        expect(beta.get("actor:shared")).toMatchObject({ ownerHostId: "session:beta" });
      },
      { timeout: 5_000, interval: 50 },
    );
  });

  // smarty-dev#367: change-driven refreshes wrote the whole shared state on every agent UI
  // update. They now write only real changes, at most once per second after the first.
  describe("change-driven refreshes", () => {
    const changing = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
      roots.push(root);
      const identity: MeshIdentity = { id: "session:busy", name: "main", kind: "main", sessionId: "busy" };
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 180_000,
      });
      // The production Main source: MainAgentController.info() stamps updatedAt on every read.
      let status: "idle" | "running" = "idle";
      const main = new MainAgentController(
        { getThinkingLevel: () => "high" } as unknown as ExtensionAPI, identity.id, true, "/tmp/project", "busy");
      const context = {
        model: { provider: "anthropic", id: "model" },
        isIdle: () => status === "idle",
        hasPendingMessages: () => false,
      } as unknown as ExtensionContext;
      directory.registerSource(() => [directory.root(main.info(context))]);
      directories.push(directory);
      await directory.start();
      const writes = vi.spyOn(mesh, "writeBatch");
      return { directory, mesh, writes, setStatus: (next: typeof status) => { status = next; } };
    };

    it("writes nothing when no published record changed", async () => {
      const { directory, writes } = await changing();
      for (let index = 0; index < 20; index++) {
        directory.scheduleRefresh();
        await new Promise((resolve) => setTimeout(resolve, 100));   // each read gets a new updatedAt
      }
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(writes).not.toHaveBeenCalled();
    });

    it("preserves backoff and one warning across unchanged refreshes until real lock recovery", async () => {
      vi.useFakeTimers();
      // Exercise near-ceiling full-jitter draws: the fixed 200 ms tick below
      // must not consume the second outage's doubled backoff before checking it.
      vi.spyOn(Math, "random").mockReturnValue(0.999999);
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-outage-"));
      roots.push(root);
      const identity: MeshIdentity = { id: "session:busy", name: "main", kind: "main", sessionId: "busy" };
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 100 });
      const source = vi.fn(() => [rootRecord(identity.id, identity.id, "busy")]);
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity,
        heartbeatMs: 60_000, leaseMs: 180_000, reapDeadHosts: false,
      });
      directory.registerSource(source);
      directories.push(directory);
      const intervals = vi.spyOn(globalThis, "setInterval");
      const lockPath = path.join(mesh.root, ".lock");
      try {
        await directory.start();
        // Drive the real heartbeat callback without incidental ticks during backoff.
        const heartbeat = intervals.mock.calls[0]![0] as () => void;
        const runs = vi.spyOn(MeshBackgroundRetry.prototype, "run");
        const confirmations = vi.spyOn(mesh, "confirmWritable");
        const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
        const tick = async () => {
          heartbeat();
          const result = runs.mock.results.at(-1)!.value;
          await vi.advanceTimersByTimeAsync(200); // acquisition times out after 100 ms
          return await result;
        };
        const hold = () => {
          fs.mkdirSync(lockPath, { mode: 0o700 });
          fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
        };
        hold();
        const holder = fs.readFileSync(path.join(lockPath, "owner"), "utf8");
        expect(await tick()).toBe("retry");
        expect(warn).toHaveBeenCalledOnce();
        expect(warn.mock.calls[0]![0]).toContain(`pid ${process.pid}`);
        const retry = runs.mock.contexts.at(-1) as MeshBackgroundRetry;
        const recovered = vi.spyOn(retry, "success");
        await vi.advanceTimersByTimeAsync(retry.waitMs);

        const readsBefore = source.mock.calls.length;
        directory.scheduleRefresh();
        await Promise.resolve(); // run the queued change-only refresh
        expect(await runs.mock.results.at(-1)!.value).toBe("done");
        expect(source.mock.calls.length).toBeGreaterThan(readsBefore);
        expect(confirmations).toHaveBeenCalledOnce(); // unchanged snapshot took no mesh lock
        expect(recovered).not.toHaveBeenCalled();
        expect(directory.writeStalled()).toBeDefined();
        expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(holder);

        expect(await tick()).toBe("retry");
        expect(warn).toHaveBeenCalledOnce(); // same live holder, same continuous outage
        expect(retry.waitMs).toBeGreaterThan(0); // second timeout retained the doubled delay
        heartbeat();
        expect(await runs.mock.results.at(-1)!.value).toBe("skipped");
        expect(confirmations).toHaveBeenCalledTimes(2);

        fs.rmSync(lockPath, { recursive: true, force: true });
        await vi.advanceTimersByTimeAsync(retry.waitMs);
        expect(await tick()).toBe("done"); // a real lock acquisition confirms recovery
        expect(directory.writeStalled()).toBeUndefined();
        expect(recovered).toHaveBeenCalledOnce();
        expect(retry.waitMs).toBe(0);
        hold();
        expect(await tick()).toBe("retry");
        expect(warn).toHaveBeenCalledTimes(2); // a later outage gets its own warning
      } finally {
        fs.rmSync(lockPath, { recursive: true, force: true });
        await directory.close();
        vi.restoreAllMocks();
        vi.useRealTimers();
      }
    });

    it("publishes a burst of changes with at most two writes, ending on the latest", async () => {
      const { directory, mesh, writes, setStatus } = await changing();
      for (let index = 0; index < 10; index++) {
        setStatus(index % 2 === 0 ? "running" : "idle");
        directory.scheduleRefresh();
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      setStatus("running");
      directory.scheduleRefresh();
      await vi.waitFor(() => expect(
        mesh.listAll("topology/participants/").map((entry) => (entry.value as { status?: string }).status),
      ).toEqual(["running"]), { timeout: 3_000, interval: 20 });
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(writes.mock.calls.length).toBeLessThanOrEqual(2);
    });

    it("still renews the file lease on a heartbeat with unchanged records", async () => {
      const { directory, writes } = await changing();
      const confirmed = vi.spyOn(directory.mesh, "confirmWritable");
      await directory.refresh();
      expect(writes).not.toHaveBeenCalled();
      expect(confirmed).toHaveBeenCalledOnce();
      expect(readHostLeases(directory.mesh.root).get(directory.options.hostId)?.expiresAt).toBeGreaterThan(Date.now());
      confirmed.mockRestore();
    });
  });

  // smarty-dev#447: the record behind a lapsed lease stays readable for a reply and a reason.
  it("reports how long ago a participant's lease lapsed, and nothing for live or unknown ids", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [rootRecord(identity.id, identity.id, "alpha")]);
    await directory.start();
    await vi.waitFor(() => expect(directory.list({ scope: "project" })).toHaveLength(1), { timeout: 5_000, interval: 50 });
    expect(directory.lastKnown(identity.id)).toBeUndefined();                     // live
    expect(directory.lastKnown("session:nobody")).toBeUndefined();                // no record
    const later = Date.now() + 60_000;
    const known = directory.lastKnown(identity.id, later);
    expect(known?.participant).toMatchObject({ id: identity.id, kind: "root", stale: true });
    expect(known!.lapsedMs).toBeGreaterThan(0);
    expect(known!.lapsedMs).toBeLessThanOrEqual(60_000);
  });

  it("hides every participant owned by an expired host lease", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const directory = createDirectory(meshRoot, identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "alpha"),
      agentRecord("agent:child", identity.id, identity.id, identity.id),
    ]);
    await directory.start();

    // Same filesystem readback lag: poll until the fresh enumeration shows
    // both records instead of asserting one shot.
    await vi.waitFor(() => expect(directory.list({ scope: "project" })).toHaveLength(2), {
      timeout: 5_000,
      interval: 50,
    });
    expect(directory.list({ scope: "project" }, Date.now() + 1_000)).toEqual([]);
    const stale = directory.list(
      { scope: "project", includeStale: true },
      Date.now() + 1_000,
    );
    expect(stale).toHaveLength(2);
    expect(stale.every((participant) => participant.stale)).toBe(true);
  });

  it("keeps the same topology API in local-only mode", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:local",
      name: "main",
      kind: "main",
      sessionId: "local",
    };
    const directory = new ParticipantDirectory(
      new MeshStore(path.join(root, "mesh"), 64 * 1024, 100),
      { enabled: false, hostId: identity.id, rootId: identity.id, identity },
    );
    directories.push(directory);
    directory.registerSource(() => [
      rootRecord(identity.id, identity.id, "local"),
      agentRecord("agent:local", identity.id, identity.id, identity.id),
    ]);
    await directory.start();

    expect(directory.list({ scope: "project" }).map(({ id }) => id)).toEqual([
      "session:local",
      "agent:local",
    ]);
  });

  it("recovers via heartbeat when the initial publish fails at startup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const directory = createDirectory(meshRoot, identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "alpha"),
    ]);
    let failures = 1;
    const publish = directory.mesh.put.bind(directory.mesh);
    vi.spyOn(directory.mesh, "put").mockImplementation(async (input) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error("mesh offline");
      }
      return publish(input);
    });

    await expect(directory.start()).rejects.toThrow("mesh offline");

    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline && directory.mesh.listAll("topology/hosts/").length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    expect(directory.mesh.listAll("topology/hosts/")).toHaveLength(1);
    expect(directory.mesh.listAll("topology/participants/")).toHaveLength(1);
  });

  // smarty-dev#367: each heartbeat put rewrote the whole shared state file under the lock.
  it("writes one heartbeat as a single state-file write", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:batch", name: "main", kind: "main", sessionId: "batch" };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "batch"),
      agentRecord("agent:one", identity.id, identity.id, identity.id),
      agentRecord("agent:two", identity.id, identity.id, identity.id),
    ]);
    const renames = vi.spyOn(fs, "renameSync");
    await directory.refresh();
    const stateWrites = renames.mock.calls.filter(([, target]) => String(target).endsWith("state.json")).length;
    renames.mockRestore();
    // Peer-label claiming may add one write on first start; the heartbeat itself is one.
    expect(stateWrites).toBeLessThanOrEqual(2);
    expect(directory.mesh.listAll("topology/participants/")).toHaveLength(3);
    expect(directory.mesh.listAll("topology/hosts/")).toHaveLength(1);

    const again = vi.spyOn(fs, "renameSync");
    await directory.refresh();
    const steady = again.mock.calls.filter(([, target]) => String(target).endsWith("state.json")).length;
    again.mockRestore();
    expect(steady).toBe(0); // the fresh host lease needs no shared-state rewrite
  });

  // smarty-dev#266: a stopped mesh lock holder expired every lease; sessions() said [].
  // smarty-dev#883: no caller exercises expiry, so the lease is long enough that a starved Windows
  // runner cannot outlast it between start() and the first read; the 150 ms lock timeout and 100 ms
  // heartbeat still detect the stall quickly.
  const stallDirectory = (name: string, source: () => FabricParticipantRecord[]) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: "main", sessionId: name };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 150 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 10_000,
    });
    directory.registerSource(source);
    directories.push(directory);
    return { directory, mesh, identity };
  };

  it("reports a write-stalled mesh without throwing from its own reads, then recovers", async () => {
    const { directory, mesh, identity } = stallDirectory("stall", () => [rootRecord("session:stall", "session:stall", "stall")]);
    await directory.start();
    expect(directory.sessions().map((session) => session.id)).toEqual([identity.id]);
    expect(directory.writeStalled()).toBeUndefined();

    // A live holder that never releases: the store must not take its lock over.
    const lockPath = path.join(mesh.root, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    await expect.poll(() => directory.writeStalled()?.message ?? "", { timeout: 5_000, interval: 50 })
      .toMatch(/^Fabric mesh is write-stalled: FABRIC_MESH_LOCK_TIMEOUT: Timed out waiting for the Fabric mesh lock/);
    // Timers, the dashboard and ownership checks read these: they must never throw.
    expect(() => directory.sessions()).not.toThrow();
    expect(() => directory.peers()).not.toThrow();
    expect(directory.get("session:departed")).toBeUndefined();

    fs.rmSync(lockPath, { recursive: true, force: true });
    await expect.poll(() => directory.writeStalled(), { timeout: 5_000, interval: 50 }).toBeUndefined();
    expect(directory.sessions().map((session) => session.id)).toEqual([identity.id]);
  });

  // Review F1/F3 on #24: two hosts on one mesh. A peer's lease can lapse before this host's
  // lock wait times out, and this host's heartbeat can run late.
  const meshPair = (reader: { heartbeatMs: number; leaseMs: number }, peer: { heartbeatMs: number; leaseMs: number }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // The peer's identity is not "main": a main also writes a legacy session entry, whose fixed
    // 15 s lease (the production host lease) would outlive these scaled leases.
    const make = (name: string, timing: { heartbeatMs: number; leaseMs: number }) => {
      const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: name === "peer" ? "actor" : "main", sessionId: name };
      const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 10_000 });
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, ...timing,
      });
      directory.registerSource(() => [rootRecord(identity.id, identity.id, name)]);
      directories.push(directory);
      return directory;
    };
    const lockPath = path.join(meshRoot, ".lock");
    return {
      reader: make("reader", reader),
      peer: make("peer", peer),
      hold: () => {
        fs.mkdirSync(lockPath, { mode: 0o700 });
        fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
      },
      release: () => fs.rmSync(lockPath, { recursive: true, force: true }),
    };
  };
  const seesPeer = (directory: ParticipantDirectory) => directory.peers().some((peer) => peer.id === "session:peer");

  it("never settles a peer that lapsed behind a stalled lock, and reports it before the lock timeout", async () => {
    const { reader, peer, hold, release } = meshPair({ heartbeatMs: 600, leaseMs: 20_000 }, { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([reader.start(), peer.start()]);
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    await vi.waitFor(() => expect(Date.now() - reader.confirmedAt()).toBeLessThan(40), { timeout: 5_000, interval: 5 });
    hold();                                                    // right after the reader's last commit
    const started = Date.now();
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({
      poll: () => reader.peers(),
      stalled: () => reader.writeStalled(),
      confirmedAt: () => reader.confirmedAt(),
      selector: "session:peer",
      settledForMs: 60_000,
      pollMs: 20,
    }).then((settled) => { result = settled; });
    // The peer lapses while the reader's own heartbeat is not yet overdue: the plain read omits
    // it, but peer-settle does not take the absence as a departure.
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(false), { timeout: 3_000, interval: 10 });
    expect(reader.writeStalled()).toBeUndefined();
    // review/astra on a85b2c4: a settle armed now sees no peer at all; it must not succeed either.
    let late: PeerSettleResult | undefined;
    void awaitPeerSettle({
      poll: () => reader.peers(),
      stalled: () => reader.writeStalled(),
      confirmedAt: () => reader.confirmedAt(),
      settledForMs: 60_000,
      pollMs: 20,
    }).then((settled) => { late = settled; });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(result).toBeUndefined();
    expect(late).toBeUndefined();
    // Two heartbeat intervals without a commit: the lapse now reads as unknown visibility.
    await vi.waitFor(() => expect(result).toBeDefined(), { timeout: 5_000, interval: 20 });
    expect(result).toEqual({ ok: false, error: expect.stringMatching(/1 peer lease lapsed while this host's heartbeat has not committed/) });
    expect(reader.writeStalled()?.message).toMatch(/^Fabric mesh is write-stalled: 1 peer lease lapsed/);
    await vi.waitFor(() => expect(late).toEqual({ ok: false, error: expect.stringMatching(/peer lease lapsed/) }), { timeout: 2_000, interval: 20 });
    expect(Date.now() - started).toBeLessThan(5_000);          // long before the 10 s lock timeout
    release();
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    await vi.waitFor(() => expect(reader.writeStalled()).toBeUndefined(), { timeout: 5_000, interval: 20 });
  }, 20_000);

  it("settles a peer that genuinely departed on a working mesh, without a stall report", async () => {
    const { reader, peer } = meshPair({ heartbeatMs: 100, leaseMs: 2_000 }, { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([reader.start(), peer.start()]);
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    vi.spyOn(peer, "refresh").mockResolvedValue(undefined);    // the peer crashes: no renewals, no cleanup
    let flagged = false;
    const settled = awaitPeerSettle({
      poll: () => { if (reader.writeStalled()) flagged = true; return reader.peers(); },
      stalled: () => reader.writeStalled(),
      confirmedAt: () => reader.confirmedAt(),
      selector: "session:peer",
      settledForMs: 60_000,
      pollMs: 10,
    });
    await expect(settled).resolves.toEqual({ ok: true });
    expect(flagged).toBe(false);
  }, 20_000);

  it("keeps fresh listings usable while a lock wait lasts longer than a heartbeat", async () => {
    const { reader, peer, hold, release } = meshPair({ heartbeatMs: 200, leaseMs: 5_000 }, { heartbeatMs: 200, leaseMs: 5_000 });
    await Promise.all([reader.start(), peer.start()]);
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    hold();
    let flagged = false;
    let missing = false;
    const until = Date.now() + 1_500;                          // more than seven heartbeats, less than the 10 s timeout
    while (Date.now() < until) {
      if (reader.writeStalled()) flagged = true;
      if (!seesPeer(reader)) missing = true;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    release();
    expect(flagged).toBe(false);
    expect(missing).toBe(false);
  }, 20_000);

  it("does not report a stall for lock waits shorter than one heartbeat", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:brief", name: "main", kind: "main", sessionId: "brief" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 10_000 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 400, leaseMs: 1_200,
    });
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "brief")]);
    directories.push(directory);
    await directory.start();
    const lockPath = path.join(mesh.root, ".lock");
    let flagged = false;
    for (let round = 0; round < 6; round++) {
      // Ordinary contention: another writer holds the lock for 60 ms at a time.
      fs.mkdirSync(lockPath, { mode: 0o700 });
      fs.writeFileSync(path.join(lockPath, "owner"), `brief\n${process.pid}\n${Date.now()}\n`);
      const until = Date.now() + 60;
      while (Date.now() < until) {
        if (directory.writeStalled()) flagged = true;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      fs.rmSync(lockPath, { recursive: true, force: true });
      await new Promise((resolve) => setTimeout(resolve, 150));
      if (directory.writeStalled()) flagged = true;
    }
    expect(flagged).toBe(false);
  });

  it("returns an empty directory without a stall report when the mesh is healthy", async () => {
    const { directory } = stallDirectory("empty", () => []);
    await directory.start();
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(directory.sessions()).toEqual([]);
    expect(directory.writeStalled()).toBeUndefined();
  });

  it("does not report a stall for a heartbeat failure that is not a lock timeout", async () => {
    const { directory, mesh } = stallDirectory("disk", () => [rootRecord("session:disk", "session:disk", "disk")]);
    await directory.start();
    vi.spyOn(mesh, "writeBatch").mockRejectedValue(new Error("ENOSPC: no space left on device"));
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(directory.writeStalled()).toBeUndefined();
    expect(() => directory.sessions()).not.toThrow();
  });
});

// smarty-dev#557: every dashboard rebuild, peer listing and ownership check cloned the whole
// fleet directory. The directory now parses the shared state once per parse of the file.
describe("ParticipantDirectory reads", () => {
  const identity: MeshIdentity = { id: "session:reader", name: "main", kind: "main", sessionId: "reader" };
  const writer: MeshIdentity = { id: "session:writer", name: "main", kind: "main", sessionId: "writer" };

  it("reuse one parse while the state is unchanged, and see a write at once", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const reader = createDirectory(meshRoot, identity, identity.id, () => [rootRecord(identity.id, identity.id, "reader")],
      { heartbeatMs: 60_000, leaseMs: 120_000 });
    const peer = createDirectory(meshRoot, writer, writer.id, () => [rootRecord(writer.id, writer.id, "writer")],
      { heartbeatMs: 60_000, leaseMs: 120_000 });
    await reader.start();
    await peer.start();
    const shared = vi.spyOn(reader.mesh, "listAllShared");
    const first = reader.list({ scope: "project" });
    const calls = shared.mock.calls.length;
    expect(first.map((participant) => participant.id)).toContain(writer.id);
    for (let i = 0; i < 5; i++) reader.list({ scope: "project" });
    reader.peers();
    expect(shared.mock.calls.length).toBe(calls);                  // no read of the directory per call
    // Callers get their own top-level objects; the shared parse is frozen.
    first[0]!.status = "changed";
    expect(reader.list({ scope: "project" })[0]!.status).not.toBe("changed");
    const capabilities = reader.list({ scope: "project" }).find((participant) => participant.id === writer.id)!.capabilities;
    expect(Object.isFrozen(capabilities)).toBe(true);

    await store.put({ key: "topology/participants/" + createHash("sha256").update("session:writer:agent").digest("hex"),
      value: agentRecord("session:writer:agent", writer.id, writer.id, writer.id), identity: writer });
    const after = reader.list({ scope: "project", fresh: true });
    expect(after.map((participant) => participant.id)).toContain("session:writer:agent");
    // The write copied only the new entry: an unchanged record is the same parse.
    expect(after.find((participant) => participant.id === writer.id)!.capabilities).toBe(capabilities);
    // A changed record is read again.
    const key = "topology/participants/" + createHash("sha256").update(writer.id).digest("hex");
    const current = store.get(key)!;
    await store.put({ key, value: { ...(current.value as FabricParticipantRecord), status: "busy" }, identity: writer, ifVersion: current.version });
    expect(reader.list({ scope: "project", fresh: true }).find((participant) => participant.id === writer.id)!.status).toBe("busy");
    shared.mockRestore();
  });
});

// review/astra F1 on pi-fabric#140: a listing reuses the lease files it read for heartbeat/5.
// Under file-only renewals those files are the only liveness evidence, so a reused read must
// never turn a renewed peer into an absent one.
describe("ParticipantDirectory lease reads", () => {
  const reader: MeshIdentity = { id: "session:reader", name: "main", kind: "main", sessionId: "reader" };
  const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
  const timing = { heartbeatMs: 60_000, leaseMs: 120_000 };            // leases are reused for 12 s
  const setup = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: peer });
    const readerDirectory = createDirectory(meshRoot, reader, reader.id, () => [rootRecord(reader.id, reader.id, "reader")], timing);
    const peerDirectory = createDirectory(meshRoot, peer, peer.id, () => [rootRecord(peer.id, peer.id, "peer")], timing);
    await readerDirectory.start();
    await peerDirectory.start();
    const fileLease = readHostLeases(meshRoot).get(peer.id)!;
    const shared = store.listAll("topology/hosts/").find((entry) => (entry.value as { id?: string }).id === peer.id)!;
    // The peer's effective expiry: the later of its file lease and its shared-state record.
    const lease = { expiresAt: Math.max(fileLease.expiresAt, (shared.value as { expiresAt: number }).expiresAt) };
    const renew = (at: number) => writeHostLease(meshRoot, { ...fileLease, updatedAt: at, expiresAt: at + timing.leaseMs });
    const seesPeer = () => readerDirectory.peers().some((candidate) => candidate.id === peer.id);
    return { readerDirectory, lease, renew, seesPeer };
  };
  afterEach(() => vi.restoreAllMocks());

  it("keep a peer whose file lease was renewed after the read and before its old expiry", async () => {
    const { lease, renew, seesPeer } = await setup();
    const clock = vi.spyOn(Date, "now");
    clock.mockReturnValue(lease.expiresAt - 5_000);
    expect(seesPeer()).toBe(true);                                   // fills the lease read
    renew(lease.expiresAt - 2_000);                                  // file-only renewal
    clock.mockReturnValue(lease.expiresAt + 1);                      // old expiry passed, read reused
    expect(seesPeer()).toBe(true);
    clock.mockReturnValue(lease.expiresAt + 5_000);
    expect(seesPeer()).toBe(true);
  });

  it("drop a peer whose lease lapsed and was not renewed", async () => {
    const { lease, seesPeer } = await setup();
    const clock = vi.spyOn(Date, "now");
    clock.mockReturnValue(lease.expiresAt - 5_000);
    expect(seesPeer()).toBe(true);
    clock.mockReturnValue(lease.expiresAt + 1);
    expect(seesPeer()).toBe(false);
  });

  it("read the lease files again after this host's heartbeat commits", async () => {
    const { readerDirectory, lease, renew, seesPeer } = await setup();
    const clock = vi.spyOn(Date, "now");
    clock.mockReturnValue(lease.expiresAt + 1_000);
    expect(seesPeer()).toBe(false);                                  // lapsed when read: the read is reused
    renew(lease.expiresAt + 1_500);                                  // the peer comes back
    clock.mockReturnValue(lease.expiresAt + 2_000);
    const confirmed = readerDirectory.confirmedAt();
    await readerDirectory.refresh();                                  // a confirming heartbeat
    expect(readerDirectory.confirmedAt()).toBeGreaterThan(confirmed);
    expect(seesPeer()).toBe(true);                                   // no view older than the commit
  });
});

describe("MeshStore.list", () => {
  it("returns the first page, sorted, as copies", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
    const identity: MeshIdentity = { id: "session:a", name: "main", kind: "main" };
    for (const key of ["c", "a", "b"]) await store.put({ key: "x/" + key, value: { key }, identity });
    const page = store.list("x/", 2);
    expect(page.map((entry) => entry.key)).toEqual(["x/a", "x/b"]);
    (page[0]!.value as { key: string }).key = "mutated";
    expect((store.get("x/a")!.value as { key: string }).key).toBe("a");
  });
});
