import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, readHostLeases, STATE_LEASE_RENEW_MS } from "../src/topology/host-leases.js";
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
): ParticipantDirectory => {
  const hostId = identity.kind === "main" ? identity.id : "runtime:" + identity.sessionId;
  const directory = new ParticipantDirectory(
    new MeshStore(meshRoot, 64 * 1024, 1_000),
    { enabled: true, hostId, rootId, identity, heartbeatMs: 100, leaseMs: 300 },
  );
  directory.registerSource(source);
  directories.push(directory);
  return directory;
};

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
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

  it("under the policy, still renew the shared record every STATE_LEASE_RENEW_MS", async () => {
    const { hostEntry } = await setup(true);
    const shared = hostEntry()!;
    const now = Date.now;
    vi.spyOn(Date, "now").mockImplementation(() => now() + STATE_LEASE_RENEW_MS);
    await vi.waitFor(() => expect(hostEntry()!.version).toBeGreaterThan(shared.version), { timeout: 2_000, interval: 20 });
    vi.restoreAllMocks();
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
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ]);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [
      rootRecord(betaIdentity.id, betaIdentity.id, "beta"),
    ]);
    const recursive = createDirectory(meshRoot, recursiveIdentity, alphaIdentity.id, () => [
      agentRecord(
        "agent:grandchild",
        alphaIdentity.id,
        "runtime:recursive-session",
        recursiveIdentity.id,
      ),
    ]);

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
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "slow"),
      agentRecord("agent:slow", identity.id, identity.id, identity.id),
    ]);
    const write = directory.mesh.writeBatch.bind(directory.mesh);
    // A contended Windows mesh makes the heartbeat write outlast the 300ms lease;
    // the host lease is stamped at commit, so it must still be fresh afterwards.
    vi.spyOn(directory.mesh, "writeBatch").mockImplementation(async (input) => {
      await new Promise((resolve) => setTimeout(resolve, 350));
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
    ]);

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
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      shared(alphaIdentity),
    ]);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [
      rootRecord(betaIdentity.id, betaIdentity.id, "beta"),
      shared(betaIdentity),
    ]);

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

    it("still renews the lease on a heartbeat with unchanged records", async () => {
      const { directory, writes } = await changing();
      await directory.refresh();
      expect(writes).toHaveBeenCalledTimes(1);
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
    expect(steady).toBe(1);
  });

  // smarty-dev#266: a stopped mesh lock holder expired every lease; sessions() said [].
  const stallDirectory = (name: string, source: () => FabricParticipantRecord[]) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: "main", sessionId: name };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 150 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 300,
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
      .toMatch(/^Fabric mesh is write-stalled: Timed out waiting for the Fabric mesh lock/);
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
