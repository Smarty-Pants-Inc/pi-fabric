import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { PARTICIPANT_HEARTBEAT_MS, ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const waitFor = async (predicate: () => boolean, boundMs: number, label: string, onWait = () => {}) => {
  const deadline = Date.now() + boundMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out after ${boundMs}ms: ${label}`);
    onWait();
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

// smarty-dev#11172 moved the default participant heartbeat to 30 s (TTL 90 s, confirmation 60 s).
// This test proves physical timer/lock interleaving across several real heartbeats, so only the
// ParticipantDirectory #armHeartbeat interval physically fires every TEST_PHYSICAL_HEARTBEAT_MS.
// Every directory keeps the production 30 s heartbeat, 90 s lease and 60 s confirmation
// arithmetic: no heartbeatMs option (which would also select the legacy 15 s TTL), no fake clock.
const TEST_PHYSICAL_HEARTBEAT_MS = 5_000;
const redirectParticipantHeartbeat = () => {
  const realSetInterval = globalThis.setInterval;
  let redirected = 0;
  const spy = vi.spyOn(globalThis, "setInterval").mockImplementation(((handler: () => void, delay?: number, ...args: unknown[]) => {
    if (delay === PARTICIPANT_HEARTBEAT_MS && new Error().stack?.includes("armHeartbeat")) {
      redirected++;
      delay = TEST_PHYSICAL_HEARTBEAT_MS;
    }
    return realSetInterval(handler, delay, ...args);
  }) as unknown as typeof setInterval);
  return { spy, redirected: () => redirected };
};

/** Save a real public status change, leaving publication for the heartbeat. */
const stageStop = async (host: ResidentHost, id: string) => {
  const refresh = vi.spyOn(host.participants, "refreshPresence").mockResolvedValue();
  try { await host.actors.stop(id); }
  finally { refresh.mockRestore(); }
};

it("completes a real resident setter overlapped with automatic changed publication, an effect-free heartbeat, and stale publication refusal", async () => {
  // No fake clocks, timer advancement, manual refresh after arming, or lock adapters.
  // Gates only arrange interleaving; production registry/mesh locks and commits run.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-renewal-control-"));
  const identity = { id: "session:renewal-control", name: "Main", kind: "main" as const, sessionId: "renewal-control" };
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), identity.id),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const main = new ParticipantDirectory(new MeshStore(config.meshRoot, 65_536, 1_000), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
  });
  main.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
    runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  const host = new ResidentHost(config);
  const client = new ResidentActorClient(config.meshRoot, config.rootId, 8_000);
  const releaseSetter = deferred(), releaseHeartbeat = deferred();
  let mutation: Promise<unknown> | undefined;
  const spies: Array<{ mockRestore(): void }> = [];
  try {
    const heartbeat = redirectParticipantHeartbeat();
    spies.push(heartbeat.spy);
    await main.start();
    await host.start();
    expect(heartbeat.redirected()).toBeGreaterThanOrEqual(2); // main + resident host, never a legacy cadence
    const actor = await client.createActor({ name: "concurrent-review", instructions: "wait", residency: "durable" });
    const sibling = await client.createActor({ name: "pending-review", instructions: "wait", residency: "durable" });
    await host.participants.refresh();
    // Drain the creation's change-only publication before arming the timer probe.
    await new Promise(resolve => setTimeout(resolve, 1_100));
    const key = "topology/participants/" + createHash("sha256").update(actor.id).digest("hex");
    const registryLock = path.join(config.actorRoot, "actors.json.lock", "owner");
    const sessionLock = path.join(config.sessionActorRoot!, "actors.json.lock", "owner");
    const meshLock = path.join(config.meshRoot, ".lock", "owner");
    const initial = readParticipantFile(config.meshRoot, key)!;
    const registry = new ActorRegistryStore(config.actorRoot);
    // Leave the status change for the AUTOMATIC full heartbeat, not the registry
    // watch's earlier change-only flush. Restore scheduling once that batch is selected.
    const changeFlush = vi.spyOn(host.participants, "scheduleRefresh").mockImplementation(() => {});
    spies.push(changeFlush);
    await stageStop(host, actor.id);
    expect(readParticipantFile(config.meshRoot, key)).toEqual(initial);
    let automaticStarted = 0, automaticCompleted = 0, selected = false, sawBothLocks = false;
    const refresh = host.participants.refresh.bind(host.participants);
    spies.push(vi.spyOn(host.participants, "refresh").mockImplementation(async () => {
      const sequence = ++automaticStarted;
      await refresh();
      automaticCompleted = sequence;
    }));
    const batch = host.mesh.writeBatch.bind(host.mesh);
    spies.push(vi.spyOn(host.mesh, "writeBatch").mockImplementation(async input => {
      const overlap = automaticStarted === 1 && !selected && input.ops.some(op => op.key === key && op.kind === "put");
      if (overlap) {
        expect(input.ops.find(op => op.key === key && op.kind === "put")).toMatchObject({ kind: "put", value: { status: "stopped" } });
        selected = true;
        changeFlush.mockRestore();
        await releaseHeartbeat.promise;
      }
      return batch({ ...input, afterCommit: view => {
        if (overlap) {
          // Observe actual physical owners inside the production mesh critical section.
          // smarty-dev#8526: an actor-only round locks the registry holding that actor (the
          // project root here), never the session registry it publishes nothing from.
          sawBothLocks = [registryLock, meshLock].every(file =>
            fs.readFileSync(file, "utf8").split("\n")[1] === String(process.pid)) && !fs.existsSync(sessionLock);
        }
        input.afterCommit?.(view);
      } });
    }));
    const setInstructions = host.actors.setInstructions.bind(host.actors);
    let entered = false, saving = false, settled = false;
    spies.push(vi.spyOn(host.actors, "setInstructions").mockImplementation(async (...args) => {
      entered = true;
      await releaseSetter.promise;
      saving = true;
      return setInstructions(...args);
    }));
    mutation = client.setActor({ operation: "setInstructions", id: actor.id, instructions: "new persona", replace: true },
      undefined, { identity, hostId: identity.id }).then(value => { settled = true; return value; }, error => { settled = true; return error; });
    await waitFor(() => entered || settled, 2_000, "resident setter enters through public command dispatch");
    expect(entered).toBe(true);
    await waitFor(() => selected, 6_500, "automatic heartbeat selects real actor source", () => expect(host.participants.canConsumeMesh()).toBe(true));
    const heldRegistryOwner = fs.readFileSync(registryLock, "utf8");
    releaseSetter.resolve();
    await waitFor(() => saving, 500, "real setter begins saving");
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(settled).toBe(false);
    expect(fs.readFileSync(registryLock, "utf8")).toBe(heldRegistryOwner);
    expect(new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actor.id)?.instructions).toBe("wait");
    releaseHeartbeat.resolve();
    // An inverted mesh->registry control deadlocks here until the registry timeout
    // (5 s); the correct order commits both operations in much less than that bound.
    await waitFor(() => settled && automaticCompleted >= 1, 1_500, "setter and overlapping renewal complete",
      () => expect(host.participants.canConsumeMesh()).toBe(true));
    expect(await mutation).toMatchObject({ id: actor.id });
    expect(host.actors.instructions(actor.id)).toBe("new persona");
    expect(sawBothLocks).toBe(true);
    const afterMutation = readParticipantFile(config.meshRoot, key)!;
    expect(afterMutation.updatedAt).toBeGreaterThan(initial.updatedAt);
    expect(afterMutation.value).toMatchObject({ status: "stopped" });
    const afterPresence = host.mesh.get(`actors/${config.sessionId}/${actor.id}`, { fresh: true })!;
    const afterLease = readHostLease(config.meshRoot, host.hostId)!;
    await waitFor(() => automaticCompleted >= 2, 6_500, "next automatic heartbeat completes",
      () => expect(host.participants.canConsumeMesh()).toBe(true));
    expect(readParticipantFile(config.meshRoot, key)).toEqual(afterMutation);
    expect(host.mesh.get(`actors/${config.sessionId}/${actor.id}`, { fresh: true })).toEqual(afterPresence);
    expect(readHostLease(config.meshRoot, host.hostId)!.updatedAt).toBeGreaterThan(afterLease.updatedAt);

    // Adoption precedes a pending public change's registry-fenced publication.
    // Cached directory content must not resurrect it without successor publication.
    const staleFlush = vi.spyOn(host.participants, "scheduleRefresh").mockImplementation(() => {});
    spies.push(staleFlush);
    await stageStop(host, sibling.id);
    let adoptedRow: Record<string, unknown> | undefined;
    const next = automaticStarted + 1;
    await registry.withLock(async () => {
      await waitFor(() => automaticStarted >= next, 6_500, "automatic stale publication meets registry custody",
        () => expect(host.participants.canConsumeMesh()).toBe(true));
      registry.write(registry.records().map(row => row.id === actor.id
        ? { ...row, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [config.rootId] } : row));
      adoptedRow = registry.records().find(row => row.id === actor.id);
      staleFlush.mockRestore();
    });
    await waitFor(() => automaticCompleted >= next, 1_500, "stale publication completes without republishing",
      () => expect(host.participants.canConsumeMesh()).toBe(true));
    expect(readParticipantFile(config.meshRoot, key)).toBeUndefined();
    expect(host.mesh.get(key, { fresh: true })).toBeUndefined();
    expect(host.actors.owns(actor.id)).toBe(false);
    await host.close();
    expect(registry.records().find(row => row.id === actor.id)).toEqual(adoptedRow);
  } finally {
    releaseSetter.resolve(); releaseHeartbeat.resolve();
    await Promise.allSettled([mutation]);
    await host.close(); await main.close();
    for (const spy of spies) spy.mockRestore();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 25_000);
