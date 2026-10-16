import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { PARTICIPANT_HEARTBEAT_MS, ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { readHostLease } from "../src/topology/host-leases.js";

const waitFor = async (predicate: () => boolean, boundMs: number, label: string) => {
  const until = Date.now() + boundMs;
  while (!predicate()) {
    if (Date.now() >= until) throw new Error(`Timed out: ${label}`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

// smarty-dev#11172 default participant heartbeat is 60 s (TTL 180 s, confirmation 120 s).
// This test proves physical timer/lock interleaving across five real heartbeats, so only the
// ParticipantDirectory #armHeartbeat interval physically fires every TEST_PHYSICAL_HEARTBEAT_MS.
// Every directory keeps the production 60 s heartbeat, 180 s lease and 120 s confirmation
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

/** Save public status on disk without flushing its presence before the heartbeat. */
const stageStop = async (host: ResidentHost, id: string) => {
  const refresh = vi.spyOn(host.participants, "refreshPresence").mockResolvedValue();
  const schedule = vi.spyOn(host.participants, "scheduleRefresh").mockImplementation(() => {});
  try { await host.actors.stop(id); }
  finally { refresh.mockRestore(); schedule.mockRestore(); }
};

it("#13645 a host lease advances through a 30 s registry hold without rewriting unchanged actor envelopes", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-registry-outage-"));
  const identity = { id: "session:outage", name: "Main", kind: "main" as const, sessionId: "outage" };
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), identity.id),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const main = new ParticipantDirectory(new MeshStore(config.meshRoot, 65_536, 100), {
    enabled: true, identity, hostId: identity.id, rootId: identity.id, reapDeadHosts: false,
  });
  main.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
    runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  const host = new ResidentHost(config);
  const observer = new ParticipantDirectory(new MeshStore(config.meshRoot, 65_536, 100), {
    enabled: true, identity: { id: "observer", name: "observer", kind: "agent" }, hostId: "observer", rootId: "observer", reapDeadHosts: false,
  });
  let holder: ReturnType<typeof spawn> | undefined;
  let exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }> | undefined;
  try {
    const heartbeat = redirectParticipantHeartbeat(); // restored by vi.restoreAllMocks() below
    await main.start(); await host.start();
    expect(heartbeat.redirected()).toBeGreaterThanOrEqual(2); // main + resident host, never a legacy cadence
    const actors: Array<Awaited<ReturnType<typeof host.actors.create>>> = [];
    for (let index = 0; index < 3; index++) actors.push(await host.actors.create({ name: `outage-${index}`, instructions: "Stay live.", residency: "durable" }));
    await host.participants.refresh();
    await new Promise(resolve => setTimeout(resolve, 1_100)); // drain creation's change-only publication
    const keys = actors.map(actor => "topology/participants/" + createHash("sha256").update(actor.id).digest("hex"));
    const before = keys.map(key => readParticipantFile(config.meshRoot, key)!);
    const presenceKeys = actors.map(actor => `actors/${config.sessionId}/${actor.id}`);
    const beforePresence = presenceKeys.map(key => host.mesh.get(key, { fresh: true })!);
    const beforeShared = keys.map(key => host.mesh.get(key, { fresh: true }));
    const fence = vi.spyOn(host.participants.options, "withPublicationFence");
    // An idle round can confirm without registry custody; deliberately select a
    // real public actor effect so this round must wait for the foreign holder.
    await stageStop(host, actors[0]!.id);
    expect(keys.map(key => readParticipantFile(config.meshRoot, key))).toEqual(before);
    const lock = path.join(config.actorRoot, "actors.json.lock");
    // A genuine foreign PID owns the existing registry protocol. No fake timers,
    // mocked lock, accelerated clock, or manual refresh can make this test pass.
    holder = spawn(process.execPath, ["-e", `
      const fs = require('node:fs'), path = require('node:path');
      const lock = ${JSON.stringify(lock)};
      fs.mkdirSync(lock);
      fs.writeFileSync(path.join(lock,'owner'), 'outage\\n'+process.pid+'\\n'+Date.now()+'\\n');
      process.stdout.write('held\\n');
      setTimeout(()=>{fs.rmSync(lock,{recursive:true});}, 30000);
    `], { stdio: ["ignore", "pipe", "pipe"] });
    exited = new Promise((resolve, reject) => { holder!.once("error", reject); holder!.once("exit", (code, signal) => resolve({ code, signal })); });
    let ready = false, stderr = "";
    holder.stdout!.on("data", chunk => { if (String(chunk).includes("held")) ready = true; });
    holder.stderr!.on("data", chunk => { stderr += chunk; });
    await waitFor(() => ready, 3_000, "foreign registry holder starts");
    const confirmed = host.participants.confirmedAt();
    let previous = readHostLease(config.meshRoot, host.hostId)!.updatedAt;
    for (let tick = 0; tick < 5; tick++) {
      await waitFor(() => (readHostLease(config.meshRoot, host.hostId)?.updatedAt ?? 0) > previous,
        7_000, `automatic host lease renewal ${tick + 1}`);
      const lease = readHostLease(config.meshRoot, host.hostId)!;
      expect(lease.updatedAt).toBeGreaterThan(previous);
      expect(lease.expiresAt).toBeGreaterThan(Date.now());
      previous = lease.updatedAt;
      expect(fs.existsSync(lock)).toBe(true);
      expect(keys.map(key => readParticipantFile(config.meshRoot, key))).toEqual(before);
      expect(keys.map(key => host.mesh.get(key, { fresh: true }))).toEqual(beforeShared);
      expect(presenceKeys.map(key => host.mesh.get(key, { fresh: true }))).toEqual(beforePresence);
      for (const actor of actors) expect(observer.get(actor.id, Date.now(), { fresh: true })).toMatchObject({ stale: false, ownerHostId: host.hostId });
      // Liveness is not shared admission: the registry outage cannot falsely
      // advance the confirmed shared heartbeat while its fence is inaccessible.
      expect(host.participants.confirmedAt()).toBe(confirmed);
    }
    expect(fence.mock.calls.some(([, ids]) => ids?.has(actors[0]!.id))).toBe(true);
    expect(await exited).toEqual({ code: 0, signal: null });
    expect(stderr).toBe("");
    expect(fs.existsSync(lock)).toBe(false);
    await host.participants.refresh();
    expect(host.participants.confirmedAt()).toBeGreaterThan(confirmed);
    expect(readParticipantFile(config.meshRoot, keys[0]!)?.value).toMatchObject({ status: "stopped" });
    expect(host.mesh.get(presenceKeys[0]!, { fresh: true })?.value).toMatchObject({ status: "stopped" });
    for (let index = 1; index < actors.length; index++) {
      expect(readParticipantFile(config.meshRoot, keys[index]!)).toEqual(before[index]);
      expect(host.mesh.get(presenceKeys[index]!, { fresh: true })).toEqual(beforePresence[index]);
    }
    const registry = new ActorRegistryStore(config.actorRoot);
    // A durable adoption without successor publication immediately invalidates
    // cached renewal; a full round also removes this predecessor's old envelope.
    await registry.withLock(() => registry.write(registry.records().map(row => row.id === actors[0]!.id
      ? { ...row, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [config.rootId] } : row)));
    await host.participants.refresh();
    expect(readParticipantFile(config.meshRoot, keys[0]!)).toBeUndefined();
    expect(host.actors.owns(actors[0]!.id)).toBe(false);

    // Delay a real changed copy, then rotate its token without moving its root.
    // The per-key decision must refuse the predecessor even without a successor file.
    host.participants.suspend();
    const key = keys[1]!, copyBefore = readParticipantFile(config.meshRoot, key)!;
    await stageStop(host, actors[1]!.id);
    let copying = false, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const write = participantFiles.writeParticipantFileIf;
    const receipt = vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation(async (...args) => {
      if (args[1] === key && !copying) { copying = true; await gate; }
      return write(...args);
    });
    const delayedCopy = host.participants.refresh().catch(error => error);
    try {
      await waitFor(() => copying, 3_000, "changed publication reaches its participant-file copy");
      expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
      expect(host.mesh.get(key, { fresh: true })?.value).toMatchObject({ status: "stopped" });
      expect(readParticipantFile(config.meshRoot, key)).toEqual(copyBefore);
      await registry.withLock(() => registry.write(registry.records().map(row => row.id === actors[1]!.id
        ? { ...row, adoptedAt: Date.now(), adoptedFrom: ["previous-generation"] } : row), { durable: true }));
      const rotated = registry.records().find(row => row.id === actors[1]!.id)!;
      expect(rotated.rootId).toBe(config.rootId);
      expect(JSON.stringify([rotated.rootId, rotated.adoptedAt ?? null, rotated.adoptedFrom ?? []]))
        .not.toBe((copyBefore.value as { actorOwnershipToken?: string }).actorOwnershipToken);
      release();
      expect(await delayedCopy).toBeUndefined();
      expect(readParticipantFile(config.meshRoot, key)).toEqual(copyBefore);
      receipt.mockRestore();
      await host.participants.refresh();
      expect(readParticipantFile(config.meshRoot, key)).toBeUndefined();
      expect(host.mesh.get(key, { fresh: true })).toBeUndefined();
      expect(host.actors.owns(actors[1]!.id)).toBe(false);
      await host.close();
      expect(registry.records().find(row => row.id === actors[1]!.id)).toEqual(rotated);
    } finally { release(); await delayedCopy; receipt.mockRestore(); }
  } finally {
    vi.restoreAllMocks();
    if (holder?.exitCode === null && holder.signalCode === null) holder.kill("SIGTERM");
    await exited?.catch(() => undefined);
    // A killed holder cannot release its name; remove only after confirmed exit.
    fs.rmSync(path.join(config.actorRoot, "actors.json.lock"), { recursive: true, force: true });
    await host.close(); await main.close(); await observer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 60_000);
