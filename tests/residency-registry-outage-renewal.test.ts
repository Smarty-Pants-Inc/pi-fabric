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
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { readHostLease } from "../src/topology/host-leases.js";

const waitFor = async (predicate: () => boolean, boundMs: number, label: string) => {
  const until = Date.now() + boundMs;
  while (!predicate()) {
    if (Date.now() >= until) throw new Error(`Timed out: ${label}`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

it("#4383 real resident actor envelopes renew on every heartbeat through another process's 30 s registry hold", async () => {
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
    await main.start(); await host.start();
    const actors: Array<Awaited<ReturnType<typeof host.actors.create>>> = [];
    for (let index = 0; index < 3; index++) actors.push(await host.actors.create({ name: `outage-${index}`, instructions: "Stay live.", residency: "durable" }));
    await host.participants.refresh();
    await new Promise(resolve => setTimeout(resolve, 1_100)); // drain creation's change-only publication
    const keys = actors.map(actor => "topology/participants/" + createHash("sha256").update(actor.id).digest("hex"));
    const before = keys.map(key => readParticipantFile(config.meshRoot, key)!.updatedAt);
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
    let previous = before;
    for (let tick = 0; tick < 5; tick++) {
      await waitFor(() => keys.every((key, index) => (readParticipantFile(config.meshRoot, key)?.updatedAt ?? 0) > previous[index]!),
        7_000, `independent automatic actor renewal ${tick + 1}`);
      previous = keys.map(key => readParticipantFile(config.meshRoot, key)!.updatedAt);
      expect(fs.existsSync(lock)).toBe(true);
      expect(readHostLease(config.meshRoot, host.hostId)!.updatedAt).toBeGreaterThan(confirmed);
      for (const actor of actors) expect(observer.get(actor.id, Date.now(), { fresh: true })).toMatchObject({ stale: false, ownerHostId: host.hostId });
      // Liveness is not shared admission: the registry outage cannot falsely
      // advance the confirmed shared heartbeat while its fence is inaccessible.
      expect(host.participants.confirmedAt()).toBe(confirmed);
    }
    expect(await exited).toEqual({ code: 0, signal: null });
    expect(stderr).toBe("");
    expect(fs.existsSync(lock)).toBe(false);
    await host.participants.refresh();
    expect(host.participants.confirmedAt()).toBeGreaterThan(confirmed);
    const registry = new ActorRegistryStore(config.actorRoot);
    // A durable adoption without successor publication immediately invalidates
    // cached renewal; a full round also removes this predecessor's old envelope.
    await registry.withLock(() => registry.write(registry.records().map(row => row.id === actors[0]!.id
      ? { ...row, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [config.rootId] } : row)));
    await host.participants.refresh();
    expect(readParticipantFile(config.meshRoot, keys[0]!)).toBeUndefined();
    expect(host.actors.owns(actors[0]!.id)).toBe(false);

    // Fence a delayed renewal at its actual per-key decision, not just at timer
    // selection. Rotate the token while retaining the root: a root-only guard
    // would incorrectly renew this predecessor generation.
    const oldEnvelope = readParticipantFile(config.meshRoot, keys[1]!)!;
    const keyLock = path.join(config.meshRoot, "participants", ".locks", keys[1]!.split("/").at(-1)!);
    let rotated = false;
    let delayedRenewal: Promise<void> | undefined;
    const rename = fs.renameSync.bind(fs);
    const receipt = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (!rotated && String(to) === keyLock) {
        rotated = true;
        registry.write(registry.records().map(row => row.id === actors[1]!.id
          ? { ...row, adoptedAt: Date.now(), adoptedFrom: ["previous-generation"] } : row), { durable: true });
      }
    });
    try {
      await registry.withLock(async () => {
        delayedRenewal = host.participants.refresh();
        await waitFor(() => rotated, 1_000, "lineage rotation between key acquisition and renewal write");
        expect(readParticipantFile(config.meshRoot, keys[1]!)!.updatedAt).toBe(oldEnvelope.updatedAt);
      });
    } finally { receipt.mockRestore(); }
    await delayedRenewal;
    expect(readParticipantFile(config.meshRoot, keys[1]!)).toBeUndefined();
    expect(host.actors.owns(actors[1]!.id)).toBe(false);
  } finally {
    vi.restoreAllMocks();
    if (holder?.exitCode === null && holder.signalCode === null) holder.kill("SIGTERM");
    await exited?.catch(() => undefined);
    // A killed holder cannot release its name; remove only after confirmed exit.
    fs.rmSync(path.join(config.actorRoot, "actors.json.lock"), { recursive: true, force: true });
    await host.close(); await main.close(); await observer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 50_000);
