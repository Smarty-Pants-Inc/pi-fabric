#!/usr/bin/env bun
// Isolated, real ResidentHost; automatic 5 s heartbeats, 40 idle actors, external mesh holder.
// Run: nice -n 19 bun scripts/probe-registry-hold-wait.ts --duration=30000 --hold=9000 --gap=1000
import fs from "node:fs";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { MeshStore } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, "").split("=")));
const duration = Number(args.duration ?? 30000), hold = Number(args.hold ?? 9000), gap = Number(args.gap ?? 1000);
const phase = Number(args.phase ?? 650), files = args.files === "true";
const root = fs.mkdtempSync(path.join(os.tmpdir(), "regfence-probe-"));
const config: ResidentHostConfig = {
  format: 1, rootId: "session:isolated-probe", sessionId: "isolated-probe", cwd: root, projectRoot: root,
  meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
  residencyRoot: residentRoot(path.join(root, "mesh"), "session:isolated-probe"), fullCodeMode: true,
  agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
  retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/worker.js"),
  fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
};
fs.mkdirSync(config.residencyRoot, { recursive: true });
fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
const now = Date.now();
for (const [scope, actorRoot] of [["project", config.actorRoot], ["session", config.sessionActorRoot!]]) {
  fs.mkdirSync(actorRoot, { recursive: true });
  fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: Array.from({ length: 20 }, (_, i) => ({
    id: (i + 1 + (scope === "session" ? 20 : 0)).toString(16).padStart(32, "0"), name: `idle-${scope}-${i}`, instructions: "wait", createdAt: now, updatedAt: now,
    rootId: config.rootId, residency: "durable", runner: "pi", status: "idle", scope, events: [], topics: [], messages: [],
  })) }));
}
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const host = new ResidentHost(config);
let child: ReturnType<typeof spawn> | undefined, exited: Promise<number | null> | undefined;
let eventWrite: Promise<unknown> | undefined;
try {
  if (files) await new MeshStore(config.meshRoot, 256 * 1024, 500).put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, hostLeases: "files", participants: "files" }, identity: host.identity });
  const heartbeatStartedAt = Date.now();
  await host.start();
  await pause(50); await host.participants.refresh();
  assert.equal(host.actors.listOwned().length, 40, "must load all forty valid actors");
  const actorId = "1".padStart(32, "0");
  // Real actor dispatch telemetry without a worker launch.
  await host.actors.setActivationFilter(actorId, [{ id: "probe", topic: ["fleet.registry-probe"], kind: ["skip"] }]);
  // Start the measurement just before an automatic heartbeat enters the contention window.
  await pause(Math.max(0, heartbeatStartedAt + 5_000 - phase - Date.now()));
  child = spawn(process.execPath, [path.resolve("tests/fixtures/hold-mesh-lock.mjs"), config.meshRoot,
    String(hold), String(gap), String(duration)], { stdio: ["ignore", "pipe", "inherit"] });
  exited = new Promise((resolve, reject) => { child!.once("error", reject); child!.once("close", code => resolve(code)); });
  await new Promise<void>((resolve, reject) => {
    child!.stdout!.once("data", () => resolve()); child!.once("error", reject);
    child!.once("exit", () => reject(new Error("Holder exited before ready")));
  });
  const registryOwners = [config.actorRoot, config.sessionActorRoot!].map(root => path.join(root, "actors.json.lock", "owner"));
  const meshOwner = path.join(config.meshRoot, ".lock", "owner");
  const sample = (file: string) => { try { return fs.readFileSync(file, "utf8").split("\n")[1]; } catch { return undefined; } };
  let samples = 0, meshBusy = 0;
  const registryBusy = [0, 0], selfBusy = [0, 0];
  const started = performance.now();
  let lastCommit = host.participants.confirmedAt(), actorEventProgressMs = 0;
  const commits: number[] = [];
  eventWrite = host.mesh.publish({ topic: "fleet.registry-probe", kind: "skip", to: actorId, from: host.identity });
  // Observe rejection now; still join and surface it below (and in cleanup).
  void eventWrite.catch(() => undefined);
  while (performance.now() - started < duration) {
    samples++; if (sample(meshOwner)) meshBusy++;
    const committed = host.participants.confirmedAt();
    if (committed > lastCommit) { commits.push(Math.round(performance.now() - started)); lastCommit = committed; }
    if (!actorEventProgressMs && host.actors.status(actorId).filteredCount === 1) {
      actorEventProgressMs = Math.round(performance.now() - started);
    }
    registryOwners.forEach((file, i) => {
      const pid = sample(file); if (pid) registryBusy[i]++; if (pid === String(process.pid)) selfBusy[i]++;
    });
    await pause(50);
  }
  const percent = (count: number) => Number((100 * count / samples).toFixed(2));
  assert.equal(host.actors.listOwned().length, 40, "must still own all forty actors");
  await eventWrite;
  console.log(JSON.stringify({ durationMs: Math.round(performance.now() - started), actors: host.actors.listOwned().length, samples,
    externalHolderPid: child.pid, meshBusyPercent: percent(meshBusy),
    registryBusyPercent: registryBusy.map(percent), residentSelfBusyPercent: selfBusy.map(percent),
    command: `nice -n 19 bun scripts/probe-registry-hold-wait.ts --duration=${duration} --hold=${hold} --gap=${gap} --phase=${phase} --files=${files}`,
    automaticHeartbeatsOnly: true, phaseMs: phase, files, committedHeartbeatsDuringHold: commits.length, commitProgressMs: commits,
    actorEventProgressMs, actorEventFilteredCount: host.actors.status(actorId).filteredCount }, null, 2));
  assert.ok(registryBusy.every(count => percent(count) < 10), "registry busy ratio must stay below 10%");
  assert.ok(commits.length >= 2 && commits[0]! <= 15_000, "heartbeats must commit while holder continues, within three periods");
  assert.ok(actorEventProgressMs > 0 && actorEventProgressMs <= 15_000, "actor-event delivery must progress within three periods");
  if (await exited !== 0) throw new Error("External mesh holder failed");
} finally {
  try { if (exited) await exited; await eventWrite; }
  finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
}
