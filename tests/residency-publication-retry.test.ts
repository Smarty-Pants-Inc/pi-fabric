import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { LIVENESS_POLICY_KEY, readHostLease } from "../src/topology/host-leases.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { readParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [], hosts: ResidentHost[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(hosts.splice(0).map(host => host.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const fixture = async (files = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-presence-batch-")); roots.push(root);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:absent-main", sessionId: "absent-main", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:absent-main"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const now = Date.now();
  const records = Array.from({ length: 40 }, (_, index) => ({
    id: (index + 1).toString(16).padStart(32, "0"), name: `idle-${index}`, instructions: "wait",
    createdAt: now, updatedAt: now, rootId: config.rootId, residency: "durable", runner: "pi", status: "idle",
    scope: index < 20 ? "project" : "session", events: [], topics: [], messages: [],
  }));
  for (const [scope, actorRoot] of [["project", config.actorRoot], ["session", config.sessionActorRoot!]]) {
    fs.mkdirSync(actorRoot!, { recursive: true });
    fs.writeFileSync(path.join(actorRoot!, "actors.json"), JSON.stringify({ format: 1, actors: records.filter(row => row.scope === scope) }));
  }
  if (files) await new MeshStore(config.meshRoot, 256 * 1024, 500).put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, hostLeases: "files", participants: "files" }, identity: { id: "policy", name: "policy", kind: "agent" } });
  const host = new ResidentHost(config); hosts.push(host); const heartbeatStartedAt = Date.now(); await host.start();
  await delay(20); await host.participants.refresh();
  return { root, config, host, records, heartbeatStartedAt };
};

describe("RC3.1 #4383 outside-custody publication recovery", () => {
  it.each([
    { hold: 4_900, gap: 100, phase: 650, files: false },
    { hold: 9_000, gap: 1_000, phase: 650, files: false },
  ])("renews and dispatches actor events during periodic mesh holds (%j)", async ({ hold, gap, phase, files }) => {
    const { host, config, records, heartbeatStartedAt } = await fixture(files);
    const actor = records[0]!;
    // A skip filter records real ActorManager delivery without launching a worker.
    await host.actors.setActivationFilter(actor.id, [{ id: "probe", topic: ["fleet.phase-lock"], kind: ["skip"] }]);
    const gate = vi.spyOn(host.participants, "canConsumeMesh").mockReturnValue(false);
    const event = await host.mesh.publish({ topic: "fleet.phase-lock", kind: "skip", to: actor.id, from: host.identity });
    await delay(Math.max(0, heartbeatStartedAt + 5_000 - phase - Date.now()));
    const child = spawn(process.execPath, [path.resolve("tests/fixtures/hold-mesh-lock.mjs"), config.meshRoot,
      String(hold), String(gap), "16000"], { stdio: ["ignore", "pipe", "inherit"] });
    const exited = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    let holderExited = false; void exited.then(() => { holderExited = true; });
    try {
      await new Promise<void>((resolve, reject) => { child.stdout.once("data", () => resolve()); child.once("error", reject); });
      const started = Date.now(), prior = host.participants.confirmedAt();
      await delay(phase + 150); // first automatic heartbeat's 50 ms attempt must fail
      gate.mockRestore();
      expect(host.actors.status(actor.id).filteredCount ?? 0).toBe(0);
      expect(host.participants.confirmedAt()).toBe(prior);
      expect(host.participants.canConsumeMesh()).toBe(false);
      let progressedAt = 0;
      while (Date.now() - started < 15_000) {
        // RC3.1's phase-lock proof is the committed heartbeat/lease and real
        // event progress. Sequential participant-file copies are covered by the
        // dedicated file-only receipt/order regression below.
        const renewed = host.participants.confirmedAt() > started &&
          host.participants.canConsumeMesh();
        if (renewed && host.actors.status(actor.id).filteredCount === 1) {
          progressedAt = Date.now(); break;
        }
        await delay(25);
      }
      expect(progressedAt, "committed heartbeat and real actor-event delivery within three periods").toBeGreaterThan(0);
      expect(holderExited, "recovery must happen while the periodic holder continues").toBe(false);
      expect(readHostLease(config.meshRoot, host.hostId)!.updatedAt).toBeGreaterThan(started);
      console.log(JSON.stringify({ regression: "periodic-mesh-progress", hold, gap, phase, files,
        progressMs: progressedAt - started, actors: 40, eventId: event.id, filteredCount: 1, holderStillRunning: !holderExited }));
    } finally { gate.mockRestore(); await exited; }
    expect(await exited).toBe(0);
  }, 30000);

  it("confirms file-only writability before sequential copies and preserves receipt age", async () => {
    const { host, config } = await fixture(true);
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const receiptAt = now;
    const write = participantFiles.writeParticipantFileIf;
    let copies = 0;
    vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation(async (...args) => {
      for (const root of [config.actorRoot, config.sessionActorRoot!]) {
        expect(fs.existsSync(path.join(root, "actors.json.lock", "owner"))).toBe(true);
      }
      const result = await write(...args);
      copies++; now += 300; // Slow sequential work must not rejuvenate the receipt.
      return result;
    });
    const confirm = host.mesh.confirmWritable.bind(host.mesh);
    const proof = vi.spyOn(host.mesh, "confirmWritable").mockImplementation(callback => {
      expect(copies).toBe(0);
      return confirm(callback);
    });
    await host.participants.refresh();
    expect(copies).toBe(40);
    expect(proof).toHaveBeenCalledOnce();
    expect(now - receiptAt).toBe(12_000);
    expect(host.participants.confirmedAt()).toBe(receiptAt);
  });

  it("reselects adopted actors after the out-of-custody admission wait", async () => {
    const { host, config, records } = await fixture();
    const child = spawn(process.execPath, [path.resolve("tests/fixtures/hold-mesh-lock.mjs"), config.meshRoot, "2000"],
      { stdio: ["ignore", "pipe", "inherit"] });
    const exited = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    try {
      await new Promise<void>((resolve, reject) => { child.stdout.once("data", () => resolve()); child.once("error", reject); });
      const prior = host.participants.confirmedAt(), id = records[0]!.id, key = `actors/${config.sessionId}/${id}`;
      const presence = host.mesh.get(key, { fresh: true });
      const waitForAdmission = host.participants.options.waitForPublicationRetry!;
      let entered!: () => void;
      const waiting = new Promise<void>(resolve => { entered = resolve; });
      vi.spyOn(host.participants.options, "waitForPublicationRetry").mockImplementation(() => {
        for (const root of [config.actorRoot, config.sessionActorRoot!]) {
          expect(fs.existsSync(path.join(root, "actors.json.lock", "owner"))).toBe(false);
        }
        entered(); return waitForAdmission();
      });
      await expect(host.participants.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      await waiting;
      expect(host.participants.confirmedAt()).toBe(prior);
      expect(host.participants.canConsumeMesh()).toBe(false);
      const registry = new ActorRegistryStore(config.actorRoot);
      const started = Date.now();
      await registry.withLock(() => registry.write(registry.records().map(row => row.id === id
        ? { ...row, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [config.rootId] } : row)));
      expect(Date.now() - started).toBeLessThan(500);
      await exited;
      for (let i = 0; i < 200 && host.participants.confirmedAt() === prior; i++) await delay(25);
      expect(host.participants.confirmedAt()).toBeGreaterThan(prior);
      expect(host.participants.canConsumeMesh()).toBe(true);
      expect(host.mesh.get(key, { fresh: true })).toEqual(presence);
      expect(host.actors.owns(id)).toBe(false);
    } finally { await exited; }
  }, 10000);
});
