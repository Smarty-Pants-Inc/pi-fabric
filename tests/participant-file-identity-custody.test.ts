import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentHost } from "../src/residency/host.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const roots: string[] = [], hosts: ResidentHost[] = [], reads: Promise<unknown>[] = [];
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
afterEach(async () => {
  await Promise.all(reads.splice(0)); // join even deliberately non-cooperative native runners
  vi.restoreAllMocks();
  await Promise.all(hosts.splice(0).map(host => host.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = async (protocol: 1 | 2, files: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "cold-participant-custody-")); roots.push(root);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:cold-proof", sessionId: "cold-proof", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), "session:cold-proof"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, lockProtocol: protocol },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const actorRoots = [config.actorRoot, config.sessionActorRoot!];
  const rows = actorRoots.map((actorRoot, i) => {
    fs.mkdirSync(actorRoot, { recursive: true });
    const row = { id: String(i + 1).padStart(32, "0"), name: `idle-${i}`, instructions: "wait", createdAt: Date.now(), updatedAt: Date.now(),
      rootId: config.rootId, residency: "durable", runner: "pi", status: "idle", scope: i ? "session" : "project", events: [], topics: [], messages: [] };
    fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: [row] })); return row;
  });
  if (files) await new MeshStore(config.meshRoot, 65536, 100).put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, hostLeases: "files", participants: "files" }, identity: { id: "policy", name: "policy", kind: "agent" } });
  return { config, actorRoots, rows, custody: () => actorRoots.some(root => fs.existsSync(path.join(root, "actors.json.lock", "owner"))) };
};

const cases = (["darwin", "win32"] as const).flatMap(platform => ([1, 2] as const).flatMap(protocol =>
  [false, true].flatMap(files => (["known", "unknown", "timeout"] as const).map(outcome => ({ platform, protocol, files, outcome })))));
it.each(cases)("cold ResidentHost files=$files protocol=$protocol reader=$platform outcome=$outcome", async ({ platform, protocol, files, outcome }) => {
  const s = await fixture(protocol, files);
  const nativeValue = platform === "win32" ? "639264528000000000" : "Thu Oct  1 12:00:00 2026";
  let nativeUnderCustody = false;
  const run = vi.fn<atomic.IncarnationCommandRunner>(() => {
    nativeUnderCustody ||= s.custody();
    const work = pause(1200).then(() => outcome === "unknown" ? "unreadable" : nativeValue);
    reads.push(work); return work;
  });
  const reader = atomic.createProcessIncarnationReader({ platform, systemRoot: "C:\\Windows", run,
    timeoutMs: outcome === "timeout" ? 50 : 2000 });
  vi.spyOn(atomic, "ownProcessIncarnation").mockImplementation(reader.own);
  const custodyMs: number[] = [];
  const start = ParticipantDirectory.prototype.start;
  vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(function(this: ParticipantDirectory) {
    const fence = this.options.withPublicationFence!;
    this.options.withPublicationFence = publish => fence(async () => {
      const begin = performance.now();
      try { return await publish(); } finally { custodyMs.push(performance.now() - begin); }
    });
    return start.call(this);
  });
  let copying!: () => void;
  const copied = new Promise<void>(resolve => { copying = resolve; });
  let bothFences = false;
  const receipts: Array<{ prepared: boolean; incarnation: string | undefined }> = [];
  const write = participantFiles.writeParticipantFileIf;
  vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation((...args) => {
    bothFences ||= s.actorRoots.every(root => fs.existsSync(path.join(root, "actors.json.lock", "owner")));
    receipts.push({ prepared: Object.prototype.hasOwnProperty.call(args[3] ?? {}, "ownIncarnation"), incarnation: args[3]?.ownIncarnation });
    copying(); return write(...args);
  });
  const host = new ResidentHost(s.config); hosts.push(host);
  const starting = host.start(); void starting.catch(() => undefined);
  await copied;
  const begin = performance.now();
  await Promise.all(s.rows.map(row => host.actors.setInstructions(row.id, "durable setter during cold publication")));
  const setterMs = performance.now() - begin;
  await starting;
  console.log(JSON.stringify({ entrypoint: "cold ResidentHost participant-file publication", platform, protocol, files, outcome,
    simulatedNativeMs: 1200, setterMs, registryCustodyMs: Math.max(...custodyMs), nativeUnderCustody, durable: true }));
  expect(s.actorRoots.every(root => new ActorRegistryStore(root).records()[0]?.instructions === "durable setter during cold publication")).toBe(true);
  expect(nativeUnderCustody).toBe(false);
  expect(bothFences).toBe(true);
  expect(receipts.every(receipt => receipt.prepared && receipt.incarnation === (outcome === "known" ? `${platform}:${nativeValue}` : undefined))).toBe(true);
  expect(run).toHaveBeenCalledOnce();
  expect(setterMs).toBeLessThan(150);
  expect(Math.max(...custodyMs)).toBeLessThan(150);
  expect(participantFiles.readParticipantFiles(s.config.meshRoot)).toHaveLength(2);
}, 15000);

it.each([1, 2] as const)("busy participant-key native recovery prepares outside both registry fences (protocol=%s)", async protocol => {
  const s = await fixture(protocol, true);
  const host = new ResidentHost(s.config); hosts.push(host); await host.start();
  // Hold off the automatic retry only; each explicit refresh still uses production custody.
  let releaseRetry!: () => void;
  const retryGate = new Promise<void>(resolve => { releaseRetry = resolve; });
  vi.spyOn(host.participants.options, "waitForPublicationRetry").mockImplementation(() => retryGate);
  const keyLock = path.join(s.config.meshRoot, "participants", ".locks", createHash("sha256").update(s.rows[0]!.id).digest("hex"));
  fs.mkdirSync(keyLock, { recursive: true });
  const incarnation = (await atomic.processIncarnation(process.pid))!;
  const receipt = `${process.pid}\n${incarnation}\nheld-native-key\n`;
  fs.writeFileSync(path.join(keyLock, "owner"), receipt);
  const inode = fs.statSync(keyLock).ino;
  let nativeUnderCustody = false;
  let began!: () => void;
  const preparing = new Promise<void>(resolve => { began = resolve; });
  const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(() => {
    nativeUnderCustody ||= s.custody(); began();
    const work = pause(1200).then(() => undefined); reads.push(work); return work;
  });
  try {
    const begin = performance.now();
    await expect(host.participants.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(performance.now() - begin).toBeLessThan(150);
    expect(read).not.toHaveBeenCalled();
    const retry = host.participants.refresh().catch(error => error);
    await preparing;
    const setterBegin = performance.now();
    await Promise.all(s.actorRoots.map(root => new ActorRegistryStore(root).withLock(() => undefined)));
    const registryBlockedMs = performance.now() - setterBegin;
    expect(registryBlockedMs).toBeLessThan(150);
    expect(await retry).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(nativeUnderCustody).toBe(false); expect(read).toHaveBeenCalledOnce();
    expect(fs.readFileSync(path.join(keyLock, "owner"), "utf8")).toBe(receipt);
    expect(fs.statSync(keyLock).ino).toBe(inode); // unknown evidence never reaps a live holder
    fs.rmSync(keyLock, { recursive: true });
    await host.participants.refresh();
    expect(host.participants.canConsumeMesh()).toBe(true);
    console.log(JSON.stringify({ entrypoint: "busy participant-key retry", protocol, simulatedNativeMs: 1200, registryBlockedMs, nativeUnderCustody, unknownStayedLive: true }));
  } finally { fs.rmSync(keyLock, { recursive: true, force: true }); releaseRetry(); }
}, 10000);

it("a fenced caller missing its prepared own receipt fails closed instead of reading native identity", async () => {
  const s = await fixture(1, false);
  const mesh = new MeshStore(s.config.meshRoot, 65536, 100);
  const registries = s.actorRoots.map(root => new ActorRegistryStore(root));
  const key = `topology/participants/${createHash("sha256").update(s.rows[0]!.id).digest("hex")}`;
  const own = vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined);
  const decide = vi.fn(() => undefined);
  await ActorRegistryStore.withLocks(registries, async () => {
    await expect(participantFiles.writeParticipantFileIf(mesh, key, decide, { registryFenced: true }))
      .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(decide).not.toHaveBeenCalled();
    await expect(participantFiles.writeParticipantFileIf(mesh, key, decide, { registryFenced: true, ownIncarnation: undefined }))
      .resolves.toBe(false); // explicit prepared UNKNOWN is conservative, not unprepared
  });
  expect(own).not.toHaveBeenCalled();
});
