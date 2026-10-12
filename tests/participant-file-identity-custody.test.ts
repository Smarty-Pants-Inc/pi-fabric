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
// Every gate a test holds (fake native reads, retry admission, platform stubs) is
// released here BEFORE joining reads or closing hosts: a failed or timed-out test
// body must never leave a promise that hangs the hook.
const releases: Array<() => void> = [];
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
afterEach(async () => {
  for (const release of releases.splice(0)) release();
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
  let publicationDepth = 0;
  const start = ParticipantDirectory.prototype.start;
  vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(function(this: ParticipantDirectory) {
    const fence = this.options.withPublicationFence!;
    this.options.withPublicationFence = publish => fence(async () => {
      const begin = performance.now(); publicationDepth++;
      try { return await publish(); } finally { publicationDepth--; custodyMs.push(performance.now() - begin); }
    });
    return start.call(this);
  });
  let copying!: () => void;
  const copied = new Promise<void>(resolve => { copying = resolve; });
  let bothFences = false, copiedOutsideRegistry = false, copiesUnderKey = true, copyTokensMatch = true;
  const receipts: Array<{ prepared: boolean; incarnation: string | undefined }> = [];
  const write = participantFiles.writeParticipantFileIf;
  vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation((...args) => {
    bothFences ||= s.actorRoots.every(root => fs.existsSync(path.join(root, "actors.json.lock", "owner")));
    receipts.push({ prepared: Object.prototype.hasOwnProperty.call(args[3] ?? {}, "ownIncarnation"), incarnation: args[3]?.ownIncarnation });
    copying();
    return write(args[0], args[1], current => {
      const selected = args[2](current);
      const record = selected?.value as { kind?: string; id?: string; actorOwnershipToken?: string } | undefined;
      if (record?.kind === "actor" && record.id) {
        const lock = path.join(s.config.meshRoot, "participants", ".locks", args[1].split("/").at(-1)!);
        copiesUnderKey &&= fs.existsSync(path.join(lock, "owner"));
        copiedOutsideRegistry ||= publicationDepth === 0;
        const row = s.actorRoots.flatMap(root => new ActorRegistryStore(root).records()).find(row => row.id === record.id);
        copyTokensMatch &&= !!row && record.actorOwnershipToken === JSON.stringify([row.rootId, row.adoptedAt ?? null, row.adoptedFrom ?? []]);
      }
      return selected;
    }, args[3]);
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
  if (files) expect(bothFences).toBe(true); // files-first migration/publication retains the registry fence
  else expect(copiedOutsideRegistry).toBe(true); // accepted shared actor copies use token+key custody instead
  expect(copiesUnderKey).toBe(true);
  expect(copyTokensMatch).toBe(true);
  expect(receipts.every(receipt => receipt.prepared && receipt.incarnation === (outcome === "known" ? `${platform}:${nativeValue}` : undefined))).toBe(true);
  expect(run).toHaveBeenCalledOnce();
  // ponytail: 600 ms, half the injected 1,200 ms native read. A read awaited under custody costs >= 1,200 ms
  // and still fails here (nativeUnderCustody above is the direct check); hosted Windows NTFS file replacement
  // inside the fence measured 195 ms on CI, so the old 150 ms bound timed the runner's disk, not the defect.
  expect(setterMs).toBeLessThan(600);
  expect(Math.max(...custodyMs)).toBeLessThan(600);
  expect(participantFiles.readParticipantFiles(s.config.meshRoot)).toHaveLength(2);
}, 15000);

// A holder receipt whose start identity is valid for the platform under test, so the
// unfenced preparation lane must ask native evidence. Never take it from the real native
// reader: a cold windows-latest PowerShell read can exceed its 2 s budget and return
// UNKNOWN, and an unknown receipt is (correctly) judged live without any native read.
const holderIncarnation: Partial<Record<NodeJS.Platform, string>> = {
  linux: "4242", win32: "win32:639264528000000000", darwin: "darwin:Thu Oct  1 12:00:00 2026" };
const within = <T>(work: Promise<T>, ms: number, failure: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(failure)), ms); });
  return Promise.race([work, deadline]).finally(() => clearTimeout(timer));
};
const busyCases = ([1, 2] as const).flatMap(protocol => [...new Set([process.platform, "win32" as const])]
  .map(platform => ({ protocol, platform })));
// The order is the same for both lock protocols and every platform: #runRefresh awaits the
// prepared own identity, then prepareParticipantFileLocks (native holder evidence, no fence),
// and only then withPublicationFence (both registries -> mesh try-lock). The protocol only
// selects the mesh lock taken inside that fence, after the native read has been prepared.
it.each(busyCases)("busy participant-key native recovery prepares outside both registry fences (protocol=$protocol platform=$platform)", async ({ protocol, platform }) => {
  if (platform !== process.platform) {
    const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...descriptor, value: platform });
    releases.push(() => Object.defineProperty(process, "platform", descriptor));
  }
  const incarnation = holderIncarnation[platform];
  if (!incarnation) return; // no native start-identity reader on this platform
  expect(atomic.validProcessIncarnation(incarnation)).toBe(true);
  // Hermetic: no cold native own-identity read (PowerShell on Windows) inside the budget.
  vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined); // UNKNOWN own identity is publishable
  const s = await fixture(protocol, true);
  const host = new ResidentHost(s.config); hosts.push(host); await host.start();
  // An actual record change exercises fenced publication, not the independent
  // unchanged actor liveness lane introduced by #4383.
  const owned = host.actors.listOwned.bind(host.actors);
  vi.spyOn(host.actors, "listOwned").mockImplementation(() => owned().map((actor, index) => index === 0 ? { ...actor, status: "running" } : actor));
  // Hold off the automatic retry only; each explicit refresh still uses production custody.
  let releaseRetry!: () => void;
  const retryGate = new Promise<void>(resolve => { releaseRetry = resolve; });
  releases.push(releaseRetry);
  vi.spyOn(host.participants.options, "waitForPublicationRetry").mockImplementation(() => retryGate);
  const keyLock = path.join(s.config.meshRoot, "participants", ".locks", createHash("sha256").update(s.rows[0]!.id).digest("hex"));
  fs.mkdirSync(keyLock, { recursive: true });
  const receipt = `${process.pid}\n${incarnation}\nheld-native-key\n`;
  fs.writeFileSync(path.join(keyLock, "owner"), receipt);
  const inode = fs.statSync(keyLock).ino;
  let nativeUnderCustody = false;
  let began!: () => void;
  const preparing = new Promise<void>(resolve => { began = resolve; });
  // The native read stays pending until the test has taken both registry fences,
  // so "prepares outside the fences" is an ordering fact, not a wall-clock race.
  // afterEach releases it too, so no outcome can leave the hook joining it forever.
  let finishRead!: () => void, nativeFinished = false;
  const nativeRead = new Promise<void>(resolve => { finishRead = resolve; }).then(() => { nativeFinished = true; });
  releases.push(finishRead);
  const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(() => {
    nativeUnderCustody ||= s.custody(); began();
    const work = nativeRead.then(() => undefined); reads.push(work); return work;
  });
  try {
    const begin = performance.now();
    await expect(host.participants.refreshPresence()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    // The fenced caller fails fast: no native read (asserted directly) and no
    // 5 s LOCK_WAIT_MS key wait. The bound is a hang guard, not a latency budget.
    expect(read).not.toHaveBeenCalled();
    expect(performance.now() - begin).toBeLessThan(2_500);
    const retry = host.participants.refresh().catch(error => error);
    await within(preparing, 3_000, "the unfenced retry never asked native evidence for the held key");
    const setterBegin = performance.now();
    await within(Promise.all(s.actorRoots.map(root => new ActorRegistryStore(root).withLock(() => undefined))), 3_000,
      "a registry fence is held across the native key-holder read");
    const registryBlockedMs = performance.now() - setterBegin;
    // Both registry fences were acquired while native preparation was still in flight.
    expect(nativeFinished).toBe(false);
    finishRead();
    expect(await retry).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(nativeUnderCustody).toBe(false); expect(read).toHaveBeenCalledOnce();
    expect(fs.readFileSync(path.join(keyLock, "owner"), "utf8")).toBe(receipt);
    expect(fs.statSync(keyLock).ino).toBe(inode); // unknown evidence never reaps a live holder
    fs.rmSync(keyLock, { recursive: true });
    await host.participants.refresh();
    expect(host.participants.canConsumeMesh()).toBe(true);
    console.log(JSON.stringify({ entrypoint: "busy participant-key retry", protocol, platform, registryBlockedMs, nativeUnderCustody, unknownStayedLive: true }));
  } finally { finishRead(); fs.rmSync(keyLock, { recursive: true, force: true }); releaseRetry(); }
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

it("#4383 an adopter cannot cross a renewal's token-check/rename window and never waits on its key under registry custody", async () => {
  const s = await fixture(1, false);
  const mesh = new MeshStore(s.config.meshRoot, 65536, 100);
  const registry = new ActorRegistryStore(s.actorRoots[0]!);
  const snapshot = registry.snapshot();
  const key = `topology/participants/${createHash("sha256").update(s.rows[0]!.id).digest("hex")}`;
  const keyLock = path.join(s.config.meshRoot, "participants", ".locks", key.split("/").at(-1)!);
  const own = vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined);
  const native = vi.spyOn(atomic, "processIncarnation").mockResolvedValue(undefined);
  const prepared = registry.prepare(snapshot.actors.map(row => ({ ...row, rootId: "session:successor", adoptedAt: Date.now() })), { durable: true }, snapshot);
  let claimed = false;
  try {
    // This is exactly the renewal decision boundary: the generation has been
    // checked, but its participant envelope has not yet been renamed.
    await participantFiles.writeParticipantFileIf(mesh, key, () => {
      const receipt = fs.readFileSync(path.join(keyLock, "owner"), "utf8");
      expect(() => participantFiles.withParticipantFileTryLock(mesh, key, undefined, () => {
        claimed = true; prepared.commit();
      })).toThrow(participantFiles.ParticipantFileLockBusyError);
      expect(claimed).toBe(false);
      expect(fs.readFileSync(path.join(keyLock, "owner"), "utf8")).toBe(receipt);
      expect(registry.records()[0]!.rootId).toBe(s.config.rootId);
      return { key, value: { id: s.rows[0]!.id }, version: 1, updatedAt: Date.now(),
        updatedBy: { id: s.config.rootId, name: "owner", kind: "agent" } };
    }, { registryFenced: true, ownIncarnation: undefined });
    // The real ordering: registry -> mesh -> nonblocking key, synchronous claim.
    await registry.withLock(() => mesh.exclusive(() => participantFiles.withParticipantFileTryLock(mesh, key, undefined, () => {
      expect(prepared.valid()).toBe(true);
      prepared.commit(); claimed = true;
    }), 0));
    expect(claimed).toBe(true);
    expect(registry.records()[0]!.rootId).toBe("session:successor");
    expect(fs.existsSync(keyLock)).toBe(false);
    expect(own).not.toHaveBeenCalled();
    expect(native).not.toHaveBeenCalled();
  } finally { prepared.dispose(); }
});
