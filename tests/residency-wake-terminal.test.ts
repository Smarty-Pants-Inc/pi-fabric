import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries, residentWakeIndexPath,
  type WakeDelivery } from "../src/residency/wake-index.js";
import * as wake from "../src/residency/wake.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { mainPublicationFenced, mainPublicationFencePath } from "../src/topology/main-publication-fence.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep: a recorded internal wake-loop incident, durable-delivery safety, and a
// contention/failure path that a normal user flow cannot reach cheaply. No live
// fleet state or native child is needed: only the actual launch boundary is injected.
const from = { id: "publisher", name: "publisher", kind: "main" as const };
const cleanups: Array<() => void> = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const close of cleanups.splice(0).reverse()) close();
  vi.unstubAllEnvs();
});

interface FixtureRuntime {
  MeshStore?: typeof MeshStore;
  stateBackend?: "file" | "sqlite";
  rootId?: string;
  ensureResidentWakeArchive?: typeof wake.ensureResidentWakeArchive;
  retryResidentWakes?: typeof wake.retryResidentWakes;
}
async function fixture(runtime: FixtureRuntime = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-terminal-"));
  let allowed = false, now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const meshRoot = path.join(root, "mesh");
  const Store = runtime.MeshStore ?? MeshStore;
  const openMesh = () => new Store(meshRoot, 65_536, 100, {
    ...(runtime.stateBackend ? { stateBackend: runtime.stateBackend } : {}), canWakeResidents: () => allowed });
  let mesh = openMesh();
  const rootId = runtime.rootId ?? "session:wake-terminal";
  const sessionId = rootId.slice("session:".length);
  const config: ResidentHostConfig = { format: 1, rootId, sessionId, cwd: root,
    projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
  const directory = config.residencyRoot;
  const configFile = path.join(directory, "config.json"), routesFile = path.join(directory, "wake-routes.json");
  const failureFile = path.join(directory, "wake-failure.json"), terminalFile = path.join(directory, "wake-terminal.json");
  const participantKey = "topology/participants/" + createHash("sha256").update(rootId).digest("hex");
  const participantFile = path.join(meshRoot, "participants", participantKey.slice("topology/participants/".length) + ".json");
  const main = { id: rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(configFile, JSON.stringify(config));
  fs.writeFileSync(routesFile, JSON.stringify({ format: 1, rootId, hostId: "host:resident",
    configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "actor:listener", name: "listener", topics: ["terminal.delivery"] }] }));
  await (runtime.ensureResidentWakeArchive ?? wake.ensureResidentWakeArchive)(mesh);
  cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  const read = <T>(file: string) => wake.readWakeJson<T>(file);
  const pending = () => residentUnacknowledgedDeliveries(meshRoot).get(directory);
  const publishPending = async (topic = "terminal.delivery") => {
    const before = allowed;
    allowed = false; // Real append/archive/index, but no publication-time launch.
    try { return await mesh.publish({ topic, from }); }
    finally { allowed = before; }
  };
  const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {});
  if (!runtime.retryResidentWakes) {
    const retry = wake.retryResidentWakes;
    vi.spyOn(wake, "retryResidentWakes").mockImplementation((store, _launch, at, canWake) =>
      retry(store, launch, at, canWake));
  }
  return { root, meshRoot, config, directory, configFile, routesFile, failureFile, terminalFile,
    participantKey, participantFile, main, launch, read, pending, publishPending,
    get mesh() { return mesh; }, get now() { return now; },
    advance() { now += 5 * 60_000; }, // Beyond both maximum backoff and attempt-claim lease.
    advancePastTerminalGrace() { now += 300_000; }, // Well past the fixed positive TTL and terminal grace.
    drift() { fs.writeFileSync(configFile, JSON.stringify({ ...config, cwd: path.join(root, "changed") })); },
    repair() { fs.writeFileSync(configFile, JSON.stringify(config)); },
    async sweep() {
      allowed = true;
      if (runtime.retryResidentWakes) await runtime.retryResidentWakes(mesh, launch, now, () => allowed);
      else await mesh.retryResidentWakes();
    },
    reopen() { mesh.closeState(); mesh = openMesh(); },
    rootLease(live: boolean, expiredForMs = 10 * 60_000, ttlMs = 15_000) {
      const expiresAt = live ? now + 86_400_000 : now - expiredForMs;
      const updatedAt = live ? now - 1_000 : expiresAt - ttlMs;
      const startedAt = live ? updatedAt : updatedAt - ttlMs;
      writeHostLease(meshRoot, { id: rootId, rootId, identityId: rootId, updatedAt, expiresAt,
        startedAt, session: { id: config.sessionId, startedAt, updatedAt, expiresAt } });
    },
    async rootPresence(storage: "file" | "state", reload = false) {
      const participant: FabricParticipantRecord = { format: 1, id: rootId, rootId, ownerHostId: rootId,
        ownerIdentityId: rootId, kind: "root", name: "Main", status: reload ? "reloading" : "idle",
        ...(reload ? { reloadUntil: now + 86_400_000 } : {}), runner: "pi", transport: "host",
        capabilities: ["fabric"], controlProtocol: "v1", sessionId: config.sessionId, cwd: root,
        startedAt: now - 1_000, updatedAt: now };
      // A real legacy-compatible host state envelope makes presence independently
      // discoverable by ParticipantDirectory.get, with NO root lease file.
      await mesh.put({ key: "topology/hosts/" + createHash("sha256").update(rootId).digest("hex"), identity: main,
        value: { format: 1, id: rootId, rootId, identity: main, startedAt: now - 1_000,
          updatedAt: now, expiresAt: now + 86_400_000 } });
      if (storage === "state") await mesh.put({ key: participantKey, identity: main, value: participant });
      else writeParticipantFile(meshRoot, { key: participantKey, value: participant, version: 1, updatedAt: now, updatedBy: main });
    },
    rootParticipant() {
      const identity = { id: "session:observer", name: "observer", kind: "main" as const };
      return new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity })
        .get(rootId, now, { fresh: true });
    },
  };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

const lockStatsKey = Symbol.for("pi-fabric.mesh.lock-stats");
type LockStatsRecorder = { stats?: unknown; flush(): void; dispose(): void };
type LockStatsRuntime = FixtureRuntime & { lockStatsHost(): string; close(): void };
async function lockStatsRuntime(): Promise<LockStatsRuntime> {
  const globals = globalThis as typeof globalThis & { [lockStatsKey]?: LockStatsRecorder };
  globals[lockStatsKey]?.dispose();
  delete globals[lockStatsKey];
  vi.stubEnv("PI_FABRIC_LOCK_STATS", "1");
  vi.resetModules();
  const [store, wakeApi, stats] = await Promise.all([
    import("../src/mesh/store.js"), import("../src/residency/wake.js"), import("../src/mesh/commit-stats.js"),
  ]);
  if (!globals[lockStatsKey]?.stats) throw new Error("Native mesh lock-stats recorder is not active");
  return {
    MeshStore: store.MeshStore,
    ensureResidentWakeArchive: wakeApi.ensureResidentWakeArchive,
    retryResidentWakes: wakeApi.retryResidentWakes,
    lockStatsHost: stats.lockStatsHost,
    close() {
      globals[lockStatsKey]?.dispose();
      delete globals[lockStatsKey];
      vi.resetModules();
      vi.unstubAllEnvs();
    },
  };
}
function nativeCustodyCount(root: string, host: string): number {
  const globals = globalThis as typeof globalThis & { [lockStatsKey]?: LockStatsRecorder };
  globals[lockStatsKey]?.flush();
  const file = path.join(root, "lock-stats", `${host}-${process.pid}.json`);
  if (!fs.existsSync(file)) return 0;
  const stats = JSON.parse(fs.readFileSync(file, "utf8")) as { minutes?: Array<{ classes?: { custody?: { n?: number } } }> };
  return stats.minutes?.reduce((total, minute) => total + (minute.classes?.custody?.n ?? 0), 0) ?? 0;
}

/** Real Main activation, lease CAS and participant publication, not a raw
 * writeHostLease substitute for the native writer protocol under test. */
async function nativeMain(f: Fixture) {
  const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.config.rootId,
    rootId: f.config.rootId, identity: f.main, reapDeadHosts: false });
  let name = "Main";
  directory.registerSource(() => [{ format: 1, id: f.config.rootId, rootId: f.config.rootId,
    ownerHostId: f.config.rootId, ownerIdentityId: f.config.rootId, ownerIncarnation: directory.ownerIncarnation,
    kind: "root", name, status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"],
    controlProtocol: "v1", sessionId: f.config.sessionId, cwd: f.root, startedAt: f.now - 1_000, updatedAt: f.now }]);
  try { await directory.start(); }
  catch (error) { await directory.close(); throw error; }
  directory.suspend(); // No background heartbeat competes with the deterministic race.
  return { directory, change() { name = "Main renewed"; } };
}
interface TerminalRecord {
  format: 1; rootId: string; delivery: WakeDelivery; code: "RESIDENT_WAKE_CONFIG_MISMATCH";
  error: string; failedAt: number;
}
interface FailureRecord { delivery: WakeDelivery; error?: string; failures?: number; nextRetryAt?: number; claimId?: string }
const differentOrUnknownTerminalDelivery = new Set([
  "high-sequence", "mismatched-id", "id-type", "sequence-type", "missing-id", "missing-sequence",
  "fractional-sequence", "unsafe-sequence", "zero-sequence", "delivery-null", "delivery-array",
  "missing-delivery", "bad-json",
]);
async function interruptedTerminal(f: Fixture, relation: "same" | "higher" | "lower", backedOff = false) {
  const first = await f.publishPending();
  const second = relation === "same" ? first : await f.publishPending();
  const event = relation === "lower" ? second : first;
  const receiptEvent = relation === "higher" ? second : first;
  if (relation === "higher") fs.writeFileSync(residentWakeIndexPath(f.meshRoot), JSON.stringify({ format: 1,
    op: "pending", root: f.directory, delivery: { id: event.id, sequence: event.sequence } }) + "\n");
  f.rootLease(false); // Interrupted terminal recovery must carry valid past-grace lease evidence.
  const terminal: TerminalRecord = { format: 1, rootId: f.config.rootId,
    delivery: { id: receiptEvent.id, sequence: receiptEvent.sequence }, code: "RESIDENT_WAKE_CONFIG_MISMATCH",
    error: "ResidentWakeConfigMismatch: interrupted receipt", failedAt: f.now - 1_000 };
  writeJsonAtomic(f.terminalFile, terminal, { durable: true });
  writeJsonAtomic(f.failureFile, { format: 1, delivery: { id: event.id, sequence: event.sequence },
    error: "earlier failure", failures: 2, nextRetryAt: f.now + (backedOff ? 1_000 : -1) }, { durable: true });
  return { event, terminal, terminalBytes: fs.readFileSync(f.terminalFile, "utf8"),
    failureBytes: fs.readFileSync(f.failureFile, "utf8") };
}
interface ReceiptWrite {
  file: string; value: Record<string, unknown>; held: boolean; fileSynced: boolean;
}

/** Observe actual filesystem calls while the REAL MeshStore exclusive callback
 * runs. Claim/ACK bookkeeping is allowed; config/routes/liveness and error receipt
 * I/O are not. All spies delegate to the native operation. */
function observe(f: Fixture, unreadableFile?: string) {
  let depth = 0, exclusiveCalls = 0, namespaceSyncs = 0;
  let beforeExclusive: (() => void | Promise<void>) | undefined;
  const reads: Array<{ file: string; kind: "metadata" | "liveness"; held: boolean }> = [];
  const writes: ReceiptWrite[] = [];
  const acks: Array<{ delivery: WakeDelivery; terminalRecorded: boolean }> = [];
  const warningHeld: boolean[] = [];
  const descriptors = new Map<number, string>(), synced = new Set<string>();
  const exclusive = f.mesh.exclusive.bind(f.mesh);
  vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
    exclusiveCalls++;
    await beforeExclusive?.();
    return exclusive(() => { depth++; try { return operation(); } finally { depth--; } }, timeout);
  });
  const note = (file: fs.PathLike | number) => {
    const name = typeof file === "number" ? descriptors.get(file) : String(file);
    if (!name) return;
    if (name === f.configFile || name === f.routesFile) reads.push({ file: name, kind: "metadata", held: depth > 0 });
    if (name === hostLeasePath(f.meshRoot, f.config.rootId) || name === f.participantFile ||
        name === path.join(f.meshRoot, "participants")) reads.push({ file: name, kind: "liveness", held: depth > 0 });
  };
  const open = fs.openSync, read = fs.readFileSync, lstat = fs.lstatSync, stat = fs.statSync;
  const rename = fs.renameSync, sync = fs.fsyncSync, write = fs.writeFileSync;
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    note(file);
    const fd = Reflect.apply(open, fs, [file, ...args]) as number;
    descriptors.set(fd, String(file));
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    note(file);
    if (String(file) === unreadableFile) throw Object.assign(new Error("lease read refused"), { code: "EACCES" });
    return Reflect.apply(read, fs, [file, ...args]);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, contents: string | NodeJS.ArrayBufferView, ...args: unknown[]) => {
    if (typeof file === "number" && descriptors.get(file) === residentWakeIndexPath(f.meshRoot)) {
      for (const line of String(contents).split("\n").filter(Boolean)) {
        const record = JSON.parse(line) as { op?: string; delivery: WakeDelivery };
        if (record.op === "ack") acks.push({ delivery: record.delivery,
          terminalRecorded: writes.some(row => row.file === f.terminalFile && row.fileSynced) });
      }
    }
    return Reflect.apply(write, fs, [file, contents, ...args]);
  }) as typeof fs.writeFileSync);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    note(file); return Reflect.apply(lstat, fs, [file, ...args]);
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    note(file); return Reflect.apply(stat, fs, [file, ...args]);
  }) as typeof fs.statSync);
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    sync(fd);
    const name = descriptors.get(fd);
    if (name) synced.add(name);
    if (name === f.directory) namespaceSyncs++;
  });
  vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
    const file = String(target);
    let value: Record<string, unknown> | undefined;
    if (file === f.failureFile || file === f.terminalFile) value = JSON.parse(read(source, "utf8"));
    rename(source, target);
    if (value) writes.push({ file, value, held: depth > 0, fileSynced: synced.has(String(source)) });
  });
  const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => { warningHeld.push(depth > 0); });
  return { reads, writes, acks, warnings, warningHeld, get held() { return depth > 0; },
    beforeExclusive(operation: () => void | Promise<void>) { beforeExclusive = operation; },
    get namespaceSyncs() { return namespaceSyncs; },
    counts() {
      return { exclusiveCalls, configReads: reads.filter(row => row.file === f.configFile).length,
        claims: writes.filter(row => row.file === f.failureFile && typeof row.value.claimId === "string").length,
        errors: writes.filter(row => row.file === f.failureFile && typeof row.value.error === "string" && !row.value.claimId).length,
        terminals: writes.filter(row => row.file === f.terminalFile).length, warnings: warnings.mock.calls.length };
    },
  };
}
function expectOutsideLock(probe: ReturnType<typeof observe>) {
  expect.soft(probe.reads.filter(row => row.held), "routes/config and exceptional root liveness I/O must not hold mesh.exclusive").toEqual([]);
  expect.soft(probe.writes.filter(row => row.held && (row.file.endsWith("wake-terminal.json") ||
    (typeof row.value.error === "string" && !row.value.claimId))), "failure/terminal receipt recording must not hold mesh.exclusive").toEqual([]);
  expect.soft(probe.warningHeld, "warnings must not run under mesh.exclusive").not.toContain(true);
}
async function terminalizeDeadDelivery(f: Fixture) {
  const event = await f.publishPending();
  f.drift();
  f.rootLease(false);
  await f.sweep();
  const terminalBytes = fs.readFileSync(f.terminalFile, "utf8");
  expect(f.pending()).toBeUndefined();
  expect(f.read<TerminalRecord>(f.terminalFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence } });
  expect(fs.existsSync(f.failureFile)).toBe(false);
  return { event, terminalBytes };
}
function changeRetainedTopic(f: Fixture, topic: string): void {
  const routes = JSON.parse(fs.readFileSync(f.routesFile, "utf8")) as { actors: Array<{ topics: string[] }> };
  routes.actors[0]!.topics = [topic];
  fs.writeFileSync(f.routesFile, JSON.stringify(routes));
}

describe("terminal resident wake disposition", () => {
  it("records a dead ROOT config mismatch once, ACKs it, and never reacquires retry claims on eligible sweeps", async () => {
    const f = await fixture();
    const event = await f.publishPending();
    expect(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
    f.drift();
    f.rootLease(false);
    // A live resident process is deliberately NOT a live originating ROOT Main.
    const owner = JSON.stringify({ pid: process.pid, token: "resident-only" });
    fs.writeFileSync(path.join(f.directory, "owner.json"), owner);
    expect(f.rootParticipant()).toBeUndefined();
    const probe = observe(f);
    await Promise.all([f.sweep(), f.sweep()]); // Same-mesh concurrent callers share one attempt.
    const first = probe.counts();
    const terminalAfterFirst = f.read<TerminalRecord>(f.terminalFile);
    const bytesAfterFirst = fs.existsSync(f.terminalFile) ? fs.readFileSync(f.terminalFile, "utf8") : undefined;
    for (let n = 0; n < 8; n++) { f.advance(); await f.sweep(); }
    const last = probe.counts();
    const receipt = f.read<TerminalRecord>(f.terminalFile);
    const loops = { eligibleSweeps: 9, first, last, claimsAfterFirst: last.claims - first.claims,
      attemptsAfterFirst: last.configReads - first.configReads, errorsAfterFirst: last.errors - first.errors,
      globalLockReentriesAfterFirst: last.exclusiveCalls - first.exclusiveCalls };
    // Print BEFORE assertions: baseline RED must expose the repeated attempts,
    // error rewrites and global-lock reentries, not just a missing receipt.
    console.info(JSON.stringify({ probe: "dead-root-wake-loop", ...loops }));
    expect.soft(loops).toMatchObject({ claimsAfterFirst: 0, attemptsAfterFirst: 0, errorsAfterFirst: 0,
      last: { terminals: 1, warnings: 1 } });
    expect.soft(terminalAfterFirst).toEqual({ format: 1, rootId: f.config.rootId,
      delivery: { id: event.id, sequence: event.sequence }, code: "RESIDENT_WAKE_CONFIG_MISMATCH",
      error: expect.stringContaining("ResidentWakeConfigMismatch"), failedAt: f.now - 8 * 5 * 60_000 });
    expect.soft(receipt).toEqual(terminalAfterFirst);
    expect.soft(fs.existsSync(f.terminalFile) ? fs.readFileSync(f.terminalFile, "utf8") : undefined).toBe(bytesAfterFirst);
    expect.soft(f.pending()).toBeUndefined();
    expect.soft(fs.existsSync(f.failureFile)).toBe(false);
    expect.soft(fs.readFileSync(residentWakeIndexPath(f.meshRoot), "utf8")).toContain('"op":"ack"');
    expect.soft(probe.acks).toEqual([{ delivery: { id: event.id, sequence: event.sequence }, terminalRecorded: true }]);
    expect.soft(probe.writes.filter(row => row.file === f.terminalFile).map(row => row.fileSynced)).toEqual([true]);
    if (process.platform !== "win32") expect.soft(probe.namespaceSyncs).toBeGreaterThan(0);
    expect.soft(f.launch).not.toHaveBeenCalled();
    expect.soft(fs.readFileSync(path.join(f.directory, "owner.json"), "utf8")).toBe(owner);
    expectOutsideLock(probe);
  });

  it.each(["lease-only", "presence-file-only", "presence-state-only", "reload-presence"] as const)(
    "keeps a live ROOT retryable with %s while mismatch inspection and error recording stay outside the global lock", async mode => {
      const f = await fixture();
      const event = await f.publishPending();
      if (mode === "lease-only") {
        f.rootLease(true);
        expect(f.rootParticipant()).toBeUndefined(); // Lease alone, no published presence.
      } else {
        await f.rootPresence(mode === "presence-state-only" ? "state" : "file", mode === "reload-presence");
        expect(fs.existsSync(hostLeasePath(f.meshRoot, f.config.rootId))).toBe(false);
        expect(f.rootParticipant()).toMatchObject({ id: f.config.rootId, kind: "root", stale: false });
      }
      expect(fs.existsSync(path.join(f.directory, "owner.json"))).toBe(false);
      f.drift();
      const leaseCustody = vi.spyOn(f.mesh, "leaseCustody");
      const probe = observe(f);
      for (let n = 0; n < 4; n++) { await f.sweep(); f.advance(); }
      const counts = probe.counts();
      expect.soft(leaseCustody, "a known-live root must not take terminal lease custody").not.toHaveBeenCalled();
      expect.soft(fs.existsSync(mainPublicationFencePath(f.meshRoot, f.config.rootId))).toBe(false);
      // The real bounded candidate cursor can have an empty EOF sweep. Count
      // actual eligible claims, not an assumed one attempt per invocation.
      expect.soft(counts.claims).toBeGreaterThanOrEqual(2);
      expect.soft(f.read<FailureRecord>(f.failureFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence },
        error: expect.stringContaining("ResidentWakeConfigMismatch"), failures: counts.claims });
      expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
      expect.soft(f.launch).not.toHaveBeenCalled();
      expect.soft(counts).toMatchObject({ errors: counts.claims, terminals: 0, warnings: counts.claims });
      expect.soft(probe.reads.some(row => row.kind === "liveness")).toBe(true);
      expectOutsideLock(probe);
    });

  it("fences native Main renewal AND participant publication through the terminal ACK boundary (WAK-1)", async () => {
    const f = await fixture();
    const event = await f.publishPending();
    const main = await nativeMain(f);
    try {
      f.advancePastTerminalGrace(); // Expire the actual native lease and presence well past grace, with UUID/owner intact.
      expect(f.rootParticipant()).toBeUndefined();
      const expired = readHostLeaseCurrent(f.meshRoot, f.config.rootId)!;
      expect(expired.expiresAt).toBeLessThan(f.now);
      f.drift();
      const probe = observe(f);
      let raced = false;
      probe.beforeExclusive(async () => {
        // The durable terminal write is complete; the global ACK has NOT begun.
        if (!raced && fs.existsSync(f.terminalFile)) {
          raced = true;
          expect.soft(probe.held).toBe(false);
          expect.soft(mainPublicationFenced(f.meshRoot, f.config.rootId)).toBe(true);
          await expect.soft(renewHostLease(f.mesh, { ...expired, updatedAt: f.now, expiresAt: f.now + 60_000 }))
            .rejects.toMatchObject({ busyCode: "FABRIC_HOST_LEASE_LOCK_BUSY" });
          main.change();
          await expect.soft(main.directory.refreshPresence()).rejects.toBeDefined();
          expect.soft(readHostLeaseCurrent(f.meshRoot, f.config.rootId)).toEqual(expired);
          expect.soft(f.rootParticipant()).toBeUndefined();
        }
      });
      await f.sweep();
      expect.soft(raced, "must exercise the real terminal-record-to-ACK window").toBe(true);
      expect.soft(f.pending()).toBeUndefined();
      expect.soft(f.read<TerminalRecord>(f.terminalFile)?.delivery).toEqual({ id: event.id, sequence: event.sequence });
      expect.soft(f.launch).not.toHaveBeenCalled();
      expectOutsideLock(probe);
      // Neither fence persists after the terminal operation. A real Main can now
      // renew/publish; its refused attempt above never crossed the ACK boundary.
      expect.soft(mainPublicationFenced(f.meshRoot, f.config.rootId)).toBe(false);
      await main.directory.refresh();
      expect.soft(f.rootParticipant()).toMatchObject({ id: f.config.rootId, stale: false });
    } finally { await main.directory.close(); }
  });

  it("retains pending when native Main renewal wins the lease gate before terminal custody (WAK-1 counterexample)", async () => {
    const f = await fixture();
    const event = await f.publishPending();
    const main = await nativeMain(f);
    try {
      f.advancePastTerminalGrace();
      expect(f.rootParticipant()).toBeUndefined();
      const expired = readHostLeaseCurrent(f.meshRoot, f.config.rootId)!;
      f.drift();
      const probe = observe(f);
      const leaseCustody = f.mesh.leaseCustody.bind(f.mesh);
      let raced = false;
      vi.spyOn(f.mesh, "leaseCustody").mockImplementation(async <T>(file: string, operation: () => T | Promise<T>, timeout?: number,
        options?: { ownIncarnation?: string | undefined }): Promise<T> => {
        if (!raced && file === hostLeasePath(f.meshRoot, f.config.rootId)) {
          raced = true;
          // Native renewal enters the SAME gate first. The terminal caller has
          // observed absence, but has not yet acquired lease custody.
          await renewHostLease(f.mesh, { ...expired, updatedAt: f.now, expiresAt: f.now + 60_000 });
          // Before terminal lease custody, the independent publication fence
          // must still refuse the real directory's root participant write.
          main.change();
          await expect(main.directory.refreshPresence()).rejects.toMatchObject({ code: "FABRIC_MAIN_PUBLICATION_FENCED" });
        }
        return leaseCustody(file, operation, timeout, options);
      });
      await f.sweep();
      expect.soft(raced).toBe(true);
      expect.soft(readHostLeaseCurrent(f.meshRoot, f.config.rootId)?.expiresAt).toBeGreaterThan(f.now);
      expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
      expect.soft(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
      expect.soft(f.read<FailureRecord>(f.failureFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence }, failures: 1 });
      expect.soft(probe.acks).toEqual([]);
      expectOutsideLock(probe);
      // Once live, the prefilter must keep retry/backoff without taking either fence.
      for (let n = 0; n < 4; n++) {
        f.advance(); await main.directory.refresh();
        const entries = vi.mocked(f.mesh.leaseCustody).mock.calls.length;
        await f.sweep();
        expect.soft(vi.mocked(f.mesh.leaseCustody).mock.calls.length, "live native root is only prefiltered").toBe(entries);
      }
      expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
      expect.soft(f.read<FailureRecord>(f.failureFile)?.failures).toBeGreaterThan(1);
    } finally { await main.directory.close(); }
  });

  it("keeps a busy native root lease gate retryable without a terminal ACK (WAK-1)", async () => {
    const f = await fixture();
    const event = await f.publishPending();
    f.drift();
    f.rootLease(false); // Reach the native gate; missing lease evidence would refuse before custody.
    const probe = observe(f);
    await f.mesh.leaseCustody(hostLeasePath(f.meshRoot, f.config.rootId), () => f.sweep());
    expect.soft(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
    expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
    expect.soft(f.read<FailureRecord>(f.failureFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence }, failures: 1 });
    expect.soft(probe.acks).toEqual([]);
    expectOutsideLock(probe);
  });

  it("runs a failing launch and its real retry-error recording outside the global lock", async () => {
    const f = await fixture();
    const event = await f.publishPending();
    const probe = observe(f);
    const launchHeld: boolean[] = [], formattingHeld: boolean[] = [];
    f.launch.mockImplementation(async () => {
      launchHeld.push(probe.held);
      const error = new Error("injected launch failure");
      error.toString = () => { formattingHeld.push(probe.held); return "Error: injected launch failure"; };
      throw error;
    });
    await f.sweep();
    f.advance();
    await f.sweep();
    expect.soft(launchHeld).toEqual([false, false]); // Already true on baseline; bundled with RED receipt I/O assertions.
    expect.soft(formattingHeld.length).toBeGreaterThan(0);
    expect.soft(formattingHeld).not.toContain(true);
    expect.soft(f.read<FailureRecord>(f.failureFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence }, failures: 2,
      error: "Error: injected launch failure" });
    expect.soft(fs.existsSync(f.terminalFile)).toBe(false); // Ordinary failures are never terminalized.
    expect.soft(probe.counts()).toMatchObject({ claims: 2, errors: 2, terminals: 0 });
    expectOutsideLock(probe);
  });

  describe.each(["maintenance", "publication"] as const)("independent event after terminal evidence in %s", consumer => {
    it("repairs config, renews a native Main, changes the retained route, and serves event 2", async () => {
      const f = await fixture();
      const { terminalBytes } = await terminalizeDeadDelivery(f);
      f.repair();
      const main = await nativeMain(f);
      try {
        f.advance();
        await main.directory.refresh(); // Native renewal and participant publication make the originating root live again.
        expect(readHostLeaseCurrent(f.meshRoot, f.config.rootId)?.expiresAt).toBeGreaterThan(f.now);
        expect(f.rootParticipant()).toMatchObject({ id: f.config.rootId, kind: "root", stale: false });
        changeRetainedTopic(f, "terminal.delivery.v2");
        const event2 = await f.publishPending("terminal.delivery.v2");
        expect(f.pending()).toEqual({ id: event2.id, sequence: event2.sequence });
        const probe = observe(f);
        if (consumer === "publication") await wake.wakeResidentActors(f.mesh, [event2], f.launch);
        for (let n = 0; n < 4; n++) { f.advance(); await f.sweep(); }
        expect(f.launch).toHaveBeenCalledOnce();
        expect(f.launch).toHaveBeenCalledWith(f.configFile, f.config);
        expect(f.read(wake.residentWakeRequestPath(f.directory))).toMatchObject({ id: event2.id, sequence: event2.sequence });
        expect(f.pending()).toBeUndefined();
        expect(fs.existsSync(f.failureFile)).toBe(false);
        expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
        expect(probe.acks).toMatchObject([{ delivery: { id: event2.id, sequence: event2.sequence }, terminalRecorded: false }]);
        expect(probe.writes.filter(row => row.file === f.terminalFile)).toEqual([]);
        expectOutsideLock(probe);
      } finally { await main.directory.close(); }
    });

    it.each(["malformed", "unverifiable", "archive-unavailable", "valid-historical-different"] as const)(
      "does not borrow or quarantine an independently pending delivery when history is %s", async mode => {
        const f = await fixture();
        const { event: event1 } = await terminalizeDeadDelivery(f);
        f.repair();
        const main = await nativeMain(f);
        try {
          f.advance();
          await main.directory.refresh();
          if (mode === "malformed") fs.writeFileSync(f.terminalFile, "not-json");
          else if (mode === "unverifiable") {
            const terminal = f.read<TerminalRecord>(f.terminalFile)!;
            fs.writeFileSync(f.terminalFile, JSON.stringify({ ...terminal,
              delivery: { ...terminal.delivery, id: "not-the-archived-event" } }));
          } else if (mode === "archive-unavailable") {
            const archive = MeshArchive.fromRoot(f.meshRoot)!;
            const index = path.join(archive.dir, "sequence-index", String(Math.floor(event1.sequence / 1024)), `${event1.sequence}.json`);
            fs.renameSync(index, `${index}.historical-unavailable`);
          }
          const terminalBytes = fs.readFileSync(f.terminalFile, "utf8");
          const event2 = await f.publishPending();
          const probe = observe(f);
          if (consumer === "publication") await wake.wakeResidentActors(f.mesh, [event2], f.launch);
          for (let n = 0; n < 4; n++) { f.advance(); await f.sweep(); }
          expect(f.launch).toHaveBeenCalledOnce();
          expect(f.read(wake.residentWakeRequestPath(f.directory))).toMatchObject({ id: event2.id, sequence: event2.sequence });
          expect(f.pending()).toBeUndefined();
          expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
          expect(probe.acks).toMatchObject([{ delivery: { id: event2.id, sequence: event2.sequence }, terminalRecorded: false }]);
          expect(probe.writes.filter(row => row.file === f.terminalFile)).toEqual([]);
          expectOutsideLock(probe);
        } finally { await main.directory.close(); }
      });

    it("records ordinary event-2 failure backoff despite unusable historical evidence", async () => {
      const f = await fixture();
      const { terminalBytes } = await terminalizeDeadDelivery(f);
      f.repair();
      const main = await nativeMain(f);
      try {
        f.advance();
        await main.directory.refresh();
        changeRetainedTopic(f, "terminal.delivery.v2");
        const event2 = await f.publishPending("terminal.delivery.v2");
        const probe = observe(f);
        f.launch.mockRejectedValue(new Error("injected event-2 launch failure"));
        if (consumer === "publication") {
          await wake.wakeResidentActors(f.mesh, [event2], f.launch);
          for (let n = 0; n < 4; n++) await f.sweep(); // Respect the newly recorded backoff.
        } else {
          for (let n = 0; n < 4; n++) await f.sweep();
        }
        expect(f.launch).toHaveBeenCalledOnce();
        const failure = f.read<FailureRecord>(f.failureFile)!;
        expect(failure).toMatchObject({ delivery: { id: event2.id, sequence: event2.sequence },
          error: "Error: injected event-2 launch failure", failures: 1 });
        expect(failure.nextRetryAt).toBeGreaterThan(f.now);
        expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
        expect(probe.writes.filter(row => row.file === f.terminalFile)).toEqual([]);
        expect(f.read<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(f.directory))).toEqual({
          format: 1, id: event2.id, sequence: event2.sequence, requestedAt: f.now,
        }); // The durable intent, not a borrowed terminal ACK, preserves the failed delivery.
        if (consumer === "maintenance") {
          expect(f.pending()).toBeUndefined(); // Maintenance ACKs only after that exact intent is durable.
          expect(failure.claimId).toBeUndefined(); // Failed launch releases the attempt claim into timed backoff.
          expect(probe.acks).toEqual([{ delivery: { id: event2.id, sequence: event2.sequence }, terminalRecorded: false }]);
        } else {
          expect(f.pending()).toEqual({ id: event2.id, sequence: event2.sequence });
          expect(probe.acks).toEqual([]); // Publication failure retains its index watermark.
        }
        expectOutsideLock(probe);
      } finally { await main.directory.close(); }
    });
  });

  it("terminalizes a five-hour expired ROOT lease once on the real SQLite MeshStore backend", async () => {
    const rootId = `session:${randomUUID()}`;
    const f = await fixture({ stateBackend: "sqlite", rootId });
    expect(f.mesh.stateBackend).toBe("sqlite");
    await f.mesh.put({ key: "round2/sqlite-backend-proof", identity: f.main, value: { backend: "sqlite" } });
    expect(fs.existsSync(path.join(f.meshRoot, "state.db"))).toBe(true);
    const event = await f.publishPending();
    f.drift();
    f.rootLease(false, 5 * 60 * 60_000, 15_000); // Default Main TTL, expired well beyond grace with valid epoch fields.
    expect(fs.existsSync(hostLeasePath(f.meshRoot, f.config.rootId))).toBe(true);
    const probe = observe(f);
    await f.sweep();
    const receiptBytes = fs.readFileSync(f.terminalFile, "utf8");
    expect(f.pending()).toBeUndefined();
    expect(f.read<TerminalRecord>(f.terminalFile)).toMatchObject({
      rootId, delivery: { id: event.id, sequence: event.sequence }, code: "RESIDENT_WAKE_CONFIG_MISMATCH" });
    for (let n = 0; n < 8; n++) { f.advance(); await f.sweep(); }
    expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(receiptBytes);
    expect(f.pending()).toBeUndefined();
    expect(f.launch).not.toHaveBeenCalled();
    expect(probe.counts()).toMatchObject({ claims: 1, errors: 0, terminals: 1, warnings: 1 });
    expect(probe.acks).toMatchObject([{ delivery: { id: event.id, sequence: event.sequence }, terminalRecorded: true }]);
    expectOutsideLock(probe);
  });

  it.each(["absent", "malformed", "unknown-format", "unreadable"] as const)(
    "route retirement control: %s metadata drops delivery only for ENOENT", async mode => {
      const f = await fixture();
      const event = await f.publishPending();
      const retainedFailure = { format: 1, delivery: { id: event.id, sequence: event.sequence },
        error: "earlier failure", failures: 2, nextRetryAt: f.now - 1 };
      fs.writeFileSync(f.failureFile, JSON.stringify(retainedFailure));
      if (mode === "absent") fs.renameSync(f.routesFile, f.routesFile + ".retired");
      else if (mode === "malformed") fs.writeFileSync(f.routesFile, "not-json");
      else if (mode === "unknown-format") {
        const routes = JSON.parse(fs.readFileSync(f.routesFile, "utf8"));
        fs.writeFileSync(f.routesFile, JSON.stringify({ ...routes, format: 2 }));
      } else {
        const open = fs.openSync;
        vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
          if (String(file) === f.routesFile) throw Object.assign(new Error("route read refused"), { code: "EACCES" });
          return Reflect.apply(open, fs, [file, ...args]);
        }) as typeof fs.openSync);
      }
      vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
      for (let n = 0; n < 4; n++) { await f.sweep(); f.advance(); }
      expect(f.launch).not.toHaveBeenCalled();
      expect(fs.existsSync(f.terminalFile)).toBe(false);
      expect(f.pending()).toEqual(mode === "absent" ? undefined : { id: event.id, sequence: event.sequence });
      expect(f.read<FailureRecord>(f.failureFile)).toEqual(mode === "absent" ? undefined : retainedFailure);
    });

  it.each(["invalid-lease", "unreadable-lease", "invalid-presence", "mirrored-lease"] as const)(
    "keeps unknown ROOT liveness retryable: %s", async mode => {
      const f = await fixture();
      const event = await f.publishPending();
      f.drift();
      const leaseFile = hostLeasePath(f.meshRoot, f.config.rootId);
      if (mode === "invalid-presence") {
        fs.mkdirSync(path.dirname(f.participantFile), { recursive: true });
        fs.writeFileSync(f.participantFile, "invalid presence");
      } else {
        f.rootLease(false);
        if (mode === "invalid-lease") fs.writeFileSync(leaseFile, "invalid lease");
        if (mode === "mirrored-lease") writeHostLease(f.meshRoot, { ...readHostLeaseCurrent(f.meshRoot, f.config.rootId)!, mirrorLease: 1 });
      }
      const probe = observe(f, mode === "unreadable-lease" ? leaseFile : undefined);
      await f.sweep(); f.advance(); await f.sweep();
      expect.soft(fs.existsSync(f.terminalFile)).toBe(false);
      expect.soft(f.read<FailureRecord>(f.failureFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence }, failures: 2 });
      expect.soft(probe.counts()).toMatchObject({ claims: 2, errors: 2, terminals: 0 });
      expectOutsideLock(probe);
    });

  it("finishes an interrupted terminal ACK after reopening without rewriting, warning, launching, or claiming", async () => {
    const f = await fixture();
    const event = await f.publishPending();
    f.drift();
    f.rootLease(false); // Recovery after restart needs valid past-grace evidence for terminal custody.
    const terminal: TerminalRecord = { format: 1, rootId: f.config.rootId, delivery: { id: event.id, sequence: event.sequence },
      code: "RESIDENT_WAKE_CONFIG_MISMATCH", error: "ResidentWakeConfigMismatch: interrupted wake", failedAt: f.now - 1_000 };
    writeJsonAtomic(f.terminalFile, terminal, { durable: true });
    writeJsonAtomic(f.failureFile, { format: 1, delivery: terminal.delivery, claimId: "crashed-claim", nextRetryAt: f.now - 1 }, { durable: true });
    const bytes = fs.readFileSync(f.terminalFile, "utf8");
    f.reopen(); // Release the real retry/index directory custody, as a process restart would.
    const probe = observe(f);
    for (let n = 0; n < 4; n++) { await f.sweep(); f.advance(); }
    console.info(JSON.stringify({ probe: "terminal-crash-recovery", ...probe.counts() }));
    expect.soft(f.pending()).toBeUndefined();
    expect.soft(fs.existsSync(f.failureFile)).toBe(false);
    expect.soft(fs.readFileSync(f.terminalFile, "utf8")).toBe(bytes);
    expect.soft(probe.counts()).toMatchObject({ claims: 0, errors: 0, terminals: 0, warnings: 0 });
    expect.soft(f.launch).not.toHaveBeenCalled();
    expectOutsideLock(probe);
  });

  it.each(["high-sequence", "mismatched-id", "id-type", "sequence-type", "missing-id", "missing-sequence",
    "fractional-sequence", "unsafe-sequence", "zero-sequence", "missing-error", "error-type", "missing-failedAt",
    "failedAt-type", "failedAt-negative", "missing-rootId", "rootId-type", "wrong-rootId", "missing-code", "code-type",
    "missing-format", "format-type", "delivery-null", "delivery-array", "missing-delivery", "unknown-field", "bad-json"] as const)(
    "retains pending AND exact prior failure for an untrusted terminal receipt: %s (WAK-2)", async mode => {
      const f = await fixture();
      const event = await f.publishPending();
      const delivery: Record<string, unknown> = { id: event.id, sequence: event.sequence };
      const terminal: Record<string, unknown> = { format: 1, rootId: f.config.rootId, delivery,
        code: "RESIDENT_WAKE_CONFIG_MISMATCH", error: "ResidentWakeConfigMismatch: interrupted", failedAt: f.now - 1 };
      if (mode === "high-sequence") { delivery.id = "not-the-event"; delivery.sequence = event.sequence + 1_000; }
      else if (mode === "mismatched-id") delivery.id = "not-the-event";
      else if (mode === "id-type") delivery.id = 123;
      else if (mode === "sequence-type") delivery.sequence = String(event.sequence);
      else if (mode === "missing-id") delete delivery.id;
      else if (mode === "missing-sequence") delete delivery.sequence;
      else if (mode === "fractional-sequence") delivery.sequence = 1.5;
      else if (mode === "unsafe-sequence") delivery.sequence = Number.MAX_SAFE_INTEGER + 1;
      else if (mode === "zero-sequence") delivery.sequence = 0;
      else if (mode === "error-type") terminal.error = {};
      else if (mode === "failedAt-type") terminal.failedAt = String(f.now);
      else if (mode === "failedAt-negative") terminal.failedAt = -1;
      else if (mode === "rootId-type") terminal.rootId = 123;
      else if (mode === "wrong-rootId") terminal.rootId = "session:other";
      else if (mode === "code-type") terminal.code = 123;
      else if (mode === "format-type") terminal.format = "1";
      else if (mode === "delivery-null") terminal.delivery = null;
      else if (mode === "delivery-array") terminal.delivery = [delivery];
      else if (mode === "unknown-field") terminal.unrecognized = true;
      else if (mode.startsWith("missing-")) delete terminal[mode.slice("missing-".length)];
      const terminalBytes = mode === "bad-json" ? "not-json" : JSON.stringify(terminal);
      const failureBytes = JSON.stringify({ format: 1, delivery: { id: event.id, sequence: event.sequence },
        error: "earlier failure", failedAt: f.now - 10, failures: 2, nextRetryAt: f.now - 1 });
      fs.writeFileSync(f.terminalFile, terminalBytes);
      fs.writeFileSync(f.failureFile, failureBytes);
      const probe = observe(f); // Config remains valid: an erroneous fall-through WOULD launch.
      for (let n = 0; n < 4; n++) { await f.sweep(); f.advance(); }
      expect.soft(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
      if (differentOrUnknownTerminalDelivery.has(mode)) {
        expect.soft(f.launch).toHaveBeenCalledOnce(); // A valid ordinary wake, not a borrowed terminal ACK.
        expect.soft(f.read(wake.residentWakeRequestPath(f.directory))).toMatchObject({ id: event.id, sequence: event.sequence });
        expect.soft(f.pending()).toBeUndefined();
        expect.soft(fs.existsSync(f.failureFile)).toBe(false);
        expect.soft(probe.acks).toMatchObject([{ delivery: { id: event.id, sequence: event.sequence }, terminalRecorded: false }]);
      } else {
        expect.soft(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
        expect.soft(fs.readFileSync(f.failureFile, "utf8")).toBe(failureBytes);
        expect.soft(f.launch).not.toHaveBeenCalled();
        expect.soft(probe.acks).toEqual([]); // Exact but untrusted authority never ACKs the delivery.
        expect.soft(probe.counts()).toMatchObject({ claims: 0, errors: 0, terminals: 0, warnings: 0 });
      }
      expectOutsideLock(probe);
    });

  it.each(["unrelated-committed-event", "route-no-longer-matches", "exact-but-uncommitted"] as const)(
    "requires exact original pending AND committed matching archive authority: %s (WAK-2 counterexample)", async mode => {
      const f = await fixture();
      const event = await f.publishPending();
      let delivery: WakeDelivery = { id: event.id, sequence: event.sequence };
      if (mode === "unrelated-committed-event") {
        const unrelated = await f.mesh.publish({ topic: "unrelated.delivery", from });
        delivery = { id: unrelated.id, sequence: unrelated.sequence };
      } else if (mode === "route-no-longer-matches") {
        const routes = JSON.parse(fs.readFileSync(f.routesFile, "utf8"));
        routes.actors[0].topics = ["different.delivery"];
        fs.writeFileSync(f.routesFile, JSON.stringify(routes));
      } else if (mode === "exact-but-uncommitted") {
        const uncommitted = { ...event, id: "uncommitted-event", sequence: event.sequence + 1, dedupeKey: "uncommitted" };
        const archive = MeshArchive.fromRoot(f.meshRoot)!;
        await f.mesh.exclusive(() => archive.begin({ event: uncommitted, line: JSON.stringify(uncommitted) }));
        expect(archive.lookupEntry(uncommitted.sequence)?.committed).toBe(false);
        delivery = { id: uncommitted.id, sequence: uncommitted.sequence };
        // Seed the exact corrupt hint in the native index: equality alone must
        // not turn an archive append that never committed into recovery authority.
        fs.writeFileSync(residentWakeIndexPath(f.meshRoot), JSON.stringify({ format: 1, op: "pending", root: f.directory, delivery }) + "\n");
      }
      const pending = f.pending();
      const terminalBytes = JSON.stringify({ format: 1, rootId: f.config.rootId, delivery,
        code: "RESIDENT_WAKE_CONFIG_MISMATCH", error: "ResidentWakeConfigMismatch: interrupted", failedAt: f.now - 1 });
      const failureBytes = JSON.stringify({ format: 1, delivery: pending, error: "earlier failure", failures: 2, nextRetryAt: f.now - 1 });
      fs.writeFileSync(f.terminalFile, terminalBytes);
      fs.writeFileSync(f.failureFile, failureBytes);
      const probe = observe(f);
      for (let n = 0; n < 4; n++) { await f.sweep(); f.advance(); }
      expect.soft(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
      if (mode === "unrelated-committed-event") {
        expect.soft(f.launch).toHaveBeenCalledOnce();
        expect.soft(f.read(wake.residentWakeRequestPath(f.directory))).toMatchObject({ id: event.id, sequence: event.sequence });
        expect.soft(f.pending()).toBeUndefined();
        expect.soft(fs.existsSync(f.failureFile)).toBe(false);
        expect.soft(probe.acks).toMatchObject([{ delivery: { id: event.id, sequence: event.sequence }, terminalRecorded: false }]);
      } else {
        expect.soft(f.pending()).toEqual(pending);
        expect.soft(fs.readFileSync(f.failureFile, "utf8")).toBe(failureBytes);
        expect.soft(f.launch).not.toHaveBeenCalled();
        expect.soft(probe.acks).toEqual([]);
        expect.soft(probe.counts()).toMatchObject({ claims: 0, errors: 0, terminals: 0, warnings: 0 });
      }
      expectOutsideLock(probe);
    });

  describe.each(["maintenance", "publication"] as const)("%s receipt consumer (WAK-3)", consumer => {
    it.each(["same", "higher", "lower"] as const)("retries repaired config with a valid %s receipt, without borrowing its ACK", async relation => {
      const f = await fixture();
      const { event, terminalBytes, failureBytes } = await interruptedTerminal(f, relation, true);
      f.drift(); f.repair();
      const probe = observe(f);
      const consume = () => consumer === "maintenance" ? f.sweep() : wake.wakeResidentActors(f.mesh, [event], f.launch);
      if (consumer === "maintenance") {
        await consume(); // A valid receipt does not bypass legitimate retry backoff.
        expect.soft(f.launch).not.toHaveBeenCalled();
        expect.soft(fs.readFileSync(f.failureFile, "utf8")).toBe(failureBytes);
        expect.soft(probe.acks).toEqual([]);
      }
      for (let n = 0; n < (consumer === "maintenance" ? 4 : 1); n++) { f.advance(); await consume(); }
      expect.soft(f.launch).toHaveBeenCalledOnce();
      expect.soft(f.launch).toHaveBeenCalledWith(f.configFile, f.config);
      expect.soft(f.read(wake.residentWakeRequestPath(f.directory))).toMatchObject({ id: event.id, sequence: event.sequence });
      expect.soft(f.pending()).toBeUndefined();
      expect.soft(fs.existsSync(f.failureFile)).toBe(false);
      expect.soft(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
      expect.soft(probe.acks.length).toBeGreaterThan(0);
      expect.soft(probe.acks.every(row => row.delivery.id === event.id && row.delivery.sequence === event.sequence && !row.terminalRecorded)).toBe(true);
      expect.soft(probe.counts()).toMatchObject({ terminals: 0, errors: 0, warnings: 0 });
      expectOutsideLock(probe);
    });

    it.each(["same", "higher", "lower"] as const)("keeps a live root retryable with a valid %s receipt and still-mismatched config", async relation => {
      const f = await fixture();
      const { event, terminalBytes } = await interruptedTerminal(f, relation);
      f.drift(); f.rootLease(true);
      const probe = observe(f);
      const consume = () => consumer === "maintenance" ? f.sweep() : wake.wakeResidentActors(f.mesh, [event], f.launch);
      for (let n = 0; n < 4; n++) { await consume(); f.advance(); }
      expect.soft(f.pending()).toEqual({ id: event.id, sequence: event.sequence });
      expect.soft(f.read<FailureRecord>(f.failureFile)).toMatchObject({ delivery: { id: event.id, sequence: event.sequence },
        error: expect.stringContaining("ResidentWakeConfigMismatch"), failures: expect.any(Number) });
      expect.soft(f.read<FailureRecord>(f.failureFile)!.failures).toBeGreaterThan(2);
      expect.soft(probe.counts().configReads).toBeGreaterThan(1);
      expect.soft(f.launch).not.toHaveBeenCalled();
      expect.soft(probe.acks).toEqual([]);
      expect.soft(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
      expect.soft(probe.counts().terminals).toBe(0);
      f.repair();
      for (let n = 0; n < (consumer === "maintenance" ? 4 : 1); n++) { f.advance(); await consume(); }
      expect.soft(f.launch).toHaveBeenCalledOnce();
      expect.soft(f.pending()).toBeUndefined();
      expect.soft(fs.existsSync(f.failureFile)).toBe(false);
      expect.soft(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
      expectOutsideLock(probe);
    });

    it.each(["higher", "lower"] as const)("records fresh exact mismatch evidence rather than reusing a different %s receipt", async relation => {
      const f = await fixture();
      const { event, terminal } = await interruptedTerminal(f, relation);
      f.drift(); f.rootLease(false); // A new terminal record needs valid expired lease evidence.
      const probe = observe(f);
      if (consumer === "maintenance") await f.sweep();
      else await wake.wakeResidentActors(f.mesh, [event], f.launch);
      const recorded = f.read<TerminalRecord>(f.terminalFile);
      expect.soft(recorded).toMatchObject({ delivery: { id: event.id, sequence: event.sequence }, failedAt: f.now });
      expect.soft(recorded?.error).not.toBe(terminal.error);
      expect.soft(probe.writes.filter(row => row.file === f.terminalFile)).toHaveLength(1);
      expect.soft(probe.acks).toEqual([{ delivery: { id: event.id, sequence: event.sequence }, terminalRecorded: true }]);
      expect.soft(f.pending()).toBeUndefined();
      expect.soft(fs.existsSync(f.failureFile)).toBe(false);
      expect.soft(f.launch).not.toHaveBeenCalled();
      expectOutsideLock(probe);
    });
  });

  it("a terminal ACK cannot consume a newer matching delivery, which can wake after config repair", async () => {
    const f = await fixture();
    const first = await f.publishPending();
    f.drift(); f.rootLease(false); // Supply aged valid lease evidence before terminalizing event 1.
    let newer: Awaited<ReturnType<Fixture["publishPending"]>> | undefined;
    const exclusive = f.mesh.exclusive.bind(f.mesh);
    vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      // The receipt is already durable, but its short ACK/cleanup custody has
      // not yet started. Publish a real newer watermark in that exact window.
      if (!newer && fs.existsSync(f.terminalFile)) newer = await f.publishPending();
      return exclusive(operation, timeout);
    });
    const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    await f.sweep();
    const receipt = f.read<TerminalRecord>(f.terminalFile);
    const pendingAfterFirst = f.pending();
    expect.soft(newer, "the terminal disposition must reach its ACK boundary").toBeDefined();
    expect.soft(receipt).toMatchObject({ delivery: { id: first.id, sequence: first.sequence } });
    if (newer) expect.soft(pendingAfterFirst).toMatchObject({ id: newer.id, sequence: newer.sequence });
    f.repair();
    for (let n = 0; n < 4; n++) { f.advance(); await f.sweep(); }
    expect.soft(f.launch).toHaveBeenCalledOnce();
    if (newer) expect.soft(f.read<{ id: string; sequence: number }>(wake.residentWakeRequestPath(f.directory)))
      .toMatchObject({ id: newer.id, sequence: newer.sequence });
    expect.soft(f.pending()).toBeUndefined();
    expect.soft(fs.existsSync(f.failureFile)).toBe(false);
    expect.soft(f.read<TerminalRecord>(f.terminalFile)).toEqual(receipt);
    expect.soft(warnings).toHaveBeenCalledOnce();
  });

  it("uses zero actual native mesh-lock custody for a terminal idle root across sweeps and reopen", async () => {
    const runtime = await lockStatsRuntime();
    cleanups.push(() => runtime.close());
    const f = await fixture(runtime);
    const event1 = await f.publishPending();
    f.drift();
    f.rootLease(false);
    await f.sweep();
    const terminalBytes = fs.readFileSync(f.terminalFile, "utf8");
    expect(f.pending()).toBeUndefined();
    const initialCustody = nativeCustodyCount(f.meshRoot, runtime.lockStatsHost());
    expect(initialCustody).toBeGreaterThan(0); // Positive control: the native recorder saw real first acquisitions.
    const idleAcquisitions: number[] = [];
    let before = initialCustody;
    for (let n = 0; n < 3; n++) {
      f.advance(); await f.sweep();
      const after = nativeCustodyCount(f.meshRoot, runtime.lockStatsHost());
      idleAcquisitions.push(after - before); before = after;
    }
    f.reopen(); // Close the real native recovery cursor, then measure post-reopen sweeps too.
    for (let n = 0; n < 3; n++) {
      f.advance(); await f.sweep();
      const after = nativeCustodyCount(f.meshRoot, runtime.lockStatsHost());
      idleAcquisitions.push(after - before); before = after;
    }
    console.info(JSON.stringify({ probe: "terminal-idle-native-lock-stats", delivery: event1.id,
      sweeps: idleAcquisitions.length, idleCustodyAcquisitions: idleAcquisitions }));
    expect(idleAcquisitions).toEqual([0, 0, 0, 0, 0, 0]);
    expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
    expect(f.pending()).toBeUndefined();
    expect(f.launch).not.toHaveBeenCalled();
    f.repair(); // Repair alone cannot recreate a nudge that the terminal ACK already consumed.
    for (let n = 0; n < 4; n++) { f.advance(); await f.sweep(); }
    expect(f.launch).not.toHaveBeenCalled();
    expect(f.pending()).toBeUndefined();
    const event2 = await f.publishPending();
    for (let n = 0; n < 4; n++) { f.advance(); await f.sweep(); }
    expect(f.launch).toHaveBeenCalledOnce();
    expect(f.read(wake.residentWakeRequestPath(f.directory))).toMatchObject({ id: event2.id, sequence: event2.sequence });
    expect(f.pending()).toBeUndefined();
    expect(fs.readFileSync(f.terminalFile, "utf8")).toBe(terminalBytes);
  });
});
