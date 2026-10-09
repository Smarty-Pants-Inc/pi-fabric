import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MAIN_RELOAD_LEASE_MS, ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#6729 gate 1 (pi-fabric#646): a root Main reloads while the shared mesh lock is busy,
// so quiesce("reload")'s shared write times out (FABRIC_MESH_LOCK_TIMEOUT). The REAL mesh .lock is
// held (a live owner receipt, never a mock), the old release tears down (quiesce + close), and the
// same release is re-imported in-process (vi.resetModules + dynamic import) and started. A 100 ms
// sampler checks the host lease file and the root record the whole time.

const LOCK_TIMEOUT_MS = 1_000;
const MAX_LEASE_AGE_MS = 15_000;
const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
});

const sessionId = "6729ffff-bbbb-cccc-dddd-eeeeeeeeeeee";
const identity: MeshIdentity = { id: "session:" + sessionId, sessionId, name: "Main", kind: "main" };
const reader: MeshIdentity = { id: "session:reader", sessionId: "reader", name: "reader", kind: "main" };
const record = (): FabricParticipantRecord => ({
  format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  kind: "root", name: "Main", status: "idle", capabilities: ["steer", "followUp", "fabric"],
  runner: "pi", transport: "host", controlProtocol: "v1", sessionId, cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
});

type Sample = { at: number; phase: string; lease: boolean; leaseAgeMs: number; leaseExpired: boolean; root: boolean };

const fixture = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-reload-continuity-"));
  roots.push(base);
  const meshRoot = path.join(base, "mesh");
  const realNow = Date.now.bind(Date);
  let skew = 0;
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
  const options = { lockTimeoutMs: LOCK_TIMEOUT_MS };
  const observer = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, options), {
    enabled: true, hostId: reader.id, rootId: reader.id, identity: reader, reapDeadHosts: false,
  });
  const lockPath = path.join(meshRoot, ".lock");
  // A live holder: our own pid, a complete protocol-1 receipt. Never stale, so every
  // acquisition in this process waits LOCK_TIMEOUT_MS and throws MeshLockTimeoutError.
  const holdLock = () => {
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), randomUUID() + "\n" + process.pid + "\n" + realNow() + "\n");
  };
  const releaseLock = () => fs.rmSync(lockPath, { recursive: true, force: true });
  cleanup.push(releaseLock);
  let phase = "start";
  const samples: Sample[] = [];
  const sample = (): Sample => {
    const now = Date.now();
    const lease = readHostLeases(meshRoot).get(identity.id);
    let root = false;
    try { root = observer.list({ kinds: ["root"], fresh: true }).some((p) => p.id === identity.id); } catch { root = false; }
    const s = { at: now, phase, lease: lease !== undefined, leaseAgeMs: lease ? now - lease.updatedAt : Infinity,
      leaseExpired: lease ? lease.expiresAt <= now : true, root };
    samples.push(s);
    return s;
  };
  let timer: NodeJS.Timeout | undefined;
  const startSampling = () => { timer = setInterval(sample, 100); };
  const stopSampling = () => { if (timer) clearInterval(timer); timer = undefined; sample(); };
  cleanup.push(stopSampling);
  return { meshRoot, options, holdLock, releaseLock, sample, samples, startSampling, stopSampling,
    setPhase: (p: string) => { phase = p; }, advance: (ms: number) => { skew += ms; } };
};

/** A Fabric release's directory, built from the given (possibly re-imported) modules. */
const directoryFrom = (modules: { MeshStore: typeof MeshStore; ParticipantDirectory: typeof ParticipantDirectory },
  meshRoot: string, options: { lockTimeoutMs: number }, timing: { leaseMs?: number; heartbeatMs?: number } = {}) => {
  const directory = new modules.ParticipantDirectory(new modules.MeshStore(meshRoot, 64 * 1024, 1_000, options), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false, ...timing,
  });
  directory.registerSource(() => [record()]);
  cleanup.push(() => directory.close());
  return directory;
};

/** The next release's import: a fresh module graph in this process. */
const reimport = async () => {
  vi.resetModules();
  const [store, directory] = await Promise.all([
    import("../src/mesh/store.js"), import("../src/topology/participant-directory.js"),
  ]);
  return { MeshStore: store.MeshStore, ParticipantDirectory: directory.ParticipantDirectory };
};

const summary = (samples: Sample[]) => ({
  samples: samples.length,
  leaseMissing: samples.filter((s) => !s.lease).length,
  leaseExpired: samples.filter((s) => s.leaseExpired).length,
  rootMissing: samples.filter((s) => !s.root).length,
  maxLeaseAgeMs: Math.max(...samples.filter((s) => s.lease).map((s) => s.leaseAgeMs), 0),
  firstLeaseMissing: samples.find((s) => !s.lease)?.phase,
  firstRootMissing: samples.find((s) => !s.root)?.phase,
  maxSampleGapMs: samples.slice(1).reduce((max, s, i) => Math.max(max, s.at - samples[i]!.at), 0),
});

const report = (name: string, value: unknown) => {
  const line = JSON.stringify({ test: name, ...(value as object) });
  console.log("RELOAD-LEASE " + line);
  if (process.env.RELOAD_LEASE_REPORT) fs.appendFileSync(process.env.RELOAD_LEASE_REPORT, line + "\n");
};

describe("root Main lease continuity through a reload whose quiesce write times out (smarty-dev#6729 gate 1)", () => {
  it("keeps the root record and a fresh host lease file through teardown + in-process re-import", async () => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    expect(f.sample()).toMatchObject({ lease: true, root: true, leaseExpired: false });
    f.startSampling();

    // Fleet lock load: the shared mesh lock is busy through the old release's whole teardown.
    f.holdLock();
    f.setPhase("quiesce");
    const t0 = Date.now();
    const quiesceError = await old.quiesce("reload").then(() => undefined, (error: unknown) => error);
    const quiesceMs = Date.now() - t0;
    // session_shutdown swallows the failure (fabric-runtime-state.ts) and closes anyway.
    expect((quiesceError as { code?: string } | undefined)?.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
    f.setPhase("close");
    const t1 = Date.now();
    await old.close();
    const closeMs = Date.now() - t1;
    f.releaseLock();

    f.setPhase("import");
    const t2 = Date.now();
    const next = await reimport();
    const importMs = Date.now() - t2;
    f.setPhase("first-heartbeat");
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    await fresh.start();
    f.setPhase("after");
    const heartbeatGapMs = Date.now() - t1;
    await new Promise((resolve) => setTimeout(resolve, 300));
    f.stopSampling();

    const s = summary(f.samples);
    report("in-process", { quiesceMs, closeMs, importMs, closeToFirstHeartbeatMs: heartbeatGapMs, ...s });
    expect(s.firstLeaseMissing, "host lease file missing").toBeUndefined();
    expect(s.firstRootMissing, "root record missing").toBeUndefined();
    expect(s.leaseExpired, "host lease expired").toBe(0);
    expect(s.maxLeaseAgeMs, "host lease age").toBeLessThanOrEqual(MAX_LEASE_AGE_MS);
    expect(s.maxSampleGapMs, "sampler starved").toBeLessThan(MAX_LEASE_AGE_MS);
  }, 60_000);

  // Second gap: the soak's self-reload import took p50 51 s, max 68 s, with nothing renewing the
  // lease in between. Measured, not gated on age: the lease is kept, but how stale does it get, and
  // what does the new release's first heartbeat do while the lock is still busy?
  it.each([
    [51_000, false], [68_000, false], [51_000, true], [68_000, true],
  ])("second gap: a %i ms import under lock load (files-only policy: %s)", async (importDelayMs, filesOnly) => {
    const f = fixture();
    if (filesOnly) {
      await new MeshStore(f.meshRoot, 64 * 1024, 1_000, f.options).put({ key: LIVENESS_POLICY_KEY,
        value: { version: 1, participants: "files", hostLeases: "files" }, identity });
    }
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    f.holdLock();
    f.setPhase("quiesce");
    await old.quiesce("reload").catch(() => undefined);
    f.setPhase("close");
    await old.close();
    f.sample();
    const reloadLease = readHostLeases(f.meshRoot).get(identity.id);
    // The import: injected clock advanced in 100 ms steps, sampled at every step.
    f.setPhase("import");
    const next = await reimport();
    for (let elapsed = 0; elapsed < importDelayMs; elapsed += 100) { f.advance(100); f.sample(); }
    const teardownAndImport = summary(f.samples);
    // The new release's first heartbeat while the lock is STILL busy.
    f.setPhase("first-heartbeat-locked");
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    const startError = await fresh.start().then(() => undefined, (error: unknown) => error);
    const lockedLease = readHostLeases(f.meshRoot).get(identity.id);
    const afterLockedStart = f.sample();
    // Lock load clears; the next heartbeat commits.
    f.releaseLock();
    f.setPhase("heartbeat-unlocked");
    await fresh.refresh();
    const afterCommit = f.sample();
    report("second-gap-" + importDelayMs + (filesOnly ? "-files" : "-mesh"), {
      importDelayMs, filesOnly, ...teardownAndImport,
      reloadLeaseExpiresInMs: (reloadLease?.expiresAt ?? 0) - (reloadLease?.updatedAt ?? 0),
      lockedStart: { error: (startError as { code?: string } | undefined)?.code ?? null,
        leaseRewrittenByNewIncarnation: lockedLease?.startedAt !== reloadLease?.startedAt,
        leaseTtlMs: (lockedLease?.expiresAt ?? 0) - Date.now(), leaseAgeMs: afterLockedStart.leaseAgeMs,
        rootVisible: afterLockedStart.root },
      afterCommit: { leaseAgeMs: afterCommit.leaseAgeMs, rootVisible: afterCommit.root },
    });
    // Kept through teardown and the whole import, on the reload grace, never refreshed.
    expect(teardownAndImport.firstLeaseMissing, "host lease file missing").toBeUndefined();
    expect(teardownAndImport.firstRootMissing, "root record missing").toBeUndefined();
    expect(teardownAndImport.leaseExpired, "host lease expired").toBe(0);
    expect(teardownAndImport.maxLeaseAgeMs).toBeGreaterThanOrEqual(importDelayMs);
    expect(afterCommit).toMatchObject({ lease: true, root: true, leaseExpired: false });
    expect(afterCommit.leaseAgeMs).toBeLessThanOrEqual(MAX_LEASE_AGE_MS);
  }, 60_000);
});

// The directory gap (smarty-dev#6729 gate 1, after #646): the soak kept every lease, yet 12 root
// Mains vanished from the fleet directory for ~10-55 s each during their reload. A listing pairs a
// root record with its owner's SHARED host record, extended only by a lease file of the same
// incarnation (hostLiveness: matching startedAt). The new release's first heartbeat replaced the
// predecessor's live reload lease file with its own (new startedAt) before its shared host record
// could commit under lock load, so the old host record lost its lease and the root dropped out
// until that commit landed. Sampled from a reader's directory listing every 100 ms.
describe("root Main stays in the directory listing through a reload under lock load (smarty-dev#6729 gate 1)", () => {
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  it("lists the Main through close(), a 51 s import and the new release's locked first heartbeats", async () => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    expect(f.sample()).toMatchObject({ lease: true, root: true });
    f.startSampling();
    f.holdLock();
    f.setPhase("quiesce");
    await old.quiesce("reload").catch(() => undefined);
    f.setPhase("close");
    await old.close();
    f.setPhase("import");
    const next = await reimport();
    for (let elapsed = 0; elapsed < 51_000; elapsed += 100) { f.advance(100); f.sample(); }
    // The new release starts while the lock is still busy: its first heartbeat and a retry time out.
    f.setPhase("first-heartbeat-locked");
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    const startError = await fresh.start().then(() => undefined, (error: unknown) => error);
    f.setPhase("retry-locked");
    const retryError = await fresh.refresh().then(() => undefined, (error: unknown) => error);
    await sleep(500);
    f.releaseLock();
    f.setPhase("heartbeat-unlocked");
    await fresh.refresh();
    await sleep(300);
    f.stopSampling();
    const s = summary(f.samples);
    const missing = f.samples.filter((x) => !x.root);
    report("directory-gap", { ...s, rootMissingPhases: [...new Set(missing.map((x) => x.phase))],
      rootMissingSpanMs: missing.length ? missing.at(-1)!.at - missing[0]!.at : 0 });
    expect((startError as { code?: string } | undefined)?.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
    expect((retryError as { code?: string } | undefined)?.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
    expect(s.firstRootMissing, "root Main missing from the directory listing").toBeUndefined();
    expect(s.rootMissing).toBe(0);
    expect(s.leaseExpired, "host lease expired").toBe(0);
    expect(f.samples.at(-1)).toMatchObject({ lease: true, root: true, leaseExpired: false });
    expect(f.samples.at(-1)!.leaseAgeMs).toBeLessThanOrEqual(MAX_LEASE_AGE_MS);
  }, 60_000);

  it.each(["ordinary heartbeat", "unpaired reload"] as const)("does not keep a predecessor's %s lease during first commit", async (kind) => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    const ordinary = readHostLeases(f.meshRoot).get(identity.id)!;
    f.holdLock();
    await old.quiesce("reload").catch(() => undefined);
    await old.close();
    const reload = readHostLeases(f.meshRoot).get(identity.id)!;
    const predecessor = kind === "ordinary heartbeat" ? ordinary : { ...reload, startedAt: reload.startedAt! + 1 };
    writeHostLease(f.meshRoot, predecessor);
    f.advance(1_000);
    const next = await reimport();
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    expect((await fresh.start().then(() => undefined, (error: unknown) => error) as { code?: string })?.code)
      .toBe("FABRIC_MESH_LOCK_TIMEOUT");
    // Ordinary/unpaired evidence cannot withhold the new incarnation's liveness file.
    expect(readHostLeases(f.meshRoot).get(identity.id)!.startedAt).not.toBe(predecessor.startedAt);
    f.releaseLock();
    await fresh.refresh();
    expect(f.sample()).toMatchObject({ root: true, leaseExpired: false });
  }, 30_000);

  it("takes over the reload lease as soon as its own host record commits, before post-commit file work", async () => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    f.holdLock();
    await old.quiesce("reload").catch(() => undefined);
    await old.close();
    f.advance(1_000);
    const next = await reimport();
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    await fresh.start().catch(() => undefined);
    const predecessor = readHostLeases(f.meshRoot).get(identity.id)!;
    // Force a root update so the first successful host commit has a post-commit file copy.
    vi.spyOn(fresh, "scheduleRefresh").mockImplementation(() => {});
    fresh.registerSource(() => [{ ...record(), status: "running" }]);
    f.releaseLock();
    const files = await import("../src/topology/participant-files.js");
    const write = files.writeParticipantFileIf;
    const keyHash = createHash("sha256").update(identity.id).digest("hex");
    let observed: { hostStartedAt: number; leaseStartedAt: number | undefined } | undefined;
    vi.spyOn(files, "writeParticipantFileIf").mockImplementation(async (mesh, key, decide, options) => {
      if (!observed && key === "topology/participants/" + keyHash) {
        const host = fresh.mesh.get("topology/hosts/" + keyHash, { fresh: true })!.value as { startedAt: number };
        observed = { hostStartedAt: host.startedAt, leaseStartedAt: readHostLeases(f.meshRoot).get(identity.id)!.startedAt };
      }
      return write(mesh, key, decide, options);
    });
    await fresh.refresh();
    expect(observed).toBeDefined();
    expect(observed!.hostStartedAt).not.toBe(predecessor.startedAt);
    expect(observed!.leaseStartedAt).toBe(observed!.hostStartedAt);
    expect(f.sample()).toMatchObject({ root: true, leaseExpired: false });
  }, 30_000);

  it.each([false, true])("keeps a live reload lease through its final seconds (legacy writer=%s)", async (legacy) => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    f.holdLock();
    await old.quiesce("reload").catch(() => undefined);
    await old.close();
    const kept = readHostLeases(f.meshRoot).get(identity.id)!;
    expect(kept.reloadUntil).toBe(kept.expiresAt);
    if (legacy) {
      const { reloadUntil: _reloadUntil, ...oldLease } = kept;
      writeHostLease(f.meshRoot, oldLease);
    }
    f.advance(kept.expiresAt - Date.now() - (legacy ? 5_000 : 15_000));
    const next = await reimport();
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    expect((await fresh.start().then(() => undefined, (error: unknown) => error) as { code?: string })?.code)
      .toBe("FABRIC_MESH_LOCK_TIMEOUT");
    expect(readHostLeases(f.meshRoot).get(identity.id)!.startedAt).toBe(kept.startedAt);
    expect(f.sample()).toMatchObject({ root: true, leaseExpired: false });
    f.releaseLock();
    await fresh.refresh();
    expect(readHostLeases(f.meshRoot).get(identity.id)!.reloadUntil).toBeUndefined();
  }, 30_000);

  it("drops a Main whose process is gone once its reload lease expires", async () => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    f.holdLock();
    await old.quiesce("reload").catch(() => undefined);
    await old.close();
    f.releaseLock();
    // The process died mid-reload: nothing renews the reload lease and no release starts.
    f.advance(MAIN_RELOAD_LEASE_MS - 5_000);
    expect(f.sample()).toMatchObject({ root: true, leaseExpired: false });
    f.advance(10_000);
    expect(f.sample()).toMatchObject({ root: false, leaseExpired: true });
  }, 30_000);

  it("drops a Main whose next release never commits once the predecessor's reload lease expires", async () => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options);
    await old.start();
    f.holdLock();
    await old.quiesce("reload").catch(() => undefined);
    await old.close();
    const next = await reimport();
    const fresh = directoryFrom(next, f.meshRoot, f.options);
    await fresh.start().catch(() => undefined);
    expect(f.sample()).toMatchObject({ root: true });
    // The lock never clears for it: the reload lease, not the stalled release, bounds the listing.
    f.advance(MAIN_RELOAD_LEASE_MS + 5_000);
    await fresh.refresh().catch(() => undefined);
    expect(f.sample()).toMatchObject({ root: false });
    f.releaseLock();
  }, 30_000);

  // PR #663 round 2: the kept lease is recognised by its own live expiry, not by outlasting this
  // release's lease (a 180 s reload lease never outlasts leaseMs >= 180 s, nor heartbeatMs >= 90 s).
  it.each([
    { name: "leaseMs = 180 s", timing: { leaseMs: 180_000 } },
    { name: "heartbeatMs = 90 s", timing: { heartbeatMs: 90_000 } },
  ])("keeps the predecessor's reload lease with $name until it expires", async ({ timing }) => {
    const f = fixture();
    const old = directoryFrom({ MeshStore, ParticipantDirectory }, f.meshRoot, f.options, timing);
    await old.start();
    f.holdLock();
    await old.quiesce("reload").catch(() => undefined);
    await old.close();
    const kept = readHostLeases(f.meshRoot).get(identity.id)!;
    const next = await reimport();
    f.advance(1_000);
    const fresh = directoryFrom(next, f.meshRoot, f.options, timing);
    expect((await fresh.start().then(() => undefined, (error: unknown) => error) as { code?: string })?.code)
      .toBe("FABRIC_MESH_LOCK_TIMEOUT");
    expect(readHostLeases(f.meshRoot).get(identity.id)).toMatchObject({ startedAt: kept.startedAt, expiresAt: kept.expiresAt });
    expect(f.sample()).toMatchObject({ root: true, leaseExpired: false });
    f.advance(kept.expiresAt - Date.now() - 5_000);
    await fresh.refresh().catch(() => undefined);
    expect(f.sample()).toMatchObject({ root: true, leaseExpired: false });
    // Expired: no longer kept, and the stalled release no longer lists the Main.
    f.advance(10_000);
    await fresh.refresh().catch(() => undefined);
    expect(readHostLeases(f.meshRoot).get(identity.id)!.startedAt).not.toBe(kept.startedAt);
    expect(f.sample()).toMatchObject({ root: false });
    f.releaseLock();
  }, 30_000);
});
