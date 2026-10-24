import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import childProcess from "node:child_process";
import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { LIVENESS_POLICY_KEY, readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import * as fileLock from "../src/residency/file-lock.js";
import * as removal from "../src/storage/verified-remove.js";
import * as atomic from "../src/core/atomic-write.js";
import { windowsStateAclRefusal } from "../src/mesh/windows-state-acl.js";

const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const fixtureRemove = fs.rmSync.bind(fs); // Only the freshly allocated fixture root may actually be removed.

// sha256 of the exact UTF-16LE -EncodedCommand bytes: windowsSecurityPowerShell's fixed module prefix plus the
// reviewed read-only SNAPSHOT_ACLS reader (src/mesh/windows-state-acl.ts). A change to either is re-reviewed and re-pinned.
const ACL_READER_SHA256 = "23ef37816a7b37876a9becaa552fa07c60716c1e978e75ba9a4e966e84841b46";
const ACL_READER_FLAGS = ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand"];
const ACL_READER_OPTIONS = "encoding,env,maxBuffer,stdio,timeout,windowsHide"; // No shell, cwd, argv0, input, ...
type ChildHost = { platform: NodeJS.Platform; environment: NodeJS.ProcessEnv; execute: (...call: unknown[]) => unknown };
const plain = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
// Windows environment names are case-insensitive: exactly one spelling, the production one, or nothing.
const onlyVariable = (env: Record<string, unknown>, name: string): unknown => {
  const hits = Object.entries(env).filter(([key]) => key.toLowerCase() === name.toLowerCase());
  return hits.length === 1 && hits[0]![0] === name ? hits[0]![1] : undefined;
};

/** The startup child exception as a pure predicate of one execFileSync call (platform-injectable, so the Windows
 * input contract is checked on every OS). It returns the first snapshot path only for the exact pinned reader:
 * pinned PowerShell 5.1 file, exact flags with no extra argv, canonical encoding of the pinned script bytes, the
 * exact option keys, its own system module path, and a read-only (`protect: null`) request whose every path is one
 * of `roots`, inside them, or a lexical/real ancestor. Anything else is undefined. */
const aclSnapshotReader = (platform: NodeJS.Platform, environment: NodeJS.ProcessEnv, roots: string[]) => {
  const bases = roots.map(base => path.win32.resolve(base).toLowerCase());
  const covered = (target: unknown) => {
    if (typeof target !== "string") return false;
    const at = path.win32.resolve(target).toLowerCase();
    return bases.some(base => at === base || at.startsWith(base + "\\") || base.startsWith(at.endsWith("\\") ? at : at + "\\"));
  };
  return (file: unknown, args: unknown, options: unknown): string | undefined => {
    if (platform !== "win32") return undefined;
    const systemRoot = Object.entries(environment).find(([key]) => key.toLowerCase() === "systemroot")?.[1];
    if (!systemRoot || !path.win32.isAbsolute(systemRoot)) return undefined;
    const home = path.win32.join(systemRoot, "System32", "WindowsPowerShell", "v1.0");
    if (file !== path.win32.join(home, "powershell.exe")) return undefined;
    if (!Array.isArray(args) || args.length !== ACL_READER_FLAGS.length + 1
      || ACL_READER_FLAGS.some((flag, index) => args[index] !== flag)) return undefined;
    const encoded: unknown = args[ACL_READER_FLAGS.length];
    if (typeof encoded !== "string") return undefined;
    const script = Buffer.from(encoded, "base64");
    if (script.toString("base64") !== encoded || createHash("sha256").update(script).digest("hex") !== ACL_READER_SHA256) return undefined;
    if (!plain(options) || Object.keys(options).sort().join() !== ACL_READER_OPTIONS || !plain(options.env)) return undefined;
    if (onlyVariable(options.env, "PSModulePath") !== path.win32.join(home, "Modules")) return undefined;
    let request: unknown;
    try { request = JSON.parse(String(onlyVariable(options.env, "PI_FABRIC_STATE_ACL"))); } catch { return undefined; }
    if (!plain(request) || Object.keys(request).sort().join() !== "paths,protect") return undefined;
    const { protect, paths } = request;
    // Read-only snapshot only: a protect request would run icacls, which startup here never needs.
    if (protect !== null || !Array.isArray(paths) || paths.length === 0 || !paths.every(covered)) return undefined;
    return String(paths[0]);
  };
};

/** #4664: every product deletion is recorded BEFORE emulation by rename. No
 * original delete is delegated, even for lock receipts or already absent files.
 * /proc/self/fd paths are resolved while their directory descriptor is held.
 * Child processes are violations. The one exception is startup admission before
 * `seal()`: the exact pinned read-only native mesh-state ACL snapshot (aclSnapshotReader;
 * Windows SQLite only) runs for real, so the gate admits this root as production does
 * instead of failing open uncached on a guard throw. It deletes nothing and sets no ACL.
 * After `seal()` every child, that reader included, is a violation. `host` is injectable
 * only so the guard probe below can stub the executor and pose as Windows. */
const recordDeletes = (root: string, host: ChildHost = {
  platform: process.platform, environment: process.env, execute: childProcess.execFileSync as unknown as ChildHost["execute"],
}) => {
  const grave = path.join(root, "delete-emulation");
  fs.mkdirSync(grave, { mode: 0o700 });
  const records: Array<{ api: string; target: string; present: boolean }> = [];
  const violations: string[] = [];
  const admissions: string[] = [];
  let sealed = false;
  const aclSnapshot = aclSnapshotReader(host.platform, host.environment, [root, fs.realpathSync.native(root)]);
  const emulate = (api: string, target: fs.PathLike, force = false) => {
    const raw = String(target);
    const directory = path.dirname(raw);
    let resolved: string;
    try { resolved = path.join(fs.realpathSync(directory), path.basename(raw)); }
    catch { resolved = path.resolve(raw); }
    const present = fs.lstatSync(resolved, { throwIfNoEntry: false }) !== undefined;
    records.push({ api, target: resolved, present });
    if (process.env.FAST_PATH_EVIDENCE_DIR) fs.appendFileSync(path.join(process.env.FAST_PATH_EVIDENCE_DIR, "deletes.jsonl"),
      JSON.stringify(records.at(-1)) + "\n");
    if (!resolved.startsWith(root + path.sep) || resolved.startsWith(grave + path.sep)) {
      violations.push(resolved);
      throw new Error(`Deletion outside this fixture: ${resolved}`);
    }
    if (!present) {
      if (force) return;
      throw Object.assign(new Error(`Missing emulated deletion: ${resolved}`), { code: "ENOENT" });
    }
    if (api.includes("rmdir") && fs.readdirSync(resolved).length > 0) {
      throw Object.assign(new Error("Emulated rmdir refuses nonempty directories"), { code: "ENOTEMPTY" });
    }
    fs.renameSync(resolved, path.join(grave, String(records.length)));
  };
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => emulate("rmSync", target, options?.force));
  vi.spyOn(fs, "unlinkSync").mockImplementation(target => emulate("unlinkSync", target));
  vi.spyOn(fs, "rmdirSync").mockImplementation(target => emulate("rmdirSync", target));
  vi.spyOn(fsp, "rm").mockImplementation(async (target, options) => emulate("promises.rm", target, options?.force));
  vi.spyOn(fsp, "unlink").mockImplementation(async target => emulate("promises.unlink", target));
  vi.spyOn(fsp, "rmdir").mockImplementation(async target => emulate("promises.rmdir", target));
  // fs.promises and the default node:fs/promises import share these methods.
  expect(fs.promises.rm).toBe(fsp.rm);
  vi.spyOn(fs, "rm").mockImplementation((target, options, callback) => {
    try { emulate("rm", target, options.force); callback(null); } catch (error) { callback(error as NodeJS.ErrnoException); }
  });
  vi.spyOn(fs, "unlink").mockImplementation((target, callback) => {
    try { emulate("unlink", target); callback(null); } catch (error) { callback(error as NodeJS.ErrnoException); }
  });
  vi.spyOn(fs, "rmdir").mockImplementation(((target: fs.PathLike, options: unknown, callback?: fs.NoParamCallback) => {
    const done = typeof options === "function" ? options as fs.NoParamCallback : callback!;
    try { emulate("rmdir", target); done(null); } catch (error) { done(error as NodeJS.ErrnoException); }
  }) as typeof fs.rmdir);
  const verify = removal.verifiedRemove;
  vi.spyOn(removal, "verifiedRemove").mockImplementation((target, judged, options) => {
    // Force the real verifier's non-destructive branch; never invoke a product fallback/helper delete.
    const result = verify(target, judged, { ...options, dryRun: true,
      fallback: () => { throw new Error("Real deletion fallback forbidden"); } });
    if (result.removed) emulate("verifiedRemove.dryRun", target);
    return result;
  });
  for (const api of ["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync", "fork"] as const) {
    vi.spyOn(childProcess, api).mockImplementation(((...call: unknown[]) => {
      const admitted = !sealed && api === "execFileSync" ? aclSnapshot(call[0], call[1], call[2]) : undefined;
      if (admitted !== undefined) { admissions.push(admitted); return host.execute(...call); }
      violations.push(`child:${api}`); throw new Error(`No child processes: ${api}`);
    }) as never);
  }
  return { records, violations, admissions, seal: () => { sealed = true; } };
};

const fixture = async (files = false, backend: "file" | "sqlite" = "file") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "effect-free-publication-"));
  // ParticipantDirectory prepares this memoized native identity before publication fencing.
  // Its one-time Windows PowerShell probe is startup, not idle heartbeat work (#7651).
  // Prime it before the strict no-child guard, as for the unrelated native host lock below.
  await atomic.ownProcessIncarnation();
  const safety = recordDeletes(root);
  // This probe does not test the unrelated native host-startup flock helper.
  // Keep its real descriptor/inode checks, but avoid launching a child process.
  vi.spyOn(fileLock, "lockFile").mockImplementation(async file => {
    if (!file.startsWith(root + path.sep)) throw new Error("Foreign host lock");
    return fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
  });
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:effect-free", sessionId: "effect-free", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend: backend },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention, legacyRunArchiveEnabled: false },
    workerPath: path.join(root, "unused-worker.js"), fabricExtensionPath: path.join(root, "unused-index.js"),
    piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  };
  const host = new ResidentHost(config);
  const actorRoots = [config.actorRoot, config.sessionActorRoot!];
  const acquired = vi.spyOn(fs, "writeFileSync"); // Call-through: count actual registry owner publications, not wrapper calls.
  const acquisitions = () => acquired.mock.calls.filter(([target]) => actorRoots.some(actorRoot =>
    String(target) === path.join(actorRoot, "actors.json.lock", "owner"))).map(([target]) => String(target));
  const custody = () => actorRoots.filter(actorRoot => fs.existsSync(path.join(actorRoot, "actors.json.lock", "owner")));
  const clear = () => acquired.mockClear();
  const observers: Array<{ mockRestore(): void }> = [];
  const observe = <T extends { mockRestore(): void }>(spy: T): T => { observers.push(spy); return spy; };
  const close = async () => {
    try {
      // Stop assertion-only observers before withdrawal (which is a different path).
      for (const spy of observers) spy.mockRestore();
      await host.close();
      expect(safety.violations).toEqual([]);
      expect(safety.records.length).toBeGreaterThan(0);
    } finally {
      vi.restoreAllMocks();
      fixtureRemove(root, { recursive: true, force: true });
    }
  };
  try {
    if (files) {
      const { MeshStore } = await import("../src/mesh/store.js");
      const policy = new MeshStore(config.meshRoot, 65536, 100, { stateBackend: backend });
      try { await policy.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" },
        identity: { id: "policy", name: "policy", kind: "agent" } }); }
      finally { policy.closeState(); }
    }
    await host.start();
    expect(host.mesh.stateBackend).toBe(backend); // Never count a silent backend fallback as SQLite coverage.
    // Default 5-second timer is armed by real startup. Manual refresh executes
    // the same full heartbeat, without background races in the acquisition count.
    expect(host.participants.heartbeatArmed).toBe(true);
    expect(host.participants.options.heartbeatMs).toBeUndefined();
    host.participants.suspend();
    // Startup admission is real and complete (non-vacuous exactly for Windows SQLite). From here on, hot path,
    // failed-mesh and registry/generation counterexamples and close included, any child is a violation.
    expect(safety.admissions.length > 0).toBe(process.platform === "win32" && backend === "sqlite");
    safety.seal();
    return { root, config, host, actorRoots, acquired, acquisitions, custody, clear, observe, close };
  } catch (error) { await close(); throw error; }
};

const participant = (f: Awaited<ReturnType<typeof fixture>>, id: string): FabricParticipantRecord =>
  readParticipantFile(f.config.meshRoot, participantKey(id))!.value as FabricParticipantRecord;

const modes = (["file", "sqlite"] as const).flatMap(backend => [false, true].map(files => ({ backend, files })));
describe("effect-free resident publication (#8526)", () => {
  it.each(modes)("idle heartbeat takes zero registries and retains lease/confirmation ($backend files=$files)", async ({ files, backend }) => {
    const f = await fixture(files, backend);
    try {
      const confirmed = f.host.participants.confirmedAt();
      const lease = readHostLease(f.config.meshRoot, f.host.hostId)!;
      const now = Date.now() + 5_000;
      vi.spyOn(Date, "now").mockReturnValue(now);
      const writable = vi.spyOn(f.host.mesh, "confirmWritable");
      const batch = vi.spyOn(f.host.mesh, "writeBatch");
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence"); // Call-through, no replacement fence.
      const locks = vi.spyOn(ActorRegistryStore, "withLocks");
      f.clear();
      await f.host.participants.refresh();
      console.info(JSON.stringify({ probe: "idle-heartbeat", backend, files, registryAcquisitions: f.acquisitions().length,
        actorIds: fence.mock.calls[0]?.[1] === undefined ? "uncertain" : [...fence.mock.calls[0][1]!],
        leaseRenewed: readHostLease(f.config.meshRoot, f.host.hostId)!.updatedAt > lease.updatedAt,
        meshConfirmed: f.host.participants.confirmedAt() > confirmed }));
      expect(readHostLease(f.config.meshRoot, f.host.hostId)?.updatedAt).toBeGreaterThan(lease.updatedAt);
      expect(f.host.participants.confirmedAt()).toBeGreaterThan(confirmed);
      expect(f.host.participants.canConsumeMesh()).toBe(true);
      expect(writable.mock.calls.length + batch.mock.calls.length).toBeGreaterThan(0);
      expect(f.acquisitions()).toEqual([]);
      expect(fence).toHaveBeenCalledOnce();
      expect(fence.mock.calls[0]?.[1]).toEqual(new Set());
      expect(Array.from(locks.mock.calls[0]!)[2]).toBe("presence-fence");
    } finally { await f.close(); }
  });

  it.each(modes)("effect-free heartbeat does not confirm a failed mesh ($backend files=$files)", async ({ files, backend }) => {
    const f = await fixture(files, backend);
    try {
      const confirmed = f.host.participants.confirmedAt();
      const lease = readHostLease(f.config.meshRoot, f.host.hostId)!;
      vi.spyOn(Date, "now").mockReturnValue(Date.now() + 5_000);
      const unavailable = new Error("mesh confirmation unavailable");
      const writable = f.observe(vi.spyOn(f.host.mesh, "confirmWritable").mockRejectedValue(unavailable));
      f.observe(vi.spyOn(f.host.mesh, "writeBatch").mockRejectedValue(unavailable));
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence");
      f.clear();
      await expect(f.host.participants.refresh()).rejects.toThrow(unavailable.message);
      expect(writable).toHaveBeenCalledOnce();
      expect(readHostLease(f.config.meshRoot, f.host.hostId)!.updatedAt).toBeGreaterThan(lease.updatedAt);
      expect(f.host.participants.confirmedAt()).toBe(confirmed);
      expect(f.host.participants.canConsumeMesh()).toBe(false);
      expect(f.acquisitions()).toEqual([]);
      expect(fence.mock.calls[0]?.[1]).toEqual(new Set());
    } finally { await f.close(); }
  });

  it.each(modes)("empty preparation replacement still revalidates generation ($backend files=$files)", async ({ files, backend }) => {
    const f = await fixture(files, backend);
    try {
      const registry = new ActorRegistryStore(f.config.actorRoot);
      const lock = ActorRegistryStore.withLocks;
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence");
      const prepare = vi.spyOn(f.host.participants.options, "preparePublicationFence");
      vi.spyOn(ActorRegistryStore, "withLocks").mockImplementationOnce(async (stores, operation) => {
        await registry.withLock(() => registry.write([])); // A new atomic registry generation after effect classification.
        return lock(stores, operation);
      });
      f.clear();
      await f.host.participants.refresh();
      expect(prepare).toHaveBeenCalledTimes(2);
      expect(fence).toHaveBeenCalledTimes(2);
      expect(fence.mock.calls.every(([, ids]) => ids !== undefined && ids.size === 0)).toBe(true);
      // Only the injected real replacement needs a registry; both idle attempts bypass it.
      expect(f.acquisitions()).toEqual([path.join(f.config.actorRoot, "actors.json.lock", "owner")]);
      expect(f.host.participants.canConsumeMesh()).toBe(true);
    } finally { await f.close(); }
  });

  it.each(modes)("actor put retains its real holding registry ($backend files=$files)", async ({ files, backend }) => {
    const f = await fixture(files, backend);
    try {
      const actor = await f.host.actors.create({ name: "fenced-actor", instructions: "wait", residency: "durable" });
      await f.host.participants.refresh();
      const record = { ...participant(f, actor.id), name: "changed actor" };
      f.host.participants.registerSource(() => [record]);
      const publish = f.host.mesh.writeBatch.bind(f.host.mesh);
      f.observe(vi.spyOn(f.host.mesh, "writeBatch").mockImplementation(async input => {
        if (input.ops.some(op => op.key === participantKey(actor.id))) expect(f.custody()).toEqual([f.config.actorRoot]);
        return publish(input);
      }));
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence");
      f.clear();
      await f.host.participants.refreshPresence();
      expect(participant(f, actor.id).name).toBe(record.name);
      expect(f.acquisitions()).toEqual([path.join(f.config.actorRoot, "actors.json.lock", "owner")]);
      expect(fence.mock.calls[0]?.[1]).toEqual(new Set([actor.id]));
    } finally { await f.close(); }
  });

  it.each([{ actorIds: undefined }, { actorIds: ["unknown-actor"] }])("uncertain/unheld batch remains fenced (ids=$actorIds)", async ({ actorIds }) => {
    const f = await fixture();
    try {
      f.host.participants.options.publicationBatch = () => ({
        ops: [{ kind: "put", key: "actors/effect-free/unknown-actor", value: { status: "idle" } }],
        ...(actorIds === undefined ? {} : { actorIds }), committed: () => {},
      });
      const publish = f.host.mesh.writeBatch.bind(f.host.mesh);
      f.observe(vi.spyOn(f.host.mesh, "writeBatch").mockImplementation(async input => {
        expect(f.custody()).toEqual(f.actorRoots);
        return publish(input);
      }));
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence");
      f.clear();
      await f.host.participants.refreshPresence();
      expect(f.acquisitions().sort()).toEqual(f.actorRoots.map(root => path.join(root, "actors.json.lock", "owner")).sort());
      expect(fence.mock.calls[0]?.[1]).toEqual(actorIds === undefined ? undefined : new Set(actorIds));
    } finally { await f.close(); }
  });

  it.each(modes)("adoption after preparation discards the generation before publication ($backend files=$files)", async ({ files, backend }) => {
    const f = await fixture(files, backend);
    try {
      const actor = await f.host.actors.create({ name: "adopted-actor", instructions: "wait", residency: "durable" });
      await f.host.participants.refresh();
      const stale = { ...participant(f, actor.id), name: "stale prepared actor" };
      f.host.participants.registerSource(() => [stale]);
      const registry = new ActorRegistryStore(f.config.actorRoot);
      const lock = ActorRegistryStore.withLocks;
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence");
      vi.spyOn(ActorRegistryStore, "withLocks").mockImplementationOnce(async (stores, operation) => {
        // Atomic adoption between preparation and the genuine fence acquisition.
        await registry.withLock(() => registry.write(registry.records().map(row => row.id === actor.id
          ? { ...row, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [f.config.rootId] } : row)));
        return lock(stores, operation);
      });
      const publish = f.host.mesh.writeBatch.bind(f.host.mesh);
      f.observe(vi.spyOn(f.host.mesh, "writeBatch").mockImplementation(async input => {
        expect(input.ops.some(op => op.kind === "put" && op.key === participantKey(actor.id))).toBe(false);
        expect(f.custody()).toEqual(f.actorRoots); // Own-record removal is uncertain, so it keeps the all-registry fence.
        return publish(input);
      }));
      f.clear();
      await f.host.participants.refreshPresence();
      expect(fence.mock.calls[0]?.[1]).toEqual(new Set([actor.id]));
      expect(fence.mock.calls.some(([, ids]) => ids === undefined)).toBe(true);
      expect(registry.records().find(row => row.id === actor.id)?.rootId).toBe("session:successor");
      expect(readParticipantFile(f.config.meshRoot, participantKey(actor.id))).toBeUndefined();
      expect(f.host.mesh.get(participantKey(actor.id), { fresh: true })).toBeUndefined();
      expect(f.acquisitions().length).toBeGreaterThanOrEqual(4); // Adoption, vetoed publication, conservative retry.
    } finally { await f.close(); }
  });

  it.each(modes)("the first hourly heartbeat leaves stopped actor envelopes unchanged ($backend files=$files)", async ({ files, backend }) => {
    const f = await fixture(files, backend);
    try {
      const actor = await f.host.actors.create({ name: "stopped-actor", instructions: "wait", residency: "durable" });
      await f.host.actors.stop(actor.id);
      await f.host.participants.refresh();
      const key = participantKey(actor.id), before = readParticipantFile(f.config.meshRoot, key)!;
      const presenceKey = `actors/${f.config.sessionId}/${actor.id}`;
      const presence = f.host.mesh.get(presenceKey, { fresh: true })!;
      const shared = f.host.mesh.get(key, { fresh: true });
      const lease = readHostLease(f.config.meshRoot, f.host.hostId)!;
      // Crossing the former hourly renewal floor must not rewrite either actor
      // envelope. The FIRST heartbeat is effect-free; no settling round is allowed.
      vi.spyOn(Date, "now").mockReturnValue(Date.now() + 60 * 60 * 1_000 + 5_000);
      const fence = vi.spyOn(f.host.participants.options, "withPublicationFence");
      const batch = vi.spyOn(f.host.mesh, "writeBatch");
      f.clear();
      await f.host.participants.refresh();
      expect(f.acquisitions()).toEqual([]);
      expect(fence).toHaveBeenCalledOnce();
      expect(fence.mock.calls[0]?.[1]).toEqual(new Set());
      expect(batch.mock.calls.flatMap(([input]) => input.ops).filter(op =>
        op.key === key || op.key === presenceKey)).toEqual([]);
      expect(readParticipantFile(f.config.meshRoot, key)).toEqual(before);
      expect(f.host.mesh.get(key, { fresh: true })).toEqual(shared);
      expect(f.host.mesh.get(presenceKey, { fresh: true })).toEqual(presence);
      const renewed = readHostLease(f.config.meshRoot, f.host.hostId)!;
      expect(renewed.updatedAt).toBeGreaterThan(lease.updatedAt);
      expect(renewed.expiresAt).toBeGreaterThan(Date.now());
      expect(f.host.participants.get(actor.id, Date.now(), { fresh: true })).toMatchObject({ status: "stopped", stale: false });
      expect(f.host.participants.canConsumeMesh()).toBe(true);
    } finally { await f.close(); }
  });
});

// Guard matching only, on every OS: no NTFS ACL is read here and no child runs. Each executor is a stub.
describe("startup ACL reader exception admits only the exact pinned reader", () => {
  it("refuses marker lookalikes, extra argv, module, protect and foreign-target variants, and everything after seal", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "effect-free-publication-"));
    const foreign = fs.mkdtempSync(path.join(os.tmpdir(), "effect-free-publication-"));
    try {
      vi.stubEnv("SystemRoot", "C:\\Windows");
      // Record the production reader's own execFileSync calls; the stub returns non-JSON, so nothing runs.
      const captured: unknown[][] = [];
      const capture = vi.spyOn(childProcess, "execFileSync").mockImplementation(((...call: unknown[]) => {
        captured.push(call); return "not a snapshot";
      }) as never);
      const read = (at: string, protect?: true) => windowsStateAclRefusal({ root: at, real: fs.realpathSync.native(at),
        files: [path.join(at, "state.db")], directories: [], ...(protect ? { protect } : {}) });
      for (const refusal of [read(root), read(root, true), read(foreign)]) expect(refusal).toMatch(/unreadable Windows ACL snapshot/);
      capture.mockRestore();
      expect(captured).toHaveLength(3);
      const [exact, protect, foreignTarget] = captured as [unknown[], unknown[], unknown[]];
      const [file, args, options] = exact as [string, string[], { env: NodeJS.ProcessEnv }];
      const request = (call: unknown[]) => JSON.parse((call[2] as typeof options).env.PI_FABRIC_STATE_ACL!) as { protect: unknown };
      expect(request(exact).protect).toBeNull();
      expect(request(protect).protect).toBe(path.win32.resolve(root));
      const encode = (script: string) => Buffer.from(script, "utf16le").toString("base64");
      const reader = Buffer.from(args[4]!, "base64").toString("utf16le");
      const prefix = reader.slice(0, reader.indexOf("\n", reader.indexOf("Import-Module")) + 1);
      const destructive = "Remove-Item -LiteralPath $env:TEMP -Recurse -Force"; // Never executed: the executor is a stub.
      const lookalike = prefix + "# $env:PI_FABRIC_STATE_ACL GetSecurityDescriptorBinaryForm\n" + destructive + "\n";
      // Both scripts contain the two markers that the earlier contains-only guard accepted.
      for (const script of [lookalike, reader + destructive]) {
        expect(script).toContain("$env:PI_FABRIC_STATE_ACL");
        expect(script).toContain("GetSecurityDescriptorBinaryForm");
      }
      const flags = args.slice(0, 4);
      const forbidden: Record<string, unknown[]> = {
        "marker lookalike script": [file, [...flags, encode(lookalike)], options],
        "reader with an appended command": [file, [...flags, encode(reader + destructive)], options],
        "non-canonical encoding of the reader": [file, [...flags, args[4] + "\n"], options],
        "extra trailing argv": [file, [...args, "-Command", destructive], options],
        "extra leading argv": [file, ["-ExecutionPolicy", "Bypass", ...args], options],
        "other PowerShell binary": ["C:\\Program Files\\PowerShell\\7\\pwsh.exe", args, options],
        "wrong module path": [file, args, { ...options, env: { ...options.env, PSModulePath: "C:\\Users\\Public\\Modules" } }],
        "second module path spelling": [file, args, { ...options, env: { ...options.env, psmodulepath: "C:\\Users\\Public\\Modules" } }],
        "shell option": [file, args, { ...options, shell: true }],
        "protect request": protect,
        "foreign target": foreignTarget,
      };
      const executed = vi.fn((..._call: unknown[]) => "executor stub: nothing ran");
      const safety = recordDeletes(root, { platform: "win32", environment: process.env, execute: executed });
      const execFile = (call: unknown[]) => (childProcess.execFileSync as unknown as (...call: unknown[]) => unknown)(...call);
      for (const [name, call] of Object.entries(forbidden)) expect(() => execFile(call), name).toThrow("No child processes: execFileSync");
      expect(() => (childProcess.spawnSync as unknown as (...call: unknown[]) => unknown)(...exact)).toThrow("No child processes: spawnSync");
      expect(aclSnapshotReader("linux", process.env, [root])(file, args, options)).toBeUndefined(); // Never off Windows.
      expect(executed).not.toHaveBeenCalled();
      expect(safety.admissions).toEqual([]);
      // The exact production reader for this root is admitted once, before seal, through the stubbed executor.
      expect(execFile(exact)).toBe("executor stub: nothing ran");
      expect(executed).toHaveBeenCalledOnce();
      expect(executed).toHaveBeenCalledWith(...exact);
      expect(safety.admissions).toEqual([path.win32.resolve(root)]);
      safety.seal();
      expect(() => execFile(exact)).toThrow("No child processes: execFileSync");
      expect(executed).toHaveBeenCalledOnce();
      expect(safety.violations).toEqual([...Object.keys(forbidden).map(() => "child:execFileSync"),
        "child:spawnSync", "child:execFileSync"]);
      expect(safety.records).toEqual([]);
    } finally {
      vi.restoreAllMocks();
      vi.unstubAllEnvs();
      for (const at of [root, foreign]) fixtureRemove(at, { recursive: true, force: true });
    }
  });
});
