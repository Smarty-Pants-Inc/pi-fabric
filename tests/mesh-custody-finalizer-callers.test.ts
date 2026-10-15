import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import * as inbox from "../src/topology/stall-alarms.js";
import {
  MeshLockReleaseError, meshLockSecondaryReleaseErrors, windowsMeshReleaser,
  type MeshLockReleaseFailureOutcome, type MeshLockReleaseHandle,
} from "../src/mesh/lock-release.js";

// Keep: data safety / T4 failures not cheaply reachable in a user flow. Invoke the REAL leaseCustody and runtime
// initializeMeshActivation callers (and real MeshLock.exclusive in dual mode), not a copy of their finally blocks.
// Only acquisition handles, native storage outcomes and lineage work are substituted. Native release is recorded,
// never executed; no child, Bun worker, deletion or fixture cleanup is requested by this file. Retain private roots.
// Injected Windows dispatch below tests routing/error contracts, NOT NTFS safety or native helper lifetime.
const boundary = vi.hoisted(() => ({ acquire: vi.fn(), prepare: vi.fn(), release: vi.fn() }));
vi.mock("../src/mesh/custody-lock.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/mesh/custody-lock.js")>()),
  acquireMeshCustodyHandle: boundary.acquire,
}));
vi.mock("../src/storage/windows-mesh-release.js", async importOriginal => ({
  windowsLogicalPathUnits: (await importOriginal<typeof import("../src/storage/windows-mesh-release.js")>()).windowsLogicalPathUnits,
  prepareWindowsMeshCapability: boundary.prepare,
  releaseWindowsMeshLock: boundary.release,
}));
vi.mock("../src/core/atomic-write.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/core/atomic-write.js")>()),
  ownProcessIncarnation: () => Promise.resolve(undefined),
}));

const capability = Object.freeze({ absolutePath: "C:\\cache\\fabric-win-remove.exe", digest: "a".repeat(64), meshAbi: 1 as const });
const refused: MeshLockReleaseFailureOutcome = { outcome: "refused-before-retirement", reason: "sharing denied" };
const unknown: MeshLockReleaseFailureOutcome = { outcome: "release-outcome-unknown", reason: "missing terminal receipt" };
const root = () => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-9569-callers-"));
const handles: Array<MeshLockReleaseHandle & { record: string }> = [];
const stores: MeshStore[] = [];
const runtimes: FabricRuntimeState[] = [];
let warn: ReturnType<typeof vi.spyOn>;

const handle = (domain: string) => {
  const lock = path.join(domain, "custody.lock"), record = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
  const held = { lock, record, release: windowsMeshReleaser(lock, record, capability) };
  handles.push(held);
  return held;
};
const rejection = async (promise: Promise<unknown>): Promise<unknown> => {
  try { await promise; } catch (error) { return error; }
  throw new Error("expected the real caller to reject");
};
const releaseError = (value: unknown): MeshLockReleaseError => {
  expect(value).toBeInstanceOf(MeshLockReleaseError);
  return value as MeshLockReleaseError;
};
const leaseStore = () => {
  const mesh = new MeshStore(root(), 256 * 1024, 100, { lockProtocol: 1, stateBackend: "file" });
  stores.push(mesh);
  return mesh;
};

beforeEach(() => {
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_") || key.startsWith("HERDR_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
  handles.length = 0;
  boundary.acquire.mockReset().mockImplementation(async (domain: string) => handle(domain));
  boundary.prepare.mockReset().mockReturnValue(capability);
  boundary.release.mockReset().mockReturnValue(unknown);
  warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  // Store close/admission housekeeping is recorded too; keep every fixture/census receipt on disk.
  vi.spyOn(fs, "rmSync").mockImplementation(() => undefined);
});
afterEach(async () => {
  try {
    // Activation tests intentionally stop before participant publication. Quiesce would start unrelated publication
    // during shutdown; suppress only that teardown work, then use the real close to release runtime resources.
    vi.spyOn(ParticipantDirectory.prototype, "quiesce").mockResolvedValue(undefined);
    for (const runtime of runtimes.splice(0)) await runtime.shutdown();
    for (const mesh of stores.splice(0)) mesh.closeState();
  } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); }
});

describe("MeshStore.leaseCustody real caller", () => {
  it.each([false, true])("returned operation + release refusal is completed/typed; release-only retry (async=%s)", async asynchronous => {
    const mesh = leaseStore(), file = path.join(mesh.root, "host-leases", "host.json");
    const value = Object.freeze({ committed: true });
    const operation = vi.fn(() => asynchronous ? Promise.resolve(value) : value);
    boundary.release.mockReturnValueOnce(refused).mockReturnValueOnce(unknown).mockReturnValue({ outcome: "not-owner" });
    const error = releaseError(await rejection(mesh.leaseCustody(file, operation, 17, { ownIncarnation: "incarnation" })));
    expect(error).toMatchObject({ operationState: "returned", operationCompleted: true, retryable: false,
      releaseRetryable: true, releaseOutcome: refused });
    const hash = createHash("sha256").update(path.basename(file)).digest("hex");
    expect(boundary.acquire).toHaveBeenCalledExactlyOnceWith(path.join(mesh.root, "host-lease-commits", hash.slice(0, 16)),
      17, { ownIncarnation: "incarnation", hostQualified: true, lockProtocol: 1 });
    const first = boundary.release.mock.calls[0]!;
    expect(first).toEqual([handles[0]!.lock, handles[0]!.record, capability]);
    let retried: unknown;
    try { error.retryRelease(); } catch (again) { retried = again; }
    expect(releaseError(retried)).toMatchObject({ operationState: "returned", operationCompleted: true, releaseOutcome: unknown });
    expect(() => error.retryRelease()).not.toThrow();
    expect(boundary.release.mock.calls).toEqual([first, first, first]);
    expect(boundary.release.mock.calls.every(call => call[2] === capability)).toBe(true);
    expect(boundary.acquire).toHaveBeenCalledTimes(1);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("returns the exact value and keeps custody until an awaited commit settles", async () => {
    const mesh = leaseStore(), value = Object.freeze({ receipt: "committed" });
    boundary.release.mockReturnValue({ outcome: "released", cleanup: "pending", retiredPath: "r.recorded" });
    let finish!: (committedValue: typeof value) => void;
    const committed = new Promise<typeof value>(resolve => { finish = resolve; });
    const operation = vi.fn(() => committed);
    const result = mesh.leaseCustody("lease.json", operation);
    await Promise.resolve(); // acquisition continuation invokes the actual callback
    expect(operation).toHaveBeenCalledTimes(1);
    expect(boundary.release).not.toHaveBeenCalled();
    finish(value);
    await expect(result).resolves.toBe(value);
    expect(boundary.release).toHaveBeenCalledTimes(1);
  });

  it.each([false, true])("preserves the original throws, including primitives (async=%s)", async asynchronous => {
    const mesh = leaseStore();
    for (const primary of [new Error("commit failed"), Object.freeze({ reason: "CAS failed" }), "plain failure", 42, null, undefined]) {
      warn.mockClear();
      const operation = vi.fn(() => { if (asynchronous) return Promise.reject(primary); throw primary; });
      expect(await rejection(mesh.leaseCustody("lease.json", operation))).toBe(primary);
      expect(operation).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("release-outcome-unknown"));
      const secondary = meshLockSecondaryReleaseErrors(primary);
      if (typeof primary === "object" && primary !== null) {
        expect(secondary).toHaveLength(1);
        expect(secondary[0]).toMatchObject({ operationState: "threw", operationCompleted: false, releaseOutcome: unknown });
        expect(secondary[0]!.cause).toBe(primary);
      } else expect(secondary).toEqual([]); // primitives are reported through the alarm, never wrapped/mutated
    }
    expect(boundary.release).toHaveBeenCalledTimes(6);
  });

  it("acquisition rejection does not run or release an unacquired operation", async () => {
    const mesh = leaseStore(), primary = Object.freeze({ admission: "refused" }), operation = vi.fn();
    boundary.acquire.mockRejectedValueOnce(primary);
    expect(await rejection(mesh.leaseCustody("lease.json", operation))).toBe(primary);
    expect(operation).not.toHaveBeenCalled();
    expect(boundary.release).not.toHaveBeenCalled();
  });
});

const activation = (mode: "own" | "dual") => {
  const base = root(), meshRoot = path.join(base, "mesh"), sessionId = "custody-finalizer-caller";
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(base, "runs"));
  vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", mode);
  // The actual dual MeshLock enters its Windows release branch, whose storage boundary is recorded above.
  vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  vi.spyOn(os, "type").mockReturnValue("Windows_NT");
  const context = {
    cwd: base, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getEntries: () => [], getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const pi = { on: () => () => {}, events: { emit: vi.fn() }, getThinkingLevel: () => "off",
    getSessionName: () => "custody-main", sendMessage: vi.fn(), appendEntry: vi.fn() } as unknown as ExtensionAPI;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(base, "unused-extension.mjs"), worker: path.join(base, "unused-worker.mjs"),
    residentHost: path.join(base, "unused-resident.mjs"), skills: base,
  } });
  runtimes.push(runtime);
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, lockProtocol: 1, stateBackend: "file" },
    residency: { enabled: false }, agents: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false }, speculation: { enabled: false },
  });
  const resume = vi.spyOn(ParticipantDirectory.prototype, "resumeLineage").mockResolvedValue(undefined);
  const register = vi.spyOn(inbox, "registerMainInbox");
  let live = true;
  return { runtime, meshRoot, sessionId, resume, register, retire: () => { live = false; },
    initialize: () => runtime.initialize(context, config, { lifecycle: () => live }) };
};

describe("FabricRuntimeState.initializeMeshActivation real caller", () => {
  it("own mode: completed inbox registration survives custody release failure; same-R retry does not replay it", async () => {
    const f = activation("own");
    const error = releaseError(await rejection(f.initialize()));
    expect(error).toMatchObject({ operationState: "returned", operationCompleted: true, releaseOutcome: unknown, retryable: false });
    expect(f.resume).toHaveBeenCalledTimes(1);
    expect(f.register).toHaveBeenCalledTimes(1);
    const owner = JSON.parse(fs.readFileSync(path.join(f.meshRoot, "main-followups", `${f.sessionId}.owner.json`), "utf8"));
    expect(owner).toMatchObject({ sessionId: f.sessionId, rootId: `session:${f.sessionId}`, activationId: f.register.mock.results[0]!.value });
    const first = boundary.release.mock.calls[0]!;
    boundary.release.mockReturnValue({ outcome: "not-owner" });
    expect(() => error.retryRelease()).not.toThrow();
    expect(boundary.release.mock.calls).toEqual([first, first]);
    expect(first).toEqual([handles[0]!.lock, handles[0]!.record, capability]);
    expect(boundary.acquire).toHaveBeenCalledTimes(1);
    expect(f.resume).toHaveBeenCalledTimes(1);
    expect(f.register).toHaveBeenCalledTimes(1);
  });

  it("dual mode: both REAL nested releases run; inner completed error stays primary and each retry is release-only", async () => {
    const f = activation("dual");
    const error = releaseError(await rejection(f.initialize()));
    expect(error).toMatchObject({ lock: path.join(f.meshRoot, ".lock"), operationState: "returned", operationCompleted: true });
    expect(boundary.release.mock.calls.map(call => path.basename(call[0] as string))).toEqual([".lock", "custody.lock"]);
    const [outer] = meshLockSecondaryReleaseErrors(error);
    // The outer callback awaited mesh.exclusive(), which rejected on INNER release, not on registration.
    expect(outer).toMatchObject({ lock: path.join(f.meshRoot, "custody.lock"), operationState: "threw", operationCompleted: false });
    expect(outer!.cause).toBe(error);
    const first = [...boundary.release.mock.calls];
    boundary.release.mockReturnValue({ outcome: "not-owner" });
    error.retryRelease();
    outer!.retryRelease();
    expect(boundary.release.mock.calls).toEqual([...first, ...first]);
    expect(boundary.acquire).toHaveBeenCalledTimes(1);
    expect(f.resume).toHaveBeenCalledTimes(1);
    expect(f.register).toHaveBeenCalledTimes(1);
  });

  it.each(["own", "dual"] as const)("%s: registration throws retain object/primitive identity and all secondary release failures", async mode => {
    // Each invocation gets a fresh runtime/root: the recorded native release intentionally does not remove locks.
    for (const primary of [Object.freeze({ storage: "failed" }), "storage failed", undefined]) {
      const f = activation(mode);
      f.register.mockImplementationOnce(() => { throw primary; });
      const before = boundary.release.mock.calls.length, alarms = warn.mock.calls.length;
      expect(await rejection(f.initialize())).toBe(primary);
      const expected = mode === "dual" ? [".lock", "custody.lock"] : ["custody.lock"];
      expect(boundary.release.mock.calls.slice(before).map(call => path.basename(call[0] as string))).toEqual(expected);
      expect(warn.mock.calls.slice(alarms)).toHaveLength(expected.length);
      expect(warn.mock.calls.slice(alarms).every((call: unknown[]) => String(call[0]).includes("release-outcome-unknown"))).toBe(true);
      if (typeof primary === "object" && primary !== null) {
        const errors = meshLockSecondaryReleaseErrors(primary);
        expect(errors.map(error => path.basename(error.lock))).toEqual(expected);
        for (const error of errors) {
          expect(error).toMatchObject({ operationState: "threw", operationCompleted: false, releaseOutcome: unknown });
          expect(error.cause).toBe(primary);
        }
        expect(Object.keys(primary)).toEqual(["storage"]);
      } else expect(meshLockSecondaryReleaseErrors(primary)).toEqual([]);
      // Restore these application spies between loop iterations; retain runtime resources for afterEach.
      f.resume.mockRestore();
      f.register.mockRestore();
    }
  });

  it("retired after custody acquisition is not-started, not a thrown application operation", async () => {
    const f = activation("own");
    boundary.acquire.mockImplementationOnce(async (domain: string) => { f.retire(); return handle(domain); });
    const primary = await rejection(f.initialize());
    expect(primary).toBeInstanceOf(Error);
    expect((primary as Error).message).toContain("mesh activation lifecycle retired");
    expect(f.resume).not.toHaveBeenCalled();
    expect(f.register).not.toHaveBeenCalled();
    expect(meshLockSecondaryReleaseErrors(primary)).toMatchObject([
      { operationState: "not-started", operationCompleted: false, releaseOutcome: unknown },
    ]);
    expect(boundary.release).toHaveBeenCalledTimes(1);
  });

  it("dual acquisition failure after lineage started is threw; registration and mesh release never ran", async () => {
    const f = activation("dual"), primary = new Error("mesh capability unavailable before acquisition");
    boundary.prepare.mockImplementationOnce(() => { throw primary; });
    expect(await rejection(f.initialize())).toBe(primary);
    expect(f.resume).toHaveBeenCalledTimes(1);
    expect(f.register).not.toHaveBeenCalled();
    expect(meshLockSecondaryReleaseErrors(primary)).toMatchObject([
      { lock: path.join(f.meshRoot, "custody.lock"), operationState: "threw", operationCompleted: false, releaseOutcome: unknown },
    ]);
    expect(boundary.release.mock.calls.map(call => path.basename(call[0] as string))).toEqual(["custody.lock"]);
  });

  it("lineage rejection is threw, not not-started, and custody still releases", async () => {
    const f = activation("own"), primary = new Error("resume failed");
    f.resume.mockRejectedValueOnce(primary);
    expect(await rejection(f.initialize())).toBe(primary);
    expect(f.register).not.toHaveBeenCalled();
    expect(meshLockSecondaryReleaseErrors(primary)).toMatchObject([
      { operationState: "threw", operationCompleted: false, releaseOutcome: unknown },
    ]);
    expect(boundary.release).toHaveBeenCalledTimes(1);
  });
});
