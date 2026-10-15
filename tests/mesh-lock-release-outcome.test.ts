import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { format } from "node:util";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// smarty-dev#9569 Commit A: release-outcome semantics (T4 typed errors, operation once, primary preservation, nested
// finalization, same-record retry) and the Windows dispatch of every lock route. Decision C: recorded calls only. The
// storage module's native release is MOCKED (it never runs, nothing is deleted); real NTFS/handle behavior is the
// hosted Windows suite's job, not this file's. Roots stay in the private TMPDIR and are never removed here.

const storage = vi.hoisted(() => ({
  prepare: vi.fn(),
  release: vi.fn(),
}));
vi.mock("../src/storage/windows-mesh-release.js", async importOriginal => ({
  windowsLogicalPathUnits: (await importOriginal<typeof import("../src/storage/windows-mesh-release.js")>()).windowsLogicalPathUnits,
  prepareWindowsMeshCapability: storage.prepare,
  releaseWindowsMeshLock: storage.release,
}));
const removed = vi.hoisted(() => vi.fn());
vi.mock("../src/storage/verified-remove.js", () => ({ verifiedRemove: removed }));
vi.mock("../src/core/atomic-write.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/core/atomic-write.js")>()),
  // No native incarnation reader under the injected platform.
  ownProcessIncarnation: () => Promise.resolve(undefined),
}));

const {
  finalizeMeshLocks, MeshLockReleaseError, meshLockSecondaryReleaseErrors, MeshLockPreEntryRefusedError, windowsMeshReleaser,
  runUnderMeshLocks,
} = await import("../src/mesh/lock-release.js");
const { MeshLock } = await import("../src/mesh/mesh-lock.js");
const { holdMeshFence, holdMeshFenceSync } = await import("../src/mesh/fence-lock.js");
const { acquireMeshCustodyLock } = await import("../src/mesh/custody-lock.js");
const mesh = await import("../src/mesh.js");

type Outcome = { outcome: string; reason?: string; cleanup?: string; retiredPath?: string };
const refused: Outcome = { outcome: "refused-before-retirement", reason: "share denied" };
const unknown: Outcome = { outcome: "release-outcome-unknown", reason: "helper killed" };
const capability = Object.freeze({ absolutePath: "C:\\cache\\fabric-win-remove.exe", digest: "a".repeat(64), meshAbi: 1 as const });

const tempRoot = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-9569-release-"));
const recorder = (...results: (Outcome | undefined)[]) => {
  const calls: number[] = [];
  const fn = vi.fn(() => { calls.push(calls.length); return (results.length > 1 ? results.shift() : results[0]) as never; });
  return fn;
};

let warn: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  storage.prepare.mockReset().mockReturnValue(capability);
  storage.release.mockReset().mockReturnValue({ outcome: "released", cleanup: "clean", retiredPath: "r.x" });
  removed.mockReset();
  warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
});
afterEach(() => { vi.restoreAllMocks(); });

describe("shared finalizer (T4, platform independent)", () => {
  it("returns the value after releasing every handle exactly once", () => {
    const inner = recorder(undefined), outer = recorder(undefined);
    expect(finalizeMeshLocks({ state: "returned", value: 7 }, [{ lock: "in", release: inner }, { lock: "out", release: outer }])).toBe(7);
    expect(inner).toHaveBeenCalledTimes(1);
    expect(outer).toHaveBeenCalledTimes(1);
  });

  it("a release failure after a returned operation is typed; retryRelease repeats the release only", () => {
    const operation = vi.fn(() => "done");
    const release = recorder(refused as never, unknown as never, undefined);
    let caught: unknown;
    try { finalizeMeshLocks({ state: "returned", value: operation() }, [{ lock: "L", release }]); } catch (error) { caught = error; }
    expect(caught).toBeInstanceOf(MeshLockReleaseError);
    const error = caught as InstanceType<typeof MeshLockReleaseError>;
    expect(error).toMatchObject({
      code: "FABRIC_MESH_LOCK_RELEASE_FAILED", operationState: "returned", operationCompleted: true, retryable: false,
      releaseRetryable: true, lock: "L", releaseOutcome: refused,
    });
    let retried: unknown;
    try { error.retryRelease(); } catch (again) { retried = again; }
    expect(retried).toBeInstanceOf(MeshLockReleaseError);
    expect((retried as InstanceType<typeof MeshLockReleaseError>).releaseOutcome).toEqual(unknown);
    expect((retried as InstanceType<typeof MeshLockReleaseError>).operationCompleted).toBe(true);
    expect(() => error.retryRelease()).not.toThrow();
    expect(release).toHaveBeenCalledTimes(3);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("preserves an Error primary by identity and reports the release failure as secondary", () => {
    const primary = new Error("operation failed");
    const release = recorder(refused as never);
    let caught: unknown;
    try { finalizeMeshLocks({ state: "threw", error: primary }, [{ lock: "L", release }]); } catch (error) { caught = error; }
    expect(caught).toBe(primary);
    expect(Object.keys(primary)).toEqual([]);
    const [secondary, ...rest] = meshLockSecondaryReleaseErrors(primary);
    expect(rest).toEqual([]);
    expect(secondary).toMatchObject({ operationState: "threw", operationCompleted: false, releaseOutcome: refused });
    expect(secondary!.cause).toBe(primary);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("FABRIC_MESH_LOCK_RELEASE_FAILED"));
  });

  // Keep: #1191 security boundary. Capture the real finalizer's warning, not just the thrown-value metadata.
  it.each(["Error instance", "string"] as const)(
    "#1191 captured secondary alarm redacts application secrets from a %s primary", category => {
      const messageMarker = "1191_APPLICATION_MESSAGE_SECRET";
      const nameMarker = "1191_APPLICATION_NAME_SECRET";
      const primitiveMarker = "1191_APPLICATION_PRIMITIVE_SECRET";
      const applicationError = new Error(messageMarker);
      applicationError.name = nameMarker;
      const primary = category === "Error instance" ? applicationError : primitiveMarker;
      const operation = vi.fn(() => { throw primary; });
      const release = recorder(refused as never);
      let caught: unknown = Symbol("not thrown");
      try { runUnderMeshLocks(operation, [{ lock: "L", release }]); } catch (error) { caught = error; }
      expect(caught).toBe(primary);
      expect(operation).toHaveBeenCalledTimes(1);
      expect(release).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledTimes(1);

      // Check every argument as well as console.warn's formatted output; no extra argument may carry the primary.
      const captured = warn.mock.calls.flatMap((args: unknown[]) => [...args.map(arg => format(arg)), format(...args)]).join("\n");
      for (const marker of [messageMarker, nameMarker, primitiveMarker]) expect(captured).not.toContain(marker);
      const expected = new MeshLockReleaseError("L", "threw", refused as never, release, { cause: primary });
      expect(warn.mock.calls).toEqual([
        [`[pi-fabric] ALARM ${expected.kind} (secondary to ${category}): ${expected.message}`],
      ]);
      const secondaries = meshLockSecondaryReleaseErrors(primary);
      if (primary instanceof Error) {
        expect(secondaries).toHaveLength(1);
        expect(secondaries[0]!.cause).toBe(primary);
        expect(secondaries[0]).toMatchObject({ operationState: "threw", operationCompleted: false, releaseOutcome: refused });
      } else {
        expect(secondaries).toEqual([]);
      }
    });

  it.each(["name", "message"] as const)(
    "#1191 captured secondary alarm never reads an application Error.%s getter", property => {
      const primary = new Error("1191_UNREAD_APPLICATION_MESSAGE");
      const read = vi.fn(() => { throw new Error("1191_HOSTILE_GETTER_SECRET"); });
      Object.defineProperty(primary, property, { get: read });
      const release = recorder(unknown as never);
      let caught: unknown = Symbol("not thrown");
      try { runUnderMeshLocks(() => { throw primary; }, [{ lock: "L", release }]); } catch (error) { caught = error; }
      expect(caught).toBe(primary);
      expect(read).not.toHaveBeenCalled();
      expect(release).toHaveBeenCalledTimes(1);
      const secondaries = meshLockSecondaryReleaseErrors(primary);
      expect(secondaries).toHaveLength(1);
      expect(secondaries[0]!.cause).toBe(primary);
      const expected = new MeshLockReleaseError("L", "threw", unknown as never, release, { cause: primary });
      expect(warn.mock.calls).toEqual([
        [`[pi-fabric] ALARM ${expected.kind} (secondary to Error instance): ${expected.message}`],
      ]);
      expect(format(...warn.mock.calls[0]!)).not.toContain("1191_");
    });

  it("#1191 revoked Proxy primary keeps the fixed fallback warning and original identity", () => {
    const { proxy: primary, revoke } = Proxy.revocable(new Error("1191_REVOKED_APPLICATION_SECRET"), {});
    revoke();
    const operation = vi.fn(() => { throw primary; });
    const release = recorder(refused as never);
    let caught: unknown = Symbol("not thrown");
    try { runUnderMeshLocks(operation, [{ lock: "L", release }]); } catch (error) { caught = error; }
    // Vitest inspects matcher inputs; compare identity before giving it the revoked object.
    expect(Object.is(caught, primary)).toBe(true);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(release).toHaveBeenCalledTimes(1);
    const secondaries = meshLockSecondaryReleaseErrors(primary);
    expect(secondaries).toHaveLength(1);
    expect(Object.is(secondaries[0]!.cause, primary)).toBe(true);
    const expected = new MeshLockReleaseError("L", "threw", refused as never, release, { cause: primary });
    expect(warn.mock.calls).toEqual([
      [`[pi-fabric] ALARM ${expected.kind} (secondary to object): ${expected.message}`],
    ]);
    expect(format(...warn.mock.calls[0]!)).not.toContain("1191_REVOKED_APPLICATION_SECRET");
  });

  it.each([["string", "plain failure"], ["number", 42], ["frozen object", Object.freeze({ reason: "x" })], ["undefined", undefined]])(
    "preserves a non-Error %s primary unmutated with an observable secondary report", (_kind, primary) => {
      const release = recorder(unknown as never);
      let caught: unknown = Symbol("none");
      try { finalizeMeshLocks({ state: "threw", error: primary }, [{ lock: "L", release }]); } catch (error) { caught = error; }
      expect(caught).toBe(primary);
      expect(release).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0]![0])).toContain("release-outcome-unknown");
      if (typeof primary === "object" && primary !== null) expect(meshLockSecondaryReleaseErrors(primary)).toHaveLength(1);
    });

  it("nested finalization releases BOTH when the inner fails and aggregates both failures", () => {
    const inner = recorder(refused as never), outer = recorder(unknown as never);
    let caught: unknown;
    try { finalizeMeshLocks({ state: "returned", value: 1 }, [{ lock: "inner", release: inner }, { lock: "outer", release: outer }]); }
    catch (error) { caught = error; }
    expect(inner).toHaveBeenCalledTimes(1);
    expect(outer).toHaveBeenCalledTimes(1);
    const error = caught as InstanceType<typeof MeshLockReleaseError>;
    expect(error.lock).toBe("inner");
    expect(error.related.map(entry => [entry.lock, entry.releaseOutcome.outcome])).toEqual([["outer", "release-outcome-unknown"]]);

    const primary = new TypeError("op");
    try { finalizeMeshLocks({ state: "threw", error: primary }, [{ lock: "inner", release: inner }, { lock: "outer", release: outer }]); }
    catch (thrown) { expect(thrown).toBe(primary); }
    expect(meshLockSecondaryReleaseErrors(primary).map(entry => entry.lock)).toEqual(["inner", "outer"]);
  });

  it("a release that throws, or an unrecognized storage outcome, is release-outcome-unknown, never pre-commit", () => {
    const thrower = vi.fn(() => { throw new Error("spawn exploded after start"); });
    let caught: unknown;
    try { finalizeMeshLocks({ state: "returned", value: 0 }, [{ lock: "L", release: thrower }]); } catch (error) { caught = error; }
    expect((caught as InstanceType<typeof MeshLockReleaseError>).releaseOutcome.outcome).toBe("release-outcome-unknown");
    storage.release.mockReturnValueOnce({ outcome: "ok" });
    expect(windowsMeshReleaser("L", "R\n", capability)()).toMatchObject({ outcome: "release-outcome-unknown" });
    storage.release.mockReturnValueOnce({ outcome: "not-owner" });
    expect(windowsMeshReleaser("L", "R\n", capability)()).toBeUndefined();
  });

  it("is exported from the public mesh entry", () => {
    expect(mesh.MeshLockReleaseError).toBe(MeshLockReleaseError);
    expect(mesh.meshLockSecondaryReleaseErrors).toBe(meshLockSecondaryReleaseErrors);
  });
});

describe("Windows dispatch with the native release mocked (platform injected; not native evidence)", () => {
  beforeEach(() => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.spyOn(os, "type").mockReturnValue("Windows_NT");
  });

  it("an injected process.platform alone keeps main's paths (existing Linux suites need no helper)", async () => {
    vi.mocked(os.type).mockReturnValue("Linux");
    const root = tempRoot();
    await expect(new MeshLock(root, { lockProtocol: 1 }, () => undefined).withLock(() => 1)).resolves.toBe(1);
    expect(holdMeshFenceSync(tempRoot(), 0, () => 2)).toBe(2);
    expect(storage.prepare).not.toHaveBeenCalled();
    expect(storage.release).not.toHaveBeenCalled();
  });

  it("mesh .lock: capability before publication; release once with the ORIGINAL record and captured capability", async () => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    storage.prepare.mockImplementation(() => { expect(fs.existsSync(lock)).toBe(false); return capability; });
    let record: string | undefined;
    const operation = vi.fn(() => { record = fs.readFileSync(path.join(lock, "owner"), "utf8"); return "ok"; });
    const onOperationError = vi.fn(() => { throw new Error("must not run after success"); });
    await expect(new MeshLock(root, { lockProtocol: 1 }, onOperationError).withLock(operation)).resolves.toBe("ok");
    expect(operation).toHaveBeenCalledTimes(1);
    expect(onOperationError).not.toHaveBeenCalled();
    expect(storage.prepare).toHaveBeenCalledTimes(1);
    expect(storage.release).toHaveBeenCalledTimes(1);
    expect(storage.release.mock.calls[0]).toEqual([lock, record, capability]);
    expect(storage.release.mock.calls[0]![2]).toBe(capability);
    expect(removed).not.toHaveBeenCalled(); // no verifiedRemove/rename fallback on Windows
  });

  // Keep: CR-A-03 recorded real catch. Exercise the actual mesh -> adapter -> finalizer path; only storage is mocked.
  it.each(["released", "not-owner"] as const)(
    "CR-A-03 mesh .lock: %s with history emits a resolved receipt; no-history success stays quiet", async outcome => {
      const root = tempRoot();
      const lock = path.join(root, ".lock");
      const resolved = outcome === "released" ? { outcome, cleanup: "pending", retiredPath: "r.recorded" } : { outcome };
      const history = ["attempt 1: release-outcome-unknown (helper killed)",
        ...(outcome === "released" ? ["cleanup pending-owner-delete"] : []), `attempt 2: ${outcome}`];
      storage.release.mockReturnValueOnce({ ...resolved, history });
      const operation = vi.fn(() => "committed");
      const onOperationError = vi.fn();
      await expect(new MeshLock(root, { lockProtocol: 1 }, onOperationError).withLock(operation)).resolves.toBe("committed");
      expect(operation).toHaveBeenCalledTimes(1);
      expect(onOperationError).not.toHaveBeenCalled();
      expect(storage.release).toHaveBeenCalledTimes(1); // adapter diagnostics never retry/replay
      expect(storage.release.mock.calls[0]).toEqual([lock, fs.readFileSync(path.join(lock, "owner"), "utf8"), capability]);
      expect(warn.mock.calls).toEqual([["[pi-fabric] mesh-lock-release-resolved", { lock, outcome, history }]]);
      expect((warn.mock.calls[0]![1] as { history: readonly string[] }).history).toBe(history); // full, ordered history survives
      expect(removed).not.toHaveBeenCalled();

      // Counterexamples paired with the regression: neither absent nor empty history emits a diagnostic.
      warn.mockClear();
      for (const quiet of [resolved, { ...resolved, history: [] }]) {
        storage.release.mockReturnValueOnce(quiet);
        await expect(new MeshLock(tempRoot(), { lockProtocol: 1 }, onOperationError).withLock(operation)).resolves.toBe("committed");
      }
      expect(warn).not.toHaveBeenCalled();
      expect(operation).toHaveBeenCalledTimes(3);
      expect(onOperationError).not.toHaveBeenCalled();
      expect(storage.release).toHaveBeenCalledTimes(3);
    });

  it.each(["released", "not-owner"] as const)(
    "CR-A-03 mesh .lock: a throwing %s-history logger cannot fail success or mask a primary throw", async outcome => {
      const resolved = outcome === "released" ? { outcome, cleanup: "clean", retiredPath: "r.recorded" } : { outcome };
      const history = ["attempt 1: release-outcome-unknown (helper killed)", `attempt 2: ${outcome}`];
      storage.release.mockReturnValue({ ...resolved, history });
      warn.mockImplementation(() => { throw new Error("diagnostic failed"); });
      const onOperationError = vi.fn();
      const operation = vi.fn(() => "committed");
      const root = tempRoot();
      await expect(new MeshLock(root, { lockProtocol: 1 }, onOperationError).withLock(operation)).resolves.toBe("committed");
      expect(operation).toHaveBeenCalledTimes(1);
      expect(onOperationError).not.toHaveBeenCalled();
      expect(storage.release).toHaveBeenCalledTimes(1);
      expect(warn.mock.calls).toEqual([["[pi-fabric] mesh-lock-release-resolved", { lock: path.join(root, ".lock"), outcome, history }]]);

      const primary = "application failure";
      const throwing = vi.fn(() => { throw primary; });
      const failedRoot = tempRoot();
      await expect(new MeshLock(failedRoot, { lockProtocol: 1 }, onOperationError).withLock(throwing)).rejects.toBe(primary);
      expect(throwing).toHaveBeenCalledTimes(1);
      expect(onOperationError).toHaveBeenCalledTimes(1);
      expect(storage.release).toHaveBeenCalledTimes(2);
      expect(warn.mock.calls).toEqual([
        ["[pi-fabric] mesh-lock-release-resolved", { lock: path.join(root, ".lock"), outcome, history }],
        ["[pi-fabric] mesh-lock-release-resolved", { lock: path.join(failedRoot, ".lock"), outcome, history }],
      ]);
      expect(meshLockSecondaryReleaseErrors(primary)).toEqual([]);
      expect(removed).not.toHaveBeenCalled();
    });

  it("mesh .lock: rejects a 240-unit retired owner before publication even when canonical owner fits", async () => {
    const base = tempRoot();
    const root = path.join(base, "x".repeat(167 - base.length - 1));
    const lock = path.join(root, ".lock");
    expect(path.resolve(lock, "owner").length).toBeLessThan(240);
    expect(path.join(path.resolve(root), `r.${"0".repeat(64)}`, "owner").length).toBe(240);
    const operation = vi.fn();
    await expect(new MeshLock(root, { lockProtocol: 1 }, () => undefined).withLock(operation))
      .rejects.toThrow("240 logical UTF-16 units");
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lock)).toBe(false);
    expect(fs.existsSync(path.join(lock, "owner"))).toBe(false);
    expect(storage.release).not.toHaveBeenCalled();
  });

  it("mesh .lock: a 150-unit Unicode root remains within the release budget", async () => {
    const base = tempRoot();
    const root = path.join(base, `${"x".repeat(150 - base.length - 3)}😀`);
    expect(root.length).toBe(150);
    expect(path.join(path.resolve(root), `r.${"0".repeat(64)}`, "owner").length).toBe(223);
    const operation = vi.fn(() => "ok");
    await expect(new MeshLock(root, { lockProtocol: 1 }, () => undefined).withLock(operation)).resolves.toBe("ok");
    expect(operation).toHaveBeenCalledTimes(1);
    expect(storage.release).toHaveBeenCalledTimes(1);
  });

  it("mesh .lock: a capability failure publishes nothing and never runs the operation", async () => {
    const root = tempRoot();
    storage.prepare.mockImplementation(() => { throw new Error("H2 refused"); });
    const operation = vi.fn();
    await expect(new MeshLock(root, { lockProtocol: 1 }, () => undefined).withLock(operation)).rejects.toThrow("H2 refused");
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(storage.release).not.toHaveBeenCalled();
  });

  it("mesh .lock: returned operation + failed release is typed, operation once, retryRelease same record", async () => {
    const root = tempRoot();
    storage.release.mockReturnValueOnce(unknown);
    const operation = vi.fn(() => 5);
    const error = await new MeshLock(root, { lockProtocol: 2 }, () => undefined).withLock(operation).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(MeshLockReleaseError);
    expect(error).toMatchObject({ operationState: "returned", operationCompleted: true, releaseOutcome: unknown });
    storage.release.mockReturnValueOnce({ outcome: "not-owner" });
    (error as InstanceType<typeof MeshLockReleaseError>).retryRelease();
    expect(storage.release).toHaveBeenCalledTimes(2);
    expect(storage.release.mock.calls[1]).toEqual(storage.release.mock.calls[0]);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("mesh .lock: a non-Error operation throw survives a failed release, reported as secondary", async () => {
    const root = tempRoot();
    storage.release.mockReturnValueOnce(refused);
    const primary = { notAnError: true };
    const onOperationError = vi.fn();
    const rejection = await new MeshLock(root, { lockProtocol: 1 }, onOperationError)
      .withLock(() => { throw primary; }).catch((e: unknown) => e);
    expect(rejection).toBe(primary);
    expect(onOperationError).toHaveBeenCalledTimes(1);
    expect(storage.release).toHaveBeenCalledTimes(1);
    expect(Object.keys(primary)).toEqual(["notAnError"]);
    expect(meshLockSecondaryReleaseErrors(primary)[0]).toMatchObject({ operationState: "threw", releaseOutcome: refused });
  });

  // Keep: #9569 data safety regression. A throwing invalidation callback used to skip finalization entirely.
  it.each([new Error("operation failed"), "primitive failure", undefined])(
    "mesh .lock: callback throw cannot mask primary %s or skip release", async primary => {
      const root = tempRoot();
      const lockPath = path.join(root, ".lock");
      const callbackError = new Error("invalidation failed");
      const onOperationError = vi.fn(() => { throw callbackError; });
      let record: string | undefined;
      const operation = vi.fn(() => {
        record = fs.readFileSync(path.join(lockPath, "owner"), "utf8");
        throw primary;
      });
      await expect(new MeshLock(root, { lockProtocol: 1 }, onOperationError).withLock(operation)).rejects.toBe(primary);
      expect(operation).toHaveBeenCalledTimes(1);
      expect(onOperationError).toHaveBeenCalledTimes(1);
      expect(storage.release).toHaveBeenCalledTimes(1);
      expect(storage.release.mock.calls[0]).toEqual([lockPath, record, capability]);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("onOperationError failed"), callbackError);
      expect(removed).not.toHaveBeenCalled();
    });

  it("mesh .lock: async primary survives callback, diagnostic and release failures", async () => {
    const primary = Object.freeze({ reason: "async operation failed" });
    const onOperationError = vi.fn(() => { throw new Error("invalidation failed"); });
    warn.mockImplementation(() => { throw new Error("diagnostic failed"); });
    storage.release.mockReturnValueOnce(refused);
    const operation = vi.fn(async () => { throw primary; });
    await expect(new MeshLock(tempRoot(), { lockProtocol: 1 }, onOperationError)
      .withLockAcrossAwait(operation)).rejects.toBe(primary);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(onOperationError).toHaveBeenCalledTimes(1);
    expect(storage.release).toHaveBeenCalledTimes(1);
    expect(meshLockSecondaryReleaseErrors(primary)[0]).toMatchObject({ operationState: "threw", releaseOutcome: refused });
  });

  it("mesh .lock: abort after publication survives callback throw and releases once before entry", async () => {
    const root = tempRoot();
    const lockPath = path.join(root, ".lock");
    const controller = new AbortController();
    const primary = "abort before operation";
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string, options?: fs.WriteFileOptions) => {
      write(file, data, options);
      if (String(file) === path.join(lockPath, "owner")) controller.abort(primary);
    }) as typeof fs.writeFileSync);
    const onOperationError = vi.fn(() => { throw new Error("invalidation failed"); });
    const operation = vi.fn();
    storage.release.mockReturnValueOnce(refused);
    await expect(new MeshLock(root, { lockProtocol: 1, writeSignal: controller.signal }, onOperationError)
      .withLock(operation)).rejects.toBe(primary);
    expect(operation).not.toHaveBeenCalled();
    expect(onOperationError).toHaveBeenCalledTimes(1);
    expect(storage.release).toHaveBeenCalledTimes(1);
    expect(storage.release.mock.calls[0]).toEqual([lockPath, fs.readFileSync(path.join(lockPath, "owner"), "utf8"), capability]);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("operation not-started"));
  });

  it("mesh .lock: pre-entry refuses an unexpected child beside owner (T6 slice); the operation never runs", async () => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string, options?: fs.WriteFileOptions) => {
      write(file, data, options);
      if (String(file) === path.join(lock, "owner")) write(path.join(lock, "owner.released.legacy"), "x\n");
    }) as typeof fs.writeFileSync);
    storage.release.mockReturnValueOnce(refused);
    const operation = vi.fn();
    const error = await new MeshLock(root, { lockProtocol: 1 }, () => undefined).withLock(operation).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(MeshLockPreEntryRefusedError);
    expect(error).toMatchObject({ reason: "unexpected-children", entries: ["owner", "owner.released.legacy"], retryable: false });
    expect(operation).not.toHaveBeenCalled();
    expect(storage.release).toHaveBeenCalledTimes(1);
    expect(meshLockSecondaryReleaseErrors(error)[0]).toMatchObject({ operationState: "not-started", operationCompleted: false });
  });

  it("mesh .lock: retirement/legacy SIBLINGS never block a free canonical lock (T6 slice)", async () => {
    const root = tempRoot();
    for (const sibling of [`r.${"b".repeat(64)}`, ".lock.released.0123", "custody.lock.released.89abcdef"]) {
      fs.mkdirSync(path.join(root, sibling), { recursive: true });
      fs.writeFileSync(path.join(root, sibling, "owner"), "foreign\n1\n1\n");
    }
    await expect(new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 500 }, () => undefined).withLock(() => "free")).resolves.toBe("free");
  });

  it("sync fence: both releases run (mesh, then custody) and both failures aggregate", () => {
    const root = tempRoot();
    storage.release.mockReturnValueOnce(refused).mockReturnValueOnce(unknown);
    const operation = vi.fn(() => 1);
    let caught: unknown;
    try { holdMeshFenceSync(root, 0, operation); } catch (error) { caught = error; }
    expect(operation).toHaveBeenCalledTimes(1);
    expect(storage.release.mock.calls.map(call => path.basename(call[0] as string))).toEqual([".lock", "custody.lock"]);
    expect(caught).toMatchObject({ lock: path.join(root, ".lock"), operationState: "returned", releaseOutcome: refused });
    expect((caught as InstanceType<typeof MeshLockReleaseError>).related[0]).toMatchObject({ lock: path.join(root, "custody.lock") });
  });

  it("sync fence: an operation throw is preserved and both locks are still released", () => {
    const root = tempRoot();
    storage.release.mockReturnValueOnce(unknown);
    const primary = new RangeError("op");
    let caught: unknown;
    try { holdMeshFenceSync(root, 0, () => { throw primary; }); } catch (error) { caught = error; }
    expect(caught).toBe(primary);
    expect(storage.release).toHaveBeenCalledTimes(2);
    expect(meshLockSecondaryReleaseErrors(primary)).toHaveLength(1);
  });

  it("sync fence: protocol-1 post-wx recheck refuses a replaced directory, releasing only the expected record", () => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    const write = fs.writeFileSync;
    let ours: string | undefined;
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string, options?: fs.WriteFileOptions) => {
      write(file, data, options);
      if (String(file) === path.join(lock, "owner") && ours === undefined) {
        ours = data;
        fs.renameSync(lock, `${lock}.moved`); // a recoverer took our directory; a successor publishes
        fs.mkdirSync(lock);
        write(path.join(lock, "owner"), "successor\n9\n1\n");
      }
    }) as typeof fs.writeFileSync);
    storage.release.mockReturnValue({ outcome: "not-owner" });
    const operation = vi.fn();
    expect(() => holdMeshFenceSync(root, 0, operation)).toThrow(expect.objectContaining({ code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST" }));
    expect(operation).not.toHaveBeenCalled();
    expect(storage.release.mock.calls[0]).toEqual([lock, ours, capability]);
    expect(path.basename(storage.release.mock.calls.at(-1)![0] as string)).toBe("custody.lock");
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe("successor\n9\n1\n");
  });

  it("custody: the returned release throws typed on failure and remains repeatable", async () => {
    const root = tempRoot();
    const release = await acquireMeshCustodyLock(root, 0, { ownIncarnation: undefined, lockProtocol: 1 });
    storage.release.mockReturnValueOnce(refused);
    expect(() => release()).toThrow(MeshLockReleaseError);
    storage.release.mockReturnValueOnce({ outcome: "released", cleanup: "pending", retiredPath: "r.y" });
    expect(() => release()).not.toThrow();
    expect(() => release()).not.toThrow();
    expect(storage.release).toHaveBeenCalledTimes(3);
    expect(new Set(storage.release.mock.calls.map(call => call[1]))).toHaveProperty("size", 1);
  });

  it("async fence: an operation throw keeps the primary and releases mesh AND custody", async () => {
    const root = tempRoot();
    storage.release.mockReturnValueOnce(refused).mockReturnValueOnce(refused);
    const primary = "not an error";
    const rejection = await holdMeshFence(root, { lockTimeoutMs: 500 }, () => { throw primary; }).catch((e: unknown) => e);
    expect(rejection).toBe(primary);
    expect(storage.release.mock.calls.map(call => path.basename(call[0] as string))).toEqual([".lock", "custody.lock"]);
    expect(warn).toHaveBeenCalledTimes(2);
  });
});
