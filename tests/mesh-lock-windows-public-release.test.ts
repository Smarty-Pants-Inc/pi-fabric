import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore } from "../src/mesh/store.js";
import { acquireMeshCustodyLock, withMeshCustody } from "../src/mesh/custody-lock.js";
import { holdMeshFence, holdMeshFenceSync } from "../src/mesh/fence-lock.js";
import {
  MeshLockPreEntryRefusedError, MeshLockReleaseError, meshLockSecondaryReleaseErrors,
} from "../src/mesh/lock-release.js";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, windowsMeshReleaseTestHooks,
  type WindowsMeshCapability, type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";

// Hosted-only reason: real Windows handle retirement, helper-cache privacy and native refusal are the proof.
// Keep reason (#9569): data safety and the native trust boundary through the PUBLIC lock routes/shared finalizer.
// No platform/helper-result mocks, explicit test children, signals, crash/cache-partition duplication, or local runs.
// The value import of verified-remove above also makes the AST product-deletion detector exclude this exact filename.
const hostedWindows = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const HOSTED_ONLY_REASON = "hosted Windows CI only: requires real NTFS sharing/retirement and the digest-pinned native helper";
const BUDGET_MS = 1_000;
type Protocol = 1 | 2;

interface Route {
  readonly name: string;
  /** Release order, innermost first. */
  readonly locks: readonly string[];
  readonly meshProtocol?: Protocol;
  readonly publication: "owner-write" | "directory-rename";
  run<T>(root: string, operation: () => T): Promise<T> | T;
}

const routes: Route[] = [];
for (const protocol of [1, 2] as const) {
  routes.push({
    name: `MeshLock protocol ${protocol}`, locks: [".lock"], meshProtocol: protocol,
    publication: protocol === 1 ? "owner-write" : "directory-rename",
    run: (root, operation) => new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: BUDGET_MS }, () => undefined).withLock(operation),
  });
  for (const mode of ["own", "dual"] as const) routes.push({
    name: `withMeshCustody ${mode}, protocol ${protocol}`,
    locks: mode === "own" ? ["custody.lock"] : [".lock", "custody.lock"],
    ...(mode === "dual" ? { meshProtocol: protocol } : {}),
    publication: mode === "dual" && protocol === 1 ? "owner-write" : "directory-rename",
    run: (root, operation) => {
      vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", mode);
      // Real MeshStore.exclusive in dual mode, not a stand-in that calls the operation without taking .lock.
      const mesh = new MeshStore(root, 64 * 1024, 500, { lockProtocol: protocol, lockTimeoutMs: BUDGET_MS });
      return withMeshCustody(mesh, operation, BUDGET_MS);
    },
  });
  routes.push({
    name: `holdMeshFence async, protocol ${protocol}`, locks: [".lock", "custody.lock"], meshProtocol: protocol,
    publication: protocol === 1 ? "owner-write" : "directory-rename",
    run: (root, operation) => holdMeshFence(root, { lockProtocol: protocol, lockTimeoutMs: BUDGET_MS }, async () => {
      await Promise.resolve(); // genuinely cross an await while both locks remain held
      return operation();
    }),
  });
}
routes.push({
  name: "holdMeshFenceSync (protocol-1 wire)", locks: [".lock", "custody.lock"], meshProtocol: 1,
  publication: "owner-write", run: (root, operation) => holdMeshFenceSync(root, 0, operation),
});

const ownerPath = (lock: string): string => path.join(lock, "owner");
const owner = (lock: string): string => fs.readFileSync(ownerPath(lock), "utf8");
const identity = (file: string) => {
  const stat = fs.lstatSync(file, { bigint: true });
  return { dev: stat.dev, ino: stat.ino, size: stat.size, mtimeNs: stat.mtimeNs };
};
const snapshot = (lock: string) => ({ directory: identity(lock), file: identity(ownerPath(lock)), record: owner(lock) });
type ProtectedSnapshot = ReturnType<typeof snapshot> & { debris: ReturnType<typeof identity> };
const debrisName = "owner.released.11111111-2222-4333-8444-555555555555";
const debrisBytes = "unjudged canonical debris: never delete on behalf of this holder\n";
const addDebris = (lock: string): ProtectedSnapshot => {
  fs.writeFileSync(path.join(lock, debrisName), debrisBytes, { flag: "wx" });
  return { ...snapshot(lock), debris: identity(path.join(lock, debrisName)) };
};
const assertProtected = (lock: string, before: ProtectedSnapshot): void => {
  expect(owner(lock)).toBe(before.record);
  expect(identity(ownerPath(lock))).toEqual(before.file);
  expect(identity(lock)).toEqual(before.directory);
  expect(identity(path.join(lock, debrisName))).toEqual(before.debris);
  expect(fs.readFileSync(path.join(lock, debrisName), "utf8")).toBe(debrisBytes);
  expect(fs.readdirSync(lock).sort()).toEqual(["owner", debrisName].sort());
};
const capture = async (operation: () => unknown): Promise<unknown> => {
  try { await operation(); } catch (error) { return error; }
  throw new Error("expected the public operation to throw");
};
const releaseErrors = (primary: unknown): MeshLockReleaseError[] => {
  const first = primary instanceof MeshLockReleaseError ? [primary, ...primary.related] : [];
  return [...first, ...meshLockSecondaryReleaseErrors(primary)];
};

/** Schedule only: delegate the REAL read unchanged, then add the child after W1 but before the native spawn.
 * No native result, filesystem observation or Windows predicate is fabricated. Each lock's own helper rehash
 * triggers its own addition, so even the second release of a nested fence reaches native W3 (not JS W1 refusal).
 */
const debrisAtNativeEntry = (capability: WindowsMeshCapability, locks: readonly string[]) => {
  const read = fs.readFileSync;
  const held = new Map<string, ProtectedSnapshot>();
  const pending = [...locks];
  const spy = vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
    const bytes = read(...args);
    if (String(args[0]) === capability.absolutePath && pending.length > 0) {
      const lock = pending.shift()!;
      held.set(lock, addDebris(lock));
    }
    return bytes;
  }) as typeof fs.readFileSync);
  return { held, restore: () => spy.mockRestore() };
};

/** Inject after actual exclusive publication, before the route's actual pre-entry check. */
const debrisAtPublication = (lock: string, publication: Route["publication"]) => {
  let held: ProtectedSnapshot | undefined;
  if (publication === "owner-write") {
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation(((...args: Parameters<typeof fs.writeFileSync>) => {
      write(...args);
      if (!held && String(args[0]) === ownerPath(lock)) held = addDebris(lock);
    }) as typeof fs.writeFileSync);
  } else {
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (!held && String(to) === lock) held = addDebris(lock);
    });
  }
  return () => {
    expect(held, "publication boundary was exercised").toBeDefined();
    return held!;
  };
};

describe.skipIf(!hostedWindows)(`Windows public lock release (#9569; ${HOSTED_ONLY_REASON})`, () => {
  let cacheFixture: string;
  let root: string;
  let capability: WindowsMeshCapability;
  let samples: WindowsMeshReleaseChildSample[];

  beforeAll(() => {
    // Same real cache preparation as the native suite; never install/compile a helper or use the user's cache.
    cacheFixture = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fwc-")));
    forgetWindowsMeshCapability();
    windowsMeshReleaseTestHooks.cacheRoot = path.join(cacheFixture, "cache");
    capability = prepareWindowsMeshCapability();
    const bootstrap = windowsHelperBootstrap();
    expect(bootstrap).toBeDefined();
    expect(capability.digest).toBe(bootstrap!.digest);
    expect(capability.absolutePath).toBe(path.join(fs.realpathSync.native(windowsMeshReleaseTestHooks.cacheRoot), bootstrap!.digest, "fabric-win-remove.exe"));
    expect(createHash("sha256").update(fs.readFileSync(capability.absolutePath)).digest("hex")).toBe(bootstrap!.digest);
  });
  beforeEach(() => {
    root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fwp-")));
    samples = [];
    windowsMeshReleaseTestHooks.afterChild = sample => { samples.push(sample); };
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    delete windowsMeshReleaseTestHooks.afterChild;
    // Production native children are synchronous, bounded and joined before any public API settles.
    if (root) fs.rmSync(root, { recursive: true, force: true });
  });
  afterAll(() => {
    delete windowsMeshReleaseTestHooks.cacheRoot;
    forgetWindowsMeshCapability();
    if (cacheFixture) fs.rmSync(cacheFixture, { recursive: true, force: true });
  });

  const assertNative = (locks: readonly string[], outcome = "released"): void => {
    expect(samples).toHaveLength(locks.length);
    for (const sample of samples) expect(sample).toMatchObject({
      absolutePath: capability.absolutePath, digest: capability.digest, outcome, attempt: 1,
    });
  };
  const assertClean = (locks: readonly string[]): void => {
    for (const lock of locks) expect(fs.existsSync(lock)).toBe(false);
    // Clean release also disposes its off-canonical retirement, not just the canonical owner.
    expect(fs.readdirSync(root).filter(name => name.startsWith("r.") || name.includes(".released.") || name.includes(".pending.") || name.startsWith("custody.lock.p."))).toEqual([]);
  };
  const assertHeld = (route: Route, locks: readonly string[]): void => {
    for (const lock of locks) {
      expect(fs.readdirSync(lock)).toEqual(["owner"]);
      const fields = owner(lock).split("\n");
      expect(fields[0]).toMatch(/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/);
      expect(fields[1]).toBe(String(process.pid));
      expect(owner(lock).endsWith("\n")).toBe(true);
      if (path.basename(lock) === ".lock" && route.meshProtocol === 1) expect(fields).toHaveLength(4);
    }
  };

  for (const route of routes) describe(route.name, () => {
    it("returns the exact value after clean native release; a second public section can enter", async () => {
      const locks = route.locks.map(base => path.join(root, base));
      const value = Object.freeze({ result: route.name });
      let calls = 0;
      expect(await route.run(root, () => { calls++; assertHeld(route, locks); return value; })).toBe(value);
      expect(calls).toBe(1);
      assertNative(locks);
      assertClean(locks);
      samples.length = 0;
      expect(await route.run(root, () => { assertHeld(route, locks); return value; })).toBe(value);
      assertNative(locks);
      assertClean(locks);
    });

    it("preserves an operation Error by identity while cleanly releasing every lock", async () => {
      const locks = route.locks.map(base => path.join(root, base));
      const primary = new RangeError("application failed after its side effect");
      let calls = 0;
      expect(await capture(() => route.run(root, () => { calls++; assertHeld(route, locks); throw primary; }))).toBe(primary);
      expect(calls).toBe(1);
      expect(meshLockSecondaryReleaseErrors(primary)).toEqual([]);
      assertNative(locks);
      assertClean(locks);
    });

    for (const state of ["returned", "threw"] as const) it(`native extra-child refusal after operation ${state}: no replay, proof preserved, same-receipt retry protects successor`, async () => {
      const locks = route.locks.map(base => path.join(root, base));
      // Exercise Error identity on MeshLock, and immutable non-Error throws on the nested/custody routes.
      const primary = route.name.startsWith("MeshLock") ? new Error(`application failure: ${route.name}`)
        : Object.freeze({ applicationFailure: route.name });
      const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      let scheduled: ReturnType<typeof debrisAtNativeEntry> | undefined;
      let calls = 0;
      const caught = await capture(() => route.run(root, () => {
        calls++;
        assertHeld(route, locks);
        scheduled = debrisAtNativeEntry(capability, locks);
        if (state === "threw") throw primary;
        return "application completed once";
      }));
      scheduled?.restore();
      expect(calls).toBe(1);
      expect(scheduled!.held.size).toBe(locks.length);
      if (state === "threw") {
        expect(caught).toBe(primary);
        expect(Object.keys(primary)).toEqual(primary instanceof Error ? [] : ["applicationFailure"]);
      } else {
        expect(caught).toBeInstanceOf(MeshLockReleaseError);
        expect(caught).toMatchObject({ operationState: "returned", operationCompleted: true, retryable: false });
      }
      const failures = releaseErrors(caught);
      expect(failures.map(error => error.lock)).toEqual(locks);
      for (const failure of failures) {
        expect(failure).toMatchObject({
          code: "FABRIC_MESH_LOCK_RELEASE_FAILED", releaseRetryable: true, retryable: false,
          // Exact native W3 reason: a JS-only precheck refusal must NOT pass this test.
          releaseOutcome: { outcome: "refused-before-retirement", reason: "owner: unexpected-entries" },
        });
        if (state === "threw") expect(failure).toMatchObject({ operationState: "threw", operationCompleted: false, cause: primary });
      }
      expect(warn).toHaveBeenCalledTimes(state === "threw" ? locks.length : meshLockSecondaryReleaseErrors(caught).length);
      assertNative(locks, "refused-before-retirement");
      for (const lock of locks) assertProtected(lock, scheduled!.held.get(lock)!);
      for (const lock of locks) fs.unlinkSync(path.join(lock, debrisName)); // fixture resolves ONLY the extra child it created
      samples.length = 0;
      for (const failure of failures) failure.retryRelease();
      assertNative(locks);
      assertClean(locks);
      expect(calls).toBe(1);

      samples.length = 0;
      await route.run(root, () => {
        const successor = locks.map(lock => snapshot(lock));
        for (const failure of failures) { failure.retryRelease(); failure.retryRelease(); }
        // W1 not-owner does not spawn or acquire a new helper, nor detach B's owner/directory.
        expect(samples).toEqual([]);
        locks.forEach((lock, index) => expect(snapshot(lock)).toEqual(successor[index]));
      });
      assertNative(locks);
      assertClean(locks);
      expect(calls).toBe(1);
    });

    it("pre-entry debris refuses without invoking the operation; outer finalization still releases custody", async () => {
      const locks = route.locks.map(base => path.join(root, base));
      const blocked = locks[0]!;
      const published = debrisAtPublication(blocked, route.publication);
      let calls = 0;
      const caught = await capture(() => route.run(root, () => { calls++; return "must not enter"; }));
      expect(calls).toBe(0);
      expect(caught).toBeInstanceOf(MeshLockPreEntryRefusedError);
      expect(caught).toMatchObject({ lock: blocked, reason: "unexpected-children", retryable: false });
      expect((caught as MeshLockPreEntryRefusedError).entries).toEqual(["owner", debrisName].sort());
      assertProtected(blocked, published());
      const [secondary] = meshLockSecondaryReleaseErrors(caught);
      expect(meshLockSecondaryReleaseErrors(caught)).toHaveLength(1);
      expect(secondary).toMatchObject({ lock: blocked, operationState: "not-started", operationCompleted: false,
        releaseOutcome: { outcome: "refused-before-retirement" } });
      const outer = locks.slice(1);
      assertNative(outer); // blocked lock refuses in W1; outer custody still reaches native release
      for (const lock of outer) expect(fs.existsSync(lock)).toBe(false);
      vi.restoreAllMocks();
      fs.unlinkSync(path.join(blocked, debrisName));
      secondary!.retryRelease();
      assertClean(locks);
    });

    it("legacy retirement/release siblings do not gate public entry and remain byte/identity intact", async () => {
      const siblings = [".lock.released.11111111-2222-4333-8444-555555555555", "custody.lock.released.1234abcd", `r.${"e".repeat(64)}`];
      const before = siblings.map(base => {
        const dir = path.join(root, base);
        fs.mkdirSync(dir);
        fs.writeFileSync(path.join(dir, "raw"), "unjudged sibling: keep\n");
        return { directory: identity(dir), file: identity(path.join(dir, "raw")) };
      });
      const locks = route.locks.map(base => path.join(root, base));
      expect(await route.run(root, () => { assertHeld(route, locks); return "entered"; })).toBe("entered");
      assertNative(locks);
      for (const lock of locks) expect(fs.existsSync(lock)).toBe(false);
      siblings.forEach((base, index) => {
        const dir = path.join(root, base);
        expect({ directory: identity(dir), file: identity(path.join(dir, "raw")) }).toEqual(before[index]);
        expect(fs.readFileSync(path.join(dir, "raw"), "utf8")).toBe("unjudged sibling: keep\n");
      });
      expect(fs.readdirSync(root).filter(name => name.startsWith("r.") || name.includes(".released.")).sort()).toEqual([...siblings].sort());
    });
  });

  for (const protocol of [1, 2] as const) describe(`host-qualified custody, protocol ${protocol}`, () => {
    for (const state of ["returned", "threw"] as const) it(`native refusal preserves the explicit ${state} release state and retry uses the full original receipt`, async () => {
      const lock = path.join(root, "custody.lock");
      const options = { hostQualified: true, lockProtocol: protocol };
      const release = await acquireMeshCustodyLock(root, BUDGET_MS, options);
      const scheduled = debrisAtNativeEntry(capability, [lock]);
      const caught = await capture(() => release(state));
      scheduled.restore();
      expect(caught).toBeInstanceOf(MeshLockReleaseError);
      expect(caught).toMatchObject({ operationState: state, operationCompleted: state === "returned", retryable: false,
        releaseOutcome: { outcome: "refused-before-retirement", reason: "owner: unexpected-entries" } });
      assertNative([lock], "refused-before-retirement");
      assertProtected(lock, scheduled.held.get(lock)!);
      expect(scheduled.held.get(lock)!.record.split("\n")).toHaveLength(7);
      fs.unlinkSync(path.join(lock, debrisName));
      samples.length = 0;
      const failure = caught as MeshLockReleaseError;
      failure.retryRelease();
      assertNative([lock]);
      assertClean([lock]);
      const releaseB = await acquireMeshCustodyLock(root, BUDGET_MS, options);
      try {
        const successor = snapshot(lock);
        samples.length = 0;
        failure.retryRelease(); release();
        expect(samples).toEqual([]);
        expect(snapshot(lock)).toEqual(successor);
      } finally { releaseB(); }
      assertNative([lock]);
      assertClean([lock]);
    });

    it("pre-entry debris protects the full host-qualified record and prevents handing out custody", async () => {
      const lock = path.join(root, "custody.lock");
      const published = debrisAtPublication(lock, "directory-rename");
      const caught = await capture(() => acquireMeshCustodyLock(root, BUDGET_MS, { hostQualified: true, lockProtocol: protocol }));
      expect(caught).toBeInstanceOf(MeshLockPreEntryRefusedError);
      expect(caught).toMatchObject({ reason: "unexpected-children", retryable: false });
      const held = published();
      expect(held.record.split("\n")).toHaveLength(7);
      assertProtected(lock, held);
      expect(samples).toEqual([]); // no native mutation of a known pre-entry child set
      const failures = meshLockSecondaryReleaseErrors(caught);
      expect(failures).toHaveLength(1);
      expect(failures[0]).toMatchObject({ operationState: "not-started", operationCompleted: false });
      vi.restoreAllMocks();
      fs.unlinkSync(path.join(lock, debrisName));
      failures[0]!.retryRelease();
      assertNative([lock]);
      assertClean([lock]);
    });
  });

  for (const hostQualified of [false, true]) for (const protocol of [1, 2] as const) {
    it(`acquireMeshCustodyLock protocol ${protocol}, hostQualified=${hostQualified}: repeatable receipt cannot delete a successor`, async () => {
      const lock = path.join(root, "custody.lock");
      const options = { hostQualified, lockProtocol: protocol };
      const releaseA = await acquireMeshCustodyLock(root, BUDGET_MS, options);
      const fields = owner(lock).split("\n");
      if (hostQualified) expect(fields).toHaveLength(7); // complete full host-qualified record, even if host evidence is unknown
      releaseA();
      assertNative([lock]);
      assertClean([lock]);
      const releaseB = await acquireMeshCustodyLock(root, BUDGET_MS, options);
      try {
        const successor = snapshot(lock);
        samples.length = 0;
        releaseA(); releaseA();
        expect(samples).toEqual([]);
        expect(snapshot(lock)).toEqual(successor);
        // Explicit nested public acquisition cannot enter while B remains owner.
        await expect(acquireMeshCustodyLock(root, 0, options)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        expect(snapshot(lock)).toEqual(successor);
      } finally { releaseB(); }
      assertNative([lock]);
      assertClean([lock]);
    });
  }
});
