import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { windowsMeshCacheFixture } from "./windows-mesh-cache-fixture.js";
// Deliberate VALUE import: the UNCHANGED local product-deletion detector excludes this hosted native suite.
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { MeshLockReleaseError } from "../src/mesh/lock-release.js";
import {
  encodeMeshRetireRequest, forgetWindowsMeshCapability, interpretMeshRetireResult, meshRetirementName,
  prepareWindowsMeshCapability, releaseWindowsMeshLock, windowsMeshReleaseTestHooks,
  type WindowsMeshCapability, type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";
import {
  encodeMeshOwnerRecoveryRequest, interpretMeshRecoveryResult, windowsMeshRecoveryFence, windowsMeshRecoveryTestHooks,
} from "../src/storage/windows-mesh-recovery.js";
import {
  ACCESS, SHARE, WindowsHandleHarness, proceedCheckpoint, readDispositions, waitCheckpoint,
  type ChildReceipt, type HeldHandle, type NativeRun,
} from "./fixtures/mesh-win32-handles.js";

// Keep reason (#9569 T8 / #13215): data safety at the REAL Win32 sharing/disposition boundary and public finalizers.
// No fabricated native result, death probe, platform predicate, filesystem return or application replay.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const HOSTED_ONLY_REASON = "hosted Windows CI only: real CreateFile access/share modes, pinned native helper and classic delete-pending semantics";
const NATIVE_DEADLINE_MS = 10_000;
const TEST_DEADLINE_MS = 30_000;
const bases = [".lock", "custody.lock"] as const;
type Base = typeof bases[number];
type Mode = "release" | "owner";
type Disposition = ReturnType<typeof readDispositions>[number];

const id = (file: string) => {
  const stat = fs.lstatSync(file, { bigint: true });
  return { dev: stat.dev, ino: stat.ino };
};
const proof = (lock: string) => {
  const directory = fs.lstatSync(lock, { bigint: true });
  const owner = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
  return {
    directory: { dev: directory.dev, ino: directory.ino, mtimeNs: directory.mtimeNs },
    owner: { dev: owner.dev, ino: owner.ino, size: owner.size, mtimeNs: owner.mtimeNs },
    record: fs.readFileSync(path.join(lock, "owner"), "utf8"),
  };
};
type Proof = ReturnType<typeof proof>;
const record = (pid = process.pid): string => `${randomUUID()}\n${pid}\n${Date.now()}\n`;
const publish = (root: string, base: Base, bytes: string): string => {
  const lock = path.join(root, base);
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), bytes, { flag: "wx" });
  return lock;
};
const errno = (error: unknown): string | undefined => (error as NodeJS.ErrnoException).code;
/** Unlike existsSync, an access-denied/delete-pending name is NOT reported as absent. */
const observation = (file: string): "present" | "absent" | "blocked" => {
  try { fs.lstatSync(file); return "present"; }
  catch (error) {
    if (errno(error) === "ENOENT" || errno(error) === "ENOTDIR") return "absent";
    if (errno(error) === "EACCES" || errno(error) === "EPERM") return "blocked";
    throw error;
  }
};
const assertAbsent = (file: string): void => { expect(observation(file), file).toBe("absent"); };
const assertProof = (lock: string, before: Proof): void => {
  expect(proof(lock)).toEqual(before);
  expect(fs.readdirSync(lock)).toEqual(["owner"]);
};
const hexId = (value: string): bigint => BigInt(/^0x/i.test(value) ? value : `0x${value}`);
const traceId = (entry: Pick<Disposition, "volume" | "index">) => ({ dev: hexId(entry.volume), ino: hexId(entry.index) });
const assertJoined = (receipt: ChildReceipt, code: number): void => {
  expect({ code: receipt.code, signal: receipt.signal, error: receipt.error, stderr: receipt.stderr }).toEqual({ code, signal: null, error: undefined, stderr: "" });
};
const interpreted = (run: NativeRun, receipt: ChildReceipt, retired: string) => interpretMeshRetireResult({
  status: receipt.code, signal: receipt.signal, stdout: receipt.stdout, pid: run.child.pid,
}, retired);
const recovered = (run: NativeRun, receipt: ChildReceipt) => interpretMeshRecoveryResult({
  status: receipt.code, signal: receipt.signal, stdout: receipt.stdout, pid: run.child.pid,
}, "owner");

/** Construct ONCE from the ORIGINAL bigint S / full R, not from a successor after a refused/unknown call. */
const request = (lock: string, original: string, mode: Mode) => {
  const directory = fs.lstatSync(lock, { bigint: true });
  const owner = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
  const retired = path.join(path.dirname(lock), meshRetirementName(path.basename(lock), directory.dev, directory.ino, Buffer.from(original)));
  const fence = windowsMeshRecoveryFence(lock, directory, original);
  const message = mode === "owner" ? encodeMeshOwnerRecoveryRequest(lock, original, directory, owner, NATIVE_DEADLINE_MS)
    : encodeMeshRetireRequest({
      lockPath: path.toNamespacedPath(lock), retiredPath: path.toNamespacedPath(retired),
      dirVolume: directory.dev, dirIndex: directory.ino, ownerVolume: owner.dev, ownerIndex: owner.ino,
      ownerFileTime: owner.mtimeNs / 100n + 116_444_736_000_000_000n, ownerSize: owner.size,
      record: Buffer.from(original), deadlineMs: NATIVE_DEADLINE_MS,
    });
  return { directory, owner, retired, fence, message };
};

/** Every trace row describes a REAL syscall. In forced mode the skipped Ex call must NOT be invented in the trace. */
const trace = (gate: string, pid?: number): Disposition[] => {
  const rows = readDispositions(gate);
  expect(rows.length, "missing trace is not evidence of classic disposition").toBeGreaterThan(0);
  expect(rows.length).toBeLessThanOrEqual(64);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]!;
    expect(Number.isSafeInteger(row.pid) && row.pid > 0).toBe(true);
    expect(row.pid).toBe(pid ?? rows[0]!.pid);
    expect(Number.isSafeInteger(row.sequence) && row.sequence >= 0).toBe(true);
    if (i > 0) expect(row.sequence).toBeGreaterThan(rows[i - 1]!.sequence);
    expect(Number.isSafeInteger(row.error) && row.error >= 0).toBe(true);
    if (!row.ok) expect(row.error).toBeGreaterThan(0);
    expect(row.volume).toMatch(/^(?:0x)?[0-9a-f]+$/i);
    expect(row.index).toMatch(/^(?:0x)?[0-9a-f]+$/i);
  }
  return rows;
};
const assertCall = (row: Disposition, api: "classic" | "extended", directory: boolean, expectedId: { dev: bigint; ino: bigint }, ok: boolean): void => {
  expect({ api: row.api, forced: row.forced, directory: row.directory, ok: row.ok }).toEqual({ api, forced: api === "classic", directory, ok });
  expect(traceId(row)).toEqual({ dev: expectedId.dev, ino: expectedId.ino });
};
const assertCleanTrace = (gate: string, before: Proof, api: "classic" | "extended", pid?: number): Disposition[] => {
  const rows = trace(gate, pid);
  expect(rows).toHaveLength(2);
  assertCall(rows[0]!, api, false, before.owner, true);
  assertCall(rows[1]!, api, true, before.directory, true);
  return rows;
};

/** The fixture's terminal JSON reports its actual post-CloseHandle census, not merely an EOF acknowledgement. */
const closeHandle = async (held: HeldHandle): Promise<ChildReceipt> => {
  const receipt = await held.close(); // terminal join FIRST; assertions cannot strand the child
  assertJoined(receipt, 0);
  expect(await held.done).toEqual(receipt);
  const rows: unknown[] = receipt.stdout.trim().split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line) as unknown);
  expect(rows).toHaveLength(2);
  const terminal = rows.find((row): row is { event: "closed"; pid: number; baselineHandles: number; heldHandles: number; closedHandles: number } =>
    typeof row === "object" && row !== null && "event" in row && row.event === "closed");
  expect(terminal, "fixture must report actual baseline/open/closed handle census").toBeDefined();
  expect(terminal!.pid).toBe(held.pid);
  expect(terminal!.baselineHandles).toBe(held.ready.baselineHandles);
  expect(terminal!.heldHandles).toBe(held.ready.heldHandles);
  expect(terminal!.closedHandles).toBe(held.ready.baselineHandles);
  return receipt;
};
const assertHeld = (held: HeldHandle, file: string, access: number, share: number, directory: boolean): void => {
  expect(held.pid).toBe(held.ready.pid);
  expect(held.ready).toMatchObject({ access, share, directory });
  expect(traceId(held.ready)).toEqual(id(file));
  expect(Number.isSafeInteger(held.ready.baselineHandles) && held.ready.baselineHandles > 0).toBe(true);
  expect(held.ready.heldHandles).toBe(held.ready.baselineHandles + 1);
};

// Omit DELETE sharing in both rows. Omitting WRITE alone is NOT claimed to conflict with W3's read-only access.
const ownerShares = [
  { name: "READ/WRITE but no DELETE sharing", share: SHARE.READ | SHARE.WRITE },
  { name: "READ only (no WRITE or DELETE sharing)", share: SHARE.READ },
] as const;
const directoryHandles = [
  { name: "READ_ATTRIBUTES without DELETE sharing", access: ACCESS.READ_ATTRIBUTES, share: SHARE.READ | SHARE.WRITE },
  { name: "DELETE access conflicts with the guard's no-delete-share", access: ACCESS.READ_ATTRIBUTES | ACCESS.DELETE, share: SHARE.ALL },
] as const;

describe.skipIf(!hosted)(`Windows T8 classic disposition / external sharing (${HOSTED_ONLY_REASON})`, () => {
  let root: string | undefined;
  let cacheFixture: string | undefined;
  let locks: string;
  let cap: WindowsMeshCapability;
  let harness: WindowsHandleHarness | undefined;
  let harnessCreation: Promise<WindowsHandleHarness> | undefined;
  let gateNumber: number;
  let samples: WindowsMeshReleaseChildSample[];
  let savedHooks: typeof windowsMeshReleaseTestHooks;
  let savedRecoveryHooks: typeof windowsMeshRecoveryTestHooks;

  const gate = (): string => {
    const dir = path.join(root!, `g${gateNumber++}`);
    fs.mkdirSync(dir);
    return dir;
  };
  const handles = (): WindowsHandleHarness => {
    if (!harness) throw new Error("fixture harness was not established");
    return harness;
  };
  /** Checkpoint arrival loses immediately to a terminal early exit; no 8s wait after an already-refused helper. */
  const paused = async (run: NativeRun, dir: string, name: string): Promise<void> => {
    await Promise.race([
      waitCheckpoint(dir, name),
      run.done.then(receipt => { throw new Error(`helper exited before ${name}: ${JSON.stringify(receipt)}`); }),
    ]);
  };
  // Complete, actually dead fixture PID; no fake process.kill/probe or guessed PID is used for C's fixture records.
  const deadRecord = async (): Promise<string> => {
    const seed = path.join(root!, `pid-${randomUUID()}`);
    fs.writeFileSync(seed, "owned fixture PID producer\n", { flag: "wx" });
    const held = await handles().hold(seed, ACCESS.READ_DATA, SHARE.ALL);
    assertHeld(held, seed, ACCESS.READ_DATA, SHARE.ALL, false);
    await closeHandle(held);
    return record(held.pid);
  };
  const note = (row: string, value: unknown): void => { console.info(`T8 ${row} ${JSON.stringify(value)}`); };

  beforeEach(async () => {
    root = undefined;
    cacheFixture = undefined;
    harness = undefined;
    harnessCreation = undefined;
    gateNumber = 0;
    samples = [];
    savedHooks = { ...windowsMeshReleaseTestHooks };
    savedRecoveryHooks = { ...windowsMeshRecoveryTestHooks };
    for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
    for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
    root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fcsl-")));
    locks = path.join(root, "locks");
    fs.mkdirSync(locks);
    const fixtureRoot = path.join(root, "handles");
    fs.mkdirSync(fixtureRoot);
    forgetWindowsMeshCapability();
    // Register the exact unique profile allocation BEFORE preparation can publish any cache bytes.
    // Locks/gates/external fixture and Zig temps stay under os.tmpdir(); the production default cache is untouched.
    cacheFixture = windowsMeshCacheFixture("fcsl-cache-");
    windowsMeshReleaseTestHooks.cacheRoot = path.join(cacheFixture, "cache");
    windowsMeshReleaseTestHooks.deadlineMs = NATIVE_DEADLINE_MS;
    windowsMeshReleaseTestHooks.afterChild = sample => { samples.push(sample); };
    const boot = windowsHelperBootstrap();
    expect(boot).toBeDefined();
    cap = prepareWindowsMeshCapability(); // actual digest/private-cache/capability handshake, no substitute executable
    expect(cap.digest).toBe(boot!.digest);
    harnessCreation = WindowsHandleHarness.create(fixtureRoot); // retain ownership even if setup times out while compiling
    harness = await harnessCreation;
  }, 180_000); // bounded 120s external-fixture compile plus synchronous, bounded capability setup

  afterEach(async () => {
    // FIRST close/join ALL externally held handles and native children, even after an assertion/checkpoint failure.
    // No fixture deletion in finally: a failed join deliberately preserves its root rather than racing a deleter.
    let receipts: ChildReceipt[] | undefined;
    try {
      // create() joins its compiler before rejecting. A timed-out setup may still have a pending creation promise.
      const owned = harness ?? await harnessCreation?.catch(() => undefined);
      receipts = await owned?.close();
    }
    finally {
      for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
      Object.assign(windowsMeshReleaseTestHooks, savedHooks);
      for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
      Object.assign(windowsMeshRecoveryTestHooks, savedRecoveryHooks);
      forgetWindowsMeshCapability();
    }
    note("terminal-custody", receipts ?? []);
    const cleanup = [
      { dir: root, prefix: "fcsl-" },
      { dir: cacheFixture, prefix: "fcsl-cache-" },
    ];
    // Validate EVERY owned allocation before cleaning ANY; never clean a default/shared cache.
    for (const { dir, prefix } of cleanup) if (dir && !path.basename(dir).startsWith(prefix)) throw new Error("unsafe owned fixture cleanup");
    note("owned-residue-before-cleanup", cleanup.filter(entry => entry.dir).map(({ dir }) => ({
      root: dir!, entries: fs.existsSync(dir!) ? fs.readdirSync(dir!) : [],
    })));
    for (const { dir } of cleanup) if (dir) {
      fs.rmSync(dir, { recursive: true, force: true }); // strictly after all owned compiler/handle/native terminal joins
      expect(fs.existsSync(dir)).toBe(false);
    }
    harness = undefined;
    harnessCreation = undefined;
    root = undefined;
    cacheFixture = undefined;
  }, 150_000); // can join a pending 120s compiler after a setup timeout; no cleanup if a join fails

  for (const base of bases) {
    for (const selection of ["classic", "trace"] as const) {
      it(`1W/7 ${base}: ${selection === "classic" ? "forces real classic owner and empty-T cleanup" : "ordinary extended-success counterexample"}`, async () => {
        const a = record(), lock = publish(locks, base, a), before = proof(lock), dir = gate();
        const original = request(lock, a, "release");
        // Precreated command is consumed by the EXISTING checkpoint, not an environment/CLI force switch.
        proceedCheckpoint(dir, selection);
        const run = handles().native(cap, "release", original.message, { name: "before-owner-dispose", dir });
        const receipt = await run.done;
        assertJoined(receipt, 0);
        expect(receipt.stdout).toBe("FMR1 released cleanup clean\n");
        expect(interpreted(run, receipt, original.retired)).toEqual({ outcome: "released", cleanup: "clean", retiredPath: original.retired });
        assertCleanTrace(dir, before, selection === "classic" ? "classic" : "extended", run.child.pid);
        assertAbsent(lock);
        assertAbsent(original.retired);
        expect(fs.readdirSync(locks)).toEqual([]);
        note(`${base}-${selection}-clean`, { receipt, dispositions: readDispositions(dir), residue: [] });
      }, TEST_DEADLINE_MS);
    }

    it(`2/7 ${base}: compatible external owner reader opens AFTER W4, spans classic disposition, never reaches B`, async () => {
      const a = record(), lock = publish(locks, base, a), before = proof(lock), dir = gate();
      const original = request(lock, a, "release");
      const run = handles().native(cap, "release", original.message, { name: "after-commit", dir });
      await paused(run, dir, "after-commit");
      assertAbsent(lock); // W4 is real; opening before W4 can itself deny parent rename on Windows
      assertProof(original.retired, before);
      const ownerPath = path.join(original.retired, "owner");
      const held = await handles().hold(ownerPath, ACCESS.READ_DATA, SHARE.ALL);
      assertHeld(held, ownerPath, ACCESS.READ_DATA, SHARE.ALL, false);
      const b = record();
      publish(locks, base, b); // B publishes ONLY after W4, while A's exact original S/R request is paused off-canonical
      const successor = proof(lock);
      proceedCheckpoint(dir, "classic");
      const receipt = await run.done;
      assertJoined(receipt, 0);
      const rows = trace(dir, run.child.pid);
      expect(rows).toHaveLength(2);
      assertCall(rows[0]!, "classic", false, before.owner, true);
      expect(traceId(rows[0]!)).toEqual(traceId(held.ready));
      expect({ api: rows[1]!.api, forced: rows[1]!.forced, directory: rows[1]!.directory }).toEqual({ api: "classic", forced: true, directory: true });
      expect(traceId(rows[1]!)).toEqual({ dev: before.directory.dev, ino: before.directory.ino });
      // Narrow, syscall-backed classification: failed directory disposition is permitted ONLY for real nonempty T.
      if (rows[1]!.ok) {
        expect(receipt.stdout).toBe("FMR1 released cleanup clean\n");
        expect(interpreted(run, receipt, original.retired)).toMatchObject({ outcome: "released", cleanup: "clean" });
      } else {
        expect(rows[1]!.error).toBe(145); // ERROR_DIR_NOT_EMPTY: owner is classic-delete-pending until last reader closes
        expect(receipt.stdout).toBe("FMR1 released cleanup pending-directory-not-empty\n");
        expect(interpreted(run, receipt, original.retired)).toMatchObject({ outcome: "released", cleanup: "pending" });
        expect(id(original.retired)).toEqual({ dev: before.directory.dev, ino: before.directory.ino });
      }
      assertProof(lock, successor);
      const during = { retired: observation(original.retired), owner: observation(ownerPath) };
      const closed = await closeHandle(held);
      assertAbsent(ownerPath); // must follow actual last-close result, not just trust the helper's cleanup label
      if (rows[1]!.ok) assertAbsent(original.retired);
      else {
        expect(id(original.retired)).toEqual({ dev: before.directory.dev, ino: before.directory.ino });
        expect(fs.readdirSync(original.retired)).toEqual([]); // no auto cleaner; failed empty-dir disposal leaves only empty T
      }
      assertProof(lock, successor); // complete UUID/pid/time and both exact ids after helper AND fixture close
      expect(releaseWindowsMeshLock(lock, a, cap)).toEqual({ outcome: "not-owner" });
      assertProof(lock, successor);
      note(`${base}-owner-classic-pending`, { access: held.ready, receipt, closed, dispositions: rows, during,
        afterClose: { retired: observation(original.retired), owner: observation(ownerPath) }, classification: rows[1]!.ok ? "classic-last-close-clean" : "classic-owner-pending-then-empty-T" });
    }, TEST_DEADLINE_MS);

    it(`4/7 ${base}: compatible READ_ATTRIBUTES/SHARE_ALL directory handle opens after W4 and isolates pending T from B`, async () => {
      const a = record(), lock = publish(locks, base, a), before = proof(lock), dir = gate();
      const original = request(lock, a, "release");
      const run = handles().native(cap, "release", original.message, { name: "after-commit", dir });
      await paused(run, dir, "after-commit");
      assertAbsent(lock);
      const held = await handles().hold(original.retired, ACCESS.READ_ATTRIBUTES, SHARE.ALL);
      assertHeld(held, original.retired, ACCESS.READ_ATTRIBUTES, SHARE.ALL, true);
      publish(locks, base, record());
      const successor = proof(lock);
      proceedCheckpoint(dir, "classic");
      const receipt = await run.done;
      assertJoined(receipt, 0);
      expect(receipt.stdout).toBe("FMR1 released cleanup clean\n");
      const rows = assertCleanTrace(dir, before, "classic", run.child.pid);
      expect(traceId(rows[1]!)).toEqual(traceId(held.ready));
      assertProof(lock, successor);
      const during = observation(original.retired); // preserve actual inaccessible vs present vs already-unlinked classification
      const closed = await closeHandle(held);
      assertAbsent(original.retired);
      expect(fs.readdirSync(locks)).toEqual([base]);
      assertProof(lock, successor);
      expect(releaseWindowsMeshLock(lock, a, cap)).toEqual({ outcome: "not-owner" });
      assertProof(lock, successor);
      note(`${base}-directory-classic-pending`, { access: held.ready, receipt, closed, dispositions: rows, during, afterClose: observation(original.retired) });
    }, TEST_DEADLINE_MS);

    for (const mode of ["release", "owner"] as const) {
      for (const sharing of ownerShares) {
        it(`3W/C ${base} ${mode}: external owner ${sharing.name} refuses before W4; joined close permits explicit same-S/R retry`, async () => {
          const a = mode === "owner" ? await deadRecord() : record();
          const lock = publish(locks, base, a), before = proof(lock), original = request(lock, a, mode);
          const held = await handles().hold(path.join(lock, "owner"), ACCESS.READ_DATA, sharing.share);
          assertHeld(held, path.join(lock, "owner"), ACCESS.READ_DATA, sharing.share, false);
          const first = handles().native(cap, mode, original.message);
          const refused = await first.done; // original helper is terminal BEFORE any retry or fixture close
          assertJoined(refused, 3);
          // Do not assume the path: an external child handle can prevent directory rename even with compatible reads.
          expect(refused.stdout).toMatch(/^FMR1 refused (?:owner owner-not-held|commit rename-denied)\n$/);
          if (mode === "release") expect(interpreted(first, refused, original.retired).outcome).toBe("refused-before-retirement");
          else expect(recovered(first, refused).outcome).toBe("refused");
          assertProof(lock, before);
          assertAbsent(original.retired);
          assertAbsent(original.fence);
          expect(fs.readdirSync(locks)).toEqual([base]); // no F/T or source replacement
          const closed = await closeHandle(held);
          assertProof(lock, before);
          await retryOriginal(mode, lock, before, original);
          note(`${base}-${mode}-owner-sharing-retry`, { access: held.ready, refused, closed });
        }, TEST_DEADLINE_MS);
      }

      for (const external of directoryHandles) {
        it(`4 ${base} ${mode}: pre-guard ${external.name} refuses without mutation, then succeeds after close/join`, async () => {
          const a = mode === "owner" ? await deadRecord() : record();
          const lock = publish(locks, base, a), before = proof(lock), original = request(lock, a, mode);
          const held = await handles().hold(lock, external.access, external.share);
          assertHeld(held, lock, external.access, external.share, true);
          const first = handles().native(cap, mode, original.message);
          const refused = await first.done;
          assertJoined(refused, 3);
          expect(refused.stdout).toBe("FMR1 refused guard directory-not-held\n");
          if (mode === "release") expect(interpreted(first, refused, original.retired)).toEqual({ outcome: "refused-before-retirement", reason: "guard: directory-not-held" });
          else expect(recovered(first, refused)).toEqual({ outcome: "refused", reason: "guard: directory-not-held" });
          assertProof(lock, before);
          expect(fs.readdirSync(locks)).toEqual([base]);
          assertAbsent(original.retired);
          assertAbsent(original.fence);
          const closed = await closeHandle(held);
          assertProof(lock, before);
          await retryOriginal(mode, lock, before, original);
          note(`${base}-${mode}-directory-sharing-retry`, { access: held.ready, refused, closed });
        }, TEST_DEADLINE_MS);
      }
    }
  }

  /** No resnapshot substitutes for the first call's S/R. C permanently retains its exact original F/owner. */
  async function retryOriginal(mode: Mode, lock: string, before: Proof, original: ReturnType<typeof request>): Promise<void> {
    const dir = gate();
    proceedCheckpoint(dir, "classic");
    const run = handles().native(cap, mode, original.message, { name: mode === "release" ? "before-owner-dispose" : "before-commit", dir });
    const receipt = await run.done;
    assertJoined(receipt, 0);
    assertAbsent(lock);
    if (mode === "release") {
      expect(receipt.stdout).toBe("FMR1 released cleanup clean\n");
      expect(interpreted(run, receipt, original.retired)).toEqual({ outcome: "released", cleanup: "clean", retiredPath: original.retired });
      assertCleanTrace(dir, before, "classic", run.child.pid);
      assertAbsent(original.retired);
      assertAbsent(original.fence);
    } else {
      expect(receipt.stdout).toBe("FMREC1 recovered commit retained\n");
      expect(recovered(run, receipt)).toEqual({ outcome: "recovered" });
      assertProof(original.fence, before);
      assertAbsent(original.retired);
      // C MUST NOT fall through to disposal, even after accepting the classic command at its existing checkpoint.
      const traceFile = path.join(dir, "disposition.jsonl");
      if (observation(traceFile) !== "absent") expect(readDispositions(dir)).toEqual([]);
    }
    const base = path.basename(lock) as Base;
    publish(locks, base, record());
    const successor = proof(lock);
    const stale = handles().native(cap, mode, original.message);
    const staleReceipt = await stale.done;
    if (mode === "release") {
      assertJoined(staleReceipt, 5);
      expect(interpreted(stale, staleReceipt, original.retired)).toEqual({ outcome: "not-owner" });
    } else {
      assertJoined(staleReceipt, 5);
      expect(recovered(stale, staleReceipt).outcome).toBe("refused");
      assertProof(original.fence, before);
    }
    assertProof(lock, successor);
    expect(releaseWindowsMeshLock(lock, successor.record, cap)).toMatchObject({ outcome: "released", cleanup: "clean" });
    assertAbsent(lock);
    if (mode === "owner") assertProof(original.fence, before); // B's cleanup does not collect F either
    note(`${base}-${mode}-same-S-R`, { receipt, staleReceipt, dispositions: mode === "release" ? readDispositions(dir) : [], retainedF: mode === "owner" });
  }

  // PUBLIC routes: synchronous release cannot parent-await a paused native checkpoint. Precreate go instead.
  // These use the real finalizer/captured capability; native-only success is not substituted for API evidence.
  for (const domain of ["mesh", "custody"] as const) {
    it(`7 public ${domain}: actual joined-dead owner takes C, retains full original F/owner, then B releases through real classic`, async () => {
      const base: Base = domain === "mesh" ? ".lock" : "custody.lock";
      const a = await deadRecord(), lock = publish(locks, base, a), before = proof(lock), original = request(lock, a, "owner");
      const recoverySamples: unknown[] = [];
      windowsMeshRecoveryTestHooks.afterChild = sample => { recoverySamples.push(sample); };
      const dir = gate();
      proceedCheckpoint(dir, "classic");
      windowsMeshReleaseTestHooks.checkpoint = { name: "before-owner-dispose", dir };
      let successor: Proof | undefined;
      let calls = 0;
      const entered = (): string => {
        calls++;
        successor = proof(lock);
        expect(successor.record).not.toBe(a);
        assertProof(original.fence, before);
        return "new holder after admissible C";
      };
      if (domain === "mesh") {
        const mesh = new MeshLock(locks, { lockProtocol: 1, lockTimeoutMs: 4000 }, () => undefined);
        expect(await mesh.withLock(entered)).toBe("new holder after admissible C");
      } else {
        const release = await acquireMeshCustodyLock(locks, 4000);
        try { expect(entered()).toBe("new holder after admissible C"); } finally { release("returned"); }
      }
      expect(calls).toBe(1);
      expect(recoverySamples).toEqual([{ absolutePath: cap.absolutePath, digest: cap.digest, mode: "owner", outcome: "recovered" }]);
      expect(samples).toEqual([expect.objectContaining({ absolutePath: cap.absolutePath, digest: cap.digest, outcome: "released", attempt: 1 })]);
      const rows = assertCleanTrace(dir, successor!, "classic"); // these are B's objects; C must NEVER dispose A's F
      assertProof(original.fence, before);
      assertAbsent(lock);
      assertAbsent(original.retired);
      note(`public-${domain}-C-then-classic-W`, { recoverySamples, dispositions: rows, callbackCalls: calls, retainedF: original.fence });
    }, TEST_DEADLINE_MS);

    it(`1W/7 public ${domain}: precreated checkpoint forces classic, callback/result or returned release is preserved, B is independent`, async () => {
      const base: Base = domain === "mesh" ? ".lock" : "custody.lock";
      const lock = path.join(locks, base), dir = gate();
      let before: Proof | undefined;
      let calls = 0;
      const value = Object.freeze({ domain, once: true });
      proceedCheckpoint(dir, "classic");
      windowsMeshReleaseTestHooks.checkpoint = { name: "before-owner-dispose", dir };
      if (domain === "mesh") {
        const mesh = new MeshLock(locks, { lockProtocol: 1, lockTimeoutMs: 1000 }, () => undefined);
        expect(await mesh.withLock(() => { calls++; before = proof(lock); return value; })).toBe(value);
      } else {
        const release = await acquireMeshCustodyLock(locks, 1000);
        calls++;
        before = proof(lock);
        release("returned");
      }
      expect(calls).toBe(1);
      expect(before).toBeDefined();
      expect(before!.record.split("\n")[0]).toMatch(/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i);
      expect(before!.record.split("\n")[1]).toBe(String(process.pid));
      expect(samples).toEqual([expect.objectContaining({ absolutePath: cap.absolutePath, digest: cap.digest, outcome: "released", attempt: 1 })]);
      const rows = assertCleanTrace(dir, before!, "classic");
      expect(fs.readFileSync(path.join(dir, "before-owner-dispose.paused"), "utf8").trim()).toBe(String(rows[0]!.pid));
      assertAbsent(lock);
      expect(fs.readdirSync(locks).filter(name => name === base || name.startsWith("r.") || name.startsWith(`${base}.dead.`))).toEqual([]);
      delete windowsMeshReleaseTestHooks.checkpoint;
      await publicSuccessor(domain, locks, before!.record, () => { expect(calls).toBe(1); });
      note(`public-${domain}-classic`, { dispositions: rows, samples, callbackCalls: calls });
    }, TEST_DEADLINE_MS);

    it(`3W/7 public ${domain}: incompatible real owner handle reports a release-only error, no replay; terminal close permits retry`, async () => {
      const base: Base = domain === "mesh" ? ".lock" : "custody.lock";
      const lock = path.join(locks, base);
      let held: HeldHandle | undefined;
      let before: Proof | undefined;
      let calls = 0;
      let failure: unknown;
      if (domain === "mesh") {
        const mesh = new MeshLock(locks, { lockProtocol: 1, lockTimeoutMs: 1000 }, () => undefined);
        try {
          await mesh.withLockAcrossAwait(async () => {
            calls++;
            before = proof(lock);
            held = await handles().hold(path.join(lock, "owner"), ACCESS.READ_DATA, SHARE.READ);
            assertHeld(held, path.join(lock, "owner"), ACCESS.READ_DATA, SHARE.READ, false);
            return "application result was produced exactly once";
          });
        } catch (error) { failure = error; }
      } else {
        const release = await acquireMeshCustodyLock(locks, 1000);
        calls++;
        before = proof(lock);
        held = await handles().hold(path.join(lock, "owner"), ACCESS.READ_DATA, SHARE.READ);
        assertHeld(held, path.join(lock, "owner"), ACCESS.READ_DATA, SHARE.READ, false);
        try { release("returned"); } catch (error) { failure = error; }
      }
      expect(failure).toBeInstanceOf(MeshLockReleaseError);
      const releaseError = failure as MeshLockReleaseError;
      expect(releaseError).toMatchObject({ operationState: "returned", operationCompleted: true, retryable: false,
        releaseRetryable: true, releaseOutcome: { outcome: "refused-before-retirement" } });
      expect(releaseError.releaseOutcome).toMatchObject({ reason: expect.stringMatching(/^(?:owner: owner-not-held|commit: rename-denied)$/) });
      expect(calls).toBe(1);
      expect(samples).toEqual([expect.objectContaining({ outcome: "refused-before-retirement", attempt: 1, digest: cap.digest })]);
      assertProof(lock, before!);
      // Public MeshLock also writes lock-stats; inspect the lock namespace, not unrelated telemetry.
      expect(fs.readdirSync(locks).filter(name => name === base || name.startsWith("r.") || name.startsWith(`${base}.dead.`) || name.startsWith(`${base}.released.`))).toEqual([base]);
      const closed = await closeHandle(held!); // the original sync helper has ALREADY joined before API throws
      assertProof(lock, before!);
      const dir = gate();
      proceedCheckpoint(dir, "classic");
      windowsMeshReleaseTestHooks.checkpoint = { name: "before-owner-dispose", dir };
      releaseError.retryRelease(); // release ONLY; never re-enter the application callback
      assertCleanTrace(dir, before!, "classic");
      assertAbsent(lock);
      expect(calls).toBe(1);
      expect(samples.map(sample => sample.outcome)).toEqual(["refused-before-retirement", "released"]);
      delete windowsMeshReleaseTestHooks.checkpoint;
      await publicSuccessor(domain, locks, before!.record, () => {
        const count = samples.length;
        releaseError.retryRelease();
        expect(samples).toHaveLength(count); // not-owner from full original R, no new native child against B
        expect(calls).toBe(1);
      });
      note(`public-${domain}-refusal-retry`, { access: held!.ready, closed, dispositions: readDispositions(dir), samples, callbackCalls: calls });
    }, TEST_DEADLINE_MS);
  }

  async function publicSuccessor(domain: "mesh" | "custody", root: string, original: string, check: () => void): Promise<void> {
    const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
    const verify = (): void => {
      const successor = proof(lock);
      expect(successor.record).not.toBe(original);
      check();
      expect(releaseWindowsMeshLock(lock, original, cap)).toEqual({ outcome: "not-owner" });
      assertProof(lock, successor);
    };
    if (domain === "mesh") {
      const mesh = new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 1000 }, () => undefined);
      await mesh.withLock(verify);
    } else {
      const release = await acquireMeshCustodyLock(root, 1000);
      try { verify(); } finally { release(); }
    }
    assertAbsent(lock);
  }
});
