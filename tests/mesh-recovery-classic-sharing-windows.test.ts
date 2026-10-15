import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { windowsMeshCacheFixture } from "./windows-mesh-cache-fixture.js";
// Deliberate VALUE import of the real bootstrap; explicit win32 + GITHUB_ACTIONS gating below controls execution.
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import {
  captureMeshLegacyDebris, judgeMeshLegacyDebris, readMeshRecoveryOwner,
} from "../src/mesh/legacy-debris.js";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, windowsMeshReleaseTestHooks,
  type WindowsMeshCapability, type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";
import {
  encodeMeshDebrisRecoveryRequest, interpretMeshRecoveryResult, windowsMeshRecoveryTestHooks,
  type WindowsMeshRecoveryEntry,
} from "../src/storage/windows-mesh-recovery.js";
import {
  ACCESS, SHARE, WindowsHandleHarness, proceedCheckpoint, readDispositions, waitCheckpoint,
  type ChildReceipt, type HeldHandle, type NativeRun,
} from "./fixtures/mesh-win32-handles.js";

// Expected skip, NOT NTFS evidence: execution and fixture compilation require actual hosted Windows.
// Keep: data safety across public E/D and native ALL-before-ANY/classic/delete-pending boundaries (#9569 T8).
// No platform/death/native-result mocks, Bun/.ts children, application replay, or production changes.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const SKIP = "actual NTFS external handles and digest-pinned classic disposition require hosted Windows";
const NATIVE_MS = 10_000;
const REFUSAL_MS = 400; // >= MeshLock's 100ms minimum; setup occurs before the first-attempt deadline.
type Domain = "mesh" | "custody";
type RecoverySample = { absolutePath: string; digest: string; mode: "owner" | "debris"; outcome: string };
const roots: string[] = [];
const temp = (): string => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fcsr-")));
  roots.push(root);
  return root;
};
const lockAt = (root: string, domain: Domain): string => path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
const stat = (file: string): fs.BigIntStats => fs.lstatSync(file, { bigint: true });
const id = (file: string): string => { const s = stat(file); return `${s.dev}:${s.ino}`; };
const fileSnapshot = (file: string) => {
  const s = stat(file);
  return { dev: s.dev, ino: s.ino, size: s.size, mtimeNs: s.mtimeNs, bytes: fs.readFileSync(file) };
};
const directorySnapshot = (file: string) => {
  const s = stat(file);
  return { dev: s.dev, ino: s.ino, mtimeNs: s.mtimeNs };
};
const age = (file: string): void => { const time = new Date(Date.now() - 60_000); fs.utimesSync(file, time, time); };
const assertNoDetach = (root: string, lock: string): void => {
  expect(fs.readdirSync(root).filter(name => name.startsWith(`${path.basename(lock)}.dead.`) ||
    name.startsWith(`${path.basename(lock)}.released.`) || name.startsWith("r."))).toEqual([]);
};
const diagnostic = (event: string, data: unknown): void => {
  console.info(`[mesh-classic-sharing] ${event} ${JSON.stringify(data, (_key, value: unknown) =>
    typeof value === "bigint" ? value.toString() : value)}`);
};

/** Plain PATH Node, never process.execPath (which may be Bun). Synchronous terminal wait owns the child for its
 * entire lifetime; OS deadline and bounded pipes apply even if publishing its PID fails. Each file gets a NEW full R. */
const deadRecord = (): string => {
  const receipt = spawnSync("node", ["-e", "process.stdout.write(String(process.pid))"], {
    encoding: "utf8", windowsHide: true, timeout: 4000, maxBuffer: 4096, input: "",
  });
  diagnostic("joined-independent-pid", { pid: receipt.pid, code: receipt.status, signal: receipt.signal, error: receipt.error?.message });
  expect(receipt.error).toBeUndefined();
  expect(receipt.status).toBe(0);
  expect(receipt.signal).toBeNull();
  expect(receipt.stderr).toBe("");
  const pid = Number(receipt.stdout);
  expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
  expect(pid).toBe(receipt.pid);
  const record = `${randomUUID()}\n${pid}\n${Date.now()}\n`;
  assertDead(record);
  return record;
};
const assertDead = (record: string): void => {
  expect(record).toMatch(/^[0-9a-f-]{36}\n[1-9]\d*\n\d+\n$/);
  const pid = Number(record.split("\n")[1]);
  // Only a REAL ESRCH from this owned, terminally joined producer grants death authority; PID reuse fails closed.
  expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
};
interface Tree { lock: string; entries: WindowsMeshRecoveryEntry[]; records: Map<string, string> }
const deadTree = (root: string, domain: Domain = "mesh"): Tree => {
  const lock = lockAt(root, domain), first = path.join(lock, "r.first"), last = path.join(lock, "r.last");
  fs.mkdirSync(first, { recursive: true }); fs.mkdirSync(last);
  const files = [path.join(first, "owner"), path.join(lock, `owner.released.${randomUUID()}`), path.join(last, "owner")];
  const records = new Map(files.map(file => [file, deadRecord()]));
  for (const [file, record] of records) fs.writeFileSync(file, record, { flag: "wx" });
  // Legal child-first order: a pending FIRST owner makes its parent fail before the two later full records.
  // The LAST selected FILE is independent of the FIRST and follows that first directory in the request.
  const order = [files[0]!, first, files[1]!, files[2]!, last, lock];
  return { lock, records, entries: order.map(file => ({ path: file, stat: stat(file),
    ...(records.has(file) ? { record: records.get(file)! } : {}) })) };
};
const judge = async (tree: Tree, entries: readonly WindowsMeshRecoveryEntry[] = tree.entries): Promise<void> => {
  await judgeMeshLegacyDebris(tree.lock, entries, async entry => {
    expect(tree.records.get(entry.path)).toBe(entry.record); // this leaf's OWN complete R, never a vanished neighbor
    assertDead(entry.record!);
    return undefined;
  });
};
const freshSurvivors = async (tree: Tree): Promise<readonly WindowsMeshRecoveryEntry[]> => {
  // Entirely NEW enumeration, bigint S, full R and per-leaf death judgments after external close+JOIN.
  const entries = captureMeshLegacyDebris(tree.lock, stat(tree.lock));
  await judge(tree, entries);
  for (const entry of entries) {
    if (entry.record !== undefined) expect(readMeshRecoveryOwner(tree.lock, entry.path)?.record).toBe(tree.records.get(entry.path));
    else expect(entry.stat.isDirectory()).toBe(true); // directories carry no inherited/dependent death proof
  }
  return entries;
};
const result = (run: NativeRun, receipt: ChildReceipt) => interpretMeshRecoveryResult({
  status: receipt.code, signal: receipt.signal, stdout: receipt.stdout, pid: run.child.pid,
  ...(receipt.error ? { error: new Error(receipt.error) } : {}),
}, "debris");
const atGate = async (run: NativeRun, gate: string): Promise<void> => {
  await Promise.race([waitCheckpoint(gate, "debris-verified"), run.done.then(receipt => {
    throw new Error(`native exited before debris-verified: ${JSON.stringify(receipt)}`);
  })]);
};
const hex = (value: string): bigint => BigInt(value.startsWith("0x") ? value : `0x${value}`);
const assertTrace = (gate: string, entries: readonly WindowsMeshRecoveryEntry[], mode: "classic" | "trace",
  complete = true): ReturnType<typeof readDispositions> => {
  const trace = readDispositions(gate);
  diagnostic("actual-dispositions", { gate, entries: entries.map(entry => ({ path: entry.path, dev: entry.stat.dev,
    ino: entry.stat.ino, directory: entry.stat.isDirectory() })), trace });
  expect(trace.length).toBeGreaterThan(0); // no trace is NEVER fallback evidence
  expect(trace.length).toBeLessThanOrEqual(64);
  const pid = Number(fs.readFileSync(path.join(gate, "debris-verified.paused"), "utf8").trim());
  expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
  for (let i = 0; i < trace.length; i++) {
    const call = trace[i]!;
    expect(call.pid).toBe(pid);
    if (i > 0) expect(call.sequence).toBeGreaterThan(trace[i - 1]!.sequence);
    expect(Number.isInteger(call.error)).toBe(true);
    expect(call.forced).toBe(mode === "classic");
    expect(entries.some(entry => hex(call.volume) === entry.stat.dev && hex(call.index) === entry.stat.ino &&
      call.directory === entry.stat.isDirectory())).toBe(true);
    if (mode === "classic") expect(call.api).toBe("classic"); // real calls only, not a synthetic failed Ex call
  }
  if (complete) for (const entry of entries) {
    expect(trace.some(call => call.ok && call.api === (mode === "classic" ? "classic" : "extended") &&
      hex(call.volume) === entry.stat.dev && hex(call.index) === entry.stat.ino && call.directory === entry.stat.isDirectory()),
    `successful actual ${mode} disposal of ${entry.path}`).toBe(true);
  }
  if (mode === "trace") expect(trace[0]!.api).toBe("extended");
  return trace;
};

// Exactly 17 cases: 6 native D controls, 6 actual classic crash-after-i rows, 2 public D routes, 3 public E configurations.
// Each crash row holds a compatible reader on entry i through its real classic disposal and helper crash+JOIN,
// then closes+JOINS that reader before checking removal and freshly judging survivors; the partial control stays separate.
describe.skipIf(!hosted)(`Windows classic D and canonical E sharing (${SKIP})`, () => {
  let root: string, cap: WindowsMeshCapability, harness: WindowsHandleHarness | undefined;
  let harnessCreation: Promise<WindowsHandleHarness> | undefined;
  let cacheFixture: string | undefined;
  let savedHooks: typeof windowsMeshReleaseTestHooks;
  let savedRecoveryHooks: typeof windowsMeshRecoveryTestHooks;
  let recoveries: RecoverySample[], releases: WindowsMeshReleaseChildSample[];
  let nativeCount: number, handleCount: number;
  beforeEach(async () => {
    harness = undefined; harnessCreation = undefined; cacheFixture = undefined;
    root = temp(); recoveries = []; releases = []; nativeCount = 0; handleCount = 0;
    savedHooks = { ...windowsMeshReleaseTestHooks };
    savedRecoveryHooks = { ...windowsMeshRecoveryTestHooks };
    for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
    for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
    forgetWindowsMeshCapability();
    // Register the unique actual-profile allocation BEFORE preparation; never use RUNNER_TEMP or the default cache.
    // Only this cache lives in the profile. Locks/gates/external fixture and compiler temps remain under os.tmpdir().
    cacheFixture = windowsMeshCacheFixture("fcsr-cache-");
    windowsMeshReleaseTestHooks.cacheRoot = path.join(cacheFixture, "cache");
    windowsMeshReleaseTestHooks.deadlineMs = NATIVE_MS;
    windowsMeshReleaseTestHooks.afterChild = sample => { releases.push(sample); };
    windowsMeshRecoveryTestHooks.afterChild = sample => { recoveries.push(sample); };
    const bootstrap = windowsHelperBootstrap();
    expect(bootstrap).toBeDefined();
    cap = prepareWindowsMeshCapability();
    expect(cap.digest).toBe(bootstrap!.digest);
    expect(createHash("sha256").update(fs.readFileSync(cap.absolutePath)).digest("hex")).toBe(cap.digest);
    harnessCreation = WindowsHandleHarness.create(temp()); // owned before awaiting compilation/readiness
    harness = await harnessCreation;
    diagnostic("captured-capability", { path: cap.absolutePath, digest: cap.digest });
  }, 180_000); // bounded 120s external-fixture compile plus synchronous, bounded capability setup
  afterEach(async () => {
    // Harness owns every child BEFORE a wait, including failed spawns, and closes+JOINS ALL before fixture cleanup.
    // If close itself fails, retain fixtures: never recursively clean under a possibly live native writer.
    let receipts: ChildReceipt[] = [];
    try {
      // create() joins on failure; teardown also joins a still-pending compiler if setup timed out.
      const owned = harness ?? await harnessCreation?.catch(() => undefined);
      receipts = owned ? await owned.close() : [];
    } finally {
      vi.restoreAllMocks();
      for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
      Object.assign(windowsMeshRecoveryTestHooks, savedRecoveryHooks);
      for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
      Object.assign(windowsMeshReleaseTestHooks, savedHooks);
      forgetWindowsMeshCapability();
    }
    diagnostic("terminal-census", { nativeCount, handleCount, joined: receipts.length, receipts,
      synchronousRecoveryChildren: recoveries, synchronousReleaseChildren: releases });
    const cleanup = [
      ...roots.map(dir => ({ dir, prefix: "fcsr-" })),
      ...(cacheFixture ? [{ dir: cacheFixture, prefix: "fcsr-cache-" }] : []),
    ];
    // Check EVERY basename before ANY root cleanup, and only after ALL owned children have joined.
    for (const { dir, prefix } of cleanup) if (!path.basename(dir).startsWith(prefix)) throw new Error("unsafe owned fixture cleanup");
    diagnostic("owned-residue-before-cleanup", cleanup.map(({ dir }) => ({ root: dir, entries: fs.existsSync(dir) ? fs.readdirSync(dir) : [] })));
    for (const { dir } of cleanup) {
      fs.rmSync(dir, { recursive: true, force: true });
      expect(fs.existsSync(dir)).toBe(false);
    }
    roots.length = 0;
    cacheFixture = undefined;
    harness = undefined;
    harnessCreation = undefined;
    expect(receipts.length).toBeGreaterThanOrEqual(nativeCount + handleCount);
  }, 150_000); // may join a pending 120s compiler; join failure never reaches cleanup
  const hold = async (file: string, access: number, share: number): Promise<HeldHandle> => {
    const held = await harness!.hold(file, access, share); // ownership registration is inside the shared harness
    handleCount += 1;
    diagnostic("external-handle-ready", { file, ...held.ready });
    expect(held.pid).toBe(held.ready.pid);
    expect(held.ready.access).toBe(access); expect(held.ready.share).toBe(share);
    const s = stat(file);
    expect(hex(held.ready.volume)).toBe(s.dev); expect(hex(held.ready.index)).toBe(s.ino);
    expect(held.ready.directory).toBe(s.isDirectory());
    expect(held.ready.heldHandles).toBe(held.ready.baselineHandles + 1);
    return held;
  };
  const closeHeld = async (held: HeldHandle): Promise<void> => {
    const receipt = await held.close(); // actual CloseHandle + terminal JOIN, not an exit-only observation
    diagnostic("external-handle-closed", { pid: held.pid, baselineHandles: held.ready.baselineHandles, receipt });
    expect(await held.done).toEqual(receipt);
    expect(receipt.error).toBeUndefined(); expect(receipt.code).toBe(0); expect(receipt.signal).toBeNull();
    expect(receipt.stderr).toBe("");
    const rows: unknown[] = receipt.stdout.trim().split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line) as unknown);
    expect(rows).toHaveLength(2);
    const closed = rows.find((row): row is { event: "closed"; pid: number; baselineHandles: number; heldHandles: number; closedHandles: number } =>
      typeof row === "object" && row !== null && "event" in row && row.event === "closed");
    expect(closed, "actual post-CloseHandle census is required").toBeDefined();
    expect(closed!.pid).toBe(held.pid);
    expect(closed!.baselineHandles).toBe(held.ready.baselineHandles);
    expect(closed!.heldHandles).toBe(held.ready.heldHandles);
    expect(closed!.closedHandles).toBe(held.ready.baselineHandles);
  };
  const start = (lock: string, entries: readonly WindowsMeshRecoveryEntry[], gate: string): NativeRun => {
    nativeCount += 1;
    return harness!.native(cap, "debris", encodeMeshDebrisRecoveryRequest(lock, entries, NATIVE_MS),
      { name: "debris-verified", dir: gate });
  };
  const finish = async (tree: Tree, entries: readonly WindowsMeshRecoveryEntry[], mode: "classic" | "trace" = "classic"): Promise<void> => {
    const gate = temp(), run = start(tree.lock, entries, gate);
    await atGate(run, gate); proceedCheckpoint(gate, mode);
    const receipt = await run.done;
    diagnostic("native-terminal", { path: cap.absolutePath, mode: "debris", receipt });
    expect(result(run, receipt)).toEqual({ outcome: "recovered" });
    assertTrace(gate, entries, mode);
    expect(fs.existsSync(tree.lock)).toBe(false);
  };
  const acquire = async (domain: Domain, target: string, operation: () => string, timeout = NATIVE_MS,
    hostQualified = false): Promise<string> => {
    if (domain === "mesh") return new MeshLock(target, { lockProtocol: 1, lockTimeoutMs: timeout }, () => undefined).withLock(operation);
    const release = await acquireMeshCustodyLock(target, timeout, { lockProtocol: 1, hostQualified });
    try { return operation(); } finally { release(); }
  };
  const assertTimeout = (error: unknown): void => {
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect(error).toMatchObject({ name: "MeshLockTimeoutError", code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect((error as MeshLockTimeoutError).attempts).toBeGreaterThan(0);
  };

  it("D forces real classic selected-file AND empty-directory disposal without detaching canonical L", async () => {
    const tree = deadTree(root); await judge(tree);
    await finish(tree, tree.entries);
    assertNoDetach(root, tree.lock);
  }, 30_000);

  for (const i of [0, 1, 2, 3, 4, 5] as const) it(`D actual classic crash after ${i} with selected reader held through disposal and helper JOIN: close+JOIN reader, independently rejudge survivors, finish without detach, and preserve successor B against original D`, async () => {
    const tree = deadTree(root); await judge(tree);
    expect(tree.entries).toHaveLength(6);
    const originalRequest = encodeMeshDebrisRecoveryRequest(tree.lock, tree.entries, NATIVE_MS);
    const originalLeaves = new Map(tree.entries.filter(entry => entry.record !== undefined)
      .map(entry => [entry.path, fileSnapshot(entry.path)]));
    const selected = tree.entries[i]!;
    // Hold ONLY entry i: earlier disposals stay unblocked, including each earlier child before its parent.
    // Real external file/directory access + DELETE sharing permits classic disposition, not last-close removal.
    const reader = await hold(selected.path, selected.stat.isDirectory() ? ACCESS.READ_ATTRIBUTES : ACCESS.READ_DATA, SHARE.ALL);
    const canonicalId = id(tree.lock), gate = temp(), run = start(tree.lock, tree.entries, gate);
    await atGate(run, gate);
    const command = `classic-debris-after-${i}` as const, checkpoint = `debris-after-${i}`;
    proceedCheckpoint(gate, command);
    // The harness registry owns only debris-verified. An unowned/stale later marker alone is NOT a live pause.
    await Promise.race([waitCheckpoint(gate, checkpoint), run.done.then(receipt => {
      throw new Error(`native exited before owned ${checkpoint}: ${JSON.stringify(receipt)}`);
    })]);
    expect(fs.readFileSync(path.join(gate, `${checkpoint}.paused`), "ascii")).toBe(`${run.child.pid}\n`);
    // Keep the original go file present: it MUST NOT accidentally release this second, checkpoint-specific pause.
    expect(fs.readFileSync(path.join(gate, "go"), "ascii")).toBe(`${command}\n`);
    expect(fs.existsSync(path.join(gate, `${checkpoint}.go`))).toBe(false);
    // Reader remains held through the actual classic call, this owned later pause, and the helper's terminal JOIN.
    const receipt = await harness!.killNative(run); // ownership-checked signal, then finite terminal JOIN
    expect(await run.done).toEqual(receipt);
    diagnostic("classic-crash-joined", { checkpoint, helper: cap.absolutePath, digest: cap.digest, pid: run.child.pid, receipt });
    expect(receipt.error).toBeUndefined(); expect(receipt.stderr).toBe(""); expect(receipt.stdout).toBe("");
    expect(receipt.code === 0 && receipt.signal === null).toBe(false);
    expect(receipt.code).not.toBe(124); // watchdog expiry is not the owned crash requested at this pause
    expect(result(run, receipt).outcome).toBe("recovery-outcome-unknown");
    await closeHeld(reader); // actual CloseHandle + terminal JOIN + baseline census AFTER the helper JOIN
    // Only after BOTH joins may disposed paths be ENOENT or survivors supply fresh enumeration/death authority.
    const disposed = tree.entries.slice(0, i + 1), remaining = tree.entries.slice(i + 1);
    const trace = assertTrace(gate, disposed, "classic");
    expect(trace).toHaveLength(i + 1); // no call beyond the selected existing checkpoint
    for (let n = 0; n <= i; n++) {
      const entry = tree.entries[n]!, call = trace[n]!;
      expect(call).toMatchObject({ api: "classic", forced: true, ok: true, error: 0, directory: entry.stat.isDirectory() });
      expect(hex(call.volume)).toBe(entry.stat.dev); expect(hex(call.index)).toBe(entry.stat.ino);
      expect(() => stat(entry.path)).toThrow(expect.objectContaining({ code: "ENOENT" })); // actual last-close removal
    }
    assertNoDetach(root, tree.lock);
    if (i === 5) {
      // Canonical L may stay delete-pending through debris-after-5 and helper JOIN, until reader close+JOIN. No invented survivor.
      expect(remaining).toEqual([]);
      expect(() => stat(tree.lock)).toThrow(expect.objectContaining({ code: "ENOENT" }));
    } else {
      expect(id(tree.lock)).toBe(canonicalId);
      // A NEW enumeration/full R/death judgment for EVERY surviving leaf, only after BOTH helper and reader JOINs.
      const fresh = await freshSurvivors(tree);
      expect(fresh.map(entry => entry.path).sort()).toEqual(remaining.map(entry => entry.path).sort());
      expect(fresh.filter(entry => entry.record !== undefined)).toHaveLength(remaining.filter(entry => entry.record !== undefined).length);
      for (const entry of fresh) {
        if (entry.record !== undefined) {
          expect(fileSnapshot(entry.path)).toEqual(originalLeaves.get(entry.path));
          expect(entry.record).toBe(tree.records.get(entry.path)); assertDead(entry.record);
        } else {
          const children = fresh.filter(child => child.path !== entry.path && path.dirname(child.path) === entry.path)
            .map(child => path.basename(child.path)).sort();
          // Includes [] for each empty survivor: emptiness, never the vanished neighboring leaf's death proof.
          expect(fs.readdirSync(entry.path).sort()).toEqual(children);
          expect(entry.stat.isDirectory()).toBe(true);
        }
      }
      await finish(tree, fresh); // remaining admissible D only; this helper is also joined before any cleanup
    }
    expect(fs.existsSync(tree.lock)).toBe(false); assertNoDetach(root, tree.lock);

    // Publish a fresh, live B without acquiring an application lock or replaying an application callback.
    fs.mkdirSync(tree.lock);
    const owner = path.join(tree.lock, "owner"), recordB = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
    fs.writeFileSync(owner, recordB, { flag: "wx" });
    const directoryB = directorySnapshot(tree.lock), ownerB = fileSnapshot(owner);
    nativeCount += 1;
    const stale = harness!.native(cap, "debris", originalRequest), staleReceipt = await stale.done;
    diagnostic("stale-original-D-versus-B", { checkpoint, receipt: staleReceipt, directoryB, ownerB });
    expect(staleReceipt.error).toBeUndefined(); expect(staleReceipt.stderr).toBe("");
    expect(staleReceipt.code).toBe(3); expect(staleReceipt.signal).toBeNull();
    expect(staleReceipt.stdout).toMatch(/^FMREC1 refused verify [a-z-]+\n$/);
    expect(result(stale, staleReceipt).outcome).toBe("refused");
    expect(directorySnapshot(tree.lock)).toEqual(directoryB); // B directory's OWN volume/id (and mtime)
    expect(fileSnapshot(owner)).toEqual(ownerB); // B owner's OWN volume/id, time, size, and complete bytes
    expect(readMeshRecoveryOwner(tree.lock)!.record).toBe(recordB);
    expect(fs.readdirSync(tree.lock)).toEqual(["owner"]); assertNoDetach(root, tree.lock);
  }, 45_000);

  it("D ordinary extended-success counterexample is traced, not misreported as classic fallback", async () => {
    const tree = deadTree(root); await judge(tree);
    await finish(tree, tree.entries, "trace");
    assertNoDetach(root, tree.lock);
  }, 30_000);

  it("D incompatible external handle on LAST selected FILE refuses ALL before deleting ANY FIRST proof; joined close permits fresh classic retry", async () => {
    const tree = deadTree(root); await judge(tree);
    const leaves = tree.entries.filter(entry => entry.record !== undefined);
    const original = new Map(leaves.map(entry => [entry.path, fileSnapshot(entry.path)]));
    const directory = directorySnapshot(tree.lock), last = leaves[leaves.length - 1]!;
    const held = await hold(last.path, ACCESS.READ_DATA, SHARE.READ | SHARE.WRITE); // denies native DELETE
    const gate = temp();
    proceedCheckpoint(gate, "classic"); // verification must fail BEFORE this gate, no disposition is authorized
    const run = start(tree.lock, tree.entries, gate), receipt = await run.done;
    expect(result(run, receipt)).toMatchObject({ outcome: "refused", reason: "verify: entry-unverified" });
    // Tracing is enabled only by the selected checkpoint command, so preflight may not create a trace at all.
    // Absence alone proves nothing: the strict refused terminal and unchanged EVERY full proof below are essential.
    if (fs.existsSync(path.join(gate, "disposition.jsonl"))) expect(readDispositions(gate)).toEqual([]);
    expect(fs.existsSync(path.join(gate, "debris-verified.paused"))).toBe(false);
    for (const entry of leaves) expect(fileSnapshot(entry.path)).toEqual(original.get(entry.path));
    expect(directorySnapshot(tree.lock)).toEqual(directory);
    assertNoDetach(root, tree.lock);
    await closeHeld(held);
    const fresh = await freshSurvivors(tree);
    await finish(tree, fresh);
    assertNoDetach(root, tree.lock);
  }, 30_000);

  it("D real classic delete-pending FIRST reader produces PARTIAL, not clean refusal; fresh independent survivors finish after external close+JOIN", async () => {
    const tree = deadTree(root); await judge(tree);
    const first = tree.entries[0]!, firstDirectory = tree.entries[1]!;
    const remaining = tree.entries.filter(entry => entry.record !== undefined && entry.path !== first.path);
    const original = new Map(remaining.map(entry => [entry.path, fileSnapshot(entry.path)]));
    const canonicalId = id(tree.lock);
    const held = await hold(first.path, ACCESS.READ_DATA, SHARE.ALL);
    const gate = temp(), run = start(tree.lock, tree.entries, gate);
    await atGate(run, gate); proceedCheckpoint(gate, "classic");
    const receipt = await run.done;
    diagnostic("pending-partial", { receipt, first: first.path, access: held.ready.access, share: held.ready.share });
    expect(receipt.code).toBe(2);
    expect(receipt.stdout).toBe("FMREC1 partial delete entry-delete\n");
    expect(result(run, receipt).outcome).toBe("recovery-outcome-unknown");
    const trace = assertTrace(gate, tree.entries, "classic", false);
    expect(trace).toHaveLength(2);
    expect(trace[0]).toMatchObject({ api: "classic", forced: true, ok: true, directory: false });
    expect(hex(trace[0]!.index)).toBe(first.stat.ino);
    expect(trace[1]).toMatchObject({ api: "classic", forced: true, ok: false, directory: true });
    expect(hex(trace[1]!.index)).toBe(firstDirectory.stat.ino);
    expect(() => fs.readFileSync(first.path)).toThrow(); // real delete-pending leaf is inaccessible while held
    for (const entry of remaining) {
      expect(fileSnapshot(entry.path)).toEqual(original.get(entry.path));
      const fresh = readMeshRecoveryOwner(tree.lock, entry.path)!;
      expect(fresh.record).toBe(tree.records.get(entry.path)); assertDead(fresh.record);
    }
    expect(id(tree.lock)).toBe(canonicalId); assertNoDetach(root, tree.lock);
    await closeHeld(held);
    expect(fs.existsSync(first.path)).toBe(false);
    expect(fs.readdirSync(firstDirectory.path)).toEqual([]); // emptiness only, NOT a vanished neighbor's death
    const fresh = await freshSurvivors(tree);
    expect(fresh.some(entry => entry.path === first.path)).toBe(false);
    expect(fresh.filter(entry => entry.record !== undefined)).toHaveLength(2);
    await finish(tree, fresh);
    assertNoDetach(root, tree.lock);
  }, 30_000);

  for (const late of ["owner", "unjudged"] as const) it(`D late canonical ${late} after real guarded verification survives all classic selected deletes and top empty-only failure; L never detaches`, async () => {
    const tree = deadTree(root); await judge(tree);
    const canonicalId = id(tree.lock), gate = temp(), run = start(tree.lock, tree.entries, gate);
    await atGate(run, gate); // actual helper holds its verified tree/guard here
    const file = path.join(tree.lock, late === "owner" ? "owner" : "unjudged-payload");
    const bytes = late === "owner" ? `${randomUUID()}\n${process.pid}\n${Date.now()}\n` : "unjudged live payload\n";
    fs.writeFileSync(file, bytes, { flag: "wx" });
    const before = fileSnapshot(file);
    proceedCheckpoint(gate, "classic");
    const receipt = await run.done;
    expect(receipt.code).toBe(2); expect(receipt.stdout).toBe("FMREC1 partial delete entry-delete\n");
    expect(result(run, receipt).outcome).toBe("recovery-outcome-unknown");
    const trace = assertTrace(gate, tree.entries, "classic", false);
    expect(trace).toHaveLength(tree.entries.length);
    for (let i = 0; i < tree.entries.length - 1; i++) {
      const entry = tree.entries[i]!;
      expect(trace[i]!.ok).toBe(true); expect(hex(trace[i]!.index)).toBe(entry.stat.ino);
      expect(fs.existsSync(entry.path)).toBe(false);
    }
    expect(trace[trace.length - 1]).toMatchObject({ ok: false, directory: true });
    expect(hex(trace[trace.length - 1]!.index)).toBe(tree.entries[tree.entries.length - 1]!.stat.ino);
    expect(fileSnapshot(file)).toEqual(before); expect(id(tree.lock)).toBe(canonicalId);
    expect(fs.readdirSync(tree.lock)).toEqual([path.basename(file)]);
    assertNoDetach(root, tree.lock);
  }, 30_000);

  for (const domain of ["mesh", "custody"] as const) it(`public ${domain} D uses independently dead full records and the captured real helper/classic calls, enters ONCE and releases without replay`, async () => {
    const tree = deadTree(root, domain); await judge(tree);
    const initial = captureMeshLegacyDebris(tree.lock, stat(tree.lock));
    await judge(tree, initial);
    const gate = temp(); proceedCheckpoint(gate, "classic");
    windowsMeshRecoveryTestHooks.checkpoint = { name: "debris-verified", dir: gate };
    let entered = 0;
    const value = await acquire(domain, root, () => {
      entered += 1;
      const owner = readMeshRecoveryOwner(tree.lock)!;
      expect(Number(owner.record.split("\n")[1])).toBe(process.pid);
      diagnostic("public-successor-owner", { domain, directory: directorySnapshot(tree.lock),
        owner: fileSnapshot(path.join(tree.lock, "owner")), record: owner.record });
      expect(fs.readdirSync(tree.lock)).toEqual(["owner"]);
      return "entered";
    });
    delete windowsMeshRecoveryTestHooks.checkpoint;
    expect(value).toBe("entered"); expect(entered).toBe(1);
    expect(recoveries).toEqual([{ absolutePath: cap.absolutePath, digest: cap.digest, mode: "debris", outcome: "recovered" }]);
    expect(releases).toHaveLength(1);
    expect(releases[0]).toMatchObject({ absolutePath: cap.absolutePath, digest: cap.digest, outcome: "released", attempt: 1 });
    assertTrace(gate, initial, "classic");
    expect(fs.existsSync(tree.lock)).toBe(false); assertNoDetach(root, tree.lock);
  }, 30_000);

  const eConfigurations = [
    { domain: "mesh", hostQualified: false, label: "mesh" },
    { domain: "custody", hostQualified: false, label: "custody" },
    { domain: "custody", hostQualified: true, label: "host-qualified custody (E before missing-pid alarm)" },
  ] as const;
  for (const { domain, hostQualified, label } of eConfigurations) it(`public E ${label}: fresh control, incompatible external directory denial, actual close+JOIN aged retry, and REAL final-rmdir late owner preservation`, async () => {
    // Fresh genuine-empty L is NOT age/death authority, even in the host-qualified missing-pid configuration.
    const freshRoot = temp(), freshLock = lockAt(freshRoot, domain);
    fs.mkdirSync(freshLock);
    const freshBefore = directorySnapshot(freshLock);
    let freshEntered = 0;
    const freshError = await acquire(domain, freshRoot, () => { freshEntered += 1; return "unsafe"; }, REFUSAL_MS, hostQualified).catch(error => error);
    assertTimeout(freshError); expect(freshEntered).toBe(0);
    expect(directorySnapshot(freshLock)).toEqual(freshBefore); expect(fs.readdirSync(freshLock)).toEqual([]);
    expect(recoveries).toEqual([]); expect(releases).toEqual([]); assertNoDetach(freshRoot, freshLock);

    // A REAL external LIST_DIRECTORY handle denying DELETE sharing makes the atomic empty-only syscall fail safely.
    const lock = lockAt(root, domain); fs.mkdirSync(lock); age(lock);
    const before = directorySnapshot(lock), held = await hold(lock, ACCESS.LIST_DIRECTORY, SHARE.READ | SHARE.WRITE);
    const realRmdir = fs.rmdirSync;
    let attempts = 0;
    const sharingFailures: string[] = [];
    const syscall = vi.spyOn(fs, "rmdirSync").mockImplementation(file => {
      if (String(file) === lock) attempts += 1;
      try { return realRmdir(file); } // observer only: never fabricate syscall/native results
      catch (error) {
        if (String(file) === lock) sharingFailures.push((error as NodeJS.ErrnoException).code ?? "unknown");
        throw error;
      }
    });
    let entered = 0;
    let sharingError: unknown;
    try { sharingError = await acquire(domain, root, () => { entered += 1; return "unsafe"; }, REFUSAL_MS, hostQualified).catch(error => error); }
    finally { syscall.mockRestore(); }
    assertTimeout(sharingError); expect(entered).toBe(0); expect(attempts).toBeGreaterThan(0);
    expect(sharingFailures).toHaveLength(attempts);
    for (const code of sharingFailures) expect(["EPERM", "EACCES", "EBUSY"]).toContain(code);
    expect(directorySnapshot(lock)).toEqual(before); expect(fs.readdirSync(lock)).toEqual([]);
    expect(recoveries).toEqual([]); expect(releases).toEqual([]); assertNoDetach(root, lock);
    await closeHeld(held);
    expect(directorySnapshot(lock)).toEqual(before); // original ID AND original aged mtime, not a replacement
    expect(await acquire(domain, root, () => { entered += 1; return "aged unchanged empty recovered"; }, NATIVE_MS, hostQualified))
      .toBe("aged unchanged empty recovered");
    expect(entered).toBe(1); expect(fs.existsSync(lock)).toBe(false);
    expect(recoveries).toEqual([]); // E has NO C/D helper or detach workaround
    expect(releases).toHaveLength(1);
    expect(releases[0]).toMatchObject({ absolutePath: cap.absolutePath, digest: cap.digest, outcome: "released", attempt: 1 });

    // Inject a REAL unjudged, live owner at the LAST empty-only syscall, then invoke the REAL syscall.
    // No fs/native return is mocked. ENOTEMPTY from NTFS is what stops this originally-empty E attempt.
    const raceRoot = temp(), raceLock = lockAt(raceRoot, domain);
    fs.mkdirSync(raceLock); age(raceLock);
    const canonicalId = id(raceLock), ownerFile = path.join(raceLock, "owner");
    const ownerRecord = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
    let inserted: ReturnType<typeof fileSnapshot> | undefined, lateEntered = 0, raceAttempts = 0;
    const lateFailures: string[] = [];
    const lateSyscall = vi.spyOn(fs, "rmdirSync").mockImplementation(file => {
      if (String(file) === raceLock) {
        raceAttempts += 1;
        if (!inserted) { fs.writeFileSync(ownerFile, ownerRecord, { flag: "wx" }); inserted = fileSnapshot(ownerFile); }
      }
      try { return realRmdir(file); } // actual empty-only removal must see the new owner and fail
      catch (error) {
        if (String(file) === raceLock) lateFailures.push((error as NodeJS.ErrnoException).code ?? "unknown");
        throw error;
      }
    });
    // Mesh/plain custody retry sees a live PID. Host-qualified custody protects the new plain record as unproven,
    // but neither path is authorized to enter or dispatch C/D from this late unjudged receipt.
    let lateError: unknown;
    try { lateError = await acquire(domain, raceRoot, () => { lateEntered += 1; return "unsafe"; }, REFUSAL_MS, hostQualified).catch(error => error); }
    finally { lateSyscall.mockRestore(); }
    assertTimeout(lateError); expect(lateEntered).toBe(0); expect(raceAttempts).toBe(1);
    expect(lateFailures).toEqual(["ENOTEMPTY"]);
    expect(inserted).toBeDefined(); expect(fileSnapshot(ownerFile)).toEqual(inserted);
    expect(fs.readFileSync(ownerFile, "utf8")).toBe(ownerRecord);
    expect(id(raceLock)).toBe(canonicalId); expect(fs.readdirSync(raceLock)).toEqual(["owner"]);
    expect(recoveries).toEqual([]); expect(releases).toHaveLength(1); assertNoDetach(raceRoot, raceLock);
    diagnostic("E-preserved-and-retried", { label, freshBefore, originalAged: before,
      api: "fs.rmdirSync", originalPath: lock, attempts, sharingFailures, racePath: raceLock, raceAttempts, lateFailures,
      lateDirectory: directorySnapshot(raceLock), lateOwner: fileSnapshot(ownerFile) });
  }, 30_000);
});
