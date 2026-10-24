import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, withInitialMeshLockAcquisition, withInitialMeshLockAdmission } from "../src/mesh/mesh-lock.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// KEEP: bounded concurrency/data safety. Inspect actual FIFO receipt identities and
// canonical exclusion, not source text or mocked admission. Scratch is left to the test environment.
const B = 1_000; // 2B retains the existing queue's ordinary (>= 2 s) admission policy.
const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const close of cleanups.splice(0).reverse()) close();
  vi.restoreAllMocks();
  vi.useRealTimers();
});
const scratch = () => fs.mkdtempSync(path.join(os.tmpdir(), "mesh-initial-admission-"));
const tickets = (root: string) => fs.readdirSync(meshLockQueueDirectory(root))
  .filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort();
const owner = (root: string) => fs.readFileSync(path.join(root, ".lock", "owner"), "utf8");
const retain = <T>(root: string, work: () => Promise<T>) => withInitialMeshLockAdmission(root, work,
  { isPreAppendRefusal: () => false });
const designated = <T>(lock: MeshLock, work: () => T, budget = B) =>
  withInitialMeshLockAcquisition(lock.root, () => lock.withLock(work, budget));
const fixture = () => {
  const root = scratch(), lock = new MeshLock(root, { lockTimeoutMs: B }, () => undefined);
  vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
  fs.mkdirSync(path.join(root, ".lock"));
  const holder = `live-holder\n${process.pid}\n${Date.now()}\n`;
  fs.writeFileSync(path.join(root, ".lock", "owner"), holder);
  // As in mesh-lock-clock: suppress nondeterministic native watch delivery. The
  // existing <=20 ms wait safety net drives these deterministic late-wake cases.
  vi.spyOn(fs, "watch").mockImplementation((() => Object.assign(new EventEmitter(), {
    close() { /* no native watch was opened */ },
  })) as unknown as typeof fs.watch);
  return { root, lock, holder, release() { fs.renameSync(path.join(root, ".lock"), path.join(root, "released-holder")); } };
};

describe("retained initial mesh admission", () => {
  it("keeps the SAME receipt past original B and acquires the real canonical lock before 2B", async () => {
    const f = fixture();
    let settled = false;
    let atEntry: string[] = [], canonical = "";
    const work = retain(f.root, () => designated(f.lock, () => {
      atEntry = tickets(f.root); canonical = owner(f.root); return "admitted";
    })).finally(() => { settled = true; });
    await vi.advanceTimersByTimeAsync(0);
    const original = tickets(f.root);
    expect(original).toHaveLength(1);
    const receipt = fs.statSync(path.join(meshLockQueueDirectory(f.root), original[0]!));
    await vi.advanceTimersByTimeAsync(B + 250);
    expect(settled).toBe(false);
    expect(tickets(f.root)).toEqual(original);
    expect(fs.statSync(path.join(meshLockQueueDirectory(f.root), original[0]!)).ino).toBe(receipt.ino);
    expect(owner(f.root)).toBe(f.holder);
    f.release();
    await vi.advanceTimersByTimeAsync(20);
    expect(await work).toBe("admitted");
    expect(performance.now()).toBeLessThan(2 * B);
    expect(atEntry).toEqual(original);
    expect(canonical).not.toBe(f.holder);
    expect(canonical.split("\n")[1]).toBe(String(process.pid));
    expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
    expect(tickets(f.root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([false, true])("a stuck holder spends at most 2B, including backward wall time (backward=%s)", async backward => {
    const f = fixture();
    let settledAt = -1, entered = false;
    const work = retain(f.root, () => designated(f.lock, () => { entered = true; }))
      .catch((error: unknown) => error).finally(() => { settledAt = performance.now(); });
    await vi.advanceTimersByTimeAsync(B + 100);
    expect(settledAt).toBe(-1);
    expect(tickets(f.root)).toHaveLength(1);
    if (backward) vi.setSystemTime(Date.now() - 60_000);
    // Existing lock-clock late-wake seam: fire the pending wait, then resume its
    // continuation just past expiry. Async advancement at exactly 2B can freeze
    // a synthetic zero-delay clock at the boundary rather than model elapsed time.
    vi.advanceTimersByTime(2 * B + 1 - performance.now());
    await vi.advanceTimersByTimeAsync(0);
    expect(await work).toBeInstanceOf(MeshLockTimeoutError);
    expect(settledAt).toBeGreaterThanOrEqual(2 * B);
    // Existing relative lock clock permits the probe due exactly at the deadline;
    // the +1 ms guard is not another acquisition window.
    expect(settledAt).toBeLessThanOrEqual(2 * B + 1);
    expect(entered).toBe(false);
    expect(owner(f.root)).toBe(f.holder);
    expect(tickets(f.root)).toEqual([]);
    f.release();
    await vi.advanceTimersByTimeAsync(20);
    expect(entered).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([0, 50])("never extends an inherited %s ms try scope", async budget => {
    const f = fixture();
    let entered = false, settledAt = -1;
    const work = f.lock.withTryLock(() => retain(f.root, () => designated(f.lock, () => { entered = true; })), budget)
      .catch((error: unknown) => error).finally(() => { settledAt = performance.now(); });
    await vi.advanceTimersByTimeAsync(budget + 2);
    expect(await work).toBeInstanceOf(MeshLockTimeoutError);
    expect(settledAt).toBeLessThanOrEqual(budget + 1);
    expect(entered).toBe(false);
    expect(owner(f.root)).toBe(f.holder);
    expect(tickets(f.root)).toEqual([]);
  });

  it("does not extend an explicitly shortened acquisition", async () => {
    const f = fixture();
    let settledAt = -1;
    const work = retain(f.root, () => designated(f.lock, () => "must not enter", 50))
      .catch((error: unknown) => error).finally(() => { settledAt = performance.now(); });
    await vi.advanceTimersByTimeAsync(52);
    expect(await work).toBeInstanceOf(MeshLockTimeoutError);
    expect(settledAt).toBeLessThanOrEqual(51);
    expect(tickets(f.root)).toEqual([]);
  });

  it.each(["other-root", "escaped-context"] as const)("does not lend its 2B lifetime to %s work", async mode => {
    const f = fixture();
    let resume!: () => void;
    const gate = new Promise<void>(done => { resume = done; });
    let work!: Promise<unknown>;
    if (mode === "other-root") work = retain(scratch(), () => designated(f.lock, () => "must not enter"));
    else {
      await retain(f.root, async () => { work = gate.then(() => designated(f.lock, () => "must not enter")); });
    }
    let settledAt = -1;
    const result = work.catch((error: unknown) => error).finally(() => { settledAt = performance.now(); });
    resume();
    await vi.advanceTimersByTimeAsync(B + 2);
    expect(await result).toBeInstanceOf(MeshLockTimeoutError);
    expect(settledAt).toBeLessThanOrEqual(B + 1);
    expect(owner(f.root)).toBe(f.holder);
    expect(tickets(f.root)).toEqual([]);
  });
});

it("normal archive-enabled held ingress retries a REAL busy fence with the SAME ticket and publishes once", async () => {
  const base = scratch(), root = path.join(base, "hub"), archiveDir = path.join(root, "archive");
  fs.mkdirSync(archiveDir, { recursive: true });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archiveDir }));
  const hub = new MeshStore(root, 65_536, 100, { stateBackend: "sqlite", lockTimeoutMs: B });
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, { stateBackend: "file" });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const now = Date.now(), id = "session:initial-admission-far";
  const from: MeshIdentity = { id, name: "main", kind: "main", sessionId: id };
  const hash = (value: string) => createHash("sha256").update(value).digest("hex");
  await far.put({ key: `topology/hosts/${hash(id)}`, identity: from, value: {
    format: 1, id, rootId: id, identity: from, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(far.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(far.root, { key: `topology/participants/${hash(id)}`, version: 1, updatedAt: now, updatedBy: from,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id, label: id,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  const side = new StoreBridgeSide(hub, "far"), peer = new StoreBridgeSide(far, "hub");
  await side.mirror(await peer.presence());
  expect(side.holds(id, from)).toBe(true);
  await hub.publish({ topic: "history.seed", from, text: "warm same-boot archive" });
  const archive = new MeshArchive(archiveDir, root);
  expect(archive.pending()).toBeUndefined();
  const raw = new DatabaseSync(path.join(root, "state.db"));
  raw.exec("PRAGMA busy_timeout = 0");
  raw.exec("BEGIN IMMEDIATE");
  let held = true, busy = 0;
  cleanups.push(() => { if (held) raw.exec("ROLLBACK"); raw.close(); });
  const snapshots: Array<{ ticket: string[]; canonical: string }> = [];
  const exec = DatabaseSync.prototype.exec;
  vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql) {
    if (this !== raw && sql === "BEGIN IMMEDIATE") snapshots.push({ ticket: tickets(root), canonical: owner(root) });
    try { return exec.call(this, sql); }
    catch (error) {
      if (this !== raw && sql === "BEGIN IMMEDIATE" && held) {
        busy++; exec.call(raw, "ROLLBACK"); held = false; // next genuine attempt can now commit
      }
      throw error;
    }
  });
  await side.publish({ topic: "fabric.control.ack", kind: "ack", from, to: "session:initial-admission-hub",
    text: "archive-busy-once", data: { commandId: "archive-busy-once", bridge: { from: "far", id: "archive-busy-once" } } }, [id]);
  expect(busy, "SQLite's real zero-wait BEGIN must actually refuse once").toBe(1);
  expect(snapshots).toHaveLength(2);
  expect(snapshots[0]!.ticket).toHaveLength(1);
  expect(snapshots[1]!.ticket).toEqual(snapshots[0]!.ticket);
  expect(snapshots[1]!.canonical).not.toBe(snapshots[0]!.canonical); // released and reacquired, not held over delay
  const received = hub.read({ topic: "fabric.control.ack", after: 0 });
  expect(received).toHaveLength(1);
  expect(received[0]).toMatchObject({ verification: "bridge", text: "archive-busy-once" });
  expect(archive.lookup(received[0]!.sequence)).toEqual(received[0]);
  expect(JSON.parse(fs.readFileSync(path.join(root, "event-receipts", `${hash(received[0]!.dedupeKey!)}.json`), "utf8")))
    .toEqual(received[0]);
  expect(archive.pending()).toBeUndefined();
  expect(tickets(root)).toEqual([]);
  expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
});
