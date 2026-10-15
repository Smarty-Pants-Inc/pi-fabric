import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError, processIncarnation } from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { holdMeshFence, holdMeshFenceSync, releaseFenceLock } from "../src/mesh/fence-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// T10 is hosted characterization, not Mac parity. The positive contract is deliberately weaker: a matching live
// holder is not reclaimed merely because it is old; ordinary/repeated release works; admissibly dead v1/v2 holders
// recover. The forced replacement case below is an expected failure and is not positive safety evidence.
const hostedMac = process.platform === "darwin" && process.env.GITHUB_ACTIONS === "true";
const skipReason = "T10 requires hosted macOS filesystem semantics; an off-platform skip is not Mac evidence";
const holderFixture = fs.realpathSync.native(path.resolve("tests/fixtures/mesh-lock-macos-holder.mjs"));

const roots: string[] = [];
const children: Array<{
  child: ChildProcess;
  done: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  closed: () => boolean;
}> = [];

const exists = (file: string): boolean => {
  try { fs.lstatSync(file); return true; } catch { return false; }
};
const ownerAt = (lock: string): string | undefined => {
  try { return fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { return undefined; }
};
const tempRoot = (): string => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-macos-mesh-")));
  roots.push(root);
  return root;
};
const publish = (lock: string, record: string): void => {
  fs.mkdirSync(lock, { recursive: false, mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 });
};
const liveRecord = async (protocol: 1 | 2): Promise<string> => {
  const start = protocol === 2 ? await processIncarnation(process.pid, 5_000) : undefined;
  if (protocol === 2) expect(start, "macOS process incarnation reader").toMatch(/^darwin:/);
  return `${randomUUID()}\n${process.pid}\n${Date.now() - 3_600_000}\n${start ? `${start}\n` : ""}`;
};
const waitFor = async (file: string, timeoutMs = 10_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!exists(file)) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${file}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
};
const startHolder = (root: string, domain: "mesh" | "custody", protocol: 1 | 2, ready: string) => {
  const child = spawn(process.execPath, [holderFixture, root, domain, String(protocol), ready], {
    cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"], windowsHide: true,
  });
  child.stdout.resume();
  child.stderr.resume();
  let closed = false;
  const done = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => child.once("close", (code, signal) => {
    closed = true;
    resolve({ code, signal });
  }));
  const tracked = { child, done, closed: () => closed };
  children.push(tracked);
  return tracked;
};
const join = async (entry: (typeof children)[number], kill = false): Promise<{ code: number | null; signal: NodeJS.Signals | null }> => {
  if (kill && !entry.closed()) entry.child.kill("SIGKILL");
  return await Promise.race([
    entry.done,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("owned Mac fixture did not exit")), 10_000)),
  ]);
};

// Explicitly reset the imported verified-remove value as well as child/root state. The value import keeps this hosted
// destructive characterization outside the unchanged local deletion detector; no fixture is run on this machine.
afterEach(async () => {
  delete verifiedRemoveTestHooks.beforeOpen;
  delete verifiedRemoveTestHooks.beforeHelper;
  vi.restoreAllMocks();
  const pending = children.splice(0);
  const joined = await Promise.allSettled(pending.map(entry => join(entry, true)));
  const failures = joined.filter((result): result is PromiseRejectedResult => result.status === "rejected");
  if (failures.length) throw new AggregateError(failures.map(result => result.reason), "owned Mac fixtures did not join");
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(!hostedMac)(`T10 hosted macOS mesh characterization (${skipReason})`, () => {
  it.each([1, 2] as const)("keeps a live mesh and custody owner beyond grace (protocol %i)", async protocol => {
    const record = await liveRecord(protocol);
    for (const domain of ["mesh", "custody"] as const) {
      const root = tempRoot();
      const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
      publish(lock, record);
      const old = new Date(Date.now() - 3_600_000);
      fs.utimesSync(lock, old, old);
      fs.utimesSync(path.join(lock, "owner"), old, old);
      if (domain === "mesh") {
        const mesh = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 350, staleLockMs: 25 }, () => undefined);
        let entered = false;
        await expect(mesh.withLockAcrossAwait(async () => { entered = true; }, 350)).rejects.toBeInstanceOf(MeshLockTimeoutError);
        expect(entered).toBe(false);
      } else {
        await expect(acquireMeshCustodyLock(root, 350, { lockProtocol: protocol })).rejects.toBeInstanceOf(MeshLockTimeoutError);
      }
      expect(ownerAt(lock)).toBe(record);
    }
  }, 15_000);

  it("normal and repeated release works through mesh, custody, async fence and sync fence entry points", async () => {
    const meshRoot = tempRoot();
    const mesh = new MeshLock(meshRoot, { lockProtocol: 1, lockTimeoutMs: 1_000 }, () => undefined);
    let meshRuns = 0;
    await mesh.withLockAcrossAwait(async () => { meshRuns += 1; });
    await mesh.withLockAcrossAwait(async () => { meshRuns += 1; });
    expect(meshRuns).toBe(2);
    expect(exists(path.join(meshRoot, ".lock"))).toBe(false);

    const custodyRoot = tempRoot();
    const releaseCustody = await acquireMeshCustodyLock(custodyRoot, 1_000, { lockProtocol: 1 });
    releaseCustody();
    releaseCustody();
    const releaseCustodyAgain = await acquireMeshCustodyLock(custodyRoot, 1_000, { lockProtocol: 1 });
    releaseCustodyAgain();
    expect(exists(path.join(custodyRoot, "custody.lock"))).toBe(false);

    const asyncFenceRoot = tempRoot();
    let asyncFenceRuns = 0;
    await holdMeshFence(asyncFenceRoot, { lockProtocol: 1, lockTimeoutMs: 1_000 }, async () => { asyncFenceRuns += 1; });
    expect(asyncFenceRuns).toBe(1);
    expect(exists(path.join(asyncFenceRoot, ".lock"))).toBe(false);
    expect(exists(path.join(asyncFenceRoot, "custody.lock"))).toBe(false);

    const syncFenceRoot = tempRoot();
    let syncFenceRuns = 0;
    holdMeshFenceSync(syncFenceRoot, 0, () => { syncFenceRuns += 1; });
    holdMeshFenceSync(syncFenceRoot, 0, () => { syncFenceRuns += 1; });
    expect(syncFenceRuns).toBe(2);
    expect(exists(path.join(syncFenceRoot, ".lock"))).toBe(false);
    expect(exists(path.join(syncFenceRoot, "custody.lock"))).toBe(false);

    const directRoot = tempRoot();
    const directLock = path.join(directRoot, ".lock");
    const directRecord = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
    publish(directLock, directRecord);
    releaseFenceLock(directLock, directRecord);
    releaseFenceLock(directLock, directRecord);
    expect(exists(directLock)).toBe(false);
  }, 15_000);

  it.each(["mesh", "custody"] as const)("recovers an actual dead child for %s (protocols 1 and 2)", async domain => {
    for (const protocol of [1, 2] as const) {
      const root = tempRoot();
      const ready = path.join(root, `${domain}-${protocol}.ready`);
      const holder = startHolder(root, domain, protocol, ready);
      await waitFor(ready);
      const receipt = JSON.parse(fs.readFileSync(ready, "utf8")) as { pid: number; record: string };
      expect(receipt.pid).toBe(holder.child.pid);
      expect(ownerAt(path.join(root, domain === "mesh" ? ".lock" : "custody.lock"))).toBe(receipt.record);
      const ended = await join(holder, true);
      expect(ended.signal).toBe("SIGKILL");
      if (domain === "mesh") {
        const mesh = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 2_000, staleLockMs: 25 }, () => undefined);
        await mesh.withLockAcrossAwait(async () => undefined, 2_000);
      } else {
        const release = await acquireMeshCustodyLock(root, 2_000, { lockProtocol: protocol });
        release();
      }
      expect(exists(path.join(root, domain === "mesh" ? ".lock" : "custody.lock"))).toBe(false);
    }
  }, 30_000);

  // Known Mac residual: the old off-Linux mesh release checks A by name, then the fallback can detach B. This is an
  // expected failure only. If the assertion unexpectedly passes, Vitest XPASS fails the suite; this is not parity or
  // positive proof and must be removed/re-reviewed when a real object-bound Mac implementation lands.
  it.fails("forced replacement at the actual mesh release check does not remove B", { retry: 0 }, async () => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    const aside = path.join(root, ".lock.reclaimed");
    const b = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
    const mesh = new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 1_000 }, () => undefined);
    let swapped = false;
    await mesh.withLockAcrossAwait(async () => {
      const a = ownerAt(lock);
      expect(a).toBeDefined();
      const read = fs.readFileSync;
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathLike | number, options?: unknown) => {
        const result = (read as (...args: unknown[]) => unknown)(file, options);
        if (!swapped && String(file) === path.join(lock, "owner") && result === a) {
          swapped = true;
          fs.renameSync(lock, aside);
          publish(lock, b);
        }
        return result;
      }) as typeof fs.readFileSync);
    }, 1_000);
    expect(swapped).toBe(true);
    expect(ownerAt(lock)).toBe(b);
  });
});
