import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

// smarty-dev#8907: lock and ticket judge-then-delete paths remove only the inode they judged. Nothing here deletes:
// rm/unlink/rmdir are recorded probes that move their target into a trash directory (a rename), and verifiedRemove runs
// the real verifier as a dry run whose "removal" is emulated the same way (the off-Linux case runs main's fallback,
// whose primitives are the same probes). `swapAt` replaces the target at the first deletion point after the judgment.
type Call = { target: string; ino: number | undefined; removed: boolean; reason?: string };
const calls: Call[] = [];
let dryRun = true;
let unpinned = false;
let swapAt: ((target: string) => void) | undefined;
const deletionPoint = (target: string): void => { const swap = swapAt; swapAt = undefined; swap?.(target); };
vi.mock("../src/storage/verified-remove.js", async (load) => {
  const actual = await load<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: (...[target, judged, options]: Parameters<typeof actual.verifiedRemove>) => {
    deletionPoint(target);
    const result = actual.verifiedRemove(target, judged, dryRun ? { ...options, dryRun: true } : options);
    calls.push({ target, ino: judged?.ino, removed: result.removed, ...(result.removed ? {} : { reason: result.reason }) });
    if (dryRun && result.removed) moveAway(target);
    return result;
  } };
});
vi.mock("../src/mesh/fence-lock.js", async (load) => {
  const actual = await load<typeof import("../src/mesh/fence-lock.js")>();
  return { ...actual, pinFenceLock: (lock: string, record: string) => unpinned ? undefined : actual.pinFenceLock(lock, record) };
});
const { ActorBindingStore } = await import("../src/actors/binding-store.js");
const { MeshLockTicket, meshLockQueueDirectory } = await import("../src/mesh/lock-queue.js");
const { MeshLock } = await import("../src/mesh/mesh-lock.js");

const real = { renameSync: fs.renameSync, rmSync: fs.rmSync };
const REPLACEMENT = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`; // a live holder's record
let dir: string, trash: string, primitive: Array<{ op: string; target: string }>;
const moveAway = (target: string): boolean => {
  if (!fs.lstatSync(target, { throwIfNoEntry: false })) return false;
  real.renameSync(target, path.join(trash, randomUUID()));
  return true;
};
/** Replace `target` with a new inode of the same kind holding REPLACEMENT; returns its ino. */
const replace = (target: string): number => {
  const directory = fs.lstatSync(target).isDirectory();
  moveAway(target);
  if (directory) { fs.mkdirSync(target, { mode: 0o700 }); fs.writeFileSync(path.join(target, "owner"), REPLACEMENT, { mode: 0o600 }); }
  else fs.writeFileSync(target, REPLACEMENT, { mode: 0o600 });
  return fs.lstatSync(target).ino;
};
const holds = (target: string, ino: number): boolean => {
  const stat = fs.lstatSync(target, { throwIfNoEntry: false });
  const file = stat?.isDirectory() ? path.join(target, "owner") : target;
  return stat?.ino === ino && fs.readFileSync(file, "utf8") === REPLACEMENT;
};

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "lock-vremove-"));
  trash = path.join(dir, "trash");
  fs.mkdirSync(trash, { mode: 0o700 });
  calls.length = 0; dryRun = true; unpinned = false; swapAt = undefined; primitive = [];
  const probe = (op: string) => (file: fs.PathLike): void => {
    const target = String(file);
    deletionPoint(target);
    primitive.push({ op, target });
    if (!moveAway(target) && op !== "rm") throw Object.assign(new Error(`ENOENT: ${op} ${target}`), { code: "ENOENT" });
  };
  vi.spyOn(fs, "rmSync").mockImplementation(probe("rm"));
  vi.spyOn(fs, "unlinkSync").mockImplementation(probe("unlink"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(probe("rmdir"));
});
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); real.rmSync(dir, { recursive: true, force: true }); });

/** A binding store whose `.lock` holds a dead pid's stale record. */
const staleBinding = () => {
  const store = new ActorBindingStore("session-8907", dir);
  const lock = `${store.filePath!}.lock`;
  fs.mkdirSync(lock, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), `${randomUUID()}\n999999999\n0\n`, { mode: 0o600 });
  return { store, lock, ino: fs.lstatSync(lock).ino };
};

// The real verifier's inode/mtime checks are Linux only: off Linux main's accepted fallback (#7858) compares no inode,
// and a dry run there authorizes without verification. Swap and judged-stat cases run on Linux only; the fallback case
// below stays platform-neutral through a controlled darwin stub.
const linuxIt = it.skipIf(process.platform !== "linux");
const elapsedClock = () => {
  vi.useFakeTimers({ toFake: ["Date", "performance"] });
  // Faking hrtime itself would collapse receipt prefixes at one tick and let UUIDs reverse FIFO.
  const hostNow = process.hrtime.bigint.bind(process.hrtime);
  vi.spyOn(process.hrtime, "bigint").mockImplementation(() => hostNow() + BigInt(Math.round(performance.now() * 1_000_000)));
};

linuxIt("binding store: a stale lock re-taken after its judgment is refused, kept and never rm'd", async () => {
  const { store, lock, ino } = staleBinding();
  let replacement = -1, kept = false;
  swapAt = (target) => {
    replacement = replace(target);
    // Its holder releases a little later (a rename, the test's own move): the store then acquires normally.
    setTimeout(() => { kept = holds(lock, replacement); moveAway(lock); }, 50);
  };
  await store.setModel("actor", "model-a");
  expect(replacement).toBeGreaterThan(0);
  expect(kept).toBe(true);
  expect(primitive.filter(call => call.target === lock)).toEqual([]);
  expect(calls[0]).toEqual({ target: lock, ino, removed: false, reason: expect.stringMatching(/replaced since judgment/) });
  expect(calls.at(-1)).toMatchObject({ target: lock, removed: true }); // its own release, unchanged
});

linuxIt("binding store: an unchanged stale lock and the release are removed with their judged stats (dry run)", async () => {
  const { store, lock, ino } = staleBinding();
  await store.setModel("actor", "model-a");
  expect(calls).toEqual([
    { target: lock, ino, removed: true },
    { target: lock, ino: expect.any(Number), removed: true },
  ]);
  expect(calls[1]!.ino).not.toBe(ino);
  expect(primitive.filter(call => call.target === lock)).toEqual([]);
});

/** A mesh root with a dead pid's queue receipt ahead of ours. */
const deadReceipt = () => {
  const root = path.join(dir, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  const queue = meshLockQueueDirectory(root);
  fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
  const receipt = path.join(queue, `${"1".padStart(24, "0")}-999999999-${randomUUID()}`);
  fs.writeFileSync(receipt, "", { mode: 0o600 });
  return { ticket: new MeshLockTicket(root, randomUUID(), 5_000), receipt, ino: fs.lstatSync(receipt).ino };
};

linuxIt("lock queue: a dead receipt replaced after its judgment is kept, waited on and never unlinked", () => {
  const { ticket, receipt, ino } = deadReceipt();
  let replacement = -1;
  swapAt = (target) => { replacement = replace(target); };
  try {
    expect(ticket.mayContend()).toBe(false);
    expect(holds(receipt, replacement)).toBe(true);
    expect(primitive.filter(call => call.target === receipt)).toEqual([]);
    expect(calls).toEqual([{ target: receipt, ino, removed: false, reason: expect.stringMatching(/replaced since judgment/) }]);
  } finally { ticket.close(); }
});

linuxIt("lock queue: an unchanged dead receipt is removed with its judged stat (dry run) and the waiter becomes head", () => {
  const { ticket, receipt, ino } = deadReceipt();
  try {
    expect(ticket.mayContend()).toBe(true);
    expect(calls).toEqual([{ target: receipt, ino, removed: true }]);
    expect(primitive.filter(call => call.target === receipt)).toEqual([]);
  } finally { ticket.close(); }
});

// PR1412: plain tickets must reclaim through the real off-Linux fallback, while an opted-in canonical holder stays protected.
it.each([
  { kind: "dead", canonical: false },
  { kind: "stale", canonical: false },
  { kind: "dead", canonical: true },
  { kind: "stale", canonical: true },
])("off Linux lock queue: $kind predecessor with canonical guard=$canonical", ({ kind, canonical }) => {
  elapsedClock();
  dryRun = false;
  const root = path.join(dir, "mesh-fallback");
  fs.mkdirSync(root, { mode: 0o700 });
  const lock = path.join(root, ".lock");
  if (canonical) fs.mkdirSync(lock, { mode: 0o700 });
  const queue = meshLockQueueDirectory(root);
  fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
  const pid = kind === "dead" ? 999999999 : process.pid;
  const receipt = path.join(queue, `${"1".padStart(24, "0")}-${pid}-${randomUUID()}`);
  fs.writeFileSync(receipt, "", { mode: 0o600 });
  if (kind === "stale") {
    const stale = (Date.now() - 31_000) / 1000; // Older than the 30s stale bound for a 5s budget, despite a live pid.
    fs.utimesSync(receipt, stale, stale);
  }
  const ino = fs.lstatSync(receipt).ino;
  const ticket = new MeshLockTicket(root, randomUUID(), 5_000, canonical ? { stalledHeadBypassLock: lock } : {});
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { value: "darwin" });
  try {
    if (kind === "stale") {
      expect(ticket.mayContend()).toBe(false); // first sight of an old wall mtime must never authorize removal
      expect(fs.lstatSync(receipt).ino).toBe(ino);
      expect(calls).toEqual([]);
      expect(primitive.filter(call => call.target === receipt)).toEqual([]);
      vi.advanceTimersByTime(canonical ? 501 : 30_001); // early canonical reaper / ordinary predecessor observation
      expect(fs.lstatSync(receipt).ino).toBe(ino);
    } else {
      expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
      expect(performance.now()).toBe(0); // genuine death evidence, not an old wall-time substitute
    }
    if (canonical) {
      expect(ticket.mayContend()).toBe(false);
      expect(fs.existsSync(receipt)).toBe(true);
      expect(calls).toEqual([]);
      expect(primitive.filter(call => call.target === receipt)).toEqual([]);
      expect(moveAway(lock)).toBe(true);
    }
    expect(ticket.mayContend()).toBe(true);
    expect(ticket.queued).toBe(true); // Promoted to FIFO head, not closed into the plain contest.
    expect(fs.existsSync(receipt)).toBe(false);
    expect(primitive.filter(call => call.target === receipt)).toEqual([{ op: "unlink", target: receipt }]);
    expect(calls).toEqual([{ target: receipt, ino, removed: true }]);
  } finally {
    Object.defineProperty(process, "platform", platform);
    ticket.close();
  }
});

linuxIt("lock queue: a stalled canonical head is reaped through judged verifiedRemove", () => {
  elapsedClock();
  const root = path.join(dir, "mesh-stalled");
  fs.mkdirSync(root, { mode: 0o700 });
  const canonical = path.join(root, ".lock");
  const head = new MeshLockTicket(root, randomUUID(), 5_000);
  const follower = new MeshLockTicket(root, randomUUID(), 5_000, { stalledHeadBypassLock: canonical });
  const headReceipt = path.join(meshLockQueueDirectory(root), fs.readdirSync(meshLockQueueDirectory(root)).sort()[0]!);
  const ino = fs.lstatSync(headReceipt).ino;
  try {
    expect(head.mayContend()).toBe(true);
    expect(follower.mayContend()).toBe(false); // first observation starts the stall lease
    expect(calls).toEqual([]);
    expect(fs.lstatSync(headReceipt).ino).toBe(ino);
    vi.advanceTimersByTime(501); // #stallMs is 500 ms for this budget
    expect(follower.mayContend()).toBe(true);
    expect(calls).toEqual([{ target: headReceipt, ino, removed: true }]);
    expect(fs.existsSync(headReceipt)).toBe(false);
    expect(follower.queued).toBe(true); // reaping promoted it; it did not close into a plain contest
  } finally {
    head.close();
    follower.close();
  }
});

linuxIt("lock queue: an existing canonical holder keeps even a dead head receipt", () => {
  const root = path.join(dir, "mesh-holder");
  fs.mkdirSync(root, { mode: 0o700 });
  const canonical = path.join(root, ".lock");
  fs.mkdirSync(canonical, { mode: 0o700 });
  fs.writeFileSync(path.join(canonical, "owner"), `${randomUUID()}\n${process.pid}\n${Date.now()}\n`);
  const queue = meshLockQueueDirectory(root);
  fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
  const headReceipt = path.join(queue, `${"0".repeat(24)}-999999999-${randomUUID()}`);
  fs.writeFileSync(headReceipt, "", { mode: 0o600 });
  const follower = new MeshLockTicket(root, randomUUID(), 5_000, { stalledHeadBypassLock: canonical });
  try {
    expect(follower.mayContend()).toBe(false);
    expect(calls).toEqual([]);
    expect(fs.existsSync(headReceipt)).toBe(true);
    expect(moveAway(canonical)).toBe(true); // recorded owner release, not destructive canonical cleanup
    expect(follower.mayContend()).toBe(true); // the same proven-dead PID is cleaned immediately after release
    expect(follower.queued).toBe(true);
    expect(fs.existsSync(headReceipt)).toBe(false);
    expect(calls).toEqual([{ target: headReceipt, ino: expect.any(Number), removed: true }]);
    expect(primitive.filter(call => call.target === headReceipt)).toEqual([]);
  } finally { follower.close(); }
});

it("lock queue: a dead receipt name holding a directory is waited on, never removed (file-only, as main's unlink)", () => {
  const { ticket, receipt } = deadReceipt();
  real.renameSync(receipt, path.join(trash, "file"));
  fs.mkdirSync(receipt, { mode: 0o700 });
  try {
    expect(ticket.mayContend()).toBe(false);
    expect(fs.lstatSync(receipt).isDirectory()).toBe(true);
    expect(calls).toEqual([]);
    expect(primitive.filter(call => call.target === receipt)).toEqual([]);
  } finally { ticket.close(); }
});

linuxIt("mesh lock: an unpinned release whose .lock was replaced after the owner check is refused and kept", async () => {
  unpinned = true; // the Linux pin-failure path releases by name
  const root = path.join(dir, "mesh");
  const lock = path.join(root, ".lock");
  let replacement = -1;
  swapAt = (target) => { replacement = replace(target); };
  const operation = vi.fn();
  await expect(new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 200 }, () => undefined).withLock(operation)).rejects.toThrow();
  expect(operation).not.toHaveBeenCalled();
  expect(replacement).toBeGreaterThan(0);
  expect(holds(lock, replacement)).toBe(true);
  expect(primitive.filter(call => call.target.startsWith(lock))).toEqual([]);
  expect(calls).toEqual([{ target: lock, ino: expect.any(Number), removed: false, reason: expect.stringMatching(/replaced since judgment/) }]);
});

it("off Linux every release and reclaim runs main's removal as the fallback, and a failed one is not a removal", async () => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { value: "darwin" });
  try {
    dryRun = false;
    const { store, lock } = staleBinding();
    let failed = false;
    vi.mocked(fs.rmSync).mockImplementation((file) => {
      primitive.push({ op: "rm", target: String(file) });
      if (!failed) { failed = true; throw Object.assign(new Error("busy"), { code: "EBUSY" }); }
      moveAway(String(file));
    });
    await store.setModel("actor", "model-a");
    expect(calls.map(call => [call.target, call.removed])).toEqual([[lock, false], [lock, true], [lock, true]]);
    expect(primitive.filter(call => call.target === lock).length).toBe(3);

    calls.length = 0; primitive.length = 0;
    const root = path.join(dir, "mesh");
    const meshLock = path.join(root, ".lock");
    await expect(new MeshLock(root, { lockProtocol: 1 }, () => undefined).withLock(() => "ran")).resolves.toBe("ran");
    expect(calls).toEqual([{ target: meshLock, ino: expect.any(Number), removed: true }]);
    expect(primitive).toContainEqual({ op: "rm", target: expect.stringMatching(/\.lock\.released\./) });
    expect(fs.existsSync(meshLock)).toBe(false);
  } finally { Object.defineProperty(process, "platform", platform); }
});
