import { fork, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { buildSync } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// smarty-dev#9805 (stalled FIFO head) and data-safety mutual exclusion, through the real MeshLock in real processes.
// A "stalled" process is a child whose event loop is blocked by a synchronous busy loop: alive (kill -0 succeeds), its
// receipt present, but it makes no progress. Barriers are stdout lines written synchronously by the child (fs.writeSync,
// so a line is out before the child blocks) and IPC commands from the parent; the parent never sleeps.

const parents: string[] = [];
const children: ChildProcess[] = [];
let bundleDir: string;
let bundle: string;
let childScript: string;
const noop = (): void => {};

// A finite-lifetime child driving its own MeshLock. Commands arrive over IPC; replies are synchronous stdout lines.
const CHILD = String.raw`
import fs from "node:fs";
const [moduleUrl, root, budget] = process.argv.slice(2);
const { MeshLock } = await import(moduleUrl);
const lock = new MeshLock(root, { lockTimeoutMs: Number(budget) }, () => {});
const say = message => fs.writeSync(1, JSON.stringify({ pid: process.pid, ...message }) + "\n");
const busy = ms => { const end = Date.now() + ms; while (Date.now() < end) { /* event loop blocked */ } };
const finish = () => { say({ done: true }); process.disconnect?.(); };
// Hard lifetime bound: never outlive a lost parent.
setTimeout(() => { say({ fatal: "lifetime" }); process.exit(2); }, 45_000).unref();
let pending;
process.on("message", async command => {
  try {
    if (command.cmd === "enqueue") {
      pending = lock.withLock(() => say({ acquired: Date.now() }));
      say({ queued: true }); // protocol 1: the receipt exists once withLock has returned its promise
      pending.then(finish, error => { say({ error: String(error?.code ?? error) }); finish(); });
    } else if (command.cmd === "block") {
      say({ blocked: Date.now() });
      busy(command.ms);
    } else if (command.cmd === "hold") {
      const inside = root + "/inside";
      await lock.withLock(() => {
        fs.writeFileSync(inside, String(process.pid), { flag: "wx" });
        say({ entered: Date.now() });
        busy(command.ms); // a live, slow holder that cannot heartbeat its receipt
        fs.unlinkSync(inside);
        fs.writeFileSync(root + "/exit", String(Date.now()));
      });
      finish();
    } else if (command.cmd === "stress") {
      let seed = command.seed >>> 0;
      const random = () => { seed = (seed + 0x6d2b79f5) >>> 0; let t = seed; t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
      const holder = root + "/holder";
      let acquisitions = 0, violations = 0;
      const errors = [];
      for (let round = 0; round < command.rounds; round += 1) {
        // Sometimes stall this waiter's whole event loop while it is queued (possibly as the FIFO head).
        if (random() < 0.4) { const ms = Math.floor(random() * command.queuedStallMs); setTimeout(() => busy(ms), Math.floor(random() * 5)); }
        const holdMs = Math.floor(random() * command.holdStallMs);
        try {
          await lock.withLock(() => {
            let mine = false;
            try { fs.writeFileSync(holder, String(process.pid), { flag: "wx" }); mine = true; }
            catch (error) { if (error.code === "EEXIST") violations += 1; else throw error; }
            busy(holdMs);
            if (mine) fs.unlinkSync(holder);
            acquisitions += 1;
          });
        } catch (error) { errors.push(String(error?.code ?? error)); }
      }
      say({ result: { acquisitions, violations, errors } });
      finish();
    }
  } catch (error) { say({ error: String(error?.code ?? error) }); finish(); }
});
say({ ready: true });
`;

interface Message { pid: number; [key: string]: unknown }
interface Child { process: ChildProcess; messages: Message[]; next(key: string): Promise<Message>; send(command: object): void }

const spawn = (root: string, budgetMs: number): Child => {
  const child = fork(childScript, [`file://${bundle}`, root, String(budgetMs)], {
    silent: true, env: { ...process.env, PI_FABRIC_LOCK_STATS: "0" },
  });
  children.push(child);
  const messages: Message[] = [];
  const waiters: Array<{ key: string; resolve: (message: Message) => void; reject: (error: Error) => void }> = [];
  let buffer = "";
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += String(chunk); });
  child.stdout!.on("data", chunk => {
    buffer += String(chunk);
    for (let index = buffer.indexOf("\n"); index >= 0; index = buffer.indexOf("\n")) {
      const message = JSON.parse(buffer.slice(0, index)) as Message;
      buffer = buffer.slice(index + 1);
      messages.push(message);
      for (const waiter of waiters.filter(w => w.key in message)) { waiters.splice(waiters.indexOf(waiter), 1); waiter.resolve(message); }
    }
  });
  child.on("exit", code => {
    for (const waiter of waiters.splice(0)) waiter.reject(new Error(`child exited ${code} before "${waiter.key}": ${stderr}`));
  });
  return {
    process: child, messages,
    next: key => {
      const seen = messages.find(message => key in message);
      if (seen) return Promise.resolve(seen);
      if (child.exitCode !== null) return Promise.reject(new Error(`child exited before "${key}": ${stderr}`));
      return new Promise((resolve, reject) => waiters.push({ key, resolve, reject }));
    },
    send: command => { child.send(command); },
  };
};

const scratch = (): string => {
  // Own parent per root: the admission namespace (a sibling of the root) stays inside this scratch.
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-stalled-head-"));
  parents.push(parent);
  const root = path.join(parent, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  return root;
};
const receipts = (root: string): string[] => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-/.test(name)).sort(); }
  catch { return []; }
};
const pidOf = (receipt: string): number => Number(receipt.split("-")[1]);
const ownerPid = (root: string): number => Number(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1]);
const reclaimFences = (root: string): string[] => fs.readdirSync(root).filter(name => name.startsWith(".lock.dead."));

beforeAll(() => {
  bundleDir = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-stalled-head-bundle-"));
  bundle = path.join(bundleDir, "mesh-lock.mjs");
  buildSync({ entryPoints: [path.resolve("src/mesh/mesh-lock.ts")], bundle: true, platform: "node", format: "esm", outfile: bundle,
    banner: { js: 'import { createRequire as __cr } from "node:module"; const require = __cr(import.meta.url);' } });
  childScript = path.join(bundleDir, "child.mjs");
  fs.writeFileSync(childScript, CHILD);
});
afterEach(() => {
  for (const child of children.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  for (const parent of parents.splice(0)) fs.rmSync(parent, { recursive: true, force: true });
});
afterAll(() => { fs.rmSync(bundleDir, { recursive: true, force: true }); });

describe.skipIf(process.platform === "win32")("mesh lock with a stalled live FIFO head (smarty-dev#9805)", () => {
  it.each([false, true])("passes a live blocked head and preserves its receipt (follower queued before head promotion: %s)", async (queuedBeforePromotion) => {
    const root = scratch();
    // A holds .lock (in this process) so B queues behind it.
    const holderA = new MeshLock(root, { lockTimeoutMs: 2500 }, noop);
    let releaseA = noop;
    let enteredA = noop;
    const aEntered = new Promise<void>(resolve => { enteredA = resolve; });
    const a = holderA.withLockAcrossAwait(() => new Promise<void>(resolve => { releaseA = resolve; enteredA(); }));
    await aEntered;

    const b = spawn(root, 10_000);
    await b.next("ready");
    b.send({ cmd: "enqueue" });
    await b.next("queued");
    expect(receipts(root).map(pidOf)).toEqual([process.pid, b.process.pid]);
    // Block B's event loop (it stays alive), then free .lock: B is now the FIFO head but cannot act on it.
    b.send({ cmd: "block", ms: 1800 });
    await b.next("blocked");
    // Queue C either before B is promoted (A -> B -> C), or once B is already the head. The former catches a
    // cached index > 1 that never notices its still-present predecessor became head when an older receipt left.
    const follower = new MeshLock(root, { lockTimeoutMs: 2500 }, noop);
    const started = Date.now();
    let during: { at: number; receipts: string[]; owner: number } | undefined;
    const acquireFollower = () => follower.withLock(() => {
      during = { at: Date.now(), receipts: receipts(root), owner: ownerPid(root) };
    });
    const pending = queuedBeforePromotion ? acquireFollower() : undefined;
    if (queuedBeforePromotion) expect(receipts(root).map(pidOf)).toEqual([process.pid, b.process.pid, process.pid]);
    releaseA();
    await a;
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    const [headReceipt] = receipts(root);
    expect(receipts(root)).toHaveLength(queuedBeforePromotion ? 2 : 1);
    expect(pidOf(headReceipt!)).toBe(b.process.pid);

    await (pending ?? acquireFollower());
    const releasedAt = Date.now();
    const bAcquired = await b.next("acquired");
    await b.next("done");

    // The regression: C must not wait for the stalled head's 80% fallback (~2000 ms) or for B to resume (~1800 ms).
    console.info(`STALLED_HEAD_PROBE promotion=${queuedBeforePromotion} followerWaitMs=${during!.at - started}`);
    expect(during!.at - started).toBeLessThan(1000);
    expect(b.messages.some(message => "error" in message)).toBe(false);
    // C passed B; B was neither reaped nor reordered: its receipt was still the queue head during C's hold.
    expect(during!.owner).toBe(process.pid);
    expect(during!.receipts[0]).toBe(headReceipt);
    expect(Number(bAcquired.acquired)).toBeGreaterThanOrEqual(releasedAt);
    expect(reclaimFences(root)).toEqual([]);
    expect(receipts(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  }, 20_000);

  it("never bypasses a live slow .lock holder, even when its receipt looks stalled", async () => {
    const root = scratch();
    const h = spawn(root, 10_000);
    await h.next("ready");
    h.send({ cmd: "hold", ms: 1200 });
    const entered = await h.next("entered");
    expect(ownerPid(root)).toBe(h.process.pid);

    const follower = new MeshLock(root, { lockTimeoutMs: 2500 }, noop);
    let during: { at: number; inside: boolean; exitAt: number; owner: number; fences: string[] } | undefined;
    const f = follower.withLock(() => {
      during = {
        at: Date.now(), inside: fs.existsSync(path.join(root, "inside")),
        exitAt: Number(fs.readFileSync(path.join(root, "exit"), "utf8")), owner: ownerPid(root), fences: reclaimFences(root),
      };
    });
    // F queued behind the holder's receipt (kept through its critical section).
    expect(receipts(root).map(pidOf)).toEqual([h.process.pid, process.pid]);
    await f;
    await h.next("done");

    expect(during!.inside).toBe(false);
    expect(during!.exitAt).toBeGreaterThanOrEqual(Number(entered.entered));
    expect(during!.at).toBeGreaterThanOrEqual(during!.exitAt);
    expect(during!.owner).toBe(process.pid);
    expect(during!.fences).toEqual([]); // a live holder is never reclaimed
    expect(h.messages.some(message => "error" in message)).toBe(false);
    expect(receipts(root)).toEqual([]);
  }, 20_000);

  it("20 processes with random bounded stalls keep the wx owner-file invariant and all finish without timeout", async () => {
    const root = scratch();
    const crowd = Array.from({ length: 20 }, () => spawn(root, 10_000));
    await Promise.all(crowd.map(child => child.next("ready")));
    crowd.forEach((child, index) => child.send({ cmd: "stress", seed: 9805 + index, rounds: 3, queuedStallMs: 1200, holdStallMs: 20 }));
    const results = await Promise.all(crowd.map(child => child.next("result")));
    await Promise.all(crowd.map(child => child.next("done")));
    for (const message of results) expect(message.result).toEqual({ acquisitions: 3, violations: 0, errors: [] });
    console.info("OWNER_FILE_INVARIANT processes=20 acquisitions=60 overlaps=0 errors=0");
    expect(fs.existsSync(path.join(root, "holder"))).toBe(false);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(reclaimFences(root)).toEqual([]);
    expect(receipts(root)).toEqual([]);
  }, 60_000);
});
