import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";

const roots: string[] = [];
const policies = [...new Set([process.platform, "win32"])] as NodeJS.Platform[];
const identity: MeshIdentity = { id: "session:receipt-main", name: "receipt-main", kind: "main", sessionId: "receipt-main" };
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const deliveryKey = (id: string) => `residency/deliveries/${hash("recipient").slice(0, 32)}/${id}`;
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const setup = (count: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-receipt-pass-"));
  roots.push(root);
  const old = Date.now() - 7 * 60 * 60 * 1000;
  const dir = path.join(root, "agent-completions", "receipts");
  fs.mkdirSync(dir, { recursive: true });
  const entries: Record<string, MeshStateEntry> = {};
  for (let n = 0; n < count; n++) {
    const id = String(n).padStart(4, "0");
    const key = deliveryKey(id);
    entries[key] = { key, version: n + 1, updatedAt: old, updatedBy: identity, value: {
      format: 1, id, rootId: "recipient", createdAt: old, from: { id, kind: "agent" }, agentCompletionId: id,
      message: "backlog".repeat(640),
    } };
    fs.writeFileSync(path.join(dir, `${hash(id)}.json`), JSON.stringify({ id, sessionId: "original", consumedAt: old }));
  }
  fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries }));
  return { root, dir, mesh: new MeshStore(root, 64 * 1024, 1000), receipt: path.join(dir, `${hash("0000")}.json`) };
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.each(policies)("Astra F5 confirmed namespace handoff (%s)", policy => {
  beforeEach(() => { vi.spyOn(process, "platform", "get").mockReturnValue(policy); });
  const endpoint = (file: string) => {
    const stat = fs.statSync(file);
    return [stat.dev, stat.ino, stat.size, stat.mtimeMs, stat.ctimeMs];
  };

  it("retains a delivery after a lock-acquisition ancestor swap until the new namespace is confirmed", async () => {
    const { root, dir, mesh, receipt } = setup(1);
    const before = endpoint(receipt);
    const mkdir = fs.mkdirSync.bind(fs);
    let swapped = false;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const result = (mkdir as (...args: unknown[]) => unknown)(file, ...args);
      if (!swapped && String(file) === path.join(root, ".lock")) {
        swapped = true;
        fs.renameSync(dir, `${dir}-moved`);
        fs.symlinkSync(`${dir}-moved`, dir, process.platform === "win32" ? "junction" : "dir");
        expect(endpoint(receipt)).toEqual(before);
      }
      return result;
    }) as typeof fs.mkdirSync);
    expect(await mesh.compactReceipts()).toBe(0); // new link has no confirmed barrier
    expect(swapped).toBe(true);
    expect(mesh.get(deliveryKey("0000"))).toBeDefined();

    const open = fs.promises.open.bind(fs.promises);
    const barrier = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (String(args[0]) === `${dir}-moved`) throw Object.assign(new Error("new namespace barrier failed"), { code: "EIO" });
      return open(...args);
    });
    if (process.platform !== "win32") {
      expect(await mesh.compactReceipts()).toBe(0);
      expect(mesh.get(deliveryKey("0000"))).toBeDefined();
    }
    barrier.mockRestore();
    expect(endpoint(receipt)).toEqual(before);
    expect(await mesh.compactReceipts()).toBe(1); // fresh successful confirmation only
  });

  it("retains an earlier confirmed candidate when a later candidate's replacement namespace barrier fails", async () => {
    const { dir, mesh, receipt } = setup(2);
    const before = endpoint(receipt);
    const later = path.join(dir, `${hash("0001")}.json`);
    const open = fs.promises.open.bind(fs.promises);
    const rename = fs.renameSync.bind(fs);
    let swapped = false, receiptHandles = 0;
    // Reproduce Windows' open-handle rename denial even on a POSIX runner.
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === dir && receiptHandles > 0) throw Object.assign(new Error("open receipt prevents directory rename"), { code: "EPERM" });
      return rename(from, to);
    });
    const barrier = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (swapped && String(args[0]) === `${dir}-moved`) throw Object.assign(new Error("later namespace barrier failed"), { code: "EIO" });
      const swap = !swapped && String(args[0]) === later;
      if (swap) {
        // The first candidate has finished confirmation. Swap BEFORE opening the
        // second receipt: Windows can deny renaming its directory with that handle
        // open. A failed injection otherwise leaves the first namespace valid and
        // leaks the handle, never exercising the intended custody handoff.
        fs.renameSync(dir, `${dir}-moved`);
        fs.symlinkSync(`${dir}-moved`, dir, process.platform === "win32" ? "junction" : "dir");
        swapped = true;
        expect(endpoint(receipt)).toEqual(before);
      }
      const handle = await open(...args);
      if (String(args[0]).startsWith(dir + path.sep)) {
        receiptHandles++;
        const close = handle.close.bind(handle);
        handle.close = async () => { try { await close(); } finally { receiptHandles--; } };
      }
      if (swap && process.platform === "win32") handle.sync = async () => { throw new Error("later confirmation failed"); };
      return handle;
    });
    // Deterministic attempt budget: a busy host must still reach the second receipt.
    vi.spyOn(performance, "now").mockReturnValue(0);
    expect(await mesh.compactReceipts()).toBe(0);
    expect(swapped).toBe(true);
    expect(receiptHandles).toBe(0);
    expect(mesh.get(deliveryKey("0000"))).toBeDefined();
    expect(mesh.get(deliveryKey("0001"))).toBeDefined();
    barrier.mockRestore();
    expect(await mesh.compactReceipts()).toBe(2);
  });

  it("rejects unreadable ancestors and replacement links to the same directory inode", async () => {
    const { dir, root, receipt } = setup(1);
    fs.renameSync(dir, `${dir}-moved`);
    fs.symlinkSync(`${dir}-moved`, dir, process.platform === "win32" ? "junction" : "dir");
    const { prepareReceiptCompaction, receiptCandidateUnchanged } = await import("../src/mesh/state-retention.js");
    const entry = JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8")).entries[deliveryKey("0000")] as MeshStateEntry;
    const prepared = await prepareReceiptCompaction([entry], root, Date.now(), undefined, Infinity);
    expect(prepared.confirmed).toHaveLength(1);
    const candidate = prepared.confirmed[0]!;
    expect(receiptCandidateUnchanged(candidate, entry)).toBe(true);
    const lstat = fs.lstatSync.bind(fs);
    const unreadable = vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (String(file) === path.dirname(dir)) throw Object.assign(new Error("unreadable ancestor"), { code: "EACCES" });
      return (lstat as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.lstatSync);
    expect(receiptCandidateUnchanged(candidate, entry)).toBe(false);
    unreadable.mockRestore();
    expect(receiptCandidateUnchanged(candidate, entry)).toBe(true);
    const before = endpoint(receipt);
    // Retarget a new link to an equivalent spelling of the very same endpoint.
    fs.unlinkSync(dir);
    fs.symlinkSync(`${dir}-moved${path.sep}.`, dir, process.platform === "win32" ? "junction" : "dir");
    expect(endpoint(receipt)).toEqual(before);
    expect(receiptCandidateUnchanged(candidate, entry)).toBe(false);
  });
});

describe.each(policies)("Astra F4 unlocked bounded receipt passes (%s)", policy => {
  beforeEach(() => { vi.spyOn(process, "platform", "get").mockReturnValue(policy); });
  it("keeps Main's heartbeat, foreground and a competing real mutation responsive with a 3 MB backlog", async () => {
    const { root, dir, mesh } = setup(600);
    // Control only scheduling/admission time. Receipt reads, barriers, mutation
    // locks, publications and competitor writes remain real filesystem work.
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    const realNow = performance.now.bind(performance);
    const { RECEIPT_PASS_BUDGET_MS } = await import("../src/mesh/state-retention.js");
    const budgetClock = vi.spyOn(performance, "now").mockReturnValue(0);
    const competitor = new MeshStore(root, 64 * 1024, 1000, { lockTimeoutMs: 500 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, identity, rootId: identity.id, hostId: identity.id, heartbeatMs: 100, reapDeadHosts: false,
    });
    const main = new MainAgentController({ getThinkingLevel: () => "off" } as ExtensionAPI, identity.id, true, root, identity.sessionId!);
    directory.registerSource(() => [directory.root(main.info())]);
    const passSpy = vi.spyOn(mesh, "compactReceipts");
    const started = deferred(), release = deferred();
    const open = fs.promises.open.bind(fs.promises);
    let receiptAttempts = 0, barriers = 0;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      // Includes directory barriers on platforms that support them.
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      const handle = await open(...args);
      const sync = handle.sync.bind(handle);
      const receipt = String(args[0]).startsWith(dir + path.sep);
      if (receipt) receiptAttempts++;
      handle.sync = async () => {
        expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
        barriers++;
        if (receipt) { started.resolve(); await release.promise; }
        await sync();
      };
      return handle;
    });
    const holds: number[] = [];
    let acquired: number | undefined;
    const mkdir = fs.mkdirSync.bind(fs), rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const result = (mkdir as (...args: unknown[]) => unknown)(file, ...args);
      if (String(file) === path.join(root, ".lock")) acquired = realNow();
      return result;
    }) as typeof fs.mkdirSync);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      const result = rename(from, to);
      if (String(from) === path.join(root, ".lock") && acquired !== undefined) {
        holds.push(realNow() - acquired); acquired = undefined;
      }
      return result;
    });
    let foregroundTicks = 0;
    const timer = setInterval(() => { foregroundTicks++; }, 10);
    try {
      await directory.start();
      expect(passSpy).not.toHaveBeenCalled(); // no timer tick on startup/foreground stack
      const initialLease = readHostLeases(root).get(identity.id)!.updatedAt;
      vi.advanceTimersByTime(100); // invoke the production heartbeat callback
      await directory.refresh();
      await started.promise;
      const beforeHeartbeats = holds.length;
      for (let tick = 0; tick < 3; tick++) {
        vi.advanceTimersByTime(100);
        await directory.refresh(); // completion, not a wall-clock sleep, proves renewal
        expect(directory.canConsumeMesh()).toBe(true);
      }
      expect(holds).toHaveLength(beforeHeartbeats); // blocked receipt work takes no lock
      expect(passSpy).toHaveBeenCalledTimes(1); // heartbeats never drain/restart the backlog
      const beforeMutations = holds.length;
      await expect(mesh.put({ key: "probe/foreground-reply", value: true, identity })).resolves.toBeDefined();
      await expect(competitor.put({ key: "probe/competing-mutation", value: true, identity })).resolves.toBeDefined();
      await expect(competitor.writeBatch({ identity, ops: [{ kind: "put", key: "probe/batch", value: true }] })).resolves.toHaveLength(1);
      await expect(competitor.delete({ key: "probe/batch" })).resolves.toMatchObject({ deleted: true });
      expect(holds).toHaveLength(beforeMutations + 4); // one real lock per public mutation
      expect(foregroundTicks).toBe(40);
      expect(readHostLeases(root).get(identity.id)!.updatedAt).toBeGreaterThan(initialLease);
      expect(directory.canConsumeMesh()).toBe(true);
      expect(receiptAttempts).toBe(1);
      const pass = passSpy.mock.results[0]!.value as Promise<number>;
      expect(mesh.compactReceipts()).toBe(pass); // coalesces with the still-running timer pass
      // Exhaust the scheduling budget explicitly: exactly one started receipt,
      // independently of descheduling/GC or how fast the real barriers complete.
      budgetClock.mockReturnValue(RECEIPT_PASS_BUDGET_MS + 1);
      const beforeCommit = holds.length;
      release.resolve();
      expect(await pass).toBe(1);
      expect(holds).toHaveLength(beforeCommit + 1); // one bounded maintenance commit
      expect(receiptAttempts).toBe(1);
      expect(barriers).toBeGreaterThan(0);
      expect(mesh.get(deliveryKey("0000"))).toBeUndefined();
      expect(mesh.listAll("residency/deliveries/")).toHaveLength(599);
      expect(passSpy).toHaveBeenCalledTimes(2); // no immediate follow-on pass
    } finally {
      clearInterval(timer); release.resolve(); await directory.close();
    }
    expect(Math.max(...holds)).toBeLessThan(5_000); // gross-regression sanity; counts are the gate
    process.stdout.write(JSON.stringify({ probe: "F4 blocked receipt pass", policy, receiptAttempts, barriers,
      foregroundTicks, maxLockHoldMs: Math.max(...holds) }) + "\n");
  });

  it.each(["confirmed", "failed"] as const)("bounds %s attempts and mutation-lock holds per pass with a frozen budget clock", async outcome => {
    const { root, dir, mesh } = setup(40);
    vi.spyOn(performance, "now").mockReturnValue(0); // exercise count limit, never incidental elapsed time
    const open = fs.promises.open.bind(fs.promises), mkdir = fs.mkdirSync.bind(fs);
    let attempts = 0, locks = 0;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      if (String(args[0]).startsWith(dir + path.sep)) {
        attempts++;
        if (outcome === "failed") throw Object.assign(new Error("receipt temporarily unreadable"), { code: "EACCES" });
      }
      return open(...args);
    });
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const result = Reflect.apply(mkdir, fs, [file, ...args]);
      if (String(file) === path.join(root, ".lock")) locks++;
      return result;
    }) as typeof fs.mkdirSync);
    const work: Array<{ attempts: number; locks: number }> = [];
    for (const expected of [16, 16, 8]) {
      const beforeAttempts = attempts, beforeLocks = locks;
      expect(await mesh.compactReceipts()).toBe(outcome === "confirmed" ? expected : 0);
      work.push({ attempts: attempts - beforeAttempts, locks: locks - beforeLocks });
      expect(work.at(-1)).toEqual({ attempts: expected, locks: outcome === "confirmed" ? 1 : 0 });
    }
    expect(mesh.listAll("residency/deliveries/")).toHaveLength(outcome === "confirmed" ? 0 : 40);
    if (outcome === "failed") {
      const before = attempts;
      expect(await mesh.compactReceipts()).toBe(0);
      expect(attempts - before).toBe(16); // exhausted cursor wraps only on a later pass
      expect(locks).toBe(0);
    }
    process.stdout.write(JSON.stringify({ probe: "F4 count-bounded receipt passes", policy, outcome, work }) + "\n");
  });

  it("bounds ineligible-prefix scanning independently of elapsed time and wraps on a later pass", async () => {
    const { root, mesh } = setup(600);
    const { prepareReceiptCompaction } = await import("../src/mesh/state-retention.js");
    const entries = mesh.listAll("residency/deliveries/");
    let scanned = 0;
    for (const entry of entries) Object.defineProperty(entry, "value", { get: () => { scanned++; return { format: 0 }; } });
    const open = vi.spyOn(fs.promises, "open");
    const first = await prepareReceiptCompaction(entries, root, Date.now(), undefined, Infinity);
    expect(scanned).toBe(500);
    expect(first.confirmed).toEqual([]);
    expect(first.after).toBe(entries[499]!.key);
    const second = await prepareReceiptCompaction(entries, root, Date.now(), first.after, Infinity);
    expect(scanned).toBe(600);
    expect(second).toEqual({ confirmed: [], after: undefined });
    expect(open).not.toHaveBeenCalled(); // malformed backlog never owes a receipt barrier
  });

  it("retains a delivery changed by a competing writer while unlocked confirmation is in flight", async () => {
    const { root, dir, mesh } = setup(1);
    const competitor = new MeshStore(root, 64 * 1024, 1000);
    const started = deferred(), release = deferred();
    const open = fs.promises.open.bind(fs.promises);
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      if (String(args[0]).startsWith(dir + path.sep)) {
        const sync = handle.sync.bind(handle);
        handle.sync = async () => { started.resolve(); await release.promise; await sync(); };
      }
      return handle;
    });
    const pass = mesh.compactReceipts();
    try {
      await started.promise;
      await competitor.put({ key: deliveryKey("0000"), value: { custody: "changed" }, identity });
    } finally { release.resolve(); await pass; }
    expect(mesh.get(deliveryKey("0000"))!.value).toEqual({ custody: "changed" });
  });

  it("retains a receipt replaced between unlocked confirmation and cheap locked revalidation", async () => {
    const { root, mesh, receipt } = setup(1);
    const mkdir = fs.mkdirSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const result = (mkdir as (...args: unknown[]) => unknown)(file, ...args);
      if (String(file) === path.join(root, ".lock")) fs.writeFileSync(receipt, JSON.stringify({ id: "0000", sessionId: "other", consumedAt: Date.now() }));
      return result;
    }) as typeof fs.mkdirSync);
    expect(await mesh.compactReceipts()).toBe(0);
    expect(mesh.get(deliveryKey("0000"))).toBeDefined();
  });

  it("makes just an immediate background lock attempt and reconfirms on a later pass", async () => {
    const { root, mesh } = setup(1);
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    await expect(mesh.compactReceipts()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 1 });
    expect(mesh.get(deliveryKey("0000"))).toBeDefined();
    fs.rmSync(lock, { recursive: true });
    expect(await mesh.compactReceipts()).toBe(1);
  });
});
