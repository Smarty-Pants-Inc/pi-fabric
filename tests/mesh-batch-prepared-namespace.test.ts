import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshArchive, MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { FabricIdempotencyKeyReusedError } from "../src/mesh/control-idempotency.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import * as receiptNamespace from "../src/mesh/receipt-namespace.js";
import { MeshStore, type MeshEvent, type MeshPublishInput } from "../src/mesh/store.js";

// Keep reason: data safety. Independent before-implementation regression checks at the real
// MeshStore.publishBatch entry point: preparation cannot replace fresh crash fences or authorize
// a changed namespace. All successful OS calls are forwarded; EIO/mutations are deterministic.
// This is not a power-loss simulation. No bridge, remote store, subprocess, or signal is used.
const bases: string[] = [], stores: MeshStore[] = [];
const from = { id: "session:batch-namespace", name: "batch-namespace", kind: "main" as const };
const hash = (key: string) => createHash("sha256").update(key).digest("hex");
const keyed = (key: string): MeshPublishInput => ({ topic: "mesh.batch.namespace", from, dedupeKey: key, text: key });
const chain = (directory: string): string[] => {
  const result: string[] = [];
  for (let p = directory; ; p = path.dirname(p)) {
    result.push(p);
    if (path.dirname(p) === p) return result;
  }
};
const sorted = (values: string[]) => [...values].sort();
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const physical = (file: fs.PathLike | number): string => {
  const name = typeof file === "number" ? fs.readlinkSync(`/proc/self/fd/${file}`) : String(file);
  try { return path.join(fs.realpathSync(path.dirname(name)), path.basename(name)); }
  catch { return name; }
};
const eio = () => Object.assign(new Error("batch-namespace-fsync-EIO"), { code: "EIO" });

async function fixture(options: { alias?: boolean; receipts?: boolean } = {}) {
  // The runner pins a private child of TMPDIR; invoke with TMPDIR on tmpfs locally.
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "mesh-batch-namespace-"))); bases.push(base);
  const common = path.join(base, "common"), real = path.join(common, "real", "mesh");
  const archive = path.join(common, "archive");
  fs.mkdirSync(real, { recursive: true }); fs.mkdirSync(archive, { recursive: true });
  let root = real;
  if (options.alias) {
    // Distinct alias parent must be included by the full namespace resolver.
    fs.mkdirSync(path.join(common, "aliases"));
    fs.symlinkSync(path.dirname(real), path.join(common, "aliases", "link"), "dir");
    root = path.join(common, "aliases", "link", "mesh");
  }
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  const mesh = new MeshStore(root, 65536, 1000, { canWakeResidents: () => false }); stores.push(mesh);
  // Deterministic batch extent even on a slow filesystem; Date.now/createdAt remain real.
  vi.spyOn(performance, "now").mockReturnValue(0);
  await mesh.publish({ topic: "mesh.batch.namespace", from, text: "seed" });
  const receipts = path.join(real, "event-receipts"), live = path.join(real, "events.jsonl");
  if (options.receipts !== false) fs.mkdirSync(receipts, { recursive: true });
  const leaf = path.join(archive, "sequence-index", "0");
  return { base, common, root, real, archive, receipts, live, leaf, mesh, alias: !!options.alias };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;
const receipt = (f: Fixture, key: string, pending = false) => path.join(f.receipts, hash(key) + (pending ? ".pending.json" : ".json"));
const liveEvents = (f: Fixture): MeshEvent[] => fs.readFileSync(f.live, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
const fullReceiptChain = (f: Fixture) => [...new Set([
  ...chain(f.receipts), ...(f.alias ? chain(path.join(f.common, "aliases")) : []),
])];
interface Op {
  kind: "fsync" | "fdatasync" | "rename" | "append" | "rm" | "lock";
  file: string; held: boolean; directory?: boolean; action: string;
  sequence: number; phase: number; operation: string;
}
function observe(f: Fixture, hooks: { beforeLock?: () => void; sync?: (op: Op) => void; afterUnlink?: (file: string) => void } = {}) {
  const descriptors = new Map<number, string>(), ops: Op[] = [];
  const open = fs.openSync.bind(fs), close = fs.closeSync.bind(fs), sync = fs.fsyncSync.bind(fs);
  const data = fs.fdatasyncSync.bind(fs), rename = fs.renameSync.bind(fs), append = fs.appendFileSync.bind(fs);
  const rm = fs.rmSync.bind(fs), mkdir = fs.mkdirSync.bind(fs);
  let action = "", sequence = -1, phase = -1, attempted = false;
  const op = (kind: Op["kind"], file: string): Op => ({ kind, file, held: fs.existsSync(path.join(f.root, ".lock")), action, sequence, phase, operation: "" });
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    const fd = open(file, flags, mode); descriptors.set(fd, physical(file)); return fd;
  });
  vi.spyOn(fs, "closeSync").mockImplementation(fd => { try { close(fd); } finally { descriptors.delete(fd); } });
  vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
    if (String(file) === path.join(f.root, ".lock")) {
      ops.push(op("lock", path.join(f.real, ".lock")));
      if (!attempted) { attempted = true; hooks.beforeLock?.(); }
    }
    return mkdir(file, options);
  }) as typeof fs.mkdirSync);
  vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
    rename(source, target);
    const file = physical(target);
    if (path.dirname(file) === f.leaf && /^\d+\.json$/.test(path.basename(file))) {
      const entry = JSON.parse(fs.readFileSync(target, "utf8"));
      sequence = entry.sequence; phase = entry.absent ? 0 : entry.committed === false ? 1 : 2;
      action = "index";
    }
    if (path.dirname(file) === f.receipts) action = file.endsWith(".pending.json") ? "intent" : "receipt";
    ops.push(op("rename", file));
  });
  vi.spyOn(fs, "appendFileSync").mockImplementation(((file, ...args) => {
    (append as (...args: unknown[]) => void)(file, ...args); ops.push(op("append", physical(file)));
  }) as typeof fs.appendFileSync);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    const name = physical(file); rm(file, options);
    if (path.dirname(name) === f.receipts && name.endsWith(".pending.json")) action = "unlink";
    ops.push(op("rm", name));
    if (path.dirname(name) === f.receipts && name.endsWith(".pending.json")) hooks.afterUnlink?.(name);
  });
  const barrier = (fd: number, kind: "fsync" | "fdatasync") => {
    const entry = op(kind, descriptors.get(fd) ?? "unknown");
    entry.directory = fs.fstatSync(fd).isDirectory();
    const stack = new Error().stack ?? "";
    entry.operation = stack.includes("writeIndex") ? "index" : stack.includes("confirmEventFile") ? "live" : "other";
    hooks.sync?.(entry);
    (kind === "fsync" ? sync : data)(fd); ops.push(entry);
  };
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => barrier(fd, "fsync"));
  vi.spyOn(fs, "fdatasyncSync").mockImplementation(fd => barrier(fd, "fdatasync"));
  return ops;
}
const dirs = (ops: Op[]) => ops.filter(o => o.directory && o.kind === "fsync").map(o => o.file);
const indexDirs = (ops: Op[], sequence: number, phase: number) => dirs(ops.filter(o => o.held && o.operation === "index" && o.sequence === sequence && o.phase === phase));
const receiptDirs = (ops: Op[], action: string) => dirs(ops.filter(o => o.held && o.action === action));
function preparedBeforeLock(f: Fixture, ops: Op[]) {
  const lock = ops.findIndex(o => o.kind === "lock"); expect(lock).toBeGreaterThanOrEqual(0);
  const before = ops.slice(0, lock);
  expect(before.every(o => !o.held)).toBe(true);
  expect(sorted(dirs(before))).toEqual(sorted(fullReceiptChain(f)));
  expect(before.some(o => o.kind === "append" || o.kind === "rename")).toBe(false);
}
function assertStored(f: Fixture, events: MeshEvent[]) {
  const archive = new MeshArchive(f.archive, f.root);
  for (const event of events) {
    expect(liveEvents(f).filter(e => e.id === event.id)).toEqual([event]);
    expect(archive.lookup(event.sequence)).toEqual(event);
    if (event.dedupeKey) {
      expect(JSON.parse(fs.readFileSync(receipt(f, event.dedupeKey), "utf8"))).toEqual(event);
      expect(fs.existsSync(receipt(f, event.dedupeKey, true))).toBe(false);
    }
  }
}
// Assert the strict archived-keyed fence order, not just final success or aggregate sync counts.
function strictProtocol(f: Fixture, ops: Op[], event: MeshEvent) {
  const key = event.dedupeKey!, index = path.join(f.leaf, `${event.sequence}.json`);
  const indices = ops.flatMap((o, i) => o.kind === "rename" && o.file === index ? [i] : []);
  expect(indices).toHaveLength(3);
  expect(indices.map(i => ops[i]!.phase)).toEqual([0, 1, 2]);
  const intent = ops.findIndex(o => o.kind === "rename" && o.file === receipt(f, key, true));
  const received = ops.findIndex(o => o.kind === "rename" && o.file === receipt(f, key));
  const unlink = ops.findIndex(o => o.kind === "rm" && o.file === receipt(f, key, true));
  const append = ops.findIndex((o, i) => i > indices[1]! && o.kind === "append" && o.file === f.live);
  const data = ops.findIndex((o, i) => i > intent && o.kind === "fdatasync");
  const live = ops.findIndex((o, i) => i > indices[2]! && o.kind === "fsync" && o.file === f.live);
  const order = [indices[0]!, intent, data, indices[1]!, append, indices[2]!, live, received, unlink];
  expect(order.every(i => i >= 0)).toBe(true);
  expect(order).toEqual([...order].sort((a, b) => a - b));
  expect(new Set(order).size).toBe(order.length);
  expect(order.every(i => ops[i]!.held)).toBe(true);
  expect(ops[data]!.file.startsWith(f.archive + path.sep)).toBe(true);
  // Every durable rename has its own fresh temporary-file fsync before it.
  for (const at of [...indices, intent, received]) {
    const target = ops[at]!.file;
    const prior = ops.slice(0, at).reverse().find(o => o.kind === "fsync" && !o.directory);
    expect(prior?.file.startsWith(target + ".")).toBe(true); expect(prior?.held).toBe(true);
  }
  for (const [start, end] of [[intent, data], [received, unlink]] as const) {
    const barriers = ops.slice(start + 1, end).filter(o => o.directory);
    expect(barriers.some(o => o.file === f.receipts && o.held)).toBe(true);
  }
  const afterUnlink = ops.slice(unlink + 1);
  const nextFile = afterUnlink.findIndex(o => (o.kind === "fsync" || o.kind === "fdatasync") && !o.directory);
  const unlinkBarriers = nextFile < 0 ? afterUnlink : afterUnlink.slice(0, nextFile);
  expect(unlinkBarriers.some(o => o.directory && o.file === f.receipts && o.held)).toBe(true);
  // Three index files + intent + live + receipt, and the segment's data fence: none may disappear.
  const start = ops.findIndex(o => o.kind === "fsync" && o.file.startsWith(index + "."));
  const own = ops.slice(start, unlink + 1);
  expect(own.filter(o => o.kind === "fsync" && !o.directory && o.held)).toHaveLength(6);
  expect(own.filter(o => o.kind === "fdatasync" && o.held)).toHaveLength(1);
  for (const phase of [0, 1, 2]) expect(indexDirs(ops, event.sequence, phase)).toContain(f.leaf);
  for (const [start, end] of [[indices[0]!, intent], [indices[1]!, append], [indices[2]!, live]] as const) {
    expect(ops.slice(start + 1, end).some(o => o.directory && o.file === f.leaf && o.held)).toBe(true);
  }
  expect(ops.slice(live + 1, received).some(o => o.directory && o.file === f.real && o.held)).toBe(true);
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const base of bases.splice(0)) fs.rmSync(base, { recursive: true, force: true });
});

describe.skipIf(process.platform !== "linux")("MeshStore.publishBatch: prepared archived keyed namespace", () => {
  it.each([false, true])("prepares the complete namespace off-lock and pays three fresh locked leaves (existing=%s)", async existing => {
    const f = await fixture({ receipts: existing }), ops = observe(f);
    const published = await f.mesh.publishBatch([keyed("prepared-a"), keyed("prepared-b")]);
    expect(published).toHaveLength(2);
    for (const event of published) strictProtocol(f, ops, event);
    assertStored(f, published);
    preparedBeforeLock(f, ops);
    // Only receipt ancestors are borrowed; index/live fences are accounted independently above.
    for (const action of ["intent", "receipt", "unlink"]) expect(receiptDirs(ops, action)).toEqual([f.receipts, f.receipts]);
    const heldReceiptDirs = ops.filter(o => o.held && o.directory && ["intent", "receipt", "unlink"].includes(o.action) && o.file === f.receipts);
    expect(heldReceiptDirs).toHaveLength(6);
    const finalLive = ops.filter(o => o.kind === "fsync" && o.file === f.live && !o.held);
    expect(finalLive).toHaveLength(1); // Shared batch barrier is still after release.
    expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
  });

  it("keeps strict crash-fence order and all fresh file/data/leaf barriers", async () => {
    const f = await fixture(), ops = observe(f);
    const result = await f.mesh.publishBatch([keyed("fences")]);
    expect(result).toHaveLength(1); strictProtocol(f, ops, result[0]!); assertStored(f, result);
  });

  it.each(["receipt-directory", "symlink-ancestor"] as const)("uses complete receipt/index/live fallback after %s changes before lock", async mutation => {
    const f = await fixture({ alias: mutation === "symlink-ancestor" }); let changed = false;
    const ops = observe(f, { beforeLock: () => {
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      if (mutation === "receipt-directory") {
        fs.renameSync(f.receipts, `${f.receipts}-old`); fs.mkdirSync(f.receipts);
      } else {
        const link = path.join(f.common, "aliases", "link");
        // Preserve the old symlink so inode reuse cannot conceal replacement. Same endpoint!
        fs.renameSync(link, `${link}-old`); fs.symlinkSync(path.dirname(f.real), link, "dir");
      }
      changed = true;
    } });
    const result = await f.mesh.publishBatch([keyed("changed-a"), keyed("changed-b")]);
    expect(changed).toBe(true); expect(result).toHaveLength(2);
    for (const event of result) {
      strictProtocol(f, ops, event);
      for (const phase of [0, 1, 2]) expect(sorted(indexDirs(ops, event.sequence, phase))).toEqual(sorted(chain(f.leaf)));
    }
    for (const action of ["intent", "receipt", "unlink"]) {
      const paid = dirs(ops.filter(o => o.held && o.action === action));
      expect(sorted(paid)).toEqual(sorted([...fullReceiptChain(f), ...fullReceiptChain(f)]));
    }
    const liveChain = [...new Set([...chain(f.real), ...(f.alias ? chain(path.join(f.common, "aliases")) : [])])];
    for (const phase of [0, 1]) {
      const ownLive = ops.filter(o => o.operation === "live" && o.held && o.sequence === result[phase]!.sequence);
      expect(sorted(dirs(ownLive))).toEqual(sorted(liveChain));
    }
    assertStored(f, result); preparedBeforeLock(f, ops);
  });

  it("interleaved batch publishers revalidate a conflicting namespace before committing exactly once in lock order", async () => {
    const f = await fixture();
    const competitor = new MeshStore(f.root, 65536, 1000, { canWakeResidents: () => false }); stores.push(competitor);
    const ops = observe(f), prepared: Array<ReturnType<typeof receiptNamespace.prepareKeyedPublishNamespace>> = [];
    const prepare = receiptNamespace.prepareKeyedPublishNamespace;
    vi.spyOn(receiptNamespace, "prepareKeyedPublishNamespace").mockImplementation(root => {
      const proof = prepare(root); expect(proof.receiptNamespace).toBeDefined(); prepared.push(proof); return proof;
    });
    // Pause only at the acquisition boundary, forwarding the real lock/commit/release after
    // each gate opens. Both invocations finish off-lock preparation before either may acquire.
    const gates = [deferred(), deferred()];
    const ready = deferred(), boundaries: number[] = [];
    const withLock = MeshLock.prototype.withLock;
    const locks = vi.spyOn(MeshLock.prototype, "withLock").mockImplementation(async function<T>(
      this: MeshLock, operation: () => T, timeout?: number, lockClass?: Parameters<MeshLock["withLock"]>[2],
      bounded?: boolean, timing?: Parameters<MeshLock["withLock"]>[4],
    ): Promise<T> {
      if (this.root === f.root && boundaries.length < 2) {
        const turn = boundaries.length; boundaries.push(ops.length);
        expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
        if (boundaries.length === 2) ready.resolve();
        await gates[turn]!.promise;
      }
      return withLock.call(this, operation, timeout, lockClass, bounded, timing) as Promise<T>;
    });
    const firstInputs = [keyed("competing-a"), keyed("competing-b")];
    const secondInputs = [keyed("competing-c"), keyed("competing-d")];
    const first = f.mesh.publishBatch(firstInputs), second = competitor.publishBatch(secondInputs);
    try {
      await ready.promise;
      expect(prepared).toHaveLength(2);
      expect(prepared[0]!.receiptNamespace!.key).toBe(prepared[1]!.receiptNamespace!.key);
      for (const [start, end] of [[0, boundaries[0]!], [boundaries[0]!, boundaries[1]!]]) {
        const preparation = ops.slice(start, end);
        expect(preparation.every(o => !o.held)).toBe(true);
        expect(sorted(dirs(preparation))).toEqual(sorted(fullReceiptChain(f)));
        expect(preparation.some(o => ["lock", "append", "rename"].includes(o.kind))).toBe(false);
      }
      gates[0]!.resolve();
      const winner = await first;
      expect(winner.map(e => e.sequence)).toEqual([2, 3]); assertStored(f, winner);
      for (const event of winner) strictProtocol(f, ops, event);
      // Change identity while the other publisher still holds its old proof, preserving every
      // winner receipt. Keeping the old directory alive excludes accidental inode reuse.
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      const oldIdentity = fs.statSync(f.receipts);
      fs.renameSync(f.receipts, `${f.receipts}-old`); fs.mkdirSync(f.receipts);
      for (const name of fs.readdirSync(`${f.receipts}-old`)) fs.renameSync(path.join(`${f.receipts}-old`, name), path.join(f.receipts, name));
      expect(fs.statSync(f.receipts).ino).not.toBe(oldIdentity.ino);
      const delayedStart = ops.length;
      gates[1]!.resolve();
      const delayed = await second, delayedOps = ops.slice(delayedStart);
      expect(delayed.map(e => e.sequence)).toEqual([4, 5]);
      expect(prepared).toHaveLength(2); // No new proof hides the stale-proof fallback.
      expect(ops.filter(o => o.kind === "lock")).toHaveLength(2);
      for (const event of delayed) {
        strictProtocol(f, delayedOps, event);
        for (const phase of [0, 1, 2]) expect(sorted(indexDirs(delayedOps, event.sequence, phase))).toEqual(sorted(chain(f.leaf)));
        expect(sorted(dirs(delayedOps.filter(o => o.held && o.operation === "live" && o.sequence === event.sequence)))).toEqual(sorted(chain(f.real)));
        for (const action of ["intent", "receipt", "unlink"]) {
          expect(sorted(dirs(delayedOps.filter(o => o.held && o.sequence === event.sequence && o.action === action)))).toEqual(sorted(fullReceiptChain(f)));
        }
      }
      const all = [...winner, ...delayed]; assertStored(f, all);
      expect(liveEvents(f).slice(1)).toEqual(all); expect(f.mesh.latestSequence()).toBe(5);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      locks.mockRestore();
      const bytes = fs.readFileSync(f.live, "utf8");
      expect(await f.mesh.publishBatch(firstInputs)).toEqual(winner);
      expect(await competitor.publishBatch(secondInputs)).toEqual(delayed);
      expect(fs.readFileSync(f.live, "utf8")).toBe(bytes); assertStored(f, all);
    } finally {
      for (const gate of gates) gate.resolve();
      await Promise.allSettled([first, second]);
    }
  });

  it("an abort after successful namespace preparation throws before locking without partial state or leaked intent; following publish works", async () => {
    const f = await fixture(), ops = observe(f), controller = new AbortController();
    const inputs = [keyed("after-preparation-a"), keyed("after-preparation-b")];
    const sequence = f.mesh.latestSequence();
    const snapshot = () => [f.real, f.archive].map(directory => fs.readdirSync(directory, { recursive: true })
      .map(name => String(name)).sort().filter(name => fs.statSync(path.join(directory, name)).isFile())
      .map(name => [name, fs.readFileSync(path.join(directory, name), "hex")]));
    const before = snapshot(), prepare = receiptNamespace.prepareKeyedPublishNamespace;
    const failure = new Error("batch-aborted-after-preparation");
    const preparation = vi.spyOn(receiptNamespace, "prepareKeyedPublishNamespace").mockImplementationOnce(root => {
      const proof = prepare(root);
      expect(proof.receiptNamespace).toBeDefined();
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      expect(sorted(dirs(ops))).toEqual(sorted(fullReceiptChain(f)));
      // Return the genuine completed proof; the real pre-lock input check throws next.
      controller.abort(failure); return proof;
    });
    await expect(f.mesh.publishBatch(inputs.map(input => ({ ...input, signal: controller.signal })))).rejects.toBe(failure);
    expect(preparation).toHaveBeenCalledTimes(1);
    expect(ops.every(o => !o.held)).toBe(true);
    expect(ops.some(o => ["lock", "append", "rename"].includes(o.kind))).toBe(false);
    expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
    expect(fs.readdirSync(meshLockQueueDirectory(f.root))).toEqual([]);
    expect(snapshot()).toEqual(before); expect(f.mesh.latestSequence()).toBe(sequence);
    for (const input of inputs) for (const pending of [false, true]) expect(fs.existsSync(receipt(f, input.dedupeKey!, pending))).toBe(false);
    const result = await f.mesh.publishBatch(inputs);
    expect(preparation).toHaveBeenCalledTimes(2); // A fresh invocation prepares again, not a leaked proof.
    expect(result.map(e => e.sequence)).toEqual([sequence + 1, sequence + 2]);
    for (const event of result) strictProtocol(f, ops, event);
    assertStored(f, result);
    const bytes = fs.readFileSync(f.live, "utf8");
    expect(await f.mesh.publishBatch(inputs)).toEqual(result);
    expect(fs.readFileSync(f.live, "utf8")).toBe(bytes); assertStored(f, result);
    expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
  });

  it("off-lock preparation fsync EIO rejects before lock, reservation, archive writes, or append; retry is exact-once", async () => {
    const f = await fixture(); let faults = 0;
    const before = fs.readFileSync(f.live, "utf8"), sequence = f.mesh.latestSequence();
    const head = fs.readFileSync(path.join(f.archive, "HEAD.json"), "utf8");
    const ops = observe(f, { sync: o => {
      if (!o.held && o.directory && o.file === f.real && faults === 0) { faults++; throw eio(); }
    } });
    await expect(f.mesh.publishBatch([keyed("prep-failure"), keyed("not-started")])).rejects.toThrow("batch-namespace-fsync-EIO");
    expect(faults).toBe(1); expect(ops.some(o => o.kind === "lock" || o.kind === "append" || o.kind === "rename")).toBe(false);
    expect(fs.readFileSync(f.live, "utf8")).toBe(before); expect(f.mesh.latestSequence()).toBe(sequence);
    expect(fs.readFileSync(path.join(f.archive, "HEAD.json"), "utf8")).toBe(head);
    expect(fs.existsSync(receipt(f, "prep-failure", true))).toBe(false);
    const result = await f.mesh.publishBatch([keyed("prep-failure")]);
    expect(await f.mesh.publishBatch([keyed("prep-failure")])).toEqual(result); assertStored(f, result);
  });

  it("mixed unkeyed/keyed events and same-key retries preserve identities, bytes, monotonic sequences and fingerprints", async () => {
    const f = await fixture(), ops = observe(f);
    const first = { ...keyed("same-key"), dedupeFingerprint: "a".repeat(64), data: { value: 1 } };
    const plain = (text: string): MeshPublishInput => ({ topic: "mesh.batch.namespace", from, text });
    const result = await f.mesh.publishBatch([plain("plain-a"), first, keyed("other-key"), plain("plain-b"), first]);
    expect(result).toHaveLength(5); expect(result[4]).toEqual(result[1]);
    const unique = result.slice(0, 4);
    expect(unique.map(e => e.sequence)).toEqual([2, 3, 4, 5]); assertStored(f, unique);
    for (const event of unique.filter(e => e.dedupeKey)) strictProtocol(f, ops, event);
    expect(ops.filter(o => o.kind === "rename" && o.file === receipt(f, "same-key"))).toHaveLength(1);
    const bytes = fs.readFileSync(f.live, "utf8"), saved = fs.readFileSync(receipt(f, "same-key"), "utf8");
    expect(await f.mesh.publishBatch([first, keyed("other-key"), first])).toEqual([result[1], result[2], result[1]]);
    await expect(f.mesh.publishBatch([{ ...first, dedupeFingerprint: "b".repeat(64), data: { value: 2 } }])).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
    expect(fs.readFileSync(f.live, "utf8")).toBe(bytes); expect(fs.readFileSync(receipt(f, "same-key"), "utf8")).toBe(saved);
    expect(f.mesh.latestSequence()).toBe(5); preparedBeforeLock(f, ops);
  });

  it("a namespace replacement during the batch makes later events pay all old barriers without losing earlier receipts", async () => {
    const f = await fixture(); let changed = false;
    const ops = observe(f, { afterUnlink: file => {
      if (changed || file !== receipt(f, "mid-a", true)) return;
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(true);
      fs.renameSync(f.receipts, `${f.receipts}-old`); fs.mkdirSync(f.receipts);
      // Preserve all existing evidence, changing only the enclosing directory identity.
      for (const name of fs.readdirSync(`${f.receipts}-old`)) fs.renameSync(path.join(`${f.receipts}-old`, name), path.join(f.receipts, name));
      changed = true;
    } });
    const result = await f.mesh.publishBatch([keyed("mid-a"), keyed("mid-b"), keyed("mid-c")]);
    expect(changed).toBe(true); expect(result).toHaveLength(3);
    for (const event of result) strictProtocol(f, ops, event);
    for (const event of result.slice(1)) {
      for (const phase of [0, 1, 2]) expect(sorted(indexDirs(ops, event.sequence, phase))).toEqual(sorted(chain(f.leaf)));
      expect(sorted(dirs(ops.filter(o => o.held && o.operation === "live" && o.sequence === event.sequence)))).toEqual(sorted(chain(f.real)));
      for (const action of ["intent", "receipt", "unlink"]) {
        expect(sorted(dirs(ops.filter(o => o.held && o.sequence === event.sequence && o.action === action)))).toEqual(sorted(fullReceiptChain(f)));
      }
    }
    assertStored(f, result);
    expect(await f.mesh.publishBatch([keyed("mid-a"), keyed("mid-b"), keyed("mid-c")])).toEqual(result);
    preparedBeforeLock(f, ops);
  });

  it.each(["intent-file", "intent-leaf", "archive-data", "live-file", "live-leaf", "receipt-file", "receipt-leaf", "unlink-leaf"] as const)(
    "%s EIO is not acknowledged; persisted evidence supports an exact-identity retry", async cut => {
      const f = await fixture(); let faults = 0, acknowledged = false;
      observe(f, { sync: o => {
        const hit = cut === "intent-file" ? !o.directory && o.file.startsWith(receipt(f, "cut", true) + ".")
          : cut === "intent-leaf" ? o.directory && o.file === f.receipts && o.action === "intent"
          : cut === "archive-data" ? o.kind === "fdatasync"
          : cut === "live-file" ? !o.directory && o.file === f.live
          : cut === "live-leaf" ? o.directory && o.file === f.real && o.operation === "live"
          : cut === "receipt-file" ? !o.directory && o.file.startsWith(receipt(f, "cut") + ".")
          : o.directory && o.file === f.receipts && o.action === (cut === "receipt-leaf" ? "receipt" : "unlink");
        if (o.held && hit && faults === 0) { faults++; throw eio(); }
      } });
      await expect(f.mesh.publishBatch([keyed("cut")]).then(result => { acknowledged = true; return result; })).rejects.toThrow(/batch-namespace-fsync-EIO|outcome is uncertain/);
      expect(acknowledged).toBe(false); expect(faults).toBe(1); expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      const prior = liveEvents(f).filter(e => e.dedupeKey === "cut");
      if (cut === "intent-file" || cut === "intent-leaf" || cut === "archive-data") expect(prior).toEqual([]);
      if (cut === "receipt-file") expect(fs.existsSync(receipt(f, "cut", true))).toBe(true);
      if (cut === "unlink-leaf") expect(JSON.parse(fs.readFileSync(receipt(f, "cut"), "utf8"))).toEqual(prior[0]);
      const reopened = new MeshStore(f.root, 65536, 1000, { canWakeResidents: () => false }); stores.push(reopened);
      const result = await reopened.publishBatch([keyed("cut")]);
      expect(result).toHaveLength(1); if (prior.length) expect(result).toEqual(prior);
      expect(await reopened.publishBatch([keyed("cut")])).toEqual(result); assertStored(f, result);
    });
});
