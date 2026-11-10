import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshDedupeStoreFullError } from "../src/mesh/event-log.js";
import * as diagnosticLog from "../src/core/diagnostics.js";

const roots: string[] = [];
const children = new Set<ChildProcess>();
const from = { id: "session:prune", name: "prune", kind: "main" as const };
const packet = (dedupeKey: string) => ({ topic: "mesh.prune", from, dedupeKey });
const receipt = (root: string, key: string, suffix = ".json") =>
  path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-prune-lock-"));
  roots.push(root);
  return root;
};
const storeAt = (root: string, maxDedupeReceipts = 2) => new MeshStore(root, 1024, 100, {
  maxEventLogBytes: 64 * 1024, maxDedupeReceipts,
});

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all([...children].map(child => new Promise<void>(resolve => {
    if (child.exitCode !== null || child.signalCode !== null) { resolve(); return; }
    child.once("exit", () => resolve());
    child.kill();
  })));
  children.clear();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("receipt pruning outside mesh custody", () => {
  // KEEP: a refused control publication must not destroy an earlier command's idempotency evidence.
  it.each(["admit", "fence", "fixed-size", "stamp", "stamp-size", "outbox"] as const)("preserves the at-cap receipt after pre-append %s refusal", async refusal => {
    const root = scratch(), store = storeAt(root, 1);
    const first = await store.publish(packet("first"));
    const before = fs.readFileSync(receipt(root, "first"));
    const reject = vi.fn(() => { throw new Error("publication refused"); });
    const stamp = vi.fn(() => refusal === "stamp" ? reject() : "x".repeat(2048));
    const binding = refusal === "outbox" ? store.bindEventPublisher("refusal", async () => undefined, reject) : undefined;
    try {
      await expect(store.publish({ ...packet("rejected"),
        ...(refusal === "admit" ? { admit: reject } : {}),
        ...(refusal === "fence" ? { fence: reject } : {}),
        ...(refusal === "fixed-size" ? { text: "x".repeat(2048) } : {}),
        ...(refusal === "stamp" || refusal === "stamp-size" ? { data: stamp } : {}),
      })).rejects.toThrow(refusal.endsWith("size") ? "Mesh event exceeds" : "publication refused");
      expect(fs.existsSync(receipt(root, "first"))).toBe(true);
      expect(fs.readFileSync(receipt(root, "first"))).toEqual(before);
      expect(await store.publish(packet("first"))).toEqual(first);
      expect(store.read()).toEqual([first]);
      expect(store.latestSequence()).toBe(first.sequence);
      expect(fs.existsSync(receipt(root, "rejected", ".pending.json"))).toBe(false);
      if (refusal === "stamp" || refusal === "stamp-size") expect(stamp).toHaveBeenCalledTimes(1);
      if (!refusal.endsWith("size")) expect(reject).toHaveBeenCalledTimes(1);
    } finally { await binding?.close(); }
  });

  it("still evicts within TTL only after the new key passes admission and validation", async () => {
    const root = scratch(), store = storeAt(root, 1);
    const first = await store.publish(packet("first"));
    const admit = vi.fn(() => expect(fs.existsSync(receipt(root, "first"))).toBe(true));
    const stamp = vi.fn(() => { expect(fs.existsSync(receipt(root, "first"))).toBe(true); return { accepted: true }; });
    const accepted = await store.publish({ ...packet("accepted"), admit, data: stamp });
    expect(admit).toHaveBeenCalledTimes(1);
    expect(stamp).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(receipt(root, "first"))).toBe(false);
    expect(await store.publish(packet("accepted"))).toEqual(accepted);
    expect(store.read()).toEqual([first, accepted]);
  });

  it.each(["renewal", "pending"] as const)("does not replay admission or retire a planned victim changed by %s during validation", async mutation => {
    const root = scratch(), store = storeAt(root, 1);
    const first = await store.publish(packet("first"));
    const renewed = { ...first, id: "renewed", createdAt: first.createdAt + 1000 };
    const admit = vi.fn(() => {
      if (mutation === "renewal") {
        const temporary = path.join(root, "renewed");
        fs.writeFileSync(temporary, JSON.stringify(renewed));
        fs.renameSync(temporary, receipt(root, "first"));
      } else fs.writeFileSync(receipt(root, "first", ".pending.json"), JSON.stringify({
        dedupeKey: "first", eventId: first.id, reservedSequence: first.sequence, liveOffset: 0,
      }));
    });
    await expect(store.publish({ ...packet("rejected"), admit })).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
    expect(admit).toHaveBeenCalledTimes(1);
    expect(JSON.parse(fs.readFileSync(receipt(root, "first"), "utf8"))).toEqual(mutation === "renewal" ? renewed : first);
    expect(fs.existsSync(receipt(root, "first", ".pending.json"))).toBe(mutation === "pending");
    expect(store.read()).toEqual([first]);
    expect(store.latestSequence()).toBe(first.sequence);
  });

  it.each([1, 2])("keeps the 32-victim admission bound when an existing store lowers its cap to %s", async cap => {
    const root = scratch(), seed = storeAt(root, 40);
    for (let index = 0; index < 33; index++) await seed.publish(packet(`seed-${index}`));
    const store = storeAt(root, cap), admit = vi.fn();
    const rename = vi.spyOn(fs, "renameSync");
    const publishing = store.publish({ ...packet("new"), admit });
    if (cap === 1) {
      await expect(publishing).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
      expect(admit).not.toHaveBeenCalled();
      expect(fs.readdirSync(path.join(root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(33);
      expect(store.latestSequence()).toBe(33);
    } else {
      const event = await publishing;
      expect(admit).toHaveBeenCalledTimes(1);
      expect(await store.publish(packet("new"))).toEqual(event);
      expect(fs.readdirSync(path.join(root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(2);
    }
    const retirements = () => rename.mock.calls.filter(([, target]) => path.dirname(String(target)) === path.join(root, "event-receipts-pruned"));
    expect(retirements()).toHaveLength(cap === 1 ? 0 : 32);
    expect(store.read()).toHaveLength(cap === 1 ? 33 : 34);
    if (cap === 1) {
      // Refusal itself cannot make room, even on repeated attempts. Ordinary
      // successful maintenance must then recover without waiting for compaction.
      await expect(store.publish({ ...packet("new"), admit })).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
      expect(retirements()).toHaveLength(0);
      expect(admit).not.toHaveBeenCalled();
      await store.publish({ topic: "mesh.prune", from });
      expect(fs.existsSync(path.join(root, "generation"))).toBe(false);
      expect(retirements()).toHaveLength(32);
      expect(fs.readdirSync(path.join(root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(1);
      const accepted = await store.publish({ ...packet("new"), admit });
      expect(admit).toHaveBeenCalledTimes(1);
      expect(accepted.sequence).toBe(35);
      expect(await store.publish(packet("new"))).toEqual(accepted);
    }
  });

  it("a process stalled after listing lets another lock user and a no-archive finalizer progress", async () => {
    const root = scratch();
    const store = storeAt(root);
    const oldest = await store.publish(packet("oldest"));
    let release!: () => void;
    let queued!: () => void;
    const barrierQueued = new Promise<void>(resolve => { queued = resolve; });
    vi.spyOn(globalThis, "setImmediate").mockImplementationOnce(callback => {
      release = () => callback();
      queued();
      return {} as NodeJS.Immediate;
    });
    const pendingPublish = store.publish(packet("finalizing"));
    let released = false;
    try {
      await barrierQueued;
      const original = store.read().find(event => event.dedupeKey === "finalizing")!;
      expect(fs.existsSync(receipt(root, "finalizing", ".pending.json"))).toBe(true);
      const script = path.join(root, "pruner.mjs");
      const scanned = path.join(root, "scanned");
      const gate = path.join(root, "continue");
      const result = path.join(root, "result");
      fs.writeFileSync(script, `
        import fs from 'node:fs';
        import path from 'node:path';
        import { createRequire } from 'node:module';
        const require = createRequire(${JSON.stringify(fileURLToPath(new URL("../package.json", import.meta.url)))});
        const { createJiti } = require('jiti');
        const { MeshStore } = await createJiti(import.meta.url).import(${JSON.stringify(fileURLToPath(new URL("../src/mesh/store.ts", import.meta.url)))});
        const root = ${JSON.stringify(root)};
        const listing = fs.readdirSync.bind(fs);
        let paused = false;
        fs.readdirSync = (directory, ...args) => {
          const names = listing(directory, ...args);
          if (!paused && String(directory) === path.join(root, 'event-receipts')) {
            paused = true;
            fs.writeFileSync(${JSON.stringify(scanned)}, JSON.stringify({ lockHeld: fs.existsSync(path.join(root, '.lock')) }));
            const deadline = Date.now() + 20000;
            while (!fs.existsSync(${JSON.stringify(gate)})) {
              if (Date.now() >= deadline) throw new Error('listing gate timeout');
              Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
            }
          }
          return names;
        };
        const store = new MeshStore(root, 1024, 100, { maxDedupeReceipts: 2, maxEventLogBytes: 65536 });
        const event = await store.publish(${JSON.stringify(packet("newcomer"))});
        fs.writeFileSync(${JSON.stringify(result)}, JSON.stringify(event));
      `);
      const child = spawn(process.execPath, [script], { stdio: ["ignore", "pipe", "pipe"] });
      children.add(child);
      let stderr = "";
      child.stderr!.on("data", data => { stderr += String(data); });
      const exited = new Promise<number | null>((resolve, reject) => {
        child.once("error", reject);
        child.once("exit", resolve);
      });
      await vi.waitFor(() => expect(fs.existsSync(scanned)).toBe(true), { timeout: 15_000, interval: 20 });
      expect(JSON.parse(fs.readFileSync(scanned, "utf8"))).toEqual({ lockHeld: false });
      // An independent process is blocked in the real synchronous listing hook.
      // This acquisition must finish BEFORE it can finish scanning or enter custody.
      let progressed = false;
      await store.exclusive(() => { progressed = true; });
      expect(progressed).toBe(true);
      release();
      released = true;
      expect(await pendingPublish).toEqual(original);
      expect(fs.existsSync(receipt(root, "finalizing", ".pending.json"))).toBe(false);
      expect(JSON.parse(fs.readFileSync(receipt(root, "finalizing"), "utf8"))).toEqual(original);
      fs.writeFileSync(gate, "go");
      expect(await exited, stderr).toBe(0);
      children.delete(child);
      const newcomer = JSON.parse(fs.readFileSync(result, "utf8"));
      expect(newcomer.sequence).toBe(original.sequence + 1);
      expect(await store.publish(packet("finalizing"))).toEqual(original);
      expect(await store.publish(packet("newcomer"))).toEqual(newcomer);
      expect(fs.existsSync(receipt(root, "oldest"))).toBe(false);
      expect(store.read().filter(event => event.id === oldest.id)).toHaveLength(1);
      expect(store.read().filter(event => event.id === newcomer.id)).toHaveLength(1);
    } finally {
      if (!released && release) { release(); await pendingPublish.catch(() => undefined); }
    }
  }, 30_000);

  it("refreshes protected capacity when a no-archive finalizer beats commit but the directory stamp repeats", async () => {
    const root = scratch(), store = storeAt(root, 1);
    let release!: () => void, queued!: () => void;
    const barrierQueued = new Promise<void>(resolve => { queued = resolve; });
    vi.spyOn(globalThis, "setImmediate").mockImplementationOnce(callback => {
      release = () => callback();
      queued();
      return {} as NodeJS.Immediate;
    });
    const publishing = store.publish(packet("late-finalizer"));
    let released = false;
    try {
      await barrierQueued;
      const original = store.read()[0]!;
      const directory = path.join(root, "event-receipts"), frozen = fs.statSync(directory, { bigint: true });
      const stat = fs.statSync.bind(fs);
      vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
        if (String(target) === directory && (options as { bigint?: boolean } | undefined)?.bigint) return frozen;
        return stat(target, options as never);
      }) as typeof fs.statSync);
      const withLock = MeshLock.prototype.withLock;
      let finalized = false;
      vi.spyOn(MeshLock.prototype, "withLock").mockImplementation((async function(this: MeshLock, ...args: Parameters<typeof withLock>) {
        if (!finalized && this.root === root) {
          // The admission scan already saw only the protected pending key. Finish
          // exactly here: no reservation/counter change and no observable dir stamp.
          finalized = true;
          release();
          released = true;
          expect(await publishing).toEqual(original);
        }
        return withLock.apply(this, args);
      }) as typeof withLock);
      const next = await store.publish(packet("after-finalizer"));
      expect(finalized).toBe(true);
      expect(next.sequence).toBe(original.sequence + 1);
      expect(fs.existsSync(receipt(root, "late-finalizer"))).toBe(false);
      expect(await store.publish(packet("after-finalizer"))).toEqual(next);
      expect(store.read().filter(event => event.id === original.id)).toHaveLength(1);
    } finally {
      if (!released && release) { release(); await publishing.catch(() => undefined); }
    }
  });

  it.each(["renewal", "pending"] as const)("point CAS protects a concurrent %s even when directory stamps repeat", async mutation => {
    const root = scratch();
    const store = storeAt(root);
    const oldest = await store.publish(packet("protected"));
    await store.publish(packet("other"));
    const directory = path.join(root, "event-receipts");
    const frozen = fs.statSync(directory, { bigint: true });
    const stat = fs.statSync.bind(fs);
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
      if (String(target) === directory && (options as { bigint?: boolean } | undefined)?.bigint) return frozen;
      return stat(target, options as never);
    }) as typeof fs.statSync);
    const mkdir = fs.mkdirSync.bind(fs);
    let changed = false;
    const renewed = { ...oldest, createdAt: Date.now() + 1000 };
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      const result = mkdir(target, options);
      if (!changed && String(target) === path.join(root, ".lock")) {
        changed = true;
        if (mutation === "renewal") {
          const temporary = path.join(root, "renewed");
          fs.writeFileSync(temporary, JSON.stringify(renewed));
          fs.renameSync(temporary, receipt(root, "protected"));
        } else {
          fs.writeFileSync(receipt(root, "protected", ".pending.json"), JSON.stringify({
            dedupeKey: "protected", eventId: oldest.id, reservedSequence: oldest.sequence, liveOffset: 0,
          }));
        }
      }
      return result;
    }) as typeof fs.mkdirSync);
    const admit = vi.fn();
    const next = await store.publish({ ...packet("new"), admit });
    expect(changed).toBe(true);
    expect(admit).toHaveBeenCalledTimes(1);
    expect(JSON.parse(fs.readFileSync(receipt(root, "protected"), "utf8"))).toEqual(mutation === "renewal" ? renewed : oldest);
    expect(fs.existsSync(receipt(root, "other"))).toBe(false);
    expect(fs.existsSync(receipt(root, "protected", ".pending.json"))).toBe(mutation === "pending");
    expect(store.read().filter(event => event.id === next.id)).toHaveLength(1);
  });

  it.each(["normal", "coarse"] as const)("refuses a post-scan replacement across %s directory stamps without retiring the successor", async stamps => {
    const root = scratch(), store = storeAt(root, 1);
    const original = await store.publish(packet("victim"));
    const directory = path.join(root, "event-receipts"), file = receipt(root, "victim");
    const frozen = fs.statSync(directory, { bigint: true });
    const stat = fs.statSync.bind(fs);
    if (stamps === "coarse") vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
      if (String(target) === directory && (options as { bigint?: boolean } | undefined)?.bigint) return frozen;
      return stat(target, options as never);
    }) as typeof fs.statSync);
    const withLock = MeshLock.prototype.withLock;
    const renames = vi.spyOn(fs, "renameSync");
    let successor!: { ino: number; bytes: Buffer };
    let changed = false;
    vi.spyOn(MeshLock.prototype, "withLock").mockImplementation((async function(this: MeshLock, ...args: Parameters<typeof withLock>) {
      if (!changed && this.root === root) {
        // Preparation has completed, but admission has not acquired custody or
        // validated its stamp. Normal stamps force a rescan; coarse ones reach CAS.
        changed = true;
        const temporary = path.join(root, "replacement");
        fs.writeFileSync(temporary, JSON.stringify({ ...original, id: "successor", createdAt: Date.now() + 1000 }), { mode: 0o600 });
        fs.renameSync(temporary, file);
        successor = { ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) };
        if (stamps === "normal") {
          // Force a distinct observable namespace generation even on coarse filesystems.
          const changedTime = new Date(Number(frozen.mtimeMs) + 10_000);
          fs.utimesSync(directory, changedTime, changedTime);
          expect(stat(directory, { bigint: true }).mtimeNs).not.toBe(frozen.mtimeNs);
        }
      }
      return withLock.apply(this, args);
    }) as typeof withLock);
    const admit = vi.fn();
    await expect(store.publish({ ...packet("new"), admit })).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
    expect(changed).toBe(true);
    expect(admit).not.toHaveBeenCalled();
    expect(fs.lstatSync(file).ino).toBe(successor.ino);
    expect(fs.readFileSync(file)).toEqual(successor.bytes);
    expect(renames.mock.calls.filter(([, target]) => path.dirname(String(target)) === path.join(root, "event-receipts-pruned"))).toEqual([]);
    expect(fs.existsSync(receipt(root, "new"))).toBe(false);
    expect(store.read()).toEqual([original]);
  });

  it("expires a backlog in bounded, genuinely separate holds with payload reads outside custody", async () => {
    const root = scratch();
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const seed = new MeshStore(root, 1024, 100, { maxEventLogBytes: 64 * 1024, dedupeReceiptTtlMs: 1000 });
    for (let index = 0; index < 70; index++) await seed.publish(packet(`expired-${index}`));
    now += 1000;
    const directory = path.join(root, "event-receipts");
    const read = fs.readFileSync.bind(fs), rename = fs.renameSync.bind(fs), listing = fs.readdirSync.bind(fs);
    const victims = new Map<string, number>();
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
      if (String(target) === directory) expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      return listing(target, options as never);
    }) as typeof fs.readdirSync);
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, options?: unknown) => {
      if (typeof target === "string" && path.dirname(target) === directory && /^[a-f0-9]{64}\.json$/.test(path.basename(target))) {
        expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      }
      return read(target, options as never);
    }) as typeof fs.readFileSync);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (path.dirname(String(source)) === directory && path.dirname(String(target)) === path.join(root, "event-receipts-pruned")) {
        const owner = read(path.join(root, ".lock", "owner"), "utf8").split("\n")[0]!;
        victims.set(owner, (victims.get(owner) ?? 0) + 1);
      }
      return rename(source, target);
    });
    const compacting = new MeshStore(root, 1024, 100, {
      maxEventLogBytes: 4096, retainedEventLogBytes: 1025, dedupeReceiptTtlMs: 1000,
    });
    await compacting.transact(append => append({ topic: "mesh.prune", from }));
    expect([...victims.values()].reduce((sum, count) => sum + count, 0)).toBe(70);
    expect(victims.size).toBeGreaterThanOrEqual(3);
    expect(Math.max(...victims.values())).toBeLessThanOrEqual(32);
    expect(listing(directory).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toEqual([]);
  });

  it.runIf(process.platform === "linux")("returns success through retired unlink failure, logs once per outage, and removes it next pass", async () => {
    const root = scratch(), store = storeAt(root, 1);
    const original = await store.publish(packet("retiring"));
    const hash = path.basename(receipt(root, "retiring"), ".json");
    const unlink = fs.unlinkSync.bind(fs);
    const logs = vi.spyOn(diagnosticLog, "logDiagnostic").mockImplementation(() => undefined);
    let fail = true, failures = 0;
    vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
      if (path.basename(String(target)).startsWith(`${hash}.`) && fail) {
        expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
        failures++;
        throw Object.assign(new Error("EPERM"), { code: "EPERM" });
      }
      return unlink(target);
    });
    const event = await store.publish(packet("new"));
    expect(failures).toBe(1);
    const directory = path.join(root, "event-receipts-pruned");
    const leftovers = () => fs.readdirSync(directory, { recursive: true }).map(String).filter(name => path.basename(name).startsWith(`${hash}.`));
    expect(leftovers()).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(directory, leftovers()[0]!), "utf8"))).toEqual(original);
    await expect(store.publish(packet("new"))).resolves.toEqual(event);
    expect(failures).toBe(2);
    expect(logs.mock.calls.filter(([, message]) => message.startsWith("mesh receipt cleanup deferred"))).toHaveLength(1);
    fail = false;
    await expect(store.publish(packet("new"))).resolves.toEqual(event);
    expect(leftovers()).toEqual([]);
    expect(logs.mock.calls.filter(([, message]) => message === "mesh receipt cleanup recovered")).toHaveLength(1);
    expect(store.read()).toEqual([original, event]);
  });

  it("an archived batch can evict receipts it just settled without another directory scan", async () => {
    const root = scratch();
    const archive = path.join(root, "archive");
    fs.mkdirSync(archive);
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
    const store = storeAt(root, 1);
    await store.publish(packet("before-batch"));
    // This assertion concerns admission, not the separately tested elapsed-time prefix bound.
    vi.spyOn(performance, "now").mockReturnValue(1);
    const events = await store.publishBatch([packet("batch-first"), packet("batch-second")]);
    expect(events).toHaveLength(2);
    expect(fs.existsSync(receipt(root, "before-batch"))).toBe(false);
    expect(fs.existsSync(receipt(root, "batch-first"))).toBe(false);
    expect(await store.publish(packet("batch-second"))).toEqual(events[1]);
  });

  it.each(["transact", "schedule", "release"] as const)("%s prepares intent settlement before appendLocked compaction", async caller => {
    const root = scratch();
    const seed = storeAt(root, 10);
    const original = await seed.publish(packet("anchored"));
    const pending = receipt(root, "anchored", ".pending.json");
    fs.writeFileSync(pending, JSON.stringify({ dedupeKey: "anchored", eventId: original.id,
      reservedSequence: original.sequence, liveOffset: 0 }));
    fs.rmSync(receipt(root, "anchored"));
    const now = Date.now();
    if (caller === "release") await seed.schedule({ topic: "mesh.prune", from, dueAt: now + 1000 }, now);
    for (let index = 0; index < 8; index++) await seed.publish({ topic: "mesh.prune", from, text: "x".repeat(600) });
    const store = new MeshStore(root, 1024, 100, { maxEventLogBytes: 4096, retainedEventLogBytes: 1025 });
    const directory = path.join(root, "event-receipts"), listing = fs.readdirSync.bind(fs);
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
      if (String(target) === directory) expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      return listing(target, options as never);
    }) as typeof fs.readdirSync);
    if (caller === "transact") await store.transact(append => append({ topic: "mesh.prune", from }));
    else if (caller === "schedule") await store.schedule({ topic: "mesh.prune", from, dueAt: now }, now);
    else await store.releaseDueSchedules(now + 1000);
    expect(Number(fs.readFileSync(path.join(root, "generation"), "utf8"))).toBeGreaterThan(0);
    expect(fs.existsSync(pending)).toBe(false);
    expect(await store.publish(packet("anchored"))).toEqual(original);
  });
});
