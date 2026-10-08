import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#6477 E1: the fsyncs of no-archive publish, recovery and live compaction run after
// `.lock` is released, and a publish still resolves only once its bytes are durable. Adapted
// from pi-fabric#550's tests/mesh-fsync-holds.test.ts.

const roots: string[] = [];
const from = { id: "session:fsync", name: "fsync", kind: "main" as const };
const store = (options: ConstructorParameters<typeof MeshStore>[3] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fsync-e1-"));
  roots.push(root);
  return new MeshStore(root, 1024, 100, options);
};
const compacting = () => store({ maxEventLogBytes: 2800, retainedEventLogBytes: 1025 });
const events = (root: string) => path.join(root, "events.jsonl");
const receipt = (root: string, key: string, suffix = ".json") =>
  path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const sameFile = (fd: number, file: string): boolean => {
  try {
    const opened = fs.fstatSync(fd), named = fs.statSync(file);
    return opened.isFile() && opened.dev === named.dev && opened.ino === named.ino;
  } catch { return false; }
};

/** Every fsync/fdatasync, with whether `.lock` was held at that moment (the L8 question). */
const watchBarriers = (root: string, onSync?: (fd: number) => void) => {
  const syncs: Array<{ held: boolean; live: boolean; afterAppend: boolean }> = [];
  let appended = false;
  const append = fs.appendFileSync.bind(fs);
  const originals = [fs.fsyncSync.bind(fs), fs.fdatasyncSync.bind(fs)];
  const spies = [
    vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
      append(file, data, options);
      if (file === events(root)) appended = true;
    }),
    ...(["fsyncSync", "fdatasyncSync"] as const).map((name, index) =>
      vi.spyOn(fs, name).mockImplementation(fd => {
        syncs.push({ held: fs.existsSync(path.join(root, ".lock")), live: sameFile(fd, events(root)), afterAppend: appended });
        onSync?.(fd);
        originals[index]!(fd);
      })),
  ];
  return {
    syncs,
    held: () => syncs.filter(sync => sync.held),
    reset: () => { syncs.length = 0; appended = false; },
    restore: () => spies.forEach(spy => spy.mockRestore()),
  };
};

/** A power-cut model of the live log: its bytes as of the last fsync of its inode. */
const durableImage = (root: string) => {
  let image = Buffer.alloc(0);
  return {
    capture: (fd: number) => { if (sameFile(fd, events(root))) image = fs.readFileSync(events(root)); },
    get: () => image,
  };
};

afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("no fsync under .lock (smarty-dev#6477 E1)", () => {
  it("publish: a durable unkeyed append fsyncs the live log only after release, before it resolves", async () => {
    const mesh = store();
    const image = durableImage(mesh.root);
    const watcher = watchBarriers(mesh.root, image.capture);
    const event = await mesh.publish({ topic: "mesh.fsync", from, text: "durable", durable: true });
    // Resolved => the live inode was fsynced (and its namespace), and never while held.
    expect(watcher.syncs.some(sync => sync.live)).toBe(true);
    expect(watcher.held()).toEqual([]);
    expect(image.get().toString("utf8")).toContain(`"id":"${event.id}"`);
  });

  it("publish: concurrent durable appends share one group barrier; a later append gets a fresh one", async () => {
    const mesh = store(), other = new MeshStore(mesh.root, 1024, 100);
    const watcher = watchBarriers(mesh.root);
    const published = await Promise.all(Array.from({ length: 6 }, (_, index) => (index % 2 ? mesh : other)
      .publish({ topic: "mesh.fsync", from, text: String(index), durable: true })));
    expect(watcher.syncs.filter(sync => sync.live)).toHaveLength(1);
    expect(watcher.held()).toEqual([]);
    expect(mesh.read()).toEqual(published);
    await mesh.publish({ topic: "mesh.fsync", from, durable: true, text: "later" });
    expect(watcher.syncs.filter(sync => sync.live)).toHaveLength(2);
  });

  it("publish: a failed after-unlock barrier rejects (no success without durability) and never re-appends", async () => {
    const mesh = store();
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
      sync(fd);
    });
    const outcomes = await Promise.allSettled([0, 1, 2].map(index => mesh.publish({ topic: "mesh.fsync", from, text: String(index), durable: true })));
    expect(outcomes.every(result => result.status === "rejected")).toBe(true);
    fail.mockRestore();
    const next = await mesh.publish({ topic: "mesh.fsync", from, durable: true, text: "next" });
    expect(next.sequence).toBe(4);
    expect(mesh.read().map(event => event.sequence)).toEqual([1, 2, 3, 4]);
  });

  it("publish: a fresh no-archive keyed append keeps only its intent fence under the lock", async () => {
    const mesh = store();
    const watcher = watchBarriers(mesh.root);
    const packet = { topic: "mesh.fsync", from, dedupeKey: "fresh", text: "once" };
    const event = await mesh.publish(packet);
    // The intent's durability precedes the live append (crash fence, held); nothing after it is held.
    expect(watcher.syncs.filter(sync => sync.held && sync.afterAppend)).toEqual([]);
    expect(watcher.syncs.some(sync => sync.live && !sync.held)).toBe(true);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(receipt(mesh.root, packet.dedupeKey), "utf8"))).toEqual(event);
    watcher.reset();
    // A repeat returns the receipt: its confirmation runs after release too.
    expect(await mesh.publish(packet)).toEqual(event);
    expect(watcher.held()).toEqual([]);
    expect(mesh.read()).toEqual([event]);
  });

  it("recovery: a same-key retry settles a committed live append with every barrier after release", async () => {
    const mesh = store();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "failed-live", text: "once" };
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("live barrier failed");
    fail.mockRestore();
    // The state a death after the live append leaves: intent + live line, no receipt.
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey))).toBe(false);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(true);
    const committed = mesh.read()[0]!;
    const watcher = watchBarriers(mesh.root);
    expect(await new MeshStore(mesh.root, 1024, 100).publish(packet)).toEqual(committed);
    expect(watcher.held()).toEqual([]);
    expect(watcher.syncs.some(sync => sync.live)).toBe(true);
    expect(JSON.parse(fs.readFileSync(receipt(mesh.root, packet.dedupeKey), "utf8"))).toEqual(committed);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
    expect(mesh.read()).toEqual([committed]);
  });

  it.skipIf(process.platform === "win32")("recovery: a publisher SIGKILLed after its live append is recovered exactly once", async () => {
    const mesh = store({ lockTimeoutMs: 5_000, staleLockMs: 100 });
    const packet = { topic: "mesh.fsync", from, dedupeKey: "killed", text: "once" };
    const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-archive-before-live-crash.mjs"), mesh.root, JSON.stringify(packet)], {
      env: { ...process.env, PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND: "1" }, stdio: ["ignore", "ignore", "pipe"],
    });
    let stderr = "";
    child.stderr.on("data", bytes => { stderr += bytes; });
    const result = await new Promise<NodeJS.Signals | null>(resolve => child.once("close", (_code, signal) => resolve(signal)));
    expect(result, stderr).toBe("SIGKILL");
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey))).toBe(false);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(true);
    const live = mesh.read();
    expect(live).toHaveLength(1);
    const recovered = await mesh.publish(packet);
    expect(recovered).toEqual(live[0]);
    expect(await mesh.publish(packet)).toEqual(recovered);
    expect(mesh.read()).toEqual(live);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
  });

  it("recovery: a torn tail is truncated and the next durable publish is fsynced after release", async () => {
    const mesh = store();
    const first = await mesh.publish({ topic: "mesh.fsync", from, text: "before", durable: true });
    fs.appendFileSync(events(mesh.root), '{"sequence":2,"id":"torn"');
    const watcher = watchBarriers(mesh.root);
    const next = await mesh.publish({ topic: "mesh.fsync", from, text: "after", durable: true });
    expect(watcher.held()).toEqual([]);
    expect(watcher.syncs.some(sync => sync.live)).toBe(true);
    expect(next.sequence).toBe(2);
    expect(mesh.read()).toEqual([first, next]);
    expect(fs.readFileSync(events(mesh.root), "utf8").endsWith("\n")).toBe(true);
  });

  it("power cut: a returned publish survives; the unsynced torn tail after it is recovered", async () => {
    const mesh = store();
    const image = durableImage(mesh.root);
    const watcher = watchBarriers(mesh.root, image.capture);
    const returned = await mesh.publish({ topic: "mesh.fsync", from, text: "acknowledged", durable: true });
    // A later append is visible to readers before any fsync, as it always was (readers take no lock).
    await mesh.publish({ topic: "mesh.fsync", from, text: "not durable" });
    expect(mesh.read()).toHaveLength(2);
    watcher.restore();
    // Power cut: only the fsynced image survives, plus a torn prefix of the unsynced line.
    fs.writeFileSync(events(mesh.root), Buffer.concat([image.get(), Buffer.from('{"sequence":2,"id":"lost","top')]));
    const rebooted = new MeshStore(mesh.root, 1024, 100);
    expect(rebooted.read()).toEqual([returned]);
    expect(rebooted.latestCursor().last).toEqual({ sequence: returned.sequence, id: returned.id });
    const after = await rebooted.publish({ topic: "mesh.fsync", from, text: "after reboot", durable: true });
    expect(after.sequence).toBeGreaterThan(returned.sequence);
    expect(rebooted.read()).toEqual([returned, after]);
  });

  it("compaction: stages and fsyncs the retained tail off-lock; the rename under the lock has no fsync", async () => {
    const mesh = compacting();
    const watcher = watchBarriers(mesh.root);
    const published = [];
    for (let index = 0; index < 30; index++) published.push(await mesh.publish({ topic: "mesh.fsync", from, text: `event ${index} ${"x".repeat(120)}` }));
    expect(fs.readFileSync(path.join(mesh.root, "generation"), "utf8")).not.toBe("0");
    expect(watcher.syncs.length).toBeGreaterThan(0); // stage data + the rename's directory barrier
    expect(watcher.held()).toEqual([]);
    expect(fs.statSync(events(mesh.root)).size).toBeLessThanOrEqual(2800);
    const live = mesh.read({ limit: 100 });
    expect(live.at(-1)).toEqual(published.at(-1));
    expect(published.map(event => event.id)).toEqual(expect.arrayContaining(live.map(event => event.id)));
    expect(fs.readdirSync(mesh.root).filter(name => name.endsWith(".compacting"))).toEqual([]);
  });

  it("compaction: an append that lands while the stage is prepared is folded in, not lost", async () => {
    const mesh = compacting();
    for (let index = 0; index < 15; index++) await mesh.publish({ topic: "mesh.fsync", from, text: `seed ${index} ${"x".repeat(120)}` });
    const generation = fs.readFileSync(path.join(mesh.root, "generation"), "utf8");
    let injected: string | undefined;
    const other = new MeshStore(mesh.root, 1024, 100);
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      sync(fd);
      // The first stage fsync is off-lock: another writer's (simulated) committed append lands now.
      if (injected === undefined && fs.fstatSync(fd).isFile() && !sameFile(fd, events(mesh.root)) &&
          fs.readdirSync(mesh.root).some(name => name.endsWith(".compacting"))) {
        const event = { id: "folded", sequence: 10_000, topic: "mesh.fsync", kind: "message", from, verification: "mesh", createdAt: Date.now() };
        injected = JSON.stringify(event);
        fs.appendFileSync(events(mesh.root), `${injected}\n`);
      }
    });
    for (let index = 0; injected === undefined && index < 30; index++) await mesh.publish({ topic: "mesh.fsync", from, text: `more ${index} ${"x".repeat(120)}` });
    vi.restoreAllMocks();
    expect(injected).toBeDefined();
    expect(fs.readFileSync(path.join(mesh.root, "generation"), "utf8")).not.toBe(generation);
    expect(fs.readFileSync(events(mesh.root), "utf8").trimEnd().split("\n").at(-1)).toBe(injected);
    expect((await other.publish({ topic: "mesh.fsync", from, text: "next" })).sequence).toBe(10_001);
  });

  it("compaction: a concurrent repair truncating a torn tail during staging restarts the snapshot; a later append is not folded mid-line", async () => {
    const mesh = compacting();
    const file = events(mesh.root);
    const held = () => fs.existsSync(path.join(mesh.root, ".lock"));
    for (let index = 0; index < 15; index++) await mesh.publish({ topic: "mesh.fsync", from, text: `seed ${index} ${"x".repeat(120)}` });
    const generation = fs.readFileSync(path.join(mesh.root, "generation"), "utf8");
    const torn = '{"sequence":9999,"id":"torn","topic":"mesh.fs';
    let phase = "armed" as "armed" | "torn" | "done";
    let injected: string | undefined;
    const stat = fs.statSync.bind(fs) as (...args: unknown[]) => fs.Stats;
    const read = fs.readSync.bind(fs) as (...args: unknown[]) => number;
    vi.spyOn(fs, "statSync").mockImplementation(((...args: unknown[]) => {
      // A dead writer's torn tail is on the log when the off-lock compaction trigger looks.
      if (phase === "armed" && args[0] === file && !held() && stat(file).size > 2800) {
        fs.appendFileSync(file, torn);
        phase = "torn";
      }
      return stat(...args);
    }) as typeof fs.statSync);
    vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
      if (phase !== "torn" || held() || !sameFile(args[0] as number, file)) return read(...args);
      phase = "done";
      // Between the snapshot stat and its read, another process's repair truncates the torn
      // tail (the read returns short) ...
      fs.truncateSync(file, stat(file).size - torn.length);
      const count = read(...args);
      // ... and another writer's committed append then lands at the truncated end.
      injected = JSON.stringify({ id: "after-repair", sequence: 10_000, topic: "mesh.fsync", kind: "message", from, verification: "mesh", createdAt: Date.now() });
      fs.appendFileSync(file, `${injected}\n`);
      return count;
    }) as typeof fs.readSync);
    for (let index = 0; phase !== "done" && index < 30; index++) await mesh.publish({ topic: "mesh.fsync", from, text: `more ${index} ${"x".repeat(120)}` });
    vi.restoreAllMocks();
    expect(phase).toBe("done");
    expect(fs.readFileSync(path.join(mesh.root, "generation"), "utf8")).not.toBe(generation);
    const lines = fs.readFileSync(file, "utf8").trimEnd().split("\n");
    // No mid-line fold: every committed line is a whole event, the torn bytes are gone and
    // the append after the repair survives intact.
    expect(lines.map(line => () => JSON.parse(line)).every(parse => { try { parse(); return true; } catch { return false; } })).toBe(true);
    expect(lines.some(line => line.includes('"id":"torn"'))).toBe(false);
    expect(lines.at(-1)).toBe(injected);
    expect(mesh.read({ limit: 100 }).at(-1)?.id).toBe("after-repair");
    expect((await mesh.publish({ topic: "mesh.fsync", from, text: "next" })).sequence).toBe(10_001);
  });

  it("compaction: a pending no-archive intent defers compaction (no barrier under the lock); the publish that settles it compacts", async () => {
    const mesh = compacting();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "pending-over-cap", text: "once" };
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("live barrier failed");
    fail.mockRestore();
    const intentPath = receipt(mesh.root, packet.dedupeKey, ".pending.json");
    expect(fs.existsSync(intentPath)).toBe(true);
    const committed = mesh.read()[0]!;
    const generation = () => fs.existsSync(path.join(mesh.root, "generation")) ? fs.readFileSync(path.join(mesh.root, "generation"), "utf8") : "0";
    const watcher = watchBarriers(mesh.root);
    // Over the cap (2800) but under the hard bound (5600): every trigger defers.
    for (let index = 0; fs.statSync(events(mesh.root)).size <= 2800 + 300 && index < 40; index++) {
      await mesh.publish({ topic: "mesh.fsync", from, text: `fill ${index} ${"x".repeat(120)}` });
    }
    expect(fs.statSync(events(mesh.root)).size).toBeLessThanOrEqual(5600);
    expect(watcher.held()).toEqual([]);
    expect(generation()).toBe("0");
    expect(fs.existsSync(intentPath)).toBe(true);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey))).toBe(false);
    // The same-key retry settles the intent off-lock; its own compaction trigger then compacts.
    expect(await new MeshStore(mesh.root, 1024, 100, { maxEventLogBytes: 2800, retainedEventLogBytes: 1025 }).publish(packet)).toEqual(committed);
    expect(watcher.held()).toEqual([]);
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receipt(mesh.root, packet.dedupeKey), "utf8"))).toEqual(committed);
    expect(generation()).not.toBe("0");
    expect(fs.statSync(events(mesh.root)).size).toBeLessThanOrEqual(2800);
    expect(await mesh.publish(packet)).toEqual(committed);
  });

  it("compaction: past twice the cap a pending no-archive intent is settled under the lock so the log stays bounded", async () => {
    const mesh = compacting();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "pending-hard-bound", text: "once" };
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("live barrier failed");
    fail.mockRestore();
    const intentPath = receipt(mesh.root, packet.dedupeKey, ".pending.json");
    const committed = mesh.read()[0]!;
    for (let index = 0; fs.existsSync(intentPath) && index < 60; index++) {
      await mesh.publish({ topic: "mesh.fsync", from, text: `fill ${index} ${"x".repeat(120)}` });
    }
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receipt(mesh.root, packet.dedupeKey), "utf8"))).toEqual(committed);
    expect(fs.readFileSync(path.join(mesh.root, "generation"), "utf8")).not.toBe("0");
    expect(fs.statSync(events(mesh.root)).size).toBeLessThanOrEqual(5600);
    expect(await mesh.publish(packet)).toEqual(committed);
  });

  it("recovery: a receipt the original publisher installs after the first lookup is confirmed and its intent unlinked after release", async () => {
    const mesh = store();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "late-receipt", text: "once" };
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("live barrier failed");
    fail.mockRestore();
    const intentPath = receipt(mesh.root, packet.dedupeKey, ".pending.json");
    const receiptPath = receipt(mesh.root, packet.dedupeKey);
    expect(fs.existsSync(receiptPath)).toBe(false);
    expect(fs.existsSync(intentPath)).toBe(true);
    const committed = mesh.read()[0]!;
    const held = () => fs.existsSync(path.join(mesh.root, ".lock"));
    const readFile = fs.readFileSync.bind(fs) as (...args: unknown[]) => string | Buffer;
    let installed = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
      // The retry's first receipt lookup (under the lock) has missed. Now the original
      // publisher, which holds no lock for this step, installs its receipt.
      if (!installed && args[0] === intentPath && held()) {
        fs.writeFileSync(receiptPath, JSON.stringify(committed));
        installed = true;
      }
      return readFile(...args);
    }) as typeof fs.readFileSync);
    let receiptSynced = false;
    const watcher = watchBarriers(mesh.root, fd => { if (sameFile(fd, receiptPath) && !held()) receiptSynced = true; });
    expect(await new MeshStore(mesh.root, 1024, 100).publish(packet)).toEqual(committed);
    expect(installed).toBe(true);
    // No fsync (receipt, live log or namespace directory) while `.lock` is held ...
    expect(watcher.held()).toEqual([]);
    // ... yet the receipt is confirmed and the intent removed before the retry resolves.
    expect(receiptSynced).toBe(true);
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receiptPath, "utf8"))).toEqual(committed);
    expect(mesh.read()).toEqual([committed]);
  });
});
