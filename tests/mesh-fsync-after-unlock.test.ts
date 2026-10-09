import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#6477 E1: the live-log fsync of no-archive publish/recovery runs after `.lock`
// is released. Receipt installation reacquires it for an intent-identity CAS (PR #755 round 3).
// A publish still resolves only once its bytes are durable. Compaction stays under
// the lock as on main (off-lock compaction: smarty-dev#7002). Adapted from pi-fabric#550's
// tests/mesh-fsync-holds.test.ts.

const roots: string[] = [];
const from = { id: "session:fsync", name: "fsync", kind: "main" as const };
const store = (options: ConstructorParameters<typeof MeshStore>[3] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fsync-e1-"));
  roots.push(root);
  return new MeshStore(root, 1024, 100, options);
};
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

/** The file an fd names (Linux /proc), for classifying a barrier. */
const fdPath = (fd: number): string => { try { return fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return ""; } };

/** A no-archive keyed publish whose live barrier fails: the "died after the live append" state
 * (intent + live line, no receipt). Returns the committed event and the intent path. */
const strandIntent = async (mesh: MeshStore, dedupeKey: string) => {
  const packet = { topic: "mesh.fsync", from, dedupeKey, text: "once" };
  const sync = fs.fsyncSync.bind(fs);
  const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    if (sameFile(fd, events(mesh.root))) throw new Error("live barrier failed");
    sync(fd);
  });
  await expect(mesh.publish(packet)).rejects.toThrow("live barrier failed");
  fail.mockRestore();
  const intentPath = receipt(mesh.root, dedupeKey, ".pending.json");
  expect(fs.existsSync(intentPath)).toBe(true);
  return { packet, intentPath, committed: mesh.read().find(event => event.dedupeKey === dedupeKey)! };
};

afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("off-lock live barriers and locked receipt CAS (smarty-dev#6477 E1)", () => {
  it("publish: a durable unkeyed append fsyncs the live log only after release, before it resolves", async () => {
    const mesh = store();
    const image = durableImage(mesh.root);
    const watcher = watchBarriers(mesh.root, image.capture);
    const event = await mesh.publish({ topic: "mesh.fsync", from, text: "durable", durable: true });
    // Resolved => the live inode was fsynced (and its namespace), and never while held.
    expect(watcher.syncs.some(sync => sync.live)).toBe(true);
    expect(watcher.held()).toEqual([]);
    expect(image.get().toString("utf8")).toContain(`"id":"${event.id}"`);
    expect(event.dedupeKey).toBeUndefined();
    expect(fs.existsSync(path.join(mesh.root, "event-receipts"))).toBe(false);
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

  it("publish: a fresh no-archive keyed append keeps the live barrier off-lock and finalizes its receipt under CAS", async () => {
    const mesh = store();
    const watcher = watchBarriers(mesh.root);
    const packet = { topic: "mesh.fsync", from, dedupeKey: "fresh", text: "once" };
    const event = await mesh.publish(packet);
    // The intent fence precedes append; the later receipt CAS holds the lock, not the live fsync.
    expect(watcher.syncs.filter(sync => sync.held && sync.live)).toEqual([]);
    expect(watcher.syncs.some(sync => sync.held && sync.afterAppend)).toBe(true);
    expect(watcher.syncs.some(sync => sync.live && !sync.held)).toBe(true);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey, ".pending.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(receipt(mesh.root, packet.dedupeKey), "utf8"))).toEqual(event);
    watcher.reset();
    // A repeat returns the receipt: its confirmation runs after release too.
    expect(await mesh.publish(packet)).toEqual(event);
    expect(watcher.held()).toEqual([]);
    expect(mesh.read()).toEqual([event]);
  });

  it("recovery: a same-key retry uses the off-lock live barrier and a locked receipt CAS", async () => {
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
    expect(watcher.syncs.filter(sync => sync.held && sync.live)).toEqual([]);
    expect(watcher.held().length).toBeGreaterThan(0);
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

  it.skipIf(process.platform !== "linux")("publish/recovery holds barriers only for fresh intent fences or receipt identity CAS, never the live log", async () => {
    const mesh = store();
    const root = mesh.root;
    const lockOwner = path.join(root, ".lock", "owner");
    const holdOf = () => { try { return fs.readFileSync(lockOwner, "utf8").split("\n")[0] ?? ""; } catch { return ""; } };
    // Per hold: has the live append happened yet? A fence is a fresh intent's durable write
    // (its file and namespace chain, from #preparePublish), in a hold before its live append.
    let hold = "", appendedInHold = false;
    const enter = () => { const now = holdOf(); if (now !== hold) { hold = now; appendedInHold = false; } };
    const held: Array<{ path: string; fence: boolean; cas: boolean; via: string }> = [];
    const record = (fd: number) => {
      if (!fs.existsSync(path.join(root, ".lock"))) return;
      enter();
      const limit = Error.stackTraceLimit;
      Error.stackTraceLimit = 100;
      const stack = new Error().stack ?? "";
      Error.stackTraceLimit = limit;
      const via = /settleDedupeIntent|removeDedupeIntent|finishLiveReceipt|confirmEventFile|syncPathNamespace|writeFileAtomic/.exec(
        stack.split("\n").filter(line => !line.includes("atomic-write")).join("\n"))?.[0] ?? "other";
      // The fresh intent's write is the only writeFileAtomic called directly from the publish
      // commit step before the append: the anonymous publish closure, or its `append` step that
      // main's R20 bridge fence (input.fence) runs (smarty-dev#6477 L2b). Settlement frames are named.
      const frames = stack.split("\n").map(line => line.trim());
      const writer = frames.findIndex(line => line.startsWith("at writeFileAtomic "));
      const fence = !appendedInHold && writer >= 0 && /^at (?:\S*src\/mesh\/event-log\.ts:\d+:\d+|append \(\S*src\/mesh\/event-log\.ts:\d+:\d+\))$/.test(frames[writer + 1] ?? "") &&
        !/settleDedupeIntent|removeDedupeIntent|finishLiveReceipt/.test(stack);
      const cas = !appendedInHold && /finalizeReceipt|cleanupReceiptIntent/.test(stack);
      held.push({ path: fdPath(fd), fence, cas, via });
    };
    const append = fs.appendFileSync.bind(fs);
    vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
      enter();
      append(file, data, options);
      if (file === events(root)) appendedInHold = true;
    });
    const spyBarriers = () => {
      for (const name of ["fsyncSync", "fdatasyncSync"] as const) {
        const original = fs[name].bind(fs);
        vi.spyOn(fs, name).mockImplementation(fd => {
          record(fd);
          original(fd);
        });
      }
    };
    spyBarriers();
    // 1. Durable unkeyed publishes: the group barrier after release.
    for (let index = 0; index < 5; index++) await mesh.publish({ topic: "mesh.fsync", from, durable: true, text: `durable ${index}` });
    // 2. A fresh keyed publish (its fence) and its repeat.
    const fresh = { topic: "mesh.fsync", from, dedupeKey: "fresh", text: "once" };
    const first = await mesh.publish(fresh);
    expect(await mesh.publish(fresh)).toEqual(first);
    // 3. A stranded committed intent (dead after the live append), recovered by a same-key retry ...
    vi.mocked(fs.fsyncSync).mockRestore();
    const stranded = await strandIntent(mesh, "stranded");
    spyBarriers();
    expect(await mesh.publish(stranded.packet)).toEqual(stranded.committed);
    expect(fs.existsSync(stranded.intentPath)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receipt(root, "stranded"), "utf8"))).toEqual(stranded.committed);
    // ... and a never-committed one (dead before the append), retried: unlinked, then a fresh fence.
    const dead = receipt(root, "retried", ".pending.json");
    fs.writeFileSync(dead, JSON.stringify({ dedupeKey: "retried", reservedSequence: 9_000, eventId: "never-retried", liveOffset: fs.statSync(events(root)).size + 50_000 }));
    const retried = await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "retried", text: "retried" });
    expect(fs.existsSync(dead)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receipt(root, "retried"), "utf8"))).toEqual(retried);
    // 4. A torn tail, repaired under the lock; the next durable publish is fsynced after release.
    fs.appendFileSync(events(root), '{"sequence":99999,"id":"torn"');
    const last = await mesh.publish({ topic: "mesh.fsync", from, durable: true, text: "after torn" });
    expect(mesh.read({ limit: 100 }).at(-1)).toEqual(last);
    expect(await mesh.publish({ topic: "mesh.fsync", from, dedupeKey: "retried", text: "retried" })).toEqual(retried);
    // Only intent fences and receipt CAS hold barriers; the live barrier always follows release.
    expect(held.filter(sync => !sync.fence && !sync.cas)).toEqual([]);
    expect(held.filter(sync => sync.path === events(root))).toEqual([]);
    expect(held.some(sync => sync.fence)).toBe(true);
    expect(held.some(sync => sync.cas)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("power cut: a keyed publish cut during its locked receipt CAS recovers exactly once from the durable image and intent", async () => {
    const mesh = store();
    const packet = { topic: "mesh.fsync", from, dedupeKey: "power-cut", text: "once" };
    await mesh.publish({ topic: "mesh.fsync", from, text: "before", durable: true });
    const image = durableImage(mesh.root);
    const held = () => fs.existsSync(path.join(mesh.root, ".lock"));
    let cut = false;
    const heldSyncs: string[] = [];
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = fdPath(fd);
      if (held()) heldSyncs.push(file);
      // Power fails as the locked receipt CAS stage is about to become durable: the live barrier
      // has run, the receipt has not, the intent is still there.
      if (!cut && held() && /\/event-receipts\/[a-f0-9]{64}\.json\.\d+\..+\.tmp$/.test(file)) {
        cut = true;
        throw new Error("power cut");
      }
      sync(fd);
      image.capture(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("power cut");
    vi.restoreAllMocks();
    expect(cut).toBe(true);
    // The live barrier already ran off-lock; receipt CAS runs under the lock and may fail safely.
    expect(heldSyncs.filter(file => file.endsWith("/events.jsonl"))).toEqual([]);
    expect(heldSyncs.some(file => /[a-f0-9]{64}\.json\./.test(file))).toBe(true);
    const committed = mesh.read().find(event => event.dedupeKey === packet.dedupeKey)!;
    // Only the fsynced image survives, plus a torn prefix; the unsynced receipt stage is gone.
    expect(image.get().toString("utf8")).toContain(`"id":"${committed.id}"`);
    fs.writeFileSync(events(mesh.root), Buffer.concat([image.get(), Buffer.from('{"sequence":99999,"id":"lost","to')]));
    const receipts = path.join(mesh.root, "event-receipts");
    for (const name of fs.readdirSync(receipts)) if (name.endsWith(".tmp")) fs.rmSync(path.join(receipts, name));
    const intentPath = receipt(mesh.root, packet.dedupeKey, ".pending.json");
    expect(fs.existsSync(intentPath)).toBe(true);
    expect(fs.existsSync(receipt(mesh.root, packet.dedupeKey))).toBe(false);
    const rebooted = new MeshStore(mesh.root, 1024, 100);
    expect(await rebooted.publish(packet)).toEqual(committed);
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(await rebooted.publish(packet)).toEqual(committed);
    expect(rebooted.read().filter(event => event.dedupeKey === packet.dedupeKey)).toEqual([committed]);
  });

  it("recovery: a late receipt is confirmed off-lock and its intent is cleaned up under an identity CAS", async () => {
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
    // The first receipt confirmation stays off-lock; cleanup rechecks and confirms the
    // still-matching receipt under CAS before unlinking the intent (portable file barrier).
    expect(watcher.syncs.filter(sync => sync.held && sync.live)).toEqual([]);
    expect(watcher.held().length).toBeGreaterThan(0);
    // The receipt is confirmed and the original intent removed before retry resolves.
    expect(receiptSynced).toBe(true);
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receiptPath, "utf8"))).toEqual(committed);
    expect(mesh.read()).toEqual([committed]);
  });

  it.each([false, true])("recovery: receipt cleanup has a file barrier without directory fsync (batch=%s)", async batch => {
    const mesh = store();
    const { packet, intentPath, committed } = await strandIntent(mesh, `portable-cleanup-${batch}`);
    const receiptPath = receipt(mesh.root, packet.dedupeKey);
    fs.writeFileSync(receiptPath, JSON.stringify(committed));
    // Exercise Windows' actual barrier branch on every CI host, not a directory-fsync
    // assertion that passes on Linux but cannot observe any barrier on native Windows.
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const held = () => fs.existsSync(path.join(mesh.root, ".lock"));
    let offLockReceiptSynced = false, lockedReceiptSynced = false, directorySynced = false;
    const watcher = watchBarriers(mesh.root, fd => {
      if (fs.fstatSync(fd).isDirectory()) directorySynced = true;
      if (sameFile(fd, receiptPath)) {
        if (held()) lockedReceiptSynced = true;
        else offLockReceiptSynced = true;
      }
    });
    const result = batch ? (await mesh.publishBatch([packet]))[0] : await mesh.publish(packet);
    expect(result).toEqual(committed);
    expect(offLockReceiptSynced).toBe(true);
    expect(lockedReceiptSynced).toBe(true);
    expect(directorySynced).toBe(false);
    expect(watcher.syncs.filter(sync => sync.held && sync.live)).toEqual([]);
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(JSON.parse(fs.readFileSync(receiptPath, "utf8"))).toEqual(committed);
    expect(mesh.read()).toEqual([committed]);
    platform.mockRestore();
  });

  it("recovery: a failed cleanup receipt barrier preserves the intent and retry never re-appends", async () => {
    const mesh = store();
    const { packet, intentPath, committed } = await strandIntent(mesh, "cleanup-barrier-failure");
    const receiptPath = receipt(mesh.root, packet.dedupeKey);
    fs.writeFileSync(receiptPath, JSON.stringify(committed));
    const intentBefore = fs.readFileSync(intentPath, "utf8");
    const sync = fs.fsyncSync.bind(fs);
    const fail = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (sameFile(fd, receiptPath) && fs.existsSync(path.join(mesh.root, ".lock"))) {
        throw new Error("cleanup receipt barrier failed");
      }
      sync(fd);
    });
    await expect(mesh.publish(packet)).rejects.toThrow("cleanup receipt barrier failed");
    fail.mockRestore();
    expect(fs.readFileSync(intentPath, "utf8")).toBe(intentBefore);
    expect(JSON.parse(fs.readFileSync(receiptPath, "utf8"))).toEqual(committed);
    expect(await mesh.publish(packet)).toEqual(committed);
    expect(fs.existsSync(intentPath)).toBe(false);
    expect(mesh.read()).toEqual([committed]);
  });

  it.each(["missing", "replaced"] as const)("recovery: cleanup retains its intent if the confirmed receipt is %s before CAS", async change => {
    const mesh = store();
    const { packet, intentPath, committed } = await strandIntent(mesh, `receipt-${change}-before-cas`);
    const receiptPath = receipt(mesh.root, packet.dedupeKey);
    fs.writeFileSync(receiptPath, JSON.stringify(committed));
    const intentBefore = fs.readFileSync(intentPath, "utf8");
    const replacement = { ...committed, id: "replacement-receipt", sequence: committed.sequence + 1 };
    const close = fs.closeSync.bind(fs);
    let changed = false;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => {
      const confirmedReceipt = sameFile(fd, receiptPath) && !fs.existsSync(path.join(mesh.root, ".lock"));
      close(fd);
      if (!changed && confirmedReceipt) {
        changed = true;
        if (change === "missing") fs.rmSync(receiptPath);
        else fs.writeFileSync(receiptPath, JSON.stringify(replacement));
      }
    });
    expect(await mesh.publish(packet)).toEqual(committed);
    expect(changed).toBe(true);
    expect(fs.readFileSync(intentPath, "utf8")).toBe(intentBefore);
    if (change === "missing") expect(fs.existsSync(receiptPath)).toBe(false);
    else expect(JSON.parse(fs.readFileSync(receiptPath, "utf8"))).toEqual(replacement);
    expect(mesh.read()).toEqual([committed]);
  });
});
