// smarty-dev#8907: event-log dedupe receipt expiry removes only the receipt it judged. No product deletion runs here:
// primitive removals under event-receipts are recorded no-ops; every other product rm/unlink/rmdir (lock releases) is
// emulated by a rename into this test's own mkdtemp trash. Every verifiedRemove on Linux is the real verifier forced to
// a dry run (an auxiliary lock release's dry-run success is emulated by the same rename, so later acquisitions work);
// under the forced non-Linux platform it is main's fallback over those probed primitives.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

const probe = vi.hoisted(() => ({
  dryRun: true,
  moveAway: undefined as ((target: string) => void) | undefined,
  /** A verifier refusal shape the dry run cannot produce (an entry held in a graveyard), emulated by a test rename. */
  graveyard: undefined as ((target: string) => { removed: false; reason: string; graveyard: string }) | undefined,
  calls: [] as Array<{ target: string; ino: number | undefined; deleter: string; removed: boolean }>,
}));
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const real = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return {
    ...real,
    verifiedRemove: (target: string, judged: fs.Stats | undefined, options: Parameters<typeof real.verifiedRemove>[2]) => {
      // Never a real Linux removal: a dry run whenever requested and always on Linux.
      const dryRun = probe.dryRun || process.platform === "linux";
      const result = options.deleter === "meshDedupeIntent" && probe.graveyard ? probe.graveyard(target)
        : real.verifiedRemove(target, judged, dryRun ? { ...options, dryRun: true } : options);
      // Only this lane's deleters are recorded; an auxiliary (lock) release's dry-run success is emulated by a rename.
      if (!options.deleter.startsWith("meshDedupe")) { if (dryRun && result.removed) probe.moveAway?.(target); return result; }
      probe.calls.push({ target, ino: judged?.ino, deleter: options.deleter, removed: result.removed });
      return result;
    },
  };
});

const { MeshStore } = await import("../src/mesh/store.js");
const { MeshDedupeStoreFullError, MeshDedupeRecoveryError } = await import("../src/mesh/event-log.js");

const from = { id: "session:8907", name: "8907", kind: "main" as const };
const roots: string[] = [];
let removals: string[] = [];
let failRemoval = false;
const receiptPath = (root: string, key: string) =>
  path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + ".json");
const receiptText = (key: string, id: string, createdAt: number) =>
  JSON.stringify({ id, dedupeKey: key, sequence: 1, topic: "mesh.8907", kind: "message", from, createdAt });

/** One expired-by-capacity receipt "old" in a store that holds one key. */
const seed = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-"));
  roots.push(root);
  fs.mkdirSync(path.join(root, "event-receipts"), { mode: 0o700 });
  const file = receiptPath(root, "old");
  fs.writeFileSync(file, receiptText("old", "old-id", 1_000), { mode: 0o600 });
  return { root, file, ino: fs.lstatSync(file).ino, store: new MeshStore(root, 1024, 100, { maxDedupeReceipts: 1 }) };
};
const publishNew = (store: InstanceType<typeof MeshStore>) => store.publish({ topic: "mesh.8907", from, dedupeKey: "new" });
const receiptRemovals = (file: string) => removals.filter(target => target === file);

const real = { lstatSync: fs.lstatSync, readdirSync: fs.readdirSync, renameSync: fs.renameSync, rmSync: fs.rmSync };
let trash = "";
const fsError = (code: string, op: string, target: string) => Object.assign(new Error(`${code}: ${op} ${target}`), { code });
/** Emulates a product rm/unlink/rmdir by renaming the entry itself (never a link's target) into this test's trash.
 * Keeps the primitive's refusals: ENOENT (rm without force), EISDIR, ENOTDIR and rmdir's ENOTEMPTY. */
const emulate = (op: "rmSync" | "unlinkSync" | "rmdirSync", target: string, options?: fs.RmOptions): void => {
  const stat = real.lstatSync(target, { throwIfNoEntry: false });
  if (!stat) { if (op === "rmSync" && options?.force) return; throw fsError("ENOENT", op, target); }
  if (stat.isDirectory()) {
    if (op === "unlinkSync") throw fsError("EISDIR", op, target);
    if (op === "rmSync" && !options?.recursive) throw fsError("ERR_FS_EISDIR", op, target);
    if (op === "rmdirSync" && real.readdirSync(target).length > 0) throw fsError("ENOTEMPTY", op, target);
  } else if (op === "rmdirSync") throw fsError("ENOTDIR", op, target);
  real.renameSync(target, path.join(trash, randomUUID()));
};

beforeEach(() => {
  probe.dryRun = true; probe.calls = []; removals = []; failRemoval = false; probe.graveyard = undefined;
  trash = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-trash-"));
  probe.moveAway = target => { if (real.lstatSync(target, { throwIfNoEntry: false })) real.renameSync(target, path.join(trash, randomUUID())); };
  for (const name of ["rmSync", "unlinkSync", "rmdirSync"] as const) {
    vi.spyOn(fs, name).mockImplementation(((target: fs.PathLike, options?: fs.RmOptions) => {
      if (!String(target).includes(`${path.sep}event-receipts${path.sep}`)) return emulate(name, String(target), options);
      removals.push(String(target)); // recorded, never executed
      if (failRemoval) throw Object.assign(new Error("EPERM"), { code: "EPERM" });
    }) as never);
  }
});
afterEach(() => {
  vi.restoreAllMocks();
  probe.moveAway = undefined;
  // Fixture cleanup only: this case's own mkdtemp roots and trash, with the real primitive.
  for (const root of [...roots.splice(0), trash]) real.rmSync(root, { recursive: true, force: true });
});

it.runIf(process.platform === "linux")("refuses a receipt swapped after its age judgment and keeps admission closed", async () => {
  const { file, ino, store } = seed();
  const readFileSync = fs.readFileSync;
  let swapped = false;
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    const text = (readFileSync as (...args: unknown[]) => unknown)(target, ...rest);
    if (target === file && !swapped) {
      // The judgment just read the old receipt: a same-key successor now takes its name (a new inode).
      swapped = true;
      fs.writeFileSync(`${file}.swap`, receiptText("old", "successor-id", Date.now()), { mode: 0o600 });
      fs.renameSync(`${file}.swap`, file);
    }
    return text;
  }) as never);
  await expect(publishNew(store)).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
  expect(swapped).toBe(true);
  expect(receiptRemovals(file)).toEqual([]);
  expect(JSON.parse(readFileSync(file, "utf8") as string).id).toBe("successor-id");
  expect(probe.calls).toEqual([{ target: file, ino, deleter: "meshDedupeReceiptPrune", removed: false }]);
});

it.runIf(process.platform === "linux")("verifies an unchanged expired receipt by its judged inode (dry run)", async () => {
  const { file, ino, store } = seed();
  await expect(publishNew(store)).resolves.toMatchObject({ dedupeKey: "new" });
  expect(probe.calls[0]).toEqual({ target: file, ino, deleter: "meshDedupeReceiptPrune", removed: true });
  expect(receiptRemovals(file)).toEqual([]);
  expect(fs.existsSync(file)).toBe(true); // dry run: nothing was deleted
});

// F-01: a dead publisher's intent (nothing committed at its offset) in a no-archive store, judged by the retry.
const intentText = (key: string, eventId: string, reservedSequence: number) =>
  JSON.stringify({ dedupeKey: key, reservedSequence, eventId, liveOffset: 0 });
const seedIntent = (options: ConstructorParameters<typeof MeshStore>[3] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-"));
  roots.push(root);
  fs.mkdirSync(path.join(root, "event-receipts"), { mode: 0o700 });
  const file = receiptPath(root, "k").replace(/\.json$/, ".pending.json");
  fs.writeFileSync(file, intentText("k", "dead-id", 1), { mode: 0o600 });
  return { root, file, ino: fs.lstatSync(file).ino, store: new MeshStore(root, 1024, 100, options) };
};
/** Runs `act` once, right after the settle read the intent's bytes (between judgment and removal, shared by main's
 * by-path primitive and the verifier). */
const afterIntentRead = (file: string, act: () => void) => {
  const readFileSync = fs.readFileSync;
  const state = { fired: false };
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    const text = (readFileSync as (...args: unknown[]) => unknown)(target, ...rest);
    if (target === file && !state.fired) { state.fired = true; act(); }
    return text;
  }) as never);
  return state;
};
const installSuccessor = (file: string) => {
  fs.writeFileSync(`${file}.swap`, intentText("k", "successor-id", 7), { mode: 0o600 });
  fs.renameSync(`${file}.swap`, file);
};
const nothingWritten = (root: string) => {
  for (const name of ["sequence", "events.jsonl", "generation"]) expect(fs.existsSync(path.join(root, name))).toBe(false);
  expect(fs.existsSync(receiptPath(root, "k"))).toBe(false);
};

it.runIf(process.platform === "linux")("refuses a no-archive intent swapped after judgment and never overwrites it", async () => {
  const { root, file, ino, store } = seedIntent();
  const swap = afterIntentRead(file, () => installSuccessor(file));
  await expect(store.publish({ topic: "mesh.8907", from, dedupeKey: "k" })).rejects.toBeInstanceOf(MeshDedupeRecoveryError);
  expect(swap.fired).toBe(true);
  const successor = fs.lstatSync(file).ino;
  expect(successor).not.toBe(ino); // a real new inode was installed
  expect(probe.calls).toEqual([{ target: file, ino, deleter: "meshDedupeIntent", removed: false }]);
  expect(fs.lstatSync(file).ino).toBe(successor);
  expect(JSON.parse(fs.readFileSync(file, "utf8") as string)).toMatchObject({ eventId: "successor-id", reservedSequence: 7 });
  expect(receiptRemovals(file)).toEqual([]); // the intent name's primitive was never reached
  nothingWritten(root);
});

it.runIf(process.platform === "linux")("settles an unchanged stale intent by its judged inode and publishes fresh", async () => {
  const { file, ino, store } = seedIntent();
  await expect(store.publish({ topic: "mesh.8907", from, dedupeKey: "k" })).resolves.toMatchObject({ dedupeKey: "k", sequence: 1 });
  expect(probe.calls[0]).toEqual({ target: file, ino, deleter: "meshDedupeIntent", removed: true });
  expect(receiptRemovals(file)).toEqual([]);
});

it.runIf(process.platform === "linux")("treats an intent confirmed gone before verification as settled", async () => {
  const { file, ino, store } = seedIntent();
  // Moved away (a test rename, not a product delete) before the verifier opens it: confirmed absent, no graveyard.
  afterIntentRead(file, () => real.renameSync(file, path.join(trash, randomUUID())));
  await expect(store.publish({ topic: "mesh.8907", from, dedupeKey: "k" })).resolves.toMatchObject({ dedupeKey: "k", sequence: 1 });
  expect(probe.calls[0]).toEqual({ target: file, ino, deleter: "meshDedupeIntent", removed: false });
  expect(receiptRemovals(file)).toEqual([]);
});

it.runIf(process.platform === "linux")("never treats a graveyard-held refusal as an absent intent", async () => {
  const { root, file, store } = seedIntent();
  const held = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-grave-"));
  roots.push(held);
  probe.graveyard = target => {
    const graveyard = path.join(held, path.basename(target));
    real.renameSync(target, graveyard); // the name is now absent, the entry held
    return { removed: false, reason: "target inode was replaced since judgment (swapped during removal; left in the graveyard)", graveyard };
  };
  await expect(store.publish({ topic: "mesh.8907", from, dedupeKey: "k" })).rejects.toBeInstanceOf(MeshDedupeRecoveryError);
  expect(fs.existsSync(file)).toBe(false);
  expect(probe.calls).toHaveLength(1);
  nothingWritten(root);
});

it.runIf(process.platform === "linux")("compaction stops before rewriting the log when an intent removal is refused", async () => {
  const { root, file, ino, store } = seedIntent({ maxEventLogBytes: 1 });
  const line = (sequence: number) => JSON.stringify({ id: `fill-${sequence}`, sequence, topic: "mesh.8907", kind: "message", from, text: "x".repeat(600), createdAt: 1 });
  fs.writeFileSync(path.join(root, "events.jsonl"), `${line(1)}\n${line(2)}\n`, { mode: 0o600 });
  const swap = afterIntentRead(file, () => installSuccessor(file));
  await expect(store.publish({ topic: "mesh.8907", from })).rejects.toBeInstanceOf(MeshDedupeRecoveryError);
  expect(swap.fired).toBe(true);
  expect(probe.calls).toEqual([{ target: file, ino, deleter: "meshDedupeIntent", removed: false }]);
  expect(JSON.parse(fs.readFileSync(file, "utf8") as string)).toMatchObject({ eventId: "successor-id" });
  expect(fs.existsSync(path.join(root, "generation"))).toBe(false); // the log was not rewritten
  expect((fs.readFileSync(path.join(root, "events.jsonl"), "utf8") as string).startsWith(`${line(1)}\n`)).toBe(true);
  expect(receiptRemovals(file)).toEqual([]);
});

// F-02: a batch whose keyed suffix is refused after a committed prefix. The 50 ms batch budget reads a frozen clock so the
// prefix/suffix split never depends on host speed (the cap itself is unchanged).
const seedOversizedBatch = () => {
  vi.spyOn(performance, "now").mockReturnValue(1);
  const seeded = seedIntent({ maxEventLogBytes: 8192, retainedEventLogBytes: 6000 });
  const line = (sequence: number) => JSON.stringify({ id: `fill-${sequence}`, sequence, topic: "mesh.8907", kind: "message", from, text: "x".repeat(600), createdAt: 1 });
  const log = Array.from({ length: 15 }, (_, index) => `${line(index + 1)}\n`).join("");
  fs.writeFileSync(path.join(seeded.root, "events.jsonl"), log, { mode: 0o600 });
  return { ...seeded, log };
};

it.runIf(process.platform === "linux")("a batch refusal after a committed prefix finishes the prefix and never re-judges the successor", async () => {
  const { root, file, ino, store, log } = seedOversizedBatch();
  const swap = afterIntentRead(file, () => installSuccessor(file));
  const outcome = await store.publishBatch([
    { topic: "mesh.8907", from, dedupeKey: "p" },
    { topic: "mesh.8907", from, dedupeKey: "k" },
  ]).then(() => undefined, (error: unknown) => error);
  expect(swap.fired).toBe(true);
  const successor = fs.lstatSync(file).ino;
  expect(successor).not.toBe(ino);
  // Only the original inode was judged (and refused); compaction never authorized the successor.
  expect(probe.calls.filter(call => call.target === file)).toEqual([{ target: file, ino, deleter: "meshDedupeIntent", removed: false }]);
  expect(outcome).toBeInstanceOf(MeshDedupeRecoveryError);
  expect(JSON.parse(fs.readFileSync(file, "utf8") as string)).toMatchObject({ eventId: "successor-id", reservedSequence: 7 });
  expect(fs.existsSync(path.join(root, "generation"))).toBe(false); // no compaction rewrite
  const text = fs.readFileSync(path.join(root, "events.jsonl"), "utf8") as string;
  expect(text.startsWith(log)).toBe(true);
  const appended = text.slice(log.length).trim().split("\n").map(entry => JSON.parse(entry) as { dedupeKey?: string; id: string });
  expect(appended.map(event => event.dedupeKey)).toEqual(["p"]); // the refused key appended nothing
  // The committed prefix still finished through the outer handler: its receipt is durable and its own intent was
  // settled once, by the after-release receipt CAS (dry run, so the name stays), not by a same-hold compaction.
  expect(JSON.parse(fs.readFileSync(receiptPath(root, "p"), "utf8") as string)).toMatchObject({ id: appended[0]!.id, dedupeKey: "p" });
  const prefixIntent = receiptPath(root, "p").replace(/\.json$/, ".pending.json");
  expect(probe.calls.filter(call => call.target === prefixIntent)).toMatchObject([{ deleter: "meshDedupeIntent", removed: true }]);
  expect(fs.existsSync(receiptPath(root, "k"))).toBe(false);
  expect(receiptRemovals(file)).toEqual([]);
});

it.runIf(process.platform === "linux")("an ordinary batch tail failure still returns the committed prefix and compacts", async () => {
  const { root, file, ino, store } = seedOversizedBatch();
  const events = await store.publishBatch([
    { topic: "mesh.8907", from, dedupeKey: "p" },
    { topic: "mesh.8907", from, text: "y".repeat(2048) }, // exceeds maxEventBytes before any append
  ]);
  expect(events.map(event => event.dedupeKey)).toEqual(["p"]);
  expect(JSON.parse(fs.readFileSync(receiptPath(root, "p"), "utf8") as string)).toMatchObject({ id: events[0]!.id });
  // Normal cleanup: compaction settled the unchanged stale intent by its judged inode (dry run) and rewrote the log.
  expect(probe.calls.filter(call => call.target === file)).toEqual([{ target: file, ino, deleter: "meshDedupeIntent", removed: true }]);
  expect(fs.existsSync(path.join(root, "generation"))).toBe(true);
  expect(receiptRemovals(file)).toEqual([]);
});

it("off Linux runs main's removal as the fallback and propagates its failure", async () => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
  try {
    probe.dryRun = false;
    const first = seed();
    await expect(publishNew(first.store)).resolves.toMatchObject({ dedupeKey: "new" });
    expect(receiptRemovals(first.file)).toEqual([first.file]);
    expect(probe.calls[0]).toMatchObject({ target: first.file, deleter: "meshDedupeReceiptPrune", removed: true });

    probe.calls = []; failRemoval = true;
    const second = seed();
    await expect(publishNew(second.store)).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
    expect(probe.calls[0]).toMatchObject({ target: second.file, deleter: "meshDedupeReceiptPrune", removed: false });
  } finally {
    Object.defineProperty(process, "platform", platform);
  }
});
