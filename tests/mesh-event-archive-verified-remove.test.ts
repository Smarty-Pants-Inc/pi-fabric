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
  calls: [] as Array<{ target: string; ino: number | undefined; deleter: string; removed: boolean }>,
}));
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const real = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return {
    ...real,
    verifiedRemove: (target: string, judged: fs.Stats | undefined, options: Parameters<typeof real.verifiedRemove>[2]) => {
      // Never a real Linux removal: a dry run whenever requested and always on Linux.
      const dryRun = probe.dryRun || process.platform === "linux";
      const result = real.verifiedRemove(target, judged, dryRun ? { ...options, dryRun: true } : options);
      // Only this lane's deleters are recorded; an auxiliary (lock) release's dry-run success is emulated by a rename.
      if (!options.deleter.startsWith("meshDedupe")) { if (dryRun && result.removed) probe.moveAway?.(target); return result; }
      probe.calls.push({ target, ino: judged?.ino, deleter: options.deleter, removed: result.removed });
      return result;
    },
  };
});

const { MeshStore } = await import("../src/mesh/store.js");
const { MeshDedupeStoreFullError } = await import("../src/mesh/event-log.js");

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
  probe.dryRun = true; probe.calls = []; removals = []; failRemoval = false;
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
