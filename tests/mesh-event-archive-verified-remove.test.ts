// smarty-dev#8907: event-log dedupe receipt expiry removes only the receipt it judged. No product deletion runs here:
// primitive removals under event-receipts and event-receipts-pruned are recorded no-ops (or test-trash renames for retry); every other product rm/unlink/rmdir (lock releases) is
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
      if (options.deleter === "meshDedupeReceiptPrune") {
        expect(fs.existsSync(path.join(path.dirname(path.dirname(target)), ".lock"))).toBe(false);
      }
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
let emulateReceiptRemoval = false;
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
const retiredReceipt = (file: string) => {
  expect(probe.calls).toHaveLength(1);
  const target = probe.calls[0]!.target;
  expect(path.dirname(target)).toBe(path.join(path.dirname(path.dirname(file)), "event-receipts-pruned"));
  expect(path.basename(target)).toMatch(new RegExp(`^${path.basename(file, ".json")}\\.[a-f0-9-]{36}\\.json$`));
  return target;
};

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
  probe.dryRun = true; probe.calls = []; removals = []; failRemoval = false; emulateReceiptRemoval = false;
  trash = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-trash-"));
  probe.moveAway = target => { if (real.lstatSync(target, { throwIfNoEntry: false })) real.renameSync(target, path.join(trash, randomUUID())); };
  for (const name of ["rmSync", "unlinkSync", "rmdirSync"] as const) {
    vi.spyOn(fs, name).mockImplementation(((target: fs.PathLike, options?: fs.RmOptions) => {
      if (!/^event-receipts(?:-pruned)?$/.test(path.basename(path.dirname(String(target))))) return emulate(name, String(target), options);
      removals.push(String(target)); // recorded, never executed
      // Fail only a receipt's physical removal, not an atomic writer's no-op temp cleanup.
      if (/^[a-f0-9]{64}(?:\.[a-f0-9-]{36})?\.json$/.test(path.basename(String(target)))) {
        if (failRemoval) throw Object.assign(new Error("EPERM"), { code: "EPERM" });
        if (emulateReceiptRemoval) probe.moveAway?.(String(target));
      }
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
  const { root, file, store } = seed();
  const readFileSync = fs.readFileSync;
  let swapped = false;
  let successor!: { ino: number; bytes: Buffer };
  const renames = vi.spyOn(fs, "renameSync");
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    const text = (readFileSync as (...args: unknown[]) => unknown)(target, ...rest);
    if (target === file && !swapped) {
      // The judgment just read the old receipt: a same-key successor now takes its name (a new inode).
      swapped = true;
      fs.writeFileSync(`${file}.swap`, receiptText("old", "successor-id", Date.now()), { mode: 0o600 });
      fs.renameSync(`${file}.swap`, file);
      successor = { ino: fs.lstatSync(file).ino, bytes: readFileSync(file) as Buffer };
    }
    return text;
  }) as never);
  await expect(publishNew(store)).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
  expect(swapped).toBe(true);
  expect(receiptRemovals(file)).toEqual([]);
  expect(JSON.parse(readFileSync(file, "utf8") as string).id).toBe("successor-id");
  // Approved seam change: the locked identity CAS now refuses before verifiedRemove.
  // No retirement rename and no removal may touch the successor, whose exact inode/bytes survive.
  expect(renames.mock.calls.filter(([, target]) => path.dirname(String(target)) === path.join(root, "event-receipts-pruned"))).toEqual([]);
  expect(probe.calls).toEqual([]);
  expect(fs.lstatSync(file).ino).toBe(successor.ino);
  expect(readFileSync(file)).toEqual(successor.bytes);
  expect(fs.existsSync(receiptPath(root, "new"))).toBe(false);
  expect(store.read()).toEqual([]);
});

it.runIf(process.platform === "linux").each(["expired", "within TTL"] as const)("verifies an unchanged %s receipt by its judged inode (dry run)", async age => {
  const { file, ino, store } = seed();
  if (age === "within TTL") fs.writeFileSync(file, receiptText("old", "old-id", Date.now()));
  await expect(publishNew(store)).resolves.toMatchObject({ dedupeKey: "new" });
  const retired = retiredReceipt(file);
  expect(probe.calls[0]).toEqual({ target: retired, ino, deleter: "meshDedupeReceiptPrune", removed: true });
  expect(receiptRemovals(retired)).toEqual([]);
  expect(fs.existsSync(file)).toBe(false); // logical retirement moved the canonical name
  expect(fs.lstatSync(retired).ino).toBe(ino); // dry run: the judged receipt was not deleted
  expect(JSON.parse(fs.readFileSync(retired, "utf8")).id).toBe("old-id");
});

it("off Linux keeps a committed publication successful when fallback cleanup fails, then retries next pass", async () => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  if (process.platform === "linux") Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
  try {
    probe.dryRun = false;
    const first = seed();
    await expect(publishNew(first.store)).resolves.toMatchObject({ dedupeKey: "new" });
    const firstRetired = retiredReceipt(first.file);
    expect(receiptRemovals(firstRetired)).toEqual([firstRetired]);
    expect(probe.calls[0]).toEqual({ target: firstRetired, ino: first.ino, deleter: "meshDedupeReceiptPrune", removed: true });
    expect(fs.lstatSync(firstRetired).ino).toBe(first.ino); // fallback is recorded, never executed

    probe.calls = []; failRemoval = true;
    const second = seed();
    const event = await publishNew(second.store);
    expect(event).toMatchObject({ dedupeKey: "new" });
    const secondRetired = retiredReceipt(second.file);
    expect(receiptRemovals(secondRetired)).toEqual([secondRetired]);
    expect(probe.calls[0]).toEqual({ target: secondRetired, ino: second.ino, deleter: "meshDedupeReceiptPrune", removed: false });
    expect(fs.lstatSync(secondRetired).ino).toBe(second.ino);
    expect(fs.existsSync(second.file)).toBe(false);
    // Logical retirement and append succeeded: cleanup cannot fail the publication.
    expect(second.store.read()).toEqual([event]);
    await expect(publishNew(second.store)).resolves.toEqual(event); // ongoing outage still succeeds
    expect(fs.existsSync(secondRetired)).toBe(true);
    failRemoval = false; emulateReceiptRemoval = true;
    await expect(publishNew(second.store)).resolves.toEqual(event);
    expect(fs.existsSync(secondRetired)).toBe(false); // emulated removal is a rename into test-owned trash
    expect(probe.calls.at(-1)).toEqual({ target: secondRetired, ino: second.ino, deleter: "meshDedupeReceiptPrune", removed: true });
    expect(second.store.read()).toEqual([event]);
  } finally {
    Object.defineProperty(process, "platform", platform);
  }
});
