// smarty-dev#8907: mesh migration/rollback temps, the blank state.json drop and abandoned prepared state remove only
// the inode they judged. No product deletion runs here: verifiedRemove is the real verifier forced to dryRun (always on
// Linux), the primitive rmSync/unlinkSync/rmdirSync of state.json* names are no-op recorders, and every other product
// primitive (lock releases) is emulated by a rename into this test's own mkdtemp trash. An auxiliary (non-state.json*)
// dry-run success is emulated by the same rename, so later lock acquisitions work.
import childProcess from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const probe = vi.hoisted(() => ({ dry: true, moveAway: undefined as ((target: string) => void) | undefined,
  judged: [] as Array<{ target: string; ino: number; mtimeMs: number }> }));
vi.mock("../src/storage/verified-remove.js", async (original) => {
  const real = await original<typeof import("../src/storage/verified-remove.js")>();
  return { ...real, verifiedRemove: (target: string, judged: fs.Stats | undefined, options: Parameters<typeof real.verifiedRemove>[2]) => {
    probe.judged.push({ target, ino: judged?.ino ?? -1, mtimeMs: judged?.mtimeMs ?? -1 });
    const dryRun = probe.dry || process.platform === "linux";
    const result = real.verifiedRemove(target, judged, dryRun ? { ...options, dryRun: true } : options);
    if (dryRun && result.removed && !path.basename(target).startsWith("state.json")) probe.moveAway?.(target);
    return result;
  } };
});

const { verifiedRemoveTestHooks } = await import("../src/storage/verified-remove.js");
const { sweepAbandonedStateTemporaries } = await import("../src/mesh/temp-janitor.js");
const { StateFile } = await import("../src/mesh/state-file.js");
const { MeshStore } = await import("../src/mesh/store.js");
const { abortMeshRollback, importMeshState, rollbackMeshState, MeshBackendRefusedError } = await import("../src/mesh/backend-migration.js");

const roots: string[] = [];
const primitive: string[] = [];
let failPrimitive = false;
const rootOf = (): string => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-")); roots.push(root); return root; };
const write = (file: string, text: string): fs.Stats => { fs.writeFileSync(file, text, { mode: 0o600 }); return fs.lstatSync(file); };
/** Planned one-shot swaps (target -> successor text) and the successor inode each actually installed. */
const pendingSwaps = new Map<string, string>();
const swapped = new Map<string, number>();
/** The first removal decision for a planned target, after its judgment: the landed verifier's beforeOpen, or (on base)
 * the no-op primitive recorder before it records. A successor replaces the inode once; the rename is proven here. */
const injectSwap = (at: string): void => {
  const text = pendingSwaps.get(at);
  if (text === undefined) return;
  pendingSwaps.delete(at);
  const before = real.lstatSync(at, { throwIfNoEntry: false });
  const staged = `${at}.swap`;
  fs.writeFileSync(staged, text, { mode: 0o600 });
  real.renameSync(staged, at);
  const now = real.lstatSync(at);
  if (before && now.ino === before.ino) throw new Error(`swap kept the judged inode of ${at}`);
  if (fs.readFileSync(at, "utf8") !== text) throw new Error(`swap content missing at ${at}`);
  swapped.set(at, now.ino);
};
/** A swap between the judgment and the delete, at whichever removal decision comes first (shared by both paths). */
const swapAt = (target: string, text = "successor"): void => {
  pendingSwaps.set(target, text);
  verifiedRemoveTestHooks.beforeOpen = injectSwap;
};
/** The swap really happened: the successor inode was installed at the removal decision. Asserted before any no-removal
 * or control-flow assertion, so a run whose hook never fired cannot pass or count as RED. */
const expectSwapFired = (target: string): void => { expect(swapped.get(target), `swap fired for ${target}`).toBeTypeOf("number"); };
/** The successor installed by the swap still occupies the name with its content. */
const expectSuccessorKept = (target: string, text = "successor"): void => {
  expect(real.lstatSync(target, { throwIfNoEntry: false })?.ino).toBe(swapped.get(target));
  expect(fs.readFileSync(target, "utf8")).toBe(text);
};
const settle = (run: Promise<unknown>): Promise<unknown> => run.then(() => undefined, (error: unknown) => error);
const deadPid = (): number => childProcess.spawnSync(process.execPath, ["-e", ""], { timeout: 5_000 }).pid;
const asPlatform = (platform: string): void => { Object.defineProperty(process, "platform", { value: platform, configurable: true }); };
const realPlatform = process.platform;
// Swap refusals and judged-inode assertions need the real Linux verifier; off Linux main's fallback compares no inode
// (smarty-dev#7858) and is covered by the darwin-stubbed cases on every host.
const linuxIt = it.runIf(realPlatform === "linux");

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
  probe.dry = true; probe.judged.length = 0; primitive.length = 0; failPrimitive = false;
  pendingSwaps.clear(); swapped.clear();
  trash = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-8907-trash-"));
  probe.moveAway = (target) => { if (real.lstatSync(target, { throwIfNoEntry: false })) real.renameSync(target, path.join(trash, randomUUID())); };
  // Recorders for the judged classes (every name starting "state.json"); anything else (locks) is emulated.
  for (const name of ["rmSync", "unlinkSync", "rmdirSync"] as const) {
    vi.spyOn(fs, name).mockImplementation(((file: fs.PathLike, options?: fs.RmOptions) => {
      if (!path.basename(String(file)).startsWith("state.json")) return emulate(name, String(file), options);
      injectSwap(String(file)); // same one-shot swap as beforeOpen, before the attempt is recorded (never executed)
      primitive.push(String(file));
      if (failPrimitive) throw Object.assign(new Error("EBUSY"), { code: "EBUSY" });
    }) as never);
  }
});
afterEach(() => {
  asPlatform(realPlatform);
  delete verifiedRemoveTestHooks.beforeOpen;
  pendingSwaps.clear();
  vi.restoreAllMocks();
  probe.moveAway = undefined;
  // Fixture cleanup only: this case's own mkdtemp roots and trash, with the real primitive.
  for (const root of [...roots.splice(0), trash]) real.rmSync(root, { recursive: true, force: true });
});

const oldTemp = (root: string, pid: number): { file: string; stat: fs.Stats } => {
  const file = path.join(root, `state.json.${pid}.${randomUUID()}.prepared.tmp`);
  write(file, "staged state");
  const old = (Date.now() - 2 * 60 * 60_000) / 1000;
  fs.utimesSync(file, old, old);
  return { file, stat: fs.lstatSync(file) };
};
const openStateFile = (root: string): void => { new StateFile({ root, maxEventBytes: 64 * 1024, maxReadEvents: 1_000, lock: {} as never }, {}); };

/** A sqlite root left at backend=exporting (a rollback crashed after its flag), for abort-rollback's temp cleanup. */
const exportingRoot = async (): Promise<string> => {
  const root = rootOf();
  await new MeshStore(root, 64 * 1024, 1_000).put({ key: "k", value: 1, identity: { id: "t", name: "t", kind: "agent" } });
  await importMeshState(root);
  const crash = new Error("crash after the flag");
  await expect(rollbackMeshState(root, { onStep: (step) => { if (step === "rollback-flag") throw crash; } })).rejects.toBe(crash);
  primitive.length = 0; probe.judged.length = 0;
  return root;
};

describe("abandoned prepared state (smarty-dev#8907)", () => {
  linuxIt("janitor: a successor swapped in after the age judgment is kept, no primitive delete", () => {
    const root = rootOf(), { file } = oldTemp(root, 999_999);
    swapAt(file);
    const swept = sweepAbandonedStateTemporaries(root, { dead: () => true });
    expectSwapFired(file);
    expect(primitive).toEqual([]);
    expectSuccessorKept(file);
    expect(swept).toEqual([]);
  });

  linuxIt("janitor: an unchanged temp is judged with its original stat and verified (dry run), no primitive delete", () => {
    const root = rootOf(), { file, stat } = oldTemp(root, 999_999);
    expect(sweepAbandonedStateTemporaries(root, { dead: () => true })).toEqual([path.basename(file)]);
    expect(probe.judged).toEqual([{ target: file, ino: stat.ino, mtimeMs: stat.mtimeMs }]);
    expect(primitive).toEqual([]);
  });

  linuxIt("store open sweep: a successor swapped in after the dead-pid judgment is kept, no primitive delete", () => {
    const root = rootOf(), { file } = oldTemp(root, deadPid());
    swapAt(file);
    openStateFile(root);
    expectSwapFired(file);
    expect(primitive).toEqual([]);
    expectSuccessorKept(file);
  });

  linuxIt("store open sweep: an unchanged dead-pid temp is verified with its original stat (dry run)", () => {
    const root = rootOf(), { file, stat } = oldTemp(root, deadPid());
    openStateFile(root);
    expect(probe.judged).toEqual([{ target: file, ino: stat.ino, mtimeMs: stat.mtimeMs }]);
    expect(primitive).toEqual([]);
  });

  it("off Linux: main's path removal is the fallback, and its failure is not reported as removed", () => {
    probe.dry = false;
    asPlatform("darwin");
    const root = rootOf(), janitor = oldTemp(root, 999_999).file;
    expect(sweepAbandonedStateTemporaries(root, { dead: () => true })).toEqual([path.basename(janitor)]);
    expect(primitive).toEqual([janitor]);
    failPrimitive = true; primitive.length = 0;
    expect(sweepAbandonedStateTemporaries(root, { dead: () => true })).toEqual([]);
    expect(primitive).toEqual([janitor]);
    failPrimitive = false; primitive.length = 0;
    const other = rootOf(), swept = oldTemp(other, deadPid()).file;
    openStateFile(other);
    expect(primitive).toEqual([swept]);
  });
});

describe("migration and rollback temps (smarty-dev#8907)", () => {
  linuxIt("abort-rollback: a rollback temp swapped after its judgment is kept and the abort refused", async () => {
    const root = await exportingRoot();
    const temp = path.join(root, "state.json.rollback-9.tmp");
    write(temp, "old export");
    swapAt(temp);
    const outcome = await settle(abortMeshRollback(root));
    expectSwapFired(temp);
    expect(primitive).toEqual([]);
    expectSuccessorKept(temp);
    expect(outcome).toBeInstanceOf(MeshBackendRefusedError);
  });

  linuxIt("abort-rollback: unchanged temps are verified with their original stats (dry run), no primitive delete", async () => {
    const root = await exportingRoot();
    const rollback = path.join(root, "state.json.rollback-9.tmp"), stale = path.join(root, "state.json.mesh-backend-1-x.tmp");
    const a = write(rollback, "old export"), b = write(stale, "old marker");
    await expect(abortMeshRollback(root)).resolves.toMatchObject({ backend: "sqlite" });
    expect(probe.judged).toEqual([{ target: rollback, ino: a.ino, mtimeMs: a.mtimeMs }, { target: stale, ino: b.ino, mtimeMs: b.mtimeMs }]);
    expect(primitive).toEqual([]);
  });

  it("off Linux: a failed fallback removal refuses the abort instead of continuing", async () => {
    const root = await exportingRoot();
    const temp = path.join(root, "state.json.rollback-9.tmp");
    write(temp, "old export");
    probe.dry = false; failPrimitive = true;
    asPlatform("darwin");
    await expect(abortMeshRollback(root)).rejects.toThrow(); // main threw the rmSync error; now the refusal
    asPlatform(realPlatform);
    expect(primitive).toEqual([temp]);
  });

  linuxIt("import: a blank state.json swapped after its content judgment is kept and the import refused", async () => {
    const root = rootOf(), state = path.join(root, "state.json");
    write(state, "  \n");
    swapAt(state, "{}");
    const outcome = await settle(importMeshState(root));
    expectSwapFired(state);
    expect(primitive).toEqual([]);
    expectSuccessorKept(state, "{}");
    expect(outcome).toBeInstanceOf(MeshBackendRefusedError);
    expect(String((outcome as Error).message)).toMatch(/blank state.json was not dropped/);
  });
});
