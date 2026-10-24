import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { pinFenceLock, releaseFenceLock } from "../src/mesh/fence-lock.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, withInitialMeshLockAcquisition, withInitialMeshLockAdmission } from "../src/mesh/mesh-lock.js";

// Canonical release confirmation. A release is confirmed once OUR owner record provably no longer exists at the
// canonical name: after our own pinned owner unlink, the name may be gone or held by another party's directory or
// record. rename(2): a directory rename atomically replaces an EMPTY directory at newpath and fails with ENOTEMPTY or
// EEXIST on a nonempty one (Node's fs.renameSync is that call), so a staged successor can land only after our unlink.
// Off Linux the canonical directory is renamed away whole: a successful rename has already freed the name, and Node's
// recursive fs.rmSync of the detached copy may still throw (EBUSY, EPERM, ...; maxRetries defaults to 0). That is a
// cleanup outcome of the detached copy, which keeps its record. Counterexamples stay unconfirmed: before our unlink a
// missing or foreign name proves nothing (our record may sit in a detached directory), and a failed proof read, unlink,
// rmdir of our own same directory, or off-Linux rename keeps its native cause. Each fault hits one exact path of this
// test's private mkdtemp tree and is restored before the assertions and before teardown, which deletes only that tree.

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const bases: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(process, "platform", platform);
  for (const base of bases.splice(0)) fs.rmSync(base, { recursive: true, force: true });
});
const privateBase = (): string => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-release-confirmation-"));
  bases.push(base);
  return base;
};

const codeOf = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;
const shape = (error: unknown) => error === undefined ? undefined
  : { name: error instanceof Error ? error.name : typeof error, code: codeOf(error), cause: codeOf((error as { cause?: unknown }).cause) };
const fault = (code: string, syscall: string, errno: number): NodeJS.ErrnoException =>
  Object.assign(new Error(`${code}: injected ${syscall} failure`), { code, errno, syscall });
const record = (name: string): string => `${name}-${randomUUID()}\n${process.pid}\n${Date.now()}\n`;
const ownerOf = (directory: string): string => fs.readFileSync(path.join(directory, "owner"), "utf8");
const sameDirectory = (stat: fs.Stats | undefined, other: fs.Stats): boolean =>
  stat !== undefined && stat.isDirectory() && stat.dev === other.dev && stat.ino === other.ino;

/** Our lock: a complete record published at the canonical name (protocol-1 wire), its directory pinned by an fd. */
const pinnedLock = (lock: string) => {
  const owner = record("ours");
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), owner, { encoding: "utf8", flag: "wx", mode: 0o600 });
  const directory = fs.lstatSync(lock);
  const pin = pinFenceLock(lock, owner);
  expect(pin, "our own directory is pinned").toBeGreaterThanOrEqual(0);
  return { owner, directory, pin: pin!, pinnedOwner: path.join("/proc/self/fd", String(pin), "owner") };
};
/** The release closed its pin: the descriptor is closed, or a later open reused its number for another inode. */
const pinClosed = (pin: number, directory: fs.Stats): boolean => {
  try { return !sameDirectory(fs.fstatSync(pin), directory); }
  catch (error) { return codeOf(error) === "EBADF"; }
};
/** A protocol-2 contender (the mesh lock's staged wire): a complete owner in its own mkdtemp staging directory whose
 * absence check passed while the name was free, paused before its rename onto the canonical name. */
const pausedProtocol2Contender = (lock: string) => {
  const owner = record("successor");
  const staging = fs.mkdtempSync(`${lock}.pending.${randomUUID()}.`);
  fs.writeFileSync(path.join(staging, "owner"), owner, { encoding: "utf8", flag: "wx", mode: 0o600 });
  expect(fs.lstatSync(lock, { throwIfNoEntry: false }), "its absence check passed while the name was free").toBeUndefined();
  return { owner, publish: (): fs.Stats => { fs.renameSync(staging, lock); return fs.lstatSync(lock); } };
};
const expectHeldBy = (lock: string, directory: fs.Stats | undefined, owner: string): void => {
  expect(directory, "the successor published").toBeDefined();
  expect(sameDirectory(fs.lstatSync(lock, { throwIfNoEntry: false }), directory!), "the successor's directory holds the name").toBe(true);
  expect(fs.readdirSync(lock)).toEqual(["owner"]);
  expect(ownerOf(lock), "the successor's exact record").toBe(owner);
};

describe.runIf(process.platform === "linux")("pinned release: confirmed only after our own owner unlink", () => {
  it("a protocol-2 successor renamed over our emptied canonical directory before the recheck: released, successor intact", () => {
    const root = privateBase(), lock = path.join(root, ".lock");
    const contender = pausedProtocol2Contender(lock);
    const ours = pinnedLock(lock);
    let fenced: string | undefined;
    let successor: fs.Stats | undefined;
    const result = releaseFenceLock(lock, ours.owner, {
      // Before our unlink our record keeps the canonical directory nonempty: the contender's rename cannot land.
      afterCheck: () => { try { contender.publish(); fenced = "published"; } catch (error) { fenced = codeOf(error); } },
      afterTake: () => { successor = contender.publish(); },
    }, ours.pin);
    expect(["ENOTEMPTY", "EEXIST"], "rename(2) refuses a nonempty target").toContain(fenced);
    expectHeldBy(lock, successor, contender.owner);
    expect(fs.readdirSync(root), "nothing else is left or created").toEqual([".lock"]);
    expect(pinClosed(ours.pin, ours.directory), "the pin is closed").toBe(true);
    expect(result, "our owner was provably unlinked; the name now belongs to the successor").toMatchObject({ released: true });
  });

  it("the same successor rename landing after the inode recheck, right before rmdir (native ENOTEMPTY): released, successor intact", () => {
    const root = privateBase(), lock = path.join(root, ".lock");
    const contender = pausedProtocol2Contender(lock);
    const ours = pinnedLock(lock);
    const rmdir = fs.rmdirSync;
    let successor: fs.Stats | undefined;
    let nativeRmdir: string | undefined;
    vi.spyOn(fs, "rmdirSync").mockImplementation((...args: Parameters<typeof fs.rmdirSync>) => {
      // Only the release's rmdir of OUR canonical directory, already emptied by our own owner unlink.
      if (successor === undefined && path.resolve(String(args[0])) === lock &&
        sameDirectory(fs.lstatSync(lock, { throwIfNoEntry: false }), ours.directory) && fs.readdirSync(lock).length === 0) {
        successor = contender.publish();
        try { return rmdir(...args); } catch (error) { nativeRmdir = codeOf(error); throw error; }
      }
      return rmdir(...args);
    });
    const result = releaseFenceLock(lock, ours.owner, {}, ours.pin);
    vi.restoreAllMocks();
    expect(["ENOTEMPTY", "EEXIST"], "the real rmdir met the successor's record").toContain(nativeRmdir);
    expectHeldBy(lock, successor, contender.owner);
    expect(fs.readdirSync(root), "nothing else is left or created").toEqual([".lock"]);
    expect(pinClosed(ours.pin, ours.directory), "the pin is closed").toBe(true);
    expect(result, "our owner was provably unlinked; the name now belongs to the successor").toMatchObject({ released: true });
  });

  it("our emptied canonical directory removed by another party after our owner unlink: released", () => {
    const root = privateBase(), lock = path.join(root, ".lock");
    const ours = pinnedLock(lock);
    let removed = false;
    // An empty ownerless directory past its grace: a recoverer's atomic rmdir (it fails closed on any record).
    const result = releaseFenceLock(lock, ours.owner, { afterTake: () => { fs.rmdirSync(lock); removed = true; } }, ours.pin);
    expect(removed).toBe(true);
    expect(fs.readdirSync(root), "the name is gone; nothing else was created").toEqual([]);
    expect(pinClosed(ours.pin, ours.directory), "the pin is closed").toBe(true);
    expect(result, "our owner was provably unlinked and the name is gone").toMatchObject({ released: true });
  });

  it("a paused protocol-1 writer publishing into our emptied directory after our owner unlink: released, its record kept", () => {
    const root = privateBase(), lock = path.join(root, ".lock");
    const ours = pinnedLock(lock);
    const foreign = record("protocol-1-writer");
    let written = false;
    const result = releaseFenceLock(lock, ours.owner, { afterTake: () => {
      // Paused since an earlier mkdir at this name: its exclusive owner write can land only once ours is gone.
      fs.writeFileSync(path.join(lock, "owner"), foreign, { encoding: "utf8", flag: "wx", mode: 0o600 });
      written = true;
    } }, ours.pin);
    expect(written).toBe(true);
    expect(sameDirectory(fs.lstatSync(lock), ours.directory), "the same directory, now holding only the foreign record").toBe(true);
    expect(fs.readdirSync(lock)).toEqual(["owner"]);
    expect(ownerOf(lock), "the foreign writer's exact record").toBe(foreign);
    expect(pinClosed(ours.pin, ours.directory), "the pin is closed").toBe(true);
    expect(result, "our owner was provably unlinked; the directory now holds only another owner's record").toMatchObject({ released: true });
  });

  it.each(["a successor", "nothing"] as const)("counter: our directory detached WITH our record before our unlink, %s at the name: unconfirmed, untouched", atName => {
    const root = privateBase(), lock = path.join(root, ".lock"), receipt = `${lock}.dead.test-receipt`;
    const contender = atName === "a successor" ? pausedProtocol2Contender(lock) : undefined;
    const ours = pinnedLock(lock);
    let successor: fs.Stats | undefined;
    const result = releaseFenceLock(lock, ours.owner, { afterCheck: () => {
      fs.renameSync(lock, receipt); // a recoverer's whole-directory receipt
      successor = contender?.publish();
    } }, ours.pin);
    expect(sameDirectory(fs.lstatSync(receipt), ours.directory), "our directory is the detached receipt").toBe(true);
    expect(fs.readdirSync(receipt)).toEqual(["owner"]);
    expect(ownerOf(receipt), "our exact record is still in the detached directory").toBe(ours.owner);
    if (contender) expectHeldBy(lock, successor, contender.owner);
    else expect(fs.lstatSync(lock, { throwIfNoEntry: false }), "a missing name before our unlink proves nothing").toBeUndefined();
    expect(pinClosed(ours.pin, ours.directory), "the pin is closed").toBe(true);
    expect(result).toMatchObject({ released: false });
  });

  it.each([
    { step: "owner-unlink", label: "EPERM on our pinned owner unlink" },
    { step: "proof-read", label: "EIO on our pinned owner proof read" },
    { step: "rmdir", label: "EPERM on rmdir of our SAME emptied canonical directory" },
  ] as const)("counter: $label: unconfirmed, keeping that native cause", ({ step }) => {
    const root = privateBase(), lock = path.join(root, ".lock");
    const ours = pinnedLock(lock);
    const injected = step === "proof-read" ? fault("EIO", "read", -5) : fault("EPERM", step === "rmdir" ? "rmdir" : "unlink", -1);
    let hits = 0;
    if (step === "owner-unlink") {
      const unlink = fs.unlinkSync;
      vi.spyOn(fs, "unlinkSync").mockImplementation((...args: Parameters<typeof fs.unlinkSync>) => {
        if (String(args[0]) === ours.pinnedOwner) { hits += 1; throw injected; }
        return unlink(...args);
      });
    } else if (step === "proof-read") {
      const read = fs.readFileSync;
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
        if (String(file) === ours.pinnedOwner) { hits += 1; throw injected; }
        return (read as (...args: unknown[]) => unknown)(file, options);
      }) as typeof fs.readFileSync);
    } else {
      const rmdir = fs.rmdirSync;
      vi.spyOn(fs, "rmdirSync").mockImplementation((...args: Parameters<typeof fs.rmdirSync>) => {
        if (path.resolve(String(args[0])) === lock && sameDirectory(fs.lstatSync(lock, { throwIfNoEntry: false }), ours.directory) &&
          fs.readdirSync(lock).length === 0) { hits += 1; throw injected; }
        return rmdir(...args);
      });
    }
    const result = releaseFenceLock(lock, ours.owner, {}, ours.pin);
    vi.restoreAllMocks();
    expect(hits, "the fault met that exact step").toBeGreaterThanOrEqual(1);
    expect(sameDirectory(fs.lstatSync(lock), ours.directory), "our directory still holds the name").toBe(true);
    if (step === "rmdir") expect(fs.readdirSync(lock), "ownerless after our real unlink, and still ours").toEqual([]);
    else {
      expect(fs.readdirSync(lock)).toEqual(["owner"]);
      expect(ownerOf(lock), "our exact record remains").toBe(ours.owner);
    }
    expect(pinClosed(ours.pin, ours.directory), "the pin is closed").toBe(true);
    expect(result).toMatchObject({ released: false });
    expect((result as { cause?: unknown }).cause, "the native cause itself").toBe(injected);
  });
});

const B = 1_000;
const tickets = (root: string): string[] => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)); }
  catch (error) { if (codeOf(error) === "ENOENT") return []; throw error; }
};
/** A real MeshLock on a root nested in a private base, so the FIFO queue directory beside the root is private too. */
const offLinuxFixture = () => {
  const base = privateBase();
  const root = path.join(base, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  const lockPath = path.join(root, ".lock");
  const lock = new MeshLock(root, { lockTimeoutMs: B }, () => undefined);
  const refusal = new Error("certified pre-effect refusal (test)");
  const held: string[] = [];
  const rmTargets: string[] = [];
  const operation = (): string => {
    held.push(ownerOf(lockPath));
    if (held.length === 1) throw refusal;
    return "published";
  };
  /** The held-ingress shape: one retained lifetime around the designated initial acquisition, under the darwin model. */
  const run = async (): Promise<{ value?: string; error?: unknown }> => {
    Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
    try {
      return { value: await withInitialMeshLockAdmission(root,
        () => withInitialMeshLockAcquisition(root, () => lock.withLock(operation)),
        { isPreAppendRefusal: error => error === refusal }) };
    } catch (error) { return { error }; }
    finally {
      // The faults and the platform model end with the operation: no assertion or teardown meets them.
      vi.restoreAllMocks();
      Object.defineProperty(process, "platform", platform);
    }
  };
  const lockEntries = (): string[] => fs.readdirSync(root).filter(name => name.startsWith(".lock")).sort();
  return { root, lockPath, refusal, held, rmTargets, run, lockEntries };
};

// Off Linux there is no /proc/self/fd pin, so the canonical release is the rename-away fallback; the darwin model
// forces it on any POSIX host. Windows hosts are skipped: their directory-rename semantics are not this model.
describe.skipIf(process.platform === "win32")("off-Linux MeshLock release (darwin model): the rename-away is the canonical release", () => {
  it("only the detached copy's cleanup fails after a successful rename: the retained helper retries once and publishes", async () => {
    const f = offLinuxFixture();
    const rm = fs.rmSync;
    const injected = fault("EPERM", "rmdir", -1);
    let detached: string | undefined;
    let canonicalFreeAtFault: boolean | undefined;
    vi.spyOn(fs, "rmSync").mockImplementation((...args: Parameters<typeof fs.rmSync>) => {
      const target = path.resolve(String(args[0]));
      f.rmTargets.push(target);
      // Persistent, and only on the FIRST release's detached copy: receipts, the retry's release and teardown pass.
      if (target.startsWith(`${f.lockPath}.released.`) && (detached ??= target) === target) {
        canonicalFreeAtFault ??= fs.lstatSync(f.lockPath, { throwIfNoEntry: false }) === undefined;
        throw injected;
      }
      return rm(...args);
    });
    const outcome = await f.run();
    expect(detached, "the fallback renamed our canonical directory to its detached copy").toBeDefined();
    expect(canonicalFreeAtFault, "the canonical name was already free when the cleanup failed").toBe(true);
    expect(fs.readdirSync(detached!)).toEqual(["owner"]);
    expect(ownerOf(detached!), "the detached copy keeps our first record").toBe(f.held[0]);
    expect(f.rmTargets.filter(target => target === f.lockPath), "never a recursive removal through the canonical name").toEqual([]);
    expect(fs.readdirSync(f.root).filter(name => name.startsWith(".retention-graveyard-")), "the off-Linux fallback ran").toEqual([]);
    expect(tickets(f.root), "the retained ticket is closed").toEqual([]);
    expect(shape(outcome.error), "a completed canonical release leaves the certified refusal retryable").toBeUndefined();
    expect(outcome.value).toBe("published");
    expect(f.held, "exactly one retained retry").toHaveLength(2);
    expect(f.held[1]).not.toBe(f.held[0]);
    expect(f.held.map(owner => owner.split("\n")[1])).toEqual([String(process.pid), String(process.pid)]);
    expect(f.lockEntries(), "the retry's own release completed; only the first detached copy remains").toEqual([path.basename(detached!)]);
  }, 10_000);

  it("counter: the rename itself fails and OUR record stays at the canonical name: nonretryable, one operation, same refusal and cause", async () => {
    const f = offLinuxFixture();
    const rename = fs.renameSync, rm = fs.rmSync;
    const injected = fault("EBUSY", "rename", -16);
    let hits = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((...args: Parameters<typeof fs.renameSync>) => {
      if (path.resolve(String(args[0])) === f.lockPath && path.resolve(String(args[1])).startsWith(`${f.lockPath}.released.`)) {
        hits += 1;
        throw injected;
      }
      return rename(...args);
    });
    vi.spyOn(fs, "rmSync").mockImplementation((...args: Parameters<typeof fs.rmSync>) => {
      f.rmTargets.push(path.resolve(String(args[0])));
      return rm(...args);
    });
    const outcome = await f.run();
    expect(hits, "the fallback's rename met the fault").toBeGreaterThanOrEqual(1);
    expect(f.held, "one operation: no retained retry").toHaveLength(1);
    expect(fs.readdirSync(f.lockPath)).toEqual(["owner"]);
    expect(ownerOf(f.lockPath), "our exact record still holds the canonical name").toBe(f.held[0]);
    expect(f.lockEntries()).toEqual([".lock"]);
    expect(f.rmTargets.filter(target => target === f.lockPath || target.startsWith(`${f.lockPath}.`)),
      "no recursive removal at or beside the canonical name").toEqual([]);
    expect(tickets(f.root), "the ticket is closed").toEqual([]);
    const rejection = outcome.error as { code?: unknown; retryable?: unknown; fenceRefusal?: unknown; cause?: unknown } | undefined;
    expect([rejection?.code, rejection?.retryable], "a nonretryable release-unconfirmed rejection")
      .toEqual(["FABRIC_MESH_LOCK_RELEASE_UNCONFIRMED", false]);
    expect(rejection?.fenceRefusal, "the same certified refusal object").toBe(f.refusal);
    expect(rejection?.cause, "the same native rename error object").toBe(injected);
  }, 10_000);
});
