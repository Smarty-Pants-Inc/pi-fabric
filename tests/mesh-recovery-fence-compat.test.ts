import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";

// Keep: data safety (#9569 C/E): a paused OLD by-name recoverer must encounter
// the SAME permanently occupied F, not detach a new canonical owner into another name.
// Real private-TMPDIR filesystem and real rename/rmdir/release; ONLY directory bigint
// lstat IDs and ESRCH evidence are recorded. No native-delete or success mocks.
// This characterizes retained-name compatibility, NOT stronger macOS release parity.
// Source-based fail-before: db92b754 interpolates exact bigint dev/ino in non-Windows
// F. 9007199254740993n becomes "9007199254740993", whereas legacy fs.Stats Number
// interpolation is "9007199254740992". Old F stays absent, so the attempted real
// old rename below succeeds against B instead of failing on the occupied A receipt.
const DEAD = 987654321;
const RECORD = `01234567-89ab-cdef-0123-456789abcdef\n${DEAD}\n1\n`;
const HIGH = 9007199254740993n;
const scratch = fs.realpathSync(os.tmpdir());
type Domain = "mesh" | "custody";
type Ids = { dev: bigint; ino: bigint };
type RecordedIdentity = { physical: fs.BigIntStats; ids: Ids };
const legacyFence = (lock: string, stat: { dev: number; ino: number }, owner: string): string =>
  `${lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`;
const numberStats = (ids: Ids): { dev: number; ino: number } => ({ dev: Number(ids.dev), ino: Number(ids.ino) });

const fixture = (domain: Domain) => {
  // Vitest's fleet-isolation setup installs the deletion guard before this file.
  // Fail rather than using a shared temp root; leave fixtures for the run's owner to collect.
  const temp = fs.lstatSync(scratch);
  if (!temp.isDirectory() || temp.isSymbolicLink() || (temp.mode & 0o777) !== 0o700 ||
      temp.uid !== process.getuid?.() || ["/tmp", "/var/tmp", "/private/tmp", "/dev/shm", path.parse(scratch).root].includes(scratch)) {
    throw new Error("fence compatibility fixture requires a private physical TMPDIR");
  }
  const root = fs.mkdtempSync(path.join(scratch, "fabric-9569-fence-compat-"));
  const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
  fs.mkdirSync(lock, { mode: 0o700 });
  return { root, lock, owner: path.join(lock, "owner") };
};

const recordDirectoryIds = (lock: string, identities: RecordedIdentity[]): void => {
  const lstat = fs.lstatSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean }) => {
    const stat = lstat(file, options as never);
    if (file === lock && options?.bigint) {
      const exact = stat as unknown as fs.BigIntStats;
      const identity = identities.find(entry => entry.physical.dev === exact.dev && entry.physical.ino === exact.ino);
      if (identity) return Object.assign(Object.create(exact), identity.ids) as fs.BigIntStats;
    }
    return stat;
  }) as typeof fs.lstatSync);
};
const recordDeath = (afterProbe?: () => void): void => {
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (pid !== DEAD || signal !== 0) throw new Error("unexpected process probe; no signals allowed");
    afterProbe?.();
    throw Object.assign(new Error("recorded ESRCH"), { code: "ESRCH" });
  });
};
const acquire = async (domain: Domain, root: string, operation: () => void, timeoutMs = 2000): Promise<void> => {
  if (domain === "mesh") {
    const mesh = new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 2000, staleLockMs: 100 }, () => undefined);
    await mesh.withLock(operation, timeoutMs);
  } else {
    const release = await acquireMeshCustodyLock(root, timeoutMs, { ownIncarnation: undefined });
    try { operation(); } finally { release(); }
  }
};

// Actually execute the paused old reclaimer's already-computed by-name syscall.
// Do it BEFORE hash assertions: on the unfixed source this detaches B and fails here,
// rather than merely testing a string helper or mocked rename outcome.
const attemptOldRename = (lock: string, oldF: string): void => {
  const before = fs.lstatSync(lock);
  const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
  let failure: unknown;
  try { fs.renameSync(lock, oldF); } catch (error) { failure = error; }
  expect(failure).toBeDefined();
  expect(["ENOTEMPTY", "EEXIST"]).toContain((failure as NodeJS.ErrnoException).code);
  const after = fs.lstatSync(lock);
  expect([after.dev, after.ino]).toEqual([before.dev, before.ino]);
  expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
};

afterEach(() => { vi.restoreAllMocks(); });

for (const domain of ["mesh", "custody"] as const) describe.skipIf(process.platform === "win32")(`${domain}: legacy non-Windows F compatibility`, () => {
  it.each(["high ino", "high dev and ino", "safe integers"] as const)(
    "C retains the original F/full owner and rejects a paused old rename of B (%s)", async kind => {
      const f = fixture(domain);
      fs.writeFileSync(f.owner, RECORD, { flag: "wx", mode: 0o600 });
      const physical = fs.lstatSync(f.lock, { bigint: true });
      const ids: Ids = kind === "safe integers" ? { dev: 17n, ino: 23n }
        : { dev: kind === "high dev and ino" ? HIGH + 4n : physical.dev, ino: HIGH };
      // The old recoverer paused after judging A and computing this legacy Number F.
      const oldF = legacyFence(f.lock, numberStats(ids), RECORD);
      expect(fs.existsSync(oldF)).toBe(false);
      if (kind !== "safe integers") {
        const exactF = `${f.lock}.dead.${createHash("sha256").update(`${ids.dev}:${ids.ino}:${RECORD}`).digest("hex")}`;
        expect(exactF).not.toBe(oldF); // ensure the recorded case really exercises the precision boundary
      }
      recordDirectoryIds(f.lock, [{ physical, ids }]);
      recordDeath();
      let entered = false;
      await acquire(domain, f.root, () => {
        entered = true;
        expect(fs.readFileSync(f.owner, "utf8")).not.toBe(RECORD); // updated acquisition published B
        attemptOldRename(f.lock, oldF);
        expect(fs.readdirSync(f.root).filter(name => name.startsWith(`${path.basename(f.lock)}.dead.`)))
          .toEqual([path.basename(oldF)]); // updated F equals the old paused recoverer's F
        expect(fs.readdirSync(oldF)).toEqual(["owner"]);
        expect(fs.readFileSync(path.join(oldF, "owner"), "utf8")).toBe(RECORD);
      });
      expect(entered).toBe(true); // positive dead-owner recovery, not blanket refusal
      expect(fs.readdirSync(oldF)).toEqual(["owner"]); // release never empties/collects A's retained F
      expect(fs.readFileSync(path.join(oldF, "owner"), "utf8")).toBe(RECORD);
    });

  it("Number conversion is naming-only: distinct bigint IDs in the same Number bin still veto a replacement", async () => {
    const f = fixture(domain);
    fs.writeFileSync(f.owner, RECORD, { flag: "wx", mode: 0o600 });
    const physical = fs.lstatSync(f.lock, { bigint: true });
    const ids = { dev: physical.dev, ino: HIGH + 2n };
    const successorIds = { dev: physical.dev, ino: HIGH + 3n };
    expect(ids.ino).not.toBe(successorIds.ino);
    expect(Number(ids.ino)).toBe(Number(successorIds.ino));
    const identities: RecordedIdentity[] = [{ physical, ids }];
    recordDirectoryIds(f.lock, identities);
    const saved = path.join(f.root, "saved-A");
    let replaced = false;
    recordDeath(() => {
      if (replaced) return;
      replaced = true;
      fs.renameSync(f.lock, saved);
      fs.mkdirSync(f.lock, { mode: 0o700 });
      fs.writeFileSync(f.owner, RECORD, { flag: "wx", mode: 0o600 }); // same full R, different exact S
      identities.push({ physical: fs.lstatSync(f.lock, { bigint: true }), ids: successorIds });
    });
    const operation = vi.fn();
    await expect(acquire(domain, f.root, operation, 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(replaced).toBe(true);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(f.owner, "utf8")).toBe(RECORD);
    expect(fs.readFileSync(path.join(saved, "owner"), "utf8")).toBe(RECORD);
    expect(fs.existsSync(legacyFence(f.lock, numberStats(ids), RECORD))).toBe(false);
  });
});

describe.skipIf(process.platform === "win32")("mesh E: legacy non-Windows empty-owner F naming", () => {
  it.each([false, true])("preserves high-ID legacy F occupancy (existing historical receipt: %s)", async existing => {
    const f = fixture("mesh");
    const old = new Date(Date.now() - 60_000);
    fs.utimesSync(f.lock, old, old);
    const physical = fs.lstatSync(f.lock, { bigint: true });
    const ids = { dev: HIGH + 4n, ino: HIGH };
    const oldF = legacyFence(f.lock, numberStats(ids), "");
    if (existing) {
      fs.mkdirSync(oldF, { mode: 0o700 });
      fs.writeFileSync(path.join(oldF, ".recovery-fence"), "historical-receipt\n", { flag: "wx", mode: 0o600 });
    }
    recordDirectoryIds(f.lock, [{ physical, ids }]);
    await acquire("mesh", f.root, () => {
      attemptOldRename(f.lock, oldF);
      expect(fs.readdirSync(f.root).filter(name => name.startsWith(".lock.dead."))).toEqual([path.basename(oldF)]);
    });
    expect(fs.readdirSync(oldF)).toEqual([existing ? ".recovery-fence" : "f"]);
    expect(fs.readFileSync(path.join(oldF, existing ? ".recovery-fence" : "f"), "utf8"))
      .toBe(existing ? "historical-receipt\n" : "1\n");
  });
});
