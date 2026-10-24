import fs from "node:fs";
import path from "node:path";
import { isNamespaceRace, namespaceBarrierAvailable, namespaceIdentitySnapshot, namespaceKey, syncDirectoryChain, syncPathNamespace } from "../core/atomic-write.js";

/**
 * A checked preparation of the `event-receipts` namespace, made BEFORE `.lock` (fv2-coldpub C).
 * The complete reopenable namespace (every ancestor and symlink parent through the root) was
 * fsynced while `key` named it, so its entries are durable while the key still matches. It is
 * NOT a file or leaf receipt: every installed or unlinked receipt file still owes a fresh file
 * fsync and a fresh leaf-directory fsync. A mismatched key (recreated or retargeted directory,
 * replaced ancestor, changed symlink) uses the complete existing barrier instead.
 * Same-boot invariant: cooperating mesh writers never remove or relocate `event-receipts`,
 * the mesh root, or its ancestors; they only create, replace or unlink receipt files in it.
 */
export interface ReceiptNamespace { directory: string; key: string }

/** Idempotent. Fails closed (throws) on an I/O failure, so no append follows a failed preparation.
 * A namespace race or an unsupported platform yields no preparation: the full barrier applies. */
export const prepareReceiptNamespace = (directory: string): ReceiptNamespace | undefined => {
  if (!namespaceBarrierAvailable()) return undefined;
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  try {
    const key = namespaceKey(directory);
    syncPathNamespace(directory);
    return namespaceKey(directory) === key ? { directory, key } : undefined;
  } catch (error) { if (isNamespaceRace(error)) return undefined; throw error; }
};

const unchanged = (prepared: ReceiptNamespace | undefined, directory: string): prepared is ReceiptNamespace => {
  if (!prepared || prepared.directory !== directory) return false;
  // Any lookup failure denies the fast path; the complete barrier then reports it.
  try { return namespaceKey(directory) === prepared.key; } catch { return false; }
};

/** Fresh fsync of the leaf directory itself, then a recheck that the prepared namespace still
 * names it. False means the caller must run the complete barrier. */
const syncLeaf = (prepared: ReceiptNamespace): boolean => {
  const fd = fs.openSync(prepared.directory, fs.constants.O_RDONLY);
  try {
    const opened = fs.fstatSync(fd), named = fs.statSync(prepared.directory);
    if (!opened.isDirectory() || opened.dev !== named.dev || opened.ino !== named.ino) return false;
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  return unchanged(prepared, prepared.directory);
};

/**
 * Directory barrier after a receipt-file rename or unlink in `directory`: a fresh leaf fsync when
 * the prepared namespace still matches before and after it, else the complete existing barrier
 * (whose I/O failures and namespace races propagate unchanged). With no preparation this is
 * exactly syncPathNamespace, the barrier these callers used before.
 */
export const receiptDirectoryBarrier = (prepared: ReceiptNamespace | undefined) => (directory: string): void => {
  if (!unchanged(prepared, directory) || !syncLeaf(prepared)) syncPathNamespace(directory);
};

type FileIdentity = Pick<fs.Stats, "dev" | "ino"> | Pick<fs.BigIntStats, "dev" | "ino">;
export type LiveNamespaceBarrier = (file: string, fd: number, receipt: FileIdentity) => void;
export type IndexDirectoryBarrier = (directory: string, archiveDir: string, meshRoot: string, sequence: number) => void;
export interface KeyedPublishPrefix {
  forArchive: (archiveDir: string, sequence: number) => {
    syncIndexDirectory: IndexDirectoryBarrier;
    syncLiveNamespace: LiveNamespaceBarrier;
  } | undefined;
}
type IdentitySnapshot = ReturnType<typeof namespaceIdentitySnapshot>;
type Mount = { id: string; dev: number };
const canonical = (directory: string): boolean => path.isAbsolute(directory) && path.resolve(directory) === directory;
const below = (directory: string, ancestor: string): boolean => directory === ancestor || directory.startsWith(ancestor + path.sep);
const sameDirectory = (left: IdentitySnapshot["directories"][number], right: IdentitySnapshot["directories"][number]): boolean =>
  left.directory === right.directory && left.dev === right.dev && left.ino === right.ino && left.mode === right.mode;

// Only a missing/unsupported capability denies eligibility. In particular EIO is never absence.
const capabilityAbsent = (error: unknown): boolean => ["ENOENT", "ENOTDIR", "ENOSYS", "ENOTSUP", "EOPNOTSUPP", "EACCES"].includes(
  String((error as NodeJS.ErrnoException)?.code));
const openedMount = (fd: number, dev: number): Mount | undefined => {
  try {
    // These paths inspect ONLY this invocation's own open descriptor. mnt_id is not a generation.
    const ids = [...fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8").matchAll(/^mnt_id:\s*(\d+)\s*$/gm)];
    const kind = fs.statfsSync(`/proc/self/fd/${fd}`, { bigint: true }).type;
    if (ids.length !== 1 || (kind !== 0xef53n && kind !== 0x58465342n)) return undefined; // ext / XFS only
    return { id: ids[0]![1]!, dev };
  } catch (error) { if (capabilityAbsent(error)) return undefined; throw error; }
};
const inspect = (target: string, receipt?: FileIdentity): IdentitySnapshot | undefined => {
  try {
    const snapshot = namespaceIdentitySnapshot(target, receipt);
    if (snapshot.hasSymlinks || new Set(snapshot.directories.map(d => `${d.dev}:${d.ino}`)).size !== snapshot.directories.length) return undefined;
    return snapshot;
  } catch (error) {
    if (isNamespaceRace(error) || ["ENOENT", "ENOTDIR"].includes(String((error as NodeJS.ErrnoException)?.code))) return undefined;
    throw error;
  }
};

/** The factory itself performs the real off-lock preparation. Structural {directory,key} values,
 * snapshots, archive instances and process lifetime cannot mint this per-publish authority.
 * Only links ABOVE C are borrowed; C's contents and the whole divergent suffix are freshly paid.
 * Hidden same-inode shared-prefix ABA retains ReceiptNamespace's existing contract limitation. */
export const prepareKeyedPublishNamespace = (meshRoot: string): {
  receiptNamespace: ReceiptNamespace | undefined; prefixScope?: KeyedPublishPrefix;
} => {
  const directory = path.join(meshRoot, "event-receipts");
  const receiptNamespace = prepareReceiptNamespace(directory);
  const result = { receiptNamespace };
  if (!receiptNamespace || process.platform !== "linux" || !canonical(meshRoot)) return result;
  const prepared = inspect(directory);
  if (!prepared || prepared.key !== receiptNamespace.key) return result;
  const meshIndex = prepared.directories.findIndex(d => d.directory === meshRoot);
  if (meshIndex < 0) return result;

  // Open/fstat/close failures always propagate, including when no fsync has yet been attempted.
  // Qualify only C and the relevant suffix/mesh branch: borrowed ancestors above C may be on
  // another mount (e.g. a mounted scratch tree); their actual preparation already paid them.
  const visit = (snapshot: IdentitySnapshot, first: number, mount?: Mount, sync = false): Mount | undefined => {
    let expectedMount = mount;
    for (const expected of snapshot.directories.slice(first).reverse()) {
      const fd = fs.openSync(expected.directory, fs.constants.O_RDONLY);
      try {
        const opened = fs.fstatSync(fd);
        if (!opened.isDirectory() || opened.dev !== expected.dev || opened.ino !== expected.ino) return undefined;
        const actualMount = openedMount(fd, opened.dev);
        if (!actualMount || (expectedMount && (actualMount.id !== expectedMount.id || actualMount.dev !== expectedMount.dev))) return undefined;
        expectedMount ??= actualMount;
        if (sync) fs.fsyncSync(fd);
      } finally { fs.closeSync(fd); }
    }
    return expectedMount;
  };
  const checkPrepared = (): IdentitySnapshot | undefined => {
    const current = inspect(directory);
    return current?.key === prepared.key ? current : undefined;
  };
  const pay = (target: string, live?: { fd: number; receipt: FileIdentity }): boolean => {
    const currentPrepared = checkPrepared();
    const before = inspect(target, live?.receipt);
    if (!currentPrepared || !before) return false;
    let common = -1;
    for (let i = 0; i <= meshIndex && i < before.directories.length; i++) {
      if (!sameDirectory(prepared.directories[i]!, before.directories[i]!)) break;
      common = i;
    }
    if (common <= 0 || (live && before.directories[common]?.directory !== meshRoot)) return false;
    const liveMatches = (mount?: Mount): boolean => {
      if (!live) return true;
      const stat = typeof live.receipt.ino === "bigint" ? fs.fstatSync(live.fd, { bigint: true }) : fs.fstatSync(live.fd);
      if (!stat.isFile() || stat.dev !== live.receipt.dev || stat.ino !== live.receipt.ino) return false;
      if (!mount) return true;
      const actual = openedMount(live.fd, mount.dev);
      return !!actual && actual.id === mount.id && stat.dev === (typeof stat.dev === "bigint" ? BigInt(mount.dev) : mount.dev);
    };
    if (!liveMatches()) return false;
    const mount = visit(currentPrepared, common);
    if (!mount || !liveMatches(mount) || !visit(before, common, mount, true)) return false;
    const after = inspect(target, live?.receipt), afterPrepared = checkPrepared();
    if (!after || !afterPrepared || after.key !== before.key || !visit(after, common, mount) ||
        !visit(afterPrepared, common, mount) || !liveMatches(mount)) return false;
    // Topology inspection also opens names: finish with the same resolver's endpoint/key check,
    // so a successful-I/O replacement during that inspection cannot authorize an omission.
    return inspect(target, live?.receipt)?.key === before.key && !!checkPrepared();
  };
  return { receiptNamespace, prefixScope: {
    forArchive: (archiveDir, sequence) => {
      if (!canonical(archiveDir) || below(archiveDir, directory) || !Number.isSafeInteger(sequence) || sequence < 1) return undefined;
      const expected = path.join(archiveDir, "sequence-index", String(Math.floor(sequence / 1024)));
      const liveFile = path.join(meshRoot, "events.jsonl");
      return {
        syncIndexDirectory: (target, actualArchive, actualRoot, actualSequence) => {
          if (target !== expected || actualArchive !== archiveDir || actualRoot !== meshRoot || actualSequence !== sequence || !pay(target)) syncDirectoryChain(target);
        },
        syncLiveNamespace: (file, fd, receipt) => {
          if (file !== liveFile || !pay(file, { fd, receipt })) syncPathNamespace(file, receipt);
        },
      };
    },
  } };
};
