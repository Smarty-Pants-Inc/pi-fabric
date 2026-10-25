import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { parseNatsLinkConfig, natsLinkAllowListConfig, NATS_LINK_CONFIG_MAX_BYTES, type NatsLinkConfig } from "../mesh/bridge-nats-config.js";
import { createBridgeNatsIdentityGate, type BridgeNatsSeatPins } from "../mesh/bridge-nats-identity.js";
import { procStartTime } from "../main-drain/binding.js";
import { SeamGap, sha256 } from "../seat-drain/run.js";

/**
 * The D3 bridge seam's A' config writer (smarty-dev#15310): it changes exactly ONE allow-list entry of the nats-link
 * config, the moving seat's root, and nothing else. It is never a free-form edit:
 * - the file is the one pinned into the installed seam (never a request or packet value);
 * - the path has no symlink in any component, and every ancestor is root's or ours and not group/other-writable;
 *   every read and write goes through the verified directory's descriptor;
 * - the entry has one fixed shape, built only from sealed values (the run's session id, the org.json principal bound
 *   to it, the run's pinned NATS machine) and the reviewed send/receive constants below;
 * - the candidate must pass the link's own full validation, including the org.json seat check `start` makes;
 * - a deep diff of old and candidate must be exactly that change;
 * - every seam write holds ONE config-scoped lock across read, plan, check, rename and read-back, and the swap is
 *   compare-and-swap on the old bytes' sha256, a private temp file, fsync, rename, directory fsync and a byte read-back;
 * - a rollback puts back the recorded old bytes exactly while the file still holds publish's bytes.
 */

/** What a moved seat may send and receive across (a principal seat's class takes followUp and cancel at most). */
export const SEAT_SEND = Object.freeze(["followUp"] as const);
export const SEAT_RECEIVE = Object.freeze(["followUp", "cancel"] as const);
const LOCK_WAIT_MS = 30_000;

export interface SeatEntry {
  kind: "root"; id: string; principal: string; machine: string; send: string[]; receive: string[];
}

export interface LinkConfigFile {
  file: string;
  /** The verified directory's identity: every later write goes through a descriptor of this same directory. */
  dirDev: number;
  dirIno: number;
  bytes: Buffer;
  sha256: string;
  value: Record<string, unknown>;
  config: NatsLinkConfig;
  /** Tests only: the one state directory counted as durable (see ReadOptions). Carried to the restore parse and read-back. */
  durableStateDir?: string;
}

export interface ReadOptions {
  /**
   * Tests only: this EXACT state directory counts as durable. The real-broker test's link must keep its state in the
   * private TMPDIR (the test deletion guard refuses renames anywhere else). Production passes none: the installed
   * bundle's entry calls the seam with no deps.
   */
  durableStateDir?: string;
}

/** What publish wrote, recorded in its intent: the only change a rollback may undo. */
export interface RollbackAuthority {
  beforeSha256: string;
  afterSha256: string;
  beforeBytes: Buffer;
  natsEntry: SeatEntry;
  fileEntry: { index: number; entry: Record<string, unknown> } | null;
  beforeHadNats: boolean;
}

export interface WriterPlan {
  /**
   * add: no entry before; move: the seat's file entry becomes the nats entry; revert: the recorded old bytes, exactly;
   * remove / restore: an entry-level undo when other entries changed in between; none: no change.
   */
  change: "add" | "move" | "revert" | "remove" | "restore" | "none";
  before: LinkConfigFile;
  /** The candidate's bytes (null for "none"). */
  after: Buffer | null;
  afterSha256: string | null;
  /** The nats entry written (publish) or removed (rollback); null for "none". */
  natsEntry: SeatEntry | null;
  /** The seat's file entry, verbatim, removed (move) or put back, and its index in allow.file. */
  fileEntry: { index: number; entry: Record<string, unknown> } | null;
  /** Whether the old config had an allow.nats list at all (a rollback of an add leaves it absent again). */
  beforeHadNats: boolean;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Canonical deep equality for JSON values (key order ignored). */
export const sameJson = (left: unknown, right: unknown): boolean => {
  if (Array.isArray(left) || Array.isArray(right)) {
    return Array.isArray(left) && Array.isArray(right) && left.length === right.length && left.every((item, index) => sameJson(item, right[index]));
  }
  if (isObject(left) || isObject(right)) {
    if (!isObject(left) || !isObject(right)) return false;
    const keys = Object.keys(left);
    return keys.length === Object.keys(right).length && keys.every(key => Object.hasOwn(right, key) && sameJson(left[key], right[key]));
  }
  return left === right;
};

// ---------------------------------------------------------------- the path and its directory

const uid = (): number => process.geteuid?.() ?? -1;

/**
 * The config path: absolute, normalized, NO symlink in any component (its real path is itself), and every directory
 * from `/` down is root's or ours and not group/other-writable (a sticky shared directory only above the config's
 * own directory). The config file itself: regular, ours or root's, one link, not group/other-writable, at most 1 MiB.
 */
const verifyPath = (file: string): { directory: string; name: string } => {
  if (!path.isAbsolute(file) || path.normalize(file) !== file || file.endsWith(path.sep)) throw new SeamGap("link-config-path-unsafe");
  let real: string;
  try { real = fs.realpathSync(file); } catch { throw new SeamGap("link-config-unreadable"); }
  if (real !== file) throw new SeamGap("link-config-path-unsafe");
  const directory = path.dirname(file);
  try {
    for (let current = directory; ; current = path.dirname(current)) {
      const stat = fs.lstatSync(current);
      const shared = (stat.mode & 0o022) !== 0;
      if (!stat.isDirectory() || (stat.uid !== 0 && stat.uid !== uid()) ||
        (shared && (current === directory || (stat.mode & 0o1000) === 0))) throw new SeamGap("link-config-path-unsafe");
      if (path.dirname(current) === current) break;
    }
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    throw new SeamGap("link-config-unreadable");
  }
  return { directory, name: path.basename(file) };
};

/** The verified directory, pinned by a descriptor whose identity must match the path's (and, later, the plan's). */
const openDirectory = (directory: string, expect?: { dev: number; ino: number }): { fd: number; dev: number; ino: number } => {
  let fd: number;
  try { fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW); }
  catch { throw new SeamGap("link-config-path-unsafe"); }
  const held = fs.fstatSync(fd);
  const named = fs.lstatSync(directory);
  if (held.dev !== named.dev || held.ino !== named.ino || (expect && (held.dev !== expect.dev || held.ino !== expect.ino))) {
    fs.closeSync(fd);
    throw new SeamGap("link-config-path-unsafe");
  }
  return { fd, dev: held.dev, ino: held.ino };
};
const inDirectory = (fd: number, name: string): string => `/proc/self/fd/${fd}/${name}`;

/** The config's bytes, through the directory descriptor, no-follow, with the file checks above. */
const readThrough = (fd: number, name: string): Buffer => {
  let file: number | undefined;
  try {
    file = fs.openSync(inDirectory(fd, name), fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(file);
    if (!stat.isFile() || (stat.uid !== 0 && stat.uid !== uid()) || (stat.mode & 0o022) !== 0 || stat.nlink !== 1 ||
      stat.size > NATS_LINK_CONFIG_MAX_BYTES) throw new SeamGap("link-config-unsafe");
    return fs.readFileSync(file);
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    throw new SeamGap("link-config-unreadable");
  } finally { if (file !== undefined) fs.closeSync(file); }
};

/**
 * The link's state (its cursor, ledger and lock) must live on a durable path: temp-store reaping must never be able to
 * remove it under a running link. A path in a temporary root, or in any agent session's private temp store, refuses.
 */
const TEMPORARY_ROOTS = ["/tmp", "/var/tmp", "/dev/shm", "/run"];
const exemption = (options: ReadOptions): ReadOptions =>
  options.durableStateDir === undefined ? {} : { durableStateDir: options.durableStateDir };
export const temporaryStateDir = (stateDir: string): boolean => {
  const candidates = [stateDir];
  try { candidates.push(fs.realpathSync(stateDir)); } catch { /* not created yet: the path as written */ }
  return candidates.some(dir => dir.split(path.sep).includes("smarty-pi-tmp") ||
    TEMPORARY_ROOTS.some(root => dir === root || dir.startsWith(`${root}${path.sep}`)));
};

const parseConfig = (bytes: Buffer, options: ReadOptions = {}): { value: Record<string, unknown>; config: NatsLinkConfig } => {
  let value: unknown;
  try { value = JSON.parse(bytes.toString("utf8")); } catch { throw new SeamGap("link-config-invalid"); }
  if (!isObject(value)) throw new SeamGap("link-config-invalid");
  let config: NatsLinkConfig;
  try { config = parseNatsLinkConfig(value); } catch { throw new SeamGap("link-config-invalid"); }
  if (config.stateDir !== options.durableStateDir && temporaryStateDir(config.stateDir)) throw new SeamGap("link-state-dir-temporary");
  return { value, config };
};

/**
 * Read the pinned config: the path checks above, then the bytes through the verified directory's descriptor, parsed
 * by the link's own parser. The bytes read here are the ones the compare-and-swap protects.
 */
export const readLinkConfig = (file: string, options: ReadOptions = {}): LinkConfigFile => {
  const { directory, name } = verifyPath(file);
  const dir = openDirectory(directory);
  try {
    const bytes = readThrough(dir.fd, name);
    return { file, dirDev: dir.dev, dirIno: dir.ino, bytes, sha256: sha256(bytes), ...parseConfig(bytes, options), ...exemption(options) };
  } finally { fs.closeSync(dir.fd); }
};

// ---------------------------------------------------------------- the config-scoped lock

interface LockRecord { pid: number; startTime: string | null; at: string }
const lockName = (name: string): string => `.${name}.fabric-v2.lock`;

const readLock = (fd: number, name: string): (LockRecord & { ino: number }) | undefined => {
  try {
    const file = inDirectory(fd, lockName(name));
    const stat = fs.lstatSync(file);
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<LockRecord>;
    if (!stat.isFile() || !Number.isSafeInteger(value.pid)) return { pid: -1, startTime: null, at: "", ino: stat.ino };
    return { pid: value.pid!, startTime: typeof value.startTime === "string" ? value.startTime : null, at: String(value.at), ino: stat.ino };
  } catch { return undefined; }
};

/**
 * Hold the config-scoped lock (an O_EXCL file beside the config, through the verified directory) for `work`: every
 * bridge seam write, on any seat, takes it across read, plan, check, rename and read-back. A lock whose holder process
 * is gone (pid and start time) is removed only while it is the same file; a live holder is waited for, bounded.
 */
export const withConfigLock = async <T>(file: string, work: () => Promise<T> | T,
  options: { waitMs?: number; sleep?: (ms: number) => Promise<void>; now?: () => number } = {}): Promise<T> => {
  const { directory, name } = verifyPath(file);
  const dir = openDirectory(directory);
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
  const now = options.now ?? Date.now;
  const until = now() + (options.waitMs ?? LOCK_WAIT_MS);
  const mine: LockRecord = { pid: process.pid, startTime: procStartTime(process.pid) ?? null, at: new Date().toISOString() };
  let held: number | undefined;
  try {
    for (;;) {
      try {
        const fd = fs.openSync(inDirectory(dir.fd, lockName(name)), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL |
          fs.constants.O_NOFOLLOW, 0o600);
        try { fs.writeSync(fd, JSON.stringify(mine)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
        held = fs.lstatSync(inDirectory(dir.fd, lockName(name))).ino;
        break;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw new SeamGap("link-config-lock-failed");
      }
      const holder = readLock(dir.fd, name);
      const alive = holder !== undefined && holder.pid > 0 && holder.startTime !== null && procStartTime(holder.pid) === holder.startTime;
      if (holder && !alive && holder.pid !== process.pid) {
        // A dead holder: remove THAT file only (same inode), then try again.
        try { if (fs.lstatSync(inDirectory(dir.fd, lockName(name))).ino === holder.ino) fs.unlinkSync(inDirectory(dir.fd, lockName(name))); }
        catch { /* gone already */ }
        continue;
      }
      if (now() >= until) throw new SeamGap("link-config-busy");
      await sleep(200);
    }
    return await work();
  } finally {
    if (held !== undefined) {
      try { if (fs.lstatSync(inDirectory(dir.fd, lockName(name))).ino === held) fs.unlinkSync(inDirectory(dir.fd, lockName(name))); }
      catch { /* already gone */ }
    }
    fs.closeSync(dir.fd);
  }
};

// ---------------------------------------------------------------- planning

/** The org.json principal whose org seat is exactly this session (the name the link's seat check accepts). */
export const seatPrincipal = (pins: BridgeNatsSeatPins, sessionId: string): string => {
  const id = `session:${sessionId}`;
  const names = [...pins.seats].filter(([, seat]) => seat === id).map(([name]) => name);
  if (names.length !== 1) throw new SeamGap("seat-principal-unbound");
  return names[0]!;
};

const list = (section: Record<string, unknown>, side: "file" | "nats"): Array<Record<string, unknown>> => {
  const value = section[side];
  if (value === undefined) return [];
  if (!Array.isArray(value) || !value.every(isObject)) throw new SeamGap("link-config-invalid");
  return value as Array<Record<string, unknown>>;
};

/** The candidate must be the link's valid config, accepted by the same org.json seat check `start` makes. */
const validated = (candidate: Record<string, unknown>, pins: BridgeNatsSeatPins): NatsLinkConfig => {
  let config: NatsLinkConfig;
  try { config = parseNatsLinkConfig(candidate); } catch { throw new SeamGap("link-config-candidate-invalid"); }
  try { createBridgeNatsIdentityGate(natsLinkAllowListConfig(config), { pins }); } catch { throw new SeamGap("link-config-candidate-refused-by-seat-pins"); }
  return config;
};

/**
 * Exactly one change: every key but `allow` deep-equal; allow.nats is the old list with one element appended (or one
 * removed); allow.file is the old list, or it with exactly the seat's element removed (or re-inserted). Nothing else.
 */
const assertOneChange = (before: Record<string, unknown>, after: Record<string, unknown>, id: string,
  natsDelta: "added" | "removed", fileDelta: "none" | "removed" | "inserted"): void => {
  const fail = (): never => { throw new SeamGap("link-config-diff-not-one-entry"); };
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of keys) if (key !== "allow" && !sameJson(before[key], after[key])) fail();
  const oldAllow = isObject(before.allow) ? before.allow : {};
  const newAllow = isObject(after.allow) ? after.allow : fail();
  for (const key of new Set([...Object.keys(oldAllow), ...Object.keys(newAllow)])) if (key !== "file" && key !== "nats") fail();
  const oldNats = list(oldAllow, "nats"), newNats = list(newAllow, "nats");
  const oldFile = list(oldAllow, "file"), newFile = list(newAllow, "file");
  const without = (entries: Array<Record<string, unknown>>) => entries.filter(entry => entry.id !== id);
  if (natsDelta === "added" ? newNats.length !== oldNats.length + 1 : newNats.length !== oldNats.length - 1) fail();
  if (!sameJson(without(oldNats), without(newNats)) || !sameJson(without(oldNats), natsDelta === "added" ? oldNats : newNats)) fail();
  if (fileDelta === "none" ? !sameJson(oldFile, newFile) : !sameJson(without(oldFile), without(newFile))) fail();
  if (fileDelta === "removed" && (newFile.length !== oldFile.length - 1 || newFile.some(entry => entry.id === id))) fail();
  if (fileDelta === "inserted" && (newFile.length !== oldFile.length + 1 || oldFile.some(entry => entry.id === id))) fail();
};

const serialize = (value: unknown): Buffer => Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
const none = (before: LinkConfigFile): WriterPlan =>
  ({ change: "none", before, after: null, afterSha256: null, natsEntry: null, fileEntry: null, beforeHadNats: hasNats(before.value) });
const hasNats = (value: Record<string, unknown>): boolean => isObject(value.allow) && value.allow.nats !== undefined;

/**
 * publish: the moving seat's nats entry. The pre-state decides: the same entry already there is no change, and a
 * no-change publish records NO rollback authority (a rollback then never removes that pre-existing entry); the seat's
 * own file entry is MOVED; no entry is ADDED. Any other entry naming this session refuses.
 */
export const planPublish = (before: LinkConfigFile, pins: BridgeNatsSeatPins, seat: { sessionId: string; machine: string }): WriterPlan => {
  const { config, value } = before;
  const id = `session:${seat.sessionId}`;
  if (!config.nats.pathBMachines.includes(seat.machine) || seat.machine === config.nats.machine) throw new SeamGap("seat-machine-not-path-b");
  const entry: SeatEntry = { kind: "root", id, principal: seatPrincipal(pins, seat.sessionId), machine: seat.machine,
    send: [...SEAT_SEND], receive: [...SEAT_RECEIVE] };
  const allow = isObject(value.allow) ? value.allow : {};
  const nats = list(allow, "nats").filter(item => item.id === id || item.rootId === id);
  const file = list(allow, "file").map((item, index) => ({ item, index })).filter(({ item }) => item.id === id || item.rootId === id);
  if (nats.length === 1 && file.length === 0 && sameJson(nats[0], entry)) return none(before);
  if (nats.length > 0) throw new SeamGap("seat-entry-conflict");
  // Only the seat's ROOT entry may move: an actor under this root on the file side refuses.
  if (file.length > 1 || (file.length === 1 && (file[0]!.item.kind !== "root" || file[0]!.item.id !== id))) throw new SeamGap("seat-entry-conflict");
  const candidate = structuredClone(value);
  const nextAllow = isObject(candidate.allow) ? candidate.allow : (candidate.allow = {}) as Record<string, unknown>;
  if (file.length === 1) nextAllow.file = list(nextAllow, "file").filter(item => item.id !== id);
  nextAllow.nats = [...list(nextAllow, "nats"), entry];
  validated(candidate, pins);
  assertOneChange(value, candidate, id, "added", file.length === 1 ? "removed" : "none");
  const after = serialize(candidate);
  return { change: file.length === 1 ? "move" : "add", before, after, afterSha256: sha256(after), natsEntry: entry,
    fileEntry: file.length === 1 ? { index: file[0]!.index, entry: structuredClone(file[0]!.item) } : null, beforeHadNats: hasNats(value) };
};

/** The rollback authority a publish plan records: null when it wrote nothing. */
export const rollbackAuthority = (plan: WriterPlan): RollbackAuthority | null =>
  plan.change === "none" || plan.after === null || plan.natsEntry === null ? null : {
    beforeSha256: plan.before.sha256, afterSha256: plan.afterSha256!, beforeBytes: plan.before.bytes, natsEntry: plan.natsEntry,
    fileEntry: plan.fileEntry, beforeHadNats: plan.beforeHadNats,
  };

/**
 * rc-restore-route: undo exactly what publish wrote.
 * - No authority (publish wrote nothing in this RUN): no change, and the seat must have no nats entry of ours to undo.
 * - The file still holds publish's bytes: compare-and-swap back to the recorded old bytes, exactly (revert).
 * - The file holds the old bytes: no change.
 * - Anything else (other seats changed it since): an entry-level undo of exactly publish's entry, re-validated; the
 *   seat's nats entry must be the one publish wrote (another value refuses).
 */
export const planRestore = (before: LinkConfigFile, pins: BridgeNatsSeatPins, sessionId: string, authority: RollbackAuthority | null): WriterPlan => {
  const { value } = before;
  const id = `session:${sessionId}`;
  const allow = isObject(value.allow) ? value.allow : {};
  const nats = list(allow, "nats").filter(item => item.id === id || item.rootId === id);
  const file = list(allow, "file").filter(item => item.id === id || item.rootId === id);
  if (authority === null) {
    if (nats.length > 0) throw new SeamGap("seat-entry-conflict");
    return none(before);
  }
  if (before.sha256 === authority.beforeSha256) return none(before);
  if (before.sha256 === authority.afterSha256) {
    const reverted = parseConfig(authority.beforeBytes, before);
    validated(reverted.value, pins);
    return { change: "revert", before, after: authority.beforeBytes, afterSha256: authority.beforeSha256, natsEntry: authority.natsEntry,
      fileEntry: authority.fileEntry, beforeHadNats: authority.beforeHadNats };
  }
  const restored = authority.fileEntry === null ? file.length === 0 : file.length === 1 && sameJson(file[0], authority.fileEntry.entry);
  if (nats.length === 0 && restored) return none(before);
  if (nats.length !== 1 || !sameJson(nats[0], authority.natsEntry)) throw new SeamGap("seat-entry-conflict");
  if (file.length > 0) throw new SeamGap("seat-entry-conflict");
  const candidate = structuredClone(value);
  const nextAllow = candidate.allow as Record<string, unknown>;
  const remaining = list(nextAllow, "nats").filter(item => item.id !== id);
  // An add into a config with no allow.nats leaves it absent again.
  if (remaining.length === 0 && !authority.beforeHadNats) delete nextAllow.nats;
  else nextAllow.nats = remaining;
  if (authority.fileEntry) {
    const files = list(nextAllow, "file");
    files.splice(Math.min(authority.fileEntry.index, files.length), 0, structuredClone(authority.fileEntry.entry));
    nextAllow.file = files;
  }
  validated(candidate, pins);
  assertOneChange(value, candidate, id, "removed", authority.fileEntry ? "inserted" : "none");
  const after = serialize(candidate);
  return { change: authority.fileEntry ? "restore" : "remove", before, after, afterSha256: sha256(after),
    natsEntry: authority.natsEntry, fileEntry: authority.fileEntry, beforeHadNats: authority.beforeHadNats };
};

// ---------------------------------------------------------------- the swap

/**
 * Compare-and-swap the planned bytes in, under the config lock (the caller holds it): through a descriptor of the SAME
 * directory the plan read, the file must still hash to the plan's old bytes (before the temp write and again just
 * before the rename); the temp file is private and in that directory; both are fsynced; the result is read back byte
 * for byte. A failure before the rename leaves the old file untouched. Returns the post-state, re-read and re-validated.
 */
export const applyPlan = (plan: WriterPlan): LinkConfigFile => {
  if (plan.after === null) return readLinkConfig(plan.before.file, plan.before);
  const { directory, name } = verifyPath(plan.before.file);
  const dir = openDirectory(directory, { dev: plan.before.dirDev, ino: plan.before.dirIno });
  const temp = `.${name}.fabric-v2-${randomBytes(8).toString("hex")}.tmp`;
  let renamed = false;
  try {
    if (sha256(readThrough(dir.fd, name)) !== plan.before.sha256) throw new SeamGap("link-config-changed");
    const mode = fs.statSync(inDirectory(dir.fd, name)).mode & 0o600;
    let fd: number | undefined;
    try {
      fd = fs.openSync(inDirectory(dir.fd, temp), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, mode);
      let offset = 0;
      while (offset < plan.after.length) offset += fs.writeSync(fd, plan.after, offset, plan.after.length - offset);
      fs.fsyncSync(fd);
    } finally { if (fd !== undefined) fs.closeSync(fd); }
    if (sha256(readThrough(dir.fd, name)) !== plan.before.sha256) throw new SeamGap("link-config-changed");
    fs.renameSync(inDirectory(dir.fd, temp), inDirectory(dir.fd, name));
    renamed = true;
    fs.fsyncSync(dir.fd);
    const back = readThrough(dir.fd, name);
    if (sha256(back) !== plan.afterSha256) throw new SeamGap("link-config-readback-failed");
    return { file: plan.before.file, dirDev: dir.dev, dirIno: dir.ino, bytes: back, sha256: sha256(back), ...parseConfig(back, plan.before),
      ...exemption(plan.before) };
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    throw new SeamGap(renamed ? "link-config-sync-failed" : "link-config-write-failed");
  } finally {
    if (!renamed) { try { fs.unlinkSync(inDirectory(dir.fd, temp)); } catch { /* never created, or already gone */ } }
    fs.closeSync(dir.fd);
  }
};
