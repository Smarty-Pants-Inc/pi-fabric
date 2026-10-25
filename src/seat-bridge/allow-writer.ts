import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { loadNatsLinkConfig, parseNatsLinkConfig, natsLinkAllowListConfig, NATS_LINK_CONFIG_MAX_BYTES, type NatsLinkConfig } from "../mesh/bridge-nats-config.js";
import { createBridgeNatsIdentityGate, type BridgeNatsSeatPins } from "../mesh/bridge-nats-identity.js";
import { SeamGap, sha256 } from "../seat-drain/run.js";

/**
 * The D3 bridge seam's A' config writer (smarty-dev#15310): it changes exactly ONE allow-list entry of the nats-link
 * config, the moving seat's root, and nothing else. It is never a free-form edit:
 * - the file is the one pinned into the installed seam (never a request or packet value);
 * - the entry has one fixed shape, built only from sealed values (the run's session id, the org.json principal bound
 *   to it, the run's Path B machine) and the reviewed send/receive constants below;
 * - the candidate must pass the link's own full validation, including the org.json seat check `start` makes;
 * - a deep diff of old and candidate must be exactly that change: one element added to (or removed from) allow.nats,
 *   and at most the seat's own allow.file element moved; every other value is deep-equal;
 * - the swap is compare-and-swap on the old bytes' sha256, a private temp file in the same directory, fsync, rename,
 *   directory fsync, and a byte-for-byte read-back.
 */

/** What a moved seat may send and receive across (a principal seat's class takes followUp and cancel at most). */
export const SEAT_SEND = Object.freeze(["followUp"] as const);
export const SEAT_RECEIVE = Object.freeze(["followUp", "cancel"] as const);

export interface SeatEntry {
  kind: "root"; id: string; principal: string; machine: string; send: string[]; receive: string[];
}

export interface LinkConfigFile {
  file: string;
  bytes: Buffer;
  sha256: string;
  value: Record<string, unknown>;
  config: NatsLinkConfig;
}

export interface WriterPlan {
  /** add: no entry before; move: the seat's file entry becomes the nats entry; remove / restore: the reverse; none: no change. */
  change: "add" | "move" | "remove" | "restore" | "none";
  before: LinkConfigFile;
  /** The candidate's bytes (null for "none"). */
  after: Buffer | null;
  afterSha256: string | null;
  /** The nats entry added or removed. */
  natsEntry: SeatEntry | null;
  /** The seat's file entry, verbatim, removed (move) or restored (restore), and its index in allow.file. */
  fileEntry: { index: number; entry: Record<string, unknown> } | null;
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

/**
 * Read the pinned config: the link's own safety checks (regular file, ours or root's, not group/other-writable, safe
 * directory, at most 1 MiB), then OUR bytes through a no-follow descriptor of the same inode, parsed by the link's
 * own parser. The bytes read here are the ones the compare-and-swap protects.
 */
export const readLinkConfig = (file: string): LinkConfigFile => {
  try { loadNatsLinkConfig(file); } catch { throw new SeamGap("link-config-invalid"); }
  let fd: number | undefined;
  let bytes: Buffer;
  try {
    const named = fs.lstatSync(file);
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const held = fs.fstatSync(fd);
    if (!held.isFile() || held.dev !== named.dev || held.ino !== named.ino || held.size > NATS_LINK_CONFIG_MAX_BYTES) throw new SeamGap("link-config-unsafe");
    bytes = fs.readFileSync(fd);
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    throw new SeamGap("link-config-unreadable");
  } finally { if (fd !== undefined) fs.closeSync(fd); }
  let value: unknown;
  try { value = JSON.parse(bytes.toString("utf8")); } catch { throw new SeamGap("link-config-invalid"); }
  if (!isObject(value)) throw new SeamGap("link-config-invalid");
  let config: NatsLinkConfig;
  try { config = parseNatsLinkConfig(value); } catch { throw new SeamGap("link-config-invalid"); }
  if (temporaryStateDir(config.stateDir)) throw new SeamGap("link-state-dir-temporary");
  return { file, bytes, sha256: sha256(bytes), value, config };
};

/**
 * The link's state (its cursor, ledger and lock) must live on a durable path: temp-store reaping must never be able to
 * remove it under a running link. A path in a temporary root, or in any agent session's private temp store, refuses.
 */
const TEMPORARY_ROOTS = ["/tmp", "/var/tmp", "/dev/shm", "/run"];
export const temporaryStateDir = (stateDir: string): boolean => {
  const candidates = [stateDir];
  try { candidates.push(fs.realpathSync(stateDir)); } catch { /* not created yet: the path as written */ }
  return candidates.some(dir => dir.split(path.sep).includes("smarty-pi-tmp") ||
    TEMPORARY_ROOTS.some(root => dir === root || dir.startsWith(`${root}${path.sep}`)));
};

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

/**
 * publish: the moving seat's nats entry. The pre-state decides: the same entry already there is no change; the
 * seat's own file entry is MOVED (removed from allow.file, the fixed nats entry appended); no entry is ADDED. Any
 * other entry naming this session refuses.
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
  if (nats.length === 1 && file.length === 0 && sameJson(nats[0], entry)) {
    return { change: "none", before, after: null, afterSha256: null, natsEntry: entry, fileEntry: null };
  }
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
    fileEntry: file.length === 1 ? { index: file[0]!.index, entry: structuredClone(file[0]!.item) } : null };
};

/**
 * rc-restore-route: undo exactly what publish wrote. The nats entry must be the one publish recorded (another
 * value refuses), and the file entry publish moved is put back verbatim at its index. Nothing to undo is no change.
 */
export const planRestore = (before: LinkConfigFile, pins: BridgeNatsSeatPins, published: {
  sessionId: string; natsEntry: SeatEntry | null; fileEntry: { index: number; entry: Record<string, unknown> } | null;
}): WriterPlan => {
  const { value } = before;
  const id = `session:${published.sessionId}`;
  const allow = isObject(value.allow) ? value.allow : {};
  const nats = list(allow, "nats").filter(item => item.id === id || item.rootId === id);
  const file = list(allow, "file").filter(item => item.id === id || item.rootId === id);
  // No publish write recorded in this RUN: there is nothing of ours to undo, and the seat must have no nats entry.
  if (published.natsEntry === null) {
    if (nats.length > 0) throw new SeamGap("seat-entry-conflict");
    return { change: "none", before, after: null, afterSha256: null, natsEntry: null, fileEntry: null };
  }
  const restored = published.fileEntry === null ? file.length === 0 : file.length === 1 && sameJson(file[0], published.fileEntry.entry);
  if (nats.length === 0 && restored) return { change: "none", before, after: null, afterSha256: null, natsEntry: null, fileEntry: null };
  if (nats.length !== 1 || !sameJson(nats[0], published.natsEntry)) throw new SeamGap("seat-entry-conflict");
  if (file.length > 0) throw new SeamGap("seat-entry-conflict");
  const candidate = structuredClone(value);
  const nextAllow = candidate.allow as Record<string, unknown>;
  nextAllow.nats = list(nextAllow, "nats").filter(item => item.id !== id);
  if (published.fileEntry) {
    const files = list(nextAllow, "file");
    files.splice(Math.min(published.fileEntry.index, files.length), 0, structuredClone(published.fileEntry.entry));
    nextAllow.file = files;
  }
  validated(candidate, pins);
  assertOneChange(value, candidate, id, "removed", published.fileEntry ? "inserted" : "none");
  const after = serialize(candidate);
  return { change: published.fileEntry ? "restore" : "remove", before, after, afterSha256: sha256(after),
    natsEntry: published.natsEntry, fileEntry: published.fileEntry };
};

const currentSha = (file: string): string => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    return sha256(fs.readFileSync(fd));
  } catch { throw new SeamGap("link-config-unreadable"); } finally { if (fd !== undefined) fs.closeSync(fd); }
};

/**
 * Compare-and-swap the planned bytes in: the file must still hash to the plan's old bytes (checked before the temp
 * write and again just before the rename), the temp file is private and in the same directory, both are fsynced,
 * and the result is read back byte for byte. A failure before the rename leaves the old file untouched.
 */
export const applyPlan = (plan: WriterPlan): void => {
  if (plan.after === null) return;
  const { file } = plan.before;
  if (currentSha(file) !== plan.before.sha256) throw new SeamGap("link-config-changed");
  const original = fs.lstatSync(file);
  const temp = path.join(path.dirname(file), `.${path.basename(file)}.fabric-v2-${randomBytes(8).toString("hex")}.tmp`);
  let fd: number | undefined;
  let renamed = false;
  try {
    fd = fs.openSync(temp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, original.mode & 0o600);
    let offset = 0;
    while (offset < plan.after.length) offset += fs.writeSync(fd, plan.after, offset, plan.after.length - offset);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;
    if (currentSha(file) !== plan.before.sha256) throw new SeamGap("link-config-changed");
    fs.renameSync(temp, file);
    renamed = true;
    const directory = fs.openSync(path.dirname(file), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  } catch (error) {
    if (error instanceof SeamGap) throw error;
    throw new SeamGap(renamed ? "link-config-sync-failed" : "link-config-write-failed");
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    if (!renamed) { try { fs.unlinkSync(temp); } catch { /* never created, or already gone */ } }
  }
  if (currentSha(file) !== plan.afterSha256) throw new SeamGap("link-config-readback-failed");
};
