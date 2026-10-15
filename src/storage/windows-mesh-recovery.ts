import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  interpretMeshRetireResult, windowsLogicalPathUnits,
  type MeshRetireChildResult, type WindowsMeshCapability,
} from "./windows-mesh-release.js";

/** C/D authority comes ONLY from the caller's pre-death-judgment snapshots and full independently judged records.
 * This adapter never snapshots a canonical successor, judges death, resolves a new capability, replays a request,
 * renames in JavaScript, or collects a retained F. Native C retains F/owner permanently; native D never detaches L.
 * Every native child is synchronous, finite (native watchdog + parent timeout), and joined before return. */
export interface WindowsMeshRecoveryEntry {
  readonly path: string;
  readonly stat: fs.BigIntStats;
  readonly record?: string;
}
export type WindowsMeshRecoveryOutcome =
  | { outcome: "recovered" }
  | { outcome: "refused"; reason: string }
  | { outcome: "helper-unavailable/not-started" | "recovery-outcome-unknown"; reason: string };

export type WindowsMeshRecoveryCheckpoint = "before-commit" | "after-commit" | "debris-verified" | `debris-after-${number}`;
/** In-process only. Inherited environment checkpoints are always stripped. No substitute filesystem/child hook. */
export const windowsMeshRecoveryTestHooks: {
  checkpoint?: { name: WindowsMeshRecoveryCheckpoint; dir: string };
  afterChild?: (sample: { absolutePath: string; digest: string; mode: "owner" | "debris"; outcome: string }) => void;
} = {};

const DEFAULT_DEADLINE = 4000;
const MAX_ENTRIES = 64;
const TEST_PREFIX = "FABRIC_WIN_MESH_TEST_";
const realWindows = (): boolean => process.platform === "win32" && os.type() === "Windows_NT";
const errno = (error: unknown): string => (error as NodeJS.ErrnoException)?.code ?? "error";
const refused = (reason: string): WindowsMeshRecoveryOutcome => ({ outcome: "refused", reason });
const unknown = (reason: string): WindowsMeshRecoveryOutcome => ({ outcome: "recovery-outcome-unknown", reason });
const id = (value: bigint): string => {
  if (typeof value !== "bigint" || value < 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError("id out of range");
  return value.toString(16);
};
const fileTime = (stat: fs.BigIntStats): bigint => stat.mtimeNs / 100n + 116_444_736_000_000_000n;
const deadline = (ms: number): number => {
  if (!Number.isInteger(ms) || ms < 1 || ms > 30_000) throw new RangeError("deadline must be 1..30000 ms");
  return ms;
};
const bytesOf = (record: string): Buffer => {
  const bytes = Buffer.from(record, "utf8");
  if (bytes.length < 1 || bytes.length > 4096 || bytes.includes(0) || bytes.toString("utf8") !== record) {
    throw new RangeError("each full original record must be 1..4096 UTF-8 bytes without NUL");
  }
  return bytes;
};
/** Pure lexical validation. No realpath/lstat/read of the judged lock is permitted here. */
const nativePath = (file: string): string => {
  if (file.includes("/") || file.includes("\0") || !path.win32.isAbsolute(file)) throw new RangeError("path must be absolute Windows syntax");
  const native = path.win32.toNamespacedPath(file);
  const logical = native.startsWith("\\\\?\\UNC\\") ? `\\\\${native.slice(8)}` : native.startsWith("\\\\?\\") ? native.slice(4) : "";
  if (!(/^[a-zA-Z]:\\/.test(logical) || /^\\\\[^\\]+\\[^\\]+\\/.test(logical)) || path.win32.normalize(file) !== file ||
      Buffer.from(native, "utf8").toString("utf8") !== native || Buffer.byteLength(native, "utf8") > 32_767 ||
      windowsLogicalPathUnits(native) >= 240) throw new RangeError("path must be canonical, valid UTF-8 and below 240 logical UTF-16 units");
  // No stream, dot, slash, trailing-dot/space or device aliases below the drive/UNC root.
  const root = path.win32.parse(logical).root;
  for (const component of logical.slice(root.length).split("\\")) {
    if (!component || component === "." || component === ".." || /[\x00-\x1f<>:"/\\|?*]|[. ]$/.test(component)) throw new RangeError("illegal path component");
  }
  return native;
};
const canonicalLock = (lock: string): string => {
  const native = nativePath(lock);
  if (![".lock", "custody.lock"].includes(path.win32.basename(native))) throw new RangeError("only canonical mesh/custody lock directories are recoverable (never retained F)");
  return native;
};
const frame = (magic: "FMRC1" | "FMRD1", fields: readonly (string | Uint8Array)[]): Buffer => {
  const parts: Uint8Array[] = [Buffer.from(magic, "ascii")];
  let total = 5;
  for (const field of fields) {
    const bytes = typeof field === "string" ? Buffer.from(field, "utf8") : field;
    total += 8 + bytes.length;
    if (total > 65_536) throw new RangeError("request exceeds 64 KiB");
    parts.push(Buffer.from(bytes.length.toString(16).padStart(8, "0"), "ascii"), bytes);
  }
  return Buffer.concat(parts);
};

/** Existing F ABI: Number-Stats DECIMAL dev:ino:FULL R. Only naming rounds; original S stays bigint. */
export const windowsMeshRecoveryFence = (lock: string, directory: fs.BigIntStats, originalRecord: string): string =>
  `${lock}.dead.${createHash("sha256").update(`${Number(directory.dev)}:${Number(directory.ino)}:${originalRecord}`).digest("hex")}`;

/** C's ten fields are A's framing, with FMRC1 and the existing retained F destination instead of normal T. */
export const encodeMeshOwnerRecoveryRequest = (
  lock: string, originalRecord: string, directory: fs.BigIntStats, owner: fs.BigIntStats, deadlineMs = DEFAULT_DEADLINE,
): Buffer => {
  const source = canonicalLock(lock);
  const dest = nativePath(windowsMeshRecoveryFence(lock, directory, originalRecord));
  if (windowsLogicalPathUnits(`${source}\\owner`) >= 240 || windowsLogicalPathUnits(`${dest}\\owner`) >= 240) throw new RangeError("owner path must be below 240 logical UTF-16 units");
  const record = bytesOf(originalRecord);
  if (!directory.isDirectory() || directory.isSymbolicLink() || !owner.isFile() || owner.isSymbolicLink() || owner.size !== BigInt(record.length)) {
    throw new RangeError("provided S must identify a real directory and the full original regular owner");
  }
  return frame("FMRC1", [source, dest, id(directory.dev), id(directory.ino), id(owner.dev), id(owner.ino), id(fileTime(owner)), id(owner.size), record, String(deadline(deadlineMs))]);
};

const retiredComponent = (name: string): boolean => /^r\..+/.test(name) && !/[\x00-\x1f<>:"/\\|?*]|[. ]$/.test(name);
const releasedComponent = (name: string): boolean => /^owner\.released\.[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(name) && name.startsWith("owner.released.");
/** D is child-first, ALL-before-ANY, at most 64 entries/depth 4; every file needs its OWN record.
 * These checks do NOT replace independent caller death judgment; raw/torn/live/unproven bytes must never be submitted. */
export const encodeMeshDebrisRecoveryRequest = (
  lock: string, entries: readonly WindowsMeshRecoveryEntry[], deadlineMs = DEFAULT_DEADLINE,
): Buffer => {
  const top = canonicalLock(lock);
  if (entries.length < 1 || entries.length > MAX_ENTRIES) throw new RangeError("inventory must have 1..64 entries including top L");
  const paths = entries.map(entry => nativePath(entry.path));
  if (paths[paths.length - 1] !== top || !entries[entries.length - 1]!.stat.isDirectory()) throw new RangeError("top L must be the final directory");
  if (new Set(paths.map(file => file.toLowerCase())).size !== paths.length) throw new RangeError("duplicate/aliased inventory path");
  const fields: (string | Uint8Array)[] = [top, String(deadline(deadlineMs)), String(entries.length)];
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]!, file = paths[i]!, directory = entry.stat.isDirectory();
    if (entry.stat.isSymbolicLink() || (!directory && !entry.stat.isFile())) throw new RangeError("inventory must contain only real directories and regular files");
    if (i < entries.length - 1) {
      if (!file.startsWith(`${top}\\`)) throw new RangeError("entry is outside canonical L");
      const components = file.slice(top.length + 1).split("\\");
      const leaf = components.pop()!;
      if (components.length + 1 > 4 || !components.every(retiredComponent) ||
          (directory ? !retiredComponent(leaf) : !(releasedComponent(leaf) || (components.length > 0 && leaf === "owner")))) {
        throw new RangeError("entry is outside the independently judged legacy grammar/depth");
      }
      const parentIndex = paths.indexOf(path.win32.dirname(file), i + 1);
      if (parentIndex < 0 || !entries[parentIndex]!.stat.isDirectory()) throw new RangeError("every directory must follow all its children");
    }
    const record = directory ? Buffer.alloc(0) : bytesOf(entry.record ?? "");
    if (directory ? entry.record !== undefined : entry.stat.size !== BigInt(record.length)) throw new RangeError("record must match each regular file; directories omit it");
    fields.push(file, directory ? "d" : "f", id(entry.stat.dev), id(entry.stat.ino), id(fileTime(entry.stat)), id(entry.stat.size), record);
  }
  return frame("FMRD1", fields);
};

/** Closed native D terminal tuples. A valid partial record is NOT a claim that no mutation happened. */
const DEBRIS_DETAILS: Readonly<Record<string, readonly string[]>> = {
  "0 recovered cleanup": ["clean"],
  "3 refused parse": ["watchdog-unavailable", "stdin-unreadable", "message-too-large", "bad-magic", "bad-field-length", "truncated-field", "bad-fields", "bad-path", "mesh-path-too-long", "bad-kind", "bad-id", "bad-record", "bad-placement"],
  "3 refused guard": ["ancestor-not-held"],
  "3 refused verify": ["entry-unverified", "entry-location", "directory-modified", "record-unverified", "unexpected-entries", "deadline"],
  "2 partial delete": ["deadline", "entry-delete"],
};
/** Pure result adapter: no exit/output mismatch, timeout, signal or ambiguous start is ever a pre-mutation refusal. */
export const interpretMeshRecoveryResult = (result: MeshRetireChildResult, mode: "owner" | "debris"): WindowsMeshRecoveryOutcome => {
  const text = typeof result.stdout === "string" ? result.stdout : Buffer.from(result.stdout ?? new Uint8Array()).toString("utf8");
  const exit = result.signal ? `signal ${result.signal}` : `exit ${String(result.status)}`;
  if (result.error) {
    // Only OS spawn failures positively known not to have executed the child are not-started. In particular a
    // timeout with no pid is still ambiguous; never infer pre-mutation from absent/truncated stdout.
    if (!result.pid && result.status === null && !result.signal && text.length === 0 && ["ENOENT", "EACCES", "ENOEXEC", "ENOTDIR"].includes(errno(result.error))) {
      return { outcome: "helper-unavailable/not-started", reason: `the retained helper did not start (${errno(result.error)})` };
    }
    return unknown(`the helper did not finish (${errno(result.error)}, ${exit})`);
  }
  if (result.signal || result.status === null) return unknown(`the helper was terminated (${exit})`);
  if (mode === "owner") {
    if (result.status === 0 && text === "FMREC1 recovered commit retained\n") return { outcome: "recovered" };
    // Reuse A's CLOSED pre-commit tuple table. C never accepts A's released/cleanup tuples as completion.
    if (/^FMR1 (refused|not-owner) /.test(text)) {
      const before = interpretMeshRetireResult(result, "");
      if (before.outcome === "refused-before-retirement") return refused(before.reason);
      if (before.outcome === "not-owner") return refused(`the judged original incarnation is no longer present (${text.trim()})`);
    }
  } else {
    const match = /^FMREC1 (recovered|refused|partial) (parse|guard|verify|cleanup|delete) ([a-z0-9-]{1,48})\n$/.exec(text);
    if (match && match[0] === text) {
      const [, outcome, phase, detail] = match;
      if (DEBRIS_DETAILS[`${result.status} ${outcome} ${phase}`]?.includes(detail!)) {
        if (outcome === "recovered") return { outcome: "recovered" };
        if (outcome === "refused") return refused(`${phase}: ${detail}`);
        return unknown(`native partial repair (${phase}: ${detail}); selected entries may already be gone; independently rejudge survivors`);
      }
    }
  }
  return unknown(`no valid terminal/exit pairing (${mode}, ${exit}, ${text.length} output bytes)`);
};

const childEnv = (): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) if (!key.toUpperCase().startsWith(TEST_PREFIX)) env[key] = value;
  const hook = windowsMeshRecoveryTestHooks.checkpoint;
  if (hook && (["before-commit", "after-commit", "debris-verified"].includes(hook.name) || /^debris-after-(?:[0-9]|[1-5][0-9]|6[0-3])$/.test(hook.name))) {
    env[`${TEST_PREFIX}CHECKPOINT`] = hook.name;
    env[`${TEST_PREFIX}DIR`] = hook.dir;
  }
  return env;
};
const run = (mode: "owner" | "debris", input: Buffer, capability: WindowsMeshCapability, deadlineMs: number): WindowsMeshRecoveryOutcome => {
  // Captured acquisition capability only. No bootstrap discovery or substitute digest in recovery.
  try {
    const stat = fs.lstatSync(capability.absolutePath);
    if (capability.meshAbi !== 1 || !path.win32.isAbsolute(capability.absolutePath) || !/^[0-9a-f]{64}$/.test(capability.digest) ||
        !stat.isFile() || stat.isSymbolicLink() || createHash("sha256").update(fs.readFileSync(capability.absolutePath)).digest("hex") !== capability.digest) {
      return { outcome: "helper-unavailable/not-started", reason: "captured retained capability is unavailable or changed" };
    }
  } catch (error) { return { outcome: "helper-unavailable/not-started", reason: `captured retained capability is unreadable (${errno(error)})` }; }
  let outcome: WindowsMeshRecoveryOutcome;
  try {
    const child = spawnSync(capability.absolutePath, [mode === "owner" ? "--mesh-recover-owner-v1" : "--mesh-recover-debris-v1"], {
      input, timeout: deadlineMs + 1000, windowsHide: true, maxBuffer: 4096, env: childEnv(),
    });
    outcome = interpretMeshRecoveryResult(child, mode);
  } catch (error) { outcome = unknown(`native invocation did not produce a joined terminal result (${errno(error)})`); }
  try { windowsMeshRecoveryTestHooks.afterChild?.({ absolutePath: capability.absolutePath, digest: capability.digest, mode, outcome: outcome.outcome }); }
  catch { /* diagnostic observers never change recovery */ }
  return outcome;
};

export function retireWindowsMeshOwner(
  lock: string, originalRecord: string, directory: fs.BigIntStats, owner: fs.BigIntStats,
  capability: WindowsMeshCapability, deadlineMs = DEFAULT_DEADLINE,
): WindowsMeshRecoveryOutcome {
  if (!realWindows()) return { outcome: "helper-unavailable/not-started", reason: "Windows mesh recovery runs on real Windows only" };
  let input: Buffer;
  try { input = encodeMeshOwnerRecoveryRequest(lock, originalRecord, directory, owner, deadlineMs); }
  catch (error) { return refused(`invalid judged owner request: ${error instanceof Error ? error.message : errno(error)}`); }
  return run("owner", input, capability, deadlineMs); // exactly ONE child, even after unknown; never resnapshot B
}
export function removeWindowsMeshDebris(
  lock: string, entries: readonly WindowsMeshRecoveryEntry[], capability: WindowsMeshCapability, deadlineMs = DEFAULT_DEADLINE,
): WindowsMeshRecoveryOutcome {
  if (!realWindows()) return { outcome: "helper-unavailable/not-started", reason: "Windows mesh recovery runs on real Windows only" };
  let input: Buffer;
  try { input = encodeMeshDebrisRecoveryRequest(lock, entries, deadlineMs); }
  catch (error) { return refused(`invalid independently judged inventory: ${error instanceof Error ? error.message : errno(error)}`); }
  return run("debris", input, capability, deadlineMs);
}
