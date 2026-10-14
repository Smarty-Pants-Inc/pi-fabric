import fs from "node:fs";
import path from "node:path";
import { ContainmentUnavailableError, type AttemptLaunchReceipt } from "./attempt-containment-contract.js";

const FIELDS = ["attemptId", "scope", "cgroupPath", "launchedInScope", "at"] as const;
const MAX_BYTES = 64 * 1024;
const row = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const exactKeys = (value: Record<string, unknown>, keys: readonly string[]): boolean =>
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const text = (value: unknown, max: number): value is string =>
  typeof value === "string" && value.trim().length > 0 && value.length <= max;
const receiptFields = (value: Record<string, unknown>): boolean =>
  // A single bounded filename component, never a path supplied by the owner.
  typeof value.attemptId === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/.test(value.attemptId) &&
  text(value.scope, 256) && text(value.cgroupPath, 4096) && value.launchedInScope === true &&
  typeof value.at === "number" && Number.isSafeInteger(value.at) && value.at > 0 && value.at <= Date.now();
const unavailable = (reason: string): never => { throw new ContainmentUnavailableError(reason); };

/**
 * Read the launcher's immutable attempt-launch-<attemptId>.json, comparing
 * every receipt field verbatim with the observed owner/IPC binding. Linux only:
 * all file access is relative to an owned, non-writable-by-others root fd, with
 * O_NOFOLLOW and a bounded read of a same-uid 0600 regular file. No reconstruction
 * of cgroupPath/time, no writes, and no membership, exit or remote custody claim.
 */
export function readResidentLaunchAttestation(root: string, expected: AttemptLaunchReceipt): AttemptLaunchReceipt {
  const { O_RDONLY, O_DIRECTORY, O_NOFOLLOW, O_NONBLOCK } = fs.constants;
  const uid = process.getuid?.();
  if (process.platform !== "linux" || uid === undefined || O_DIRECTORY === undefined ||
      O_NOFOLLOW === undefined || O_NONBLOCK === undefined) {
    return unavailable("Launch attestation requires Linux directory-relative no-follow reads");
  }
  if (typeof root !== "string" || !root || !row(expected) || !exactKeys(expected, FIELDS) || !receiptFields(expected)) {
    return unavailable("Observed launch attestation is incomplete or invalid");
  }
  // Copy before opening: getters/mutation cannot change the expected binding mid-read.
  const binding: AttemptLaunchReceipt = { attemptId: expected.attemptId, scope: expected.scope,
    cgroupPath: expected.cgroupPath, launchedInScope: true, at: expected.at };
  let rootFd: number | undefined;
  let fileFd: number | undefined;
  try {
    // Normalize trailing separators so O_NOFOLLOW also rejects a symlink root/.
    rootFd = fs.openSync(path.resolve(root), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    const directory = fs.fstatSync(rootFd);
    if (!directory.isDirectory() || directory.uid !== uid || (directory.mode & 0o022) !== 0) {
      return unavailable("Launch attestation root is not an owned closed directory");
    }
    const procRoot = `/proc/self/fd/${rootFd}`;
    const pinned = fs.statSync(procRoot);
    if (pinned.dev !== directory.dev || pinned.ino !== directory.ino) {
      return unavailable("Launch attestation root descriptor cannot be verified");
    }
    fileFd = fs.openSync(`${procRoot}/attempt-launch-${binding.attemptId}.json`, O_RDONLY | O_NOFOLLOW | O_NONBLOCK);
    const before = fs.fstatSync(fileFd);
    const safeFile = (stat: fs.Stats): boolean => stat.isFile() && stat.uid === uid &&
      (stat.mode & 0o7777) === 0o600 && stat.size > 0 && stat.size <= MAX_BYTES;
    if (!safeFile(before)) return unavailable("Launch attestation is not a bounded same-user 0600 regular file");
    // Never readFileSync: a file growing after fstat must not cause an unbounded allocation.
    const bytes = Buffer.alloc(MAX_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const read = fs.readSync(fileFd, bytes, length, bytes.length - length, null);
      if (read === 0) break;
      length += read;
    }
    const after = fs.fstatSync(fileFd);
    const heldRoot = fs.fstatSync(rootFd);
    if (length > MAX_BYTES || length !== before.size || !safeFile(after) ||
        after.dev !== before.dev || after.ino !== before.ino || after.size !== before.size ||
        after.mtimeMs !== before.mtimeMs || after.ctimeMs !== before.ctimeMs ||
        heldRoot.uid !== uid || (heldRoot.mode & 0o022) !== 0) {
      return unavailable("Launch attestation changed during its bounded read");
    }
    const record: unknown = JSON.parse(bytes.subarray(0, length).toString("utf8"));
    if (!row(record) || !exactKeys(record, FIELDS) || !receiptFields(record) ||
        FIELDS.some(key => record[key] !== binding[key])) {
      return unavailable("Launcher record does not exactly match the observed launch attestation");
    }
    return binding;
  } catch (error) {
    if (error instanceof ContainmentUnavailableError) throw error;
    // Native errors can include private paths; this API exposes only a fixed refusal.
    return unavailable("Launcher launch attestation is missing, unsafe or unreadable");
  } finally {
    if (fileFd !== undefined) { try { fs.closeSync(fileFd); } catch { /* already closed */ } }
    if (rootFd !== undefined) { try { fs.closeSync(rootFd); } catch { /* already closed */ } }
  }
}
