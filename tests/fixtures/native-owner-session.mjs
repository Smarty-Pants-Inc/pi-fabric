// Test-only native session provisioning. The pinned SDK, never this fixture, writes the header.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { assertApprovedNativeSdk } from "../helpers/native-sdk-no-model.ts";

// Fence implementation lives ONLY in the actual compiled product entry. Import
// is side-effect-free; the standalone CLI below acquires BEFORE calling any SDK.
const productFence = async entry => {
  if (!entry || !path.isAbsolute(entry) || fs.realpathSync(entry) !== entry) throw new Error("Compiled native owner entry required");
  return import(pathToFileURL(entry).href);
};

export async function nativeOwnerSession(mode, sdkDirectory, file, cwd, pinFile, fenceFd, entry) {
if (!["provision", "verify"].includes(mode) || ![sdkDirectory, file, cwd, pinFile].every(value => value && path.isAbsolute(value))) {
  throw new Error("Explicit absolute native session inputs required");
}
const { assertNativeOwnerSessionFence } = await productFence(entry);
assertNativeOwnerSessionFence(file, fenceFd);
assertApprovedNativeSdk(sdkDirectory);
const pinned = fs.existsSync(pinFile) ? JSON.parse(fs.readFileSync(pinFile, "utf8")) : undefined;
if (mode === "verify" && !pinned) throw new Error("Native owner session pin missing");
let stat = fs.lstatSync(file, { throwIfNoEntry: false });
if (pinned && (!stat?.isFile() || stat.isSymbolicLink() || stat.size === 0)) throw new Error("Pinned native session missing or empty");
if (!stat) {
  if (mode !== "provision" || pinned) throw new Error("Native owner session is unavailable");
  const fd = fs.openSync(file, "wx", 0o600); fs.closeSync(fd);
  stat = fs.lstatSync(file);
}
if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || stat.uid !== process.geteuid() || (stat.mode & 0o7777) !== 0o600) {
  throw new Error("Unsafe native session file");
}
// Existing empty file is the inspected native SDK _setSessionFile initialization branch.
const { SessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
const manager = SessionManager.open(file, path.dirname(file), cwd);
const header = manager.getHeader();
const sessionId = manager.getSessionId();
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(sessionId) ||
    header?.type !== "session" || header.id !== sessionId || header.cwd !== cwd || manager.getSessionFile() !== file) {
  throw new Error("Native session header readback failed");
}
const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
try { if (!fs.fstatSync(fd).size) throw new Error("Native header was not persisted"); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
const directoryFd = fs.openSync(path.dirname(file), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
try { fs.fsyncSync(directoryFd); } finally { fs.closeSync(directoryFd); }
const reopened = SessionManager.open(file, path.dirname(file), cwd);
const value = { sessionFile: file, rootId: `session:${sessionId}`, sessionId };
if (reopened.getSessionId() !== sessionId || (pinned && JSON.stringify(pinned) !== JSON.stringify(value))) {
  throw new Error("Pinned native session identity changed");
}
if (!pinned) {
  const temporary = `${pinFile}.new`;
  const output = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeFileSync(output, JSON.stringify(value)); fs.fsyncSync(output); } finally { fs.closeSync(output); }
  fs.linkSync(temporary, pinFile); fs.unlinkSync(temporary);
  const parent = fs.openSync(path.dirname(pinFile), fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
  try { fs.fsyncSync(parent); } finally { fs.closeSync(parent); }
}
return value;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.umask(0o077);
  const [mode, sdkDirectory, file, cwd, pinFile, entry, expectedHash] = process.argv.slice(2);
  if (!/^[0-9a-f]{64}$/.test(expectedHash ?? "") || createHash("sha256").update(fs.readFileSync(entry)).digest("hex") !== expectedHash) {
    throw new Error("Native session fixture compiled product generation changed");
  }
  const { takeNativeOwnerSessionFence } = await productFence(entry);
  // Product fence FIRST, including before nativeOwnerSession's SDK import and
  // session/pin access. No duplicate fixture-only locking implementation.
  const fd = takeNativeOwnerSessionFence(file);
  try { process.stdout.write(JSON.stringify(await nativeOwnerSession(mode, sdkDirectory, file, cwd, pinFile, fd, entry)) + "\n"); }
  finally { fs.closeSync(fd); }
}
