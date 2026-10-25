// Test-only native session provisioning. The pinned SDK, never this fixture, writes the header.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { assertApprovedNativeSdk } from "../helpers/native-sdk-no-model.ts";

// Same supported POSIX open-description flock facility as residency/file-lock.
// Node's own fd stays CLOEXEC: ONLY flock's fd 3 gets an explicit duplicate.
// No resident or arbitrary descendant inherits/retains the Main's lifetime lock.
export function takeNativeOwnerSessionFence(file) {
  if (process.platform !== "linux" || !path.isAbsolute(file)) throw new Error("Native Main requires an absolute Linux session fence");
  const lock = `${file}.main.lock`;
  const fd = fs.openSync(lock, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== process.geteuid() || stat.nlink !== 1 || (stat.mode & 0o777) !== 0o600) {
      throw new Error("Unsafe native Main session fence");
    }
    const result = spawnSync("flock", ["-x", "-n", "3"], { stdio: ["ignore", "ignore", "pipe", fd], timeout: 5_000 });
    if (result.error) throw result.error;
    if (result.status === 1) throw new Error("NATIVE_OWNER_SESSION_BUSY: another Main holds the session fence");
    if (result.status !== 0) throw new Error(`Native Main flock failed (${result.status}): ${result.stderr}`);
    return fd; // Kernel descriptor lifetime, never a PID/timestamp record.
  } catch (error) { fs.closeSync(fd); throw error; }
}

export async function nativeOwnerSession(mode, sdkDirectory, file, cwd, pinFile, fenceFd) {
if (!["provision", "verify"].includes(mode) || ![sdkDirectory, file, cwd, pinFile].every(value => value && path.isAbsolute(value))) {
  throw new Error("Explicit absolute native session inputs required");
}
const held = fs.fstatSync(fenceFd), named = fs.lstatSync(`${file}.main.lock`);
if (held.dev !== named.dev || held.ino !== named.ino) throw new Error("Native Main session fence replaced");
assertApprovedNativeSdk(sdkDirectory);
const { SessionManager } = await import(pathToFileURL(path.join(sdkDirectory, "core/session-manager.js")).href);
const pinned = fs.existsSync(pinFile) ? JSON.parse(fs.readFileSync(pinFile, "utf8")) : undefined;
if (mode === "verify" && !pinned) throw new Error("Native owner session pin missing");
let stat = fs.lstatSync(file, { throwIfNoEntry: false });
if (pinned && (!stat?.isFile() || stat.isSymbolicLink() || stat.size === 0)) throw new Error("Pinned native session missing or empty");
if (!stat) {
  if (mode !== "provision" || pinned) throw new Error("Native owner session is unavailable");
  const fd = fs.openSync(file, "wx", 0o600); fs.closeSync(fd);
  stat = fs.lstatSync(file);
}
if (!stat.isFile() || stat.isSymbolicLink() || stat.uid !== process.geteuid() || (stat.mode & 0o077) !== 0) {
  throw new Error("Unsafe native session file");
}
// Existing empty file is the inspected native SDK _setSessionFile initialization branch.
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
  const [mode, sdkDirectory, file, cwd, pinFile] = process.argv.slice(2);
  // Provision and standalone verify also fence BEFORE any SDK import, session
  // stat/read/open or pin read. There is no unfenced parent-side preflight.
  const fd = takeNativeOwnerSessionFence(file);
  try { process.stdout.write(JSON.stringify(await nativeOwnerSession(mode, sdkDirectory, file, cwd, pinFile, fd)) + "\n"); }
  finally { fs.closeSync(fd); }
}
