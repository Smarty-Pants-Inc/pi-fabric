#!/usr/bin/env node
/** Opt-in native owner argv adapter. The existing task-entry owns ALL Pi SDK/RPC work.
 * Importing this module does not lock, read a session, load the SDK or start a Main.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const absolute = (file: string): boolean => !!file && path.isAbsolute(file) && path.resolve(file) === file && !/[\x00-\x1f\x7f]/.test(file);
const refused = (reason: string): never => { throw new Error(`Native owner entry refused: ${reason}`); };
// Same fail-closed local-filesystem policy as core/pid-namespace. In particular,
// overlay's backing store is unknown; NFS/CIFS/FUSE/9p/unknown are NOT local proof.
const localFilesystems = new Set([0xEF53, 0x58465342, 0x9123683E, 0x01021994, 0x2FC12FC1, 0xF2F52010]);

/** Trusted host ancestry, not a defence against malicious same-UID/root code.
 * Root-owned sticky temporary ancestors are allowed; the immediate state parent
 * must still be our private 0700 directory. No symlink normalization/adoption.
 */
const parentChain = (directory: string): void => {
  const uid = process.geteuid?.();
  if (process.platform !== "linux" || uid === undefined || !absolute(directory)) refused("absolute Linux owner path required");
  let current = path.parse(directory).root;
  for (const part of ["", ...directory.slice(current.length).split(path.sep).filter(Boolean)]) {
    if (part) current = path.join(current, part);
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.uid !== uid && stat.uid !== 0) ||
        ((stat.mode & 0o022) !== 0 && (stat.uid !== 0 || (stat.mode & 0o1000) === 0))) refused("unsafe parent chain");
  }
};
const privateParent = (file: string): number => {
  if (!absolute(file)) refused("canonical absolute session path required");
  const directory = path.dirname(file);
  parentChain(directory);
  const fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
  try {
    const held = fs.fstatSync(fd), named = fs.lstatSync(directory);
    if (!held.isDirectory() || held.uid !== process.geteuid!() || (held.mode & 0o7777) !== 0o700 ||
        held.dev !== named.dev || held.ino !== named.ino || named.isSymbolicLink()) refused("unsafe private session directory");
    if (!localFilesystems.has(Number(fs.statfsSync(`/proc/self/fd/${fd}`).type))) refused("session filesystem is nonlocal or unproven");
    return fd;
  } catch (error) { fs.closeSync(fd); throw error; }
};
const fenceIdentity = (file: string, fd: number): void => {
  const held = fs.fstatSync(fd), named = fs.lstatSync(`${file}.main.lock`);
  if (!held.isFile() || held.uid !== process.geteuid!() || held.nlink !== 1 || (held.mode & 0o7777) !== 0o600 ||
      !named.isFile() || named.isSymbolicLink() || named.dev !== held.dev || named.ino !== held.ino ||
      named.nlink !== 1 || named.uid !== held.uid || named.mode !== held.mode) refused("unsafe or replaced Main session fence");
};

/** Recheck the descriptor/path while doing fenced standalone provision/verify.
 * This checks custody invariants, not authentication of an arbitrary caller's fd.
 */
export function assertNativeOwnerSessionFence(file: string, fd: number): void {
  const parent = privateParent(file);
  try { fenceIdentity(file, fd); } finally { fs.closeSync(parent); }
}

/** Nonblocking real flock on the open description retained by THIS process.
 * Node opens CLOEXEC; ONLY the short-lived flock helper gets an explicit fd 3.
 * Resident/SDK children never inherit it. No PID/TTL, unlink, exit-hook unlock,
 * retry, fallback or separate owner process. A Main MUST NOT close the returned fd.
 * Standalone provision/verify may close it only after their actual work settles.
 */
export function takeNativeOwnerSessionFence(file: string): number {
  const parent = privateParent(file);
  let fd: number | undefined;
  try {
    // Anchor creation to the inspected directory, then compare the public path.
    fd = fs.openSync(`/proc/self/fd/${parent}/${path.basename(file)}.main.lock`,
      fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK, 0o600);
    fenceIdentity(file, fd);
    const result = spawnSync("setpriv", ["--pdeathsig", "KILL", "flock", "-x", "-n", "3"], {
      env: { PATH: "/usr/bin:/bin", LANG: "C" }, stdio: ["ignore", "ignore", "pipe", fd], timeout: 5_000, maxBuffer: 16_384,
    });
    if (result.error) throw result.error;
    if (result.status === 1) throw new Error("NATIVE_OWNER_SESSION_BUSY: another Main holds the session fence");
    if (result.status !== 0) refused(`kernel flock unavailable (${result.status}, ${result.signal}): ${result.stderr}`);
    const held = fs.fstatSync(parent), named = fs.lstatSync(path.dirname(file));
    parentChain(path.dirname(file));
    if (held.dev !== named.dev || held.ino !== named.ino || named.isSymbolicLink()) refused("session directory replaced while fencing");
    fenceIdentity(file, fd);
    return fd;
  } catch (error) { if (fd !== undefined) fs.closeSync(fd); throw error; }
  finally { fs.closeSync(parent); }
}

const assertSessionFile = (file: string): void => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const held = fs.fstatSync(fd), named = fs.lstatSync(file);
    if (!held.isFile() || held.uid !== process.geteuid!() || held.nlink !== 1 || (held.mode & 0o7777) !== 0o600 || held.size === 0 ||
        named.isSymbolicLink() || named.dev !== held.dev || named.ino !== held.ino) refused("pinned native session missing or empty, unsafe or replaced");
  } finally { fs.closeSync(fd); }
};

/** Same CLI as task-entry: <absolute-sdk-dist> <retry-scale> --mode rpc ... .
 * The optional host callback observes ACTUAL acquisition, before SDK delegation;
 * it lets the native test fixture verify its SDK-written configuration pin under
 * this very fence. The product CLI needs no fixture, hook or observation marker.
 */
export async function runNativeOwnerEntry(options: {
  readonly onMainFence?: (sessionFile: string, fenceFd: number) => void | Promise<void>;
} = {}): Promise<void> {
  const [sdkDirectory, scale, ...args] = process.argv.slice(2);
  if (!sdkDirectory || !absolute(sdkDirectory) || fs.realpathSync(sdkDirectory) !== sdkDirectory ||
      !scale || !Number.isFinite(Number(scale)) || Number(scale) <= 0) refused("explicit SDK generation and positive retry scale required");
  // Do not import the SDK parser before fencing, or let aliases/unknown flags
  // switch the actual session underneath the inspected --session argument.
  const switches = new Set(["--no-session", "--no-tools", "--no-extensions", "--no-skills", "--no-prompt-templates",
    "--no-context-files", "--no-themes", "--no-mcp", "--no-auto-compaction"]);
  const values = new Map<string, string[]>();
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (switches.has(arg)) {
      if (values.has(arg)) refused(`duplicate ${arg}`);
      values.set(arg, []);
    } else if (["--mode", "--session", "--extension", "-e"].includes(arg)) {
      const value = args[++index];
      if (!value || value.startsWith("-")) refused(`missing ${arg} value`);
      const key = arg === "-e" ? "--extension" : arg;
      const prior = values.get(key) ?? [];
      if (key !== "--extension" && prior.length) refused(`duplicate ${key}`);
      values.set(key, [...prior, value!]);
    } else refused(`unsupported native owner argument ${arg}`);
  }
  if (values.get("--mode")?.[0] !== "rpc" || !values.has("--no-tools") || !values.has("--no-extensions")) refused("native owner requires isolated RPC/no-tools/no-extensions");
  const sessionFile = values.get("--session")?.[0];
  const residentConfig = process.env.PI_FABRIC_RESIDENT_CONFIG;
  if (residentConfig !== undefined) {
    // REAL resident launcher shape: a separate in-memory Pi session, never the
    // Main's persisted file. A mere env flag cannot exempt --session from fencing.
    if (!absolute(residentConfig) || !values.has("--no-session") || sessionFile) refused("resident skip requires --no-session and no Main session");
  } else {
    if (!sessionFile || !absolute(sessionFile) || values.has("--no-session")) refused("Main requires exactly one persisted absolute --session");
    const fd = takeNativeOwnerSessionFence(sessionFile!);
    // Intentionally NO finally/close, including on hook/delegation failure. The
    // kernel, after ALL SDK exit handlers, is the Main's only release boundary.
    assertSessionFile(sessionFile!);
    await options.onMainFence?.(sessionFile!, fd);
    assertNativeOwnerSessionFence(sessionFile!, fd);
    assertSessionFile(sessionFile!);
  }
  const taskEntry = fileURLToPath(new URL("./task-entry.js", import.meta.url));
  process.argv = [process.execPath, taskEntry, sdkDirectory!, scale!, ...args];
  await import("./task-entry.js");
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  process.umask(0o077);
  await runNativeOwnerEntry();
}
