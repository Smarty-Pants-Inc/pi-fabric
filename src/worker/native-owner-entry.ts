#!/usr/bin/env node
/** Opt-in native owner argv adapter. The existing task-entry owns ALL Pi SDK/RPC work.
 * Importing this module does not lock, read a session, load the SDK or start a Main.
 */
import fs from "node:fs";
import path from "node:path";
import { homedir } from "node:os";
import { spawnSync } from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

const absolute = (file: string): boolean => !!file && path.isAbsolute(file) && path.resolve(file) === file && !/[\x00-\x1f\x7f]/.test(file);
export class NativeOwnerFenceError extends Error {
  readonly code = "NATIVE_OWNER_FENCE_REQUIRED";
  constructor(reason: string) { super(`NATIVE_OWNER_FENCE_REQUIRED: ${reason}`); this.name = "NativeOwnerFenceError"; }
}
const refused = (reason: string): never => { throw new NativeOwnerFenceError(reason); };
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

/** A FLOCK in THIS fdinfo is evidence for this open description, not a PID
 * attribution: Linux may show the already-exited helper as flock's holder.
 * An independently opened, unlocked fd to the SAME inode is not such evidence.
 */
export function assertNativeOwnerSessionFence(file: string, fd: number): void {
  let parent: number | undefined;
  try {
    if (!Number.isSafeInteger(fd) || fd < 0) refused("missing self fence descriptor");
    parent = privateParent(file);
    fenceIdentity(file, fd);
    const held = fs.fstatSync(fd), info = fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8");
    const locks = [...info.matchAll(/^lock:\s+\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s+-?\d+\s+[0-9a-f]+:[0-9a-f]+:(\d+)\s+0\s+EOF\s*$/gm)];
    if (!locks.some(lock => BigInt(lock[1]!) === BigInt(held.ino))) refused("self open description has no whole-file WRITE FLOCK");
    fenceIdentity(file, fd); // public name must still denote the held inode
  } catch (error) {
    if (error instanceof NativeOwnerFenceError) throw error;
    refused(`missing, unsafe or replaced self fence (${error instanceof Error ? error.message : String(error)})`);
  } finally { if (parent !== undefined) fs.closeSync(parent); }
}

// Share only numeric custody across compiled entry/chunk copies. No extra fd,
// hidden lifetime anchor, disk protocol, caller env assertion or PID heuristic.
const custodyKey = Symbol.for("pi-fabric.nativeOwnerSessionFence.v1");
interface NativeFenceCustody { fd: number; dev: number; ino: number }
const host = globalThis as unknown as Record<symbol, Map<string, NativeFenceCustody> | undefined>;
const custody = host[custodyKey] ?? (host[custodyKey] = new Map());

/** Require actual process-local custody. A retained old holder is NEVER rebound
 * to a replacement public lock inode. Trusted-host check-to-write races remain.
 */
export function requireNativeOwnerSessionFence(file: string): number {
  const retained = custody.get(file);
  if (retained) {
    assertNativeOwnerSessionFence(file, retained.fd);
    const held = fs.fstatSync(retained.fd);
    if (held.dev !== retained.dev || held.ino !== retained.ino) refused("original self fence descriptor replaced");
    return retained.fd;
  }
  if (process.platform !== "linux" || !absolute(file)) refused("canonical Linux native session required");
  for (const name of fs.readdirSync("/proc/self/fd")) {
    const fd = Number(name);
    try { assertNativeOwnerSessionFence(file, fd); }
    catch (error) { if (error instanceof NativeOwnerFenceError) continue; throw error; }
    const held = fs.fstatSync(fd);
    custody.set(file, { fd, dev: held.dev, ino: held.ino });
    return fd;
  }
  return refused("this process does not hold the pinned native session fence");
}

/** Host-side I/O admission, not an SDK source patch. Install before SDK loading
 * on the product route; syncBuiltinESMExports also gates already-captured named
 * fs bindings on plain CLI. Cover path AND fd opens/appends/writes. Recheck the
 * ORIGINAL self fd against the public name at each operation, never a new inode.
 * This is cooperative same-UID fencing, not OS mandatory access control.
 */
export function guardNativeOwnerSessionIo(file: string): void {
  requireNativeOwnerSessionFence(file);
  if (ioTargets.has(file)) return;
  const held = fs.lstatSync(file);
  ioTargets.set(file, { dev: held.dev, ino: held.ino });
  if (ioInstalled) return;
  ioInstalled = true;
  const check = (value: unknown): void => {
    for (const [target, pin] of ioTargets) {
      let matches = false;
      if (typeof value === "number") {
        try { const stat = fs.fstatSync(value); matches = stat.dev === pin.dev && stat.ino === pin.ino; }
        catch { /* the original fs operation diagnoses invalid descriptors */ }
      } else {
        const text = value instanceof URL ? fileURLToPath(value) : Buffer.isBuffer(value) ? value.toString() : value;
        matches = typeof text === "string" && path.resolve(text) === target;
      }
      if (matches) {
        requireNativeOwnerSessionFence(target);
        const named = fs.lstatSync(target);
        if (!named.isFile() || named.isSymbolicLink() || named.dev !== pin.dev || named.ino !== pin.ino) refused("pinned session inode replaced");
      }
    }
  };
  const wrap = (object: object, names: readonly string[]): void => {
    const methods = object as Record<string, (...args: unknown[]) => unknown>;
    for (const name of names) {
      const original = methods[name];
      if (typeof original !== "function") continue;
      methods[name] = function(...args: unknown[]) { check(args[0]); return Reflect.apply(original, this, args); };
    }
  };
  wrap(fs, ["open", "openSync", "appendFile", "appendFileSync", "writeFile", "writeFileSync", "write", "writeSync", "writev", "writevSync", "truncate", "truncateSync", "ftruncate", "ftruncateSync"]);
  wrap(fs.promises, ["open", "appendFile", "writeFile", "truncate"]);
  syncBuiltinESMExports();
}
const ioTargets = new Map<string, { dev: number; ino: number }>();
let ioInstalled = false;

interface NativeOwnerConfig { sessionFile: string; sessionId: string; rootId: string }
/** Deployment-supplied SDK identity tuple, independent of the removable lock.
 * This private config selects a gate; neither it nor its environment selector
 * proves custody. Do not open/read the native JSONL to discover its identity.
 */
const nativeOwnerConfig = (): NativeOwnerConfig | undefined => {
  const configFile = process.env.PI_FABRIC_NATIVE_OWNER_CONFIG;
  if (configFile === undefined) return undefined;
  let parent: number | undefined, fd: number | undefined;
  try {
    parent = privateParent(configFile);
    fd = fs.openSync(configFile, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
    const held = fs.fstatSync(fd), named = fs.lstatSync(configFile);
    if (!held.isFile() || held.uid !== process.geteuid!() || held.nlink !== 1 || (held.mode & 0o7777) !== 0o600 ||
        named.isSymbolicLink() || named.dev !== held.dev || named.ino !== held.ino || held.size > 16_384) refused("unsafe native identity config");
    const value = JSON.parse(fs.readFileSync(fd, "utf8")) as NativeOwnerConfig | null;
    if (!value || typeof value !== "object" || !absolute(value.sessionFile) ||
        typeof value.sessionId !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value.sessionId) ||
        value.rootId !== `session:${value.sessionId}`) refused("invalid native identity config tuple");
    return { sessionFile: value!.sessionFile, sessionId: value!.sessionId, rootId: value!.rootId };
  } catch (error) {
    if (error instanceof NativeOwnerFenceError) throw error;
    return refused(`native identity config unavailable (${error instanceof Error ? error.message : String(error)})`);
  } finally { if (fd !== undefined) fs.closeSync(fd); if (parent !== undefined) fs.closeSync(parent); }
};

/** Match 031c main.resolveSessionPath/utils.resolvePath without importing the
 * SDK or reading history. Bare IDs are discovery, NOT cwd-relative filenames.
 * Config selects policy, never custody; both routes reuse the same OFD proof.
 */
const guardNativeOwnerSelection = (file: string | undefined, cwd: string): void => {
  if (!file) return;
  const config = nativeOwnerConfig();
  if (config && file === config.sessionId) refused("native session selection must use its canonical pin");
  if (!file.includes("/") && !file.includes("\\") && !file.endsWith(".jsonl")) return;
  const normalized = file === "~" ? homedir() : file.startsWith("~/") ? path.join(homedir(), file.slice(2)) :
    /^file:\/\//.test(file) ? fileURLToPath(file) : file;
  const selected = path.resolve(cwd, normalized);
  // Configured selection must be provable, not a textual non-match or a failed
  // realpath treated as permission. No native JSONL is opened to discover ID.
  const canonical = config ? fs.realpathSync(selected) : fs.existsSync(selected) ? fs.realpathSync(selected) : selected;
  const pinned = config ? fs.realpathSync(config.sessionFile) : undefined;
  const native = pinned !== undefined && canonical === pinned;
  if (!native && !fs.lstatSync(`${canonical}.main.lock`, { throwIfNoEntry: false })) return;
  if (native ? pinned !== config!.sessionFile : selected !== canonical) refused("native session selection must use its canonical pin");
  guardNativeOwnerSessionIo(canonical);
};

/** task-entry also serves ordinary custom sessions. The explicit product config
 * selects the native pin even if its lock name is gone; the adjacent lock remains
 * a conservative additional hint for older callers. Neither is kernel proof.
 */
export function guardNativeOwnerSessionSelection(file: string | undefined, cwd: string): void {
  guardNativeOwnerSelection(file, cwd);
}

const nativeOwnerFailStop = (error: unknown): never => {
  const typed = error instanceof NativeOwnerFenceError ? error : new NativeOwnerFenceError(error instanceof Error ? error.message : String(error));
  // SDK extension loading and hook dispatch isolate ordinary throws. Refusal
  // must terminate synchronously, without shutdown hooks that could append.
  fs.writeSync(2, `${typed.name}: ${typed.message}\n`);
  process.exit(1);
};

/** Registration precedes SDK thinking metadata but has no SDK identity. With
 * config set, ambiguous selectors/unknown flags ALWAYS fail-stop, even for an
 * ordinary target. Consume known values exactly as 031c does, never treating a
 * value as another option. No last-session-wins or native bare-prefix resolver.
 */
export function guardNativeOwnerRegistration(): void {
  if (process.env.PI_FABRIC_NATIVE_OWNER_CONFIG === undefined) return;
  try {
    const args = process.argv.slice(2);
    const values = new Set(["--mode", "--provider", "--model", "--api-key", "--system-prompt", "--append-system-prompt",
      "--name", "-n", "--max-provider-requests", "--session-dir", "--models", "--tools", "-t", "--exclude-tools", "-xt",
      "--thinking", "--extension", "-e", "--skill", "--prompt-template", "--theme", "--use-theme", "--tui-mode"]);
    const switches = new Set(["--no-auto-compaction", "--no-tools", "-nt", "--no-builtin-tools", "-nbt",
      "--no-extensions", "-ne", "--no-mcp", "--no-skills", "-ns", "--no-prompt-templates", "-np", "--no-themes",
      "--no-context-files", "-nc", "--verbose", "--approve", "-a", "--no-approve", "-na", "--offline"]);
    let file: string | undefined, neverNative = false;
    for (let index = 0; index < args.length; index++) {
      const arg = args[index]!;
      if (arg === "--") break;
      if (["--continue", "-c", "--resume", "-r"].includes(arg)) refused(`ambiguous native session selector ${arg}`);
      if (arg === "--session") {
        if (file !== undefined) refused("duplicate --session");
        file = args[++index];
        if (!file) refused("missing --session value");
      } else if (["--fork", "--session-id", "--export"].includes(arg) || values.has(arg)) {
        const value = args[++index];
        if (value === undefined || (["--mode", "--use-theme", "--max-provider-requests", "--tui-mode"].includes(arg) && value.startsWith("-"))) {
          refused(`missing ${arg} value`);
        }
        if (["--fork", "--session-id", "--export"].includes(arg)) neverNative = true;
      } else if (["--no-session", "--help", "-h", "--version", "-v", "--list-models"].includes(arg)) {
        neverNative = true;
        if (arg === "--list-models" && args[index + 1] !== undefined && !args[index + 1]!.startsWith("-") && !args[index + 1]!.startsWith("@")) index++;
      } else if (arg === "--print" || arg === "-p") {
        const next = args[index + 1];
        if (next !== undefined && !next.startsWith("@") && (!next.startsWith("-") || next.startsWith("---"))) index++;
      } else if (/^--max-provider-requests=[1-9][0-9]*$/.test(arg)) {
        // The only supported inline-value spelling in 031c's safe flag set.
      } else if (!switches.has(arg) && arg.startsWith("-")) refused(`unknown native owner option ${arg}`);
    }
    if (!neverNative) guardNativeOwnerSelection(file, process.cwd());
  } catch (error) { nativeOwnerFailStop(error); }
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
    assertNativeOwnerSessionFence(file, fd);
    const original = fs.fstatSync(fd);
    custody.set(file, { fd, dev: original.dev, ino: original.ino });
    return fd;
  } catch (error) { if (fd !== undefined) fs.closeSync(fd); throw error; }
  finally { fs.closeSync(parent); }
}

/** Call only for the native root pinned by a registered application declaration.
 * The actual SDK header/id selects identity, never an envelope or env-asserted ID.
 * Fail-stop before Fabric activation; an ordinary hook throw is caught by Pi and
 * is NOT a refusal. No shutdown hooks can append after this synchronous refusal.
 * Upstream plain CLI pre-hook reads/repair are outside this cooperative boundary.
 */
export function guardNativeOwnerBootstrap(context: ExtensionContext): void {
  try {
    const manager = context.sessionManager, header = manager.getHeader();
    const file = manager.getSessionFile();
    if (!header || header.type !== "session" || header.id !== manager.getSessionId() || !file || !absolute(file) || fs.realpathSync(file) !== file) {
      refused("native bootstrap requires its pinned actual SDK header/session and canonical file");
    }
    const config = nativeOwnerConfig();
    if (config && (file !== config.sessionFile || header!.id !== config.sessionId)) refused("actual SDK session differs from native identity config");
    guardNativeOwnerSessionIo(file!);
  } catch (error) { nativeOwnerFailStop(error); }
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
    const config = nativeOwnerConfig();
    if (!config || config.sessionFile !== sessionFile) refused("Main requires its explicit pinned native identity config");
    // Provisioning creates the original fence. A pinned Main must not silently
    // replace a missing one and thereby adopt a different open description.
    if (!fs.lstatSync(`${sessionFile}.main.lock`, { throwIfNoEntry: false })) refused("configured native Main fence is missing");
    const fd = takeNativeOwnerSessionFence(sessionFile!);
    // Intentionally NO finally/close, including on hook/delegation failure. The
    // kernel, after ALL SDK exit handlers, is the Main's only release boundary.
    assertNativeOwnerSessionFence(sessionFile!, fd);
    guardNativeOwnerSessionIo(sessionFile!);
    assertSessionFile(sessionFile!);
    await options.onMainFence?.(sessionFile!, fd);
    assertNativeOwnerSessionFence(sessionFile!, fd);
    assertSessionFile(sessionFile!);
  }
  // A shared compiled gate may live under dist/chunks rather than dist/worker.
  // Both are siblings: retain the stable package-local executable path.
  const taskEntry = fileURLToPath(new URL("../worker/task-entry.js", import.meta.url));
  process.argv = [process.execPath, taskEntry, sdkDirectory!, scale!, ...args];
  await import("./task-entry.js");
}

if (process.argv[1] && new URL("../worker/native-owner-entry.js", import.meta.url).href === pathToFileURL(fs.realpathSync(process.argv[1])).href) {
  process.umask(0o077);
  // Do not keep this module's evaluation pending while task-entry imports the
  // shared gate: task-entry is the delegated runtime, not a second CLI entry.
  void runNativeOwnerEntry().catch(error => { console.error(error); process.exitCode = 1; });
}
