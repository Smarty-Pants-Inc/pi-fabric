import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

const CGROUP_ROOT = "/sys/fs/cgroup";
const PREFIX = "pi-fabric-scratch-";
const CGROUP2_MAGIC = 0x63677270;
export interface ProcessScratchScope { directory: string; dev: number; ino: number; bootId: string }
export interface ScopedScratchLaunch extends ProcessScratchScope { joinedFile: string; launchNonce: string }
const bootId = (): string => fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();

/** Kernel, not PID/session census. No uid-foreign or replaceable cgroup path. */
const scopeStat = (directory: string): fs.Stats => {
  if (process.platform !== "linux" || !directory.startsWith(CGROUP_ROOT + "/") || path.resolve(directory) !== directory) throw new Error("Unproved cgroup path");
  if (fs.statfsSync(CGROUP_ROOT).type !== CGROUP2_MAGIC) throw new Error("Not a cgroup-v2 mount");
  let result!: fs.Stats;
  for (let current = CGROUP_ROOT; ; ) {
    result = fs.lstatSync(current);
    if (!result.isDirectory() || result.isSymbolicLink() || (result.uid !== 0 && result.uid !== process.getuid!()) || (result.mode & 0o022) !== 0) throw new Error("Unsafe cgroup custody");
    if (current === directory) break;
    const component = directory.slice(current.length + 1).split("/")[0]!;
    current = path.join(current, component);
  }
  return result;
};

/** Optional delegated Linux cgroup. Other hosts keep the unresolved fence. No
 * session-start work, systemd invocation, privilege elevation or PID signalling. */
export const createProcessScratchScope = (): ProcessScratchScope | undefined => {
  if (process.platform !== "linux") return;
  // Runtime/library preloads can fork before the self-attachment gate. Preserve
  // inherited behavior, but do not advertise complete custody for those launches.
  if (["NODE_OPTIONS", "BUN_OPTIONS", "LD_PRELOAD", "LD_AUDIT"].some(key => process.env[key]?.trim())) return;
  let directory: string | undefined;
  try {
    const membership = fs.readFileSync("/proc/self/cgroup", "utf8").trim().match(/^0::(\/[^\n]*)$/)?.[1];
    if (!membership || membership.includes("..")) return;
    const parent = path.join(CGROUP_ROOT, membership);
    scopeStat(parent);
    directory = path.join(parent, PREFIX + randomUUID());
    fs.mkdirSync(directory, { mode: 0o700 });
    fs.chmodSync(directory, 0o700);
    const stat = scopeStat(directory);
    fs.accessSync(path.join(directory, "cgroup.procs"), fs.constants.W_OK);
    return { directory, dev: stat.dev, ino: stat.ino, bootId: bootId() };
  } catch {
    if (directory) { try { fs.rmdirSync(directory); } catch { /* never remove a populated scope */ } }
    return;
  }
};

export const checkedProcessScratchScope = (value: unknown): ProcessScratchScope | undefined => {
  try {
    const scope = value as ProcessScratchScope;
    if (!scope || typeof scope.directory !== "string" || !/^pi-fabric-scratch-[0-9a-f-]{36}$/.test(path.basename(scope.directory)) ||
        !Number.isSafeInteger(scope.dev) || !Number.isSafeInteger(scope.ino) || typeof scope.bootId !== "string" || scope.bootId !== bootId()) return;
    const stat = scopeStat(scope.directory);
    if (stat.uid !== process.getuid!() || stat.dev !== scope.dev || stat.ino !== scope.ino) return;
    return scope;
  } catch { return; }
};

/** rmdir is the kernel's atomic empty-scope receipt: populated cgroups cannot
 * be removed. A missing/replaced group, reboot, unreadable state or leftover
 * subgroup is NOT an exit receipt. Never infer death from a reused PID. */
export const removeEmptyProcessScratchScope = (scope: ProcessScratchScope): boolean => {
  try {
    if (!checkedProcessScratchScope(scope)) return false;
    if (!/^populated 0$/m.test(fs.readFileSync(path.join(scope.directory, "cgroup.events"), "utf8"))) return false;
    fs.rmdirSync(scope.directory);
    return true;
  } catch { return false; }
};

/** Fixed pre-import gate. Only runtime builtins execute before self-attachment;
 * the worker (and every ordinary fork/exec, including setsid/detached tools)
 * inherits the kernel scope. Deliberate same-uid cgroup migration is outside
 * the runner contract, just as deliberate tampering with its custody files is. */
export const scopedWorkerArguments = (scope: ScopedScratchLaunch, worker: string, args: string[], cwd: string): string[] => {
  const source = `import fs from "node:fs"; import { pathToFileURL } from "node:url";
const scope = ${JSON.stringify(scope)};
const stat = fs.lstatSync(scope.directory);
if (!stat.isDirectory() || stat.isSymbolicLink() || stat.dev !== scope.dev || stat.ino !== scope.ino || fs.readFileSync("/proc/sys/kernel/random/boot_id","utf8").trim() !== scope.bootId) throw new Error("Scratch scope identity changed");
fs.writeFileSync(scope.directory + "/cgroup.procs", String(process.pid));
if (fs.readFileSync("/proc/self/cgroup","utf8").trim() !== "0::" + scope.directory.slice(${CGROUP_ROOT.length})) throw new Error("Scratch scope attachment failed");
// The latest launch generation is not collectable until attachment happened.
// A private, atomic marker prevents an empty pre-attachment scope (or a
// delayed retry) from being mistaken for a complete exit receipt.
const temporaryReceipt = scope.joinedFile + "." + scope.launchNonce;
const fd = fs.openSync(temporaryReceipt,"wx",0o600);
try { fs.writeFileSync(fd,JSON.stringify(scope)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
fs.renameSync(temporaryReceipt,scope.joinedFile);
process.argv = [process.execPath, ${JSON.stringify(path.resolve(cwd, worker))}, ...${JSON.stringify(args)}];
await import(pathToFileURL(${JSON.stringify(path.resolve(cwd, worker))}).href);`;
  return ["--input-type=module", "--eval", source];
};
