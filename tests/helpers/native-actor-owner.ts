// N7 real Pi SDK Main + detached resident fixture. No provider, credentials, synthetic
// session header, FabricRuntimeState stand-in, or alternate Main implementation.
// Parent must build the candidate and supply exact installed SDK + Node paths.
import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import type { NativeActorBinding } from "../../src/actors/native-handler.js";
import { MeshStore } from "../../src/mesh/store.js";
import { residentRoot, type ResidentHostOwner } from "../../src/residency/protocol.js";
import { watchResidentChild } from "../../src/residency/child-lifetime.js";
import { launchLog, startTime, stopAllOwned } from "./owned-processes.js";
import { residentDrainTemp } from "./resident-drain-temp.js";
import { assertApprovedNativeSdk, isNativeSdkUnconfigured, NATIVE_SDK_RELEASE, NATIVE_SDK_SOURCE_PINS } from "./native-sdk-no-model.js";
import { testTempOwnerMarker, testTempProcessIdentity, testTempRunEnvironment } from "../../scripts/test-temp.js";

// TEST ONLY: libc write boundary on this owned Main. The approved SDK calls
// appendFileSync unchanged. Delegate a positive short write to the REAL syscall,
// record its fd/inode/count, then stop before returning so Node cannot finish it.
// No SDK patch/clone, generated session line, simulated crash, or alternate writer.
const startupShortWriteSource = String.raw`
#define _GNU_SOURCE
#include <dlfcn.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static int stopped;
ssize_t write(int fd, const void *data, size_t count) {
  ssize_t (*real_write)(int,const void*,size_t) = dlsym(RTLD_NEXT,"write");
  const char *target = getenv("FABRIC_NATIVE_STARTUP_SESSION");
  const char *receipt = getenv("FABRIC_NATIVE_STARTUP_RECEIPT");
  struct stat held, named;
  if (!stopped && target && receipt && count > 32 &&
      !memcmp(data,"{\"type\":\"thinking_level_change\"",31) &&
      !fstat(fd,&held) && !stat(target,&named) && held.st_dev == named.st_dev && held.st_ino == named.st_ino) {
    stopped = 1;
    size_t partial = count / 2;
    ssize_t result = real_write(fd,data,partial);
    if (result <= 0 || (size_t)result >= count) _exit(97);
    struct stat after; if (fstat(fd,&after)) _exit(96);
    char out[1024];
    int length = snprintf(out,sizeof(out),"{\"event\":\"real-libc-write-short-return\",\"pid\":%d,\"fd\":%d,\"dev\":%llu,\"ino\":%llu,\"beforeSize\":%lld,\"afterSize\":%lld,\"requested\":%zu,\"delegated\":%zu,\"written\":%zd}\n",getpid(),fd,(unsigned long long)held.st_dev,(unsigned long long)held.st_ino,(long long)held.st_size,(long long)after.st_size,count,partial,result);
    int output = open(receipt,O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
    if (output < 0 || syscall(SYS_write,output,out,length) != length || fsync(output) || close(output)) _exit(95);
    raise(SIGSTOP);
    _exit(94); /* Never fabricate completion if erroneously resumed. */
  }
  return real_write(fd,data,count);
}
ssize_t writev(int fd, const struct iovec *vectors, int n) {
  if (n == 1) return write(fd,vectors[0].iov_base,vectors[0].iov_len);
  ssize_t (*real_writev)(int,const struct iovec*,int) = dlsym(RTLD_NEXT,"writev");
  return real_writev(fd,vectors,n);
}
`;
const fixtureRoot = fileURLToPath(new URL("../fixtures/", import.meta.url));
const json = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const digest = (file: string): string => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const absoluteFile = (file: string, label: string): string => {
  if (!file || !path.isAbsolute(file)) throw new Error(`${label} must be an explicit absolute generation-pinned path`);
  const real = fs.realpathSync(file);
  if (!fs.statSync(real).isFile()) throw new Error(`${label} is not a regular file`);
  return real;
};

// Reuse the launcher's existing live-owner protocol, NOT a cleanup exemption or
// an unverified promise to rescue traces later. Parent MUST archive this root and
// verify evidence-manifest.json before calling cleanupIsolatedTestTemp(). Workers
// never register their own removal for this live launcher's inherited root.
const nativeEvidenceCustody = () => {
  const root = process.env[testTempRunEnvironment];
  if (!root || !path.isAbsolute(root) || fs.realpathSync(root) !== root ||
      !/^pi-fabric-vitest-[A-Za-z0-9]{6}$/.test(path.basename(root))) {
    throw new Error("N7 BLOCKED: live launcherTestTemp custody required before creating native sessions; parent must archive before cleanup");
  }
  const directory = fs.lstatSync(root), marker = path.join(root, testTempOwnerMarker), stat = fs.lstatSync(marker);
  if (!directory.isDirectory() || directory.isSymbolicLink() || directory.uid !== process.getuid!() || (directory.mode & 0o777) !== 0o700 ||
      !stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || stat.uid !== process.getuid!() ||
      (stat.mode & 0o777) !== 0o600 || stat.size > 4096) throw new Error("N7 BLOCKED: unsafe launcher trace custody");
  const owner = json<{ version: number; pid: number; startIdentity: string }>(marker);
  if (owner.version !== 1 || !Number.isSafeInteger(owner.pid) || owner.pid <= 1 || owner.pid === process.pid ||
      typeof owner.startIdentity !== "string" || !owner.startIdentity || testTempProcessIdentity(owner.pid) !== owner.startIdentity) {
    throw new Error("N7 BLOCKED: no live external launcher owns native trace retention");
  }
  return { root, version: owner.version, pid: owner.pid, startIdentity: owner.startIdentity };
};

/** File commits drive the normal path; kernel-only transitions may opt into a bounded safety net. */
export const observeNativeOwner = (root: string) => {
  const listeners = new Set<() => void>(), cancellations = new Set<() => void>();
  let watchFailure: Error | undefined, closed = false;
  const wake = (): void => { for (const listener of [...listeners]) listener(); };
  let watcher: fs.FSWatcher | undefined, rearm: ReturnType<typeof setImmediate> | undefined;
  let pendingClose: { watcher: fs.FSWatcher; handle: ReturnType<typeof setImmediate> } | undefined;
  const cancelClose = (): void => {
    if (pendingClose) clearImmediate(pendingClose.handle);
    pendingClose = undefined;
  };
  let attempts = 0;
  const transient = (error: unknown): boolean => {
    const code = (error as NodeJS.ErrnoException | undefined)?.code;
    return code === "ENOENT" || code === "ENOTDIR";
  };
  const fail = (error: unknown): void => {
    watchFailure ??= error instanceof Error ? error : new Error(String(error));
    if (rearm) clearImmediate(rearm); rearm = undefined;
    cancelClose();
    const previous = watcher; watcher = undefined; previous?.close(); wake();
  };
  const retry = (): void => {
    if (closed || watchFailure || rearm) return;
    rearm = setImmediate(() => { rearm = undefined; arm(); });
  };
  const arm = (): void => {
    if (closed || watchFailure || watcher) return;
    // Bound a recovery burst, including synchronous attachment races. Only a
    // real file event resets it; a vanished root must never cause an idle loop.
    if (++attempts > 8) { fail(new Error("Native owner watcher could not re-arm after transient errors")); return; }
    try {
      const current: fs.FSWatcher = fs.watch(root, { recursive: true, persistent: false }, () => {
        if (closed || watcher !== current) return;
        attempts = 0; wake();
      });
      watcher = current;
      current.on("error", error => {
        if (closed) return;
        // Fatal errors remain sticky even if delivered by a retired watcher.
        if (!transient(error)) { fail(error); return; }
        if (pendingClose?.watcher === current) {
          cancelClose(); retry();
        } else if (watcher === current) {
          watcher = undefined; current.close(); retry();
        }
        wake();
      });
      current.on("close", () => {
        if (closed || watcher !== current) return;
        watcher = undefined;
        // Pair a same-turn transient with this retired handle before failing.
        pendingClose = { watcher: current, handle: setImmediate(() => {
          if (closed || watchFailure || pendingClose?.watcher !== current) return;
          pendingClose = undefined;
          fail(new Error("Native owner watcher closed unexpectedly"));
        }) };
      });
      // close() can be requested while an attachment is in progress.
      if (closed || watchFailure) { watcher = undefined; current.close(); }
      else wake(); // Read after subscribing, covering commits during replacement.
    } catch (error) {
      if (!transient(error)) { fail(error); return; }
      try {
        if (!fs.statSync(root).isDirectory()) { fail(new Error("Native owner watch root is not a directory")); return; }
      } catch (rootError) { fail(rootError); return; }
      retry(); wake();
    }
  };
  arm();
  return {
    wake,
    close: (): void => {
      closed = true;
      if (rearm) clearImmediate(rearm); rearm = undefined;
      cancelClose();
      const previous = watcher; watcher = undefined; previous?.close();
      for (const cancel of [...cancellations]) cancel();
    },
    until: (predicate: () => boolean | Promise<boolean>, label: string, ms = 60_000,
      safety?: { pollMs: number }): Promise<void> => new Promise((resolve, reject) => {
      if (closed) { reject(new Error("Native owner observation closed")); return; }
      if (safety && (!Number.isFinite(safety.pollMs) || safety.pollMs <= 0)) {
        reject(new Error("Native owner safety poll must be positive")); return;
      }
      let finished = false, checking = false, dirty = false, expired = false;
      let poll: ReturnType<typeof setInterval> | undefined;
      const deadlineError = new Error(`Native owner observation deadline: ${label}`);
      const finish = (error?: unknown): void => {
        if (finished) return;
        finished = true; clearTimeout(timer); if (poll) clearInterval(poll);
        listeners.delete(check); cancellations.delete(cancel);
        if (error) reject(error); else resolve();
      };
      const cancel = (): void => finish(new Error("Native owner observation closed"));
      const check = (): void => {
        if (finished) return;
        if (checking) { dirty = true; return; }
        checking = true;
        void (async () => {
          do {
            dirty = false;
            const final = expired;
            if (watchFailure) throw watchFailure;
            const matched = await predicate();
            if (watchFailure) throw watchFailure;
            if (matched && !pendingClose) { finish(); return; }
            if (final) { finish(deadlineError); return; }
          } while (dirty && !finished);
        })().catch(finish).finally(() => { checking = false; if (dirty && !finished) check(); });
      };
      // Never reset the original deadline. Only opted-in kernel observations
      // serialize one final predicate behind any in-flight check at expiry.
      const timer = setTimeout(() => {
        if (!safety) { finish(deadlineError); return; }
        expired = true; if (poll) clearInterval(poll); check();
      }, ms);
      if (safety) poll = setInterval(check, safety.pollMs);
      listeners.add(check); cancellations.add(cancel); check();
    }),
  };
};

export interface NativeActorOwnerOptions {
  readonly candidateRoot: string;
  readonly sdkDirectory: string;
  readonly nodeBinary: string;
  readonly actorId: string;
  readonly residentIdleExitMs?: number;
  readonly module?: string;
  /** New actual-entry cases install compiled bytes, never source/fixture aliases. */
  readonly installCandidate?: boolean;
  readonly mainLaunch?: "adapter" | "direct" | "launcher";
}
interface NativeAuxiliaryResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  pid: number | undefined;
  stdout: string;
  stderr: string;
  started?: string | undefined;
  launcherPid?: number;
  launcherCode?: number | null;
  processIds?: number[];
}
export type NativeOwnerCliShape = "continue-native" | "short-continue-native" | "resume-native" | "long-resume-native" |
  "short-unknown-native" |
  "duplicate-native" | "relative-native" | "dotdot-native" | "symlink-native" | "unknown-native" |
  "inline-session-native" | "ordinary-continue" | "ordinary-resume" | "duplicate-ordinary" |
  "unknown-ordinary" | "ordinary-relative" | "ordinary-symlink" | "ordinary-flag-value" | "name-native" |
  "task-prefix-ordinary" | "cli-prefix-ordinary" | "task-exact-native-id";
interface NativeSessionPin { sessionFile: string; rootId: string; sessionId: string }
interface NativeOwnerDeadline { startedNs: bigint; boundMs: number }
const remainingNativeOwnerMs = (deadline: NativeOwnerDeadline): number => {
  const remaining = deadline.boundMs - Number(process.hrtime.bigint() - deadline.startedNs) / 1_000_000;
  if (!Number.isFinite(remaining) || remaining <= 0) throw new Error("Actual Main EOF/death/product-fence verification exceeded its signal-relative bound");
  return remaining;
};
interface RpcRecord { type?: string; id?: string; success?: boolean; error?: string; data?: Record<string, unknown>; [key: string]: unknown }

export const createNativeActorOwner = async (options: NativeActorOwnerOptions) => {
  if (process.platform !== "linux") throw new Error("N7 native residency fixture requires the Linux kernel OWNER fence");
  if (![options.candidateRoot, options.sdkDirectory].every(file => path.isAbsolute(file))) {
    throw new Error("N7 candidate and installed SDK require explicit absolute generation paths");
  }
  const custody = nativeEvidenceCustody();
  const sourceCandidateRoot = fs.realpathSync(options.candidateRoot);
  const sdkDirectory = fs.realpathSync(options.sdkDirectory);
  assertApprovedNativeSdk(sdkDirectory);
  const nodeBinary = absoluteFile(options.nodeBinary, "Node runtime");
  absoluteFile(path.join(sourceCandidateRoot, "dist/worker/native-owner-entry.js"), "Built PRODUCT native owner entry");
  absoluteFile(path.join(sdkDirectory, "index.js"), "Pinned Pi SDK");
  absoluteFile(path.join(sdkDirectory, "core/session-manager.js"), "Pinned native session manager");
  const privateTemp = residentDrainTemp("native-actor-owner-");
  const { root } = privateTemp;
  const relativeRoot = path.relative(custody.root, root);
  if (relativeRoot === ".." || relativeRoot.startsWith(`..${path.sep}`) || path.isAbsolute(relativeRoot)) {
    throw new Error("N7 BLOCKED: native session root escaped live launcher custody");
  }
  let candidateRoot = sourceCandidateRoot;
  if (options.installCandidate) {
    candidateRoot = path.join(root, "candidate-install");
    fs.mkdirSync(candidateRoot, { mode: 0o700 });
    const installed: { path: string; sha256: string; bytes: number }[] = [];
    const copy = (relative: string): void => {
      const source = path.join(sourceCandidateRoot, relative), target = path.join(candidateRoot, relative), stat = fs.lstatSync(source);
      if (stat.isSymbolicLink() || stat.uid !== process.geteuid!() || (stat.mode & 0o022) !== 0) throw new Error(`Unsafe candidate install input: ${relative}`);
      if (stat.isDirectory()) {
        fs.mkdirSync(target, { mode: 0o700 });
        for (const name of fs.readdirSync(source).sort()) copy(path.join(relative, name));
      } else if (stat.isFile()) {
        fs.copyFileSync(source, target, fs.constants.COPYFILE_EXCL); fs.chmodSync(target, stat.mode & 0o755);
        if (fs.realpathSync(target) !== target || digest(source) !== digest(target)) throw new Error(`Candidate install readback mismatch: ${relative}`);
        installed.push({ path: relative, sha256: digest(target), bytes: fs.statSync(target).size });
      } else throw new Error(`Nonregular candidate install input: ${relative}`);
    };
    copy("dist"); copy("package.json");
    fs.mkdirSync(path.join(candidateRoot, "config"), { mode: 0o700 });
    copy("config/landlock-roles.json");
    // Only third-party dependency resolution is shared. ALL executable product
    // dist/chunks are actual copied compiled files under the owned private prefix.
    fs.symlinkSync(path.join(sourceCandidateRoot, "node_modules"), path.join(candidateRoot, "node_modules"), "dir");
    fs.writeFileSync(path.join(root, "candidate-install.json"), JSON.stringify({ version: 1, sourceCandidateRoot, candidateRoot,
      dependencyDirectory: fs.realpathSync(path.join(candidateRoot, "node_modules")), files: installed }, null, 2) + "\n", { mode: 0o600, flag: "wx" });
  }
  const taskEntry = absoluteFile(path.join(candidateRoot, "dist/worker/task-entry.js"), "Built candidate SDK/RPC entry");
  const nativeOwnerEntry = absoluteFile(path.join(candidateRoot, "dist/worker/native-owner-entry.js"), "Compiled PRODUCT native owner entry");
  const nativeApi = absoluteFile(path.join(candidateRoot, "dist/native-actors.js"), "Built candidate native registration API");
  absoluteFile(path.join(candidateRoot, "dist/index.js"), "Built candidate extension");
  absoluteFile(path.join(candidateRoot, "dist/residency/pi-entry.js"), "Built candidate resident entry");
  fs.writeFileSync(path.join(root, "retention-custody.json"), JSON.stringify({ ...custody,
    archiveRequiredBeforeCleanup: true, rawSessionDeletionAuthorized: false }), { mode: 0o600 });
  const retainManifest = (): void => {
    const live = nativeEvidenceCustody();
    if (live.pid !== custody.pid || live.startIdentity !== custody.startIdentity) throw new Error("Native trace custodian changed");
    const files: { path: string; bytes: number; sha256: string }[] = [];
    const special: { path: string; kind: string; target?: string }[] = [];
    const visit = (directory: string): void => {
      for (const name of fs.readdirSync(directory).sort()) {
        const file = path.join(directory, name), relative = path.relative(root, file);
        if (relative === "evidence-manifest.json" || relative === "evidence-manifest.sha256") continue;
        const stat = fs.lstatSync(file);
        if (stat.isDirectory()) visit(file);
        else if (stat.isFile()) files.push({ path: relative, bytes: stat.size, sha256: digest(file) });
        else special.push({ path: relative, kind: stat.isSymbolicLink() ? "symlink" : "special",
          ...(stat.isSymbolicLink() ? { target: fs.readlinkSync(file) } : {}) });
      }
    };
    visit(root);
    const manifest = path.join(root, "evidence-manifest.json");
    fs.writeFileSync(manifest, JSON.stringify({ version: 1, root, custody, sdkRelease: NATIVE_SDK_RELEASE,
      sourcePins: NATIVE_SDK_SOURCE_PINS, files, special }, null, 2) + "\n", { mode: 0o600, flag: "wx" });
    for (const file of files) {
      if (fs.statSync(path.join(root, file.path)).size !== file.bytes || digest(path.join(root, file.path)) !== file.sha256) {
        throw new Error(`Native retained evidence changed before archive: ${file.path}`);
      }
    }
    fs.writeFileSync(path.join(root, "evidence-manifest.sha256"), `${digest(manifest)}  evidence-manifest.json\n`, { mode: 0o600, flag: "wx" });
    for (const file of [manifest, path.join(root, "evidence-manifest.sha256")]) {
      const fd = fs.openSync(file, "r"); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    }
    const fd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  };
  const owned = launchLog(root);
  const safety = privateTemp.childEnvironment(owned.env.NODE_OPTIONS!);
  const observations = observeNativeOwner(root);
  const agentDir = path.join(root, "profile"), cwd = path.join(root, "service"), sessions = path.join(root, "sessions");
  for (const directory of [agentDir, cwd, sessions]) fs.mkdirSync(directory, { mode: 0o700 });
  const meshRoot = path.join(root, "mesh"), ledgerDir = path.join(root, "ledger");
  const pinsPath = path.join(root, "pins.json");
  const sessionFile = path.join(sessions, "owner.jsonl"), sessionPin = path.join(root, "session-pin.json");
  const adapter = path.join(fixtureRoot, "native-owner-argv.mjs");
  // A positive allow-list, not {...process.env}. No API keys, parent/task IDs,
  // fleet profile, credential wrapper, model config, or discovered extension is inherited.
  const env: NodeJS.ProcessEnv = Object.fromEntries(["PATH", "LANG", "LC_ALL", "CI", "GITHUB_ACTIONS",
    "RUNNER_ENVIRONMENT", "RUNNER_TEMP", "PI_TEST_FLEET_HOST"].flatMap(key =>
    process.env[key] === undefined ? [] : [[key, process.env[key]]]));
  Object.assign(env, owned.env, safety, {
    PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir, PI_CODING_AGENT_SESSION_DIR: sessions,
    PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: cwd,
    PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
    PI_FABRIC_PI_BINARY: adapter, PI_FABRIC_NODE_BINARY: nodeBinary, FABRIC_NATIVE_OWNER_PINS: pinsPath,
    // Actual product selector, not the fixture's generation/observer envelope.
    // The existing SDK-written identity tuple selects a gate, never lock proof.
    PI_FABRIC_NATIVE_OWNER_CONFIG: sessionPin,
    FABRIC_NATIVE_TEST_SDK_DIRECTORY: sdkDirectory, FABRIC_NATIVE_TEST_SDK_IMPORTS: path.join(root, "sdk-imports.jsonl"),
  });
  // Native loader observes and delegates unchanged; never substitutes an SDK.
  env.NODE_OPTIONS = `${env.NODE_OPTIONS ?? ""} --import=${new URL("../fixtures/native-owner-launcher.mjs?observe-sdk", import.meta.url).href}`;
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false,
    // Empty auth alone does not exclude credential-free/native provider defaults.
    // This supported SDK request policy denies ALL models, without a dummy provider.
    modelPolicy: { allow: [] },
    compaction: { enabled: false }, retry: { enabled: false }, packages: [], extensions: [] }), { mode: 0o600 });
  // Pinned Pi's normal saved-trust format (core/trust-manager.js): trust ONLY
  // this canonical owned service cwd in this private profile, not any ancestor.
  // Leave defaultProjectTrust at "ask"; read actual ctx trust in the observer.
  fs.writeFileSync(path.join(agentDir, "trust.json"), JSON.stringify({ [fs.realpathSync(cwd)]: true }), { mode: 0o600, flag: "wx" });
  // The isolated profile starts with no auth/models files at all. Nothing is copied
  // from Main's profile; any auth store subsequently created is the SDK's own empty store.
  const idleExitMs = options.residentIdleExitMs ?? 1_000;
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
    // This dedicated service Main must activate normally before any actor row exists.
    mesh: { enabled: true, announce: true, transport: "file", hostTransport: "file", actorScope: "session", actorPollMs: 20,
      residentIdleExitMs: idleExitMs, stateBackend: "file" },
    agents: { notifyOnComplete: false, budgetUsd: 0, sessionExport: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false },
  }), { mode: 0o600 });

  let auxiliarySequence = 0;
  const runAuxiliary = (label: string, args: string[], deadline?: NativeOwnerDeadline, executable = nodeBinary, environment: NodeJS.ProcessEnv = env): Promise<NativeAuxiliaryResult> => new Promise((resolve, reject) => {
    if (deadline) remainingNativeOwnerMs(deadline); // Refuse even to spawn after the original signal-relative deadline.
    const name = `${label}-${++auxiliarySequence}`;
    const child = spawn(executable, args, { cwd, env: environment, stdio: ["ignore", "pipe", "pipe"] });
    const started = child.pid ? startTime(child.pid) : undefined;
    const lifetime = watchResidentChild(child);
    let stdout = "", stderr = "", expired = false;
    child.stdout.on("data", chunk => { fs.appendFileSync(path.join(root, `${name}.stdout`), chunk, { mode: 0o600 }); stdout = `${stdout}${chunk}`.slice(-128_000); });
    child.stderr.on("data", chunk => { fs.appendFileSync(path.join(root, `${name}.stderr`), chunk, { mode: 0o600 }); stderr = `${stderr}${chunk}`.slice(-32_000); });
    child.once("error", reject);
    const timer = setTimeout(() => { expired = true; void lifetime.stop().catch(reject); }, deadline
      ? Math.max(0, deadline.boundMs - Number(process.hrtime.bigint() - deadline.startedNs) / 1_000_000) : 30_000);
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      // A delayed timer or deadline cleanup can NEVER turn a late verifier into success.
      if (deadline && Number(process.hrtime.bigint() - deadline.startedNs) / 1_000_000 >= deadline.boundMs) expired = true;
      fs.writeFileSync(path.join(root, `${name}.exit.json`), JSON.stringify({ pid: child.pid, started, executable, args, code, signal, expired,
        stdoutClosed: child.stdout.destroyed, stderrClosed: child.stderr.destroyed }), { mode: 0o600 });
      if (expired) reject(new Error(`Native auxiliary ${label} did not terminate: ${stderr}`));
      else resolve({ code, signal, pid: child.pid, started, stdout, stderr });
    });
  });
  const sessionArgs = (mode: "provision" | "verify") => [path.join(fixtureRoot, "native-owner-session.mjs"), mode, sdkDirectory, sessionFile, cwd, sessionPin,
    nativeOwnerEntry, digest(nativeOwnerEntry)];
  const provision = async (): Promise<NativeSessionPin> => {
    const result = await runAuxiliary("session-provision", sessionArgs("provision"));
    if (result.code !== 0) throw new Error(`Native session provision refused: ${result.stderr}`);
    return JSON.parse(result.stdout) as NativeSessionPin;
  };

  let pin: NativeSessionPin;
  try { pin = await provision(); }
  catch (error) {
    let terminal = false;
    try { await stopAllOwned(owned.owned()); terminal = true; }
    finally { observations.close(); privateTemp.assertNoOutsideDeletion(); if (terminal) retainManifest(); }
    throw error;
  }
  const binding: NativeActorBinding = Object.freeze({ actorId: options.actorId, rootId: pin.rootId, scope: "session",
    name: "native-residency", handlerKey: "native-residency.test.v1",
    residentModule: options.module ?? path.join(fixtureRoot, "native-resident-handler.mjs"),
    ingress: Object.freeze({ link: "native-residency", ledgerDir }) });
  const callbacks = path.join(root, "callbacks.jsonl"), observerFile = path.join(root, "observations.jsonl");
  const pins = { nodeBinary, sdkDirectory, taskEntry, taskEntrySha256: digest(taskEntry),
    nativeOwnerEntry, nativeOwnerEntrySha256: digest(nativeOwnerEntry), sdkIndexSha256: digest(path.join(sdkDirectory, "index.js")),
    nativeApi, binding, sessionFile, sessionPin, meshRoot, observer: path.join(fixtureRoot, "native-owner-observer.mjs"), callbacks, observations: observerFile };
  fs.writeFileSync(pinsPath, JSON.stringify(pins), { mode: 0o600 });
  fs.writeFileSync(path.join(root, "generation-evidence.json"), JSON.stringify({ candidateRoot, nodeBinary, nodeBinarySha256: digest(nodeBinary),
    sdkDirectory, taskEntrySha256: pins.taskEntrySha256, nativeOwnerEntry, nativeOwnerEntrySha256: pins.nativeOwnerEntrySha256, sdkIndexSha256: pins.sdkIndexSha256,
    sdkRelease: NATIVE_SDK_RELEASE, approvedSdkSources: NATIVE_SDK_SOURCE_PINS,
    adapterSha256: digest(adapter), nativeApiSha256: digest(nativeApi), binding }), { mode: 0o600 });
  const mesh = new MeshStore(meshRoot, 256 * 1024, 1_000);
  const residencyRoot = residentRoot(meshRoot, pin.rootId);
  let main: ChildProcessWithoutNullStreams | undefined;
  let mainClosed: Promise<void> | undefined;
  let mainClosedNs: bigint | undefined;
  let launcherExited: Promise<void> | undefined;
  let mainIdentity: { pid: number; started: string } | undefined;
  let mainPaused = false;
  let startupCleanupFailure: Error | undefined;
  // Positive Linux death evidence, not launcher exit/EOF/kill acknowledgement.
  const death = (identity: { pid: number; started: string }): "absent" | "reused" | "zombie" | undefined => {
    let stat: string;
    try { stat = fs.readFileSync(`/proc/${identity.pid}/stat`, "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return "absent"; throw error; }
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    if (!/^\d+$/.test(fields[19] ?? "")) throw new Error("Unknown owned Main birth at death boundary");
    if (fields[19] !== identity.started) return "reused";
    return fields[0] === "Z" || fields[0] === "X" ? "zombie" : undefined;
  };
  const recordDeath = (generation: number, identity: { pid: number; started: string }): bigint => {
    const evidence = death(identity);
    if (!evidence) throw new Error("Main still live; pipe/launcher close is not death proof");
    const observedNs = process.hrtime.bigint();
    fs.writeFileSync(path.join(root, `main-${generation}.death.json`), JSON.stringify({ ...identity, evidence }), { mode: 0o600, flag: "wx" });
    return observedNs;
  };
  // Failed startup must finish its own cleanup, not outlive an afterEach timeout.
  // Only startup direct Mains are detached; all other launch/death bounds stay unchanged.
  const cleanupFailedStartup = async (generation: number): Promise<void> => {
    const child = main, identity = mainIdentity, closed = mainClosed;
    const signals: NodeJS.Signals[] = [];
    let childClosed = false, members: number[] | undefined, evidence: ReturnType<typeof death>;
    const fields = (pid: number): string[] => {
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
      const value = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
      if (value.length < 20 || !/^\d+$/.test(value[19]!)) throw new Error("Unknown startup cleanup process identity");
      return value;
    };
    const groupMembers = (pgid: number): number[] => fs.readdirSync("/proc").filter(name => /^\d+$/.test(name)).flatMap(name => {
      try { return Number(fields(Number(name))[2]) === pgid ? [Number(name)] : []; }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
    });
    const receipt = (confirmed: boolean, error?: unknown): void => {
      fs.writeFileSync(path.join(root, `startup-${generation}.cleanup.json`), JSON.stringify({ kind: "TEST_CLEANUP",
        phase: "failed-startup", identity, pgid: identity?.pid, signals, childClosed, death: evidence ?? "UNCONFIRMED",
        groupMembers: members ?? "UNCONFIRMED", stdoutClosed: child?.stdout.destroyed ?? false,
        stderrClosed: child?.stderr.destroyed ?? false, confirmed, ...(error ? { error: String(error) } : {}) }), { mode: 0o600, flag: "wx" });
    };
    try {
      if (!child || !identity || !closed || options.mainLaunch !== "direct" || child.pid !== identity.pid ||
          identity.pid <= 1 || identity.pid === process.pid || identity.pid === process.ppid) {
        throw new Error("No exact direct startup Main custody; group cleanup refused");
      }
      evidence = death(identity);
      if (!evidence) {
        const recorded = owned.owned().some(value => value.pid === identity.pid && value.started === identity.started && value.argv[0] === nativeOwnerEntry);
        const selfStat = fields(process.pid), parentStat = fields(process.ppid), mainStat = fields(identity.pid);
        if (!recorded || child.exitCode !== null || child.signalCode !== null || mainStat[19] !== identity.started ||
            Number(mainStat[1]) !== process.pid || Number(mainStat[2]) !== identity.pid || Number(mainStat[3]) !== identity.pid ||
            Number(selfStat[2]) === identity.pid || Number(parentStat[2]) === identity.pid) {
          throw new Error("Startup Main birth/direct-child/exclusive-group proof failed; group cleanup refused");
        }
        // No await between validation and BOTH signals. This direct child cannot
        // be reaped/reused by the event loop between SIGCONT and SIGKILL.
        process.kill(-identity.pid, "SIGCONT"); signals.push("SIGCONT");
        process.kill(-identity.pid, "SIGKILL"); signals.push("SIGKILL");
      }
      let closeError: unknown;
      void closed.then(() => { childClosed = true; observations.wake(); }, error => { closeError = error; observations.wake(); });
      await observations.until(() => {
        if (closeError) throw closeError;
        evidence = death(identity); members = groupMembers(identity.pid);
        return childClosed && child.stdout.destroyed && child.stderr.destroyed && !!evidence && members.length === 0;
      }, "failed-startup actual child close/death/exclusive group empty", 10_000, { pollMs: 25 });
      const retainedDeath = path.join(root, `main-${generation}.death.json`);
      if (fs.existsSync(retainedDeath)) {
        const retained = json<{ pid: number; started: string; evidence: string }>(retainedDeath);
        if (retained.pid !== identity.pid || retained.started !== identity.started || !["absent", "reused", "zombie"].includes(retained.evidence)) {
          throw new Error("Retained Main death evidence does not match startup custody");
        }
      } else recordDeath(generation, identity);
      receipt(true);
      main = undefined; mainIdentity = undefined; mainPaused = false;
    } catch (error) {
      receipt(false, error);
      throw error;
    }
  };
  let lastError = "";
  const pending = new Map<string, { resolve: (value: RpcRecord) => void; reject: (error: unknown) => void }>();
  const rpcEvents: RpcRecord[] = [];
  let sequence = 0, mainGeneration = 0;
  const mainArgs = () => [...(options.mainLaunch && options.mainLaunch !== "adapter"
    ? [nativeOwnerEntry, sdkDirectory, "1", "--extension", pins.observer] : [adapter]),
    "--mode", "rpc", "--session", sessionFile,
    "--no-tools", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files",
    "--extension", path.join(fixtureRoot, "native-owner-bootstrap.mjs"), "--extension", path.join(candidateRoot, "dist/index.js")];
  const request = (type: "get_state" | "get_messages" | "prompt" | "steer" | "follow_up", message?: string): Promise<RpcRecord> => {
    if (!main || !mainIdentity || death(mainIdentity) || main.stdin.destroyed) return Promise.reject(new Error("Actual owner Main is not running"));
    const id = `native-owner-${++sequence}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`Owner RPC ${type} deadline: ${lastError}`)); }, 60_000);
      pending.set(id, { resolve: value => { clearTimeout(timer); resolve(value); }, reject: error => { clearTimeout(timer); reject(error); } });
      main!.stdin.write(JSON.stringify({ id, type, ...(message !== undefined ? { message } : {}) }) + "\n", error => {
        if (error) { pending.get(id)?.reject(error); pending.delete(id); }
      });
    });
  };
  const start = async (startup?: { barrier: "pre-ready" | "sdk-append" }): Promise<void> => {
    if (main) throw new Error("Native owner Main already running or awaiting terminal join");
    // No parent verify/provision here: the actual Main takes its nonblocking
    // fence FIRST, then verifies the pinned file before SDK/task-entry can open it.
    const generation = ++mainGeneration;
    const launcherReceipt = path.join(root, `launcher-${generation}.custody.json`);
    const args = options.mainLaunch === "launcher"
      ? [path.join(fixtureRoot, "native-owner-launcher.mjs"), pinsPath, launcherReceipt, "--", ...mainArgs()] : mainArgs();
    let launchEnv = env;
    if (startup) {
      if (options.mainLaunch !== "direct") throw new Error("Startup crash requires the actual direct product Main");
      const receipt = path.join(root, `startup-${generation}.barrier.json`);
      launchEnv = { ...env, FABRIC_NATIVE_STARTUP_PHASE: startup.barrier, FABRIC_NATIVE_STARTUP_SESSION: sessionFile,
        FABRIC_NATIVE_STARTUP_RECEIPT: receipt,
        NODE_OPTIONS: `${env.NODE_OPTIONS ?? ""} --import=${new URL("../fixtures/native-owner-observer.mjs?startup-io", import.meta.url).href}` };
      if (startup.barrier === "sdk-append") {
        const source = path.join(root, `startup-${generation}.c`), library = path.join(root, `startup-${generation}.so`);
        fs.writeFileSync(source, startupShortWriteSource, { mode: 0o600, flag: "wx" });
        const result = await runAuxiliary("startup-interposer-compile", ["-shared", "-fPIC", "-Wall", "-Werror", "-o", library, source, "-ldl"], undefined, "/usr/bin/cc");
        fs.writeFileSync(path.join(root, `startup-${generation}.compiler.json`), JSON.stringify({ ...result,
          compiler: fs.realpathSync("/usr/bin/cc"), compilerSha256: digest("/usr/bin/cc"), sourceSha256: digest(source),
          librarySha256: result.code === 0 ? digest(library) : null, approval: "UNPINNED host compiler; test barrier only" }), { mode: 0o600, flag: "wx" });
        if (result.code !== 0 || result.signal) throw new Error(`Cannot compile actual startup short-write barrier: ${result.stderr}`);
        launchEnv.LD_PRELOAD = library;
      }
    }
    const child = spawn(nodeBinary, args, { cwd, env: launchEnv, detached: !!startup, stdio: ["pipe", "pipe", "pipe"] });
    main = child;
    if (options.mainLaunch !== "launcher") {
      if (!child.pid) throw new Error("Main spawn has no owned pid");
      const started = startTime(child.pid);
      if (!started) throw new Error("Main spawn has no native birth identity");
      mainIdentity = { pid: child.pid, started };
    }
    launcherExited = new Promise<void>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        if (options.mainLaunch === "launcher") fs.writeFileSync(path.join(root, `launcher-${generation}.exit.json`), JSON.stringify({ pid: child.pid, code, signal }), { mode: 0o600 });
        resolve();
      });
    });
    let buffer = "";
    mainClosedNs = undefined;
    mainClosed = new Promise<void>((resolve, reject) => {
      child.once("error", reject); child.once("close", () => { mainClosedNs = process.hrtime.bigint(); resolve(); });
    });
    child.on("error", error => { lastError = error.message; observations.wake(); });
    child.stderr.on("data", chunk => {
      fs.appendFileSync(path.join(root, `main-${generation}.stderr`), chunk, { mode: 0o600 });
      lastError = `${lastError}${chunk}`.slice(-12_000);
    });
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      fs.appendFileSync(path.join(root, `main-${generation}.stdout`), chunk, { mode: 0o600 });
      buffer += chunk;
      if (buffer.length > 1024 * 1024) { lastError = "Owner RPC output exceeds fixture bound"; child.stdin.end(); return; }
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline).replace(/\r$/, ""); buffer = buffer.slice(newline + 1);
        if (!line) continue;
        try {
          const value = JSON.parse(line) as RpcRecord;
          if (value.type === "response" && typeof value.id === "string") { pending.get(value.id)?.resolve(value); pending.delete(value.id); }
          else { rpcEvents.push(value); if (rpcEvents.length > 1_000) rpcEvents.shift(); }
        } catch { lastError = "Non-JSON owner RPC stdout"; }
      }
      observations.wake();
    });
    child.once("close", (code, signal) => {
      const receipt = JSON.stringify({ pid: child.pid, code, signal,
        stdoutClosed: child.stdout.destroyed, stderrClosed: child.stderr.destroyed });
      fs.writeFileSync(path.join(root, `${options.mainLaunch === "launcher" ? "launcher" : "main"}-${generation}.close.json`), receipt, { mode: 0o600 });
      if (options.mainLaunch !== "launcher") fs.writeFileSync(path.join(root, `main-${generation}.exit.json`), receipt, { mode: 0o600 });
      // Only this isolated fixture's bounded, non-secret lifecycle observations;
      // retain the actual SDK state that caused a fail-closed startup exit.
      const observed = fs.existsSync(observerFile) ? fs.readFileSync(observerFile, "utf8").slice(-12_000) : "no lifecycle observations";
      for (const waiter of pending.values()) waiter.reject(new Error(`Actual owner Main exited (${code}): ${lastError}\n${observed}`));
      pending.clear(); observations.wake();
    });
    if (options.mainLaunch === "launcher") {
      await observations.until(() => {
        if (!fs.existsSync(launcherReceipt)) {
          if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Test launcher exited before Main custody: ${lastError}`);
          return false;
        }
        const receipt = json<{ launcherPid: number; mainPid: number; mainStarted: string; entry: string; entrySha256: string; stdinCustody: string }>(launcherReceipt);
        if (receipt.stdinCustody !== "launcher-owned-pipe" || receipt.launcherPid !== child.pid || !Number.isSafeInteger(receipt.mainPid) || receipt.mainPid <= 1 ||
            receipt.mainPid === child.pid || receipt.entry !== nativeOwnerEntry || receipt.entrySha256 !== pins.nativeOwnerEntrySha256 ||
            startTime(receipt.mainPid) !== receipt.mainStarted) throw new Error("Test launcher has no matching real Main birth/custody");
        mainIdentity = { pid: receipt.mainPid, started: receipt.mainStarted };
        return true;
      }, "actual test launcher/Main birth receipt");
    }
    if (startup) {
      try {
        await observations.until(async () => {
          if (!mainIdentity || death(mainIdentity)) throw new Error(`Actual startup barrier exited: ${lastError}`);
          if (!fs.existsSync(path.join(root, `startup-${generation}.barrier.json`))) return false;
          const stat = await fs.promises.readFile(`/proc/${mainIdentity.pid}/stat`, "utf8");
          const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
          return fields[19] === mainIdentity.started && fields[0] === "T";
        }, `actual ${startup.barrier} Main stopped with kernel/byte evidence`, 60_000, { pollMs: 25 });
        mainPaused = true; // cleanup can resume this exact owned stopped child
        return; // NOT READY. No get_state, no model/user turn, no second Main.
      } catch (error) {
        try { await cleanupFailedStartup(generation); }
        catch (cleanupError) {
          startupCleanupFailure = cleanupError instanceof Error ? cleanupError : new Error(String(cleanupError));
          throw new AggregateError([error, cleanupError], "Actual startup observation failed; owned group cleanup UNCONFIRMED", { cause: error });
        }
        throw error; // Successful cleanup never turns failed observation into acceptance.
      }
    }
    const state = await request("get_state");
    if (!state.success || !isNativeSdkUnconfigured(state.data?.model, sdkDirectory) || state.data?.sessionId !== pin.sessionId ||
        state.data?.sessionFile !== sessionFile || (state.data?.capabilities as { inputAdmission?: unknown })?.inputAdmission !== 1) {
      throw new Error("Actual pinned SDK Main failed model-free/native-session readback");
    }
    await observations.until(() => {
      if (!mainIdentity || death(mainIdentity)) throw new Error(`Owner Main startup failed: ${lastError}`);
      if (!fs.existsSync(path.join(residencyRoot, "owner.json"))) return false;
      const owner = json<ResidentHostOwner>(path.join(residencyRoot, "owner.json"));
      const ready = path.join(residencyRoot, "maintenance-ready.json");
      return !!owner.nativeActors?.some(item => item.actorId === binding.actorId && item.rootId === pin.rootId) &&
        fs.existsSync(ready) && json<{ token: string }>(ready).token === owner.token;
    }, "native module + actor + resident maintenance readiness");
  };
  const ownedLiveMain = (): { pid: number; started: string } => {
    const identity = mainIdentity;
    if (!identity || !owned.owned().some(value => value.pid === identity.pid && value.started === identity.started) ||
        startTime(identity.pid) !== identity.started || death(identity)) throw new Error("Lost actual test Main signal custody");
    return { ...identity };
  };
  const resumeMain = (): void => {
    if (!mainPaused) return;
    const identity = ownedLiveMain();
    process.kill(identity.pid, "SIGCONT"); mainPaused = false;
    fs.appendFileSync(path.join(root, `main-${mainGeneration}.test-scheduling.jsonl`), JSON.stringify({ kind: "TEST_SCHEDULING",
      ...identity, signal: "SIGCONT", evidence: "signal delivered only; NOT Main death" }) + "\n", { mode: 0o600 });
  };
  const joinMainDeath = async (deadline?: NativeOwnerDeadline): Promise<{ stdioClosedNs: bigint; positiveDeathNs: bigint }> => {
    const child = main, closed = mainClosed, identity = mainIdentity;
    if (!child || !closed || !identity) throw new Error("No actual Main awaiting death join");
    // The clock belongs to the signal/resume boundary, NOT entry into this join.
    const remaining = deadline ? remainingNativeOwnerMs(deadline) : 30_000;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([closed, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Actual Main death/stdio close not observed within bound; a live old Main cannot pass")), remaining);
      })]);
      if (deadline) remainingNativeOwnerMs(deadline);
      if (mainClosedNs === undefined || !child.stdout.destroyed || !child.stderr.destroyed) throw new Error("Actual Main has no complete stdio close observation");
      const positiveDeathNs = recordDeath(mainGeneration, identity); // close/EOF/launcher exit alone NEVER suffice
      main = undefined; mainIdentity = undefined; mainPaused = false;
      if (deadline) remainingNativeOwnerMs(deadline); // Late death stays a failure, without recording it twice in cleanup.
      return { stdioClosedNs: mainClosedNs, positiveDeathNs };
    } finally { if (timer) clearTimeout(timer); }
  };
  const stopMain = async (): Promise<void> => {
    if (startupCleanupFailure) throw startupCleanupFailure;
    const child = main;
    if (!child) return;
    if (mainPaused && mainIdentity && !death(mainIdentity)) resumeMain();
    child.stdin.end(); // Held-open stdin is the lifetime owner; EOF requests native shutdown.
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([mainClosed!, new Promise<never>((_, reject) => {
        deadline = setTimeout(() => reject(new Error("Native owner orderly shutdown incomplete")), 60_000);
      })]);
    } finally { if (deadline) clearTimeout(deadline); }
    if (mainIdentity) recordDeath(mainGeneration, mainIdentity);
    main = undefined; mainIdentity = undefined; mainPaused = false;
  };
  const crashMain = async (): Promise<void> => {
    const child = main, closed = mainClosed;
    if (!child || !closed || !mainIdentity || death(mainIdentity)) throw new Error("No actual running Main to crash");
    const identity = mainIdentity;
    // Direct Main: exact ChildProcess. Orphan Main: exact launch-recorded birth
    // below. Never name/group signalling; resident retains its separate custody.
    if (options.mainLaunch === "launcher") {
      // An orphan remains OUR launch-recorded child, not an arbitrary PID. Recheck
      // exact birth and launch attribution immediately before this test-only kill.
      ownedLiveMain();
      process.kill(identity.pid, "SIGKILL");
    } else if (!child.kill("SIGKILL")) throw new Error("Actual Main SIGKILL was not delivered");
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([closed, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Actual Main crash lacks terminal close")), 30_000);
      })]);
      if (options.mainLaunch !== "launcher" && child.signalCode !== "SIGKILL") throw new Error("Actual Main crash has no SIGKILL terminal receipt");
      recordDeath(mainGeneration, identity);
      main = undefined; mainIdentity = undefined; mainPaused = false;
    } finally { if (timer) clearTimeout(timer); }
  };
  return { root, cwd, candidateRoot, sourceCandidateRoot, nativeOwnerEntry, binding, sessionFile, sessionPin, pin, mesh, meshRoot, ledgerDir, residencyRoot, idleExitMs,
    async crashLauncher(deadline?: NativeOwnerDeadline): Promise<void> {
      const child = main, exited = launcherExited;
      if (options.mainLaunch !== "launcher" || !child || !exited || child.exitCode !== null || child.signalCode !== null || !mainIdentity) throw new Error("No owned running test launcher");
      const identity = { ...mainIdentity };
      if (deadline) remainingNativeOwnerMs(deadline);
      if (!child.kill("SIGKILL")) throw new Error("Test launcher SIGKILL not delivered");
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([exited, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("No actual launcher exit receipt")), deadline ? remainingNativeOwnerMs(deadline) : 30_000); })]);
        if (deadline) remainingNativeOwnerMs(deadline);
        if (child.signalCode !== "SIGKILL") throw new Error("No actual test launcher SIGKILL terminal receipt");
        fs.writeFileSync(path.join(root, `launcher-${mainGeneration}.main-observation.json`), JSON.stringify({ ...identity,
          launcherPid: child.pid, launcherSignal: child.signalCode, mainDeath: death(identity) ?? "live",
          mainBirthReadback: startTime(identity.pid), stdinCustody: "launcher-owned-pipe" }), { mode: 0o600, flag: "wx" });
      } finally { if (timer) clearTimeout(timer); }
    },
    sdkImports: () => fs.existsSync(env.FABRIC_NATIVE_TEST_SDK_IMPORTS!) ? fs.readFileSync(env.FABRIC_NATIVE_TEST_SDK_IMPORTS!, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as { pid: number; event: string; url: string }) : [],
    ownedProcesses: () => owned.owned(),
    mainIdentity: () => mainIdentity ? { ...mainIdentity } : undefined,
    mainLive: () => !!mainIdentity && !death(mainIdentity) && startTime(mainIdentity.pid) === mainIdentity.started,
    async pauseMain(): Promise<void> {
      if (options.mainLaunch !== "launcher" || mainPaused) throw new Error("Test scheduling requires one live launcher-owned Main");
      const identity = ownedLiveMain();
      process.kill(identity.pid, "SIGSTOP"); mainPaused = true;
      const deadline = Date.now() + 5_000;
      for (;;) {
        ownedLiveMain();
        const stat = await fs.promises.readFile(`/proc/${identity.pid}/stat`, "utf8");
        const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
        if (fields[19] !== identity.started) throw new Error("Main birth changed at test scheduling boundary");
        if (fields[0] === "T") {
          fs.appendFileSync(path.join(root, `main-${mainGeneration}.test-scheduling.jsonl`), JSON.stringify({ kind: "TEST_SCHEDULING",
            ...identity, signal: "SIGSTOP", state: fields[0], evidence: "actual owned Main stopped; NOT deployment/N6 proof" }) + "\n", { mode: 0o600 });
          return;
        }
        if (Date.now() >= deadline) throw new Error("Actual owned Main stop was not observed");
        await new Promise<void>(resolve => setImmediate(resolve));
      }
    },
    until: observations.until, wake: observations.wake, start, stopMain, crashMain, joinMainDeath, resumeMain, request, rpcEvents,
    mainPid: () => mainIdentity?.pid,
    async attemptSecondMain() {
      const launchedBefore = new Set(owned.owned().map(value => `${value.pid}:${value.started}`));
      const name = `second-main-${auxiliarySequence + 1}`, receiptFile = path.join(root, `${name}.custody.json`);
      const launched = options.mainLaunch === "launcher";
      const result = await runAuxiliary("second-main", launched
        ? [path.join(fixtureRoot, "native-owner-launcher.mjs"), pinsPath, receiptFile, "--", ...mainArgs()] : mainArgs());
      const processes = owned.owned().filter(value => !launchedBefore.has(`${value.pid}:${value.started}`));
      let pid = result.pid, started: string | undefined, code = result.code, signal = result.signal;
      if (launched) {
        const receipt = json<{ launcherPid: number; mainPid: number; mainStarted: string; entry: string; entrySha256: string; stdinCustody: string }>(receiptFile);
        const terminal = json<{ pid: number; code: number | null; signal: NodeJS.Signals | null }>(`${receiptFile}.main-close.json`);
        if (receipt.launcherPid !== result.pid || receipt.mainPid === result.pid || receipt.entry !== nativeOwnerEntry ||
            receipt.entrySha256 !== pins.nativeOwnerEntrySha256 || receipt.stdinCustody !== "launcher-owned-pipe" ||
            !processes.some(value => value.pid === receipt.mainPid && value.started === receipt.mainStarted && value.argv[0] === nativeOwnerEntry) ||
            terminal.pid !== receipt.mainPid || !death({ pid: receipt.mainPid, started: receipt.mainStarted })) {
          throw new Error("Contender lacks actual compiled Main birth/death/launcher attribution");
        }
        pid = receipt.mainPid; started = receipt.mainStarted; code = terminal.code; signal = terminal.signal;
      }
      const attributed = { ...result, pid, started, code, signal, launcherPid: launched ? result.pid : undefined,
        launcherCode: launched ? result.code : undefined, processIds: processes.map(value => value.pid) };
      fs.writeFileSync(path.join(root, `${name}.attribution.json`), JSON.stringify(attributed), { mode: 0o600, flag: "wx" });
      return attributed;
    },
    attemptSessionAccess: (mode: "provision" | "verify", deadline?: NativeOwnerDeadline) => runAuxiliary(`contending-${mode}`, sessionArgs(mode), deadline),
    async probeDiscovery(): Promise<Record<string, unknown>> {
      // The ordinary selected profile has NO native session-dir selector. Native
      // file/path pins are explicit and remain outside this profile's defaults.
      const ordinaryEnv = { ...env, PI_CODING_AGENT_SESSION_DIR: undefined, TERM: "xterm-256color" };
      const args = (mode: string) => [path.join(fixtureRoot, "native-owner-session.mjs"), mode, sdkDirectory, sessionFile, cwd, sessionPin,
        nativeOwnerEntry, digest(nativeOwnerEntry)];
      const listed = await runAuxiliary("ordinary-discovery", args("discovery"), undefined, nodeBinary, ordinaryEnv);
      if (listed.code !== 0 || listed.signal) throw new Error(`Real ordinary discovery refused: ${listed.stderr}`);
      const value = JSON.parse(listed.stdout) as Record<string, unknown>;
      const bare = await runAuxiliary("ordinary-bare-native", args("discovery-bare-native"), undefined, nodeBinary, ordinaryEnv);
      const driver = path.join(root, "ordinary-resume-pty.py");
      fs.writeFileSync(driver, String.raw`import errno, fcntl, json, os, pty, select, struct, subprocess, sys, termios, time
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 120, 0, 0))
child = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)
output = bytearray(); sent = False; deadline = time.monotonic() + 25
try:
    while time.monotonic() < deadline:
        ready, _, _ = select.select([master], [], [], max(0, deadline-time.monotonic()))
        if not ready: raise RuntimeError('real resume picker deadline')
        try: data = os.read(master, 65536)
        except OSError as error:
            if error.errno == errno.EIO: break
            raise
        if not data: break
        output.extend(data)
        if not sent and b'ordinary-discovery-positive-control' in output:
            os.write(master, b'\r'); sent = True
    else: raise RuntimeError('real resume picker deadline')
    code = child.wait(timeout=2)
    sys.stdout.buffer.write(output); sys.stdout.buffer.flush()
    print(json.dumps({'realResumeChild': child.pid, 'code': code, 'selectedViaActualPicker': sent}))
    sys.exit(code)
finally:
    os.close(master)
    if child.poll() is None:
        child.kill(); child.wait(timeout=5)
`, { mode: 0o600, flag: "wx" });
      const resume = await runAuxiliary("ordinary-real-resume", [driver, nodeBinary, ...args("discovery-resume")], undefined, "/usr/bin/python3", ordinaryEnv);
      const match = /NATIVE_DISCOVERY_RESUME_RESULT:(\{[^\r\n]+\})/.exec(resume.stdout);
      const receipt = { ...value, bareNative: bare, resume, resumeSelection: match ? JSON.parse(match[1]!) : null,
        pathPinChanged: false, schemaChanged: false, migrationPerformed: false };
      fs.writeFileSync(path.join(root, "ordinary-discovery-isolation.json"), JSON.stringify(receipt, null, 2) + "\n", { mode: 0o600, flag: "wx" });
      return receipt;
    },
    async attemptOrdinaryTask(): Promise<NativeAuxiliaryResult> {
      const prepared = await runAuxiliary("ordinary-custom-session", [path.join(fixtureRoot, "native-owner-session.mjs"), "ordinary-custom-session",
        sdkDirectory, sessionFile, cwd, sessionPin, nativeOwnerEntry, digest(nativeOwnerEntry)]);
      if (prepared.code !== 0 || prepared.signal) throw new Error(`Ordinary session preparation refused: ${prepared.stderr}`);
      const ordinary = JSON.parse(prepared.stdout) as { sessionFile: string; sessionId: string };
      if (ordinary.sessionFile === sessionFile || ordinary.sessionId === pin.sessionId) throw new Error("Ordinary positive reused native identity");
      return runAuxiliary("ordinary-bare-task", [taskEntry, sdkDirectory, "1", "--mode", "rpc", "--session", ordinary.sessionFile,
        "--no-tools", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-mcp"]);
    },
    attemptFenceProof: (mode: "unlocked-proof" | "replaced-holder" | "clean-cli-session" | "inspect-cli-session") =>
      runAuxiliary(mode, [path.join(fixtureRoot, "native-owner-session.mjs"), mode, sdkDirectory, sessionFile, cwd, sessionPin,
        nativeOwnerEntry, digest(nativeOwnerEntry)]),
    async attemptUnfencedRoute(route: "plain-cli" | "bare-task"): Promise<NativeAuxiliaryResult> {
      const args = route === "plain-cli" ? [path.join(sdkDirectory, "cli.js")] : [taskEntry, sdkDirectory, "1"];
      args.push("--mode", "rpc", "--session", sessionFile, "--no-tools", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files",
        "--extension", pins.observer, "--extension", path.join(fixtureRoot, "native-owner-bootstrap.mjs"), "--extension", path.join(candidateRoot, "dist/index.js"));
      const child = spawn(nodeBinary, args, { cwd, env, stdio: ["pipe", "pipe", "pipe"] });
      const started = child.pid ? startTime(child.pid) : undefined;
      const lifetime = watchResidentChild(child);
      const label = `unfenced-${route}-${++auxiliarySequence}`;
      let stdout = "", stderr = "", expired = false;
      child.stdin.on("error", error => { if ((error as NodeJS.ErrnoException).code !== "EPIPE") throw error; });
      // Leave the ACTUAL stdin pipe open. Only product refusal may terminate it;
      // do not manufacture EOF/death to disguise a swallowed hook exception.
      child.stdin.write(JSON.stringify({ type: "prompt", id: "must-not-admit", message: "Must refuse before this input." }) + "\n");
      child.stdout.on("data", value => { stdout += value; fs.appendFileSync(path.join(root, `${label}.stdout`), value, { mode: 0o600 }); });
      child.stderr.on("data", value => { stderr += value; fs.appendFileSync(path.join(root, `${label}.stderr`), value, { mode: 0o600 }); });
      const result = await new Promise<NativeAuxiliaryResult>((resolve, reject) => {
        const timer = setTimeout(() => { expired = true; void lifetime.stop().catch(reject); }, 30_000);
        child.once("error", reject);
        child.once("close", (code, signal) => {
          clearTimeout(timer);
          const identity = { pid: child.pid!, started: started! };
          const evidence = death(identity);
          const value = { pid: child.pid, started, code, signal, stdout, stderr };
          fs.writeFileSync(path.join(root, `${label}.exit.json`), JSON.stringify({ ...value, args, executable: nodeBinary,
            expired, death: evidence, stdinEndedByParent: false, stdoutClosed: child.stdout.destroyed, stderrClosed: child.stderr.destroyed }), { mode: 0o600, flag: "wx" });
          if (expired || !evidence) reject(new Error(`Actual ${route} did not refuse/terminate: ${stderr}`));
          else resolve(value);
        });
      });
      return result;
    },
    async attemptRealCli(shape: NativeOwnerCliShape): Promise<NativeAuxiliaryResult & {
      ordinary?: NativeSessionPin; startedAt: number; closedAt: number;
    }> {
      const label = `real-cli-${shape}-${++auxiliarySequence}`;
      // Actual CLI defaults, not the native owner's isolated discovery override.
      const cliEnv = { ...env, PI_CODING_AGENT_SESSION_DIR: undefined };
      let ordinary: NativeSessionPin | undefined;
      if (shape.includes("ordinary") || shape === "duplicate-native") {
        const prepare = path.join(root, `${label}.prepare.mjs`);
        // SDK owns both the header and requested synthetic test ID. In the
        // prefix case ONLY the ordinary ID shares the native eight-char prefix.
        fs.writeFileSync(prepare, `import path from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
const [sdk, cwd, nativeId] = process.argv.slice(2);
const { SessionManager } = await import(pathToFileURL(path.join(sdk, "core/session-manager.js")).href);
const id = nativeId.slice(0, 8) + randomUUID().slice(8);
const manager = SessionManager.create(cwd, undefined, { id });
// The real SDK serializer persists this ordinary header-only discovery control;
// no hand-built header, native history, user message, or model turn is seeded.
manager._rewriteFile();
process.stdout.write(JSON.stringify({ sessionId: manager.getSessionId(), sessionFile: manager.getSessionFile(), rootId: "session:" + manager.getSessionId() }) + "\\n");
`, { mode: 0o600, flag: "wx" });
        const prepared = await runAuxiliary(`${label}-prepare`, [prepare, sdkDirectory, cwd, pin.sessionId], undefined, nodeBinary, cliEnv);
        if (prepared.code !== 0 || prepared.signal) throw new Error(`Real CLI ordinary preparation refused: ${prepared.stderr}`);
        ordinary = JSON.parse(prepared.stdout) as NativeSessionPin;
        if (ordinary.sessionId === pin.sessionId || ordinary.sessionFile === sessionFile ||
            !ordinary.sessionId.startsWith(pin.sessionId.slice(0, 8))) throw new Error("Ordinary prefix control lost its distinct SDK identity");
      }
      // Owned aliases exercise the exact reviewed spelling from the trusted
      // service cwd; the native file/pin/inodes and custody remain untouched.
      if (shape === "relative-native") fs.symlinkSync(path.dirname(sessionFile), path.join(cwd, "native"), "dir");
      if (shape === "dotdot-native") fs.symlinkSync(path.dirname(sessionFile), path.join(root, "native"), "dir");
      const link = path.join(cwd, `${label}.jsonl`);
      if (shape === "symlink-native" || shape === "ordinary-symlink") fs.symlinkSync(ordinary?.sessionFile ?? sessionFile, link);
      let selection: string[];
      switch (shape) {
        case "continue-native": selection = ["--continue", "--session", sessionFile]; break;
        case "short-continue-native": selection = ["-c", "--session", sessionFile]; break;
        case "resume-native": selection = ["-r", "--session", sessionFile]; break;
        case "long-resume-native": selection = ["--resume", "--session", sessionFile]; break;
        case "duplicate-native": selection = ["--session", ordinary!.sessionFile, "--session", sessionFile]; break;
        case "relative-native": selection = ["--session", "native/owner.jsonl"]; break;
        case "dotdot-native": selection = ["--session", `${cwd}/../native/owner.jsonl`]; break;
        case "symlink-native": selection = ["--session", link]; break;
        case "unknown-native": selection = ["--frobnicate", "--session", sessionFile]; break;
        case "short-unknown-native": selection = ["-z", "--session", sessionFile]; break;
        case "inline-session-native": selection = [`--session=${sessionFile}`, "--session", sessionFile]; break;
        case "ordinary-continue": selection = ["-c", "--session", ordinary!.sessionFile]; break;
        case "ordinary-resume": selection = ["--resume", "--session", ordinary!.sessionFile]; break;
        case "duplicate-ordinary": selection = ["--session", sessionFile, "--session", ordinary!.sessionFile]; break;
        case "unknown-ordinary": selection = ["--frobnicate", "--session", ordinary!.sessionFile]; break;
        case "ordinary-relative": selection = ["--session", path.relative(cwd, ordinary!.sessionFile)]; break;
        case "ordinary-symlink": selection = ["--session", link]; break;
        case "ordinary-flag-value": selection = ["--name", "--frobnicate", "--session", ordinary!.sessionFile]; break;
        case "name-native": selection = ["--session", sessionFile, "--name", "  native-name-residual  "]; break;
        case "task-prefix-ordinary": case "cli-prefix-ordinary": selection = ["--session", pin.sessionId.slice(0, 8)]; break;
        case "task-exact-native-id": selection = ["--session", pin.sessionId]; break;
      }
      const task = shape.startsWith("task-");
      const positive = ["ordinary-relative", "ordinary-symlink", "ordinary-flag-value", "task-prefix-ordinary", "cli-prefix-ordinary"].includes(shape);
      const args = [...(task ? [taskEntry, sdkDirectory, "1"] : [path.join(sdkDirectory, "cli.js")]), ...selection,
        "--mode", "rpc", "--no-tools", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files",
        "--extension", pins.observer, "--extension", path.join(candidateRoot, "dist/index.js")];
      // No bootstrap fixture or substitute registration gate. Native refusal
      // must come from candidate Fabric itself, with real SDK parser/loading.
      const startedAt = Date.now();
      const child = spawn(nodeBinary, args, { cwd, env: cliEnv, stdio: ["pipe", "pipe", "pipe"] });
      const started = child.pid ? startTime(child.pid) : undefined, lifetime = watchResidentChild(child);
      let stdout = "", stderr = "", expired = false, ended = false;
      child.stdin.on("error", error => { if ((error as NodeJS.ErrnoException).code !== "EPIPE") throw error; });
      child.stdin.write(JSON.stringify(positive ? { type: "get_state", id: "ordinary-readback" } :
        { type: "prompt", id: "must-not-admit", message: "Must refuse before this input." }) + "\n");
      child.stdout.on("data", chunk => {
        stdout += chunk; fs.appendFileSync(path.join(root, `${label}.stdout`), chunk, { mode: 0o600 });
        if (positive && !ended && stdout.split("\n").some(line => {
          try { const value = JSON.parse(line) as RpcRecord; return value.type === "response" && value.id === "ordinary-readback"; }
          catch { return false; }
        })) { ended = true; child.stdin.end(); } // Ordinary EOF only AFTER actual RPC readback, never a native refusal.
      });
      child.stderr.on("data", chunk => { stderr += chunk; fs.appendFileSync(path.join(root, `${label}.stderr`), chunk, { mode: 0o600 }); });
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { expired = true; void lifetime.stop().catch(reject); }, 30_000);
        child.once("error", reject);
        child.once("close", (code, signal) => {
          clearTimeout(timer);
          const evidence = death({ pid: child.pid!, started: started! }), closedAt = Date.now();
          const value = { pid: child.pid, started, code, signal, stdout, stderr, startedAt, closedAt, ...(ordinary ? { ordinary } : {}) };
          fs.writeFileSync(path.join(root, `${label}.exit.json`), JSON.stringify({ ...value, args, executable: nodeBinary,
            expired, death: evidence, stdinEndedByParent: ended, stdoutClosed: child.stdout.destroyed, stderrClosed: child.stderr.destroyed }), { mode: 0o600, flag: "wx" });
          if (expired || !evidence || (positive && !ended)) reject(new Error(`Actual ${shape} failed terminal/readback boundary: ${stderr}`));
          else resolve(value);
        });
      });
    },
    async probeModelPolicy(): Promise<Record<string, unknown>> {
      const result = await runAuxiliary("model-policy", [path.join(fixtureRoot, "native-owner-model-policy-probe.mjs")]);
      if (result.code !== 0) throw new Error(`Actual SDK no-model proof refused: ${result.stderr}`);
      return JSON.parse(result.stdout) as Record<string, unknown>;
    },
    owner: () => json<ResidentHostOwner>(path.join(residencyRoot, "owner.json")),
    observations: () => fs.existsSync(observerFile) ? fs.readFileSync(observerFile, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>) : [],
    callbacks: () => fs.existsSync(callbacks) ? fs.readFileSync(callbacks, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>) : [],
    async close(): Promise<void> {
      if (startupCleanupFailure) {
        observations.close(); mesh.closeState(); privateTemp.assertNoOutsideDeletion();
        throw startupCleanupFailure; // Keep unconfirmed custody unsealed for the parent; no fallback signals.
      }
      try { await stopMain(); }
      finally {
        let terminal = false;
        try {
          await stopAllOwned(owned.owned().filter(value => !death(value)), 2_000, 5_000);
          const processes = owned.owned().map(value => ({ ...value, death: death(value) ?? "UNKNOWN_OR_LIVE" }));
          fs.writeFileSync(path.join(root, "owned-terminal-evidence.json"), JSON.stringify(processes, null, 2) + "\n", { mode: 0o600, flag: "wx" });
          if (processes.some(value => value.death === "UNKNOWN_OR_LIVE")) throw new Error("Native fixture still has an unconfirmed owned process");
          terminal = true;
        }
        finally {
          observations.close(); mesh.closeState(); privateTemp.assertNoOutsideDeletion();
          // LAST: never seal a manifest while a known writer is still live or
          // unconfirmed. On failure the launcher retains the entire unsealed root.
          if (terminal) retainManifest();
        }
      }
    },
  };
};
export type NativeActorOwner = Awaited<ReturnType<typeof createNativeActorOwner>>;
