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
import { launchLog, stopAllOwned } from "./owned-processes.js";
import { residentDrainTemp } from "./resident-drain-temp.js";
import { assertApprovedNativeSdk, isNativeSdkUnconfigured, NATIVE_SDK_RELEASE, NATIVE_SDK_SOURCE_PINS } from "./native-sdk-no-model.js";
import { testTempOwnerMarker, testTempProcessIdentity, testTempRunEnvironment } from "../../scripts/test-temp.js";

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

/** Commits wake observation; one deadline is only the failure bound, never a poll interval. */
export const observeNativeOwner = (root: string) => {
  const listeners = new Set<() => void>();
  let watchFailure: Error | undefined;
  const wake = (): void => { for (const listener of [...listeners]) listener(); };
  const watcher = fs.watch(root, { recursive: true, persistent: false }, wake);
  watcher.on("error", error => { watchFailure = error; wake(); });
  return {
    wake,
    close: () => watcher.close(),
    until: (predicate: () => boolean | Promise<boolean>, label: string, ms = 60_000): Promise<void> => new Promise((resolve, reject) => {
      let finished = false, checking = false, dirty = false;
      const finish = (error?: unknown): void => {
        if (finished) return;
        finished = true; clearTimeout(timer); listeners.delete(check);
        if (error) reject(error); else resolve();
      };
      const check = (): void => {
        if (finished) return;
        if (checking) { dirty = true; return; }
        checking = true;
        void (async () => {
          do {
            dirty = false;
            if (watchFailure) throw watchFailure;
            if (await predicate()) { finish(); return; }
          } while (dirty && !finished);
        })().catch(finish).finally(() => { checking = false; if (dirty && !finished) check(); });
      };
      const timer = setTimeout(() => finish(new Error(`Native owner observation deadline: ${label}`)), ms);
      listeners.add(check); check();
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
}
interface NativeSessionPin { sessionFile: string; rootId: string; sessionId: string }
interface RpcRecord { type?: string; id?: string; success?: boolean; error?: string; data?: Record<string, unknown>; [key: string]: unknown }

export const createNativeActorOwner = async (options: NativeActorOwnerOptions) => {
  if (process.platform !== "linux") throw new Error("N7 native residency fixture requires the Linux kernel OWNER fence");
  if (![options.candidateRoot, options.sdkDirectory].every(file => path.isAbsolute(file))) {
    throw new Error("N7 candidate and installed SDK require explicit absolute generation paths");
  }
  const custody = nativeEvidenceCustody();
  const candidateRoot = fs.realpathSync(options.candidateRoot);
  const sdkDirectory = fs.realpathSync(options.sdkDirectory);
  assertApprovedNativeSdk(sdkDirectory);
  const nodeBinary = absoluteFile(options.nodeBinary, "Node runtime");
  const taskEntry = absoluteFile(path.join(candidateRoot, "dist/worker/task-entry.js"), "Built candidate SDK/RPC entry");
  const nativeApi = absoluteFile(path.join(candidateRoot, "dist/native-actors.js"), "Built candidate native registration API");
  absoluteFile(path.join(candidateRoot, "dist/index.js"), "Built candidate extension");
  absoluteFile(path.join(candidateRoot, "dist/residency/pi-entry.js"), "Built candidate resident entry");
  absoluteFile(path.join(sdkDirectory, "index.js"), "Pinned Pi SDK");
  absoluteFile(path.join(sdkDirectory, "core/session-manager.js"), "Pinned native session manager");
  const privateTemp = residentDrainTemp("native-actor-owner-");
  const { root } = privateTemp;
  const relativeRoot = path.relative(custody.root, root);
  if (relativeRoot === ".." || relativeRoot.startsWith(`..${path.sep}`) || path.isAbsolute(relativeRoot)) {
    throw new Error("N7 BLOCKED: native session root escaped live launcher custody");
  }
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
  });
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
  const runAuxiliary = (label: string, args: string[]): Promise<{ code: number | null; signal: NodeJS.Signals | null; pid: number | undefined; stdout: string; stderr: string }> => new Promise((resolve, reject) => {
    const name = `${label}-${++auxiliarySequence}`;
    const child = spawn(nodeBinary, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    const lifetime = watchResidentChild(child);
    let stdout = "", stderr = "", expired = false;
    child.stdout.on("data", chunk => { fs.appendFileSync(path.join(root, `${name}.stdout`), chunk, { mode: 0o600 }); stdout = `${stdout}${chunk}`.slice(-128_000); });
    child.stderr.on("data", chunk => { fs.appendFileSync(path.join(root, `${name}.stderr`), chunk, { mode: 0o600 }); stderr = `${stderr}${chunk}`.slice(-32_000); });
    child.once("error", reject);
    const timer = setTimeout(() => { expired = true; void lifetime.stop().catch(reject); }, 30_000);
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      fs.writeFileSync(path.join(root, `${name}.exit.json`), JSON.stringify({ pid: child.pid, code, signal, expired }), { mode: 0o600 });
      if (expired) reject(new Error(`Native auxiliary ${label} did not terminate: ${stderr}`));
      else resolve({ code, signal, pid: child.pid, stdout, stderr });
    });
  });
  const sessionArgs = (mode: "provision" | "verify") => [path.join(fixtureRoot, "native-owner-session.mjs"), mode, sdkDirectory, sessionFile, cwd, sessionPin];
  const provision = async (): Promise<NativeSessionPin> => {
    const result = await runAuxiliary("session-provision", sessionArgs("provision"));
    if (result.code !== 0) throw new Error(`Native session provision refused: ${result.stderr}`);
    return JSON.parse(result.stdout) as NativeSessionPin;
  };

  let pin: NativeSessionPin;
  try { pin = await provision(); }
  catch (error) {
    try { await stopAllOwned(owned.owned()); }
    finally { observations.close(); privateTemp.assertNoOutsideDeletion(); retainManifest(); }
    throw error;
  }
  const binding: NativeActorBinding = Object.freeze({ actorId: options.actorId, rootId: pin.rootId, scope: "session",
    name: "native-residency", handlerKey: "native-residency.test.v1",
    residentModule: options.module ?? path.join(fixtureRoot, "native-resident-handler.mjs"),
    ingress: Object.freeze({ link: "native-residency", ledgerDir }) });
  const callbacks = path.join(root, "callbacks.jsonl"), observerFile = path.join(root, "observations.jsonl");
  const pins = { nodeBinary, sdkDirectory, taskEntry, taskEntrySha256: digest(taskEntry), sdkIndexSha256: digest(path.join(sdkDirectory, "index.js")),
    nativeApi, binding, sessionFile, sessionPin, meshRoot, observer: path.join(fixtureRoot, "native-owner-observer.mjs"), callbacks, observations: observerFile };
  fs.writeFileSync(pinsPath, JSON.stringify(pins), { mode: 0o600 });
  fs.writeFileSync(path.join(root, "generation-evidence.json"), JSON.stringify({ candidateRoot, nodeBinary, nodeBinarySha256: digest(nodeBinary),
    sdkDirectory, taskEntrySha256: pins.taskEntrySha256, sdkIndexSha256: pins.sdkIndexSha256,
    sdkRelease: NATIVE_SDK_RELEASE, approvedSdkSources: NATIVE_SDK_SOURCE_PINS,
    adapterSha256: digest(adapter), nativeApiSha256: digest(nativeApi), binding }), { mode: 0o600 });
  const mesh = new MeshStore(meshRoot, 256 * 1024, 1_000);
  const residencyRoot = residentRoot(meshRoot, pin.rootId);
  let main: ChildProcessWithoutNullStreams | undefined;
  let mainClosed: Promise<void> | undefined;
  let lastError = "";
  const pending = new Map<string, { resolve: (value: RpcRecord) => void; reject: (error: unknown) => void }>();
  const rpcEvents: RpcRecord[] = [];
  let sequence = 0, mainGeneration = 0;
  const mainArgs = () => [adapter, "--mode", "rpc", "--session", sessionFile,
    "--no-tools", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files",
    "--extension", path.join(fixtureRoot, "native-owner-bootstrap.mjs"), "--extension", path.join(candidateRoot, "dist/index.js")];
  const request = (type: "get_state" | "get_messages" | "prompt" | "steer" | "follow_up", message?: string): Promise<RpcRecord> => {
    if (!main || main.exitCode !== null || main.signalCode !== null || main.stdin.destroyed) return Promise.reject(new Error("Actual owner Main is not running"));
    const id = `native-owner-${++sequence}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`Owner RPC ${type} deadline: ${lastError}`)); }, 60_000);
      pending.set(id, { resolve: value => { clearTimeout(timer); resolve(value); }, reject: error => { clearTimeout(timer); reject(error); } });
      main!.stdin.write(JSON.stringify({ id, type, ...(message !== undefined ? { message } : {}) }) + "\n", error => {
        if (error) { pending.get(id)?.reject(error); pending.delete(id); }
      });
    });
  };
  const start = async (): Promise<void> => {
    if (main) throw new Error("Native owner Main already running or awaiting terminal join");
    // No parent verify/provision here: the actual Main takes its nonblocking
    // fence FIRST, then verifies the pinned file before SDK/task-entry can open it.
    const generation = ++mainGeneration;
    const child = spawn(nodeBinary, mainArgs(), { cwd, env, stdio: ["pipe", "pipe", "pipe"] });
    main = child;
    let buffer = "";
    mainClosed = new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", () => resolve()); });
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
      fs.writeFileSync(path.join(root, `main-${generation}.exit.json`), JSON.stringify({ pid: child.pid, code, signal }), { mode: 0o600 });
      // Only this isolated fixture's bounded, non-secret lifecycle observations;
      // retain the actual SDK state that caused a fail-closed startup exit.
      const observed = fs.existsSync(observerFile) ? fs.readFileSync(observerFile, "utf8").slice(-12_000) : "no lifecycle observations";
      for (const waiter of pending.values()) waiter.reject(new Error(`Actual owner Main exited (${code}): ${lastError}\n${observed}`));
      pending.clear(); observations.wake();
    });
    const state = await request("get_state");
    if (!state.success || !isNativeSdkUnconfigured(state.data?.model, sdkDirectory) || state.data?.sessionId !== pin.sessionId ||
        state.data?.sessionFile !== sessionFile || (state.data?.capabilities as { inputAdmission?: unknown })?.inputAdmission !== 1) {
      throw new Error("Actual pinned SDK Main failed model-free/native-session readback");
    }
    await observations.until(() => {
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Owner Main startup failed: ${lastError}`);
      if (!fs.existsSync(path.join(residencyRoot, "owner.json"))) return false;
      const owner = json<ResidentHostOwner>(path.join(residencyRoot, "owner.json"));
      const ready = path.join(residencyRoot, "maintenance-ready.json");
      return !!owner.nativeActors?.some(item => item.actorId === binding.actorId && item.rootId === pin.rootId) &&
        fs.existsSync(ready) && json<{ token: string }>(ready).token === owner.token;
    }, "native module + actor + resident maintenance readiness");
  };
  const stopMain = async (): Promise<void> => {
    const child = main;
    if (!child) return;
    child.stdin.end(); // Held-open stdin is the lifetime owner; EOF requests native shutdown.
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([mainClosed!, new Promise<never>((_, reject) => {
        deadline = setTimeout(() => reject(new Error("Native owner orderly shutdown incomplete")), 60_000);
      })]);
    } finally { if (deadline) clearTimeout(deadline); }
    main = undefined;
  };
  const crashMain = async (): Promise<void> => {
    const child = main, closed = mainClosed;
    if (!child || !closed || child.exitCode !== null || child.signalCode !== null) throw new Error("No actual running Main to crash");
    // Exact owned ChildProcess ONLY. Never by PID lookup, name or process group;
    // the detached resident stays alive and must not retain this Main's lock fd.
    if (!child.kill("SIGKILL")) throw new Error("Actual Main SIGKILL was not delivered");
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([closed, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Actual Main crash lacks terminal close")), 30_000);
      })]);
      if (child.signalCode !== "SIGKILL") throw new Error("Actual Main crash has no SIGKILL terminal receipt");
      main = undefined;
    } finally { if (timer) clearTimeout(timer); }
  };
  return { root, cwd, candidateRoot, binding, sessionFile, sessionPin, pin, mesh, meshRoot, ledgerDir, residencyRoot, idleExitMs,
    until: observations.until, wake: observations.wake, start, stopMain, crashMain, request, rpcEvents,
    mainPid: () => main?.pid,
    attemptSecondMain: () => runAuxiliary("second-main", mainArgs()),
    attemptSessionAccess: (mode: "provision" | "verify") => runAuxiliary(`contending-${mode}`, sessionArgs(mode)),
    async probeModelPolicy(): Promise<Record<string, unknown>> {
      const result = await runAuxiliary("model-policy", [path.join(fixtureRoot, "native-owner-model-policy-probe.mjs")]);
      if (result.code !== 0) throw new Error(`Actual SDK no-model proof refused: ${result.stderr}`);
      return JSON.parse(result.stdout) as Record<string, unknown>;
    },
    owner: () => json<ResidentHostOwner>(path.join(residencyRoot, "owner.json")),
    observations: () => fs.existsSync(observerFile) ? fs.readFileSync(observerFile, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>) : [],
    callbacks: () => fs.existsSync(callbacks) ? fs.readFileSync(callbacks, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>) : [],
    async close(): Promise<void> {
      try { await stopMain(); }
      finally {
        try { await stopAllOwned(owned.owned(), 2_000, 5_000); }
        finally { observations.close(); mesh.closeState(); privateTemp.assertNoOutsideDeletion(); retainManifest(); }
      }
    },
  };
};
export type NativeActorOwner = Awaited<ReturnType<typeof createNativeActorOwner>>;
