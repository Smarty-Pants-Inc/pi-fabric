import fs from "node:fs";
import { diagnostics } from "../../core/diagnostics.js";
import path from "node:path";
import os from "node:os";
import { pathToFileURL } from "node:url";
import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { activeFabricRoot, loadedFabricRoot, resolveAgentDir } from "../../core/agent-dir.js";
import { fabricResourceRoot } from "../../core/fabric-resource.js";
import { WORKER_PROTOCOL_VERSION } from "../worker-protocol.js";
import { executeFile, findExecutable, spawnDetached } from "./process-utils.js";
import { taskAgentEnvironment, withoutTaskBoundary } from "../task-environment.js";
import { applyTaskReturnAddress } from "../task-return-address.js";
import type { AgentPlacementConfig } from "../placement-config.js";
import { agentPlacementProbe, liveAgentPlacement } from "../placement-config.js";
import { normalizeAgentCapabilityTokens } from "../../host-compatibility.js";
import { assertAgentRequiredInputsExist, normalizeAgentRequires } from "../input-validation.js";
import { isInside, readWritePolicy } from "../write-guard.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";

const regularFile = (file: string): boolean => {
  try { return fs.statSync(file).isFile(); } catch { return false; }
};

/** Resolve the installer's existing profile selector at the actual launch boundary.
 * Pin both child entrypoints to its canonical release root, never a mutable pointer.
 * Explicit source/custom workers remain caller-selected (tests and development).
 */
const selectWorkerRelease = (workerPath: string): { workerPath: string; fabricRelease?: string; extensionPath?: string } => {
  let canonicalWorker = path.resolve(workerPath);
  try { canonicalWorker = fs.realpathSync(canonicalWorker); } catch { /* Preserve normal spawn errors for missing custom paths. */ }
  const parent = loadedFabricRoot(pathToFileURL(canonicalWorker).href);
  if (!parent) return { workerPath };
  if (canonicalWorker !== path.join(parent, "dist/worker.js")) {
    return { workerPath, fabricRelease: parent };
  }
  const fallback = { workerPath: canonicalWorker, fabricRelease: parent, extensionPath: path.join(parent, "dist/index.js") };
  const current = activeFabricRoot(path.join(resolveAgentDir(), "settings.json"));
  if (!current || current === parent) return fallback;
  let version: unknown;
  try {
    version = (JSON.parse(fs.readFileSync(path.join(current, "dist/worker-protocol.json"), "utf8")) as { version?: unknown }).version;
  } catch { /* Unversioned/malformed releases are not known-compatible. */ }
  const selectedWorker = path.join(current, "dist/worker.js");
  const selectedExtension = path.join(current, "dist/index.js");
  if (version === WORKER_PROTOCOL_VERSION && regularFile(selectedWorker) && regularFile(selectedExtension)) {
    return { workerPath: selectedWorker, fabricRelease: current, extensionPath: selectedExtension };
  }
  diagnostics.warn("src/agents/transports/process-transport.ts", `[pi-fabric] Installed Fabric release ${current} (worker protocol ${typeof version === "number" ? version : "unknown"}) is incompatible or incomplete; using parent release ${parent} (worker protocol ${WORKER_PROTOCOL_VERSION}).`);
  return fallback;
};

/** Default per-task memory.max (24 GiB, swap 0), mirrored from config to keep this module light. */
const DEFAULT_TASK_MEMORY_MAX_BYTES = 24 * 1024 ** 3;

type ProbeOutcome = "supported" | "unsupported";

const errorCode = (error: unknown): string => {
  if (error && typeof error === "object" && "code" in error && typeof error.code === "string") return error.code;
  return error instanceof Error && error.message ? error.message : "unknown";
};

/** A host capability probe failed closed: it was neither present nor absent. */
export class TaskScopeProbeError extends Error {
  readonly code = "TASK_SCOPE_PROBE_UNKNOWN" as const;
  constructor(readonly probe: string, readonly errno: string, readonly cause: unknown) {
    super(`Task scope probe ${probe} returned unknown error ${errno}`);
    this.name = "TaskScopeProbeError";
  }
}

const probeStat = (file: string, predicate: (stats: fs.Stats) => boolean, probe: string): ProbeOutcome => {
  try { return predicate(fs.statSync(file)) ? "supported" : "unsupported"; }
  catch (error) {
    const errno = errorCode(error);
    if (errno === "ENOENT") return "unsupported";
    throw new TaskScopeProbeError(probe, errno, error);
  }
};

const probeSocket = (file: string, probe: string): ProbeOutcome => probeStat(file, stats => stats.isSocket(), probe);

const probeFileText = (file: string, predicate: (text: string) => boolean, probe: string): ProbeOutcome => {
  try { return predicate(fs.readFileSync(file, "utf8")) ? "supported" : "unsupported"; }
  catch (error) {
    const errno = errorCode(error);
    if (errno === "ENOENT") return "unsupported";
    throw new TaskScopeProbeError(probe, errno, error);
  }
};

const busAvailable = (environment: NodeJS.ProcessEnv): ProbeOutcome => {
  const candidates: string[] = [];
  const runtime = environment.XDG_RUNTIME_DIR;
  const bus = environment.DBUS_SESSION_BUS_ADDRESS?.match(/^unix:path=([^,;]+)/)?.[1];
  if (runtime && path.isAbsolute(runtime)) candidates.push(path.join(runtime, "bus"));
  if (bus && path.isAbsolute(bus)) candidates.push(bus);
  if (candidates.length === 0) return "unsupported";
  let unknown: TaskScopeProbeError | undefined;
  for (const candidate of candidates) {
    try {
      if (probeSocket(candidate, "bus socket") === "supported") return "supported";
    } catch (error) {
      if (error instanceof TaskScopeProbeError) unknown ??= error;
      else throw error;
    }
  }
  if (unknown) throw unknown;
  return "unsupported";
};

/**
 * smarty-dev#8257/#11898: D-Bus-free proof that `systemd-run --user --scope`
 * can create a memory-capped scope. ENOENT and positive absence are legacy
 * eligibility results; an unreadable probe is a typed refusal, never permission
 * to launch the worker uncapped.
 */
export const delegatedUserCgroupAvailable = (
  environment: NodeJS.ProcessEnv = process.env,
  cgroupRoot = "/sys/fs/cgroup",
): boolean => {
  if (process.platform !== "linux") return false;
  const uid = process.getuid?.();
  if (uid === undefined) return false;
  if (busAvailable(environment) !== "supported") return false;
  const controllers = path.join(cgroupRoot, "cgroup.controllers");
  if (probeStat(controllers, stats => stats.isFile(), "cgroup controllers") !== "supported") return false;
  if (probeFileText(controllers, text => text.split(/\s+/).includes("memory"), "cgroup controllers") !== "supported") return false;
  const manager = path.join(cgroupRoot, "user.slice", `user-${uid}.slice`, `user@${uid}.service`);
  return probeFileText(path.join(manager, "cgroup.subtree_control"), text => text.split(/\s+/).includes("memory"), "memory delegation") === "supported";
};

const findExecutableStrict = (command: string, environment: NodeJS.ProcessEnv): string | undefined => {
  let unknown: TaskScopeProbeError | undefined;
  const executable = findExecutable(command, environment, candidate => {
    try {
      const stats = fs.statSync(candidate);
      return stats.isFile() && (process.platform === "win32" || (stats.mode & 0o111) !== 0);
    } catch (error) {
      const errno = errorCode(error);
      if (errno !== "ENOENT") unknown ??= new TaskScopeProbeError("systemd-run executable", errno, error);
      return false;
    }
  });
  if (executable) return executable;
  if (unknown) throw unknown;
  return undefined;
};

/** Exact flag-position `--actor-id` only; a flag-shaped value is not an actor identity. */
const isActorLaunch = (workerArguments: readonly string[]): boolean =>
  workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-id");

/** Match write-guard containment for a temp path, including symlinks and missing leaves. */
const nearestRealPath = (target: string): string | undefined => {
  let current = target;
  for (;;) {
    try {
      fs.lstatSync(current);
    } catch {
      const parent = path.dirname(current);
      if (parent === current) return undefined;
      current = parent;
      continue;
    }
    try {
      return path.join(fs.realpathSync.native(current), path.relative(current, target));
    } catch {
      return undefined;
    }
  }
};

const launchWritePolicy = (workerArguments: readonly string[]) => {
  let serialized: string | undefined;
  for (let index = 0; index < workerArguments.length; index += 2) {
    if (workerArguments[index] === "--write-policy") serialized = workerArguments[index + 1];
  }
  return serialized === undefined ? undefined : readWritePolicy(serialized);
};

const containedByRoot = (candidate: string, cwd: string, roots: readonly string[]): boolean => {
  const lexical = path.isAbsolute(candidate) ? path.normalize(candidate) : path.resolve(cwd, candidate);
  // Preserve raw link/.. traversal for the native filesystem lookup; path.resolve
  // and the JS realpath implementation collapse it before following symlinks.
  const raw = path.isAbsolute(candidate) ? candidate : `${cwd}${path.sep}${candidate}`;
  const real = nearestRealPath(raw);
  return real !== undefined && roots.some(root => isInside(root, lexical)) && roots.some(root => isInside(root, real));
};

const writableDirectory = (candidate: string, cwd: string): boolean => {
  // Match the child's native lookup, including relative paths and link/.. traversal.
  const raw = path.isAbsolute(candidate) ? candidate : `${cwd}${path.sep}${candidate}`;
  try {
    if (!fs.statSync(raw).isDirectory()) return false;
    fs.accessSync(raw, fs.constants.W_OK | fs.constants.X_OK);
    return true;
  } catch { return false; }
};

/**
 * A confined worker must not inherit a host temp directory. Keep an already
 * safe TMPDIR verbatim; otherwise make a private directory under the grant
 * and update --tmpdir too, because worker.ts exports that option on startup.
 */
const prepareTaskTemporaryDirectory = (
  workerArguments: readonly string[],
  environment: NodeJS.ProcessEnv,
  cwd: string,
): { workerArguments: string[]; environment: NodeJS.ProcessEnv } => {
  const policy = launchWritePolicy(workerArguments);
  if (!policy || policy.readOnly || policy.writableRoots.length === 0) return { workerArguments: [...workerArguments], environment };

  const roots = policy.writableRoots;
  const optionTmpdir = (() => {
    let value: string | undefined;
    for (let index = 0; index < workerArguments.length; index += 2) {
      if (workerArguments[index] === "--tmpdir") value = workerArguments[index + 1];
    }
    return value;
  })();
  const inheritedTmpdir = environment.TMPDIR;
  let temporary = optionTmpdir && containedByRoot(optionTmpdir, cwd, roots) && writableDirectory(optionTmpdir, cwd) ? optionTmpdir
    : inheritedTmpdir && containedByRoot(inheritedTmpdir, cwd, roots) && writableDirectory(inheritedTmpdir, cwd) ? inheritedTmpdir
    : undefined;
  if (!temporary) {
    const realCwd = fs.realpathSync(cwd);
    const nonWorktreeRoot = roots.find(root => !isInside(root, realCwd) && !isInside(realCwd, root));
    const root = nonWorktreeRoot ?? roots[roots.length - 1]!;
    if (!containedByRoot(root, cwd, roots)) throw new Error("writable root changed outside the effective policy");
    temporary = fs.mkdtempSync(path.join(root, ".pi-fabric-task-tmp-"));
    fs.chmodSync(temporary, 0o700);
    if (process.platform !== "win32" && (fs.statSync(temporary).mode & 0o777) !== 0o700) {
      throw new Error("private task temporary directory is not mode 0700");
    }
    if (!containedByRoot(temporary, cwd, roots)) throw new Error("private task temporary directory escaped writable roots");
  }

  const childEnvironment = { ...environment, TMPDIR: temporary, TMP: temporary, TEMP: temporary };
  const childArguments = [...workerArguments];
  for (let index = 0; index < childArguments.length; index += 2) {
    if (childArguments[index] !== "--tmpdir") continue;
    const candidate = childArguments[index + 1];
    if (!candidate || !containedByRoot(candidate, cwd, roots) || !writableDirectory(candidate, cwd)) childArguments[index + 1] = temporary;
  }
  return { workerArguments: childArguments, environment: childEnvironment };
};

export class ProcessTransport implements AgentTransportAdapter {
  readonly kind = "process" as const;
  #scopeWarningLogged = false;

  readonly #placement: () => AgentPlacementConfig | undefined;

  readonly #memoryMaxBytes: number;

  constructor(
    private readonly processSlice?: string,
    placement?: AgentPlacementConfig,
    placementConfigPath?: string,
    memoryMaxBytes: number = DEFAULT_TASK_MEMORY_MAX_BYTES,
  ) {
    this.#placement = placementConfigPath ? liveAgentPlacement(placementConfigPath, placement) : () => placement;
    if (!Number.isSafeInteger(memoryMaxBytes) || memoryMaxBytes <= 0) {
      throw new Error("ProcessTransport memoryMaxBytes must be a positive safe integer");
    }
    this.#memoryMaxBytes = memoryMaxBytes;
  }

  #warnScope = (reason: string): void => {
    if (this.#scopeWarningLogged) return;
    this.#scopeWarningLogged = true;
    const source = this.processSlice ? `agents.processSlice=${this.processSlice}` : "default task memory cap (app.slice)";
    diagnostics.warn("src/agents/transports/process-transport.ts", `[pi-fabric] ${source}: ${reason}; launching worker normally`);
  };

  async available(): Promise<boolean> {
    return true;
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    // Snapshot once before any await: existing handles retain their original policy.
    const requires = normalizeAgentRequires(request.requires);
    const needs = normalizeAgentCapabilityTokens(request.needs);
    request = { ...request, ...(needs !== undefined ? { needs } : {}), ...(requires !== undefined ? { requires } : {}) };
    const placement = this.#placement();
    if (placement) {
      const preflightDeadline = Date.now() + Math.min(placement.commandTimeoutMs, 5_000);
      const capabilities = normalizeAgentCapabilityTokens(placement.capabilities, "agents.placement.capabilities")!;
      const unmet = (needs ?? []).filter(need => need !== "corpus" && !capabilities.includes(need));
      let reason = request.needs?.includes("local") ? "reserved need: local pins to the Main host"
        : placement.default === "local" ? "placement default is local"
        : unmet.length ? `unmet needs: ${unmet.join(", ")}` : request.placementLocalReason;
      if (!reason) reason = agentPlacementProbe(placement, request.cwd).reason;
      if (!reason) {
        const { placementLocalPathReason } = await import("./placement.js");
        reason = placementLocalPathReason(request.cwd, placement);
      }
      // --src ships the Main's workspace, unlike --cwd which names a target-local
      // lane. Require the launcher's tracked/unignored manifest branch; home,
      // non-Git and ignored roots must never enter its recursive-copy branch.
      if (!reason && placement.command.some((entry, index) => entry === "--src" && placement.command[index + 1] === "{cwd}")) {
        try {
          const cwd = fs.realpathSync(request.cwd);
          if (cwd === path.parse(cwd).root || cwd === fs.realpathSync(os.homedir())) throw new Error("home or root source");
          const git = await executeFile("git", ["-C", cwd, "rev-parse", "--is-inside-work-tree"], {
            timeoutMs: Math.max(1, Math.min(5_000, preflightDeadline - Date.now())), killSignal: "SIGKILL",
          });
          if (git.stdout.trim() !== "true") throw new Error("not a Git work tree");
          // check-ignore -q exits 0 for ignored, 1 for definitely unignored,
          // and >1 (or a signal/timeout) for indeterminate. Only 1 is safe.
          let unignored = false;
          try {
            await executeFile("git", ["-C", cwd, "check-ignore", "-q", "--", cwd], {
              timeoutMs: Math.max(1, Math.min(5_000, preflightDeadline - Date.now())), killSignal: "SIGKILL",
            });
          } catch (error) {
            const exit = error as { code?: unknown; signal?: unknown; killed?: unknown; stdout?: unknown; stderr?: unknown } | null;
            unignored = exit?.code === 1 && exit.signal == null && !exit.killed
              && exit.stdout === "" && exit.stderr === "";
          }
          if (!unignored) throw new Error("ignored or indeterminate source root");
        } catch { reason = "cwd-not-shippable"; }
      }
      if (!reason) {
        const { launchPlacedTask, selectFleetPlacement } = await import("./placement.js");
        const selection = await selectFleetPlacement(placement, request.cwd, request.signal, preflightDeadline);
        if (selection && "localReason" in selection) reason = selection.localReason;
        else return launchPlacedTask(request, placement, preflightDeadline, selection?.host);
      }
      const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      const log = args.get("--log-file");
      if (!log) throw new Error("Placement audit requires a run event log");
      fs.appendFileSync(log, JSON.stringify({ type: "placement.local", ts: Date.now(), id: request.id, reason, needs: request.needs ?? [] }) + "\n", { mode: 0o600 });
    }
    // Check on Main only after the remote decision; required paths may exist solely on the target.
    assertAgentRequiredInputsExist(requires, file => fs.existsSync(file));
    request.onWritePolicyDisposition?.("local");
    const actor = isActorLaunch(request.workerArguments);
    // smarty-dev#8257: tasks get their own memory-capped scope only on a proven
    // delegated v2 user manager. Actors and unproven hosts keep the legacy path:
    // explicit processSlice still scopes them uncapped; otherwise no systemd-run.
    const capEligible = !actor && process.platform === "linux" && delegatedUserCgroupAvailable();
    const slice = this.processSlice ?? (capEligible ? "app.slice" : undefined);
    // A nested Fabric launch needs the real backend, not its parent's command
    // shim: the shim keeps ordinary suites in the parent leaf and cannot admit
    // a new task scope. Preserve PATH for task commands; exclude only that exact
    // launcher-owned wrapper from this backend lookup.
    const wrapper = process.env.FABRIC_TASK_SYSTEMD_RUN_WRAPPER;
    const backendEnvironment = wrapper ? {
      ...process.env,
      PATH: (process.env.PATH ?? "").split(path.delimiter)
        .filter(directory => path.resolve(directory, "systemd-run") !== wrapper).join(path.delimiter),
    } : process.env;
    const executable = slice && process.platform === "linux" ? findExecutableStrict("systemd-run", backendEnvironment) : undefined;
    if (this.processSlice && process.platform === "linux" && !executable) this.#warnScope("systemd-run unavailable");
    const scope: { executable: string; slice: string; warn: (reason: string) => void; memoryMaxBytes?: number } | undefined =
      executable && slice
        ? { executable, slice, warn: this.#warnScope, ...(capEligible ? { memoryMaxBytes: this.#memoryMaxBytes } : {}) }
        : undefined;
    const selected = selectWorkerRelease(request.workerPath);
    const workerArguments = [...request.workerArguments];
    if (selected.extensionPath) {
      let pinned = false;
      for (let index = 0; index < workerArguments.length; index += 2) {
        if (workerArguments[index] === "--fabric-extension") {
          const explicitExtension = workerArguments[index + 1]!;
          // Only replace a Fabric generation. Explicit caller hooks are not
          // release selectors (e.g. resident probes testing a broken factory).
          if (fabricResourceRoot(explicitExtension, "extensions")) {
            workerArguments[index + 1] = selected.extensionPath;
          }
          pinned = true;
        }
      }
      const args = new Map<string, string>();
      for (let index = 0; index < workerArguments.length; index += 2) args.set(workerArguments[index]!, workerArguments[index + 1]!);
      if (!pinned && args.get("--runner") === "pi" && args.get("--extensions") === "true") {
        workerArguments.push("--fabric-extension", selected.extensionPath);
      }
    }
    if (selected.fabricRelease) workerArguments.push("--fabric-release", selected.fabricRelease);
    // Worker arguments are flag/value pairs. A flag-shaped value is not an
    // actor identity; explicit actor ids alone retain the parent's role env.
    const baseEnvironment = applyTaskReturnAddress(
      // Actors keep their roles, never their caller's capped-task receipt (SRC-1).
      isActorLaunch(workerArguments) ? withoutTaskBoundary(process.env) : taskAgentEnvironment(),
      workerArguments,
    );
    assertTransportLaunchAllowed(request);
    const prepared = isActorLaunch(workerArguments)
      ? { workerArguments, environment: baseEnvironment }
      : prepareTaskTemporaryDirectory(workerArguments, baseEnvironment, request.cwd);
    const processHandle = await spawnDetached(
      selected.workerPath,
      prepared.workerArguments,
      request.cwd,
      request,
      request.childEnvironment ? request.childEnvironment(prepared.environment) : prepared.environment,
      scope,
      7_000, // allow the worker's five-second execution-child cleanup
      process.platform !== "win32", // Windows retains its native-close/helper contract
    );
    // Core sets cgroup only after admitting a capped receipt; structural read keeps
    // this compatible with handles that never carry one.
    const { cgroup } = processHandle as typeof processHandle & { cgroup?: string };
    return {
      kind: this.kind,
      ...(selected.fabricRelease ? { fabricRelease: selected.fabricRelease } : {}),
      ...(cgroup ? { cgroup } : {}),
      sessionId: String(processHandle.pid),
      ...(processHandle.treeClosed ? { liveness: "events" as const } : {}),
      isAlive: processHandle.isAlive,
      lostContact: processHandle.lostContact,
      finishingGraceMs: 7_000,
      ...(processHandle.forcedCleanupCount ? { forcedCleanupCount: processHandle.forcedCleanupCount } : {}),
      ...(processHandle.remainingDescendantCount ? { remainingDescendantCount: processHandle.remainingDescendantCount } : {}),
      ...(processHandle.stopDebt ? { stopDebt: processHandle.stopDebt } : {}),
      waitForClose: processHandle.waitForClose,
      closed: processHandle.closed,
      ...(processHandle.primaryExit ? { primaryExit: processHandle.primaryExit } : {}),
      ...(processHandle.treeClosed ? { treeClosed: processHandle.treeClosed } : {}),
      stop: processHandle.stop,
    };
  }
}
