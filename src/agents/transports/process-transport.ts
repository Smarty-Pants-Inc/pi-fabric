import fs from "node:fs";
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
  console.warn(`[pi-fabric] Installed Fabric release ${current} (worker protocol ${typeof version === "number" ? version : "unknown"}) is incompatible or incomplete; using parent release ${parent} (worker protocol ${WORKER_PROTOCOL_VERSION}).`);
  return fallback;
};

/** Default per-task memory.max (24 GiB, swap 0), mirrored from config to keep this module light. */
const DEFAULT_TASK_MEMORY_MAX_BYTES = 24 * 1024 ** 3;

const isSocket = (file: string): boolean => {
  try { return fs.statSync(file).isSocket(); } catch { return false; }
};

/**
 * smarty-dev#8257: cheap, D-Bus-free proof that `systemd-run --user --scope` can
 * create a memory-capped scope. All of: Linux, unified cgroup v2, an explicit and
 * usable user-bus environment (XDG_RUNTIME_DIR/bus or a unix:path=
 * DBUS_SESSION_BUS_ADDRESS socket; a bare /run/user socket is not enough, so
 * scrubbed test environments stay legacy), and the memory controller delegated
 * to this UID's user manager. Any doubt answers false: the launch stays legacy.
 */
export const delegatedUserCgroupAvailable = (
  environment: NodeJS.ProcessEnv = process.env,
  cgroupRoot = "/sys/fs/cgroup",
): boolean => {
  if (process.platform !== "linux") return false;
  const uid = process.getuid?.();
  if (uid === undefined) return false;
  const runtime = environment.XDG_RUNTIME_DIR;
  const bus = environment.DBUS_SESSION_BUS_ADDRESS?.match(/^unix:path=([^,;]+)/)?.[1];
  const busUsable = (runtime && path.isAbsolute(runtime) && isSocket(path.join(runtime, "bus")))
    || (bus && path.isAbsolute(bus) && isSocket(bus));
  if (!busUsable) return false;
  try {
    if (!fs.statSync(path.join(cgroupRoot, "cgroup.controllers")).isFile()) return false;
    const manager = path.join(cgroupRoot, "user.slice", `user-${uid}.slice`, `user@${uid}.service`);
    return fs.readFileSync(path.join(manager, "cgroup.subtree_control"), "utf8").split(/\s+/).includes("memory");
  } catch { return false; }
};

/** Exact flag-position `--actor-id` only; a flag-shaped value is not an actor identity. */
const isActorLaunch = (workerArguments: readonly string[]): boolean =>
  workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-id");

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
    console.warn(`[pi-fabric] ${source}: ${reason}; launching worker normally`);
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
      const capabilities = normalizeAgentCapabilityTokens(placement.capabilities, "agents.placement.capabilities")!;
      const unmet = (needs ?? []).filter(need => !capabilities.includes(need));
      let reason = request.needs?.includes("local") ? "reserved need: local pins to the Main host"
        : placement.default === "local" ? "placement default is local"
        : unmet.length ? `unmet needs: ${unmet.join(", ")}` : request.placementLocalReason;
      if (!reason) reason = agentPlacementProbe(placement, request.cwd).reason;
      // --src ships the Main's workspace, unlike --cwd which names a target-local
      // lane. Require the launcher's tracked/unignored manifest branch; home,
      // non-Git and ignored roots must never enter its recursive-copy branch.
      if (!reason && placement.command.some((entry, index) => entry === "--src" && placement.command[index + 1] === "{cwd}")) {
        try {
          const cwd = fs.realpathSync(request.cwd);
          if (cwd === path.parse(cwd).root || cwd === fs.realpathSync(os.homedir())) throw new Error("home or root source");
          const git = await executeFile("git", ["-C", cwd, "rev-parse", "--is-inside-work-tree"], {
            timeoutMs: Math.min(placement.commandTimeoutMs, 5_000), killSignal: "SIGKILL",
          });
          if (git.stdout.trim() !== "true") throw new Error("not a Git work tree");
          // check-ignore -q exits 0 for ignored, 1 for definitely unignored,
          // and >1 (or a signal/timeout) for indeterminate. Only 1 is safe.
          let unignored = false;
          try {
            await executeFile("git", ["-C", cwd, "check-ignore", "-q", "--", cwd], {
              timeoutMs: Math.min(placement.commandTimeoutMs, 5_000), killSignal: "SIGKILL",
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
        const { launchPlacedTask } = await import("./placement.js");
        return launchPlacedTask(request, placement);
      }
      const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      const log = args.get("--log-file");
      if (!log) throw new Error("Placement audit requires a run event log");
      fs.appendFileSync(log, JSON.stringify({ type: "placement.local", ts: Date.now(), id: request.id, reason, needs: request.needs ?? [] }) + "\n", { mode: 0o600 });
    }
    // Check on Main only after the remote decision; required paths may exist solely on the target.
    assertAgentRequiredInputsExist(requires, file => fs.existsSync(file));
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
    const executable = slice && process.platform === "linux" ? findExecutable("systemd-run", backendEnvironment) : undefined;
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
    const processHandle = await spawnDetached(
      selected.workerPath,
      workerArguments,
      request.cwd,
      request,
      // Worker arguments are flag/value pairs. A flag-shaped value is not an
      // actor identity; explicit actor ids alone retain the parent's role env.
      applyTaskReturnAddress(
        // Actors keep their roles, never their caller's capped-task receipt (SRC-1).
        isActorLaunch(workerArguments) ? withoutTaskBoundary(process.env) : taskAgentEnvironment(),
        workerArguments,
      ),
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
      ...(processHandle.stopDebt ? { stopDebt: processHandle.stopDebt } : {}),
      waitForClose: processHandle.waitForClose,
      closed: processHandle.closed,
      ...(processHandle.treeClosed ? { treeClosed: processHandle.treeClosed } : {}),
      stop: processHandle.stop,
    };
  }
}
