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
import { taskAgentEnvironment } from "../task-environment.js";
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

export class ProcessTransport implements AgentTransportAdapter {
  readonly kind = "process" as const;
  #scopeWarningLogged = false;

  readonly #placement: () => AgentPlacementConfig | undefined;

  constructor(private readonly processSlice?: string, placement?: AgentPlacementConfig, placementConfigPath?: string) {
    this.#placement = placementConfigPath ? liveAgentPlacement(placementConfigPath, placement) : () => placement;
  }

  #warnScope = (reason: string): void => {
    if (this.#scopeWarningLogged) return;
    this.#scopeWarningLogged = true;
    console.warn(`[pi-fabric] agents.processSlice=${this.processSlice}: ${reason}; launching worker normally`);
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
    const executable = this.processSlice && process.platform === "linux" ? findExecutable("systemd-run") : undefined;
    if (this.processSlice && process.platform === "linux" && !executable) this.#warnScope("systemd-run unavailable");
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
        workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-id")
          ? { ...process.env } : taskAgentEnvironment(),
        workerArguments,
      ),
      executable ? { executable, slice: this.processSlice!, warn: this.#warnScope } : undefined,
      7_000, // allow the worker's five-second execution-child cleanup
      process.platform !== "win32", // Windows retains its native-close/helper contract
    );
    return {
      kind: this.kind,
      ...(selected.fabricRelease ? { fabricRelease: selected.fabricRelease } : {}),
      sessionId: String(processHandle.pid),
      isAlive: processHandle.isAlive,
      lostContact: processHandle.lostContact,
      ...(processHandle.stopDebt ? { stopDebt: processHandle.stopDebt } : {}),
      waitForClose: processHandle.waitForClose,
      closed: processHandle.closed,
      stop: processHandle.stop,
    };
  }
}
