import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { activeFabricRoot, loadedFabricRoot, resolveAgentDir } from "../../core/agent-dir.js";
import { fabricResourceRoot } from "../../core/fabric-resource.js";
import { WORKER_PROTOCOL_VERSION } from "../worker-protocol.js";
import { findExecutable, spawnDetached, WorkerNotStartedError } from "./process-utils.js";
import { taskAgentEnvironment } from "../task-environment.js";
import { allocateRunTmpDirectory } from "../../storage/run-scratch.js";
import { applyTaskReturnAddress } from "../task-return-address.js";

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

  constructor(private readonly processSlice?: string) {}

  #warnScope = (reason: string): void => {
    if (this.#scopeWarningLogged) return;
    this.#scopeWarningLogged = true;
    console.warn(`[pi-fabric] agents.processSlice=${this.processSlice}: ${reason}; launching worker normally`);
  };

  async available(): Promise<boolean> {
    return true;
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    // The status file is the existing run-directory address; cwd is the project/worktree.
    const statusIndex = request.workerArguments.findIndex((arg, index) => index % 2 === 0 && arg === "--status-file");
    const statusFile = statusIndex < 0 ? undefined : request.workerArguments[statusIndex + 1];
    if (!statusFile) throw new Error("Process transport requires a run status file for private scratch");
    const allocation = allocateRunTmpDirectory(path.dirname(statusFile));
    const temporaryDirectory = allocation.directory;
    const temporaryEnvironment = {
      TMPDIR: temporaryDirectory,
      ...(process.platform === "win32" ? { TMP: temporaryDirectory, TEMP: temporaryDirectory } : {}),
    };
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
          ? { ...process.env, ...temporaryEnvironment } : { ...taskAgentEnvironment(), ...temporaryEnvironment },
        workerArguments,
      ),
      executable ? { executable, slice: this.processSlice!, warn: this.#warnScope } : undefined,
      7_000, // allow the worker's five-second execution-child cleanup
      process.platform !== "win32", // preserve Windows native-close/helper contract
      allocation.scope,
    ).catch(error => {
      if (error instanceof WorkerNotStartedError) allocation.neverStarted();
      throw error;
    });
    return {
      kind: this.kind,
      ...(selected.fabricRelease ? { fabricRelease: selected.fabricRelease } : {}),
      sessionId: String(processHandle.pid),
      isAlive: processHandle.isAlive,
      lostContact: processHandle.lostContact,
      ...(processHandle.stopDebt ? { stopDebt: processHandle.stopDebt } : {}),
      async waitForClose() {
        await processHandle.waitForClose();
        if (!processHandle.lostContact()) allocation.workerClosed(processHandle.pid);
      },
      async stop() {
        await processHandle.stop();
        if (!processHandle.lostContact()) allocation.workerClosed(processHandle.pid);
      },
      closed: processHandle.closed,
    };
  }
}
