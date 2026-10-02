import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { spawnDetached } from "./process-utils.js";
import { taskAgentEnvironment } from "../task-environment.js";
import path from "node:path";
import { createRunTmpDirectory } from "../../storage/run-scratch.js";

export class ProcessTransport implements AgentTransportAdapter {
  readonly kind = "process" as const;

  async available(): Promise<boolean> {
    return true;
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    // The status file is the existing run-directory address; cwd is the project/worktree.
    const statusIndex = request.workerArguments.findIndex((arg, index) => index % 2 === 0 && arg === "--status-file");
    const statusFile = statusIndex < 0 ? undefined : request.workerArguments[statusIndex + 1];
    if (!statusFile) throw new Error("Process transport requires a run status file for private scratch");
    const temporaryDirectory = createRunTmpDirectory(path.dirname(statusFile));
    const temporaryEnvironment = {
      TMPDIR: temporaryDirectory,
      ...(process.platform === "win32" ? { TMP: temporaryDirectory, TEMP: temporaryDirectory } : {}),
    };
    const processHandle = await spawnDetached(
      request.workerPath,
      request.workerArguments,
      request.cwd,
      request,
      // Worker arguments are flag/value pairs. A flag-shaped value is not an
      // actor identity; explicit actors alone retain the parent's role env.
      request.workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-name")
        ? { ...process.env, ...temporaryEnvironment } : { ...taskAgentEnvironment(), ...temporaryEnvironment },
    );
    return {
      kind: this.kind,
      sessionId: String(processHandle.pid),
      isAlive: processHandle.isAlive,
      stop: processHandle.stop,
    };
  }
}
