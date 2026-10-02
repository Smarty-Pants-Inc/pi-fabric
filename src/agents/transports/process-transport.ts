import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { spawnDetached } from "./process-utils.js";
import { taskAgentEnvironment } from "../task-environment.js";

export class ProcessTransport implements AgentTransportAdapter {
  readonly kind = "process" as const;

  async available(): Promise<boolean> {
    return true;
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const processHandle = await spawnDetached(
      request.workerPath,
      request.workerArguments,
      request.cwd,
      request,
      // Worker arguments are flag/value pairs. A flag-shaped value is not an
      // actor identity; explicit actors alone retain the parent's role env.
      request.workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-name")
        ? { ...process.env } : taskAgentEnvironment(),
      7_000, // worker owns a separately detached child with a five-second KILL grace
      process.platform !== "win32", // Windows has only legacy native-child cleanup, not tree custody
    );
    return {
      kind: this.kind,
      sessionId: String(processHandle.pid),
      isAlive: processHandle.isAlive,
      stop: processHandle.stop,
    };
  }
}
