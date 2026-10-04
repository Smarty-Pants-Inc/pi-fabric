import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { findExecutable, spawnDetached } from "./process-utils.js";
import { taskAgentEnvironment } from "../task-environment.js";
import { applyTaskReturnAddress } from "../task-return-address.js";

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
    const executable = this.processSlice && process.platform === "linux" ? findExecutable("systemd-run") : undefined;
    if (this.processSlice && process.platform === "linux" && !executable) this.#warnScope("systemd-run unavailable");
    const processHandle = await spawnDetached(
      request.workerPath,
      request.workerArguments,
      request.cwd,
      request,
      // Worker arguments are flag/value pairs. A flag-shaped value is not an
      // actor identity; explicit actor ids alone retain the parent's role env.
      applyTaskReturnAddress(
        request.workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-id")
          ? { ...process.env } : taskAgentEnvironment(),
        request.workerArguments,
      ),
      executable ? { executable, slice: this.processSlice!, warn: this.#warnScope } : undefined,
    );
    return {
      kind: this.kind,
      sessionId: String(processHandle.pid),
      isAlive: processHandle.isAlive,
      lostContact: processHandle.lostContact,
      waitForClose: processHandle.waitForClose,
      stop: processHandle.stop,
    };
  }
}
