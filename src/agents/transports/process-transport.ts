import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { spawnDetached } from "./process-utils.js";
import { taskAgentEnvironment } from "../task-environment.js";
import { applyTaskReturnAddress } from "../task-return-address.js";

export class ProcessTransport implements AgentTransportAdapter {
  readonly kind = "process" as const;

  async available(): Promise<boolean> {
    return true;
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    // Worker arguments are flag/value pairs. A flag-shaped value is not an
    // actor identity; explicit actor ids alone retain the parent's role env.
    const childEnvironment = request.workerArguments.some((arg, index) => index % 2 === 0 && arg === "--actor-id")
      ? { ...process.env } : taskAgentEnvironment();
    // Actors also own their names; the root launch name is never inherited.
    delete childEnvironment.SMARTY_AGENT_NAME;
    const processHandle = await spawnDetached(
      request.workerPath,
      request.workerArguments,
      request.cwd,
      request,
      applyTaskReturnAddress(childEnvironment, request.workerArguments),
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
