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
      7_000, // worker owns a separately detached child with a five-second KILL grace
      process.platform !== "win32", // Windows uses its helper/native-close contract, not custody IPC
    );
    return {
      kind: this.kind,
      sessionId: String(processHandle.pid),
      isAlive: processHandle.isAlive,
      lostContact: processHandle.lostContact,
      ...(processHandle.stopDebt ? { stopDebt: processHandle.stopDebt } : {}),
      waitForClose: processHandle.waitForClose,
      stop: processHandle.stop,
    };
  }
}
