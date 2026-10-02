import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { commandAvailable, workerCommand } from "./process-utils.js";
import { externalSessionHandle, launchExternalSession } from "./external-session.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";

const sessionName = (id: string): string => `pi-fabric-${id.slice(0, 12)}`;

export class TmuxTransport implements AgentTransportAdapter {
  readonly kind = "tmux" as const;

  async available(): Promise<boolean> {
    return commandAvailable("tmux");
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const session = sessionName(request.id);
    const command = await workerCommand(request.workerPath, request.workerArguments);
    assertTransportLaunchAllowed(request);
    await launchExternalSession(this.kind, session, [
      "new-session", "-d", "-s", session, "-c", request.cwd, command,
    ], request.cwd, request.signal);
    return externalSessionHandle(this.kind, session);
  }
}
