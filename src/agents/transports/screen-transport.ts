import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { commandAvailable, scriptSpawnArgs } from "./process-utils.js";
import { externalSessionHandle, launchExternalSession } from "./external-session.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";

const sessionName = (id: string): string => `pi-fabric-${id.slice(0, 12)}`;

export class ScreenTransport implements AgentTransportAdapter {
  readonly kind = "screen" as const;

  async available(): Promise<boolean> {
    return commandAvailable("screen");
  }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const session = sessionName(request.id);
    const command = await scriptSpawnArgs(request.workerPath, request.workerArguments);
    assertTransportLaunchAllowed(request);
    await launchExternalSession(this.kind, session, ["-dmS", session, ...command], request.cwd, request.signal);
    return externalSessionHandle(this.kind, session);
  }
}
