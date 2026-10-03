import type { AgentRunRequest } from "../../src/agents/types.js";
import { residentHostId, type ResidentCommandResponse } from "../../src/residency/protocol.js";

// Faithful bounded extraction of e8b4e7af host.ts:519-528,991-1028 and
// protocol.ts's operation allowlist. Do not import the current allowlist:
// a release-transition receiver must keep its compiled-in legacy semantics.
export const MERGE_BASE_RESIDENT_COMMANDS = [
  "spawn", "foreground", "cleanup", "createActor", "removeActor", "actors", "actorStatus",
  "setInstructions", "setModel", "setThinking", "setTools", "setActivationFilter", "releaseChange",
];
export const mergeBaseResidentOwner = (rootId: string) => ({
  format: 1, hostId: residentHostId(rootId), pid: process.pid, token: "legacy",
  startedAt: Date.now(), readyAt: Date.now(), commands: MERGE_BASE_RESIDENT_COMMANDS, requestFence: 1,
});

export const receiveMergeBaseSpawn = async (
  command: { format: number; rootId: string; requestId: string; operation: string; request: AgentRunRequest },
  rootId: string, requestId: string,
  agents: { spawn: (request: AgentRunRequest, signal: undefined, trace: undefined, commit: (id: string) => void) => Promise<unknown> },
  commit: (id: string) => void,
): Promise<ResidentCommandResponse> => {
  try {
    if ((command.format !== 1 && command.format !== 2) || command.rootId !== rootId || command.requestId !== requestId) {
      throw new Error("Invalid Fabric residency request");
    }
    if (!MERGE_BASE_RESIDENT_COMMANDS.includes(command.operation)) {
      return { format: 1, requestId, ok: false, errorCode: "RESIDENT_COMMAND_UNSUPPORTED",
        error: `Unsupported Fabric residency command: ${command.operation}`, completedAt: Date.now() };
    }
    if (command.operation !== "spawn") throw new Error("Fixture handles only the legacy spawn branch");
    if (command.request.residentStartupProbe || command.request.sessionSeed || command.request.sessionFile ||
      command.request.actorId || command.request.actorName || command.request.meshRoot ||
      command.request.runnerSessionId || command.request.images) {
      throw new Error("Durable agents.spawn accepts only its public task and run settings");
    }
    // Exactly the pre-guard call: ignores an extra caller field. A launch hook
    // makes the old receiver's unbound execution/publication observable in tests.
    await agents.spawn({ ...command.request, residency: "durable" }, undefined, undefined, commit);
    throw new Error("Legacy spawn unexpectedly returned in a refusal-only fixture");
  } catch (error) {
    return { format: 1, requestId, ok: false, error: String((error as Error).message), completedAt: Date.now() };
  }
};
