import { vi } from "vitest";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import type { AgentTransportLaunch } from "../../src/agents/types.js";

// Native CI executes the full Windows manager/admission/close path. On POSIX,
// also execute the Windows scratch gate with real managers and native children;
// do not pretend POSIX can validate Windows ACLs or taskkill/native-close.
export const scratchPlatforms = [
  { label: `native ${process.platform}`, windows: process.platform === "win32", simulate: false },
  ...(process.platform === "win32" ? [] : [{ label: "simulated win32 scratch gate", windows: true, simulate: true }]),
];
const launch = ProcessTransport.prototype.launch;

export function launchWithScratchPlatform(transport: ProcessTransport, request: AgentTransportLaunch, simulate: boolean) {
  if (!simulate) return launch.call(transport, request);
  const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  try {
    // launch snapshots its allocation, environment and execution-custodian
    // gates synchronously, before spawnDetached awaits runtime resolution.
    return launch.call(transport, request);
  } finally {
    // Native spawn/stop, manager admission and filesystem operations stay real.
    platform.mockRestore();
  }
}
