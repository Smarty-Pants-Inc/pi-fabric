import fs from "node:fs";
import { recordOwnerLiveness, type LivenessProbes } from "../core/atomic-write.js";

export interface ResidentOwnerObservation {
  claimed: boolean;
  observedOwner: boolean;
  closeInput: boolean;
}

export function observeResidentOwner(
  ownerPid: number | undefined,
  childPid: number | undefined,
  claimed: boolean,
): ResidentOwnerObservation {
  if (childPid === undefined) {
    return { claimed, observedOwner: ownerPid !== undefined, closeInput: false };
  }
  if (ownerPid === childPid) {
    return { claimed: true, observedOwner: true, closeInput: false };
  }
  if (ownerPid !== undefined) {
    return { claimed, observedOwner: true, closeInput: true };
  }
  return { claimed, observedOwner: false, closeInput: claimed };
}

const signalAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

// An owner in another PID namespace is judged by its heartbeat; "unknown"
// keeps it observed. Identity-less owners keep the plain signal probe.
export const liveOwnerPid = (ownerPath: string, probes?: LivenessProbes): number | undefined => {
  try {
    const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as { pid?: unknown };
    if (typeof owner.pid !== "number") return undefined;
    const liveness = recordOwnerLiveness(owner, {
      legacyAlive: signalAlive,
      ...(probes ? { probes } : {}),
    });
    return liveness === "dead" ? undefined : owner.pid;
  } catch {
    return undefined;
  }
};
