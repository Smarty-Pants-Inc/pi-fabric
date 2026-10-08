import fs from "node:fs";
import path from "node:path";

export interface ResidentIdleExit {
  format: 1;
  reason: "root-dead-idle";
  rootId: string;
  pid: number;
  token: string;
  at: number;
  idleMs: number;
}

export const residentIdleExitPath = (root: string): string => path.join(root, "idle-exit.json");

/** An intentional stop is not a crash/recovery request. Explicit business startup clears it. */
export const readResidentIdleExit = (root: string, rootId: string): ResidentIdleExit | undefined => {
  try {
    const marker = JSON.parse(fs.readFileSync(residentIdleExitPath(root), "utf8")) as Partial<ResidentIdleExit> | null;
    if (marker?.format === 1 && marker.reason === "root-dead-idle" && marker.rootId === rootId &&
        Number.isInteger(marker.pid) && marker.pid! > 0 && typeof marker.token === "string" && marker.token.length > 0 &&
        typeof marker.at === "number" && Number.isFinite(marker.at) &&
        typeof marker.idleMs === "number" && Number.isFinite(marker.idleMs) && marker.idleMs >= 0) return marker as ResidentIdleExit;
  } catch { /* Absent/invalid diagnostics are not an intentional exit receipt. */ }
  return undefined;
};
