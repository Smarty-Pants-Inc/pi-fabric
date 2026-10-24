import { createHash } from "node:crypto";
import path from "node:path";

// Readback paths from the REAL legacy FabricControlPlane constructor/controlSeenKey, not a
// test-owned claim store. Keep these shared by the Node fixture and the parent assertions.
export const OWNER_HOST_ID = "session:owner-crash";
export const SENDER_HOST_ID = "session:owner-crash-sender";
export const TARGET_ID = OWNER_HOST_ID;
export const CONTROL_SEEN_PREFIX = "topology/control-seen/";
export const seenKey = (commandId: string): string => CONTROL_SEEN_PREFIX
  + createHash("sha256").update(`${OWNER_HOST_ID}\0${commandId}`).digest("hex");
export const fixturePaths = (root: string) => {
  const mesh = path.join(root, "mesh");
  return {
    mesh,
    seen: path.join(mesh, "control-seen", createHash("sha256").update(OWNER_HOST_ID).digest("hex").slice(0, 32)),
    journal: path.join(mesh, "main-followups", "owner-crash.json"),
    observations: path.join(root, "observations.jsonl"),
  };
};
export type CrashBoundary = "claim" | "custody" | "ack";
export interface OwnerReceipt {
  type: string;
  pid: number;
  [key: string]: unknown;
}

/** Deadlines are failure bounds, never the normal gate/receipt path. No polling or sleeps. */
export async function bounded<T>(operation: Promise<T>, label: string, ms = 30_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([operation, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Owner-crash deadline: ${label}`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}
