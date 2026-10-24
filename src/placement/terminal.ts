import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { hostLeasePath } from "../topology/host-leases.js";

export type PlacementTerminalState = "finished" | "failed" | "never-started";

export interface PlacementTerminalRecord {
  placementRunId: string;
  state: PlacementTerminalState;
  at: string;
  exitCode?: number;
  reason?: string;
}

const RUN_ID = /^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$/;
const REASONS = /^[^\u0000-\u001f\u007f]{1,256}$/;
const keys = ["placementRunId", "state", "at", "exitCode", "reason"] as const;

export class PlacementTerminalWriteError extends Error {
  readonly code = "FABRIC_PLACEMENT_TERMINAL_WRITE_FAILED" as const;
  constructor(readonly cause?: unknown) {
    super("Fabric placement terminal record could not be persisted");
    this.name = "PlacementTerminalWriteError";
  }
}

export const makePlacementTerminalRecord = (
  placementRunId: string,
  state: PlacementTerminalState,
  options: { at?: number; exitCode?: number; reason?: string } = {},
): PlacementTerminalRecord => {
  const at = options.at ?? Date.now();
  if (!RUN_ID.test(placementRunId) || !["finished", "failed", "never-started"].includes(state)
    || !Number.isSafeInteger(at) || at < 0 || !Number.isFinite(new Date(at).getTime())
    || (options.exitCode !== undefined && (!Number.isSafeInteger(options.exitCode) || options.exitCode < -255 || options.exitCode > 255))
    || (options.reason !== undefined && !REASONS.test(options.reason))) {
    throw new PlacementTerminalWriteError();
  }
  return {
    placementRunId,
    state,
    at: new Date(at).toISOString(),
    ...(options.exitCode !== undefined ? { exitCode: options.exitCode } : {}),
    ...(options.reason !== undefined ? { reason: options.reason } : {}),
  };
};

export const assertPlacementTerminalRecord: (value: unknown) => asserts value is PlacementTerminalRecord = (value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new PlacementTerminalWriteError();
  const candidate = value as Record<string, unknown>;
  if (Reflect.ownKeys(candidate).some(key => typeof key !== "string" || !(keys as readonly string[]).includes(key))
    || !RUN_ID.test(typeof candidate.placementRunId === "string" ? candidate.placementRunId : "")
    || !["finished", "failed", "never-started"].includes(candidate.state as string)
    || typeof candidate.at !== "string" || !Number.isFinite(Date.parse(candidate.at))
    || new Date(candidate.at).toISOString() !== candidate.at
    || (candidate.exitCode !== undefined && (!Number.isSafeInteger(candidate.exitCode) || (candidate.exitCode as number) < -255 || (candidate.exitCode as number) > 255))
    || (candidate.reason !== undefined && (typeof candidate.reason !== "string" || !REASONS.test(candidate.reason)))) {
    throw new PlacementTerminalWriteError();
  }
};

export const placementTerminalPath = (meshRoot: string, hostId: string, placementRunId: string): string => {
  if (!RUN_ID.test(placementRunId)) throw new PlacementTerminalWriteError();
  return path.join(path.dirname(hostLeasePath(meshRoot, hostId)), `placement-terminal-${placementRunId}.json`);
};

/** FILE-carrier receipt in the same host-leases directory as the local owner lease. */
export const writePlacementTerminalFile = (meshRoot: string, hostId: string, value: PlacementTerminalRecord): void => {
  try {
    assertPlacementTerminalRecord(value);
    const file = placementTerminalPath(meshRoot, hostId, value.placementRunId);
    try {
      const stat = fs.lstatSync(file);
      if (stat.isSymbolicLink() || !stat.isFile()) throw new PlacementTerminalWriteError();
      const previous: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
      assertPlacementTerminalRecord(previous);
      if (JSON.stringify(previous) !== JSON.stringify(value)) throw new PlacementTerminalWriteError();
      return;
    } catch (error) {
      if (error instanceof PlacementTerminalWriteError) throw error;
      if ((error as NodeJS.ErrnoException)?.code !== "ENOENT") throw error;
    }
    writeJsonAtomic(file, value, { durable: true, mode: 0o600 });
    const written: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
    assertPlacementTerminalRecord(written);
    if (JSON.stringify(written) !== JSON.stringify(value)) throw new PlacementTerminalWriteError();
  } catch (error) {
    if (error instanceof PlacementTerminalWriteError) throw error;
    throw new PlacementTerminalWriteError(error);
  }
};
