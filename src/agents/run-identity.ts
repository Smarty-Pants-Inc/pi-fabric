import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { processStartTime } from "../residency/process-identity.js";

/** A launch receipt, not a capability. Never put it in the child's writable scratch. */
export const RUN_IDENTITY_FILE = "run-identity.json";

export interface RunIdentity {
  format: 1;
  runId: string;
  runRoot: string;
  runDirectory: string;
  temporaryDirectory?: string;
  actorId?: string;
  mainAgentId?: string;
  meshRoot?: string;
  owner: { kind: "main" | "residency" | "actor" | "agent" | "process"; id: string };
  supervisor: { pid: number; processStartTime?: string };
  createdAt: number;
}

/** Main and residency use the same receipt; neither run nor scratch layout is an API. */
export const writeRunIdentity = (input: Omit<RunIdentity, "format" | "supervisor" | "createdAt">): void => {
  const started = processStartTime(process.pid);
  const identity: RunIdentity = {
    ...input,
    format: 1,
    runRoot: fs.realpathSync(input.runRoot),
    runDirectory: fs.realpathSync(input.runDirectory),
    ...(input.temporaryDirectory ? { temporaryDirectory: fs.realpathSync(input.temporaryDirectory) } : {}),
    ...(input.meshRoot ? { meshRoot: path.resolve(input.meshRoot) } : {}),
    supervisor: { pid: process.pid, ...(started ? { processStartTime: started } : {}) },
    createdAt: Date.now(),
  };
  writeJsonAtomic(path.join(identity.runDirectory, RUN_IDENTITY_FILE), identity);
};
