import { randomUUID } from "node:crypto";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { readHandoverJson, HANDOVER_DRAIN_MS, type ResidentLaunchSpec, type ResidentLauncherIdentity,
  type ResidentMainGeneration } from "./handover.js";
import type { ResidentActorCaller } from "./protocol.js";

export const RESIDENT_DRAIN_ABI = "fabric-resident-drain-1" as const;
export class ResidentDrainRefusedError extends Error {
  constructor(readonly code: string, reason: string) { super(reason); this.name = "ResidentDrainRefusedError"; }
}
export interface ResidentDrainIntent {
  caller: ResidentActorCaller;
  main: ResidentMainGeneration;
  target: ResidentLaunchSpec;
  expected: {
    releaseRoot: string;
    owner: { pid: number; processStartTime: string; token: string; hostId: string };
    launcher: ResidentLauncherIdentity;
    configDigest: string;
  };
  timeoutMs?: number;
}
export interface ResidentDrainReceipt {
  abi: typeof RESIDENT_DRAIN_ABI;
  id: string;
  rootId: string;
  owner: ResidentDrainIntent["expected"]["owner"];
  launcher: ResidentLauncherIdentity;
  previous: ResidentLaunchSpec;
  target: ResidentLaunchSpec;
  main: ResidentMainGeneration;
  phase: "refused" | "held" | "checkpointed" | "exited" | "target-ready" | "serving" | "rolled-back" | "needs-operator";
  outcome: "held" | "checkpointed" | "containment-unavailable" | "resumed" | "refused" | "needs-operator";
  code: string;
  reason: string;
  at: number;
  deadlineAt: number;
  admissionHeld: boolean;
  /** Terminal means this operation will never transfer custody or start B. */
  terminal?: boolean;
  resumedAt?: number;
  checkpoint?: unknown;
  checkpointReadback?: boolean;
  quiet?: unknown;
}

export function residentDrainReceiptPath(root: string, id: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
    throw new Error("Invalid resident drain operation ID");
  }
  return path.join(root, `drain-${id}.json`);
}
export function residentDrainDeadline(timeoutMs: number | undefined, now = Date.now()): number {
  const timeout = timeoutMs ?? HANDOVER_DRAIN_MS;
  if (!Number.isSafeInteger(timeout) || timeout <= 0 || timeout > HANDOVER_DRAIN_MS) {
    throw new Error(`Resident drain timeout must be an integer from 1 to ${HANDOVER_DRAIN_MS} ms`);
  }
  return now + timeout;
}
/** Compare authority fields explicitly; JSON object insertion order grants no authority. */
export function sameDrainOwner(a: ResidentDrainReceipt["owner"], b: ResidentDrainReceipt["owner"]): boolean {
  return !!a && !!b && a.pid === b.pid && a.processStartTime === b.processStartTime &&
    a.token === b.token && a.hostId === b.hostId;
}
export function sameDrainLauncher(a: ResidentLauncherIdentity, b: ResidentLauncherIdentity): boolean {
  return !!a && !!b && a.pid === b.pid && a.processStartTime === b.processStartTime &&
    a.token === b.token && a.entry === b.entry && a.runtime === b.runtime;
}
export function readResidentDrainReceipt(root: string, id: string, expected?: ResidentDrainReceipt): ResidentDrainReceipt | undefined {
  const receipt = readHandoverJson<ResidentDrainReceipt>(residentDrainReceiptPath(root, id));
  if (!receipt) return undefined;
  if (receipt.abi !== RESIDENT_DRAIN_ABI || receipt.id !== id || !Number.isFinite(receipt.at) ||
      !Number.isFinite(receipt.deadlineAt) || typeof receipt.admissionHeld !== "boolean" ||
      !["refused", "held", "checkpointed", "exited", "target-ready", "serving", "rolled-back", "needs-operator"].includes(receipt.phase) ||
      !["held", "checkpointed", "containment-unavailable", "resumed", "refused", "needs-operator"].includes(receipt.outcome) ||
      (expected && (receipt.rootId !== expected.rootId || receipt.deadlineAt !== expected.deadlineAt ||
        !sameDrainOwner(receipt.owner, expected.owner) || !sameDrainLauncher(receipt.launcher, expected.launcher) ||
        JSON.stringify(receipt.previous) !== JSON.stringify(expected.previous) ||
        JSON.stringify(receipt.target) !== JSON.stringify(expected.target) || JSON.stringify(receipt.main) !== JSON.stringify(expected.main)))) {
    throw new Error("Resident drain durable receipt binding changed");
  }
  return receipt;
}
/** Receipt publication is durable, and its exact bytes/identity are read back before success. */
export function writeResidentDrainReceipt(root: string, receipt: ResidentDrainReceipt): ResidentDrainReceipt {
  writeJsonAtomic(residentDrainReceiptPath(root, receipt.id), receipt, { durable: true });
  const readback = readResidentDrainReceipt(root, receipt.id, receipt);
  if (JSON.stringify(readback) !== JSON.stringify(receipt)) throw new Error("Resident drain receipt readback failed");
  return readback!;
}
export function newResidentDrainReceipt(input: Omit<ResidentDrainReceipt, "abi" | "id">): ResidentDrainReceipt {
  return structuredClone({ ...input, abi: RESIDENT_DRAIN_ABI, id: randomUUID() });
}
