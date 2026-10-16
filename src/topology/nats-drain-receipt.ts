import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";

/** E17 local custody receipt. Completeness is the rollback writer's declaration. */
export interface NatsDrainReceipt {
  version: 1;
  hostId: string;
  flippedAt: number;
  completedAt: number;
  complete: boolean;
  consumer: {
    stream: string;
    numPending: number;
    numAckPending: number;
  } | null;
  dispositions: Array<{
    commandEventId: string;
    disposition: "relay" | "completed";
  }>;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isNonemptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0;

const isNonnegativeNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value) && value >= 0;

const isCount = (value: unknown): value is number =>
  isNonnegativeNumber(value) && Number.isInteger(value);

/** Shape only: no message manifest exists here to prove disposition coverage. */
const isNatsDrainReceipt = (value: unknown): value is NatsDrainReceipt => {
  if (!isObject(value) || value.version !== 1 ||
      !isNonemptyString(value.hostId) ||
      !isNonnegativeNumber(value.flippedAt) ||
      !isNonnegativeNumber(value.completedAt) ||
      typeof value.complete !== "boolean" ||
      !Array.isArray(value.dispositions)) return false;

  const consumer = value.consumer;
  if (consumer !== null && (!isObject(consumer) ||
      !isNonemptyString(consumer.stream) ||
      !isCount(consumer.numPending) ||
      !isCount(consumer.numAckPending))) return false;

  for (const entry of value.dispositions) {
    if (!isObject(entry) || !isNonemptyString(entry.commandEventId) ||
        (entry.disposition !== "relay" && entry.disposition !== "completed")) return false;
  }
  return true;
};

/** storeDir is the host's control-seen store directory, not its parent. */
export const natsDrainReceiptPath = (storeDir: string): string =>
  path.join(storeDir, "nats-drain-receipt.json");

/** Missing, unreadable, torn, unknown-version or malformed receipts are absent.
 * Release callers must additionally check complete, hostId and completedAt freshness. */
export const readNatsDrainReceipt = (storeDir: string): NatsDrainReceipt | undefined => {
  try {
    const receipt: unknown = JSON.parse(fs.readFileSync(natsDrainReceiptPath(storeDir), "utf8"));
    return isNatsDrainReceipt(receipt) ? receipt : undefined;
  } catch {
    return undefined;
  }
};

/** Validate before I/O, then reuse the native temp/rename and durability barriers. */
export const writeNatsDrainReceipt = (storeDir: string, receipt: NatsDrainReceipt): void => {
  if (!isNatsDrainReceipt(receipt)) throw new TypeError("Invalid NATS drain receipt schema v1");
  writeJsonAtomic(natsDrainReceiptPath(storeDir), receipt, { durable: true });
};
