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

/** Release refusals keep a stable code/reason without exposing receipt contents. */
export type NatsDrainUnprovenReason =
  | "ack-pending" | "dispositions" | "stream" | "timeline" | "missing-stream"
  | "incomplete" | "host" | "stale" | "consumer" | "malformed";

export class FabricNatsDrainUnprovenError extends Error {
  override readonly name = "FabricNatsDrainUnprovenError";
  readonly code = "FABRIC_NATS_DRAIN_UNPROVEN";
  constructor(readonly reason: NatsDrainUnprovenReason) {
    super(`NATS control has an unproven drain receipt; retain A (${reason})`);
  }
}

/** Host-owned release authority; never derive the expected stream from receipt/caller data. */
export interface NatsDrainReleaseExpectation {
  hostId: string;
  writtenAt: number | undefined;
  expectedNatsInboxStream?: string;
  /** Host clock snapshot; defaults to Date.now() for direct assertion callers. */
  now?: number;
}

// ponytail: approved five-minute future bound, copied locally from src/actors/dead-root-filter.ts:30; no lazy import edge.
const NATS_DRAIN_FUTURE_TOLERANCE_MS = 5 * 60_000;

/**
 * Release proof is stricter than schema v1's writer declaration. In particular,
 * historical consumer:null disposition receipts cannot bind a stream or prove a
 * remaining-message count and now refuse release, while remaining readable/writable.
 * E20 canary rollback uses 0/0 + []; counted disposition proofs remain supported.
 * The approved local future bound allows five minutes of clock skew.
 */
export function assertNatsDrainReceiptForRelease(
  receipt: unknown,
  expected: NatsDrainReleaseExpectation,
): asserts receipt is NatsDrainReceipt {
  const refuse = (reason: NatsDrainUnprovenReason): never => {
    throw new FabricNatsDrainUnprovenError(reason);
  };
  const stream = expected.expectedNatsInboxStream;
  if (stream === undefined) refuse("missing-stream");
  // Same machine literal grammar as the transport, without importing lazy NATS code.
  if (typeof stream !== "string" || !stream.startsWith("FABRIC_IN_") ||
      !/^[a-z0-9][a-z0-9-]{0,31}$/.test(stream.slice("FABRIC_IN_".length)) ||
      stream !== stream.trim()) refuse("stream");
  if (!isNatsDrainReceipt(receipt)) return refuse("malformed");
  if (receipt.complete !== true) refuse("incomplete");
  if (receipt.hostId !== expected.hostId) refuse("host");
  const writtenAt = expected.writtenAt;
  if (!isNonnegativeNumber(writtenAt)) return refuse("malformed");
  const now = expected.now ?? Date.now();
  if (!Number.isFinite(now) || receipt.flippedAt > receipt.completedAt ||
      receipt.completedAt > now + NATS_DRAIN_FUTURE_TOLERANCE_MS) refuse("timeline");
  if (receipt.completedAt < writtenAt) refuse("stale");
  const consumer = receipt.consumer;
  if (consumer === null) return refuse("consumer");
  if (consumer.stream !== stream) refuse("stream");
  if (!Number.isSafeInteger(consumer.numPending) || !Number.isSafeInteger(consumer.numAckPending)) refuse("malformed");
  if (consumer.numAckPending !== 0) refuse("ack-pending");
  const ids = new Set(receipt.dispositions.map(entry => entry.commandEventId));
  if (ids.size !== receipt.dispositions.length || ids.size !== consumer.numPending) refuse("dispositions");
}

/** Missing, unreadable, torn, unknown-version or malformed receipts are absent.
 * Release callers must use assertNatsDrainReceiptForRelease for semantic proof. */
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
