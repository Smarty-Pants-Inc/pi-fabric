import path from "node:path";
import type {
  NativeRecoverySessionRef, NativeRecoveryTransfer, NativeRecoveryOccurrence,
  NativeRecoveryCustodyReader, NativeRecoveryCustodyResult,
} from "./session-receipts.js";

const recoveryObject = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const recoveryString = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const recoveryRef = (value: unknown): value is NativeRecoverySessionRef => {
  const ref = recoveryObject(value);
  return !!ref && recoveryString(ref.sessionId) && !ref.sessionId.startsWith("session:") &&
    recoveryString(ref.sessionFile) && path.isAbsolute(ref.sessionFile) && path.normalize(ref.sessionFile) === ref.sessionFile;
};
const sameRecoveryRef = (a: NativeRecoverySessionRef, b: NativeRecoverySessionRef): boolean =>
  a.sessionId === b.sessionId && a.sessionFile === b.sessionFile;

/** Revalidate at each decision. This checks the adapter shape, NOT native journal durability:
 * only the supplied native accessor can establish canonical headers, digests and barriers. */
export const readNativeRecoveryCustody = (
  scope: NativeRecoverySessionRef, reader: NativeRecoveryCustodyReader,
): NativeRecoveryCustodyResult => {
  try {
    if (!recoveryRef(scope)) throw new Error("Invalid native recovery session scope");
    const result = reader(scope);
    if (result?.status === "none") return { status: "none" };
    if (result?.status === "hold" && recoveryString(result.reason)) return result;
    if (result?.status !== "validated" || !Array.isArray(result.transfers) || !result.transfers.length) {
      throw new Error("Invalid native recovery custody projection");
    }
    const operations = new Set<string>(), sessionIds = new Set<string>(), files = new Set<string>();
    let previous: NativeRecoveryTransfer | undefined;
    for (const transfer of result.transfers) {
      if (!transfer || transfer.version !== 1 || !recoveryString(transfer.operationId) ||
        operations.has(transfer.operationId) || !/^[a-f0-9]{64}$/.test(transfer.preparedDigest) ||
        !recoveryString(transfer.preparedEntryId) || !recoveryRef(transfer.source) || !recoveryRef(transfer.target) ||
        (transfer.phase !== "prepared" && transfer.phase !== "received") ||
        (transfer.phase === "received" ? !recoveryString(transfer.receivedEntryId) : transfer.receivedEntryId !== undefined) ||
        !Array.isArray(transfer.occurrences)) throw new Error("Invalid native recovery binding");
      if (previous && (previous.phase !== "received" || !sameRecoveryRef(previous.target, transfer.source))) {
        throw new Error("Partial or conflicting native recovery chain");
      }
      if (!previous) { sessionIds.add(transfer.source.sessionId); files.add(transfer.source.sessionFile); }
      if (sessionIds.has(transfer.target.sessionId) || files.has(transfer.target.sessionFile)) throw new Error("Cyclic native recovery chain");
      sessionIds.add(transfer.target.sessionId); files.add(transfer.target.sessionFile); operations.add(transfer.operationId);
      const ordinals = new Set<number>();
      for (const occurrence of transfer.occurrences) {
        if (!occurrence || !Number.isSafeInteger(occurrence.ordinal) || occurrence.ordinal < 0 || ordinals.has(occurrence.ordinal) ||
          !["pending", "appended", "handled", "ambiguous"].includes(occurrence.state) ||
          (transfer.phase === "prepared" && occurrence.state !== "pending")) throw new Error("Invalid native recovery occurrence");
        ordinals.add(occurrence.ordinal);
        if (occurrence.customMessage !== undefined) {
          if (!recoveryString(occurrence.customMessage?.customType)) throw new Error("Invalid native custom-message projection");
          nativeRecoveryCarrierIdentities(occurrence.customMessage);
        }
      }
      previous = transfer;
    }
    if (!sameRecoveryRef(scope, previous!.source) && !sameRecoveryRef(scope, previous!.target)) {
      throw new Error("Native recovery projection belongs to another session");
    }
    return result;
  } catch (error) {
    return { status: "hold", reason: `Native recovery custody unavailable: ${error instanceof Error ? error.message : String(error)}` };
  }
};

export interface NativeRecoveryCarrierIdentities {
  carrierIds: readonly string[];
  deliveryIds: readonly string[];
  inboxIds: readonly string[];
}

/** Original identities only. Receipt keys are kept opaque; no text, coalesce keys, nonce
 * or arbitrary data fields establish membership or shown-content authority. */
export const nativeRecoveryCarrierIdentities = (
  message: { customType: string; details: unknown } | undefined,
): NativeRecoveryCarrierIdentities => {
  const carrierIds = new Set<string>(), deliveryIds = new Set<string>(), inboxIds = new Set<string>();
  if (!message || !["pi-fabric-agent-message", "pi-fabric-inbox"].includes(message.customType)) {
    return { carrierIds: [], deliveryIds: [], inboxIds: [] };
  }
  const details = recoveryObject(message.details);
  if (!details) throw new Error("Missing native Fabric carrier details");
  const one = (set: Set<string>, value: unknown): void => {
    if (value === undefined) return;
    if (!recoveryString(value)) throw new Error("Invalid native Fabric identity");
    set.add(value);
  };
  const many = (set: Set<string>, value: unknown): void => {
    if (value === undefined) return;
    if (!Array.isArray(value)) throw new Error("Invalid native Fabric identities");
    for (const id of value) one(set, id);
  };
  if (message.customType === "pi-fabric-inbox") {
    many(inboxIds, details.ids); many(inboxIds, details.receipts);
  } else {
    const visit = (value: unknown): void => {
      const item = recoveryObject(value);
      if (!item) throw new Error("Invalid native Fabric child carrier");
      one(carrierIds, item.id); one(carrierIds, item.chain);
      one(deliveryIds, item.deliveryId); many(deliveryIds, item.supersedes);
      one(inboxIds, item.inboxEventId); many(inboxIds, item.inboxReceipts); many(inboxIds, item.receipts);
      if (item.items !== undefined) {
        if (!Array.isArray(item.items)) throw new Error("Invalid native Fabric batch");
        for (const child of item.items) visit(child);
      }
    };
    visit(details);
  }
  return { carrierIds: [...carrierIds], deliveryIds: [...deliveryIds], inboxIds: [...inboxIds] };
};

export interface NativeRecoveryCarrierIdentity {
  carrierId?: string | undefined;
  chain?: string | undefined;
  deliveryId?: string | undefined;
  inboxId?: string | undefined;
}
export interface NativeRecoveryCarrierCustody {
  status: "none" | "hold" | "reserved" | "received" | "consumed";
  reason?: string;
  messageId?: string;
  transfer?: NativeRecoveryTransfer;
  occurrence?: NativeRecoveryOccurrence;
}

/** Later edges may carry the same still-pending original. A consumed predecessor cannot
 * become pending again, and two occurrences on one edge cannot claim the same identity. */
export const nativeRecoveryCarrierCustody = (
  result: NativeRecoveryCustodyResult, identity: NativeRecoveryCarrierIdentity,
): NativeRecoveryCarrierCustody => {
  if (result.status === "hold") return { status: "hold", reason: result.reason };
  if (result.status === "none") return { status: "none" };
  let found: NativeRecoveryCarrierCustody = { status: "none" };
  for (const transfer of result.transfers) {
    let matched = false;
    for (const occurrence of transfer.occurrences) {
      const ids = nativeRecoveryCarrierIdentities(occurrence.customMessage);
      if (!(identity.carrierId && ids.carrierIds.includes(identity.carrierId)) &&
        !(identity.chain && ids.carrierIds.includes(identity.chain)) &&
        !(identity.deliveryId && ids.deliveryIds.includes(identity.deliveryId)) &&
        !(identity.inboxId && ids.inboxIds.includes(identity.inboxId))) continue;
      if (matched || found.status === "consumed" || found.status === "hold") {
        return { status: "hold", reason: "Conflicting native Fabric occurrence identities" };
      }
      matched = true;
      let messageId = ids.carrierIds[0];
      const findChild = (value: unknown): void => {
        const item = recoveryObject(value);
        if (!item) return;
        if (recoveryString(item.id) && ((identity.carrierId && item.id === identity.carrierId) ||
          (identity.deliveryId && (item.deliveryId === identity.deliveryId ||
            (Array.isArray(item.supersedes) && item.supersedes.includes(identity.deliveryId)))))) messageId = item.id;
        if (Array.isArray(item.items)) for (const child of item.items) findChild(child);
      };
      findChild(occurrence.customMessage?.details);
      found = { status: occurrence.state === "ambiguous" ? "hold" : transfer.phase === "prepared" ? "reserved" :
        occurrence.state === "pending" ? "received" : "consumed", transfer, occurrence,
        ...(messageId === undefined ? {} : { messageId }),
        ...(occurrence.state === "ambiguous" ? { reason: "Native recovery dispatch outcome is ambiguous" } : {}) };
    }
  }
  return found;
};
