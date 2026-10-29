import path from "node:path";
import { EffectContractError } from "./contracts.js";
import type { RecoveryOwner } from "./contracts.js";
import type { RepairReceiptView } from "./repair-contracts.js";
import { canonicalEncode, sha256Bytes } from "./slots.js";

export interface RepairFrontierEntry {
  readonly key: string;
  readonly sequence: number;
  readonly digest: string;
}

export interface MaintenanceRepairRecordBody {
  readonly format: "fabric.maintenance-repair-receipt";
  readonly version: 1;
  readonly orgId: string;
  readonly logicalSessionId: string;
  readonly originalAdmissionId: string;
  readonly originalPayloadSha256: string;
  readonly originalDeadlineMs: number;
  readonly originalCarrierEntryId: string;
  readonly frontier: readonly RepairFrontierEntry[];
  readonly view: RepairReceiptView;
  readonly repairReceiptSha256: string;
}

export type StoredMaintenanceRepair = MaintenanceRepairRecordBody & { readonly recordSha256: string };

type RepairOwner = Pick<RecoveryOwner, "orgId" | "logicalSessionId" | "sessionFile">;
const HASH = /^[0-9a-f]{64}$/;
const MAX_SEQUENCE = 999_999_999_999;
const BODY_FIELDS = [
  "format", "version", "orgId", "logicalSessionId", "originalAdmissionId", "originalPayloadSha256",
  "originalDeadlineMs", "originalCarrierEntryId", "frontier", "view", "repairReceiptSha256",
] as const;
const VIEW_FIELDS = [
  "version", "sessionId", "originalInputId", "recoveryGeneration", "executionBranchEntryId", "joinedLeafEntryId",
  "joinedHistorySha256", "protectedRangeSha256", "protectedEntryIds", "settlements", "nativeEntries",
] as const;
const NATIVE_FIELDS = [
  "entryId", "deliveryId", "recordKind", "file", "offset", "length", "bytesSha256", "b6Sequence",
] as const;

function invalid(message: string): never {
  throw new EffectContractError("EFFECT_REPAIR_INVALID", message);
}

function shape(value: unknown, required: readonly string[], optional: readonly string[] = []): asserts value is Record<string, unknown> {
  // Callers canonical-validate first: no proxy, accessor, symbol, exotic object,
  // nonenumerable field or lossy JSON value can reach these structural checks.
  if (typeof value !== "object" || value === null || Array.isArray(value)) invalid("Repair fields require an object");
  const allowed = new Set([...required, ...optional]);
  if (Object.keys(value).some(field => !allowed.has(field)) || required.some(field => !Object.hasOwn(value, field))) {
    invalid("Unknown or missing repair field");
  }
}

function text(value: unknown): asserts value is string {
  if (typeof value !== "string" || value.length === 0) invalid("Repair identity must be nonempty Unicode text");
}

function hash(value: unknown): asserts value is string {
  if (typeof value !== "string" || !HASH.test(value)) invalid("Repair digest must be lowercase SHA-256 hex");
}

function integer(value: unknown, minimum = 0): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || Object.is(value, -0)) {
    invalid("Repair coordinates and sequences must be nonnegative safe integers");
  }
}

function array(value: unknown): asserts value is unknown[] {
  if (!Array.isArray(value)) invalid("Repair collection must be an array");
}

function nativeFile(value: unknown): asserts value is string {
  text(value);
  if (!path.isAbsolute(value) || path.normalize(value) !== value || value.includes("\0") || value.includes("\\") ||
      path.parse(value).root === value) invalid("Repair native file must be an exact absolute file path");
}

/** Internal storage key, not a reference hash or an effect key. */
export function maintenanceRepairKey(orgId: string, logicalSessionId: string, originalInputId: string): string {
  canonicalEncode([orgId, logicalSessionId, originalInputId]);
  text(orgId);
  text(logicalSessionId);
  text(originalInputId);
  return sha256Bytes(canonicalEncode([1, orgId, logicalSessionId, originalInputId]));
}

/** Closed-schema storage validation only. Native/B6 provenance cannot be conferred
 * by JSON, hashes, coordinates or sequences; the repair engine qualifies it. */
export function validateMaintenanceRepairBody(value: unknown, owner: RepairOwner): asserts value is MaintenanceRepairRecordBody {
  canonicalEncode(value);
  shape(value, BODY_FIELDS);
  if (value.format !== "fabric.maintenance-repair-receipt" || value.version !== 1 ||
      value.orgId !== owner.orgId || value.logicalSessionId !== owner.logicalSessionId) {
    invalid("Repair format, version or retained owner does not match");
  }
  text(value.orgId);
  text(value.logicalSessionId);
  text(value.originalAdmissionId);
  hash(value.originalPayloadSha256);
  integer(value.originalDeadlineMs, 1);
  text(value.originalCarrierEntryId);
  hash(value.repairReceiptSha256);
  shape(value.view, VIEW_FIELDS);
  const view = value.view;
  if (view.version !== 1 || view.sessionId !== owner.logicalSessionId) invalid("Repair view session/version does not match");
  text(view.sessionId);
  text(view.originalInputId);
  text(view.recoveryGeneration);
  text(view.executionBranchEntryId);
  text(view.joinedLeafEntryId);
  hash(view.joinedHistorySha256);
  hash(view.protectedRangeSha256);
  if (sha256Bytes(canonicalEncode(view)) !== value.repairReceiptSha256) invalid("Repair view digest does not match its canonical bytes");

  array(value.frontier);
  const frontierKeys = new Set<string>();
  let previousKey = "";
  for (const entry of value.frontier) {
    shape(entry, ["key", "sequence", "digest"]);
    hash(entry.key);
    integer(entry.sequence);
    hash(entry.digest);
    if (entry.sequence > MAX_SEQUENCE || entry.key <= previousKey) invalid("Repair frontier must have unique keys in lexical order");
    frontierKeys.add(entry.key);
    previousKey = entry.key;
  }

  array(view.settlements);
  const settledKeys = new Set<string>();
  const aliases = new Set<string>();
  const outcomes = new Map<string, unknown>();
  for (const settlement of view.settlements) {
    shape(settlement, ["toolCallId", "key", "outcome"], ["resultBytesHash"]);
    text(settlement.toolCallId);
    hash(settlement.key);
    if (settlement.outcome !== "committed" && settlement.outcome !== "not-committed") invalid("A repair cannot certify an uncertain outcome");
    if (Object.hasOwn(settlement, "resultBytesHash")) hash(settlement.resultBytesHash);
    if (settlement.outcome === "committed" && !Object.hasOwn(settlement, "resultBytesHash")) invalid("Committed repair settlement requires its native result hash");
    const alias = canonicalEncode([settlement.key, settlement.toolCallId]);
    if (!frontierKeys.has(settlement.key) || aliases.has(alias) ||
        (outcomes.has(settlement.key) && outcomes.get(settlement.key) !== settlement.outcome)) {
      invalid("Repair settlement does not identify a unique, consistently settled frontier alias");
    }
    aliases.add(alias);
    outcomes.set(settlement.key, settlement.outcome);
    settledKeys.add(settlement.key);
  }
  if (settledKeys.size !== frontierKeys.size) invalid("Repair settlements must cover the saved frontier");

  array(view.protectedEntryIds);
  array(view.nativeEntries);
  if (view.protectedEntryIds.length === 0 || view.protectedEntryIds.length !== view.nativeEntries.length) {
    invalid("Repair protected identities and native evidence must be nonempty and aligned");
  }
  nativeFile(owner.sessionFile);
  const ids = new Set<string>();
  const deliveries = new Set<string>();
  const sequences = new Set<number>();
  const range: [string, string][] = [];
  let end = 0;
  for (let index = 0; index < view.nativeEntries.length; index++) {
    const entry = view.nativeEntries[index];
    shape(entry, NATIVE_FIELDS);
    text(entry.entryId);
    text(entry.deliveryId);
    nativeFile(entry.file);
    integer(entry.offset);
    integer(entry.length, 1);
    // B5/B6 sequences are nonnegative; the original carrier may have sequence zero.
    integer(entry.b6Sequence);
    hash(entry.bytesSha256);
    if (entry.recordKind !== "carrier" || entry.file !== owner.sessionFile ||
        entry.entryId !== view.protectedEntryIds[index] || ids.has(entry.entryId) || deliveries.has(entry.deliveryId) ||
        sequences.has(entry.b6Sequence) || entry.offset < end || !Number.isSafeInteger(entry.offset + entry.length + 1)) {
      invalid("Repair native identity, file, range or durable sequence does not match");
    }
    ids.add(entry.entryId);
    deliveries.add(entry.deliveryId);
    sequences.add(entry.b6Sequence);
    range.push([entry.entryId, entry.bytesSha256]);
    end = entry.offset + entry.length + 1;
  }
  if (!ids.has(value.originalCarrierEntryId) || !ids.has(view.joinedLeafEntryId) ||
      sha256Bytes(canonicalEncode(["fabric.b4.protected-range", 1, range])) !== view.protectedRangeSha256) {
    invalid("Repair protected range digest or original/joined carrier identity does not match");
  }
}

/** The envelope hash binds internal metadata/frontier; it is NEVER the public
 * repairReceiptSha256, which hashes only the exact public view. */
export function validateStoredMaintenanceRepair(value: unknown, owner: RepairOwner): asserts value is StoredMaintenanceRepair {
  canonicalEncode(value);
  shape(value, [...BODY_FIELDS, "recordSha256"]);
  hash(value.recordSha256);
  const { recordSha256, ...body } = value;
  validateMaintenanceRepairBody(body, owner);
  if (sha256Bytes(canonicalEncode(body)) !== recordSha256) invalid("Repair storage envelope digest does not match its canonical body");
}
