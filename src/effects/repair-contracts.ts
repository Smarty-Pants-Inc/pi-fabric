import type {
  RecoveryEffectsSettlement, RecoveryToolResultRead, SettledRecoveryEffect,
} from "../recovery/l7-interfaces.js";

/** Repair-capable native composition contract; types alone confer no authority. */
export interface B4EffectRecoveryProviderV1 {
  settlePendingIntents(sessionId: string, recoveryGeneration: string): Promise<RecoveryEffectsSettlement>;
  readToolResult(effect: SettledRecoveryEffect): Promise<RecoveryToolResultRead>;
  /** Mandatory for this profile; called after actual join, not input execution permission. */
  repairMaintenance(input: Readonly<NativeAcceptedRecoveryInput>, executionLeafId: string | null): Promise<RepairRef>;
  /** Synchronous authenticated lookup; no provider/effect work in the B5 transaction. */
  resolveRepair(ref: RepairRef): Readonly<RepairReceiptView>;
}

/** Native host AcceptedRecoveryInput shape, distinct from the B5 admission type. */
export interface NativeAcceptedRecoveryInput {
  sessionId: string;
  originalInputId: string;
  admissionId: string;
  deliveryId: string;
  kind: "initial" | "recovery";
  text: string;
  payloadSha256: string;
  deadlineMs: number;
  consumption: { kind: "round" } | { kind: "effect-read"; effectKey: string };
}

export interface B4RepairRequestV1 {
  readonly sessionId: string;
  readonly originalInputId: string;
  readonly recoveryGeneration: string;
}

/** Four-field reference; the original execution anchor A remains immutable. */
export type RepairRef = Readonly<{
  originalInputId: string;
  recoveryGeneration: string;
  executionBranchEntryId: string;
  repairReceiptSha256: string;
}>;

/** Exact serializer evidence: length excludes LF; bytesSha256 includes LF.
 * Provenance requires native flush evidence and the unchanged B6 durable barrier,
 * not merely a sequence number or a structurally matching object. */
export interface B4NativeEvidenceRefV1 {
  readonly entryId: string;
  readonly deliveryId: string;
  readonly recordKind: "carrier";
  readonly file: string;
  readonly offset: number;
  readonly length: number;
  readonly bytesSha256: string;
  readonly b6Sequence: number;
}

export interface B4RepairReceiptV1 extends B4RepairRequestV1 {
  readonly version: 1;
  readonly executionBranchEntryId: string; // A: immutable original anchor
  readonly joinedLeafEntryId: string; // J: actual joined native view (may differ)
  /** Native baseHistorySha256 at J:
   * H(JSON.stringify(branch.map(entry => [entry.id, H(JSON.stringify(entry) + "\n")]))),
   * where H is SHA256 of UTF-8 text as lower-case hex. Preserve branch order and
   * native property order; inner hashes include LF, outer bytes have no LF. */
  readonly joinedHistorySha256: string;
  /** The public ABI does not prescribe a protected-range encoding. */
  readonly protectedRangeSha256: string;
  readonly protectedEntryIds: readonly string[];
  readonly settlements: readonly SettledRecoveryEffect[];
  readonly nativeEntries: readonly B4NativeEvidenceRefV1[];
}

/** Immutable native-validated view, authenticated against retained native/B6 evidence.
 * repairReceiptSha256 hashes UTF-8 canonicalEncode(receipt), with no newline;
 * it is distinct from native message hashes and the separate maintenance view digest. */
export type RepairReceiptView = B4RepairReceiptV1;
