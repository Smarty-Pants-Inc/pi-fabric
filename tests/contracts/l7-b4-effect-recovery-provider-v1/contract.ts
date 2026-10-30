import type {
  RecoveryEffectsSettlement, RecoveryToolResultRead, SettledRecoveryEffect,
} from "../../../src/recovery/l7-interfaces.js";

/** K1 TEST-ONLY ABI. Not a production export or proof of native/B6 authority.
 * Existing seam: src/recovery/l7-interfaces.ts:243-246.
 * Receipt additions: R3 §7.1; their spelling/encoding is frozen here, not
 * represented as an existing implementation. See B4-CONTRACT.md. */
export interface B4EffectRecoveryProviderV1 {
  settlePendingIntents(sessionId: string, recoveryGeneration: string): Promise<RecoveryEffectsSettlement>;
  readToolResult(effect: SettledRecoveryEffect): Promise<RecoveryToolResultRead>;
  /** Existing Pi host seam (cf3cf677 session-recovery-control.ts:331).
   * Mandatory for this profile; optional on Pi's general SessionRecoveryBinding.
   * Called after actual join; leaf is an observation, never caller authority. */
  repairMaintenance(input: Readonly<AcceptedRecoveryInput>, executionLeafId: string | null): Promise<RepairRef>;
  /** R3-required synchronous lookup: no provider/effect work in B5 transaction. */
  resolveRepair(ref: RepairRef): Readonly<RepairReceiptView>;
}

/** Exact mirror of Pi cf3cf677 session-recovery-control.ts:26-37. */
export interface AcceptedRecoveryInput {
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

/** Exactly B5 maintenance-admission.ts:15-18 (6993174850bf). A is immutable. */
export type RepairRef = Readonly<{
  originalInputId: string;
  recoveryGeneration: string;
  executionBranchEntryId: string;
  repairReceiptSha256: string;
}>;

/** Exact serializer bytes, including LF, and the unchanged B6 carrier barrier.
 * Native descriptor semantics: src/recovery/l7-interfaces.ts:111-179. */
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
  /** Pi cf3cf677 agent-session.ts:1757-1768 baseHistorySha256 at J:
   * H(JSON.stringify(branch.map(entry => [entry.id, H(JSON.stringify(entry) + "\n")]))),
   * where H(text) is SHA256 of UTF-8 text as lower-case hex.
   * Preserve branch order and native property order; no canonical sorting. */
  readonly joinedHistorySha256: string;
  /** Encoding OPEN: native/B4 producer + fv2-l7-main, due 2026-10-30 12:00Z.
   * The fixture's encoding is not a production prescription. */
  readonly protectedRangeSha256: string;
  readonly protectedEntryIds: readonly string[];
  readonly settlements: readonly SettledRecoveryEffect[];
  readonly nativeEntries: readonly B4NativeEvidenceRefV1[];
}

/** Immutable native-validated view, not authority merely by JSON shape. Real
 * resolveRepair authenticates the retained native/B6 provenance first. Its hash
 * is SHA256(canonicalEncode(receipt)), not a native JSON.stringify message hash.
 * Test fakes return structurally identical views with NO production authority. */
export type RepairReceiptView = B4RepairReceiptV1;

/** NEW receipt errors reuse B5 meanings; settlement/read errors retain B4 codes.
 * Other owner/I/O errors propagate; this is not a closed inventory of OS errors. */
export type B4ProviderErrorCodeV1 =
  | "EFFECT_SESSION_MISMATCH" | "EFFECT_RECOVERY_GENERATION"
  | "EFFECT_OWNER_MISMATCH" | "EFFECT_OWNER_STALE" | "EFFECT_SESSION_UNBOUND"
  | "EFFECT_SESSION_ALREADY_BOUND" | "EFFECT_SINK_AMBIGUOUS"
  | "EFFECT_STORE_UNCERTAIN" | "EFFECT_LEDGER_CORRUPT"
  | "EFFECT_SLOT_CONFLICT" | "EFFECT_RESULT_UNAVAILABLE"
  | "EFFECT_CALL_ALIAS_MISMATCH" | "EFFECT_RESULT_OUTCOME_MISMATCH"
  | "EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR" | "EFFECT_NATIVE_RESULT_INVALID"
  | "EFFECT_ADAPTER_RESULT_INVALID" | "EFFECT_SINK_RESULT" | "EFFECT_RESULT_HASH"
  | "RECOVERY_CONFLICT" | "RECOVERY_HISTORY_INCOMPLETE"
  | "RECOVERY_MAINTENANCE_REPAIR_REQUIRED";

export type { RecoveryEffectsSettlement, RecoveryToolResultRead, SettledRecoveryEffect };
