import type { ContextEvent } from "@earendil-works/pi-coding-agent";

/** Host-owned contracts for the bounded, retained-volume effect profile.
 * None of these capabilities or slot selectors belong in model arguments. */
export type JsonValue = null | boolean | number | string | readonly JsonValue[] | { readonly [key: string]: JsonValue };
export type EffectKeyTuple = readonly [version: 1, orgId: string, logicalSessionId: string,
  acceptedInputId: string, effectSlotId: string, effectKind: string];
export type EffectOutcome = "committed" | "not-committed" | "uncertain";
export type EffectPermission = "new-intent" | "settle";

export interface EffectAttemptIdentity {
  readonly podUID: string;
  readonly bootId: string;
  readonly ownerIncarnation: string;
  readonly authorityRevision: string;
}

/** B3 supplies this only after REQUIRED identity and old-writer exclusion checks.
 * Settlement permission is deliberately separate from starting a new effect.
 * assertLive must be checked again at the sink's actual commit boundary. */
export interface RecoveryOwner {
  readonly orgId: string;
  readonly logicalSessionId: string;
  readonly sessionRoot: string;
  readonly workspaceRoot: string;
  readonly sessionFile: string;
  readonly pvcUID: string;
  readonly exclusionReceipt: string;
  readonly attempt: EffectAttemptIdentity;
  readonly signal: AbortSignal;
  assertLive(permission: EffectPermission): void | Promise<void>;
}

export interface EffectSlot {
  readonly id: string;
  readonly ordinal: number;
  readonly kind: string;
  readonly canonicalRef: string;
  readonly adapterVersion: string;
  readonly canonicalTarget: string;
  readonly predecessors: readonly string[];
  /** Exact authorized validated arguments, not a model-supplied slot ordinal. */
  readonly canonicalArguments: JsonValue;
  readonly constraints: JsonValue;
  /** Optional phase supplied by admission/dispatch, never by the model. */
  readonly phase?: string;
}

export interface EffectManifest {
  readonly version: 1;
  readonly slots: readonly EffectSlot[];
}

/** B5's durable original input association survives continuation native entries. */
export interface AcceptedInput {
  readonly id: string;
  readonly digest: string;
  readonly nativeEntryId: string;
  readonly manifestDigest: string;
  readonly manifest: EffectManifest;
  readonly receivedKey?: string;
  readonly logicalRecipient?: string;
  readonly firstOfferedAt?: string;
  readonly originalDeadlineAt?: string;
  readonly cancellationRef?: string;
  readonly settlementRef?: string;
}

export interface EffectToolIdentity {
  readonly canonicalRef: string;
  readonly adapterVersion: string;
  readonly originalToolCallId: string;
  readonly parentToolCallId?: string;
  readonly nativeAssistantEntryId: string;
}

export interface EffectPayload {
  readonly encoding: "base64";
  readonly bytes: string;
  readonly digest: string;
}

/** Native-format result encoded losslessly as strict JSON, including errors/details. */
export type EffectResult = JsonValue;
export interface PreparedFileEffect {
  readonly preExists: boolean;
  readonly preHash: string | null;
  readonly preMode: number | null;
  readonly postHash: string;
  readonly postMode: number;
  readonly desiredBytes: EffectPayload;
  readonly preparedResult: EffectResult;
}

export interface PreparedPeerEffect {
  readonly logicalRecipient: string;
  readonly immutableBody: JsonValue;
  readonly bodyDigest: string;
  readonly firstOfferedAt: string;
  readonly originalDeadlineAt: string;
  /** Stable B5 command identity; never regenerated using the current host. */
  readonly envelopeIdentity: JsonValue;
}

/** Authoritative immutable binding. Physical call/attempt identity is NOT in K. */
export interface EffectIntent {
  readonly format: "fabric.effect-intent";
  readonly version: 1;
  readonly key: string;
  readonly keyTuple: EffectKeyTuple;
  readonly manifestDigest: string;
  readonly acceptedInput: Pick<AcceptedInput, "id" | "digest" | "nativeEntryId">;
  readonly slot: EffectSlot;
  readonly tool: EffectToolIdentity;
  readonly canonicalArguments: JsonValue;
  readonly payload: EffectPayload;
  readonly canonicalTarget: string;
  readonly file?: PreparedFileEffect;
  readonly message?: PreparedPeerEffect;
  /** Adapter-owned immutable preparation (fixed argv, file identity/size etc.).
   * Sink validates it against canonical target/args/payload and the bound roots. */
  readonly adapterData?: JsonValue;
  readonly originAttempt: EffectAttemptIdentity;
}

export type EffectTransitionState = "INTENT" | "ATTEMPT" | "ALIAS" | "RESULT" | "UNCERTAIN";
export interface EffectTransitionInput {
  readonly state: EffectTransitionState;
  readonly toolCallId: string;
  readonly parentToolCallId?: string;
  readonly attempt: EffectAttemptIdentity;
  readonly at: string;
  readonly recoveryGeneration?: string;
  readonly outcome?: EffectOutcome;
  readonly result?: EffectResult;
  /** Ledger integrity: SHA-256(canonicalEncode(result)), NOT a joined native hash. */
  readonly resultBytesHash?: string;
  readonly receipt?: JsonValue;
  readonly reason?: string;
}
export interface EffectTransition extends EffectTransitionInput {
  readonly format: "fabric.effect-transition";
  readonly version: 1;
  readonly key: string;
  readonly sequence: number;
  readonly previousDigest: string;
  readonly digest: string;
}

export interface EffectRecord {
  readonly intent: EffectIntent;
  readonly intentDigest: string;
  readonly transitions: readonly EffectTransition[];
}

export interface EffectSinkContext {
  readonly owner: RecoveryOwner;
  readonly recoveryGeneration: string;
}
/** A sink must query/dedupe or prove the recorded pre/post state; a negative
 * stale read alone never authorizes retry. It receives only stored intent bytes. */
export interface EffectSink {
  readonly kind: string;
  readonly adapterVersion: string;
  reconcile(intent: EffectIntent, context: EffectSinkContext): Promise<EffectSinkResult>;
}
export interface EffectSinkResult {
  readonly outcome: EffectOutcome;
  readonly result?: EffectResult;
  readonly resultBytesHash?: string;
  readonly receipt?: JsonValue;
  readonly reason?: string;
}

/** B3 settlement shape. toolCallId is a physical-call alias, never K.
 * The certified bridge projects exact JSON.stringify native hashes after joining;
 * the low-level ledger reconciler retains canonical adapter-original hashes. */
export interface SettledEffect {
  readonly toolCallId: string;
  readonly key: string;
  readonly outcome: EffectOutcome;
  /** Joined native entry: full UTF-8 JSON.stringify(message), no sorting/LF,
   * including this alias's ID/timestamp. Without a join, identifies only the
   * canonical adapter original, which still requires live native processing. */
  readonly resultBytesHash?: string;
}
export interface SettlePendingIntentsResult {
  readonly settled: SettledEffect[];
  readonly uncertain: number;
}
export type EffectToolResult = Extract<ContextEvent["messages"][number], { role: "toolResult" }>;
export type ReadToolResult = (effect: SettledEffect) => Promise<EffectToolResult>;
export type SettlePendingIntents = (sessionId: string, recoveryGeneration: string) => Promise<SettlePendingIntentsResult>;
/** Additive caller API, not a change to the frozen provider ABI. The caller
 * supplies this original's opaque generation; B4 has no pin lookup policy. */
export type SettleOriginalPendingIntents = (sessionId: string, originalInputId: string,
  recoveryGeneration: string) => Promise<SettlePendingIntentsResult>;

export interface EffectRepair {
  readonly key: string;
  readonly toolCallId: string;
  readonly parentToolCallId?: string;
  readonly argumentsDigest: string;
  readonly outcome: EffectOutcome;
  readonly result?: EffectResult;
  /** Canonical ledger-original hash, not native-entry materialization evidence. */
  readonly resultBytesHash?: string;
}
export interface ReconcilePendingResult extends SettlePendingIntentsResult {
  readonly unresolved: readonly { readonly key: string; readonly reason: string }[];
  readonly repairs: readonly EffectRepair[];
  readonly ledgerFrontier: readonly { readonly key: string; readonly sequence: number; readonly digest: string }[];
}

export class EffectContractError extends Error {
  constructor(readonly code: string, message: string, readonly original?: {
    readonly key: string; readonly state: string; readonly result?: EffectResult;
  }) {
    super(message);
    this.name = "EffectContractError";
  }
}
