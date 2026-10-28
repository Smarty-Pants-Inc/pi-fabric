import {
  RecoveryAttemptLedgerError,
  type RecoveryAttemptEventRequest, type RecoveryAttemptLedger, type RecoveryEvidenceBytes,
} from "./attempt-ledger.js";
import type {
  RecoveryDurabilityAdapter, RecoveryDurableAppendResult,
  RecoveryNativeEntryRegistrationRequest, RecoveryNativeEntryRegistrationResult,
} from "./l7-interfaces.js";
import { RecoveryJournalError } from "./recovery-journal.js";

export interface RecoveryAttemptCompletionRequest {
  /** Host-captured raw outcome, BEFORE any native tool_result transformation. */
  readonly event: RecoveryAttemptEventRequest & { readonly result: RecoveryEvidenceBytes };
  /** Native serializer's descriptor captured after append/flush and persistenceReleased.
   * Its deadline/signal are the ORIGINAL input/lifecycle budget, not a new retry budget. */
  readonly nativeEntry: RecoveryNativeEntryRegistrationRequest;
  /** Optional explicit joins resolved by the host from the original call. Actual native
   * toolResult has no acceptedInputId/effectKey/attemptId fields: never infer these from
   * tool details, message text, timestamps, parentId or the native deliveryId. */
  readonly nativeCorrelation?: {
    readonly acceptedInputId?: string;
    readonly effectKey?: string;
    readonly attemptId?: string;
  };
}

type AttemptFailure = RecoveryAttemptLedgerError["reason"] | RecoveryJournalError["reason"];
type RegistrationFailure = Extract<RecoveryNativeEntryRegistrationResult, { registered: false }>["reason"];
type BarrierFailure = Extract<RecoveryDurableAppendResult, { durable: false }>["reason"];
export type RecoveryAttemptCompletionResult =
  | { readonly ackEligible: true; readonly attemptSeq: number; readonly barrierSeq: number }
  | { readonly ackEligible: false; readonly stage: "attempt"; readonly reason: AttemptFailure }
  | { readonly ackEligible: false; readonly stage: "registration"; readonly reason: RegistrationFailure; readonly attemptSeq: number }
  | { readonly ackEligible: false; readonly stage: "barrier"; readonly reason: BarrierFailure; readonly attemptSeq: number; readonly barrierSeq?: number };

const evidence = (value: RecoveryEvidenceBytes): RecoveryEvidenceBytes => ({ bytes: Uint8Array.from(value.bytes), sha256: value.sha256 });
function snapshotEvent(value: RecoveryAttemptCompletionRequest["event"]): RecoveryAttemptCompletionRequest["event"] {
  if (!value.result || !(value.result.bytes instanceof Uint8Array) || value.result.bytes.length === 0) {
    throw new RecoveryAttemptLedgerError("invalid-evidence", "Completion requires the exact raw result bytes");
  }
  return {
    identity: { ...value.identity }, eventId: value.eventId, observedAt: value.observedAt, state: value.state,
    result: evidence(value.result), ...(value.receipt ? { receipt: evidence(value.receipt) } : {}),
    traces: value.traces.map(trace => ({ ...trace, bytes: Uint8Array.from(trace.bytes), storage: { ...trace.storage } })),
  };
}

const boundedReason = (bounds: { readonly deadlineMs: number; readonly signal?: AbortSignal }): "aborted" | "deadline" | undefined =>
  bounds.signal?.aborted ? "aborted" : !Number.isFinite(bounds.deadlineMs) || Date.now() >= bounds.deadlineMs ? "deadline" : undefined;
const storeFailure = (error: unknown): RecoveryJournalError["reason"] => error instanceof RecoveryJournalError ? error.reason : "store-unavailable";
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);

/** Parse ONLY the supplied serializer bytes, never the native file. Raw adapter results
 * need not equal post-hook native bytes. `details` has no universal input/effect schema. */
function matches(event: RecoveryAttemptEventRequest, native: RecoveryNativeEntryRegistrationRequest, joins: RecoveryAttemptCompletionRequest["nativeCorrelation"]): boolean {
  if (native.sessionId !== event.identity.sessionId || native.recordKind !== "carrier" ||
    joins?.acceptedInputId !== undefined && joins.acceptedInputId !== event.identity.acceptedInputId ||
    joins?.effectKey !== undefined && joins.effectKey !== event.identity.effectKey ||
    joins?.attemptId !== undefined && joins.attemptId !== event.identity.attemptId) return false;
  try {
    if (native.bytes.at(-1) !== 10) return false;
    const entry: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(native.bytes.subarray(0, -1)));
    if (!object(entry) || entry.type !== "message" || entry.id !== native.entryId || !object(entry.message)) return false;
    const message = entry.message;
    return message.role === "toolResult" && message.toolCallId === event.identity.toolCallId &&
      typeof message.toolName === "string" && message.toolName.length > 0 && typeof message.isError === "boolean" &&
      typeof message.timestamp === "number" && Number.isFinite(message.timestamp) && Array.isArray(message.content) &&
      message.content.every(part => object(part) && (part.type === "text" ? typeof part.text === "string" :
        part.type === "image" && typeof part.data === "string" && typeof part.mimeType === "string"));
  } catch { return false; }
}

/** Host POST-RELEASE drain only: do not call/await this from native persistence or terminal
 * queues. No tools, ACK callbacks, input admission, replay, business mutation or native-file
 * I/O. The caller alone sends an actual completion ACK after ackEligible:true, within its
 * original budget. This is storage eligibility, NOT proof that the business effect succeeded.
 *
 * The raw event is appended FIRST, even when the descriptor is invalid. Later refusal or a
 * caller ACK failure never erases evidence or claims a committed business effect was undone.
 * An append that throws/finishes late may itself have committed: absence of a returned seq
 * is not proof of absence. Reconcile the SAME immutable event/descriptor, never a new key. */
export async function persistRecoveryAttemptCompletion(
  ledger: Pick<RecoveryAttemptLedger, "appendEvent">,
  durability: RecoveryDurabilityAdapter,
  request: RecoveryAttemptCompletionRequest,
): Promise<RecoveryAttemptCompletionResult> {
  // All mutable input, including Buffer views and nested trace/receipt metadata, is copied
  // before the FIRST await. Never re-read the caller's event, deadline or signal afterward.
  const { deadlineMs, signal } = request.nativeEntry;
  const bounds = Object.freeze({ deadlineMs, ...(signal ? { signal } : {}) });
  let event: RecoveryAttemptCompletionRequest["event"];
  try { event = snapshotEvent(request.event); }
  catch (error) {
    return { ackEligible: false, stage: "attempt", reason: boundedReason(bounds) ?? (error instanceof RecoveryAttemptLedgerError ? error.reason : "invalid-evidence") };
  }
  const joins = request.nativeCorrelation ? { ...request.nativeCorrelation } : undefined;
  let native: RecoveryNativeEntryRegistrationRequest | undefined;
  try {
    native = Object.freeze({
      sessionId: request.nativeEntry.sessionId, deliveryId: request.nativeEntry.deliveryId, recordKind: request.nativeEntry.recordKind,
      entryId: request.nativeEntry.entryId, location: Object.freeze({ ...request.nativeEntry.location }), bytes: Uint8Array.from(request.nativeEntry.bytes),
      bytesSha256: request.nativeEntry.bytesSha256, flushReceipt: Object.freeze({ ...request.nativeEntry.flushReceipt }), ...bounds,
    });
  } catch { /* Invalid descriptor still must not discard a valid raw outcome. */ }
  let attemptSeq: number;
  try { attemptSeq = (await ledger.appendEvent(event, bounds)).seq; }
  catch (error) {
    return { ackEligible: false, stage: "attempt", reason: boundedReason(bounds) ?? (error instanceof RecoveryAttemptLedgerError ? error.reason : storeFailure(error)) };
  }
  const afterAttempt = boundedReason(bounds);
  if (afterAttempt) return { ackEligible: false, stage: "registration", reason: afterAttempt, attemptSeq };
  if (!native || !matches(event, native, joins)) return { ackEligible: false, stage: "registration", reason: "conflict", attemptSeq };
  try {
    const registration = await durability.registerNativeEntry(native);
    const late = boundedReason(bounds);
    if (late) return { ackEligible: false, stage: "registration", reason: late, attemptSeq };
    if (!registration.registered) return { ackEligible: false, stage: "registration", reason: registration.reason, attemptSeq };
  } catch (error) {
    return { ackEligible: false, stage: "registration", reason: boundedReason(bounds) ?? storeFailure(error), attemptSeq };
  }
  try {
    const barrier = await durability.awaitDurableAppend({
      sessionId: native.sessionId, deliveryId: native.deliveryId, recordKind: "carrier", ...bounds,
    });
    const late = boundedReason(bounds);
    if (!barrier.durable) return { ackEligible: false, stage: "barrier", reason: late ?? barrier.reason, attemptSeq };
    if (late) return { ackEligible: false, stage: "barrier", reason: late, attemptSeq, barrierSeq: barrier.seq };
    return { ackEligible: true, attemptSeq, barrierSeq: barrier.seq };
  } catch (error) {
    return { ackEligible: false, stage: "barrier", reason: boundedReason(bounds) ?? storeFailure(error), attemptSeq };
  }
}
