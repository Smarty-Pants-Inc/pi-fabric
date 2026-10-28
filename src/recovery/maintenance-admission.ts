import { AsyncLocalStorage } from "node:async_hooks";
import {
  commitRecoverySession, readRecoverySession, recoveryOwner, recoveryCanonical, recoveryHash,
  assertRecoveryEpoch, RecoveryAdmissionError, driveRecoveryInputOnce, applyInputOutcome,
  type AcceptedRecoveryInput, type RecoveryIo, type RecoverySessionLedger, type RecoveryAdmissionOwner,
  type RecoveryTaskAbort,
} from "./input-admission.js";

export type RecoveryEpoch = Readonly<{ processIncarnationId: string; bindingId: string; epoch: number }>;
export type MaintenanceOperation =
  | { kind: "compact"; reason: "manual"; targetModel?: never }
  | { kind: "compact"; reason: "model-switch"; targetModel: Readonly<{ provider: string; id: string }> }
  | { kind: "tree-nav"; targetEntryId: string | null; summarize: boolean }
  | { kind: "auto-compact-retry"; retryAnchor: string };
export type RepairRef = Readonly<{
  originalInputId: string; recoveryGeneration: string;
  executionBranchEntryId: string; repairReceiptSha256: string;
}>;
export interface MaintenanceRequest {
  readonly version: 1; readonly admissionKind: "maintenance";
  readonly sessionId: string; readonly requestId: string; readonly epoch: RecoveryEpoch;
  readonly operation: MaintenanceOperation;
  readonly baseLeafId: string | null; readonly baseHistorySha256: string; readonly viewSha256: string;
  readonly issuedAtMs: number; readonly deadlineMs: number; readonly repair?: RepairRef;
}
export interface NativePlanRef { readonly entryId: string; readonly bytesSha256: string; readonly planSha256: string }
export interface MaintenanceCompletion {
  readonly outcome: "completed" | "noop" | "cancelled" | "failed";
  readonly errorCode?: string; readonly checkpointEntryId: string;
  readonly entries: ReadonlyArray<{ readonly entryId: string; readonly bytesSha256: string; readonly seq: number }>;
}
export interface MaintenanceRecord {
  readonly request: Readonly<MaintenanceRequest>; readonly operationSha256: string;
  revision: number; activeEpoch: RecoveryEpoch; permitRevision: number;
  state: "ADMITTED" | "ACTIVE" | "PREPARED" | "CONSUMED";
  plan?: NativePlanRef; completion?: MaintenanceCompletion;
}
export interface AcceptedRecoveryMaintenance {
  readonly admissionKind: "maintenance"; readonly sessionId: string; readonly requestId: string;
  readonly epoch: RecoveryEpoch; readonly operationSha256: string; readonly permitRevision: number;
  readonly deadlineMs: number;
}
export type RecoveryGateSubject = Readonly<AcceptedRecoveryInput | AcceptedRecoveryMaintenance>;
export type ProviderPurpose = "turn" | "compact-summary" | "tree-summary" | "bug-report";
export interface MaintenanceKey { readonly sessionId: string; readonly requestId: string }
export const RECOVERY_MAINTENANCE_CODES = [
  "RECOVERY_MAINTENANCE_UNBOUND", "RECOVERY_MAINTENANCE_TOKEN_REUSED",
  "RECOVERY_MAINTENANCE_CONFLICT", "RECOVERY_MAINTENANCE_INVALID",
  "RECOVERY_MAINTENANCE_USER_TEXT", "RECOVERY_MAINTENANCE_HOOK_CONTENT",
  "RECOVERY_MAINTENANCE_KIND_MISMATCH", "RECOVERY_MAINTENANCE_PURPOSE_UNSUPPORTED",
  "RECOVERY_MAINTENANCE_INPUT_SELECTED", "RECOVERY_MAINTENANCE_BUSY", "RECOVERY_MAINTENANCE_SELF_CALL",
  "RECOVERY_MAINTENANCE_DEADLINE", "RECOVERY_MAINTENANCE_CANCELLED", "RECOVERY_MAINTENANCE_INTERRUPTED",
  "RECOVERY_MAINTENANCE_RETRY_EXHAUSTED", "RECOVERY_MAINTENANCE_REPAIR_REQUIRED", "RECOVERY_MAINTENANCE_UNCERTAIN",
] as const;
export type MaintenanceFailureCode = typeof RECOVERY_MAINTENANCE_CODES[number] | "RECOVERY_EPOCH_REVOKED" | "RECOVERY_EXECUTION_BRANCH_REQUIRED";
export type MaintenanceRefusal = Readonly<{ ok: false; code: MaintenanceFailureCode; message: string }>;
export type MaintenanceResult<T = MaintenanceRecord> = Readonly<{ ok: true; value: T }> | MaintenanceRefusal;
export interface BegunMaintenance { readonly record: MaintenanceRecord; readonly token: AcceptedRecoveryMaintenance }
export interface MaintenanceSnapshot { readonly revision: number; readonly record?: MaintenanceRecord }
export interface PendingMaintenance { readonly revision: number; readonly records: readonly MaintenanceRecord[] }
export interface MaintenanceLedgerEntry {
  record: MaintenanceRecord;
  /** Internal durable disposition, NOT permission carried by public JSON or a token. */
  settlementOnly: boolean;
  cancelRequested: boolean;
}
export interface MaintenanceJoinReconciliation extends RecoveryIo {
  /** The ACTUAL native operation/run + summary requests + persistence release, not isIdle,
   * abort notification, observer completion, or a timeout surrogate. B3 owns this promise. */
  readonly joined: Promise<void>;
  /** Native scans its own WAL, validates the exact plan/entries and completes B6 barriers.
   * It returns a control-only plan for unprepared failure/cancel/interruption; never inference.
   * A durable prepare found after an unknown ACK wins over late cancel/expiry. */
  settle(record: Readonly<MaintenanceRecord>, prepare: (plan: NativePlanRef) => Promise<MaintenanceResult>): Promise<MaintenanceCompletion>;
  /** Current-binding recovery pass, including issued intents/repair/branch restoration.
   * Consumption precedes this callback; HELD release/reconsideration follows it. */
  reconcile(record: Readonly<MaintenanceRecord>): Promise<void>;
  readonly taskAbortRequestId?: string;
}

/** Exact B5 port for B3's composition. All mutations share input-admission's synchronous
 * revision owner. No provider, hook, join, callback or B6 wait runs inside a transaction.
 * B3 keeps native HELD and its private shared attempt; a copied accepted token is not one.
 * read/list are bounded snapshots, never a second scheduler or a poller. */
export interface RecoveryMaintenanceAdapter {
  admitMaintenance(request: MaintenanceRequest, io?: RecoveryIo): Promise<MaintenanceResult>;
  beginMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, expectedRevision: number, io?: RecoveryIo): Promise<MaintenanceResult<BegunMaintenance>>;
  prepareMaintenance(token: AcceptedRecoveryMaintenance, plan: NativePlanRef, io?: RecoveryIo): Promise<MaintenanceResult>;
  consumeMaintenance(token: AcceptedRecoveryMaintenance, completion: MaintenanceCompletion, io?: RecoveryIo): Promise<MaintenanceResult>;
  adoptMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, expectedRevision: number, io?: RecoveryIo): Promise<MaintenanceResult<BegunMaintenance>>;
  read(key: MaintenanceKey, io?: RecoveryIo): Promise<MaintenanceResult<MaintenanceSnapshot>>;
  listPending(sessionId: string, limit: number, io?: RecoveryIo): Promise<MaintenanceResult<PendingMaintenance>>;
  withMaintenanceAttempt<T>(token: AcceptedRecoveryMaintenance, run: (signal: AbortSignal) => Promise<T>): Promise<T>;
  assertMaintenanceGate(token: AcceptedRecoveryMaintenance, purpose: ProviderPurpose, viewSha256: string): void;
  cancelMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, io?: RecoveryIo): Promise<MaintenanceResult>;
  reconcileAfterJoin(key: MaintenanceKey, epoch: RecoveryEpoch, edge: MaintenanceJoinReconciliation): Promise<MaintenanceResult>;
  captureTaskAbort(sessionId: string, requestId: string, epoch: RecoveryEpoch, at: number, bootId: string, io?: RecoveryIo): Promise<MaintenanceResult<RecoveryTaskAbort>>;
  releaseLostMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, io?: RecoveryIo): Promise<MaintenanceResult<void>>;
}

export class RecoveryMaintenanceError extends Error {
  override readonly name = "RecoveryMaintenanceError";
  constructor(readonly code: MaintenanceFailureCode, message: string) { super(message); }
}
function fail(suffix: string, message: string): never {
  throw new RecoveryMaintenanceError((suffix.startsWith("RECOVERY_") ? suffix : `RECOVERY_MAINTENANCE_${suffix}`) as MaintenanceFailureCode, message);
}
const plain = (x: unknown): x is Record<string, unknown> => x !== null && typeof x === "object" && !Array.isArray(x) &&
  [Object.prototype, null].includes(Object.getPrototypeOf(x));
const text = (x: unknown): x is string => typeof x === "string" && x.trim().length > 0 && x.length <= 4096;
const digest = (x: unknown): x is string => typeof x === "string" && /^[a-f0-9]{64}$/.test(x);
const time = (x: unknown): x is number => Number.isSafeInteger(x) && (x as number) > 0;
const copy = <T>(value: T): T => JSON.parse(recoveryCanonical(value)) as T;
const freeze = <T>(value: T): T => {
  if (value && typeof value === "object") { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
};
const checkedObject = (x: unknown, keys: readonly string[]): Record<string, unknown> => {
  if (!plain(x)) return fail("INVALID", "Expected a plain closed-schema maintenance object");
  const fields = Reflect.ownKeys(x);
  if (fields.some(key => typeof key === "string" && /^(text|message|content|instructions|customInstructions|customPrompt|prompt|label|summary|data)$/i.test(key))) {
    return fail("USER_TEXT", "Maintenance cannot carry user text, instructions, labels or summary bytes (including undefined)");
  }
  if (fields.some(key => typeof key !== "string" || !keys.includes(key)) || Object.values(Object.getOwnPropertyDescriptors(x)).some(d => !d.enumerable || !("value" in d))) {
    return fail("INVALID", "Unknown field, accessor or non-JSON maintenance object");
  }
  return x;
};
export function validateRecoveryEpoch(x: unknown): asserts x is RecoveryEpoch {
  const e = checkedObject(x, ["processIncarnationId", "bindingId", "epoch"]);
  if (!text(e.processIncarnationId) || !text(e.bindingId) || !Number.isSafeInteger(e.epoch) || (e.epoch as number) < 0) fail("INVALID", "Invalid full recovery epoch");
}
export function validateMaintenanceRequest(value: unknown): MaintenanceRequest {
  const r = checkedObject(value, ["version", "admissionKind", "sessionId", "requestId", "epoch", "operation", "baseLeafId", "baseHistorySha256", "viewSha256", "issuedAtMs", "deadlineMs", "repair"]);
  validateRecoveryEpoch(r.epoch);
  if (r.version !== 1 || r.admissionKind !== "maintenance" || !text(r.sessionId) || !text(r.requestId) ||
      !(r.baseLeafId === null || text(r.baseLeafId)) || !digest(r.baseHistorySha256) || !digest(r.viewSha256) ||
      !time(r.issuedAtMs) || !time(r.deadlineMs) || r.deadlineMs <= r.issuedAtMs) {
    return fail("INVALID", "Invalid maintenance identity, view or original capped deadline");
  }
  const op = checkedObject(r.operation, ["kind", "reason", "targetModel", "targetEntryId", "summarize", "retryAnchor"]);
  if (op.kind === "compact") {
    checkedObject(op, ["kind", "reason", "targetModel"]);
    if (op.reason === "model-switch") {
      const target = checkedObject(op.targetModel, ["provider", "id"]);
      if (!text(target.provider) || !text(target.id)) fail("INVALID", "Model switch requires its pinned target model");
    } else if (op.reason !== "manual" || Reflect.has(op, "targetModel")) fail("INVALID", "Invalid compact operation");
  } else if (op.kind === "tree-nav") {
    checkedObject(op, ["kind", "targetEntryId", "summarize"]);
    if (!(op.targetEntryId === null || text(op.targetEntryId)) || typeof op.summarize !== "boolean" ||
        (typeof op.targetEntryId === "string" && op.targetEntryId.startsWith("pi-maintenance:"))) fail("INVALID", "Invalid or reserved tree target");
  } else if (op.kind === "auto-compact-retry") {
    checkedObject(op, ["kind", "retryAnchor"]);
    if (!text(op.retryAnchor)) fail("INVALID", "Auto retry needs the retained original input/origin anchor");
  } else { checkedObject(op, ["kind"]); fail("KIND_MISMATCH", "Unknown maintenance operation"); }
  if (op.kind !== "auto-compact-retry" && r.deadlineMs - r.issuedAtMs > 20 * 60_000) fail("INVALID", "Manual/tree/model-switch maintenance exceeds its twenty-minute cap");
  if (Reflect.has(r, "repair")) {
    const repair = checkedObject(r.repair, ["originalInputId", "recoveryGeneration", "executionBranchEntryId", "repairReceiptSha256"]);
    if (!text(repair.originalInputId) || !text(repair.recoveryGeneration) || !text(repair.executionBranchEntryId) || !digest(repair.repairReceiptSha256)) fail("INVALID", "Invalid read-only repair reference");
  }
  return freeze(copy(value as MaintenanceRequest));
}
const checkedPlan = (value: NativePlanRef): NativePlanRef => {
  const p = checkedObject(value, ["entryId", "bytesSha256", "planSha256"]);
  if (!text(p.entryId) || !digest(p.bytesSha256) || !digest(p.planSha256)) return fail("INVALID", "Invalid native prepare reference");
  return freeze(copy(value));
};
const checkedCompletion = (value: MaintenanceCompletion, plan: NativePlanRef): MaintenanceCompletion => {
  const c = checkedObject(value, ["outcome", "errorCode", "checkpointEntryId", "entries"]);
  if (!["completed", "noop", "cancelled", "failed"].includes(c.outcome as string) || !text(c.checkpointEntryId) ||
      (Reflect.has(c, "errorCode") && !text(c.errorCode)) || !Array.isArray(c.entries) || !c.entries.length || c.entries.length > 4096) {
    return fail("INVALID", "Invalid terminal maintenance outcome/checkpoint");
  }
  // A failed/cancelled operation is still terminally accounted, never mistaken for success.
  if ((c.outcome === "failed" || c.outcome === "cancelled") && !text(c.errorCode)) fail("INVALID", "Non-success requires truthful failure evidence");
  const ids = new Set<string>(); const seqs = new Set<number>();
  for (const entry of c.entries) {
    const e = checkedObject(entry, ["entryId", "bytesSha256", "seq"]);
    if (!text(e.entryId) || !digest(e.bytesSha256) || !Number.isSafeInteger(e.seq) || (e.seq as number) < 0 || ids.has(e.entryId) || seqs.has(e.seq as number)) {
      return fail("UNCERTAIN", "Missing/duplicate native entry or B6 receipt");
    }
    ids.add(e.entryId); seqs.add(e.seq as number);
  }
  const prepared = c.entries.find(e => e.entryId === plan.entryId);
  if (c.checkpointEntryId === plan.entryId || !prepared || prepared.bytesSha256 !== plan.bytesSha256 || !ids.has(c.checkpointEntryId)) fail("UNCERTAIN", "Completion must cover the exact native prepare and checkpoint B6 receipts");
  return freeze(copy(value));
};
const result = async <T>(run: () => T | Promise<T>): Promise<MaintenanceResult<T>> => {
  try { return { ok: true, value: await run() }; }
  catch (error) {
    if (error instanceof RecoveryMaintenanceError) return { ok: false, code: error.code, message: error.message };
    if (error instanceof RecoveryAdmissionError) {
      const code = error.code === "RECOVERY_EPOCH_REVOKED" ? error.code : error.code === "RECOVERY_DEADLINE" ? "RECOVERY_MAINTENANCE_DEADLINE" :
        error.code === "RECOVERY_NOT_BOUND" ? "RECOVERY_MAINTENANCE_UNBOUND" : error.code === "RECOVERY_ABORTED" ? "RECOVERY_MAINTENANCE_CANCELLED" : "RECOVERY_MAINTENANCE_UNCERTAIN";
      return { ok: false, code, message: error.message };
    }
    return { ok: false, code: "RECOVERY_MAINTENANCE_UNCERTAIN", message: error instanceof Error ? error.message : "Unknown maintenance store failure" };
  }
};
const entryFor = (s: RecoverySessionLedger, key: MaintenanceKey): MaintenanceLedgerEntry => s.maintenance.find(e => e.record.request.requestId === key.requestId) ?? fail("UNBOUND", "No durable maintenance admission for this request");
const current = (s: RecoverySessionLedger, key: MaintenanceKey, epoch: RecoveryEpoch): MaintenanceLedgerEntry => {
  assertRecoveryEpoch(key.sessionId, epoch);
  const e = entryFor(s, key);
  if (recoveryCanonical(e.record.activeEpoch) !== recoveryCanonical(epoch)) fail("RECOVERY_EPOCH_REVOKED", "Maintenance belongs to a revoked binding epoch");
  return e;
};
const reserved = (s: RecoverySessionLedger, key: MaintenanceKey): void => {
  if (s.reservation?.kind !== "maintenance" || s.reservation.requestId !== key.requestId) fail("TOKEN_REUSED", "Maintenance no longer owns the session reservation");
};
const ioFor = (request: MaintenanceRequest, io?: RecoveryIo): RecoveryIo => ({ deadlineMs: Math.min(request.deadlineMs, io?.deadlineMs ?? request.deadlineMs), ...(io?.signal ? { signal: io.signal } : {}) });
interface Attempt { owner: RecoveryAdmissionOwner; token: AcceptedRecoveryMaintenance; controller: AbortController; entered: boolean; live: boolean; afterJoin: boolean; settlementOnly: boolean }
const attempts = new WeakMap<AcceptedRecoveryMaintenance, Attempt>();
const attemptScope = new AsyncLocalStorage<Attempt>();
const liveAttempts = new WeakMap<RecoveryAdmissionOwner, Map<string, Attempt>>();
const attemptsFor = (owner: RecoveryAdmissionOwner): Map<string, Attempt> => {
  let live = liveAttempts.get(owner); if (!live) { live = new Map(); liveAttempts.set(owner, live); } return live;
};
const settlementInFlight = (key: MaintenanceKey): boolean => attemptsFor(recoveryOwner(key.sessionId)).get(idFor(key))?.afterJoin === true;
const idFor = (key: MaintenanceKey) => recoveryCanonical([key.sessionId, key.requestId]);
const makeToken = (record: MaintenanceRecord, settlementOnly: boolean): AcceptedRecoveryMaintenance => {
  const token: AcceptedRecoveryMaintenance = freeze({ admissionKind: "maintenance", sessionId: record.request.sessionId, requestId: record.request.requestId,
    epoch: copy(record.activeEpoch), operationSha256: record.operationSha256, permitRevision: record.permitRevision, deadlineMs: record.request.deadlineMs });
  const attempt: Attempt = { owner: recoveryOwner(token.sessionId), token, controller: new AbortController(), entered: false, live: true, afterJoin: false, settlementOnly };
  const live = attemptsFor(attempt.owner);
  const previous = live.get(idFor(token));
  if (previous) { previous.live = false; previous.controller.abort(); }
  attempts.set(token, attempt); live.set(idFor(token), attempt);
  return token;
};
const checkedAttempt = (token: AcceptedRecoveryMaintenance, s: RecoverySessionLedger, afterJoin = false): { attempt: Attempt; entry: MaintenanceLedgerEntry } => {
  const attempt = attempts.get(token);
  if (!attempt || attemptScope.getStore() !== attempt || attempt.owner !== recoveryOwner(token.sessionId)) fail("UNBOUND", "A copied token or detached scope is not a private live attempt");
  if (!attempt.live) fail("TOKEN_REUSED", "The private live attempt ended or its permit was revoked");
  const entry = current(s, token, token.epoch); reserved(s, token);
  if (entry.record.permitRevision !== token.permitRevision || entry.record.operationSha256 !== token.operationSha256 || entry.record.state === "CONSUMED") fail("TOKEN_REUSED", "Maintenance permit was consumed or revoked");
  if (afterJoin && !attempt.afterJoin) fail("UNCERTAIN", "Maintenance consumption requires the actual post-join reconciliation edge");
  return { attempt, entry };
};
const revoke = (key: MaintenanceKey): void => {
  const live = attemptsFor(recoveryOwner(key.sessionId));
  const a = live.get(idFor(key));
  if (a) { a.live = false; a.controller.abort(); live.delete(idFor(key)); }
};

export async function admitMaintenance(request: MaintenanceRequest, io?: RecoveryIo): Promise<MaintenanceResult> {
  return result(() => {
    const r = validateMaintenanceRequest(request);
    // An exact retry returns the original durable admission even after expiry/adoption.
    const existing = readRecoverySession(r.sessionId, io).maintenance.find(e => e.record.request.requestId === r.requestId);
    if (existing) {
      if (recoveryCanonical(existing.record.request) !== recoveryCanonical(r)) fail("CONFLICT", "Changed immutable maintenance request");
      return copy(existing.record);
    }
    return commitRecoverySession(r.sessionId, (s, revision) => {
      assertRecoveryEpoch(r.sessionId, r.epoch);
      if (!s.complete) fail("UNCERTAIN", "Incomplete input history cannot authorize maintenance");
      if (r.operation.kind === "tree-nav" && s.currentInputId) fail("INPUT_SELECTED", "Tree navigation cannot stick while a real logical input is open");
      if (s.reservation) fail("BUSY", "The first durable session reservation wins; maintenance must release and drive B5 once");
      if (s.currentInputId) {
        const repair = r.repair && s.repairs.find(p => recoveryCanonical(p) === recoveryCanonical(r.repair));
        if (!repair || repair.originalInputId !== s.currentInputId || r.baseLeafId !== repair.executionBranchEntryId) fail("REPAIR_REQUIRED", "Compaction requires the retained repair on the original execution branch");
        const original = s.inputs.find(i => i.originalInputId === s.currentInputId && i.kind === "round")!;
        if (r.deadlineMs > original.deadlineAt) fail("DEADLINE", "Maintenance cannot renew its enclosing input deadline");
      } else if (r.repair) fail("REPAIR_REQUIRED", "No open original input owns this repair reference");
      if (r.operation.kind === "auto-compact-retry") {
        const anchor = r.operation.retryAnchor;
        const original = s.inputs.find(i => i.kind === "round" && i.originalInputId === anchor);
        const origin = s.maintenance.find(e => e.record.request.requestId === anchor);
        if (s.retrySlots.some(slot => slot.anchor === anchor)) fail("RETRY_EXHAUSTED", "This original input/origin already spent its one durable retry");
        if (origin?.record.request.operation.kind === "auto-compact-retry") fail("RETRY_EXHAUSTED", "A retry request cannot become a fresh originating retry slot");
        if (original ? (!r.repair || r.repair.originalInputId !== anchor || s.currentInputId !== anchor) : !origin || origin.record.completion?.outcome !== "failed") fail("REPAIR_REQUIRED", "Retry anchor must resolve a retained original input/failed origin, never a native runId");
        const deadline = original?.deadlineAt ?? origin!.record.request.deadlineMs;
        if (r.deadlineMs !== deadline) fail("DEADLINE", "Auto retry inherits its originating absolute deadline");
        s.retrySlots.push({ anchor, requestId: r.requestId });
      }
      const record: MaintenanceRecord = { request: r, operationSha256: recoveryHash(recoveryCanonical(r.operation)), revision,
        activeEpoch: copy(r.epoch), permitRevision: 0, state: "ADMITTED" };
      s.maintenance.push({ record, settlementOnly: false, cancelRequested: false });
      s.reservation = { kind: "maintenance", requestId: r.requestId };
      return copy(record);
    }, ioFor(r, io));
  });
}
export async function beginMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, expectedRevision: number, io?: RecoveryIo): Promise<MaintenanceResult<BegunMaintenance>> {
  return result(() => {
    let record: MaintenanceRecord;
    try { record = commitRecoverySession(key.sessionId, (s, revision) => {
      const entry = current(s, key, epoch); reserved(s, key);
      if (entry.record.state !== "ADMITTED" || entry.settlementOnly) fail("TOKEN_REUSED", "Begin cannot repeat or restart interrupted computation");
      if (s.revision !== expectedRevision) fail("CONFLICT", "Session revision changed before maintenance begin");
      if (s.reservation?.kind === "input") fail("INPUT_SELECTED", "Real native input is selected");
      if (Date.now() >= entry.record.request.deadlineMs) fail("DEADLINE", "Original maintenance deadline expired");
      entry.record.state = "ACTIVE"; entry.record.permitRevision++; entry.record.revision = revision;
      return copy(entry.record);
    }, io); } catch (error) {
      if (error instanceof RecoveryMaintenanceError) throw error;
      // A rename/fsync ACK can be lost after the ACTIVE revision commits. Read it
      // back once before returning failure; never strand an unreturned ACTIVE permit.
      const s = readRecoverySession(key.sessionId);
      const e = current(s, key, epoch);
      if (e.record.state !== "ACTIVE" || e.record.revision !== expectedRevision + 1 || attemptsFor(recoveryOwner(key.sessionId)).has(idFor(key))) throw error;
      if (Date.now() >= e.record.request.deadlineMs || io?.signal?.aborted) {
        commitRecoverySession(key.sessionId, (state, revision) => { const parked = current(state, key, epoch); parked.record.state = "ADMITTED"; parked.record.permitRevision++; parked.record.revision = revision; parked.settlementOnly = true; });
        throw error;
      }
      record = copy(e.record);
    }
    return { record, token: makeToken(record, false) };
  });
}
export async function prepareMaintenance(token: AcceptedRecoveryMaintenance, plan: NativePlanRef, io?: RecoveryIo): Promise<MaintenanceResult> {
  return result(() => {
    const p = checkedPlan(plan);
    return commitRecoverySession(token.sessionId, (s, revision) => {
      const { attempt, entry } = checkedAttempt(token, s);
      if (entry.record.state === "PREPARED") {
        if (recoveryCanonical(entry.record.plan) !== recoveryCanonical(p)) fail("UNCERTAIN", "Changed native plan after durable preparation");
        return copy(entry.record);
      }
      if (!attempt.afterJoin && (entry.settlementOnly || attempt.settlementOnly)) fail("INTERRUPTED", "Adoption is settlement-only, never new summary permission");
      if (!attempt.afterJoin && (attempt.controller.signal.aborted || entry.cancelRequested)) fail("CANCELLED", "Maintenance cancelled before preparation");
      if (!attempt.afterJoin && Date.now() >= entry.record.request.deadlineMs) fail("DEADLINE", "Expired computation cannot prepare a new plan");
      if (entry.record.state !== "ACTIVE" && !(attempt.afterJoin && entry.record.state === "ADMITTED")) fail("TOKEN_REUSED", "Request is not preparable");
      entry.record.plan = p; entry.record.state = "PREPARED"; entry.record.revision = revision;
      return copy(entry.record);
    }, io);
  });
}
const consumeInTransaction = (s: RecoverySessionLedger, revision: number, token: AcceptedRecoveryMaintenance, completion: MaintenanceCompletion): MaintenanceRecord => {
  const entry = current(s, token, token.epoch);
  if (!entry.record.plan) fail("UNCERTAIN", "No durable native plan is prepared");
  const c = checkedCompletion(completion, entry.record.plan);
  if (entry.record.state === "CONSUMED") {
    if (entry.record.permitRevision !== token.permitRevision || entry.record.operationSha256 !== token.operationSha256) fail("TOKEN_REUSED", "Consumed permit differs");
    if (recoveryCanonical(entry.record.completion) !== recoveryCanonical(c)) fail("CONFLICT", "Changed terminal maintenance outcome");
    return copy(entry.record);
  }
  reserved(s, token);
  checkedAttempt(token, s, true);
  if (entry.record.state !== "PREPARED") fail("UNCERTAIN", "Only a prepared native plan can be consumed");
  entry.record.completion = c; entry.record.state = "CONSUMED"; entry.record.revision = revision;
  return copy(entry.record);
};
export async function consumeMaintenance(token: AcceptedRecoveryMaintenance, completion: MaintenanceCompletion, io?: RecoveryIo): Promise<MaintenanceResult> {
  return result(() => commitRecoverySession(token.sessionId, (s, revision) => consumeInTransaction(s, revision, token, completion), io));
}
export async function adoptMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, expectedRevision: number, io?: RecoveryIo): Promise<MaintenanceResult<BegunMaintenance>> {
  return result(() => {
    let record: MaintenanceRecord;
    try { record = commitRecoverySession(key.sessionId, (s, revision) => {
      assertRecoveryEpoch(key.sessionId, epoch); // The binding's positive exclusion check runs inside the owner.
      const entry = entryFor(s, key); reserved(s, key);
      if (s.revision !== expectedRevision) fail("CONFLICT", "Session revision changed before adoption");
      if (recoveryCanonical(entry.record.activeEpoch) === recoveryCanonical(epoch)) fail("TOKEN_REUSED", "A binding cannot adopt itself to rearm a token");
      entry.record.activeEpoch = copy(epoch); entry.record.permitRevision++; entry.record.revision = revision;
      entry.settlementOnly = true;
      if (entry.record.state === "ADMITTED") entry.record.state = "ACTIVE";
      return copy(entry.record);
    }, io); } catch (error) {
      if (error instanceof RecoveryMaintenanceError) throw error;
      const s = readRecoverySession(key.sessionId); const e = current(s, key, epoch);
      if (e.record.revision !== expectedRevision + 1 || !e.settlementOnly || attemptsFor(recoveryOwner(key.sessionId)).has(idFor(key))) throw error;
      record = copy(e.record);
    }
    return { record, token: makeToken(record, true) };
  });
}
export async function readMaintenance(key: MaintenanceKey, io?: RecoveryIo): Promise<MaintenanceResult<MaintenanceSnapshot>> {
  return result(() => {
    const s = readRecoverySession(key.sessionId, io);
    const entry = s.maintenance.find(e => e.record.request.requestId === key.requestId);
    return { revision: s.revision, ...(entry ? { record: copy(entry.record) } : {}) };
  });
}
export async function listPendingMaintenance(sessionId: string, limit: number, io?: RecoveryIo): Promise<MaintenanceResult<PendingMaintenance>> {
  return result(() => {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 256) fail("INVALID", "Pending scan limit must be 1..256");
    const s = readRecoverySession(sessionId, io);
    const entries = s.maintenance.filter(e => e.record.state !== "CONSUMED" || s.reservation?.kind === "maintenance" && s.reservation.requestId === e.record.request.requestId);
    if (entries.length > limit) fail("UNCERTAIN", "Bounded pending scan would truncate admitted work");
    return { revision: s.revision, records: entries.map(e => copy(e.record)) };
  });
}
export async function withMaintenanceAttempt<T>(token: AcceptedRecoveryMaintenance, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const attempt = attempts.get(token);
  if (!attempt || attempt.entered || !attempt.live || attempt.owner !== recoveryOwner(token.sessionId)) return fail("TOKEN_REUSED", "One private attempt per begin; copied/reused token is not authority");
  attempt.entered = true;
  try {
    if (attempt.settlementOnly) fail("INTERRUPTED", "A successor may settle native WAL, never restart computation");
    return await attemptScope.run(attempt, () => run(attempt.controller.signal));
  } finally {
    attempt.live = false; attemptsFor(attempt.owner).delete(idFor(token));
    // If the operation ended without PREPARED, retain its obligation but revoke inference.
    // No ACTIVE record is stranded in a live process, even on provider/hook/append errors.
    commitRecoverySession(token.sessionId, (s, revision) => {
      const e = entryFor(s, token);
      if (recoveryCanonical(e.record.activeEpoch) === recoveryCanonical(token.epoch) && e.record.state === "ACTIVE" && e.record.permitRevision === token.permitRevision) {
        e.record.state = "ADMITTED"; e.record.permitRevision++; e.record.revision = revision; e.settlementOnly = true;
      }
    });
  }
}
export function assertMaintenanceGate(token: AcceptedRecoveryMaintenance, purpose: ProviderPurpose, viewSha256: string): void {
  const s = readRecoverySession(token.sessionId);
  const { attempt, entry } = checkedAttempt(token, s);
  if (attempt.afterJoin || attempt.settlementOnly || entry.settlementOnly) fail("INTERRUPTED", "Settlement scope has no inference permission");
  if (entry.cancelRequested || attempt.controller.signal.aborted) fail("CANCELLED", "Maintenance was cancelled");
  if (Date.now() >= entry.record.request.deadlineMs) fail("DEADLINE", "Original deadline expired");
  if (entry.record.state !== "ACTIVE") fail("TOKEN_REUSED", "Only ACTIVE computation can use a provider gate");
  if (viewSha256 !== entry.record.request.viewSha256) fail("CONFLICT", "Provider view differs from admitted pinned view");
  if (purpose === "turn" || purpose === "bug-report" || !["compact-summary", "tree-summary"].includes(purpose)) fail("PURPOSE_UNSUPPORTED", "Maintenance cannot authorize turn, bug-report, unknown or virtual inference");
  const op = entry.record.request.operation;
  if (purpose === "tree-summary" ? op.kind !== "tree-nav" || !op.summarize : op.kind === "tree-nav") fail("KIND_MISMATCH", "Provider purpose does not match this one operation");
}
export async function cancelMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, io?: RecoveryIo): Promise<MaintenanceResult> {
  return result(() => {
    const r = commitRecoverySession(key.sessionId, (s, revision) => {
      const e = current(s, key, epoch); reserved(s, key);
      if (e.record.state === "CONSUMED" || e.cancelRequested) return copy(e.record);
      e.cancelRequested = true; e.settlementOnly = true; e.record.revision = revision;
      if (!settlementInFlight(key)) e.record.permitRevision++;
      if (e.record.state === "ACTIVE") e.record.state = "ADMITTED";
      return copy(e.record);
    }, io);
    if (!settlementInFlight(key)) revoke(key);
    return r;
  });
}
const edges = new WeakMap<RecoveryAdmissionOwner, Map<string, Promise<MaintenanceResult>>>();
export async function reconcileMaintenanceAfterJoin(key: MaintenanceKey, epoch: RecoveryEpoch, edge: MaintenanceJoinReconciliation): Promise<MaintenanceResult> {
  let owner: RecoveryAdmissionOwner;
  try { owner = recoveryOwner(key.sessionId); assertRecoveryEpoch(key.sessionId, epoch); }
  catch (error) { return result(() => { throw error; }); }
  let memo = edges.get(owner); if (!memo) { memo = new Map(); edges.set(owner, memo); }
  const edgeId = recoveryCanonical([key, epoch]);
  const prior = memo.get(edgeId); if (prior) return prior;
  // Install the memo before even an already-resolved actual join can invoke callbacks.
  const pending = Promise.resolve().then(() => result(async () => {
    await edge.joined;
    assertRecoveryEpoch(key.sessionId, epoch);
    let s = readRecoverySession(key.sessionId, edge); let entry = current(s, key, epoch); reserved(s, key);
    if (entry.record.state !== "CONSUMED") {
      revoke(key);
      const token = makeToken(entry.record, true); const attempt = attempts.get(token)!;
      attempt.afterJoin = true; attempt.entered = true;
      try {
        const completion = await edge.settle(freeze(copy(entry.record)), plan => attemptScope.run(attempt, () => prepareMaintenance(token, plan, edge)));
        assertRecoveryEpoch(key.sessionId, epoch);
        const consumed = await attemptScope.run(attempt, () => result(() => commitRecoverySession(key.sessionId, (state, revision) => {
          const consumedRecord = consumeInTransaction(state, revision, token, completion);
          if (edge.taskAbortRequestId && !state.taskAborts.some(a => a.requestId === edge.taskAbortRequestId && a.maintenanceRequestId === key.requestId)) fail("CONFLICT", "Task abort does not own this captured reservation");
          for (const abort of state.taskAborts.filter(a => a.maintenanceRequestId === key.requestId && !a.committed)) {
            for (const originalInputId of abort.originalInputIds) applyInputOutcome(state, revision, {
              sessionId: key.sessionId, originalInputId, requestId: `${abort.requestId}:${recoveryHash(originalInputId)}`,
              kind: "user-cancel", at: abort.at, bootId: abort.bootId,
            });
            abort.committed = true;
          }
          return consumedRecord;
        }, edge)));
        if (!consumed.ok) {
          // One authoritative readback, not a replay/retry loop, resolves a lost
          // consume ACK (including E's outcomes in that same serialized revision).
          const readback = readRecoverySession(key.sessionId, edge); const terminal = current(readback, key, epoch);
          if (terminal.record.state !== "CONSUMED" || recoveryCanonical(terminal.record.completion) !== recoveryCanonical(completion) ||
              readback.taskAborts.some(a => a.maintenanceRequestId === key.requestId && !a.committed)) fail(consumed.code, consumed.message);
        }
      } finally { attempt.live = false; attemptsFor(attempt.owner).delete(idFor(key)); }
    }
    s = readRecoverySession(key.sessionId, edge); entry = current(s, key, epoch);
    await edge.reconcile(freeze(copy(entry.record)));
    assertRecoveryEpoch(key.sessionId, epoch);
    const record = commitRecoverySession(key.sessionId, state => {
      const e = current(state, key, epoch); reserved(state, key);
      if (e.record.state !== "CONSUMED") fail("UNCERTAIN", "Cannot release an unconsumed maintenance reservation");
      delete state.reservation;
      return copy(e.record);
    }, edge);
    await driveRecoveryInputOnce(key.sessionId, `maintenance-release:${edgeId}`, edge);
    return record;
  }));
  memo.set(edgeId, pending);
  const outcome = await pending;
  if (!outcome.ok) {
    // A failed edge is never automatically retried. Preserve WAL/hold and revoke compute.
    revoke(key);
    try { commitRecoverySession(key.sessionId, (s, revision) => {
      const e = current(s, key, epoch);
      if (e.record.state === "ACTIVE") { e.record.state = "ADMITTED"; e.record.permitRevision++; e.record.revision = revision; e.settlementOnly = true; }
    }); } catch { /* Ownership/store uncertainty remains held; never fabricate a durable outcome. */ }
  }
  return outcome;
}
export async function captureMaintenanceTaskAbort(sessionId: string, requestId: string, epoch: RecoveryEpoch, at: number, bootId: string, io?: RecoveryIo): Promise<MaintenanceResult<RecoveryTaskAbort>> {
  return result(() => {
    if (!text(requestId) || !time(at) || !text(bootId)) fail("INVALID", "Invalid retained task-abort identity");
    const abort = commitRecoverySession(sessionId, (s, revision) => {
      assertRecoveryEpoch(sessionId, epoch);
      const old = s.taskAborts.find(a => a.requestId === requestId);
      if (old) {
        if (old.at !== at || old.bootId !== bootId) fail("CONFLICT", "Changed task abort request identity");
        return copy(old);
      }
      if (s.reservation?.kind !== "maintenance") fail("UNBOUND", "No live maintenance reservation for this task abort");
      const e = entryFor(s, { sessionId, requestId: s.reservation.requestId });
      // The real durable original-round backlog is the sole target source. No recovery.input.
      const originalInputIds = s.inputs.filter(i => i.kind === "round" && !s.outcomes.some(o => o.originalInputId === i.originalInputId && o.kind !== "lifecycle-interrupt")).map(i => i.originalInputId);
      const a: RecoveryTaskAbort = { requestId, at, bootId, originalInputIds, maintenanceRequestId: e.record.request.requestId, committed: false };
      s.taskAborts.push(a);
      if (e.record.state === "CONSUMED") {
        for (const originalInputId of originalInputIds) applyInputOutcome(s, revision, {
          sessionId, originalInputId, requestId: `${a.requestId}:${recoveryHash(originalInputId)}`, kind: "user-cancel", at: a.at, bootId: a.bootId,
        });
        a.committed = true;
      }
      e.cancelRequested = true; e.settlementOnly = true;
      if (e.record.state !== "CONSUMED") { if (!settlementInFlight({ sessionId, requestId: a.maintenanceRequestId })) e.record.permitRevision++; e.record.revision = revision; if (e.record.state === "ACTIVE") e.record.state = "ADMITTED"; }
      return copy(a);
    }, io);
    const key = { sessionId, requestId: abort.maintenanceRequestId };
    if (!settlementInFlight(key)) revoke(key);
    return abort;
  });
}
export async function releaseLostMaintenance(key: MaintenanceKey, epoch: RecoveryEpoch, io?: RecoveryIo): Promise<MaintenanceResult<void>> {
  return result(async () => {
    assertRecoveryEpoch(key.sessionId, epoch);
    const s = readRecoverySession(key.sessionId, io);
    if (s.maintenance.some(e => e.record.request.requestId === key.requestId) || s.reservation?.kind !== "input") fail("CONFLICT", "Only a maintenance race loser may release/re-drive the selected input");
    await driveRecoveryInputOnce(key.sessionId, `maintenance-lost:${recoveryCanonical([key, epoch])}`, io);
  });
}
export function createRecoveryMaintenanceAdapter(): RecoveryMaintenanceAdapter {
  return {
    admitMaintenance, beginMaintenance, prepareMaintenance, consumeMaintenance, adoptMaintenance,
    read: readMaintenance, listPending: listPendingMaintenance, withMaintenanceAttempt, assertMaintenanceGate,
    cancelMaintenance, reconcileAfterJoin: reconcileMaintenanceAfterJoin,
    captureTaskAbort: captureMaintenanceTaskAbort, releaseLostMaintenance,
  };
}
