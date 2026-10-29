import type { ContextEvent, ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { TransportHostLeasePredecessorSnapshot } from "../topology/host-leases.js";
export type { TransportHostLeasePredecessorSnapshot } from "../topology/host-leases.js";

/** The native old-writer handoff. A different boot, a missing participant, and lease expiry
 * alone do not prove that the old machine lost access to the retained session volume. */
export interface PortableIdentityEvidence {
  readonly sessionId: string;
  /** Exact retained receipt bytes, not a reserialized object. sha256 is lower-case hex. */
  readonly predecessorReceipt: { readonly bytes: string; readonly sha256: string };
  readonly predecessorPodId: string;
  readonly predecessorBootId: string;
  readonly successorPodId: string;
  readonly successorBootId: string;
  readonly exclusionProof: {
    readonly kind: "kubelet-fence" | "volume-detach" | "lease-absent";
    readonly observedAt: number;
    /** Native receipt/resource reference. Not an operator's assertion of death. */
    readonly detail: string;
  };
}

/** B2 validates REQUIRED identity, exact session/header, PVC and every retained root before
 * returning this binding. transportMachine is the retained session-scoped credential identity,
 * never the successor's physical-machine global identity. */
export interface PortableSessionBinding {
  readonly evidence: PortableIdentityEvidence;
  readonly sessionFile: string;
  readonly stateDirectory: string;
  readonly transportMachine: string;
}

export interface PortableIdentityAdapter {
  validate(context: ExtensionContext): Promise<PortableSessionBinding>;
  /** Validate the *current exact* native predecessor receipt under lease receipt custody.
   * Called even when absent; REQUIRED state must not be mistaken for a fresh session. */
  verifyPredecessor(binding: PortableSessionBinding, predecessor: TransportHostLeasePredecessorSnapshot, signal: AbortSignal): Promise<void>;
}

/** Session-scoped activation snapshot; never copied from a physical host's global identity. */
export interface SessionRecoveryActivation {
  readonly binding: PortableSessionBinding;
  readonly identity: PortableIdentityAdapter;
}

export interface RecoveryAcceptedInput {
  readonly inputId: string;
  readonly text: string;
  readonly podId: string;
  readonly bootId: string;
  readonly settlement: "open" | "completed" | "cancelled";
  /** Complete retained cancel history, not absence of an in-memory abort flag. */
  readonly cancellation: "not-cancelled" | "cancelled" | "unknown";
}

export interface RecoveryInputSnapshot {
  readonly sessionId: string;
  /** False for missing/corrupt admission, settlement or cancellation history. */
  readonly complete: boolean;
  readonly accepted?: RecoveryAcceptedInput;
}

export interface ContinuationRequest {
  readonly sessionId: string;
  readonly originalInputId: string;
  readonly recoveryGeneration: string;
  readonly text: string;
}

export interface ContinuationAdmission {
  readonly admissionId: string;
  readonly existing: boolean;
}

/** B5 owns the durable obligation and eventual native drain. admitContinuation is STORAGE
 * ONLY: it is called behind the native input fence and must not submit/queue a native prompt
 * there. Returning existing reconciles the same retained admission, including accepted but
 * unreceipted work; it never appends another carrier or resets a deadline. B5/B6 own the
 * controlled post-recovery drain and authoritative carrier-commit lookup, not a volatile
 * sendMessage queue. A storage admission receipt alone is not native carrier commitment. */
export interface RecoveryInputAdapter {
  readRecoveryInput(sessionId: string): Promise<RecoveryInputSnapshot>;
  admitContinuation(request: ContinuationRequest): Promise<ContinuationAdmission>;
}

export interface RecoveryDurableAppendRequest {
  readonly sessionId: string;
  readonly deliveryId: string;
  readonly recordKind: "control-seen" | "carrier";
  /** Original absolute Unix-epoch deadline in milliseconds; retries do not restamp it. */
  readonly deadlineMs: number;
  readonly signal?: AbortSignal;
}

export type RecoveryDurableAppendResult =
  | { readonly durable: true; readonly seq: number }
  | { readonly durable: false; readonly reason: "deadline" | "aborted" | "store-unavailable" | "superseded" | "not-found" };

export interface RecoveryDurableEntryRequest {
  readonly sessionId: string;
  readonly deliveryId: string;
  readonly recordKind: "control-seen" | "carrier";
}

export type RecoveryDurableEntryResult =
  | { readonly found: true; readonly seq: number; readonly bytes: Uint8Array }
  | { readonly found: false; readonly reason: "not-found" | "store-unavailable" | "superseded" };

/** Captured by the native serializer, never reconstructed from a mutable event or alias. */
export interface RecoveryNativeEntryLocation {
  /** Exact B2/native-validated retained file. */
  readonly file: string;
  readonly offset: number;
  /** UTF-8 JSON byte length EXCLUDING the terminating LF. */
  readonly length: number;
}

/** Native Pi is the sole writer/fsync owner of its backing file. Ordinary receipts cover
 * bytesThrough >= offset + length + 1 after file fsync/fdatasync and required directory
 * fsync; namespaceDurable also covers an already durable unchanged namespace. Owned
 * receipts come from the actual successful NONEMPTY committed append: bytes/sha256 cover
 * its trusted committed prefix, not just this entry. currentReceipt/empty append proves
 * custody only, never a previously unknown append's durability. Recovery must establish
 * committed-append evidence rather than manufacture a receipt from a later stat/read. */
export type RecoveryNativeFlushReceipt =
  | { readonly kind: "file-fsync"; readonly file: string; readonly bytesThrough: number; readonly namespaceDurable: true }
  | { readonly kind: "owned-journal-commit"; readonly file: string; readonly bytes: number; readonly sha256: string };

export interface RecoveryNativeEntryRegistrationRequest {
  readonly sessionId: string;
  /** User carrier: original B5 deliveryId. Other native entries: pi-native-entry:v1: plus
   * lower-hex SHA-256 of UTF-8 JSON.stringify([sessionId, entryId]). B5 must reject IDs
   * beginning with that reserved prefix; these references do not admit new inputs. */
  readonly deliveryId: string;
  readonly recordKind: "carrier";
  readonly entryId: string;
  readonly location: RecoveryNativeEntryLocation;
  /** Exact serializer-emitted JSON bytes PLUS final LF; B6 must copy this mutable array. */
  readonly bytes: Uint8Array;
  /** Lower-case hex SHA-256 of bytes, INCLUDING LF (not the extracted message hash). */
  readonly bytesSha256: string;
  readonly flushReceipt: RecoveryNativeFlushReceipt;
  /** Absolute Unix milliseconds, bounded by the original input/lifecycle deadline. */
  readonly deadlineMs: number;
  readonly signal?: AbortSignal;
}

export type RecoveryNativeEntryRegistrationResult =
  | { readonly registered: true; readonly existing: boolean }
  | { readonly registered: false; readonly reason: "conflict" | "deadline" | "aborted" | "store-unavailable" | "superseded" };

/** B6's bounded post-append barrier, before application/broker ACK or carrier commitment.
 * durable:true means WRITTEN AND FSYNCED (or the store's equivalent commit), not merely
 * enqueued. Idempotent per (sessionId, deliveryId, recordKind): return the same seq and
 * never append twice. Expired deadlines return deadline; an aborted signal returns aborted.
 * Carrier correlation is post-append: a message_end observation is not a commit receipt.
 * REQUIRED activation needs successful open/recovery of B6's authoritative committed log
 * for the exact bound store/session, not merely a registered/cached adapter object. These
 * ports define an owner contract; they do not implement a store or activate composition. */
export interface RecoveryDurabilityAdapter {
  awaitDurableAppend(request: RecoveryDurableAppendRequest): Promise<RecoveryDurableAppendResult>;
  /** Exact retained UTF-8 serialized native/store bytes after tool_result hooks, not a
   * parsed/reserialized message. Registered native entries include their final LF. After
   * successful REQUIRED committed-log recovery, not-found authoritatively means no committed
   * record for this exact tuple. Cache misses, incomplete reconstruction, inaccessible
   * ranges, uncertain reads or lost availability MUST return store-unavailable; lost
   * availability invalidates negative authority until successful recovery. Superseded
   * bindings return superseded. B6 absence alone neither proves native-file absence nor
   * permits reappend/raw-result fallback; complete B5/native evidence is still required. */
  readDurableEntry(request: RecoveryDurableEntryRequest): Promise<RecoveryDurableEntryResult>;
  /** Call ONLY after persistenceReleased resolves, outside ordinary/owned persistence and
   * terminal queues, from the B5/B3 drain. Validate bound session/file, byte bounds, length
   * (bytes.length === location.length + 1), final LF, hash, parsed actual entryId and native
   * receipt coverage/provenance. An invalid/incompatible receipt refuses. Immutable binding:
   * (sessionId, deliveryId, 'carrier') -> (entryId, location, bytes, bytesSha256). Copy bytes;
   * identical repeats preserve the barrier seq, changed bindings conflict without overwrite.
   * A later valid larger-prefix receipt may strengthen the SAME entry without changing its
   * binding/seq. Deadline/signal do not change identity. registered:true is correlation ONLY,
   * NEVER durability/ACK; awaitDurableAppend commits B6's own retained bytes/index/seq.
   * Native alone scans/recovers its append domain and re-emits flush-qualified descriptors;
   * B6 never opens/scans/appends/fsyncs Pi files or reenters Pi persistence. Conflicts on
   * the unchanged barrier/read remain superseded with diagnostics, not a new reason. */
  registerNativeEntry(request: RecoveryNativeEntryRegistrationRequest): Promise<RecoveryNativeEntryRegistrationResult>;
}

export interface SettledRecoveryEffect {
  readonly toolCallId: string;
  readonly key: string;
  readonly outcome: "committed" | "not-committed" | "uncertain";
  /** SHA-256 of the full alias-bound message's UTF-8 JSON.stringify bytes, with no
   * replacer, indentation, key sorting or trailing newline. Required for committed.
   * Each alias has its OWN hash (toolCallId AND original timestamp are included); same-K
   * consistency omits ONLY top-level toolCallId and timestamp as alias/attempt correlation.
   * Every other field, including nested IDs/timestamps, remains part of that comparison. */
  readonly resultBytesHash?: string;
}

export interface RecoveryEffectsSettlement {
  readonly settled: readonly SettledRecoveryEffect[];
  readonly uncertain: number;
}

type RecoveryMessage = ContextEvent["messages"][number];
export type RecoveryToolResult = Extract<RecoveryMessage, { role: "toolResult" }>;

export type RecoveryToolResultSource = "native-entry" | "adapter-original";

export interface RecoveryToolResultRead {
  readonly message: RecoveryToolResult;
  readonly source: RecoveryToolResultSource;
  readonly postHook: boolean;
}

/** Later native boundary supplies the SAME current live tool_result transformation path,
 * with strict error propagation (ordinary observer dispatch that swallows errors is NOT
 * sufficient). Resolve the original call/input for the hook; never execute the tool again.
 * Receives a detached message: raw adapter/native history must not be mutated. */
export type RecoveryToolResultTransform = (
  message: RecoveryToolResult, effect: SettledRecoveryEffect,
) => Promise<RecoveryToolResult>;

/** Retained per-call materialization pin in the existing continuation marker. Receipts
 * form a monotonic union under the same generation/text/admission: new materialized calls
 * are added durably before admission; returned pinned calls must reproduce hash AND source.
 * Historical pins are never erased when settlement/context omits a call, and do not cause
 * that call to be materialized into a context which no longer contains it. */
export interface RecoveryMaterializedResultReceipt {
  readonly toolCallId: string;
  readonly source: RecoveryToolResultSource;
  /** SHA-256 of exactly the UTF-8 JSON.stringify message installed into context. */
  readonly resultBytesHash: string;
}

/** B4 settles immutable saved intents before any model continuation. A nested fabric_exec
 * program is never replayed wholesale. Prefer native-entry with postHook=true. An
 * adapter-original MUST undergo the native strict transformation callback regardless of
 * its postHook flag. Unavailable/failed processing blocks, never exposes raw fallback.
 * Duplicate (toolCallId,key) rows are invalid. Same-K aliases must agree on outcome and
 * their complete materialized result apart from ONLY top-level toolCallId and timestamp,
 * which correlate physical aliases/attempts. Each original timestamp/identity is preserved
 * by strict transformation and checked by its full per-alias hash. No other field, including
 * nested IDs/timestamps, is ignored; full-message hash/source pins are never relaxed. */
export interface RecoveryEffectAdapter {
  settlePendingIntents(sessionId: string, recoveryGeneration: string): Promise<RecoveryEffectsSettlement>;
  readToolResult(effect: SettledRecoveryEffect): Promise<RecoveryToolResultRead>;
}

/** B3 consumes these providers; unconfigured ordinary sessions do not activate recovery. */
export interface RecoveryIo { readonly deadlineMs?: number; readonly signal?: AbortSignal }
export interface RecoveryTaskAbort { readonly requestId: string; readonly at: number; readonly bootId: string; readonly originalInputIds: readonly string[]; readonly maintenanceRequestId: string; committed: boolean }
/** Pinned B5 maintenance-admission port; B3 must not implement a second admission ledger. */
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
export type RecoveryGateSubject = Readonly<import("@earendil-works/pi-coding-agent").AcceptedRecoveryInput | AcceptedRecoveryMaintenance>;
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


export interface SessionRecoveryAdapters {
  readonly maintenance: RecoveryMaintenanceAdapter;
  readonly identity: PortableIdentityAdapter;
  readonly inputs: RecoveryInputAdapter;
  readonly effects: RecoveryEffectAdapter;
}

export const SESSION_RECOVERY_ADAPTERS_GET = "fabric:session-recovery:adapters:get";

/** Register providers during extension registration, before session_start. Each owner may
 * supply its own part. This is the one B5 registration point; it needs no extension.ts edits.
 * Registration carries in-process APIs, not identity/ownership assertions from messages. */
export function registerSessionRecoveryAdapters(
  pi: Pick<ExtensionAPI, "events">,
  adapters: Partial<SessionRecoveryAdapters>,
): () => void {
  return pi.events.on(SESSION_RECOVERY_ADAPTERS_GET, (event: unknown) => {
    if (!event || typeof event !== "object" || !("accept" in event) || typeof event.accept !== "function") return;
    event.accept(adapters);
  });
}

/** Synchronous discovery: all extension factories register before startup is emitted. No
 * provider means ordinary startup remains inert; a partial/conflicting composition is an
 * explicit integration error, never an invented no-op settle/admission implementation. */
export function sessionRecoveryAdapters(pi: Pick<ExtensionAPI, "events">): SessionRecoveryAdapters | undefined {
  const adapters: Partial<SessionRecoveryAdapters> = {};
  let conflict: string | undefined;
  let invalid = false;
  pi.events.emit(SESSION_RECOVERY_ADAPTERS_GET, { accept: (parts: Partial<SessionRecoveryAdapters>) => {
    if (!parts || typeof parts !== "object" || Array.isArray(parts)) { invalid = true; return; }
    for (const key of ["identity", "inputs", "effects", "maintenance"] as const) {
      const adapter = parts[key];
      if (!adapter) continue;
      // Pi's event bus catches listener errors. Record the conflict here and throw outside
      // the callback; otherwise a conflicting composition could silently choose a writer.
      if (adapters[key] && adapters[key] !== adapter) conflict = key;
      Object.assign(adapters, { [key]: adapter });
    }
  } });
  if (invalid) throw new Error("Malformed session recovery provider registration");
  if (conflict) throw new Error(`Conflicting session recovery ${conflict} providers`);
  if (Object.keys(adapters).length === 0) return undefined;
  if (!adapters.identity || !adapters.inputs || !adapters.effects || !adapters.maintenance) throw new Error("Session recovery requires identity, inputs, effects and maintenance providers");
  for (const name of ["admitMaintenance", "beginMaintenance", "prepareMaintenance", "consumeMaintenance", "adoptMaintenance", "read", "listPending", "withMaintenanceAttempt", "assertMaintenanceGate", "cancelMaintenance", "reconcileAfterJoin", "captureTaskAbort", "releaseLostMaintenance"] as const) {
    if (typeof adapters.maintenance[name] !== "function") throw new Error("Malformed session recovery maintenance API");
  }
  if (typeof adapters.identity.validate !== "function" || typeof adapters.identity.verifyPredecessor !== "function" ||
    typeof adapters.inputs.readRecoveryInput !== "function" || typeof adapters.inputs.admitContinuation !== "function" ||
    typeof adapters.effects.settlePendingIntents !== "function" || typeof adapters.effects.readToolResult !== "function") {
    throw new Error("Malformed session recovery provider API");
  }
  return adapters as SessionRecoveryAdapters;
}
