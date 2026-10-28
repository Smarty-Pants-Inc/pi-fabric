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

/** B6's bounded post-append barrier, before application/broker ACK or carrier commitment.
 * durable:true means WRITTEN AND FSYNCED (or the store's equivalent commit), not merely
 * enqueued. Idempotent per (sessionId, deliveryId, recordKind): return the same seq and
 * never append twice. Expired deadlines return deadline; an aborted signal returns aborted.
 * Carrier correlation is post-append: a message_end observation is not a commit receipt. */
export interface RecoveryDurabilityAdapter {
  awaitDurableAppend(request: RecoveryDurableAppendRequest): Promise<RecoveryDurableAppendResult>;
  /** Exact retained UTF-8 serialized native/store bytes after tool_result hooks, not a
   * parsed/reserialized message. A missing entry is not permission to expose raw results. */
  readDurableEntry(request: RecoveryDurableEntryRequest): Promise<RecoveryDurableEntryResult>;
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
export interface SessionRecoveryAdapters {
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
    for (const key of ["identity", "inputs", "effects"] as const) {
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
  if (!adapters.identity || !adapters.inputs || !adapters.effects) throw new Error("Session recovery requires identity, inputs and effects providers");
  if (typeof adapters.identity.validate !== "function" || typeof adapters.identity.verifyPredecessor !== "function" ||
    typeof adapters.inputs.readRecoveryInput !== "function" || typeof adapters.inputs.admitContinuation !== "function" ||
    typeof adapters.effects.settlePendingIntents !== "function" || typeof adapters.effects.readToolResult !== "function") {
    throw new Error("Malformed session recovery provider API");
  }
  return adapters as SessionRecoveryAdapters;
}
