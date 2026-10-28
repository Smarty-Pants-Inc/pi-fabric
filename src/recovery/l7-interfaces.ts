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

/** B6's bounded post-append barrier, before application/broker ACK or carrier commitment.
 * durable:true means WRITTEN AND FSYNCED (or the store's equivalent commit), not merely
 * enqueued. Idempotent per (sessionId, deliveryId, recordKind): return the same seq and
 * never append twice. Expired deadlines return deadline; an aborted signal returns aborted.
 * Carrier correlation is post-append: a message_end observation is not a commit receipt. */
export interface RecoveryDurabilityAdapter {
  awaitDurableAppend(request: RecoveryDurableAppendRequest): Promise<RecoveryDurableAppendResult>;
}

export interface SettledRecoveryEffect {
  readonly toolCallId: string;
  readonly key: string;
  readonly outcome: "committed" | "not-committed" | "uncertain";
  readonly resultBytesHash?: string;
}

export interface RecoveryEffectsSettlement {
  readonly settled: readonly SettledRecoveryEffect[];
  readonly uncertain: number;
}

type RecoveryMessage = ContextEvent["messages"][number];
export type RecoveryToolResult = Extract<RecoveryMessage, { role: "toolResult" }>;

/** B4 settles immutable saved intents before any model continuation. A nested fabric_exec
 * program is never replayed wholesale. The result reader materializes the original native
 * call's truthful result from durable bytes and verifies resultBytesHash. */
export interface RecoveryEffectAdapter {
  settlePendingIntents(sessionId: string, recoveryGeneration: string): Promise<RecoveryEffectsSettlement>;
  readToolResult(effect: SettledRecoveryEffect): Promise<RecoveryToolResult>;
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
