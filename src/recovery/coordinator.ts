import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { repairRecoveryContext } from "./context-repair.js";
import type {
  ContinuationAdmission, ContinuationRequest, PortableSessionBinding, RecoveryAcceptedInput,
  RecoveryEffectsSettlement, RecoveryInputSnapshot, RecoveryToolResult, SessionRecoveryAdapters,
  RecoveryMaterializedResultReceipt, RecoveryToolResultTransform,
} from "./l7-interfaces.js";

type RecoveryContext = Parameters<SessionRecoveryAdapters["identity"]["validate"]>[0];
export type SessionRecoveryClassification = "interrupted" | "clean" | "completed" | "cancelled" | "unchanged-boot";

export interface SessionRecoveryOptions {
  /** Exact native session identity/path; aliases/normalization are not matches. */
  readonly sessionId: string;
  readonly sessionFile: string;
  /** Optional independently known root. B2 owns REQUIRED root validation; its returned
   * root must always be absolute and, when supplied here, must match exactly. */
  readonly stateDirectory?: string;
  /** Parent owns actual portable lease acquisition, including predecessor receipt custody.
   * Must resolve only after acquisition, and reject on loss/refusal. Called AFTER B2 and
   * BEFORE reading work or settling effects. This coordinator never invents lease success. */
  readonly activateSuccessor: (binding: PortableSessionBinding) => Promise<void>;
  /** Native boundary's live strict hook path, required ONLY for adapter-original reads.
   * No implementation/fallback is invented here; absence or any error blocks recovery. */
  readonly transformToolResult?: RecoveryToolResultTransform;
}

export interface PreparedSessionRecovery {
  readonly binding: PortableSessionBinding;
  readonly classification: SessionRecoveryClassification;
  /** Detached, frozen post-hook native results for exact calls in the original active
   * provider context, including truthful errors for not-committed outcomes, one per ID.
   * Nested/non-native aliases are checked but never invented as provider results.
   * Parent installs a provider-only view; raw history is never rewritten/truncated. */
  readonly results: readonly RecoveryToolResult[];
  readonly continuation?: Readonly<ContinuationRequest>;
  /** One-shot per preparation, including failures. A retry creates a fresh preparation,
   * revalidates identity/lease/history/effects, and uses the SAME durable generation/text.
   * beforeAdmission is synchronous context installation, NOT native-fence release. Keep
   * the native fence held through B5's durable storage-only admission and marker receipt.
   * Parent/B5 own subsequent native drain. No native send/submit happens in this module. */
  admitContinuation(beforeAdmission: () => void): Promise<ContinuationAdmission | undefined>;
}

interface ContinuationMarker {
  readonly format: 1;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly stateDirectory: string;
  readonly originalInputId: string;
  readonly originalText: string;
  readonly originalPodId: string;
  readonly originalBootId: string;
  readonly recoveryGeneration: string;
  readonly text: string;
  readonly admissionId?: string;
  /** Monotonic per-call receipt union; legacy markers/new calls are persisted before admission.
   * Historical receipts remain even when settlement or the active view omits their calls. */
  readonly resultReceipts?: readonly RecoveryMaterializedResultReceipt[];
}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const nonempty = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
const hash = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const generation = (value: unknown): value is string =>
  typeof value === "string" && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value);

function assertBinding(binding: PortableSessionBinding, expected: SessionRecoveryOptions): void {
  if (!binding || !binding.evidence || !nonempty(expected.sessionId) ||
      !path.isAbsolute(expected.sessionFile) || !nonempty(binding.stateDirectory) || !path.isAbsolute(binding.stateDirectory) ||
      binding.evidence.sessionId !== expected.sessionId || binding.sessionFile !== expected.sessionFile ||
      (expected.stateDirectory !== undefined && binding.stateDirectory !== expected.stateDirectory) || !nonempty(binding.transportMachine)) {
    throw new Error("Recovery identity does not match exact sessionId/sessionFile/stateDirectory");
  }
  const evidence = binding.evidence;
  if (!nonempty(evidence.predecessorPodId) || !nonempty(evidence.predecessorBootId) ||
      !nonempty(evidence.successorPodId) || !nonempty(evidence.successorBootId) ||
      !evidence.exclusionProof || !["kubelet-fence", "volume-detach", "lease-absent"].includes(evidence.exclusionProof.kind)) {
    throw new Error("Recovery requires validated B2 writer exclusion evidence");
  }
  // These are evidence discriminators, not proofs this module can establish. In particular,
  // lease absence ALONE cannot fence files: B2 validates the native physical exclusion chain,
  // and the mandatory parent callback verifies the predecessor under actual lease custody.
  if (typeof expected.activateSuccessor !== "function") throw new Error("Recovery requires actual parent lease activation");
}

function checkedSnapshot(value: RecoveryInputSnapshot, sessionId: string): RecoveryInputSnapshot {
  if (!record(value) || value.sessionId !== sessionId || value.complete !== true) {
    throw new Error("Recovery input history is incomplete, corrupt or bound to another session");
  }
  if (value.accepted === undefined) return value;
  const accepted = value.accepted;
  if (!record(accepted) || !nonempty(accepted.inputId) || typeof accepted.text !== "string" ||
      !nonempty(accepted.podId) || !nonempty(accepted.bootId) ||
      !["open", "completed", "cancelled"].includes(accepted.settlement) ||
      !["not-cancelled", "cancelled", "unknown"].includes(accepted.cancellation) ||
      accepted.cancellation === "unknown") {
    throw new Error("Recovery accepted/settlement/cancellation history is corrupt or cancellation is unknown");
  }
  return value;
}

function classify(snapshot: RecoveryInputSnapshot, bootId: string): SessionRecoveryClassification {
  const accepted = snapshot.accepted;
  if (!accepted) return "clean";
  if (accepted.cancellation === "cancelled" || accepted.settlement === "cancelled") return "cancelled";
  if (accepted.settlement === "completed") return "completed";
  if (accepted.bootId === bootId) return "unchanged-boot";
  return "interrupted";
}

const immutableInput = (accepted: RecoveryAcceptedInput) => ({
  inputId: accepted.inputId, text: accepted.text, podId: accepted.podId, bootId: accepted.bootId,
});

const markerPath = (binding: PortableSessionBinding, inputId: string): string =>
  path.join(binding.stateDirectory, "recovery", "continuations", `${createHash("sha256").update(inputId).digest("hex")}.json`);

function readMarker(file: string, binding: PortableSessionBinding, accepted: RecoveryAcceptedInput): ContinuationMarker | undefined {
  let value: unknown;
  try {
    // A dangling symlink is not a missing marker; never write through a marker alias.
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("not a regular marker file");
    value = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error(`Recovery continuation marker unreadable/corrupt: ${file}`, { cause: error });
  }
  const fields = ["format", "sessionId", "sessionFile", "stateDirectory", "originalInputId", "originalText",
    "originalPodId", "originalBootId", "recoveryGeneration", "text", "admissionId", "resultReceipts"];
  if (!record(value) || Object.keys(value).some(key => !fields.includes(key)) || value.format !== 1 ||
      value.sessionId !== binding.evidence.sessionId || value.sessionFile !== binding.sessionFile ||
      value.stateDirectory !== binding.stateDirectory || value.originalInputId !== accepted.inputId ||
      value.originalText !== accepted.text || value.originalPodId !== accepted.podId || value.originalBootId !== accepted.bootId ||
      !generation(value.recoveryGeneration) || !nonempty(value.text) ||
      ("admissionId" in value && !nonempty(value.admissionId)) ||
      ("resultReceipts" in value && (!Array.isArray(value.resultReceipts) || value.resultReceipts.some(receipt =>
        !record(receipt) || Object.keys(receipt).length !== 3 || !nonempty(receipt.toolCallId) ||
        !["native-entry", "adapter-original"].includes(receipt.source as string) || !hash(receipt.resultBytesHash)) ||
        new Set(value.resultReceipts.map(receipt => receipt.toolCallId)).size !== value.resultReceipts.length))) {
    throw new Error(`Recovery continuation marker malformed or identity conflict: ${file}`);
  }
  return value as unknown as ContinuationMarker;
}

function sameMarker(left: ContinuationMarker, right: ContinuationMarker): boolean {
  const { admissionId: _left, ...a } = left, { admissionId: _right, ...b } = right;
  return isDeepStrictEqual(a, b);
}

function checkedSettlement(value: RecoveryEffectsSettlement): RecoveryEffectsSettlement {
  if (!record(value) || !Array.isArray(value.settled) || !Number.isSafeInteger(value.uncertain) || value.uncertain < 0) {
    throw new Error("Recovery effect settlement is malformed");
  }
  const aliases = new Set<string>(), outcomes = new Map<string, string>();
  for (const effect of value.settled) {
    if (!record(effect) || !nonempty(effect.toolCallId) || !nonempty(effect.key) ||
        !nonempty(effect.outcome) || !["committed", "not-committed", "uncertain"].includes(effect.outcome) ||
        (effect.outcome === "committed" && !hash(effect.resultBytesHash)) ||
        (effect.resultBytesHash !== undefined && !hash(effect.resultBytesHash))) {
      throw new Error("Recovery effect settlement is malformed, conflicting or missing a committed result hash");
    }
    const pair = JSON.stringify([effect.toolCallId, effect.key]);
    if (aliases.has(pair)) throw new Error("Recovery effect settlement has a duplicate (alias,key)");
    aliases.add(pair);
    const outcome = outcomes.get(effect.key);
    if (outcome !== undefined && outcome !== effect.outcome) throw new Error("Recovery same-key outcome conflict is uncertain");
    outcomes.set(effect.key, effect.outcome);
  }
  if (value.uncertain !== 0 || value.settled.some(effect => effect.outcome === "uncertain")) {
    throw new Error("Recovery effect settlement is uncertain; continuation is blocked");
  }
  return value;
}

function checkedResult(value: RecoveryToolResult, toolCallId: string): RecoveryToolResult {
  if (!record(value) || value.role !== "toolResult" || value.toolCallId !== toolCallId || !nonempty(value.toolName) ||
      !Array.isArray(value.content) || value.content.some(content => !record(content) ||
        (content.type === "text" ? typeof content.text !== "string" :
          content.type === "image" ? typeof content.data !== "string" || !nonempty(content.mimeType) : true)) ||
      typeof value.isError !== "boolean" || !Number.isFinite(value.timestamp)) {
    throw new Error(`Recovery original native tool result is malformed or mismatched: ${toolCallId}`);
  }
  return value;
}

/** JSON is the byte contract: no replacer/spacing/sorting/newline, UTF-8, full alias ID.
 * Parse the serialization to detach adapter aliases, then freeze the exact message hashed
 * and installed. Hooks receive a separate detached copy and cannot mutate raw history. */
function snapshotResult(value: RecoveryToolResult, toolCallId: string): RecoveryToolResult {
  checkedResult(value, toolCallId);
  return checkedResult(JSON.parse(JSON.stringify(value)), toolCallId);
}

function freezeResult<T>(value: T): T {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freezeResult(child);
    Object.freeze(value);
  }
  return value;
}

/** Accepted startup SEQUENCE, not a scheduler or a new authority. Parent must hold ordinary
 * input/inbox/native gates throughout preparation and admission, fail them closed on error,
 * and retain ownership/lease through native drain. B2 and the real lease callback establish
 * ownership; this code cannot qualify physical fencing, lease expiry or cross-host runtime. */
export async function prepareSessionRecovery(
  context: RecoveryContext,
  adapters: SessionRecoveryAdapters,
  options: SessionRecoveryOptions,
): Promise<PreparedSessionRecovery> {
  const expected = { ...options };
  const validated = await adapters.identity.validate(context);
  assertBinding(validated, expected);
  // Copy AND freeze before the first activation await. The provider can retain/mutate its
  // original object; neither that alias nor the callback may redirect validated roots or
  // change the boot/receipt/exclusion evidence used by the rest of this preparation.
  const binding: PortableSessionBinding = Object.freeze({
    ...validated,
    evidence: Object.freeze({
      ...validated.evidence,
      predecessorReceipt: Object.freeze({ ...validated.evidence.predecessorReceipt }),
      exclusionProof: Object.freeze({ ...validated.evidence.exclusionProof }),
    }),
  });
  await expected.activateSuccessor(binding);
  const sessionId = binding.evidence.sessionId, bootId = binding.evidence.successorBootId;
  const snapshot = checkedSnapshot(await adapters.inputs.readRecoveryInput(sessionId), sessionId);
  // Snapshot immutable input bytes before invoking any asynchronous adapter.
  const accepted = snapshot.accepted ? { ...snapshot.accepted } : undefined;
  const classification = classify(snapshot, bootId);
  const file = accepted ? markerPath(binding, accepted.inputId) : undefined;
  const prior = accepted && file ? readMarker(file, binding, accepted) : undefined;
  const recoveryGeneration = prior?.recoveryGeneration ?? randomUUID();
  // Classification controls continuation, NOT the obligation to settle already-issued
  // intents. Reuse B4's settle port even for cancelled/completed/same-boot/clean history;
  // it owns recording verdicts, must not restart unissued slots, and uncertainty throws.
  // These non-interrupted paths never read/materialize results or create a continuation.
  const settlement = checkedSettlement(await adapters.effects.settlePendingIntents(sessionId, recoveryGeneration));
  if (classification !== "interrupted") {
    const idle = Promise.resolve(undefined);
    return { binding, classification, results: [], admitContinuation: () => idle };
  }
  // Classification above proves presence; cancelled/completed/unchanged boots never get here.
  if (!accepted || !file) throw new Error("Recovery interrupted input is missing");
  // Pi's native active-branch, compaction-aware projection is the source of actual calls.
  // getEntries()/all physical rows would include abandoned branches. Never infer native
  // IDs from B4 aliases or strip provider suffixes such as |fc_item.
  const originalMessages = structuredClone(context.sessionManager.buildSessionProjection().messages);
  const nativeCalls = new Set(originalMessages.flatMap(message => message.role === "assistant"
    ? message.content.filter(part => part.type === "toolCall").map(part => part.id) : []));
  const results = new Map<string, RecoveryToolResult>();
  const byKey = new Map<string, Omit<RecoveryToolResult, "toolCallId" | "timestamp">>();
  const receiptsByCall = new Map<string, RecoveryMaterializedResultReceipt>(
    (prior?.resultReceipts ?? []).map(receipt => [receipt.toolCallId, receipt]),
  );
  for (const effect of settlement.settled) {
    const read = await adapters.effects.readToolResult(effect);
    if (!record(read) || !["native-entry", "adapter-original"].includes(read.source as string) ||
        typeof read.postHook !== "boolean" || (read.source === "native-entry" && read.postHook !== true)) {
      throw new Error(`Recovery tool result provenance/post-hook state is invalid: ${effect.toolCallId}`);
    }
    const original = snapshotResult(read.message, effect.toolCallId);
    let result = original;
    if (read.source === "adapter-original") {
      if (typeof expected.transformToolResult !== "function") throw new Error("Recovery adapter-original requires the live strict tool_result hook transformation");
      // Do NOT catch: strict errors propagate and never permit raw fallback. An adapter's
      // postHook flag cannot bypass current live processing. Preserve native identity/error.
      result = snapshotResult(await expected.transformToolResult(snapshotResult(original, effect.toolCallId), effect), effect.toolCallId);
      if (result.toolName !== original.toolName || result.timestamp !== original.timestamp || (original.isError && !result.isError)) {
        throw new Error(`Recovery tool result transformation changed original identity/error: ${effect.toolCallId}`);
      }
    }
    const resultBytesHash = createHash("sha256").update(JSON.stringify(result), "utf8").digest("hex");
    if (read.source === "native-entry" && effect.resultBytesHash !== undefined && effect.resultBytesHash !== resultBytesHash) {
      throw new Error(`Recovery tool result hash mismatch: ${effect.toolCallId}`);
    }
    // Full alias hashes bind the physical ID AND original native timestamp. Only the
    // common same-K comparison omits these top-level correlation fields; nested fields,
    // content, toolName, isError, details and every other materialized field still agree.
    const { toolCallId: _alias, timestamp: _timestamp, ...comparable } = result;
    const previousKey = byKey.get(effect.key);
    if (previousKey && !isDeepStrictEqual(previousKey, comparable)) throw new Error(`Recovery same-key result conflict is uncertain: ${effect.key}`);
    byKey.set(effect.key, comparable);
    const receipt = { toolCallId: effect.toolCallId, source: read.source, resultBytesHash } as const;
    const previousReceipt = receiptsByCall.get(effect.toolCallId);
    // Revalidate any returned historical call even if compaction/branching hides it.
    // Absence from settlement is not erasure, nor permission to resurrect it in context.
    if (previousReceipt && !isDeepStrictEqual(previousReceipt, receipt)) throw new Error(`Recovery materialized result receipt hash/source provenance conflict: ${effect.toolCallId}`);
    if (!nativeCalls.has(effect.toolCallId)) continue;
    const previous = results.get(effect.toolCallId);
    if (previous && JSON.stringify(previous) !== JSON.stringify(result)) throw new Error(`Recovery tool result conflict: ${effect.toolCallId}`);
    if (!previousReceipt) receiptsByCall.set(effect.toolCallId, receipt);
    results.set(effect.toolCallId, freezeResult(result));
  }
  // Retain the existing repair path's exact binding and whole-assistant settlement checks
  // before writing any receipt. Parent still installs/validates its final provider view.
  repairRecoveryContext(originalMessages, [...results.values()]);
  const resultReceipts = [...receiptsByCall.values()].sort((a, b) => a.toolCallId < b.toolCallId ? -1 : a.toolCallId > b.toolCallId ? 1 : 0);
  const marker: ContinuationMarker = prior ? { ...prior, resultReceipts } : {
    format: 1, sessionId, sessionFile: binding.sessionFile, stateDirectory: binding.stateDirectory,
    originalInputId: accepted.inputId, originalText: accepted.text, originalPodId: accepted.podId, originalBootId: accepted.bootId,
    recoveryGeneration, resultReceipts,
    text: `Continue interrupted logical input ${JSON.stringify(accepted.inputId)} using the repaired original tool results. ` +
      "Do not recreate committed effects or replay the interrupted program. Finish only the remaining authorized work.\n\n" + accepted.text,
  };
  const current = readMarker(file, binding, accepted);
  if (prior ? !current || !isDeepStrictEqual(prior, current) : current !== undefined) {
    throw new Error("Recovery continuation marker changed during preparation");
  }
  // Durable file + directory barriers BEFORE B5 can enqueue anything, including new
  // distinct calls settled after continuation progress under this SAME generation/text/id.
  // Raw session/predecessor receipt files are never written. Retry this exact request.
  if (!prior || !isDeepStrictEqual(prior.resultReceipts, resultReceipts)) writeJsonAtomic(file, marker, { durable: true });
  const continuation: Readonly<ContinuationRequest> = Object.freeze({
    sessionId, originalInputId: marker.originalInputId, recoveryGeneration: marker.recoveryGeneration, text: marker.text,
  });
  let admission: Promise<ContinuationAdmission | undefined> | undefined;
  const admit = async (beforeAdmission: () => void): Promise<ContinuationAdmission | undefined> => {
    // Last asynchronous read before synchronous context install and B5 admission. Missing,
    // changed or unknown history is never "no cancel". B5 owns the serialized final native
    // cancellation/admission boundary; this reread cannot fence races inside its adapter.
    const latest = checkedSnapshot(await adapters.inputs.readRecoveryInput(sessionId), sessionId);
    if (!latest.accepted || !isDeepStrictEqual(immutableInput(latest.accepted), immutableInput(accepted))) {
      throw new Error("Recovery accepted input changed before continuation admission");
    }
    if (classify(latest, bootId) !== "interrupted") return undefined;
    const retained = readMarker(file, binding, accepted);
    if (!retained || !sameMarker(marker, retained) ||
        (marker.admissionId !== undefined && marker.admissionId !== retained.admissionId)) {
      throw new Error("Recovery continuation marker changed before admission");
    }
    const installed: unknown = beforeAdmission();
    if (installed !== null && (typeof installed === "object" || typeof installed === "function") && "then" in installed) {
      throw new Error("Recovery context installation must be synchronous");
    }
    const returned = await adapters.inputs.admitContinuation(continuation);
    if (!record(returned) || !nonempty(returned.admissionId) || typeof returned.existing !== "boolean") {
      throw new Error("Recovery continuation admission is malformed");
    }
    if (retained.admissionId !== undefined && retained.admissionId !== returned.admissionId) {
      throw new Error("Recovery continuation admission id conflict");
    }
    const after = readMarker(file, binding, accepted);
    if (!after || !sameMarker(marker, after)) throw new Error("Recovery continuation marker changed during admission");
    if (after.admissionId !== undefined && after.admissionId !== returned.admissionId) {
      throw new Error("Recovery continuation admission id conflict");
    }
    if (after.admissionId === undefined) writeJsonAtomic(file, { ...after, admissionId: returned.admissionId }, { durable: true });
    return { admissionId: returned.admissionId, existing: returned.existing };
  };
  return {
    binding, classification, results: Object.freeze([...results.values()]), continuation,
    admitContinuation: beforeAdmission => admission ??= admit(beforeAdmission),
  };
}
